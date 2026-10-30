"""Reprotect: find the data files that hold, in plain text, values a table's protection masks now
(design §15.3, handbook 22 §22.15).

What a file holds is known without opening it:

- its name says its protection label - the revision its rows were masked under, then each earlier
  one `reprotect` rewrote them from (`lakehouse/files.py`);
- the table's record says how each field was masked under each revision (`FieldProtection.states`)
  and which policy each revision stood for (its `history`);
- its manifest entry says which fields it has values of (`LiveFile.valued_fields`).

A key's values were masked as the writer's walk masks them (`protection/plan.py`): by the state its
field had under that revision when the key had a field then, else by the rules and that revision's
policy. A field is born under the revision in force when the first schema holding it was committed
(`field_births`), so a writer still under an older revision decided its first values by the rules.
Rows `reprotect` rewrote hold, for each key, the first mask a revision of their label gave it,
oldest first.

A file needs rewriting when it holds values of a key the table's revision masks that every revision
of its label wrote plain (`Program`):

- a string field's values are masked in place;
- a number field's values - a key that forked when masking made its values strings - are masked into
  the family's string field and nulled where they were, which waits until that string field exists
  (the writers create it with the key's first masked value);
- a withheld kind's values are nulled;
- JSON text - `_unmapped`, and families collapsed to JSON - is walked as the writer walks a record.

Values masked under an earlier revision stay as they are, whatever the table masks them with now: a
hash cannot become a partial, nor a partial a hash, as there is no plain text to mask again. A file a
revision of whose label the record no longer describes (recorded before states were kept, or
dropped from a long history) is left alone, and counted.

`plan_reprotect` takes the oldest such files within the job's bounds; the maintenance app rewrites
each into one of its own (`maintenance_app/reprotect_worker.py`), named with the table's revision
ahead of the input's label, and commits them in one `replace` snapshot
(`lakehouse/maintenance.py::commit_reprotect`).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import json
import re
from typing import Any
import uuid

import pyarrow as pa

from pyiceberg.schema import Schema, index_by_id
from pyiceberg.table import Table
from pyiceberg.types import IcebergType, ListType, StringType

from midstream.lakehouse.commits import fork_map_of
from midstream.lakehouse.maintenance import MAX_PLAN_INPUT_FILES, LiveFile, live_data_files
from midstream.lakehouse.schema.conform import ROOT_VALUE_PATH, json_text, unmapped_text
from midstream.lakehouse.schema.forkmap import ELEMENT
from midstream.lakehouse.schema.index import ElementIndex, Family, Member, SchemaIndex, build_index
from midstream.lakehouse.tables import UNMAPPED_COLUMN, UNMAPPED_FIELD_ID
from midstream.protection.catalog import MASK_REDACT
from midstream.protection.mask import apply
from midstream.protection.plan import Decision, ProtectionPlan, family_visits, scalar_members
from midstream.protection.policy import UNKNOWN, ProtectionPolicy, TableProtection, protection_of_properties
from midstream.protection.rules import classify

#: What `ReprotectDecider` answers for a key: how its values are masked, None for plain, or `UNKNOWN`.
State = Any

#: The actions on one field's values.
MASK = "mask"
MOVE = "move"
WITHHOLD = "withhold"


def field_births(schemas: Iterable[Schema], protection: TableProtection) -> dict[int, int]:
    """Each field's revision of birth: the protection revision in force when the first schema holding
    it was committed, from the schema each revision was recorded at; 0 before every recorded one."""
    recorded = [(change.schema_id, change.revision) for change in protection.history if change.schema_id is not None]
    births: dict[int, int] = {}
    for schema in sorted(schemas, key=lambda item: item.schema_id):
        # A revision recorded at a schema came after it: a newer schema was committed under it.
        birth = max((revision for schema_id, revision in recorded if schema_id < schema.schema_id), default=0)
        for field_id in index_by_id(schema):
            births.setdefault(field_id, birth)
    return births


def holds_strings(member: Member) -> bool:
    """Whether a field holds strings, or lists of them: where masked values are."""
    field_type: IcebergType = member.type
    while isinstance(field_type, ListType):
        field_type = field_type.element_type
    return isinstance(field_type, StringType)


def value_id(member: Member) -> int:
    """The field id a file's statistics count the member's values under: its own, or its innermost
    list element's."""
    element: ElementIndex | None = member.element
    if element is None:
        return member.field_id
    while element.element is not None:
        element = element.element
    return element.element_id


def _list_depth(member: Member) -> int:
    depth, element = 0, member.element
    while element is not None:
        depth += 1
        element = element.element
    return depth


class ReprotectDecider:
    """How the writers masked each key under each revision, and what the table's revision masks."""

    def __init__(self, protection: TableProtection, index: SchemaIndex, births: Mapping[int, int]) -> None:
        self.protection = protection
        self.index = index
        self.births = births
        self.revision = protection.revision
        self._policies: dict[int, ProtectionPolicy] = {change.revision: change.policy for change in protection.history}
        self._policies[protection.revision] = protection.policy

    def policy_at(self, revision: int) -> ProtectionPolicy | None:
        if revision <= 0:
            return ProtectionPolicy()
        return self._policies.get(revision)

    def state_at(self, family: Family | None, parent: str | None, key: str, inherited: str | None, revision: int) -> State:
        """How a writer under `revision` masked the key's values: as `_recorded_or_ruled` decides,
        against the record as it was then and the fields born by then."""
        if revision <= 0:
            return None
        members = [member for member in scalar_members(family) if self.births.get(member.field_id, 0) <= revision]
        if members:
            for member in members:
                entry = self.protection.fields.get(member.field_id)
                if entry is None:
                    continue
                state = entry.state_at(revision)
                if state is UNKNOWN:
                    return UNKNOWN
                if state is not None:
                    return Decision(*state)
            return None
        policy = self.policy_at(revision)
        if policy is None:
            return UNKNOWN
        kind_id = classify(parent, key, inherited)
        mask = policy.mask_of(kind_id)
        return None if kind_id is None or mask is None else Decision(kind_id, mask)

    def lineage_state(self, family: Family | None, parent: str | None, key: str, inherited: str | None, label: tuple[int, ...]) -> State:
        """How rows of `label` hold the key's values: the first mask a revision of the label gave
        them, oldest first; None when every one wrote them plain."""
        for revision in reversed(label):
            state = self.state_at(family, parent, key, inherited, revision)
            if state is not None:
                return state
        return None

    def target(self, family: Family | None, parent: str | None, key: str, inherited: str | None) -> Decision | None:
        """What the table's revision masks the key with; None for plain."""
        state = self.state_at(family, parent, key, inherited, self.revision)
        return state if isinstance(state, Decision) else None

    def decide(self, family: Family | None, parent: str | None, key: str, inherited: str | None, label: tuple[int, ...]) -> State:
        """What reprotect does to the key's values in rows of `label`: mask them with the table's
        revision's mask when every revision of the label wrote them plain; None otherwise, or
        `UNKNOWN`."""
        target = self.target(family, parent, key, inherited)
        if target is None:
            return None
        earlier = self.lineage_state(family, parent, key, inherited, label)
        if earlier is UNKNOWN:
            return UNKNOWN
        return target if earlier is None else None

    def program(self, label: tuple[int, ...]) -> "Program":
        """What reprotect does to the rows of files of `label`."""
        actions: list[FieldAction] = []
        blocked: list[tuple[int, str]] = []
        unknown: list[int] = []
        gap = False
        for visit in family_visits(self.index.root, None, None):
            members = scalar_members(visit.family)
            if not members:
                continue
            key = visit.family.key
            target = self.target(visit.family, visit.parent, key, visit.inherited)
            if target is None:
                continue
            earlier = self.lineage_state(visit.family, visit.parent, key, visit.inherited, label)
            strings = [member for member in members if holds_strings(member)]
            for member in members:
                name = ".".join(part for part in member.logical_path if part != ELEMENT)
                if _list_depth(member) > 1 or member.path.count(ELEMENT) > 1:
                    # A list of lists, or a field in a struct in a list in a list: past what a rewrite walks.
                    blocked.append((value_id(member), f"{name} is nested in lists too deep to rewrite"))
                    continue
                if holds_strings(member):
                    if earlier is UNKNOWN:
                        unknown.append(value_id(member))
                    elif earlier is None:
                        gap = True
                        actions.append(FieldAction.of(member, WITHHOLD if target.mask == MASK_REDACT else MASK, target))
                    continue
                # Masked values are strings: every value a number field holds was written plain.
                gap = True
                if target.mask == MASK_REDACT:
                    actions.append(FieldAction.of(member, WITHHOLD, target))
                    continue
                into = next((item for item in strings if _list_depth(item) == _list_depth(member)), None)
                if into is None:
                    blocked.append((value_id(member), f"{name} has no text field yet for its masked values; the writers add it with its next value"))
                    continue
                actions.append(FieldAction.of(member, MOVE, target, into=into))
        rules_gap, unknown_policy = self._rules_gap(label)
        return Program(
            label=label,
            revision=self.revision,
            actions=tuple(actions),
            blocked=tuple(blocked),
            unknown=tuple(unknown),
            json=gap or rules_gap,
            unknown_policy=unknown_policy,
            json_fields=_json_fields(self.index),
        )

    def _rules_gap(self, label: tuple[int, ...]) -> tuple[bool, bool]:
        """Whether a key decided by the rules could hold values the table's revision masks that every
        revision of the label wrote plain, and whether a revision's policy is lost."""
        policies = [self.policy_at(revision) for revision in label]
        unknown = any(policy is None for policy in policies)
        masked_before = {kind_id for policy in policies if policy is not None for kind_id in policy.kinds}
        return bool(set(self.protection.policy.kinds) - masked_before), unknown


def _json_fields(index: SchemaIndex) -> frozenset[int]:
    """The fields holding JSON text: `_unmapped`, and families and list elements collapsed to JSON."""
    found = {UNMAPPED_FIELD_ID}
    for member in index.members.values():
        if member.json:
            found.add(member.field_id)
    for element in index.elements.values():
        if element.json:
            found.add(element.element_id)
    return frozenset(found)


@dataclass(frozen=True, slots=True)
class FieldAction:
    """What reprotect does to one field's values."""

    field_id: int
    #: The field's physical path; list elements are `[]`.
    path: tuple[str, ...]
    #: `mask` in place, `move` into the family's string field, `withhold`.
    action: str
    kind: str
    mask: str
    #: The field id its file statistics count values under.
    value_id: int
    #: `move`: the string field the masked values go to.
    into_path: tuple[str, ...] | None = None

    @classmethod
    def of(cls, member: Member, action: str, target: Decision, *, into: Member | None = None) -> "FieldAction":
        return cls(
            field_id=member.field_id,
            path=member.path,
            action=action,
            kind=target.kind,
            mask=target.mask,
            value_id=value_id(member),
            into_path=None if into is None else into.path,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "field_id": self.field_id,
            "path": list(self.path),
            "action": self.action,
            "kind": self.kind,
            "mask": self.mask,
            "value_id": self.value_id,
            "into_path": None if self.into_path is None else list(self.into_path),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FieldAction":
        into = value.get("into_path")
        return cls(
            field_id=int(value["field_id"]),
            path=tuple(str(item) for item in value["path"]),
            action=str(value["action"]),
            kind=str(value["kind"]),
            mask=str(value["mask"]),
            value_id=int(value["value_id"]),
            into_path=None if into is None else tuple(str(item) for item in into),
        )


#: What `Program.verdict` says of a file.
WORK = "work"
BLOCKED = "blocked"
UNKNOWN_FILE = "unknown"


@dataclass(frozen=True, slots=True)
class Program:
    """What reprotect does to the rows of files of one protection label."""

    label: tuple[int, ...]
    revision: int
    actions: tuple[FieldAction, ...]
    #: Fields whose plain values cannot be masked yet, by the field id their values are counted
    #: under, and why.
    blocked: tuple[tuple[int, str], ...] = ()
    #: Fields whose state under some revision of the label the record no longer says.
    unknown: tuple[int, ...] = ()
    #: Whether JSON text may hold values to mask.
    json: bool = False
    #: Whether a revision of the label's policy is lost: its JSON text cannot be judged.
    unknown_policy: bool = False
    json_fields: frozenset[int] = field(default_factory=frozenset)

    @property
    def output_label(self) -> tuple[int, ...]:
        """The label a rewritten file of this one is named with: the table's revision first."""
        return (self.revision, *self.label)

    def verdict(self, item: LiveFile) -> str | None:
        """Whether `item` needs rewriting (`work`), cannot be yet (`blocked`), cannot be judged
        (`unknown`), or holds nothing to rewrite (None)."""
        if any(item.holds(field_id) for field_id in self.unknown):
            return UNKNOWN_FILE
        if any(item.holds(field_id) for field_id, _ in self.blocked):
            return BLOCKED
        has_json = self.json and any(item.holds(field_id) for field_id in self.json_fields)
        if has_json and self.unknown_policy:
            return UNKNOWN_FILE
        if has_json or any(item.holds(action.value_id) for action in self.actions):
            return WORK
        return None

    def blocked_reasons(self, item: LiveFile) -> list[str]:
        return [reason for field_id, reason in self.blocked if item.holds(field_id)]


@dataclass(frozen=True, slots=True)
class ReprotectBacklog:
    """What a table holds in plain text that its revision masks: the files reprotect would rewrite,
    those it cannot yet, and those it cannot judge."""

    revision: int
    snapshot_id: int | None = None
    files: int = 0
    rows: int = 0
    bytes: int = 0
    blocked_files: int = 0
    blocked: tuple[str, ...] = ()
    unknown_files: int = 0

    @property
    def empty(self) -> bool:
        return not self.files and not self.blocked_files and not self.unknown_files

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "snapshot_id": self.snapshot_id,
            "files": self.files,
            "rows": self.rows,
            "bytes": self.bytes,
            "blocked_files": self.blocked_files,
            "blocked": list(self.blocked),
            "unknown_files": self.unknown_files,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ReprotectBacklog":
        return cls(
            revision=int(value.get("revision") or 0),
            snapshot_id=value.get("snapshot_id"),
            files=int(value.get("files") or 0),
            rows=int(value.get("rows") or 0),
            bytes=int(value.get("bytes") or 0),
            blocked_files=int(value.get("blocked_files") or 0),
            blocked=tuple(str(item) for item in value.get("blocked") or ()),
            unknown_files=int(value.get("unknown_files") or 0),
        )


@dataclass(frozen=True, slots=True)
class ReprotectPlan:
    """Files to rewrite under the table's revision, each into one of its own (`plan_reprotect`)."""

    job_id: str
    revision: int
    #: The protection key the table's hashes were made with: the rewrite must hash with the same.
    key_id: str | None
    inputs: tuple[LiveFile, ...]
    planned_snapshot_id: int | None
    #: What the rewrite does to each label's rows, by label (`Program`).
    programs: Mapping[tuple[int, ...], Program] = field(default_factory=dict)

    @property
    def input_bytes(self) -> int:
        return sum(item.file_size for item in self.inputs)

    @property
    def input_records(self) -> int:
        return sum(item.record_count for item in self.inputs)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "revision": self.revision,
            "key_id": self.key_id,
            "inputs": [item.to_dict() for item in self.inputs],
            "planned_snapshot_id": self.planned_snapshot_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ReprotectPlan":
        return cls(
            job_id=str(value["job_id"]),
            revision=int(value["revision"]),
            key_id=value.get("key_id"),
            inputs=tuple(LiveFile.from_dict(item) for item in value.get("inputs") or []),
            planned_snapshot_id=value.get("planned_snapshot_id"),
        )


# -- the rewrite ---------------------------------------------------------------------------------


class LabelWalk(ProtectionPlan):
    """The writer's walk over JSON text of rows of one label, masking only what the table's revision
    masks and every revision of the label wrote plain (`ReprotectDecider.decide`)."""

    def __init__(self, decider: ReprotectDecider, label: tuple[int, ...], *, secret: bytes | None) -> None:
        super().__init__(decider.protection, decider.index, secret=secret)
        self._decider = decider
        self._label = label

    @property
    def empty(self) -> bool:
        return False

    def _recorded_or_ruled(self, family, parent, key, inherited):
        state = self._decider.decide(family, parent, key, inherited, self._label)
        return state if isinstance(state, Decision) else None


@dataclass(slots=True)
class _StructEdits:
    """What a rewrite changes in one struct's values, by the physical names of its fields."""

    #: Fields masked, or withheld, in place.
    leaves: dict[str, FieldAction] = field(default_factory=dict)
    #: Number fields whose values go, masked, to their family's string field.
    moves: list[FieldAction] = field(default_factory=list)
    structs: dict[str, "_StructEdits"] = field(default_factory=dict)
    #: Lists of structs: what changes in each element.
    elements: dict[str, "_StructEdits"] = field(default_factory=dict)
    #: JSON text fields, with their logical paths: where the walk starts.
    json: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def names(self) -> set[str]:
        found = set(self.leaves) | set(self.structs) | set(self.elements) | set(self.json)
        for action in self.moves:
            found.update((action.path[-1], action.into_path[-1]))
        return found


def _node(root: _StructEdits, path: tuple[str, ...]) -> tuple[_StructEdits, str]:
    """The edits of the struct holding the field at `path`, and the field's name in it."""
    node, position = root, 0
    while position < len(path) - 1:
        name = path[position]
        if position + 1 < len(path) - 1 and path[position + 1] == ELEMENT:
            node, position = node.elements.setdefault(name, _StructEdits()), position + 2
        else:
            node, position = node.structs.setdefault(name, _StructEdits()), position + 1
    return node, path[-1]


def _edits(program: Program, index: SchemaIndex) -> _StructEdits:
    root = _StructEdits()
    for action in program.actions:
        node, name = _node(root, action.path)
        if action.action == MOVE:
            node.moves.append(action)
        else:
            node.leaves[name] = action
    if program.json:
        for member in index.members.values():
            if member.json and member.path.count(ELEMENT) <= 1:
                node, name = _node(root, member.path)
                node.json[name] = member.logical_path
    return root


_SEGMENT = re.compile(r"\[(\d+)\]|([^.\[\]]+)")


def _unmapped_segments(path: str) -> list[str | None]:
    """An `_unmapped` key's path (`contact.extra`, `items[2].email`) as keys, None for a position."""
    return [None if position else name for position, name in _SEGMENT.findall(path)]


def _nest(segments: list[str | None], value: Any) -> Any:
    for segment in reversed(segments):
        value = [value] if segment is None else {segment: value}
    return value


def _extract(record: Any, segments: list[str | None]) -> tuple[bool, Any]:
    node = record
    for segment in segments:
        if segment is None:
            if not isinstance(node, list) or not node:
                return False, None
            node = node[0]
        else:
            if not isinstance(node, dict) or segment not in node:
                return False, None
            node = node[segment]
    return True, node


class _Rewrite:
    """Rewrites rows of one label: the edits of its program, the walk of its JSON text, and counts."""

    def __init__(self, program: Program, decider: ReprotectDecider, *, secret: bytes | None) -> None:
        self.program = program
        self.secret = secret
        self.edits = _edits(program, decider.index)
        self.walk = LabelWalk(decider, program.label, secret=secret) if program.json else None
        self.masked: Counter[tuple[str, str]] = Counter()
        self.withheld = 0
        self.moved = 0

    # -- values ----------------------------------------------------------------------------------

    def _mask(self, value: Any, action: FieldAction) -> Any:
        masked = apply(action.mask, action.kind, value, self.secret)
        self.masked[(action.kind, action.mask)] += 1
        return masked

    def _leaf(self, value: Any, action: FieldAction) -> Any:
        if value is None:
            return None
        if action.action == WITHHOLD:
            self.withheld += sum(1 for item in value if item is not None) if isinstance(value, list) else 1
            return None
        if isinstance(value, list):
            return [None if item is None else self._mask(item, action) for item in value]
        return self._mask(value, action)

    def _json(self, text: Any, segments: list[str | None]) -> Any:
        """JSON text at the path `segments`, walked as the writer walks a record there."""
        if not isinstance(text, str) or self.walk is None:
            return text
        try:
            value = json.loads(text)
        except ValueError:
            return text
        record = _nest(segments, value)
        masked = self.walk.protect(record)
        if masked is record:
            return text
        self._drain()
        found, value = _extract(masked, segments)
        return json_text(value) if found else None

    def unmapped(self, text: Any) -> Any:
        if not isinstance(text, str) or self.walk is None:
            return text
        try:
            entries = json.loads(text)
        except ValueError:
            return text
        if not isinstance(entries, dict):
            return text
        changed = False
        kept: dict[str, Any] = {}
        for path, value in entries.items():
            if path == ROOT_VALUE_PATH:
                # A whole record that was not an object: no key to recognise it by.
                kept[path] = value
                continue
            segments = _unmapped_segments(path)
            record = _nest(segments, value)
            masked = self.walk.protect(record)
            if masked is record:
                kept[path] = value
                continue
            changed = True
            found, value = _extract(masked, segments)
            if found:
                kept[path] = value
        if not changed:
            return text
        self._drain()
        return unmapped_text(kept)

    def _drain(self) -> None:
        if self.walk is None:
            return
        for (kind_id, mask), count in self.walk.drain_counters().items():
            if mask == MASK_REDACT:
                self.withheld += count
            else:
                self.masked[(kind_id, mask)] += count

    # -- structs ---------------------------------------------------------------------------------

    def struct(self, edits: _StructEdits, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        out = dict(value)
        # In place first: a moved value is masked once, as it is moved.
        for name, action in edits.leaves.items():
            if name in out:
                out[name] = self._leaf(out[name], action)
        for action in edits.moves:
            source, target = action.path[-1], action.into_path[-1]
            moved = out.get(source)
            if moved is None:
                continue
            if out.get(target) is not None:
                raise ValueError(f"a row holds a value in both {source} and {target}, which a key's values never are")
            out[target] = self._leaf(moved, action)
            out[source] = None
            self.moved += 1
        for name, inner in edits.structs.items():
            if out.get(name) is not None:
                out[name] = self.struct(inner, out[name])
        for name, inner in edits.elements.items():
            items = out.get(name)
            if isinstance(items, list):
                out[name] = [None if item is None else self.struct(inner, item) for item in items]
        for name, logical in edits.json.items():
            if out.get(name) is not None:
                out[name] = self._json(out[name], [None if part == ELEMENT else part for part in logical])
        return out

    def table(self, rows: pa.Table) -> pa.Table:
        """`rows` - read from one file, projected onto the table's schema - rewritten."""
        names = self.edits.names
        if self.walk is not None:
            names = names | {UNMAPPED_COLUMN}
        names = [name for name in rows.column_names if name in names]
        if not names:
            return rows
        columns = {name: rows.column(name).to_pylist() for name in names}
        for position in range(rows.num_rows):
            row = {name: columns[name][position] for name in names}
            if UNMAPPED_COLUMN in row:
                row[UNMAPPED_COLUMN] = self.unmapped(row[UNMAPPED_COLUMN])
            out = self.struct(self.edits, row)
            for name in names:
                columns[name][position] = out.get(name)
        for name in names:
            index = rows.schema.get_field_index(name)
            target = rows.schema.field(index)
            rows = rows.set_column(index, target, pa.array(columns[name], type=target.type))
        return rows

    def counts(self) -> dict[str, Any]:
        return {
            "masked": sum(self.masked.values()),
            "withheld": self.withheld,
            "moved": self.moved,
            "by_kind": {f"{kind_id}/{mask}": count for (kind_id, mask), count in sorted(self.masked.items())},
        }


def label_rewrite(program: Program, decider: ReprotectDecider, *, secret: bytes | None) -> _Rewrite:
    """What rewrites rows of one label: `rewrite.table(rows)` masks them, `rewrite.counts()` says how many."""
    return _Rewrite(program, decider, secret=secret)


def table_decider(table: Table) -> ReprotectDecider | None:
    """The decider of a table's current revision; None when it masks nothing (or never did)."""
    protection = protection_of_properties(table.metadata.properties)
    if protection.revision <= 0 or (protection.policy.empty and not protection.active_fields()):
        return None
    index = build_index(table.schema(), fork_map_of(table.metadata), last_column_id=table.metadata.last_column_id)
    return ReprotectDecider(protection, index, field_births(table.metadata.schemas, protection))


def plan_reprotect(
    table: Table,
    *,
    max_input_bytes: int,
    max_input_files: int = MAX_PLAN_INPUT_FILES,
    files: list[LiveFile] | None = None,
) -> tuple[ReprotectPlan | None, ReprotectBacklog]:
    """The oldest files to rewrite next, within the bounds, and what the table holds in all.

    A plan always takes at least one file, however large; None when nothing needs rewriting.
    """
    snapshot_id = table.metadata.current_snapshot_id
    decider = table_decider(table)
    if decider is None:
        protection = protection_of_properties(table.metadata.properties)
        return None, ReprotectBacklog(revision=protection.revision, snapshot_id=snapshot_id)
    programs: dict[tuple[int, ...], Program] = {}
    candidates: list[LiveFile] = []
    blocked_files = unknown_files = 0
    reasons: dict[str, None] = {}
    for item in live_data_files(table) if files is None else files:
        label = item.protection_label
        if label[0] >= decider.revision:
            continue
        program = programs.get(label)
        if program is None:
            program = programs[label] = decider.program(label)
        verdict = program.verdict(item)
        if verdict == WORK:
            candidates.append(item)
        elif verdict == BLOCKED:
            blocked_files += 1
            reasons.update(dict.fromkeys(program.blocked_reasons(item)))
        elif verdict == UNKNOWN_FILE:
            unknown_files += 1
    backlog = ReprotectBacklog(
        revision=decider.revision,
        snapshot_id=snapshot_id,
        files=len(candidates),
        rows=sum(item.record_count for item in candidates),
        bytes=sum(item.file_size for item in candidates),
        blocked_files=blocked_files,
        blocked=tuple(reasons),
        unknown_files=unknown_files,
    )
    if not candidates:
        return None, backlog
    candidates.sort(key=lambda item: (item.protection_label, item.timestamp_min or 0, item.path))
    chosen: list[LiveFile] = []
    total = 0
    for item in candidates:
        if chosen and (total + item.file_size > max_input_bytes or len(chosen) >= max_input_files):
            break
        chosen.append(item)
        total += item.file_size
    plan = ReprotectPlan(
        job_id=uuid.uuid4().hex,
        revision=decider.revision,
        key_id=decider.protection.key_id,
        inputs=tuple(chosen),
        planned_snapshot_id=snapshot_id,
        programs={label: programs[label] for label in {item.protection_label for item in chosen}},
    )
    return plan, backlog


__all__ = [
    "BLOCKED",
    "FieldAction",
    "MASK",
    "MOVE",
    "Program",
    "ReprotectBacklog",
    "ReprotectDecider",
    "ReprotectPlan",
    "UNKNOWN_FILE",
    "WITHHOLD",
    "WORK",
    "field_births",
    "holds_strings",
    "plan_reprotect",
    "table_decider",
    "value_id",
]
