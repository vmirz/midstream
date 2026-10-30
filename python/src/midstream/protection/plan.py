"""The protection plan: what the writer, the live tail and discovery do to each record (design §5.4),
and how the committer classifies a table's fields (design §5.6).

A plan is built once per schema view from the table's recorded protection (`TableProtection`) and
its schema index. Its one hot method, `protect`, walks a record's keys as drift detection does and
masks the values of protected fields, copy-on-write: a record with nothing to mask comes back as the
same object.

For each key, the table's record decides first: a key whose family already holds scalar values in
the schema is masked as the record says - protected with its kind and mask, or plain - whatever
today's rules say, so a column never flips because a release changed a rule. A key the schema does
not hold yet is decided by the rules against the policy, which is exactly what the committer
records when it adds the key's field (`record_new_fields`), so a column's values are masked from
its first value on.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import Any

from midstream.lakehouse.schema.index import ElementIndex, Family, Member, SchemaIndex, StructIndex
from midstream.protection.catalog import MASK_HASH, MASK_REDACT
from midstream.protection.mask import REDACTED, MissingProtectionKey, apply
from midstream.protection.policy import (
    NONE,
    FieldProtection,
    ProtectionChange,
    ProtectionPolicy,
    TableProtection,
)
from midstream.protection.rules import classify, container_kind

#: Decisions remembered per plan before they are forgotten: records with ids for keys grow it.
DECISION_CACHE_LIMIT = 50_000

#: Key text discovery hashes with when the control plane has no key: only the shape of a masked
#: value matters there (a 32-character string), never its text.
SHAPE_ONLY_SECRET = b"midstream-discovery-shape-only-not-a-key"


@dataclass(frozen=True, slots=True)
class Decision:
    """How a field's values are masked."""

    kind: str
    mask: str


_PLAIN = None


def _scalar_members(family: Family | None) -> list[Member]:
    """The family's members that hold scalars (or lists of scalars), newest first; collapsed ones excluded."""
    if family is None:
        return []
    found = [
        member
        for member in family.members
        if not member.json and member.struct is None and (member.element is None or _scalar_element(member.element))
    ]
    found.sort(key=lambda member: (member.fork_version, member.field_id), reverse=True)
    return found


def _scalar_element(element: ElementIndex) -> bool:
    return not element.json and element.struct is None and (element.element is None or _scalar_element(element.element))


def _struct_of(family: Family | None) -> StructIndex | None:
    """The struct a dict value of the family's key is walked against: its newest struct member's."""
    if family is None:
        return None
    for member in sorted(family.members, key=lambda member: member.fork_version, reverse=True):
        if member.struct is not None and not member.json:
            return member.struct
    return None


def _element_struct_of(family: Family | None) -> StructIndex | None:
    """The struct a list's dict elements are walked against: its newest list member's element's."""
    if family is None:
        return None
    for member in sorted(family.members, key=lambda member: member.fork_version, reverse=True):
        if member.element is not None and not member.json and member.element.struct is not None:
            return member.element.struct
    return None


class ProtectionPlan:
    """Masks records for one table and one schema; build once per schema view and reuse."""

    def __init__(
        self,
        protection: TableProtection,
        index: SchemaIndex | None = None,
        *,
        secret: bytes | None = None,
    ) -> None:
        self.protection = protection
        self.policy: ProtectionPolicy = protection.policy
        self.index = index
        self.secret = secret
        self.revision = protection.revision
        self._root = None if index is None else index.root
        self._active = protection.active_fields()
        self._decisions: dict[tuple, Decision | None] = {}
        #: Values masked, by (kind, mask), since the counters were last drained.
        self.counters: Counter[tuple[str, str]] = Counter()

    @classmethod
    def for_policy(cls, policy: ProtectionPolicy, *, secret: bytes | None = None, revision: int = 0) -> "ProtectionPlan":
        """A plan with no table yet: every key is decided by the rules (discovery)."""
        return cls(TableProtection(revision=revision, policy=policy), None, secret=secret)

    @property
    def empty(self) -> bool:
        """Nothing to mask: the walk is skipped."""
        return self.policy.empty and not self._active

    @property
    def hashes(self) -> bool:
        """Whether some value may be hashed, which needs the protection key."""
        return self.policy.hashes or any(item.mask == MASK_HASH for item in self._active.values())

    def drain_counters(self) -> Counter[tuple[str, str]]:
        counters, self.counters = self.counters, Counter()
        return counters

    # -- the walk --------------------------------------------------------------------------------

    def protect(self, value: Any) -> Any:
        """The record's value with its protected values masked; `value` itself when nothing was.

        Raises `MissingProtectionKey` when a value must be hashed and the plan has no key: the
        record must then not be written at all.
        """
        if self.empty or not isinstance(value, dict):
            return value
        return self._struct(self._root, value, None, None)

    def _struct(self, struct: StructIndex | None, value: dict, parent: str | None, inherited: str | None) -> dict:
        result = value
        families = None if struct is None else struct.families
        for key, item in value.items():
            if item is None or isinstance(item, bool):
                continue
            family = None if families is None else families.get(key)
            if isinstance(item, dict):
                new = self._struct(_struct_of(family), item, key, container_kind(parent, key, inherited))
            elif isinstance(item, list):
                new = self._list(struct, family, key, item, parent, inherited)
            else:
                decision = self._decide(struct, family, parent, key, inherited)
                if decision is None:
                    continue
                new = self._mask(decision, item)
            if new is item:
                continue
            if result is value:
                result = dict(value)
            if new is REDACTED:
                del result[key]
            else:
                result[key] = new
        return result

    def _list(
        self,
        struct: StructIndex | None,
        family: Family | None,
        key: str,
        items: list,
        parent: str | None,
        inherited: str | None,
    ) -> Any:
        decision = self._decide(struct, family, parent, key, inherited)
        child_kind = container_kind(parent, key, inherited)
        return self._items(items, decision, _element_struct_of(family), key, child_kind)

    def _items(self, items: list, decision: Decision | None, element_struct: StructIndex | None, key: str, child_kind: str | None) -> Any:
        result: list | None = None
        scalars = 0
        for position, item in enumerate(items):
            if item is None or isinstance(item, bool):
                new = item
            elif isinstance(item, dict):
                new = self._struct(element_struct, item, key, child_kind)
            elif isinstance(item, list):
                new = self._items(item, decision, None, key, child_kind)
            else:
                scalars += 1
                new = item if decision is None else self._mask(decision, item)
            if new is item and result is None:
                continue
            if result is None:
                result = list(items[:position])
            if new is not REDACTED:
                result.append(new)
        if result is None:
            return items
        if decision is not None and decision.mask == MASK_REDACT and scalars and not result:
            return REDACTED
        return result

    def _decide(
        self,
        struct: StructIndex | None,
        family: Family | None,
        parent: str | None,
        key: str,
        inherited: str | None,
    ) -> Decision | None:
        cache_key = (None if struct is None else struct.struct_id, key, parent, inherited)
        try:
            return self._decisions[cache_key]
        except KeyError:
            pass
        decision = self._recorded_or_ruled(family, parent, key, inherited)
        if len(self._decisions) >= DECISION_CACHE_LIMIT:
            self._decisions.clear()
        self._decisions[cache_key] = decision
        return decision

    def _recorded_or_ruled(self, family: Family | None, parent: str | None, key: str, inherited: str | None) -> Decision | None:
        members = _scalar_members(family)
        if members:
            for member in members:
                entry = self._active.get(member.field_id)
                if entry is not None:
                    return Decision(entry.kind, entry.mask)
            return _PLAIN
        kind_id = classify(parent, key, inherited)
        mask = self.policy.mask_of(kind_id)
        return None if mask is None or kind_id is None else Decision(kind_id, mask)

    def _mask(self, decision: Decision, value: Any) -> Any:
        if decision.mask == MASK_HASH and self.secret is None:
            raise MissingProtectionKey("a value must be hashed and there is no protection key")
        masked = apply(decision.mask, decision.kind, value, self.secret)
        if masked is not value:
            self.counters[(decision.kind, decision.mask)] += 1
        return masked


# -- classification: what the committer records --------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Visit:
    """A family met walking a schema index, with what the rules need to classify it."""

    struct: StructIndex
    family: Family
    parent: str | None
    inherited: str | None


def _families(struct: StructIndex, parent: str | None, inherited: str | None) -> Iterator[_Visit]:
    for key, family in struct.families.items():
        yield _Visit(struct, family, parent, inherited)
        child_kind = container_kind(parent, key, inherited)
        for member in family.members:
            if member.json:
                continue
            if member.struct is not None:
                yield from _families(member.struct, key, child_kind)
            elif member.element is not None:
                element = member.element
                while element is not None and element.struct is None and element.element is not None:
                    element = element.element
                if element is not None and element.struct is not None and not element.json:
                    yield from _families(element.struct, key, child_kind)


def _structs(index: SchemaIndex) -> dict[int, StructIndex]:
    found: dict[int, StructIndex] = {}

    def visit(struct: StructIndex) -> None:
        found[struct.struct_id] = struct
        for family in struct.families.values():
            for member in family.members:
                if member.json:
                    continue
                if member.struct is not None:
                    visit(member.struct)
                element = member.element
                while element is not None:
                    if element.struct is not None and not element.json:
                        visit(element.struct)
                        break
                    element = element.element

    visit(index.root)
    return found


def _family_kind(fields: dict[int, FieldProtection], members: list[Member]) -> str | None:
    """The kind a family's newest actively protected member records, if any."""
    for member in members:
        entry = fields.get(member.field_id)
        if entry is not None and entry.active:
            return entry.kind
    return None


def record_policy(
    protection: TableProtection,
    index: SchemaIndex,
    policy: ProtectionPolicy,
    *,
    revision: int,
    at: str | None = None,
    by: str | None = None,
    key_id: str | None = None,
    sequence_number: int | None = None,
    reason: str | None = None,
) -> TableProtection:
    """The table's protection once it takes on `policy` at its current schema (design §5.6).

    Every field is classified again, family by family: a family whose recorded kind is still in the
    policy keeps it, with the policy's mask for it; one whose kind left is written plain again
    (`until`); one the rules match with a kind in the policy - its kind added, or a newer release's
    rules matching it - is protected from this schema on.
    """
    schema_id = index.schema_id
    fields = dict(protection.fields)
    for visit in _families(index.root, None, None):
        members = _scalar_members(visit.family)
        if not members:
            continue
        recorded = _family_kind(fields, members)
        ruled = classify(visit.parent, visit.family.key, visit.inherited)
        state: Decision | None = None
        for candidate in (recorded, ruled):
            mask = policy.mask_of(candidate)
            if candidate is not None and mask is not None:
                state = Decision(candidate, mask)
                break
        for member in members:
            entry = fields.get(member.field_id)
            if state is None:
                if entry is not None and entry.active:
                    fields[member.field_id] = replace(entry, until=schema_id)
                continue
            if entry is not None and entry.active and entry.kind == state.kind and entry.mask == state.mask:
                continue
            fields[member.field_id] = FieldProtection(
                field_id=member.field_id,
                path=member.path,
                kind=state.kind,
                mask=state.mask,
                since=schema_id,
            )
    change = ProtectionChange(
        revision=revision,
        policy=policy,
        at=at,
        by=by,
        schema_id=schema_id,
        sequence_number=sequence_number,
        key_id=key_id if policy.hashes else None,
        reason=reason,
    )
    return TableProtection(
        revision=revision,
        policy=policy,
        key_id=key_id if (policy.hashes or any(item.active and item.mask == MASK_HASH for item in fields.values())) else protection.key_id,
        fields=fields,
        history=(*protection.history, change),
    )


def record_new_fields(protection: TableProtection, before: SchemaIndex | None, after: SchemaIndex) -> TableProtection:
    """The table's protection once a schema change added fields (design §5.6).

    A field added to a family that already held scalars takes the family's recorded state, so a
    type fork of a protected field is protected and one of a plain field plain; a field of a new
    family is classified by the rules against the policy - what the writers did to its values.
    Fields already in the schema keep their state.
    """
    known = set() if before is None else set(before.members)
    before_structs = {} if before is None else _structs(before)
    fields = dict(protection.fields)
    changed = False
    for visit in _families(after.root, None, None):
        members = _scalar_members(visit.family)
        added = [member for member in members if member.field_id not in known]
        if not added:
            continue
        before_struct = before_structs.get(visit.struct.struct_id)
        before_family = None if before_struct is None else before_struct.families.get(visit.family.key)
        before_members = _scalar_members(before_family)
        state: Decision | None
        if before_members:
            kind_id = _family_kind(fields, before_members)
            entry = None
            if kind_id is not None:
                entry = next(
                    fields[member.field_id]
                    for member in before_members
                    if member.field_id in fields and fields[member.field_id].active
                )
            state = None if entry is None else Decision(entry.kind, entry.mask)
        else:
            kind_id = classify(visit.parent, visit.family.key, visit.inherited)
            mask = protection.policy.mask_of(kind_id)
            state = None if kind_id is None or mask is None else Decision(kind_id, mask)
        if state is None:
            continue
        for member in added:
            fields[member.field_id] = FieldProtection(
                field_id=member.field_id,
                path=member.path,
                kind=state.kind,
                mask=state.mask,
                since=after.schema_id,
            )
            changed = True
    if not changed:
        return protection
    return replace(protection, fields=fields)


def initial_protection(
    index: SchemaIndex,
    policy: ProtectionPolicy,
    *,
    revision: int,
    at: str | None = None,
    by: str | None = None,
    key_id: str | None = None,
) -> TableProtection:
    """A new table's protection: its first schema's fields classified by the rules (design §5.5)."""
    if policy.empty:
        return TableProtection(
            revision=revision,
            policy=NONE,
            history=(ProtectionChange(revision=revision, policy=NONE, at=at, by=by, schema_id=index.schema_id, sequence_number=0),),
        )
    return record_policy(
        TableProtection(),
        index,
        policy,
        revision=revision,
        at=at,
        by=by,
        key_id=key_id,
        sequence_number=0,
    )


@dataclass(frozen=True, slots=True)
class ReviewItem:
    """A field whose protection would change if the current rules were applied (the review)."""

    field_id: int
    path: tuple[str, ...]
    logical_path: tuple[str, ...]
    current: Decision | None
    proposed: Decision | None

    def to_dict(self) -> dict[str, Any]:
        def text(decision: Decision | None) -> dict[str, str] | None:
            return None if decision is None else {"kind": decision.kind, "mask": decision.mask}

        return {
            "field_id": self.field_id,
            "path": list(self.path),
            "name": ".".join(self.logical_path),
            "current": text(self.current),
            "proposed": text(self.proposed),
        }


def review(protection: TableProtection, index: SchemaIndex, policy: ProtectionPolicy | None = None) -> list[ReviewItem]:
    """The fields whose protection would change if `policy` (the table's own by default) were
    recorded now with the current rules: what "apply the current rules" would do."""
    target = protection.policy if policy is None else policy
    proposed = record_policy(protection, index, target, revision=protection.revision)
    items: list[ReviewItem] = []
    for field_id, member in sorted(index.members.items()):
        before = protection.fields.get(field_id)
        after = proposed.fields.get(field_id)
        current = None if before is None or not before.active else Decision(before.kind, before.mask)
        future = None if after is None or not after.active else Decision(after.kind, after.mask)
        if current != future:
            items.append(ReviewItem(field_id, member.path, member.logical_path, current, future))
    return items
