"""Hands-free schema evolution: what the schema does when data does not fit it (spec §8.3).

The policy always has a safe move, so it never blocks a writer and never asks anyone:

| Existing | Observed | Action |
|---|---|---|
| nothing | typed | add a nullable field |
| nothing | null, or only nulls and empties | defer: no field yet |
| string | any scalar | nothing: the value is written as text |
| double | long | nothing: the value is widened |
| long | double, or scalar <-> other scalar | fork: a sibling field `<name>__vN` of the new type |
| scalar | object or array, or object <-> array | fork |
| struct | struct with new keys | add the keys inside the struct |
| list<e> | list<e'> | recurse into the element; a conflict there forks the list field |
| any | a type one of its forks already has | nothing: the value goes to that fork |
| family with `max_forks_per_field` forks | yet another type | collapse: values go to `<name>__json` as JSON text |
| struct with `max_struct_fields` fields | a new key | collapse the struct to JSON text |
| deeper than `max_nesting_depth` | | collapse at the depth limit |
| `max_schema_fields` fields in all | a new field | reject: the values go to `_unmapped` |

A type that comes back reuses the fork that already has it, without any schema change, so a field
whose values alternate between two types costs one fork and no further commits. Nothing is ever
dropped or renamed: readers of old snapshots, and the console's logical columns, need every field.

Record keys become field names as they are, except: an empty key is `_`; `.` becomes `_` (Parquet
joins nested column paths with dots, and PyIceberg's statistics are keyed by those paths); a key
equal, ignoring case, to a sibling's name gets `__k2` (`__k3`, ...); a top-level key equal to a
metadata column's name gets `__payload`. The fork map records the original key of every renamed field.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IcebergType,
    ListType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
)

from materializer_control_plane.lakehouse.schema.forkmap import (
    COLLAPSED,
    CURRENT,
    ELEMENT,
    SUPERSEDED,
    ForkEntry,
    ForkMap,
)
from materializer_control_plane.lakehouse.schema.infer import (
    BOOLEAN,
    DOUBLE,
    JSON,
    LONG,
    STRING,
    UNKNOWN,
    Observation,
    describe,
    is_list,
    is_struct,
    is_unknown,
    normalize,
)
from materializer_control_plane.lakehouse.tables import METADATA_COLUMN_NAMES, METADATA_FIELD_IDS

ADDED = "added"
FORKED = "forked"
COLLAPSED_CHANGE = "collapsed"
REJECTED = "rejected"

REJECTED_FIELD_LIMIT = "schema_field_limit"


@dataclass(frozen=True, slots=True)
class SchemaLimits:
    """The caps that keep a pathological source from growing a pathological schema (settings)."""

    max_forks_per_field: int = 3
    max_struct_fields: int = 300
    max_nesting_depth: int = 10
    max_schema_fields: int = 2000

    def __post_init__(self) -> None:
        for name in ("max_forks_per_field", "max_struct_fields", "max_nesting_depth", "max_schema_fields"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1")


@dataclass(frozen=True, slots=True)
class SchemaChange:
    """One thing a schema commit did, by logical path (record keys, `[]` for list elements)."""

    kind: str
    path: tuple[str, ...]
    field_id: int | None = None
    physical_path: tuple[str, ...] = ()
    type: str | None = None
    fork_version: int | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"kind": self.kind, "path": list(self.path)}
        if self.field_id is not None:
            payload["field_id"] = self.field_id
        if self.physical_path:
            payload["physical_path"] = list(self.physical_path)
        if self.type is not None:
            payload["type"] = self.type
        if self.fork_version is not None:
            payload["fork_version"] = self.fork_version
        if self.reason is not None:
            payload["reason"] = self.reason
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SchemaChange":
        return cls(
            kind=str(value["kind"]),
            path=tuple(value.get("path") or ()),
            field_id=value.get("field_id"),
            physical_path=tuple(value.get("physical_path") or ()),
            type=value.get("type"),
            fork_version=value.get("fork_version"),
            reason=value.get("reason"),
        )


@dataclass(frozen=True, slots=True)
class Evolution:
    schema: Schema
    fork_map: ForkMap
    changes: tuple[SchemaChange, ...]
    last_column_id: int

    @property
    def changed(self) -> bool:
        return any(change.kind != REJECTED for change in self.changes)

    @property
    def rejected(self) -> tuple[SchemaChange, ...]:
        return tuple(change for change in self.changes if change.kind == REJECTED)


# -- type acceptance, shared with drift detection and conform -----------------------------------


def scalar_accepts(field_type: IcebergType, observed: Observation) -> bool:
    """Whether a primitive field takes a value observed as `observed` without a schema change."""
    if isinstance(field_type, StringType):
        return observed in (BOOLEAN, LONG, DOUBLE, STRING, JSON)
    if isinstance(field_type, DoubleType):
        return observed in (LONG, DOUBLE)
    if isinstance(field_type, LongType):
        return observed == LONG
    if isinstance(field_type, BooleanType):
        return observed == BOOLEAN
    return False


def element_compatible(element_type: IcebergType, element_json: bool, observed: Observation) -> bool:
    """Whether a list's element takes elements observed as `observed`, struct additions allowed."""
    if is_unknown(observed) or element_json:
        return True
    if isinstance(element_type, StructType):
        return is_struct(observed)
    if isinstance(element_type, ListType):
        return is_list(observed)
    if is_struct(observed) or is_list(observed):
        return False
    return scalar_accepts(element_type, observed)


# -- the mutable working copy -------------------------------------------------------------------


@dataclass(slots=True)
class _MStruct:
    fields: list["_MField"] = field(default_factory=list)


@dataclass(slots=True)
class _MList:
    element_id: int
    element: Any
    element_required: bool = False


@dataclass(slots=True)
class _MField:
    field_id: int
    name: str
    type: Any
    required: bool = False
    doc: str | None = None


def _to_mutable(iceberg_type: IcebergType) -> Any:
    if isinstance(iceberg_type, StructType):
        return _MStruct([_mutable_field(item) for item in iceberg_type.fields])
    if isinstance(iceberg_type, ListType):
        return _MList(iceberg_type.element_id, _to_mutable(iceberg_type.element_type), iceberg_type.element_required)
    return iceberg_type


def _mutable_field(item: NestedField) -> _MField:
    return _MField(item.field_id, item.name, _to_mutable(item.field_type), item.required, item.doc)


def _to_iceberg(value: Any) -> IcebergType:
    if isinstance(value, _MStruct):
        return StructType(*[_iceberg_field(item) for item in value.fields])
    if isinstance(value, _MList):
        return ListType(value.element_id, _to_iceberg(value.element), element_required=value.element_required)
    return value


def _iceberg_field(item: _MField) -> NestedField:
    return NestedField(item.field_id, item.name, _to_iceberg(item.type), required=item.required, doc=item.doc)


@dataclass(slots=True)
class _Family:
    key: str
    members: list[_MField]

    def root(self, entries: dict[int, ForkEntry]) -> _MField:
        for member in self.members:
            entry = entries.get(member.field_id)
            if entry is None or entry.fork_of is None:
                return member
        return self.members[0]


class _StructCollapsed(Exception):
    """Adding a field would push a struct past `max_struct_fields`: its owner collapses."""

    def __init__(self, owner: tuple[int, str]) -> None:
        super().__init__(owner)
        self.owner = owner


JSON_NOTE = "json"
RENAMED_NOTE = "renamed"


@dataclass(slots=True)
class _Built:
    """A type built for a new field, with what the fork map must record about its insides."""

    type: Any
    #: `(path relative to the new field, JSON_NOTE or RENAMED_NOTE, record key)`.
    notes: list[tuple[tuple[str, ...], str, str | None]] = field(default_factory=list)
    #: The field itself holds JSON text: a depth or width cap was hit at it.
    is_json: bool = False


class _Evolver:
    def __init__(self, schema: Schema, fork_map: ForkMap, limits: SchemaLimits, last_column_id: int) -> None:
        self.root = _MStruct([_mutable_field(item) for item in schema.fields])
        self.entries: dict[int, ForkEntry] = dict(fork_map.entries)
        self.limits = limits
        self.next_id = max(last_column_id, schema.highest_field_id) + 1
        self.field_count = _count_mutable(self.root)
        self.changes: list[SchemaChange] = []
        self._rejected_paths: set[tuple[str, ...]] = set()
        #: Logical paths observed as double anywhere in this batch: a new field first seen there as
        #: a whole number is created as double, rather than as long and forked a moment later. JSON
        #: writes a zero amount as `0`, so money fields would otherwise fork on their first cent.
        self.double_paths: set[tuple[str, ...]] = set()

    # -- observations ------------------------------------------------------------------------

    def prepare(self, path: tuple[str, ...], observed: Observation) -> tuple[str, ...] | None:
        wrapped = _wrap(tuple(path), normalize(observed))
        if not is_struct(wrapped):
            return None
        _collect_double_paths(wrapped, (), self.double_paths)
        return wrapped

    def observe(self, wrapped: Observation) -> None:
        self._apply_struct(self.root, wrapped, depth=0, phys=(), logical=(), owner=None)

    def _apply_struct(
        self,
        struct: _MStruct,
        observed: dict,
        *,
        depth: int,
        phys: tuple[str, ...],
        logical: tuple[str, ...],
        owner: tuple[int, str] | None,
    ) -> None:
        for key, sub in observed["fields"].items():
            self._apply_key(struct, key, sub, depth=depth + 1, phys=phys, logical=logical, owner=owner)

    def _apply_key(
        self,
        struct: _MStruct,
        key: str,
        observed: Observation,
        *,
        depth: int,
        phys: tuple[str, ...],
        logical: tuple[str, ...],
        owner: tuple[int, str] | None,
    ) -> None:
        if is_unknown(observed):
            return
        at_root = owner is None and not phys
        family = self._families(struct, at_root=at_root).get(key)
        path = logical + (key,)
        if family is None:
            self._add_family(struct, key, observed, depth=depth, phys=phys, logical=path, owner=owner, at_root=at_root)
            return
        if self._is_collapsed(family):
            return
        here = (id(struct), key)
        try:
            if is_struct(observed):
                member = self._member_of(family, _MStruct)
                if member is not None:
                    self._apply_struct(
                        member.type,
                        observed,
                        depth=depth,
                        phys=phys + (member.name,),
                        logical=path,
                        owner=here,
                    )
                    return
            elif is_list(observed):
                lists = [item for item in family.members if isinstance(item.type, _MList)]
                for member in sorted(lists, key=self._fork_version, reverse=True):
                    element_json = self._is_json_id(member.type.element_id)
                    if element_compatible(_to_iceberg(member.type.element), element_json, observed["element"]):
                        self._apply_element(
                            member.type,
                            observed["element"],
                            depth=depth + 1,
                            phys=phys + (member.name, ELEMENT),
                            logical=path + (ELEMENT,),
                            owner=here,
                        )
                        return
            else:
                for member in family.members:
                    if isinstance(member.type, (_MStruct, _MList)):
                        continue
                    if scalar_accepts(member.type, observed):
                        return
            self._fork(struct, family, observed, depth=depth, phys=phys, logical=path, owner=owner)
        except _StructCollapsed as collapsed:
            if collapsed.owner != here:
                raise
            # A struct this family holds grew too wide: it is a map, written as JSON text from now on.
            family = self._families(struct, at_root=at_root)[key]
            self._collapse(struct, family, phys=phys, logical=path, owner=owner, reason="max_struct_fields")

    def _apply_element(
        self,
        list_node: _MList,
        observed: Observation,
        *,
        depth: int,
        phys: tuple[str, ...],
        logical: tuple[str, ...],
        owner: tuple[int, str],
    ) -> None:
        if self._is_json_id(list_node.element_id):
            return
        element = list_node.element
        if is_struct(observed) and isinstance(element, _MStruct):
            self._apply_struct(element, observed, depth=depth, phys=phys, logical=logical, owner=owner)
        elif is_list(observed) and isinstance(element, _MList):
            self._apply_element(
                element,
                observed["element"],
                depth=depth + 1,
                phys=phys + (ELEMENT,),
                logical=logical + (ELEMENT,),
                owner=owner,
            )

    # -- changes -----------------------------------------------------------------------------

    def _add_family(
        self,
        struct: _MStruct,
        key: str,
        observed: Observation,
        *,
        depth: int,
        phys: tuple[str, ...],
        logical: tuple[str, ...],
        owner: tuple[int, str] | None,
        at_root: bool,
    ) -> None:
        built = self._build(observed, depth=depth, logical=logical)
        if built is None:
            return
        name = _sanitize(key)
        if at_root and name in METADATA_COLUMN_NAMES:
            name = f"{name}__payload"
        if built.is_json:
            name = f"{name}__json"
        name = self._unique(struct, name)
        added = self._add_field(
            struct,
            name,
            built,
            phys=phys,
            logical=logical,
            owner=owner,
            kind=COLLAPSED_CHANGE if built.is_json else ADDED,
            reason="max_nesting_depth" if built.is_json else None,
        )
        if added is None:
            return
        if built.is_json or name != key:
            self.entries[added.field_id] = ForkEntry(
                field_id=added.field_id,
                path=phys + (name,),
                key=key if name != key else None,
                fork_of=None,
                fork_version=1,
                state=COLLAPSED if built.is_json else CURRENT,
            )

    def _fork(
        self,
        struct: _MStruct,
        family: _Family,
        observed: Observation,
        *,
        depth: int,
        phys: tuple[str, ...],
        logical: tuple[str, ...],
        owner: tuple[int, str] | None,
    ) -> None:
        forks = sum(
            1 for member in family.members if self._fork_version(member) >= 2 and not self._is_json_id(member.field_id)
        )
        if forks >= self.limits.max_forks_per_field:
            self._collapse(struct, family, phys=phys, logical=logical, owner=owner, reason="max_forks_per_field")
            return
        built = self._build(observed, depth=depth, logical=logical)
        if built is None:
            return
        if built.is_json:
            # Too deep or too wide to hold as a structure even as a fork: JSON from here on.
            self._collapse(struct, family, phys=phys, logical=logical, owner=owner, reason="max_nesting_depth")
            return
        root = family.root(self.entries)
        version = max(self._fork_version(member) for member in family.members) + 1
        name = self._unique(struct, f"{root.name}__v{version}")
        added = self._add_field(
            struct,
            name,
            built,
            phys=phys,
            logical=logical,
            owner=owner,
            kind=FORKED,
            fork_version=version,
        )
        if added is None:
            return
        self._supersede(family, phys=phys)
        self.entries[added.field_id] = ForkEntry(
            field_id=added.field_id,
            path=phys + (name,),
            fork_of=root.field_id,
            fork_version=version,
            state=CURRENT,
        )

    def _collapse(
        self,
        struct: _MStruct,
        family: _Family,
        *,
        phys: tuple[str, ...],
        logical: tuple[str, ...],
        owner: tuple[int, str] | None,
        reason: str,
    ) -> None:
        if self._is_collapsed(family):
            return
        root = family.root(self.entries)
        version = max(self._fork_version(member) for member in family.members) + 1
        name = self._unique(struct, f"{root.name}__json")
        added = self._add_field(
            struct,
            name,
            _Built(StringType()),
            phys=phys,
            logical=logical,
            owner=owner,
            kind=COLLAPSED_CHANGE,
            fork_version=version,
            reason=reason,
        )
        if added is None:
            return
        self._supersede(family, phys=phys)
        self.entries[added.field_id] = ForkEntry(
            field_id=added.field_id,
            path=phys + (name,),
            fork_of=root.field_id,
            fork_version=version,
            state=COLLAPSED,
        )

    def _supersede(self, family: _Family, *, phys: tuple[str, ...]) -> None:
        root = family.root(self.entries)
        for member in family.members:
            entry = self.entries.get(member.field_id)
            if entry is None:
                entry = ForkEntry(
                    field_id=member.field_id,
                    path=phys + (member.name,),
                    key=family.key if member is root and family.key != member.name else None,
                    fork_of=None if member is root else root.field_id,
                    fork_version=1,
                    state=SUPERSEDED,
                )
            elif entry.state != COLLAPSED:
                entry = ForkEntry(
                    field_id=entry.field_id,
                    path=entry.path,
                    key=entry.key,
                    fork_of=entry.fork_of,
                    fork_version=entry.fork_version,
                    state=SUPERSEDED,
                )
            self.entries[member.field_id] = entry

    def _add_field(
        self,
        struct: _MStruct,
        name: str,
        built: _Built,
        *,
        phys: tuple[str, ...],
        logical: tuple[str, ...],
        owner: tuple[int, str] | None,
        kind: str,
        fork_version: int | None = None,
        reason: str | None = None,
    ) -> _MField | None:
        needed = 1 + _count_mutable_type(built.type)
        if self.field_count + needed > self.limits.max_schema_fields:
            if logical not in self._rejected_paths:
                self._rejected_paths.add(logical)
                self.changes.append(
                    SchemaChange(kind=REJECTED, path=logical, type=_describe_mutable(built.type), reason=REJECTED_FIELD_LIMIT)
                )
            return None
        if owner is not None and len(struct.fields) + 1 > self.limits.max_struct_fields:
            raise _StructCollapsed(owner)
        new_field = _MField(self._allocate(), name, built.type, required=False)
        self._assign_ids(new_field.type)
        struct.fields.append(new_field)
        self.field_count += needed
        for relative, note, key in built.notes:
            node_id = _id_at(new_field.type, relative)
            if node_id is None:
                continue
            self.entries[node_id] = ForkEntry(
                field_id=node_id,
                path=phys + (name,) + relative,
                key=key,
                state=COLLAPSED if note == JSON_NOTE else CURRENT,
            )
        self.changes.append(
            SchemaChange(
                kind=kind,
                path=logical,
                field_id=new_field.field_id,
                physical_path=phys + (name,),
                type=_describe_mutable(new_field.type),
                fork_version=fork_version,
                reason=reason,
            )
        )
        return new_field

    # -- building new types ------------------------------------------------------------------

    def _build(self, observed: Observation, *, depth: int, logical: tuple[str, ...]) -> _Built | None:
        """A field type for `observed` at `depth`; None while there is nothing to build (only nulls)."""
        if is_unknown(observed):
            return None
        if is_struct(observed):
            if depth >= self.limits.max_nesting_depth or len(observed["fields"]) > self.limits.max_struct_fields:
                return _Built(StringType(), is_json=True)
            struct = _MStruct()
            notes: list[tuple[tuple[str, ...], str, str | None]] = []
            names: set[str] = set()
            for key, sub in observed["fields"].items():
                child = self._build(sub, depth=depth + 1, logical=logical + (key,))
                if child is None:
                    continue
                name = _sanitize(key)
                if child.is_json:
                    name = f"{name}__json"
                name = _unique_in(names, name)
                names.add(name.lower())
                struct.fields.append(_MField(0, name, child.type))
                notes.extend(((name,) + relative, note, note_key) for relative, note, note_key in child.notes)
                if child.is_json:
                    notes.append(((name,), JSON_NOTE, key if name != key else None))
                elif name != key:
                    notes.append(((name,), RENAMED_NOTE, key))
            if not struct.fields:
                return None
            return _Built(struct, notes)
        if is_list(observed):
            if depth >= self.limits.max_nesting_depth:
                return _Built(StringType(), is_json=True)
            element = self._build(observed["element"], depth=depth + 1, logical=logical + (ELEMENT,))
            if element is None:
                return None
            if element.is_json:
                return _Built(_MList(0, StringType()), [((ELEMENT,), JSON_NOTE, None)])
            return _Built(
                _MList(0, element.type),
                [((ELEMENT,) + relative, note, key) for relative, note, key in element.notes],
            )
        if observed == JSON:
            return _Built(StringType(), is_json=True)
        if observed == LONG and logical in self.double_paths:
            return _Built(DoubleType())
        return _Built(_scalar_type(observed))

    # -- ids, names, families ----------------------------------------------------------------

    def _allocate(self) -> int:
        value = self.next_id
        self.next_id += 1
        return value

    def _assign_ids(self, node: Any) -> None:
        """Ids for a new type's nested fields: a struct's fields first, then each one's type."""
        if isinstance(node, _MStruct):
            for item in node.fields:
                item.field_id = self._allocate()
            for item in node.fields:
                self._assign_ids(item.type)
        elif isinstance(node, _MList):
            node.element_id = self._allocate()
            self._assign_ids(node.element)

    def _families(self, struct: _MStruct, *, at_root: bool) -> dict[str, _Family]:
        by_id = {item.field_id: item for item in struct.fields}
        roots: dict[int, str] = {}
        families: dict[str, _Family] = {}
        pending: list[_MField] = []
        for item in struct.fields:
            if at_root and item.field_id in METADATA_FIELD_IDS:
                continue
            entry = self.entries.get(item.field_id)
            if entry is not None and entry.fork_of is not None and entry.fork_of in by_id:
                pending.append(item)
                continue
            key = entry.key if entry is not None and entry.key is not None else item.name
            roots[item.field_id] = key
            families.setdefault(key, _Family(key, [])).members.append(item)
        for item in pending:
            entry = self.entries[item.field_id]
            key = roots.get(entry.fork_of, item.name)
            families.setdefault(key, _Family(key, [])).members.append(item)
        for family in families.values():
            family.members.sort(key=lambda member: (self._fork_version(member), member.field_id))
        return families

    def _member_of(self, family: _Family, node_type: type) -> _MField | None:
        candidates = [
            member
            for member in family.members
            if isinstance(member.type, node_type) and not self._is_json_id(member.field_id)
        ]
        if not candidates:
            return None
        return max(candidates, key=self._fork_version)

    def _fork_version(self, member: _MField) -> int:
        entry = self.entries.get(member.field_id)
        return 1 if entry is None else entry.fork_version

    def _is_json_id(self, field_id: int) -> bool:
        entry = self.entries.get(field_id)
        return entry is not None and entry.state == COLLAPSED

    def _is_collapsed(self, family: _Family) -> bool:
        return any(self._is_json_id(member.field_id) for member in family.members)

    def _unique(self, struct: _MStruct, name: str) -> str:
        return _unique_in({item.name.lower() for item in struct.fields}, name)


def evolve(
    schema: Schema,
    fork_map: ForkMap,
    observations: Iterable[tuple[tuple[str, ...] | list[str], Observation]],
    *,
    limits: SchemaLimits | None = None,
    last_column_id: int | None = None,
    schema_id: int | None = None,
) -> Evolution:
    """Apply observations to `schema` in order; the result's schema is `schema` itself when unchanged.

    Each observation is `(path, observed)`: the logical path of record keys (`[]` for a list's
    element) where `observed` was seen, `()` for a whole record. New field ids continue from
    `last_column_id` (the table's, which may be above the schema's own highest id).
    """
    resolved_limits = SchemaLimits() if limits is None else limits
    base_last_id = schema.highest_field_id if last_column_id is None else max(last_column_id, schema.highest_field_id)
    evolver = _Evolver(schema, fork_map, resolved_limits, base_last_id)
    prepared = [evolver.prepare(tuple(path), observed) for path, observed in observations]
    for wrapped in prepared:
        if wrapped is not None:
            evolver.observe(wrapped)
    changes = tuple(evolver.changes)
    if not any(change.kind != REJECTED for change in changes):
        return Evolution(schema=schema, fork_map=fork_map, changes=changes, last_column_id=base_last_id)
    new_schema = Schema(
        *[_iceberg_field(item) for item in evolver.root.fields],
        schema_id=schema.schema_id + 1 if schema_id is None else schema_id,
        identifier_field_ids=list(schema.identifier_field_ids),
    )
    return Evolution(
        schema=new_schema,
        fork_map=ForkMap(dict(evolver.entries)),
        changes=changes,
        last_column_id=evolver.next_id - 1,
    )


def _id_at(node: Any, relative: tuple[str, ...]) -> int | None:
    """The id of the node at `relative` inside a new field's type."""
    found: int | None = None
    for segment in relative:
        if segment == ELEMENT:
            if not isinstance(node, _MList):
                return None
            found = node.element_id
            node = node.element
            continue
        if not isinstance(node, _MStruct):
            return None
        match = next((item for item in node.fields if item.name == segment), None)
        if match is None:
            return None
        found = match.field_id
        node = match.type
    return found


def _collect_double_paths(observed: Observation, path: tuple[str, ...], into: set[tuple[str, ...]]) -> None:
    if is_struct(observed):
        for key, sub in observed["fields"].items():
            _collect_double_paths(sub, path + (key,), into)
    elif is_list(observed):
        _collect_double_paths(observed["element"], path + (ELEMENT,), into)
    elif observed == DOUBLE:
        into.add(path)


def _wrap(path: tuple[str, ...], observed: Observation) -> Observation:
    """`observed` at `path`, as one observation of the whole record."""
    wrapped = observed
    for segment in reversed(path):
        if segment == ELEMENT:
            wrapped = {"type": "list", "element": wrapped}
        else:
            wrapped = {"type": "struct", "fields": {segment: wrapped}}
    return wrapped


def _sanitize(key: str) -> str:
    name = key.replace(".", "_")
    return name or "_"


def _unique_in(names_lower: set[str], name: str) -> str:
    if name.lower() not in names_lower:
        return name
    counter = 2
    while f"{name}__k{counter}".lower() in names_lower:
        counter += 1
    return f"{name}__k{counter}"


def _scalar_type(observed: Observation) -> PrimitiveType:
    if observed == BOOLEAN:
        return BooleanType()
    if observed == LONG:
        return LongType()
    if observed == DOUBLE:
        return DoubleType()
    return StringType()


def _count_mutable(struct: _MStruct) -> int:
    return sum(1 + _count_mutable_type(item.type) for item in struct.fields)


def _count_mutable_type(node: Any) -> int:
    if isinstance(node, _MStruct):
        return _count_mutable(node)
    if isinstance(node, _MList):
        return 1 + _count_mutable_type(node.element)
    if isinstance(node, IcebergType) and hasattr(node, "key_id"):
        return 2
    return 0


def _describe_mutable(node: Any) -> str:
    return type_text(_to_iceberg(node))


def type_text(iceberg_type: IcebergType) -> str:
    """The type as the API shows it: `long`, `struct`, `list<string>`, `list<struct>`."""
    if isinstance(iceberg_type, StructType):
        return "struct"
    if isinstance(iceberg_type, ListType):
        return f"list<{type_text(iceberg_type.element_type)}>"
    if hasattr(iceberg_type, "key_type") and hasattr(iceberg_type, "value_type"):
        return f"map<{type_text(iceberg_type.key_type)}, {type_text(iceberg_type.value_type)}>"
    return str(iceberg_type)


__all__ = [
    "ADDED",
    "COLLAPSED_CHANGE",
    "Evolution",
    "FORKED",
    "REJECTED",
    "REJECTED_FIELD_LIMIT",
    "SchemaChange",
    "SchemaLimits",
    "describe",
    "element_compatible",
    "evolve",
    "scalar_accepts",
    "type_text",
    "UNKNOWN",
]


def limits_of(config: Any) -> SchemaLimits:
    """The schema caps a runtime configuration (or anything with the same fields) sets."""
    return SchemaLimits(
        max_forks_per_field=int(getattr(config, "max_forks_per_field", 3)),
        max_struct_fields=int(getattr(config, "max_struct_fields", 300)),
        max_nesting_depth=int(getattr(config, "max_nesting_depth", 10)),
        max_schema_fields=int(getattr(config, "max_schema_fields", 2000)),
    )
