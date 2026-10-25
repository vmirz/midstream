"""An index of a table schema by record key: which fields hold each key's values.

Built once per schema id from the Iceberg schema and the fork map, and shared by everything that
maps record values onto fields: the seal's conform step, the writer's drift detection, the
evolution policy and the API projection.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyiceberg.schema import Schema
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, StructType

from materializer_control_plane.lakehouse.schema.forkmap import (
    COLLAPSED,
    CURRENT,
    ELEMENT,
    SUPERSEDED,
    ForkMap,
)
from materializer_control_plane.lakehouse.tables import METADATA_FIELD_IDS

ROOT_STRUCT_ID = 0


@dataclass(slots=True)
class ElementIndex:
    """A list's element."""

    element_id: int
    type: IcebergType
    required: bool
    json: bool
    path: tuple[str, ...]
    logical_path: tuple[str, ...]
    struct: "StructIndex | None" = None
    element: "ElementIndex | None" = None


@dataclass(slots=True)
class Member:
    """One field of a family."""

    field_id: int
    name: str
    type: IcebergType
    required: bool
    fork_version: int
    state: str
    json: bool
    path: tuple[str, ...]
    logical_path: tuple[str, ...]
    fork_of: int | None = None
    struct: "StructIndex | None" = None
    element: ElementIndex | None = None

    @property
    def kind(self) -> str:
        """`json`, `struct`, `list`, `map`, or the primitive type's name."""
        if self.json:
            return "json"
        return type_kind(self.type)


@dataclass(slots=True)
class Family:
    """Every field holding one record key's values in one struct, oldest first."""

    key: str
    members: list[Member] = field(default_factory=list)

    @property
    def root(self) -> Member:
        return self.members[0]

    @property
    def collapsed(self) -> bool:
        """Whether the key's values are now written as JSON text (only a collapse adds a JSON field)."""
        return any(member.json for member in self.members)

    @property
    def json_member(self) -> Member | None:
        for member in reversed(self.members):
            if member.json:
                return member
        return None

    @property
    def forks(self) -> int:
        """Members created by forking (fork version 2 and up), the collapse field excluded."""
        return sum(1 for member in self.members if member.fork_version >= 2 and not member.json)

    @property
    def newest(self) -> Member:
        return max(self.members, key=lambda member: member.fork_version)


@dataclass(slots=True)
class StructIndex:
    struct_id: int
    path: tuple[str, ...]
    logical_path: tuple[str, ...]
    families: dict[str, Family] = field(default_factory=dict)
    #: Lower-cased physical names of every field in the struct: names are unique case-insensitively.
    names: set[str] = field(default_factory=set)
    width: int = 0


@dataclass(slots=True)
class SchemaIndex:
    schema_id: int
    root: StructIndex
    members: dict[int, Member]
    elements: dict[int, ElementIndex]
    field_count: int
    last_column_id: int


def type_kind(iceberg_type: IcebergType) -> str:
    if isinstance(iceberg_type, StructType):
        return "struct"
    if isinstance(iceberg_type, ListType):
        return "list"
    if isinstance(iceberg_type, MapType):
        return "map"
    return str(iceberg_type)


def build_index(schema: Schema, fork_map: ForkMap, *, last_column_id: int | None = None) -> SchemaIndex:
    """Index `schema`'s payload fields by record key, with `fork_map`'s families and renames."""
    members: dict[int, Member] = {}
    elements: dict[int, ElementIndex] = {}
    payload = [item for item in schema.fields if item.field_id not in METADATA_FIELD_IDS]
    root = _index_struct(
        payload,
        struct_id=ROOT_STRUCT_ID,
        path=(),
        logical_path=(),
        fork_map=fork_map,
        members=members,
        elements=elements,
    )
    # Metadata names count for collisions at the top level; they are not families.
    for item in schema.fields:
        if item.field_id in METADATA_FIELD_IDS:
            root.names.add(item.name.lower())
    return SchemaIndex(
        schema_id=schema.schema_id,
        root=root,
        members=members,
        elements=elements,
        field_count=_count_ids(schema.as_struct()),
        last_column_id=schema.highest_field_id if last_column_id is None else last_column_id,
    )


def _index_struct(
    fields: list[NestedField] | tuple[NestedField, ...],
    *,
    struct_id: int,
    path: tuple[str, ...],
    logical_path: tuple[str, ...],
    fork_map: ForkMap,
    members: dict[int, Member],
    elements: dict[int, ElementIndex],
) -> StructIndex:
    index = StructIndex(struct_id=struct_id, path=path, logical_path=logical_path, width=len(fields))
    by_id = {item.field_id: item for item in fields}
    roots: dict[int, str] = {}
    pending: list[tuple[NestedField, object]] = []
    for item in fields:
        index.names.add(item.name.lower())
        entry = fork_map.get(item.field_id)
        if entry is not None and entry.fork_of is not None and entry.fork_of in by_id:
            pending.append((item, entry))
            continue
        key = entry.key if entry is not None and entry.key is not None else item.name
        roots[item.field_id] = key
        family = index.families.setdefault(key, Family(key=key))
        family.members.append(
            _member(item, entry, key=key, path=path, logical_path=logical_path, fork_map=fork_map, members=members, elements=elements)
        )
    for item, entry in pending:
        key = roots.get(entry.fork_of) or item.name
        family = index.families.setdefault(key, Family(key=key))
        family.members.append(
            _member(item, entry, key=key, path=path, logical_path=logical_path, fork_map=fork_map, members=members, elements=elements)
        )
    for family in index.families.values():
        family.members.sort(key=lambda member: (member.fork_version, member.field_id))
        if len(family.members) == 1 and family.members[0].fork_of is None and family.members[0].state == SUPERSEDED:
            family.members[0].state = CURRENT
    return index


def _member(
    item: NestedField,
    entry,
    *,
    key: str,
    path: tuple[str, ...],
    logical_path: tuple[str, ...],
    fork_map: ForkMap,
    members: dict[int, Member],
    elements: dict[int, ElementIndex],
) -> Member:
    member_path = path + (item.name,)
    member_logical = logical_path + (key,)
    member = Member(
        field_id=item.field_id,
        name=item.name,
        type=item.field_type,
        required=item.required,
        fork_version=1 if entry is None else entry.fork_version,
        state=CURRENT if entry is None else entry.state,
        json=entry is not None and entry.is_json,
        path=member_path,
        logical_path=member_logical,
        fork_of=None if entry is None else entry.fork_of,
    )
    if not member.json:
        if isinstance(item.field_type, StructType):
            member.struct = _index_struct(
                item.field_type.fields,
                struct_id=item.field_id,
                path=member_path,
                logical_path=member_logical,
                fork_map=fork_map,
                members=members,
                elements=elements,
            )
        elif isinstance(item.field_type, ListType):
            member.element = _element(
                item.field_type,
                path=member_path,
                logical_path=member_logical,
                fork_map=fork_map,
                members=members,
                elements=elements,
            )
    members[item.field_id] = member
    return member


def _element(
    list_type: ListType,
    *,
    path: tuple[str, ...],
    logical_path: tuple[str, ...],
    fork_map: ForkMap,
    members: dict[int, Member],
    elements: dict[int, ElementIndex],
) -> ElementIndex:
    entry = fork_map.get(list_type.element_id)
    element_path = path + (ELEMENT,)
    element_logical = logical_path + (ELEMENT,)
    element = ElementIndex(
        element_id=list_type.element_id,
        type=list_type.element_type,
        required=list_type.element_required,
        json=entry is not None and entry.is_json,
        path=element_path,
        logical_path=element_logical,
    )
    if not element.json:
        if isinstance(list_type.element_type, StructType):
            element.struct = _index_struct(
                list_type.element_type.fields,
                struct_id=list_type.element_id,
                path=element_path,
                logical_path=element_logical,
                fork_map=fork_map,
                members=members,
                elements=elements,
            )
        elif isinstance(list_type.element_type, ListType):
            element.element = _element(
                list_type.element_type,
                path=element_path,
                logical_path=element_logical,
                fork_map=fork_map,
                members=members,
                elements=elements,
            )
    elements[list_type.element_id] = element
    return element


def _count_ids(struct: StructType) -> int:
    count = 0
    for item in struct.fields:
        count += 1 + _count_type_ids(item.field_type)
    return count


def _count_type_ids(iceberg_type: IcebergType) -> int:
    if isinstance(iceberg_type, StructType):
        return _count_ids(iceberg_type)
    if isinstance(iceberg_type, ListType):
        return 1 + _count_type_ids(iceberg_type.element_type)
    if isinstance(iceberg_type, MapType):
        return 2 + _count_type_ids(iceberg_type.key_type) + _count_type_ids(iceberg_type.value_type)
    return 0
