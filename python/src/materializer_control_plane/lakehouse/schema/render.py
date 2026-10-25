"""The schema as the API and the console show it (spec §9.3).

One entry per field, with the Iceberg type as text, the physical path, the logical name a person
reads (`customer.email`, `items[].sku`, and one name for every member of a fork family) and the
member's state. `since_schema_id` comes from the table's schema history when it is given.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pyiceberg.schema import Schema
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, StructType

from materializer_control_plane.lakehouse.schema.forkmap import ELEMENT, ForkMap
from materializer_control_plane.lakehouse.schema.index import ElementIndex, Member, StructIndex, build_index
from materializer_control_plane.lakehouse.tables import METADATA_FIELD_IDS

KIND_VALUE = "value"
KIND_CONTAINER = "container"
KIND_JSON = "json"
KIND_METADATA = "metadata"


def logical_name(path: Iterable[str]) -> str:
    """`("items", "[]", "sku")` -> `items[].sku`."""
    text = ""
    for segment in path:
        if segment == ELEMENT:
            text += "[]"
        elif text:
            text += f".{segment}"
        else:
            text = segment
    return text


def type_text(iceberg_type: IcebergType, *, element_json: bool = False) -> str:
    if isinstance(iceberg_type, StructType):
        return "struct"
    if isinstance(iceberg_type, ListType):
        if element_json:
            return "list<json>"
        return f"list<{type_text(iceberg_type.element_type)}>"
    if isinstance(iceberg_type, MapType):
        return f"map<{type_text(iceberg_type.key_type)}, {type_text(iceberg_type.value_type)}>"
    return str(iceberg_type)


def first_seen_schema_ids(schemas: Iterable[Schema]) -> dict[int, int]:
    """For each field id, the lowest schema id that has it."""
    since: dict[int, int] = {}
    for schema in sorted(schemas, key=lambda item: item.schema_id):
        for field_id in _field_ids(schema.as_struct()):
            since.setdefault(field_id, schema.schema_id)
    return since


def render_fields(
    schema: Schema,
    fork_map: ForkMap,
    *,
    since: dict[int, int] | None = None,
    include_metadata: bool = True,
) -> list[dict[str, Any]]:
    index = build_index(schema, fork_map)
    rendered: list[dict[str, Any]] = []
    if include_metadata:
        for item in schema.fields:
            if item.field_id in METADATA_FIELD_IDS:
                rendered.append(_metadata_entry(item, since))
    _render_struct(index.root, rendered, since)
    return rendered


def render_schema(
    schema: Schema,
    fork_map: ForkMap,
    *,
    schemas: Iterable[Schema] = (),
    include_metadata: bool = True,
) -> dict[str, Any]:
    """`{schema_id, field_count, fields}` for one schema; `schemas` is the history for `since_schema_id`."""
    history = list(schemas) or [schema]
    fields = render_fields(schema, fork_map, since=first_seen_schema_ids(history), include_metadata=include_metadata)
    return {
        "schema_id": schema.schema_id,
        "field_count": sum(1 for item in fields if item["kind"] != KIND_METADATA),
        "fields": fields,
    }


def _metadata_entry(item: NestedField, since: dict[int, int] | None) -> dict[str, Any]:
    return {
        "field_id": item.field_id,
        "path": [item.name],
        "name": item.name,
        "type": type_text(item.field_type),
        "required": item.required,
        "since_schema_id": 0 if since is None else since.get(item.field_id, 0),
        "fork_version": 1,
        "state": "current",
        "kind": KIND_METADATA,
        "doc": item.doc,
    }


def _render_struct(struct: StructIndex, out: list[dict[str, Any]], since: dict[int, int] | None) -> None:
    for family in struct.families.values():
        for member in family.members:
            out.append(_member_entry(member, since))
            if member.struct is not None:
                _render_struct(member.struct, out, since)
            elif member.element is not None:
                _render_element(member.element, out, since)


def _render_element(element: ElementIndex, out: list[dict[str, Any]], since: dict[int, int] | None) -> None:
    if element.struct is not None:
        _render_struct(element.struct, out, since)
    elif element.element is not None:
        _render_element(element.element, out, since)


def _member_entry(member: Member, since: dict[int, int] | None) -> dict[str, Any]:
    element_json = member.element is not None and member.element.json
    if member.json:
        kind = KIND_JSON
    elif isinstance(member.type, (StructType, ListType, MapType)):
        kind = KIND_CONTAINER if not element_json else KIND_JSON
    else:
        kind = KIND_VALUE
    entry: dict[str, Any] = {
        "field_id": member.field_id,
        "path": list(member.path),
        "name": logical_name(member.logical_path),
        "type": type_text(member.type, element_json=element_json),
        "required": member.required,
        "fork_version": member.fork_version,
        "state": member.state,
        "kind": kind,
    }
    if since is not None and member.field_id in since:
        entry["since_schema_id"] = since[member.field_id]
    if member.fork_of is not None:
        entry["fork_of"] = member.fork_of
    return entry


def _field_ids(struct: StructType) -> list[int]:
    ids: list[int] = []
    for item in struct.fields:
        ids.append(item.field_id)
        ids.extend(_type_ids(item.field_type))
    return ids


def _type_ids(iceberg_type: IcebergType) -> list[int]:
    if isinstance(iceberg_type, StructType):
        return _field_ids(iceberg_type)
    if isinstance(iceberg_type, ListType):
        return [iceberg_type.element_id, *_type_ids(iceberg_type.element_type)]
    if isinstance(iceberg_type, MapType):
        return [iceberg_type.key_id, iceberg_type.value_id, *_type_ids(iceberg_type.value_type)]
    return []
