"""Conform a raw record to a table schema: each value into the field that takes it (spec §7.3).

Runs in the seal's child process, once per staged row. A value goes to the member of its key's
family whose type matches it exactly, newest first, else to the newest member that accepts it (a
whole number into a double, any scalar into a string), else - nothing taking it - into
`_unmapped`, as JSON keyed by its logical path. A collapsed family takes every value as JSON text.
Nothing is ever dropped: a value is in exactly one place.

A key the schema does not know yet whose value is null or empty (`{}`, `[]`) is not unmapped: it
carries nothing a field could hold, and counting it would flag every record with an empty array.
"""

from __future__ import annotations

import json
from typing import Any

from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, LongType, StringType

from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.index import (
    ElementIndex,
    Family,
    Member,
    SchemaIndex,
    StructIndex,
    build_index,
)

ROOT_VALUE_PATH = "$"

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

#: Returned by `scalar_value` (and internally) when a node does not take a value.
NOT_TAKEN = object()
_NO = NOT_TAKEN


class Conformer:
    """Conforms records to one schema; build once per schema id and reuse for every row."""

    def __init__(self, schema: Schema, fork_map: ForkMap, *, index: SchemaIndex | None = None) -> None:
        self.schema = schema
        self.fork_map = fork_map
        self.index = build_index(schema, fork_map) if index is None else index
        self._orders: dict[int, dict[str, list[Member]]] = {}

    def conform(self, value: Any) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """The payload columns for one record (top-level field name -> value) and its unmapped values."""
        unmapped: dict[str, Any] = {}
        row: dict[str, Any] = {}
        if value is None:
            return row, None
        if not isinstance(value, dict):
            unmapped[ROOT_VALUE_PATH] = value
            return row, unmapped
        self._struct_into(self.index.root, value, row, unmapped, "")
        return row, (unmapped or None)

    # -- structs ---------------------------------------------------------------------------------

    def _struct_into(
        self,
        struct: StructIndex,
        value: dict,
        out: dict[str, Any],
        unmapped: dict[str, Any],
        prefix: str,
    ) -> None:
        families = struct.families
        for key, item in value.items():
            if item is None:
                continue
            path = f"{prefix}.{key}" if prefix else str(key)
            family = families.get(key)
            if family is None:
                if not _is_empty(item):
                    unmapped[path] = item
                continue
            if family.collapsed:
                member = family.json_member
                out[member.name] = _json_text(item)
                continue
            for member in self._candidates(struct, family, item):
                local: dict[str, Any] = {}
                converted = self._member_value(member, item, path, local)
                if converted is not _NO:
                    out[member.name] = converted
                    if local:
                        unmapped.update(local)
                    break
            else:
                unmapped[path] = item

    def _candidates(self, struct: StructIndex, family: Family, value: Any) -> list[Member]:
        """Members to try for `value`: exact kind first, newest first; then the rest, newest first."""
        kind = _natural_kind(value)
        by_kind = self._orders.setdefault(id(family), {})
        order = by_kind.get(kind)
        if order is None:
            newest_first = sorted(family.members, key=lambda member: member.fork_version, reverse=True)
            exact = [member for member in newest_first if _member_kind(member) == kind]
            rest = [member for member in newest_first if _member_kind(member) != kind]
            order = exact + rest
            by_kind[kind] = order
        return order

    # -- members and elements --------------------------------------------------------------------

    def _member_value(self, member: Member, value: Any, path: str, unmapped: dict[str, Any]) -> Any:
        if member.json:
            return _json_text(value)
        if member.struct is not None:
            if not isinstance(value, dict):
                return _NO
            out: dict[str, Any] = {}
            self._struct_into(member.struct, value, out, unmapped, path)
            return out
        if member.element is not None:
            if not isinstance(value, list):
                return _NO
            return self._list_value(member.element, value, path, unmapped)
        return scalar_value(member.type, value)

    def _list_value(self, element: ElementIndex, value: list, path: str, unmapped: dict[str, Any]) -> Any:
        items: list[Any] = []
        local: dict[str, Any] = {}
        for position, item in enumerate(value):
            if item is None:
                items.append(None)
                continue
            converted = self._element_value(element, item, f"{path}[{position}]", local)
            if converted is _NO:
                return _NO
            items.append(converted)
        if local:
            unmapped.update(local)
        return items

    def _element_value(self, element: ElementIndex, value: Any, path: str, unmapped: dict[str, Any]) -> Any:
        if element.json:
            return _json_text(value)
        if element.struct is not None:
            if not isinstance(value, dict):
                return _NO
            out: dict[str, Any] = {}
            self._struct_into(element.struct, value, out, unmapped, path)
            return out
        if element.element is not None:
            if not isinstance(value, list):
                return _NO
            return self._list_value(element.element, value, path, unmapped)
        return scalar_value(element.type, value)


def scalar_value(field_type, value: Any) -> Any:
    """`value` as a primitive field of `field_type` holds it, or `NOT_TAKEN`."""
    if isinstance(field_type, StringType):
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return json.dumps(value)
        return _NO
    if isinstance(field_type, DoubleType):
        if isinstance(value, bool):
            return _NO
        if isinstance(value, (int, float)):
            return float(value)
        return _NO
    if isinstance(field_type, LongType):
        if isinstance(value, bool) or not isinstance(value, int):
            return _NO
        return value if _INT64_MIN <= value <= _INT64_MAX else _NO
    if isinstance(field_type, BooleanType):
        return value if isinstance(value, bool) else _NO
    return _NO


def _natural_kind(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long" if _INT64_MIN <= value <= _INT64_MAX else "double"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "struct"
    if isinstance(value, list):
        return "list"
    return "json"


def _member_kind(member: Member) -> str:
    if member.json:
        return "json"
    if member.struct is not None:
        return "struct"
    if member.element is not None:
        return "list"
    return str(member.type)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (dict, list)) and not value:
        return True
    return False


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=False, default=str)


def unmapped_text(unmapped: dict[str, Any] | None) -> str | None:
    """The `_unmapped` column's value for a row: its unmapped values as one JSON object."""
    if not unmapped:
        return None
    return json.dumps(unmapped, ensure_ascii=False, separators=(",", ":"), default=str)
