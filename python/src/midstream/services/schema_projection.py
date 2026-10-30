"""Building the control plane's projection of one table's Iceberg schema (spec §9.3)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pyiceberg.schema import Schema

from midstream.lakehouse.lifecycle import schema_to_json
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.render import render_schema
from midstream.models import MaterializationSchema
from midstream.protection.policy import TableProtection


def annotate_protection(fields: Iterable[dict[str, Any]], protection: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The rendered fields, each protected one with `protection: {kind, mask, since_schema_id}`
    (design §6.3). Fields of a table that records no protection come back as they were."""
    items = [dict(item) for item in fields]
    if not protection:
        return items
    active = TableProtection.from_dict(protection).active_fields()
    if not active:
        return items
    for item in items:
        field_id = item.get("field_id")
        entry = active.get(field_id) if isinstance(field_id, int) else None
        if entry is not None:
            item["protection"] = {"kind": entry.kind, "mask": entry.mask, "since_schema_id": entry.since}
    return items


def build_schema_projection(
    materialization_id: str,
    table: str,
    schema: Schema,
    fork_map: ForkMap,
    *,
    since: str,
    metadata_location: str | None = None,
    changes: Iterable[dict[str, Any]] = (),
    last_column_id: int | None = None,
    schemas: Iterable[Schema] = (),
    rejected_paths: Iterable[Iterable[str]] = (),
    protection: dict[str, Any] | None = None,
) -> MaterializationSchema:
    rendered = render_schema(schema, fork_map, schemas=list(schemas) or [schema])
    return MaterializationSchema(
        materialization_id=materialization_id,
        table=table,
        schema_id=schema.schema_id,
        since=since,
        metadata_location=metadata_location,
        field_count=rendered["field_count"],
        fields=annotate_protection(rendered["fields"], protection),
        fork_map=fork_map.to_dict(),
        iceberg_schema=schema_to_json(schema),
        changes=[dict(item) for item in changes],
        last_column_id=schema.highest_field_id if last_column_id is None else last_column_id,
        rejected_paths=[list(item) for item in rejected_paths],
        protection=dict(protection or {}),
    )
