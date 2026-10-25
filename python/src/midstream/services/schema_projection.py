"""Building the control plane's projection of one table's Iceberg schema (spec §9.3)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pyiceberg.schema import Schema

from midstream.lakehouse.lifecycle import schema_to_json
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.render import render_schema
from midstream.models import MaterializationSchema


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
) -> MaterializationSchema:
    rendered = render_schema(schema, fork_map, schemas=list(schemas) or [schema])
    return MaterializationSchema(
        materialization_id=materialization_id,
        table=table,
        schema_id=schema.schema_id,
        since=since,
        metadata_location=metadata_location,
        field_count=rendered["field_count"],
        fields=rendered["fields"],
        fork_map=fork_map.to_dict(),
        iceberg_schema=schema_to_json(schema),
        changes=[dict(item) for item in changes],
        last_column_id=schema.highest_field_id if last_column_id is None else last_column_id,
        rejected_paths=[list(item) for item in rejected_paths],
    )
