"""Persistence of the schema projections: each table's current schema and an archive of earlier ones.

The tables' Iceberg metadata is the truth (spec §9.3); these records are what the API answers from.
Every table of a materialization has schemas of its own: `materialization-schema:<id>:<table>`
holds a table's current projection and `materialization-schema-version:<id>:<table>:<schema_id>`
archives each one, bounded by `schema_history_limit` per table.
"""

from __future__ import annotations

from collections.abc import Iterator
import logging
from typing import Any

from materializer_control_plane.config import ControlPlaneConfig, get_config
from materializer_control_plane.models import MaterializationSchema, RecordEnvelope
from materializer_control_plane.storage.envelopes import decode_envelope, encode_payload_envelope
from materializer_control_plane.storage.keys import (
    materialization_schema_key,
    materialization_schema_prefix,
    materialization_schema_version_key,
    materialization_schema_version_prefix,
)
from materializer_control_plane.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

ENTITY_TYPE = "materialization-schema"
VERSION_ENTITY_TYPE = "materialization-schema-version"
#: Version 3 envelopes hold one table's projection; 2 held a single-table materialization's and
#: 1 the retired authority's schema. Neither is read.
ENVELOPE_VERSION = 3


def save_materialization_schema(
    table: TableProtocol,
    schema: MaterializationSchema,
    *,
    archive: bool = True,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    """Persist `schema` as its table's current projection (and archive it). An older one never wins."""
    current = get_materialization_schema(table, schema.materialization_id, schema.table)
    stored = _encode(ENTITY_TYPE, schema)
    if current is None or current.schema_id <= schema.schema_id:
        table[materialization_schema_key(schema.materialization_id, schema.table)] = stored
    if archive:
        save_schema_version(table, schema, config=config)
    return stored


def get_materialization_schema(table: TableProtocol, materialization_id: str, table_name: str) -> MaterializationSchema | None:
    envelope = _decode(table.get(materialization_schema_key(materialization_id, table_name)), ENTITY_TYPE)
    return None if envelope is None else MaterializationSchema.from_dict(envelope.payload)


def resolve_materialization_schema(table: TableProtocol, materialization_id: str, table_name: str) -> MaterializationSchema | None:
    """A table's current projection, restored from the archive if a changelog replay rolled it back.

    The control-plane table's changelog is partitioned by the triggering event's partition, so a
    lagging partition can replay a stale value over a newer one during recovery (handbook 09). Each
    archived schema owns its own key, so the highest archived id is reliable.
    """
    current = get_materialization_schema(table, materialization_id, table_name)
    latest = _latest_archived(table, materialization_id, table_name)
    if latest is None or (current is not None and current.schema_id >= latest.schema_id):
        return current
    LOGGER.warning(
        "Schema projection of %s table %s regressed to %s; restoring schema %s from the archive.",
        materialization_id,
        table_name,
        None if current is None else current.schema_id,
        latest.schema_id,
    )
    table[materialization_schema_key(materialization_id, table_name)] = _encode(ENTITY_TYPE, latest)
    return latest


def delete_materialization_schemas(table: TableProtocol, materialization_id: str) -> int:
    """Delete every table's current projection; return how many."""
    keys = [key for key, _ in table.prefix_scan(materialization_schema_prefix(materialization_id))]
    for key in keys:
        del table[key]
    return len(keys)


def delete_table_schemas(table: TableProtocol, materialization_id: str, table_name: str) -> int:
    """Delete one table's current projection and its archive, as a dropped table leaves; return how many."""
    projection = materialization_schema_key(materialization_id, table_name)
    keys = [key for key, _ in table.prefix_scan(projection) if key == projection]
    keys += [key for key, _ in table.prefix_scan(materialization_schema_version_prefix(materialization_id, table_name))]
    for key in keys:
        del table[key]
    return len(keys)


def save_schema_version(
    table: TableProtocol,
    schema: MaterializationSchema,
    *,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    stored = _encode(VERSION_ENTITY_TYPE, schema)
    table[materialization_schema_version_key(schema.materialization_id, schema.table, schema.schema_id)] = stored
    cfg = get_config() if config is None else config
    _prune(table, schema.materialization_id, schema.table, limit=cfg.schema_history_limit)
    return stored


def get_schema_version(
    table: TableProtocol, materialization_id: str, table_name: str, schema_id: int
) -> MaterializationSchema | None:
    envelope = _decode(
        table.get(materialization_schema_version_key(materialization_id, table_name, schema_id)),
        VERSION_ENTITY_TYPE,
    )
    return None if envelope is None else MaterializationSchema.from_dict(envelope.payload)


def list_schema_versions(
    table: TableProtocol,
    materialization_id: str,
    table_name: str,
    *,
    limit: int | None = None,
) -> list[MaterializationSchema]:
    """A table's archived schemas, newest first."""
    schemas = [
        MaterializationSchema.from_dict(envelope.payload)
        for envelope in _iter_versions(table, materialization_id, table_name)
    ]
    schemas.sort(key=lambda item: item.schema_id, reverse=True)
    return schemas if limit is None else schemas[:limit]


def delete_schema_versions(table: TableProtocol, materialization_id: str) -> int:
    """Delete every archived schema of every table of the materialization; return how many."""
    keys = [key for key, _ in table.prefix_scan(materialization_schema_version_prefix(materialization_id))]
    for key in keys:
        del table[key]
    return len(keys)


def _encode(entity_type: str, schema: MaterializationSchema) -> dict[str, Any]:
    return encode_payload_envelope(
        entity_type=entity_type,
        entity_id=f"{schema.materialization_id}:{schema.table}",
        version=ENVELOPE_VERSION,
        updated_at=schema.since,
        payload=schema.to_dict(),
    )


def _decode(raw: dict[str, Any] | None, entity_type: str) -> RecordEnvelope | None:
    envelope = decode_envelope(raw, expected_entity_type=entity_type)
    if envelope is None or envelope.version < ENVELOPE_VERSION:
        # Written by a control plane before materializations read several topics.
        return None
    return envelope


def _latest_archived(table: TableProtocol, materialization_id: str, table_name: str) -> MaterializationSchema | None:
    latest = None
    for envelope in _iter_versions(table, materialization_id, table_name):
        candidate = MaterializationSchema.from_dict(envelope.payload)
        if latest is None or candidate.schema_id > latest.schema_id:
            latest = candidate
    return latest


def _iter_versions(table: TableProtocol, materialization_id: str, table_name: str) -> Iterator[RecordEnvelope]:
    for _, raw in table.prefix_scan(materialization_schema_version_prefix(materialization_id, table_name)):
        envelope = _decode(raw, VERSION_ENTITY_TYPE)
        if envelope is not None:
            yield envelope


def _prune(table: TableProtocol, materialization_id: str, table_name: str, *, limit: int) -> None:
    for schema in list_schema_versions(table, materialization_id, table_name)[limit:]:
        del table[materialization_schema_version_key(materialization_id, table_name, schema.schema_id)]
