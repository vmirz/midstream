"""Persistence helpers for idempotency and operation records."""

from __future__ import annotations

from typing import Any

from materializer_control_plane.storage.envelopes import decode_envelope, encode_payload_envelope
from materializer_control_plane.storage.keys import (
    materialization_operation_key,
    materialization_operation_prefix,
)
from materializer_control_plane.storage.table import TableProtocol

ENTITY_TYPE = "materialization-op"


def save_operation_record(
    table: TableProtocol,
    *,
    materialization_id: str,
    operation_id: str,
    updated_at: str,
    payload: dict[str, Any],
    version: int = 1,
) -> dict[str, Any]:
    """Persist an operation record used for workflow traceability and idempotency."""
    stored = encode_payload_envelope(
        entity_type=ENTITY_TYPE,
        entity_id=f"{materialization_id}:{operation_id}",
        version=version,
        updated_at=updated_at,
        payload=dict(payload),
    )
    table[materialization_operation_key(materialization_id, operation_id)] = stored
    return stored


def get_operation_record(
    table: TableProtocol,
    *,
    materialization_id: str,
    operation_id: str,
) -> dict[str, Any] | None:
    """Load a previously persisted operation record, if one exists."""
    envelope = decode_envelope(
        table.get(materialization_operation_key(materialization_id, operation_id)),
        expected_entity_type=ENTITY_TYPE,
    )
    if envelope is None:
        return None
    return envelope.to_dict()


def delete_operation_record(
    table: TableProtocol,
    *,
    materialization_id: str,
    operation_id: str,
) -> None:
    """Delete a persisted operation record."""
    del table[materialization_operation_key(materialization_id, operation_id)]


def delete_operation_records(table: TableProtocol, materialization_id: str) -> int:
    """Delete every operation record retained for a materialization."""
    keys = [
        key
        for key, _ in table.prefix_scan(materialization_operation_prefix(materialization_id))
    ]
    for key in keys:
        del table[key]
    return len(keys)