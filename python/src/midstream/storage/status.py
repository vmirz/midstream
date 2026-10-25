"""Persistence helpers for materialization status records."""

from __future__ import annotations

from typing import Any

from midstream.models import MaterializationStatus
from midstream.storage.envelopes import decode_envelope, encode_payload_envelope
from midstream.storage.keys import materialization_status_key
from midstream.storage.table import TableProtocol

ENTITY_TYPE = "materialization-status"


def save_materialization_status(
    table: TableProtocol,
    status: MaterializationStatus,
    *,
    version: int = 1,
) -> dict[str, Any]:
    """Persist the current status projection for a materialization."""
    stored = encode_payload_envelope(
        entity_type=ENTITY_TYPE,
        entity_id=status.materialization_id,
        version=version,
        updated_at=status.last_transition_time,
        payload=status.to_dict(),
    )
    table[materialization_status_key(status.materialization_id)] = stored
    return stored


def get_materialization_status(
    table: TableProtocol,
    materialization_id: str,
) -> MaterializationStatus | None:
    """Load a persisted status projection for a materialization, if present."""
    envelope = decode_envelope(
        table.get(materialization_status_key(materialization_id)),
        expected_entity_type=ENTITY_TYPE,
    )
    if envelope is None:
        return None
    return MaterializationStatus.from_dict(envelope.payload)


def delete_materialization_status(table: TableProtocol, materialization_id: str) -> None:
    """Delete the persisted status projection for a materialization."""
    del table[materialization_status_key(materialization_id)]