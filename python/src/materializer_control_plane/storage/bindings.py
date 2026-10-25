"""Persistence helpers for runtime binding records."""

from __future__ import annotations

from typing import Any

from materializer_control_plane.models import RuntimeBinding
from materializer_control_plane.storage.envelopes import decode_envelope, encode_payload_envelope
from materializer_control_plane.storage.keys import runtime_binding_key
from materializer_control_plane.storage.table import TableProtocol

ENTITY_TYPE = "runtime-binding"


def save_runtime_binding(
    table: TableProtocol,
    binding: RuntimeBinding,
    *,
    version: int = 1,
) -> dict[str, Any]:
    """Persist the runtime binding that links a materialization to its KasprApp."""
    updated_at = binding.updated_at or binding.created_at or "unknown"
    stored = encode_payload_envelope(
        entity_type=ENTITY_TYPE,
        entity_id=binding.materialization_id,
        version=version,
        updated_at=updated_at,
        payload=binding.to_dict(),
    )
    table[runtime_binding_key(binding.materialization_id)] = stored
    return stored


def get_runtime_binding(table: TableProtocol, materialization_id: str) -> RuntimeBinding | None:
    """Load the runtime binding for a materialization, if one exists."""
    envelope = decode_envelope(
        table.get(runtime_binding_key(materialization_id)),
        expected_entity_type=ENTITY_TYPE,
    )
    if envelope is None:
        return None
    return RuntimeBinding.from_dict(envelope.payload)


def delete_runtime_binding(table: TableProtocol, materialization_id: str) -> None:
    """Delete a materialization's persisted runtime binding record."""
    del table[runtime_binding_key(materialization_id)]