"""Persistence helpers for a materialization's runtime autoscaling record."""

from __future__ import annotations

from typing import Any

from midstream.models.autoscale import RuntimeAutoscale
from midstream.storage.envelopes import decode_envelope, encode_payload_envelope
from midstream.storage.keys import runtime_autoscale_key
from midstream.storage.table import TableProtocol

ENTITY_TYPE = "runtime-autoscale"


def save_runtime_autoscale(table: TableProtocol, autoscale: RuntimeAutoscale) -> dict[str, Any]:
    """Persist how the control plane scaled a materialization's runtime, and from what."""
    stored = encode_payload_envelope(
        entity_type=ENTITY_TYPE,
        entity_id=autoscale.materialization_id,
        version=1,
        updated_at=autoscale.updated_at or "unknown",
        payload=autoscale.to_dict(),
    )
    table[runtime_autoscale_key(autoscale.materialization_id)] = stored
    return stored


def get_runtime_autoscale(table: TableProtocol, materialization_id: str) -> RuntimeAutoscale | None:
    """The materialization's autoscaling record, if it has one."""
    envelope = decode_envelope(table.get(runtime_autoscale_key(materialization_id)), expected_entity_type=ENTITY_TYPE)
    if envelope is None:
        return None
    return RuntimeAutoscale.from_dict(envelope.payload)


def delete_runtime_autoscale(table: TableProtocol, materialization_id: str) -> None:
    """Forget a materialization's autoscaling: its runtime runs its own replica count."""
    del table[runtime_autoscale_key(materialization_id)]
