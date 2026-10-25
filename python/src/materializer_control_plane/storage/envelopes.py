"""Envelope helpers for persisted table records."""

from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any, TypeVar

from materializer_control_plane.models import RecordEnvelope

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

# What a model's `from_dict` raises for a payload of another shape.
_UNREADABLE_ERRORS = (KeyError, TypeError, ValueError)

# The records already reported as unreadable, so a fleet list polled every few seconds logs each once.
_REPORTED_UNREADABLE: set[tuple[str, str]] = set()


def encode_payload_envelope(
    *,
    entity_type: str,
    entity_id: str,
    updated_at: str,
    payload: dict[str, Any],
    version: int = 1,
) -> dict[str, Any]:
    envelope = RecordEnvelope(
        entity_type=entity_type,
        entity_id=entity_id,
        version=version,
        updated_at=updated_at,
        payload=payload,
    )
    return envelope.to_dict()


def decode_envelope(raw_value: dict[str, Any] | None, *, expected_entity_type: str | None = None) -> RecordEnvelope | None:
    if raw_value is None:
        return None
    envelope = RecordEnvelope.from_dict(raw_value)
    if expected_entity_type is not None and envelope.entity_type != expected_entity_type:
        raise ValueError(
            f"expected entity_type {expected_entity_type}, got {envelope.entity_type}"
        )
    return envelope


def read_payload(envelope: RecordEnvelope, decode: Callable[[dict[str, Any]], T]) -> T | None:
    """`decode(envelope.payload)`, or None for a record this version cannot read.

    A record an earlier, incompatible release wrote (2.0 reads no 1.0 materialization or table) can
    outlive the deletion that should have removed it: a copy stranded in another shard of the table.
    Nothing can be shown or done with it, and one such record must not fail the whole fleet list,
    the sweep or the catalog, so it is skipped, logged once, and left to the retention sweep.
    """
    try:
        return decode(envelope.payload)
    except _UNREADABLE_ERRORS as error:
        reported = (envelope.entity_type, envelope.entity_id)
        if reported not in _REPORTED_UNREADABLE:
            _REPORTED_UNREADABLE.add(reported)
            LOGGER.warning(
                "Skipping %s record %s, which this version cannot read: %s: %s",
                envelope.entity_type,
                envelope.entity_id,
                type(error).__name__,
                error,
            )
        return None
