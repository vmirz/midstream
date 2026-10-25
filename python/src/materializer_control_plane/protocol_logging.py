"""Optional structured protocol logging helpers."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
import time
from typing import Any


def monotonic_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def elapsed_ms(start_ms: int) -> int:
    return max(0, monotonic_ms() - start_ms)


def timestamp_latency_ms(
    started_at: str | None,
    *,
    ended_at: str | None = None,
) -> int | None:
    started = _parse_timestamp(started_at)
    if started is None:
        return None
    ended = _parse_timestamp(ended_at) if ended_at is not None else datetime.now(tz=UTC)
    if ended is None:
        return None
    return max(0, int((ended - started).total_seconds() * 1000))


def log_protocol_event(
    logger: logging.Logger,
    *,
    enabled: bool,
    event: str,
    **fields: Any,
) -> None:
    if not enabled:
        return
    payload = {
        "event": event,
        "occurred_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    payload.update({key: value for key, value in fields.items() if value is not None})
    logger.info(
        "protocol_event %s",
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
    )


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
