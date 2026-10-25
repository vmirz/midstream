"""Helpers for extracting runtime source-record metadata from Kaspr context."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
import math
import os
import uuid

from midstream.runtime_app.models import RuntimeSourceRecordMetadata

#: This process's epoch, made once at its start. The member id is the pod's name, which a restarted
#: process keeps, while its session generation (the app's rebalance count) starts again from 0: the
#: epoch tells the committer which process a generation belongs to. A random id, not a start time,
#: so that no clock is compared with another.
PROCESS_EPOCH = uuid.uuid4().hex


def current_member_id() -> str:
    return os.getenv("MATERIALIZER_RUNTIME_MEMBER_ID") or os.getenv("HOSTNAME", "unknown-member")


def current_session_epoch() -> str:
    return PROCESS_EPOCH


def current_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def current_timestamp_ms() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


#: Epoch values above this are milliseconds rather than seconds. Seconds will not reach it until
#: the year 5138; milliseconds passed it in 1973.
_EPOCH_MILLISECONDS_THRESHOLD = 100_000_000_000


def format_timestamp(value) -> str | None:
    """Render a timestamp as UTC ISO-8601 with millisecond precision, or ``None``.

    Millisecond precision matches Kafka's own timestamp resolution, and a fixed width keeps every
    rendered value sortable as text. Epoch numbers are accepted because that is how Faust hands over
    a record's timestamp: float seconds. A non-positive epoch is Kafka's "no timestamp" (``-1``,
    which Faust divides into ``-0.001``), not 1970.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, (int, float)):
        if not math.isfinite(value) or value <= 0:
            return None
        seconds = value / 1000 if value >= _EPOCH_MILLISECONDS_THRESHOLD else value
        try:
            return format_timestamp(datetime.fromtimestamp(seconds, tz=UTC))
        except (OverflowError, OSError, ValueError):
            return None
    return None


def record_timestamp(event) -> str | None:
    """The Kafka timestamp of the record behind ``event``.

    Faust keeps it on ``event.message.timestamp``; the event itself has no such attribute. Reading
    ``event.timestamp`` instead left ``_kafka_timestamp`` null in every row ever written, and with
    it every object's event-time bounds — so no time range could prune anything. ``event.timestamp``
    is still honoured as a fallback for callers that construct events themselves.
    """
    message = getattr(event, "message", None)
    timestamp = format_timestamp(getattr(message, "timestamp", None))
    if timestamp is not None:
        return timestamp
    return format_timestamp(getattr(event, "timestamp", None))


def decode_kafka_bytes(value) -> str | None:
    """A Kafka key or header value as text: UTF-8, or `b64:` + base64 when it is not."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return "b64:" + base64.b64encode(raw).decode("ascii")
    return str(value)


def record_headers(message) -> tuple[tuple[str, str | None], ...]:
    """A record's headers as `(name, value)` pairs; Faust hands over a sequence or a mapping."""
    headers = getattr(message, "headers", None)
    if not headers:
        return ()
    items = headers.items() if hasattr(headers, "items") else headers
    pairs: list[tuple[str, str | None]] = []
    for item in items:
        try:
            name, value = item
        except (TypeError, ValueError):
            continue
        pairs.append((decode_kafka_bytes(name) or "", decode_kafka_bytes(value)))
    return tuple(pairs)


def extract_source_record_metadata(
    materialization_id: str,
    event,
    *,
    source_topic: str | None = None,
    observed_at: str | None = None,
) -> RuntimeSourceRecordMetadata | None:
    message = getattr(event, "message", None)
    if message is None:
        return None

    partition = getattr(message, "partition", None)
    offset = getattr(message, "offset", None)
    if partition is None or offset is None:
        return None

    topic_name = (
        source_topic
        or getattr(message, "topic", None)
        or getattr(event, "topic", None)
        or "unknown-source-topic"
    )
    return RuntimeSourceRecordMetadata(
        materialization_id=materialization_id,
        source_topic=topic_name,
        partition=partition,
        offset=offset,
        member_id=current_member_id(),
        observed_at=observed_at or current_timestamp_ms(),
        timestamp=record_timestamp(event),
        key=decode_kafka_bytes(getattr(message, "key", None)),
        headers=record_headers(message),
    )
