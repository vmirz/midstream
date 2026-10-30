"""A Kafka record into a table row, exactly as the runtime's writer stages it and its seal builds it.

The runtime's writer gets a record's value from Kaspr's `json` serializer - Faust's `json` codec,
`json.loads` of the value as UTF-8 text - and stages it as one JSON line with the record's
partition, offset, Kafka timestamp, key and headers and the time it was read
(`runtime_app/writer/batches.py::staged_line`). Its seal reads that line back and builds the row
(`lakehouse/rows.py::build_row`). The tail does the same three steps with the same functions, so
its rows are the seal's: the one difference is the read time, `_materialized_at`, which is the
tail's own.

A value Kaspr cannot decode reaches no writer - Faust acknowledges it and moves on - so it gets no
row here either, and is counted `undecodable`. A tombstone (a null value) is a row with a null
payload, or no row for a materialization that skips tombstones, as the writer does.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
from typing import Any

from midstream.lakehouse.lifecycle import schema_from_json
from midstream.lakehouse.parquet import arrow_schema
from midstream.lakehouse.rows import BuiltRow, build_row
from midstream.lakehouse.schema.conform import Conformer
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.index import build_index
from midstream.protection.plan import ProtectionPlan
from midstream.protection.policy import TableProtection
from midstream.protection.secrets import get_protection_key
from midstream.runtime_app.models import RuntimeSourceRecordMetadata
from midstream.runtime_app.schema.drift import DriftDetector
from midstream.runtime_app.writer.batches import staged_line
from midstream.runtime_app.writer.context import (
    current_timestamp_ms,
    decode_kafka_bytes,
    format_timestamp,
    record_headers,
)

#: A decoded value that is no value: the record cannot be decoded.
UNDECODABLE = object()

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def epoch_ms(moment: datetime | None) -> int | None:
    """A row's Kafka timestamp as epoch milliseconds, exactly, as the console's time bounds are.

    Not through a float: every row's timestamp is on a whole millisecond, and about one in a hundred
    comes out one below it that way - a time range starting at the newest row would then prune the
    answer that holds it."""
    if moment is None:
        return None
    aware = moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)
    return (aware - _EPOCH) // timedelta(milliseconds=1)


class TailSchema:
    """One schema of a table, ready to build rows with: what a seal conforms with, its drift rule,
    and what the writers mask records with (handbook 22)."""

    def __init__(
        self,
        *,
        schema_id: int,
        iceberg_schema: dict[str, Any],
        fork_map: dict[str, Any],
        last_column_id: int | None = None,
        rejected_paths: list[list[str]] | tuple[tuple[str, ...], ...] = (),
        protection: dict[str, Any] | None = None,
    ) -> None:
        self.schema_id = int(schema_id)
        self.iceberg_schema = dict(iceberg_schema)
        self.schema = schema_from_json(iceberg_schema)
        self.fork_map = ForkMap.from_dict(fork_map)
        self.index = build_index(self.schema, self.fork_map, last_column_id=last_column_id)
        self.conformer = Conformer(self.schema, self.fork_map, index=self.index)
        self.arrow_schema = arrow_schema(self.schema)
        self.drift = DriftDetector(self.index, rejected_paths=tuple(tuple(path) for path in rejected_paths))
        self.rejected_paths = tuple(tuple(path) for path in rejected_paths)
        # The table's recorded protection: the writers mask each record with the plan it makes, before
        # staging, so the tail does the same, before its own staging.
        self.protection = TableProtection.from_dict(protection or {})
        key = get_protection_key()
        self.plan = ProtectionPlan(self.protection, self.index, secret=None if key is None else key.secret)

    @property
    def protection_revision(self) -> int:
        return self.protection.revision

    @property
    def position(self) -> tuple[int, int]:
        """Which of two schemas is newer: by schema, then by protection revision."""
        return (self.schema_id, self.protection.revision)


@dataclass(frozen=True, slots=True)
class TailRow:
    """One record's row, and whether a schema change in flight will move some of its values."""

    built: BuiltRow
    #: Values went to `_unmapped` that the writer's drift detection asks a schema change for: once the
    #: change is committed, the seal puts them in columns.
    settling: bool


def decode_value(raw: bytes | None) -> Any:
    """A record's value as Kaspr's `json` serializer decodes it for the writer, or `UNDECODABLE`."""
    if raw is None:
        return None
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray, memoryview)) else str(raw)
        return json.loads(text)
    except (UnicodeDecodeError, ValueError):
        return UNDECODABLE


def protect(value: Any, schema: TailSchema) -> Any:
    """`value` masked as the table's writers mask it before staging (handbook 22): the tail's buffer
    then holds only what the table will hold."""
    return value if schema.plan.empty else schema.plan.protect(value)


def stage(record: Any, value: Any, *, materialization_id: str, source_topic: str, observed_at: str | None = None) -> bytes:
    """The staged line of `record`, with its decoded `value`, as the runtime's writer stages it."""
    # Faust hands the writer Kafka's milliseconds as float seconds (`_to_message`), and
    # `format_timestamp` reads a small number as seconds: converting the same way keeps a record
    # from before 1973 the same instant on both sides.
    timestamp = getattr(record, "timestamp", None)
    seconds = None if timestamp is None else timestamp / 1000.0
    metadata = RuntimeSourceRecordMetadata(
        materialization_id=materialization_id,
        source_topic=source_topic,
        partition=int(record.partition),
        offset=int(record.offset),
        member_id="tail",
        observed_at=observed_at or current_timestamp_ms(),
        timestamp=format_timestamp(seconds),
        key=decode_kafka_bytes(getattr(record, "key", None)),
        headers=record_headers(record),
    )
    return staged_line(metadata, value)


def build(line: bytes, schema: TailSchema, *, default_partition: int) -> TailRow:
    """The row of one staged line under `schema`, as the seal builds it."""
    staged = json.loads(line)
    built = build_row(staged, conformer=schema.conformer, default_partition=default_partition)
    settling = built.unmapped_values > 0 and bool(schema.drift.observations(staged.get("v")))
    return TailRow(built=built, settling=settling)
