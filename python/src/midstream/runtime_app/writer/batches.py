"""Batches: staging raw records as JSONL, rolling them, and re-sealing what a commit left out.

The writer stages each record as it arrived - its value untouched, beside its Kafka metadata - one
JSON line per record (spec §7.2). Nothing is normalized per record: conforming the values to the
table's schema is the seal's work, in a child process, against the schema current at seal time.

A staged line is `{"m": {"p", "o", "t", "k", "h", "a"}, "v": <value>}`: partition, offset, Kafka
timestamp, key, headers and the time the record was read; then the value.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from midstream.runtime_app.config import RuntimeAppConfig
from midstream.runtime_app.models import RuntimeBatchState, RuntimeSourceRecordMetadata

#: Distinct drift observations a batch remembers for its seal's schema wait.
BATCH_OBSERVATION_LIMIT = 200


def staged_line(source_record: RuntimeSourceRecordMetadata, value: Any) -> bytes:
    metadata = {
        "p": source_record.partition,
        "o": source_record.offset,
        "t": source_record.timestamp,
        "k": source_record.key,
        "h": [list(pair) for pair in source_record.headers] if source_record.headers else None,
        "a": source_record.observed_at,
    }
    return json.dumps({"m": metadata, "v": value}, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")


def batch_directory(config: RuntimeAppConfig, materialization_id: str, table: str, partition: int) -> Path:
    return Path(config.staging_root_path) / materialization_id / table / f"p{partition}"


def create_open_batch(
    *,
    config: RuntimeAppConfig,
    source_record: RuntimeSourceRecordMetadata,
    table: str,
    after_offset: int,
    session_generation: int | None,
) -> RuntimeBatchState:
    """A new open batch of `table` starting at `source_record`, continuing its partition after `after_offset`.

    Its id is deterministic, and any file left at its path by an earlier process is removed, so a
    batch never appends to a stale file. The table is part of the id: partition 0 of two topics are
    two batches.
    """
    batch_id = f"batch-{source_record.member_id}-{table}-p{source_record.partition}-o{source_record.offset}"
    directory = batch_directory(config, source_record.materialization_id, table, source_record.partition)
    directory.mkdir(parents=True, exist_ok=True)
    local_path = directory / f"{batch_id}.jsonl"
    local_path.unlink(missing_ok=True)
    return RuntimeBatchState(
        batch_id=batch_id,
        materialization_id=source_record.materialization_id,
        table=table,
        owner_member_id=source_record.member_id,
        partition=source_record.partition,
        local_path=str(local_path),
        state="open",
        start_offset=source_record.offset,
        end_offset=source_record.offset,
        after_offset=min(after_offset, source_record.offset - 1),
        session_generation=session_generation,
        created_at=source_record.observed_at,
        updated_at=source_record.observed_at,
    )


def append_row_to_batch(
    batch: RuntimeBatchState,
    *,
    line: bytes,
    source_record: RuntimeSourceRecordMetadata,
    observations: tuple[str, ...] = (),
) -> RuntimeBatchState:
    if batch.state != "open":
        raise ValueError("can only append rows to an open batch")
    local_path = Path(batch.local_path)
    try:
        handle = local_path.open("ab")
    except FileNotFoundError:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        handle = local_path.open("ab")
    with handle:
        handle.write(line)
        handle.write(b"\n")
    event_time_min, event_time_max = _update_event_time_bounds(
        batch.event_time_min,
        batch.event_time_max,
        source_record.timestamp,
    )
    remembered = batch.observations
    if observations and len(remembered) < BATCH_OBSERVATION_LIMIT:
        known = set(remembered)
        remembered = remembered + tuple(item for item in observations if item not in known)
        remembered = remembered[:BATCH_OBSERVATION_LIMIT]
    return replace(
        batch,
        end_offset=source_record.offset,
        row_count=batch.row_count + 1,
        estimated_bytes=batch.estimated_bytes + len(line) + 1,
        event_time_min=event_time_min,
        event_time_max=event_time_max,
        observations=remembered,
        updated_at=source_record.observed_at,
    )


def should_roll_batch(batch: RuntimeBatchState, *, config: RuntimeAppConfig, observed_at: str) -> bool:
    """Whether an open batch should be closed and sealed.

    A schema change deliberately does not roll the batch: the seal conforms every row to the schema
    current when it runs.
    """
    if batch.row_count >= config.max_rows_per_file:
        return True
    if batch.estimated_bytes >= config.target_file_size_bytes:
        return True
    if batch.created_at is None:
        return False
    age_seconds = int((parse_timestamp(observed_at) - parse_timestamp(batch.created_at)).total_seconds())
    if age_seconds >= config.max_open_file_age_ceiling_seconds:
        return True
    if age_seconds < config.max_open_file_age_seconds:
        return False
    # Past the soft age a batch only seals once it holds enough to be worth a file. A quiet topic
    # would otherwise emit one near-empty file per partition per interval, forever.
    return batch.row_count >= config.min_rows_per_file or batch.estimated_bytes >= config.min_file_size_bytes


def roll_batch(batch: RuntimeBatchState, *, observed_at: str) -> RuntimeBatchState:
    """Close an open batch to further rows, ready to be sealed."""
    if batch.state != "open":
        raise ValueError("can only roll an open batch")
    if batch.row_count < 1:
        raise ValueError("cannot roll an empty batch")
    return replace(batch, state="rolled", rolled_at=observed_at, updated_at=observed_at)


def reseal_tail(batch: RuntimeBatchState, *, covered_through: int, observed_at: str) -> RuntimeBatchState | None:
    """The batch without its rows at or below `covered_through`, rolled again for a new seal.

    The committer refused the batch's seal because the table already holds its first part (spec
    §7.8). The rest is staged again from the batch's own JSONL - still here, as nothing is deleted
    before a commit covers it - continuing the partition after `covered_through`. None when nothing
    is left.
    """
    source = Path(batch.local_path)
    target = source.with_name(f"{source.stem}-r{batch.reseals + 1}.jsonl")
    kept = 0
    first: int | None = None
    last: int | None = None
    size = 0
    time_min: str | None = None
    time_max: str | None = None
    with source.open("rb") as reader, target.open("wb") as writer:
        for raw in reader:
            line = raw.rstrip(b"\n")
            if not line:
                continue
            metadata = json.loads(line).get("m") or {}
            offset = int(metadata.get("o", -1))
            if offset <= covered_through:
                continue
            writer.write(line)
            writer.write(b"\n")
            kept += 1
            size += len(line) + 1
            first = offset if first is None else first
            last = offset
            time_min, time_max = _update_event_time_bounds(time_min, time_max, metadata.get("t"))
    source.unlink(missing_ok=True)
    if kept == 0 or first is None or last is None:
        target.unlink(missing_ok=True)
        return None
    return replace(
        batch,
        local_path=str(target),
        state="rolled",
        start_offset=first,
        end_offset=last,
        after_offset=covered_through,
        row_count=kept,
        estimated_bytes=size,
        event_time_min=time_min,
        event_time_max=time_max,
        seal_id=None,
        files=(),
        schema_id=None,
        announced_at=None,
        announce_attempts=0,
        upload_attempts=0,
        seal_attempts=0,
        last_error=None,
        reseals=batch.reseals + 1,
        rolled_at=observed_at,
        updated_at=observed_at,
    )


def parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(normalized).astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    # Millisecond precision, as Kafka records it.
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _update_event_time_bounds(
    current_min: str | None,
    current_max: str | None,
    next_value: str | None,
) -> tuple[str | None, str | None]:
    if next_value is None:
        return current_min, current_max
    next_timestamp = parse_timestamp(next_value)
    min_timestamp = next_timestamp if current_min is None else min(parse_timestamp(current_min), next_timestamp)
    max_timestamp = next_timestamp if current_max is None else max(parse_timestamp(current_max), next_timestamp)
    return format_timestamp(min_timestamp), format_timestamp(max_timestamp)
