"""In-memory runtime processing state, checkpointed by the tables' committed offsets.

Process-local and lost on restart, on purpose: the only checkpoint is the tables themselves. A
runtime reads several topics, one table each, so a partition's state belongs to a table: it is kept
per table and source partition. A partition's session starts from its table's committed offset for
it (spec §7.8); what this member staged but no commit covered is read again by whoever owns the
partition next.

The upload backlog, though, is the member's: its limits bound every table's pending batches
together, so a member reading more topics never seals, uploads or stages more at once.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from threading import RLock

from midstream.runtime_app.config import RuntimeAppConfig
from midstream.runtime_app.models import (
    RuntimeBatchState,
    RuntimeMalformedRecord,
    RuntimeProcessingSummary,
    RuntimeSkipReason,
    RuntimeWriteResult,
)

#: Batch states that count against the backlog: everything after `open` until a commit covers it.
PENDING_STATES = frozenset({"rolled", "sealing", "sealed", "uploading", "uploaded", "failed"})

# Distinct skip categories retained per partition and reported per member. Categories embed field
# paths, so a wide drifting schema could otherwise grow these without bound.
SKIP_REASON_LIMIT = 20


@dataclass(slots=True)
class RuntimePartitionState:
    materialization_id: str
    #: The table the partition's records go to: its topic's.
    table: str
    partition: int
    owner_member_id: str
    table_location: str
    #: The table's committed offset for the partition: every record at or below it is in the table
    #: or needs no row. -1 when nothing of the partition is in the table yet.
    committed_offset: int = -1
    #: The highest offset this session has finished with, staged or not.
    handled_offset: int = -1
    #: Where the next batch continues the partition from: the end of the last batch this session
    #: staged, or the committed offset the session started at.
    chain_offset: int = -1
    latest_event_time: str | None = None
    updated_at: str | None = None
    last_error: str | None = None
    last_error_kind: str | None = None
    last_file_path: str | None = None
    last_malformed_record: RuntimeMalformedRecord | None = None
    rows_written: int = 0
    bytes_written: int = 0
    files_written: int = 0
    records_skipped: int = 0
    unmapped_values: int = 0
    active_batch: RuntimeBatchState | None = None
    pending_batches: dict[str, RuntimeBatchState] = field(default_factory=dict)
    skip_reasons: dict[str, RuntimeSkipReason] = field(default_factory=dict)
    #: The app's rebalance count when this member's current session for the partition began.
    #: Every rebalance seeks the partition back to its committed offset, which ends the session:
    #: what it staged is read again, so its batches are abandoned (`runtime_app/pipeline.py`).
    session_generation: int | None = None
    #: Set when a session ended without a new one starting: the next record reloads the committed
    #: offset before it is judged.
    checkpoint_stale: bool = False


#: Partition states by table name, then source partition.
_RUNTIME_STATES: dict[str, dict[int, RuntimePartitionState]] = {}
_STATE_LOCK = RLock()


def reset_runtime_state() -> None:
    with _STATE_LOCK:
        _RUNTIME_STATES.clear()


def install_partition_state(
    *,
    materialization_id: str,
    table: str,
    partition: int,
    owner_member_id: str,
    table_location: str,
    committed_offset: int,
    config: RuntimeAppConfig,
) -> RuntimePartitionState:
    """Install a partition's state from its table's committed offset, unless another caller already has.

    Staging files an earlier process left for the partition are deleted, not adopted: that process
    acknowledged none of their records, so Kafka delivers them again from the committed offset.
    """
    with _STATE_LOCK:
        existing = _RUNTIME_STATES.get(table, {}).get(partition)
        if existing is not None:
            return existing
    discard_staged_files(
        config.staging_root_path,
        materialization_id=materialization_id,
        table=table,
        partition=partition,
        owner_member_id=owner_member_id,
    )
    state = RuntimePartitionState(
        materialization_id=materialization_id,
        table=table,
        partition=partition,
        owner_member_id=owner_member_id,
        table_location=table_location,
        committed_offset=committed_offset,
        handled_offset=committed_offset,
        chain_offset=committed_offset,
    )
    with _STATE_LOCK:
        return _RUNTIME_STATES.setdefault(table, {}).setdefault(partition, state)


def restart_partition_session(
    *,
    table: str,
    partition: int,
    committed_offset: int,
    session_generation: int | None,
) -> RuntimePartitionState | None:
    """Start a partition's new session from its table's committed offset; its counters are kept."""
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(table, {}).get(partition)
        if state is None:
            return None
        state.committed_offset = committed_offset
        state.handled_offset = committed_offset
        state.chain_offset = committed_offset
        state.session_generation = session_generation
        state.checkpoint_stale = False
        return state


def get_partition_state(table: str, partition: int) -> RuntimePartitionState | None:
    with _STATE_LOCK:
        return _RUNTIME_STATES.get(table, {}).get(partition)


def list_partition_states(
    *,
    table: str | None = None,
    owner_member_id: str | None = None,
) -> list[RuntimePartitionState]:
    with _STATE_LOCK:
        if table is None:
            states = [state for partitions in _RUNTIME_STATES.values() for state in partitions.values()]
        else:
            states = list(_RUNTIME_STATES.get(table, {}).values())
        if owner_member_id is None:
            return list(states)
        return [state for state in states if state.owner_member_id == owner_member_id]


def pending_upload_backlog(*, owner_member_id: str) -> tuple[int, int]:
    """Batches and staged bytes this member holds that no commit covers yet, every table's together."""
    with _STATE_LOCK:
        pending_batches = 0
        pending_bytes = 0
        for partitions in _RUNTIME_STATES.values():
            for state in partitions.values():
                if state.owner_member_id != owner_member_id:
                    continue
                pending_batches += len(state.pending_batches)
                pending_bytes += sum(batch.estimated_bytes for batch in state.pending_batches.values())
        return pending_batches, pending_bytes


def within_pending_upload_limits(
    *,
    owner_member_id: str,
    next_batch_bytes: int,
    config: RuntimeAppConfig,
) -> bool:
    pending_batches, pending_bytes = pending_upload_backlog(owner_member_id=owner_member_id)
    if pending_batches >= config.max_pending_upload_batches:
        return False
    return pending_bytes + next_batch_bytes <= config.max_pending_upload_bytes


def record_runtime_write(
    *,
    table: str,
    partition: int,
    offset: int,
    event_time: str | None,
    write_result: RuntimeWriteResult,
    updated_at: str,
    active_batch: RuntimeBatchState | None = None,
    rolled_batch: RuntimeBatchState | None = None,
) -> RuntimePartitionState:
    """Account for one finished record: its offset handled, its row (if any) in `active_batch`."""
    with _STATE_LOCK:
        state = _require_partition_state(table, partition)
        state.handled_offset = max(offset, state.handled_offset)
        state.latest_event_time = _max_timestamp(state.latest_event_time, event_time)
        state.updated_at = updated_at
        state.rows_written += write_result.rows_increment
        state.records_skipped += write_result.records_skipped_increment
        if write_result.last_error is not None:
            state.last_error = write_result.last_error
            state.last_error_kind = write_result.last_error_kind
        elif state.last_error_kind == "data_validation" and write_result.rows_increment:
            state.last_error = None
            state.last_error_kind = None
        if write_result.last_malformed_record is not None:
            state.last_malformed_record = write_result.last_malformed_record
            if write_result.records_skipped_increment > 0:
                _count_skip(state, write_result.last_malformed_record, increment=write_result.records_skipped_increment, seen_at=updated_at)
        if active_batch is not None:
            state.active_batch = active_batch
        if rolled_batch is not None:
            if state.active_batch is not None and state.active_batch.batch_id == rolled_batch.batch_id:
                state.active_batch = None
            state.pending_batches[rolled_batch.batch_id] = rolled_batch
            state.chain_offset = max(state.chain_offset, rolled_batch.end_offset)
        return state


def record_rolled_batch(rolled_batch: RuntimeBatchState) -> RuntimePartitionState | None:
    """Move a partition's open batch, now rolled, into its pending batches."""
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(rolled_batch.table, {}).get(rolled_batch.partition)
        if state is None:
            return None
        if state.active_batch is not None and state.active_batch.batch_id == rolled_batch.batch_id:
            state.active_batch = None
        state.pending_batches[rolled_batch.batch_id] = rolled_batch
        state.chain_offset = max(state.chain_offset, rolled_batch.end_offset)
        return state


def update_pending_batch(batch: RuntimeBatchState, *, error: str | None = None, error_kind: str | None = None) -> RuntimeBatchState | None:
    """Replace a pending batch with its new form; None when it is no longer pending (session ended)."""
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(batch.table, {}).get(batch.partition)
        if state is None or batch.batch_id not in state.pending_batches:
            return None
        state.pending_batches[batch.batch_id] = batch
        if error is not None:
            state.last_error = error
            state.last_error_kind = error_kind or "infrastructure"
        elif state.last_error_kind == "infrastructure":
            state.last_error = None
            state.last_error_kind = None
        return batch


def get_pending_batch(table: str, partition: int, batch_id: str) -> RuntimeBatchState | None:
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(table, {}).get(partition)
        return None if state is None else state.pending_batches.get(batch_id)


def record_sealed_batch(batch: RuntimeBatchState) -> RuntimeBatchState | None:
    """Count a sealed batch's files and unmapped values, and keep it pending for its upload."""
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(batch.table, {}).get(batch.partition)
        if state is None or batch.batch_id not in state.pending_batches:
            return None
        state.pending_batches[batch.batch_id] = batch
        state.unmapped_values += batch.unmapped_values
        if state.last_error_kind == "infrastructure":
            state.last_error = None
            state.last_error_kind = None
        return batch


def record_uploaded_batch(batch: RuntimeBatchState) -> RuntimeBatchState | None:
    """The batch's files are in the bucket: count them, and wait for a commit to cover them."""
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(batch.table, {}).get(batch.partition)
        if state is None or batch.batch_id not in state.pending_batches:
            return None
        state.pending_batches[batch.batch_id] = batch
        state.files_written += len(batch.files)
        state.bytes_written += batch.sealed_bytes
        if batch.files:
            state.last_file_path = batch.files[-1].uri
        if state.last_error_kind == "infrastructure":
            state.last_error = None
            state.last_error_kind = None
        return batch


def release_covered_batches(table: str, partition: int, committed_offset: int) -> list[RuntimeBatchState]:
    """Remove and return the partition's pending batches its table's committed offset covers."""
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(table, {}).get(partition)
        if state is None:
            return []
        state.committed_offset = max(state.committed_offset, committed_offset)
        released = [batch for batch in state.pending_batches.values() if batch.end_offset <= committed_offset]
        for batch in released:
            del state.pending_batches[batch.batch_id]
        return released


def abandon_partition_batches(table: str, partition: int) -> list[RuntimeBatchState]:
    """Drop a partition's open and pending batches, returning them so their files can be removed.

    For a session that has ended: the records they staged were never acknowledged, so Kafka
    delivers them again to whoever owns the partition now.
    """
    with _STATE_LOCK:
        state = _RUNTIME_STATES.get(table, {}).get(partition)
        if state is None:
            return []
        abandoned = list(state.pending_batches.values())
        if state.active_batch is not None:
            abandoned.append(state.active_batch)
        state.pending_batches = {}
        state.active_batch = None
        return abandoned


def build_runtime_processing_summary(
    materialization_id: str,
    *,
    member_id: str | None = None,
) -> RuntimeProcessingSummary | None:
    """This member's processing across every table of the runtime, with each table's own counters."""
    with _STATE_LOCK:
        partition_states = [state for partitions in _RUNTIME_STATES.values() for state in partitions.values()]
        partition_states = [state for state in partition_states if state.materialization_id == materialization_id]
        if member_id is not None:
            partition_states = [state for state in partition_states if state.owner_member_id == member_id]
        if not partition_states:
            return None
        handled: dict[str, dict[str, int]] = {}
        committed: dict[str, dict[str, int]] = {}
        table_counters: dict[str, dict[str, int]] = {}
        totals = {"rows": 0, "bytes": 0, "files": 0, "skipped": 0, "unmapped": 0, "batches": 0, "pending_bytes": 0, "files_pending": 0}
        event_time = None
        updated_at = None
        last_error = last_error_kind = last_file_path = None
        last_malformed_record = None
        skip_reasons: dict[str, RuntimeSkipReason] = {}
        for state in partition_states:
            for reason in state.skip_reasons.values():
                skip_reasons[reason.category] = _merge_skip_reasons(skip_reasons.get(reason.category), reason)
            if state.handled_offset >= 0:
                handled.setdefault(state.table, {})[str(state.partition)] = state.handled_offset
            if state.committed_offset >= 0:
                committed.setdefault(state.table, {})[str(state.partition)] = state.committed_offset
            counters = table_counters.setdefault(
                state.table, {"rows_written": 0, "bytes_written": 0, "files_written": 0, "unmapped_values": 0}
            )
            counters["rows_written"] += state.rows_written
            counters["bytes_written"] += state.bytes_written
            counters["files_written"] += state.files_written
            counters["unmapped_values"] += state.unmapped_values
            totals["rows"] += state.rows_written
            totals["bytes"] += state.bytes_written
            totals["files"] += state.files_written
            totals["skipped"] += state.records_skipped
            totals["unmapped"] += state.unmapped_values
            totals["batches"] += len(state.pending_batches)
            totals["pending_bytes"] += sum(batch.estimated_bytes for batch in state.pending_batches.values())
            totals["files_pending"] += sum(
                len(batch.files) for batch in state.pending_batches.values() if batch.state == "uploaded"
            )
            event_time = _max_timestamp(event_time, state.latest_event_time)
            if _is_newer(state.updated_at, updated_at):
                updated_at = state.updated_at
                last_error = state.last_error
                last_error_kind = state.last_error_kind
                last_file_path = state.last_file_path
                last_malformed_record = state.last_malformed_record
            elif state.last_error is not None and last_error is None:
                last_error, last_error_kind = state.last_error, state.last_error_kind
        return RuntimeProcessingSummary(
            materialization_id=materialization_id,
            member_id=member_id,
            latest_handled_offsets=handled,
            latest_committed_offsets=committed,
            table_counters=table_counters,
            event_time=event_time,
            rows_written=totals["rows"] or None,
            bytes_written=totals["bytes"] or None,
            files_written=totals["files"] or None,
            records_skipped=totals["skipped"] or None,
            pending_upload_batches=totals["batches"] or None,
            pending_upload_bytes=totals["pending_bytes"] or None,
            files_pending_commit=totals["files_pending"],
            unmapped_values=totals["unmapped"],
            last_error=last_error,
            last_error_kind=last_error_kind,
            last_file_path=last_file_path,
            last_malformed_record=last_malformed_record,
            updated_at=updated_at,
            skip_reasons=tuple(sorted(skip_reasons.values(), key=lambda reason: reason.count, reverse=True)[:SKIP_REASON_LIMIT]),
        )


def discard_staged_files(
    staging_root_path: str,
    *,
    materialization_id: str,
    table: str,
    partition: int,
    owner_member_id: str,
) -> int:
    """Remove what an earlier process left in a partition's staging directory; return how many files.

    Staging is local to the pod and the directory to one materialization, table and partition, so all
    of it is this member's: JSONL from batches never covered by a commit, and seal output.
    """
    root = Path(staging_root_path) / materialization_id / table / f"p{partition}"
    if not root.exists():
        return 0
    removed = 0
    for path in list(root.iterdir()):
        if not path.is_file():
            continue
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
    return removed


def _count_skip(state: RuntimePartitionState, record: RuntimeMalformedRecord, *, increment: int, seen_at: str) -> None:
    category = record.category or record.reason
    previous = state.skip_reasons.get(category)
    state.skip_reasons[category] = RuntimeSkipReason(
        category=category,
        count=(0 if previous is None else previous.count) + increment,
        last_reason=record.reason,
        last_seen_at=seen_at,
        last_partition=record.partition,
        last_offset=record.offset,
        last_schema_version=record.schema_version,
    )
    if len(state.skip_reasons) > SKIP_REASON_LIMIT:
        stalest = min(state.skip_reasons.values(), key=lambda reason: reason.last_seen_at or "")
        del state.skip_reasons[stalest.category]


def _merge_skip_reasons(current: RuntimeSkipReason | None, incoming: RuntimeSkipReason) -> RuntimeSkipReason:
    if current is None:
        return incoming
    latest = incoming if _is_newer(incoming.last_seen_at, current.last_seen_at) else current
    return replace(latest, count=current.count + incoming.count)


def _require_partition_state(table: str, partition: int) -> RuntimePartitionState:
    state = _RUNTIME_STATES.get(table, {}).get(partition)
    if state is None:
        raise RuntimeError(f"runtime partition state is not initialized for table {table} partition {partition}")
    return state


def _max_timestamp(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return right if right > left else left


def _is_newer(candidate: str | None, current: str | None) -> bool:
    if candidate is None:
        return False
    if current is None:
        return True
    return candidate > current
