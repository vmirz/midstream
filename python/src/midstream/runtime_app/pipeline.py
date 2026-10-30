"""The runtime's batch pipeline: sealing, uploading and announcing off the writer's path.

The writer only stages rows and rolls batches. Everything slow happens here, without holding the
event loop that also runs the Kafka consumer, the other agents and the status heartbeat. A runtime
reads several topics, each into a table of its own; the pipeline is the member's, shared by every
table: `seal_worker_count` seals and `upload_worker_count` uploads at a time, taken oldest roll
first whichever table a batch belongs to, so a member reading more topics never does more at once.

- a rolled batch is sealed - its staged rows conformed to the table's schema and written as one
  Parquet file per partition day - in a child process (`writer/seal_worker.py`);
- a sealed batch's files are uploaded to the table's `data/` location on a thread of a small pool,
  then announced to the committer through the outbox (`runtime_app/outbox.py`);
- once the table's committed offsets cover a batch, its held acknowledgements are released
  (`runtime_app/acks.py`), and only then can its offsets commit to Kafka (spec §7.7).

The pipeline task (every `upload_interval`) and the writer (on every roll) call `schedule`, which
starts whatever is due and returns at once. The task's pass also drains the outbox: `advance_pipeline`
returns a generator over it for the task's `topicSend`.

**Exactly once (spec §7.8).** A partition's session starts from the table's committed offset for
it. Every batch says where it continues the partition from (`after_offset`), so the committer can
refuse what is already in the table and never skip what is not. A seal it refuses as overlapping is
sealed again here from the batch's own JSONL, keeping only the rows past what the table holds.
Files stay announced - again every `announce_retry_seconds` - until a commit covers them, which is
what lets a new committer rebuild its pending set from nothing.

**Sessions.** Every rebalance ends with Faust seeking each assigned partition back to its committed
offset, so a member reads again whatever no commit covered. Its batches of the ended session are
abandoned, and the next record starts a new session from the table's offsets.

**Backpressure.** While the member's pending batches - every table's together - are at
`max_pending_upload_batches` or `max_pending_upload_bytes`, its partitions of every source topic are
paused (`runtime_app/flow.py`); they resume once the backlog is down to `BACKLOG_RESUME_FRACTION` of
both limits.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from functools import partial
import json
import logging
from pathlib import Path
import sys
from typing import Any
import uuid

from midstream.lakehouse.commits import AnnouncedFile
from midstream.lakehouse.offsets import CommittedOffsets
from midstream.lakehouse.schema.policy import evolve, limits_of
from midstream.protocol_logging import elapsed_ms, log_protocol_event, monotonic_ms
from midstream.runtime_app import acks, flow
from midstream.runtime_app.config import RuntimeAppConfig, get_runtime_app_config
from midstream.runtime_app.models import RuntimeBatchState, SealedFile
from midstream.runtime_app.outbox import outbox
from midstream.runtime_app.performance import record_seal_processing, record_upload_processing
from midstream.runtime_app.state import (
    RuntimePartitionState,
    abandon_partition_batches,
    get_partition_state,
    get_pending_batch,
    install_partition_state,
    list_partition_states,
    pending_upload_backlog,
    record_rolled_batch,
    record_sealed_batch,
    record_uploaded_batch,
    release_covered_batches,
    restart_partition_session,
    update_pending_batch,
    within_pending_upload_limits,
)
from midstream.runtime_app.table_state import (
    TableContext,
    current_view,
    load_table_offsets,
    read_committed_offsets,
    read_file_decisions,
)
from midstream.runtime_app.writer import current_member_id, current_session_epoch, current_timestamp_ms
from midstream.runtime_app.writer.batches import parse_timestamp, reseal_tail, roll_batch, should_roll_batch
from midstream.runtime_app.writer.seal import SealJob, SealOutput, run_seal
from midstream.runtime_app.writer.upload import retry_backoff_seconds, upload_batch_files

LOGGER = logging.getLogger(__name__)

SEAL_WORKER_MODULE = "midstream.runtime_app.writer.seal_worker"
#: A seal still running after this long is killed and retried.
SEAL_TIMEOUT_SECONDS = 1800
#: How long a new session waits for its partition's in-flight seal and upload before reading the
#: table's offsets, so a batch announced a moment ago is not staged again for nothing.
SESSION_START_WAIT_SECONDS = 120
#: A paused member resumes once its backlog is down to this fraction of both limits.
BACKLOG_RESUME_FRACTION = 0.75


class SealError(RuntimeError):
    """A seal process failed; the batch is sealed again after a backoff."""


async def seal_in_subprocess(job_path: Path) -> SealOutput:
    """Run one seal in a child process; return what it wrote."""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        SEAL_WORKER_MODULE,
        str(job_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=SEAL_TIMEOUT_SECONDS)
    except BaseException:
        if process.returncode is None:
            try:
                process.kill()
                await process.wait()
            except (ProcessLookupError, asyncio.CancelledError):
                pass
        raise
    if process.returncode != 0:
        lines = stderr.decode("utf-8", errors="replace").strip().splitlines()
        detail = lines[-1] if lines else f"seal process exited with status {process.returncode}"
        raise SealError(detail)
    return SealOutput.from_dict(json.loads(stdout.decode("utf-8").strip().splitlines()[-1]))


async def seal_in_process(job_path: Path) -> SealOutput:
    """Seal in this process, blocking its event loop throughout. For tests and tools only."""
    return run_seal(SealJob.from_dict(json.loads(Path(job_path).read_text(encoding="utf-8"))))


class BatchPipeline:
    """Seals, uploads and announces this member's rolled batches. One per process: `batch_pipeline`."""

    def __init__(self) -> None:
        self.sealer = seal_in_subprocess
        #: Storage client for uploads and table reads; None builds the process's shared one.
        self.storage_client: Any = None
        self._app: Any = None
        self._state_table: Any = None
        #: The runtime's tables, by table name: all of them once the writer has handled a record.
        self._contexts: dict[str, TableContext] = {}
        #: The tables last registered whole (`register_tables`), so that doing it again costs nothing.
        self._registered: tuple[TableContext, ...] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        #: Seals under way, by batch id, with their (table, partition).
        self._seals: dict[str, tuple[tuple[str, int], asyncio.Task]] = {}
        #: Uploads under way, by (table, partition): one at a time per partition.
        self._uploads: dict[tuple[str, int], asyncio.Task] = {}
        self._executor: ThreadPoolExecutor | None = None
        self._executor_workers = 0
        #: Whether the member's source partitions are paused for its backlog.
        self._backlog_paused = False
        self._uploads_enabled = True
        #: Each table's offsets read for a rebalance generation, shared by the sessions it starts.
        self._session_offsets: dict[str, tuple[int | None, CommittedOffsets]] = {}
        #: Whether an observation (by key) still needs the schema, per schema id.
        self._resolved: dict[tuple[str, int, str], bool] = {}
        #: When each batch waiting for the schema was last logged as waiting (monotonic ms).
        self._wait_logged: dict[str, int] = {}

    def reset(self) -> None:
        """Forget every task, setting and pause. Tests use this between cases."""
        self.__init__()  # type: ignore[misc]

    # -- context ---------------------------------------------------------------------------------

    def register(self, context: TableContext, *, app=None, state_table=None) -> None:
        self._contexts[context.name] = context
        self._remember(app, state_table)

    def register_tables(self, tables: tuple[TableContext, ...], *, app=None, state_table=None) -> None:
        """Know every table of the runtime, not only those written to so far: backpressure pauses
        every source topic, and a topic whose records have not arrived yet must pause too."""
        if self._registered is not tables:
            for context in tables:
                self._contexts.setdefault(context.name, context)
            self._registered = tables
        self._remember(app, state_table)

    def _remember(self, app, state_table) -> None:
        if app is not None:
            self._app = app
        if state_table is not None:
            self._state_table = state_table

    def _source_topics(self) -> list[str]:
        """Every source topic of the runtime's tables."""
        return sorted({context.source_topic for context in self._contexts.values() if context.source_topic})

    # -- sessions --------------------------------------------------------------------------------

    async def ensure_partition_session(
        self,
        *,
        app,
        context: TableContext,
        partition: int,
        owner_member_id: str,
        config: RuntimeAppConfig,
        state_table=None,
    ) -> RuntimePartitionState:
        """Return the state of a partition of the context's table, starting a new session if a
        rebalance ended its last.

        The writer calls this before each record. The table's offsets are read off the event loop.
        """
        self.register(context, app=app, state_table=state_table)
        state = get_partition_state(context.name, partition)
        if state is None:
            offsets = await self._offsets_for_session(app, context)
            state = install_partition_state(
                materialization_id=context.materialization_id,
                table=context.name,
                partition=partition,
                owner_member_id=owner_member_id,
                table_location=context.location,
                committed_offset=offsets.get(partition),
                config=config,
            )
        else:
            state.owner_member_id = owner_member_id
        generation = _generation(app)
        if generation is None or _rebalancing(app):
            return state
        if state.session_generation == generation:
            return state
        if state.session_generation is None and not state.checkpoint_stale:
            state.session_generation = generation
            return state
        await self._start_new_session(app, state, context=context, generation=generation, config=config)
        return state

    async def _start_new_session(self, app, state: RuntimePartitionState, *, context: TableContext, generation: int, config: RuntimeAppConfig) -> None:
        abandoned = self._end_session(state, reason="rebalance")
        await self._wait_for_partition_work(state.table, state.partition)
        offsets = await self._offsets_for_session(app, context, fresh=True)
        restart_partition_session(
            table=state.table,
            partition=state.partition,
            committed_offset=offsets.get(state.partition),
            session_generation=generation,
        )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="runtime_partition_session_started",
            materialization_id=state.materialization_id,
            table=state.table,
            partition=state.partition,
            generation=generation,
            committed_offset=state.committed_offset,
            abandoned_batches=abandoned["batches"],
            abandoned_rows=abandoned["rows"],
        )

    async def _offsets_for_session(self, app, context: TableContext, *, fresh: bool = False) -> CommittedOffsets:
        """The table's committed offsets: from GCS once per rebalance generation, never older than the state table's."""
        generation = _generation(app)
        cached = self._session_offsets.get(context.name)
        if fresh or cached is None or cached[0] != generation or generation is None:
            offsets = await load_table_offsets(context, app=app, client=self.storage_client)
            self._session_offsets[context.name] = (generation, offsets)
        else:
            offsets = cached[1]
        local = read_committed_offsets(self._state_table, context.name)
        merged = dict(offsets.offsets)
        for partition, offset in local.offsets.items():
            merged[partition] = max(offset, merged.get(partition, -1))
        return CommittedOffsets(snapshot_id=offsets.snapshot_id, offsets=merged, event_times=offsets.event_times)

    def _end_session(self, state: RuntimePartitionState, *, reason: str) -> dict[str, int]:
        """Abandon a partition's batches and held acknowledgements, unacknowledged."""
        batches = abandon_partition_batches(state.table, state.partition)
        forgotten = acks.forget_partition(state.table, state.partition)
        batch_ids = {batch.batch_id for batch in batches}
        outbox.discard(lambda key, message: message.get("batch_id") in batch_ids)
        for batch in batches:
            _remove_batch_files(batch)
        state.session_generation = None
        state.checkpoint_stale = True
        rows = sum(batch.row_count for batch in batches)
        if batches or forgotten:
            LOGGER.info(
                "Ended the session of materialization %s table %s partition %s (%s): abandoned %s "
                "batch(es) holding %s row(s) and %s unacknowledged record(s); they are read again "
                "from the table's committed offset.",
                state.materialization_id,
                state.table,
                state.partition,
                reason,
                len(batches),
                rows,
                forgotten,
            )
        return {"batches": len(batches), "rows": rows}

    async def _wait_for_partition_work(self, table: str, partition: int) -> None:
        key = (table, partition)
        tasks = [task for owner, task in self._seals.values() if owner == key]
        upload = self._uploads.get(key)
        if upload is not None:
            tasks.append(upload)
        if tasks:
            await asyncio.wait(tasks, timeout=SESSION_START_WAIT_SECONDS)

    # -- scheduling ------------------------------------------------------------------------------

    def schedule(self, app=None, state_table=None) -> None:
        """Start the seals and uploads that are due, release what is committed, apply backpressure."""
        self._remember(app, state_table)
        self._schedule(uploads=self._uploads_enabled, roll_due=True)

    def advance(self, app=None, state_table=None) -> Iterator[dict[str, Any]]:
        """The pipeline task's pass: schedule, then hand back the outbox for the task to send."""
        self.schedule(app, state_table)
        return outbox.drain()

    async def drain(self, app=None, state_table=None, *, upload: bool = True, roll_due: bool = True) -> None:
        """Run due work until none is left: seal every rolled batch and, with `upload`, upload it.

        For tests and tools - a runtime never waits on its pipeline.
        """
        self._remember(app, state_table)
        self._uploads_enabled = upload
        try:
            while True:
                self._schedule(uploads=upload, roll_due=roll_due)
                tasks = [task for _, task in self._seals.values()]
                if upload:
                    tasks.extend(self._uploads.values())
                if not tasks:
                    return
                await asyncio.wait(tasks)
        finally:
            self._uploads_enabled = True

    def _schedule(self, *, uploads: bool = True, roll_due: bool = True) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._loop is not loop:
            # A new event loop (tests start one per case): tasks of the old one are gone with it.
            self._loop = loop
            self._seals.clear()
            self._uploads.clear()
        app = self._app
        config = get_runtime_app_config()
        member_id = current_member_id()
        rebalancing = _rebalancing(app)
        generation = _generation(app)
        states = list_partition_states(owner_member_id=member_id)
        now = current_timestamp_ms()
        live: list[RuntimePartitionState] = []
        for state in states:
            if generation is not None and not rebalancing and state.session_generation is not None and state.session_generation != generation:
                self._end_session(state, reason="rebalance")
                continue
            live.append(state)
        if not rebalancing:
            self._release_committed(live, config=config)
            self._apply_decisions(live, config=config, now=now)
            if roll_due:
                for state in live:
                    self._roll_if_due(state, config=config, observed_at=now)
            self._start_seals(loop, live, config=config, now=now)
            if uploads:
                self._start_uploads(loop, live, config=config, now=now)
            self._reannounce(live, config=config, now=now)
        self._apply_backpressure(app, states, config=config, member_id=member_id)

    def _roll_if_due(self, state: RuntimePartitionState, *, config: RuntimeAppConfig, observed_at: str) -> None:
        # The idle flush: a partition that stopped receiving records still has its open batch
        # rolled once it is old enough, under the same policy the writer applies.
        batch = state.active_batch
        if batch is None or batch.state != "open" or batch.row_count < 1:
            return
        if not should_roll_batch(batch, config=config, observed_at=observed_at):
            return
        if not within_pending_upload_limits(
            owner_member_id=state.owner_member_id,
            next_batch_bytes=batch.estimated_bytes,
            config=config,
        ):
            return
        record_rolled_batch(roll_batch(batch, observed_at=observed_at))

    # -- release and decisions -------------------------------------------------------------------

    def _release_committed(self, states: list[RuntimePartitionState], *, config: RuntimeAppConfig) -> None:
        committed_by_table: dict[str, CommittedOffsets] = {}
        for state in states:
            committed = committed_by_table.get(state.table)
            if committed is None:
                committed = committed_by_table[state.table] = read_committed_offsets(self._state_table, state.table)
            offset = committed.get(state.partition)
            if offset < 0:
                continue
            for batch in release_covered_batches(state.table, state.partition, offset):
                acks.batch_durable(
                    self._app,
                    table=state.table,
                    partition=state.partition,
                    batch_id=batch.batch_id,
                )
                outbox.discard(lambda key, message, batch_id=batch.batch_id: message.get("batch_id") == batch_id)
                _remove_batch_files(batch)
                log_protocol_event(
                    LOGGER,
                    enabled=config.protocol_logging_enabled,
                    event="runtime_batch_committed",
                    materialization_id=state.materialization_id,
                    table=state.table,
                    partition=state.partition,
                    batch_id=batch.batch_id,
                    offset_start=batch.start_offset,
                    offset_end=batch.end_offset,
                    row_count=batch.row_count,
                    committed_offset=offset,
                    files=len(batch.files),
                )

    def _apply_decisions(self, states: list[RuntimePartitionState], *, config: RuntimeAppConfig, now: str) -> None:
        decisions_by_table: dict[str, dict] = {}
        for state in states:
            decisions = decisions_by_table.get(state.table)
            if decisions is None:
                decisions = decisions_by_table[state.table] = read_file_decisions(self._state_table, state.table)
            if not decisions:
                continue
            for batch in list(state.pending_batches.values()):
                if batch.state != "uploaded" or batch.seal_id is None:
                    continue
                relevant = [
                    decision
                    for decision in (decisions.get(item.uri) for item in batch.files)
                    if decision is not None and decision.seal_id == batch.seal_id
                ]
                kinds = {decision.decision for decision in relevant}
                if "overlap" in kinds:
                    covered_through = max(
                        int(decision.covered_through)
                        for decision in relevant
                        if decision.decision == "overlap" and decision.covered_through is not None
                    )
                    self._reseal(batch, covered_through=covered_through, reason="overlap", config=config, now=now)
                elif "error" in kinds:
                    self._reseal(batch, covered_through=batch.after_offset, reason="error", config=config, now=now)

    def _reseal(self, batch: RuntimeBatchState, *, covered_through: int, reason: str, config: RuntimeAppConfig, now: str) -> None:
        outbox.discard(lambda key, message: message.get("seal_id") == batch.seal_id)
        _remove_sealed_files(batch)
        resealed = reseal_tail(batch, covered_through=covered_through, observed_at=now)
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="runtime_file_decision",
            materialization_id=batch.materialization_id,
            table=batch.table,
            partition=batch.partition,
            batch_id=batch.batch_id,
            seal_id=batch.seal_id,
            decision=reason,
            covered_through=covered_through,
            remaining_rows=0 if resealed is None else resealed.row_count,
        )
        if resealed is None:
            # Everything is in the table already; the committed offsets release the batch.
            update_pending_batch(replace(batch, files=(), seal_id=None))
            return
        update_pending_batch(resealed)

    # -- seals -----------------------------------------------------------------------------------

    def _start_seals(self, loop, states: list[RuntimePartitionState], *, config: RuntimeAppConfig, now: str) -> None:
        free = config.seal_worker_count - len(self._seals)
        if free <= 0:
            return
        candidates = [
            (state, batch)
            for state in states
            for batch in state.pending_batches.values()
            if batch.state == "rolled"
            and batch.batch_id not in self._seals
            and _backoff_elapsed(batch.updated_at, batch.seal_attempts, now=now, config=config)
            and not self._waiting_for_schema(batch, config=config, now=now)
        ]
        # Oldest roll first, whichever table: every table's batches wait their turn together.
        candidates.sort(key=lambda item: (item[1].rolled_at or "", item[1].table, item[1].start_offset))
        for state, batch in candidates[:free]:
            key = (state.table, state.partition)
            self._seals[batch.batch_id] = (key, loop.create_task(self._seal(key, batch.batch_id)))

    def _waiting_for_schema(self, batch: RuntimeBatchState, *, config: RuntimeAppConfig, now: str) -> bool:
        """Whether the batch holds values the schema cannot take yet, and may still wait for it.

        The writer asked for them when it staged them; evolution normally lands in seconds. A batch
        waits up to `seal_schema_wait_seconds` from its roll, then seals anyway: whatever still has
        no field goes to `_unmapped` (spec §7.3).
        """
        if not batch.observations or config.seal_schema_wait_seconds <= 0 or batch.rolled_at is None:
            return False
        waited = (parse_timestamp(now) - parse_timestamp(batch.rolled_at)).total_seconds()
        if waited >= config.seal_schema_wait_seconds:
            return False
        view = current_view(self._state_table, batch.table)
        if view is None:
            return False
        limits = limits_of(config)
        for key in batch.observations:
            cache_key = (batch.table, view.schema_id, key)
            resolved = self._resolved.get(cache_key)
            if resolved is None:
                path, observed = json.loads(key)
                resolved = not evolve(
                    view.schema,
                    view.fork_map,
                    [(tuple(path), observed)],
                    limits=limits,
                    last_column_id=view.record.last_column_id,
                ).changed
                if len(self._resolved) > 50_000:
                    self._resolved.clear()
                self._resolved[cache_key] = resolved
            if not resolved:
                self._log_schema_wait(batch, waited=waited, config=config)
                return True
        return False

    def _log_schema_wait(self, batch: RuntimeBatchState, *, waited: float, config: RuntimeAppConfig) -> None:
        """Log that a seal still waits for the schema, at most every `protocol_log_slow_wait_seconds`."""
        now_ms = monotonic_ms()
        last = self._wait_logged.get(batch.batch_id)
        if last is not None and now_ms - last < config.protocol_log_slow_wait_seconds * 1000:
            return
        if len(self._wait_logged) > 10_000:
            self._wait_logged.clear()
        self._wait_logged[batch.batch_id] = now_ms
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="runtime_seal_waiting_for_schema",
            materialization_id=batch.materialization_id,
            table=batch.table,
            partition=batch.partition,
            batch_id=batch.batch_id,
            waited_ms=int(waited * 1000),
            limit_ms=config.seal_schema_wait_seconds * 1000,
        )

    async def _seal(self, key: tuple[str, int], batch_id: str) -> None:
        table, partition = key
        config = get_runtime_app_config()
        started_ms = monotonic_ms()
        batch = None
        job_path = None
        materialization_id = None
        try:
            batch = get_pending_batch(table, partition, batch_id)
            context = self._contexts.get(table)
            view = current_view(self._state_table, table)
            if batch is None or context is None or view is None:
                return
            materialization_id = batch.materialization_id
            seal_id = uuid.uuid4().hex
            batch = replace(batch, state="sealing", seal_attempts=batch.seal_attempts + 1, seal_id=seal_id, updated_at=current_timestamp_ms())
            if update_pending_batch(batch) is None:
                return
            jsonl = Path(batch.local_path)
            job = SealJob(
                jsonl_path=str(jsonl),
                output_dir=str(jsonl.parent),
                schema=view.record.schema,
                fork_map=view.record.fork_map,
                granularity=context.partition_granularity,
                location=context.location,
                member_id=batch.owner_member_id,
                partition=batch.partition,
                offset_start=batch.start_offset,
                offset_end=batch.end_offset,
                seal_id=seal_id,
                row_group_bytes=config.target_row_group_size_bytes,
                protection_revision=batch.protection_revision,
            )
            job_path = jsonl.with_name(f"{jsonl.stem}-{seal_id[:8]}.seal.json")
            job_path.write_text(json.dumps(job.to_dict()), encoding="utf-8")
            output = await self.sealer(job_path)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = f"Sealing batch {batch_id} failed: {exc}"
            if batch is not None:
                update_pending_batch(replace(batch, state="rolled", last_error=error, updated_at=current_timestamp_ms()), error=error)
            LOGGER.warning("%s", error)
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="runtime_batch_seal_failed",
                materialization_id=materialization_id,
                table=table,
                partition=partition,
                batch_id=batch_id,
                attempts=None if batch is None else batch.seal_attempts,
                error=str(exc),
            )
        else:
            files = tuple(
                SealedFile(
                    local_path=item["local_path"],
                    uri=item["uri"],
                    partition_text=item.get("partition_text"),
                    partition_value=dict(item.get("partition_value") or {}),
                    record_count=int(item["record_count"]),
                    file_size=int(item["file_size"]),
                    event_time_min=item.get("event_time_min"),
                    event_time_max=item.get("event_time_max"),
                )
                for item in output.files
            )
            sealed = replace(
                batch,
                state="sealed",
                files=files,
                schema_id=output.schema_id,
                unmapped_values=output.unmapped_values,
                last_error=None,
                updated_at=current_timestamp_ms(),
            )
            if record_sealed_batch(sealed) is None:
                _remove_sealed_files(sealed)
                return
            seal_ms = elapsed_ms(started_ms)
            record_seal_processing(materialization_id=materialization_id, observed_at=current_timestamp_ms(), seal_ms=seal_ms)
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="runtime_batch_sealed",
                materialization_id=materialization_id,
                table=table,
                partition=partition,
                batch_id=batch_id,
                seal_id=sealed.seal_id,
                row_count=output.rows,
                files=len(files),
                parquet_bytes=sealed.sealed_bytes,
                unmapped_values=output.unmapped_values,
                schema_id=output.schema_id,
                seal_ms=seal_ms,
            )
            if output.unmapped_values:
                log_protocol_event(
                    LOGGER,
                    enabled=config.protocol_logging_enabled,
                    event="runtime_unmapped_values",
                    materialization_id=materialization_id,
                    table=table,
                    partition=partition,
                    batch_id=batch_id,
                    unmapped_values=output.unmapped_values,
                    schema_id=output.schema_id,
                )
        finally:
            if job_path is not None:
                job_path.unlink(missing_ok=True)
            self._seals.pop(batch_id, None)
            self.schedule()

    # -- uploads ---------------------------------------------------------------------------------

    def _start_uploads(self, loop, states: list[RuntimePartitionState], *, config: RuntimeAppConfig, now: str) -> None:
        free = config.upload_worker_count - len(self._uploads)
        if free <= 0:
            return
        candidates = []
        for state in states:
            key = (state.table, state.partition)
            if key in self._uploads:
                continue
            ready = [
                batch
                for batch in state.pending_batches.values()
                if batch.state == "sealed"
                or (batch.state == "failed" and _backoff_elapsed(batch.updated_at, batch.upload_attempts, now=now, config=config))
            ]
            if not ready:
                continue
            # One upload per partition at a time, oldest first: announcements then arrive in offset
            # order, so the committer seldom has to wait for a gap to fill.
            candidates.append((key, min(ready, key=lambda batch: batch.start_offset)))
        # Oldest roll first, whichever table, as for seals.
        candidates.sort(key=lambda item: (item[1].rolled_at or "", item[1].table, item[1].start_offset))
        for key, batch in candidates[:free]:
            self._uploads[key] = loop.create_task(self._upload(key, batch.batch_id))

    async def _upload(self, key: tuple[str, int], batch_id: str) -> None:
        table, partition = key
        config = get_runtime_app_config()
        started_ms = monotonic_ms()
        batch = None
        materialization_id = None
        try:
            batch = get_pending_batch(table, partition, batch_id)
            if batch is None:
                return
            materialization_id = batch.materialization_id
            batch = replace(batch, state="uploading", upload_attempts=batch.upload_attempts + 1, updated_at=current_timestamp_ms())
            if update_pending_batch(batch) is None:
                return
            loop = asyncio.get_running_loop()
            files = await loop.run_in_executor(
                self._executor_for(config),
                partial(upload_batch_files, batch, client=self.storage_client),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempts = None if batch is None else batch.upload_attempts
            if batch is not None:
                update_pending_batch(
                    replace(batch, state="failed", last_error=str(exc), updated_at=current_timestamp_ms()),
                    error=str(exc),
                )
            LOGGER.warning(
                "Uploading batch %s of materialization %s table %s partition %s failed (attempt %s): %s. "
                "Its offsets stay uncommitted until it succeeds.",
                batch_id,
                materialization_id,
                table,
                partition,
                attempts,
                exc,
            )
            record_upload_processing(
                materialization_id=materialization_id,
                observed_at=current_timestamp_ms(),
                upload_batch_ms=elapsed_ms(started_ms),
                uploaded_batches=0,
                failures=1,
            )
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="runtime_batch_upload_failed",
                materialization_id=materialization_id,
                table=table,
                partition=partition,
                batch_id=batch_id,
                attempts=attempts,
                retry_in_seconds=None if attempts is None else retry_backoff_seconds(attempts, config=config),
                error=str(exc),
            )
        else:
            now = current_timestamp_ms()
            uploaded = replace(batch, state="uploaded", files=files, announced_at=now, announce_attempts=1, updated_at=now)
            for item in files:
                Path(item.local_path).unlink(missing_ok=True)
            if record_uploaded_batch(uploaded) is None:
                # The session ended while the upload ran: its records are read again. The files
                # are orphans unless a committer already took them from an earlier announcement.
                return
            self._announce(uploaded, attempt=1, now=now)
            upload_ms = elapsed_ms(started_ms)
            record_upload_processing(
                materialization_id=materialization_id,
                observed_at=now,
                upload_batch_ms=upload_ms,
                uploaded_batches=1,
                failures=0,
            )
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="runtime_file_announced",
                materialization_id=materialization_id,
                table=table,
                partition=partition,
                batch_id=batch_id,
                seal_id=uploaded.seal_id,
                offset_start=uploaded.start_offset,
                offset_end=uploaded.end_offset,
                after_offset=uploaded.after_offset,
                files=len(files),
                upload_ms=upload_ms,
            )
        finally:
            self._uploads.pop(key, None)
            self.schedule()

    # -- announcements ---------------------------------------------------------------------------

    def _announce(self, batch: RuntimeBatchState, *, attempt: int, now: str) -> None:
        for item in batch.files:
            announced = AnnouncedFile(
                file_path=item.uri,
                materialization_id=batch.materialization_id,
                table=batch.table,
                member_id=batch.owner_member_id,
                batch_id=batch.batch_id,
                seal_id=batch.seal_id or "",
                file_count=len(batch.files),
                partition=batch.partition,
                after_offset=batch.after_offset,
                offset_start=batch.start_offset,
                offset_end=batch.end_offset,
                file_size=item.file_size,
                record_count=item.record_count,
                partition_value=item.partition_value,
                event_time_min=item.event_time_min,
                event_time_max=item.event_time_max,
                schema_id=batch.schema_id,
                session_generation=batch.session_generation,
                # Not the batch's: a batch never outlives the process that staged it.
                session_epoch=current_session_epoch(),
                announced_at=now,
                attempt=attempt,
            )
            outbox.put(f"file:{item.uri}", announced.to_dict())

    def _reannounce(self, states: list[RuntimePartitionState], *, config: RuntimeAppConfig, now: str) -> None:
        """Announce again every file no commit has covered after `announce_retry_seconds` (spec §7.4)."""
        moment = parse_timestamp(now)
        for state in states:
            for batch in list(state.pending_batches.values()):
                if batch.state != "uploaded" or not batch.files or batch.announced_at is None:
                    continue
                if (moment - parse_timestamp(batch.announced_at)).total_seconds() < config.announce_retry_seconds:
                    continue
                attempts = batch.announce_attempts + 1
                updated = update_pending_batch(replace(batch, announced_at=now, announce_attempts=attempts))
                if updated is not None:
                    self._announce(updated, attempt=attempts, now=now)

    # -- backpressure ----------------------------------------------------------------------------

    def note_backlog_full(self, app) -> None:
        """Pause at once when the writer finds the backlog full, rather than at the next pass."""
        if self._backlog_paused:
            return
        self._remember(app, None)
        pending_batches, pending_bytes = pending_upload_backlog(owner_member_id=current_member_id())
        self._pause_for_backlog(self._app, pending_batches, pending_bytes)

    def _apply_backpressure(self, app, states: list[RuntimePartitionState], *, config: RuntimeAppConfig, member_id: str) -> None:
        """Pause every source topic while the member's backlog is full; resume once it drains.

        One backlog for the member: a table whose partitions fill it holds every topic back, and
        none can stage more than the limits allow together.
        """
        if not states and not self._backlog_paused:
            return
        pending_batches, pending_bytes = pending_upload_backlog(owner_member_id=member_id)
        full = pending_batches >= config.max_pending_upload_batches or pending_bytes >= config.max_pending_upload_bytes
        drained = (
            pending_batches <= int(config.max_pending_upload_batches * BACKLOG_RESUME_FRACTION)
            and pending_bytes <= int(config.max_pending_upload_bytes * BACKLOG_RESUME_FRACTION)
        )
        if full and not self._backlog_paused:
            self._pause_for_backlog(app, pending_batches, pending_bytes)
        elif drained and self._backlog_paused:
            self._resume_after_backlog(app, pending_batches, pending_bytes)
        consumer = _consumer(app)
        if consumer is not None:
            for topic in self._source_topics():
                flow.reassert(consumer, topic, _assigned_partitions(consumer, topic))

    def _pause_for_backlog(self, app, pending_batches=None, pending_bytes=None) -> None:
        consumer = _consumer(app)
        if consumer is None:
            return
        paused: dict[str, int] = {}
        for topic in self._source_topics():
            tps = _assigned_partitions(consumer, topic)
            if not tps:
                continue
            flow.acquire(consumer, topic, flow.UPLOAD_BACKLOG, tps)
            paused[topic] = len(tps)
        if not paused:
            return
        self._backlog_paused = True
        materialization_id = next(iter(self._contexts.values())).materialization_id
        LOGGER.warning(
            "Pausing %s source partition(s) of materialization %s: the backlog reached its limits "
            "(%s batch(es), %s bytes not yet committed). Consumption resumes once it drains.",
            sum(paused.values()),
            materialization_id,
            pending_batches,
            pending_bytes,
        )
        log_protocol_event(
            LOGGER,
            enabled=get_runtime_app_config().protocol_logging_enabled,
            event="runtime_source_paused_for_upload_backlog",
            materialization_id=materialization_id,
            source_topics=sorted(paused),
            paused_partition_count=sum(paused.values()),
            pending_upload_batches=pending_batches,
            pending_upload_bytes=pending_bytes,
        )

    def _resume_after_backlog(self, app, pending_batches, pending_bytes) -> None:
        self._backlog_paused = False
        consumer = _consumer(app)
        if consumer is None:
            return
        resumed_count = 0
        topics = self._source_topics()
        for topic in topics:
            resumed = flow.release(consumer, topic, flow.UPLOAD_BACKLOG)
            resumed_count += 0 if resumed is None else len(resumed)
        materialization_id = next(iter(self._contexts.values())).materialization_id if self._contexts else None
        LOGGER.info(
            "Resuming the source partitions of materialization %s: the backlog drained to %s batch(es), %s bytes.",
            materialization_id,
            pending_batches,
            pending_bytes,
        )
        log_protocol_event(
            LOGGER,
            enabled=get_runtime_app_config().protocol_logging_enabled,
            event="runtime_source_resumed_after_upload_backlog",
            materialization_id=materialization_id,
            source_topics=topics,
            resumed_partition_count=resumed_count,
            pending_upload_batches=pending_batches,
            pending_upload_bytes=pending_bytes,
        )

    # -- helpers ---------------------------------------------------------------------------------

    def _executor_for(self, config: RuntimeAppConfig) -> ThreadPoolExecutor:
        # A pool of its own: uploads must not wait behind, or crowd out, the loop's default executor.
        workers = max(1, config.upload_worker_count)
        if self._executor is None or self._executor_workers != workers:
            if self._executor is not None:
                self._executor.shutdown(wait=False)
            self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="batch-upload")
            self._executor_workers = workers
        return self._executor

    def inflight(self) -> dict[str, int]:
        return {"seals": len(self._seals), "uploads": len(self._uploads), "outbox": len(outbox)}


#: The process's pipeline. Runtime state is per process, and so is this.
batch_pipeline = BatchPipeline()


def advance_pipeline(app=None, state_table=None) -> Iterator[dict[str, Any]]:
    """The pipeline task's entry point: schedule due work and return the outbox to send."""
    return batch_pipeline.advance(app, state_table)


def _remove_batch_files(batch: RuntimeBatchState) -> None:
    Path(batch.local_path).unlink(missing_ok=True)
    _remove_sealed_files(batch)


def _remove_sealed_files(batch: RuntimeBatchState) -> None:
    for item in batch.files:
        Path(item.local_path).unlink(missing_ok=True)


def _backoff_elapsed(updated_at: str | None, attempts: int, *, now: str, config: RuntimeAppConfig) -> bool:
    if attempts < 1 or updated_at is None:
        return True
    waited = (parse_timestamp(now) - parse_timestamp(updated_at)).total_seconds()
    return waited >= retry_backoff_seconds(attempts, config=config)


def _consumer(app):
    return None if app is None else getattr(app, "consumer", None)


def _assigned_partitions(consumer, topic: str) -> set:
    assignment = getattr(consumer, "assignment", None)
    if not callable(assignment):
        return set()
    try:
        current = assignment() or set()
    except Exception:
        return set()
    return {tp for tp in current if getattr(tp, "topic", None) == topic}


def _generation(app) -> int | None:
    generation = getattr(app, "rebalancing_count", None)
    return generation if isinstance(generation, int) and not isinstance(generation, bool) else None


def _rebalancing(app) -> bool:
    return bool(getattr(app, "rebalancing", False))
