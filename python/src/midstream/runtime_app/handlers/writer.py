"""The writer agent: stage each source record for its topic's table, never waiting (spec §7.2).

The agent reads every source topic of the materialization; each topic has a table of its own, and
a record is written to its topic's. Per record, on the event loop:

0. find the record's table by the topic it came from;
1. note the session the record arrived in, and the table's committed offset for its partition
   (read from the table when the partition's session starts);
2. the replay guard: a record the table already holds is acknowledged without being written;
2a. data protection: the values of the table's protected fields are masked - hashed, shortened or
   withheld - so nothing after this step ever holds them (design §5.4); a batch holds rows of one
   protection revision, so a new revision rolls the open batch first;
3. drift detection against the table's current schema: whatever the schema cannot take yet becomes a
   `schema_change_request` for the committer, through the outbox - the record is not held;
4. stage the record as masked, one JSON line in the partition's open batch, and roll the batch
   when the roll policy says so;
5. hand the record to the ack ledger, which acknowledges it once a commit covers its batch.

The agent runs with Kaspr's `noack`, so nothing is acknowledged but through the ledger.
"""

from __future__ import annotations

import asyncio
import logging

from midstream.protection.catalog import MASK_REDACT
from midstream.protection.secrets import get_expected_key_id, get_protection_key, key_problem
from midstream.protocol_logging import elapsed_ms, log_protocol_event, monotonic_ms
from midstream.runtime_app import acks
from midstream.runtime_app.config import get_runtime_app_config
from midstream.runtime_app.live_settings import refresh_runtime_settings
from midstream.runtime_app.models import RuntimeWriteResult
from midstream.runtime_app.outbox import outbox
from midstream.runtime_app.performance import record_source_record_processing
from midstream.runtime_app.pipeline import batch_pipeline
from midstream.runtime_app.protection_state import note_key_problem
from midstream.runtime_app.schema.drift import RequestDeduper, observation_key
from midstream.runtime_app.state import record_rolled_batch, record_runtime_write, within_pending_upload_limits
from midstream.runtime_app.table_state import TableContext, contexts_by_topic, ensure_view
from midstream.runtime_app.writer import current_timestamp_ms, extract_source_record_metadata
from midstream.runtime_app.writer.batches import (
    append_row_to_batch,
    create_open_batch,
    roll_batch,
    should_roll_batch,
    staged_line,
)

LOGGER = logging.getLogger(__name__)

#: Each distinct observation is requested at most once per this many seconds per member, per table.
_DEDUPERS: dict[str, RequestDeduper] = {}

#: How often a writer that may not hash checks again, and logs that it waits.
PROTECTION_KEY_WAIT_SECONDS = 15.0


class RuntimeShutdownInterrupted(RuntimeError):
    """Raised when runtime shutdown interrupts a record that has not finished safely."""


def reset_writer() -> None:
    """Forget request deduplication. Tests use this between cases."""
    _DEDUPERS.clear()


def _deduper(table: str) -> RequestDeduper:
    deduper = _DEDUPERS.get(table)
    if deduper is None:
        deduper = _DEDUPERS[table] = RequestDeduper()
    return deduper


async def stage_source_record(
    value,
    state_table,
    *,
    context=None,
    materialization_id: str,
    runtime_app_ref: str | None = None,
    tables: tuple[TableContext, ...],
    skip_tombstones: bool = False,
) -> None:
    """Stage one source record in its topic's table; see the module docstring.

    `tables` is the runtime's table contexts (`table_state.table_contexts`), built once by the
    agent's init block. Returns nothing: the agent has no outputs.
    """
    started_ms = monotonic_ms()
    app = None if context is None else context.get("app")
    event = None if context is None else context.get("event")
    # Taken before anything awaits: the session this record was delivered in.
    received_generation = None if _rebalancing(app) else acks.current_generation(app)
    message = getattr(event, "message", None)
    topic = getattr(message, "topic", None)
    table_context = contexts_by_topic(tuple(tables)).get(topic) if topic is not None else (tables[0] if len(tables) == 1 else None)
    outcome = {"batch_id": None, "starts_batch": False, "acknowledge": True, "table": table_context.name if table_context else str(topic)}
    try:
        # Settings changed from the console reach every member through the replicated table;
        # reading them here, throttled, is what applies a change to the batches this member writes.
        refresh_runtime_settings(state_table)
        _raise_if_runtime_stopping(app, materialization_id=materialization_id)
        # Every table, so that a full backlog pauses topics this member has not read from yet too.
        batch_pipeline.register_tables(tables, app=app, state_table=state_table)
        if table_context is None:
            # Not one of the runtime's topics: nothing to write. Acknowledged, so its offsets do not
            # hold, as Faust would have.
            LOGGER.warning("Ignoring a record of topic %r, which materialization %s does not read", topic, materialization_id)
            return
        source_record = (
            None
            if event is None
            else extract_source_record_metadata(materialization_id, event, source_topic=table_context.source_topic)
        )
        if source_record is None:
            return
        config = get_runtime_app_config()
        view = await ensure_view(state_table, table_context, app=app, client=batch_pipeline.storage_client)
        plan = view.plan
        if plan.hashes and key_problem(get_protection_key(), get_expected_key_id()) is not None:
            # Never stage a value the table protects as it arrived: without the key the deployment
            # hashes with, wait until the runtime is re-applied with it.
            await _wait_for_protection_key(app, table_context.name, materialization_id=materialization_id)
            view = await ensure_view(state_table, table_context, app=app, client=batch_pipeline.storage_client)
            plan = view.plan
        partition_state = await batch_pipeline.ensure_partition_session(
            app=app,
            context=table_context,
            partition=source_record.partition,
            owner_member_id=source_record.member_id,
            config=config,
            state_table=state_table,
        )
        if _session_ended(app, received_generation):
            # A rebalance began since the record was delivered: Faust has sought its partition back,
            # and delivers it again in the new session.
            outcome["acknowledge"] = False
            return
        if source_record.offset <= partition_state.committed_offset:
            # Already in the table: acknowledged without writing it again (the replay guard).
            return
        if source_record.offset <= partition_state.handled_offset:
            # Staged earlier in this session but not committed yet - a fetch that went back. Its
            # first delivery carries the acknowledgement, which waits for its batch.
            outcome["acknowledge"] = False
            return
        if value is None and skip_tombstones:
            # A tombstone deletes its key upstream and carries no row: the offset is handled, nothing
            # is counted, and the open batch is kept.
            record_runtime_write(
                table=table_context.name,
                partition=source_record.partition,
                offset=source_record.offset,
                event_time=source_record.timestamp,
                write_result=RuntimeWriteResult(),
                updated_at=source_record.observed_at,
            )
            return

        protected = withheld = 0
        if not plan.empty:
            value = plan.protect(value)
            if plan.counters:
                for (_, mask), count in plan.drain_counters().items():
                    if mask == MASK_REDACT:
                        withheld += count
                    else:
                        protected += count

        observations = view.drift.observations(value) if isinstance(value, dict) else []
        requests = 0
        if observations:
            admitted = _deduper(table_context.name).admit(observations)
            if admitted:
                requests = 1
                _enqueue_schema_request(
                    materialization_id=materialization_id,
                    table=table_context.name,
                    runtime_app_ref=runtime_app_ref,
                    member_id=source_record.member_id,
                    base_schema_id=view.schema_id,
                    observations=admitted,
                    partition=source_record.partition,
                    offset=source_record.offset,
                    config=config,
                )

        write_started_ms = monotonic_ms()
        active_batch = _current_open_batch(
            partition_state,
            config=config,
            observed_at=source_record.observed_at,
            app=app,
            protection_revision=plan.revision,
        )
        starts_batch = active_batch is None
        if active_batch is None:
            active_batch = create_open_batch(
                config=config,
                source_record=source_record,
                table=table_context.name,
                after_offset=partition_state.chain_offset,
                session_generation=partition_state.session_generation,
                protection_revision=plan.revision,
            )
        staged = append_row_to_batch(
            active_batch,
            line=staged_line(source_record, value),
            source_record=source_record,
            observations=tuple(observation_key(path, observed) for path, observed in observations),
        )
        rolled = None
        if _roll_due(staged, config=config, observed_at=source_record.observed_at) and _may_roll(
            app, partition_state, staged, config=config
        ):
            rolled = roll_batch(staged, observed_at=source_record.observed_at)
        record_runtime_write(
            table=table_context.name,
            partition=source_record.partition,
            offset=source_record.offset,
            event_time=source_record.timestamp,
            write_result=RuntimeWriteResult(rows_increment=1, protected_increment=protected, withheld_increment=withheld),
            updated_at=source_record.observed_at,
            active_batch=None if rolled is not None else staged,
            rolled_batch=rolled,
        )
        outcome["batch_id"] = staged.batch_id
        outcome["starts_batch"] = starts_batch
        if rolled is not None:
            # Sealing and uploading happen in the pipeline, off this path; start them now rather
            # than at the pipeline task's next pass.
            batch_pipeline.schedule(app, state_table)
        record_source_record_processing(
            materialization_id=materialization_id,
            observed_at=source_record.observed_at,
            total_ms=elapsed_ms(started_ms),
            write_ms=elapsed_ms(write_started_ms),
            schema_requests=requests,
        )
    except BaseException as exc:
        _on_record_failure(context, exc, materialization_id=materialization_id, outcome=outcome)
        raise
    finally:
        if outcome["acknowledge"] and not outcome.get("failed"):
            _acknowledge(context, outcome, materialization_id=materialization_id, generation=received_generation)


def _current_open_batch(partition_state, *, config, observed_at: str, app, protection_revision: int = 0):
    """The partition's open batch to append to, after rolling it first if it is due.

    A batch staged under another protection revision is rolled whatever the roll policy and the
    backlog say: a batch's rows, and so its files, are of one revision (design §5.4).
    """
    batch = partition_state.active_batch
    if batch is None or batch.state != "open":
        return None
    if batch.protection_revision != protection_revision and batch.row_count >= 1:
        record_rolled_batch(roll_batch(batch, observed_at=observed_at))
        batch_pipeline.schedule(app)
        return None
    if not _roll_due(batch, config=config, observed_at=observed_at):
        return batch
    if batch.row_count < 1:
        return None
    if not _may_roll(app, partition_state, batch, config=config):
        return batch
    record_rolled_batch(roll_batch(batch, observed_at=observed_at))
    batch_pipeline.schedule(app)
    return None


async def _wait_for_protection_key(app, table: str, *, materialization_id: str) -> None:
    """Return once this member may hash; until then, wait, report why, and log it now and then.

    The key comes from the pod's environment, so a member without it - or with another than the
    deployment expects - waits until the control plane re-applies the runtime with it, which it does
    whenever the key's id it renders changes. Nothing is written meanwhile: the record stays
    unacknowledged and is read again if the member restarts.
    """
    problem = key_problem(get_protection_key(), get_expected_key_id())
    if problem is None:
        note_key_problem(None)
        return
    note_key_problem(problem)
    while problem is not None:
        LOGGER.warning(
            "Table %s of materialization %s masks values with a keyed hash, and this member %s: "
            "nothing is written until the runtime runs with the deployment's protection key.",
            table,
            materialization_id,
            "has no protection key" if problem == "protection_key_missing" else "has another protection key than the deployment's",
        )
        _raise_if_runtime_stopping(app, materialization_id=materialization_id)
        await asyncio.sleep(PROTECTION_KEY_WAIT_SECONDS)
        _raise_if_runtime_stopping(app, materialization_id=materialization_id)
        problem = key_problem(get_protection_key(), get_expected_key_id())
    note_key_problem(None)


def _roll_due(batch, *, config, observed_at: str) -> bool:
    """Whether a batch should roll: by the roll policy, or early when its records are held in order."""
    if should_roll_batch(batch, config=config, observed_at=observed_at):
        return True
    return batch.row_count >= acks.ORDERED_BATCH_MAX_ROWS and acks.batch_is_ordered(
        batch.table,
        batch.partition,
        batch.batch_id,
    )


def _may_roll(app, partition_state, batch, *, config) -> bool:
    # The backlog is the member's, every table's batches counted together: more topics never
    # mean more seals and uploads at once.
    within = within_pending_upload_limits(
        owner_member_id=partition_state.owner_member_id,
        next_batch_bytes=batch.estimated_bytes,
        config=config,
    )
    if not within:
        # The open batch keeps filling for now, and the member stops fetching every topic until
        # the backlog drains, so what it stages is bounded by what was already fetched.
        batch_pipeline.note_backlog_full(app)
    return within


def _enqueue_schema_request(
    *,
    materialization_id: str,
    table: str,
    runtime_app_ref: str | None,
    member_id: str,
    base_schema_id: int,
    observations,
    partition: int,
    offset: int,
    config,
) -> None:
    requested_at = current_timestamp_ms()
    message = {
        "kind": "schema_change_request",
        "materialization_id": materialization_id,
        "table": table,
        "runtime_app_ref": runtime_app_ref,
        "member_id": member_id,
        "base_schema_id": base_schema_id,
        # Shapes only - never a value - so no record content travels on the commits topic.
        "observations": [{"path": list(path), "observed": observed} for path, observed in observations],
        "sample": {"partition": partition, "offset": offset},
        "requested_at": requested_at,
    }
    key = f"schema:{table}:" + "|".join(sorted(observation_key(path, observed) for path, observed in observations))
    outbox.put(key, message)
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="runtime_schema_change_requested",
        materialization_id=materialization_id,
        table=table,
        base_schema_id=base_schema_id,
        observations=len(observations),
        paths=[".".join(path) for path, _ in observations][:20],
        partition=partition,
        offset=offset,
    )


def _acknowledge(context, outcome, *, materialization_id: str, generation) -> None:
    if context is None:
        return
    event = context.get("event")
    message = getattr(event, "message", None)
    partition = getattr(message, "partition", None)
    if not isinstance(partition, int):
        return
    acks.register_record(
        context.get("app"),
        event,
        table=outcome["table"],
        partition=partition,
        generation=generation,
        batch_id=outcome["batch_id"],
        starts_batch=bool(outcome["starts_batch"]),
    )


def _on_record_failure(context, exc: BaseException, *, materialization_id: str, outcome) -> None:
    """Decide a record's fate when an exception ends its processing.

    Shutdown, cancellation (a revoked partition) and the generator being closed leave the record
    unacknowledged: it is read again. Anything else is unexpected, and acknowledging the record would
    lose it; leaving it unacknowledged would hold the partition's committed offset - and Faust's
    acknowledgement bookkeeping - for the rest of the session. So the member is restarted instead,
    and reads the record again from the committed offset.
    """
    outcome["failed"] = True
    if isinstance(exc, (RuntimeShutdownInterrupted, asyncio.CancelledError, GeneratorExit)):
        return
    if not isinstance(exc, Exception):
        return
    app = None if context is None else context.get("app")
    event = None if context is None else context.get("event")
    message = getattr(event, "message", None)
    LOGGER.error(
        "Writing the source record at partition %s offset %s of materialization %s failed; it is "
        "not acknowledged, and this member restarts to read it again from the committed offset.",
        getattr(message, "partition", None),
        getattr(message, "offset", None),
        materialization_id,
        exc_info=exc,
    )
    crash = getattr(app, "crash", None)
    if callable(crash):
        try:
            asyncio.get_running_loop().create_task(crash(exc))
        except RuntimeError:
            pass


def _raise_if_runtime_stopping(app, *, materialization_id: str) -> None:
    if app is None or not getattr(app, "should_stop", False):
        return
    raise RuntimeShutdownInterrupted(f"Runtime shutdown interrupted a record of materialization {materialization_id}.")


def _session_ended(app, received_generation) -> bool:
    """Whether a rebalance has begun since the record was delivered (outside an app: never)."""
    current = acks.current_generation(app)
    if current is None:
        return False
    return _rebalancing(app) or received_generation != current


def _rebalancing(app) -> bool:
    return bool(getattr(app, "rebalancing", False))
