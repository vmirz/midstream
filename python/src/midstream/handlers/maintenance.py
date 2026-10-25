"""Thin Kaspr-facing handlers for scheduled maintenance work.

A scheduled step arrives on a `__system__` work event and only reads the state table. A write lands
in the shard of the event being handled, and a materialization's records live in its own, so what a
step finds to do for a materialization goes on as work keyed by that materialization: a retention
purge for the workflow agent, a measured lag for `record_consumer_lag` here. Each is then handled in
the materialization's own shard. `schedule_table_maintenance` writes nothing at all: it ticks every
maintained table on the maintenance app's topic (`services/table_maintenance.py`), and names every
table to the tail app on its topic (`services/live_tail.py`).
"""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator

from midstream.config import ControlPlaneConfig
from midstream.kafka.metadata import KafkaMetadataClient
from midstream.models import EventCause, MaterializationWorkEvent
from midstream.scheduler import build_direct_work_output
from midstream.services.autoscaling import autoscale_after_lag
from midstream.services.live_tail import build_live_tail_messages, live_tail_output
from midstream.services.consumer_lag import (
    record_consumer_lag,
    refresh_consumer_lag,
    refresh_consumer_lag_async,
)
from midstream.services.retention import expired_materialization_purges
from midstream.services.sweep_cursor import SweepCursor
from midstream.services.table_maintenance import build_table_maintenance_messages, maintenance_output
from midstream.settings import effective_runtime_config, resolve_config
from midstream.storage.table import TableProtocol

SYSTEM_MATERIALIZATION_ID = "__system__"

#: Every scheduled maintenance step, mapped to the helper its KasprTask calls to trigger it.
#:
#: The agent's input filter and the task manifests are YAML and cannot import this module, so
#: `tests/test_agent_manifests.py` reads this mapping to prove all three agree. A step missing
#: from the agent filter is consumed and dropped in silence; a step no task references never runs.
MAINTENANCE_STEP_TRIGGERS: dict[str, str] = {
    "purge_deleted_materializations": "build_purge_deleted_trigger",
    "refresh_consumer_lag": "build_consumer_lag_refresh_trigger",
    "schedule_table_maintenance": "build_table_maintenance_trigger",
}

#: Work a scheduled step hands on, keyed by the materialization it is about, that this agent handles.
MAINTENANCE_FOLLOW_UP_STEPS = frozenset({"record_consumer_lag"})

MAINTENANCE_STEPS = frozenset(MAINTENANCE_STEP_TRIGGERS) | MAINTENANCE_FOLLOW_UP_STEPS

# Where the next lag refresh starts, so successive ones cover the whole fleet. Memory is enough: the
# triggers come from `onLeader` tasks, all keyed `materialization:__system__`, so they land on one
# partition of the work topic, consumed by one control-plane member at a time (the app runs one
# replica in every overlay). A restart or a rebalance starts from the first materialization again,
# which delays a part of the fleet by at most one rotation.
_CONSUMER_LAG_CURSOR = SweepCursor()


def build_purge_deleted_trigger(
    *,
    requested_at: str,
    task_id: str = "purge-deleted-materializations-task",
) -> dict:
    """Build the synthetic work event emitted by the retention purge task."""
    return _build_system_work_event(
        step="purge_deleted_materializations",
        requested_at=requested_at,
        task_id=task_id,
    )


def build_consumer_lag_refresh_trigger(
    *,
    requested_at: str,
    task_id: str = "refresh-consumer-lag-task",
) -> dict:
    """Build the synthetic work event emitted by the consumer-lag refresh task."""
    return _build_system_work_event(
        step="refresh_consumer_lag",
        requested_at=requested_at,
        task_id=task_id,
    )


def build_table_maintenance_trigger(
    *,
    requested_at: str,
    task_id: str = "schedule-table-maintenance-task",
) -> dict:
    """Build the synthetic work event emitted by the table maintenance task, each minute."""
    return _build_system_work_event(
        step="schedule_table_maintenance",
        requested_at=requested_at,
        task_id=task_id,
    )


def handle_maintenance_work(
    value: dict,
    table: TableProtocol,
    *,
    metadata_client: KafkaMetadataClient | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> Iterator[dict]:
    """Execute a maintenance work event and yield the routed work it hands on.

    Blocks on Kafka. The maintenance agent runs `handle_maintenance_work_async`.
    """
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    if work_event.step == "purge_deleted_materializations":
        yield from _routed(expired_materialization_purges(table, now=work_event.requested_at, config=cfg))
    elif work_event.step == "refresh_consumer_lag":
        records = refresh_consumer_lag(
            table,
            metadata_client=_required_metadata_client(metadata_client),
            measured_at=work_event.requested_at,
            config=cfg,
            cursor=_CONSUMER_LAG_CURSOR,
        )
        yield from _routed(records)
    elif work_event.step == "record_consumer_lag":
        if record_consumer_lag(table, work_event):
            yield from _routed(autoscale_after_lag(table, work_event, config=cfg))
    elif work_event.step == "schedule_table_maintenance":
        yield from _table_maintenance(table, work_event, settings_table=settings_table, config=cfg)


async def handle_maintenance_work_async(
    value: dict,
    table: TableProtocol,
    *,
    metadata_client: KafkaMetadataClient | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> Generator[dict, None, None]:
    """`handle_maintenance_work` for the maintenance agent's async map.

    The agent shares its event loop with the API and every other agent. The Kafka lookups run on
    the metadata client's loop, so that loop keeps running while they wait; the retention sweep only
    reads the table, a bounded batch, and runs as before.

    Returns a generator over outputs already computed. Kaspr iterates a generator that a later
    pipeline operation returns, but sends a list as one value and cannot iterate an async
    generator there at all.
    """
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    outputs: list[dict] = []
    if work_event.step == "purge_deleted_materializations":
        outputs = _routed(expired_materialization_purges(table, now=work_event.requested_at, config=cfg))
    elif work_event.step == "refresh_consumer_lag":
        records = await refresh_consumer_lag_async(
            table,
            metadata_client=_required_metadata_client(metadata_client),
            measured_at=work_event.requested_at,
            config=cfg,
            cursor=_CONSUMER_LAG_CURSOR,
        )
        outputs = _routed(records)
    elif work_event.step == "record_consumer_lag":
        # Recorded in the materialization's shard; autoscaling decides there, from the measurement.
        if record_consumer_lag(table, work_event):
            outputs = _routed(autoscale_after_lag(table, work_event, config=cfg))
    elif work_event.step == "schedule_table_maintenance":
        outputs = _table_maintenance(table, work_event, settings_table=settings_table, config=cfg)
    return (output for output in outputs)


def _table_maintenance(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    settings_table: TableProtocol | None,
    config: ControlPlaneConfig,
) -> list[dict]:
    """This minute's message for every table, for the maintenance app's topic and the tail app's."""
    messages = build_table_maintenance_messages(
        table,
        runtime_config=effective_runtime_config(settings_table, config),
        issued_at=work_event.requested_at,
    )
    tails = build_live_tail_messages(table, config=config, issued_at=work_event.requested_at)
    return [maintenance_output(message) for message in messages] + [live_tail_output(message) for message in tails]


def _routed(work_events: Iterable[MaterializationWorkEvent]) -> list[dict]:
    """Each event as work for the work topic, keyed by its materialization."""
    return [build_direct_work_output(work_event) for work_event in work_events]


def _required_metadata_client(metadata_client: KafkaMetadataClient | None) -> KafkaMetadataClient:
    if metadata_client is None:
        raise ValueError("metadata_client is required for refresh_consumer_lag")
    return metadata_client


def _build_system_work_event(*, step: str, requested_at: str, task_id: str) -> dict:
    return MaterializationWorkEvent(
        work_id=f"work-{step}-{task_id}-{requested_at}",
        materialization_id=SYSTEM_MATERIALIZATION_ID,
        step=step,
        requested_at=requested_at,
        caused_by=EventCause(kind="scheduler", id=task_id),
        dedupe_key=f"{step}:{task_id}:{requested_at}",
    ).to_dict()
