"""What the maintenance app is told to do with each table (`models/maintenance.py`).

Every minute the control plane ticks every table that is written or kept, of every materialization
that is not being deleted - stopped and paused ones included, so their tables stay compacted and their
snapshots expire. A tick carries all the app needs: where the table is, the runtimes' maintenance
settings as they are now, when each job last finished, and which job the record says is running. The
app keeps nothing of its own, so a restarted app picks up where its predecessor left off.

A table being dropped, or one of a materialization being deleted, is sent `maintenance_cancel`
instead, until it is gone: whatever the app runs on it is stopped before the drop deletes its files.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from midstream.models import (
    MAINTAINED_TABLE_STATES,
    MaintenanceMessage,
    MaintenanceTarget,
    Materialization,
    MaterializationTable,
)
from midstream.runtime_app.config import RuntimeAppConfig
from midstream.storage import iter_materializations, list_materialization_tables
from midstream.storage.table import TableProtocol

#: The runtime settings that steer maintenance, which every tick and request carries.
MAINTENANCE_SETTING_FIELDS = (
    "compaction_enabled",
    "compaction_interval_minutes",
    "compaction_min_input_files",
    "compaction_small_file_bytes",
    "compaction_target_file_size_bytes",
    "compaction_max_input_bytes",
    "compaction_hot_partition_delay_minutes",
    "compaction_timeout_minutes",
    "compaction_max_attempts",
    "snapshot_retention_hours",
    "min_snapshots_to_keep",
    "snapshot_keep_all_minutes",
    "snapshot_expiry_interval_minutes",
    "orphan_cleanup_enabled",
    "orphan_min_age_hours",
    "orphan_cleanup_interval_hours",
    "protocol_logging_enabled",
)

_DELETION_PHASES = frozenset({"deleting", "deleted"})


def maintenance_settings(runtime_config: RuntimeAppConfig) -> dict[str, Any]:
    """The maintenance settings of `runtime_config`, as a tick carries them."""
    return {name: getattr(runtime_config, name) for name in MAINTENANCE_SETTING_FIELDS}


def maintenance_target(record: MaterializationTable) -> MaintenanceTarget:
    return MaintenanceTarget(
        materialization_id=record.materialization_id,
        namespace=record.namespace,
        table=record.name,
        location=record.location,
        source_topic=record.source_topic,
        partition_granularity=record.partition_granularity,
        state=record.state,
    )


def last_runs(record: MaterializationTable) -> dict[str, str]:
    """When each job last finished on the table, whatever its outcome, from the record's history."""
    latest: dict[str, str] = {}
    for run in record.maintenance:
        if run.completed_at and run.completed_at > latest.get(run.job, ""):
            latest[run.job] = run.completed_at
    return latest


def build_table_maintenance_messages(
    table: TableProtocol,
    *,
    runtime_config: RuntimeAppConfig,
    issued_at: str,
) -> list[MaintenanceMessage]:
    """This minute's tick for every maintained table, and a cancel for every table on its way out.

    Reads only: the state table is global, so this sees every materialization from any shard.
    """
    settings = maintenance_settings(runtime_config)
    messages: list[MaintenanceMessage] = []
    for materialization in iter_materializations(table):
        if materialization.desired_phase == "deleted":
            continue
        for record in list_materialization_tables(table, materialization.materialization_id):
            messages.extend(_messages_for(materialization, record, settings=settings, issued_at=issued_at))
    return messages


def _messages_for(
    materialization: Materialization,
    record: MaterializationTable,
    *,
    settings: dict[str, Any],
    issued_at: str,
) -> Iterator[MaintenanceMessage]:
    if materialization.desired_phase in _DELETION_PHASES or record.state == "dropping":
        yield MaintenanceMessage(
            kind="maintenance_cancel",
            target=maintenance_target(record),
            issued_at=issued_at,
            running=record.maintenance_job,
            reason="materialization_deleting" if materialization.desired_phase in _DELETION_PHASES else "table_dropping",
        )
        return
    if record.state not in MAINTAINED_TABLE_STATES:
        return
    yield MaintenanceMessage(
        kind="maintenance_tick",
        target=maintenance_target(record),
        issued_at=issued_at,
        settings=settings,
        last_runs=last_runs(record),
        running=record.maintenance_job,
    )


def maintenance_request_messages(
    records: Iterable[MaterializationTable],
    *,
    job: str,
    runtime_config: RuntimeAppConfig,
    request_id: str,
    requested_by: str,
    issued_at: str,
) -> list[MaintenanceMessage]:
    """A job someone asked for, one message for each of `records` that is maintained."""
    settings = maintenance_settings(runtime_config)
    return [
        MaintenanceMessage(
            kind="maintenance_request",
            target=maintenance_target(record),
            issued_at=issued_at,
            settings=settings,
            job=job,
            request_id=request_id,
            requested_by=requested_by,
        )
        for record in records
        if record.state in MAINTAINED_TABLE_STATES
    ]


def maintenance_output(message: MaintenanceMessage) -> dict[str, Any]:
    """`message` routed to the maintenance app's topic, keyed by its table."""
    return {"route": "maintenance", "key": message.key, "value": message.to_dict()}
