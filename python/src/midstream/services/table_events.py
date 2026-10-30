"""Applying what runtimes report about their tables: the `table-events` agent's job (spec §9.2).

Each runtime's committer publishes what it did to each of its tables on
`materialization-table-events`: a schema change, a commit, a finished maintenance job, or an error,
naming the table. The tables are the truth; this keeps the control plane's projections of them -
each table's schema and its archive, each table record, the materialization's status - in step, so
the API answers without opening a table.

Events can arrive late or twice. Each one is applied only when it is news: a schema older than the
stored one, a commit at or below the stored sequence number, or a maintenance run already recorded
changes nothing.
"""

from __future__ import annotations

from dataclasses import replace
import logging
from typing import Any

from midstream.config import ControlPlaneConfig, get_config
from midstream.models import (
    MaterializationSchema,
    MaterializationStatus,
    MaterializationTable,
    RuntimeHealth,
    TableEvent,
)
from midstream.models.table import MaintenanceRun, PlaintextReport, RunningMaintenance, TableError
from midstream.protocol_logging import log_protocol_event
from midstream.services.schema_projection import annotate_protection
from midstream.storage import (
    get_materialization,
    get_materialization_schema,
    get_materialization_status,
    get_materialization_table,
    list_materialization_tables,
    list_schema_versions,
    save_materialization_schema,
    save_materialization_status,
    save_materialization_table,
)
from midstream.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

_DELETION_PHASES = frozenset({"deleting", "deleted"})

#: Table errors a successful maintenance job clears, as a commit clears the others.
_CLEARED_BY_MAINTENANCE = {"compaction": frozenset({"compaction_failed"}), "reprotect": frozenset({"reprotect_failed"})}


def apply_table_event(
    table: TableProtocol,
    event: TableEvent,
    *,
    config: ControlPlaneConfig | None = None,
) -> bool:
    """Apply one table event to the control plane's records; True when it changed anything."""
    cfg = get_config() if config is None else config
    materialization = get_materialization(table, event.materialization_id)
    if materialization is None:
        # About a materialization already purged.
        return False
    if materialization.desired_phase in _DELETION_PHASES:
        # Reported by a runtime being torn down - all but the end of a maintenance job, which the
        # delete's drop waits for (the app is told to stop it): it waits for the record to say so.
        record = get_materialization_table(table, event.materialization_id, event.table)
        if record is None or event.kind != "maintenance_completed":
            return False
        return _clear_running_job(table, record, event)
    record = get_materialization_table(table, event.materialization_id, event.table)
    if record is None:
        LOGGER.warning(
            "Ignoring a %s event for table %s of %s, which has no table record",
            event.kind,
            event.table,
            event.materialization_id,
        )
        return False
    if record.state == "dropping" and event.kind == "maintenance_completed":
        # A job stopped for the drop: the drop waits for the record to say nothing runs.
        changed = _clear_running_job(table, record, event)
    elif record.state in {"dropping", "dropped"}:
        # Reported before its topic was removed; what is left of it goes with the drop.
        LOGGER.info(
            "Ignoring a %s event for table %s of %s, which is %s",
            event.kind,
            event.table,
            event.materialization_id,
            record.state,
        )
        return False
    elif event.kind == "schema_changed":
        changed = _apply_schema_changed(table, materialization, record, event, config=cfg)
    elif event.kind == "table_committed":
        changed = _apply_table_committed(table, record, event)
    elif event.kind == "maintenance_started":
        changed = _apply_maintenance_started(table, record, event)
    elif event.kind == "maintenance_completed":
        changed = _apply_maintenance_completed(table, record, event)
    elif event.kind == "plaintext_counted":
        changed = _apply_plaintext_counted(table, record, event)
    else:
        changed = _apply_table_error(table, record, event)
    log_protocol_event(
        LOGGER,
        enabled=cfg.protocol_logging_enabled,
        event="table_event_applied",
        materialization_id=event.materialization_id,
        table=event.table,
        kind=event.kind,
        table_event_id=event.event_id,
        changed=changed,
    )
    return changed


def _apply_schema_changed(
    table: TableProtocol,
    materialization,
    record: MaterializationTable,
    event: TableEvent,
    *,
    config: ControlPlaneConfig,
) -> bool:
    payload = event.payload
    protection = payload.get("protection") if isinstance(payload.get("protection"), dict) else {}
    projection = MaterializationSchema(
        materialization_id=event.materialization_id,
        table=record.name,
        schema_id=int(payload["schema_id"]),
        since=event.occurred_at,
        metadata_location=payload.get("metadata_location"),
        field_count=int(payload.get("field_count") or 0),
        fields=annotate_protection(payload.get("fields") or [], protection),
        fork_map=dict(payload.get("fork_map") or {}),
        iceberg_schema=dict(payload.get("schema") or {}),
        changes=[dict(item) for item in payload.get("changes") or []],
        last_column_id=payload.get("last_column_id"),
        rejected_paths=[list(item) for item in payload.get("rejected_paths") or []],
        protection=dict(protection),
    )
    current = get_materialization_schema(table, event.materialization_id, record.name)
    # A policy the committer recorded keeps the schema and raises the protection revision.
    current_revision = current.protection_revision if current is not None and current.schema_id == record.schema_id else 0
    late = record.schema_id is not None and (record.schema_id, current_revision) >= projection.position
    before = _stored_schemas(table, event.materialization_id, record.name) if late else None
    # The archive takes every schema, even one that arrives after a newer one.
    save_materialization_schema(table, projection, archive=True, config=config)
    if late:
        # Late or delivered again: news only when the projection or the archive took it.
        return _stored_schemas(table, event.materialization_id, record.name) != before
    save_materialization_table(
        table,
        replace(
            record,
            schema_id=projection.schema_id,
            metadata_location=projection.metadata_location or record.metadata_location,
            updated_at=event.occurred_at,
        ),
    )
    return True


def _stored_schemas(
    table: TableProtocol, materialization_id: str, table_name: str
) -> tuple[MaterializationSchema | None, list[MaterializationSchema]]:
    """A table's current schema projection and its archive, as stored now."""
    return (
        get_materialization_schema(table, materialization_id, table_name),
        list_schema_versions(table, materialization_id, table_name),
    )


def _apply_table_committed(table: TableProtocol, record: MaterializationTable, event: TableEvent) -> bool:
    payload = event.payload
    sequence_number = payload.get("sequence_number")
    if (
        isinstance(sequence_number, int)
        and record.sequence_number is not None
        and sequence_number <= record.sequence_number
    ):
        return False
    schema_id = payload.get("schema_id")
    cleared = record.last_error is not None
    save_materialization_table(
        table,
        replace(
            record,
            current_snapshot_id=payload.get("snapshot_id"),
            sequence_number=sequence_number,
            last_commit_at=payload.get("committed_at") or event.occurred_at,
            last_commit_duration_ms=payload.get("duration_ms"),
            totals=_int_map(payload.get("totals")) or record.totals,
            offsets=_int_map(payload.get("offsets")) or record.offsets,
            valid_through=payload.get("valid_through") or record.valid_through,
            metadata_location=payload.get("metadata_location") or record.metadata_location,
            snapshots=payload.get("snapshots") if payload.get("snapshots") is not None else record.snapshots,
            schema_id=max(schema_id, record.schema_id or 0) if isinstance(schema_id, int) else record.schema_id,
            last_error=None,
            updated_at=event.occurred_at,
        ),
    )
    if cleared:
        _clear_table_degradation(table, event.materialization_id, reason=record.last_error.reason)
    return True


def _apply_maintenance_started(table: TableProtocol, record: MaterializationTable, event: TableEvent) -> bool:
    """The maintenance app started a long job on the table: the console shows it, a drop waits for it."""
    payload = event.payload
    started_at = str(payload.get("started_at") or event.occurred_at)
    current = record.maintenance_job
    if current is not None and current.started_at >= started_at:
        return False
    if any(run.started_at == started_at for run in record.maintenance if run.started_at):
        # Its completion arrived first.
        return False
    running = RunningMaintenance(
        job=str(payload.get("job") or "unknown"),
        started_at=started_at,
        partition=payload.get("partition"),
        attempt=int(payload.get("attempt") or 1),
        member_id=event.member_id,
    )
    save_materialization_table(table, replace(record, maintenance_job=running, updated_at=event.occurred_at))
    return True


def _finishes(record: MaterializationTable, payload: dict) -> bool:
    """Whether a completion is that of the job the record shows running."""
    running = record.maintenance_job
    if running is None:
        return False
    started_at = payload.get("started_at")
    if started_at:
        return str(started_at) == running.started_at
    return str(payload.get("job") or "") == running.job


def _clear_running_job(table: TableProtocol, record: MaterializationTable, event: TableEvent) -> bool:
    if not _finishes(record, event.payload):
        return False
    save_materialization_table(table, replace(record, maintenance_job=None, updated_at=event.occurred_at))
    return True


def _apply_maintenance_completed(table: TableProtocol, record: MaterializationTable, event: TableEvent) -> bool:
    payload = event.payload
    if any(run.event_id == event.event_id for run in record.maintenance):
        return False
    run = MaintenanceRun(
        job=str(payload.get("job") or "unknown"),
        completed_at=event.occurred_at,
        outcome=str(payload.get("outcome") or "succeeded"),
        duration_ms=payload.get("duration_ms"),
        snapshot_id=payload.get("snapshot_id"),
        counts=dict(payload.get("counts") or {}),
        message=payload.get("message"),
        partition=payload.get("partition"),
        event_id=event.event_id,
        started_at=payload.get("started_at"),
    )
    updated = record.with_maintenance(run, updated_at=event.occurred_at)
    if _finishes(record, payload):
        updated = replace(updated, maintenance_job=None)
    # A compaction replaces files: the table's file count and size change, and its snapshot. The
    # maintenance app and the runtime report apart, so a report the table's next commit has already
    # overtaken changes none of that.
    sequence = payload.get("sequence_number")
    newer = isinstance(sequence, int) and (updated.sequence_number is None or sequence > updated.sequence_number)
    if run.outcome == "succeeded" and newer and isinstance(payload.get("totals"), dict):
        updated = replace(updated, totals=_int_map(payload["totals"]) or updated.totals)
    if run.outcome == "succeeded" and newer and payload.get("metadata_location"):
        updated = replace(
            updated,
            metadata_location=payload["metadata_location"],
            current_snapshot_id=payload.get("snapshot_id") or updated.current_snapshot_id,
            sequence_number=sequence,
            snapshots=payload.get("snapshots") if payload.get("snapshots") is not None else updated.snapshots,
        )
    cleared = None
    if (
        run.job in ("compaction", "reprotect")
        and run.outcome == "succeeded"
        and record.last_error is not None
        and record.last_error.reason in _CLEARED_BY_MAINTENANCE.get(run.job, ())
    ):
        cleared = record.last_error.reason
        updated = replace(updated, last_error=None)
    request = updated.reprotect
    if run.job == "reprotect" and payload.get("done") and request is not None and payload.get("request_id") == request.request_id:
        # Done - rewritten, nothing to rewrite, or given up: the ticks stop asking for it.
        updated = replace(updated, reprotect=None)
    save_materialization_table(table, updated)
    if cleared is not None:
        _clear_table_degradation(table, event.materialization_id, reason=cleared)
    return True


def _apply_plaintext_counted(table: TableProtocol, record: MaterializationTable, event: TableEvent) -> bool:
    """What the table holds in plain text that its protection masks, as the maintenance app counted
    it: kept unless an older count arrives after a newer one."""
    report = PlaintextReport.from_dict({**event.payload, "counted_at": event.payload.get("counted_at") or event.occurred_at})
    if report is None:
        return False
    current = record.plaintext
    if current is not None and current.counted_at > report.counted_at:
        return False
    save_materialization_table(table, replace(record, plaintext=report, updated_at=event.occurred_at))
    return True


def _apply_table_error(table: TableProtocol, record: MaterializationTable, event: TableEvent) -> bool:
    reason = str(event.payload.get("reason") or "table_error")
    message = str(event.payload.get("message") or "The runtime reported a problem with the table.")
    save_materialization_table(
        table,
        replace(
            record,
            last_error=TableError(reason=reason, message=message, occurred_at=event.occurred_at),
            updated_at=event.occurred_at,
        ),
    )
    status = get_materialization_status(table, event.materialization_id)
    if status is not None and status.desired_phase == "running":
        save_materialization_status(
            table,
            degrade_for_table_error(status, reason=reason, message=table_error_message(record.name, message), at=event.occurred_at),
        )
    return True


def table_error_message(table_name: str, message: str) -> str:
    """A table's error as the materialization's status says it: naming the table, as it has several."""
    return f"Table {table_name}: {message}"


def degrade_for_table_error(
    status: MaterializationStatus,
    *,
    reason: str,
    message: str,
    at: str,
) -> MaterializationStatus:
    """`status` as degraded by a table error: the error's reason is the phase reason."""
    return replace(
        status,
        observed_phase="degraded",
        phase_reason=reason,
        phase_message=message,
        last_transition_time=at if status.observed_phase != "degraded" else status.last_transition_time,
        runtime_health=RuntimeHealth(level="degraded", reason=reason, message=message),
        last_error=message,
    )


def _clear_table_degradation(table: TableProtocol, materialization_id: str, *, reason: str) -> None:
    """Undo what a table error did to the status; the next heartbeat then says how the runtime is.

    Only once no table has an open error: another table's keeps the materialization degraded, now
    with its own reason.
    """
    status = get_materialization_status(table, materialization_id)
    if status is None or status.phase_reason != reason:
        return
    still_open = next(
        (record for record in sorted(list_materialization_tables(table, materialization_id), key=lambda item: item.name) if record.error_holds),
        None,
    )
    if still_open is not None:
        save_materialization_status(
            table,
            degrade_for_table_error(
                status,
                reason=still_open.last_error.reason,
                message=table_error_message(still_open.name, still_open.last_error.message),
                at=still_open.last_error.occurred_at,
            ),
        )
        return
    save_materialization_status(
        table,
        replace(
            status,
            observed_phase="running",
            phase_reason="runtime_ready",
            phase_message="The table committed again.",
            runtime_health=RuntimeHealth(level="healthy"),
            last_error=None,
        ),
    )


def _int_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, item in value.items():
        if isinstance(item, bool) or not isinstance(item, int):
            continue
        result[str(key)] = item
    return result


def _newer(candidate: Any, current: int | None) -> int | None:
    if isinstance(candidate, int) and (current is None or candidate > current):
        return candidate
    return current
