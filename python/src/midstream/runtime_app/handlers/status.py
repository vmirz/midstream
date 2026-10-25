"""The runtime's heartbeat: what each member reports on `materialization-status` (spec §7.12).

Every member reports its own counters, in all and per table; the member running the committer adds
each table's block - its snapshot, totals, committed offsets and files waiting for a commit - which
the control plane merges as the materialization's table state. Maintenance reports itself: the
maintenance app's table events (`maintenance_app/`).
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging
from typing import Any

from midstream.protocol_logging import log_protocol_event
from midstream.runtime_app.acks import ack_snapshot
from midstream.runtime_app.config import get_runtime_app_config
from midstream.runtime_app.live_settings import current_settings_position, refresh_runtime_settings
from midstream.runtime_app.performance import build_runtime_performance_snapshot
from midstream.runtime_app.pipeline import batch_pipeline
from midstream.runtime_app.state import build_runtime_processing_summary, list_partition_states
from midstream.runtime_app.table_state import current_view
from midstream.runtime_app.writer.batches import parse_timestamp
from midstream.runtime_app.writer.context import current_member_id
from midstream.version import PACKAGE_VERSION

LOGGER = logging.getLogger(__name__)

#: Files uploaded but uncommitted for this many commit intervals report the runtime degraded.
COMMITS_STALLED_INTERVALS = 10


def build_runtime_heartbeat(
    state_table,
    *,
    materialization_id: str,
    runtime_app_ref: str,
    agent_name: str,
    tables: list[str] | tuple[str, ...] = (),
    consume_performance: bool = True,
) -> dict[str, Any]:
    """The heartbeat this member sends now; `tables` names the runtime's tables."""
    now = _utc_now()
    member_id = current_member_id()
    snapshot = build_runtime_status_snapshot(
        state_table,
        materialization_id=materialization_id,
        tables=tables,
        consume_performance=consume_performance,
        now=now,
    )
    payload: dict[str, Any] = {
        "event_id": f"evt-status-{member_id}-{now}",
        "materialization_id": materialization_id,
        "runtime_app_ref": runtime_app_ref,
        "member_id": member_id,
        "agent_name": agent_name,
        "reported_at": now,
        **snapshot,
    }
    from midstream.runtime_app.committer import committer_table_blocks

    blocks = committer_table_blocks(materialization_id)
    if blocks:
        # Named, so the console can say which member commits.
        payload["tables"] = {name: {**block, "member_id": member_id} for name, block in blocks.items()}
    return payload


def build_runtime_status_snapshot(
    state_table,
    *,
    materialization_id: str,
    tables: list[str] | tuple[str, ...] = (),
    consume_performance: bool = False,
    now: str | None = None,
) -> dict[str, Any]:
    """This member's status: phase, health and counters, from runtime-local state."""
    member_id = current_member_id()
    config = get_runtime_app_config()
    # A member with no records to write still picks up changed settings here, and reports which
    # lineage and revision it runs so the control plane can show how far a change has reached.
    refresh_runtime_settings(state_table)
    settings_lineage, settings_revision = current_settings_position()
    views = {name: current_view(state_table, name) for name in tables}
    # The member can write once it knows every table's schema; until then it is loading them.
    loaded = bool(views) and all(view is not None for view in views.values())
    summary = build_runtime_processing_summary(materialization_id, member_id=member_id)
    snapshot: dict[str, Any] = {
        "phase": "running",
        "health": "healthy" if loaded else "unknown",
        # Always reported: knowing which code a runtime is executing is most valuable precisely
        # when it is not yet doing anything useful.
        "runtime_version": PACKAGE_VERSION,
        "settings_revision": settings_revision,
        # "" while it runs settings without a lineage, so the control plane can tell it from a
        # runtime that predates lineages, which sends none.
        "settings_lineage": settings_lineage,
        "latest_offsets": {},
    }
    schema_versions = {name: view.schema_id for name, view in views.items() if view is not None}
    if schema_versions:
        snapshot["schema_versions"] = schema_versions
    if summary is not None:
        snapshot["latest_offsets"] = {table: dict(offsets) for table, offsets in summary.latest_handled_offsets.items()}
        if summary.latest_committed_offsets:
            snapshot["latest_committed_offsets"] = {
                table: dict(offsets) for table, offsets in summary.latest_committed_offsets.items()
            }
        if summary.table_counters:
            snapshot["table_counters"] = {table: dict(counters) for table, counters in summary.table_counters.items()}
        for key in (
            "event_time",
            "rows_written",
            "bytes_written",
            "files_written",
            "records_skipped",
            "pending_upload_batches",
            "pending_upload_bytes",
            "files_pending_commit",
            "unmapped_values",
        ):
            value = getattr(summary, key)
            if value is not None:
                snapshot[key] = value
        if summary.skip_reasons:
            snapshot["skip_reasons"] = [reason.to_dict() for reason in summary.skip_reasons]
    performance = build_runtime_performance_snapshot(materialization_id, reset_window=consume_performance)
    if performance is not None:
        # What this member holds back from committing, and the seals and uploads under way.
        performance["acknowledgements"] = ack_snapshot(materialization_id)
        performance["pipeline"] = batch_pipeline.inflight()
        snapshot["performance"] = performance
        log_protocol_event(
            LOGGER,
            enabled=consume_performance and config.protocol_logging_enabled,
            event="runtime_processing_performance_window",
            materialization_id=materialization_id,
            member_id=member_id,
            performance=performance,
        )
    stalled = _oldest_uncommitted_seconds(materialization_id, member_id=member_id, now=now or _utc_now())
    if summary is not None and summary.last_error:
        snapshot["phase"] = "degraded"
        snapshot["health"] = "degraded"
        snapshot["phase_reason"] = "runtime_reported_degraded"
        snapshot["phase_message"] = summary.last_error
        snapshot["last_error"] = summary.last_error
        if summary.last_error_kind is not None:
            snapshot["health_reason"] = summary.last_error_kind
            snapshot["phase_reason"] = f"runtime_{summary.last_error_kind}_degraded"
    elif stalled is not None and stalled >= config.commit_interval_seconds * COMMITS_STALLED_INTERVALS:
        snapshot["phase"] = "degraded"
        snapshot["health"] = "degraded"
        snapshot["health_reason"] = "commits_stalled"
        snapshot["phase_reason"] = "commits_stalled"
        snapshot["phase_message"] = (
            f"Files this member uploaded {int(stalled)} s ago are not committed yet: the committer is "
            "not committing. Its member's logs say why."
        )
    else:
        reason, message = _phase(loaded=loaded, missing=[name for name, view in views.items() if view is None], summary=summary)
        snapshot["phase_reason"] = reason
        snapshot["phase_message"] = message
    return snapshot


def _phase(*, loaded: bool, missing: list[str], summary) -> tuple[str, str]:
    if not loaded:
        if missing:
            return "waiting_for_table", f"The runtime is loading its Iceberg tables ({', '.join(missing)})."
        return "waiting_for_table", "The runtime is loading its Iceberg tables."
    if _is_waiting_for_source_records(summary):
        return "waiting_for_source_records", "The table is ready; the runtime is waiting for source records."
    return "processing", "Runtime is actively processing source records."


def _is_waiting_for_source_records(summary) -> bool:
    if summary is None:
        return True
    if summary.latest_handled_offsets:
        return False
    for field_name in ("rows_written", "bytes_written", "files_written", "pending_upload_batches", "pending_upload_bytes"):
        value = getattr(summary, field_name, None)
        if value not in (None, 0):
            return False
    return True


def _oldest_uncommitted_seconds(materialization_id: str, *, member_id: str, now: str) -> float | None:
    """Seconds since the oldest batch whose files are uploaded but not committed was rolled, any table."""
    oldest = None
    for state in list_partition_states(owner_member_id=member_id):
        if state.materialization_id != materialization_id:
            continue
        for batch in state.pending_batches.values():
            if batch.state != "uploaded" or batch.rolled_at is None or not batch.files:
                continue
            if oldest is None or batch.rolled_at < oldest:
                oldest = batch.rolled_at
    if oldest is None:
        return None
    return (parse_timestamp(now) - parse_timestamp(oldest)).total_seconds()


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
