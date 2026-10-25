"""Response shapers for WebView handlers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from materializer_control_plane.identity.principals import Principal
from materializer_control_plane.storage.materializations import observed_phase_for
from materializer_control_plane.models import (
    Materialization,
    MaterializationRequestEvent,
    MaterializationSchema,
    MaterializationStatus,
    MaterializationTable,
    RuntimeAutoscale,
    RuntimeBinding,
    TableSpec,
)


@dataclass(frozen=True, slots=True)
class MutationAcceptance:
    """Bundle the accepted response with the records created for a mutation request."""

    response: dict[str, Any]
    request_event: MaterializationRequestEvent
    materialization: Materialization
    status: MaterializationStatus


def build_accepted_response(
    *,
    materialization_id: str,
    request_id: str,
    desired_phase: str,
    observed_phase: str,
    phase_reason: str,
    accepted_at: str,
) -> dict[str, Any]:
    """Build the synchronous acceptance payload returned to web clients."""
    return {
        "materialization_id": materialization_id,
        "request_id": request_id,
        "desired_phase": desired_phase,
        "observed_phase": observed_phase,
        "phase_reason": phase_reason,
        "status_url": f"/materializations/{materialization_id}",
        "accepted_at": accepted_at,
    }


def build_materialization_detail(
    *,
    materialization: Materialization,
    status: MaterializationStatus,
    runtime_binding: RuntimeBinding | None,
    table_records: list[MaterializationTable] | None = None,
    principal: Principal | None = None,
    autoscale: RuntimeAutoscale | None = None,
) -> dict[str, Any]:
    """Build the detail payload shown by the materialization detail view.

    `tables` summarizes each table that exists so far, with its `state`: the written ones in the
    order of the materialization's topics, then the kept ones, then any being dropped or dropped.
    With the caller, each says whether they may read it.
    """
    reconciled_phase, reconciled_reason = observed_phase_for(materialization, status)
    payload: dict[str, Any] = {
        "materialization": {
            "materialization_id": materialization.materialization_id,
            "name": materialization.name,
            "tables": [table.to_dict() for table in materialization.tables],
            "retired_tables": [table.to_dict() for table in materialization.retired_tables],
            "source_topics": list(materialization.source_topics),
            "destination_uri": materialization.destination_uri,
            "runtime_mode": materialization.runtime_mode,
            "namespace": materialization.namespace,
            "resolved_runtime_name": materialization.resolved_runtime_name,
            "desired_phase": materialization.desired_phase,
            "resource_profile": materialization.resource_profile,
            "storage_size": materialization.storage_size,
            "replicas": materialization.replicas,
            "skip_tombstones": materialization.skip_tombstones,
            "autoscaling": materialization.autoscaling,
            "partition_granularity": materialization.partition_granularity,
            # Always true - every materialization has a live tail - and still sent: a 4.1 console offers Live only when it reads it.
            "live_tail": True,
            "created_at": materialization.created_at,
            "updated_at": materialization.updated_at,
            "created_by": materialization.created_by,
            "created_by_id": materialization.created_by_id,
            "labels": dict(materialization.labels),
            "annotations": dict(materialization.annotations),
            "spec_version": materialization.spec_version,
            "datasets": sorted({table.dataset for table in materialization.all_tables}),
            "pending_topic_change": (
                None
                if materialization.pending_topic_change is None
                else {
                    "request_id": materialization.pending_topic_change.request_id,
                    "claim_request_id": materialization.pending_topic_change.claim_request_id,
                    "submitted_at": materialization.pending_topic_change.submitted_at,
                    "added": [spec.to_dict() for spec in materialization.pending_topic_change.plan.added],
                }
            ),
        },
        # How the control plane scaled the runtime: `replicas` the members it runs while scaled up
        # (null at its own count), why, since when, and the time to catch up at the pace measured.
        "autoscale": describe_autoscale(materialization, autoscale),
        "status": {
            # Same reconciliation the fleet list applies, so a materialization cannot read as
            # deleted in one view and running in the other.
            "desired_phase": materialization.desired_phase,
            "observed_phase": reconciled_phase,
            "phase_reason": reconciled_reason,
            "phase_message": status.phase_message,
            "last_transition_time": status.last_transition_time,
            "last_heartbeat": status.last_heartbeat,
            "last_resource_observation_time": status.last_resource_observation_time,
            "observation_source": status.observation_source,
            "runtime_version": status.runtime_version,
            "last_error": status.last_error,
            "runtime_health": status.runtime_health.to_dict(),
            "resource_observation": status.resource_observation.to_dict(),
        },
        "runtime_binding": None,
        "tables": [
            describe_table_summary(record, principal=principal)
            for record in tables_in_order(materialization, table_records or [])
        ],
    }
    runtime_progress = status.runtime_progress.to_dict()
    if runtime_progress:
        payload["status"]["runtime_progress"] = runtime_progress
    if status.consumer_lag is not None:
        payload["status"]["consumer_lag"] = status.consumer_lag.to_dict()
    if runtime_binding is not None:
        payload["runtime_binding"] = {
            "kaspr_app_name": runtime_binding.kaspr_app_name,
            "namespace": runtime_binding.namespace,
            "managed_agent_names": list(runtime_binding.managed_agent_names),
            "runtime_mode": runtime_binding.runtime_mode,
            "template_version": runtime_binding.template_version,
            "source_topics": list(runtime_binding.source_topics),
            "last_applied_spec_hash": runtime_binding.last_applied_spec_hash,
            "last_apply_operation_id": runtime_binding.last_apply_operation_id,
            "created_at": runtime_binding.created_at,
            "updated_at": runtime_binding.updated_at,
        }
    return payload


def tables_in_order(materialization: Materialization, records: list[MaterializationTable]) -> list[MaterializationTable]:
    """The table records that exist: the written ones in the order of the materialization's topics,
    the kept ones next, and any other - being dropped, or dropped - by name."""
    by_name = {record.name: record for record in records}
    ordered = [by_name.pop(table.name) for table in materialization.all_tables if table.name in by_name]
    return ordered + sorted(by_name.values(), key=lambda record: record.name)


def table_access(record: MaterializationTable, principal: Principal | None) -> dict[str, bool]:
    """Whether a caller may see a table's schema and query its data, on its dataset."""
    if principal is None:
        return {}
    return {
        "readable": principal.can("dataset.view_schema", record.dataset_id),
        "queryable": principal.can("data.query", record.dataset_id),
    }


def describe_table_summary(record: MaterializationTable, *, principal: Principal | None = None) -> dict[str, Any]:
    """A table as the detail view summarizes it: where it is and what it holds."""
    return {
        "dataset": record.dataset,
        "dataset_id": record.dataset_id,
        "name": record.name,
        "source_topic": record.source_topic,
        "identifier": record.qualified_name,
        "location": record.location,
        "partition_granularity": record.partition_granularity,
        "schema_id": record.schema_id,
        "current_snapshot_id": record.current_snapshot_id,
        "last_commit_at": record.last_commit_at,
        "valid_through": record.valid_through,
        "totals": dict(record.totals),
        "last_error": None if record.last_error is None else record.last_error.to_dict(),
        "state": record.state,
        "maintenance_job": None if record.maintenance_job is None else record.maintenance_job.to_dict(),
        **table_access(record, principal),
    }


def build_materialization_table(
    *,
    materialization: Materialization,
    record: MaterializationTable,
    status: MaterializationStatus | None,
    principal: Principal | None = None,
) -> dict[str, Any]:
    """A table record (spec §9.3): location, snapshot, totals, offsets, maintenance history.

    `live` is what the committer member last reported about the table on its heartbeat, such as
    files waiting for a commit; it is None until the runtime reports.
    """
    live = None if status is None else status.runtime_progress.table(record.name)
    return {
        "materialization_id": materialization.materialization_id,
        **record.to_dict(),
        "identifier": record.qualified_name,
        "live": None if live is None else dict(live),
        **table_access(record, principal),
    }


def build_materialization_schema(
    *,
    materialization: Materialization,
    table_spec: TableSpec,
    schema: MaterializationSchema | None,
    status: MaterializationStatus | None = None,
    raw: bool = False,
) -> dict[str, Any]:
    """A table's schema projection (spec §9.3): one entry per field, with its logical name and state.

    With a status, `unmapped` says how many of the table's values went to `_unmapped` and when one
    last did. `raw` adds the Iceberg schema and the fork map as the table stores them.
    """
    payload: dict[str, Any] = {
        "materialization_id": materialization.materialization_id,
        "table": table_spec.name,
        "dataset": table_spec.dataset,
        "dataset_id": table_spec.dataset_id,
        "source_topic": table_spec.source_topic,
        "schema_id": None,
        "since": None,
        "metadata_location": None,
        "field_count": 0,
        "fields": [],
        "changes": [],
    }
    if schema is not None:
        payload.update(
            {
                "schema_id": schema.schema_id,
                "since": schema.since,
                "metadata_location": schema.metadata_location,
                "field_count": schema.field_count,
                "fields": [dict(item) for item in schema.fields],
                "changes": [dict(item) for item in schema.changes],
            }
        )
        if raw:
            payload["iceberg_schema"] = dict(schema.iceberg_schema)
            payload["fork_map"] = dict(schema.fork_map)
            # What the live tail conforms its rows with, as the runtime's seal does (handbook 19).
            payload["last_column_id"] = schema.last_column_id
            payload["rejected_paths"] = [list(path) for path in schema.rejected_paths]
    if status is not None:
        progress = status.runtime_progress
        payload["unmapped"] = {
            "total_values": progress.cumulative_unmapped_by_table.get(table_spec.name, 0),
            "last_seen_at": progress.unmapped_last_seen_by_table.get(table_spec.name),
        }
    return payload


def build_materialization_schema_versions(
    *,
    materialization: Materialization,
    table_spec: TableSpec,
    schemas: list[MaterializationSchema],
    current_schema_id: int | None,
) -> dict[str, Any]:
    """The archived schema index of one of a materialization's tables, newest first."""
    return {
        "materialization_id": materialization.materialization_id,
        "table": table_spec.name,
        "dataset": table_spec.dataset,
        "dataset_id": table_spec.dataset_id,
        "source_topic": table_spec.source_topic,
        "current_schema_id": current_schema_id,
        "versions": [
            _describe_schema_version(schema, current_schema_id=current_schema_id)
            for schema in schemas
        ],
        "returned": len(schemas),
    }


def _describe_schema_version(
    schema: MaterializationSchema,
    *,
    current_schema_id: int | None,
) -> dict[str, Any]:
    change_counts: dict[str, int] = {}
    for change in schema.changes:
        kind = change.get("kind")
        if isinstance(kind, str):
            change_counts[kind] = change_counts.get(kind, 0) + 1
    return {
        "schema_id": schema.schema_id,
        "since": schema.since,
        "field_count": schema.field_count,
        "change_counts": change_counts,
        "current": schema.schema_id == current_schema_id,
    }


def describe_autoscale(materialization: Materialization, autoscale: RuntimeAutoscale | None) -> dict[str, Any]:
    """The detail's account of the runtime's autoscaling, against the materialization's own count."""
    scaled = (
        autoscale is not None
        and autoscale.replicas is not None
        and materialization.autoscaling
        and autoscale.replicas > materialization.replicas
    )
    return {
        "enabled": materialization.autoscaling,
        "own_replicas": materialization.replicas,
        "replicas": autoscale.replicas if scaled else None,
        "reason": autoscale.reason if scaled else None,
        "changed_at": None if autoscale is None else autoscale.changed_at,
        "eta_seconds": None if autoscale is None else autoscale.eta_seconds,
        "lag": None if autoscale is None or not autoscale.points else autoscale.points[-1].lag,
        "measured_at": None if autoscale is None or not autoscale.points else autoscale.points[-1].at,
    }
