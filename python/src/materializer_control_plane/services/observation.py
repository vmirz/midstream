"""Storage-backed observation update services."""

from __future__ import annotations

from dataclasses import replace
import logging

from materializer_control_plane.config import ControlPlaneConfig, get_config
from materializer_control_plane.models import (
    EventCause,
    Materialization,
    MaterializationStatus,
    MaterializationWorkEvent,
    ObservationPolicy,
    RuntimeHealth,
    RuntimeProgress,
    RuntimeStatusEvent,
    TargetRuntime,
)
from materializer_control_plane.runtime import (
    build_dedicated_runtime_template_input,
    render_dedicated_runtime_resources,
    spec_hash,
)
from materializer_control_plane.runtime.naming import resolve_runtime_app_name
from materializer_control_plane.observation import (
    build_one_shot_observation_policy,
    derive_status_from_resource_observation,
    desired_observed_mismatch_is_stale,
    heartbeat_is_stale,
    merge_member_progress,
    normalize_resource_observation,
    resource_observation_is_stale,
    summarize_member_states,
)
from materializer_control_plane.runtime.naming import runtime_component_names
from materializer_control_plane.protocol_logging import log_protocol_event
from materializer_control_plane.observation.reader import KasprAppObservationInput
from materializer_control_plane.observation.restarts import (
    RESTARTING_HEALTH_REASON,
    RESTARTING_PHASE_REASON,
    restart_instability,
)
from materializer_control_plane.scheduler import (
    build_direct_work_output,
    build_scheduled_work_output,
)
from materializer_control_plane.services.autoscaling import scaled_materialization
from materializer_control_plane.services.table_events import degrade_for_table_error, table_error_message
from materializer_control_plane.storage import (
    get_materialization,
    get_materialization_status,
    list_materialization_tables,
    get_runtime_autoscale,
    get_runtime_binding,
    save_materialization_status,
)
from materializer_control_plane.storage.keys import materialization_prefix
from materializer_control_plane.storage.materializations import get_materialization
from materializer_control_plane.models.table import TableError
from materializer_control_plane.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

# Lifecycle phases past which a runtime's own reports are no longer meaningful.
_DELETION_PHASES = frozenset({"deleting", "deleted"})


def apply_runtime_status_event(
    table: TableProtocol,
    event: RuntimeStatusEvent,
) -> MaterializationStatus | None:
    """Merge a runtime-emitted status event into persisted materialization status.

    A status from the runtime of a materialization the control plane does not hold - purged while
    its runtime still reported, or one an incompatible earlier release left behind - is dropped,
    and None returned. Raising would stop the agent on that event, and with it every runtime's
    status behind it in the partition.
    """
    materialization = get_materialization(table, event.materialization_id)
    if materialization is None:
        _report_unknown_runtime(event)
        return None
    current_status = _get_or_build_status(table, materialization)

    # A materialization on its way out does not come back because a status event arrived.
    #
    # Runtime status is emitted continuously and can be delivered late, retried, or replayed after
    # a restart. Once deletion is requested the runtime is being torn down, so any status still in
    # flight describes a world that no longer exists — applying it flips `observed_phase` back to
    # `running` and the materialization reappears as alive in the console.
    if materialization.desired_phase in _DELETION_PHASES:
        LOGGER.debug(
            "ignoring runtime status for %s: materialization is %s",
            event.materialization_id,
            materialization.desired_phase,
        )
        return current_status

    observed_phase = current_status.observed_phase
    phase_reason = current_status.phase_reason
    phase_message = current_status.phase_message
    runtime_progress = merge_member_progress(
        current_status.runtime_progress,
        member_id=event.member_id,
        rows_written=event.rows_written,
        bytes_written=event.bytes_written,
        files_written=event.files_written,
        records_skipped=event.records_skipped,
        latest_offsets=event.latest_offsets,
        reported_at=event.reported_at,
        health="degraded" if event.phase == "degraded" else event.health,
        health_reason=event.health_reason,
        phase_reason=event.phase_reason,
        phase_message=event.phase_message,
        last_error=event.last_error,
        skip_reasons=event.skip_reasons,
        settings_revision=event.settings_revision,
        settings_lineage=event.settings_lineage,
        unmapped_values=event.unmapped_values,
        files_pending_commit=event.files_pending_commit,
        tables=event.tables,
        table_unmapped={
            name: counters["unmapped_values"]
            for name, counters in event.table_counters.items()
            if "unmapped_values" in counters
        },
    )
    # A stopping runtime keeps reporting until its last member exits, and those reports are true
    # about the rows it wrote — but not about the lifecycle: the stop is confirmed by observing the
    # KasprApp at zero members, and a final heartbeat must not flip the phase back to running. So
    # too for a runtime held at zero while its topics change: the drain decides when it runs again.
    binding = get_runtime_binding(table, event.materialization_id)
    if materialization.desired_phase == "stopped" or (binding is not None and binding.draining):
        updated_status = replace(
            current_status,
            last_heartbeat=event.reported_at,
            runtime_version=event.runtime_version or current_status.runtime_version,
            runtime_progress=runtime_progress,
        )
        save_materialization_status(table, updated_status)
        return updated_status

    # Status describes every recently reporting member, not just the one that spoke last.
    members = summarize_member_states(
        runtime_progress.member_progress,
        reference_time=event.reported_at,
    )
    health = event.health if members is None else members.health
    if event.phase == "degraded" and members is None:
        health = "degraded"
    health_reason = event.health_reason if members is None else members.health_reason
    reported_reason = event.phase_reason if members is None else members.phase_reason
    reported_message = event.phase_message if members is None else members.phase_message
    last_error = event.last_error if members is None else members.last_error
    runtime_health = RuntimeHealth(
        level=health,
        reason=health_reason if health == "degraded" else None,
        message=last_error,
    )
    # A member that is up reports healthy, however often it is killed: while the pods the last
    # observation read show members restarting, a heartbeat cannot make the runtime healthy. The
    # restarts age out of their window by the heartbeat's own time, observed again or not.
    instability = (
        None
        if health == "degraded"
        else restart_instability(current_status.resource_observation.members, now=event.reported_at)
    )

    if health == "degraded":
        observed_phase = "degraded"
        phase_reason = _runtime_degraded_phase_reason(health_reason)
        phase_message = (
            reported_message
            if members is not None and members.member_count > 1
            else _runtime_degraded_phase_message(last_error, health_reason)
        )
    elif instability is not None:
        observed_phase = "degraded"
        phase_reason = RESTARTING_PHASE_REASON
        phase_message = instability.message
        runtime_health = RuntimeHealth(level="degraded", reason=RESTARTING_HEALTH_REASON, message=instability.message)
    elif reported_reason == "waiting_for_table":
        observed_phase = "deploying"
        phase_reason = reported_reason
        phase_message = reported_message or "The runtime is loading its Iceberg tables."
    elif (
        current_status.resource_observation.ready_condition is not None
        and current_status.resource_observation.ready_condition.status == "True"
        and health == "healthy"
    ):
        observed_phase = "running"
        phase_reason = reported_reason or "runtime_ready"
        phase_message = reported_message or "Healthy runtime status aligns with ready resource observation."
    elif (
        current_status.resource_observation.ready_condition is not None
        and current_status.resource_observation.ready_condition.status == "True"
    ):
        observed_phase = "deploying"
        phase_reason = reported_reason or "waiting_for_runtime_ready"
        phase_message = reported_message or "KasprApp is ready but runtime is still warming up."

    updated_status = replace(
        current_status,
        observed_phase=observed_phase,
        phase_reason=phase_reason,
        phase_message=phase_message,
        last_transition_time=(
            event.reported_at if current_status.observed_phase != observed_phase else current_status.last_transition_time
        ),
        last_heartbeat=event.reported_at,
        observation_source=(
            "runtime_event"
            if current_status.last_resource_observation_time is None
            else "hybrid"
        ),
        runtime_health=runtime_health,
        # Keep the last version a runtime reported rather than clearing it when an event omits
        # one: an older runtime that predates this field should not blank a known value.
        runtime_version=event.runtime_version or current_status.runtime_version,
        runtime_progress=runtime_progress,
        last_error=last_error,
    )
    table_error = first_table_error(list_materialization_tables(table, event.materialization_id))
    if table_error is not None and updated_status.observed_phase != "degraded":
        # A table error holds until the table commits again (spec §9.2), whatever members report.
        updated_status = degrade_for_table_error(
            updated_status,
            reason=table_error.reason,
            message=table_error.message,
            at=event.reported_at,
        )
    save_materialization_status(table, updated_status)
    return updated_status


def first_table_error(records) -> TableError | None:
    """The open error of the first table that has one, by name - what holds the status degraded -
    worded as the status says it."""
    for record in sorted(records, key=lambda item: item.name):
        if record.error_holds:
            return TableError(
                reason=record.last_error.reason,
                message=table_error_message(record.name, record.last_error.message),
                occurred_at=record.last_error.occurred_at,
            )
    return None


#: Degradations a runtime reports that are their own phase reason.
RUNTIME_DEGRADED_REASONS = frozenset({"commits_stalled"})


def _runtime_degraded_phase_reason(health_reason: str | None) -> str:
    if health_reason == "data_validation":
        return "runtime_data_validation_degraded"
    if health_reason == "infrastructure":
        return "runtime_infrastructure_degraded"
    if health_reason in RUNTIME_DEGRADED_REASONS:
        return health_reason
    return "runtime_reported_degraded"


def _runtime_degraded_phase_message(last_error: str | None, health_reason: str | None) -> str:
    if last_error:
        return last_error
    if health_reason == "commits_stalled":
        return "Uploaded files are not being committed to the table."
    if health_reason == "data_validation":
        return "Runtime reported data validation issues."
    if health_reason == "infrastructure":
        return "Runtime reported infrastructure degradation."
    return "Runtime reported degraded health."


def apply_resource_observation(
    table: TableProtocol,
    materialization_id: str,
    observation: KasprAppObservationInput,
    *,
    observed_at: str,
    observation_policy: ObservationPolicy | None = None,
) -> MaterializationStatus:
    """Merge a KasprApp resource observation into persisted materialization status."""
    materialization = _require_materialization(table, materialization_id)
    current_status = _get_or_build_status(table, materialization)
    binding = get_runtime_binding(table, materialization_id)
    managed_agent_names = None if binding is None else list(binding.managed_agent_names)
    normalized = normalize_resource_observation(
        observation,
        managed_agent_names=managed_agent_names,
    )
    updated_status = derive_status_from_resource_observation(
        current_status,
        normalized,
        observed_at=observed_at,
        observation_policy=observation_policy,
    )
    if binding is not None and binding.draining and materialization.desired_phase not in _DELETION_PHASES:
        # Held at zero until its consumer group empties: a KasprApp ready with no members is the
        # drain under way, not a phase of its own. The drain step says when it is over.
        updated_status = replace(
            updated_status,
            observed_phase=current_status.observed_phase,
            phase_reason=current_status.phase_reason,
            phase_message=current_status.phase_message,
            last_transition_time=current_status.last_transition_time,
            runtime_health=current_status.runtime_health,
        )
    save_materialization_status(table, updated_status)
    return updated_status


def build_deleted_runtime_cleanup_output(
    updated_status: MaterializationStatus,
    work_event: MaterializationWorkEvent,
    *,
    observed_at: str,
) -> dict | None:
    """Emit Kafka cleanup work once delete observation confirms runtime absence."""
    if updated_status.observed_phase != "deleted":
        return None
    target_runtime = work_event.target_runtime
    if target_runtime is None or target_runtime.namespace is None or target_runtime.kaspr_app_name is None:
        return None
    return build_direct_work_output(
        MaterializationWorkEvent(
            work_id=f"work-cleanup-kafka-{work_event.materialization_id}",
            materialization_id=work_event.materialization_id,
            step="cleanup_runtime_kafka",
            requested_at=observed_at,
            caused_by=EventCause(
                kind="resource_observation",
                id=f"runtime-deleted:{work_event.materialization_id}",
            ),
            dedupe_key=f"cleanup_runtime_kafka:{work_event.materialization_id}",
            target_runtime=TargetRuntime(
                namespace=target_runtime.namespace,
                kaspr_app_name=target_runtime.kaspr_app_name,
            ),
        )
    )


def build_observation_follow_up_output(
    table: TableProtocol,
    updated_status: MaterializationStatus,
    work_event: MaterializationWorkEvent,
    *,
    observed_at: str,
    deliver_to: str,
    config: ControlPlaneConfig | None = None,
) -> dict | None:
    """Schedule another observation when a one-shot observation still sees deployment in progress."""
    cfg = get_config() if config is None else config
    policy = work_event.observation_policy
    if updated_status.observed_phase != "deploying":
        return None
    if policy is None or policy.mode != "one_shot" or policy.attempt is None or policy.max_attempts is None:
        return None
    if policy.attempt >= policy.max_attempts:
        return None
    next_policy = build_one_shot_observation_policy(attempt=policy.attempt + 1, requested_at=observed_at)
    next_event = MaterializationWorkEvent(
        work_id=work_event.work_id,
        materialization_id=work_event.materialization_id,
        step="observe_runtime",
        requested_at=observed_at,
        caused_by=EventCause(kind="scheduler", id=f"observe:{work_event.materialization_id}"),
        dedupe_key=f"observe:{work_event.materialization_id}:{policy.attempt + 1}",
        target_runtime=work_event.target_runtime,
        observation_policy=next_policy,
    )
    log_protocol_event(
        LOGGER,
        enabled=cfg.protocol_logging_enabled,
        event="control_plane_runtime_observation_follow_up_scheduled",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        current_phase_reason=updated_status.phase_reason,
        next_attempt=next_policy.attempt,
        scheduled_for=next_policy.scheduled_for,
    )
    return build_scheduled_work_output(
        next_event,
        request_id=f"observe:{work_event.materialization_id}",
        deliver_to=deliver_to,
        deliver_at=next_policy.scheduled_for or observed_at,
    )


def build_sweep_observation_outputs(
    table: TableProtocol,
    *,
    observed_at: str,
) -> list[dict]:
    """Build sweep-triggered observation work for stale or transitional materializations."""
    reapply_outputs: list[dict] = []
    observation_outputs: list[dict] = []
    for key, _ in table.prefix_scan(materialization_prefix()):
        materialization_id = key.removeprefix(materialization_prefix())
        materialization = get_materialization(table, materialization_id)
        if materialization is None:
            continue
        status = get_materialization_status(table, materialization_id)
        binding = get_runtime_binding(table, materialization_id)
        if status is None or binding is None:
            continue
        reapply_output = _build_runtime_reapply_output_if_needed(
            table,
            materialization,
            status,
            binding,
            requested_at=observed_at,
        )
        if reapply_output is not None:
            reapply_outputs.append(reapply_output)
            continue
        if not _should_sweep(status, now=observed_at):
            continue
        observation_outputs.append(
            build_direct_work_output(
                MaterializationWorkEvent(
                    work_id=f"work-observe-{materialization_id}",
                    materialization_id=materialization_id,
                    step="observe_runtime",
                    requested_at=observed_at,
                    caused_by=EventCause(kind="scheduler", id="sweep-observations"),
                    dedupe_key=f"observe:{materialization_id}:sweep",
                    target_runtime=None if binding is None else __import__(
                        "materializer_control_plane.models", fromlist=["TargetRuntime"]
                    ).TargetRuntime(
                        namespace=binding.namespace,
                        kaspr_app_name=binding.kaspr_app_name,
                    ),
                    observation_policy=ObservationPolicy(attempt=1, max_attempts=1, mode="sweep"),
                )
            )
        )
    return [*reapply_outputs, *observation_outputs]


def _build_runtime_reapply_output_if_needed(
    table: TableProtocol,
    materialization: Materialization,
    status: MaterializationStatus,
    binding,
    *,
    requested_at: str,
) -> dict | None:
    if materialization.desired_phase in {"deleting", "deleted"}:
        return None
    if binding.draining:
        # Held at zero on purpose until its consumer group empties; the drain applies it again.
        return None
    table_records = list_materialization_tables(table, materialization.materialization_id)
    if not set(materialization.table_names) <= {record.name for record in table_records}:
        # A table never made it: apply_runtime would refuse it anyway.
        return None
    # Rendered as the apply renders it, at the count autoscaling gave it: otherwise every scale-up
    # would read as drift, and be undone.
    effective_materialization = scaled_materialization(
        materialization, get_runtime_autoscale(table, materialization.materialization_id)
    )
    desired_resources = render_dedicated_runtime_resources(
        build_dedicated_runtime_template_input(
            effective_materialization,
            table_records=table_records,
        )
    )
    desired_hash = spec_hash(desired_resources)
    if desired_hash == binding.last_applied_spec_hash:
        return None

    runtime_name = resolve_runtime_app_name(effective_materialization)
    return build_direct_work_output(
        MaterializationWorkEvent(
            work_id=f"work-apply-reconcile-{materialization.materialization_id}-{desired_hash[:12]}",
            materialization_id=materialization.materialization_id,
            step="apply_runtime",
            requested_at=requested_at,
            caused_by=EventCause(kind="scheduler", id="reconcile-runtime-drift"),
            dedupe_key=f"apply_runtime:{materialization.materialization_id}:reconcile:{desired_hash}",
            target_runtime=TargetRuntime(
                namespace=materialization.namespace,
                kaspr_app_name=runtime_name,
            ),
        )
    )


def _should_sweep(status: MaterializationStatus, *, now: str) -> bool:
    if status.observed_phase in {"deploying", "deleting"}:
        return True
    if resource_observation_is_stale(status.last_resource_observation_time, now=now):
        return True
    if desired_observed_mismatch_is_stale(
        desired_phase=status.desired_phase,
        observed_phase=status.observed_phase,
        last_transition_time=status.last_transition_time,
        now=now,
    ):
        return True
    return heartbeat_is_stale(status.last_heartbeat, now=now) and status.observed_phase in {"running", "degraded", "paused"}


#: The materializations a dropped runtime status was about, so each is logged once.
_UNKNOWN_RUNTIMES: set[str] = set()
_UNKNOWN_RUNTIMES_LIMIT = 1000


def _report_unknown_runtime(event: RuntimeStatusEvent) -> None:
    if event.materialization_id in _UNKNOWN_RUNTIMES or len(_UNKNOWN_RUNTIMES) >= _UNKNOWN_RUNTIMES_LIMIT:
        return
    _UNKNOWN_RUNTIMES.add(event.materialization_id)
    LOGGER.warning(
        "Ignoring runtime status from %s: materialization %s is not one this control plane holds",
        event.runtime_app_ref,
        event.materialization_id,
    )


def _require_materialization(table: TableProtocol, materialization_id: str) -> Materialization:
    materialization = get_materialization(table, materialization_id)
    if materialization is None:
        raise ValueError(f"materialization {materialization_id} is missing")
    return materialization


def _get_or_build_status(table: TableProtocol, materialization: Materialization) -> MaterializationStatus:
    status = get_materialization_status(table, materialization.materialization_id)
    if status is not None:
        return status
    return MaterializationStatus(
        materialization_id=materialization.materialization_id,
        desired_phase=materialization.desired_phase,
        observed_phase=materialization.desired_phase,
        phase_reason=materialization.desired_phase,
        last_transition_time=materialization.updated_at,
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="unknown"),
        resource_observation=normalize_resource_observation(
            KasprAppObservationInput(
                namespace=materialization.namespace,
                kaspr_app_name=resolve_runtime_app_name(materialization),
                resource_exists=False,
                managed_agent_states={},
            )
        ),
    )