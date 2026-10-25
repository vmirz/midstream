"""Observed-phase mapping from runtime and resource inputs."""

from __future__ import annotations

from dataclasses import replace

from midstream.models import MaterializationStatus, ObservationPolicy, ResourceObservation, RuntimeHealth
from midstream.observation.policy import heartbeat_is_stale
from midstream.observation.reader import KasprAppObservationInput
from midstream.observation.restarts import (
    RESTARTING_HEALTH_REASON,
    RESTARTING_PHASE_REASON,
    restart_instability,
)


def normalize_resource_observation(
    observation: KasprAppObservationInput,
    *,
    managed_agent_names: list[str] | None = None,
) -> ResourceObservation:
    filtered_states = dict(observation.managed_agent_states)
    if managed_agent_names is not None and managed_agent_names:
        filtered_states = {
            name: state
            for name, state in observation.managed_agent_states.items()
            if name in managed_agent_names
        }
    total = len(filtered_states)
    running = sum(1 for state in filtered_states.values() if state == "running")
    paused = sum(1 for state in filtered_states.values() if state == "paused")
    unknown = total - running - paused
    return ResourceObservation(
        namespace=observation.namespace,
        kaspr_app_name=observation.kaspr_app_name,
        resource_exists=observation.resource_exists,
        managed_agent_states=filtered_states,
        metadata_generation=observation.metadata_generation,
        observed_generation=observation.observed_generation,
        ready_condition=observation.ready_condition,
        progressing_condition=observation.progressing_condition,
        rollout_in_progress=observation.rollout_in_progress,
        available_members=observation.available_members,
        rebalance_required=observation.rebalance_required,
        rebalancing_members=observation.rebalancing_members,
        managed_agents_total=total,
        managed_agents_running=running,
        managed_agents_paused=paused,
        managed_agents_unknown=unknown,
        members=observation.members,
    )


def derive_status_from_resource_observation(
    current_status: MaterializationStatus,
    resource_observation: ResourceObservation,
    *,
    observed_at: str,
    observation_policy: ObservationPolicy | None = None,
) -> MaterializationStatus:
    desired_phase = current_status.desired_phase
    ready_is_true = resource_observation.ready_condition is not None and resource_observation.ready_condition.status == "True"
    has_recent_runtime = not heartbeat_is_stale(current_status.last_heartbeat, now=observed_at)
    has_recent_healthy_runtime = (
        current_status.runtime_health.level == "healthy"
        and has_recent_runtime
    )
    all_paused = (
        resource_observation.managed_agents_total is not None
        and resource_observation.managed_agents_total > 0
        and resource_observation.managed_agents_total == resource_observation.managed_agents_paused
    )

    if not resource_observation.resource_exists:
        if desired_phase == "deleting":
            return _replace_status(
                current_status,
                observed_phase="deleted",
                phase_reason="resource_missing_after_delete",
                phase_message="KasprApp resource is absent during delete flow.",
                observed_at=observed_at,
                resource_observation=resource_observation,
                observation_source="resource_observation",
            )
        return _replace_status(
            current_status,
            observed_phase="degraded",
            phase_reason="resource_not_ready",
            phase_message="KasprApp resource is missing unexpectedly.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="resource_observation",
            runtime_health=RuntimeHealth(level="unreachable"),
        )

    if (
        resource_observation.metadata_generation is not None
        and (
            resource_observation.observed_generation is None
            or resource_observation.observed_generation < resource_observation.metadata_generation
        )
    ):
        return _replace_status(
            current_status,
            observed_phase="deploying",
            phase_reason="resource_generation_not_observed",
            phase_message="KasprApp generation has not been observed yet.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
        )

    # A stop scales the runtime to zero. The KasprApp then reports itself ready with no members,
    # so the stop is confirmed by the member count, not by readiness or heartbeats: a heartbeat
    # sent just before the last member exited would otherwise keep it looking alive.
    if desired_phase == "stopped":
        available = available_member_count(resource_observation.available_members)
        if available == 0:
            return _replace_status(
                current_status,
                observed_phase="stopped",
                phase_reason="runtime_scaled_to_zero",
                phase_message="Runtime is stopped: its KasprApp is scaled to zero replicas.",
                observed_at=observed_at,
                resource_observation=resource_observation,
                observation_source="hybrid",
                runtime_health=RuntimeHealth(level="unknown"),
            )
        if _observation_timeout_reached(observation_policy):
            return _replace_status(
                current_status,
                observed_phase="degraded",
                phase_reason="runtime_scale_down_stalled",
                phase_message=(
                    "Runtime members are still running after the stop was applied "
                    f"({resource_observation.available_members or 'unknown'} available)."
                ),
                observed_at=observed_at,
                resource_observation=resource_observation,
                observation_source="hybrid",
                runtime_health=RuntimeHealth(level="degraded", reason="runtime_scale_down_stalled"),
            )
        return _replace_status(
            current_status,
            observed_phase="deploying",
            phase_reason="waiting_for_runtime_scale_down",
            phase_message="Stopping: waiting for runtime members to shut down.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
        )

    # Members killed and restarted every few minutes report healthy between the kills, and the
    # KasprApp is ready whenever all of them happen to be up: the pods are what show it.
    instability = restart_instability(resource_observation.members, now=observed_at)
    if instability is not None and desired_phase == "running":
        return _replace_status(
            current_status,
            observed_phase="degraded",
            phase_reason=RESTARTING_PHASE_REASON,
            phase_message=instability.message,
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
            runtime_health=RuntimeHealth(level="degraded", reason=RESTARTING_HEALTH_REASON, message=instability.message),
        )

    if resource_observation.rollout_in_progress is True:
        return _replace_status(
            current_status,
            observed_phase="deploying",
            phase_reason="resource_rollout_in_progress",
            phase_message="KasprApp rollout is still in progress.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
        )

    if ready_is_true and all_paused:
        return _replace_status(
            current_status,
            observed_phase="paused",
            phase_reason="all_managed_agents_paused",
            phase_message="All managed runtime agents are paused.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
        )

    if ready_is_true and has_recent_runtime and current_status.observed_phase in {"running", "degraded", "paused"}:
        return _replace_status(
            current_status,
            observed_phase=current_status.observed_phase,
            phase_reason=current_status.phase_reason or "runtime_ready",
            phase_message=current_status.phase_message or "Recent runtime heartbeat remains authoritative.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
            runtime_health=current_status.runtime_health,
        )

    if ready_is_true and has_recent_healthy_runtime:
        return _replace_status(
            current_status,
            observed_phase="running",
            phase_reason="runtime_ready",
            phase_message="KasprApp is ready and healthy runtime heartbeat is recent.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
            runtime_health=RuntimeHealth(level="healthy"),
        )

    if ready_is_true and not has_recent_healthy_runtime:
        if _observation_timeout_reached(observation_policy):
            return _replace_status(
                current_status,
                observed_phase="degraded",
                phase_reason="resource_ready_but_runtime_silent",
                phase_message="KasprApp is ready but runtime has not emitted a recent healthy heartbeat.",
                observed_at=observed_at,
                resource_observation=resource_observation,
                observation_source="hybrid",
                runtime_health=RuntimeHealth(level="unreachable"),
            )
        return _replace_status(
            current_status,
            observed_phase="deploying",
            phase_reason="waiting_for_runtime_ready",
            phase_message="KasprApp is ready but runtime heartbeat is still pending.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
        )

    if _has_mixed_or_unknown_states(resource_observation):
        if _observation_timeout_reached(observation_policy):
            return _replace_status(
                current_status,
                observed_phase="degraded",
                phase_reason="mixed_agent_states",
                phase_message="Managed runtime agents are still mixed or unknown after observation retries.",
                observed_at=observed_at,
                resource_observation=resource_observation,
                observation_source="hybrid",
                runtime_health=RuntimeHealth(level="degraded"),
            )
        return _replace_status(
            current_status,
            observed_phase="deploying",
            phase_reason="waiting_for_runtime_agents",
            phase_message="Managed runtime agents are still converging.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
        )

    if _observation_timeout_reached(observation_policy):
        return _replace_status(
            current_status,
            observed_phase="degraded",
            phase_reason="resource_not_ready",
            phase_message="KasprApp ready condition is not True after observation retries.",
            observed_at=observed_at,
            resource_observation=resource_observation,
            observation_source="hybrid",
            runtime_health=RuntimeHealth(level="degraded"),
        )
    return _replace_status(
        current_status,
        observed_phase="deploying",
        phase_reason="waiting_for_runtime_ready",
        phase_message="KasprApp ready condition is not True yet.",
        observed_at=observed_at,
        resource_observation=resource_observation,
        observation_source="hybrid",
    )


def available_member_count(available_members: str | None) -> int | None:
    """Members the KasprApp reports available, from its `available/desired` status string.

    Older operators report a bare count. Anything else is unknown, which is not zero.
    """
    if available_members is None:
        return None
    available = available_members.split("/", 1)[0].strip()
    return int(available) if available.isdigit() else None


def _replace_status(
    current_status: MaterializationStatus,
    *,
    observed_phase: str,
    phase_reason: str,
    phase_message: str,
    observed_at: str,
    resource_observation: ResourceObservation,
    observation_source: str,
    runtime_health: RuntimeHealth | None = None,
) -> MaterializationStatus:
    transition_time = current_status.last_transition_time
    if current_status.observed_phase != observed_phase:
        transition_time = observed_at
    return replace(
        current_status,
        observed_phase=observed_phase,
        phase_reason=phase_reason,
        phase_message=phase_message,
        last_transition_time=transition_time,
        last_resource_observation_time=observed_at,
        resource_observation=resource_observation,
        observation_source=observation_source,
        runtime_health=current_status.runtime_health if runtime_health is None else runtime_health,
    )


def _observation_timeout_reached(observation_policy: ObservationPolicy | None) -> bool:
    if observation_policy is None:
        return False
    if observation_policy.mode != "one_shot":
        return False
    if observation_policy.attempt is None or observation_policy.max_attempts is None:
        return False
    return observation_policy.attempt >= observation_policy.max_attempts


def _has_mixed_or_unknown_states(resource_observation: ResourceObservation) -> bool:
    states = set(resource_observation.managed_agent_states.values())
    if "unknown" in states:
        return True
    return "running" in states and "paused" in states