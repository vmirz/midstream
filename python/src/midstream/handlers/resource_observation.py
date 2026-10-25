"""Thin Kaspr-facing resource observation handler."""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
import logging

from midstream.config import ControlPlaneConfig
from midstream.kubernetes import KasprAppClient
from midstream.models import (
    EventCause,
    MaterializationStatus,
    MaterializationWorkEvent,
    ObservationPolicy,
    TargetRuntime,
)
from midstream.observation.reader import KasprAppObservationInput
from midstream.services.observation import (
    apply_resource_observation,
    build_deleted_runtime_cleanup_output,
    build_observation_follow_up_output,
    build_sweep_observation_outputs,
)
from midstream.protocol_logging import elapsed_ms, log_protocol_event, monotonic_ms
from midstream.settings import resolve_config
from midstream.storage.table import TableProtocol
from midstream.utils.background_loop import call_adapter

LOGGER = logging.getLogger(__name__)


def build_sweep_observation_trigger(
    *,
    requested_at: str,
    task_id: str = "sweep-observations-task",
) -> dict:
    """Build the synthetic work event used by sweep-observations tasks."""
    return MaterializationWorkEvent(
        work_id=f"work-sweep-{task_id}-{requested_at}",
        materialization_id="__system__",
        step="sweep_observations",
        requested_at=requested_at,
        caused_by=EventCause(kind="scheduler", id=task_id),
        dedupe_key=f"sweep:{task_id}:{requested_at}",
    ).to_dict()


def handle_resource_observation(
    materialization_id: str,
    observation: dict,
    table: TableProtocol,
    *,
    observed_at: str,
    observation_policy: dict | None = None,
) -> MaterializationStatus:
    """Apply a raw KasprApp observation payload to persisted materialization status."""
    return apply_resource_observation(
        table,
        materialization_id,
        KasprAppObservationInput.from_dict(observation),
        observed_at=observed_at,
        observation_policy=ObservationPolicy.from_dict(observation_policy),
    )


def handle_observation_work_for_output(
    value: dict,
    table: TableProtocol,
    kaspr_app_client: KasprAppClient,
    *,
    observed_at: str,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> Iterator[dict]:
    """Execute observation work and yield routed outputs when needed.

    Blocks while the KasprApp is read. The resource-observation agent runs
    `handle_observation_work_for_output_async` instead, which awaits it.
    """
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    if work_event.step == "sweep_observations":
        yield from build_sweep_observation_outputs(table, observed_at=observed_at)
        return
    target_runtime = _observation_target(work_event)
    if target_runtime is None:
        return
    started_ms = monotonic_ms()
    try:
        observation = kaspr_app_client.observe(
            namespace=target_runtime.namespace,
            name=target_runtime.kaspr_app_name,
        )
    except TimeoutError as exc:
        _log_observation_timed_out(work_event, target_runtime, exc, config=cfg, started_ms=started_ms)
        return
    yield from _observation_outputs(
        table,
        work_event,
        target_runtime,
        observation,
        observed_at=observed_at,
        config=cfg,
        started_ms=started_ms,
    )


async def handle_observation_work_for_output_async(
    value: dict,
    table: TableProtocol,
    kaspr_app_client: KasprAppClient,
    *,
    observed_at: str,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> Generator[dict, None, None]:
    """`handle_observation_work_for_output` for the resource-observation agent's async map.

    The KasprApp is read on the Kubernetes client's own loop, under its deadline, so the event loop
    that also serves the API keeps running while the API server answers. The read comes before any
    table access; the status is updated after it, on the loop, from the records as they are then.

    Returns a generator over outputs already computed. Kaspr iterates a generator that a later
    pipeline operation returns, but sends a list as one value and cannot iterate an async
    generator there at all.
    """
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    if work_event.step == "sweep_observations":
        return _generator(build_sweep_observation_outputs(table, observed_at=observed_at))
    target_runtime = _observation_target(work_event)
    if target_runtime is None:
        return _generator(())
    started_ms = monotonic_ms()
    try:
        observation = await call_adapter(
            kaspr_app_client,
            "observe",
            namespace=target_runtime.namespace,
            name=target_runtime.kaspr_app_name,
        )
    except TimeoutError as exc:
        _log_observation_timed_out(work_event, target_runtime, exc, config=cfg, started_ms=started_ms)
        return _generator(())
    return _generator(
        _observation_outputs(
            table,
            work_event,
            target_runtime,
            observation,
            observed_at=observed_at,
            config=cfg,
            started_ms=started_ms,
        )
    )


def _generator(outputs: Iterable[dict]) -> Generator[dict, None, None]:
    # Evaluated now, so any table reads happen before the operation returns.
    computed = list(outputs)
    return (output for output in computed)


def _observation_target(work_event: MaterializationWorkEvent) -> TargetRuntime | None:
    """The runtime an `observe_runtime` event names; None for any other step."""
    if work_event.step != "observe_runtime":
        return None
    target_runtime = work_event.target_runtime
    if target_runtime is None or target_runtime.namespace is None or target_runtime.kaspr_app_name is None:
        raise ValueError("observe_runtime requires target_runtime namespace and kaspr_app_name")
    return target_runtime


def _observation_outputs(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    target_runtime: TargetRuntime,
    observation: KasprAppObservationInput,
    *,
    observed_at: str,
    config: ControlPlaneConfig,
    started_ms: int,
) -> Iterator[dict]:
    updated_status = apply_resource_observation(
        table,
        work_event.materialization_id,
        observation,
        observed_at=observed_at,
        observation_policy=work_event.observation_policy,
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_runtime_observation_completed",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        runtime_app_name=target_runtime.kaspr_app_name,
        observation_mode=(
            None
            if work_event.observation_policy is None
            else work_event.observation_policy.mode
        ),
        observation_attempt=(
            None
            if work_event.observation_policy is None
            else work_event.observation_policy.attempt
        ),
        observed_phase=updated_status.observed_phase,
        phase_reason=updated_status.phase_reason,
        duration_ms=elapsed_ms(started_ms),
    )
    cleanup_output = build_deleted_runtime_cleanup_output(
        updated_status,
        work_event,
        observed_at=observed_at,
    )
    if cleanup_output is not None:
        yield cleanup_output
        return
    follow_up = build_observation_follow_up_output(
        table,
        updated_status,
        work_event,
        observed_at=observed_at,
        deliver_to=config.materialization_work_topic,
        config=config,
    )
    if follow_up is not None:
        yield follow_up


def _log_observation_timed_out(
    work_event: MaterializationWorkEvent,
    target_runtime: TargetRuntime,
    error: TimeoutError,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> None:
    """Report a read the API server did not answer in time.

    Nothing else happens: the status keeps what it had and nothing is scheduled, because the sweep
    observes every runtime in transition or gone stale again within two minutes. Letting the error
    escape would crash the agent instead, and Kaspr would rebuild its clients on restart.
    """
    LOGGER.warning(
        "Observing KasprApp %s/%s for materialization %s timed out; the next sweep observes it again: %s",
        target_runtime.namespace,
        target_runtime.kaspr_app_name,
        work_event.materialization_id,
        error,
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_runtime_observation_timed_out",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        runtime_app_name=target_runtime.kaspr_app_name,
        observation_mode=(
            None
            if work_event.observation_policy is None
            else work_event.observation_policy.mode
        ),
        observation_attempt=(
            None
            if work_event.observation_policy is None
            else work_event.observation_policy.attempt
        ),
        error=str(error),
        duration_ms=elapsed_ms(started_ms),
    )
