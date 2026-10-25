"""Thin Kaspr-facing workflow apply handler."""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from datetime import UTC, datetime

from midstream.config import ControlPlaneConfig
from midstream.kafka import KafkaAdminClient
from midstream.models import EventCause, MaterializationWorkEvent
from midstream.observation import DEFAULT_OBSERVATION_BACKOFF_SECONDS, build_one_shot_observation_policy
from midstream.kubernetes import KasprAppClient
from midstream.runtime_app.config import RuntimeAppConfig
from midstream.scheduler import (
    build_direct_work_output,
    build_scheduled_work_output,
)
from midstream.services import (
    DiscoveryService,
    MissingWorkflowStateError,
    process_work_event,
    process_work_event_async,
)
from midstream.services.workflow import FollowUp, follow_ups, scheduled_request_id
from midstream.services.settings import runtime_settings_output_for_apply
from midstream.settings import effective_runtime_config, resolve_config
from midstream.storage.objects import ObjectStoreClientProtocol
from midstream.storage.table import TableProtocol

MAX_MISSING_STATE_RETRIES = len(DEFAULT_OBSERVATION_BACKOFF_SECONDS)

#: Steps that act with the runtimes' settings: discovery evolves schema 0 under their schema caps,
#: and table creation records their snapshot retention.
RUNTIME_CONFIG_STEPS = frozenset({"start_discovery", "discover_tables", "create_table"})


def handle_work_event(
    value: dict,
    table: TableProtocol,
    discovery_service: DiscoveryService,
    *,
    kaspr_app_client: KasprAppClient | None = None,
    kafka_admin_client: KafkaAdminClient | None = None,
    object_store_client: ObjectStoreClientProtocol | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> FollowUp:
    """Deserialize a work message and execute the bounded workflow step."""
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    return process_work_event(
        table,
        work_event,
        discovery_service,
        kaspr_app_client=kaspr_app_client,
        kafka_admin_client=kafka_admin_client,
        object_store_client=object_store_client,
        config=cfg,
        runtime_config=_runtime_config_for(work_event, settings_table, cfg),
    )


def handle_work_event_for_output(
    value: dict,
    table: TableProtocol,
    discovery_service: DiscoveryService,
    *,
    kaspr_app_client: KasprAppClient | None = None,
    kafka_admin_client: KafkaAdminClient | None = None,
    object_store_client: ObjectStoreClientProtocol | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> dict | None:
    """Execute a work event and return a plain follow-up payload for Kaspr maps: the first, when a
    step queues several."""
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    follow_up = process_work_event(
        table,
        work_event,
        discovery_service,
        kaspr_app_client=kaspr_app_client,
        kafka_admin_client=kafka_admin_client,
        object_store_client=object_store_client,
        config=cfg,
        runtime_config=_runtime_config_for(work_event, settings_table, cfg),
    )
    queued = follow_ups(follow_up)
    return queued[0].to_dict() if queued else None


def handle_work_event_for_routed_output(
    value: dict,
    table: TableProtocol,
    discovery_service: DiscoveryService,
    *,
    kaspr_app_client: KasprAppClient | None = None,
    kafka_admin_client: KafkaAdminClient | None = None,
    object_store_client: ObjectStoreClientProtocol | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> Iterator[dict]:
    """Execute a work event and yield routed outputs for Kaspr-native publishing."""
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    try:
        follow_up = process_work_event(
            table,
            work_event,
            discovery_service,
            kaspr_app_client=kaspr_app_client,
            kafka_admin_client=kafka_admin_client,
            object_store_client=object_store_client,
            config=cfg,
            runtime_config=_runtime_config_for(work_event, settings_table, cfg),
        )
    except MissingWorkflowStateError:
        yield from _missing_state_retry_outputs(work_event, config=cfg)
        return
    except ValueError as exc:
        if _is_stale_missing_state_retry(work_event, exc):
            return
        raise
    yield from _routed_outputs(work_event, follow_up, settings_table=settings_table, config=cfg)


async def handle_work_event_for_routed_output_async(
    value: dict,
    table: TableProtocol,
    discovery_service: DiscoveryService,
    *,
    kaspr_app_client: KasprAppClient | None = None,
    kafka_admin_client: KafkaAdminClient | None = None,
    object_store_client: ObjectStoreClientProtocol | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> Generator[dict, None, None]:
    """`handle_work_event_for_routed_output` for the workflow agent's async map.

    Every step that waits on the network awaits it (`process_work_event_async`): discovery and the
    Kubernetes and Kafka admin calls on their adapters' own loops, under deadlines, and destination
    cleanup on a worker thread. The event loop that also serves the API keeps running meanwhile.

    Returns a generator over outputs already computed. Kaspr iterates a generator that a later
    pipeline operation returns, but sends a list as one value and cannot iterate an async
    generator there at all.
    """
    cfg = resolve_config(settings_table) if config is None else config
    work_event = MaterializationWorkEvent.from_dict(value)
    try:
        follow_up = await process_work_event_async(
            table,
            work_event,
            discovery_service,
            kaspr_app_client=kaspr_app_client,
            kafka_admin_client=kafka_admin_client,
            object_store_client=object_store_client,
            config=cfg,
            runtime_config=_runtime_config_for(work_event, settings_table, cfg),
        )
    except MissingWorkflowStateError:
        return _generator(_missing_state_retry_outputs(work_event, config=cfg))
    except ValueError as exc:
        if _is_stale_missing_state_retry(work_event, exc):
            return _generator(())
        raise
    return _generator(
        _routed_outputs(work_event, follow_up, settings_table=settings_table, config=cfg)
    )


def _runtime_config_for(
    work_event: MaterializationWorkEvent,
    settings_table: TableProtocol | None,
    config: ControlPlaneConfig,
) -> RuntimeAppConfig | None:
    if work_event.step not in RUNTIME_CONFIG_STEPS:
        return None
    return effective_runtime_config(settings_table, config)


def _generator(outputs: Iterable[dict]) -> Generator[dict, None, None]:
    # Evaluated now, so any table reads happen before the operation returns.
    computed = list(outputs)
    return (output for output in computed)


def _missing_state_retry_outputs(
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
) -> Iterator[dict]:
    retry_output = _build_missing_state_retry_output(work_event, config=config)
    if retry_output is not None:
        yield retry_output


def _routed_outputs(
    work_event: MaterializationWorkEvent,
    follow_up: FollowUp,
    *,
    settings_table: TableProtocol | None,
    config: ControlPlaneConfig,
) -> Iterator[dict]:
    queued = follow_ups(follow_up)
    if work_event.step == "apply_runtime" and queued:
        # A runtime being (re)deployed may have missed a settings change, or never seen one, so
        # every apply brings it onto the current runtime settings.
        settings_output = runtime_settings_output_for_apply(
            settings_table,
            materialization_id=work_event.materialization_id,
            published_at=work_event.requested_at,
            config=config,
        )
        if settings_output is not None:
            yield settings_output
    for item in queued:
        if item.observation_policy is not None:
            yield build_scheduled_work_output(
                item,
                request_id=scheduled_request_id(item),
                deliver_to=config.materialization_work_topic,
                deliver_at=item.observation_policy.scheduled_for or item.requested_at,
            )
        else:
            yield build_direct_work_output(item)


def _build_missing_state_retry_output(
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
) -> dict | None:
    attempt = 1
    if work_event.observation_policy is not None and work_event.observation_policy.attempt is not None:
        attempt = work_event.observation_policy.attempt + 1
    if attempt > MAX_MISSING_STATE_RETRIES:
        return None

    requested_at = _utc_now_timestamp()
    retry_policy = build_one_shot_observation_policy(attempt=attempt, requested_at=requested_at)
    retry_event = MaterializationWorkEvent(
        work_id=work_event.work_id,
        materialization_id=work_event.materialization_id,
        step=work_event.step,
        requested_at=requested_at,
        caused_by=EventCause(kind="scheduler", id=f"retry-missing-state:{work_event.materialization_id}"),
        dedupe_key=(
            f"{work_event.step}:{work_event.materialization_id}:{work_event.work_id}:"
            f"missing-state:{attempt}"
        ),
        target_runtime=work_event.target_runtime,
        observation_policy=retry_policy,
    )
    return build_scheduled_work_output(
        retry_event,
        request_id=f"missing-state:{work_event.materialization_id}:{work_event.step}",
        deliver_to=config.materialization_work_topic,
        deliver_at=retry_policy.scheduled_for or requested_at,
    )


def _utc_now_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_stale_missing_state_retry(
    work_event: MaterializationWorkEvent,
    error: ValueError,
) -> bool:
    return (
        work_event.step == "start_discovery"
        and work_event.caused_by.kind == "scheduler"
        and work_event.caused_by.id.startswith("retry-missing-state:")
        and str(error).startswith("start_discovery requires observed_phase requested, got ")
    )