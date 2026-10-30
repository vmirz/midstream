"""Bounded workflow execution for control-plane work events."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from functools import partial
import logging
from typing import Any, Protocol, TypeVar

from pyiceberg.exceptions import TableAlreadyExistsError

from midstream.config import ControlPlaneConfig, get_config
from midstream.kafka import EMPTY_GROUP_STATES, KafkaAdminClient
from midstream.kafka.admin import ConsumerGroupNotEmptyError
from midstream.kafka.metadata import KafkaMetadataClient, get_metadata_client
from midstream.lakehouse.lifecycle import CreatedTable, create_materializer_table, schema_from_json
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.index import build_index
from midstream.lakehouse.schema.infer import infer
from midstream.lakehouse.schema.policy import evolve, limits_of
from midstream.lakehouse.tables import TableIdentity, build_schema, table_identity, table_properties
from midstream.protection.plan import SHAPE_ONLY_SECRET, ProtectionPlan, initial_protection
from midstream.protection.policy import NONE as NO_PROTECTION, ProtectionPolicy, TableProtection
from midstream.protection.secrets import get_protection_key
from midstream.models import (
    EventCause,
    TableSpec,
    TargetRuntime,
    Materialization,
    MaterializationSchema,
    MaterializationStatus,
    MaterializationTable,
    MaterializationWorkEvent,
    ObservationPolicy,
    RuntimeBinding,
    RuntimeHealth,
    TableError,
)
from midstream.observation import (
    DEFAULT_OBSERVATION_BACKOFF_SECONDS,
    build_one_shot_observation_policy,
    heartbeat_is_stale,
)
from midstream.runtime import (
    build_dedicated_runtime_template_input,
    build_runtime_binding,
    iter_rendered_resources,
    render_dedicated_runtime_resources,
    runtime_consumer_group_name,
    resolve_runtime_app_name,
    retired_runtime_components,
    runtime_component_names,
    spec_hash,
)
from midstream.runtime_app.config import RuntimeAppConfig
from midstream.services.autoscaling import scaled_materialization
from midstream.services.schema_projection import annotate_protection, build_schema_projection
from midstream.services.topic_analysis import missing_topics_message
from midstream.services.topic_changes import apply_topic_change, describe_topic_change
from midstream.storage import (
    get_materialization,
    get_materialization_status,
    get_operation_record,
    list_materialization_tables,
    get_runtime_autoscale,
    get_runtime_binding,
    delete_runtime_binding,
    resolve_materialization_schema,
    save_materialization,
    save_materialization_schema,
    save_materialization_status,
    save_materialization_table,
    save_operation_record,
    save_runtime_binding,
    delete_materialization_table,
    delete_table_schemas,
    get_materialization_table,
)
from midstream.storage.objects import (
    DestinationCleanupResult,
    ObjectStoreClientProtocol,
    drop_table_objects_pass,
    drop_table_objects_pass_async,
    retire_table_pointer,
    retire_table_pointer_async,
    table_object_prefixes,
)
from midstream.storage.table import TableProtocol
from midstream.kubernetes import KasprAppClient
from midstream.protocol_logging import elapsed_ms, log_protocol_event, monotonic_ms
from midstream.services.retention import purge_materialization_records
from midstream.utils.background_loop import call_adapter
from midstream.version import PACKAGE_VERSION


class MissingWorkflowStateError(ValueError):
    """Raised when a workflow event references state that is not currently persisted."""


class TableLocationInUseError(RuntimeError):
    """The table's location already holds another materialization's table."""

    phase_reason = "table_location_in_use"


class InvalidTableLocationError(RuntimeError):
    """The materialization's destination cannot hold a table: not a `gs://` bucket and prefix."""

    phase_reason = "invalid_destination"


class TableMissingError(RuntimeError):
    """A materialization with no table record and a table with no discovered schema: a create whose
    discovery failed, or one created before 1.0, which has no table records at all."""

    phase_reason = "table_missing"


class SourceTopicMissingError(RuntimeError):
    """A new materialization's source topic that the cluster's metadata does not list: missing, or
    hidden from the Kafka user. Its runtime would never be assigned the topic."""

    phase_reason = "source_topic_missing"


# One thread: table creation is a handful of small GCS requests, and the workflow agent handles one
# event at a time. A pool of its own keeps a slow bucket off the loop's default executor.
_TABLE_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="table-lifecycle")


LOGGER = logging.getLogger(__name__)
ClientT = TypeVar("ClientT")
MAX_CLEANUP_KAFKA_RETRY_ATTEMPTS = len(DEFAULT_OBSERVATION_BACKOFF_SECONDS)
#: Destination cleanup passes that may fail in a row, each retried after the observation backoff,
#: before the step fails for good. A pass that succeeds starts the count again.
MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS = len(DEFAULT_OBSERVATION_BACKOFF_SECONDS)


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    """What discovery sampled from each source topic: the records each table's schema 0 is evolved from."""

    records_by_topic: dict[str, list] = field(default_factory=dict)
    discovered_at: str = ""
    samples: dict[str, Any] = field(default_factory=dict)

    def records(self, topic: str) -> list:
        return list(self.records_by_topic.get(topic) or [])

    def missing_topics(self) -> list[str]:
        """The topics the cluster's metadata did not list, in the order sampled."""
        return [topic for topic, sample in self.samples.items() if getattr(sample, "missing", False)]


class DiscoveryService(Protocol):
    """Service contract for sampling a materialization's source topics, every one at once.

    A service may also offer `discover_async`, which the workflow agent awaits instead so that its
    event loop keeps running while the source topic is sampled.
    """

    def discover(self, materialization: Materialization, **budget: Any) -> DiscoveryResult:
        ...


# Steps `process_work_event` dispatches, and therefore the exact set the workflow-apply agent must
# accept on its input filter.
#
# The agent filters its input in a YAML predicate, which cannot import this module, so the list is
# duplicated in `charts/midstream/templates/agents/workflow-apply.yaml`. A step missing from that copy is
# published, consumed and dropped in silence — no error, no log, the delete chain simply stops.
# `test_workflow_apply_agent_filter_matches_dispatcher` keeps the two in step.
WORKFLOW_APPLY_STEPS = frozenset(
    {
        "tables_claimed",
        "claim_rejected",
        "start_discovery",
        "discover_tables",
        "create_table",
        "apply_runtime",
        "await_runtime_drained",
        "delete_runtime",
        "cleanup_runtime_kafka",
        "drop_table",
        "purge_materialization_records",
    }
)

#: The phase reason of a new materialization whose tables' names the directory is claiming.
CLAIMING_TABLES = "claiming_tables"

#: What a step queues: nothing, one work event, or several (an applied runtime is observed while the
#: tables it no longer writes are dropped).
FollowUp = MaterializationWorkEvent | tuple[MaterializationWorkEvent, ...] | None


def follow_ups(follow_up: FollowUp) -> tuple[MaterializationWorkEvent, ...]:
    """A step's follow-up as a tuple, however many it queued."""
    if follow_up is None:
        return ()
    if isinstance(follow_up, MaterializationWorkEvent):
        return (follow_up,)
    return tuple(follow_up)


def process_work_event(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_service: DiscoveryService,
    *,
    kaspr_app_client: KasprAppClient | None = None,
    kafka_admin_client: KafkaAdminClient | None = None,
    object_store_client: ObjectStoreClientProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    runtime_config: RuntimeAppConfig | None = None,
    metadata_client: KafkaMetadataClient | None = None,
) -> FollowUp:
    """Dispatch a bounded workflow event to the matching control-plane step."""
    cfg = get_config() if config is None else config
    runtime_cfg = RuntimeAppConfig() if runtime_config is None else runtime_config
    started_ms = monotonic_ms()
    try:
        if work_event.step == "tables_claimed":
            follow_up = _process_tables_claimed(table, work_event, config=cfg)
        elif work_event.step == "claim_rejected":
            _process_claim_rejected(table, work_event, config=cfg)
            follow_up = None
        elif work_event.step == "start_discovery":
            follow_up = _process_start_discovery(
                table,
                work_event,
                discovery_service,
                config=cfg,
                runtime_config=runtime_cfg,
            )
        elif work_event.step == "discover_tables":
            follow_up = _process_discover_tables(
                table,
                work_event,
                discovery_service,
                config=cfg,
                runtime_config=runtime_cfg,
            )
        elif work_event.step == "await_runtime_drained":
            follow_up = _process_await_runtime_drained(
                table,
                work_event,
                _required_client(kafka_admin_client, "kafka_admin_client", work_event.step),
                metadata_client=metadata_client,
                config=cfg,
            )
        elif work_event.step == "create_table":
            follow_up = _process_create_table(
                table,
                work_event,
                object_store_client=object_store_client,
                config=cfg,
                runtime_config=runtime_cfg,
            )
        elif work_event.step == "apply_runtime":
            follow_up = _process_apply_runtime(
                table,
                work_event,
                _required_client(kaspr_app_client, "kaspr_app_client", work_event.step),
                config=cfg,
            )
        elif work_event.step == "delete_runtime":
            follow_up = _process_delete_runtime(
                table,
                work_event,
                _required_client(kaspr_app_client, "kaspr_app_client", work_event.step),
                config=cfg,
            )
        elif work_event.step == "cleanup_runtime_kafka":
            follow_up = _process_cleanup_runtime_kafka(
                table,
                work_event,
                _required_client(kafka_admin_client, "kafka_admin_client", work_event.step),
                config=cfg,
            )
        elif work_event.step == "drop_table":
            follow_up = _process_drop_table(
                table,
                work_event,
                object_store_client=object_store_client,
                config=cfg,
            )
        elif work_event.step == "purge_materialization_records":
            follow_up = _process_purge_materialization_records(
                table,
                work_event,
                config=cfg,
            )
        else:
            follow_up = None
        _log_work_completed(work_event, follow_up, config=cfg, started_ms=started_ms)
        return follow_up
    except Exception as exc:
        if not _record_work_failure(table, work_event, exc, config=cfg, started_ms=started_ms):
            raise
        return None


async def process_work_event_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_service: DiscoveryService,
    *,
    kaspr_app_client: KasprAppClient | None = None,
    kafka_admin_client: KafkaAdminClient | None = None,
    object_store_client: ObjectStoreClientProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    runtime_config: RuntimeAppConfig | None = None,
    metadata_client: KafkaMetadataClient | None = None,
) -> FollowUp:
    """`process_work_event` for the event loop that also serves the API and every other agent.

    No step holds the loop while it waits on the network. Discovery and the Kubernetes and Kafka
    admin calls are awaited on their adapters' own loops, each under a deadline, and table creation
    and the table drop's deletes run on worker threads. Table reads and writes stay on the loop. Other
    agents write while a step waits, so what it writes afterwards it builds on the records as they
    are then. The purge touches only the table and never gives up the loop.
    """
    cfg = get_config() if config is None else config
    runtime_cfg = RuntimeAppConfig() if runtime_config is None else runtime_config
    started_ms = monotonic_ms()
    try:
        if work_event.step == "tables_claimed":
            follow_up = _process_tables_claimed(table, work_event, config=cfg)
        elif work_event.step == "claim_rejected":
            _process_claim_rejected(table, work_event, config=cfg)
            follow_up = None
        elif work_event.step == "start_discovery":
            follow_up = await _process_start_discovery_async(
                table,
                work_event,
                discovery_service,
                config=cfg,
                runtime_config=runtime_cfg,
            )
        elif work_event.step == "discover_tables":
            follow_up = await _process_discover_tables_async(
                table,
                work_event,
                discovery_service,
                config=cfg,
                runtime_config=runtime_cfg,
            )
        elif work_event.step == "await_runtime_drained":
            follow_up = await _process_await_runtime_drained_async(
                table,
                work_event,
                _required_client(kafka_admin_client, "kafka_admin_client", work_event.step),
                metadata_client=metadata_client,
                config=cfg,
            )
        elif work_event.step == "create_table":
            follow_up = await _process_create_table_async(
                table,
                work_event,
                object_store_client=object_store_client,
                config=cfg,
                runtime_config=runtime_cfg,
            )
        elif work_event.step == "apply_runtime":
            follow_up = await _process_apply_runtime_async(
                table,
                work_event,
                _required_client(kaspr_app_client, "kaspr_app_client", work_event.step),
                config=cfg,
            )
        elif work_event.step == "delete_runtime":
            follow_up = await _process_delete_runtime_async(
                table,
                work_event,
                _required_client(kaspr_app_client, "kaspr_app_client", work_event.step),
                config=cfg,
            )
        elif work_event.step == "cleanup_runtime_kafka":
            follow_up = await _process_cleanup_runtime_kafka_async(
                table,
                work_event,
                _required_client(kafka_admin_client, "kafka_admin_client", work_event.step),
                config=cfg,
            )
        elif work_event.step == "drop_table":
            follow_up = await _process_drop_table_async(
                table,
                work_event,
                object_store_client=object_store_client,
                config=cfg,
            )
        elif work_event.step == "purge_materialization_records":
            follow_up = _process_purge_materialization_records(
                table,
                work_event,
                config=cfg,
            )
        else:
            follow_up = None
        _log_work_completed(work_event, follow_up, config=cfg, started_ms=started_ms)
        return follow_up
    except Exception as exc:
        if not _record_work_failure(table, work_event, exc, config=cfg, started_ms=started_ms):
            raise
        return None


def _required_client(client: ClientT | None, name: str, step: str) -> ClientT:
    if client is None:
        raise ValueError(f"{name} is required for {step}")
    return client


def _log_work_completed(
    work_event: MaterializationWorkEvent,
    follow_up: FollowUp,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> None:
    queued = follow_ups(follow_up)
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_work_completed",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        step=work_event.step,
        caused_by_kind=work_event.caused_by.kind,
        caused_by_id=work_event.caused_by.id,
        duration_ms=elapsed_ms(started_ms),
        follow_up_step=",".join(item.step for item in queued) or None,
        follow_up_work_id=",".join(item.work_id for item in queued) or None,
    )


def _record_work_failure(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    error: Exception,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> bool:
    """Log a failed step and persist it. False means the caller must re-raise.

    A `ValueError` is a contract violation and always propagates, as does a failure that cannot be
    recorded because the materialization's state is missing.
    """
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_work_failed",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        step=work_event.step,
        caused_by_kind=work_event.caused_by.kind,
        caused_by_id=work_event.caused_by.id,
        duration_ms=elapsed_ms(started_ms),
        error_type=type(error).__name__,
        error=str(error),
    )
    if isinstance(error, ValueError):
        return False
    return _persist_workflow_failure(table, work_event, error)


def _process_tables_claimed(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """The directory has claimed the names of the materialization's new tables: go on.

    A new materialization goes on to discovery. An update's change of topics applies as it was
    planned - no other change of topics was taken meanwhile - and its new tables go on to discovery.
    An answer to a claim already answered, or one for a materialization being deleted, changes
    nothing: deleting it released every name.
    """
    started_ms = monotonic_ms()
    claim = work_event.claim
    materialization = _require_materialization(table, work_event.materialization_id)
    status = _require_status(table, work_event.materialization_id)
    target_runtime = TargetRuntime(
        namespace=materialization.namespace,
        kaspr_app_name=resolve_runtime_app_name(materialization, config=config),
    )
    if _should_drop_start_discovery(materialization, status):
        _log_step_skipped(work_event, reason="materialization_deleting", config=config, started_ms=started_ms)
        return None
    if claim.purpose == "create":
        if status.observed_phase != "requested" or status.phase_reason != CLAIMING_TABLES:
            _log_step_skipped(work_event, reason="claim_answered", config=config, started_ms=started_ms)
            return None
        save_materialization_status(
            table,
            replace(
                status,
                phase_reason="tables_claimed",
                phase_message="Claimed the table names " + ", ".join(spec.qualified_name for spec in materialization.tables),
                last_transition_time=work_event.requested_at,
            ),
        )
        request_id = work_event.caused_by.id
        return MaterializationWorkEvent(
            work_id=f"work-{request_id}",
            materialization_id=materialization.materialization_id,
            step="start_discovery",
            requested_at=work_event.requested_at,
            caused_by=work_event.caused_by,
            dedupe_key=f"start_discovery:{materialization.materialization_id}:{request_id}",
            target_runtime=target_runtime,
        )
    pending = materialization.pending_topic_change
    if pending is None or pending.claim_request_id != claim.request_id:
        _log_step_skipped(work_event, reason="claim_answered", config=config, started_ms=started_ms)
        return None
    applied = apply_topic_change(
        table,
        materialization,
        pending.plan,
        now=work_event.requested_at,
        spec_version_before=materialization.spec_version,
    )
    save_materialization(table, replace(applied, pending_topic_change=None, updated_at=work_event.requested_at))
    save_materialization_status(table, replace(status, phase_message=describe_topic_change(pending.plan)))
    return MaterializationWorkEvent(
        work_id=f"work-{pending.request_id}",
        materialization_id=materialization.materialization_id,
        step="discover_tables",
        requested_at=work_event.requested_at,
        caused_by=EventCause(kind="request", id=pending.request_id),
        dedupe_key=f"discover_tables:{materialization.materialization_id}:{pending.request_id}",
        target_runtime=target_runtime,
    )


def _process_claim_rejected(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
) -> None:
    """The directory refused the names: nothing that would have had them happens.

    A new materialization is deleted - nothing was created, so there is nothing to drop - with the
    reason kept on its status, for whoever created it to see and try again. An update's change of
    topics is dropped, leaving the materialization as it was, and its status says why.
    """
    started_ms = monotonic_ms()
    claim = work_event.claim
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    materialization, status = state
    reason = claim.reason or "claim_rejected"
    message = claim.message or "The table names could not be claimed."
    if claim.purpose == "create":
        if materialization.desired_phase != "running" or status.observed_phase != "requested":
            _log_step_skipped(work_event, reason="claim_answered", config=config, started_ms=started_ms)
            return None
        save_materialization(table, replace(materialization, desired_phase="deleted", updated_at=work_event.requested_at))
        save_materialization_status(
            table,
            replace(
                status,
                desired_phase="deleted",
                observed_phase="deleted",
                phase_reason=reason,
                phase_message=f"Not created: {message}",
                last_transition_time=work_event.requested_at,
                runtime_health=RuntimeHealth(level="unknown"),
                last_error=message,
            ),
        )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_create_refused",
            materialization_id=materialization.materialization_id,
            work_id=work_event.work_id,
            reason=reason,
            duration_ms=elapsed_ms(started_ms),
        )
        return None
    pending = materialization.pending_topic_change
    if pending is None or pending.claim_request_id != claim.request_id:
        _log_step_skipped(work_event, reason="claim_answered", config=config, started_ms=started_ms)
        return None
    save_materialization(table, replace(materialization, pending_topic_change=None, updated_at=work_event.requested_at))
    save_materialization_status(table, replace(status, phase_message=f"The change of topics was not applied: {message}"))
    return None


def _process_start_discovery(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_service: DiscoveryService,
    *,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
) -> MaterializationWorkEvent | None:
    """Sample the source topics, persist each table's schema 0 evolved from its topic's samples, and
    queue the tables' creation.

    Blocks while discovery samples the topics. The workflow agent runs
    `_process_start_discovery_async` instead, which awaits it.
    """
    started_ms = monotonic_ms()
    materialization = _begin_start_discovery(table, work_event)
    if materialization is None:
        return None
    discovery_result = discovery_service.discover(
        materialization,
        sample_records=config.discovery_sample_records,
        sample_seconds=config.discovery_sample_seconds,
    )
    return _record_start_discovery(
        table,
        work_event,
        discovery_result,
        tables=materialization.tables,
        config=config,
        runtime_config=runtime_config,
        started_ms=started_ms,
    )


async def _process_start_discovery_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_service: DiscoveryService,
    *,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
) -> MaterializationWorkEvent | None:
    """`_process_start_discovery`, awaiting discovery so the event loop runs meanwhile."""
    started_ms = monotonic_ms()
    materialization = _begin_start_discovery(table, work_event)
    if materialization is None:
        return None
    discovery_result = await call_adapter(
        discovery_service,
        "discover",
        materialization,
        sample_records=config.discovery_sample_records,
        sample_seconds=config.discovery_sample_seconds,
    )
    return _record_start_discovery(
        table,
        work_event,
        discovery_result,
        tables=materialization.tables,
        config=config,
        runtime_config=runtime_config,
        started_ms=started_ms,
    )


def _begin_start_discovery(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
) -> Materialization | None:
    """Mark the materialization discovering, or record why it no longer needs discovery (None)."""
    materialization = _require_materialization(table, work_event.materialization_id)
    status = _require_status(table, work_event.materialization_id)
    if _should_drop_start_discovery(materialization, status):
        _save_start_discovery_skipped(table, work_event, materialization, status)
        return None
    _validate_start_discovery(materialization, status, work_event)
    discovering_status = replace(
        status,
        desired_phase=materialization.desired_phase,
        observed_phase="discovering",
        phase_reason="discovering",
        phase_message="Sampling the source topics" if len(materialization.tables) > 1 else "Sampling the source topic",
        last_transition_time=work_event.requested_at,
    )
    save_materialization_status(table, discovering_status)
    return materialization


def _record_start_discovery(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_result: DiscoveryResult,
    *,
    tables,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    """Evolve each table's schema 0 from its topic's samples, persist the projections, and queue the
    tables' creation.

    The samples go through the very policy the runtime's committer applies later, so a topic with
    several historical shapes starts with its forks in place. Discovery waits on Kafka while other
    agents write; a delete in that window drops the discovery, anything else (a stop) is kept.

    A topic the cluster's metadata does not list fails the step (`SourceTopicMissingError`) rather
    than passing for an empty one: its runtime would be assigned none of its partitions, and its
    table would stay empty. The API refuses such a topic first; this catches one gone since.
    """
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    materialization, status = state
    if _should_drop_start_discovery(materialization, status):
        _save_start_discovery_skipped(table, work_event, materialization, status)
        _log_step_skipped(
            work_event,
            reason="materialization_no_longer_requires_discovery",
            config=config,
            started_ms=started_ms,
        )
        return None
    missing = discovery_result.missing_topics()
    if missing:
        raise SourceTopicMissingError(
            f"{missing_topics_message(missing)}; once {'it' if len(missing) == 1 else 'they'} can be read, "
            "delete the materialization and create it again"
        )
    evolutions = _evolve_schema_zero(
        work_event,
        discovery_result,
        tables,
        protection=materialization.protection,
        runtime_config=runtime_config,
        config=config,
        started_ms=started_ms,
    )
    for spec in materialization.tables:
        _save_schema_zero(table, materialization.materialization_id, spec, evolutions[spec.name][0], discovery_result, config=config)

    updated_materialization = replace(materialization, updated_at=discovery_result.discovered_at)
    save_materialization(table, updated_materialization)

    sampled = sum(count for _, count in evolutions.values())
    if len(materialization.tables) == 1:
        message = (
            f"Schema 0 discovered from {sampled} sampled record(s)"
            if sampled
            else "The topic holds no records yet: the table starts with the Kafka metadata columns"
        )
    else:
        empty = [spec.source_topic for spec in materialization.tables if evolutions[spec.name][1] == 0]
        message = f"Schema 0 discovered for {len(materialization.tables)} tables from {sampled} sampled record(s)"
        if empty:
            message += f"; {', '.join(empty)} hold(s) no records yet and start(s) with the Kafka metadata columns"
    discovered_status = replace(
        status,
        desired_phase=materialization.desired_phase,
        observed_phase="discovered",
        phase_reason="schema_discovered",
        phase_message=message,
        last_transition_time=discovery_result.discovered_at,
    )
    save_materialization_status(table, discovered_status)

    return MaterializationWorkEvent(
        work_id=f"work-create-table-{updated_materialization.materialization_id}",
        materialization_id=updated_materialization.materialization_id,
        step="create_table",
        requested_at=discovery_result.discovered_at,
        caused_by=work_event.caused_by,
        dedupe_key=f"create_table:{updated_materialization.materialization_id}:{work_event.work_id}",
        target_runtime=work_event.target_runtime,
    )


def _evolve_schema_zero(
    work_event: MaterializationWorkEvent,
    discovery_result: DiscoveryResult,
    tables,
    *,
    protection: ProtectionPolicy = NO_PROTECTION,
    runtime_config: RuntimeAppConfig,
    config: ControlPlaneConfig,
    started_ms: int,
) -> dict[str, tuple[Any, int]]:
    """Each table's schema 0, evolved from its topic's samples, and how many there were, by table.

    The samples go through the very policy the runtime's committer applies later, so a topic with
    several historical shapes starts with its forks in place. They are masked first, as the writer
    will mask every record (design §5.5): a protected number is a string field, a withheld value no
    field. Only their shapes matter, so they are hashed with a placeholder key, never the real one.
    """
    plan = ProtectionPlan.for_policy(protection, secret=SHAPE_ONLY_SECRET)
    evolutions = {}
    for spec in tables:
        records = [plan.protect(record) for record in discovery_result.records(spec.source_topic)]
        evolution = evolve(
            build_schema(),
            ForkMap(),
            [((), infer(record)) for record in records],
            limits=limits_of(runtime_config),
            schema_id=0,
        )
        evolutions[spec.name] = (evolution, len(records))
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_schema_discovery_completed",
            materialization_id=work_event.materialization_id,
            work_id=work_event.work_id,
            table=spec.name,
            source_topic=spec.source_topic,
            sampled_records=len(records),
            schema_fields=evolution.last_column_id,
            forks=sum(1 for change in evolution.changes if change.kind == "forked"),
            duration_ms=elapsed_ms(started_ms),
        )
    return evolutions


def _save_schema_zero(
    table: TableProtocol,
    materialization_id: str,
    spec: TableSpec,
    evolution: Any,
    discovery_result: DiscoveryResult,
    *,
    config: ControlPlaneConfig,
) -> None:
    projection = build_schema_projection(
        materialization_id,
        spec.name,
        evolution.schema,
        evolution.fork_map,
        since=discovery_result.discovered_at,
        changes=[change.to_dict() for change in evolution.changes],
        last_column_id=evolution.last_column_id,
        rejected_paths=[change.path for change in evolution.rejected],
    )
    save_materialization_schema(table, projection, config=config)


def _process_discover_tables(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_service: DiscoveryService,
    *,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
) -> FollowUp:
    """Sample the topics an update added, persist their tables' schema 0, and queue their creation.

    The runtime keeps writing the materialization's other tables meanwhile, so its phase is left as
    it is. Blocks while discovery samples; the workflow agent runs `_process_discover_tables_async`.
    """
    started_ms = monotonic_ms()
    pending = _begin_discover_tables(table, work_event, config=config, started_ms=started_ms)
    if not isinstance(pending, tuple):
        return pending
    materialization, specs = pending
    discovery_result = discovery_service.discover(
        replace(materialization, tables=specs, retired_tables=()),
        sample_records=config.discovery_sample_records,
        sample_seconds=config.discovery_sample_seconds,
    )
    return _record_discover_tables(
        table, work_event, discovery_result, specs, config=config, runtime_config=runtime_config, started_ms=started_ms
    )


async def _process_discover_tables_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_service: DiscoveryService,
    *,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
) -> FollowUp:
    """`_process_discover_tables`, awaiting discovery so the event loop runs meanwhile."""
    started_ms = monotonic_ms()
    pending = _begin_discover_tables(table, work_event, config=config, started_ms=started_ms)
    if not isinstance(pending, tuple):
        return pending
    materialization, specs = pending
    discovery_result = await call_adapter(
        discovery_service,
        "discover",
        replace(materialization, tables=specs, retired_tables=()),
        sample_records=config.discovery_sample_records,
        sample_seconds=config.discovery_sample_seconds,
    )
    return _record_discover_tables(
        table, work_event, discovery_result, specs, config=config, runtime_config=runtime_config, started_ms=started_ms
    )


def _begin_discover_tables(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> tuple[Materialization, tuple[TableSpec, ...]] | MaterializationWorkEvent | None:
    """The tables still to discover; their creation when none is left; None to drop the step."""
    materialization = _require_materialization(table, work_event.materialization_id)
    status = _require_status(table, work_event.materialization_id)
    if _should_drop_start_discovery(materialization, status):
        _log_step_skipped(work_event, reason="materialization_deleting", config=config, started_ms=started_ms)
        return None
    created = {record.name for record in list_materialization_tables(table, materialization.materialization_id)}
    specs = tuple(
        spec
        for spec in materialization.tables
        if spec.name not in created
        and resolve_materialization_schema(table, materialization.materialization_id, spec.name) is None
    )
    if not specs:
        _log_step_skipped(work_event, reason="tables_discovered", config=config, started_ms=started_ms)
        return _build_create_tables_after_discovery(work_event)
    topics = ", ".join(spec.source_topic for spec in specs)
    save_materialization_status(
        table,
        replace(status, phase_message=f"Sampling {topics} for {'its' if len(specs) == 1 else 'their'} new table{'s' if len(specs) > 1 else ''}"),
    )
    return materialization, specs


def _record_discover_tables(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    discovery_result: DiscoveryResult,
    specs: tuple[TableSpec, ...],
    *,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
    started_ms: int,
) -> FollowUp:
    """Persist the sampled tables' schema 0, unless the update was undone meanwhile, and queue their creation.

    A topic the cluster's metadata does not list is sampled as an empty one, and only logged: the
    API asked Kafka for it when the change was requested (`require_listed_topics`), and failing here
    would leave the running materialization with a table it cannot create while the topic is gone.
    """
    evolutions = _evolve_schema_zero(
        work_event,
        discovery_result,
        specs,
        protection=_protection_of(table, work_event.materialization_id),
        runtime_config=runtime_config,
        config=config,
        started_ms=started_ms,
    )
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    materialization, status = state
    if _should_drop_start_discovery(materialization, status):
        _log_step_skipped(work_event, reason="materialization_deleting", config=config, started_ms=started_ms)
        return None
    missing = discovery_result.missing_topics()
    if missing:
        LOGGER.warning(
            "materialization %s: %s; its table starts empty",
            work_event.materialization_id,
            missing_topics_message(missing),
        )
    for spec in materialization.tables:
        if spec.name in evolutions and resolve_materialization_schema(table, materialization.materialization_id, spec.name) is None:
            _save_schema_zero(table, materialization.materialization_id, spec, evolutions[spec.name][0], discovery_result, config=config)
    return _build_create_tables_after_discovery(work_event)


def _protection_of(table: TableProtocol, materialization_id: str) -> ProtectionPolicy:
    """The materialization's protection policy; none when its record is gone."""
    materialization = get_materialization(table, materialization_id)
    return NO_PROTECTION if materialization is None else materialization.protection


def _build_create_tables_after_discovery(work_event: MaterializationWorkEvent) -> MaterializationWorkEvent:
    """`create_table` for an update's new tables; it queues the runtime's apply once they exist."""
    return MaterializationWorkEvent(
        work_id=f"work-create-table-{work_event.materialization_id}-{work_event.work_id}",
        materialization_id=work_event.materialization_id,
        step="create_table",
        requested_at=work_event.requested_at,
        caused_by=work_event.caused_by,
        dedupe_key=f"create_table:{work_event.materialization_id}:{work_event.work_id}",
        target_runtime=work_event.target_runtime,
    )


@dataclass(frozen=True, slots=True)
class _TableCreate:
    """The table `create_table` creates in this pass, decided from the records as they were when it started."""

    materialization: Materialization
    spec: TableSpec
    identity: TableIdentity
    schema: MaterializationSchema
    properties: dict[str, str]
    #: The table's place among the materialization's, from 0, and how many there are.
    position: int
    total: int


def _process_create_table(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    object_store_client: ObjectStoreClientProtocol | None,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
) -> MaterializationWorkEvent | None:
    """Create the next of the materialization's Iceberg tables: metadata v0 and its pointer (spec §9.1).

    One table a pass: the step queues itself until every table exists, then queues the runtime's
    apply. Blocks while GCS answers. The workflow agent runs `_process_create_table_async` instead.
    """
    started_ms = monotonic_ms()
    plan = _plan_create_table(table, work_event, config=config, runtime_config=runtime_config, started_ms=started_ms)
    if plan is None:
        return None
    if isinstance(plan, MaterializationWorkEvent):
        return plan
    created = _create_table(plan, object_store_client)
    return _record_create_table(table, work_event, plan, created, config=config, started_ms=started_ms)


async def _process_create_table_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    object_store_client: ObjectStoreClientProtocol | None,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
) -> MaterializationWorkEvent | None:
    """`_process_create_table` with the GCS calls on the table-lifecycle thread."""
    started_ms = monotonic_ms()
    plan = _plan_create_table(table, work_event, config=config, runtime_config=runtime_config, started_ms=started_ms)
    if plan is None:
        return None
    if isinstance(plan, MaterializationWorkEvent):
        return plan
    created = await asyncio.get_running_loop().run_in_executor(
        _TABLE_EXECUTOR,
        partial(_create_table, plan, object_store_client),
    )
    return _record_create_table(table, work_event, plan, created, config=config, started_ms=started_ms)


def _plan_create_table(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    runtime_config: RuntimeAppConfig,
    started_ms: int,
) -> _TableCreate | MaterializationWorkEvent | None:
    """The next table to create; the apply that follows once every table exists; None to drop the step."""
    materialization = _require_materialization(table, work_event.materialization_id)
    status = _require_status(table, work_event.materialization_id)
    if _should_drop_start_discovery(materialization, status):
        _log_step_skipped(work_event, reason="materialization_deleting", config=config, started_ms=started_ms)
        return None
    existing = {record.name: record for record in list_materialization_tables(table, materialization.materialization_id)}
    total = len(materialization.tables)
    for position, spec in enumerate(materialization.tables):
        # An unusable destination fails the step before any I/O, as no retry can fix it. The create
        # view refuses one; this catches records written before it did.
        try:
            identity = table_identity(
                destination_uri=materialization.destination_uri,
                storage_prefix=config.storage_prefix,
                dataset=spec.dataset,
                materialization_id=materialization.materialization_id,
                table=spec.name,
            )
            table_object_prefixes(identity.location, materialization.materialization_id)
        except ValueError as error:
            raise InvalidTableLocationError(
                f"{materialization.destination_uri} cannot hold a table: {error}"
            ) from error
        record = existing.get(spec.name)
        if record is not None and record.location == identity.location:
            # Created by an earlier pass, or a redelivered one: on to the next.
            continue
        projection = resolve_materialization_schema(table, materialization.materialization_id, spec.name)
        if projection is None:
            raise ValueError(f"create_table requires the discovered schema of table {spec.name}")
        protection_json = _initial_protection_json(materialization, projection)
        if protection_json is not None:
            protection = TableProtection.from_json(protection_json).to_dict()
            projection = replace(projection, fields=annotate_protection(projection.fields, protection), protection=protection)
        properties = table_properties(
            materialization_id=materialization.materialization_id,
            source_topic=spec.source_topic,
            partition_granularity=materialization.partition_granularity,
            created_by=materialization.created_by,
            created_at=materialization.created_at,
            package_version=PACKAGE_VERSION,
            snapshot_retention_hours=runtime_config.snapshot_retention_hours,
            min_snapshots_to_keep=runtime_config.min_snapshots_to_keep,
            fork_map_json=ForkMap.from_dict(projection.fork_map).to_json(),
            protection_json=protection_json,
        )
        return _TableCreate(
            materialization=materialization,
            spec=spec,
            identity=identity,
            schema=projection,
            properties=properties,
            position=position,
            total=total,
        )
    # Every table is there: go on with what follows them.
    _log_step_skipped(work_event, reason="table_already_created", config=config, started_ms=started_ms)
    return _build_apply_after_create(work_event, materialization.materialization_id)


def _initial_protection_json(materialization: Materialization, projection: MaterializationSchema) -> str | None:
    """A new table's `materializer.protection`: its first schema's fields classified by the
    materialization's policy (design §5.5); None for a materialization created before data
    protection, whose tables record none until an edit."""
    if materialization.protection_revision < 1:
        return None
    index = build_index(
        schema_from_json(projection.iceberg_schema),
        ForkMap.from_dict(projection.fork_map),
        last_column_id=projection.last_column_id,
    )
    key = get_protection_key()
    protection = initial_protection(
        index,
        materialization.protection,
        revision=materialization.protection_revision,
        at=materialization.created_at,
        by=materialization.created_by,
        key_id=None if key is None else key.key_id,
    )
    return protection.to_json()


def _create_table(plan: _TableCreate, client: ObjectStoreClientProtocol | None) -> CreatedTable:
    try:
        return create_materializer_table(
            plan.identity,
            schema=schema_from_json(plan.schema.iceberg_schema),
            fork_map=ForkMap.from_dict(plan.schema.fork_map),
            granularity=plan.materialization.partition_granularity,
            properties=plan.properties,
            client=client,
            writer="control-plane",
        )
    except TableAlreadyExistsError as error:
        raise TableLocationInUseError(str(error)) from error


def _record_create_table(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    plan: _TableCreate,
    created: CreatedTable,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    materialization, status = state
    now = work_event.requested_at
    record = MaterializationTable(
        materialization_id=materialization.materialization_id,
        dataset=plan.spec.dataset,
        dataset_id=plan.spec.dataset_id,
        name=plan.identity.name,
        source_topic=plan.spec.source_topic,
        location=created.location,
        table_uuid=created.table_uuid,
        partition_granularity=materialization.partition_granularity,
        created_at=now,
        updated_at=now,
        metadata_location=created.metadata_location,
        schema_id=created.schema_id,
        totals={"records": 0, "files": 0, "bytes": 0},
        snapshots=0,
    )
    save_materialization_table(table, record)
    save_materialization_schema(
        table,
        replace(plan.schema, metadata_location=created.metadata_location),
        archive=True,
        config=config,
    )
    progress = f" ({plan.position + 1} of {plan.total})" if plan.total > 1 else ""
    if not _should_drop_start_discovery(materialization, status):
        save_materialization_status(
            table,
            replace(
                status,
                phase_reason="table_created",
                phase_message=f"Iceberg table {plan.identity.namespace}.{plan.identity.name} created{progress}",
                last_transition_time=now,
            ),
        )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_create_table_completed",
        materialization_id=materialization.materialization_id,
        work_id=work_event.work_id,
        table=plan.identity.name,
        source_topic=plan.spec.source_topic,
        location=created.location,
        metadata_location=created.metadata_location,
        position=plan.position + 1,
        tables=plan.total,
        duration_ms=elapsed_ms(started_ms),
    )
    if plan.position + 1 < plan.total:
        # The next table, in a pass of its own: the step stays bounded however many there are.
        return MaterializationWorkEvent(
            work_id=work_event.work_id,
            materialization_id=materialization.materialization_id,
            step="create_table",
            requested_at=work_event.requested_at,
            caused_by=work_event.caused_by,
            dedupe_key=work_event.dedupe_key,
            target_runtime=work_event.target_runtime,
        )
    return _build_apply_after_create(work_event, materialization.materialization_id)


def _build_apply_after_create(work_event: MaterializationWorkEvent, materialization_id: str) -> MaterializationWorkEvent:
    # `work-create-table-<id>[-<resumed work>]` applies as `work-apply-<id>[-<resumed work>]`: an
    # apply that finishes a resumed create is an operation of its own, never a duplicate of the first.
    prefix = f"work-create-table-{materialization_id}"
    suffix = work_event.work_id.removeprefix(prefix) if work_event.work_id.startswith(prefix) else ""
    return MaterializationWorkEvent(
        work_id=f"work-apply-{materialization_id}{suffix}",
        materialization_id=materialization_id,
        step="apply_runtime",
        requested_at=work_event.requested_at,
        caused_by=work_event.caused_by,
        dedupe_key=f"apply_runtime:{materialization_id}:{work_event.work_id}",
        target_runtime=work_event.target_runtime,
    )


def _save_start_discovery_skipped(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    materialization: Materialization,
    status: MaterializationStatus,
) -> None:
    save_operation_record(
        table,
        materialization_id=materialization.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": work_event.step,
            "status": "skipped",
            "dedupe_key": work_event.dedupe_key,
            "reason": "materialization_no_longer_requires_discovery",
            "desired_phase": materialization.desired_phase,
            "observed_phase": status.observed_phase,
        },
    )


def _should_drop_start_discovery(
    materialization: Materialization,
    status: MaterializationStatus,
) -> bool:
    return materialization.desired_phase in {"deleting", "deleted"} or status.observed_phase in {"deleting", "deleted"}


@dataclass(frozen=True, slots=True)
class _RuntimeApply:
    """What `apply_runtime` applies, rendered from the records as they were when it started."""

    materialization: Materialization
    runtime_name: str
    resources: tuple[dict[str, object], ...]
    spec_hash: str
    managed_agent_names: tuple[str, ...]
    #: Every table the runtime writes, by name.
    tables: tuple[str, ...]
    #: No runtime was bound before this apply. Only then is a failure part-way rolled back: a
    #: re-apply's resources are the live runtime, which the next apply or reconcile converges.
    first_apply: bool
    #: This apply holds the runtime at zero members, because the topics it reads changed: Kaspr's
    #: static members keep their assignment, so only a group that emptied is assigned the new ones.
    drain: bool = False


def _process_apply_runtime(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kaspr_app_client: KasprAppClient,
    *,
    config: ControlPlaneConfig,
) -> FollowUp:
    """Apply the rendered runtime, binding it and queueing its observation.

    Blocks while each resource is applied. The workflow agent runs `_process_apply_runtime_async`
    instead, which awaits them.
    """
    started_ms = monotonic_ms()
    plan = _plan_apply_runtime(table, work_event, config=config, started_ms=started_ms)
    if plan is None or isinstance(plan, MaterializationWorkEvent):
        return plan
    applied_resources: list[dict[str, object]] = []
    try:
        for resource in plan.resources:
            kaspr_app_client.apply(resource)
            applied_resources.append(resource)
    except Exception:
        if plan.first_apply:
            _cleanup_partially_applied_resources(
                kaspr_app_client,
                namespace=plan.materialization.namespace,
                applied_resources=applied_resources,
            )
        raise
    for kind, name in retired_runtime_components(plan.runtime_name):
        try:
            kaspr_app_client.delete(namespace=plan.materialization.namespace, name=name, kind=kind)
        except Exception as error:  # noqa: BLE001 - the next apply tries again
            LOGGER.warning("Could not delete the retired %s %s: %s", kind, name, error)
    return _record_apply_runtime(table, work_event, plan, config=config, started_ms=started_ms)


async def _process_apply_runtime_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kaspr_app_client: KasprAppClient,
    *,
    config: ControlPlaneConfig,
) -> FollowUp:
    """`_process_apply_runtime`, awaiting each apply so the event loop runs meanwhile."""
    started_ms = monotonic_ms()
    plan = _plan_apply_runtime(table, work_event, config=config, started_ms=started_ms)
    if plan is None or isinstance(plan, MaterializationWorkEvent):
        return plan
    applied_resources: list[dict[str, object]] = []
    try:
        for resource in plan.resources:
            await call_adapter(kaspr_app_client, "apply", resource)
            applied_resources.append(resource)
    except Exception:
        if plan.first_apply:
            await _cleanup_partially_applied_resources_async(
                kaspr_app_client,
                namespace=plan.materialization.namespace,
                applied_resources=applied_resources,
            )
        raise
    # What an earlier version rendered and this one does not: the runtime's own maintenance clock.
    for kind, name in retired_runtime_components(plan.runtime_name):
        try:
            await call_adapter(kaspr_app_client, "delete", namespace=plan.materialization.namespace, name=name, kind=kind)
        except Exception as error:  # noqa: BLE001 - the next apply tries again
            LOGGER.warning("Could not delete the retired %s %s: %s", kind, name, error)
    return _record_apply_runtime(table, work_event, plan, config=config, started_ms=started_ms)


def _plan_apply_runtime(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> _RuntimeApply | MaterializationWorkEvent | None:
    """Render the runtime and decide whether to apply it; None, logged, for a duplicate.

    The runtime writes every table of the materialization, so it needs them all. A table whose
    creation did not finish (its schema was discovered, its record is missing) is created first:
    the step answers `create_table`, which queues the apply again once every table exists - so
    `POST .../resume` finishes an interrupted create. A table never discovered, beside tables that
    exist, is what a change of topics leaves when its discovery failed: the step answers
    `discover_tables`, which creates it and applies, so a resume finishes that change too. A
    materialization with no table at all and one never discovered - a create whose discovery
    failed, or one from before 1.0 - fails the step with `table_missing`.
    """
    materialization = _require_materialization(table, work_event.materialization_id)
    _require_status(table, work_event.materialization_id)
    table_records = list_materialization_tables(table, work_event.materialization_id)
    created = {record.name for record in table_records}
    missing = [name for name in materialization.table_names if name not in created]
    if missing:
        undiscovered = [
            name
            for name in missing
            if resolve_materialization_schema(table, materialization.materialization_id, name) is None
        ]
        if undiscovered and not table_records:
            raise TableMissingError(
                f"Iceberg table(s) {', '.join(undiscovered)} of this materialization were never created "
                "and have no discovered schema: delete it and create it again."
            )
        if undiscovered:
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="control_plane_apply_runtime_discovering_tables",
                materialization_id=materialization.materialization_id,
                work_id=work_event.work_id,
                tables=undiscovered,
                duration_ms=elapsed_ms(started_ms),
            )
            return MaterializationWorkEvent(
                work_id=f"work-discover-tables-{materialization.materialization_id}-{work_event.work_id}",
                materialization_id=materialization.materialization_id,
                step="discover_tables",
                requested_at=work_event.requested_at,
                caused_by=work_event.caused_by,
                dedupe_key=f"discover_tables:{materialization.materialization_id}:{work_event.work_id}",
                target_runtime=work_event.target_runtime,
            )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_apply_runtime_creating_tables",
            materialization_id=materialization.materialization_id,
            work_id=work_event.work_id,
            tables=missing,
            duration_ms=elapsed_ms(started_ms),
        )
        return MaterializationWorkEvent(
            work_id=f"work-create-table-{materialization.materialization_id}-{work_event.work_id}",
            materialization_id=materialization.materialization_id,
            step="create_table",
            requested_at=work_event.requested_at,
            caused_by=work_event.caused_by,
            dedupe_key=f"create_table:{materialization.materialization_id}:{work_event.work_id}",
            target_runtime=work_event.target_runtime,
        )
    runtime_name = resolve_runtime_app_name(materialization, config=config)
    existing_binding = get_runtime_binding(table, work_event.materialization_id)
    drain = _needs_drain(existing_binding, materialization)
    template_input = build_dedicated_runtime_template_input(
        # At the count autoscaling gave it, when more than its own (`services/autoscaling.py`).
        scaled_materialization(materialization, get_runtime_autoscale(table, materialization.materialization_id)),
        table_records=[record for record in table_records if record.name in set(materialization.table_names)],
        config=config,
        scaled_to_zero=drain,
    )
    rendered_resources = render_dedicated_runtime_resources(template_input)
    rendered_hash = spec_hash(rendered_resources)
    duplicate_reason = _duplicate_apply_skip_reason(
        table=table,
        work_event=work_event,
        existing_binding=existing_binding,
        rendered_hash=rendered_hash,
    )
    if duplicate_reason is not None:
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_apply_runtime_skipped",
            materialization_id=materialization.materialization_id,
            work_id=work_event.work_id,
            runtime_app_name=runtime_name,
            reason=duplicate_reason,
            duration_ms=elapsed_ms(started_ms),
        )
        return None
    return _RuntimeApply(
        materialization=materialization,
        runtime_name=runtime_name,
        resources=tuple(iter_rendered_resources(rendered_resources)),
        spec_hash=rendered_hash,
        managed_agent_names=tuple(template_input["managed_agent_names"]),
        tables=tuple(table["name"] for table in template_input["tables"]),
        first_apply=existing_binding is None,
        drain=drain,
    )


def _needs_drain(binding: RuntimeBinding | None, materialization: Materialization) -> bool:
    """Whether the runtime must be held at zero members until its consumer group has emptied.

    So it is while a drain is under way, and from the moment the topics the runtime reads change:
    the members are static (KIP-345), and a group that keeps its members keeps their assignment, so
    a runtime applied with another set of topics would never be assigned the new ones. A first apply
    has no group yet; a change of order alone changes nothing a group is assigned.
    """
    if binding is None:
        return False
    return binding.draining or set(binding.source_topics) != set(materialization.source_topics)


def _record_apply_runtime(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    plan: _RuntimeApply,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> FollowUp:
    """Bind what was applied and update the status as it is now, then queue the observation.

    The applies wait on the Kubernetes API while other agents write: heartbeats, observations, a
    user's stop. The binding and the operation record describe this apply alone. The status keeps
    whatever they wrote and takes from this step only the desired phase, the deploying phase and
    the runtime's name, deciding whether a healthy runtime keeps `running` on the status it replaces.
    """
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    materialization, status = state
    previous_binding = get_runtime_binding(table, materialization.materialization_id)
    binding = build_runtime_binding(
        materialization=plan.materialization,
        runtime_app_name=plan.runtime_name,
        managed_agent_names=list(plan.managed_agent_names),
        template_version=config.default_runtime_template_version,
        spec_hash=plan.spec_hash,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
    )
    if plan.drain:
        # A drain already under way keeps when it started and what the runtime read before it.
        draining = previous_binding is not None and previous_binding.draining
        binding = replace(
            binding,
            created_at=None if previous_binding is None else previous_binding.created_at,
            drain_started_at=previous_binding.drain_started_at if draining else work_event.requested_at,
            topics_before_drain=(
                list(previous_binding.topics_before_drain)
                if draining
                else ([] if previous_binding is None else list(previous_binding.source_topics))
            ),
        )
    save_runtime_binding(table, binding)
    save_operation_record(
        table,
        materialization_id=materialization.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "apply_runtime",
            "status": "applied",
            "dedupe_key": work_event.dedupe_key,
            "runtime_app_name": plan.runtime_name,
            "spec_hash": plan.spec_hash,
        },
    )
    resource_observation = replace(
        status.resource_observation,
        namespace=plan.materialization.namespace,
        kaspr_app_name=plan.runtime_name,
    )
    if plan.drain:
        save_materialization_status(
            table,
            replace(
                status,
                desired_phase=materialization.desired_phase,
                observed_phase="deploying",
                phase_reason="changing_topics",
                phase_message=(
                    "Changing the runtime's topics: its members are stopped until its consumer group "
                    "empties, so that it starts over with the topics it reads now."
                ),
                last_transition_time=work_event.requested_at,
                resource_observation=replace(
                    status.resource_observation,
                    namespace=plan.materialization.namespace,
                    kaspr_app_name=plan.runtime_name,
                ),
            ),
        )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_apply_runtime_draining",
            materialization_id=materialization.materialization_id,
            work_id=work_event.work_id,
            runtime_app_name=plan.runtime_name,
            topics_before=list(binding.topics_before_drain),
            topics=list(plan.materialization.source_topics),
            duration_ms=elapsed_ms(started_ms),
        )
        return _build_await_drained_event(
            work_event,
            materialization_id=materialization.materialization_id,
            attempt=1,
            now=work_event.requested_at,
        )
    spec_unchanged = previous_binding is not None and previous_binding.last_applied_spec_hash == plan.spec_hash
    if _should_preserve_running_status_during_reapply(status, work_event, now=work_event.requested_at, spec_unchanged=spec_unchanged):
        apply_status = replace(
            status,
            desired_phase=materialization.desired_phase,
            resource_observation=resource_observation,
        )
    else:
        apply_status = replace(
            status,
            desired_phase=materialization.desired_phase,
            observed_phase="deploying",
            phase_reason="runtime_apply_in_progress",
            phase_message="Runtime spec applied; awaiting observation.",
            last_transition_time=work_event.requested_at,
            resource_observation=resource_observation,
        )
    save_materialization_status(table, apply_status)

    follow_up_event = _build_observe_runtime_follow_up(
        materialization_id=materialization.materialization_id,
        runtime_name=plan.runtime_name,
        namespace=plan.materialization.namespace,
        operation_id=work_event.work_id,
        requested_at=work_event.requested_at,
        caused_by=work_event.caused_by,
    )
    # Tables being dropped are not the runtime's any more: an apply that holds no drain started
    # its group over without them, or the runtime never wrote them (kept tables). So the drop of
    # each starts here - after a change of topics, and again after a drop that gave up.
    dropping = [
        record.name
        for record in list_materialization_tables(table, materialization.materialization_id)
        if record.state == "dropping" and record.name not in plan.tables
    ]
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_apply_runtime_completed",
        materialization_id=materialization.materialization_id,
        work_id=work_event.work_id,
        runtime_app_name=plan.runtime_name,
        tables=list(plan.tables),
        tables_to_drop=dropping,
        resource_count=len(plan.resources),
        duration_ms=elapsed_ms(started_ms),
    )
    if not dropping:
        return follow_up_event
    drop_event = build_live_drop_event(
        materialization.materialization_id,
        stamp=work_event.work_id,
        requested_at=work_event.requested_at,
        target_runtime=work_event.target_runtime,
    )
    return follow_up_event, drop_event


#: How often a runtime held at zero is checked for an empty consumer group, and how many times. The
#: group empties once Kafka gives up on its static members: a session timeout (Kaspr's default is
#: 120 s) after the last of them stopped, so 15 minutes allows for slow terminations several times over.
DRAIN_CHECK_SECONDS = 15
DRAIN_MAX_ATTEMPTS = 60


def _build_await_drained_event(
    work_event: MaterializationWorkEvent,
    *,
    materialization_id: str,
    attempt: int,
    now: str,
) -> MaterializationWorkEvent:
    """The next check of a runtime held at zero, `DRAIN_CHECK_SECONDS` from `now`."""
    scheduled_for = (_parse_utc(now) + timedelta(seconds=DRAIN_CHECK_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return MaterializationWorkEvent(
        work_id=f"work-await-drained-{materialization_id}",
        materialization_id=materialization_id,
        step="await_runtime_drained",
        requested_at=now,
        caused_by=EventCause(kind="scheduler", id=f"await-runtime-drained:{materialization_id}"),
        dedupe_key=f"await_runtime_drained:{materialization_id}:{attempt}",
        target_runtime=work_event.target_runtime,
        observation_policy=ObservationPolicy(
            attempt=attempt,
            max_attempts=DRAIN_MAX_ATTEMPTS,
            scheduled_for=scheduled_for,
            mode="one_shot",
        ),
    )


def _process_await_runtime_drained(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kafka_admin_client: KafkaAdminClient,
    *,
    metadata_client: KafkaMetadataClient | None,
    config: ControlPlaneConfig,
) -> FollowUp:
    """Check whether a runtime held at zero has left its consumer group; once it has, set where the
    group resumes each topic the change added, and apply the runtime again.

    Blocks on Kafka; the workflow agent runs `_process_await_runtime_drained_async` instead.
    """
    started_ms = monotonic_ms()
    target = _plan_await_drained(table, work_event, config=config, started_ms=started_ms)
    if target is None:
        return None
    materialization, binding, group_id = target
    state = kafka_admin_client.consumer_group_state(group_id)
    if state not in EMPTY_GROUP_STATES:
        return _drain_not_done(table, work_event, group_id, state, config=config, started_ms=started_ms)
    added = _added_by_change(materialization, binding)
    client = get_metadata_client() if metadata_client is None and added else metadata_client
    topics = client.describe_topics([spec.source_topic for spec in added]) if added else {}
    offsets = _resume_offsets(table, materialization, added, topics)
    if offsets:
        kafka_admin_client.commit_group_offsets(group_id, offsets)
    return _record_drained(table, work_event, group_id, offsets, config=config, started_ms=started_ms)


async def _process_await_runtime_drained_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kafka_admin_client: KafkaAdminClient,
    *,
    metadata_client: KafkaMetadataClient | None,
    config: ControlPlaneConfig,
) -> FollowUp:
    """`_process_await_runtime_drained`, awaiting Kafka so the event loop runs meanwhile."""
    started_ms = monotonic_ms()
    target = _plan_await_drained(table, work_event, config=config, started_ms=started_ms)
    if target is None:
        return None
    materialization, binding, group_id = target
    state = await call_adapter(kafka_admin_client, "consumer_group_state", group_id)
    if state not in EMPTY_GROUP_STATES:
        return _drain_not_done(table, work_event, group_id, state, config=config, started_ms=started_ms)
    added = _added_by_change(materialization, binding)
    client = get_metadata_client() if metadata_client is None and added else metadata_client
    topics = await call_adapter(client, "describe_topics", [spec.source_topic for spec in added]) if added else {}
    offsets = _resume_offsets(table, materialization, added, topics)
    if offsets:
        await call_adapter(kafka_admin_client, "commit_group_offsets", group_id, offsets)
    return _record_drained(table, work_event, group_id, offsets, config=config, started_ms=started_ms)


def _plan_await_drained(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> tuple[Materialization, RuntimeBinding, str] | None:
    """The materialization, its draining binding and its runtime's group; None, logged, when nothing drains."""
    materialization = get_materialization(table, work_event.materialization_id)
    binding = get_runtime_binding(table, work_event.materialization_id)
    if materialization is None or materialization.desired_phase in {"deleting", "deleted"}:
        _log_step_skipped(work_event, reason="materialization_deleting", config=config, started_ms=started_ms)
        return None
    if binding is None or not binding.draining:
        _log_step_skipped(work_event, reason="not_draining", config=config, started_ms=started_ms)
        return None
    group_id = runtime_consumer_group_name(resolve_runtime_app_name(materialization, config=config))
    return materialization, binding, group_id


def _added_by_change(materialization: Materialization, binding: RuntimeBinding) -> list[TableSpec]:
    """The tables the runtime reads now and did not before the change."""
    before = set(binding.topics_before_drain)
    return [spec for spec in materialization.tables if spec.source_topic not in before]


def _resume_offsets(
    table: TableProtocol,
    materialization: Materialization,
    added: list[TableSpec],
    topics: dict[str, Any],
) -> dict[str, dict[int, int]]:
    """Where the emptied group resumes each partition of each added table's topic.

    A table kept earlier resumes after the last offset it committed; a new table, and a partition its
    table never committed, from the partition's first offset. The group may hold an offset for the
    topic from a table dropped earlier, which a new table must not resume from: it would never read
    what came before. Records already in a table are skipped by the runtime whatever this says.
    """
    records = {record.name: record for record in list_materialization_tables(table, materialization.materialization_id)}
    offsets: dict[str, dict[int, int]] = {}
    for spec in added:
        metadata = topics.get(spec.source_topic)
        if metadata is None or not metadata.exists:
            continue
        record = records.get(spec.name)
        committed = {int(partition): int(offset) for partition, offset in (record.offsets if record else {}).items()}
        offsets[spec.source_topic] = {
            int(partition): committed[int(partition)] + 1 if int(partition) in committed else int(beginning)
            for partition, beginning in metadata.beginning_offsets.items()
        }
    return offsets


def _drain_not_done(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    group_id: str,
    state: str,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> FollowUp:
    """Check again later, or give up once the group has kept members for `DRAIN_MAX_ATTEMPTS` checks."""
    status = _require_status(table, work_event.materialization_id)
    attempt = (work_event.observation_policy.attempt if work_event.observation_policy and work_event.observation_policy.attempt else 1) + 1
    now = _utc_now_timestamp()
    if attempt > DRAIN_MAX_ATTEMPTS:
        save_materialization_status(
            table,
            replace(
                status,
                observed_phase="failed",
                phase_reason="consumer_group_not_empty",
                phase_message=(
                    f"Consumer group {group_id} kept members ({state}) for "
                    f"{DRAIN_CHECK_SECONDS * DRAIN_MAX_ATTEMPTS // 60} minutes after the runtime was stopped, so it "
                    "cannot start over with the new topics. Look for runtime members still running, then resume."
                ),
                last_transition_time=now,
            ),
        )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_await_runtime_drained_failed",
            materialization_id=work_event.materialization_id,
            work_id=work_event.work_id,
            consumer_group=group_id,
            group_state=state,
            attempts=attempt - 1,
            duration_ms=elapsed_ms(started_ms),
        )
        return None
    save_materialization_status(
        table,
        replace(
            status,
            phase_reason="changing_topics",
            phase_message=(
                f"Changing the runtime's topics: waiting for consumer group {group_id} to empty ({state}); "
                "Kafka drops stopped members after their session timeout."
            ),
        ),
    )
    return _build_await_drained_event(work_event, materialization_id=work_event.materialization_id, attempt=attempt, now=now)


def _record_drained(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    group_id: str,
    offsets: dict[str, dict[int, int]],
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> FollowUp:
    """End the drain: apply the runtime with the topics it reads now - which, holding no drain,
    also starts the drop of the tables being dropped."""
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    materialization, status = state
    binding = get_runtime_binding(table, materialization.materialization_id)
    if binding is None or not binding.draining:
        _log_step_skipped(work_event, reason="not_draining", config=config, started_ms=started_ms)
        return None
    stamp = _id_safe(str(binding.drain_started_at))
    now = _utc_now_timestamp()
    save_runtime_binding(table, replace(binding, drain_started_at=None, topics_before_drain=[], updated_at=now))
    save_materialization_status(
        table,
        replace(
            status,
            phase_reason="consumer_group_empty",
            phase_message="The runtime's consumer group emptied: applying the runtime with the topics it reads now.",
            last_transition_time=now,
        ),
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_runtime_drained",
        materialization_id=materialization.materialization_id,
        work_id=work_event.work_id,
        consumer_group=group_id,
        topics_resumed={topic: len(partitions) for topic, partitions in offsets.items()},
        duration_ms=elapsed_ms(started_ms),
    )
    return MaterializationWorkEvent(
        work_id=f"work-apply-{materialization.materialization_id}-drained-{stamp}",
        materialization_id=materialization.materialization_id,
        step="apply_runtime",
        requested_at=now,
        caused_by=EventCause(kind="scheduler", id=f"runtime-drained:{materialization.materialization_id}"),
        dedupe_key=f"apply_runtime:{materialization.materialization_id}:drained:{stamp}",
        target_runtime=work_event.target_runtime,
    )


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _cleanup_partially_applied_resources(
    kaspr_app_client: KasprAppClient,
    *,
    namespace: str,
    applied_resources: list[dict[str, object]],
) -> None:
    for kind, name in _rollback_targets(applied_resources):
        try:
            kaspr_app_client.delete(namespace=namespace, name=name, kind=kind)
        except Exception:
            continue


async def _cleanup_partially_applied_resources_async(
    kaspr_app_client: KasprAppClient,
    *,
    namespace: str,
    applied_resources: list[dict[str, object]],
) -> None:
    for kind, name in _rollback_targets(applied_resources):
        try:
            await call_adapter(kaspr_app_client, "delete", namespace=namespace, name=name, kind=kind)
        except Exception:
            continue


def _rollback_targets(applied_resources: list[dict[str, object]]) -> list[tuple[str, str]]:
    """The kind and name of each applied resource that has them, last applied first."""
    targets: list[tuple[str, str]] = []
    for resource in reversed(applied_resources):
        metadata = resource.get("metadata")
        kind = resource.get("kind")
        if not isinstance(metadata, dict) or not isinstance(kind, str):
            continue
        name = metadata.get("name")
        if not isinstance(name, str):
            continue
        targets.append((kind, name))
    return targets


@dataclass(frozen=True, slots=True)
class _RuntimeDelete:
    """The runtime resources `delete_runtime` deletes, as kind and name, in deletion order."""

    namespace: str
    runtime_name: str
    resources: tuple[tuple[str, str], ...]


def _process_delete_runtime(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kaspr_app_client: KasprAppClient,
    *,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """Delete the runtime's resources and queue the observation that confirms they are gone.

    Blocks while each resource is deleted. The workflow agent runs `_process_delete_runtime_async`
    instead, which awaits them.
    """
    started_ms = monotonic_ms()
    plan = _plan_delete_runtime(table, work_event, config=config)
    for kind, name in plan.resources:
        kaspr_app_client.delete(namespace=plan.namespace, name=name, kind=kind)
    return _record_delete_runtime(table, work_event, plan, config=config, started_ms=started_ms)


async def _process_delete_runtime_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kaspr_app_client: KasprAppClient,
    *,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """`_process_delete_runtime`, awaiting each delete so the event loop runs meanwhile."""
    started_ms = monotonic_ms()
    plan = _plan_delete_runtime(table, work_event, config=config)
    for kind, name in plan.resources:
        await call_adapter(kaspr_app_client, "delete", namespace=plan.namespace, name=name, kind=kind)
    return _record_delete_runtime(table, work_event, plan, config=config, started_ms=started_ms)


def _plan_delete_runtime(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
) -> _RuntimeDelete:
    materialization = _require_materialization(table, work_event.materialization_id)
    _require_status(table, work_event.materialization_id)
    runtime_name = resolve_runtime_app_name(materialization, config=config)
    component_names = runtime_component_names(runtime_name)
    return _RuntimeDelete(
        namespace=materialization.namespace,
        runtime_name=runtime_name,
        # Tasks first, then agents with the committer last so a final commit can land, then the
        # state table and the app (spec §4.4).
        resources=(
            ("KasprTask", component_names["pipeline_task_name"]),
            ("KasprTask", component_names["commit_timer_task_name"]),
            *retired_runtime_components(runtime_name),
            ("KasprTask", component_names["status_task_name"]),
            ("KasprAgent", component_names["settings_agent_name"]),
            ("KasprAgent", component_names["writer_agent_name"]),
            ("KasprAgent", component_names["committer_agent_name"]),
            ("KasprTable", component_names["state_table_name"]),
            ("KasprApp", runtime_name),
        ),
    )


def _record_delete_runtime(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    plan: _RuntimeDelete,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    """Mark the status deleting as it is now, keeping whatever other agents wrote meanwhile."""
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    _, status = state
    save_operation_record(
        table,
        materialization_id=work_event.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "delete_runtime",
            "status": "applied",
            "dedupe_key": work_event.dedupe_key,
            "runtime_app_name": plan.runtime_name,
        },
    )
    deleting_status = replace(
        status,
        desired_phase="deleting",
        observed_phase="deleting",
        phase_reason="runtime_apply_in_progress",
        phase_message="Runtime delete applied; awaiting resource absence.",
        last_transition_time=work_event.requested_at,
    )
    save_materialization_status(table, deleting_status)
    follow_up_event = _build_observe_runtime_follow_up(
        materialization_id=work_event.materialization_id,
        runtime_name=plan.runtime_name,
        namespace=plan.namespace,
        operation_id=work_event.work_id,
        requested_at=work_event.requested_at,
        caused_by=work_event.caused_by,
        scheduled=False,
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_delete_runtime_completed",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        runtime_app_name=plan.runtime_name,
        resource_count=len(plan.resources),
        duration_ms=elapsed_ms(started_ms),
    )
    return follow_up_event


@dataclass(frozen=True, slots=True)
class _KafkaCleanup:
    """The runtime whose Kafka topics and consumer group `cleanup_runtime_kafka` deletes."""

    runtime_name: str
    consumer_group_name: str


def _process_cleanup_runtime_kafka(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kafka_admin_client: KafkaAdminClient,
    *,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """Delete the runtime's topics and consumer group, then mark the materialization deleted.

    Blocks while Kafka answers. The workflow agent runs `_process_cleanup_runtime_kafka_async`
    instead, which awaits it.
    """
    started_ms = monotonic_ms()
    plan = _plan_cleanup_runtime_kafka(table, work_event, config=config, started_ms=started_ms)
    if plan is None:
        return None
    matched_topics = _runtime_owned_topics(kafka_admin_client.list_topics(), plan.runtime_name)
    has_consumer_group = plan.consumer_group_name in kafka_admin_client.list_consumer_groups()
    if matched_topics:
        kafka_admin_client.delete_topics(matched_topics)
    if has_consumer_group:
        try:
            kafka_admin_client.delete_consumer_group(plan.consumer_group_name)
        except ConsumerGroupNotEmptyError as exc:
            return _retry_cleanup_runtime_kafka(
                table, work_event, plan, matched_topics, exc, config=config, started_ms=started_ms
            )
    return _record_cleanup_runtime_kafka(
        table,
        work_event,
        plan,
        matched_topics,
        deleted_consumer_group=has_consumer_group,
        config=config,
        started_ms=started_ms,
    )


async def _process_cleanup_runtime_kafka_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    kafka_admin_client: KafkaAdminClient,
    *,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """`_process_cleanup_runtime_kafka`, awaiting Kafka so the event loop runs meanwhile."""
    started_ms = monotonic_ms()
    plan = _plan_cleanup_runtime_kafka(table, work_event, config=config, started_ms=started_ms)
    if plan is None:
        return None
    matched_topics = _runtime_owned_topics(
        await call_adapter(kafka_admin_client, "list_topics"),
        plan.runtime_name,
    )
    has_consumer_group = plan.consumer_group_name in await call_adapter(kafka_admin_client, "list_consumer_groups")
    if matched_topics:
        await call_adapter(kafka_admin_client, "delete_topics", matched_topics)
    if has_consumer_group:
        try:
            await call_adapter(kafka_admin_client, "delete_consumer_group", plan.consumer_group_name)
        except ConsumerGroupNotEmptyError as exc:
            return _retry_cleanup_runtime_kafka(
                table, work_event, plan, matched_topics, exc, config=config, started_ms=started_ms
            )
    return _record_cleanup_runtime_kafka(
        table,
        work_event,
        plan,
        matched_topics,
        deleted_consumer_group=has_consumer_group,
        config=config,
        started_ms=started_ms,
    )


def _plan_cleanup_runtime_kafka(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> _KafkaCleanup | None:
    """Name the runtime to clean up, once it is confirmed gone; None, logged, for a duplicate."""
    materialization = _require_materialization(table, work_event.materialization_id)
    status = _require_status(table, work_event.materialization_id)
    duplicate_reason = _duplicate_cleanup_skip_reason(table=table, work_event=work_event)
    if duplicate_reason is not None:
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_cleanup_runtime_kafka_skipped",
            materialization_id=materialization.materialization_id,
            work_id=work_event.work_id,
            reason=duplicate_reason,
            duration_ms=elapsed_ms(started_ms),
        )
        return None
    if not _cleanup_runtime_kafka_ready(status):
        raise ValueError(
            "cleanup_runtime_kafka requires deleted runtime observation before Kafka cleanup, "
            f"got observed_phase={status.observed_phase} phase_reason={status.phase_reason}"
        )
    runtime_name = (
        work_event.target_runtime.kaspr_app_name
        if work_event.target_runtime is not None and work_event.target_runtime.kaspr_app_name
        else resolve_runtime_app_name(materialization, config=config)
    )
    return _KafkaCleanup(
        runtime_name=runtime_name,
        consumer_group_name=runtime_consumer_group_name(runtime_name),
    )


def _runtime_owned_topics(existing_topics: list[str], runtime_name: str) -> list[str]:
    return sorted(topic for topic in existing_topics if topic.startswith(runtime_name))


def _record_cleanup_runtime_kafka(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    plan: _KafkaCleanup,
    matched_topics: list[str],
    *,
    deleted_consumer_group: bool,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    """Mark the materialization and its status deleted as they are now, and queue what follows.

    Other agents write while Kafka answers, an observation of the absent runtime among them; what
    they wrote stays, under this step's phase, health and error.
    """
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    materialization, status = state
    delete_runtime_binding(table, materialization.materialization_id)
    deleted_materialization = replace(
        materialization,
        desired_phase="deleted",
        updated_at=work_event.requested_at,
    )
    save_materialization(table, deleted_materialization)
    deleted_status = replace(
        status,
        desired_phase="deleted",
        observed_phase="deleted",
        phase_reason="runtime_and_kafka_resources_deleted",
        phase_message="Runtime app, runtime-owned Kafka topics, and consumer group are deleted.",
        last_transition_time=work_event.requested_at,
        runtime_health=RuntimeHealth(level="unknown"),
        last_error=None,
    )
    save_materialization_status(table, deleted_status)
    save_operation_record(
        table,
        materialization_id=materialization.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "cleanup_runtime_kafka",
            "status": "applied",
            "dedupe_key": work_event.dedupe_key,
            "runtime_app_name": plan.runtime_name,
            "consumer_group_name": plan.consumer_group_name,
            "deleted_topics": matched_topics,
            "deleted_consumer_group": deleted_consumer_group,
        },
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_cleanup_runtime_kafka_completed",
        materialization_id=materialization.materialization_id,
        work_id=work_event.work_id,
        runtime_app_name=plan.runtime_name,
        consumer_group_name=plan.consumer_group_name,
        deleted_topics_count=len(matched_topics),
        deleted_consumer_group=deleted_consumer_group,
        duration_ms=elapsed_ms(started_ms),
    )
    return _build_drop_table_event(work_event)


def _retry_cleanup_runtime_kafka(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    plan: _KafkaCleanup,
    matched_topics: list[str],
    error: ConsumerGroupNotEmptyError,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    """Record the wait for a consumer group that still has members, and schedule the retry.

    Raises once `MAX_CLEANUP_KAFKA_RETRY_ATTEMPTS` retries have found the group still in use, which
    records the step as failed.
    """
    state = _state_after_wait(table, work_event, config=config, started_ms=started_ms)
    if state is None:
        return None
    retry_event = _build_cleanup_runtime_kafka_retry(work_event)
    if retry_event is None:
        raise RuntimeError(
            "consumer group "
            f"{plan.consumer_group_name} remained non-empty after Kafka cleanup retry budget "
            "was exhausted"
        ) from error
    materialization, status = state
    _persist_cleanup_runtime_kafka_waiting(
        table=table,
        materialization=materialization,
        status=status,
        work_event=work_event,
        runtime_name=plan.runtime_name,
        consumer_group_name=plan.consumer_group_name,
        deleted_topics=matched_topics,
        retry_event=retry_event,
        error_message=str(error),
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_cleanup_runtime_kafka_retry_scheduled",
        materialization_id=materialization.materialization_id,
        work_id=work_event.work_id,
        runtime_app_name=plan.runtime_name,
        consumer_group_name=plan.consumer_group_name,
        deleted_topics_count=len(matched_topics),
        retry_attempt=retry_event.observation_policy.attempt
        if retry_event.observation_policy is not None
        else None,
        retry_scheduled_for=retry_event.observation_policy.scheduled_for
        if retry_event.observation_policy is not None
        else None,
        duration_ms=elapsed_ms(started_ms),
    )
    return retry_event


#: The work id of every drop of a live materialization's tables - removed topics', kept ones' -
#: starts so. Unlike the drop that deleting a materialization ends with, such a drop goes through
#: the tables being dropped only, and leaves the materialization's other records alone.
LIVE_DROP_WORK_PREFIX = "work-drop-tables-"


def live_drop_work_id(materialization_id: str, stamp: str) -> str:
    """The work id of a drop of a live materialization's tables; `stamp` tells one drop from another."""
    return f"{LIVE_DROP_WORK_PREFIX}{materialization_id}-{_id_safe(stamp)}"


def is_live_drop(work_event: MaterializationWorkEvent) -> bool:
    """Whether a `drop_table` event drops a live materialization's tables rather than all of them."""
    return work_event.step == "drop_table" and work_event.work_id.startswith(LIVE_DROP_WORK_PREFIX)


def build_live_drop_event(
    materialization_id: str,
    *,
    stamp: str,
    requested_at: str,
    caused_by: EventCause | None = None,
    target_runtime: TargetRuntime | None = None,
) -> MaterializationWorkEvent:
    """Drop the tables of a live materialization marked `dropping`."""
    work_id = live_drop_work_id(materialization_id, stamp)
    return MaterializationWorkEvent(
        work_id=work_id,
        materialization_id=materialization_id,
        step="drop_table",
        requested_at=requested_at,
        caused_by=caused_by or EventCause(kind="scheduler", id=f"drop-tables:{materialization_id}"),
        dedupe_key=f"drop_table:{work_id}",
        target_runtime=target_runtime,
    )


def scheduled_request_id(work_event: MaterializationWorkEvent) -> str:
    """The scheduler request a delayed work event replaces: one per materialization and step.

    A live drop's retries have their own, so that they never replace the retries of the drop that
    deleting the materialization meanwhile started - nor are replaced by them.
    """
    if is_live_drop(work_event):
        return f"drop_tables:{work_event.materialization_id}"
    if work_event.step == "observe_runtime":
        return f"observe:{work_event.materialization_id}"
    return f"{work_event.step}:{work_event.materialization_id}"


def _id_safe(value: str) -> str:
    """`value` with only what an id may hold: `2026-10-19T08:00:00Z` becomes `20261019T080000Z`."""
    return "".join(character for character in value if character.isalnum() or character == "-").strip("-") or "0"


def _build_drop_table_event(work_event: MaterializationWorkEvent) -> MaterializationWorkEvent:
    return MaterializationWorkEvent(
        work_id=f"{work_event.work_id}-drop-table",
        materialization_id=work_event.materialization_id,
        step="drop_table",
        requested_at=work_event.requested_at,
        caused_by=EventCause(kind="scheduler", id=f"drop-table:{work_event.materialization_id}"),
        dedupe_key=f"drop_table:{work_event.materialization_id}",
        target_runtime=work_event.target_runtime,
    )


def _process_drop_table(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    object_store_client: ObjectStoreClientProtocol | None,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """Run one bounded pass of the tables' drop and queue what follows it (spec §9.1).

    The tables are dropped one after another, a table's passes until it is gone. With destination
    cleanup on, a pass deletes `data/`, then `metadata/`, then the pointer. Off, the pointer alone
    is retired, so the table leaves the catalog but stays readable by path. Blocks while GCS
    answers; the workflow agent runs `_process_drop_table_async` instead.
    """
    started_ms = monotonic_ms()
    target = _plan_drop_table(table, work_event, config=config, started_ms=started_ms)
    if isinstance(target, MaterializationWorkEvent) or target is None:
        return target
    try:
        if config.destination_cleanup_enabled:
            result = drop_table_objects_pass(
                location=target.location,
                materialization_id=target.materialization_id,
                client=object_store_client,
            )
        else:
            result = retire_table_pointer(
                location=target.location,
                materialization_id=target.materialization_id,
                client=object_store_client,
            )
    except Exception as exc:
        return _retry_drop_table(table, work_event, exc, config=config, started_ms=started_ms)
    return _record_drop_table_pass(table, work_event, target, result, config=config, started_ms=started_ms)


async def _process_drop_table_async(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    object_store_client: ObjectStoreClientProtocol | None,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """`_process_drop_table` with the pass on the cleanup thread.

    Tables are read before the pass and written after it, on the event loop; only the object-store
    calls leave it. Other work runs while the pass does, so what follows it re-reads what it needs.
    """
    started_ms = monotonic_ms()
    target = _plan_drop_table(table, work_event, config=config, started_ms=started_ms)
    if isinstance(target, MaterializationWorkEvent) or target is None:
        return target
    try:
        if config.destination_cleanup_enabled:
            result = await drop_table_objects_pass_async(
                location=target.location,
                materialization_id=target.materialization_id,
                client=object_store_client,
            )
        else:
            result = await retire_table_pointer_async(
                location=target.location,
                materialization_id=target.materialization_id,
                client=object_store_client,
            )
    except Exception as exc:
        return _retry_drop_table(table, work_event, exc, config=config, started_ms=started_ms)
    return _record_drop_table_pass(table, work_event, target, result, config=config, started_ms=started_ms)


def _plan_drop_table(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationTable | MaterializationWorkEvent | None:
    """The table this pass drops: the first not dropped yet; the purge once none is left.

    A live drop goes through the tables being dropped, and stops if the materialization is deleted
    meanwhile: the delete drops every table, these included.
    """
    materialization = _require_materialization(table, work_event.materialization_id)
    live = is_live_drop(work_event)
    if live and materialization.desired_phase in {"deleting", "deleted"}:
        return _skip_drop_table(work_event, reason="materialization_deleting", config=config, started_ms=started_ms)
    # The materialization is already marked deleted at this point, so only a previously applied
    # operation record can mark this drop as redundant.
    duplicate_reason = _completed_operation_skip_reason(table=table, work_event=work_event)
    if duplicate_reason is not None:
        return _skip_drop_table(work_event, reason=duplicate_reason, config=config, started_ms=started_ms)
    records = _tables_to_drop(table, materialization, live=live)
    if not records:
        # Deleted before its tables were created: nothing in the bucket. A live drop finds none
        # when another drop got there first.
        return _skip_drop_table(work_event, reason="no_table", config=config, started_ms=started_ms)
    dropped = set(_drop_table_progress(table, work_event)[2])
    remaining = [record for record in records if record.name not in dropped]
    if not remaining:
        return _skip_drop_table(work_event, reason="tables_dropped", config=config, started_ms=started_ms)
    record = remaining[0]
    # An unusable location is a contract violation that no retry fixes: raise before any I/O.
    table_object_prefixes(record.location, record.materialization_id)
    if record.maintenance_job is not None:
        wait = _wait_for_maintenance(table, work_event, record, config=config, started_ms=started_ms)
        if wait is not None:
            return wait
    return record


#: How long a drop waits for the maintenance app to stop the job it runs on the table: the app is
#: told within a minute (`maintenance_cancel`) and stops at once, so only an app that is down - or
#: one restarted mid-job, which never reports that job done - keeps a drop waiting this long. After
#: it the job is forgotten and the drop goes on; what a job writes later is an orphan.
MAINTENANCE_WAIT_SECONDS = 300
MAINTENANCE_RECHECK_SECONDS = 15


def _wait_for_maintenance(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    record: MaterializationTable,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    """Check again shortly while a maintenance job runs on the table about to be dropped; None to drop it now.

    A compaction would go on writing files into the table's directory as the drop deletes it.
    """
    now = _utc_now_timestamp()
    operation = get_operation_record(table, materialization_id=work_event.materialization_id, operation_id=work_event.work_id)
    payload = {} if operation is None else dict(operation.get("payload", {}))
    waiting = payload.get("maintenance_wait") or {}
    since = waiting.get("since") if waiting.get("table") == record.name else None
    since = since or now
    running = record.maintenance_job
    if (_parse_utc(now) - _parse_utc(since)).total_seconds() >= MAINTENANCE_WAIT_SECONDS:
        # The job is not going to end. Forget it on the table's record: each pass records the drop's
        # progress afresh, so a job still recorded would be waited for, in full, before every pass.
        save_materialization_table(table, replace(record, maintenance_job=None, updated_at=now))
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_drop_table_maintenance_wait_expired",
            materialization_id=work_event.materialization_id,
            work_id=work_event.work_id,
            table=record.name,
            job=running.job,
            job_started_at=running.started_at,
            waited_since=since,
            duration_ms=elapsed_ms(started_ms),
        )
        return None
    deleted_count, passes, dropped = _drop_table_progress(table, work_event)
    save_operation_record(
        table,
        materialization_id=work_event.materialization_id,
        operation_id=work_event.work_id,
        updated_at=now,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "drop_table",
            "status": "in_progress",
            "dedupe_key": work_event.dedupe_key,
            "deleted_count": deleted_count,
            "passes": passes,
            "tables_dropped": dropped,
            "maintenance_wait": {"table": record.name, "since": since, "job": running.job},
        },
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_drop_table_waiting_for_maintenance",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        table=record.name,
        job=running.job,
        job_started_at=running.started_at,
        waited_since=since,
        duration_ms=elapsed_ms(started_ms),
    )
    policy = work_event.observation_policy
    return MaterializationWorkEvent(
        work_id=work_event.work_id,
        materialization_id=work_event.materialization_id,
        step="drop_table",
        requested_at=now,
        caused_by=EventCause(kind="scheduler", id=f"drop-table-maintenance-wait:{work_event.materialization_id}"),
        dedupe_key=work_event.dedupe_key,
        target_runtime=work_event.target_runtime,
        # The same failed attempts as before: waiting is not failing.
        observation_policy=ObservationPolicy(
            attempt=None if policy is None else policy.attempt,
            max_attempts=None if policy is None else policy.max_attempts,
            scheduled_for=(_parse_utc(now) + timedelta(seconds=MAINTENANCE_RECHECK_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            mode="one_shot",
        ),
    )


def _tables_to_drop(
    table: TableProtocol,
    materialization: Materialization,
    *,
    live: bool = False,
) -> list[MaterializationTable]:
    """The table records a drop goes through.

    Deleting the materialization drops every one, in the order of its topics and any other after
    them; a live drop, those being dropped, by name.
    """
    records = list_materialization_tables(table, materialization.materialization_id)
    if live:
        return sorted((record for record in records if record.state == "dropping"), key=lambda record: record.name)
    by_name = {record.name: record for record in records}
    ordered = [by_name.pop(spec.name) for spec in materialization.tables if spec.name in by_name]
    return ordered + sorted(by_name.values(), key=lambda record: record.name)


def _skip_drop_table(
    work_event: MaterializationWorkEvent,
    *,
    reason: str,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_drop_table_skipped",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        reason=reason,
        duration_ms=elapsed_ms(started_ms),
    )
    if is_live_drop(work_event):
        return None
    # The table is gone, but the records this materialization left behind are not, so the chain
    # still has to reach the purge.
    return _build_purge_materialization_records_event(work_event, config=config)


def _record_drop_table_pass(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    target: MaterializationTable,
    result: DestinationCleanupResult,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    """Record a pass, then queue the next one - or the purge, once no table is left.

    Every pass of one drop shares its `work_id`, so a single operation record accumulates the
    count and the tables already dropped: `in_progress` until the last table's last pass marks it
    `applied`, which is also what makes a redelivered event skip. A live drop settles each table as
    it goes (`_settle_dropped_table`) and purges nothing else.
    """
    materialization = _materialization_after_pass(table, work_event, config=config, started_ms=started_ms)
    if materialization is None:
        return None
    live = is_live_drop(work_event)
    previous_deleted, previous_passes, previous_dropped = _drop_table_progress(table, work_event)
    deleted_count = previous_deleted + result.deleted_count
    passes = previous_passes + 1
    dropped = list(previous_dropped)
    if result.complete and target.name not in dropped:
        dropped.append(target.name)
    if live and result.complete:
        _settle_dropped_table(
            table,
            materialization.materialization_id,
            target.name,
            files_kept=result.skipped_reason == "destination_cleanup_disabled",
            now=_utc_now_timestamp(),
        )
    remaining = [record for record in _tables_to_drop(table, materialization, live=live) if record.name not in dropped]
    finished = result.complete and not remaining
    save_operation_record(
        table,
        materialization_id=materialization.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "drop_table",
            "status": "applied" if finished else "in_progress",
            "dedupe_key": work_event.dedupe_key,
            "location": target.location,
            "table": target.name,
            **result.to_dict(),
            "deleted_count": deleted_count,
            "passes": passes,
            "tables_dropped": dropped,
        },
    )
    if not finished:
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_drop_table_progress",
            materialization_id=materialization.materialization_id,
            work_id=work_event.work_id,
            table=target.name,
            tables_dropped=len(dropped),
            bucket=result.bucket,
            pass_deleted_count=result.deleted_count,
            deleted_count=deleted_count,
            passes=passes,
            duration_ms=elapsed_ms(started_ms),
        )
        return _build_drop_table_continuation(work_event)
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_drop_table_completed",
        materialization_id=materialization.materialization_id,
        work_id=work_event.work_id,
        bucket=result.bucket,
        deleted_count=deleted_count,
        passes=passes,
        retired_only=result.skipped_reason == "destination_cleanup_disabled",
        tables_dropped=dropped,
        duration_ms=elapsed_ms(started_ms),
    )
    if live:
        return None
    return _build_purge_materialization_records_event(work_event, config=config)


def _settle_dropped_table(
    table: TableProtocol,
    materialization_id: str,
    name: str,
    *,
    files_kept: bool,
    now: str,
) -> None:
    """What is left of a live materialization's table once it is dropped.

    Its schemas go. With its files deleted its record goes too, which frees the name for the table
    of a topic added later. With them kept (destination cleanup off) the record stays, `dropped`, so
    the name - and with it the location - is never used again: a table created where the files are
    would take them for orphans and delete them.
    """
    delete_table_schemas(table, materialization_id, name)
    record = get_materialization_table(table, materialization_id, name)
    if record is None:
        return
    if files_kept:
        save_materialization_table(table, replace(record, state="dropped", last_error=None, updated_at=now))
    else:
        delete_materialization_table(table, materialization_id, name)


def _retry_drop_table(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    error: Exception,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> MaterializationWorkEvent | None:
    """Schedule a failed pass again after the observation backoff.

    Raises once `MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS` retries in a row have failed too, which
    records the step as failed. Objects already deleted stay deleted either way. A live drop gives
    up on the table instead, which keeps the error and stays `dropping` until the next apply of the
    runtime starts another drop: its materialization goes on running.
    """
    materialization = _materialization_after_pass(table, work_event, config=config, started_ms=started_ms)
    if materialization is None:
        return None
    retry_event = _build_drop_table_retry(work_event)
    if retry_event is None and is_live_drop(work_event):
        _give_up_live_drop(table, work_event, error, config=config, started_ms=started_ms)
        return None
    if retry_event is None:
        raise RuntimeError(
            f"dropping the table still failing after {MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS} retries: {error}"
        ) from error
    retry_policy = retry_event.observation_policy
    deleted_count, passes, dropped = _drop_table_progress(table, work_event)
    save_operation_record(
        table,
        materialization_id=materialization.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "drop_table",
            "status": "in_progress",
            "dedupe_key": work_event.dedupe_key,
            "deleted_count": deleted_count,
            "passes": passes,
            "tables_dropped": dropped,
            "retry_attempt": retry_policy.attempt,
            "retry_scheduled_for": retry_policy.scheduled_for,
            "error": str(error),
        },
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_drop_table_retry_scheduled",
        materialization_id=materialization.materialization_id,
        work_id=work_event.work_id,
        error_type=type(error).__name__,
        error=str(error),
        deleted_count=deleted_count,
        retry_attempt=retry_policy.attempt,
        retry_scheduled_for=retry_policy.scheduled_for,
        duration_ms=elapsed_ms(started_ms),
    )
    return retry_event


def _give_up_live_drop(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    error: Exception,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> None:
    """Stop a live drop that keeps failing: its tables keep the error, and stay `dropping`."""
    now = _utc_now_timestamp()
    deleted_count, passes, dropped = _drop_table_progress(table, work_event)
    for record in list_materialization_tables(table, work_event.materialization_id):
        if record.state == "dropping":
            save_materialization_table(
                table,
                replace(
                    record,
                    last_error=TableError(
                        reason="drop_failed",
                        message=(
                            f"Dropping the table failed {MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS + 1} times in a row: "
                            f"{error}. It is tried again the next time the runtime is applied, as resuming does."
                        ),
                        occurred_at=now,
                    ),
                    updated_at=now,
                ),
            )
    save_operation_record(
        table,
        materialization_id=work_event.materialization_id,
        operation_id=work_event.work_id,
        updated_at=now,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "drop_table",
            "status": "failed",
            "dedupe_key": work_event.dedupe_key,
            "deleted_count": deleted_count,
            "passes": passes,
            "tables_dropped": dropped,
            "error": str(error),
        },
    )
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_drop_table_failed",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        error_type=type(error).__name__,
        error=str(error),
        deleted_count=deleted_count,
        duration_ms=elapsed_ms(started_ms),
    )


def _materialization_after_pass(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> Materialization | None:
    """Re-read the materialization once a pass is over; None, logged, when its records are gone.

    Other work runs while a pass does, and a purge in that window would otherwise be followed by
    an operation record that nothing ever collects.
    """
    materialization = get_materialization(table, work_event.materialization_id)
    if materialization is None:
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="control_plane_drop_table_skipped",
            materialization_id=work_event.materialization_id,
            work_id=work_event.work_id,
            reason="materialization_records_purged",
            duration_ms=elapsed_ms(started_ms),
        )
    return materialization


def _drop_table_progress(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
) -> tuple[int, int, list[str]]:
    """Objects deleted, passes run and tables dropped so far by this drop, from its `in_progress` record."""
    operation = get_operation_record(
        table,
        materialization_id=work_event.materialization_id,
        operation_id=work_event.work_id,
    )
    payload = {} if operation is None else operation.get("payload", {})
    if payload.get("status") != "in_progress":
        return 0, 0, []
    dropped = [str(name) for name in payload.get("tables_dropped") or []]
    return int(payload.get("deleted_count", 0)), int(payload.get("passes", 0)), dropped


def _build_drop_table_continuation(work_event: MaterializationWorkEvent) -> MaterializationWorkEvent:
    """The next pass: the same operation, requested now.

    Requested now rather than at the original time, so the operation record's `updated_at` shows
    when the drop last made progress - which is what keeps the retention sweep off its records.
    """
    return MaterializationWorkEvent(
        work_id=work_event.work_id,
        materialization_id=work_event.materialization_id,
        step="drop_table",
        requested_at=_utc_now_timestamp(),
        caused_by=EventCause(kind="scheduler", id=f"drop-table:{work_event.materialization_id}"),
        dedupe_key=work_event.dedupe_key,
        target_runtime=work_event.target_runtime,
    )


def _build_drop_table_retry(work_event: MaterializationWorkEvent) -> MaterializationWorkEvent | None:
    current_attempt = (
        work_event.observation_policy.attempt
        if work_event.observation_policy is not None and work_event.observation_policy.attempt is not None
        else 0
    )
    next_attempt = current_attempt + 1
    if next_attempt > MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS:
        return None
    # Backed off from the failure, not from when the pass was requested.
    retry_policy = build_one_shot_observation_policy(attempt=next_attempt, requested_at=_utc_now_timestamp())
    return MaterializationWorkEvent(
        work_id=work_event.work_id,
        materialization_id=work_event.materialization_id,
        step="drop_table",
        requested_at=retry_policy.scheduled_for or work_event.requested_at,
        caused_by=EventCause(kind="scheduler", id=f"retry-drop-table:{work_event.materialization_id}"),
        dedupe_key=work_event.dedupe_key,
        target_runtime=work_event.target_runtime,
        observation_policy=retry_policy,
    )


def _build_purge_materialization_records_event(
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """Queue the final step that removes everything this materialization left behind.

    Returns ``None`` when ``purge_records_on_delete`` is disabled, which leaves the records for the
    retention sweep to collect later — the pre-existing behaviour, kept as an escape hatch for
    deployments that want a tombstone window.
    """
    if not config.purge_records_on_delete:
        return None
    return MaterializationWorkEvent(
        work_id=f"{work_event.work_id}-purge-records",
        materialization_id=work_event.materialization_id,
        step="purge_materialization_records",
        requested_at=work_event.requested_at,
        caused_by=EventCause(
            kind="scheduler",
            id=f"purge-materialization-records:{work_event.materialization_id}",
        ),
        dedupe_key=f"purge_materialization_records:{work_event.materialization_id}",
        target_runtime=work_event.target_runtime,
    )


def _process_purge_materialization_records(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
) -> MaterializationWorkEvent | None:
    """Remove every control-plane record retained for a deleted materialization.

    Deleting a materialization is expected to remove the materialization, so the records go with
    the runtime, the Kafka resources and the destination objects rather than lingering until the
    retention sweep. The sweep still exists and will collect anything this step misses.

    Deliberately does not require the materialization to still exist: the step is idempotent, and a
    retried or duplicated event must not fail simply because the first attempt succeeded.
    """
    started_ms = monotonic_ms()
    purge_materialization_records(table, work_event.materialization_id)
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_purge_materialization_records_completed",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        duration_ms=elapsed_ms(started_ms),
    )
    return None


def _cleanup_runtime_kafka_ready(status: MaterializationStatus) -> bool:
    return status.observed_phase == "deleted" or (
        status.desired_phase == "deleting"
        and status.observed_phase == "deleting"
        and status.phase_reason == "waiting_for_consumer_group_empty"
    )


def _build_cleanup_runtime_kafka_retry(
    work_event: MaterializationWorkEvent,
) -> MaterializationWorkEvent | None:
    current_attempt = (
        work_event.observation_policy.attempt
        if work_event.observation_policy is not None and work_event.observation_policy.attempt is not None
        else 0
    )
    next_attempt = current_attempt + 1
    if next_attempt > MAX_CLEANUP_KAFKA_RETRY_ATTEMPTS:
        return None
    retry_policy = build_one_shot_observation_policy(
        attempt=next_attempt,
        requested_at=work_event.requested_at,
    )
    return MaterializationWorkEvent(
        work_id=work_event.work_id,
        materialization_id=work_event.materialization_id,
        step="cleanup_runtime_kafka",
        requested_at=retry_policy.scheduled_for or work_event.requested_at,
        caused_by=EventCause(
            kind="scheduler",
            id=f"retry-cleanup-runtime-kafka:{work_event.materialization_id}",
        ),
        dedupe_key=(
            f"cleanup_runtime_kafka:{work_event.materialization_id}:"
            f"{work_event.work_id}:retry:{next_attempt}"
        ),
        target_runtime=work_event.target_runtime,
        observation_policy=retry_policy,
    )


def _persist_cleanup_runtime_kafka_waiting(
    *,
    table: TableProtocol,
    materialization: Materialization,
    status: MaterializationStatus,
    work_event: MaterializationWorkEvent,
    runtime_name: str,
    consumer_group_name: str,
    deleted_topics: list[str],
    retry_event: MaterializationWorkEvent,
    error_message: str,
) -> None:
    retry_scheduled_for = (
        retry_event.observation_policy.scheduled_for
        if retry_event.observation_policy is not None
        else None
    )
    waiting_message = (
        f"Runtime resources are deleted; waiting for consumer group {consumer_group_name} "
        "to become empty before retrying Kafka cleanup."
    )
    waiting_status = replace(
        status,
        desired_phase="deleting",
        observed_phase="deleting",
        phase_reason="waiting_for_consumer_group_empty",
        phase_message=waiting_message,
        last_transition_time=work_event.requested_at,
        runtime_health=RuntimeHealth(
            level="degraded",
            reason="waiting_for_consumer_group_empty",
            message=error_message,
        ),
        last_error=error_message,
    )
    save_materialization_status(table, waiting_status)
    save_operation_record(
        table,
        materialization_id=materialization.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": "cleanup_runtime_kafka",
            "status": "waiting",
            "dedupe_key": work_event.dedupe_key,
            "runtime_app_name": runtime_name,
            "consumer_group_name": consumer_group_name,
            "deleted_topics": deleted_topics,
            "deleted_consumer_group": False,
            "retry_attempt": (
                retry_event.observation_policy.attempt
                if retry_event.observation_policy is not None
                else None
            ),
            "retry_scheduled_for": retry_scheduled_for,
            "error": error_message,
        },
    )


def _build_observe_runtime_follow_up(
    *,
    materialization_id: str,
    runtime_name: str,
    namespace: str,
    operation_id: str,
    requested_at: str,
    caused_by,
    scheduled: bool = True,
) -> MaterializationWorkEvent:
    """Create the observation work item emitted after runtime apply or delete."""
    return MaterializationWorkEvent(
        work_id=f"work-observe-{materialization_id}",
        materialization_id=materialization_id,
        step="observe_runtime",
        requested_at=requested_at,
        caused_by=caused_by,
        dedupe_key=f"observe:{materialization_id}:{operation_id}:1",
        target_runtime=TargetRuntime(
            namespace=namespace,
            kaspr_app_name=runtime_name,
        ),
        observation_policy=(
            build_one_shot_observation_policy(attempt=1, requested_at=requested_at)
            if scheduled
            else None
        ),
    )


def _duplicate_apply_skip_reason(
    *,
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    existing_binding,
    rendered_hash: str,
) -> str | None:
    if work_event.caused_by.kind == "scheduler" and work_event.caused_by.id == "reconcile-runtime-drift":
        # The sweep names a re-apply after the spec it wants, so a runtime taken to another spec and
        # back - a release, a build of a branch, the release again - asks for work done once before.
        # It is a duplicate only if the runtime already has that spec.
        if existing_binding is not None and existing_binding.last_applied_spec_hash == rendered_hash:
            return "duplicate_reconcile_noop"
        return None
    operation = get_operation_record(
        table,
        materialization_id=work_event.materialization_id,
        operation_id=work_event.work_id,
    )
    if operation is not None and operation.get("payload", {}).get("status") in {"applied", "skipped"}:
        return "duplicate_apply_work"
    if existing_binding is None:
        return None
    if existing_binding.last_apply_operation_id == work_event.work_id:
        return "duplicate_apply_work"
    return None


def _duplicate_cleanup_skip_reason(
    *,
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
) -> str | None:
    completed_reason = _completed_operation_skip_reason(table=table, work_event=work_event)
    if completed_reason is not None:
        return completed_reason
    materialization = get_materialization(table, work_event.materialization_id)
    if materialization is not None and materialization.desired_phase == "deleted":
        return "materialization_already_deleted"
    return None


def _completed_operation_skip_reason(
    *,
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
) -> str | None:
    operation = get_operation_record(
        table,
        materialization_id=work_event.materialization_id,
        operation_id=work_event.work_id,
    )
    if operation is not None and operation.get("payload", {}).get("status") in {"applied", "skipped"}:
        return "duplicate_cleanup_work"
    return None


def _should_preserve_running_status_during_reapply(
    status: MaterializationStatus,
    work_event: MaterializationWorkEvent,
    *,
    now: str,
    spec_unchanged: bool = False,
) -> bool:
    """Whether a healthy running runtime keeps `running` through this apply: the sweep's drift fix,
    or an apply of the spec it already runs - a change of its protection policy, delivered with the
    apply - which changes nothing it runs on."""
    return (
        (
            spec_unchanged
            or (work_event.caused_by.kind == "scheduler" and work_event.caused_by.id == "reconcile-runtime-drift")
        )
        and status.observed_phase == "running"
        and status.runtime_health.level == "healthy"
        and not heartbeat_is_stale(status.last_heartbeat, now=now)
    )


def _persist_workflow_failure(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    error: Exception,
) -> bool:
    materialization = get_materialization(table, work_event.materialization_id)
    status = get_materialization_status(table, work_event.materialization_id)
    if materialization is None or status is None:
        return False

    error_message = str(error)
    phase_reason = getattr(error, "phase_reason", None) or f"{work_event.step}_failed"
    failed_status = replace(
        status,
        desired_phase=materialization.desired_phase,
        observed_phase="failed",
        phase_reason=phase_reason,
        phase_message=(
            f"Workflow step {work_event.step} failed: {error_message}"
            if error_message
            else f"Workflow step {work_event.step} failed."
        ),
        last_transition_time=work_event.requested_at,
        runtime_health=RuntimeHealth(
            level="degraded",
            reason=phase_reason,
            message=error_message or None,
        ),
        last_error=error_message or None,
    )
    save_materialization_status(table, failed_status)
    save_operation_record(
        table,
        materialization_id=materialization.materialization_id,
        operation_id=work_event.work_id,
        updated_at=work_event.requested_at,
        payload={
            "operation_id": work_event.work_id,
            "operation_type": work_event.step,
            "status": "failed",
            "dedupe_key": work_event.dedupe_key,
            "error": error_message,
        },
    )
    return True


def _state_after_wait(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig,
    started_ms: int,
) -> tuple[Materialization, MaterializationStatus] | None:
    """The materialization and its status as other work left them while a step waited.

    A step writes onto these rather than onto what it read before waiting, which would undo what
    others wrote meanwhile: runtime status, resource observation, a user's request. None, logged,
    once a purge has removed them, since anything written then would never be collected.
    """
    materialization = get_materialization(table, work_event.materialization_id)
    status = get_materialization_status(table, work_event.materialization_id)
    if materialization is None or status is None:
        _log_step_skipped(
            work_event,
            reason="materialization_records_purged",
            config=config,
            started_ms=started_ms,
        )
        return None
    return materialization, status


def _log_step_skipped(
    work_event: MaterializationWorkEvent,
    *,
    reason: str,
    config: ControlPlaneConfig,
    started_ms: int,
) -> None:
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event=f"control_plane_{work_event.step}_skipped",
        materialization_id=work_event.materialization_id,
        work_id=work_event.work_id,
        reason=reason,
        duration_ms=elapsed_ms(started_ms),
    )


def _require_materialization(table: TableProtocol, materialization_id: str) -> Materialization:
    materialization = get_materialization(table, materialization_id)
    if materialization is None:
        raise MissingWorkflowStateError(f"materialization {materialization_id} is missing")
    return materialization


def _require_status(table: TableProtocol, materialization_id: str) -> MaterializationStatus:
    status = get_materialization_status(table, materialization_id)
    if status is None:
        raise MissingWorkflowStateError(
            f"materialization status {materialization_id} is missing"
        )
    return status


def _validate_start_discovery(
    materialization: Materialization,
    status: MaterializationStatus,
    work_event: MaterializationWorkEvent,
) -> None:
    if work_event.step != "start_discovery":
        raise ValueError(f"unsupported workflow step {work_event.step}")
    if materialization.desired_phase != "running":
        raise ValueError(
            "start_discovery requires desired_phase running, "
            f"got {materialization.desired_phase}"
        )
    if status.observed_phase != "requested":
        raise ValueError(
            "start_discovery requires observed_phase requested, "
            f"got {status.observed_phase}"
        )


def _utc_now_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")