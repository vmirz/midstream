"""Request intake: from accepted requests to bounded work, and to the directory for table names.

Every request is keyed by its materialization, and intake writes only that materialization's
records. What follows goes out as routed outputs: work events, keyed by the materialization, for
the workflow; and requests for the directory, keyed `identity`, when table names are taken or
given up (`services/table_claims.py`):

- **create** saves the materialization and its tables, waiting for their names (`claiming_tables`),
  and claims them; the directory's answer starts discovery, or deletes the materialization.
- **update** applies a new label, live tail and replica count at once. A change of topics that adds tables
  waits for their names (`PendingTopicChange`); one that adds none applies at once, freeing the
  names of the tables it drops.
- **delete** starts the runtime's deletion and frees every name the materialization holds: its
  tables leave the catalog as the delete is accepted.
- **reprotect** puts a `ReprotectRequest` on the tables' records, which every maintenance tick
  carries until the maintenance app reports it done; an update whose protection change asks for it
  does the same once the change is applied, waiting for the new revision (handbook 22 §22.15).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, replace
import logging
from typing import Any, Final

from midstream.config import ControlPlaneConfig, get_config
from midstream.models import (
    MAINTAINED_TABLE_STATES,
    EventCause,
    IdentityRequest,
    Materialization,
    MaterializationRequestEvent,
    MaterializationStatus,
    PendingTopicChange,
    ReprotectRequest,
    ResourceObservation,
    RuntimeHealth,
    MaterializationWorkEvent,
    TargetRuntime,
    TopicChangePlan,
)
from midstream.protection.policy import NONE as NO_PROTECTION
from midstream.runtime.naming import normalize_runtime_app_name, resolve_runtime_app_name
from midstream.scale import normalize_replicas, normalize_storage_size
from midstream.protocol_logging import elapsed_ms, log_protocol_event, monotonic_ms
from midstream.services.autoscaling import forget_autoscale
from midstream.services.table_claims import (
    claim_request,
    identity_output,
    release_request,
    work_output,
)
from midstream.services.topic_changes import (
    TopicChangeError,
    apply_topic_change,
    describe_topic_change,
    plan_topic_change,
)
from midstream.services.workflow import CLAIMING_TABLES, build_live_drop_event
from midstream.storage import (
    get_materialization,
    get_materialization_status,
    get_runtime_binding,
    list_materialization_tables,
    save_materialization,
    save_materialization_status,
    save_materialization_table,
)
from midstream.storage.table import TableProtocol

REQUEST_ACTION_TO_STEP: Final[dict[str, str]] = {
    "pause": "apply_runtime",
    "stop": "apply_runtime",
    "resume": "apply_runtime",
    "delete": "delete_runtime",
    "update": "apply_runtime",
}

LOGGER = logging.getLogger(__name__)

#: The desired phase each action sets. `update` is absent: it changes settings, not the phase, so a
#: stopped materialization stays stopped and picks the new settings up when it is resumed.
EXPECTED_DESIRED_PHASE: Final[dict[str, str]] = {
    "create": "running",
    "pause": "paused",
    "stop": "stopped",
    "resume": "running",
    "delete": "deleting",
}

_DELETION_PHASES: Final = frozenset({"deleting", "deleted"})


@dataclass(frozen=True, slots=True)
class IntakeResult:
    """What a request hands on: work for the workflow, and requests for the directory."""

    work: tuple[MaterializationWorkEvent, ...] = ()
    identity: tuple[IdentityRequest, ...] = ()

    def outputs(self) -> list[dict[str, Any]]:
        """Each, routed for the request-intake agent's outputs."""
        return [*(identity_output(request) for request in self.identity), *(work_output(event) for event in self.work)]


@dataclass(frozen=True, slots=True)
class _Update:
    """What persisting an update did: the plan applied at once, the one left pending, a refusal."""

    applied: TopicChangePlan | None = None
    pending: PendingTopicChange | None = None
    replicas_changed: bool = False
    #: Autoscaling was turned off while it had the runtime scaled up: it goes back to its own count.
    autoscale_released: bool = False
    #: The protection policy changed (or was recorded again with the current rules): the runtime is
    #: applied, which delivers it (design §5.8); its spec is unchanged, so nothing restarts.
    protection_changed: bool = False


def process_request_event(
    table: TableProtocol,
    request_event: MaterializationRequestEvent,
    *,
    config: ControlPlaneConfig | None = None,
    work_id: str | None = None,
) -> IntakeResult:
    """Persist what an accepted request asks for, and say what follows it."""
    cfg = get_config() if config is None else config
    started_ms = monotonic_ms()
    try:
        result = _process(table, request_event, config=cfg, work_id=work_id)
        log_protocol_event(
            LOGGER,
            enabled=cfg.protocol_logging_enabled,
            event="control_plane_request_intake_completed",
            materialization_id=request_event.materialization_id,
            request_id=request_event.request_id,
            action=request_event.action,
            duration_ms=elapsed_ms(started_ms),
            emitted_work_id=",".join(event.work_id for event in result.work) or None,
            emitted_step=",".join(event.step for event in result.work) or None,
            emitted_identity=",".join(request.action for request in result.identity) or None,
        )
        return result
    except Exception as exc:
        log_protocol_event(
            LOGGER,
            enabled=cfg.protocol_logging_enabled,
            event="control_plane_request_intake_failed",
            materialization_id=request_event.materialization_id,
            request_id=request_event.request_id,
            action=request_event.action,
            duration_ms=elapsed_ms(started_ms),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise


def iter_request_outputs(
    table: TableProtocol,
    request_event: MaterializationRequestEvent,
    *,
    config: ControlPlaneConfig | None = None,
    work_id: str | None = None,
) -> Iterator[dict[str, Any]]:
    """`process_request_event`'s outputs, routed, as a generator: Kaspr sends each one it yields."""
    yield from process_request_event(table, request_event, config=config, work_id=work_id).outputs()


def _process(
    table: TableProtocol,
    request_event: MaterializationRequestEvent,
    *,
    config: ControlPlaneConfig,
    work_id: str | None,
) -> IntakeResult:
    if request_event.action == "create":
        return _process_create(table, request_event, config=config)
    if request_event.action == "reprotect":
        materialization = _require_materialization(table, request_event.materialization_id)
        if materialization.desired_phase not in _DELETION_PHASES:
            _ask_reprotect(table, materialization, request_event)
        return IntakeResult()
    update = _persist_mutation_request_state(table, request_event, config=config)
    materialization = _require_materialization(table, request_event.materialization_id)
    _validate_materialization_alignment(materialization, request_event)
    target_runtime = TargetRuntime(
        namespace=materialization.namespace,
        kaspr_app_name=resolve_runtime_app_name(materialization, config=config),
    )
    if request_event.action == "delete":
        return IntakeResult(
            work=(_work_event(request_event, "delete_runtime", target_runtime, work_id=work_id),),
            identity=(
                release_request(
                    request_id=f"idr-release-{request_event.request_id}",
                    materialization_id=request_event.materialization_id,
                    everything=True,
                    reason="deleted",
                    actor_id=request_event.submitted_by_id,
                    actor_email=request_event.submitted_by,
                    submitted_at=request_event.submitted_at,
                ),
            ),
        )
    if request_event.action != "update":
        step = REQUEST_ACTION_TO_STEP[request_event.action]
        return IntakeResult(work=(_work_event(request_event, step, target_runtime, work_id=work_id),))
    return _update_outputs(table, request_event, update, target_runtime, work_id=work_id)


def _process_create(
    table: TableProtocol,
    request_event: MaterializationRequestEvent,
    *,
    config: ControlPlaneConfig,
) -> IntakeResult:
    """Save the new materialization and claim its tables' names; the directory's answer goes on.

    A create delivered again once its claim was answered - the materialization moved on, or was
    deleted for a name it could not have - hands nothing on.
    """
    _persist_create_request_state(table, request_event, config=config)
    materialization = _require_materialization(table, request_event.materialization_id)
    status = get_materialization_status(table, request_event.materialization_id)
    if materialization.desired_phase != "running" or status is None or status.observed_phase != "requested":
        LOGGER.info(
            "Create request %s of %s was handled already (%s); nothing to do",
            request_event.request_id,
            request_event.materialization_id,
            "no status" if status is None else status.observed_phase,
        )
        return IntakeResult()
    return IntakeResult(
        identity=(
            claim_request(
                request_id=claim_request_id(request_event),
                materialization_id=materialization.materialization_id,
                purpose="create",
                materialization_request_id=request_event.request_id,
                tables=materialization.tables,
                actor_id=request_event.submitted_by_id,
                actor_email=request_event.submitted_by,
                submitted_at=request_event.submitted_at,
            ),
        ),
    )


def claim_request_id(request_event: MaterializationRequestEvent) -> str:
    """The directory request claiming a request's tables: the one the API chose, so a client can
    wait on it, or one derived from the request."""
    return request_event.claim_request_id or f"idr-claim-{request_event.request_id}"


def _update_outputs(
    table: TableProtocol,
    request_event: MaterializationRequestEvent,
    update: _Update,
    target_runtime: TargetRuntime,
    *,
    work_id: str | None,
) -> IntakeResult:
    """What an update hands on.

    A change of topics left pending claims its tables' names, freeing in the same step those of the
    tables it drops; the runtime is applied now only for a new replica count. A change applied at
    once frees the dropped tables' names and goes on as it always has: discovery is not needed (it
    adds nothing), so the runtime is applied - starting its consumer group over with the topics it
    reads now - or, when only kept tables are dropped, they are dropped at once, unless the runtime
    is still draining from an earlier change: then the apply that ends the drain drops them.
    """
    identity: list[IdentityRequest] = []
    work: list[MaterializationWorkEvent] = []
    if update.pending is not None:
        identity.append(
            claim_request(
                request_id=update.pending.claim_request_id,
                materialization_id=request_event.materialization_id,
                purpose="update",
                materialization_request_id=request_event.request_id,
                tables=update.pending.plan.added,
                release=update.pending.plan.dropped,
                actor_id=request_event.submitted_by_id,
                actor_email=request_event.submitted_by,
                submitted_at=request_event.submitted_at,
            )
        )
        if update.replicas_changed or update.autoscale_released or update.protection_changed:
            work.append(_work_event(request_event, "apply_runtime", target_runtime, work_id=work_id))
        return IntakeResult(work=tuple(work), identity=tuple(identity))
    plan = update.applied
    if plan is not None and plan.dropped:
        identity.append(
            release_request(
                request_id=f"idr-release-{request_event.request_id}",
                materialization_id=request_event.materialization_id,
                tables=plan.dropped,
                reason="dropped",
                actor_id=request_event.submitted_by_id,
                actor_email=request_event.submitted_by,
                submitted_at=request_event.submitted_at,
            )
        )
    if (
        plan is not None
        and not plan.changes_runtime
        and plan.dropped
        and not update.replicas_changed
        and not update.autoscale_released
        and not update.protection_changed
    ):
        binding = get_runtime_binding(table, request_event.materialization_id)
        if binding is None or not binding.draining:
            # Kept tables, which the runtime does not write: dropped at once, and nothing else.
            work.append(
                build_live_drop_event(
                    request_event.materialization_id,
                    stamp=request_event.request_id,
                    requested_at=request_event.submitted_at,
                    caused_by=EventCause(kind="request", id=request_event.request_id),
                    target_runtime=target_runtime,
                )
            )
            return IntakeResult(work=tuple(work), identity=tuple(identity))
    if plan is not None or update.replicas_changed or update.autoscale_released or update.protection_changed:
        work.append(_work_event(request_event, "apply_runtime", target_runtime, work_id=work_id))
    return IntakeResult(work=tuple(work), identity=tuple(identity))


def _work_event(
    request_event: MaterializationRequestEvent,
    step: str,
    target_runtime: TargetRuntime,
    *,
    work_id: str | None,
) -> MaterializationWorkEvent:
    return MaterializationWorkEvent(
        work_id=work_id or f"work-{request_event.request_id}",
        materialization_id=request_event.materialization_id,
        step=step,
        requested_at=request_event.submitted_at,
        caused_by=EventCause(kind="request", id=request_event.request_id),
        dedupe_key=f"{step}:{request_event.materialization_id}:{request_event.request_id}",
        target_runtime=target_runtime,
    )


def _require_materialization(table: TableProtocol, materialization_id: str) -> Materialization:
    materialization = get_materialization(table, materialization_id)
    if materialization is None:
        raise ValueError(f"materialization {materialization_id} is missing")
    return materialization


def _validate_materialization_alignment(
    materialization: Materialization,
    request_event: MaterializationRequestEvent,
) -> None:
    if request_event.action == "update":
        if materialization.desired_phase in _DELETION_PHASES:
            raise ValueError(
                "update request cannot apply to a materialization that is "
                f"{materialization.desired_phase}"
            )
        return
    expected_phase = EXPECTED_DESIRED_PHASE[request_event.action]
    if materialization.desired_phase != expected_phase:
        raise ValueError(
            "materialization desired_phase does not match request action: "
            f"expected {expected_phase}, got {materialization.desired_phase}"
        )


def _persist_create_request_state(
    table: TableProtocol,
    request_event: MaterializationRequestEvent,
    *,
    config: ControlPlaneConfig,
) -> None:
    existing = get_materialization(table, request_event.materialization_id)
    if existing is None:
        resolved_runtime_name = normalize_runtime_app_name(
            request_event.spec.runtime_name
            or config.runtime_name_for_materialization(request_event.materialization_id)
        )
        materialization = Materialization(
            materialization_id=request_event.materialization_id,
            name=request_event.spec.name or request_event.materialization_id,
            tables=request_event.spec.tables,
            destination_uri=request_event.spec.destination_uri,
            runtime_mode=request_event.spec.runtime_mode,
            resolved_runtime_name=resolved_runtime_name,
            namespace=request_event.spec.namespace or config.default_namespace,
            desired_phase="running",
            created_at=request_event.submitted_at,
            updated_at=request_event.submitted_at,
            created_by=request_event.submitted_by,
            created_by_id=request_event.submitted_by_id,
            resource_profile=request_event.spec.resource_profile,
            storage_size=normalize_storage_size(
                request_event.spec.storage_size,
                default=config.default_runtime_storage_size,
            ),
            # Limits were checked when the API accepted the request; the maximum may have changed
            # since, and an accepted request must not fail here because of it.
            replicas=normalize_replicas(
                request_event.spec.replicas,
                default=config.default_runtime_replicas,
            ),
            skip_tombstones=request_event.spec.skip_tombstones,
            autoscaling=True if request_event.spec.autoscaling is None else request_event.spec.autoscaling,
            partition_granularity=(
                request_event.spec.partition_granularity or config.default_partition_granularity
            ),
            labels=dict(request_event.spec.labels),
            annotations=dict(request_event.spec.annotations),
            # The API resolved the policy - the deployment default when the request named none. A
            # create from a control plane before data protection carries none, and protects nothing.
            protection=NO_PROTECTION if request_event.spec.protection is None else request_event.spec.protection,
            protection_revision=0 if request_event.spec.protection is None else 1,
            protection_changed_by=None if request_event.spec.protection is None else request_event.submitted_by,
        )
        save_materialization(table, materialization)

    status = get_materialization_status(table, request_event.materialization_id)
    if status is None:
        materialization = _require_materialization(table, request_event.materialization_id)
        names = ", ".join(spec.qualified_name for spec in materialization.tables)
        save_materialization_status(
            table,
            MaterializationStatus(
                materialization_id=request_event.materialization_id,
                desired_phase="running",
                observed_phase="requested",
                phase_reason=CLAIMING_TABLES,
                last_transition_time=request_event.submitted_at,
                observation_source="hybrid",
                runtime_health=RuntimeHealth(level="unknown"),
                resource_observation=ResourceObservation(
                    namespace=materialization.namespace,
                    kaspr_app_name=resolve_runtime_app_name(materialization, config=config),
                    resource_exists=False,
                    managed_agent_states={},
                ),
                phase_message=f"Claiming the table names {names} in their datasets",
            ),
        )


def _persist_mutation_request_state(
    table: TableProtocol,
    request_event: MaterializationRequestEvent,
    *,
    config: ControlPlaneConfig,
) -> _Update:
    """Persist what an update, stop, resume or delete asks for; for an update, what it did to the topics."""
    materialization = _require_materialization(table, request_event.materialization_id)
    update = _Update()
    refused: str | None = None
    protection_refused: str | None = None
    if request_event.action == "update":
        if materialization.desired_phase in _DELETION_PHASES:
            # Raised again, with the reason, once this returns.
            return update
        desired_phase = materialization.desired_phase
        updated_materialization = _apply_settings_update(materialization, request_event, config=config)
        update = replace(update, replicas_changed=updated_materialization.replicas != materialization.replicas)
        updated_materialization, protection_refused = _apply_protection_update(updated_materialization, request_event)
        update = replace(update, protection_changed=updated_materialization.protection_revision != materialization.protection_revision)
        if protection_refused is not None:
            refused = protection_refused
        if materialization.autoscaling and not updated_materialization.autoscaling:
            # Turned off: whatever members autoscaling gave its runtime go, at once.
            update = replace(update, autoscale_released=forget_autoscale(table, materialization.materialization_id))
        if request_event.topic_change is not None:
            try:
                update, updated_materialization = _apply_topic_change(
                    table,
                    updated_materialization,
                    request_event,
                    update,
                    config=config,
                    spec_version_before=materialization.spec_version,
                )
            except TopicChangeError as error:
                # The view planned it against the records as they were then; another change was
                # applied since, and this one no longer fits. None of it is applied, and the status
                # says why: raising would only stop the agent on a request no retry can apply.
                refused = f"The change of topics was not applied: {error}"
                LOGGER.warning(
                    "Refusing the change of topics of request %s for %s: %s",
                    request_event.request_id,
                    request_event.materialization_id,
                    error,
                )
    else:
        desired_phase = EXPECTED_DESIRED_PHASE[request_event.action]
        updated_materialization = replace(
            materialization,
            desired_phase=desired_phase,
            updated_at=request_event.submitted_at,
        )
        if request_event.action in ("stop", "delete"):
            # A resumed runtime starts at its own count; autoscaling decides afresh from its lag.
            forget_autoscale(table, materialization.materialization_id)
    save_materialization(table, updated_materialization)
    if request_event.action == "update" and request_event.reprotect is not None and protection_refused is None:
        _ask_reprotect(table, updated_materialization, request_event)

    existing_status = get_materialization_status(table, request_event.materialization_id)
    status = _build_fallback_status(updated_materialization, config=config) if existing_status is None else existing_status
    status = replace(status, desired_phase=desired_phase)
    if update.pending is not None:
        names = ", ".join(spec.qualified_name for spec in update.pending.plan.added)
        status = replace(status, phase_message=f"Claiming the table names {names} in their datasets")
    elif update.applied is not None and update.applied.changes_anything:
        status = replace(status, phase_message=describe_topic_change(update.applied))
    elif refused is not None:
        status = replace(status, phase_message=refused)
    save_materialization_status(table, status)
    return update


def _ask_reprotect(table: TableProtocol, materialization: Materialization, request_event: MaterializationRequestEvent) -> int:
    """Put the request's reprotect on the records of the tables it names (or every one kept); return
    how many. A written table waits for the materialization's protection revision - an edit's takes
    seconds to reach it - and a kept one, which no runtime writes, for none. A newer request replaces
    one not done yet."""
    asked = request_event.reprotect
    if asked is None:
        return 0
    names = None if asked.tables is None else set(asked.tables)
    count = 0
    for record in list_materialization_tables(table, materialization.materialization_id):
        if record.state not in MAINTAINED_TABLE_STATES or (names is not None and record.name not in names):
            continue
        request = ReprotectRequest(
            request_id=request_event.request_id,
            requested_at=request_event.submitted_at,
            requested_by=request_event.submitted_by,
            revision=materialization.protection_revision if record.state == "active" else 0,
            purge=asked.purge,
        )
        save_materialization_table(table, replace(record, reprotect=request, updated_at=request_event.submitted_at))
        count += 1
    return count


def _apply_topic_change(
    table: TableProtocol,
    materialization: Materialization,
    request_event: MaterializationRequestEvent,
    update: _Update,
    *,
    config: ControlPlaneConfig,
    spec_version_before: int,
) -> tuple[_Update, Materialization]:
    """Apply an update's change of topics, planned against the records as they are now.

    A change that adds tables waits for their names: it is kept as pending, and applied as planned
    once the directory has claimed them (`tables_claimed`). While one waits, no other change of
    topics is taken. A change that adds none applies at once. A redelivered request plans no
    change, since the materialization already reads what it asks for.
    """
    pending = materialization.pending_topic_change
    if pending is not None:
        if pending.request_id == request_event.request_id:
            # This very request, delivered again: it is pending already.
            return replace(update, pending=pending), materialization
        raise TopicChangeError(
            "another change of topics is still waiting for its tables' names to be claimed; try again in a moment"
        )
    plan = plan_topic_change(
        materialization,
        list_materialization_tables(table, materialization.materialization_id),
        request_event.topic_change,
        maximum=config.max_materialization_topics,
    )
    if plan.added:
        pending = PendingTopicChange(
            request_id=request_event.request_id,
            claim_request_id=claim_request_id(request_event),
            submitted_at=request_event.submitted_at,
            plan=plan,
        )
        return (
            replace(update, pending=pending),
            replace(materialization, pending_topic_change=pending, updated_at=request_event.submitted_at),
        )
    applied = apply_topic_change(
        table,
        materialization,
        plan,
        now=request_event.submitted_at,
        spec_version_before=spec_version_before,
    )
    return replace(update, applied=plan), applied


def _apply_settings_update(
    materialization: Materialization,
    request_event: MaterializationRequestEvent,
    *,
    config: ControlPlaneConfig,
) -> Materialization:
    """The materialization with an update request's mutable settings applied: its label and its
    replica count.

    The request carries the rest of the spec as it was, and none of it is read back; the label only
    when the update changes it. The spec version counts real changes of the runtime's spec, so a new
    label - which the runtime knows nothing of - and a redelivered request that finds its change
    applied leave it alone.
    """
    updated = materialization
    if request_event.label is not None and request_event.label != materialization.name:
        updated = replace(updated, name=request_event.label)
    autoscaling = request_event.spec.autoscaling
    if autoscaling is not None and autoscaling != materialization.autoscaling:
        # Not part of the runtime's spec, so its version stays.
        updated = replace(updated, autoscaling=autoscaling)
    # Checked against the maximum when the API accepted the request, as on create.
    replicas = normalize_replicas(
        request_event.spec.replicas,
        default=materialization.replicas,
    )
    if replicas == materialization.replicas:
        return replace(updated, updated_at=request_event.submitted_at)
    return replace(
        updated,
        replicas=replicas,
        spec_version=materialization.spec_version + 1,
        updated_at=request_event.submitted_at,
    )


def _apply_protection_update(
    materialization: Materialization,
    request_event: MaterializationRequestEvent,
) -> tuple[Materialization, str | None]:
    """The materialization with an update's protection policy applied, and why it was not, if not.

    The policy applies only to the revision the API read (`protection_base_revision`): a request
    delivered again after it applied finds its revision gone and changes nothing, and one overtaken
    by another edit is refused - its status says so - rather than undo the newer choice. Applying it
    raises the revision, even for the same policy when the request asks to apply the current rules.
    """
    spec = request_event.spec
    if spec.protection is None or spec.protection_base_revision is None:
        return materialization, None
    if materialization.protection_revision != spec.protection_base_revision:
        if spec.protection == materialization.protection:
            # What it asks for is in force: applied already, or chosen by someone else meanwhile.
            return materialization, None
        LOGGER.warning(
            "Not applying the protection change of request %s for %s: it was made from revision %s, "
            "and the materialization is at revision %s",
            request_event.request_id,
            request_event.materialization_id,
            spec.protection_base_revision,
            materialization.protection_revision,
        )
        return materialization, (
            "The protection change was not applied: another change of protection was applied first. "
            "Review the protection in force and change it again if needed."
        )
    if spec.protection == materialization.protection and not spec.apply_protection_rules:
        return materialization, None
    return (
        replace(
            materialization,
            protection=spec.protection,
            protection_revision=materialization.protection_revision + 1,
            protection_changed_by=request_event.submitted_by,
            protection_reason="apply_rules" if spec.protection == materialization.protection else None,
            updated_at=request_event.submitted_at,
        ),
        None,
    )


def _build_fallback_status(
    materialization: Materialization,
    *,
    config: ControlPlaneConfig,
) -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id=materialization.materialization_id,
        desired_phase=materialization.desired_phase,
        observed_phase=materialization.desired_phase,
        phase_reason=materialization.desired_phase,
        last_transition_time=materialization.updated_at,
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="unknown"),
        resource_observation=ResourceObservation(
            namespace=materialization.namespace,
            kaspr_app_name=(
                materialization.resolved_runtime_name
                or config.runtime_name_for_materialization(materialization.materialization_id)
            ),
            resource_exists=False,
            managed_agent_states={},
        ),
    )
