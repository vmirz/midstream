"""WebView-facing materialization request handlers.

A materialization's tables are each in a dataset, named when their topics are picked:
`tables: [{source_topic, dataset, name?}]`. Creating a materialization, and an update that adds
topics, claim the new tables' names in the directory; the view previews the claim with the
directory agent's own checks, so a conflict it can see - a dataset that does not exist, one the
caller cannot add tables to, a name another table has - is refused at once (404, 403, 409). Adding,
removing or dropping a table, and deleting a materialization, need `dataset.write` on each dataset
it touches; its schemas, `dataset.view_schema` on its dataset.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from collections.abc import Iterator
from typing import Any
from uuid import uuid4

from midstream.config import (
    PARTITION_GRANULARITIES,
    ControlPlaneConfig,
    get_config,
    is_gcs_destination_uri,
)
from midstream.identity.principals import Principal
from midstream.models import (
    Dataset,
    Materialization,
    MaterializationRequestEvent,
    MaterializationRequestSpec,
    MaterializationStatus,
    ResourceObservation,
    RuntimeHealth,
    TableSpec,
    TopicChangePlan,
    TopicChangeRequest,
)
from midstream.models.table import MAINTENANCE_JOBS
from midstream.runtime.naming import normalize_runtime_app_name, resolve_runtime_app_name
from midstream.scale import InvalidScaleError, normalize_replicas
from midstream.services.identity import IdentityRejection, new_request_id, preview_identity_request
from midstream.services.table_claims import claim_request
from midstream.services.table_maintenance import maintenance_request_messages
from midstream.services.topic_changes import TopicChangeError, plan_topic_change
from midstream.settings import effective_runtime_config, resolve_config
from midstream.storage import (
    get_materialization,
    get_materialization_status,
    get_materialization_table,
    get_runtime_autoscale,
    get_runtime_binding,
    get_schema_version,
    list_materialization_tables,
    list_materializations,
    list_schema_versions,
    resolve_materialization_schema,
)
from midstream.storage.identity import find_dataset_by_name, get_dataset
from midstream.storage.table import TableProtocol
from midstream.table_names import (
    InvalidNameError,
    InvalidSourceTopicsError,
    normalize_dataset_name,
    normalize_materialization_label,
    normalize_source_topics,
    normalize_table_name,
    table_name_for_topic,
    TABLE_NAME_MAX_LENGTH,
)
from midstream.web.errors import WebError, already_exists, conflict, forbidden, invalid_request, not_found
from midstream.web.responses import (
    MutationAcceptance,
    build_accepted_response,
    build_materialization_detail,
    build_materialization_schema,
    build_materialization_schema_versions,
    build_materialization_table,
)

#: Settings an update may change: its label, the runtime's members and whether they are autoscaled,
#: and the topics it reads - with the tables of new topics and what becomes of the tables of removed
#: ones. Everything else a materialization was created with is fixed, its tables' datasets and names
#: above all.
MUTABLE_SETTINGS = frozenset({"name", "replicas", "autoscaling", "tables", "removed_tables"})
#: What a request is told when it asks for a materialization without a live tail.
LIVE_TAIL_ALWAYS = (
    "every materialization has a live tail; only the deployment's live_tail_enabled turns it off, for all of them"
)
#: The settings a create request carries, and what a client reads back; an update may repeat their
#: current values unchanged.
_CREATION_SETTINGS = frozenset(
    {
        "source_topics",
        "retired_tables",
        "destination_uri",
        "runtime_mode",
        "namespace",
        "resource_profile",
        "storage_size",
        "skip_tombstones",
        "partition_granularity",
        "labels",
        "annotations",
    }
)
_DELETION_PHASES = frozenset({"deleting", "deleted"})


def list_materializations_view(
    table: TableProtocol,
    *,
    limit: str | None = None,
    status: str | None = None,
    q: str | None = None,
) -> dict[str, Any]:
    """Return the bounded materialization list payload exposed by the web layer.

    `q` finds materializations by label, id, topic, dataset or table, without regard to case.
    """
    try:
        return list_materializations(table, limit=limit, status=status, query=q)
    except ValueError as error:
        field_name = "status" if "status" in str(error) else "limit"
        raise invalid_request(str(error), field_name=field_name) from error


def get_materialization_detail(
    table: TableProtocol,
    materialization_id: str,
    *,
    principal: Principal | None = None,
) -> dict[str, Any]:
    """Return the detail payload for one materialization or raise a web error.

    With the caller, each table says whether they may read its schema (`readable`) and query its
    data (`queryable`), on its dataset.
    """
    materialization = get_materialization(table, materialization_id)
    if materialization is None:
        raise not_found(
            f"materialization {materialization_id} was not found",
            resource="materialization",
            identifier=materialization_id,
        )
    status = get_materialization_status(table, materialization_id)
    if status is None:
        status = _fallback_status(materialization)
    runtime_binding = get_runtime_binding(table, materialization_id)
    return build_materialization_detail(
        materialization=materialization,
        status=status,
        runtime_binding=runtime_binding,
        autoscale=get_runtime_autoscale(table, materialization_id),
        table_records=list_materialization_tables(table, materialization_id),
        principal=principal,
    )


def get_materialization_schema_view(
    table: TableProtocol,
    materialization_id: str,
    table_name: str,
    *,
    principal: Principal | None = None,
    raw: str | bool | None = None,
) -> dict[str, Any]:
    """Return the current schema projection of one of a materialization's tables (spec §9.3).

    The caller needs to be allowed to see the schemas of the table's dataset.
    """
    materialization = _require_materialization(table, materialization_id)
    spec = _require_table(materialization, table_name)
    _require_schema_access(principal, spec)
    return build_materialization_schema(
        materialization=materialization,
        table_spec=spec,
        schema=resolve_materialization_schema(table, materialization_id, spec.name),
        status=get_materialization_status(table, materialization_id),
        raw=_parse_flag(raw, field_name="raw"),
    )


def list_materialization_schema_versions_view(
    table: TableProtocol,
    materialization_id: str,
    table_name: str,
    *,
    principal: Principal | None = None,
    limit: str | int | None = None,
) -> dict[str, Any]:
    """Return the archived schema index of one of a materialization's tables, newest first."""
    materialization = _require_materialization(table, materialization_id)
    spec = _require_table(materialization, table_name)
    _require_schema_access(principal, spec)
    current = resolve_materialization_schema(table, materialization_id, spec.name)
    return build_materialization_schema_versions(
        materialization=materialization,
        table_spec=spec,
        schemas=list_schema_versions(
            table,
            materialization_id,
            spec.name,
            limit=_normalize_version_limit(limit),
        ),
        current_schema_id=None if current is None else current.schema_id,
    )


def get_materialization_schema_version_view(
    table: TableProtocol,
    materialization_id: str,
    table_name: str,
    schema_id: str | int,
    *,
    principal: Principal | None = None,
    raw: str | bool | None = None,
) -> dict[str, Any]:
    """Return one archived schema of one of a materialization's tables."""
    materialization = _require_materialization(table, materialization_id)
    spec = _require_table(materialization, table_name)
    _require_schema_access(principal, spec)
    version = _parse_schema_id(schema_id)
    schema = get_schema_version(table, materialization_id, spec.name, version)
    if schema is None:
        current = resolve_materialization_schema(table, materialization_id, spec.name)
        if current is None or current.schema_id != version:
            raise not_found(
                f"schema {version} was not found for table {spec.name} of materialization {materialization_id}",
                resource="materialization-schema-version",
                identifier=f"{materialization_id}:{spec.name}:{version}",
            )
        schema = current
    return build_materialization_schema(
        materialization=materialization,
        table_spec=spec,
        schema=schema,
        raw=_parse_flag(raw, field_name="raw"),
    )


def get_materialization_table_view(
    table: TableProtocol,
    materialization_id: str,
    table_name: str,
    *,
    principal: Principal | None = None,
) -> dict[str, Any]:
    """Return one of a materialization's table records (spec §9.3), or 404 before it exists.

    With the caller, it says whether they may read the table (`readable`, `queryable`)."""
    materialization = _require_materialization(table, materialization_id)
    spec = _require_table(materialization, table_name)
    record = get_materialization_table(table, materialization_id, spec.name)
    if record is None:
        raise not_found(
            f"table {spec.name} of materialization {materialization_id} is not created yet; "
            "it is created once discovery finishes",
            resource="materialization-table",
            identifier=f"{materialization_id}:{spec.name}",
        )
    return build_materialization_table(
        materialization=materialization,
        record=record,
        status=get_materialization_status(table, materialization_id),
        principal=principal,
    )


def request_maintenance(
    table: TableProtocol,
    materialization_id: str,
    body: Any,
    *,
    submitted_by: str,
    request_id: str | None = None,
    accepted_at: str | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> Iterator[dict[str, Any]]:
    """Validate a maintenance request; yield, for each table it names, the message for the maintenance app.

    Every value carries the same response. The maintenance app runs the job at the table's next
    opportunity, whatever the runtime is doing - stopped included - and reports it with
    `maintenance_completed`. Without a table, the job runs on every table, kept ones included.
    """
    cfg = resolve_config(settings_table) if config is None else config
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    job = body.get("job")
    if job not in MAINTENANCE_JOBS:
        raise invalid_request(f"job must be one of: {', '.join(MAINTENANCE_JOBS)}", field_name="job")
    materialization = _require_materialization(table, materialization_id)
    requested_table = body.get("table")
    if requested_table not in (None, "") and not isinstance(requested_table, str):
        raise invalid_request("table must be the name of one of the materialization's tables", field_name="table")
    spec = None if requested_table in (None, "") else _require_table(materialization, requested_table, status=400)
    _require_not_deleting(materialization, verb="maintained")
    names = {table_spec.name for table_spec in (materialization.all_tables if spec is None else (spec,))}
    records = [record for record in list_materialization_tables(table, materialization_id) if record.name in names]
    if not records:
        raise conflict(
            f"materialization {materialization_id} has no table {'yet' if spec is None else spec.name + ' yet'}",
            reason="table_not_created",
        )
    resolved_request_id = request_id or _new_identifier("mnt")
    resolved_accepted_at = accepted_at or _utc_now()
    messages = maintenance_request_messages(
        records,
        job=job,
        runtime_config=effective_runtime_config(settings_table, cfg),
        request_id=resolved_request_id,
        requested_by=submitted_by,
        issued_at=resolved_accepted_at,
    )
    if not messages:
        raise conflict(
            f"materialization {materialization_id} has no table to maintain",
            reason="table_not_created",
        )
    response = {
        "materialization_id": materialization_id,
        "request_id": resolved_request_id,
        "job": job,
        "table": None if spec is None else spec.name,
        "tables": [message.target.table for message in messages],
        "accepted_at": resolved_accepted_at,
    }
    # Computed now, so a refusal is raised before anything is sent.
    values = [{"response": response, "key": message.key, "message": message.to_dict()} for message in messages]
    return (value for value in values)


def _require_materialization(table: TableProtocol, materialization_id: str) -> Materialization:
    materialization = get_materialization(table, materialization_id)
    if materialization is None:
        raise not_found(
            f"materialization {materialization_id} was not found",
            resource="materialization",
            identifier=materialization_id,
        )
    return materialization


def _require(principal: Principal, permission: str, *, dataset: Dataset) -> None:
    """`identity.authorization.require`, imported when first needed: authorization reads
    `web.errors`, and this module is imported with the `web` package."""
    from midstream.identity.authorization import require

    require(principal, permission, dataset=dataset)


def _require_schema_access(principal: Principal | None, spec: TableSpec) -> None:
    """Refuse a caller who may not see the schemas of the table's dataset."""
    if principal is None:
        return
    _require(principal, "dataset.view_schema", dataset=_dataset_of(spec))


def _dataset_of(spec: TableSpec) -> Dataset:
    """The dataset a table is in, as far as a refusal needs to name it."""
    return Dataset(
        dataset_id=spec.dataset_id,
        name=spec.dataset,
        created_at="-",
        created_by="-",
        updated_at="-",
        updated_by="-",
    )


def _require_table(
    materialization: Materialization,
    table_name: str,
    *,
    status: int = 404,
    kept: bool = True,
) -> TableSpec:
    """The materialization's table called `table_name`, written or (with `kept`) kept read-only: a
    404 (or, in a body, a 400) when it has none."""
    name = str(table_name or "")
    spec = materialization.any_table(name) if kept else materialization.table(name)
    if spec is not None:
        return spec
    known = ", ".join(table.name for table in (materialization.all_tables if kept else materialization.tables))
    message = f"materialization {materialization.materialization_id} has no table {table_name}; its tables are {known}"
    if status == 400:
        raise invalid_request(message, field_name="table")
    raise not_found(
        message,
        resource="materialization-table",
        identifier=f"{materialization.materialization_id}:{table_name}",
    )


def _parse_schema_id(value: str | int) -> int:
    try:
        schema_id = int(value)
    except (TypeError, ValueError):
        raise invalid_request("schema_id must be a whole number", field_name="schema_id") from None
    if schema_id < 0:
        raise invalid_request("schema_id must be a whole number", field_name="schema_id")
    return schema_id


def _parse_flag(value: str | bool | None, *, field_name: str) -> bool:
    if value in (None, "", False):
        return False
    if value is True:
        return True
    text = str(value).strip().lower()
    if text in ("true", "1", "yes"):
        return True
    if text in ("false", "0", "no"):
        return False
    raise invalid_request(f"{field_name} must be true or false", field_name=field_name)


def _normalize_version_limit(limit: str | int | None) -> int | None:
    if limit in (None, "", "all"):
        return None
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        raise invalid_request(
            "limit must be a positive integer or 'all'",
            field_name="limit",
        ) from None
    if parsed < 1:
        raise invalid_request(
            "limit must be a positive integer or 'all'",
            field_name="limit",
        )
    return parsed


def create_materialization(
    table: TableProtocol,
    body: dict[str, Any],
    *,
    submitted_by: str,
    submitted_by_id: str | None = None,
    principal: Principal | None = None,
    directory_table: TableProtocol | None = None,
    request_id: str | None = None,
    materialization_id: str | None = None,
    claim_request_id: str | None = None,
    accepted_at: str | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> MutationAcceptance:
    """Validate a new materialization request and return an accepted request event.

    Each table's dataset must exist, the caller must be able to put tables in it, and its name
    must be free there: checked now, against the directory, with the directory agent's own checks.
    Request intake then claims the names, and the response's `claim_request_id` is what a client
    waits on (`GET /identity/requests/{id}`) to learn whether they were taken.

    Defaults and limits are the effective settings when the request is accepted; the request
    carries the values they resolved to, so a later settings change does not affect it.
    """
    cfg = resolve_config(settings_table) if config is None else config
    resolved_request_id = request_id or _new_identifier("req")
    resolved_materialization_id = materialization_id or _new_identifier("mat")
    resolved_claim_id = claim_request_id or new_request_id()
    resolved_accepted_at = accepted_at or _utc_now()

    if get_materialization(table, resolved_materialization_id) is not None:
        raise already_exists(
            f"materialization {resolved_materialization_id} already exists",
            resource="materialization",
            identifier=resolved_materialization_id,
        )

    normalized_spec, resolved_runtime_name = _normalize_request_spec(
        body,
        config=cfg,
        materialization_id=resolved_materialization_id,
        directory_table=directory_table,
    )
    _preview_claim(
        directory_table,
        claim_request(
            request_id=resolved_claim_id,
            materialization_id=resolved_materialization_id,
            purpose="create",
            materialization_request_id=resolved_request_id,
            tables=normalized_spec.tables,
            actor_id=submitted_by_id,
            actor_email=submitted_by,
            submitted_at=resolved_accepted_at,
        ),
        principal=principal,
    )
    materialization = Materialization(
        materialization_id=resolved_materialization_id,
        name=normalized_spec.name or resolved_materialization_id,
        tables=normalized_spec.tables,
        destination_uri=normalized_spec.destination_uri,
        runtime_mode=normalized_spec.runtime_mode,
        resolved_runtime_name=resolved_runtime_name,
        namespace=normalized_spec.namespace or cfg.default_namespace,
        desired_phase="running",
        created_at=resolved_accepted_at,
        updated_at=resolved_accepted_at,
        created_by=submitted_by,
        created_by_id=submitted_by_id,
        resource_profile=normalized_spec.resource_profile,
        storage_size=normalized_spec.storage_size or cfg.default_runtime_storage_size,
        replicas=normalize_replicas(
            normalized_spec.replicas,
            default=cfg.default_runtime_replicas,
            maximum=cfg.max_runtime_replicas,
        ),
        skip_tombstones=normalized_spec.skip_tombstones,
        autoscaling=True if normalized_spec.autoscaling is None else normalized_spec.autoscaling,
        partition_granularity=normalized_spec.partition_granularity or cfg.default_partition_granularity,
        labels=normalized_spec.labels,
        annotations=normalized_spec.annotations,
    )
    status = MaterializationStatus(
        materialization_id=resolved_materialization_id,
        desired_phase="running",
        observed_phase="requested",
        phase_reason="claiming_tables",
        last_transition_time=resolved_accepted_at,
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="unknown"),
        resource_observation=ResourceObservation(
            namespace=materialization.namespace,
            kaspr_app_name=resolved_runtime_name,
            resource_exists=False,
            managed_agent_states={},
        ),
        phase_message="Request accepted",
    )
    request_event = MaterializationRequestEvent(
        request_id=resolved_request_id,
        materialization_id=resolved_materialization_id,
        action="create",
        submitted_at=resolved_accepted_at,
        submitted_by=submitted_by,
        spec=normalized_spec,
        submitted_by_id=submitted_by_id,
        claim_request_id=resolved_claim_id,
    )
    response = build_accepted_response(
        materialization_id=resolved_materialization_id,
        request_id=resolved_request_id,
        desired_phase=materialization.desired_phase,
        observed_phase=status.observed_phase,
        phase_reason=status.phase_reason,
        accepted_at=resolved_accepted_at,
    )
    response["name"] = materialization.name
    # Always true - every materialization has a live tail - and still sent: a 4.1 console offers Live only when it reads it.
    response["live_tail"] = True
    # What SQL will call each topic's rows, `<dataset>.<table>`: claimed next, fixed from then on.
    response["tables"] = [table.to_dict() for table in materialization.tables]
    response["claim_request_id"] = resolved_claim_id
    response["claim_status_url"] = f"/identity/requests/{resolved_claim_id}"
    acceptance = MutationAcceptance(
        response=response,
        request_event=request_event,
        materialization=materialization,
        status=status,
    )
    return acceptance


def stop_materialization(
    table: TableProtocol,
    materialization_id: str,
    *,
    submitted_by: str,
    submitted_by_id: str | None = None,
    request_id: str | None = None,
    accepted_at: str | None = None,
    config: ControlPlaneConfig | None = None,
) -> MutationAcceptance:
    """Request that a materialization stop: its runtime is scaled to zero replicas.

    The configured replica count is kept, so resuming scales the runtime back to what it was.
    """
    materialization = _require_materialization(table, materialization_id)
    _require_not_deleting(materialization, verb="stopped")
    _require_deployed(table, materialization, doing="stop")
    return _request_change(
        table,
        materialization,
        desired_phase="stopped",
        action="stop",
        submitted_by=submitted_by,
        submitted_by_id=submitted_by_id,
        request_id=request_id,
        accepted_at=accepted_at,
        config=config,
    )


def resume_materialization(
    table: TableProtocol,
    materialization_id: str,
    *,
    submitted_by: str,
    submitted_by_id: str | None = None,
    request_id: str | None = None,
    accepted_at: str | None = None,
    config: ControlPlaneConfig | None = None,
) -> MutationAcceptance:
    """Request a transition of an existing materialization back to the running phase."""
    materialization = _require_materialization(table, materialization_id)
    _require_not_deleting(materialization, verb="resumed")
    return _request_change(
        table,
        materialization,
        desired_phase="running",
        action="resume",
        submitted_by=submitted_by,
        submitted_by_id=submitted_by_id,
        request_id=request_id,
        accepted_at=accepted_at,
        config=config,
    )


def update_materialization(
    table: TableProtocol,
    materialization_id: str,
    body: Any,
    *,
    submitted_by: str,
    submitted_by_id: str | None = None,
    principal: Principal | None = None,
    directory_table: TableProtocol | None = None,
    request_id: str | None = None,
    claim_request_id: str | None = None,
    accepted_at: str | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> MutationAcceptance:
    """Change a materialization's label, replica count or topics, and re-apply its runtime.

    `name` sets the label; the runtime knows nothing of it, so it re-applies nothing. A request may
    still say `live_tail: true`, which a 4.1 console sends back, but not `false`: every
    materialization has a live tail (handbook 19). `replicas` sets the runtime's
    members. `tables` sets the topics it reads from now on, in order, each `{source_topic, dataset,
    name?}`: a topic it reads, or kept a table of, keeps that table; a new topic gets a table in the
    dataset given, its name claimed before it applies. `removed_tables` says, for each table whose
    topic it removes, whether to `drop` the table with its data or `keep` it read-only; it may also
    drop a table kept earlier (`services/topic_changes.py`). Adding, resuming, keeping or dropping a
    table needs `dataset.write` on its dataset. A request may repeat any other setting at its
    current value - so a client can send back what it read - but not change it. The desired phase
    is left alone: a stopped materialization stays stopped, and runs with the new settings once
    resumed.
    """
    cfg = resolve_config(settings_table) if config is None else config
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    materialization = _require_materialization(table, materialization_id)

    current = materialization.to_dict()
    _refuse_live_tail_off(body)
    for field_name, value in body.items():
        if field_name in MUTABLE_SETTINGS or field_name == "live_tail":
            continue
        if field_name in _CREATION_SETTINGS:
            if value == current.get(field_name):
                continue
            message = (
                "change the topics with tables: [{source_topic, dataset, name}]"
                if field_name == "source_topics"
                else f"{field_name} cannot be changed after creation"
            )
            raise invalid_request(message, field_name=field_name)
        raise invalid_request(f"{field_name} is not a setting that can be changed", field_name=field_name)
    if all(_absent(body.get(field_name)) for field_name in MUTABLE_SETTINGS) and "name" not in body:
        raise invalid_request(
            "say what to change: name, replicas, autoscaling, tables or removed_tables",
            field_name="replicas",
        )
    autoscaling = body.get("autoscaling")
    if autoscaling is not None and not isinstance(autoscaling, bool):
        raise invalid_request("autoscaling must be a boolean", field_name="autoscaling")
    label: str | None = None
    if "name" in body:
        try:
            label = normalize_materialization_label(body.get("name")) or materialization_id
        except InvalidNameError as error:
            raise invalid_request(str(error), field_name=error.field_name) from error
    replicas = materialization.replicas
    if not _absent(body.get("replicas")):
        try:
            replicas = normalize_replicas(body["replicas"], maximum=cfg.max_runtime_replicas)
        except InvalidScaleError as error:
            raise invalid_request(str(error), field_name=error.field_name) from error
    records = list_materialization_tables(table, materialization_id)
    topic_change = _topic_change_request(body, materialization, records, directory_table=directory_table, config=cfg)

    _require_not_deleting(materialization, verb="changed")
    if topic_change is not None or replicas != materialization.replicas:
        _require_deployed(table, materialization, doing="change")
    changes: dict[str, Any] = {}
    plan: TopicChangePlan | None = None
    resolved_claim_id: str | None = None
    resolved_request_id = request_id or _new_identifier("req")
    resolved_accepted_at = accepted_at or _utc_now()
    if topic_change is not None:
        try:
            plan = plan_topic_change(materialization, records, topic_change, maximum=cfg.max_materialization_topics)
        except TopicChangeError as error:
            raise invalid_request(str(error), field_name=error.field_name) from error
        if plan.tables != materialization.tables or plan.retired_tables != materialization.retired_tables or plan.dropped:
            if materialization.pending_topic_change is not None:
                raise conflict(
                    f"materialization {materialization_id} is still adding topics: their tables' names are being "
                    "claimed. Try again in a moment.",
                    reason="topic_change_pending",
                )
            _require_dataset_write(principal, (*plan.added, *plan.resumed, *plan.removed, *plan.dropped), directory_table)
            if plan.added:
                resolved_claim_id = claim_request_id or new_request_id()
                _preview_claim(
                    directory_table,
                    claim_request(
                        request_id=resolved_claim_id,
                        materialization_id=materialization_id,
                        purpose="update",
                        materialization_request_id=resolved_request_id,
                        tables=plan.added,
                        release=plan.dropped,
                        actor_id=submitted_by_id,
                        actor_email=submitted_by,
                        submitted_at=resolved_accepted_at,
                    ),
                    principal=principal,
                )
            changes.update(tables=plan.tables, retired_tables=plan.retired_tables)
        else:
            # Nothing to do with the topics: the request only repeated them.
            topic_change, plan = None, None
    if replicas != materialization.replicas:
        changes["replicas"] = replicas
    if changes:
        changes["spec_version"] = materialization.spec_version + 1
    if autoscaling is not None and autoscaling != materialization.autoscaling:
        # Not part of the runtime's spec: its version stays.
        changes["autoscaling"] = autoscaling
    if label is not None and label != materialization.name:
        changes["name"] = label
    else:
        label = None
    acceptance = _request_change(
        table,
        materialization,
        desired_phase=materialization.desired_phase,
        action="update",
        submitted_by=submitted_by,
        submitted_by_id=submitted_by_id,
        request_id=resolved_request_id,
        accepted_at=resolved_accepted_at,
        config=cfg,
        changes=changes,
        topic_change=topic_change,
        claim_request_id=resolved_claim_id,
        label=label,
    )
    acceptance.response["name"] = acceptance.materialization.name
    acceptance.response["live_tail"] = True
    if plan is not None:
        acceptance.response["topic_change"] = _describe_topic_change(plan)
    if resolved_claim_id is not None:
        acceptance.response["claim_request_id"] = resolved_claim_id
        acceptance.response["claim_status_url"] = f"/identity/requests/{resolved_claim_id}"
    return acceptance


def _absent(value: Any) -> bool:
    return value is None or value == "" or value == {} or value == []


def _topic_change_request(
    body: dict[str, Any],
    materialization: Materialization,
    records: list[Any],
    *,
    directory_table: TableProtocol | None,
    config: ControlPlaneConfig,
) -> TopicChangeRequest | None:
    """The topics an update reads from now on, with the tables of new ones, and what becomes of
    tables; None when it changes neither."""
    tables = body.get("tables")
    actions = body.get("removed_tables")
    if _absent(tables) and _absent(actions):
        return None
    existing = {spec.source_topic: spec for spec in materialization.all_tables}
    if _absent(tables):
        # Dropping a kept table changes no topic.
        topics, new_tables = tuple(materialization.source_topics), ()
    else:
        taken = {spec.name for spec in materialization.all_tables} | {record.name for record in records}
        topics, new_tables = requested_tables(
            tables,
            maximum=config.max_materialization_topics,
            directory_table=directory_table,
            existing=existing,
            taken=taken,
        )
    if actions is None:
        actions = {}
    if not isinstance(actions, dict) or not all(isinstance(name, str) and isinstance(action, str) for name, action in actions.items()):
        raise invalid_request(
            'removed_tables must map each table to "drop" or "keep"',
            field_name="removed_tables",
        )
    try:
        return TopicChangeRequest(source_topics=topics, table_actions=dict(actions), new_tables=new_tables)
    except ValueError as error:
        raise invalid_request(str(error), field_name="removed_tables" if actions else "tables") from error


def _require_dataset_write(
    principal: Principal | None,
    specs: tuple[TableSpec, ...],
    directory_table: TableProtocol | None,
) -> None:
    """Refuse a caller who may not add or drop tables in every dataset these tables are in."""
    if principal is None:
        return
    seen: set[str] = set()
    for spec in specs:
        if spec.dataset_id in seen:
            continue
        seen.add(spec.dataset_id)
        dataset = get_dataset(directory_table, spec.dataset_id) if directory_table is not None else None
        _require(principal, "dataset.write", dataset=dataset or _dataset_of(spec))


def _describe_topic_change(plan: TopicChangePlan) -> dict[str, Any]:
    """What an accepted change of topics does to each table, for the client to show at once."""

    def names(specs: tuple[TableSpec, ...]) -> list[dict[str, str]]:
        return [spec.to_dict() for spec in specs]

    return {
        "tables": names(plan.tables),
        "added": names(plan.added),
        "resumed": names(plan.resumed),
        "kept": names(plan.retired),
        "dropped": names(plan.dropped),
    }


def delete_materialization(
    table: TableProtocol,
    materialization_id: str,
    *,
    submitted_by: str,
    submitted_by_id: str | None = None,
    principal: Principal | None = None,
    directory_table: TableProtocol | None = None,
    request_id: str | None = None,
    accepted_at: str | None = None,
    config: ControlPlaneConfig | None = None,
) -> MutationAcceptance:
    """Request deletion of a persisted materialization and its runtime.

    Its tables leave their datasets, so the caller needs `dataset.write` on each of them.
    """
    materialization = _require_materialization(table, materialization_id)
    _require_dataset_write(principal, materialization.all_tables, directory_table)
    return _request_change(
        table,
        materialization,
        desired_phase="deleting",
        action="delete",
        submitted_by=submitted_by,
        submitted_by_id=submitted_by_id,
        request_id=request_id,
        accepted_at=accepted_at,
        config=config,
    )


def _require_not_deleting(materialization: Materialization, *, verb: str) -> None:
    if materialization.desired_phase in _DELETION_PHASES:
        raise conflict(
            f"materialization {materialization.materialization_id} is being deleted and cannot be {verb}",
            reason="materialization_deleting",
        )


def _require_deployed(table: TableProtocol, materialization: Materialization, *, doing: str) -> None:
    """Refuse a change that needs a runtime before its tables exist.

    Stopping and rescaling both re-apply the runtime, and applying needs every table, which follow
    discovery; asking before then would fail in the workflow instead of here.
    """
    created = {record.name for record in list_materialization_tables(table, materialization.materialization_id)}
    if not set(materialization.table_names) <= created:
        raise conflict(
            f"materialization {materialization.materialization_id} has no runtime to {doing} yet; "
            "try again once it is deployed",
            reason="runtime_not_deployed",
        )


def _request_change(
    table: TableProtocol,
    materialization: Materialization,
    *,
    desired_phase: str,
    action: str,
    submitted_by: str,
    request_id: str | None,
    accepted_at: str | None,
    config: ControlPlaneConfig | None,
    changes: dict[str, Any] | None = None,
    submitted_by_id: str | None = None,
    topic_change: TopicChangeRequest | None = None,
    claim_request_id: str | None = None,
    label: str | None = None,
) -> MutationAcceptance:
    cfg = get_config() if config is None else config
    materialization_id = materialization.materialization_id
    resolved_request_id = request_id or _new_identifier("req")
    resolved_accepted_at = accepted_at or _utc_now()
    updated_materialization = replace(
        materialization,
        desired_phase=desired_phase,
        updated_at=resolved_accepted_at,
        **(changes or {}),
    )
    existing_status = get_materialization_status(table, materialization_id)
    status = _fallback_status(materialization) if existing_status is None else existing_status
    updated_status = replace(status, desired_phase=desired_phase)

    request_event = MaterializationRequestEvent(
        request_id=resolved_request_id,
        materialization_id=materialization_id,
        action=action,
        submitted_at=resolved_accepted_at,
        submitted_by=submitted_by,
        spec=_materialization_to_request_spec(updated_materialization, cfg),
        submitted_by_id=submitted_by_id,
        topic_change=topic_change,
        claim_request_id=claim_request_id,
        label=label,
    )
    response = build_accepted_response(
        materialization_id=materialization_id,
        request_id=resolved_request_id,
        desired_phase=updated_materialization.desired_phase,
        observed_phase=updated_status.observed_phase,
        phase_reason=updated_status.phase_reason,
        accepted_at=resolved_accepted_at,
    )
    acceptance = MutationAcceptance(
        response=response,
        request_event=request_event,
        materialization=updated_materialization,
        status=updated_status,
    )
    return acceptance


def _refuse_live_tail_off(body: dict[str, Any]) -> None:
    """Refuse a request for a materialization without a live tail; `live_tail: true` changes nothing.

    Before 4.3 each materialization could switch its live tail off; now every one has it, and only
    the deployment's `live_tail_enabled` turns it off. A 4.1 console still sends `true` back, and
    is told why when it sends `false`.
    """
    if "live_tail" in body and body["live_tail"] is not True:
        raise invalid_request(LIVE_TAIL_ALWAYS, field_name="live_tail")


def _normalize_request_spec(
    body: dict[str, Any],
    *,
    config: ControlPlaneConfig,
    materialization_id: str,
    directory_table: TableProtocol | None,
) -> tuple[MaterializationRequestSpec, str]:
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    if "tables" not in body and ("source_topics" in body or "source_topic" in body):
        raise invalid_request(
            "source_topics is now tables: a list of {source_topic, dataset, name} - each topic's "
            "table, the dataset it is in and, optionally, its name",
            field_name="tables",
        )
    resolved_runtime_name = normalize_runtime_app_name(config.runtime_name_for_materialization(materialization_id))
    _refuse_deployment_choices(body, config=config, runtime_name=resolved_runtime_name)
    _refuse_live_tail_off(body)
    destination_uri = deployment_destination(config)
    topics, tables = requested_tables(
        body.get("tables"),
        maximum=config.max_materialization_topics,
        directory_table=directory_table,
    )
    try:
        name = normalize_materialization_label(body.get("name"))
    except InvalidNameError as error:
        raise invalid_request(str(error), field_name=error.field_name) from error
    runtime_mode = body.get("runtime_mode", "dedicated")
    labels = body.get("labels") or {}
    annotations = body.get("annotations") or {}
    if not isinstance(labels, dict):
        raise invalid_request("labels must be an object", field_name="labels")
    if not isinstance(annotations, dict):
        raise invalid_request("annotations must be an object", field_name="annotations")
    skip_tombstones = body.get("skip_tombstones")
    if skip_tombstones is None:
        skip_tombstones = False
    if not isinstance(skip_tombstones, bool):
        raise invalid_request("skip_tombstones must be a boolean", field_name="skip_tombstones")
    autoscaling = body.get("autoscaling")
    if autoscaling is None:
        autoscaling = True
    if not isinstance(autoscaling, bool):
        raise invalid_request("autoscaling must be a boolean", field_name="autoscaling")
    # Resolved now, like the other defaults, so a later settings change does not affect the request.
    partition_granularity = body.get("partition_granularity")
    if partition_granularity in (None, ""):
        partition_granularity = config.default_partition_granularity
    if partition_granularity not in PARTITION_GRANULARITIES:
        raise invalid_request(
            f"partition_granularity must be one of: {', '.join(PARTITION_GRANULARITIES)}",
            field_name="partition_granularity",
        )

    try:
        replicas = normalize_replicas(
            body.get("replicas"),
            default=config.default_runtime_replicas,
            maximum=config.max_runtime_replicas,
        )
    except InvalidScaleError as error:
        raise invalid_request(str(error), field_name=error.field_name) from error
    try:
        spec = MaterializationRequestSpec(
            name=name,
            tables=tables,
            destination_uri=destination_uri,
            runtime_mode=runtime_mode,
            runtime_name=resolved_runtime_name,
            namespace=config.default_namespace,
            resource_profile=body.get("resource_profile"),
            storage_size=config.default_runtime_storage_size,
            replicas=replicas,
            skip_tombstones=skip_tombstones,
            autoscaling=autoscaling,
            partition_granularity=partition_granularity,
            labels=dict(labels),
            annotations=dict(annotations),
        )
    except ValueError as error:
        field_name = _infer_field_name_from_error(str(error))
        raise invalid_request(str(error), field_name=field_name) from error
    return spec, resolved_runtime_name


def _refuse_deployment_choices(body: dict[str, Any], *, config: ControlPlaneConfig, runtime_name: str) -> None:
    """Refuse what a create request cannot choose: every materialization takes the deployment's.

    Its tables go under the deployment's destination, its runtime runs in the deployment's
    namespace under a name generated from its id, and each member gets the deployment's storage.
    A request that names one is refused rather than quietly overruled.
    """
    destination = config.default_destination_uri
    reasons = {
        "destination_uri": (
            f"its tables are created under the deployment's destination, {destination}"
            if destination
            else "its tables are created under the deployment's destination"
        ),
        "namespace": f"its runtime runs in the deployment's namespace, {config.default_namespace}",
        "runtime_name": f"its runtime's name is generated from its id: {runtime_name}",
        "storage_size": f"every runtime member gets the deployment's storage, {config.default_runtime_storage_size}",
    }
    for field_name, reason in reasons.items():
        if body.get(field_name) is not None:
            raise invalid_request(f"{field_name} is not chosen per materialization: {reason}", field_name=field_name)


def deployment_destination(config: ControlPlaneConfig) -> str:
    """Where every new materialization's tables are created; refused while none is configured."""
    destination = config.default_destination_uri
    if not destination:
        raise conflict(
            "No materialization can be created: this control plane has no destination for tables. "
            "An administrator sets one on the Settings page, under Materializations.",
            reason="destination_not_configured",
        )
    # Refused here, not when the tables are created: they live in Google Cloud Storage.
    if not is_gcs_destination_uri(destination):
        raise conflict(
            f"No materialization can be created: the destination for tables, {destination}, is not a "
            "gs:// URI. An administrator sets one on the Settings page, under Materializations.",
            reason="destination_not_configured",
        )
    return destination


def requested_tables(
    value: Any,
    *,
    maximum: int,
    directory_table: TableProtocol | None,
    existing: dict[str, TableSpec] | None = None,
    taken: set[str] | None = None,
) -> tuple[tuple[str, ...], tuple[TableSpec, ...]]:
    """The topics a request names, in order, and the tables of those `existing` has none of.

    Each entry is `{source_topic, dataset, name?}`. A topic already in `existing` (an update's
    written and kept tables) keeps its table, so it may only repeat its dataset and name. A new
    topic needs a dataset, which must exist; its name, when not given, is derived from the topic,
    and must differ from every name in `taken` and of the request's other tables.
    """
    existing = existing or {}
    used = {name.lower() for name in taken or set()}
    if value is None or value == []:
        raise invalid_request("tables is required: choose at least one topic, with the dataset its table goes in", field_name="tables")
    if not isinstance(value, list):
        raise invalid_request("tables must be a list of {source_topic, dataset, name}", field_name="tables")
    if len(value) > maximum:
        raise invalid_request(f"a materialization reads at most {maximum} topics; {len(value)} were given", field_name="tables")
    topics: list[str] = []
    new_tables: list[TableSpec] = []
    datasets: dict[str, Dataset] = {}
    for index, item in enumerate(value):
        field = f"tables.{index}"
        if not isinstance(item, dict):
            raise invalid_request("each table is {source_topic, dataset, name}", field_name=field)
        unknown = sorted(set(item) - {"source_topic", "dataset", "dataset_id", "name"})
        if unknown:
            raise invalid_request(f"{unknown[0]} is not something a table has", field_name=f"{field}.{unknown[0]}")
        source = item.get("source_topic")
        if not isinstance(source, str) or not source.strip():
            raise invalid_request("each table needs the topic it holds: source_topic", field_name=f"{field}.source_topic")
        try:
            (topic,) = normalize_source_topics([source], maximum=1)
        except InvalidSourceTopicsError as error:
            raise invalid_request(str(error), field_name=f"{field}.source_topic") from error
        if topic in topics:
            raise invalid_request(f"topic {topic} is chosen twice; each topic has one table", field_name=f"{field}.source_topic")
        topics.append(topic)
        current = existing.get(topic)
        if current is not None:
            if item.get("dataset") not in (None, "", current.dataset) or item.get("name") not in (None, "", current.name):
                raise invalid_request(
                    f"topic {topic} has table {current.qualified_name}, and a table's dataset and name never "
                    "change; to put the topic in another dataset, drop its table first",
                    field_name=field,
                )
            continue
        try:
            dataset_name = normalize_dataset_name(item.get("dataset"), field_name=f"{field}.dataset")
        except InvalidNameError as error:
            raise invalid_request(str(error), field_name=error.field_name) from error
        dataset = datasets.get(dataset_name) or find_dataset_by_name(directory_table, dataset_name)
        if dataset is None:
            error = not_found(f"There is no dataset {dataset_name}.", resource="dataset", identifier=dataset_name)
            error.details.update(field=f"{field}.dataset", reason="dataset_not_found", dataset=dataset_name, source_topic=topic)
            raise error
        datasets[dataset_name] = dataset
        if item.get("name") in (None, ""):
            name = _derived_name(topic, used)
        else:
            try:
                name = normalize_table_name(item.get("name"), field_name=f"{field}.name")
            except InvalidNameError as error:
                raise invalid_request(str(error), field_name=error.field_name) from error
            if name.lower() in used:
                raise invalid_request(
                    f"this materialization already has a table named {name}; choose another name",
                    field_name=f"{field}.name",
                )
        used.add(name.lower())
        new_tables.append(TableSpec(name=name, source_topic=topic, dataset=dataset.name, dataset_id=dataset.dataset_id))
    return tuple(topics), tuple(new_tables)


def _derived_name(topic: str, used: set[str]) -> str:
    """The name a topic's table gets when none is chosen: derived from the topic, with the lowest
    free `_2`, `_3`, … when the materialization has that name already."""
    base = table_name_for_topic(topic)
    name = base
    suffix = 2
    while name.lower() in used:
        ending = f"_{suffix}"
        name = f"{base[: TABLE_NAME_MAX_LENGTH - len(ending)].rstrip('_')}{ending}"
        suffix += 1
    return name


def _preview_claim(directory_table: TableProtocol | None, request: Any, *, principal: Principal | None) -> None:
    """Refuse now what the directory agent would refuse when it claims these names."""
    if directory_table is None:
        return
    try:
        preview_identity_request(directory_table, request)
    except IdentityRejection as rejection:
        raise claim_error(rejection) from rejection


def claim_error(rejection: IdentityRejection) -> WebError:
    """The HTTP error for a claim of table names the directory refuses: what it was about in `details`."""
    details = {key: value for key, value in rejection.details.items() if value is not None}
    details["reason"] = rejection.reason
    if rejection.field:
        details["field"] = rejection.field
    if rejection.reason == "dataset_not_found":
        error = not_found(rejection.message, resource="dataset", identifier=str(details.get("dataset") or ""))
    elif rejection.reason == "forbidden":
        error = forbidden(rejection.message, permission="dataset.write")
    elif rejection.reason == "invalid":
        error = invalid_request(rejection.message, field_name=rejection.field)
    else:
        error = conflict(rejection.message, reason=rejection.reason)
    error.details.update(details)
    return error


def _materialization_to_request_spec(
    materialization: Materialization,
    config: ControlPlaneConfig,
) -> MaterializationRequestSpec:
    resolved_runtime_name = resolve_runtime_app_name(materialization, config=config)
    return MaterializationRequestSpec(
        name=materialization.name,
        tables=materialization.tables,
        destination_uri=materialization.destination_uri,
        runtime_mode=materialization.runtime_mode,
        runtime_name=resolved_runtime_name,
        namespace=materialization.namespace,
        resource_profile=materialization.resource_profile,
        storage_size=materialization.storage_size,
        replicas=materialization.replicas,
        skip_tombstones=materialization.skip_tombstones,
        autoscaling=materialization.autoscaling,
        partition_granularity=materialization.partition_granularity,
        labels=dict(materialization.labels),
        annotations=dict(materialization.annotations),
    )


def _fallback_status(materialization: Materialization) -> MaterializationStatus:
    resolved_runtime_name = resolve_runtime_app_name(materialization)
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
            kaspr_app_name=resolved_runtime_name,
            resource_exists=False,
            managed_agent_states={},
        ),
    )


def _infer_field_name_from_error(message: str) -> str | None:
    # A message names the field it is about; topics are named in the singular ("source topics").
    fields = (
        ("source topic", "source_topics"),
        ("table", "source_topics"),
        ("destination_uri", "destination_uri"),
        ("runtime_mode", "runtime_mode"),
    )
    for fragment, field_name in fields:
        if fragment in message:
            return field_name
    return None


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"