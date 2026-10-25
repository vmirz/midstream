"""WebView handlers for datasets: list and read them, create, change and delete them, and share them.

A dataset is a named group of tables and their Iceberg namespace: SQL reads `<dataset>.<table>`.
Everyone with `datasets.view` sees every dataset - its name, description, owners and its tables'
names - and a dataset they cannot read is marked `locked`: its schemas and data need a role on it.
Reads answer from the directory - datasets, their grants and the claims of their tables' names -
and, for what each table is, from the state table. A change is validated here, with the identity
agent's own checks against the directory as it stands, and published; the agent applies it, and the
client waits on `GET /identity/requests/{request_id}` for the outcome. Nothing here writes a table.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from materializer_control_plane.config import ControlPlaneConfig
from materializer_control_plane.identity.authorization import require_dataset
from materializer_control_plane.identity.directory import DirectoryView
from materializer_control_plane.identity.permissions import DATASET_OWNER_ROLE_ID, GUEST_GROUP_ID, dataset_grantable, expand_permissions
from materializer_control_plane.identity.principals import Grants, Principal
from materializer_control_plane.identity.validation import IdentityValidationError, clean_description, clean_ids
from materializer_control_plane.models import Dataset, DatasetGrant, MaterializationTable, Role
from materializer_control_plane.services.identity import new_dataset_id
from materializer_control_plane.storage import get_materialization, get_materialization_table, iter_materialization_tables
from materializer_control_plane.storage.identity import get_table_claim, iter_table_claims
from materializer_control_plane.storage.table import TableProtocol
from materializer_control_plane.table_names import dataset_name_problem
from materializer_control_plane.web.errors import conflict, invalid_request, not_found
from materializer_control_plane.web.identity import (
    IdentityAcceptance,
    accept_request,
    directory_request,
    preview_request,
)

#: The states a table of a dataset is listed in: written, kept, or on its way out.
LISTED_TABLE_STATES = frozenset({"active", "retired", "dropping"})


def list_datasets_view(
    directory_table: TableProtocol | None,
    principal: Principal,
    query: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Every dataset, by name: `?q=` finds them by name, description or table name."""
    view = DirectoryView.load(directory_table)
    search = str((query or {}).get("q") or "").strip().lower()
    claims_by_dataset: dict[str, list[str]] = {}
    for claim in iter_table_claims(directory_table):
        claims_by_dataset.setdefault(claim.dataset_id, []).append(claim.table)
    items = []
    for dataset in sorted(view.datasets.values(), key=lambda item: item.name):
        tables = sorted(claims_by_dataset.get(dataset.dataset_id, []))
        if search and not (
            search in dataset.name
            or search in dataset.description.lower()
            or any(search in table for table in tables)
        ):
            continue
        items.append(describe_dataset(dataset, view, principal, tables=tables))
    return {
        "datasets": items,
        "returned": len(items),
        "can_create": principal.can("datasets.create"),
    }


def get_dataset_view(
    directory_table: TableProtocol | None,
    control_plane_table: TableProtocol,
    principal: Principal,
    name: str,
) -> dict[str, Any]:
    """One dataset and its tables: each table's name, topic, state and materialization.

    A table whose name is claimed but which is not created yet reads `pending`; one being dropped
    is listed until it is gone. Schemas are on the tables' own endpoints.
    """
    dataset = require_dataset(directory_table, name)
    view = DirectoryView.load(directory_table)
    tables = _dataset_tables(directory_table, control_plane_table, dataset)
    return {
        **describe_dataset(dataset, view, principal, tables=[table["name"] for table in tables if table["state"] != "dropping"]),
        "tables": tables,
    }


def get_dataset_table_view(
    directory_table: TableProtocol | None,
    control_plane_table: TableProtocol,
    principal: Principal,
    dataset_name: str,
    table_name: str,
) -> dict[str, Any]:
    """Resolve `<dataset>.<table>` to its table and the materialization that writes it.

    Where it is, and its metadata, only for a caller who may read the dataset.
    """
    dataset = require_dataset(directory_table, dataset_name)
    claim = get_table_claim(directory_table, dataset.dataset_id, table_name)
    record = None if claim is None else get_materialization_table(control_plane_table, claim.materialization_id, claim.table)
    if claim is None:
        raise not_found(
            f"Dataset {dataset.name} has no table {table_name}.",
            resource="dataset-table",
            identifier=f"{dataset.name}.{table_name}",
        )
    materialization = get_materialization(control_plane_table, claim.materialization_id)
    readable = principal.can("dataset.view_schema", dataset.dataset_id)
    payload: dict[str, Any] = {
        "dataset": dataset.name,
        "dataset_id": dataset.dataset_id,
        "name": claim.table,
        "qualified_name": f"{dataset.name}.{claim.table}",
        "source_topic": claim.source_topic,
        "state": "pending" if record is None else record.state,
        "materialization_id": claim.materialization_id,
        "materialization_name": None if materialization is None else materialization.label,
        "readable": readable,
        "queryable": principal.can("data.query", dataset.dataset_id),
    }
    if readable and record is not None:
        payload.update(
            {
                "location": record.location,
                "table_uuid": record.table_uuid,
                "metadata_location": record.metadata_location,
                "current_snapshot_id": record.current_snapshot_id,
                "schema_id": record.schema_id,
                "partition_granularity": record.partition_granularity,
                "last_commit_at": record.last_commit_at,
                "valid_through": record.valid_through,
                "totals": dict(record.totals),
            }
        )
    return payload


def create_dataset_request(
    directory_table: TableProtocol,
    principal: Principal,
    body: Any,
    *,
    request: Any,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Create a dataset: its creator owns it, and everyone signed in may read it.

    Body: `{"name": "sales", "description": "Orders, refunds and the rest of sales"}`.
    """
    options = _object(body, allowed={"name", "description"})
    problem = dataset_name_problem(options.get("name"))
    if problem is not None:
        raise invalid_request(problem[0].upper() + problem[1:], field_name="name")
    name = str(options["name"]).strip()
    dataset_id = new_dataset_id()
    event = directory_request(
        principal,
        "create_dataset",
        {"dataset_id": dataset_id, "name": name, "description": _description(options.get("description"))},
        request=request,
    )
    preview_request(directory_table, event, config)
    acceptance = accept_request(event, "dataset", dataset_id)
    acceptance.response["name"] = name
    return acceptance


def update_dataset_request(
    directory_table: TableProtocol,
    principal: Principal,
    name: str,
    body: Any,
    *,
    request: Any,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Change a dataset's description. Body: `{"description": "...", "base_revision": 3}`."""
    dataset = require_dataset(directory_table, name)
    options = _object(body, allowed={"description", "base_revision", "name"})
    if "name" in options and options["name"] != dataset.name:
        raise invalid_request("A dataset's name never changes.", field_name="name")
    payload: dict[str, Any] = {"dataset_id": dataset.dataset_id}
    if "description" in options:
        payload["description"] = _description(options["description"])
    event = directory_request(
        principal,
        "update_dataset",
        payload,
        request=request,
        base_revision=_base_revision(options.get("base_revision")),
    )
    preview_request(directory_table, event, config)
    return accept_request(event, "dataset", dataset.dataset_id)


def delete_dataset_request(
    directory_table: TableProtocol,
    control_plane_table: TableProtocol,
    principal: Principal,
    name: str,
    *,
    request: Any,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Delete a dataset that holds no tables - none written, kept, or still being dropped."""
    dataset = require_dataset(directory_table, name)
    dropping = sorted(
        record.name
        for record in iter_materialization_tables(control_plane_table)
        if record.dataset_id == dataset.dataset_id and record.state == "dropping"
    )
    if dropping:
        raise conflict(
            f"Dataset {dataset.name} still holds tables being dropped: {', '.join(dropping)}. Try again once they are gone.",
            reason="dataset_not_empty",
        )
    event = directory_request(principal, "delete_dataset", {"dataset_id": dataset.dataset_id}, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "dataset", dataset.dataset_id)


def list_dataset_grants_view(directory_table: TableProtocol | None, principal: Principal, name: str) -> dict[str, Any]:
    """Who may use a dataset, and with which roles; the roles the caller can grant on it; and whom
    they can grant them to.

    `candidates` are the active people and every group but Guest, so an owner who may not browse
    users and groups can still share the dataset; the caller already holds `dataset.manage` on it.
    """
    dataset = require_dataset(directory_table, name)
    view = DirectoryView.load(directory_table)
    grants = [describe_grant(grant, view) for grant in view.grants_on(dataset.dataset_id)]
    grants.sort(key=lambda item: (item["principal_type"] != "group", item["name"].lower()))
    actor = principal.grants
    return {
        "dataset": dataset.name,
        "dataset_id": dataset.dataset_id,
        "grants": grants,
        "roles": [
            {**_role_ref(role), "grantable": _can_grant(actor, role, dataset.dataset_id)}
            for role in view.roles.values()
            if dataset_grantable(role.permissions)
        ],
        "candidates": {
            "users": sorted(
                (
                    {"user_id": user.user_id, "name": user.display_name or user.email, "email": user.email}
                    for user in view.users.values()
                    if user.active
                ),
                key=lambda item: item["name"].lower(),
            ),
            "groups": sorted(
                (
                    {"group_id": group.group_id, "name": group.name}
                    for group in view.groups.values()
                    if group.group_id != GUEST_GROUP_ID
                ),
                key=lambda item: item["name"].lower(),
            ),
        },
    }


def set_dataset_grant_request(
    directory_table: TableProtocol,
    principal: Principal,
    name: str,
    principal_id: str,
    body: Any,
    *,
    request: Any,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Set the roles a person or a group holds on a dataset. Body: `{"role_ids": ["dataset-reader"]}`;
    an empty list removes their grant."""
    dataset = require_dataset(directory_table, name)
    options = _object(body, allowed={"role_ids", "principal_type"})
    try:
        role_ids = clean_ids(options.get("role_ids"), field="role_ids")
    except IdentityValidationError as error:
        raise invalid_request(str(error), field_name=error.field) from error
    view = DirectoryView.load(directory_table)
    principal_type = options.get("principal_type")
    if principal_type is None:
        principal_type = "user" if principal_id in view.users else "group" if principal_id in view.groups else None
    if principal_type not in ("user", "group"):
        raise not_found("There is no person or group with that id.", resource="principal", identifier=principal_id)
    event = directory_request(
        principal,
        "set_dataset_grant",
        {
            "dataset_id": dataset.dataset_id,
            "principal_type": principal_type,
            "principal_id": principal_id,
            "role_ids": role_ids,
        },
        request=request,
    )
    preview_request(directory_table, event, config)
    return accept_request(event, "dataset", dataset.dataset_id)


# ---------------------------------------------------------------------------------------- helpers


def describe_dataset(dataset: Dataset, view: DirectoryView, principal: Principal, *, tables: list[str]) -> dict[str, Any]:
    """A dataset as lists show it: what it is, who owns it, its tables' names, and what the caller may
    do with it. `locked` when the caller cannot read it."""
    held = principal.grants.on(dataset.dataset_id)
    return {
        "dataset_id": dataset.dataset_id,
        "name": dataset.name,
        "description": dataset.description,
        "revision": dataset.revision,
        "created_at": dataset.created_at,
        "created_by": dataset.created_by,
        "updated_at": dataset.updated_at,
        "updated_by": dataset.updated_by,
        "owners": owners(dataset, view),
        "tables": tables,
        "table_count": len(tables),
        "permissions": sorted(held),
        "locked": "dataset.view_schema" not in held,
    }


def owners(dataset: Dataset, view: DirectoryView) -> list[dict[str, str]]:
    """Whoever may manage a dataset through a grant on it: the people and groups whose roles there
    include `dataset.manage`."""
    found = []
    for grant in view.grants_on(dataset.dataset_id):
        permissions = expand_permissions(
            permission
            for role_id in grant.role_ids
            if (role := view.roles.get(role_id)) is not None
            for permission in role.permissions
        )
        if "dataset.manage" in permissions or DATASET_OWNER_ROLE_ID in grant.role_ids:
            found.append(_principal_ref(grant, view))
    return sorted(found, key=lambda item: (item["type"] != "user", item["name"].lower()))


def describe_grant(grant: DatasetGrant, view: DirectoryView) -> dict[str, Any]:
    reference = _principal_ref(grant, view)
    return {
        "principal_type": grant.principal_type,
        "principal_id": grant.principal_id,
        "name": reference["name"],
        "email": reference.get("email"),
        "role_ids": list(grant.role_ids),
        "roles": [_role_ref(view.roles[role_id]) for role_id in grant.role_ids if role_id in view.roles],
        "updated_at": grant.updated_at,
        "updated_by": grant.updated_by,
    }


def _dataset_tables(
    directory_table: TableProtocol | None,
    control_plane_table: TableProtocol,
    dataset: Dataset,
) -> list[dict[str, Any]]:
    tables: dict[tuple[str, str], dict[str, Any]] = {}
    labels: dict[str, str | None] = {}

    def label(materialization_id: str) -> str | None:
        if materialization_id not in labels:
            materialization = get_materialization(control_plane_table, materialization_id)
            labels[materialization_id] = None if materialization is None else materialization.label
        return labels[materialization_id]

    for claim in iter_table_claims(directory_table, dataset.dataset_id):
        record = get_materialization_table(control_plane_table, claim.materialization_id, claim.table)
        tables[(claim.materialization_id, claim.table)] = _describe_table(
            claim.table,
            claim.source_topic,
            "pending" if record is None else record.state,
            claim.materialization_id,
            label(claim.materialization_id),
            record,
        )
    # Being dropped, a table has left the catalog and freed its name, but it is still there.
    for record in iter_materialization_tables(control_plane_table):
        if record.dataset_id == dataset.dataset_id and record.state == "dropping":
            tables.setdefault(
                (record.materialization_id, record.name),
                _describe_table(record.name, record.source_topic, record.state, record.materialization_id, label(record.materialization_id), record),
            )
    return sorted(tables.values(), key=lambda item: (item["name"], item["materialization_id"]))


def _describe_table(
    name: str,
    source_topic: str,
    state: str,
    materialization_id: str,
    materialization_name: str | None,
    record: MaterializationTable | None,
) -> dict[str, Any]:
    return {
        "name": name,
        "source_topic": source_topic,
        "state": state,
        "materialization_id": materialization_id,
        "materialization_name": materialization_name,
        "created_at": None if record is None else record.created_at,
        "last_commit_at": None if record is None else record.last_commit_at,
    }


def _principal_ref(grant: DatasetGrant, view: DirectoryView) -> dict[str, str]:
    if grant.principal_type == "user":
        user = view.users.get(grant.principal_id)
        if user is None:
            return {"type": "user", "id": grant.principal_id, "name": grant.principal_id}
        return {"type": "user", "id": user.user_id, "name": user.display_name or user.email, "email": user.email}
    group = view.groups.get(grant.principal_id)
    return {"type": "group", "id": grant.principal_id, "name": group.name if group is not None else grant.principal_id}


def _role_ref(role: Role) -> dict[str, Any]:
    return {"role_id": role.role_id, "name": role.name, "built_in": role.built_in, "permissions": sorted(role.permissions)}


def _can_grant(actor: Grants, role: Role, dataset_id: str) -> bool:
    """Whether the caller holds, on the dataset, everything the role would give there."""
    permissions = frozenset(expand_permissions(role.permissions))
    return actor.covers(Grants(group_ids=(), role_ids=(), permissions=frozenset(), datasets={dataset_id: permissions}))


def _object(body: Any, *, allowed: set[str]) -> dict[str, Any]:
    if body is None:
        return {}
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    unknown = sorted(set(body) - allowed)
    if unknown:
        raise invalid_request(f"{unknown[0]} is not something a dataset has", field_name=unknown[0])
    return body


def _description(value: Any) -> str:
    try:
        return clean_description(value)
    except IdentityValidationError as error:
        raise invalid_request(str(error), field_name=error.field) from error


def _base_revision(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise invalid_request("base_revision must be a positive whole number", field_name="base_revision")
    return value
