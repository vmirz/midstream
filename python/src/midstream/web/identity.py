"""WebView handlers for people, groups, roles, the permission catalog and the audit log.

Reads answer from the directory. A change is validated here — by running the identity agent's
own checks against the directory as it stands — and published; the agent applies it, and the
client waits on `GET /identity/requests/{request_id}` for the outcome. Nothing here writes a table.

Every function takes the `Principal` its WebView authorized, so none can be called without one.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from midstream.config import ControlPlaneConfig
from midstream.identity.authorization import request_context, require
from midstream.identity.directory import DirectoryView
from midstream.identity.passwords import password_problem
from midstream.identity.permissions import (
    PERMISSIONS,
    built_in_group,
    dataset_grantable,
    describe_catalog,
    expand_permissions,
)
from midstream.identity.principals import Grants, Principal, user_group_ids
from midstream.identity.validation import (
    IdentityValidationError,
    clean_email,
    clean_person_name,
)
from midstream.models.identity import Group, IdentityRequest, Role, User
from midstream.services.identity import (
    IdentityRejection,
    new_group_id,
    new_role_id,
    new_user_id,
    preview_identity_request,
)
from midstream.settings import resolve_config
from midstream.storage.identity import (
    activity_by_user,
    get_request_outcome,
    get_sign_in_guard,
    get_user_activity,
    iter_audit_entries,
    iter_sign_in_guards,
    iter_user_sessions,
)
from midstream.storage.table import TableProtocol
from midstream.web.auth import describe_session, hash_new_password
from midstream.web.errors import (
    WebError,
    already_exists,
    conflict,
    forbidden,
    invalid_request,
    not_found,
    unauthenticated,
)

USER_FILTERS = ("all", "active", "deactivated", "locked", "password_change")
USER_SORTS = ("name", "last_active", "created")
AUDIT_PAGE_LIMIT = 200
#: A password a preview stands in with; the real one is hashed only once the preview passes.
_PREVIEW_HASH = "$scrypt$preview"


@dataclass(frozen=True, slots=True)
class IdentityAcceptance:
    """The 202 response to a directory change, and the request that carries it."""

    response: dict[str, Any]
    request_event: IdentityRequest

    def to_value(self) -> dict[str, Any]:
        return {"response": self.response, "request_event": self.request_event.to_dict()}


# ------------------------------------------------------------------------------------------- users


def list_users_view(
    directory_table: TableProtocol | None,
    principal: Principal,
    query: Mapping[str, Any] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Every person, with their groups, status and last activity.

    `status` narrows the list (`active`, `deactivated`, `locked`, `password_change`), `q`
    searches names and emails, and `sort` orders it by `name`, `last_active` or `created`.
    `counts` gives the size of every status filter, whatever the one chosen.
    """
    require(principal, "users.view")
    params = dict(query or {})
    status = str(params.get("status") or "all")
    if status not in USER_FILTERS:
        raise invalid_request(f"status must be one of: {', '.join(USER_FILTERS)}", field_name="status")
    sort = str(params.get("sort") or "name")
    if sort not in USER_SORTS:
        raise invalid_request(f"sort must be one of: {', '.join(USER_SORTS)}", field_name="sort")
    search = str(params.get("q") or "").strip().lower()
    current = now or datetime.now(tz=UTC)
    view = DirectoryView.load(directory_table)
    activities = activity_by_user(directory_table)
    locked = {
        guard.email: guard.locked_until
        for guard in iter_sign_in_guards(directory_table)
        if _after(guard.locked_until, current)
    }
    items = [
        _describe_user(user, view, principal, activity=activities.get(user.user_id), locked_until=locked.get(user.email))
        for user in view.users.values()
    ]
    counts = {name: sum(1 for item in items if _matches_status(item, name)) for name in USER_FILTERS}
    items = [item for item in items if _matches_status(item, status)]
    if search:
        items = [
            item
            for item in items
            if search in item["email"] or search in item["display_name"].lower()
        ]
    if sort == "last_active":
        items.sort(key=lambda item: item["last_active_at"] or "", reverse=True)
    elif sort == "created":
        items.sort(key=lambda item: item["created_at"], reverse=True)
    else:
        items.sort(key=lambda item: (item["display_name"].lower(), item["email"]))
    return {"users": items, "counts": counts, "returned": len(items), "can_manage": principal.can("users.manage")}


def get_user_view(
    directory_table: TableProtocol | None,
    principal: Principal,
    user_id: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """One person's account: groups, roles, effective permissions and where each comes from,
    sessions and sign-in activity."""
    require(principal, "users.view_details")
    view = DirectoryView.load(directory_table)
    user = view.users.get(user_id)
    if user is None:
        raise not_found(f"user {user_id} was not found", resource="user", identifier=user_id)
    current = now or datetime.now(tz=UTC)
    guard = get_sign_in_guard(directory_table, user.email)
    detail = _describe_user(
        user,
        view,
        principal,
        activity=get_user_activity(directory_table, user.user_id),
        locked_until=guard.locked_until if guard is not None and _after(guard.locked_until, current) else None,
    )
    grants = view.grants_for_user(user)
    detail.update(
        {
            "memberships": _memberships(view, user),
            "permissions": _permission_sources(view, user),
            "all_permissions": grants.all_permissions,
            # The roles they hold on datasets: granted to them, or to a group of theirs.
            "dataset_grants": _dataset_grants_of(view, {("user", user.user_id), *(("group", group_id) for group_id in user_group_ids(user))}),
            "failed_sign_ins": 0 if guard is None else guard.failed_attempts,
            "sessions": [
                describe_session(session, current=principal.session_id, now=int(current.timestamp()))
                for session in iter_user_sessions(directory_table, user.user_id)
            ],
            "sign_in_methods": user.sign_in_methods(),
        }
    )
    return detail


async def create_user_request(
    directory_table: TableProtocol,
    principal: Principal,
    body: Any,
    *,
    request: Any = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Add a person with an initial password, which they must replace when they first sign in.

    Body: `first_name`, `last_name`, `email`, `password`, and `group_ids` — the groups they join
    besides All Users.
    """
    require(principal, "users.manage")
    cfg = resolve_config(settings_table) if config is None else config
    fields = _object(body, allowed={"first_name", "last_name", "email", "password", "group_ids"})
    password = fields.get("password")
    try:
        email = clean_email(fields.get("email"))
        first_name = clean_person_name(fields.get("first_name"), field="first_name", label="First name")
        last_name = clean_person_name(fields.get("last_name"), field="last_name", label="Last name")
    except IdentityValidationError as error:
        raise invalid_request(str(error), field_name=error.field) from error
    problem = password_problem(password, min_length=cfg.password_min_length, email=email, first_name=first_name, last_name=last_name)
    if problem is not None:
        raise invalid_request(problem, field_name="password")
    payload = {
        "user_id": new_user_id(),
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "group_ids": fields.get("group_ids") or [],
    }
    event = directory_request(principal, "create_user", {**payload, "password_hash": _PREVIEW_HASH}, request=request)
    preview_request(directory_table, event, cfg)
    password_hash = await hash_new_password(password)
    return accept_request(directory_request(principal, "create_user", {**payload, "password_hash": password_hash}, request=request, like=event), "user", payload["user_id"])


def update_user_request(
    directory_table: TableProtocol,
    principal: Principal,
    user_id: str,
    body: Any,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Change a person's name, email, groups or status (`active` or `deactivated`).

    Deactivating someone ends every session they have. Send `base_revision`, the account's
    `revision` as you read it, to be refused rather than overwrite a change you did not see.
    """
    require(principal, "users.manage")
    fields = _object(body, allowed={"first_name", "last_name", "email", "group_ids", "status", "base_revision"})
    base_revision = _base_revision(fields.pop("base_revision", None))
    if not fields:
        raise invalid_request("nothing to change")
    event = directory_request(principal, "update_user", {"user_id": user_id, **fields}, request=request, base_revision=base_revision)
    preview_request(directory_table, event, config)
    return accept_request(event, "user", user_id)


async def set_user_password_request(
    directory_table: TableProtocol,
    principal: Principal,
    user_id: str,
    body: Any,
    *,
    request: Any = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Give a person a new password, which they must replace at their next sign-in.

    Every session they have ends, and a lockout on their email is lifted.
    """
    require(principal, "users.manage")
    cfg = resolve_config(settings_table) if config is None else config
    fields = _object(body, allowed={"password"})
    target = DirectoryView.load(directory_table).users.get(user_id)
    if target is None:
        raise not_found(f"user {user_id} was not found", resource="user", identifier=user_id)
    password = fields.get("password")
    problem = password_problem(
        password,
        min_length=cfg.password_min_length,
        email=target.email,
        first_name=target.first_name,
        last_name=target.last_name,
    )
    if problem is not None:
        raise invalid_request(problem, field_name="password")
    event = directory_request(principal, "set_user_password", {"user_id": user_id, "password_hash": _PREVIEW_HASH}, request=request)
    preview_request(directory_table, event, cfg)
    password_hash = await hash_new_password(password)
    return accept_request(
        directory_request(principal, "set_user_password", {"user_id": user_id, "password_hash": password_hash}, request=request, like=event),
        "user",
        user_id,
    )


def sign_out_user_request(
    directory_table: TableProtocol,
    principal: Principal,
    user_id: str,
    body: Any = None,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """End one of a person's sessions (`session_id`), or every one of them."""
    require(principal, "users.manage")
    fields = _object(body, allowed={"session_id"}) if body not in (None, b"", "") else {}
    payload: dict[str, Any] = {"user_id": user_id}
    if fields.get("session_id"):
        payload["session_id"] = fields["session_id"]
    event = directory_request(principal, "sign_out_user", payload, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "user", user_id)


def unlock_user_request(
    directory_table: TableProtocol,
    principal: Principal,
    user_id: str,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Lift a lockout on a person's email after too many failed sign-ins."""
    require(principal, "users.manage")
    event = directory_request(principal, "unlock_user", {"user_id": user_id}, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "user", user_id)


def delete_user_request(
    directory_table: TableProtocol,
    principal: Principal,
    user_id: str,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Delete a deactivated person's account. Their email can then be used again."""
    require(principal, "users.manage")
    event = directory_request(principal, "delete_user", {"user_id": user_id}, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "user", user_id)


# ------------------------------------------------------------------------------------------ groups


def list_groups_view(directory_table: TableProtocol | None, principal: Principal) -> dict[str, Any]:
    """Every group, built-in ones first, with member and role counts."""
    require(principal, "groups.view")
    view = DirectoryView.load(directory_table)
    return {
        "groups": [_describe_group(group, view, principal) for group in view.groups.values()],
        "can_manage": principal.can("groups.manage"),
    }


def get_group_view(directory_table: TableProtocol | None, principal: Principal, group_id: str) -> dict[str, Any]:
    """One group: its roles, the permissions they grant, and its members."""
    require(principal, "groups.view")
    view = DirectoryView.load(directory_table)
    group = view.groups.get(group_id)
    if group is None:
        raise not_found(f"group {group_id} was not found", resource="group", identifier=group_id)
    detail = _describe_group(group, view, principal)
    grants = view.grants_for_groups([group.group_id])
    detail["permissions"] = sorted(grants.permissions)
    detail["all_permissions"] = grants.all_permissions
    detail["roles"] = [_role_ref(view.roles[role_id]) for role_id in group.role_ids if role_id in view.roles]
    detail["dataset_grants"] = _dataset_grants_of(view, {("group", group.group_id)})
    if principal.can("users.view"):
        activities = activity_by_user(directory_table)
        detail["members"] = sorted(
            (
                {
                    "user_id": user.user_id,
                    "email": user.email,
                    "display_name": user.display_name,
                    "status": user.status,
                    "last_active_at": (activities.get(user.user_id).last_active_at if user.user_id in activities else None),
                }
                for user in view.members(group.group_id)
            ),
            key=lambda member: member["display_name"].lower(),
        )
    else:
        detail["members"] = None
    return detail


def create_group_request(
    directory_table: TableProtocol,
    principal: Principal,
    body: Any,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Create a group. Body: `name`, `description`, `role_ids`, `member_ids`."""
    require(principal, "groups.manage")
    fields = _object(body, allowed={"name", "description", "role_ids", "member_ids"})
    group_id = new_group_id()
    event = directory_request(principal, "create_group", {"group_id": group_id, **fields}, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "group", group_id)


def update_group_request(
    directory_table: TableProtocol,
    principal: Principal,
    group_id: str,
    body: Any,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Rename a group, change its description or roles, or add and remove members.

    Body: `name`, `description`, `role_ids` (the full list), `add_member_ids`,
    `remove_member_ids`, and `base_revision`.
    """
    require(principal, "groups.manage")
    fields = _object(
        body,
        allowed={"name", "description", "role_ids", "add_member_ids", "remove_member_ids", "base_revision"},
    )
    base_revision = _base_revision(fields.pop("base_revision", None))
    if not fields:
        raise invalid_request("nothing to change")
    event = directory_request(principal, "update_group", {"group_id": group_id, **fields}, request=request, base_revision=base_revision)
    preview_request(directory_table, event, config)
    return accept_request(event, "group", group_id)


def delete_group_request(
    directory_table: TableProtocol,
    principal: Principal,
    group_id: str,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Delete a group. Its members lose what it granted; built-in groups cannot be deleted."""
    require(principal, "groups.manage")
    event = directory_request(principal, "delete_group", {"group_id": group_id}, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "group", group_id)


# ------------------------------------------------------------------------------------------- roles


def list_roles_view(directory_table: TableProtocol | None, principal: Principal) -> dict[str, Any]:
    """Every role, built-in ones first, with the groups that hold it."""
    require(principal, "roles.view")
    view = DirectoryView.load(directory_table)
    return {
        "roles": [_describe_role(role, view) for role in view.roles.values()],
        "can_manage": principal.can("roles.manage"),
    }


def get_role_view(directory_table: TableProtocol | None, principal: Principal, role_id: str) -> dict[str, Any]:
    """One role, with every permission it grants once implications are added."""
    require(principal, "roles.view")
    view = DirectoryView.load(directory_table)
    role = view.roles.get(role_id)
    if role is None:
        raise not_found(f"role {role_id} was not found", resource="role", identifier=role_id)
    return _describe_role(role, view)


def create_role_request(
    directory_table: TableProtocol,
    principal: Principal,
    body: Any,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Create a custom role. Body: `name`, `description`, `permissions`."""
    require(principal, "roles.manage")
    fields = _object(body, allowed={"name", "description", "permissions"})
    role_id = new_role_id()
    event = directory_request(principal, "create_role", {"role_id": role_id, **fields}, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "role", role_id)


def update_role_request(
    directory_table: TableProtocol,
    principal: Principal,
    role_id: str,
    body: Any,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Change a custom role's name, description or permissions (the full list)."""
    require(principal, "roles.manage")
    fields = _object(body, allowed={"name", "description", "permissions", "base_revision"})
    base_revision = _base_revision(fields.pop("base_revision", None))
    if not fields:
        raise invalid_request("nothing to change")
    event = directory_request(principal, "update_role", {"role_id": role_id, **fields}, request=request, base_revision=base_revision)
    preview_request(directory_table, event, config)
    return accept_request(event, "role", role_id)


def delete_role_request(
    directory_table: TableProtocol,
    principal: Principal,
    role_id: str,
    *,
    request: Any = None,
    config: ControlPlaneConfig | None = None,
) -> IdentityAcceptance:
    """Delete a custom role, removing it from every group that holds it."""
    require(principal, "roles.manage")
    event = directory_request(principal, "delete_role", {"role_id": role_id}, request=request)
    preview_request(directory_table, event, config)
    return accept_request(event, "role", role_id)


def permissions_view(principal: Principal) -> dict[str, Any]:
    """The permission catalog: categories, and each permission with what it implies.

    Anyone signed in may read it: it is the application's vocabulary, which pages use to say in
    words what a person may do, not anything about who may do it.
    """
    if principal.kind == "guest":
        raise unauthenticated("Sign in to see the permissions.", reason="sign_in_required")
    return describe_catalog()


# ------------------------------------------------------------------------------- audit, outcomes


def audit_view(
    audit_table: TableProtocol | None,
    principal: Principal,
    query: Mapping[str, Any] | None = None,
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """The audit log, newest first, a page at a time.

    `limit` (1–200, default 100); `before`, the `next` cursor of the previous page; `action`,
    an action or a prefix such as `user.`; `outcome`, `success` or `failure`; `q`, text to find
    in who did it or what it was done to.
    """
    require(principal, "audit.view")
    cfg = resolve_config(settings_table) if config is None else config
    params = dict(query or {})
    limit = _limit(params.get("limit"))
    before = params.get("before") or None
    action = str(params.get("action") or "").strip()
    outcome = str(params.get("outcome") or "").strip()
    if outcome and outcome not in ("success", "failure"):
        raise invalid_request("outcome must be success or failure", field_name="outcome")
    search = str(params.get("q") or "").strip().lower()
    current = now or datetime.now(tz=UTC)
    oldest = current - timedelta(days=cfg.audit_log_retention_days)
    entries: list[dict[str, Any]] = []
    next_cursor: str | None = None
    for key, entry in iter_audit_entries(
        audit_table,
        newest=current.isoformat(),
        oldest=oldest.isoformat(),
        before=before if isinstance(before, str) else None,
    ):
        if action and not (entry.action == action or (action.endswith(".") and entry.action.startswith(action))):
            continue
        if outcome and entry.outcome != outcome:
            continue
        if search and not any(
            search in (value or "").lower()
            for value in (entry.actor_email, entry.target_label, entry.target_id, entry.actor_id, entry.ip)
        ):
            continue
        if len(entries) == limit:
            next_cursor = entries[-1]["cursor"]
            break
        entries.append({**entry.to_dict(), "cursor": key})
    return {
        "entries": entries,
        "returned": len(entries),
        "next": next_cursor,
        "retention_days": cfg.audit_log_retention_days,
    }


def identity_request_view(
    directory_table: TableProtocol | None,
    principal: Principal,
    request_id: str,
) -> dict[str, Any]:
    """Whether a directory request has been applied, refused, or is still pending.

    Only the person who sent it, or someone who can see users, may ask.
    """
    outcome = get_request_outcome(directory_table, request_id)
    if outcome is None:
        return {"request_id": request_id, "status": "pending"}
    if outcome.actor_id != principal.user_id and not principal.can("users.view"):
        raise forbidden("You can only follow requests you sent.")
    return outcome.to_dict()


# ---------------------------------------------------------------------------------------- helpers


def directory_request(
    principal: Principal,
    action: str,
    payload: dict[str, Any],
    *,
    request: Any,
    base_revision: int | None = None,
    like: IdentityRequest | None = None,
) -> IdentityRequest:
    if principal.kind != "user" or principal.user_id is None:
        raise forbidden("Only a signed-in person can change users, groups or roles.")
    return IdentityRequest(
        request_id=like.request_id if like is not None else f"idr-{uuid4().hex[:12]}",
        action=action,
        submitted_at=like.submitted_at if like is not None else _utc_now(),
        actor_id=principal.user_id,
        actor_email=principal.email,
        payload=payload,
        base_revision=base_revision if like is None else like.base_revision,
        context=request_context(request) if request is not None else (like.context if like is not None else request_context(None)),
    )


def preview_request(directory_table: TableProtocol, event: IdentityRequest, config: ControlPlaneConfig | None) -> None:
    try:
        preview_identity_request(directory_table, event, config=config)
    except IdentityRejection as rejection:
        raise _rejection_error(rejection) from rejection


def _rejection_error(rejection: IdentityRejection) -> WebError:
    """The HTTP error for a refusal the agent would make, with what it was about in `details`."""
    if rejection.reason == "invalid":
        error = invalid_request(rejection.message, field_name=rejection.field)
    elif rejection.reason == "not_found":
        error = not_found(rejection.message, resource="identity", identifier=rejection.field or "")
    elif rejection.reason == "already_exists":
        error = already_exists(rejection.message, resource="identity", identifier="", field_name=rejection.field)
    elif rejection.reason == "forbidden":
        error = forbidden(rejection.message, reason="forbidden")
    else:
        error = conflict(rejection.message, reason=rejection.reason)
        if rejection.field:
            error.details["field"] = rejection.field
    error.details.update({key: value for key, value in rejection.details.items() if value is not None})
    return error


def accept_request(event: IdentityRequest, entity_type: str, entity_id: str) -> IdentityAcceptance:
    return IdentityAcceptance(
        response={
            "request_id": event.request_id,
            "action": event.action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "accepted_at": event.submitted_at,
            "status_url": f"/identity/requests/{event.request_id}",
        },
        request_event=event,
    )


def _object(body: Any, *, allowed: set[str]) -> dict[str, Any]:
    if body is None:
        body = {}
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    unknown = sorted(set(body) - allowed)
    if unknown:
        raise invalid_request(f"{unknown[0]} is not something that can be set here", field_name=unknown[0])
    return dict(body)


def _base_revision(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise invalid_request("base_revision must be a positive integer", field_name="base_revision")
    return value


def _limit(value: Any) -> int:
    if value in (None, ""):
        return 100
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise invalid_request(f"limit must be a whole number from 1 to {AUDIT_PAGE_LIMIT}", field_name="limit") from None
    if not 1 <= parsed <= AUDIT_PAGE_LIMIT:
        raise invalid_request(f"limit must be a whole number from 1 to {AUDIT_PAGE_LIMIT}", field_name="limit")
    return parsed


def _describe_user(
    user: User,
    view: DirectoryView,
    principal: Principal,
    *,
    activity: Any,
    locked_until: str | None,
) -> dict[str, Any]:
    target_grants = view.grants_for_user(user)
    return {
        "user_id": user.user_id,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "display_name": user.display_name,
        "status": user.status,
        "groups": [
            {"group_id": group_id, "name": view.groups[group_id].name}
            for group_id in user.group_ids
            if group_id in view.groups
        ],
        "must_change_password": user.must_change_password,
        "has_password": user.password is not None,
        "locked_until": locked_until,
        "last_sign_in_at": None if activity is None else activity.last_sign_in_at,
        "last_active_at": None if activity is None else activity.last_active_at,
        "created_at": user.created_at,
        "created_by": user.created_by,
        "updated_at": user.updated_at,
        "updated_by": user.updated_by,
        "deactivated_at": user.deactivated_at,
        "deactivated_by": user.deactivated_by,
        "revision": user.revision,
        "administrator": target_grants.all_permissions,
        "manageable": principal.can("users.manage") and principal.grants.covers(target_grants),
        "self": user.user_id == principal.user_id,
    }


def _matches_status(item: dict[str, Any], status: str) -> bool:
    if status == "all":
        return True
    if status == "locked":
        return item["locked_until"] is not None
    if status == "password_change":
        return item["must_change_password"] and item["status"] == "active"
    return item["status"] == status


def _memberships(view: DirectoryView, user: User) -> list[dict[str, Any]]:
    memberships: list[dict[str, Any]] = []
    for group_id in user_group_ids(user):
        group = view.groups.get(group_id)
        if group is None:
            continue
        memberships.append(
            {
                "group_id": group.group_id,
                "name": group.name,
                "membership": group.membership,
                "automatic": group.membership != "assigned",
                "roles": [_role_ref(view.roles[role_id]) for role_id in group.role_ids if role_id in view.roles],
            }
        )
    return memberships


def _permission_sources(view: DirectoryView, user: User) -> list[dict[str, Any]]:
    """Each permission a person holds, and every group and role it comes through."""
    sources: dict[str, list[dict[str, str]]] = {}
    for group_id in user_group_ids(user):
        group = view.groups.get(group_id)
        if group is None:
            continue
        for role_id in group.role_ids:
            role = view.roles.get(role_id)
            if role is None:
                continue
            for key in sorted(expand_permissions(role.permissions)):
                sources.setdefault(key, []).append(
                    {"group_id": group.group_id, "group": group.name, "role_id": role.role_id, "role": role.name}
                )
    order = {definition.key: index for index, definition in enumerate(PERMISSIONS)}
    return [
        {"key": key, "via": via}
        for key, via in sorted(sources.items(), key=lambda item: order.get(item[0], len(order)))
    ]


def _describe_group(group: Group, view: DirectoryView, principal: Principal) -> dict[str, Any]:
    definition = built_in_group(group.group_id)
    grants = view.grants_for_groups([group.group_id])
    members = view.members(group.group_id)
    return {
        "group_id": group.group_id,
        "name": group.name,
        "description": group.description,
        "membership": group.membership,
        "built_in": definition is not None,
        "locked_role_ids": [] if definition is None else list(definition.locked_role_ids),
        "role_ids": list(group.role_ids),
        "role_names": [view.roles[role_id].name for role_id in group.role_ids if role_id in view.roles],
        "member_count": len(members) if group.membership != "guests" else None,
        "administrators": grants.all_permissions,
        "revision": group.revision,
        "created_at": group.created_at,
        "created_by": group.created_by,
        "updated_at": group.updated_at,
        "updated_by": group.updated_by,
        "manageable": principal.can("groups.manage") and principal.grants.covers(grants),
        # Whether the person asking holds everything the group grants, on datasets too, so may add
        # people to it.
        "grantable": principal.grants.covers(grants),
    }


def _describe_role(role: Role, view: DirectoryView) -> dict[str, Any]:
    grants = Grants(group_ids=(), role_ids=(role.role_id,), permissions=expand_permissions(role.permissions), all_permissions="*" in role.permissions)
    holders = [group for group in view.groups.values() if role.role_id in group.role_ids]
    return {
        "role_id": role.role_id,
        "name": role.name,
        "description": role.description,
        "built_in": role.built_in,
        "permissions": list(role.permissions),
        "effective_permissions": sorted(grants.permissions),
        "all_permissions": grants.all_permissions,
        # Whether it can be granted on a dataset: it holds dataset permissions only.
        "dataset_role": dataset_grantable(role.permissions),
        "groups": [{"group_id": group.group_id, "name": group.name} for group in holders],
        "revision": role.revision,
        "created_at": role.created_at,
        "created_by": role.created_by,
        "updated_at": role.updated_at,
        "updated_by": role.updated_by,
    }


def _role_ref(role: Role) -> dict[str, Any]:
    return {"role_id": role.role_id, "name": role.name, "built_in": role.built_in}


def _dataset_grants_of(view: DirectoryView, principal_ids: set[tuple[str, str]]) -> list[dict[str, Any]]:
    """The roles held on datasets by any of `principal_ids` - (user or group, id) - by dataset."""
    held = []
    for grant in view.dataset_grants.values():
        dataset = view.datasets.get(grant.dataset_id)
        if (grant.principal_type, grant.principal_id) not in principal_ids or dataset is None:
            continue
        group = view.groups.get(grant.principal_id) if grant.principal_type == "group" else None
        held.append(
            {
                "dataset_id": dataset.dataset_id,
                "dataset": dataset.name,
                "principal_type": grant.principal_type,
                "principal_id": grant.principal_id,
                "via_group": None if group is None else group.name,
                "roles": [_role_ref(view.roles[role_id]) for role_id in grant.role_ids if role_id in view.roles],
            }
        )
    return sorted(held, key=lambda item: (item["dataset"], item["via_group"] or ""))


def _after(value: str | None, moment: datetime) -> bool:
    if not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed > moment


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
