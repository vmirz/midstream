"""Access tokens from the account page: make one, see yours, revoke one (spec §11.4).

Making a token creates its secret here and shows it once, in the response; the request that
stores it carries only the secret's hash, like a password's. The identity agent applies it a moment
later, so a brand-new token can be refused for that moment.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from materializer_control_plane.identity.access_tokens import (
    DEFAULT_TOKEN_PERMISSIONS,
    MAX_TOKEN_LIFETIME_DAYS,
    MAX_TOKENS_PER_USER,
    TOKEN_NAME_MAX_LENGTH,
    TOKEN_PERMISSIONS,
    issue_token,
)
from materializer_control_plane.identity.authorization import request_context
from materializer_control_plane.identity.permissions import permission_definition
from materializer_control_plane.identity.principals import Principal
from materializer_control_plane.models.identity import AccessToken, IdentityRequest
from materializer_control_plane.services.identity import MAX_TOKEN_DATASETS
from materializer_control_plane.storage.identity import (
    find_dataset_by_name,
    get_access_token,
    iter_datasets,
    iter_user_access_tokens,
)
from materializer_control_plane.storage.table import TableProtocol
from materializer_control_plane.web.errors import forbidden, invalid_request, not_found, unauthenticated
from materializer_control_plane.web.identity import IdentityAcceptance

#: A token made without an expiry lasts this long.
DEFAULT_TOKEN_LIFETIME_DAYS = 90


def create_access_token_request(
    principal: Principal,
    body: Any,
    *,
    request: Any,
    directory_table: TableProtocol | None = None,
    now: datetime | None = None,
) -> IdentityAcceptance:
    """Make a token for the caller; the response carries its text, shown this once.

    Body: `{"name": "DuckDB on my laptop", "permissions": ["catalog.read"], "expires_in_days": 90,
    "datasets": ["sales"]}`. `datasets`, by name, limits the token to those datasets; without it,
    the token may use its permissions on every dataset its owner may.
    """
    if principal.kind != "user" or principal.user_id is None or principal.session_id is None:
        # A token cannot make another: only a person signed in makes tokens.
        raise forbidden("Access tokens are made by a signed-in person, from the account page.", reason="session_required")
    options = body if isinstance(body, dict) else {}
    if body is not None and not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    unknown = sorted(set(options) - {"name", "permissions", "expires_in_days", "datasets"})
    if unknown:
        raise invalid_request(f"{unknown[0]} is not something a token has", field_name=unknown[0])
    name = options.get("name")
    if not isinstance(name, str) or not name.strip():
        raise invalid_request("Name the token after where it will be used", field_name="name")
    name = " ".join(name.split())
    if len(name) > TOKEN_NAME_MAX_LENGTH:
        raise invalid_request(f"Name must be at most {TOKEN_NAME_MAX_LENGTH} characters", field_name="name")
    permissions = options.get("permissions", list(DEFAULT_TOKEN_PERMISSIONS))
    if not isinstance(permissions, list) or not permissions or any(item not in TOKEN_PERMISSIONS for item in permissions):
        raise invalid_request(
            "A token carries catalog.read, catalog.write or both",
            field_name="permissions",
        )
    datasets = _requested_datasets(directory_table, options.get("datasets"))
    for permission in permissions:
        definition = permission_definition(permission)
        label = definition.label if definition else permission
        lacking = [dataset.name for dataset in datasets if not principal.can(permission, dataset.dataset_id)]
        if lacking:
            raise forbidden(
                f"You cannot make a token with “{label}” on dataset {', '.join(lacking)}: you do not have it there.",
                permission=permission,
            )
        if not datasets and not principal.can_anywhere(permission):
            raise forbidden(f"You cannot make a token with “{label}”: you do not have it.", permission=permission)
    days = options.get("expires_in_days", DEFAULT_TOKEN_LIFETIME_DAYS)
    if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= MAX_TOKEN_LIFETIME_DAYS:
        raise invalid_request(
            f"expires_in_days must be a whole number from 1 to {MAX_TOKEN_LIFETIME_DAYS}",
            field_name="expires_in_days",
        )
    moment = datetime.now(tz=UTC) if now is None else now
    expires_at = _format(moment + timedelta(days=days))
    issued = issue_token()
    event = IdentityRequest(
        request_id=f"idr-{uuid4().hex[:12]}",
        action="create_access_token",
        submitted_at=_format(moment),
        actor_id=principal.user_id,
        actor_email=principal.email,
        payload={
            "token_id": issued.token_id,
            "name": name,
            "permissions": sorted(set(permissions)),
            "secret_hash": issued.secret_hash,
            "expires_at": expires_at,
            "datasets": [dataset.dataset_id for dataset in datasets],
        },
        context=request_context(request),
    )
    return IdentityAcceptance(
        response={
            "request_id": event.request_id,
            "action": event.action,
            "entity_type": "access_token",
            "entity_id": issued.token_id,
            "accepted_at": event.submitted_at,
            "status_url": f"/identity/requests/{event.request_id}",
            "token": issued.text,
            "token_id": issued.token_id,
            "name": name,
            "permissions": sorted(set(permissions)),
            "expires_at": expires_at,
            "datasets": [{"dataset_id": dataset.dataset_id, "name": dataset.name} for dataset in datasets],
        },
        request_event=event,
    )


def _requested_datasets(directory_table: TableProtocol | None, value: Any) -> list[Any]:
    """The datasets a token is limited to, by name; none, for a token not limited."""
    if value in (None, []):
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise invalid_request("datasets must be a list of dataset names", field_name="datasets")
    if len(value) > MAX_TOKEN_DATASETS:
        raise invalid_request(f"A token can be limited to at most {MAX_TOKEN_DATASETS} datasets", field_name="datasets")
    found = []
    for name in dict.fromkeys(item.strip().lower() for item in value):
        dataset = find_dataset_by_name(directory_table, name)
        if dataset is None:
            raise not_found(f"There is no dataset {name}.", resource="dataset", identifier=name)
        found.append(dataset)
    return found


def my_access_tokens_view(
    directory_table: TableProtocol | None,
    principal: Principal,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """The caller's tokens, newest first: never their secrets."""
    if principal.kind != "user" or principal.user_id is None:
        raise unauthenticated("Sign in to see your access tokens.", reason="sign_in_required")
    moment = datetime.now(tz=UTC) if now is None else now
    names = {dataset.dataset_id: dataset.name for dataset in iter_datasets(directory_table)}
    return {
        "tokens": [
            describe_access_token(token, now=moment, dataset_names=names)
            for token in iter_user_access_tokens(directory_table, principal.user_id)
        ],
        "limit": MAX_TOKENS_PER_USER,
        "permissions": [
            {"key": key, "label": permission_definition(key).label, "granted": principal.can_anywhere(key)}
            for key in TOKEN_PERMISSIONS
        ],
        # The datasets a token could be limited to: those the caller reads through the catalog.
        "datasets": [
            {
                "dataset_id": dataset_id,
                "name": name,
                "permissions": [key for key in TOKEN_PERMISSIONS if principal.can(key, dataset_id)],
            }
            for dataset_id, name in sorted(names.items(), key=lambda item: item[1])
            if principal.can("catalog.read", dataset_id)
        ],
        "max_datasets": MAX_TOKEN_DATASETS,
        "default_expires_in_days": DEFAULT_TOKEN_LIFETIME_DAYS,
        "max_expires_in_days": MAX_TOKEN_LIFETIME_DAYS,
    }


def revoke_access_token_request(
    directory_table: TableProtocol | None,
    principal: Principal,
    token_id: str,
    *,
    request: Any,
    now: datetime | None = None,
) -> IdentityAcceptance:
    """Revoke one of the caller's tokens (with Manage users, someone else's)."""
    if principal.kind != "user" or principal.user_id is None:
        raise unauthenticated("Sign in to revoke an access token.", reason="sign_in_required")
    token = get_access_token(directory_table, token_id)
    if token is None or (token.user_id != principal.user_id and not principal.can("users.manage")):
        raise not_found(f"access token {token_id} was not found", resource="access_token", identifier=token_id)
    moment = datetime.now(tz=UTC) if now is None else now
    event = IdentityRequest(
        request_id=f"idr-{uuid4().hex[:12]}",
        action="revoke_access_token",
        submitted_at=_format(moment),
        actor_id=principal.user_id,
        actor_email=principal.email,
        payload={"token_id": token_id},
        context=request_context(request),
    )
    return IdentityAcceptance(
        response={
            "request_id": event.request_id,
            "action": event.action,
            "entity_type": "access_token",
            "entity_id": token_id,
            "accepted_at": event.submitted_at,
            "status_url": f"/identity/requests/{event.request_id}",
        },
        request_event=event,
    )


def describe_access_token(token: AccessToken, *, now: datetime, dataset_names: dict[str, str] | None = None) -> dict[str, Any]:
    expires = _parse(token.expires_at)
    names = dataset_names or {}
    return {
        "token_id": token.token_id,
        "name": token.name,
        "permissions": list(token.permissions),
        # Empty: every dataset its owner may use it on. A deleted dataset is named by its id.
        "datasets": [{"dataset_id": dataset_id, "name": names.get(dataset_id, dataset_id)} for dataset_id in token.datasets],
        "created_at": token.created_at,
        "expires_at": token.expires_at,
        "revoked_at": token.revoked_at,
        "revoked_by": token.revoked_by,
        "active": not token.revoked and (expires is None or expires > now),
    }


def _format(moment: datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
