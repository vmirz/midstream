"""Access tokens from the account page (handbook 20): make one, see yours, revoke one; and the
revocations every domain shares.

A token is signed here, with the federation key every domain holds, and shown once, in the
response; the request that records it carries no secret, since a token needs no record to be
checked. The identity agent records it a moment later, so this domain can list it and its owner can
revoke it. A token is good in every domain where its owner has an account; a revocation made here
is handed to every other domain by the console, signed, and each records it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from midstream.config import current_domain_id
from midstream.identity.access_tokens import (
    DEFAULT_TOKEN_PERMISSIONS,
    MAX_TOKEN_LIFETIME_DAYS,
    MAX_TOKENS_PER_USER,
    TOKEN_NAME_MAX_LENGTH,
    TOKEN_PERMISSIONS,
    TokenClaims,
    TokenOwner,
    new_token_id,
    read_revocation,
    sign_access_token,
    sign_revocation,
)
from midstream.identity.authorization import request_context
from midstream.identity.permissions import permission_definition
from midstream.identity.principals import Principal
from midstream.identity.secrets import IdentitySecrets, get_identity_secrets
from midstream.models.identity import AccessToken, IdentityRequest
from midstream.services.identity import MAX_TOKEN_DATASETS
from midstream.storage.identity import (
    find_dataset_by_name,
    get_access_token,
    get_user,
    iter_datasets,
    iter_token_revocations,
    iter_user_access_tokens,
)
from midstream.storage.table import TableProtocol
from midstream.table_names import dataset_name_problem, domain_id_problem
from midstream.web.errors import conflict, forbidden, invalid_request, not_found, unauthenticated, unavailable
from midstream.web.identity import IdentityAcceptance

#: A token made without an expiry lasts this long.
DEFAULT_TOKEN_LIFETIME_DAYS = 90
#: The most revocations one request may hand over.
MAX_REVOCATIONS_PER_REQUEST = 100


def this_domain() -> str:
    """The domain tokens made here name as their issuer, and their dataset limits are read against."""
    return current_domain_id() or "default"


def create_access_token_request(
    principal: Principal,
    body: Any,
    *,
    request: Any,
    directory_table: TableProtocol | None = None,
    secrets: IdentitySecrets | None = None,
    domain: str | None = None,
    now: datetime | None = None,
) -> IdentityAcceptance:
    """Make a token for the caller; the response carries its text, shown this once.

    Body: `{"name": "DuckDB on my laptop", "permissions": ["catalog.read"], "expires_in_days": 90,
    "datasets": ["domain_a.sales", "domain_b.finance"]}`. `datasets` limits the token to those
    datasets - a name without its domain is this domain's; without it, the token may use its
    permissions on every dataset its owner may, in every domain. This domain checks the caller holds
    each permission on its own datasets named, or somewhere here; another domain's limits are
    checked where the token is used, against what the owner holds there.
    """
    identity_secrets = get_identity_secrets() if secrets is None else secrets
    issuer = domain or this_domain()
    if principal.kind != "user" or principal.user_id is None or principal.session_id is None:
        # A token cannot make another: only a person signed in makes tokens.
        raise forbidden("Access tokens are made by a signed-in person, from the account page.", reason="session_required")
    if identity_secrets.current_federation_key is None:
        raise unavailable("Access tokens cannot be made here: this domain has no federation key.", reason="no_federation_key")
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
        raise invalid_request("A token carries catalog.read, catalog.write or both", field_name="permissions")
    permissions = sorted(set(permissions))
    qualified, own = _requested_datasets(directory_table, options.get("datasets"), issuer)
    for permission in permissions:
        definition = permission_definition(permission)
        label = definition.label if definition else permission
        lacking = [dataset.name for dataset in own if not principal.can(permission, dataset.dataset_id)]
        if lacking:
            raise forbidden(
                f"You cannot make a token with “{label}” on dataset {', '.join(lacking)}: you do not have it there.",
                permission=permission,
            )
        if not own and not principal.can_anywhere(permission):
            raise forbidden(f"You cannot make a token with “{label}” here: you do not have it.", permission=permission)
    days = options.get("expires_in_days", DEFAULT_TOKEN_LIFETIME_DAYS)
    if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= MAX_TOKEN_LIFETIME_DAYS:
        raise invalid_request(
            f"expires_in_days must be a whole number from 1 to {MAX_TOKEN_LIFETIME_DAYS}",
            field_name="expires_in_days",
        )
    moment = (datetime.now(tz=UTC) if now is None else now).replace(microsecond=0)
    live = [token for token in iter_user_access_tokens(directory_table, principal.user_id) if _active(token, moment)]
    if len(live) >= MAX_TOKENS_PER_USER:
        raise conflict(f"You have {MAX_TOKENS_PER_USER} tokens here already. Revoke one you no longer use first.", reason="limit")

    user = get_user(directory_table, principal.user_id)
    claims = TokenClaims(
        token_id=new_token_id(),
        issuer=issuer,
        owner=_owner(principal, user),
        permissions=tuple(permissions),
        issued_at=int(moment.timestamp()),
        expires_at=int((moment + timedelta(days=days)).timestamp()),
        datasets=qualified,
    )
    expires_at = _format(datetime.fromtimestamp(claims.expires_at, tz=UTC))
    event = IdentityRequest(
        request_id=f"idr-{uuid4().hex[:12]}",
        action="create_access_token",
        submitted_at=_format(moment),
        actor_id=principal.user_id,
        actor_email=principal.email,
        payload={
            "token_id": claims.token_id,
            "name": name,
            "permissions": permissions,
            "expires_at": expires_at,
            "issuer": issuer,
            "datasets": list(qualified),
        },
        context=request_context(request),
    )
    return IdentityAcceptance(
        response={
            "request_id": event.request_id,
            "action": event.action,
            "entity_type": "access_token",
            "entity_id": claims.token_id,
            "accepted_at": event.submitted_at,
            "status_url": f"/identity/requests/{event.request_id}",
            "token": sign_access_token(claims, identity_secrets),
            "token_id": claims.token_id,
            "name": name,
            "permissions": permissions,
            "expires_at": expires_at,
            "issuer": issuer,
            "datasets": [{"name": item} for item in qualified],
        },
        request_event=event,
    )


def _owner(principal: Principal, user: Any) -> TokenOwner:
    """Who a token acts for: the provider account the caller signed in with, if any, and their email."""
    email = principal.email or (user.email if user is not None else "")
    method = principal.sign_in_method or ""
    identities = list(user.external_identities) if user is not None else []
    if method.startswith("sso:"):
        provider = method[len("sso:"):]
        chosen = next((identity for identity in identities if identity.provider == provider), None)
    else:
        chosen = identities[0] if identities else None
    if chosen is None:
        return TokenOwner(email=email)
    return TokenOwner(email=email, provider=chosen.provider, subject=chosen.subject)


def _requested_datasets(directory_table: TableProtocol | None, value: Any, issuer: str) -> tuple[tuple[str, ...], list[Any]]:
    """The datasets a token is limited to, as `<domain>.<dataset>`, and those of them in this domain."""
    if value in (None, []):
        return (), []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise invalid_request("datasets must be a list of dataset names", field_name="datasets")
    if len(value) > MAX_TOKEN_DATASETS:
        raise invalid_request(f"A token can be limited to at most {MAX_TOKEN_DATASETS} datasets", field_name="datasets")
    qualified: list[str] = []
    own = []
    for raw in dict.fromkeys(item.strip().lower() for item in value):
        domain, separator, name = raw.rpartition(".")
        domain = domain if separator else issuer
        problem = dataset_name_problem(name) or (domain_id_problem(domain) if domain != issuer else None)
        if problem:
            raise invalid_request(f"{raw}: {problem}", field_name="datasets")
        if domain == issuer:
            dataset = find_dataset_by_name(directory_table, name)
            if dataset is None:
                raise not_found(f"There is no dataset {name}.", resource="dataset", identifier=name)
            own.append(dataset)
        qualified.append(f"{domain}.{name}")
    return tuple(qualified), own


def my_access_tokens_view(
    directory_table: TableProtocol | None,
    principal: Principal,
    *,
    domain: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """The caller's tokens made here, newest first, never their text; and what a new one may carry."""
    if principal.kind != "user" or principal.user_id is None:
        raise unauthenticated("Sign in to see your access tokens.", reason="sign_in_required")
    issuer = domain or this_domain()
    moment = datetime.now(tz=UTC) if now is None else now
    datasets = sorted(iter_datasets(directory_table), key=lambda dataset: dataset.name)
    return {
        "domain": issuer,
        "tokens": [describe_access_token(token, now=moment) for token in iter_user_access_tokens(directory_table, principal.user_id)],
        "limit": MAX_TOKENS_PER_USER,
        "permissions": [
            {"key": key, "label": permission_definition(key).label, "granted": principal.can_anywhere(key)}
            for key in TOKEN_PERMISSIONS
        ],
        # The datasets a token could be limited to here: those the caller reads through the catalog.
        "datasets": [
            {
                "dataset_id": dataset.dataset_id,
                "name": f"{issuer}.{dataset.name}",
                "permissions": [key for key in TOKEN_PERMISSIONS if principal.can(key, dataset.dataset_id)],
            }
            for dataset in datasets
            if principal.can("catalog.read", dataset.dataset_id)
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
    secrets: IdentitySecrets | None = None,
    now: datetime | None = None,
) -> IdentityAcceptance:
    """Revoke one of the caller's tokens made here (with Manage users, someone else's).

    The answer carries the revocation, signed, for the console to hand to every other domain.
    """
    identity_secrets = get_identity_secrets() if secrets is None else secrets
    if principal.kind != "user" or principal.user_id is None:
        raise unauthenticated("Sign in to revoke an access token.", reason="sign_in_required")
    token = get_access_token(directory_table, token_id)
    if token is None or (token.user_id != principal.user_id and not principal.can("users.manage")):
        raise not_found(f"access token {token_id} was not found", resource="access_token", identifier=token_id)
    if identity_secrets.current_federation_key is None:
        raise unavailable("Access tokens cannot be revoked here: this domain has no federation key.", reason="no_federation_key")
    moment = datetime.now(tz=UTC) if now is None else now
    expires = _parse(token.expires_at) or moment
    revocation = sign_revocation(token_id, int(expires.timestamp()), identity_secrets)
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
            "revocation": revocation.to_dict(),
        },
        request_event=event,
    )


def apply_revocations_request(
    body: Any,
    *,
    request: Any,
    secrets: IdentitySecrets | None = None,
    now: datetime | None = None,
) -> IdentityAcceptance:
    """Record revocations other domains signed: `{"revocations": [{"jti", "exp", "signature"}], "source"}`.

    Needs no session - the federation key's signature is the proof - and refuses the whole request
    when any revocation in it is not signed with a key this domain holds.
    """
    identity_secrets = get_identity_secrets() if secrets is None else secrets
    options = body if isinstance(body, dict) else None
    items = options.get("revocations") if options is not None else None
    if not isinstance(items, list) or not items:
        raise invalid_request("revocations must be a list of signed revocations", field_name="revocations")
    if len(items) > MAX_REVOCATIONS_PER_REQUEST:
        raise invalid_request(f"At most {MAX_REVOCATIONS_PER_REQUEST} revocations at once", field_name="revocations")
    source = options.get("source") if isinstance(options.get("source"), str) else ""
    revocations = []
    for item in items:
        revocation = read_revocation(item, identity_secrets)
        if revocation is None:
            raise forbidden("A revocation is not signed with this deployment's federation key.", reason="invalid_signature")
        revocations.append(revocation.to_dict())
    moment = datetime.now(tz=UTC) if now is None else now
    event = IdentityRequest(
        request_id=f"idr-{uuid4().hex[:12]}",
        action="apply_token_revocation",
        submitted_at=_format(moment),
        actor_id="system",
        actor_email=None,
        payload={"revocations": revocations, "source": source[:64]},
        context=request_context(request),
    )
    return IdentityAcceptance(
        response={"request_id": event.request_id, "accepted": len(revocations), "status_url": f"/identity/requests/{event.request_id}"},
        request_event=event,
    )


def revocations_view(directory_table: TableProtocol | None, principal: Principal, since: Any = None) -> dict[str, Any]:
    """Every token revocation this domain holds, oldest first, from `since` (an instant) on, signed so
    the console can hand them to the other domains. For the console's service credential, and for
    administrators who manage users."""
    if principal.kind != "service" and not principal.can("users.manage"):
        raise forbidden("Only the console and administrators who manage users may list revocations.")
    after = since if isinstance(since, str) and since else ""
    revocations = sorted(
        (revocation for revocation in iter_token_revocations(directory_table) if revocation.revoked_at > after),
        key=lambda revocation: revocation.revoked_at,
    )
    return {
        "revocations": [
            {
                "jti": revocation.token_id,
                "exp": revocation.expires_at,
                "signature": revocation.signature,
                "revoked_at": revocation.revoked_at,
                "source": revocation.source,
            }
            for revocation in revocations
        ]
    }


def describe_access_token(token: AccessToken, *, now: datetime) -> dict[str, Any]:
    return {
        "token_id": token.token_id,
        "name": token.name,
        "permissions": list(token.permissions),
        # Empty: every dataset its owner may use it on, in every domain.
        "datasets": [{"name": name} for name in token.datasets],
        "issuer": token.issuer,
        "created_at": token.created_at,
        "expires_at": token.expires_at,
        "revoked_at": token.revoked_at,
        "revoked_by": token.revoked_by,
        "active": _active(token, now),
    }


def _active(token: AccessToken, now: datetime) -> bool:
    expires = _parse(token.expires_at)
    return not token.revoked and (expires is None or expires > now)


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
