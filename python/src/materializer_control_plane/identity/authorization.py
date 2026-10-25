"""Authorizing a WebView request: the first thing every view does.

```python
principal = authorize(request, directory_table, "materializations.view")
principal = authorize(request, directory_table, "dataset.manage", dataset=name)   # on one dataset
principal = authorize(request, directory_table, "catalog.read", any_dataset=True)  # on some dataset
```

A request with no session is a guest's, and a guest may do what the Guest group allows. A
request whose session cannot be used — expired, ended, signed by another deployment, or belonging
to a deactivated person — is refused with 401 rather than quietly treated as a guest's, so a client
knows to sign in again. A signed-in person who lacks the permission gets 403.

A dataset permission is checked on the dataset a request names, when it names one; with
`any_dataset`, it is enough to hold it on one dataset, and the view checks each dataset it then
touches; with neither, it must be held on every dataset.
"""

from __future__ import annotations

from datetime import UTC, datetime
import hmac
from typing import Any

from materializer_control_plane.identity.access_tokens import ACCESS_TOKEN_PREFIX, parse_token, secret_matches
from materializer_control_plane.identity.permissions import (
    PERMISSION_KEYS,
    expand_permissions,
    is_dataset_permission,
    permission_definition,
)
from materializer_control_plane.identity.principals import (
    Grants,
    Principal,
    cached_grants,
    guest_principal,
    service_principal,
)
from materializer_control_plane.identity.secrets import IdentitySecrets, get_identity_secrets
from materializer_control_plane.identity.tokens import InvalidToken, read_token
from materializer_control_plane.models.identity import Dataset, RequestContext
from materializer_control_plane.storage.identity import get_access_token, get_session, get_user, resolve_dataset
from materializer_control_plane.storage.table import TableProtocol
from materializer_control_plane.web.errors import forbidden, not_found, unauthenticated, unavailable

_BEARER = "bearer "
_MAX_HEADER = 512


def header(request: Any, name: str) -> str | None:
    """A request header, without regard to case, whatever kind of request object this is."""
    headers = getattr(request, "headers", None)
    if headers is None:
        return None
    value = headers.get(name)
    if value is None and hasattr(headers, "items"):
        lowered = name.lower()
        value = next((item for key, item in headers.items() if str(key).lower() == lowered), None)
    return value if isinstance(value, str) else None


def bearer_token(request: Any) -> str | None:
    """The token of an `Authorization: Bearer …` header, or None."""
    value = header(request, "authorization")
    if not value or value[: len(_BEARER)].lower() != _BEARER:
        return None
    token = value[len(_BEARER):].strip()
    return token or None


def request_context(request: Any) -> RequestContext:
    """Where a request came from, for the audit log: as reported, so informational only.

    The console forwards the browser's address and user agent; a direct caller is described by
    its own connection.
    """
    forwarded = (header(request, "x-forwarded-for") or "").split(",")[0].strip()
    ip = forwarded or getattr(request, "remote", None)
    agent = header(request, "x-forwarded-user-agent") or header(request, "user-agent")
    return RequestContext(
        ip=ip[:_MAX_HEADER] if isinstance(ip, str) and ip else None,
        user_agent=agent[:_MAX_HEADER] if agent else None,
    )


def directory_ready(directory_table: TableProtocol | None) -> bool:
    """Whether this process has read its tables back since it started.

    Kaspr answers requests while a restarted app is still reading its tables back from their
    changelogs, and until then the directory holds only some of its records: a person it does not
    find yet has not left, and grants it has not read yet were not taken away. Faust says when that
    first reading is done; a table that is not an app's, as in tests, is taken as ready.
    """
    recovery = getattr(getattr(getattr(directory_table, "app", None), "tables", None), "recovery", None)
    is_set = getattr(getattr(recovery, "completed", None), "is_set", None)
    return bool(is_set()) if callable(is_set) else True


def require_directory_ready(directory_table: TableProtocol | None) -> None:
    """Refuse with 503 while the directory is still being read back: a client keeps what it knew."""
    if not directory_ready(directory_table):
        raise unavailable(
            "The control plane is starting and still reading its tables back. Try again in a moment.",
            reason="starting",
        )


def authenticate(
    request: Any,
    directory_table: TableProtocol | None,
    *,
    secrets: IdentitySecrets | None = None,
    now: int | None = None,
) -> Principal:
    """Who made this request: a person with a valid session, the console's service, or a guest.

    `503 starting` until the directory has been read back after a start (`directory_ready`), for
    every caller: a session must not read as ended, nor a list as empty, because they were not read yet.
    """
    require_directory_ready(directory_table)
    token = bearer_token(request)
    if token is None:
        return guest_principal(directory_table)
    identity_secrets = get_identity_secrets() if secrets is None else secrets
    if identity_secrets.service_token is not None and hmac.compare_digest(
        token.encode("utf-8"), identity_secrets.service_token.encode("utf-8")
    ):
        return service_principal()
    if token.startswith(ACCESS_TOKEN_PREFIX):
        return _token_principal(token, directory_table, now=_now() if now is None else now)
    if not identity_secrets.can_sign_in:
        raise unauthenticated(
            "Sign-in is not configured on this control plane",
            reason="sign_in_unavailable",
        )
    current = _now() if now is None else now
    try:
        claims = read_token(token, identity_secrets, now=current)
    except InvalidToken as error:
        raise unauthenticated(
            "Your session has expired. Sign in again." if error.reason == "expired" else "Your session is not valid. Sign in again.",
            reason="session_expired" if error.reason == "expired" else "invalid_session",
        ) from error
    user = get_user(directory_table, claims.user_id)
    if user is None:
        raise unauthenticated("Your session is not valid. Sign in again.", reason="invalid_session")
    if not user.active:
        raise unauthenticated("This account is deactivated.", reason="account_deactivated")
    # An older epoch means every session of this person was ended after this one began. A newer
    # one can only come from a replica that has read the directory a moment sooner.
    if claims.session_epoch < user.session_epoch:
        raise unauthenticated("This session was ended. Sign in again.", reason="session_ended")
    session = get_session(directory_table, user.user_id, claims.session_id)
    if session is not None and session.ended:
        raise unauthenticated("This session was ended. Sign in again.", reason="session_ended")
    return Principal(
        kind="user",
        grants=cached_grants(directory_table, user),
        user_id=user.user_id,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        session_id=claims.session_id,
        session_expires_at=claims.session_expires_at,
        expires_at=claims.expires_at,
        issued_at=claims.issued_at,
        signed_in_at=claims.signed_in_at,
        sign_in_method=claims.method,
        password_change_required=user.must_change_password,
    )


def _token_principal(text: str, directory_table: TableProtocol | None, *, now: int) -> Principal:
    """The person an access token belongs to, with only what the token carries and they still hold."""
    parsed = parse_token(text)
    token = None if parsed is None else get_access_token(directory_table, parsed[0])
    if parsed is None or token is None or not secret_matches(parsed[1], token.secret_hash):
        raise unauthenticated("This access token is not valid.", reason="invalid_token")
    if token.revoked:
        raise unauthenticated("This access token was revoked.", reason="token_revoked")
    expires = _epoch(token.expires_at)
    if expires is not None and expires <= now:
        raise unauthenticated("This access token has expired. Make a new one.", reason="token_expired")
    user = get_user(directory_table, token.user_id)
    if user is None or not user.active:
        raise unauthenticated("The account this token belongs to can no longer sign in.", reason="account_deactivated")
    owner = cached_grants(directory_table, user)
    carried = expand_permissions(token.permissions)
    permissions = carried if owner.all_permissions else frozenset(carried & owner.permissions)
    if owner.all_permissions:
        datasets = {}
    else:
        datasets = {
            dataset_id: frozenset(carried & held)
            for dataset_id, held in owner.datasets.items()
            if carried & held
        }
    return Principal(
        kind="token",
        grants=Grants(
            group_ids=owner.group_ids,
            role_ids=owner.role_ids,
            permissions=permissions,
            datasets=datasets,
            dataset_limit=frozenset(token.datasets) if token.datasets else None,
        ),
        user_id=user.user_id,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        expires_at=expires,
        extra={"access_token_id": token.token_id, "access_token_name": token.name},
    )


def _epoch(value: str | None) -> int | None:
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int((moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)).timestamp())


def authorize(
    request: Any,
    directory_table: TableProtocol | None,
    permission: str | None = None,
    *,
    dataset: str | None = None,
    any_dataset: bool = False,
    allow_password_change: bool = False,
    secrets: IdentitySecrets | None = None,
    now: int | None = None,
) -> Principal:
    """Authenticate a request and require a permission of it.

    `permission=None` asks only who the caller is. A dataset permission is required on `dataset`
    (its name or id; a dataset that does not exist is a 404 once the caller is known), on at least
    one dataset with `any_dataset`, or on every dataset with neither. Until a person replaces a
    password someone else chose, they may only do what passes `allow_password_change` — change it,
    see their session, sign out.
    """
    if permission is not None and permission not in PERMISSION_KEYS:
        raise ValueError(f"{permission!r} is not a permission")
    if (dataset is not None or any_dataset) and (permission is None or not is_dataset_permission(permission)):
        raise ValueError(f"{permission!r} is not a dataset permission")
    principal = authenticate(request, directory_table, secrets=secrets, now=now)
    if principal.password_change_required and not allow_password_change:
        raise forbidden(
            "Choose a new password before you continue.",
            reason="password_change_required",
        )
    if permission is None:
        return principal
    if dataset is not None:
        found = require_dataset(directory_table, dataset)
        require(principal, permission, dataset=found)
    elif any_dataset:
        if not principal.can_anywhere(permission):
            raise denial(principal, permission)
    elif not principal.can(permission):
        raise denial(principal, permission)
    return principal


def require_dataset(directory_table: TableProtocol | None, name_or_id: str) -> Dataset:
    """The dataset a request names, by its name or id; 404 when there is none."""
    found = resolve_dataset(directory_table, name_or_id)
    if found is None:
        raise not_found(f"There is no dataset {name_or_id}.", resource="dataset", identifier=str(name_or_id))
    return found


def require(principal: Principal, permission: str, *, dataset: Dataset | None = None) -> None:
    """Refuse a principal that lacks a permission, as `authorize` would: on `dataset` when one is given."""
    if not principal.can(permission, None if dataset is None else dataset.dataset_id):
        raise denial(principal, permission, dataset=dataset)


def denial(principal: Principal, permission: str, *, dataset: Dataset | None = None):
    """The error for a principal that lacks a permission: sign in, or ask for access."""
    definition = permission_definition(permission)
    label = definition.label if definition is not None else permission
    where = "" if dataset is None else f" on dataset {dataset.name}"
    if principal.kind == "guest":
        return unauthenticated(f"Sign in to continue: this needs “{label}”{where}.", reason="sign_in_required")
    error = forbidden(f"You do not have permission to do this: it needs “{label}”{where}.", permission=permission)
    if dataset is not None:
        error.details["dataset"] = dataset.name
    return error


def _now() -> int:
    return int(datetime.now(tz=UTC).timestamp())
