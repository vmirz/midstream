"""Authorizing a request to the tail app, as the control plane would: the first thing every view does.

```python
principal = await authorize_tail(request, "data.query", dataset_id=..., dataset=...)
```

The tail app has no `directory` table, so it cannot resolve a session itself. It asks the control
plane who the caller is - `GET /auth/session` with the caller's own bearer, as the console does - and
rebuilds the answer's principal (`identity/principals.py::principal_from_description`), so the check
is the control plane's own `Principal.can`. The answer is trusted for `session_check_seconds` (5),
keyed by a hash of the token, and for `session_grace_seconds` (60) more while the control plane cannot
be reached.

Two rules come first:

- **The console's alone.** Every request carries the console's service credential in
  `X-Materializer-Console` besides the person's bearer, or it is `403 forbidden`, `console_only`: a
  script holding a valid token cannot use these endpoints, which are no public API.
- **People only, for rows.** A guest, and the service credential itself, hold no `data.query` by
  default, and a person who must change their password is refused, as everywhere.

A member that forwards a request checks first; the owner checks again (one cached lookup).
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import hmac
import time
from typing import Any

from midstream.identity.authorization import bearer_token, header
from midstream.identity.permissions import permission_definition
from midstream.identity.principals import Principal, principal_from_description
from midstream.tail_app.control_plane import ControlPlane, ControlPlaneError
from midstream.web.errors import WebError, forbidden, unauthenticated, unavailable

CONSOLE_HEADER = "x-materializer-console"
#: Sessions remembered at most, the console's own bound.
SESSION_CACHE_LIMIT = 10_000
#: The request headers a check passes on, so the control plane records where a request came from.
FORWARDED_HEADERS = ("x-forwarded-for", "x-forwarded-user-agent", "user-agent")


@dataclass(frozen=True, slots=True)
class _Checked:
    payload: dict[str, Any]
    at: float


class SessionChecker:
    """Who a bearer is, as the control plane says, remembered briefly by a hash of the token."""

    def __init__(self, control_plane: ControlPlane, *, check_seconds: float, grace_seconds: float, clock: Any = time.monotonic) -> None:
        self.control_plane = control_plane
        self.check_seconds = check_seconds
        self.grace_seconds = grace_seconds
        self._clock = clock
        self._cache: OrderedDict[str, _Checked] = OrderedDict()

    async def check(self, token: str | None, *, forwarded: dict[str, str] | None = None) -> dict[str, Any]:
        """The control plane's `GET /auth/session` answer for `token`."""
        key = hashlib.sha256((token or "").encode("utf-8")).hexdigest()
        now = self._clock()
        cached = self._cache.get(key)
        if cached is not None and now - cached.at < self.check_seconds:
            self._cache.move_to_end(key)
            return cached.payload
        try:
            payload = await self.control_plane.session(token, forwarded=forwarded)
        except ControlPlaneError as error:
            if cached is not None and now - cached.at < self.check_seconds + self.grace_seconds:
                return cached.payload
            raise unavailable(
                "The tail cannot check who you are: the control plane cannot be reached. Try again in a moment.",
                reason="control_plane_unreachable",
            ) from error
        self._cache[key] = _Checked(payload=payload, at=now)
        self._cache.move_to_end(key)
        while len(self._cache) > SESSION_CACHE_LIMIT:
            self._cache.popitem(last=False)
        return payload

    def forget(self) -> None:
        self._cache.clear()


def require_console(request: Any, console_token: str | None) -> None:
    """Refuse a request that does not carry the console's own credential."""
    presented = header(request, CONSOLE_HEADER) or ""
    if not console_token or not presented or not hmac.compare_digest(presented.encode("utf-8"), console_token.encode("utf-8")):
        raise forbidden(
            "The live tail answers the Materializer Console only.",
            reason="console_only",
        )


async def authorize_tail(
    request: Any,
    permission: str,
    *,
    sessions: SessionChecker,
    console_token: str | None,
    dataset_id: str | None = None,
    dataset: str | None = None,
) -> Principal:
    """Authenticate a request through the control plane and require `permission` of it.

    A dataset permission is required on `dataset_id` (its name, `dataset`, only names it in the
    refusal); a deployment permission across the deployment.
    """
    require_console(request, console_token)
    token = bearer_token(request)
    forwarded = {name: value for name in FORWARDED_HEADERS if (value := header(request, name))}
    payload = await sessions.check(token, forwarded=forwarded)
    session_error = payload.get("session_error")
    if token and session_error:
        raise unauthenticated(_session_message(str(session_error)), reason=str(session_error))
    principal = principal_from_description(payload["principal"])
    if principal.password_change_required:
        raise forbidden("Choose a new password before you continue.", reason="password_change_required")
    if not principal.can(permission, dataset_id):
        raise refusal(principal, permission, dataset=dataset)
    return principal


def refusal(principal: Principal, permission: str, *, dataset: str | None = None) -> WebError:
    """The error for a principal that lacks a permission, worded as the control plane words it."""
    definition = permission_definition(permission)
    label = definition.label if definition is not None else permission
    where = "" if dataset is None else f" on dataset {dataset}"
    if principal.kind == "guest":
        return unauthenticated(f"Sign in to continue: this needs “{label}”{where}.", reason="sign_in_required")
    error = forbidden(f"You do not have permission to do this: it needs “{label}”{where}.", permission=permission)
    if dataset is not None:
        error.details["dataset"] = dataset
    return error


def _session_message(reason: str) -> str:
    if reason == "session_expired":
        return "Your session has expired. Sign in again."
    if reason == "session_ended":
        return "This session was ended. Sign in again."
    if reason in {"token_revoked", "token_expired", "invalid_token"}:
        return "This access token cannot be used any more."
    return "Your session is not valid. Sign in again."
