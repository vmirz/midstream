"""Web-layer error types and response helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class WebError(Exception):
    """Structured application error that can be returned directly by the web layer."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    status_code: int = 400

    def to_response(self) -> dict[str, Any]:
        """Convert the error into the JSON payload expected by web clients."""
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": dict(self.details),
            }
        }


def error_response(error: WebError) -> dict[str, Any]:
    """Render a structured web error into its JSON response payload."""
    return error.to_response()


def invalid_request(message: str, *, field_name: str | None = None) -> WebError:
    """Build a 400 error for malformed or semantically invalid client input."""
    details = {} if field_name is None else {"field": field_name}
    return WebError(
        code="invalid_request",
        message=message,
        details=details,
        status_code=400,
    )


def not_found(message: str, *, resource: str, identifier: str) -> WebError:
    """Build a 404 error for a missing domain resource."""
    return WebError(
        code="not_found",
        message=message,
        details={"resource": resource, "id": identifier},
        status_code=404,
    )


def unavailable(message: str, *, reason: str) -> WebError:
    """Build a 503 error for a dependency the control plane cannot reach right now."""
    return WebError(
        code="unavailable",
        message=message,
        details={"reason": reason},
        status_code=503,
    )


def unauthenticated(message: str, *, reason: str) -> WebError:
    """Build a 401 error: the request needs a valid session, and has none.

    `reason` says why: `sign_in_required` for a guest asking for something guests cannot do;
    `invalid_session`, `session_expired`, `session_ended` or `account_deactivated` for a session
    that can no longer be used; `invalid_credentials` and the like for a failed sign-in.
    """
    return WebError(
        code="unauthenticated",
        message=message,
        details={"reason": reason},
        status_code=401,
    )


def forbidden(message: str, *, reason: str = "forbidden", permission: str | None = None) -> WebError:
    """Build a 403 error: the caller is known, and may not do this.

    `permission` names what they would need; `reason` is `password_change_required` while a
    person must still replace a password someone else chose.
    """
    details: dict[str, Any] = {"reason": reason}
    if permission is not None:
        details["permission"] = permission
    return WebError(
        code="forbidden",
        message=message,
        details=details,
        status_code=403,
    )


def too_many_requests(message: str, *, reason: str) -> WebError:
    """Build a 429 error: the caller should wait a moment and try again.

    `reason` is `busy` when too many passwords are already waiting to be hashed.
    """
    return WebError(
        code="too_many_attempts",
        message=message,
        details={"reason": reason},
        status_code=429,
    )


def conflict(message: str, *, reason: str) -> WebError:
    """Build a 409 error for a request the materialization's current state does not allow."""
    return WebError(
        code="conflict",
        message=message,
        details={"reason": reason},
        status_code=409,
    )


def already_exists(
    message: str,
    *,
    resource: str,
    identifier: str,
    field_name: str | None = None,
) -> WebError:
    """Build a 409 error for a conflicting resource creation request."""
    details: dict[str, Any] = {"resource": resource, "id": identifier}
    if field_name is not None:
        details["field"] = field_name
    return WebError(
        code="already_exists",
        message=message,
        details=details,
        status_code=409,
    )