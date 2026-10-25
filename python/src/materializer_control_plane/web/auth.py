"""WebView handlers for signing in and out, sessions, and changing your own password.

Signing in is answered at once: the password is checked against the directory here, and the
session token is issued here, so nothing waits on an agent. What happened is still published —
a sign-in, a failed attempt, a renewal, a sign-out — and the identity agent records it: the
session, the person's activity, the failed-attempt count that leads to a lockout, the audit log.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from materializer_control_plane.config import ControlPlaneConfig, get_config
from materializer_control_plane.identity.authorization import authenticate, authorize, directory_ready, request_context
from materializer_control_plane.identity.passwords import (
    PASSWORD_MAX_LENGTH,
    PasswordHashingBusy,
    hash_password_async,
    password_problem,
    verify_password_async,
)
from materializer_control_plane.identity.principals import Principal, cached_grants, guest_principal
from materializer_control_plane.identity.secrets import IdentitySecrets, get_identity_secrets
from materializer_control_plane.identity.tokens import SessionClaims, issue_token
from materializer_control_plane.identity.validation import EMAIL_MAX_LENGTH
from materializer_control_plane.models.identity import IdentityRequest, RequestContext, Session
from materializer_control_plane.settings import resolve_config
from materializer_control_plane.storage.identity import (
    find_user_by_email,
    get_sign_in_guard,
    get_user,
    iter_user_sessions,
    normalize_email,
)
from materializer_control_plane.storage.table import TableProtocol
from materializer_control_plane.web.errors import WebError, invalid_request, too_many_requests, unauthenticated

#: How people can sign in. Single sign-on providers join this list; the console's sign-in page
#: offers whatever it holds.
SIGN_IN_PROVIDERS: tuple[dict[str, str], ...] = (
    {"id": "password", "type": "password", "label": "Email and password"},
)

_INVALID_CREDENTIALS = "Email or password is incorrect."
_HASHING_BUSY = "Too many passwords are being checked at once. Try again in a moment."


@dataclass(frozen=True, slots=True)
class AuthResult:
    """A response whose status the view picks, and the event it publishes, if any."""

    status: int
    body: dict[str, Any]
    request_event: IdentityRequest | None = None

    def to_value(self) -> dict[str, Any]:
        """The value the WebView pipeline carries to its send and response steps."""
        return {
            "status": self.status,
            "body": self.body,
            "request_event": None if self.request_event is None else self.request_event.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class PasswordChangeAcceptance:
    """The 202 response to a password change, and the request that applies it."""

    response: dict[str, Any]
    request_event: IdentityRequest


async def sign_in(
    directory_table: TableProtocol | None,
    body: Any,
    *,
    request: Any = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    secrets: IdentitySecrets | None = None,
    now: int | None = None,
) -> AuthResult:
    """Check an email and password and start a session.

    Answers 200 with the session token; 401 for a wrong email or password, saying no more than
    that; 403 for a deactivated account, which only someone who knows its password learns; 429
    while the email is locked out; 503 when this deployment has no signing key, or while it is
    starting and has not read its directory back yet.
    """
    cfg = resolve_config(settings_table) if config is None else config
    identity_secrets = get_identity_secrets() if secrets is None else secrets
    current = _now() if now is None else now
    context = request_context(request) if request is not None else RequestContext()
    if not identity_secrets.can_sign_in:
        return AuthResult(
            503,
            _error(
                "unavailable",
                "Sign-in is not configured on this control plane: it has no session signing key.",
                reason="sign_in_unavailable",
            ),
        )
    if not directory_ready(directory_table):
        # A person not read back yet would be told their password is wrong, and counted for it.
        return AuthResult(
            503,
            _error(
                "unavailable",
                "The control plane is starting and still reading its tables back. Try again in a moment.",
                reason="starting",
            ),
        )
    if not isinstance(body, dict):
        return AuthResult(400, invalid_request("request body must be an object").to_response())
    raw_email = body.get("email")
    password = body.get("password")
    if not isinstance(raw_email, str) or not raw_email.strip():
        return AuthResult(400, invalid_request("Enter your email", field_name="email").to_response())
    if not isinstance(password, str) or not password:
        return AuthResult(400, invalid_request("Enter your password", field_name="password").to_response())
    email = normalize_email(raw_email)
    if len(email) > EMAIL_MAX_LENGTH or len(password) > PASSWORD_MAX_LENGTH:
        return AuthResult(401, unauthenticated(_INVALID_CREDENTIALS, reason="invalid_credentials").to_response())

    guard = get_sign_in_guard(directory_table, email)
    locked_until = _parse_time(None if guard is None else guard.locked_until)
    if locked_until is not None and locked_until > current:
        minutes = max(1, (locked_until - current + 59) // 60)
        return AuthResult(
            429,
            _error(
                "too_many_attempts",
                f"Too many failed sign-ins. Try again in {minutes} minute{'s' if minutes != 1 else ''}, "
                "or ask an administrator to unlock your account.",
                reason="locked_out",
                retry_after=locked_until - current,
            ),
        )

    user = find_user_by_email(directory_table, email)
    encoded = None if user is None or user.password is None else user.password.hash
    try:
        verified = await verify_password_async(password, encoded)
    except PasswordHashingBusy:
        return AuthResult(
            429,
            _error("too_many_attempts", "Too many sign-ins at once. Try again in a moment.", reason="busy"),
        )
    if not verified:
        reason = "unknown_email" if user is None else ("no_password" if user.password is None else "wrong_password")
        return AuthResult(
            401,
            unauthenticated(_INVALID_CREDENTIALS, reason="invalid_credentials").to_response(),
            _event(
                "sign_in_failed",
                actor_id=user.user_id if user is not None else "anonymous",
                actor_email=email,
                payload={"email": email, "reason": reason},
                context=context,
                now=current,
            ),
        )
    if not user.active:
        return AuthResult(
            403,
            _error(
                "forbidden",
                "This account is deactivated. Ask an administrator to reactivate it.",
                reason="account_deactivated",
            ),
            _event(
                "sign_in_failed",
                actor_id=user.user_id,
                actor_email=email,
                payload={"email": email, "reason": "deactivated"},
                context=context,
                now=current,
            ),
        )

    session_expires_at = current + cfg.session_max_lifetime_hours * 3600
    claims = SessionClaims(
        session_id=f"ses-{uuid4().hex[:16]}",
        user_id=user.user_id,
        session_epoch=user.session_epoch,
        issued_at=current,
        expires_at=min(current + cfg.session_idle_timeout_minutes * 60, session_expires_at),
        session_expires_at=session_expires_at,
        signed_in_at=current,
        method="password",
    )
    principal = Principal(
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
    return AuthResult(
        200,
        session_payload(principal, config=cfg, token=issue_token(claims, identity_secrets)),
        _event(
            "sign_in_succeeded",
            actor_id=user.user_id,
            actor_email=user.email,
            payload={
                "session_id": claims.session_id,
                "user_id": user.user_id,
                "method": claims.method,
                "created_at": _iso(current),
                "expires_at": _iso(claims.session_expires_at),
            },
            context=context,
            now=current,
        ),
    )


def refresh_session(
    directory_table: TableProtocol | None,
    request: Any,
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    secrets: IdentitySecrets | None = None,
    now: int | None = None,
) -> AuthResult:
    """Renew the caller's session: a new token whose idle timeout starts again.

    It never outlives the session's maximum length, which is fixed at sign-in.
    """
    cfg = resolve_config(settings_table) if config is None else config
    identity_secrets = get_identity_secrets() if secrets is None else secrets
    current = _now() if now is None else now
    principal = authorize(request, directory_table, None, allow_password_change=True, secrets=identity_secrets, now=current)
    if principal.kind != "user" or principal.session_id is None or principal.session_expires_at is None:
        raise unauthenticated("Sign in to renew a session.", reason="sign_in_required")
    user = get_user(directory_table, principal.user_id or "")
    if user is None:
        raise unauthenticated("Your session is not valid. Sign in again.", reason="invalid_session")
    claims = SessionClaims(
        session_id=principal.session_id,
        user_id=user.user_id,
        session_epoch=user.session_epoch,
        issued_at=current,
        expires_at=min(current + cfg.session_idle_timeout_minutes * 60, principal.session_expires_at),
        session_expires_at=principal.session_expires_at,
        signed_in_at=principal.signed_in_at or current,
        method=principal.sign_in_method or "password",
    )
    renewed = Principal(
        kind="user",
        grants=principal.grants,
        user_id=principal.user_id,
        email=principal.email,
        first_name=principal.first_name,
        last_name=principal.last_name,
        session_id=claims.session_id,
        session_expires_at=claims.session_expires_at,
        expires_at=claims.expires_at,
        issued_at=claims.issued_at,
        signed_in_at=claims.signed_in_at,
        sign_in_method=claims.method,
        password_change_required=principal.password_change_required,
    )
    return AuthResult(
        200,
        session_payload(renewed, config=cfg, token=issue_token(claims, identity_secrets)),
        _event(
            "session_refreshed",
            actor_id=user.user_id,
            actor_email=user.email,
            payload={"session_id": claims.session_id, "user_id": user.user_id},
            context=request_context(request),
            now=current,
        ),
    )


def sign_out(
    directory_table: TableProtocol | None,
    request: Any,
    body: Any = None,
    *,
    secrets: IdentitySecrets | None = None,
    now: int | None = None,
) -> AuthResult:
    """End the caller's session, another of their sessions, or all their others.

    Body `{"scope": "current" | "others" | "session", "session_id": …}`; `current` by default. A
    session that is already unusable is simply reported signed out.
    """
    current = _now() if now is None else now
    try:
        principal = authenticate(request, directory_table, secrets=secrets, now=current)
    except WebError as error:
        if error.status_code >= 500:
            # Not read back yet after a start: whether the session is usable is not known.
            raise
        return AuthResult(200, {"signed_out": True, "scope": "current"})
    if principal.kind != "user":
        return AuthResult(200, {"signed_out": False, "scope": "current"})
    options = body if isinstance(body, dict) else {}
    scope = options.get("scope") or "current"
    if scope not in ("current", "others", "session"):
        raise invalid_request("scope must be current, others or session", field_name="scope")
    target = options.get("session_id")
    if scope == "session" and (not isinstance(target, str) or not target):
        raise invalid_request("session_id is required to sign out another session", field_name="session_id")
    return AuthResult(
        200,
        {"signed_out": True, "scope": scope},
        _event(
            "sign_out",
            actor_id=principal.user_id or "",
            actor_email=principal.email,
            payload={
                "session_id": principal.session_id,
                "scope": scope,
                "target_session_id": target if scope == "session" else None,
                "session_expires_at": principal.session_expires_at,
            },
            context=request_context(request),
            now=current,
        ),
    )


def session_view(
    directory_table: TableProtocol | None,
    request: Any,
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    secrets: IdentitySecrets | None = None,
    now: int | None = None,
) -> dict[str, Any]:
    """Who the caller is and what they may do — a guest when they have no usable session.

    Never refuses: a session that cannot be used is reported as `session_error`, beside the guest
    the caller now is, so a client can tell an expired session from never having signed in.
    """
    cfg = resolve_config(settings_table) if config is None else config
    try:
        principal = authenticate(request, directory_table, secrets=secrets, now=now)
    except WebError as error:
        if error.status_code >= 500:
            # Still reading the directory back after a start: a session not found yet has not
            # ended, and a client that took it for one would sign its person out.
            raise
        return {
            **session_payload(guest_principal(directory_table), config=cfg),
            "session_error": error.details.get("reason", "invalid_session"),
        }
    return session_payload(principal, config=cfg)


def session_payload(principal: Principal, *, config: ControlPlaneConfig | None = None, token: str | None = None) -> dict[str, Any]:
    """A principal and their session as the auth endpoints report them."""
    cfg = get_config() if config is None else config
    payload: dict[str, Any] = {
        "authenticated": principal.kind == "user",
        "principal": principal.describe(),
        "session": None,
        "providers": [dict(provider) for provider in SIGN_IN_PROVIDERS],
        "password_policy": {"min_length": cfg.password_min_length, "max_length": PASSWORD_MAX_LENGTH},
    }
    if principal.kind == "user" and principal.session_id is not None:
        issued = principal.issued_at or principal.signed_in_at or _now()
        payload["session"] = {
            "session_id": principal.session_id,
            "method": principal.sign_in_method,
            "signed_in_at": _iso(principal.signed_in_at),
            "issued_at": _iso(issued),
            "expires_at": _iso(principal.expires_at),
            "session_expires_at": _iso(principal.session_expires_at),
            "refresh_after": _iso(issued + cfg.session_refresh_minutes * 60),
        }
    if token is not None:
        payload["token"] = token
        payload["token_type"] = "Bearer"
    return payload


async def change_password(
    directory_table: TableProtocol | None,
    request: Any,
    body: Any,
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    accepted_at: str | None = None,
) -> PasswordChangeAcceptance:
    """Replace the caller's password, given their current one.

    Every session of theirs ends once it applies, this one included, so the client signs in again
    with the new password.
    """
    cfg = resolve_config(settings_table) if config is None else config
    principal = authorize(request, directory_table, None, allow_password_change=True)
    if principal.kind != "user":
        raise unauthenticated("Sign in to change your password.", reason="sign_in_required")
    user = get_user(directory_table, principal.user_id or "")
    if user is None:
        raise unauthenticated("Your session is not valid. Sign in again.", reason="invalid_session")
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    current_password = body.get("current_password")
    new_password = body.get("new_password")
    if not isinstance(current_password, str) or not current_password:
        raise invalid_request("Enter your current password", field_name="current_password")
    try:
        verified = len(current_password) <= PASSWORD_MAX_LENGTH and await verify_password_async(
            current_password,
            None if user.password is None else user.password.hash,
        )
    except PasswordHashingBusy as error:
        raise too_many_requests(_HASHING_BUSY, reason="busy") from error
    if not verified:
        raise invalid_request("Your current password is not correct", field_name="current_password")
    problem = password_problem(
        new_password,
        min_length=cfg.password_min_length,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    if problem is not None:
        raise invalid_request(problem, field_name="new_password")
    if new_password == current_password:
        raise invalid_request("Choose a password different from your current one", field_name="new_password")
    password_hash = await hash_new_password(new_password)
    submitted_at = accepted_at or _iso(_now())
    event = IdentityRequest(
        request_id=f"idr-{uuid4().hex[:12]}",
        action="change_password",
        submitted_at=submitted_at,
        actor_id=user.user_id,
        actor_email=user.email,
        payload={"user_id": user.user_id, "password_hash": password_hash},
        context=request_context(request),
    )
    return PasswordChangeAcceptance(
        response={
            "request_id": event.request_id,
            "action": event.action,
            "entity_type": "user",
            "entity_id": user.user_id,
            "accepted_at": submitted_at,
            "status_url": f"/identity/requests/{event.request_id}",
        },
        request_event=event,
    )


def my_sessions_view(
    directory_table: TableProtocol | None,
    request: Any,
    *,
    now: int | None = None,
) -> dict[str, Any]:
    """The caller's recorded sessions, newest first, marking the one making this request."""
    principal = authorize(request, directory_table, None, allow_password_change=True, now=now)
    if principal.kind != "user":
        raise unauthenticated("Sign in to see your sessions.", reason="sign_in_required")
    current = _now() if now is None else now
    sessions = iter_user_sessions(directory_table, principal.user_id or "")
    return {
        "current_session_id": principal.session_id,
        "sessions": [describe_session(session, current=principal.session_id, now=current) for session in sessions],
    }


async def hash_new_password(password: str) -> str:
    """Hash a password someone is setting, or refuse with 429 while the hashing pool is full."""
    try:
        return await hash_password_async(password)
    except PasswordHashingBusy as error:
        raise too_many_requests(_HASHING_BUSY, reason="busy") from error


def describe_session(session: Session, *, current: str | None, now: int) -> dict[str, Any]:
    """A session as the account pages show it."""
    expires = _parse_time(session.expires_at)
    return {
        "session_id": session.session_id,
        "method": session.method,
        "created_at": session.created_at,
        "last_seen_at": session.last_seen_at,
        "expires_at": session.expires_at,
        "ip": session.ip,
        "user_agent": session.user_agent,
        "current": session.session_id == current,
        "active": not session.ended and (expires is None or expires > now),
        "ended_at": session.ended_at,
        "ended_reason": session.ended_reason,
    }


def _event(
    action: str,
    *,
    actor_id: str,
    actor_email: str | None,
    payload: dict[str, Any],
    context: RequestContext,
    now: int,
) -> IdentityRequest:
    return IdentityRequest(
        request_id=f"idr-{uuid4().hex[:12]}",
        action=action,
        submitted_at=_iso(now),
        actor_id=actor_id or "anonymous",
        actor_email=actor_email,
        payload=payload,
        context=context,
    )


def _error(code: str, message: str, **details: Any) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details}}


def _now() -> int:
    return int(datetime.now(tz=UTC).timestamp())


def _iso(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_time(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp())
