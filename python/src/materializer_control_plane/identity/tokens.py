"""Session tokens: what a signed-in client presents on every request.

A token is `v1.<claims>.<signature>`: base64url JSON claims and an HMAC-SHA256 over them, made with
the deployment's signing key. It is valid as soon as it is issued — nothing has to be written
first — and a request with it is authorized against the current directory, so deactivating a
person or ending their sessions takes effect at their next request whatever the token says.

The claims are signed, not encrypted: they hold ids and times, never anything secret.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
from typing import Any

from materializer_control_plane.identity.secrets import IdentitySecrets

TOKEN_VERSION = "v1"
#: The longest token accepted, so a header full of junk is refused before any work is done.
MAX_TOKEN_LENGTH = 2048
#: Clock skew tolerated between the replica that issued a token and the one reading it.
CLOCK_SKEW_SECONDS = 30


class InvalidToken(Exception):
    """A token that cannot be used. `reason` is `malformed`, `signature` or `expired`."""

    def __init__(self, reason: str, message: str) -> None:
        self.reason = reason
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class SessionClaims:
    """What a session token says: whose session it is, and until when it may be used.

    `expires_at` is when the session ends unless it is renewed — the idle timeout — and is never
    later than `session_expires_at`, when it ends however active it is. Times are epoch seconds.
    `session_epoch` must equal the person's current epoch, which ending all their sessions raises.
    """

    session_id: str
    user_id: str
    session_epoch: int
    issued_at: int
    expires_at: int
    session_expires_at: int
    signed_in_at: int
    method: str = "password"

    def to_dict(self) -> dict[str, Any]:
        return {
            "sid": self.session_id,
            "sub": self.user_id,
            "ep": self.session_epoch,
            "iat": self.issued_at,
            "exp": self.expires_at,
            "max": self.session_expires_at,
            "auth": self.signed_in_at,
            "amr": self.method,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SessionClaims":
        try:
            claims = cls(
                session_id=_text(value["sid"]),
                user_id=_text(value["sub"]),
                session_epoch=_integer(value["ep"]),
                issued_at=_integer(value["iat"]),
                expires_at=_integer(value["exp"]),
                session_expires_at=_integer(value["max"]),
                signed_in_at=_integer(value["auth"]),
                method=_text(value.get("amr", "password")),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise InvalidToken("malformed", "the session token is malformed") from error
        return claims


def issue_token(claims: SessionClaims, secrets: IdentitySecrets) -> str:
    """Sign a session's claims with the current signing key."""
    key = secrets.current_key
    if key is None:
        raise RuntimeError("sessions cannot be issued without a signing key")
    payload = _encode(
        json.dumps({**claims.to_dict(), "kid": key.key_id}, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signed = f"{TOKEN_VERSION}.{payload}"
    signature = hmac.new(key.secret, signed.encode("ascii"), hashlib.sha256).digest()
    return f"{signed}.{_encode(signature)}"


def read_token(token: str, secrets: IdentitySecrets, *, now: int) -> SessionClaims:
    """Verify a token's signature and lifetime and return its claims.

    Raises `InvalidToken` for anything that is not a token this deployment issued and that is still
    within both its idle and its absolute lifetime.
    """
    if not token or len(token) > MAX_TOKEN_LENGTH:
        raise InvalidToken("malformed", "the session token is malformed")
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != TOKEN_VERSION:
        raise InvalidToken("malformed", "the session token is malformed")
    try:
        raw = json.loads(_decode(parts[1]))
    except (ValueError, UnicodeDecodeError) as error:
        raise InvalidToken("malformed", "the session token is malformed") from error
    if not isinstance(raw, dict):
        raise InvalidToken("malformed", "the session token is malformed")
    key = secrets.key(str(raw.get("kid", "")))
    if key is None:
        raise InvalidToken("signature", "the session was not issued by this deployment")
    expected = hmac.new(key.secret, f"{parts[0]}.{parts[1]}".encode("ascii"), hashlib.sha256).digest()
    try:
        presented = _decode(parts[2])
    except ValueError as error:
        raise InvalidToken("malformed", "the session token is malformed") from error
    if not hmac.compare_digest(expected, presented):
        raise InvalidToken("signature", "the session was not issued by this deployment")
    claims = SessionClaims.from_dict(raw)
    if now > claims.expires_at or now > claims.session_expires_at:
        raise InvalidToken("expired", "the session has expired")
    if claims.issued_at > now + CLOCK_SKEW_SECONDS:
        raise InvalidToken("malformed", "the session token is not valid yet")
    return claims


def peek_claims(token: str) -> SessionClaims | None:
    """A token's claims without verifying it, for code that only needs to read them back.

    Never use this to decide whether a token may be used; `read_token` does that.
    """
    try:
        parts = token.split(".")
        return SessionClaims.from_dict(json.loads(_decode(parts[1])))
    except (IndexError, ValueError, InvalidToken, UnicodeDecodeError):
        return None


def _text(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("expected text")
    return value


def _integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("expected an integer")
    return value


def _encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode(text: str) -> bytes:
    if not text or any(char not in _ALPHABET for char in text):
        raise ValueError("not base64url")
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


_ALPHABET = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
