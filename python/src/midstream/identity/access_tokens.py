"""Access tokens (handbook 20): one token for an engine, good in every domain where its owner has an account.

A token reads `tok_<claims>.<signature>`: base64url JSON claims and an HMAC-SHA256 over them, made
with the **federation key** - the same key in every domain's auth Secret. Any domain checks a token
on its own, with no call to the domain that made it: the signature with its current or previous
federation key, the expiry, that it was not revoked, and then who it belongs to there.

The claims are signed, not encrypted, and hold nothing secret:

| Claim | Holds |
| --- | --- |
| `jti` | The token id, 12 hex characters |
| `iss` | The domain that made it |
| `own` | Its owner: the provider account they signed in with, if any, and their email |
| `perm` | `catalog.read`, and perhaps `catalog.write` |
| `ds` | The datasets it is limited to, as `<domain>.<dataset>`; absent, every dataset its owner may use |
| `iat`, `exp` | When it was made, and when it stops working: at most 365 days later |

A token carries a subset of its owner's catalog permissions and never more than they hold where it
is used: removing someone from a group narrows their tokens at once, and deactivating them stops
every one. Revoking one records `revoked:<jti>` in the domain that made it and, through the console,
in every other: a **revocation** is signed with the federation key too, so a domain records one it
is handed without anyone signing in.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import re
import secrets
from typing import Any

from midstream.identity.secrets import IdentitySecrets

ACCESS_TOKEN_PREFIX = "tok_"
#: What a token may carry. Tokens are for engines reading (or committing to) tables, nothing else.
TOKEN_PERMISSIONS = ("catalog.read", "catalog.write")
DEFAULT_TOKEN_PERMISSIONS = ("catalog.read",)
#: Tokens one person may hold at once in a domain, revoked ones aside.
MAX_TOKENS_PER_USER = 20
#: Longest a token may live, in days; a token made without an expiry lives this long.
MAX_TOKEN_LIFETIME_DAYS = 365
TOKEN_NAME_MAX_LENGTH = 64
#: The longest token text accepted: room for a hundred `<domain>.<dataset>` limits.
MAX_TOKEN_LENGTH = 16 * 1024
#: Clock skew tolerated between the domain that made a token and the one reading it.
CLOCK_SKEW_SECONDS = 60

_TOKEN_ID = re.compile(r"^[0-9a-f]{12}$")
_ALPHABET = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")


class InvalidAccessToken(Exception):
    """A token that cannot be used: `reason` is `malformed`, `signature` or `expired`."""

    def __init__(self, reason: str, message: str) -> None:
        self.reason = reason
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class TokenOwner:
    """Who a token acts for: the provider account they signed in with, if any, and their email.

    A domain finds them by the provider account first, else by the email; someone it does not know
    cannot use the token there.
    """

    email: str
    provider: str | None = None
    subject: str | None = None

    def to_dict(self) -> dict[str, str]:
        owner = {"email": self.email}
        if self.provider and self.subject:
            owner.update(provider=self.provider, subject=self.subject)
        return owner

    @classmethod
    def from_dict(cls, value: Any) -> "TokenOwner":
        if not isinstance(value, dict) or not isinstance(value.get("email"), str) or not value["email"]:
            raise ValueError("a token's owner needs an email")
        provider, subject = value.get("provider"), value.get("subject")
        if (provider is None) != (subject is None):
            raise ValueError("a token's provider account needs both its provider and its subject")
        if provider is not None and not (isinstance(provider, str) and isinstance(subject, str) and provider and subject):
            raise ValueError("a token's provider account must be text")
        return cls(email=value["email"], provider=provider, subject=subject)


@dataclass(frozen=True, slots=True)
class TokenClaims:
    """What a token says. Times are epoch seconds."""

    token_id: str
    issuer: str
    owner: TokenOwner
    permissions: tuple[str, ...]
    issued_at: int
    expires_at: int
    #: `<domain>.<dataset>` names; empty for every dataset its owner may use.
    datasets: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        claims: dict[str, Any] = {
            "jti": self.token_id,
            "iss": self.issuer,
            "own": self.owner.to_dict(),
            "perm": list(self.permissions),
            "iat": self.issued_at,
            "exp": self.expires_at,
        }
        if self.datasets:
            claims["ds"] = list(self.datasets)
        return claims

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TokenClaims":
        try:
            permissions = value["perm"]
            datasets = value.get("ds") or []
            if not isinstance(permissions, list) or not permissions or not all(item in TOKEN_PERMISSIONS for item in permissions):
                raise ValueError("permissions")
            if not isinstance(datasets, list) or not all(isinstance(item, str) and "." in item for item in datasets):
                raise ValueError("datasets")
            token_id = value["jti"]
            if not is_token_id(token_id):
                raise ValueError("jti")
            issuer = value["iss"]
            if not isinstance(issuer, str) or not issuer:
                raise ValueError("iss")
            return cls(
                token_id=token_id,
                issuer=issuer,
                owner=TokenOwner.from_dict(value["own"]),
                permissions=tuple(permissions),
                issued_at=_integer(value["iat"]),
                expires_at=_integer(value["exp"]),
                datasets=tuple(datasets),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise InvalidAccessToken("malformed", "the access token is malformed") from error

    def datasets_in(self, domain_id: str) -> tuple[str, ...] | None:
        """The names of this domain's datasets the token is limited to; None when it is not limited."""
        if not self.datasets:
            return None
        prefix = f"{domain_id}."
        return tuple(name[len(prefix):] for name in self.datasets if name.startswith(prefix))


def new_token_id() -> str:
    """A token's id: 12 random hex characters."""
    return secrets.token_hex(6)


def is_token_id(value: Any) -> bool:
    return isinstance(value, str) and _TOKEN_ID.match(value) is not None


def sign_access_token(claims: TokenClaims, identity_secrets: IdentitySecrets) -> str:
    """The text of a token: its claims, signed with the current federation key."""
    key = identity_secrets.current_federation_key
    if key is None:
        raise RuntimeError("access tokens cannot be made without a federation key")
    payload = _encode(json.dumps({**claims.to_dict(), "kid": key.key_id}, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(key.secret, f"{ACCESS_TOKEN_PREFIX}{payload}".encode("ascii"), hashlib.sha256).digest()
    return f"{ACCESS_TOKEN_PREFIX}{payload}.{_encode(signature)}"


def read_access_token(text: str, identity_secrets: IdentitySecrets, *, now: int) -> TokenClaims:
    """Check a token's signature and lifetime, and return its claims.

    Raises `InvalidAccessToken` for anything that is not a token one of the domains made with a
    federation key this domain holds, or that has expired. Whether it was revoked, and whom it acts
    for here, the caller decides against the directory.
    """
    if not text.startswith(ACCESS_TOKEN_PREFIX) or len(text) > MAX_TOKEN_LENGTH:
        raise InvalidAccessToken("malformed", "the access token is malformed")
    payload, separator, signature = text[len(ACCESS_TOKEN_PREFIX):].partition(".")
    if not separator or not payload or not signature:
        raise InvalidAccessToken("malformed", "the access token is malformed")
    try:
        raw = json.loads(_decode(payload))
        presented = _decode(signature)
    except (ValueError, UnicodeDecodeError) as error:
        raise InvalidAccessToken("malformed", "the access token is malformed") from error
    if not isinstance(raw, dict):
        raise InvalidAccessToken("malformed", "the access token is malformed")
    key = identity_secrets.federation_key(str(raw.get("kid", "")))
    if key is None:
        raise InvalidAccessToken("signature", "the access token was not made with this deployment's federation key")
    expected = hmac.new(key.secret, f"{ACCESS_TOKEN_PREFIX}{payload}".encode("ascii"), hashlib.sha256).digest()
    if not hmac.compare_digest(expected, presented):
        raise InvalidAccessToken("signature", "the access token was not made with this deployment's federation key")
    claims = TokenClaims.from_dict(raw)
    if now >= claims.expires_at:
        raise InvalidAccessToken("expired", "the access token has expired")
    if claims.issued_at > now + CLOCK_SKEW_SECONDS:
        raise InvalidAccessToken("malformed", "the access token is not valid yet")
    return claims


@dataclass(frozen=True, slots=True)
class SignedRevocation:
    """That a token was revoked, signed so any domain records it from whoever hands it over."""

    token_id: str
    expires_at: int
    signature: str

    def to_dict(self) -> dict[str, Any]:
        return {"jti": self.token_id, "exp": self.expires_at, "signature": self.signature}


def sign_revocation(token_id: str, expires_at: int, identity_secrets: IdentitySecrets) -> SignedRevocation:
    key = identity_secrets.current_federation_key
    if key is None:
        raise RuntimeError("revocations cannot be signed without a federation key")
    mac = hmac.new(key.secret, _revocation_text(token_id, expires_at), hashlib.sha256).digest()
    return SignedRevocation(token_id=token_id, expires_at=expires_at, signature=f"{key.key_id}.{_encode(mac)}")


def read_revocation(value: Any, identity_secrets: IdentitySecrets) -> SignedRevocation | None:
    """A revocation a domain signed, or None when it is not one this domain can trust."""
    if not isinstance(value, dict):
        return None
    token_id, expires_at, signature = value.get("jti"), value.get("exp"), value.get("signature")
    if not is_token_id(token_id) or isinstance(expires_at, bool) or not isinstance(expires_at, int) or not isinstance(signature, str):
        return None
    key_id, _, encoded = signature.partition(".")
    key = identity_secrets.federation_key(key_id)
    if key is None:
        return None
    try:
        presented = _decode(encoded)
    except ValueError:
        return None
    expected = hmac.new(key.secret, _revocation_text(token_id, expires_at), hashlib.sha256).digest()
    if not hmac.compare_digest(expected, presented):
        return None
    return SignedRevocation(token_id=token_id, expires_at=expires_at, signature=signature)


def _revocation_text(token_id: str, expires_at: int) -> bytes:
    return f"revoked:{token_id}:{expires_at}".encode("ascii")


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
