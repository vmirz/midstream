"""Access tokens: long-lived credentials a person makes for tools that read the catalog (spec §11.4).

A token reads `tok_<token id>_<secret>`. It is shown once, when it is made; the directory keeps
only a SHA-256 hash of its secret, which is enough: the secret is 32 random bytes, so there is
nothing to guess and no reason to hash it slowly the way a password is.

A token carries a subset of its owner's permissions, of `TOKEN_PERMISSIONS` only, and never more
than its owner holds at the moment it is used: removing someone from a group narrows their tokens
at once, and deactivating them stops every one.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import secrets

ACCESS_TOKEN_PREFIX = "tok_"
#: What a token may carry. Tokens are for engines reading (or committing to) tables, nothing else.
TOKEN_PERMISSIONS = ("catalog.read", "catalog.write")
DEFAULT_TOKEN_PERMISSIONS = ("catalog.read",)
#: Tokens one person may hold at once, revoked ones aside.
MAX_TOKENS_PER_USER = 20
#: Longest a token may live, in days; a token made without an expiry lives this long.
MAX_TOKEN_LIFETIME_DAYS = 365
TOKEN_NAME_MAX_LENGTH = 64

_SECRET_BYTES = 32
_HASH_PREFIX = "sha256:"


@dataclass(frozen=True, slots=True)
class IssuedToken:
    """A token just made: the text to show once, and what the directory keeps of it."""

    token_id: str
    text: str
    secret_hash: str


def issue_token() -> IssuedToken:
    """A new token: its id, its text, and the hash of its secret."""
    token_id = secrets.token_hex(6)
    secret = secrets.token_urlsafe(_SECRET_BYTES)
    return IssuedToken(
        token_id=token_id,
        text=f"{ACCESS_TOKEN_PREFIX}{token_id}_{secret}",
        secret_hash=hash_token_secret(secret),
    )


def hash_token_secret(secret: str) -> str:
    return _HASH_PREFIX + hashlib.sha256(secret.encode("utf-8")).hexdigest()


def parse_token(text: str) -> tuple[str, str] | None:
    """`(token id, secret)` of a token's text, or None when it is not shaped like one."""
    if not text.startswith(ACCESS_TOKEN_PREFIX):
        return None
    token_id, separator, secret = text[len(ACCESS_TOKEN_PREFIX):].partition("_")
    if not separator or len(token_id) != 12 or not secret:
        return None
    try:
        int(token_id, 16)
    except ValueError:
        return None
    return token_id, secret


def secret_matches(secret: str, secret_hash: str) -> bool:
    return hmac.compare_digest(hash_token_secret(secret).encode("utf-8"), secret_hash.encode("utf-8"))
