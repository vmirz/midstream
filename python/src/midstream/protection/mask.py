"""The masks a protected value can take (design §5.2).

| Mask | Value | Type |
|---|---|---|
| `hash` | `HMAC-SHA256(key, canonical(value))`, the first 128 bits as 32 lowercase hex characters | string |
| `partial` | `****` with the fragment the value's kind keeps: `****9848`, `****@example.com`, `J****` | string |
| `redact` | nothing: the key is dropped from the record | — |

Every mask works on a value's canonical text, its JSON text: a string as it is, a number as its
decimal text, so a phone number arriving as `5551234567` and as `"5551234567"` masks alike. A null
and a boolean are never masked: no kind's values are true or false, and a missing value stays
missing.

The partial mask hides with exactly four asterisks, however long the hidden part, so a value's
length leaks nothing, and never shows more than half of a value's significant characters, nor more
than four.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import re
from typing import Any

from midstream.protection.catalog import (
    KINDS_BY_ID,
    MASK_HASH,
    MASK_PARTIAL,
    MASK_REDACT,
    PARTIAL_EMAIL_DOMAIN,
    PARTIAL_FIRST_LETTER,
    PARTIAL_IP_PREFIX,
    PARTIAL_LAST_CHARACTERS,
    PARTIAL_LAST_DIGITS,
    PARTIAL_NOTHING,
    PARTIAL_YEAR,
)

#: What a partial mask puts in place of what it hides.
HIDDEN = "****"
#: Hex characters of a hashed value: 128 bits.
HASH_HEX_LENGTH = 32
#: Most significant characters a partial mask keeps from a value's end.
PARTIAL_TAIL = 4

_ISO_DATE = re.compile(r"^(\d{4})-\d{2}-\d{2}")


class Redacted:
    """What `apply` returns for a withheld value: the walk drops the key."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "REDACTED"


REDACTED = Redacted()


def canonical(value: Any) -> str | None:
    """The text a value is masked from; None for a value that is never masked (null, boolean, container)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return json.dumps(value)
    return None


def hash_text(secret: bytes, text: str) -> str:
    """The keyed hash of a canonical text."""
    return hmac.new(secret, text.encode("utf-8"), hashlib.sha256).hexdigest()[:HASH_HEX_LENGTH]


def partial_text(kind_id: str, text: str) -> str:
    """What a partial mask of `kind_id` makes of a canonical text."""
    item = KINDS_BY_ID.get(kind_id)
    rule = PARTIAL_NOTHING if item is None else item.partial
    if rule == PARTIAL_LAST_DIGITS:
        return HIDDEN + _tail([character for character in text if character.isdigit()])
    if rule == PARTIAL_LAST_CHARACTERS:
        return HIDDEN + _tail([character for character in text if character.isalnum()])
    if rule == PARTIAL_EMAIL_DOMAIN:
        if "@" not in text:
            return HIDDEN
        domain = text.rsplit("@", 1)[1].strip()
        return f"{HIDDEN}@{domain}" if domain else HIDDEN
    if rule == PARTIAL_FIRST_LETTER:
        letters = [character for character in text if character.isalpha()]
        if len(letters) < 2:
            return HIDDEN
        return letters[0] + HIDDEN
    if rule == PARTIAL_IP_PREFIX:
        return _ip_prefix(text)
    if rule == PARTIAL_YEAR:
        match = _ISO_DATE.match(text.strip())
        return f"{match.group(1)}-**-**" if match else HIDDEN
    return HIDDEN


def _tail(significant: list[str]) -> str:
    keep = min(PARTIAL_TAIL, len(significant) // 2)
    return "".join(significant[len(significant) - keep :]) if keep else ""


def _ip_prefix(text: str) -> str:
    try:
        address = ipaddress.ip_address(text.strip())
    except ValueError:
        return HIDDEN
    if isinstance(address, ipaddress.IPv4Address):
        first, second, _, _ = str(address).split(".")
        return f"{first}.{second}.*.*"
    groups = address.exploded.split(":")
    return f"{groups[0].lstrip('0') or '0'}:{groups[1].lstrip('0') or '0'}:{HIDDEN}"


def apply(mask: str, kind_id: str, value: Any, secret: bytes | None) -> Any:
    """`value` masked: a string for `hash` and `partial`, `REDACTED` for `redact`; the value itself
    when it is never masked (`canonical` returns None)."""
    text = canonical(value)
    if text is None:
        return value
    if mask == MASK_REDACT:
        return REDACTED
    if mask == MASK_HASH:
        if secret is None:
            raise MissingProtectionKey("hashing needs the protection key")
        return hash_text(secret, text)
    if mask == MASK_PARTIAL:
        return partial_text(kind_id, text)
    raise ValueError(f"unknown mask {mask!r}")


class MissingProtectionKey(RuntimeError):
    """A value had to be hashed and there is no protection key: it must not be written at all."""
