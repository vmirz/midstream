"""The protection key: what `hash` masks are keyed with (design §5.2, §5.11).

`protection-key` in the deployment's auth Secret, given to the control plane, every runtime, the
tail app and the maintenance app as `PROTECTION_KEY`. It is never a setting and never leaves the
Secret; only its id — the first eight hex characters of its SHA-256 — is shown, recorded beside the
hashes it made, and rendered into every runtime's spec as `PROTECTION_KEY_ID`, so a runtime can
tell that the key it loaded is the one the deployment hashes with.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import logging
import os

LOGGER = logging.getLogger(__name__)

PROTECTION_KEY_ENV = "PROTECTION_KEY"
#: The id of the key the control plane holds, rendered into each runtime's spec.
PROTECTION_KEY_ID_ENV = "PROTECTION_KEY_ID"

#: Shorter keys are refused: an HMAC key is only as strong as it is long.
MIN_PROTECTION_KEY_LENGTH = 32


@dataclass(frozen=True, slots=True)
class ProtectionKey:
    key_id: str
    secret: bytes

    def __repr__(self) -> str:  # never the secret, in a log or a traceback
        return f"ProtectionKey(key_id={self.key_id!r})"


def protection_key(text: str) -> ProtectionKey:
    """A protection key from its secret text."""
    raw = text.encode("utf-8")
    return ProtectionKey(key_id=key_id_of(raw), secret=raw)


def key_id_of(secret: bytes) -> str:
    return hashlib.sha256(secret).hexdigest()[:8]


def protection_key_from_environment(environ: Mapping[str, str]) -> ProtectionKey | None:
    """The key of an environment; None when it is unset or too short to use."""
    value = (environ.get(PROTECTION_KEY_ENV) or "").strip()
    if not value:
        return None
    if len(value) < MIN_PROTECTION_KEY_LENGTH:
        LOGGER.error(
            "%s is shorter than %d characters and is ignored: nothing can be hashed",
            PROTECTION_KEY_ENV,
            MIN_PROTECTION_KEY_LENGTH,
        )
        return None
    return protection_key(value)


def expected_key_id_from_environment(environ: Mapping[str, str]) -> str | None:
    """The id of the key the deployment hashes with, as the control plane rendered it; None when it
    rendered none (no key, or a control plane before data protection)."""
    value = (environ.get(PROTECTION_KEY_ID_ENV) or "").strip()
    return value or None


@lru_cache(maxsize=1)
def get_protection_key() -> ProtectionKey | None:
    """The protection key of this process's environment, read once."""
    return protection_key_from_environment(os.environ)


@lru_cache(maxsize=1)
def get_expected_key_id() -> str | None:
    """The key id this process's environment expects, read once."""
    return expected_key_id_from_environment(os.environ)


def key_problem(key: ProtectionKey | None, expected: str | None) -> str | None:
    """Why `key` cannot be used to hash, or None when it can.

    `protection_key_missing` without a key; `protection_key_mismatch` when the deployment expects
    another key than this process loaded - two apps reading different Secrets, or a runtime started
    before the key changed and not yet re-applied.
    """
    if key is None:
        return "protection_key_missing"
    if expected is not None and key.key_id != expected:
        return "protection_key_mismatch"
    return None
