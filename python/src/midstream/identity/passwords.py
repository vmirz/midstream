"""Password hashing, verification, and the rules a new password must meet.

Passwords are hashed with scrypt from the standard library, at the parameters OWASP recommends
(N=2^15, r=8, p=3: 32 MiB and about 0.2 s per hash). The parameters travel inside each stored hash,
so raising them later only needs `needs_rehash` and a new hash at the person's next sign-in.

A password is hashed where it arrives — the WebView — before anything is published, so plain text
never reaches a Kafka topic or a table. Hashing is slow on purpose; the `*_async` variants run it
on a small dedicated pool so a sign-in never stalls the event loop that serves every other request.
"""

from __future__ import annotations

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import os
import re

#: scrypt cost: log2(N), block size r and parallelism p.
SCRYPT_LOG_N = 15
SCRYPT_R = 8
SCRYPT_P = 3
_SALT_BYTES = 16
_KEY_BYTES = 32
# Twice what the parameters above need, so a hash stored with the next step up still verifies.
_MAX_MEMORY = 128 * 1024 * 1024
_SCHEME = "scrypt"
_ENCODED = re.compile(r"^\$scrypt\$ln=(\d+),r=(\d+),p=(\d+)\$([A-Za-z0-9+/=_-]+)\$([A-Za-z0-9+/=_-]+)$")

#: Bounds that hold whatever the configured minimum: a password longer than this is refused
#: rather than hashed, so nobody can make the control plane hash megabytes.
PASSWORD_MAX_LENGTH = 256

# Two hashes at once is plenty for sign-ins and bounds the memory they take to 64 MiB.
_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="password-hash")
#: Hashes allowed to wait for the pool. Beyond it a sign-in is turned away at once, so a flood of
#: attempts costs a queue of bounded length rather than every caller an ever longer wait.
MAX_QUEUED_HASHES = 32
_queued = 0


class PasswordFormatError(ValueError):
    """A stored hash this module cannot read."""


class PasswordHashingBusy(RuntimeError):
    """Too many passwords are already waiting to be hashed."""


def hash_password(password: str) -> str:
    """Hash a password for storage: `$scrypt$ln=15,r=8,p=3$<salt>$<hash>`."""
    salt = os.urandom(_SALT_BYTES)
    derived = _derive(password, salt, log_n=SCRYPT_LOG_N, r=SCRYPT_R, p=SCRYPT_P)
    return f"${_SCHEME}$ln={SCRYPT_LOG_N},r={SCRYPT_R},p={SCRYPT_P}${_encode(salt)}${_encode(derived)}"


def verify_password(password: str, encoded: str | None) -> bool:
    """Whether `password` matches a stored hash. Takes as long when there is no hash to match."""
    if not encoded:
        # Spend what a real check would, so timing does not say which emails have accounts.
        _derive(password, b"\0" * _SALT_BYTES, log_n=SCRYPT_LOG_N, r=SCRYPT_R, p=SCRYPT_P)
        return False
    try:
        log_n, r, p, salt, expected = _parse(encoded)
    except PasswordFormatError:
        return False
    derived = _derive(password, salt, log_n=log_n, r=r, p=p)
    return hmac.compare_digest(derived, expected)


def needs_rehash(encoded: str) -> bool:
    """Whether a stored hash was made with weaker parameters than this release uses."""
    try:
        log_n, r, p, _, _ = _parse(encoded)
    except PasswordFormatError:
        return True
    return (log_n, r, p) != (SCRYPT_LOG_N, SCRYPT_R, SCRYPT_P)


async def hash_password_async(password: str) -> str:
    """`hash_password` on the hashing pool."""
    return await _on_pool(hash_password, password)


async def verify_password_async(password: str, encoded: str | None) -> bool:
    """`verify_password` on the hashing pool. Raises `PasswordHashingBusy` when it is full."""
    return await _on_pool(verify_password, password, encoded)


async def _on_pool(function, *args):
    global _queued
    if _queued >= MAX_QUEUED_HASHES:
        raise PasswordHashingBusy("too many passwords are waiting to be checked")
    _queued += 1
    try:
        return await asyncio.get_running_loop().run_in_executor(_EXECUTOR, function, *args)
    finally:
        _queued -= 1


def password_problem(
    password: object,
    *,
    min_length: int,
    email: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> str | None:
    """Why a password cannot be used, or None when it can.

    Length is what makes a password strong, so that is the rule, as NIST SP 800-63B advises: no
    required character classes. A password that is the person's email or name is refused.
    """
    if not isinstance(password, str):
        return "Password must be text"
    if len(password) < min_length:
        return f"Password must be at least {min_length} characters"
    if len(password) > PASSWORD_MAX_LENGTH:
        return f"Password must be at most {PASSWORD_MAX_LENGTH} characters"
    if not password.strip():
        return "Password cannot be only spaces"
    lowered = password.strip().lower()
    candidates = {
        value.strip().lower()
        for value in (email, (email or "").split("@")[0], first_name, last_name)
        if value and value.strip()
    }
    full_name = " ".join(part for part in (first_name, last_name) if part).strip().lower()
    if full_name:
        candidates.add(full_name)
        candidates.add(full_name.replace(" ", ""))
    if lowered in candidates:
        return "Password cannot be your email or name"
    if len(set(password)) < 4:
        return "Password must not repeat the same few characters"
    return None


def _derive(password: str, salt: bytes, *, log_n: int, r: int, p: int) -> bytes:
    return hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=2**log_n,
        r=r,
        p=p,
        maxmem=_MAX_MEMORY,
        dklen=_KEY_BYTES,
    )


def _parse(encoded: str) -> tuple[int, int, int, bytes, bytes]:
    match = _ENCODED.match(encoded)
    if match is None:
        raise PasswordFormatError("not an scrypt hash")
    log_n, r, p = (int(match.group(index)) for index in (1, 2, 3))
    if not (10 <= log_n <= 20 and 1 <= r <= 32 and 1 <= p <= 16) or 128 * r * 2**log_n > _MAX_MEMORY // 2:
        raise PasswordFormatError("scrypt parameters out of range")
    return log_n, r, p, _decode(match.group(4)), _decode(match.group(5))


def _encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
