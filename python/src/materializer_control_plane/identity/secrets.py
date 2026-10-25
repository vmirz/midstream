"""Credentials sign-in depends on, read from the deployment's environment.

They come from a Kubernetes Secret, never from the Settings page, and are kept apart from
`ControlPlaneConfig` so no settings view can ever report one. Only whether each is configured is
shown, on the Settings page's Deployment section.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import logging
import os

LOGGER = logging.getLogger(__name__)

SIGNING_KEY_ENV = "MATERIALIZER_AUTH_SIGNING_KEY"
PREVIOUS_SIGNING_KEY_ENV = "MATERIALIZER_AUTH_PREVIOUS_SIGNING_KEY"
SERVICE_TOKEN_ENV = "MATERIALIZER_AUTH_SERVICE_TOKEN"
BOOTSTRAP_ADMIN_EMAIL_ENV = "MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_EMAIL"
BOOTSTRAP_ADMIN_PASSWORD_ENV = "MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_PASSWORD"
BOOTSTRAP_ADMIN_FIRST_NAME_ENV = "MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_FIRST_NAME"
BOOTSTRAP_ADMIN_LAST_NAME_ENV = "MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_LAST_NAME"

#: Shorter keys are refused: an HMAC key is only as strong as it is long.
MIN_SECRET_LENGTH = 32


@dataclass(frozen=True, slots=True)
class SigningKey:
    """A session signing key and the short id tokens name it by, so keys can rotate."""

    key_id: str
    secret: bytes


@dataclass(frozen=True, slots=True)
class BootstrapAdmin:
    """Who to make the first administrator when no administrator exists."""

    email: str
    password: str
    first_name: str
    last_name: str


@dataclass(frozen=True, slots=True)
class IdentitySecrets:
    """Signing keys, the console's service token, and the bootstrap administrator.

    `signing_keys[0]` signs new sessions; every key verifies them, so a key can be rotated by
    moving the current one to the previous slot without signing everyone out.
    """

    signing_keys: tuple[SigningKey, ...] = ()
    service_token: str | None = None
    bootstrap_admin: BootstrapAdmin | None = None

    @property
    def can_sign_in(self) -> bool:
        return bool(self.signing_keys)

    @property
    def current_key(self) -> SigningKey | None:
        return self.signing_keys[0] if self.signing_keys else None

    def key(self, key_id: str) -> SigningKey | None:
        return next((key for key in self.signing_keys if key.key_id == key_id), None)


def signing_key(secret: str) -> SigningKey:
    """A signing key from its secret text."""
    raw = secret.encode("utf-8")
    return SigningKey(key_id=hashlib.sha256(raw).hexdigest()[:8], secret=raw)


def identity_secrets_from_environment(environ: Mapping[str, str]) -> IdentitySecrets:
    """Read the identity secrets from an environment, dropping any that are too weak to use."""
    keys: list[SigningKey] = []
    # A previous key only verifies, so it matters only beside a current key that signs.
    for name in (SIGNING_KEY_ENV, PREVIOUS_SIGNING_KEY_ENV):
        value = (environ.get(name) or "").strip()
        if not value or (name == PREVIOUS_SIGNING_KEY_ENV and not keys):
            continue
        if len(value) < MIN_SECRET_LENGTH:
            LOGGER.error("%s is shorter than %d characters and is ignored", name, MIN_SECRET_LENGTH)
            continue
        keys.append(signing_key(value))

    service_token = (environ.get(SERVICE_TOKEN_ENV) or "").strip() or None
    if service_token is not None and len(service_token) < MIN_SECRET_LENGTH:
        LOGGER.error("%s is shorter than %d characters and is ignored", SERVICE_TOKEN_ENV, MIN_SECRET_LENGTH)
        service_token = None

    bootstrap: BootstrapAdmin | None = None
    email = (environ.get(BOOTSTRAP_ADMIN_EMAIL_ENV) or "").strip().lower()
    password = environ.get(BOOTSTRAP_ADMIN_PASSWORD_ENV) or ""
    if email and password:
        bootstrap = BootstrapAdmin(
            email=email,
            password=password,
            first_name=(environ.get(BOOTSTRAP_ADMIN_FIRST_NAME_ENV) or "").strip() or "Admin",
            last_name=(environ.get(BOOTSTRAP_ADMIN_LAST_NAME_ENV) or "").strip() or "User",
        )
    return IdentitySecrets(signing_keys=tuple(keys), service_token=service_token, bootstrap_admin=bootstrap)


@lru_cache(maxsize=1)
def get_identity_secrets() -> IdentitySecrets:
    """The identity secrets of this process's environment, read once."""
    secrets = identity_secrets_from_environment(os.environ)
    if not secrets.can_sign_in:
        LOGGER.warning(
            "%s is not set: nobody can sign in, and every request is treated as a guest",
            SIGNING_KEY_ENV,
        )
    return secrets
