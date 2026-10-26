"""Single sign-on providers (handbook 20): checking what the console brings back from one.

The console runs a provider's flow and gives each domain the result - an ID token, or for GitHub an
access token - and each domain checks it here, itself, against the provider's keys or API, so a
console can never sign anyone in as someone else. A provider is configured by the deployment
(`AUTH_SSO_*`, `ProviderSettings`) and turned on or off per domain by a setting
(`sso_<provider>_enabled`).

What a check gives is an `ExternalAccount`: the provider's id for the person, which never changes,
and an email the provider vouches for. Anything less is refused with a `ProviderError`.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
import os
import re
import time
from typing import Any

#: Every provider, in the order a sign-in page lists them.
PROVIDER_IDS = ("entra", "github", "google", "oidc")

#: What a sign-in button says each provider is; the generic provider's label is configured.
PROVIDER_LABELS = {"entra": "Microsoft", "github": "GitHub", "google": "Google", "oidc": "Single sign-on"}

#: How old an ID token may be when a domain sees it: the console hands it on within seconds.
MAX_TOKEN_AGE_SECONDS = 5 * 60

ENTRA_TENANT_ID_ENV = "AUTH_SSO_ENTRA_TENANT_ID"
ENTRA_CLIENT_ID_ENV = "AUTH_SSO_ENTRA_CLIENT_ID"
GITHUB_CLIENT_ID_ENV = "AUTH_SSO_GITHUB_CLIENT_ID"
GITHUB_CLIENT_SECRET_ENV = "AUTH_SSO_GITHUB_CLIENT_SECRET"
GOOGLE_CLIENT_ID_ENV = "AUTH_SSO_GOOGLE_CLIENT_ID"
GOOGLE_HOSTED_DOMAINS_ENV = "AUTH_SSO_GOOGLE_HOSTED_DOMAINS"
OIDC_ISSUER_ENV = "AUTH_SSO_OIDC_ISSUER"
OIDC_CLIENT_ID_ENV = "AUTH_SSO_OIDC_CLIENT_ID"
OIDC_LABEL_ENV = "AUTH_SSO_OIDC_LABEL"
OIDC_TRUST_EMAILS_ENV = "AUTH_SSO_OIDC_TRUST_EMAILS"

_NAME_MAX_LENGTH = 100
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


class ProviderError(Exception):
    """A sign-in through a provider that is refused: `reason` for code, the message for people."""

    def __init__(self, reason: str, message: str, *, status: int) -> None:
        self.reason = reason
        self.status = status
        super().__init__(message)

    @property
    def message(self) -> str:
        return str(self)


@dataclass(frozen=True, slots=True)
class ProviderSettings:
    """Which providers the deployment configured, and what this domain accepts from each.

    The values must match the console's registration with each provider: the same tenant and client
    ids. GitHub's client secret is a credential, read from a Secret and never shown.
    """

    entra_tenant_id: str | None = None
    entra_client_id: str | None = None
    github_client_id: str | None = None
    github_client_secret: str | None = None
    google_client_id: str | None = None
    google_hosted_domains: tuple[str, ...] = ()
    oidc_issuer: str | None = None
    oidc_client_id: str | None = None
    oidc_label: str | None = None
    oidc_trust_emails: bool = False

    def configured(self, provider: str) -> bool:
        """Whether the deployment configured a provider, so a setting can turn it on."""
        if provider == "entra":
            return bool(self.entra_tenant_id and self.entra_client_id)
        if provider == "github":
            return bool(self.github_client_id and self.github_client_secret)
        if provider == "google":
            return bool(self.google_client_id)
        if provider == "oidc":
            return bool(self.oidc_issuer and self.oidc_client_id)
        return False

    def label(self, provider: str) -> str:
        if provider == "oidc" and self.oidc_label:
            return self.oidc_label
        return PROVIDER_LABELS.get(provider, provider)


def provider_settings_from_environment(environ: Mapping[str, str]) -> ProviderSettings:
    """Read the providers' settings from an environment."""

    def value(name: str) -> str | None:
        return (environ.get(name) or "").strip() or None

    hosted = tuple(
        domain for domain in (part.strip().lower() for part in (environ.get(GOOGLE_HOSTED_DOMAINS_ENV) or "").split(",")) if domain
    )
    issuer = value(OIDC_ISSUER_ENV)
    return ProviderSettings(
        entra_tenant_id=value(ENTRA_TENANT_ID_ENV),
        entra_client_id=value(ENTRA_CLIENT_ID_ENV),
        github_client_id=value(GITHUB_CLIENT_ID_ENV),
        github_client_secret=value(GITHUB_CLIENT_SECRET_ENV),
        google_client_id=value(GOOGLE_CLIENT_ID_ENV),
        google_hosted_domains=hosted,
        oidc_issuer=issuer.rstrip("/") if issuer else None,
        oidc_client_id=value(OIDC_CLIENT_ID_ENV),
        oidc_label=value(OIDC_LABEL_ENV),
        oidc_trust_emails=(environ.get(OIDC_TRUST_EMAILS_ENV) or "").strip().lower() in ("1", "true", "yes", "on"),
    )


@lru_cache(maxsize=1)
def get_provider_settings() -> ProviderSettings:
    """The providers' settings of this process's environment, read once."""
    return provider_settings_from_environment(os.environ)


@dataclass(frozen=True, slots=True)
class ExternalAccount:
    """Who a provider says signed in: its id for them, which never changes, and the email it vouches for."""

    provider: str
    subject: str
    email: str
    first_name: str
    last_name: str


def person_names(given: Any, family: Any, full: Any, email: str) -> tuple[str, str]:
    """A first and last name from what a provider says: its given and family names, else its full
    name split at the last space, else the email's local part. The last name may be empty."""
    first, last = _name(given), _name(family)
    if first:
        return first, last
    whole = _name(full)
    if whole:
        head, _, tail = whole.rpartition(" ")
        return (head, tail) if head else (tail, "")
    return (_name(email.split("@", 1)[0]) or "Someone"), ""


def _name(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return _CONTROL.sub("", " ".join(value.split()))[:_NAME_MAX_LENGTH].strip()


class NonceCache:
    """The nonces this process has seen in ID tokens, until those tokens expire: a token is used once."""

    def __init__(self, limit: int = 20_000) -> None:
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._limit = limit

    def use(self, nonce: str, *, expires_at: float, now: float | None = None) -> bool:
        """Record a nonce; False when it was seen before."""
        current = time.time() if now is None else now
        while self._seen:
            oldest, expiry = next(iter(self._seen.items()))
            if expiry > current and len(self._seen) < self._limit:
                break
            self._seen.pop(oldest)
        if nonce in self._seen:
            return False
        self._seen[nonce] = expires_at
        return True


NONCES = NonceCache()


async def verify_credential(
    provider: str,
    credential: Any,
    nonce: Any,
    *,
    settings: ProviderSettings,
    http: Any = None,
    now: float | None = None,
) -> ExternalAccount:
    """Check what the console brings back from a provider, and say who signed in.

    Raises `ProviderError`: `invalid_credential` (401) for anything that does not check out,
    `email_unverified` (403) when the provider does not vouch for the email, `email_not_allowed`
    (403) outside Google's hosted domains, `provider_unreachable` (503).
    """
    from midstream.identity.providers.http import ProviderUnreachable, default_http

    client = default_http() if http is None else http
    current = time.time() if now is None else now
    if not isinstance(credential, dict):
        raise ProviderError("invalid_credential", "The sign-in did not carry a credential.", status=401)
    try:
        if provider == "github":
            from midstream.identity.providers.github import verify_github

            return await verify_github(credential, settings=settings, http=client)
        from midstream.identity.providers.oidc import verify_openid

        return await verify_openid(provider, credential, nonce, settings=settings, http=client, now=current)
    except ProviderUnreachable as error:
        raise ProviderError(
            "provider_unreachable",
            f"{settings.label(provider)} could not be reached to check the sign-in. Try again in a moment.",
            status=503,
        ) from error
