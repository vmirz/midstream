"""OpenID Connect providers: Microsoft Entra ID, Google, and any issuer configured as `oidc`.

Each signs an ID token with a key from its published key set. A domain checks the signature, that
the token was issued by the provider for this deployment's client (`iss`, `aud`), that it has not
expired and was issued within the last few minutes, that it carries the nonce the console's flow
chose - and has not been seen before - then reads who signed in from its claims.
"""

from __future__ import annotations

import time
from typing import Any

import jwt

from midstream.identity.providers import (
    MAX_TOKEN_AGE_SECONDS,
    NONCES,
    ExternalAccount,
    ProviderError,
    ProviderSettings,
    person_names,
)
from midstream.identity.providers.http import ProviderHttp, ProviderUnreachable
from midstream.identity.providers.keys import key_set

#: Signature algorithms an ID token may use; never `none`, never a shared secret.
ALGORITHMS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512")
#: Clock difference between this process and the provider that is tolerated.
LEEWAY_SECONDS = 60
#: The longest ID token accepted, and the longest nonce.
MAX_TOKEN_LENGTH = 16 * 1024
MAX_NONCE_LENGTH = 256

GOOGLE_ISSUERS = ("https://accounts.google.com", "accounts.google.com")
GOOGLE_JWKS_URI = "https://www.googleapis.com/oauth2/v3/certs"
#: How long an issuer's discovery document is kept.
DISCOVERY_MAX_AGE_SECONDS = 3600

_DISCOVERY: dict[str, tuple[float, dict[str, Any]]] = {}


def entra_issuer(tenant_id: str) -> str:
    return f"https://login.microsoftonline.com/{tenant_id}/v2.0"


def entra_jwks_uri(tenant_id: str) -> str:
    return f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"


async def verify_openid(
    provider: str,
    credential: dict[str, Any],
    nonce: Any,
    *,
    settings: ProviderSettings,
    http: ProviderHttp,
    now: float,
) -> ExternalAccount:
    """Check an ID token from Entra ID, Google or the configured issuer; say who signed in."""
    token = credential.get("id_token")
    if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_LENGTH:
        raise _invalid("The sign-in did not carry an ID token.")
    if not isinstance(nonce, str) or not nonce or len(nonce) > MAX_NONCE_LENGTH:
        raise _invalid("The sign-in did not carry its nonce.")

    if provider == "entra":
        tenant = settings.entra_tenant_id or ""
        claims = await _verified_claims(
            token,
            issuer=entra_issuer(tenant),
            audience=settings.entra_client_id or "",
            jwks_uri=entra_jwks_uri(tenant),
            http=http,
        )
        if claims.get("tid") != tenant:
            raise _invalid("The Microsoft account is not in this deployment's tenant.")
        subject = claims.get("oid")
        email = claims.get("email") or claims.get("preferred_username")
        # Within the configured tenant, the directory vouches for its users' addresses.
        verified = True
    elif provider == "google":
        claims = await _verified_claims(
            token,
            issuer=GOOGLE_ISSUERS,
            audience=settings.google_client_id or "",
            jwks_uri=GOOGLE_JWKS_URI,
            http=http,
        )
        subject = claims.get("sub")
        email = claims.get("email")
        verified = claims.get("email_verified") is True
        if settings.google_hosted_domains and claims.get("hd") not in settings.google_hosted_domains:
            raise ProviderError(
                "email_not_allowed",
                "This Google account is not one of the organization's: sign in with your work account.",
                status=403,
            )
    elif provider == "oidc":
        issuer = settings.oidc_issuer or ""
        document = await discovery(issuer, http, now=now)
        jwks_uri = document.get("jwks_uri")
        if document.get("issuer", "").rstrip("/") != issuer or not isinstance(jwks_uri, str):
            raise ProviderUnreachable(f"{issuer} published a discovery document for another issuer, or no key set")
        claims = await _verified_claims(
            token,
            issuer=document["issuer"],
            audience=settings.oidc_client_id or "",
            jwks_uri=jwks_uri,
            http=http,
        )
        subject = claims.get("sub")
        email = claims.get("email")
        verified = settings.oidc_trust_emails or claims.get("email_verified") is True
    else:
        raise _invalid(f"{provider} is not a provider this control plane knows.")

    if claims.get("nonce") != nonce:
        raise _invalid("The sign-in's nonce does not match.")
    issued_at = claims.get("iat")
    if not isinstance(issued_at, (int, float)) or now - issued_at > MAX_TOKEN_AGE_SECONDS + LEEWAY_SECONDS:
        raise _invalid("The sign-in is too old. Sign in again.")
    if not isinstance(subject, str) or not subject:
        raise _invalid("The ID token does not say who signed in.")
    if not isinstance(email, str) or "@" not in email:
        raise ProviderError("email_unverified", "The provider did not give an email for this account.", status=403)
    if not verified:
        raise ProviderError(
            "email_unverified",
            "The provider has not verified this account's email. Verify it with the provider, then sign in again.",
            status=403,
        )
    if not NONCES.use(f"{provider}:{nonce}", expires_at=float(claims.get("exp", now + MAX_TOKEN_AGE_SECONDS)), now=now):
        raise _invalid("This sign-in was already used. Sign in again.")
    normalized = email.strip().lower()
    first, last = person_names(claims.get("given_name"), claims.get("family_name"), claims.get("name"), normalized)
    return ExternalAccount(provider=provider, subject=subject, email=normalized, first_name=first, last_name=last)


async def _verified_claims(
    token: str,
    *,
    issuer: str | tuple[str, ...],
    audience: str,
    jwks_uri: str,
    http: ProviderHttp,
) -> dict[str, Any]:
    try:
        header = jwt.get_unverified_header(token)
    except jwt.InvalidTokenError as error:
        raise _invalid("The ID token cannot be read.") from error
    algorithm = header.get("alg")
    kid = header.get("kid")
    if algorithm not in ALGORITHMS or not isinstance(kid, str):
        raise _invalid("The ID token is not signed in a way this control plane accepts.")
    key = await key_set(jwks_uri).key(kid, http)
    if key is None:
        raise _invalid("The ID token is signed with a key the provider does not publish.")
    try:
        claims = jwt.decode(
            token,
            key=key.key,
            algorithms=[algorithm],
            audience=audience,
            issuer=issuer,
            leeway=LEEWAY_SECONDS,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
    except jwt.ExpiredSignatureError as error:
        raise _invalid("The sign-in has expired. Sign in again.") from error
    except jwt.InvalidTokenError as error:
        raise _invalid(f"The ID token is not valid: {error}.") from error
    if not isinstance(claims, dict):
        raise _invalid("The ID token cannot be read.")
    return claims


async def discovery(issuer: str, http: ProviderHttp, *, now: float | None = None) -> dict[str, Any]:
    """An issuer's OpenID Connect discovery document, kept an hour."""
    current = time.time() if now is None else now
    cached = _DISCOVERY.get(issuer)
    if cached is not None and current - cached[0] < DISCOVERY_MAX_AGE_SECONDS:
        return cached[1]
    answer = await http.request("GET", f"{issuer}/.well-known/openid-configuration")
    if answer.status != 200 or not isinstance(answer.body, dict):
        raise ProviderUnreachable(f"{issuer} answered {answer.status} for its discovery document")
    _DISCOVERY[issuer] = (current, answer.body)
    return answer.body


def forget_discovery() -> None:
    """Drop every discovery document read, as a test does between cases."""
    _DISCOVERY.clear()


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_credential", message, status=401)
