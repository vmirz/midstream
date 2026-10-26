"""GitHub: OAuth 2.0 without ID tokens, so a domain asks GitHub's API who a token belongs to.

The console brings back an access token. A domain first asks GitHub, with the App's client secret,
whether the token was issued to this deployment's App - a token any other App obtained is refused -
then reads the account and its primary, verified email with it. The token is used for this and
nothing else; the console revokes it once every domain has answered.
"""

from __future__ import annotations

from typing import Any

from midstream.identity.providers import ExternalAccount, ProviderError, ProviderSettings, person_names
from midstream.identity.providers.http import ProviderHttp, ProviderUnreachable

API = "https://api.github.com"
HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "midstream"}
MAX_TOKEN_LENGTH = 512


async def verify_github(credential: dict[str, Any], *, settings: ProviderSettings, http: ProviderHttp) -> ExternalAccount:
    """Check a GitHub access token; say who signed in."""
    token = credential.get("access_token")
    if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_LENGTH:
        raise _invalid("The sign-in did not carry an access token.")
    client_id = settings.github_client_id or ""
    checked = await http.request(
        "POST",
        f"{API}/applications/{client_id}/token",
        headers=HEADERS,
        json_body={"access_token": token},
        basic_auth=(client_id, settings.github_client_secret or ""),
    )
    if checked.status in (404, 422):
        raise _invalid("GitHub does not know this sign-in, or it was made for another app.")
    if checked.status != 200 or not isinstance(checked.body, dict):
        raise ProviderUnreachable(f"GitHub answered {checked.status} checking a token")
    owner = checked.body.get("user") if isinstance(checked.body.get("user"), dict) else {}
    account_id = owner.get("id")

    authorized = {**HEADERS, "Authorization": f"Bearer {token}"}
    user = await http.request("GET", f"{API}/user", headers=authorized)
    if user.status == 401:
        raise _invalid("GitHub refused the sign-in's token.")
    if user.status != 200 or not isinstance(user.body, dict):
        raise ProviderUnreachable(f"GitHub answered {user.status} for the account")
    if account_id is None or user.body.get("id") != account_id:
        raise _invalid("The sign-in's token does not belong to the account GitHub named.")

    emails = await http.request("GET", f"{API}/user/emails", headers=authorized)
    if emails.status != 200 or not isinstance(emails.body, list):
        if emails.status in (403, 404):
            raise ProviderError("email_unverified", "GitHub did not share this account's email.", status=403)
        raise ProviderUnreachable(f"GitHub answered {emails.status} for the account's emails")
    primary = next(
        (
            entry
            for entry in emails.body
            if isinstance(entry, dict) and entry.get("primary") is True and isinstance(entry.get("email"), str)
        ),
        None,
    )
    if primary is None or primary.get("verified") is not True:
        raise ProviderError(
            "email_unverified",
            "This GitHub account's primary email is not verified. Verify it on GitHub, then sign in again.",
            status=403,
        )
    email = str(primary["email"]).strip().lower()
    first, last = person_names(None, None, user.body.get("name") or user.body.get("login"), email)
    return ExternalAccount(provider="github", subject=str(account_id), email=email, first_name=first, last_name=last)


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_credential", message, status=401)
