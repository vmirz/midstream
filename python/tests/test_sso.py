"""Single sign-on (handbook 20): each provider's check, finding or making the account, the policies,
password sign-in turned off, and the identity agent's side of a provider sign-in."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import time
from typing import Any
import unittest
import uuid

from cryptography.hazmat.primitives.asymmetric import rsa
import jwt

from midstream.config import ControlPlaneConfig, email_domain_allowed, normalize_email_domains
from midstream.identity.authorization import authenticate
from midstream.identity.permissions import ADMIN_GROUP_ID
from midstream.identity.principals import clear_grants_cache
from midstream.identity.providers import (
    NONCES,
    ProviderError,
    ProviderSettings,
    person_names,
    verify_credential,
)
from midstream.identity.providers.http import HttpAnswer, ProviderUnreachable
from midstream.identity.providers.keys import forget_key_sets
from midstream.identity.providers.oidc import GOOGLE_JWKS_URI, entra_issuer, entra_jwks_uri, forget_discovery
from midstream.models.identity import ExternalIdentity, IdentityRequest, RequestContext
from midstream.services.identity import apply_identity_request
from midstream.settings.catalog import SettingValueError, coerce_setting_value, definition_for
from midstream.storage.identity import (
    find_user_by_external_identity,
    get_request_outcome,
    get_session,
    get_sign_in_guard,
    get_user,
    iter_audit_entries,
    save_user,
)
from midstream.web import auth

from test_identity import CONFIG, PASSWORD, SECRETS, FakeTable, directory, http_request, make_user, store_user

TENANT = "11111111-2222-3333-4444-555555555555"
ISSUER = "https://dex.example.com"
PROVIDERS = ProviderSettings(
    entra_tenant_id=TENANT,
    entra_client_id="entra-client",
    github_client_id="gh-client",
    github_client_secret="gh-secret",
    google_client_id="google-client",
    oidc_issuer=ISSUER,
    oidc_client_id="dex-client",
    oidc_label="Dex",
)
ENABLED = replace(
    CONFIG,
    sso_entra_enabled=True,
    sso_github_enabled=True,
    sso_google_enabled=True,
    sso_oidc_enabled=True,
)

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def jwks(*pairs: tuple[str, Any]) -> dict[str, Any]:
    keys = []
    for kid, private in pairs:
        public = jwt.algorithms.RSAAlgorithm.to_jwk(private.public_key(), as_dict=True)
        keys.append({**public, "kid": kid, "use": "sig", "alg": "RS256"})
    return {"keys": keys}


class FakeHttp:
    """Answers for the providers, by method and URL; counts what was asked."""

    def __init__(self, answers: dict[tuple[str, str], Any] | None = None) -> None:
        self.answers: dict[tuple[str, str], Any] = dict(answers or {})
        self.calls: list[tuple[str, str, Any]] = []
        self.unreachable = False

    async def request(self, method: str, url: str, *, headers=None, json_body=None, basic_auth=None, timeout: float = 10.0) -> HttpAnswer:
        self.calls.append((method, url, {"headers": headers, "json": json_body, "auth": basic_auth}))
        if self.unreachable:
            raise ProviderUnreachable(url)
        answer = self.answers.get((method, url))
        if answer is None:
            return HttpAnswer(404, None)
        if callable(answer):
            return answer(headers=headers, json_body=json_body, basic_auth=basic_auth)
        return answer if isinstance(answer, HttpAnswer) else HttpAnswer(200, answer)


def provider_http() -> FakeHttp:
    return FakeHttp(
        {
            ("GET", entra_jwks_uri(TENANT)): jwks(("k1", KEY)),
            ("GET", GOOGLE_JWKS_URI): jwks(("g1", KEY)),
            ("GET", f"{ISSUER}/.well-known/openid-configuration"): {"issuer": ISSUER, "jwks_uri": f"{ISSUER}/keys"},
            ("GET", f"{ISSUER}/keys"): jwks(("d1", KEY)),
        }
    )


def id_token(claims: dict[str, Any], *, kid: str = "k1", key: Any = KEY, algorithm: str = "RS256") -> str:
    now = int(time.time())
    body = {"iat": now, "exp": now + 3600, **claims}
    return jwt.encode(body, key, algorithm=algorithm, headers={"kid": kid})


def entra_token(nonce: str, **claims: Any) -> str:
    return id_token(
        {
            "iss": entra_issuer(TENANT),
            "aud": "entra-client",
            "sub": "pairwise-sub",
            "oid": "oid-ada",
            "tid": TENANT,
            "nonce": nonce,
            "preferred_username": "Ada@Example.com",
            "name": "Ada Lovelace",
            **claims,
        }
    )


def google_token(nonce: str, **claims: Any) -> str:
    return id_token(
        {
            "iss": "https://accounts.google.com",
            "aud": "google-client",
            "sub": "g-123",
            "nonce": nonce,
            "email": "grace@example.com",
            "email_verified": True,
            "given_name": "Grace",
            "family_name": "Hopper",
            **claims,
        },
        kid="g1",
    )


def dex_token(nonce: str, **claims: Any) -> str:
    return id_token(
        {"iss": ISSUER, "aud": "dex-client", "sub": "dex-alan", "nonce": nonce, "email": "alan@example.com", "email_verified": True, "name": "Alan Turing", **claims},
        kid="d1",
    )


def audit_entries(audit: FakeTable) -> list[Any]:
    """Every audit entry, newest first, whatever day the test runs on."""
    today = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 86400))
    return [entry for _, entry in iter_audit_entries(audit, newest=today, oldest="2026-01-01")]


def audited(audit: FakeTable) -> list[str]:
    return [entry.action for entry in audit_entries(audit)]


def fresh_nonce() -> str:
    return uuid.uuid4().hex


def run(awaitable):
    return asyncio.run(awaitable)


def verify(provider: str, credential: Any, nonce: Any, *, http: FakeHttp, settings: ProviderSettings = PROVIDERS):
    return run(verify_credential(provider, credential, nonce, settings=settings, http=http, now=time.time()))


class ProviderSetUp(unittest.TestCase):
    def setUp(self) -> None:
        forget_key_sets()
        forget_discovery()


class EntraTests(ProviderSetUp):
    def test_a_token_from_the_tenant_signs_in_by_object_id(self) -> None:
        nonce = fresh_nonce()
        account = verify("entra", {"id_token": entra_token(nonce)}, nonce, http=provider_http())
        self.assertEqual((account.provider, account.subject, account.email), ("entra", "oid-ada", "ada@example.com"))
        self.assertEqual((account.first_name, account.last_name), ("Ada", "Lovelace"))

    def test_another_tenant_another_client_or_another_key_is_refused(self) -> None:
        cases = {
            "tenant": entra_token(fresh_nonce(), tid="99999999-0000-0000-0000-000000000000"),
            "audience": entra_token(fresh_nonce(), aud="someone-else"),
            "issuer": entra_token(fresh_nonce(), iss="https://login.microsoftonline.com/other/v2.0"),
        }
        for name, token in cases.items():
            with self.subTest(name):
                nonce = jwt.decode(token, options={"verify_signature": False})["nonce"]
                with self.assertRaises(ProviderError) as caught:
                    verify("entra", {"id_token": token}, nonce, http=provider_http())
                self.assertEqual(caught.exception.reason, "invalid_credential")
                self.assertEqual(caught.exception.status, 401)
        nonce = fresh_nonce()
        forged = id_token({"iss": entra_issuer(TENANT), "aud": "entra-client", "sub": "s", "oid": "o", "tid": TENANT, "nonce": nonce}, key=OTHER_KEY)
        with self.assertRaises(ProviderError):
            verify("entra", {"id_token": forged}, nonce, http=provider_http())

    def test_the_nonce_must_match_and_is_used_once(self) -> None:
        nonce = fresh_nonce()
        token = entra_token(nonce)
        with self.assertRaises(ProviderError):
            verify("entra", {"id_token": token}, "another-nonce", http=provider_http())
        verify("entra", {"id_token": token}, nonce, http=provider_http())
        with self.assertRaises(ProviderError) as caught:
            verify("entra", {"id_token": token}, nonce, http=provider_http())
        self.assertIn("already used", caught.exception.message)

    def test_an_old_or_expired_token_is_refused(self) -> None:
        now = int(time.time())
        for name, claims in {"old": {"iat": now - 3600, "exp": now + 3600}, "expired": {"iat": now - 7200, "exp": now - 3600}}.items():
            with self.subTest(name):
                nonce = fresh_nonce()
                with self.assertRaises(ProviderError):
                    verify("entra", {"id_token": entra_token(nonce, **claims)}, nonce, http=provider_http())

    def test_an_unsigned_token_is_refused(self) -> None:
        nonce = fresh_nonce()
        unsigned = jwt.encode(
            {"iss": entra_issuer(TENANT), "aud": "entra-client", "sub": "s", "oid": "o", "tid": TENANT, "nonce": nonce, "iat": int(time.time()), "exp": int(time.time()) + 60},
            None,
            algorithm="none",
            headers={"kid": "k1"},
        )
        with self.assertRaises(ProviderError):
            verify("entra", {"id_token": unsigned}, nonce, http=provider_http())

    def test_an_unknown_key_reads_the_key_set_again_but_not_more_than_once_a_minute(self) -> None:
        http = provider_http()
        nonce = fresh_nonce()
        verify("entra", {"id_token": entra_token(nonce)}, nonce, http=http)
        reads = lambda: sum(1 for method, url, _ in http.calls if url == entra_jwks_uri(TENANT))  # noqa: E731
        self.assertEqual(reads(), 1)
        for _ in range(3):
            nonce = fresh_nonce()
            with self.assertRaises(ProviderError):
                verify("entra", {"id_token": id_token({"nonce": nonce}, kid="rotated")}, nonce, http=http)
        self.assertEqual(reads(), 1)

    def test_a_provider_that_cannot_be_reached_says_so(self) -> None:
        http = provider_http()
        http.unreachable = True
        nonce = fresh_nonce()
        with self.assertRaises(ProviderError) as caught:
            verify("entra", {"id_token": entra_token(nonce)}, nonce, http=http)
        self.assertEqual((caught.exception.reason, caught.exception.status), ("provider_unreachable", 503))


class GoogleTests(ProviderSetUp):
    def test_a_verified_google_account_signs_in(self) -> None:
        for issuer in ("https://accounts.google.com", "accounts.google.com"):
            with self.subTest(issuer):
                nonce = fresh_nonce()
                account = verify("google", {"id_token": google_token(nonce, iss=issuer)}, nonce, http=provider_http())
                self.assertEqual((account.subject, account.email, account.first_name, account.last_name), ("g-123", "grace@example.com", "Grace", "Hopper"))

    def test_an_unverified_email_is_refused(self) -> None:
        nonce = fresh_nonce()
        with self.assertRaises(ProviderError) as caught:
            verify("google", {"id_token": google_token(nonce, email_verified=False)}, nonce, http=provider_http())
        self.assertEqual(caught.exception.reason, "email_unverified")

    def test_hosted_domains_admit_only_their_accounts(self) -> None:
        settings = replace(PROVIDERS, google_hosted_domains=("example.com",))
        nonce = fresh_nonce()
        verify("google", {"id_token": google_token(nonce, hd="example.com")}, nonce, http=provider_http(), settings=settings)
        for claims in ({}, {"hd": "elsewhere.com"}):
            nonce = fresh_nonce()
            with self.assertRaises(ProviderError) as caught:
                verify("google", {"id_token": google_token(nonce, **claims)}, nonce, http=provider_http(), settings=settings)
            self.assertEqual(caught.exception.reason, "email_not_allowed")


class GenericOidcTests(ProviderSetUp):
    def test_the_issuer_is_discovered_and_its_token_checked(self) -> None:
        nonce = fresh_nonce()
        account = verify("oidc", {"id_token": dex_token(nonce)}, nonce, http=provider_http())
        self.assertEqual((account.provider, account.subject, account.email), ("oidc", "dex-alan", "alan@example.com"))
        self.assertEqual((account.first_name, account.last_name), ("Alan", "Turing"))

    def test_an_email_the_issuer_does_not_verify_needs_the_deployment_to_trust_it(self) -> None:
        nonce = fresh_nonce()
        token = dex_token(nonce, email_verified=None)
        with self.assertRaises(ProviderError) as caught:
            verify("oidc", {"id_token": token}, nonce, http=provider_http())
        self.assertEqual(caught.exception.reason, "email_unverified")
        nonce = fresh_nonce()
        account = verify("oidc", {"id_token": dex_token(nonce, email_verified=None)}, nonce, http=provider_http(), settings=replace(PROVIDERS, oidc_trust_emails=True))
        self.assertEqual(account.email, "alan@example.com")

    def test_a_discovery_document_for_another_issuer_is_not_trusted(self) -> None:
        http = provider_http()
        http.answers[("GET", f"{ISSUER}/.well-known/openid-configuration")] = {"issuer": "https://evil.example.com", "jwks_uri": f"{ISSUER}/keys"}
        nonce = fresh_nonce()
        with self.assertRaises(ProviderError) as caught:
            verify("oidc", {"id_token": dex_token(nonce)}, nonce, http=http)
        self.assertEqual(caught.exception.reason, "provider_unreachable")

    def test_a_credential_without_its_token_or_nonce_is_refused(self) -> None:
        for credential, nonce in (({}, "n"), ({"id_token": "x"}, None), ("text", "n")):
            with self.assertRaises(ProviderError):
                verify("oidc", credential, nonce, http=provider_http())


def github_http(*, token_owner: int = 42, user_id: int = 42, emails: Any = None, check_status: int = 200) -> FakeHttp:
    api = "https://api.github.com"

    def check(*, headers, json_body, basic_auth):
        if basic_auth != ("gh-client", "gh-secret") or json_body != {"access_token": "gho_token"}:
            return HttpAnswer(404, None)
        return HttpAnswer(check_status, {"user": {"id": token_owner, "login": "octo"}} if check_status == 200 else None)

    def user(*, headers, json_body, basic_auth):
        if headers.get("Authorization") != "Bearer gho_token":
            return HttpAnswer(401, None)
        return HttpAnswer(200, {"id": user_id, "login": "octo", "name": "Mona Lisa Octocat"})

    return FakeHttp(
        {
            ("POST", f"{api}/applications/gh-client/token"): check,
            ("GET", f"{api}/user"): user,
            ("GET", f"{api}/user/emails"): emails
            if emails is not None
            else [{"email": "other@example.com", "primary": False, "verified": True}, {"email": "Mona@Example.com", "primary": True, "verified": True}],
        }
    )


class GitHubTests(unittest.TestCase):
    def test_a_token_issued_to_this_app_signs_in_by_account_id_and_primary_email(self) -> None:
        account = verify("github", {"access_token": "gho_token"}, None, http=github_http())
        self.assertEqual((account.subject, account.email, account.first_name, account.last_name), ("42", "mona@example.com", "Mona Lisa", "Octocat"))

    def test_a_token_this_app_did_not_issue_is_refused(self) -> None:
        with self.assertRaises(ProviderError) as caught:
            verify("github", {"access_token": "gho_other"}, None, http=github_http())
        self.assertEqual(caught.exception.reason, "invalid_credential")

    def test_the_token_must_belong_to_the_account_it_reads(self) -> None:
        with self.assertRaises(ProviderError):
            verify("github", {"access_token": "gho_token"}, None, http=github_http(token_owner=7))

    def test_an_unverified_primary_email_is_refused(self) -> None:
        emails = [{"email": "mona@example.com", "primary": True, "verified": False}]
        with self.assertRaises(ProviderError) as caught:
            verify("github", {"access_token": "gho_token"}, None, http=github_http(emails=emails))
        self.assertEqual(caught.exception.reason, "email_unverified")

    def test_github_down_reads_as_unreachable(self) -> None:
        with self.assertRaises(ProviderError) as caught:
            verify("github", {"access_token": "gho_token"}, None, http=github_http(check_status=502))
        self.assertEqual(caught.exception.reason, "provider_unreachable")


class NameTests(unittest.TestCase):
    def test_names_come_from_what_the_provider_gives(self) -> None:
        self.assertEqual(person_names("Ada", "Lovelace", "ignored", "a@x.io"), ("Ada", "Lovelace"))
        self.assertEqual(person_names(None, None, "Ada  King Lovelace", "a@x.io"), ("Ada King", "Lovelace"))
        self.assertEqual(person_names(None, None, "octo", "a@x.io"), ("octo", ""))
        self.assertEqual(person_names(None, None, None, "ada.l@x.io"), ("ada.l", ""))


class SsoSignInTests(ProviderSetUp):
    def setUp(self) -> None:
        super().setUp()
        clear_grants_cache()
        self.table, self.audit = directory()
        self.now = int(time.time())

    def sign_in(self, body: Any, *, config: ControlPlaneConfig = ENABLED, http: FakeHttp | None = None) -> auth.AuthResult:
        return run(
            auth.sign_in(
                self.table,
                body,
                request=http_request(),
                config=config,
                secrets=SECRETS,
                providers=PROVIDERS,
                http=http or provider_http(),
                now=self.now,
            )
        )

    def dex(self, **claims: Any) -> dict[str, Any]:
        nonce = fresh_nonce()
        return {"provider": "oidc", "credential": {"id_token": dex_token(nonce, **claims)}, "nonce": nonce}

    def apply(self, event: IdentityRequest):
        return apply_identity_request(self.table, self.audit, event, config=CONFIG, secrets=SECRETS)

    def test_a_provider_off_here_or_not_configured_is_refused(self) -> None:
        result = self.sign_in(self.dex(), config=CONFIG)
        self.assertEqual(result.status, 403)
        self.assertEqual(result.body["error"]["details"]["reason"], "provider_disabled")
        result = run(auth.sign_in(self.table, self.dex(), config=ENABLED, secrets=SECRETS, providers=ProviderSettings(), http=provider_http(), now=self.now))
        self.assertEqual(result.body["error"]["details"]["reason"], "provider_disabled")

    def test_someone_known_by_email_is_linked_then_known_by_their_provider_account(self) -> None:
        store_user(self.table, make_user("alan", "alan@example.com", groups=["viewer"]))
        first = self.sign_in(self.dex())
        self.assertEqual(first.status, 200)
        self.assertEqual(first.request_event.action, "link_identity")
        self.assertIsNone(first.provisioning)
        self.assertEqual(first.body["principal"]["user_id"], "alan")
        self.assertEqual(first.body["session"]["method"], "sso:oidc")
        self.assertEqual(self.apply(first.request_event).status, "applied")
        self.assertEqual(find_user_by_external_identity(self.table, "oidc", "dex-alan").user_id, "alan")
        self.assertIsNotNone(get_session(self.table, "alan", first.request_event.payload["session_id"]))
        self.assertIn("user.identity_linked", audited(self.audit))

        # The email changes at the provider; the account stays theirs.
        second = self.sign_in(self.dex(email="alan.turing@example.com"))
        self.assertEqual(second.request_event.action, "sign_in_succeeded")
        self.assertEqual(second.body["principal"]["user_id"], "alan")

    def test_open_makes_an_account_for_someone_new_and_the_sign_in_waits_for_it(self) -> None:
        result = self.sign_in(self.dex())
        self.assertEqual(result.status, 200)
        self.assertEqual(result.request_event.action, "provision_user")
        user_id = result.provisioning["user_id"]
        self.assertEqual(user_id, auth.provisioned_user_id("oidc", "dex-alan"))
        token = result.body["token"]
        # Until the agent has made the account, its session is not usable.
        self.assertEqual(run(auth.await_provisioned(self.table, result.to_value(), timeout=0.05))["status"], 503)

        self.assertEqual(self.apply(result.request_event).status, "applied")
        self.assertEqual(run(auth.await_provisioned(self.table, result.to_value()))["status"], 200)
        user = get_user(self.table, user_id)
        self.assertEqual((user.email, user.first_name, user.last_name, user.group_ids, user.password), ("alan@example.com", "Alan", "Turing", [], None))
        self.assertEqual(user.external_identities, [ExternalIdentity("oidc", "dex-alan")])
        self.assertIn("user.provisioned", audited(self.audit))
        principal = authenticate(http_request(token), self.table, secrets=SECRETS)
        self.assertEqual(principal.user_id, user_id)
        self.assertTrue(principal.can("materializations.view"))

    def test_two_first_sign_ins_at_once_make_one_account(self) -> None:
        first, second = self.sign_in(self.dex()), self.sign_in(self.dex())
        self.assertEqual(first.provisioning["user_id"], second.provisioning["user_id"])
        self.assertEqual(self.apply(first.request_event).status, "applied")
        self.assertEqual(self.apply(second.request_event).status, "applied")
        self.assertIsNotNone(get_session(self.table, first.provisioning["user_id"], second.request_event.payload["session_id"]))

    def test_an_account_given_by_an_administrator_meanwhile_refuses_the_provisioning(self) -> None:
        result = self.sign_in(self.dex())
        store_user(self.table, make_user("alan", "alan@example.com"))
        self.assertEqual(self.apply(result.request_event).status, "rejected")
        waited = run(auth.await_provisioned(self.table, result.to_value()))
        self.assertEqual(waited["status"], 409)

    def test_invited_refuses_someone_new_without_counting_it_towards_a_lockout(self) -> None:
        result = self.sign_in(self.dex(), config=replace(ENABLED, sso_first_sign_in="invited"))
        self.assertEqual(result.status, 403)
        self.assertEqual(result.body["error"]["details"]["reason"], "no_account")
        self.assertEqual(result.request_event.action, "sign_in_failed")
        self.apply(result.request_event)
        self.assertIsNone(get_sign_in_guard(self.table, "alan@example.com"))
        failures = [entry for entry in audit_entries(self.audit) if entry.action == "session.sign_in_failed"]
        self.assertEqual(failures[0].details["method"], "sso:oidc")

        # Invited: an administrator adds them, and they sign in to that account.
        store_user(self.table, make_user("alan", "alan@example.com"))
        self.assertEqual(self.sign_in(self.dex(), config=replace(ENABLED, sso_first_sign_in="invited")).status, 200)

    def test_allowed_email_domains_are_checked_first(self) -> None:
        config = replace(ENABLED, sso_allowed_email_domains="corp.example.com")
        result = self.sign_in(self.dex(), config=config)
        self.assertEqual(result.body["error"]["details"]["reason"], "email_not_allowed")
        self.assertEqual(self.sign_in(self.dex(email="alan@corp.example.com"), config=config).status, 200)

    def test_a_deactivated_account_is_refused(self) -> None:
        store_user(self.table, make_user("alan", "alan@example.com", status="deactivated"))
        result = self.sign_in(self.dex())
        self.assertEqual(result.body["error"]["details"]["reason"], "account_deactivated")

    def test_a_provider_session_needs_no_password_change(self) -> None:
        store_user(self.table, make_user("alan", "alan@example.com", must_change=True))
        result = self.sign_in(self.dex())
        self.apply(result.request_event)
        principal = authenticate(http_request(result.body["token"]), self.table, secrets=SECRETS)
        self.assertFalse(principal.password_change_required)

    def test_a_bad_credential_says_why(self) -> None:
        nonce = fresh_nonce()
        result = self.sign_in({"provider": "oidc", "credential": {"id_token": dex_token(nonce)}, "nonce": "wrong"})
        self.assertEqual(result.status, 401)
        self.assertEqual(result.body["error"]["details"]["reason"], "invalid_credential")
        self.assertIsNone(result.request_event)
        self.assertEqual(self.sign_in({"provider": "myspace"}).status, 400)

    def test_deleting_an_account_forgets_its_provider_accounts(self) -> None:
        store_user(self.table, make_user("alan", "alan@example.com"))
        self.apply(self.sign_in(self.dex()).request_event)
        save_user(self.table, replace(get_user(self.table, "alan"), status="deactivated"))
        outcome = apply_identity_request(
            self.table,
            self.audit,
            IdentityRequest(request_id="idr-del", action="delete_user", submitted_at="2026-10-26T12:00:00Z", actor_id="admin", payload={"user_id": "alan"}, context=RequestContext()),
            config=CONFIG,
            secrets=SECRETS,
        )
        self.assertEqual(outcome.status, "applied")
        self.assertIsNone(find_user_by_external_identity(self.table, "oidc", "dex-alan"))


class PasswordSignInSwitchTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_grants_cache()
        self.table, _ = directory()
        self.off = replace(CONFIG, password_sign_in_enabled=False)

    def sign_in(self, email: str, password: str = PASSWORD) -> auth.AuthResult:
        return run(auth.sign_in(self.table, {"email": email, "password": password}, request=http_request(), config=self.off, secrets=SECRETS))

    def test_off_only_administrators_sign_in_with_a_password(self) -> None:
        refused = self.sign_in("viewer@example.com")
        self.assertEqual(refused.status, 403)
        self.assertEqual(refused.body["error"]["details"]["reason"], "password_sign_in_disabled")
        self.assertEqual(self.sign_in("admin@example.com").status, 200)

    def test_a_wrong_password_is_still_only_a_wrong_password(self) -> None:
        self.assertEqual(self.sign_in("viewer@example.com", "not the password").status, 401)

    def test_the_refusal_does_not_count_towards_a_lockout(self) -> None:
        _, audit = directory()
        event = self.sign_in("viewer@example.com").request_event
        apply_identity_request(self.table, audit, event, config=CONFIG, secrets=SECRETS)
        self.assertIsNone(get_sign_in_guard(self.table, "viewer@example.com"))


class ProvidersListedTests(unittest.TestCase):
    def test_a_domain_lists_the_providers_it_accepts(self) -> None:
        listed = auth.sign_in_providers(replace(CONFIG, sso_oidc_enabled=True, sso_google_enabled=True, password_sign_in_enabled=False), PROVIDERS)
        self.assertEqual([item["id"] for item in listed], ["password", "google", "oidc"])
        self.assertTrue(listed[0]["administrators_only"])
        self.assertEqual(listed[2]["label"], "Dex")
        # On but not configured: not offered.
        self.assertEqual([item["id"] for item in auth.sign_in_providers(replace(CONFIG, sso_entra_enabled=True), ProviderSettings())], ["password"])


class EmailDomainSettingTests(unittest.TestCase):
    def test_email_domains_are_normalized_and_checked(self) -> None:
        self.assertEqual(normalize_email_domains(" Example.com,@corp.example.org  example.com "), "example.com,corp.example.org")
        self.assertEqual(normalize_email_domains(""), "")
        for bad in ("not a domain!", "localhost", "-bad.com"):
            with self.subTest(bad), self.assertRaises(ValueError):
                normalize_email_domains(bad)
        self.assertTrue(email_domain_allowed("a@Example.com", "example.com"))
        self.assertFalse(email_domain_allowed("a@sub.example.com", "example.com"))
        self.assertTrue(email_domain_allowed("a@anything.io", ""))

    def test_the_setting_takes_a_list_or_nothing(self) -> None:
        definition = definition_for("control-plane", "sso_allowed_email_domains")
        self.assertEqual(coerce_setting_value(definition, "B.com, a.com"), "b.com,a.com")
        self.assertEqual(coerce_setting_value(definition, None), "")
        with self.assertRaises(SettingValueError):
            coerce_setting_value(definition, "nope")


if __name__ == "__main__":
    unittest.main()
