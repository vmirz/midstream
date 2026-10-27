"""Federated access tokens (handbook 20): made in one domain, checked by every domain on its own,
acting for whoever owns them there, and revoked everywhere."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import unittest
from unittest.mock import patch

from midstream.identity.access_tokens import (
    InvalidAccessToken,
    TokenClaims,
    TokenOwner,
    read_access_token,
    read_revocation,
    sign_access_token,
    sign_revocation,
)
from midstream.identity.authorization import authorize
from midstream.identity.principals import clear_grants_cache, service_principal
from midstream.identity.secrets import IdentitySecrets, signing_key
from midstream.models.identity import ExternalIdentity
from midstream.services.identity import apply_identity_request
from midstream.storage.identity import (
    get_access_token,
    get_token_revocation,
    get_user,
    save_external_identity,
    save_user,
)
from midstream.web.errors import WebError
from midstream.web.tokens import (
    apply_revocations_request,
    create_access_token_request,
    my_access_tokens_view,
    revocations_view,
    revoke_access_token_request,
)

from test_identity import CONFIG, NOW, NOW_EPOCH, SECRETS, directory, http_request, make_user, request, session_token, store_user

MOMENT = datetime(2026, 10, 13, 12, tzinfo=UTC)
#: Domain B holds the same federation key, under its own signing key.
SECRETS_B = replace(SECRETS, signing_keys=(signing_key("b" * 40),))


def claims(**overrides) -> TokenClaims:
    values = dict(
        token_id="0123456789ab",
        issuer="domain_a",
        owner=TokenOwner(email="viewer@example.com"),
        permissions=("catalog.read",),
        issued_at=NOW_EPOCH,
        expires_at=NOW_EPOCH + 86400,
    )
    values.update(overrides)
    return TokenClaims(**values)


class TokenFormatTests(unittest.TestCase):
    def test_a_token_reads_back_as_it_was_made(self) -> None:
        made = claims(owner=TokenOwner(email="a@example.com", provider="google", subject="g-1"), datasets=("domain_a.sales", "domain_b.finance"))
        text = sign_access_token(made, SECRETS)
        self.assertTrue(text.startswith("tok_"))
        self.assertEqual(read_access_token(text, SECRETS, now=NOW_EPOCH), made)
        self.assertEqual(made.datasets_in("domain_b"), ("finance",))
        self.assertEqual(made.datasets_in("domain_c"), ())
        self.assertIsNone(claims().datasets_in("domain_a"))

    def test_anything_changed_breaks_the_signature(self) -> None:
        text = sign_access_token(claims(), SECRETS)
        tampered = sign_access_token(claims(permissions=("catalog.read", "catalog.write")), SECRETS)
        forged = f"{tampered.split('.')[0]}.{text.split('.')[1]}"
        for bad in (forged, text[:-3] + "AAA", "tok_", "tok_x.y", text.replace("tok_", "tok")):
            with self.subTest(bad=bad[:20]), self.assertRaises(InvalidAccessToken):
                read_access_token(bad, SECRETS, now=NOW_EPOCH)

    def test_the_previous_federation_key_still_checks_and_another_key_does_not(self) -> None:
        old = sign_access_token(claims(), SECRETS)
        rotated = replace(SECRETS, federation_keys=(signing_key("g" * 40), signing_key("f" * 40)))
        self.assertEqual(read_access_token(old, rotated, now=NOW_EPOCH).token_id, "0123456789ab")
        with self.assertRaises(InvalidAccessToken) as caught:
            read_access_token(old, replace(SECRETS, federation_keys=(signing_key("g" * 40),)), now=NOW_EPOCH)
        self.assertEqual(caught.exception.reason, "signature")

    def test_an_expired_or_future_token_is_refused(self) -> None:
        text = sign_access_token(claims(), SECRETS)
        with self.assertRaises(InvalidAccessToken) as caught:
            read_access_token(text, SECRETS, now=NOW_EPOCH + 86400)
        self.assertEqual(caught.exception.reason, "expired")
        with self.assertRaises(InvalidAccessToken):
            read_access_token(sign_access_token(claims(issued_at=NOW_EPOCH + 3600), SECRETS), SECRETS, now=NOW_EPOCH)

    def test_a_revocation_is_signed_and_cannot_be_forged(self) -> None:
        signed = sign_revocation("0123456789ab", NOW_EPOCH + 86400, SECRETS).to_dict()
        self.assertEqual(read_revocation(signed, SECRETS).token_id, "0123456789ab")
        for forged in ({**signed, "jti": "ba9876543210"}, {**signed, "exp": NOW_EPOCH}, {**signed, "signature": "nope"}, None):
            with self.subTest(forged=forged):
                self.assertIsNone(read_revocation(forged, SECRETS))
        self.assertIsNone(read_revocation(signed, replace(SECRETS, federation_keys=(signing_key("g" * 40),))))


class AcrossDomainsTests(unittest.TestCase):
    """A token made in Domain A, used in Domain B: two directories sharing a federation key."""

    def setUp(self) -> None:
        clear_grants_cache()
        self.a, self.audit_a = directory()
        self.b, self.audit_b = directory()

    def make_in_a(self, body: dict | None = None, *, user: str = "viewer") -> dict:
        principal = authorize(http_request(session_token(get_user(self.a, user))), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        acceptance = create_access_token_request(
            principal,
            body or {"name": "DuckDB"},
            request=http_request(),
            directory_table=self.a,
            secrets=SECRETS,
            domain="domain_a",
            now=MOMENT,
        )
        outcome = apply_identity_request(self.a, self.audit_a, acceptance.request_event, config=CONFIG, secrets=SECRETS, applied_at=NOW)
        self.assertEqual(outcome.status, "applied", outcome.to_dict())
        return acceptance.response

    def use_in_b(self, token: str, permission: str = "catalog.read", **kwargs):
        clear_grants_cache()
        with patch("midstream.identity.authorization.current_domain_id", return_value="domain_b"):
            return authorize(http_request(token), self.b, permission, secrets=SECRETS_B, now=NOW_EPOCH, **kwargs)

    def test_a_token_made_in_one_domain_reads_in_another_as_its_owner_there(self) -> None:
        token = self.make_in_a()["token"]
        principal = self.use_in_b(token, any_dataset=True)
        self.assertEqual((principal.kind, principal.user_id), ("token", "viewer"))
        self.assertEqual(principal.extra["access_token_issuer"], "domain_a")
        # B's grants decide: the viewer reads shop in B as in A.
        self.assertTrue(principal.can("catalog.read", "ds-000000000001"))

    def test_it_never_grants_more_than_the_owner_holds_in_the_domain_that_answers(self) -> None:
        token = self.make_in_a()["token"]
        save_user(self.b, replace(get_user(self.b, "viewer"), group_ids=[]))
        with self.assertRaises(WebError) as caught:
            self.use_in_b(token, any_dataset=True)
        self.assertEqual(caught.exception.status_code, 403)

    def test_dataset_limits_apply_to_each_domain_s_own_datasets(self) -> None:
        token = self.make_in_a({"name": "BI", "datasets": ["domain_a.shop", "domain_b.shop"]})["token"]
        principal = self.use_in_b(token, any_dataset=True)
        self.assertEqual(principal.describe()["dataset_limit"], ["ds-000000000001"])
        only_a = self.make_in_a({"name": "A only", "datasets": ["shop"]})["token"]
        with self.assertRaises(WebError):
            self.use_in_b(only_a, any_dataset=True)

    def test_its_owner_is_found_by_provider_account_before_email(self) -> None:
        user = get_user(self.a, "viewer")
        save_user(self.a, replace(user, external_identities=[ExternalIdentity("google", "g-viewer")]))
        save_external_identity(self.a, "google", "g-viewer", "viewer", updated_at=NOW)
        principal = authorize(http_request(session_token(user)), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        acceptance = create_access_token_request(principal, {"name": "x"}, request=http_request(), directory_table=self.a, secrets=SECRETS, domain="domain_a", now=MOMENT)
        claims_ = read_access_token(acceptance.response["token"], SECRETS, now=NOW_EPOCH)
        self.assertEqual((claims_.owner.provider, claims_.owner.subject), ("google", "g-viewer"))

        # In B the provider account belongs to someone with another email: they own it there.
        store_user(self.b, make_user("grace", "grace@example.com", groups=["viewer"]))
        save_user(self.b, replace(get_user(self.b, "grace"), external_identities=[ExternalIdentity("google", "g-viewer")]))
        save_external_identity(self.b, "google", "g-viewer", "grace", updated_at=NOW)
        self.assertEqual(self.use_in_b(acceptance.response["token"], any_dataset=True).user_id, "grace")

    def test_a_domain_that_does_not_know_the_owner_refuses_the_token(self) -> None:
        token = self.make_in_a(user="manager")["token"]
        from midstream.storage.identity import delete_user_email, delete_user_record

        delete_user_record(self.b, "manager")
        delete_user_email(self.b, "manager@example.com")
        with self.assertRaises(WebError) as caught:
            self.use_in_b(token, None)
        self.assertEqual(caught.exception.details["reason"], "token_unknown_owner")

    def test_revoked_in_one_domain_it_is_refused_in_every_domain_the_revocation_reaches(self) -> None:
        made = self.make_in_a()
        principal = authorize(http_request(session_token(get_user(self.a, "viewer"))), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        revoked = revoke_access_token_request(self.a, principal, made["token_id"], request=http_request(), secrets=SECRETS)
        apply_identity_request(self.a, self.audit_a, revoked.request_event, config=CONFIG, secrets=SECRETS, applied_at=NOW)
        self.assertIsNotNone(get_token_revocation(self.a, made["token_id"]))
        self.assertIsNotNone(get_access_token(self.a, made["token_id"]).revoked_at)

        # Still good in B until the revocation reaches it.
        self.use_in_b(made["token"], any_dataset=True)
        handed = apply_revocations_request({"revocations": [revoked.response["revocation"]], "source": "domain_a"}, request=http_request(), secrets=SECRETS_B)
        self.assertEqual(handed.response["accepted"], 1)
        apply_identity_request(self.b, self.audit_b, handed.request_event, config=CONFIG, secrets=SECRETS_B, applied_at=NOW)
        with self.assertRaises(WebError) as caught:
            self.use_in_b(made["token"], any_dataset=True)
        self.assertEqual(caught.exception.details["reason"], "token_revoked")
        self.assertEqual(get_token_revocation(self.b, made["token_id"]).source, "domain_a")

        # Handed again, nothing changes.
        again = apply_revocations_request({"revocations": [revoked.response["revocation"]]}, request=http_request(), secrets=SECRETS_B)
        apply_identity_request(self.b, self.audit_b, again.request_event, config=CONFIG, secrets=SECRETS_B, applied_at=NOW)

    def test_a_forged_revocation_is_refused_by_the_view_and_by_the_agent(self) -> None:
        made = self.make_in_a()
        forged = sign_revocation(made["token_id"], NOW_EPOCH + 86400, replace(SECRETS, federation_keys=(signing_key("x" * 40),))).to_dict()
        with self.assertRaises(WebError) as caught:
            apply_revocations_request({"revocations": [forged]}, request=http_request(), secrets=SECRETS_B)
        self.assertEqual(caught.exception.status_code, 403)
        for body in ({}, {"revocations": []}, {"revocations": "x"}, None):
            with self.subTest(body=body), self.assertRaises(WebError):
                apply_revocations_request(body, request=http_request(), secrets=SECRETS_B)
        # Sent straight to the agent, it is ignored.
        apply_identity_request(
            self.b,
            self.audit_b,
            request("apply_token_revocation", "system", {"revocations": [forged], "source": "evil"}),
            config=CONFIG,
            secrets=SECRETS_B,
            applied_at=NOW,
        )
        self.assertIsNone(get_token_revocation(self.b, made["token_id"]))

    def test_the_console_lists_revocations_to_hand_on(self) -> None:
        made = self.make_in_a()
        principal = authorize(http_request(session_token(get_user(self.a, "viewer"))), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        revoked = revoke_access_token_request(self.a, principal, made["token_id"], request=http_request(), secrets=SECRETS)
        apply_identity_request(self.a, self.audit_a, revoked.request_event, config=CONFIG, secrets=SECRETS, applied_at=NOW)
        listed = revocations_view(self.a, service_principal())["revocations"]
        self.assertEqual([item["jti"] for item in listed], [made["token_id"]])
        self.assertIsNotNone(read_revocation(listed[0], SECRETS))
        self.assertEqual(revocations_view(self.a, service_principal(), since=listed[0]["revoked_at"])["revocations"], [])
        viewer = authorize(http_request(session_token(get_user(self.a, "viewer"))), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        with self.assertRaises(WebError):
            revocations_view(self.a, viewer)

    def test_a_revocation_is_kept_until_its_token_has_expired(self) -> None:
        made = self.make_in_a({"name": "short", "expires_in_days": 1})
        principal = authorize(http_request(session_token(get_user(self.a, "viewer"))), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        revoked = revoke_access_token_request(self.a, principal, made["token_id"], request=http_request(), secrets=SECRETS)
        apply_identity_request(self.a, self.audit_a, revoked.request_event, config=CONFIG, secrets=SECRETS, applied_at=NOW)
        apply_identity_request(self.a, self.audit_a, request("maintenance", "system"), config=CONFIG, secrets=SECRETS, applied_at="2026-10-14T13:00:00Z")
        self.assertIsNotNone(get_token_revocation(self.a, made["token_id"]))
        apply_identity_request(self.a, self.audit_a, request("maintenance", "system"), config=CONFIG, secrets=SECRETS, applied_at="2026-10-16T13:00:00Z")
        self.assertIsNone(get_token_revocation(self.a, made["token_id"]))

    def test_the_account_page_lists_this_domain_s_tokens_with_qualified_dataset_names(self) -> None:
        self.make_in_a({"name": "BI", "datasets": ["shop", "domain_b.finance"]})
        principal = authorize(http_request(session_token(get_user(self.a, "viewer"))), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        listed = my_access_tokens_view(self.a, principal, domain="domain_a", now=MOMENT)
        self.assertEqual(listed["tokens"][0]["datasets"], [{"name": "domain_a.shop"}, {"name": "domain_b.finance"}])
        self.assertEqual(listed["tokens"][0]["issuer"], "domain_a")
        self.assertEqual([item["name"] for item in listed["datasets"]], ["domain_a.shop"])

    def test_without_a_federation_key_no_token_is_made(self) -> None:
        principal = authorize(http_request(session_token(get_user(self.a, "viewer"))), self.a, None, secrets=SECRETS, now=NOW_EPOCH)
        with self.assertRaises(WebError) as caught:
            create_access_token_request(principal, {"name": "x"}, request=http_request(), directory_table=self.a, secrets=IdentitySecrets(signing_keys=SECRETS.signing_keys), now=MOMENT)
        self.assertEqual(caught.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
