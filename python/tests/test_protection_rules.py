"""The rules that recognise sensitive data by field name: the design's precision table, as fixtures.

Every kind lists the names it must match and the near-misses it must not. Adding a rule means adding
both its hits and its near-misses here (design §5.1).
"""

from __future__ import annotations

import unittest

from midstream.protection.catalog import GENERIC_LEAVES, KIND_IDS, KINDS, KINDS_BY_ID, MASK_HASH, MASK_PARTIAL, MASK_REDACT
from midstream.protection.rules import classify, container_kind, normalize, rule_kind

#: kind -> (names it matches, names it must not match). Names are raw keys, in several spellings.
PRECISION = {
    "email": (
        ["email", "Email", "email_address", "emailAddress", "EMAIL_ADDR", "email_id", "billing_email", "contactEmail",
         "customer_email_address", "emails"],
        ["email_verified", "emailDomain", "email_count", "mail", "e_mail_opt_in"],
    ),
    "phone": (
        ["phone", "phone_number", "phoneNumber", "Phone-Number", "PHONE NUMBER", "phone_no", "phonenum", "telephone",
         "tel", "mobile", "mobile_number", "cell_phone", "cellNumber", "fax", "fax_number", "msisdn", "home_phone",
         "workPhone", "contact_phone_number", "phones", "phone_numbers"],
        ["phone_type", "phoneCountryCode", "cell", "microphone", "headphone", "iphone", "smartphone", "automobile"],
    ),
    "national_id": (
        ["ssn", "SSN", "social_security_number", "national_id", "national_insurance_number", "nino", "tax_id",
         "taxpayer_id", "tin", "itin", "passport", "passport_number", "drivers_license", "driving_licence",
         "dl_number", "id_card_number", "identity_number", "personnummer", "bsn", "cpf", "curp", "dni", "nif",
         "pesel", "aadhaar", "sin", "nric", "hkid", "ssns"],
        ["ein", "license_number", "id", "user_id", "customer_id", "national", "tax", "taxes"],
    ),
    "card_number": (
        ["card_number", "cardNumber", "card_no", "credit_card", "credit_card_number", "debit_card", "cc_number",
         "pan", "primary_account_number", "gift_card_number"],
        ["cc", "last4", "last_four", "masked_pan", "card_brand", "expiry", "card_type"],
    ),
    "bank_account": (
        ["iban", "IBAN", "account_number", "acct_no", "bank_account", "bank_account_number", "routing_number", "aba",
         "sort_code", "bsb", "clabe"],
        ["swift", "bic", "account_id", "account", "bank_name"],
    ),
    "address": (
        ["address", "street_address", "street", "address_line_1", "addressLine2", "address1", "home_address",
         "billing_address", "shipping_address", "mailing_address", "postal_address", "house_number", "addresses"],
        ["city", "state", "region", "country", "postal_code", "zip", "zip_code", "ip_address", "email_address",
         "mac_address", "line1", "address_type"],
    ),
    "ip_address": (
        ["ip", "IP", "ip_address", "ipAddr", "client_ip", "remote_ip", "source_ip", "src_ip", "remote_addr",
         "user_ip", "ipv4", "ipv6", "x_forwarded_for", "client_ip_address"],
        ["zip", "tooltip", "membership", "ship", "tip", "skip", "vip_level"],
    ),
    "date_of_birth": (
        ["dob", "DOB", "date_of_birth", "dateOfBirth", "birthdate", "birth_date", "birthday", "customer_birthdate"],
        ["age", "birth_year", "birthplace", "date"],
    ),
    "device_id": (
        ["device_id", "deviceId", "imei", "imsi", "meid", "mac_address", "mac", "hwid", "hardware_id", "android_id",
         "advertising_id", "idfa", "idfv", "gaid", "udid"],
        ["serial_number", "device_type", "device_model", "device", "machine"],
    ),
    "secret": (
        ["password", "Password", "passwd", "pwd", "passphrase", "secret", "secret_key", "client_secret", "api_key",
         "apiKey", "access_token", "refresh_token", "id_token", "auth_token", "bearer_token", "session_token",
         "csrf_token", "private_key", "encryption_key", "signing_key", "cvv", "cvc", "cvv2", "security_code", "pin",
         "otp", "otp_code", "one_time_code", "mfa_code", "totp", "recovery_code", "security_answer",
         "old_password", "newPassword", "webhook_secret", "stripe_api_key", "passwords"],
        ["token", "cid", "authorization", "key", "password_hash_algorithm", "password_updated_at", "pin_code",
         "pincode", "secrets_count"],
    ),
}

#: Common field names of unrelated data that no rule may match.
UNRELATED = [
    "id", "status", "total", "created_at", "updated_at", "name", "description", "items", "sku", "quantity",
    "price", "currency", "country", "city", "zip", "type", "category", "url", "message", "count", "score",
    "order_id", "event", "timestamp", "amount", "tags", "version", "source", "title", "notes", "product",
    "region", "state", "level", "kind", "value", "number", "key", "token", "code", "date", "first", "last",
]


class NormalizationTests(unittest.TestCase):
    def test_names_are_compared_as_lowercase_letters_and_digits(self) -> None:
        for name in ("phone_number", "phoneNumber", "Phone-Number", "PHONE NUMBER", "phone.number"):
            self.assertEqual(normalize(name), "phonenumber")
        self.assertEqual(normalize("address_line_1"), "addressline1")
        self.assertEqual(normalize(""), "")
        self.assertEqual(normalize("___"), "")


class PrecisionTests(unittest.TestCase):
    def test_every_kind_matches_its_names(self) -> None:
        for kind_id, (hits, _) in PRECISION.items():
            for name in hits:
                with self.subTest(kind=kind_id, name=name):
                    self.assertEqual(rule_kind(None, name), kind_id)

    def test_no_kind_matches_its_near_misses(self) -> None:
        for kind_id, (_, misses) in PRECISION.items():
            for name in misses:
                with self.subTest(kind=kind_id, name=name):
                    self.assertNotEqual(rule_kind(None, name), kind_id)

    def test_common_unrelated_names_match_nothing(self) -> None:
        for name in UNRELATED:
            with self.subTest(name=name):
                self.assertIsNone(rule_kind(None, name))

    def test_every_kind_is_in_the_precision_table(self) -> None:
        self.assertEqual(set(PRECISION) | {"person_name"}, set(KIND_IDS))

    def test_no_exact_name_of_one_kind_matches_another(self) -> None:
        for item in KINDS:
            for rule in item.rules:
                if rule.under:
                    continue
                for name in rule.exact:
                    with self.subTest(kind=item.id, name=name):
                        self.assertEqual(rule_kind(None, name), item.id)


class PersonNameTests(unittest.TestCase):
    def test_names_of_people_match_anywhere(self) -> None:
        for name in ("first_name", "lastName", "surname", "given_name", "family_name", "middle_name", "full_name",
                     "maiden_name", "legal_name", "customer_first_name", "billing_last_name"):
            with self.subTest(name=name):
                self.assertEqual(rule_kind(None, name), "person_name")

    def test_a_bare_name_matches_only_under_a_person(self) -> None:
        for parent in ("customer", "user", "passenger", "Customer", "account_holder", "customers", "passengers", "contacts"):
            with self.subTest(parent=parent):
                self.assertEqual(rule_kind(parent, "name"), "person_name")
                self.assertEqual(rule_kind(parent, "display_name"), "person_name")
        for parent in (None, "product", "event", "category", "store", "items"):
            with self.subTest(parent=parent):
                self.assertIsNone(rule_kind(parent, "name"))

    def test_handles_and_initials_are_not_names(self) -> None:
        for name in ("username", "user_name", "nickname", "initials", "name"):
            with self.subTest(name=name):
                self.assertIsNone(rule_kind(None, name))


class AddressLineTests(unittest.TestCase):
    def test_numbered_lines_match_only_under_an_address(self) -> None:
        for parent in ("address", "billing_address", "shipping_address", "residence", "addresses"):
            with self.subTest(parent=parent):
                self.assertEqual(rule_kind(parent, "line1"), "address")
        self.assertIsNone(rule_kind(None, "line1"))
        self.assertIsNone(rule_kind("invoice", "line2"))


class InheritanceTests(unittest.TestCase):
    def test_a_generic_leaf_takes_its_containers_kind(self) -> None:
        self.assertEqual(classify("phone", "number", "phone"), "phone")
        self.assertEqual(classify("phone", "e164", "phone"), "phone")
        self.assertEqual(classify("email", "address", "email"), "email")
        self.assertEqual(classify("billing_address", "line1", "address"), "address")
        self.assertEqual(classify("customer_name", "first", "person_name"), "person_name")

    def test_other_leaves_keep_their_own_kind_or_none(self) -> None:
        self.assertIsNone(classify("phone", "type", "phone"))
        self.assertIsNone(classify("phone", "country_code", "phone"))
        self.assertIsNone(classify("address", "city", "address"))
        self.assertEqual(classify("phone", "email", "phone"), "email")

    def test_generic_leaves_outside_a_matched_container_are_plain(self) -> None:
        for leaf in ("number", "value", "line1", "first", "address_type"):
            with self.subTest(leaf=leaf):
                self.assertNotIn(classify("order", leaf, None), {"phone", "email", "person_name"})

    def test_every_leaf_of_a_secret_takes_it(self) -> None:
        self.assertEqual(classify("credentials_secret", "hash", "secret"), "secret")
        self.assertEqual(classify("secret", "anything", "secret"), "secret")
        self.assertEqual(container_kind("secret", "aws", "secret"), "secret")
        self.assertIsNone(container_kind("phone", "carrier", "phone"))

    def test_a_container_passes_its_own_kind(self) -> None:
        self.assertEqual(container_kind(None, "phone"), "phone")
        self.assertEqual(container_kind(None, "billing_address"), "address")
        self.assertEqual(container_kind("customer", "name"), "person_name")
        self.assertIsNone(container_kind(None, "customer"))

    def test_generic_leaves_are_normalised(self) -> None:
        for leaf in GENERIC_LEAVES:
            self.assertEqual(leaf, normalize(leaf))


class CatalogTests(unittest.TestCase):
    def test_secrets_are_never_hashed(self) -> None:
        secret = KINDS_BY_ID["secret"]
        self.assertFalse(secret.allows(MASK_HASH))
        self.assertTrue(secret.allows(MASK_PARTIAL))
        self.assertTrue(secret.allows(MASK_REDACT))
        self.assertEqual(secret.default_mask, MASK_REDACT)

    def test_every_other_kind_allows_every_mask_and_hashes_by_default(self) -> None:
        for item in KINDS:
            if item.id == "secret":
                continue
            with self.subTest(kind=item.id):
                self.assertEqual(set(item.masks), {MASK_HASH, MASK_PARTIAL, MASK_REDACT})
                self.assertEqual(item.default_mask, MASK_HASH)

    def test_every_kind_describes_itself(self) -> None:
        for item in KINDS:
            payload = item.to_dict()
            self.assertTrue(payload["label"])
            self.assertTrue(payload["description"])
            self.assertIn(payload["default_mask"], payload["masks"])
            self.assertTrue(payload["partial_example"]["partial"].startswith("****") or "****" in payload["partial_example"]["partial"] or "*.*" in payload["partial_example"]["partial"] or "**-**" in payload["partial_example"]["partial"])


if __name__ == "__main__":
    unittest.main()
