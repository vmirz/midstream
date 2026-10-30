"""The masks: keyed hashes, partial forms and withholding, and the key they are made with."""

from __future__ import annotations

import hashlib
import hmac
import re
import time
import unittest

from midstream.protection.catalog import KINDS, MASK_HASH, MASK_PARTIAL, MASK_REDACT
from midstream.protection.mask import HIDDEN, REDACTED, MissingProtectionKey, apply, canonical, hash_text, partial_text
from midstream.protection.plan import ProtectionPlan
from midstream.protection.policy import ProtectionPolicy
from midstream.protection.secrets import (
    MIN_PROTECTION_KEY_LENGTH,
    expected_key_id_from_environment,
    key_problem,
    protection_key,
    protection_key_from_environment,
)

KEY = b"k" * 40


class CanonicalTextTests(unittest.TestCase):
    def test_strings_and_numbers_have_their_json_text(self) -> None:
        self.assertEqual(canonical("5551234567"), "5551234567")
        self.assertEqual(canonical(5551234567), "5551234567")
        self.assertEqual(canonical(2.5), "2.5")
        self.assertEqual(canonical(-0.0), "-0.0")

    def test_nulls_booleans_and_containers_are_never_masked(self) -> None:
        for value in (None, True, False, {"a": 1}, [1]):
            self.assertIsNone(canonical(value))
            for mask in (MASK_HASH, MASK_PARTIAL, MASK_REDACT):
                self.assertIs(apply(mask, "phone", value, KEY), value)


class HashTests(unittest.TestCase):
    def test_a_hash_is_the_first_128_bits_of_the_hmac_in_hex(self) -> None:
        expected = hmac.new(KEY, b"jane@example.com", hashlib.sha256).hexdigest()[:32]
        self.assertEqual(apply(MASK_HASH, "email", "jane@example.com", KEY), expected)
        self.assertRegex(expected, r"^[0-9a-f]{32}$")

    def test_equal_values_hash_alike_and_a_number_like_its_text(self) -> None:
        self.assertEqual(apply(MASK_HASH, "phone", 5551234567, KEY), apply(MASK_HASH, "phone", "5551234567", KEY))
        self.assertEqual(apply(MASK_HASH, "email", "a@b", KEY), apply(MASK_HASH, "phone", "a@b", KEY))
        self.assertNotEqual(apply(MASK_HASH, "email", "a@b", KEY), apply(MASK_HASH, "email", "A@b", KEY))

    def test_another_key_hashes_differently(self) -> None:
        self.assertNotEqual(hash_text(KEY, "x"), hash_text(b"j" * 40, "x"))

    def test_hashing_without_a_key_refuses(self) -> None:
        with self.assertRaises(MissingProtectionKey):
            apply(MASK_HASH, "email", "a@b", None)


class PartialTests(unittest.TestCase):
    CASES = [
        ("phone", "540-948-9848", "****9848"),
        ("phone", "+1 (540) 948-9848", "****9848"),
        ("phone", "5409489848", "****9848"),
        ("card_number", "4242 4242 4242 4242", "****4242"),
        ("national_id", "123-45-6789", "****6789"),
        ("bank_account", "DE89 3704 0044 0532 0130 00", "****3000"),
        ("device_id", "35-209900-176148-1", "****1481"),
        ("email", "jane.doe@example.com", "****@example.com"),
        ("email", "a@b@example.org", "****@example.org"),
        ("email", "no-at-sign", "****"),
        ("person_name", "Johansson", "J****"),
        ("person_name", "Jane Doe", "J****"),
        ("person_name", "J", "****"),
        ("ip_address", "203.0.113.42", "203.0.*.*"),
        ("ip_address", "2001:db8:85a3::8a2e:370:7334", "2001:db8:****"),
        ("ip_address", "203.0.113.42:8080", "****"),
        ("date_of_birth", "1984-07-21", "1984-**-**"),
        ("date_of_birth", "1984-07-21T00:00:00Z", "1984-**-**"),
        ("date_of_birth", "21/07/1984", "****"),
        ("address", "221B Baker Street", "****"),
        ("secret", "correct horse battery staple", "****"),
    ]

    def test_each_kind_keeps_its_fragment(self) -> None:
        for kind_id, value, expected in self.CASES:
            with self.subTest(kind=kind_id, value=value):
                self.assertEqual(partial_text(kind_id, value), expected)
                self.assertEqual(apply(MASK_PARTIAL, kind_id, value, None), expected)

    def test_a_tail_never_shows_more_than_half_nor_more_than_four(self) -> None:
        self.assertEqual(partial_text("phone", "12345"), "****45")
        self.assertEqual(partial_text("phone", "1234"), "****34")
        self.assertEqual(partial_text("phone", "123"), "****3")
        self.assertEqual(partial_text("phone", "1"), "****")
        self.assertEqual(partial_text("phone", ""), "****")
        self.assertEqual(partial_text("phone", "no digits"), "****")
        self.assertEqual(partial_text("national_id", "1234567890123"), "****0123")

    def test_the_hidden_part_is_always_four_asterisks(self) -> None:
        for value in ("1", "12345678901234567890", "x" * 300):
            self.assertTrue(partial_text("secret", value) == HIDDEN)

    def test_a_number_is_shortened_from_its_text(self) -> None:
        self.assertEqual(apply(MASK_PARTIAL, "phone", 5409489848, None), "****9848")

    def test_every_kind_example_is_what_partial_makes(self) -> None:
        for item in KINDS:
            value, expected = item.partial_example
            with self.subTest(kind=item.id):
                self.assertEqual(partial_text(item.id, value), expected)


class RedactTests(unittest.TestCase):
    def test_a_withheld_value_is_redacted(self) -> None:
        for value in ("pw", 1234, 0.5):
            self.assertIs(apply(MASK_REDACT, "secret", value, None), REDACTED)


class KeyTests(unittest.TestCase):
    def test_a_key_has_a_short_id_and_never_shows_its_secret(self) -> None:
        key = protection_key("s" * 48)
        self.assertEqual(key.key_id, hashlib.sha256(b"s" * 48).hexdigest()[:8])
        self.assertNotIn("sss", repr(key))

    def test_a_short_key_is_refused(self) -> None:
        with self.assertLogs("midstream.protection.secrets", level="ERROR"):
            self.assertIsNone(protection_key_from_environment({"PROTECTION_KEY": "s" * (MIN_PROTECTION_KEY_LENGTH - 1)}))
        self.assertIsNone(protection_key_from_environment({}))
        self.assertIsNotNone(protection_key_from_environment({"PROTECTION_KEY": "s" * MIN_PROTECTION_KEY_LENGTH}))

    def test_the_expected_id_says_whether_a_key_may_hash(self) -> None:
        key = protection_key("s" * 48)
        self.assertEqual(key_problem(None, None), "protection_key_missing")
        self.assertIsNone(key_problem(key, None))
        self.assertIsNone(key_problem(key, key.key_id))
        self.assertEqual(key_problem(key, "deadbeef"), "protection_key_mismatch")
        self.assertEqual(expected_key_id_from_environment({"PROTECTION_KEY_ID": " abc "}), "abc")
        self.assertIsNone(expected_key_id_from_environment({}))


class CostTests(unittest.TestCase):
    def test_a_record_with_three_protected_fields_costs_little(self) -> None:
        policy = ProtectionPolicy({"email": MASK_HASH, "phone": MASK_PARTIAL, "secret": MASK_REDACT})
        plan = ProtectionPlan.for_policy(policy, secret=KEY)
        record = {
            "order_id": "O-1", "status": "paid", "total": 12.5, "currency": "EUR", "created_at": "2026-10-29T10:00:00Z",
            "customer": {"id": 7, "email": "jane@example.com", "phone": "540-948-9848", "tier": "gold"},
            "items": [{"sku": "A", "qty": 1, "price": 2.5}, {"sku": "B", "qty": 2, "price": 5.0}],
            "password": "pw", "channel": "web", "tags": ["a", "b"],
        }
        plan.protect(record)
        started = time.perf_counter()
        for _ in range(10_000):
            plan.protect(record)
        per_record_us = (time.perf_counter() - started) / 10_000 * 1e6
        self.assertLess(per_record_us, 60, f"{per_record_us:.1f} µs a record")

    def test_a_record_with_nothing_to_mask_comes_back_as_itself(self) -> None:
        plan = ProtectionPlan.for_policy(ProtectionPolicy({"email": MASK_HASH}), secret=KEY)
        record = {"a": 1, "b": {"c": [1, 2, {"d": "x"}]}}
        self.assertIs(plan.protect(record), record)
        self.assertIs(plan.protect(record)["b"], record["b"])


if __name__ == "__main__":
    unittest.main()
