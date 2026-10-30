"""The protection plan: the per-record walk, the policy and table-property models, and the committer's
classification of a table's fields (design §5.3–§5.6)."""

from __future__ import annotations

import json
import random
import unittest

from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.index import build_index
from midstream.lakehouse.schema.infer import infer
from midstream.lakehouse.schema.policy import evolve
from midstream.lakehouse.tables import build_schema
from midstream.protection.catalog import MASK_HASH, MASK_PARTIAL, MASK_REDACT
from midstream.protection.mask import MissingProtectionKey, hash_text
from midstream.protection.plan import (
    Decision,
    ProtectionPlan,
    initial_protection,
    record_new_fields,
    record_policy,
    review,
)
from midstream.protection.policy import (
    NONE,
    PROPERTY_PROTECTION,
    FieldProtection,
    PolicyError,
    ProtectionPolicy,
    TableProtection,
    parse_policy,
    protection_of_properties,
)

KEY = b"k" * 40
DEFAULT = ProtectionPolicy({"email": MASK_HASH, "phone": MASK_HASH, "national_id": MASK_HASH, "secret": MASK_REDACT})


def h(value) -> str:
    return hash_text(KEY, value if isinstance(value, str) else json.dumps(value))


def schema_from(*records, base=None):
    """The schema evolution would make of masked records, as the committer does from drift."""
    schema, fork_map, last = (build_schema(), ForkMap(), None) if base is None else base
    evolution = evolve(schema, fork_map, [((), infer(record)) for record in records], last_column_id=last, schema_id=None if base else 0)
    return evolution


def index_of(evolution):
    return build_index(evolution.schema, evolution.fork_map, last_column_id=evolution.last_column_id)


def field_id(index, *logical) -> int:
    for member in index.members.values():
        if member.logical_path == logical:
            return member.field_id
    raise KeyError(logical)


class PolicyParsingTests(unittest.TestCase):
    def test_a_list_of_kinds_takes_each_kinds_default_mask(self) -> None:
        policy = parse_policy({"kinds": ["email", "secret", "phone"]})
        self.assertEqual(policy.to_dict(), {"kinds": {"email": "hash", "phone": "hash", "secret": "redact"}})

    def test_a_map_chooses_each_mask(self) -> None:
        policy = parse_policy({"kinds": {"phone": "partial", "email": "hash", "secret": "partial"}})
        self.assertEqual(policy.kinds, {"phone": "partial", "email": "hash", "secret": "partial"})
        self.assertTrue(policy.hashes)
        self.assertFalse(parse_policy({"kinds": {"phone": "partial"}}).hashes)

    def test_an_empty_policy_protects_nothing(self) -> None:
        self.assertEqual(parse_policy({"kinds": {}}), NONE)
        self.assertEqual(parse_policy({"kinds": []}), NONE)
        self.assertEqual(parse_policy({}), NONE)
        self.assertTrue(NONE.empty)

    def test_refusals_name_the_field(self) -> None:
        cases = [
            (None, "protection"),
            ("email", "protection"),
            ({"kinds": "email"}, "protection.kinds"),
            ({"kinds": ["email", "nope"]}, "protection.kinds.1"),
            ({"kinds": {"nope": "hash"}}, "protection.kinds.nope"),
            ({"kinds": {"email": "encrypt"}}, "protection.kinds.email"),
            ({"kinds": {"secret": "hash"}}, "protection.kinds.secret"),
            ({"kinds": {}, "fields": {}}, "protection.fields"),
            ({"kinds": {}, "tables": {}}, "protection.tables"),
            ({"kinds": {}, "kafka_key": "hash"}, "protection.kafka_key"),
        ]
        for value, field_name in cases:
            with self.subTest(value=value):
                with self.assertRaises(PolicyError) as raised:
                    parse_policy(value)
                self.assertEqual(raised.exception.field, field_name)

    def test_a_secret_hash_says_what_is_allowed(self) -> None:
        with self.assertRaises(PolicyError) as raised:
            parse_policy({"kinds": {"secret": "hash"}})
        self.assertIn("partial, redact", str(raised.exception))

    def test_keys_the_caller_handles_are_let_through(self) -> None:
        self.assertEqual(parse_policy({"kinds": ["email"], "apply_rules": True}, allow=("apply_rules",)).kinds, {"email": "hash"})

    def test_a_stored_policy_drops_what_this_release_does_not_know(self) -> None:
        policy = ProtectionPolicy.from_dict({"kinds": {"email": "hash", "future_kind": "hash", "secret": "hash", "phone": "encrypt"}})
        self.assertEqual(policy.kinds, {"email": "hash"})
        self.assertEqual(ProtectionPolicy.from_dict(None), NONE)
        self.assertEqual(ProtectionPolicy.from_dict({"kinds": []}), NONE)

    def test_policies_compare_by_their_kinds(self) -> None:
        self.assertEqual(ProtectionPolicy({"email": "hash", "phone": "hash"}), ProtectionPolicy({"phone": "hash", "email": "hash"}))
        self.assertEqual(len({ProtectionPolicy({"email": "hash"}), ProtectionPolicy({"email": "hash"})}), 1)


class TablePropertyTests(unittest.TestCase):
    def test_the_property_round_trips(self) -> None:
        protection = TableProtection(
            revision=3,
            policy=ProtectionPolicy({"email": "hash"}),
            key_id="3f9a1c2e",
            fields={
                14: FieldProtection(14, ("customer", "email"), "email", "hash", since=0),
                40: FieldProtection(40, ("ssn__v2",), "national_id", "hash", since=7, until=9),
            },
        )
        again = TableProtection.from_json(protection.to_json())
        self.assertEqual(again, protection)
        self.assertEqual(set(again.active_fields()), {14})
        self.assertEqual(protection_of_properties({PROPERTY_PROTECTION: protection.to_json()}), protection)

    def test_nothing_recorded_reads_as_nothing_protected(self) -> None:
        for value in (None, "", "not json", "[]", '{"fields": {"x": {}}}'):
            with self.subTest(value=value):
                protection = TableProtection.from_json(value)
                self.assertTrue(protection.empty)
                self.assertEqual(protection.revision, 0)
        self.assertTrue(protection_of_properties(None).empty)
        self.assertTrue(protection_of_properties({"other": "x"}).empty)


class WalkTests(unittest.TestCase):
    def plan(self, policy=DEFAULT, index=None, protection=None) -> ProtectionPlan:
        if protection is None:
            protection = TableProtection(revision=1, policy=policy)
        return ProtectionPlan(protection, index, secret=KEY)

    def test_protected_values_are_masked_and_secrets_dropped(self) -> None:
        masked = self.plan().protect({"id": 1, "email": "a@b.com", "ssn": 123456789, "password": "pw", "note": "x"})
        self.assertEqual(masked, {"id": 1, "email": h("a@b.com"), "ssn": h(123456789), "note": "x"})

    def test_masks_follow_the_policy(self) -> None:
        policy = ProtectionPolicy({"phone": MASK_PARTIAL, "secret": MASK_PARTIAL, "email": MASK_REDACT})
        masked = self.plan(policy).protect({"phone": "540-948-9848", "password": "pw", "email": "a@b"})
        self.assertEqual(masked, {"phone": "****9848", "password": "****"})

    def test_containers_are_walked_and_generic_leaves_inherit(self) -> None:
        masked = self.plan().protect({"phone": {"number": "555-0100", "type": "mobile"}, "customer": {"email": "x@y", "tier": 1}})
        self.assertEqual(masked, {"phone": {"number": h("555-0100"), "type": "mobile"}, "customer": {"email": h("x@y"), "tier": 1}})

    def test_lists_mask_each_scalar_and_walk_each_object(self) -> None:
        masked = self.plan().protect({"emails": ["a@x", None, "b@y"], "contacts": [{"phone": "1"}, {"name": "n"}]})
        self.assertEqual(masked, {"emails": [h("a@x"), None, h("b@y")], "contacts": [{"phone": h("1")}, {"name": "n"}]})

    def test_a_withheld_list_is_dropped(self) -> None:
        self.assertEqual(self.plan().protect({"passwords": ["a", "b"], "id": 1}), {"id": 1})

    def test_every_leaf_of_a_secret_is_withheld(self) -> None:
        masked = self.plan().protect({"secret": {"hash": "abc", "salt": "def", "nested": {"value": "x", "flag": True}}, "id": 1})
        self.assertEqual(masked, {"secret": {"nested": {"flag": True}}, "id": 1})

    def test_a_container_emptied_by_withholding_is_withheld_too(self) -> None:
        # Its empty shape would otherwise land in `_unmapped`: a withheld value lands nowhere.
        record = {"id": 1, "credentials": {"secret": {"token": "t", "hint": "h"}}, "logins": [{"password": "a"}, {"password": "b"}]}
        self.assertEqual(self.plan().protect(record), {"id": 1})
        mixed = {"logins": [{"password": "a"}, {"password": "b", "at": 5}, None]}
        self.assertEqual(self.plan().protect(mixed), {"logins": [{"at": 5}, None]})

    def test_an_empty_container_is_left_as_it_came(self) -> None:
        record = {"email": "a@b", "meta": {}, "tags": [], "credentials": {"secret": {}}}
        masked = self.plan().protect(record)
        self.assertEqual(masked, {"email": h("a@b"), "meta": {}, "tags": [], "credentials": {"secret": {}}})
        self.assertIs(masked["meta"], record["meta"])

    def test_booleans_and_nulls_are_never_masked(self) -> None:
        record = {"mobile": True, "email": None, "is_secret": False}
        self.assertIs(self.plan().protect(record), record)

    def test_a_record_with_nothing_to_mask_is_returned_as_is(self) -> None:
        record = {"id": 1, "items": [{"sku": "a"}], "customer": {"tier": "gold"}}
        self.assertIs(self.plan().protect(record), record)

    def test_non_objects_pass_through(self) -> None:
        for value in (None, "a@b.com", 5, [1, 2]):
            self.assertIs(self.plan().protect(value), value)

    def test_an_empty_plan_skips_the_walk(self) -> None:
        plan = ProtectionPlan(TableProtection(), None, secret=None)
        self.assertTrue(plan.empty)
        record = {"email": "a@b"}
        self.assertIs(plan.protect(record), record)

    def test_hashing_without_a_key_refuses_rather_than_write_plain(self) -> None:
        plan = ProtectionPlan(TableProtection(revision=1, policy=DEFAULT), None, secret=None)
        self.assertTrue(plan.hashes)
        with self.assertRaises(MissingProtectionKey):
            plan.protect({"email": "a@b"})
        partial_only = ProtectionPlan(TableProtection(revision=1, policy=ProtectionPolicy({"phone": "partial"})), None, secret=None)
        self.assertFalse(partial_only.hashes)
        self.assertEqual(partial_only.protect({"phone": "540-948-9848"}), {"phone": "****9848"})

    def test_the_counters_count_masked_values_by_kind_and_mask(self) -> None:
        plan = self.plan()
        plan.protect({"email": "a", "emails": ["b", "c"], "password": "x", "id": 1})
        self.assertEqual(plan.drain_counters(), {("email", "hash"): 3, ("secret", "redact"): 1})
        self.assertEqual(plan.drain_counters(), {})

    def test_the_tables_record_beats_the_rules(self) -> None:
        # The table holds `email` plain (no entry) - created before the kind was protected - and
        # records `contact` as an email, which no rule would match.
        evolution = schema_from({"email": "a@b", "contact": "x"})
        index = index_of(evolution)
        contact = field_id(index, "contact")
        protection = TableProtection(
            revision=2,
            policy=DEFAULT,
            fields={contact: FieldProtection(contact, ("contact",), "email", MASK_HASH, since=0)},
        )
        masked = self.plan(index=index, protection=protection).protect({"email": "a@b", "contact": "c@d", "phone": "1"})
        self.assertEqual(masked, {"email": "a@b", "contact": h("c@d"), "phone": h("1")})

    def test_a_field_written_plain_again_is_not_masked(self) -> None:
        evolution = schema_from({"email": "x"})
        index = index_of(evolution)
        email = field_id(index, "email")
        protection = TableProtection(revision=3, policy=DEFAULT, fields={email: FieldProtection(email, ("email",), "email", "hash", 0, until=2)})
        self.assertEqual(self.plan(index=index, protection=protection).protect({"email": "a@b"}), {"email": "a@b"})

    def test_a_new_shape_of_a_known_key_is_decided_by_the_rules(self) -> None:
        # `phone` is a struct in the schema; a scalar phone forks a new field, which the rules decide.
        evolution = schema_from({"phone": {"number": "x", "type": "y"}})
        index = index_of(evolution)
        protection = initial_protection(index, DEFAULT, revision=1)
        masked = self.plan(index=index, protection=protection).protect({"phone": "555"})
        self.assertEqual(masked, {"phone": h("555")})


class ClassificationTests(unittest.TestCase):
    def test_a_new_tables_fields_are_classified_by_the_rules(self) -> None:
        masked = ProtectionPlan.for_policy(DEFAULT, secret=KEY).protect(
            {"id": 1, "email": "a@b", "phone": {"number": "1", "type": "m"}, "ssn": 5, "password": "x", "name": "n"}
        )
        index = index_of(schema_from(masked))
        protection = initial_protection(index, DEFAULT, revision=1, at="t", by="jane@example.com", key_id="abcd1234")
        protected = {item.path: (item.kind, item.mask, item.since) for item in protection.fields.values()}
        self.assertEqual(
            protected,
            {("email",): ("email", "hash", 0), ("phone", "number"): ("phone", "hash", 0), ("ssn",): ("national_id", "hash", 0)},
        )
        self.assertEqual(protection.revision, 1)
        self.assertEqual(protection.key_id, "abcd1234")
        self.assertEqual([(change.revision, change.by, change.sequence_number) for change in protection.history], [(1, "jane@example.com", 0)])
        self.assertNotIn("password", {member.name for member in index.members.values()})

    def test_a_table_without_a_policy_records_nothing_protected(self) -> None:
        index = index_of(schema_from({"email": "a@b"}))
        protection = initial_protection(index, NONE, revision=1)
        self.assertEqual(protection.fields, {})
        self.assertTrue(protection.empty)
        self.assertEqual(protection.history[0].revision, 1)

    def test_adding_a_kind_protects_its_fields_from_the_current_schema(self) -> None:
        index = index_of(schema_from({"email": "a@b", "dob": "1990-01-01", "id": 1}))
        protection = initial_protection(index, NONE, revision=1)
        policy = ProtectionPolicy({"email": "hash", "date_of_birth": "partial"})
        recorded = record_policy(protection, index, policy, revision=2, by="o@example.com", key_id="k1")
        self.assertEqual(
            {item.path: (item.kind, item.mask, item.since, item.until) for item in recorded.fields.values()},
            {("email",): ("email", "hash", 0, None), ("dob",): ("date_of_birth", "partial", 0, None)},
        )
        self.assertEqual(recorded.revision, 2)
        self.assertEqual(recorded.policy, policy)
        self.assertEqual([change.revision for change in recorded.history], [1, 2])

    def test_removing_a_kind_writes_its_fields_plain_again(self) -> None:
        index = index_of(schema_from({"email": "x", "phone": "y"}))
        protection = initial_protection(index, DEFAULT, revision=1)
        recorded = record_policy(protection, index, ProtectionPolicy({"phone": "hash"}), revision=2)
        email = recorded.fields[field_id(index, "email")]
        self.assertEqual(email.until, index.schema_id)
        self.assertFalse(email.active)
        self.assertTrue(recorded.fields[field_id(index, "phone")].active)

    def test_changing_a_mask_changes_it_for_existing_fields(self) -> None:
        index = index_of(schema_from({"phone": "y"}))
        protection = initial_protection(index, DEFAULT, revision=1)
        recorded = record_policy(protection, index, ProtectionPolicy({"phone": "partial"}), revision=2)
        phone = recorded.fields[field_id(index, "phone")]
        self.assertEqual((phone.kind, phone.mask, phone.until), ("phone", "partial", None))

    def test_recording_the_same_policy_changes_no_field(self) -> None:
        index = index_of(schema_from({"phone": "y", "email": "z"}))
        protection = initial_protection(index, DEFAULT, revision=1)
        recorded = record_policy(protection, index, DEFAULT, revision=2, reason="apply_rules")
        self.assertEqual(recorded.fields, protection.fields)
        self.assertEqual(recorded.history[-1].reason, "apply_rules")

    def test_a_field_added_to_a_protected_family_is_protected(self) -> None:
        # `ssn` was a number; hashing it makes a string, which forks the family.
        before = schema_from({"ssn": 123})
        before_index = index_of(before)
        protection = record_policy(initial_protection(before_index, NONE, revision=1), before_index, DEFAULT, revision=2)
        self.assertTrue(protection.fields[field_id(before_index, "ssn")].active)
        after = schema_from({"ssn": "a" * 32}, base=(before.schema, before.fork_map, before.last_column_id))
        after_index = index_of(after)
        updated = record_new_fields(protection, before_index, after_index)
        forks = [item for item in updated.fields.values() if item.path != ("ssn",)]
        self.assertEqual(len(forks), 1)
        self.assertEqual((forks[0].kind, forks[0].mask, forks[0].since), ("national_id", "hash", after_index.schema_id))

    def test_a_field_added_to_a_plain_family_stays_plain(self) -> None:
        before = schema_from({"email": 5})
        before_index = index_of(before)
        protection = initial_protection(before_index, NONE, revision=1)
        protection = TableProtection(revision=1, policy=DEFAULT, fields=protection.fields, history=protection.history)
        after = schema_from({"email": "text"}, base=(before.schema, before.fork_map, before.last_column_id))
        updated = record_new_fields(protection, before_index, index_of(after))
        self.assertEqual(updated.fields, {})

    def test_new_families_follow_the_rules_and_the_policy(self) -> None:
        before = schema_from({"id": 1})
        before_index = index_of(before)
        protection = initial_protection(before_index, DEFAULT, revision=1)
        after = schema_from(
            {"contact_email": "h", "dob": "1990-01-01", "phone": {"number": "h", "type": "m"}},
            base=(before.schema, before.fork_map, before.last_column_id),
        )
        after_index = index_of(after)
        updated = record_new_fields(protection, before_index, after_index)
        self.assertEqual(
            {item.path: item.kind for item in updated.fields.values()},
            {("contact_email",): "email", ("phone", "number"): "phone"},
        )

    def test_the_review_lists_what_applying_the_rules_would_change(self) -> None:
        index = index_of(schema_from({"email": "x", "ip": "y"}))
        protection = initial_protection(index, NONE, revision=1)
        protection = TableProtection(revision=1, policy=ProtectionPolicy({"email": "hash", "ip_address": "partial"}), fields={}, history=protection.history)
        items = review(protection, index)
        self.assertEqual(
            [(item.path, item.current, item.proposed) for item in items],
            [(("email",), None, Decision("email", "hash")), (("ip",), None, Decision("ip_address", "partial"))],
        )
        self.assertEqual(items[0].to_dict()["proposed"], {"kind": "email", "mask": "hash"})


class StabilityTests(unittest.TestCase):
    """What the writer masks, the committer records; what is recorded, the next plan masks the same."""

    POOL = {
        "email": lambda r: f"user{r.randint(0, 50)}@example.com",
        "phone": lambda r: r.choice([f"555-{r.randint(1000, 9999)}", r.randint(5550000000, 5559999999)]),
        "ssn": lambda r: r.choice([r.randint(100000000, 999999999), f"{r.randint(100, 999)}-45-6789"]),
        "password": lambda r: "pw",
        "name": lambda r: "n",
        "total": lambda r: r.choice([1, 2.5]),
        "customer": lambda r: {"email": "c@x", "phone": {"number": "1", "type": "m"}, "tier": r.choice(["a", "b"])},
        "tags": lambda r: ["a", "b"],
        "contacts": lambda r: [{"email": "z@x", "label": "work"}],
    }

    def test_masked_columns_hold_only_masked_values_across_schema_changes(self) -> None:
        rng = random.Random(7)
        schema, fork_map, last = build_schema(), ForkMap(), None
        protection = TableProtection(revision=1, policy=DEFAULT)
        index = None
        for step in range(40):
            keys = rng.sample(sorted(self.POOL), rng.randint(1, len(self.POOL)))
            record = {key: self.POOL[key](rng) for key in keys}
            plan = ProtectionPlan(protection, index, secret=KEY)
            masked = plan.protect(record)
            again = ProtectionPlan(protection, index, secret=KEY).protect(record)
            self.assertEqual(masked, again)
            evolution = evolve(schema, fork_map, [((), infer(masked))], last_column_id=last, schema_id=0 if index is None else None)
            if evolution.changed or index is None:
                new_index = build_index(evolution.schema, evolution.fork_map, last_column_id=evolution.last_column_id)
                protection = record_new_fields(protection, index, new_index) if index is not None else initial_protection(new_index, DEFAULT, revision=1)
                schema, fork_map, last, index = evolution.schema, evolution.fork_map, evolution.last_column_id, new_index
            # Every value under a protected field id is a hash.
            for field_entry in protection.active_fields().values():
                value = masked
                for part in field_entry.path:
                    value = value.get(part) if isinstance(value, dict) else None
                if isinstance(value, str):
                    self.assertRegex(value, r"^[0-9a-f]{32}$", f"step {step}: {field_entry.path}")
            self.assertNotIn("password", json.dumps(masked))
            self.assertNotIn("@example.com", json.dumps(masked))


if __name__ == "__main__":
    unittest.main()
