"""Reprotect: which files hold, in plain text, values a table's protection masks now; how they are
rewritten and committed; and the job in the maintenance app (design §15.3, handbook 22 §22.14)."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import LakeFixture  # noqa: E402

from midstream.lakehouse.commits import commit_protection, commit_schema, fork_map_of  # noqa: E402
from midstream.lakehouse.files import build_data_file, protection_label_of, read_parquet_footer  # noqa: E402
from midstream.lakehouse.lifecycle import schema_to_json  # noqa: E402
from midstream.lakehouse.maintenance import commit_reprotect, live_data_files, purge_before  # noqa: E402
from midstream.lakehouse.store import PreconditionFailedError  # noqa: E402
from midstream.maintenance_app.reprotect_worker import run as run_reprotect  # noqa: E402
from midstream.protection.mask import hash_text  # noqa: E402
from midstream.protection.secrets import protection_key  # noqa: E402
from pyiceberg.manifest import ManifestContent  # noqa: E402
from pyiceberg.typedef import Record  # noqa: E402
from midstream.lakehouse.reprotect import (  # noqa: E402
    BLOCKED,
    MASK,
    MOVE,
    UNKNOWN_FILE,
    WITHHOLD,
    field_births,
    plan_reprotect,
)
from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.schema.index import build_index  # noqa: E402
from midstream.lakehouse.schema.infer import infer  # noqa: E402
from midstream.lakehouse.schema.policy import evolve  # noqa: E402
from midstream.protection.plan import record_new_fields, record_policy  # noqa: E402
from midstream.protection.policy import ProtectionPolicy, TableProtection, protection_of_properties  # noqa: E402

KEY_TEXT = "reprotect-test-key-" + "k" * 24
KEY_ID = protection_key(KEY_TEXT).key_id
EMAIL = ProtectionPolicy({"email": "hash"})
SEED = {"order_id": "O-0", "email": "e", "phone": "p", "ip_address": "1.2.3.4"}
BIG = 1 << 40


class ReprotectTable:
    """A table whose protection a test edits as the committer would, and fills with files of chosen labels."""

    def __init__(self, *records, policy: ProtectionPolicy | None = None) -> None:
        self.lake = LakeFixture(*(records or (SEED,)), protection_policy=policy)
        self.offset = -1

    def close(self) -> None:
        self.lake.close()

    def load(self):
        return self.lake.load()

    def protection(self) -> TableProtection:
        return protection_of_properties(self.load().metadata.properties)

    def index(self, table=None):
        table = table or self.load()
        return build_index(table.schema(), fork_map_of(table.metadata), last_column_id=table.metadata.last_column_id)

    def edit(self, policy: ProtectionPolicy, revision: int) -> None:
        """A delivered policy, recorded as the committer records it."""
        table = self.load()
        index = self.index(table)
        protection = record_policy(
            self.protection(),
            index,
            policy,
            revision=revision,
            at="2026-10-30T00:00:00Z",
            by="owner@example.com",
            key_id=KEY_ID,
            sequence_number=table.metadata.last_sequence_number,
        )
        commit_protection(table, protection.to_json(), schema_id=index.schema_id)

    def add_fields(self, record: dict) -> None:
        """A schema change from a record the writers saw, with its new fields recorded."""
        table = self.load()
        fork_map = fork_map_of(table.metadata)
        evolution = evolve(table.schema(), fork_map, [((), infer(record))], last_column_id=table.metadata.last_column_id)
        before = build_index(table.schema(), fork_map, last_column_id=table.metadata.last_column_id)
        after = build_index(evolution.schema, evolution.fork_map, last_column_id=evolution.last_column_id)
        commit_schema(table, evolution, protection_json=record_new_fields(self.protection(), before, after).to_json())

    def write(self, *values: dict, label: tuple[int, ...]) -> None:
        """One committed file of `values`, named with `label`."""
        offsets = [self.offset + 1 + position for position in range(len(values))]
        announced = self.lake.write_seal(partition=0, after_offset=self.offset, offsets=offsets, values=list(values), protection_label=label)
        self.lake.commit(announced)
        self.offset = offsets[-1]

    def plan(self, **bounds):
        return plan_reprotect(self.load(), max_input_bytes=bounds.get("max_input_bytes", BIG), **{k: v for k, v in bounds.items() if k != "max_input_bytes"})


def actions(program) -> set[tuple]:
    return {(".".join(action.path), action.action, action.kind, action.mask) for action in program.actions}


class PlanTests(unittest.TestCase):
    def table(self, *records, policy=None) -> ReprotectTable:
        table = ReprotectTable(*records, policy=policy)
        self.addCleanup(table.close)
        return table

    def test_files_from_before_data_protection_are_rewritten_once_a_policy_masks_their_values(self) -> None:
        table = self.table()
        table.write({"order_id": "O-1", "email": "a@example.com"}, {"order_id": "O-2", "email": "b@example.com"}, label=(0,))
        table.edit(EMAIL, revision=1)
        table.write({"order_id": "O-3", "email": "hashed"}, label=(1,))
        plan, backlog = table.plan()
        self.assertEqual([(0,)], [item.protection_label for item in plan.inputs])
        self.assertEqual({("email", MASK, "email", "hash")}, actions(plan.programs[(0,)]))
        self.assertEqual((1, 2, 0, 0), (backlog.files, backlog.rows, backlog.blocked_files, backlog.unknown_files))
        self.assertEqual(1, backlog.revision)
        self.assertEqual((1, 0), plan.programs[(0,)].output_label)

    def test_a_kind_added_rewrites_the_files_holding_its_values_and_only_those(self) -> None:
        table = self.table(policy=EMAIL)
        table.write({"order_id": "O-1", "email": "h1", "ip_address": "203.0.113.1"}, label=(1,))
        table.write({"order_id": "O-2", "email": "h2"}, label=(1,))
        table.edit(ProtectionPolicy({"email": "hash", "ip_address": "partial"}), revision=2)
        table.write({"order_id": "O-3", "ip_address": "203.0.*.*"}, label=(2,))
        plan, backlog = table.plan()
        self.assertEqual(1, len(plan.inputs), "only the revision-1 file holding IP addresses")
        self.assertEqual({("ip_address", MASK, "ip_address", "partial")}, actions(plan.programs[(1,)]))
        self.assertEqual(1, backlog.files)

    def test_a_changed_mask_rewrites_nothing(self) -> None:
        # The phone numbers revision 1 hashed cannot become partial: there is no plain text left.
        table = self.table(policy=ProtectionPolicy({"phone": "hash"}))
        table.write({"order_id": "O-1", "phone": "hashed"}, label=(1,))
        table.edit(ProtectionPolicy({"phone": "partial"}), revision=2)
        plan, backlog = table.plan()
        self.assertIsNone(plan)
        self.assertTrue(backlog.empty)

    def test_a_withheld_kind_nulls_the_values_written_plain(self) -> None:
        table = self.table(policy=EMAIL)
        table.write({"order_id": "O-1", "phone": "540-948-9848"}, label=(1,))
        table.edit(ProtectionPolicy({"email": "hash", "phone": "redact"}), revision=2)
        plan, _ = table.plan()
        self.assertEqual({("phone", WITHHOLD, "phone", "redact")}, actions(plan.programs[(1,)]))

    def test_a_number_field_waits_for_its_text_field_then_moves_into_it(self) -> None:
        table = self.table({"order_id": "O-0", "account_number": 1}, policy=EMAIL)
        table.write({"order_id": "O-1", "account_number": 880000001}, label=(1,))
        table.edit(ProtectionPolicy({"email": "hash", "bank_account": "hash"}), revision=2)
        plan, backlog = table.plan()
        self.assertIsNone(plan)
        self.assertEqual((0, 1), (backlog.files, backlog.blocked_files))
        self.assertIn("account_number has no text field yet", backlog.blocked[0])
        # The writers fork the family with the first masked value; the fork is recorded protected.
        table.add_fields({"account_number": "0123456789abcdef0123456789abcdef"})
        plan, backlog = table.plan()
        program = plan.programs[(1,)]
        (move,) = [action for action in program.actions if action.action == MOVE]
        self.assertEqual(("account_number",), move.path)
        self.assertEqual(("bank_account", "hash"), (move.kind, move.mask))
        self.assertTrue(move.into_path[0].startswith("account_number_"), move.into_path)
        self.assertEqual((1, 0), (backlog.files, backlog.blocked_files))

    def test_rows_rewritten_before_keep_the_masks_their_lineage_gave_them(self) -> None:
        table = self.table(policy=EMAIL)
        table.edit(ProtectionPolicy({"email": "hash", "ip_address": "hash"}), revision=2)
        # Rows written under 1 and rewritten under 2: IP addresses hashed by the rewrite, phones plain.
        table.write({"order_id": "O-1", "ip_address": "hashed", "phone": "540-948-9848"}, label=(2, 1))
        table.edit(ProtectionPolicy({"email": "hash", "ip_address": "partial", "phone": "hash"}), revision=3)
        plan, _ = table.plan()
        program = plan.programs[(2, 1)]
        self.assertEqual({("phone", MASK, "phone", "hash")}, actions(program), "IP addresses are masked already, as revision 2 hashed them")
        self.assertEqual((3, 2, 1), program.output_label)

    def test_a_record_from_before_states_cannot_judge_files_of_earlier_revisions(self) -> None:
        table = self.table(policy=EMAIL)
        table.write({"order_id": "O-1", "email": "hashed", "phone": "540-948-9848"}, label=(1,))
        table.write({"order_id": "O-2", "email": "a@example.com"}, label=(0,))
        protection = table.protection().to_dict()
        for entry in protection["fields"].values():
            # As a record of the first release of data protection reads: no states.
            entry.pop("states", None)
            entry.pop("known_from", None)
        protection["revision"] = 2
        protection["history"].append({**protection["history"][0], "revision": 2})
        legacy = TableProtection.from_dict(protection)
        current = table.load()
        commit_protection(current, legacy.to_json(), schema_id=current.schema().schema_id)
        plan, backlog = table.plan()
        self.assertEqual(1, backlog.unknown_files, "the revision-1 file: what revision 1 did to its emails is not known")
        self.assertEqual([(0,)], [item.protection_label for item in plan.inputs], "revision 0 masked nothing, whatever the record")

    def test_text_in_unmapped_is_rewritten_when_a_kind_it_may_hold_was_plain(self) -> None:
        table = self.table(policy=EMAIL)
        table.write({"order_id": "O-1", "extra": {"ip_address": "203.0.113.1"}}, label=(1,))
        table.edit(ProtectionPolicy({"email": "hash", "phone": "partial"}), revision=2)
        plan, _ = table.plan()
        self.assertIsNotNone(plan, "phone numbers were plain under revision 1: unmapped text may hold some")
        self.assertTrue(plan.programs[(1,)].json)

    def test_unmapped_text_is_left_when_no_kind_became_masked(self) -> None:
        table = self.table(policy=ProtectionPolicy({"email": "hash", "phone": "hash"}))
        table.write({"order_id": "O-1", "extra": {"phone": "hashed"}}, label=(1,))
        table.edit(ProtectionPolicy({"email": "hash", "phone": "partial"}), revision=2)
        plan, backlog = table.plan()
        self.assertIsNone(plan)
        self.assertTrue(backlog.empty)

    def test_a_field_born_under_a_later_revision_was_masked_by_the_rules_in_files_of_earlier_ones(self) -> None:
        # A writer still under revision 1 saw contact_email first; it masked it by the rules and
        # revision 1's policy, which hashes emails. The field was born under revision 2.
        table = self.table(policy=EMAIL)
        table.edit(ProtectionPolicy({"email": "hash", "phone": "hash"}), revision=2)
        table.add_fields({"contact_email": "hashed"})
        table.write({"order_id": "O-1", "contact_email": "hashed"}, label=(1,))
        births = field_births(table.load().metadata.schemas, table.protection())
        contact = next(member for member in table.index().members.values() if member.logical_path == ("contact_email",))
        self.assertEqual(2, births[contact.field_id])
        plan, backlog = table.plan()
        self.assertIsNone(plan, "masked already, by revision 1's rules: hashing it again would ruin it")
        self.assertTrue(backlog.empty)

    def test_a_plan_takes_the_oldest_files_within_its_bounds_and_counts_them_all(self) -> None:
        table = self.table()
        for offset in range(4):
            table.write({"order_id": f"O-{offset}", "email": f"u{offset}@example.com"}, label=(0,))
        table.edit(EMAIL, revision=1)
        plan, backlog = table.plan(max_input_bytes=1)
        self.assertEqual(1, len(plan.inputs), "at least one file, however large")
        self.assertEqual(4, backlog.files)
        plan, _ = table.plan(max_input_files=3)
        self.assertEqual(3, len(plan.inputs))

    def test_a_table_that_masks_nothing_has_nothing_to_rewrite(self) -> None:
        table = self.table()
        table.write({"order_id": "O-1", "email": "a@example.com"}, label=(0,))
        plan, backlog = table.plan()
        self.assertIsNone(plan)
        self.assertEqual(0, backlog.revision)


def h(value) -> str:
    return hash_text(KEY_TEXT.encode(), value if isinstance(value, str) else json.dumps(value))


PLAIN_SEED = {
    "order_id": "O-0",
    "email": "e@x",
    "phone": "540-948-0000",
    "password": "pw",
    "contact": {"email": "c@x", "tier": "gold"},
    "emails": ["a@x"],
    "account_number": 1,
    "note": "n",
}
POLICY = ProtectionPolicy({"email": "hash", "phone": "partial", "secret": "redact", "bank_account": "hash"})


def plain_record(n: int) -> dict:
    return {
        "order_id": f"O-{n}",
        "email": f"jane{n}@example.com",
        "phone": f"540-948-{n:04d}",
        "password": f"hunter{n}",
        "contact": {"email": f"c{n}@example.com", "tier": "gold"},
        "emails": [f"a{n}@example.com", None, f"b{n}@example.com"],
        "account_number": 880000000 + n,
        "note": f"note {n}",
        "extra": {"email": f"u{n}@example.com", "flag": True},
    }


class RewriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = ReprotectTable(PLAIN_SEED)
        self.addCleanup(self.table.close)
        # The reprotect worker finds its store by URI, as in the maintenance app.
        self.table.lake.install()
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.work = work.name
        environment = patch.dict(os.environ, {"PROTECTION_KEY": KEY_TEXT})
        environment.start()
        self.addCleanup(environment.stop)

    def reprotect(self, *, key: str | None = None):
        """Plan, rewrite in this process and commit, as the maintenance app does; return the result."""
        table = self.table.load()
        plan, backlog = plan_reprotect(table, max_input_bytes=BIG)
        if plan is None:
            return None, backlog
        from midstream.lakehouse.reprotect import field_births

        job = {
            "plan": plan.to_dict(),
            "schema": schema_to_json(table.schema()),
            "fork_map": fork_map_of(table.metadata).to_dict(),
            "last_column_id": table.metadata.last_column_id,
            "protection": self.table.protection().to_dict(),
            "births": {str(field_id): revision for field_id, revision in field_births(table.metadata.schemas, self.table.protection()).items()},
            "work_dir": f"reprotect-{plan.job_id[:8]}",
            "location": table.location(),
            "granularity": "day",
        }
        environment = {"PROTECTION_KEY": key} if key is not None else {}
        with patch.dict(os.environ, environment):
            result = run_reprotect(job, work_root=self.work)
        inputs = {item.path: item for item in plan.inputs}
        rewritten = []
        for output in result["outputs"]:
            footer, size = read_parquet_footer(self.table.lake.store, output["uri"])
            data_file = build_data_file(
                table.metadata, file_path=output["uri"], file_size=size, parquet_metadata=footer, granularity="day", partition=Record(*output["partition"])
            )
            rewritten.append((inputs[output["input"]], data_file))
        snapshot_id = commit_reprotect(table, plan, rewritten, runtime="test-maintenance")
        return (plan, result, snapshot_id), backlog

    def rows(self) -> list[dict]:
        return sorted(self.table.load().scan().to_arrow().to_pylist(), key=lambda row: row["_kafka_offset"])

    def fork_name(self) -> str:
        index = self.table.index()
        (fork,) = [member for member in index.members.values() if member.logical_path == ("account_number",) and member.fork_version > 1]
        return fork.name

    def test_rows_written_plain_are_masked_as_the_policy_masks_them_now(self) -> None:
        self.table.write(plain_record(1), plain_record(2), label=(0,))
        self.table.edit(POLICY, revision=1)
        # A writer under revision 1 masked an account number, forking the family to text.
        self.table.add_fields({"account_number": h(880000009)})
        (plan, result, snapshot_id), backlog = self.reprotect()
        self.assertEqual((1, 2), (backlog.files, backlog.rows))
        self.assertEqual([[1, 0]], [output["protection_label"] for output in result["outputs"]])
        (row_1, row_2) = self.rows()
        fork = self.fork_name()
        self.assertEqual(h("jane1@example.com"), row_1["email"])
        self.assertEqual("****0001", row_1["phone"])
        self.assertIsNone(row_1["password"])
        self.assertEqual({"email": h("c1@example.com"), "tier": "gold"}, row_1["contact"])
        self.assertEqual([h("a1@example.com"), None, h("b1@example.com")], row_1["emails"])
        self.assertIsNone(row_1["account_number"])
        self.assertEqual(h(880000001), row_1[fork])
        self.assertEqual("note 1", row_1["note"])
        self.assertEqual({"extra": {"email": h("u1@example.com"), "flag": True}}, json.loads(row_1["_unmapped"]))
        self.assertEqual(h("jane2@example.com"), row_2["email"])
        counts = result["counts"]
        # Per row: two emails, two of the list, the unmapped email, the account number; a phone.
        self.assertEqual((14, 2, 2), (counts["masked"], counts["withheld"], counts["moved"]))
        self.assertEqual({(1, 0)}, {protection_label_of(item.path) for item in live_data_files(self.table.load())})
        # Rewritten once: nothing is left to do, however often it is asked.
        nothing, backlog = self.reprotect()
        self.assertIsNone(nothing)
        self.assertTrue(backlog.empty)

    def test_a_family_collapsed_to_json_has_its_text_walked_as_a_record(self) -> None:
        # `profile` forked for each new type until a list of strings collapsed it: its values are
        # JSON text from then on.
        for value in (7, "text", {"phone": "x"}, [1], ["s"]):
            self.table.add_fields({"profile": value})
        (collapsed,) = [member for member in self.table.index().members.values() if member.json and member.logical_path == ("profile",)]
        self.table.edit(ProtectionPolicy({"email": "hash"}), revision=1)
        # Under revision 1 the writers hashed the email inside it and left the phone number plain.
        self.table.write({"order_id": "O-1", "profile": {"phone": "540-948-0001", "email": h("p1@example.com")}}, label=(1,))
        self.table.edit(ProtectionPolicy({"email": "hash", "phone": "partial"}), revision=2)
        (_, result, _), _ = self.reprotect()
        (row,) = self.rows()
        self.assertEqual({"phone": "****0001", "email": h("p1@example.com")}, json.loads(row[collapsed.name]))
        self.assertEqual(1, result["counts"]["masked"])

    def test_the_current_snapshot_keeps_no_trace_of_the_files_it_replaced(self) -> None:
        self.table.write(plain_record(1), label=(0,))
        self.table.edit(ProtectionPolicy({"email": "hash"}), revision=1)
        replaced = [item.path for item in live_data_files(self.table.load())]
        before = committed_offsets(self.table.load().metadata)
        (_, _, snapshot_id), _ = self.reprotect()
        table = self.table.load()
        self.assertEqual(snapshot_id, table.metadata.current_snapshot_id)
        after = committed_offsets(table.metadata)
        self.assertEqual((before.offsets, before.event_times), (after.offsets, after.event_times), "the runtime's accounting is carried forward")
        listed = set()
        for manifest in table.current_snapshot().manifests(table.io):
            if manifest.content == ManifestContent.DATA:
                listed.update(entry.data_file.file_path for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False))
        self.assertFalse(set(replaced) & listed, "a deleted entry carries the replaced file's plain-text bounds")
        summary = dict(table.current_snapshot().summary.additional_properties)
        self.assertEqual(("reprotect", "1"), (summary["materializer.operation"], summary["materializer.protection-revision"]))

    def test_values_masked_under_an_earlier_revision_are_left_as_they_are(self) -> None:
        self.table.edit(ProtectionPolicy({"email": "hash"}), revision=1)
        masked = {**plain_record(1), "email": h("jane1@example.com"), "contact": {"email": h("c1@example.com"), "tier": "gold"}}
        masked["emails"] = [h("a1@example.com"), None, h("b1@example.com")]
        masked["extra"] = {"email": h("u1@example.com"), "flag": True}
        self.table.write(masked, label=(1,))
        self.table.edit(ProtectionPolicy({"email": "partial", "phone": "hash"}), revision=2)
        (_, result, _), _ = self.reprotect()
        (row,) = self.rows()
        self.assertEqual(h("jane1@example.com"), row["email"], "a hash cannot become a partial: there is no plain text")
        self.assertEqual(h("540-948-0001"), row["phone"])
        self.assertEqual([[2, 1]], [output["protection_label"] for output in result["outputs"]])

    def test_a_purge_expires_the_older_snapshots_and_deletes_what_only_they_referenced(self) -> None:
        self.table.write(plain_record(1), label=(0,))
        self.table.write(plain_record(2), label=(0,))
        self.table.edit(ProtectionPolicy({"email": "hash"}), revision=1)
        replaced = [item.path for item in live_data_files(self.table.load())]
        (_, _, snapshot_id), _ = self.reprotect()
        store = self.table.lake.store
        result = purge_before(
            self.table.load,
            snapshot_id,
            store=store,
            now=datetime.now(tz=UTC),
            orphan_min_age_hours=24,
            delete=store.delete,
        )
        table = self.table.load()
        self.assertEqual([snapshot_id], [item.snapshot_id for item in table.metadata.snapshots])
        self.assertEqual(2, result.expired)
        for path in replaced:
            self.assertNotIn(path, store.names(), f"{path} is still in the bucket")
        self.assertGreaterEqual(result.deleted, len(replaced))
        self.assertEqual(0, result.failed)
        self.assertEqual([h("jane1@example.com"), h("jane2@example.com")], [row["email"] for row in self.rows()])

    def test_an_output_whose_first_upload_landed_before_its_retry_was_refused_counts(self) -> None:
        # The store's library lost the answer to a create-only upload and sent it again: the first
        # landed, so the retry is refused. Only this job's own write can be at an output's fresh name.
        self.table.write(plain_record(1), label=(0,))
        self.table.write(plain_record(2), label=(0,))
        self.table.edit(ProtectionPolicy({"email": "hash"}), revision=1)
        store = self.table.lake.store
        upload = store.put_file
        refused: list[str] = []

        def landed_then_refused(uri, path, **kwargs):
            upload(uri, path, **kwargs)
            refused.append(uri)
            raise PreconditionFailedError(f"{uri}: the object already exists", uri=uri)

        with patch.object(store, "put_file", landed_then_refused):
            (_, result, snapshot_id), _ = self.reprotect()
        self.assertEqual([output["uri"] for output in result["outputs"]], refused)
        self.assertEqual(2, len(refused))
        self.assertEqual(snapshot_id, self.table.load().metadata.current_snapshot_id)
        self.assertEqual([h("jane1@example.com"), h("jane2@example.com")], [row["email"] for row in self.rows()])

    def test_an_output_name_that_holds_another_object_fails_the_job(self) -> None:
        self.table.write(plain_record(1), label=(0,))
        self.table.edit(ProtectionPolicy({"email": "hash"}), revision=1)
        store = self.table.lake.store
        for name, stage, refusal in (
            ("another object", lambda uri: store.put(uri, b"not this output"), "a different object already exists at .*: 15 bytes, not this output's"),
            ("nothing", lambda uri: None, "was refused, yet no object is there"),
        ):
            with self.subTest(found=name):

                def refused(uri, path, **kwargs):
                    stage(uri)
                    raise PreconditionFailedError(f"{uri}: the object already exists", uri=uri)

                with patch.object(store, "put_file", refused):
                    with self.assertRaisesRegex(RuntimeError, refusal):
                        self.reprotect()
        self.assertEqual(["jane1@example.com"], [row["email"] for row in self.rows()], "nothing was committed")

    def test_a_key_other_than_the_tables_is_refused(self) -> None:
        self.table.write(plain_record(1), label=(0,))
        self.table.edit(ProtectionPolicy({"email": "hash"}), revision=1)
        with self.assertRaisesRegex(ValueError, "protection_key_mismatch"):
            self.reprotect(key="another-key-" + "x" * 30)


if __name__ == "__main__":
    unittest.main()
