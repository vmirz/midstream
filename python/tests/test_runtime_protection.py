"""Data protection in a runtime: the writer masks before staging, the committer records what is
protected, a delivered policy takes effect without a restart, and files say which revision they hold
(design §5.4–§5.6)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from runtime_support import PROTECTION_KEY, RuntimeHarness, run  # noqa: E402

from midstream.lakehouse.commits import protection_json_of  # noqa: E402
from midstream.lakehouse.files import protection_revision_of  # noqa: E402
from midstream.protection.mask import hash_text  # noqa: E402
from midstream.protection.policy import ProtectionPolicy, TableProtection  # noqa: E402
from midstream.protection.secrets import protection_key  # noqa: E402
from midstream.runtime_app.handlers.runtime_settings import handle_control_message  # noqa: E402
from midstream.runtime_app.protection_state import (  # noqa: E402
    PROTECTION_POLICY_KEY,
    current_key_problem,
    read_delivered_policy,
)
from midstream.runtime_app.state import get_partition_state, build_runtime_processing_summary  # noqa: E402
from midstream.runtime_app.table_state import SchemaRecord, current_view, schema_key  # noqa: E402

DEFAULT = ProtectionPolicy({"email": "hash", "phone": "hash", "national_id": "hash", "secret": "redact"})
SEED = {"order_id": "O-0", "email": "seed@example.com", "phone": "555-0000", "ssn": 123456789, "password": "pw"}
KEY = PROTECTION_KEY.encode()


def h(value) -> str:
    return hash_text(KEY, value if isinstance(value, str) else json.dumps(value))


def table_protection(runtime: RuntimeHarness) -> TableProtection:
    return TableProtection.from_json(protection_json_of(runtime.table().metadata))


def deliver_policy(runtime: RuntimeHarness, policy: ProtectionPolicy, revision: int, *, reason: str | None = None) -> None:
    """What the settings agent does with the control plane's `protection_policy` message."""
    handle_control_message(
        {
            "kind": "protection_policy",
            "materialization_id": "mat-a1b2c3d4e5f6",
            "revision": revision,
            "policy": policy.to_dict(),
            "changed_by": "owner@example.com",
            "published_at": "2026-10-29T10:00:00Z",
            "reason": reason,
        },
        runtime.state_table,
        materialization_id="mat-a1b2c3d4e5f6",
    )


class WriterMaskingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeHarness(SEED, protection_policy=DEFAULT)
        self.addCleanup(self.runtime.close)

    def staged_lines(self) -> list[dict]:
        batch = get_partition_state("orders", 0).active_batch
        return [json.loads(line) for line in Path(batch.local_path).read_text(encoding="utf-8").splitlines()]

    def test_protected_values_are_masked_before_they_are_staged(self) -> None:
        run(self.runtime.write({"order_id": "O-1", "email": "jane@example.com", "phone": 5551234567, "ssn": 987654321, "password": "hunter2"}, offset=0))
        (line,) = self.staged_lines()
        self.assertEqual(
            {"order_id": "O-1", "email": h("jane@example.com"), "phone": h(5551234567), "ssn": h(987654321)},
            line["v"],
        )
        staged_text = Path(get_partition_state("orders", 0).active_batch.local_path).read_text(encoding="utf-8")
        for plaintext in ("jane@example.com", "5551234567", "987654321", "hunter2"):
            self.assertNotIn(plaintext, staged_text)

    def test_the_new_table_records_its_protected_fields(self) -> None:
        protection = table_protection(self.runtime)
        self.assertEqual(1, protection.revision)
        self.assertEqual({("email",), ("phone",), ("ssn",)}, {item.path for item in protection.active_fields().values()})
        self.assertNotIn("password", {field.name for field in self.runtime.table().schema().fields})

    def test_the_batch_and_its_files_carry_the_tables_revision(self) -> None:
        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}", "email": f"u{offset}@example.com"}, offset=offset)
            return await self.runtime.cycle()

        events = run(scenario())
        self.assertIn("table_committed", [event["kind"] for event in events])
        files = [task.file.file_path for task in self.runtime.table().scan().plan_files()]
        self.assertTrue(files)
        self.assertEqual({1}, {protection_revision_of(path) for path in files})
        rows = self.runtime.rows()
        self.assertEqual([h(f"u{offset}@example.com") for offset in range(3)], [row["email"] for row in rows])

    def test_a_new_protected_key_is_masked_from_its_first_value_and_recorded(self) -> None:
        async def scenario():
            await self.runtime.write({"order_id": "O-1", "contact_email": "c1@example.com"}, offset=0)
            await self.runtime.deliver(await self.runtime.pipeline(upload=False))
            self.runtime.close_schema_window()
            await self.runtime.deliver([])
            for offset in (1, 2):
                await self.runtime.write({"order_id": f"O-{offset}", "contact_email": f"c{offset + 1}@example.com"}, offset=offset)
            return await self.runtime.cycle()

        run(scenario())
        protection = table_protection(self.runtime)
        contact = [item for item in protection.active_fields().values() if item.path == ("contact_email",)]
        self.assertEqual(1, len(contact))
        self.assertEqual(("email", "hash"), (contact[0].kind, contact[0].mask))
        self.assertGreaterEqual(contact[0].since, 1)
        values = {row.get("contact_email") for row in self.runtime.rows()}
        self.assertEqual({h("c1@example.com"), h("c2@example.com"), h("c3@example.com")}, values)

    def test_the_heartbeat_counts_masked_and_withheld_values(self) -> None:
        run(self.runtime.write({"email": "a@example.com", "phone": "1", "password": "x", "order_id": "O"}, offset=0))
        summary = build_runtime_processing_summary("mat-a1b2c3d4e5f6")
        counters = summary.table_counters["orders"]
        self.assertEqual(2, counters["protected_values"])
        self.assertEqual(1, counters["withheld_values"])

    def test_a_table_without_protection_stages_records_as_they_arrived(self) -> None:
        plain = RuntimeHarness({"order_id": "O-0", "email": "seed@example.com"}, topics=("payments",))
        try:
            run(plain.write({"order_id": "O-1", "email": "jane@example.com"}, offset=0, topic="payments"))
            batch = get_partition_state("payments", 0).active_batch
            (line,) = [json.loads(item) for item in Path(batch.local_path).read_text(encoding="utf-8").splitlines()]
            self.assertEqual("jane@example.com", line["v"]["email"])
            self.assertEqual(0, batch.protection_revision)
            self.assertIsNone(protection_json_of(plain.table().metadata))
        finally:
            plain.close()


class MissingKeyTests(unittest.TestCase):
    def test_a_member_without_the_key_writes_nothing_and_says_why(self) -> None:
        runtime = RuntimeHarness(SEED, protection_policy=DEFAULT, protection_key=None)
        self.addCleanup(runtime.close)

        async def scenario():
            message = runtime.message(partition=0, offset=0)
            task = asyncio.ensure_future(runtime.write_message({"order_id": "O-1", "email": "jane@example.com"}, message))
            await asyncio.sleep(0.05)
            done = task.done()
            task.cancel()
            try:
                await task
            except BaseException:  # noqa: BLE001 - cancelled while it waits
                pass
            return done, message

        with self.assertLogs("midstream.runtime_app.handlers.writer", level="WARNING"):
            done, message = run(scenario())
        self.assertFalse(done)
        self.assertFalse(message.acked)
        self.assertEqual("protection_key_missing", current_key_problem())

    def test_a_member_with_another_key_than_expected_writes_nothing(self) -> None:
        runtime = RuntimeHarness(SEED, protection_policy=DEFAULT, expected_key_id="deadbeef")
        self.addCleanup(runtime.close)

        async def scenario():
            task = asyncio.ensure_future(runtime.write({"order_id": "O-1", "email": "jane@example.com"}, offset=0))
            await asyncio.sleep(0.05)
            done = task.done()
            task.cancel()
            try:
                await task
            except BaseException:  # noqa: BLE001
                pass
            return done

        with self.assertLogs("midstream.runtime_app.handlers.writer", level="WARNING"):
            self.assertFalse(run(scenario()))
        self.assertEqual("protection_key_mismatch", current_key_problem())

    def test_a_table_that_hashes_nothing_needs_no_key(self) -> None:
        runtime = RuntimeHarness(
            SEED, protection_policy=ProtectionPolicy({"phone": "partial", "secret": "redact"}), protection_key=None
        )
        self.addCleanup(runtime.close)
        run(runtime.write({"order_id": "O-1", "phone": "540-948-9848", "password": "x"}, offset=0))
        batch = get_partition_state("orders", 0).active_batch
        (line,) = [json.loads(item) for item in Path(batch.local_path).read_text(encoding="utf-8").splitlines()]
        self.assertEqual({"order_id": "O-1", "phone": "****9848"}, line["v"])

    def test_the_expected_key_hashes(self) -> None:
        runtime = RuntimeHarness(SEED, protection_policy=DEFAULT, expected_key_id=protection_key(PROTECTION_KEY).key_id)
        self.addCleanup(runtime.close)
        run(runtime.write({"order_id": "O-1", "email": "jane@example.com"}, offset=0))
        self.assertIsNone(current_key_problem())


class PolicyDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeHarness(SEED, protection_policy=DEFAULT)
        self.addCleanup(self.runtime.close)

    def test_a_delivered_policy_is_stored_once_and_only_when_newer(self) -> None:
        deliver_policy(self.runtime, ProtectionPolicy({"email": "partial"}), 2)
        deliver_policy(self.runtime, DEFAULT, 1)
        delivered = read_delivered_policy(self.runtime.state_table)
        self.assertEqual(2, delivered.revision)
        self.assertEqual({"email": "partial"}, dict(delivered.policy.kinds))
        self.assertEqual("owner@example.com", delivered.changed_by)

    def test_a_policy_for_another_materialization_is_ignored(self) -> None:
        handle_control_message(
            {"kind": "protection_policy", "materialization_id": "mat-other", "revision": 5, "policy": {"kinds": {}}},
            self.runtime.state_table,
            materialization_id="mat-a1b2c3d4e5f6",
        )
        self.assertNotIn(PROTECTION_POLICY_KEY, self.runtime.state_table.values)

    def test_a_malformed_policy_is_ignored(self) -> None:
        with self.assertLogs("midstream.runtime_app.protection_state", level="WARNING"):
            for value in ({"kind": "protection_policy", "revision": "x"}, {"kind": "protection_policy", "revision": -1}):
                handle_control_message(value, self.runtime.state_table, materialization_id="mat-a1b2c3d4e5f6")
        self.assertNotIn(PROTECTION_POLICY_KEY, self.runtime.state_table.values)

    def test_the_committer_records_a_newer_policy_and_the_writers_follow_without_a_restart(self) -> None:
        async def scenario():
            for offset in range(2):
                await self.runtime.write({"order_id": f"O-{offset}", "email": f"u{offset}@example.com", "phone": "540-948-9848"}, offset=offset)
            before = get_partition_state("orders", 0).active_batch
            deliver_policy(self.runtime, ProtectionPolicy({"phone": "partial", "secret": "redact"}), 2)
            events = await self.runtime.deliver([])
            await self.runtime.write({"order_id": "O-2", "email": "u2@example.com", "phone": "540-948-9848"}, offset=2)
            after = get_partition_state("orders", 0).active_batch
            for offset in (3, 4):
                await self.runtime.write({"order_id": f"O-{offset}", "email": f"u{offset}@example.com", "phone": "540-948-9848"}, offset=offset)
            committed = await self.runtime.cycle()
            committed += await self.runtime.cycle()
            return before, events, after, committed

        before, events, after, _ = run(scenario())
        protection = table_protection(self.runtime)
        self.assertEqual(2, protection.revision)
        self.assertEqual({"phone": "partial", "secret": "redact"}, dict(protection.policy.kinds))
        fields = {item.path: item for item in protection.fields.values()}
        self.assertFalse(fields[("email",)].active)
        self.assertEqual("partial", fields[("phone",)].mask)
        self.assertEqual([1, 2], [change.revision for change in protection.history])
        self.assertEqual("owner@example.com", protection.history[-1].by)
        (changed,) = [event for event in events if event["kind"] == "schema_changed"]
        self.assertEqual([], changed["payload"]["changes"])
        self.assertEqual(2, changed["payload"]["protection"]["revision"])
        record = SchemaRecord.from_dict(self.runtime.state_table.get(schema_key("orders")))
        self.assertEqual(2, record.protection_revision)
        # The open batch of revision 1 was rolled; the new record began a batch of revision 2.
        self.assertEqual(1, before.protection_revision)
        self.assertEqual(2, after.protection_revision)
        self.assertNotEqual(before.batch_id, after.batch_id)
        rows = self.runtime.rows()
        self.assertEqual(
            [h("u0@example.com"), h("u1@example.com"), "u2@example.com", "u3@example.com", "u4@example.com"],
            [row["email"] for row in rows],
        )
        self.assertEqual([h("540-948-9848")] * 2 + ["****9848"] * 3, [row["phone"] for row in rows])
        revisions = sorted(protection_revision_of(task.file.file_path) for task in self.runtime.table().scan().plan_files())
        self.assertEqual({1, 2}, set(revisions))

    def test_recording_the_same_revision_again_changes_nothing(self) -> None:
        async def scenario():
            deliver_policy(self.runtime, DEFAULT, 1)
            return await self.runtime.deliver([])

        events = run(scenario())
        self.assertEqual([], [event for event in events if event["kind"] == "schema_changed"])
        self.assertEqual(1, table_protection(self.runtime).revision)

    def test_applying_the_rules_records_the_policy_again(self) -> None:
        async def scenario():
            deliver_policy(self.runtime, DEFAULT, 2, reason="apply_rules")
            return await self.runtime.deliver([])

        run(scenario())
        protection = table_protection(self.runtime)
        self.assertEqual(2, protection.revision)
        self.assertEqual("apply_rules", protection.history[-1].reason)


class HeartbeatTests(unittest.TestCase):
    def test_a_member_reports_its_protection_revision_and_key(self) -> None:
        from midstream.runtime_app.handlers.status import build_runtime_status_snapshot

        runtime = RuntimeHarness(SEED, protection_policy=DEFAULT, protection_revision=4)
        self.addCleanup(runtime.close)
        run(runtime.write({"order_id": "O-1", "email": "a@example.com"}, offset=0))
        snapshot = build_runtime_status_snapshot(runtime.state_table, materialization_id="mat-a1b2c3d4e5f6", tables=("orders",))
        self.assertEqual({"orders": 4}, snapshot["protection_revisions"])
        self.assertEqual(protection_key(PROTECTION_KEY).key_id, snapshot["protection_key_id"])
        self.assertEqual(1, snapshot["table_counters"]["orders"]["protected_values"])
        self.assertEqual("running", snapshot["phase"])

    def test_a_member_that_may_not_hash_reports_itself_degraded(self) -> None:
        from midstream.runtime_app.handlers.status import build_runtime_status_snapshot
        from midstream.runtime_app.protection_state import note_key_problem

        runtime = RuntimeHarness(SEED, protection_policy=DEFAULT, protection_key=None)
        self.addCleanup(runtime.close)
        note_key_problem("protection_key_missing")
        snapshot = build_runtime_status_snapshot(runtime.state_table, materialization_id="mat-a1b2c3d4e5f6", tables=("orders",))
        self.assertEqual(("degraded", "degraded", "protection_key_missing"), (snapshot["phase"], snapshot["health"], snapshot["health_reason"]))
        self.assertIn("protection-key", snapshot["last_error"])
        self.assertNotIn("protection_key_id", snapshot)

    def test_a_runtime_without_protection_reports_none(self) -> None:
        from midstream.runtime_app.handlers.status import build_runtime_status_snapshot

        runtime = RuntimeHarness({"order_id": "O-0"}, protection_key=None)
        self.addCleanup(runtime.close)
        run(runtime.write({"order_id": "O-1"}, offset=0))
        snapshot = build_runtime_status_snapshot(runtime.state_table, materialization_id="mat-a1b2c3d4e5f6", tables=("orders",))
        self.assertNotIn("protection_revisions", snapshot)
        self.assertNotIn("protected_values", snapshot["table_counters"]["orders"])

    def test_the_control_plane_keeps_monotonic_totals_per_table(self) -> None:
        from midstream.models.status import RuntimeProgress
        from midstream.observation.progress import merge_member_progress

        def beat(progress, protected, withheld, member="m-0"):
            return merge_member_progress(
                progress,
                member_id=member,
                rows_written=1,
                bytes_written=1,
                files_written=0,
                records_skipped=0,
                latest_offsets={},
                table_protected={"orders": protected},
                table_withheld={"orders": withheld},
                protection_revisions={"orders": 2},
                protection_key_id="abcd1234",
            )

        progress = beat(RuntimeProgress(), 5, 1)
        progress = beat(progress, 8, 2)
        progress = beat(progress, 3, 0)  # restarted: its counters began again
        self.assertEqual({"orders": 11}, progress.cumulative_protected_by_table)
        self.assertEqual({"orders": 2}, progress.cumulative_withheld_by_table)
        again = RuntimeProgress.from_dict(progress.to_dict())
        self.assertEqual(progress.cumulative_protected_by_table, again.cumulative_protected_by_table)
        self.assertEqual({"orders": 2}, again.member_progress["m-0"].protection_revisions)
        self.assertEqual("abcd1234", again.member_progress["m-0"].protection_key_id)


class SchemaViewTests(unittest.TestCase):
    def test_a_newer_protection_revision_of_the_same_schema_replaces_the_view(self) -> None:
        runtime = RuntimeHarness(SEED, protection_policy=DEFAULT)
        self.addCleanup(runtime.close)
        record = SchemaRecord.from_table(runtime.table())
        runtime.state_table[schema_key("orders")] = record.to_dict()
        first = current_view(runtime.state_table, "orders")
        self.assertEqual(1, first.plan.revision)
        newer = dict(record.to_dict())
        newer["protection"] = dict(newer["protection"], revision=2)
        runtime.state_table[schema_key("orders")] = newer
        second = current_view(runtime.state_table, "orders")
        self.assertIsNot(first, second)
        self.assertEqual(2, second.plan.revision)
        self.assertIs(second, current_view(runtime.state_table, "orders"))


if __name__ == "__main__":
    unittest.main()
