"""The committer's event handling: what it commits, defers, refuses and reports (spec §7.6, §7.8)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import BASE_TIME, LOCATION, MATERIALIZATION_ID  # noqa: E402
from runtime_support import RuntimeHarness, run  # noqa: E402

from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.store import StoreError  # noqa: E402
from midstream.runtime_app.table_state import file_decisions_key, schema_key  # noqa: E402


class CommitterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeHarness({"order_id": "O-0"})
        self.addCleanup(self.runtime.close)
        self.lake = self.runtime.lake

    def seal(self, *, after: int, offsets: list[int], partition: int = 0, member: str = "member-1", generation: int = 1, epoch: str | None = None, **kwargs) -> list[dict]:
        written = self.lake.write_seal(partition=partition, after_offset=after, offsets=offsets, member=member, generation=generation, epoch=epoch, **kwargs)
        return [item.to_dict() for item in written]

    def offsets(self) -> dict[int, int]:
        return committed_offsets(self.lake.load().metadata).offsets

    def decisions(self) -> dict[str, dict]:
        return dict(self.runtime.state_table.get(file_decisions_key("orders")) or {})

    def test_a_seal_after_a_gap_waits_for_the_seal_before_it(self) -> None:
        later = self.seal(after=1, offsets=[2, 3])
        earlier = self.seal(after=-1, offsets=[0, 1])

        async def scenario():
            deferred = await self.runtime.deliver(later)
            offsets_after_gap = self.offsets()
            self.runtime.allow_commit()
            both = await self.runtime.deliver(earlier)
            return deferred, offsets_after_gap, both

        deferred, offsets_after_gap, both = run(scenario())
        self.assertEqual([], [event for event in deferred if event["kind"] == "table_committed"])
        self.assertEqual({}, offsets_after_gap)
        (committed,) = [event for event in both if event["kind"] == "table_committed"]
        self.assertEqual(4, committed["payload"]["records_added"])
        self.assertEqual({0: 3}, self.offsets())

    def test_an_incomplete_seal_the_table_already_covers_is_forgotten(self) -> None:
        # A member that lost its partition after announcing only some of a seal's files leaves that
        # seal incomplete for good, while the partition's new owner commits the same records.
        two_days = [BASE_TIME, BASE_TIME + timedelta(days=1)]
        partial = self.seal(after=-1, offsets=[0, 1], times=two_days)[:1]
        whole = self.seal(after=-1, offsets=[0, 1], member="member-2")

        async def scenario():
            await self.runtime.deliver(partial)
            waiting = len(self.runtime.committer.pending)
            self.runtime.allow_commit()
            await self.runtime.deliver(whole)
            return waiting, len(self.runtime.committer.pending)

        waiting, left = run(scenario())
        self.assertEqual(1, waiting)
        self.assertEqual(0, left)
        self.assertEqual({0: 1}, self.offsets())
        # Its file is not the table's, but it is left to orphan cleanup like any covered file.
        self.assertIsNotNone(self.lake.store.read(partial[0]["file_path"]))

    def test_an_incomplete_seal_announced_after_its_records_are_committed_is_forgotten(self) -> None:
        two_days = [BASE_TIME, BASE_TIME + timedelta(days=1)]
        whole = self.seal(after=-1, offsets=[0, 1])
        partial = self.seal(after=-1, offsets=[0, 1], member="member-2", times=two_days)[:1]

        async def scenario():
            await self.runtime.deliver(whole)
            self.runtime.allow_commit()
            await self.runtime.deliver(partial)
            return len(self.runtime.committer.pending)

        self.assertEqual(0, run(scenario()))
        self.assertEqual({0: 1}, self.offsets())

    def test_a_seal_the_table_already_holds_is_refused_and_left_to_orphan_cleanup(self) -> None:
        first = self.seal(after=-1, offsets=[0, 1])
        again = self.seal(after=-1, offsets=[0, 1], member="member-2")

        async def scenario():
            await self.runtime.deliver(first)
            self.runtime.allow_commit()
            return await self.runtime.deliver(again)

        events = run(scenario())
        self.assertEqual([], [event for event in events if event["kind"] == "table_committed"])
        # The duplicate is not the table's, but the committer cannot tell it from one that is.
        self.assertIsNotNone(self.lake.store.read(again[0]["file_path"]))
        self.assertEqual({0: 1}, self.offsets())
        self.assertEqual(2, len(self.lake.load().scan().to_arrow()))

    def test_a_committed_seal_announced_again_keeps_its_file(self) -> None:
        # A member announces a file again until it sees the commit that took it, and a new committer
        # hears every file a member still holds: the same file arrives once it is the table's own.
        seal = self.seal(after=-1, offsets=[0, 1])

        async def scenario():
            committed = await self.runtime.deliver(seal)
            self.runtime.allow_commit()
            again = await self.runtime.deliver(seal)
            return committed, again

        committed, again = run(scenario())
        self.assertEqual(["table_committed"], [event["kind"] for event in committed if event["kind"] == "table_committed"])
        self.assertEqual([], [event for event in again if event["kind"] == "table_committed"])
        self.assertIsNotNone(self.lake.store.read(seal[0]["file_path"]))
        self.assertEqual(2, len(self.lake.load().scan().to_arrow()))
        self.assertEqual({0: 1}, self.offsets())

    def test_an_overlapping_seals_files_are_deleted(self) -> None:
        first = self.seal(after=-1, offsets=[0, 1])
        overlapping = self.seal(after=0, offsets=[1, 2], member="member-2")

        async def scenario():
            await self.runtime.deliver(first)
            self.runtime.allow_commit()
            return await self.runtime.deliver(overlapping)

        run(scenario())
        self.assertIsNone(self.lake.store.read(overlapping[0]["file_path"]))
        self.assertIsNotNone(self.lake.store.read(first[0]["file_path"]))
        (decision,) = self.decisions().values()
        self.assertEqual(("overlap", 1), (decision["decision"], decision["covered_through"]))
        self.assertEqual(2, len(self.lake.load().scan().to_arrow()))

    def test_seals_of_several_partitions_commit_together(self) -> None:
        async def scenario():
            await self.runtime.deliver([], tick=True)
            self.runtime.committer.last_commit_at = time.monotonic()
            await self.runtime.deliver(
                self.seal(after=-1, offsets=[0, 1], partition=0) + self.seal(after=-1, offsets=[0, 1, 2], partition=3),
                tick=False,
            )
            self.runtime.allow_commit()
            return await self.runtime.deliver([], tick=True)

        (committed,) = [event for event in run(scenario()) if event["kind"] == "table_committed"]
        self.assertEqual({"0": 1, "3": 2}, committed["payload"]["offsets"])
        self.assertEqual({0: 1, 3: 2}, self.offsets())

    def test_an_announced_file_that_is_not_there_is_decided_an_error(self) -> None:
        missing = self.seal(after=-1, offsets=[0, 1])
        self.lake.store.objects.pop(missing[0]["file_path"])

        async def scenario():
            return await self.runtime.deliver(missing)

        events = run(scenario())
        self.assertEqual([], [event for event in events if event["kind"] == "table_committed"])
        (decision,) = self.decisions().values()
        self.assertEqual("error", decision["decision"])
        self.assertIn("missing", decision["reason"])
        self.assertEqual({}, self.offsets())

    def test_a_failing_commit_reports_once_backs_off_and_keeps_the_files_pending(self) -> None:
        pending = self.seal(after=-1, offsets=[0, 1])

        def outage(operation, uri):
            if operation == "put" and uri.endswith(".metadata.json"):
                return StoreError(f"{uri}: the store is down", uri=uri, status=503, code="ServiceUnavailable")
            return None

        async def scenario():
            await self.runtime.deliver([], tick=True)
            self.lake.store.fail = outage
            failed = await self.runtime.deliver(pending)
            backoff = self.runtime.committer.next_attempt_at - time.monotonic()
            still_pending = len(self.runtime.committer.pending)
            self.runtime.allow_commit()
            again = await self.runtime.deliver([], tick=True)
            self.lake.store.fail = None
            self.runtime.allow_commit()
            recovered = await self.runtime.deliver([], tick=True)
            return failed, backoff, still_pending, again, recovered

        failed, backoff, still_pending, again, recovered = run(scenario())
        self.assertEqual(["commit_failed"], [event["payload"]["reason"] for event in failed if event["kind"] == "table_error"])
        # The store's own failure, the metadata file's write, is what failed the commit.
        (error,) = [event["payload"] for event in failed if event["kind"] == "table_error"]
        self.assertIn("the store is down", error["message"])
        self.assertGreater(backoff, 0)
        self.assertEqual(1, still_pending)
        # The same error is reported again only after a while.
        self.assertEqual([], [event for event in again if event["kind"] == "table_error"])
        self.assertEqual(["table_committed"], [event["kind"] for event in recovered])
        self.assertEqual({0: 1}, self.offsets())

    def test_announcements_from_outside_the_table_or_malformed_are_ignored(self) -> None:
        stray = self.seal(after=-1, offsets=[0, 1])
        stray[0]["file_path"] = "gs://elsewhere/data/x.parquet"

        async def scenario():
            await self.runtime.deliver([{"kind": "data_file_ready", "file_path": "gs://x"}] + stray)
            return len(self.runtime.committer.pending)

        self.assertEqual(0, run(scenario()))

    def test_an_older_session_of_a_member_is_superseded_by_its_newer_one(self) -> None:
        old = self.seal(after=-1, offsets=[0, 1], generation=1)
        new = self.seal(after=-1, offsets=[0, 1, 2], generation=2)

        async def scenario():
            await self.runtime.deliver([], tick=True)
            self.runtime.committer.last_commit_at = time.monotonic()
            await self.runtime.deliver(new + old, tick=False)
            return [seal.session_generation for seal in self.runtime.committer.pending.seals()]

        self.assertEqual([2], run(scenario()))

    def test_a_restarted_member_is_committed_though_its_generation_starts_again(self) -> None:
        """The member restarted under its pod's name, its rebalance count back at 1, while its committer
        ran on. Judged by generation alone, each of its new seals was older than its last process's, and
        none was ever committed (`commits_stalled` on develop, 2026-10-22)."""
        before = self.seal(after=-1, offsets=[0, 1], generation=3, epoch="process-a")
        after = self.seal(after=1, offsets=[2, 3], generation=1, epoch="process-b")

        async def scenario():
            await self.runtime.deliver(before)
            self.runtime.allow_commit()
            return await self.runtime.deliver(after)

        events = run(scenario())
        self.assertEqual(["table_committed"], [event["kind"] for event in events if event["kind"] == "table_committed"])
        self.assertEqual({0: 3}, self.offsets())
        self.assertEqual(0, self.runtime.committer.stale_announcements)

    def test_an_announcement_of_an_older_session_is_logged_and_counted(self) -> None:
        newer = self.seal(after=-1, offsets=[0, 1], generation=2, epoch="process-a")
        older = self.seal(after=-1, offsets=[0, 1, 2], generation=1, epoch="process-a")

        async def scenario():
            await self.runtime.deliver([], tick=True)
            self.runtime.committer.last_commit_at = time.monotonic()
            await self.runtime.deliver(newer, tick=False)
            # Announced twice, as a member announces again what no commit has covered.
            with self.assertLogs("midstream.runtime_app.committer", level="WARNING") as logs:
                await self.runtime.deliver(older + older, tick=False)
            return logs.output, self.runtime.committer.table_block()

        output, block = run(scenario())
        self.assertEqual(1, len(output), "logged at most once a minute per member and partition")
        self.assertIn("older session", output[0])
        self.assertIn("member member-1, partition 0", output[0])
        self.assertIn("session 1 (process process-a), while the member's newest is session 2 (process process-a)", output[0])
        self.assertEqual(2, block["stale_announcements"])
        self.assertEqual(1, block["pending_files"])

    def test_new_fields_past_the_schema_limit_are_refused_and_reported(self) -> None:
        self.runtime.configure(max_schema_fields=50)
        observations = [{"path": [f"field_{index}"], "observed": "string"} for index in range(60)]

        async def scenario():
            await self.runtime.deliver(
                [{"kind": "schema_change_request", "materialization_id": MATERIALIZATION_ID, "observations": observations}],
                tick=False,
            )
            self.runtime.close_schema_window()
            return await self.runtime.deliver([], tick=True)

        events = run(scenario())
        errors = [event["payload"] for event in events if event["kind"] == "table_error"]
        self.assertEqual(["schema_field_limit"], [error["reason"] for error in errors])
        record = self.runtime.state_table[schema_key("orders")]
        self.assertTrue(record["rejected_paths"])
        schema = self.lake.load().schema()
        self.assertLessEqual(len(schema.fields), 50)

    def test_requests_for_refused_paths_are_dropped_before_the_window(self) -> None:
        self.runtime.configure(max_schema_fields=50)
        observations = [{"path": [f"field_{index}"], "observed": "string"} for index in range(60)]
        message = {"kind": "schema_change_request", "materialization_id": MATERIALIZATION_ID, "observations": observations}

        async def scenario():
            await self.runtime.deliver([message], tick=False)
            self.runtime.close_schema_window()
            await self.runtime.deliver([], tick=True)
            rejected = sorted(self.runtime.committer.rejected_paths)[0]
            await self.runtime.deliver(
                [{**message, "observations": [{"path": list(rejected), "observed": "string"}]}], tick=False
            )
            return self.runtime.committer.window

        self.assertIsNone(run(scenario()))

    def test_a_schema_change_is_published_with_its_projection(self) -> None:
        async def scenario():
            await self.runtime.deliver(
                [
                    {
                        "kind": "schema_change_request",
                        "materialization_id": MATERIALIZATION_ID,
                        "observations": [{"path": ["customer"], "observed": {"type": "struct", "fields": {"email": "string"}}}],
                    }
                ],
                tick=False,
            )
            self.runtime.close_schema_window()
            return await self.runtime.deliver([], tick=True)

        (changed,) = [event for event in run(scenario()) if event["kind"] == "schema_changed"]
        payload = changed["payload"]
        self.assertEqual(1, payload["schema_id"])
        names = {field["name"] for field in payload["fields"]}
        self.assertIn("customer.email", names)
        self.assertTrue(payload["metadata_location"].startswith(f"{LOCATION}/metadata/"))
        self.assertEqual(MATERIALIZATION_ID, changed["materialization_id"])


if __name__ == "__main__":
    unittest.main()


class RecreatedTableTests(unittest.TestCase):
    """A topic removed with its table dropped, then added back: a new table under the old name.

    The runtime's state table still holds what the dropped table left under that name. Taken for the
    new table's, its committed offsets skipped every record as already in the table, and released
    them unwritten (found on a test cluster: a re-added topic's 3,005 records, none in the new table).
    """

    def setUp(self) -> None:
        from midstream.runtime_app.table_state import SchemaRecord, committed_offsets_key, table_contexts

        self.runtime = RuntimeHarness({"order_id": "O-0"})
        self.addCleanup(self.runtime.close)
        table = self.runtime.table()
        self.uuid = str(table.metadata.table_uuid)
        spec = self.runtime.specs[0]
        # As the template passes the tables: with the UUID of the table the control plane created.
        self.runtime.tables = table_contexts(
            MATERIALIZATION_ID,
            [
                {
                    "name": spec.name,
                    "dataset": self.runtime.tables[0].namespace,
                    "source_topic": spec.source_topic,
                    "location": self.runtime.lake.location,
                    "table_uuid": self.uuid,
                }
            ],
            partition_granularity="day",
        )
        dropped = SchemaRecord.from_table(table)
        self.offsets_key = committed_offsets_key("orders")
        self.runtime.state_table[self.offsets_key] = {"snapshot_id": 1234, "offsets": {"0": 50}, "event_times": {}, "table_uuid": "dropped-table"}
        self.runtime.state_table[schema_key("orders")] = {**dropped.to_dict(), "schema_id": 7, "table_uuid": "dropped-table"}

    def test_the_new_table_gets_every_record_despite_what_the_dropped_one_left(self) -> None:
        async def scenario():
            # Two batches of three, each committed in a cycle of its own.
            for start in (0, 3):
                for offset in range(start, start + 3):
                    await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
                await self.runtime.cycle()
            return self.runtime.rows()

        rows = run(scenario())

        self.assertEqual(list(range(6)), [row["_kafka_offset"] for row in rows])
        stored = self.runtime.state_table[self.offsets_key]
        self.assertEqual((self.uuid, {"0": 5}), (stored["table_uuid"], stored["offsets"]))
        self.assertEqual(self.uuid, self.runtime.state_table[schema_key("orders")]["table_uuid"])

    def test_what_an_earlier_version_left_without_a_uuid_counts_as_another_tables(self) -> None:
        self.runtime.state_table[self.offsets_key] = {"snapshot_id": 1234, "offsets": {"0": 50}, "event_times": {}}

        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            await self.runtime.cycle()
            return self.runtime.rows()

        self.assertEqual(3, len(run(scenario())))
