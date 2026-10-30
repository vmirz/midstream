"""The committer's decisions and appends: exactly once by construction (spec §7.6, §7.8)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import BASE_TIME, LOCATION, LakeFixture  # noqa: E402

from pyiceberg.expressions import GreaterThanOrEqual  # noqa: E402
from pyiceberg.transforms import MonthTransform  # noqa: E402
from pyiceberg.types import TimestamptzType  # noqa: E402

from midstream.lakehouse.commits import (  # noqa: E402
    ACCEPTED,
    COVERED,
    OVERLAP,
    AnnouncedFile,
    PendingFiles,
    Seal,
    commit_append,
    plan_append,
    table_totals,
)
from midstream.lakehouse.files import (  # noqa: E402
    compacted_file_uri,
    data_file_uri,
    partition_record,
    partition_text,
    partition_text_of,
    partition_values,
    protection_label_of,
    protection_revision_of,
    read_parquet_footer,
    reprotected_file_uri,
)
from midstream.lakehouse.offsets import (  # noqa: E402
    OFFSETS_PROPERTY,
    VALID_THROUGH_PROPERTY,
    CommittedOffsets,
    committed_offsets,
    format_millis,
    parse_millis,
)

UNLIMITED = {"max_files": 10_000, "max_bytes": 1 << 50}


def seal(seal_id: str, partition: int, after: int, start: int, end: int, *, files: int = 1, member: str = "m", announced: int | None = None) -> Seal:
    result = Seal(
        seal_id=seal_id,
        member_id=member,
        batch_id=f"b-{seal_id}",
        partition=partition,
        after_offset=after,
        offset_start=start,
        offset_end=end,
        file_count=files,
    )
    for index in range(files if announced is None else announced):
        path = f"gs://lake/t/data/{seal_id}-{index}.parquet"
        result.files[path] = AnnouncedFile(
            file_path=path,
            materialization_id="mat",
            member_id=member,
            batch_id=result.batch_id,
            seal_id=seal_id,
            file_count=files,
            partition=partition,
            after_offset=after,
            offset_start=start,
            offset_end=end,
            file_size=100,
            record_count=end - start + 1,
            partition_value={"ts_day": "2026-10-18"},
            event_time_max="2026-10-18T10:00:00.000Z",
        )
    return result


def decisions_by_seal(plan) -> dict[str, str]:
    return {item.seal_id: item.decision for item in plan.decisions}


class PlanTests(unittest.TestCase):
    def test_a_seal_continuing_the_table_is_accepted(self) -> None:
        plan = plan_append([seal("a", 3, -1, 0, 99)], CommittedOffsets(), **UNLIMITED)
        self.assertEqual(decisions_by_seal(plan), {"a": ACCEPTED})
        self.assertEqual(plan.offsets.get(3), 99)

    def test_skipped_records_between_seals_are_no_gap(self) -> None:
        # 100-104 were tombstones: the second seal says it continues after 99.
        plan = plan_append([seal("a", 0, 99, 105, 199)], CommittedOffsets(offsets={0: 99}), **UNLIMITED)
        self.assertEqual(decisions_by_seal(plan), {"a": ACCEPTED})

    def test_covered_and_overlapping_seals_are_rejected(self) -> None:
        committed = CommittedOffsets(offsets={0: 199})
        plan = plan_append([seal("old", 0, 99, 100, 199), seal("mid", 0, 99, 100, 250)], committed, **UNLIMITED)
        self.assertEqual(decisions_by_seal(plan), {"old": COVERED, "mid": OVERLAP})
        overlap = next(item for item in plan.decisions if item.seal_id == "mid")
        self.assertEqual(overlap.covered_through, 199)
        self.assertEqual(plan.offsets.get(0), 199)

    def test_a_gap_is_never_jumped(self) -> None:
        """The seal before this one was announced to a committer that died: wait for it."""
        plan = plan_append([seal("later", 0, 149, 150, 199)], CommittedOffsets(offsets={0: 99}), **UNLIMITED)
        self.assertEqual(plan.decisions, ())
        self.assertEqual([item.seal_id for item in plan.deferred], ["later"])
        self.assertEqual(plan.offsets.get(0), 99)
        # Once the missing seal is re-announced, both go in, in order.
        plan = plan_append([seal("later", 0, 149, 150, 199), seal("earlier", 0, 99, 100, 149)], CommittedOffsets(offsets={0: 99}), **UNLIMITED)
        self.assertEqual(decisions_by_seal(plan), {"earlier": ACCEPTED, "later": ACCEPTED})
        self.assertEqual([item.seal_id for item in plan.accepted], ["earlier", "later"])
        self.assertEqual(plan.offsets.get(0), 199)

    def test_two_owners_after_a_rebalance_commit_each_record_once(self) -> None:
        # The old owner's seal and the new owner's overlap; whichever is chained first wins.
        old = seal("old", 0, 99, 100, 149, member="a")
        new = seal("new", 0, 99, 100, 199, member="b")
        plan = plan_append([old, new], CommittedOffsets(offsets={0: 99}), **UNLIMITED)
        self.assertEqual(decisions_by_seal(plan), {"new": ACCEPTED, "old": COVERED})
        self.assertEqual(plan.offsets.get(0), 199)

    def test_incomplete_seals_wait_for_their_other_files(self) -> None:
        plan = plan_append([seal("a", 0, -1, 0, 99, files=2, announced=1)], CommittedOffsets(), **UNLIMITED)
        self.assertEqual(plan.decisions, ())
        self.assertEqual(plan.accepted, ())

    def test_multi_file_seals_are_decided_as_a_unit(self) -> None:
        plan = plan_append([seal("a", 0, -1, 0, 99, files=3)], CommittedOffsets(), **UNLIMITED)
        self.assertEqual([item.decision for item in plan.decisions], [ACCEPTED] * 3)
        self.assertEqual(len(plan.files), 3)

    def test_budgets_defer_the_rest_to_the_next_cycle(self) -> None:
        seals = [seal("a", 0, -1, 0, 9), seal("b", 0, 9, 10, 19), seal("c", 1, -1, 0, 9)]
        plan = plan_append(seals, CommittedOffsets(), max_files=2, max_bytes=1 << 40)
        self.assertEqual(len(plan.accepted), 2)
        self.assertEqual(len(plan.deferred), 1)
        # A single seal larger than the budget still goes in alone, so nothing can wait forever.
        plan = plan_append([seal("big", 0, -1, 0, 9, files=5)], CommittedOffsets(), max_files=2, max_bytes=1)
        self.assertEqual(len(plan.accepted), 1)

    def test_partitions_are_independent(self) -> None:
        plan = plan_append([seal("p0", 0, -1, 0, 9), seal("p1", 1, 49, 50, 59)], CommittedOffsets(offsets={1: 30}), **UNLIMITED)
        self.assertEqual(decisions_by_seal(plan), {"p0": ACCEPTED})
        self.assertEqual(plan.offsets.offsets, {0: 9, 1: 30})


class PendingFilesTests(unittest.TestCase):
    def announced(self, seal_id: str, generation: int, *, member: str = "m", partition: int = 0, epoch: str | None = None) -> AnnouncedFile:
        base = next(iter(seal(seal_id, partition, -1, 0, 9, member=member).files.values()))
        return AnnouncedFile.from_dict({**base.to_dict(), "session_generation": generation, "session_epoch": epoch})

    def test_a_restarted_member_is_heard_whatever_its_generation(self) -> None:
        """A restarted member keeps its pod's name and counts its rebalances from the first again.

        Judged by generation alone, its every announcement was older than its last process's, and
        ignored for as long as the committer ran (`commits_stalled` on develop, 2026-10-22).
        """
        pending = PendingFiles()
        pending.add(self.announced("before", 3, epoch="process-a"))
        pending.add(self.announced("before-other-partition", 3, partition=1, epoch="process-a"))
        pending.add(self.announced("other-member", 5, member="x", epoch="process-x"))
        self.assertTrue(pending.add(self.announced("after", 1, epoch="process-b")))
        # The last process's seals of the partition go; its other partitions' and other members' stay.
        self.assertEqual(["after", "before-other-partition", "other-member"], sorted(item.seal_id for item in pending.seals()))
        self.assertEqual(("process-b", 1), pending.session_of("m", 0))
        # The new process's sessions are judged among themselves.
        self.assertTrue(pending.add(self.announced("after-rebalance", 2, epoch="process-b")))
        self.assertFalse(pending.add(self.announced("late", 1, epoch="process-b")))
        self.assertEqual(["after-rebalance", "before-other-partition", "other-member"], sorted(item.seal_id for item in pending.seals()))

    def test_within_one_process_an_older_session_is_stale_and_a_newer_one_supersedes(self) -> None:
        pending = PendingFiles()
        self.assertTrue(pending.add(self.announced("three", 3, epoch="process-a")))
        self.assertFalse(pending.add(self.announced("one", 1, epoch="process-a")))
        self.assertTrue(pending.add(self.announced("four", 4, epoch="process-a")))
        self.assertEqual(["four"], [item.seal_id for item in pending.seals()])

    def test_a_process_of_another_version_is_another_process(self) -> None:
        # An upgrade's restart: the last process predates epochs.
        pending = PendingFiles()
        pending.add(self.announced("old-version", 3))
        self.assertTrue(pending.add(self.announced("new-version", 1, epoch="process-b")))
        self.assertEqual(["new-version"], [item.seal_id for item in pending.seals()])
        # And a rollback's.
        self.assertTrue(pending.add(self.announced("rolled-back", 1)))
        self.assertEqual(["rolled-back"], [item.seal_id for item in pending.seals()])

    def test_announcements_of_an_older_session_are_stale(self) -> None:
        pending = PendingFiles()
        self.assertTrue(pending.add(self.announced("new", 5)))
        self.assertFalse(pending.add(self.announced("old", 4)))
        self.assertEqual([item.seal_id for item in pending.seals()], ["new"])

    def test_a_newer_session_drops_the_member_partitions_older_seals(self) -> None:
        pending = PendingFiles()
        pending.add(self.announced("old", 4))
        pending.add(self.announced("other-member", 1, member="x"))
        pending.add(self.announced("new", 5))
        self.assertEqual(sorted(item.seal_id for item in pending.seals()), ["new", "other-member"])

    def test_re_announcements_are_idempotent(self) -> None:
        pending = PendingFiles()
        pending.add(self.announced("a", 1))
        pending.add(self.announced("a", 1))
        self.assertEqual(len(pending), 1)

    def test_announcement_round_trips(self) -> None:
        for item in (self.announced("a", 3), self.announced("b", 3, epoch="process-a")):
            self.assertEqual(AnnouncedFile.from_dict(item.to_dict()), item)

    def test_an_announcement_of_an_earlier_runtime_has_no_epoch(self) -> None:
        payload = self.announced("a", 3, epoch="process-a").to_dict()
        del payload["session_epoch"]
        self.assertIsNone(AnnouncedFile.from_dict(payload).session_epoch)

    def test_offsets_must_be_ordered(self) -> None:
        payload = self.announced("a", 1).to_dict()
        with self.assertRaises(ValueError):
            AnnouncedFile.from_dict({**payload, "after_offset": 5, "offset_start": 5})


class AppendCommitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lake = LakeFixture({"order_id": "A", "total": 1.5})

    def tearDown(self) -> None:
        self.lake.close()

    def commit(self, *announced_lists):
        pending = PendingFiles()
        for announced in announced_lists:
            for item in announced:
                pending.add(item)
        table = self.lake.load()
        plan = plan_append(pending.seals(), committed_offsets(table.metadata), **UNLIMITED)
        return plan, commit_append(table, plan, runtime="develop-mat-a1b2", store=self.lake.store)

    def test_snapshot_carries_offsets_and_totals(self) -> None:
        plan, result = self.commit(
            self.lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1, 2]),
            self.lake.write_seal(partition=1, after_offset=-1, offsets=[10, 11]),
        )
        self.assertEqual(result.files, 2)
        self.assertEqual(result.records, 5)
        table = self.lake.load()
        summary = table.current_snapshot().summary.additional_properties
        self.assertEqual(summary["materializer.files"], "2")
        self.assertEqual(summary["materializer.runtime"], "develop-mat-a1b2")
        self.assertEqual(committed_offsets(table.metadata).offsets, {0: 2, 1: 11})
        self.assertIn(OFFSETS_PROPERTY, summary)
        self.assertEqual(table_totals(table), {"records": 5, "files": 2, "bytes": result.bytes})

    def test_offsets_carry_forward_across_commits(self) -> None:
        self.commit(self.lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1]))
        self.commit(self.lake.write_seal(partition=1, after_offset=-1, offsets=[5]))
        offsets = committed_offsets(self.lake.load().metadata)
        self.assertEqual(offsets.offsets, {0: 1, 1: 5})

    def test_valid_through_is_the_slowest_partition(self) -> None:
        early = [BASE_TIME + timedelta(minutes=1)]
        late = [BASE_TIME + timedelta(minutes=9)]
        self.commit(
            self.lake.write_seal(partition=0, after_offset=-1, offsets=[0], times=early),
            self.lake.write_seal(partition=1, after_offset=-1, offsets=[0], times=late),
        )
        summary = self.lake.load().current_snapshot().summary.additional_properties
        self.assertEqual(summary[VALID_THROUGH_PROPERTY], "2026-10-18T10:01:00.000Z")

    def test_a_seal_spanning_days_is_one_file_per_day_in_one_snapshot(self) -> None:
        times = [datetime(2026, 10, 17, 23, 59, tzinfo=UTC), datetime(2026, 10, 18, 0, 1, tzinfo=UTC)]
        announced = self.lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1], times=times)
        self.assertEqual(len(announced), 2)
        self.assertTrue(all(item.file_count == 2 for item in announced))
        _, result = self.commit(announced)
        table = self.lake.load()
        partitions = sorted(task.file.partition[0] for task in table.scan().plan_files())
        self.assertEqual(partitions, [20743, 20744])
        self.assertEqual(result.files, 2)

    def test_a_month_table_holds_a_file_per_month_partitioned_as_iceberg_computes_it(self) -> None:
        lake = LakeFixture({"order_id": "A"}, granularity="month")
        try:
            times = [
                datetime(2026, 9, 30, 23, 59, 59, tzinfo=UTC),
                datetime(2026, 10, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 10, 31, 12, 0, tzinfo=UTC),
            ]
            announced = lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1, 2], times=times)
            self.assertEqual(["2026-09", "2026-10"], sorted(item.partition_value["ts_month"] for item in announced))
            self.assertTrue(all("/data/ts_month=" in item.file_path for item in announced))
            lake.commit(announced)
            table = lake.load()
            self.assertEqual("month", str(table.spec().fields[0].transform))
            to_month = MonthTransform().transform(TimestamptzType())
            files = [task.file for task in table.scan().plan_files()]
            self.assertEqual(2, len(files))
            for data_file in files:
                lower = int.from_bytes(data_file.lower_bounds[1], "little", signed=True)
                self.assertEqual(to_month(lower), data_file.partition[0])
            # PyIceberg prunes by the partition: October's file alone holds October's rows.
            october = table.scan(row_filter=GreaterThanOrEqual("_kafka_timestamp", "2026-10-01T00:00:00+00:00"))
            self.assertEqual(1, len(list(october.plan_files())))
            self.assertEqual([1, 2], sorted(row["_kafka_offset"] for row in october.to_arrow().to_pylist()))
        finally:
            lake.close()

    def test_data_file_statistics_include_nested_leaves_and_metadata_bounds(self) -> None:
        announced = self.lake.write_seal(
            partition=4,
            after_offset=-1,
            offsets=[7, 8],
            values=[{"order_id": "A", "total": 2.5}, {"order_id": "B", "total": 9.0}],
        )
        self.commit(announced)
        table = self.lake.load()
        data_file = next(iter(table.scan().plan_files())).file
        self.assertEqual(data_file.record_count, 2)
        for field_id in (1, 2, 3):
            self.assertIn(field_id, data_file.lower_bounds)
            self.assertIn(field_id, data_file.upper_bounds)
        total_id = table.schema().find_field("total").field_id
        self.assertIn(total_id, data_file.lower_bounds)

    def test_struct_members_get_bounds_list_elements_only_counts(self) -> None:
        lake = LakeFixture({"customer": {"email": "a"}, "items": [{"sku": "s"}]})
        try:
            announced = lake.write_seal(
                partition=0,
                after_offset=-1,
                offsets=[1, 2],
                values=[{"customer": {"email": "b@x"}, "items": [{"sku": "A"}]}, {"customer": {"email": "a@x"}, "items": [{"sku": "B"}]}],
            )
            pending = PendingFiles()
            for item in announced:
                pending.add(item)
            table = lake.load()
            commit_append(table, plan_append(pending.seals(), committed_offsets(table.metadata), **UNLIMITED), runtime="rt", store=lake.store)
            table = lake.load()
            data_file = next(iter(table.scan().plan_files())).file
            email_id = table.schema().find_field("customer.email").field_id
            sku_id = table.schema().find_field("items.element.sku").field_id
            self.assertEqual(data_file.lower_bounds[email_id], b"a@x")
            self.assertEqual(data_file.upper_bounds[email_id], b"b@x")
            self.assertNotIn(sku_id, data_file.lower_bounds)
            self.assertIn(sku_id, data_file.value_counts)
        finally:
            lake.close()


class FileNamingTests(unittest.TestCase):
    def test_partition_text_and_records(self) -> None:
        moment = datetime(2026, 10, 18, 10, 30, tzinfo=UTC)
        self.assertEqual(partition_text("day", moment), "2026-10-18")
        self.assertEqual(partition_text("hour", moment), "2026-10-18-10")
        self.assertIsNone(partition_text("none", moment))
        self.assertEqual(partition_text("day", None), "__null__")
        self.assertEqual(partition_values("day", "__null__"), {"ts_day": None})
        self.assertEqual(partition_values("hour", "2026-10-18-10"), {"ts_hour": "2026-10-18-10"})
        self.assertEqual(partition_record("day", {"ts_day": "1970-01-02"})[0], 1)
        self.assertEqual(partition_record("hour", {"ts_hour": "1970-01-01-05"})[0], 5)
        self.assertEqual(len(partition_record("none", {})), 0)

    def test_month_partitions(self) -> None:
        self.assertEqual(partition_text("month", datetime(2026, 10, 18, 10, 30, tzinfo=UTC)), "2026-10")
        # Months follow UTC, whatever zone a timestamp was written in.
        self.assertEqual(partition_text("month", datetime(2026, 11, 1, 0, 30, tzinfo=timezone(timedelta(hours=2)))), "2026-10")
        self.assertEqual(partition_text("month", None), "__null__")
        self.assertEqual(partition_values("month", "2026-10"), {"ts_month": "2026-10"})
        self.assertEqual(partition_values("month", "__null__"), {"ts_month": None})
        self.assertEqual(partition_record("month", {"ts_month": "1970-02"})[0], 1)
        self.assertEqual(partition_record("month", {"ts_month": "2026-10"})[0], 681)
        self.assertIsNone(partition_record("month", {"ts_month": None})[0])
        for value, text in ((0, "1970-01"), (11, "1970-12"), (12, "1971-01"), (681, "2026-10"), (-1, "1969-12")):
            with self.subTest(value=value):
                self.assertEqual(text, partition_text_of("month", (value,)))
                self.assertEqual(value, partition_record("month", {"ts_month": text})[0])
        uri = data_file_uri(LOCATION, granularity="month", partition_value_text="2026-10", kafka_partition=0,
                            offset_start=1, offset_end=2, member_id="m", unique="u")
        self.assertIn("/data/ts_month=2026-10/p00-", uri)

    def test_data_file_uri(self) -> None:
        uri = data_file_uri(LOCATION, granularity="day", partition_value_text="2026-10-18", kafka_partition=3,
                            offset_start=100, offset_end=199, member_id="pod/1", unique="abc")
        self.assertEqual(uri, f"{LOCATION}/data/ts_day=2026-10-18/p03-o0000000100-0000000199-pod_1-abc.parquet")

    def test_a_files_name_says_its_protection_label(self) -> None:
        common = {"granularity": "day", "partition_value_text": "2026-10-18"}
        written = data_file_uri(LOCATION, kafka_partition=3, offset_start=1, offset_end=2, member_id="m", unique="u", protection_revision=2, **common)
        compacted = compacted_file_uri(LOCATION, unique="c", protection_label=(3, 1), **common)
        rewritten = reprotected_file_uri(LOCATION, unique="r", protection_label=(5, 3, 0), **common)
        self.assertTrue(written.endswith("-m-u-r2.parquet"))
        self.assertTrue(compacted.endswith("/compact-c-r3p1.parquet"))
        self.assertTrue(rewritten.endswith("/reprotect-r-r5p3p0.parquet"))
        expected = {written: (2,), compacted: (3, 1), rewritten: (5, 3, 0), f"{LOCATION}/data/x.parquet": (0,), "gs://b/odd-r2px.parquet": (0,)}
        for path, label in expected.items():
            with self.subTest(path=path):
                self.assertEqual(label, protection_label_of(path))
                self.assertEqual(label[0], protection_revision_of(path))
        self.assertTrue(compacted_file_uri(LOCATION, unique="c", protection_revision=0, **common).endswith("/compact-c.parquet"))

    def test_footer_reads_and_millis(self) -> None:
        lake = LakeFixture({"order_id": "x"})
        try:
            announced = lake.write_seal(partition=0, after_offset=-1, offsets=[1])
            footer, size = read_parquet_footer(lake.store, announced[0].file_path)
            self.assertEqual(footer.num_rows, 1)
            self.assertEqual(size, announced[0].file_size)
        finally:
            lake.close()
        self.assertEqual(format_millis(parse_millis("2026-10-18T10:00:00.250Z")), "2026-10-18T10:00:00.250Z")
        self.assertIsNone(parse_millis("garbage"))


if __name__ == "__main__":
    unittest.main()
