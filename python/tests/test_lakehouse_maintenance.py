"""Table maintenance: which partition compaction takes, which snapshots expire, which files are orphans."""

from __future__ import annotations

from contextlib import redirect_stderr
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import BASE_TIME, LOCATION, LakeFixture  # noqa: E402

from midstream.lakehouse.files import build_data_file, read_parquet_footer  # noqa: E402
from midstream.lakehouse.gcs import set_shared_storage_client  # noqa: E402
from midstream.lakehouse.lifecycle import schema_to_json  # noqa: E402
from midstream.lakehouse.maintenance import (  # noqa: E402
    CompactionInputGone,
    CompactionMismatch,
    commit_compaction,
    expire_snapshots,
    find_orphans,
    live_data_files,
    plan_compaction,
    plan_expiry,
    referenced_files,
    small_file_count,
)
from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.pointer import POINTER_NAME  # noqa: E402
from midstream.maintenance_app import compact_worker  # noqa: E402
from midstream.maintenance_app.compact_worker import run as run_compaction  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from pyiceberg.typedef import Record  # noqa: E402

LATER = datetime.now(tz=UTC) + timedelta(hours=3)


def plan(table, **overrides):
    values = dict(
        now=LATER,
        min_input_files=3,
        small_file_bytes=64 * 1024 * 1024,
        max_input_bytes=1 << 40,
        hot_partition_delay_minutes=60,
    )
    values.update(overrides)
    return plan_compaction(table, **values)


class CompactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lake = LakeFixture({"order_id": "O-0", "amount": 1})
        self.addCleanup(self.lake.close)
        set_shared_storage_client(self.lake.client)
        self.addCleanup(set_shared_storage_client, None)
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        # As the runner makes one: a directory of the job's own, inside the compaction work root.
        self.job_dir = os.path.join(self.work.name, "compact-job")

    def job(self, work_dir) -> dict:
        """A job for a compaction of the first day, naming `work_dir`."""
        self.commit_files(3, partition=0, day_offset=-30)
        table = self.lake.load()
        return {
            "plan": plan(table).to_dict(),
            "schema": schema_to_json(table.schema()),
            "work_dir": work_dir,
            "location": LOCATION,
            "granularity": "day",
        }

    def commit_files(
        self,
        count: int,
        *,
        partition: int = 0,
        day_offset: int = 0,
        at: datetime | None = None,
        lake=None,
        protection_revision: int = 0,
    ) -> None:
        lake = lake or self.lake
        after = committed_offsets(lake.load().metadata).get(partition)
        for _ in range(count):
            offsets = [after + 1, after + 2]
            moment = at or BASE_TIME + timedelta(days=day_offset)
            lake.commit(
                lake.write_seal(
                    partition=partition,
                    after_offset=after,
                    offsets=offsets,
                    times=[moment, moment],
                    protection_revision=protection_revision,
                )
            )
            after = offsets[-1]

    def compact(self, compaction_plan, **job):
        """Run the compaction worker in this process and commit what it wrote."""
        table = self.lake.load()
        result = run_compaction(
            {
                "plan": compaction_plan.to_dict(),
                "schema": schema_to_json(table.schema()),
                "work_dir": self.job_dir,
                "location": LOCATION,
                "granularity": "day",
                **job,
            },
            work_root=self.work.name,
        )
        data_files = []
        for output in result["outputs"]:
            footer, size = read_parquet_footer(self.lake.client, output["uri"])
            data_files.append(
                build_data_file(
                    table.metadata,
                    file_path=output["uri"],
                    file_size=size,
                    parquet_metadata=footer,
                    granularity="day",
                    partition=Record(*output["partition"]),
                )
            )
        bounds = [(output["timestamp_min"], output["timestamp_max"]) for output in result["outputs"]]
        return commit_compaction(table, compaction_plan, data_files, runtime="test", output_bounds=bounds), result

    def rows(self):
        return sorted(
            self.lake.load().scan().to_arrow().to_pylist(),
            key=lambda row: (row["_kafka_partition"], row["_kafka_offset"]),
        )

    def test_a_settled_partition_with_enough_small_files_is_planned(self) -> None:
        self.commit_files(4)
        table = self.lake.load()
        result = plan(table)
        self.assertIsNotNone(result)
        self.assertEqual(4, len(result.inputs))
        self.assertEqual(8, result.input_records)
        self.assertEqual(table.metadata.current_snapshot_id, result.planned_snapshot_id)
        self.assertEqual(4, small_file_count(live_data_files(table), small_file_bytes=64 * 1024 * 1024))

    def test_files_of_two_protection_revisions_are_compacted_into_outputs_of_their_own(self) -> None:
        # A policy edit between commits: the partition holds files masked under revision 1 and 2,
        # interleaved. No output may mix them, so each file stays attributable to its policy.
        for revision in (1, 2, 1, 2):
            self.commit_files(1, protection_revision=revision)
        table = self.lake.load()
        compaction_plan = plan(table)
        self.assertEqual([1, 1, 2, 2], [item.protection_revision for item in compaction_plan.inputs])
        _, result = self.compact(compaction_plan)
        self.assertEqual([1, 2], sorted(output["protection_revision"] for output in result["outputs"]))
        live = live_data_files(self.lake.load())
        self.assertEqual({1: 1, 2: 1}, {revision: sum(1 for item in live if item.protection_revision == revision) for revision in (1, 2)})
        self.assertTrue(all(item.path.endswith(f"-r{item.protection_revision}.parquet") for item in live))
        self.assertEqual(8, len(self.rows()))

    def test_files_without_a_revision_compact_as_before(self) -> None:
        self.commit_files(3)
        _, result = self.compact(plan(self.lake.load()))
        (output,) = result["outputs"]
        self.assertEqual(0, output["protection_revision"])
        self.assertNotIn("-r", output["uri"].rsplit("/", 1)[1].replace("compact-", ""))

    def test_the_partition_being_written_waits_for_the_hot_delay(self) -> None:
        now = datetime.now(tz=UTC)
        self.commit_files(4, at=now)
        table = self.lake.load()
        self.assertIsNone(plan(table, now=now))
        self.assertIsNotNone(plan(table, now=now, hot_partition_delay_minutes=0))
        # An hour after its last commit it has settled.
        self.assertIsNotNone(plan(table, now=now + timedelta(minutes=61)))

    def test_a_past_partition_is_compacted_however_recently_a_backfill_added_to_it(self) -> None:
        # Committed a moment ago, but into a day that is long over: nothing live writes it.
        self.commit_files(4, day_offset=-30)
        result = plan(self.lake.load(), now=datetime.now(tz=UTC))
        self.assertIsNotNone(result)
        self.assertEqual(("2026-09-18",), result.partition_keys)

    def test_where_no_time_bounds_a_partition_each_file_waits_until_it_is_old_enough(self) -> None:
        lake = LakeFixture({"order_id": "O-0", "amount": 1}, granularity="none")
        self.addCleanup(lake.close)
        self.commit_files(4, lake=lake)
        table = lake.load()
        now = datetime.now(tz=UTC)
        self.assertIsNone(plan(table, now=now))
        self.assertEqual(4, len(plan(table, now=now + timedelta(minutes=61)).inputs))
        # Only the files old enough are taken, however recently others joined them.
        older = plan(table, now=now + timedelta(minutes=61), files=[
            *live_data_files(table)[:3],
            replace(live_data_files(table)[3], added_at_ms=int((now + timedelta(minutes=30)).timestamp() * 1000)),
        ])
        self.assertEqual(3, len(older.inputs))

    def test_this_months_files_settle_one_by_one_as_they_age(self) -> None:
        # A busy month never goes an hour without a commit: waiting for it to settle as a whole would
        # leave its small files until the month ended, so each file settles as it ages. A day still
        # waits until nothing has been added to it for the delay.
        now = datetime.now(tz=UTC)
        two_hours_ago = int((now - timedelta(hours=2)).timestamp() * 1000)
        just_now = int((now - timedelta(minutes=5)).timestamp() * 1000)
        for granularity, expected in (("month", 4), ("day", None)):
            with self.subTest(granularity=granularity):
                lake = LakeFixture({"order_id": "O-0", "amount": 1}, granularity=granularity)
                self.addCleanup(lake.close)
                self.commit_files(5, at=now, lake=lake)
                table = lake.load()
                files = sorted(live_data_files(table), key=lambda item: item.path)
                aged = [replace(item, added_at_ms=two_hours_ago) for item in files[:4]]
                aged.append(replace(files[4], added_at_ms=just_now))
                result = plan(table, now=now, files=aged)
                self.assertEqual(expected, None if result is None else len(result.inputs))

    def test_a_past_month_is_compacted_into_files_of_its_own_month(self) -> None:
        self.lake = LakeFixture({"order_id": "O-0", "amount": 1}, granularity="month")
        self.addCleanup(self.lake.close)
        set_shared_storage_client(self.lake.client)
        # August, written a moment ago by a backfill: the month is over, so it has settled.
        self.commit_files(4, day_offset=-60)
        before = self.rows()
        compaction_plan = plan(self.lake.load(), now=datetime.now(tz=UTC))
        self.assertEqual(("2026-08",), compaction_plan.partition_keys)
        self.assertEqual(4, len(compaction_plan.inputs))
        _, output = self.compact(compaction_plan, granularity="month")
        (live,) = live_data_files(self.lake.load())
        self.assertEqual("2026-08", live.partition_key)
        self.assertIn("/data/ts_month=2026-08/compact-", live.path)
        self.assertEqual(["2026-08"], [item["partition_key"] for item in output["outputs"]])
        self.assertEqual(before, self.rows())

    def test_too_few_small_files_are_left_alone(self) -> None:
        self.commit_files(2)
        self.assertIsNone(plan(self.lake.load()))

    def test_the_oldest_partition_goes_first_and_skipped_ones_are_passed_over(self) -> None:
        self.commit_files(3, day_offset=1)
        self.commit_files(3, partition=1, day_offset=0)
        table = self.lake.load()
        first = plan(table)
        self.assertEqual("2026-10-18", first.partition_key)
        second = plan(table, skip={first.partition_key})
        self.assertEqual("2026-10-19", second.partition_key)

    def test_small_partitions_are_compacted_together_each_into_its_own_files(self) -> None:
        for day in range(3):
            self.commit_files(3, partition=day, day_offset=-30 + day)
        before = self.rows()
        result = plan(self.lake.load())
        self.assertEqual(("2026-09-18", "2026-09-19", "2026-09-20"), result.partition_keys)
        self.assertEqual("2026-09-18 … 2026-09-20 (3 partitions)", result.label)
        self.assertEqual(9, len(result.inputs))
        _, output = self.compact(result)
        self.assertEqual(["2026-09-18", "2026-09-19", "2026-09-20"], [item["partition_key"] for item in output["outputs"]])
        live = live_data_files(self.lake.load())
        self.assertEqual(["2026-09-18", "2026-09-19", "2026-09-20"], sorted(item.partition_key for item in live))
        self.assertTrue(all(f"/ts_day={item.partition_key}/" in item.path for item in live))
        self.assertEqual(before, self.rows())

    def output_table(self, output):
        bucket, _, name = output["uri"].removeprefix("gs://").partition("/")
        return pq.ParquetFile(pa.BufferReader(self.lake.client.bucket(bucket).blob(name).download_as_bytes()))

    def seal_newest_first(self) -> list:
        """Six rows written newest first, then two smaller files: three inputs of one day."""
        newest_first = [BASE_TIME + timedelta(minutes=5 - index) for index in range(6)]
        self.lake.commit(self.lake.write_seal(partition=0, after_offset=-1, offsets=list(range(6)), times=newest_first))
        self.lake.commit(self.lake.write_seal(partition=0, after_offset=5, offsets=[6, 7], times=[BASE_TIME, BASE_TIME]))
        self.lake.commit(self.lake.write_seal(partition=0, after_offset=7, offsets=[8, 9], times=[BASE_TIME, BASE_TIME]))
        return self.rows()

    def test_an_input_larger_than_the_memory_budget_is_read_and_written_a_budget_at_a_time(self) -> None:
        # Two rows read at a time, and past a budget of one byte each read is written at once: no
        # input is ever held whole, and the output is a run of sorted row groups.
        before = self.seal_newest_first()
        compaction_plan = plan(self.lake.load())
        self.assertEqual(3, len(compaction_plan.inputs))
        with patch.object(compact_worker, "READ_BATCH_ROWS", 2):
            _, output = self.compact(compaction_plan, memory_budget_bytes=1)
        [item] = output["outputs"]
        parquet = self.output_table(item)
        self.assertEqual([2, 2, 2, 2, 2], [parquet.metadata.row_group(index).num_rows for index in range(parquet.num_row_groups)])
        for index in range(parquet.num_row_groups):
            stamps = parquet.read_row_group(index, columns=["_kafka_timestamp"]).column(0).to_pylist()
            self.assertEqual(sorted(stamps), stamps)
        self.assertEqual(10, item["record_count"])
        self.assertEqual((0, 9), (item["offset_min"], item["offset_max"]))
        self.assertEqual(before, self.rows())

    def test_an_output_is_closed_once_it_reaches_the_target_file_size(self) -> None:
        before = self.seal_newest_first()
        with patch.object(compact_worker, "READ_BATCH_ROWS", 2):
            _, output = self.compact(plan(self.lake.load()), memory_budget_bytes=1, target_file_size_bytes=1)
        self.assertEqual([2, 2, 2, 2, 2], [item["record_count"] for item in output["outputs"]])
        for item in output["outputs"]:
            stamps = self.output_table(item).read(columns=["_kafka_timestamp"]).column(0).to_pylist()
            self.assertEqual(sorted(stamps), stamps)
            self.assertEqual(
                (item["timestamp_min"], item["timestamp_max"]),
                (int(stamps[0].timestamp() * 1_000_000), int(stamps[-1].timestamp() * 1_000_000)),
            )
        self.assertEqual(before, self.rows())

    def test_an_output_is_written_sorted_a_row_group_at_a_time(self) -> None:
        newest_first = [BASE_TIME + timedelta(minutes=9 - index) for index in range(10)]
        for start in (0, 4, 7):
            end = {0: 4, 4: 7, 7: 10}[start]
            self.lake.commit(
                self.lake.write_seal(partition=0, after_offset=start - 1, offsets=list(range(start, end)), times=newest_first[start:end])
            )
        before = self.rows()
        _, output = self.compact(plan(self.lake.load()), row_group_rows=3)
        [item] = output["outputs"]
        parquet = self.output_table(item)
        self.assertEqual([3, 3, 3, 1], [parquet.metadata.row_group(index).num_rows for index in range(parquet.num_row_groups)])
        stamps = parquet.read(columns=["_kafka_timestamp"]).column(0).to_pylist()
        self.assertEqual(sorted(stamps), stamps)
        self.assertEqual(before, self.rows())

    def test_the_worker_works_only_inside_the_compaction_work_root(self) -> None:
        # Whatever directory the job names, the worker creates, writes and deletes files only inside
        # the work root, and refuses the job before reading or writing anything.
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        escape = os.path.join(outside.name, "escaped")
        os.symlink(outside.name, os.path.join(self.work.name, "link"))
        job = self.job(self.job_dir)
        objects_before = set(self.lake.store.names())
        for work_dir in (
            escape,  # an absolute path elsewhere
            os.path.relpath(escape, self.work.name),  # a relative one that climbs out
            os.path.join(self.work.name, "compact-job", "..", "..", os.path.basename(outside.name), "escaped"),
            os.path.join(self.work.name, "link", "escaped"),  # a symlink inside the root that leads out
            self.work.name,  # the root itself: a job works in a directory of its own
            "",
            None,
        ):
            with self.subTest(work_dir=work_dir), self.assertRaises(ValueError):
                run_compaction({**job, "work_dir": work_dir}, work_root=self.work.name)
        self.assertFalse(os.path.exists(escape))
        self.assertEqual(["link"], os.listdir(self.work.name))
        self.assertEqual(objects_before, set(self.lake.store.names()))

    def test_the_worker_refuses_a_job_id_the_planner_did_not_make(self) -> None:
        job = self.job(self.job_dir)
        objects_before = set(self.lake.store.names())
        for job_id in ("../../escaped", "a" * 31, "A" * 32, "a" * 33, ""):
            with self.subTest(job_id=job_id), self.assertRaises(ValueError):
                run_compaction({**job, "plan": {**job["plan"], "job_id": job_id}}, work_root=self.work.name)
        self.assertFalse(os.path.exists(self.job_dir))
        self.assertEqual(objects_before, set(self.lake.store.names()))

    def test_the_worker_writes_only_directly_inside_its_work_directory(self) -> None:
        # Every local file is named by the worker alone, by its position in the job.
        job = self.job(self.job_dir)
        blob_type = type(self.lake.client.bucket("lake").blob("x"))
        written: list[str] = []
        download = blob_type.download_to_filename

        def recording(blob, filename, **kwargs):
            written.append(filename)
            return download(blob, filename, **kwargs)

        with patch.object(blob_type, "download_to_filename", recording):
            result = run_compaction(job, work_root=self.work.name)
        self.assertEqual(1, len(result["outputs"]))
        self.assertEqual(3, len(written))
        self.assertEqual({os.path.realpath(self.job_dir)}, {os.path.dirname(os.path.realpath(path)) for path in written})
        self.assertEqual([], os.listdir(self.job_dir))

    def test_the_command_line_worker_refuses_a_job_outside_its_work_root(self) -> None:
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        escape = os.path.join(outside.name, "escaped")
        job_file = os.path.join(self.work.name, "job.json")
        Path(job_file).write_text(json.dumps(self.job(escape)), encoding="utf-8")
        stderr = io.StringIO()
        with patch.object(compact_worker.os, "nice"), patch.object(compact_worker, "_die_with_parent"), redirect_stderr(stderr):
            self.assertEqual(1, compact_worker.main([job_file]))
        self.assertIn("ValueError", stderr.getvalue())
        self.assertFalse(os.path.exists(escape))

    def test_compaction_keeps_a_missing_list_null_and_an_empty_one_empty(self) -> None:
        # A record without a key, a tombstone, and a record with the key holding an empty list: a
        # compaction must not tell them apart any differently from the files it rewrites.
        lake = LakeFixture({"order_id": "O-0", "items": [{"sku": "S-0", "qty": 1}], "fulfillment": {"type": "ship", "shipments": [{"id": "SH-0"}]}})
        self.addCleanup(lake.close)
        set_shared_storage_client(lake.client)
        values = [
            {"order_id": "O-1", "items": [{"sku": "S-1", "qty": 2}], "fulfillment": {"type": "ship", "shipments": [{"id": "SH-1"}]}},
            {"order_id": "O-2"},
            None,
            {"order_id": "O-3", "items": [], "fulfillment": {"type": "pickup", "shipments": []}},
        ]
        after = -1
        for value in values:
            lake.commit(lake.write_seal(partition=0, after_offset=after, offsets=[after + 1], values=[value]))
            after += 1
        before = sorted(lake.load().scan(selected_fields=("_kafka_offset",)).to_arrow().to_pylist(), key=lambda row: row["_kafka_offset"])
        self.assertEqual(4, len(before))
        self.lake = lake
        _, output = self.compact(plan(lake.load()))
        [item] = output["outputs"]
        rows = sorted(self.output_table(item).read().to_pylist(), key=lambda row: row["_kafka_offset"])
        self.assertEqual([{"sku": "S-1", "qty": 2}], rows[0]["items"])
        self.assertIsNone(rows[1]["items"])
        self.assertIsNone(rows[1]["fulfillment"])
        self.assertIsNone(rows[2]["items"])
        self.assertEqual([], rows[3]["items"])
        self.assertEqual([], rows[3]["fulfillment"]["shipments"])

    def test_a_partition_joins_a_plan_only_whole_and_only_while_it_fits(self) -> None:
        self.commit_files(3, partition=0, day_offset=-30)
        self.commit_files(4, partition=1, day_offset=-29)
        table = self.lake.load()
        files = live_data_files(table)
        first_day = sum(item.file_size for item in files if item.partition_key == "2026-09-18")
        largest = max(item.file_size for item in files)
        self.assertEqual(("2026-09-18", "2026-09-19"), plan(table).partition_keys)
        self.assertEqual(("2026-09-18",), plan(table, max_input_bytes=first_day + largest).partition_keys)
        self.assertEqual(("2026-09-18",), plan(table, max_input_files=6).partition_keys)
        self.assertEqual(7, len(plan(table, max_input_files=7).inputs))

    def test_a_partition_that_failed_among_others_is_only_ever_planned_alone(self) -> None:
        for day in range(3):
            self.commit_files(3, partition=day, day_offset=-30 + day)
        table = self.lake.load()
        self.assertEqual(("2026-09-18",), plan(table, solo={"2026-09-18"}).partition_keys)
        # A plan stops before a partition that must run alone; the next plan takes it.
        self.assertEqual(("2026-09-18",), plan(table, solo={"2026-09-19"}).partition_keys)
        self.assertEqual(("2026-09-19",), plan(table, skip={"2026-09-18"}, solo={"2026-09-19"}).partition_keys)

    def test_outputs_must_hold_each_partitions_rows_in_that_partition(self) -> None:
        self.commit_files(3, partition=0, day_offset=-30)
        self.commit_files(3, partition=1, day_offset=-29)
        compaction_plan = plan(self.lake.load())
        table = self.lake.load()
        result = run_compaction(
            {
                "plan": compaction_plan.to_dict(),
                "schema": schema_to_json(table.schema()),
                "work_dir": self.job_dir,
                "location": LOCATION,
                "granularity": "day",
            },
            work_root=self.work.name,
        )
        # Every output claimed for the first partition: its row count is wrong, and so is the second's.
        mislabelled = []
        for output in result["outputs"]:
            footer, size = read_parquet_footer(self.lake.client, output["uri"])
            mislabelled.append(
                build_data_file(
                    table.metadata,
                    file_path=output["uri"],
                    file_size=size,
                    parquet_metadata=footer,
                    granularity="day",
                    partition=Record(*compaction_plan.partition),
                )
            )
        with self.assertRaisesRegex(CompactionMismatch, "partition 2026-09-1[89]: the outputs hold"):
            commit_compaction(table, compaction_plan, mislabelled, runtime="test")
        self.assertEqual(6, len(live_data_files(self.lake.load())))

    def test_inputs_stop_at_the_byte_bound_but_never_below_the_minimum(self) -> None:
        self.commit_files(6)
        table = self.lake.load()
        one_file = live_data_files(table)[0].file_size
        bounded = plan(table, max_input_bytes=one_file * 4)
        self.assertEqual(4, len(bounded.inputs))
        self.assertIsNone(plan(table, max_input_bytes=one_file * 2))

    def test_compaction_replaces_small_files_without_changing_a_row_or_the_offsets(self) -> None:
        self.commit_files(4)
        before = self.rows()
        offsets_before = committed_offsets(self.lake.load().metadata).offsets
        snapshot_id, result = self.compact(plan(self.lake.load()))
        table = self.lake.load()
        self.assertEqual(snapshot_id, table.metadata.current_snapshot_id)
        self.assertEqual("replace", table.current_snapshot().summary.operation.value)
        self.assertEqual(1, len(result["outputs"]))
        self.assertEqual(1, len(live_data_files(table)))
        self.assertEqual(before, self.rows())
        self.assertEqual(offsets_before, committed_offsets(table.metadata).offsets)

    def test_a_compaction_whose_inputs_are_gone_commits_nothing(self) -> None:
        self.commit_files(4)
        stale = plan(self.lake.load())
        self.compact(stale)
        with self.assertRaises(CompactionInputGone):
            self.compact(stale)
        self.assertEqual(8, len(self.rows()))

    def test_outputs_that_do_not_hold_the_inputs_rows_are_refused(self) -> None:
        self.commit_files(4)
        compaction_plan = plan(self.lake.load())
        table = self.lake.load()
        with self.assertRaises(CompactionMismatch):
            commit_compaction(table, compaction_plan, [], runtime="test")
        self.assertEqual(4, len(live_data_files(self.lake.load())))


    def test_once_expired_away_the_inputs_a_compaction_replaced_are_orphans(self) -> None:
        self.commit_files(4)
        compaction_plan = plan(self.lake.load())
        self.compact(compaction_plan)
        table = self.lake.load()
        inputs = {item.path for item in compaction_plan.inputs}
        # While a snapshot that reads them is kept, they are referenced.
        self.assertTrue(inputs <= referenced_files(table))

        ids = plan_expiry(table.metadata, now=datetime.now(tz=UTC) + timedelta(hours=100), retention_hours=72, min_snapshots_to_keep=1, keep_all_minutes=60)
        expire_snapshots(table, ids)
        table = self.lake.load()
        orphans = find_orphans(self.lake.client, LOCATION, referenced_files(table), now=datetime.now(tz=UTC) + timedelta(hours=25), min_age_hours=24)
        self.assertTrue(inputs <= set(orphans.candidates))
        self.assertFalse({item.path for item in live_data_files(table)} & set(orphans.candidates))


class ExpiryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lake = LakeFixture({"order_id": "O-0"})
        self.addCleanup(self.lake.close)
        after = -1
        for _ in range(5):
            offsets = [after + 1]
            self.lake.commit(self.lake.write_seal(partition=0, after_offset=after, offsets=offsets))
            after = offsets[-1]

    def snapshots(self):
        return sorted(self.lake.load().metadata.snapshots, key=lambda item: item.timestamp_ms)

    def test_snapshots_past_retention_expire_but_the_newest_are_kept(self) -> None:
        metadata = self.lake.load().metadata
        expired = plan_expiry(
            metadata,
            now=datetime.now(tz=UTC) + timedelta(hours=100),
            retention_hours=72,
            min_snapshots_to_keep=2,
            keep_all_minutes=60,
        )
        oldest_three = [item.snapshot_id for item in self.snapshots()[:3]]
        self.assertEqual(sorted(oldest_three), sorted(expired))

    def test_the_current_snapshot_is_never_expired(self) -> None:
        metadata = self.lake.load().metadata
        expired = plan_expiry(
            metadata,
            now=datetime.now(tz=UTC) + timedelta(hours=100),
            retention_hours=1,
            min_snapshots_to_keep=1,
            keep_all_minutes=1,
        )
        self.assertNotIn(metadata.current_snapshot_id, expired)
        self.assertEqual(4, len(expired))

    def test_recent_snapshots_are_all_kept_and_older_ones_thinned_to_one_an_hour(self) -> None:
        metadata = self.lake.load().metadata
        within = plan_expiry(metadata, now=datetime.now(tz=UTC), retention_hours=72, min_snapshots_to_keep=1, keep_all_minutes=60)
        self.assertEqual([], within)
        thinned = plan_expiry(
            metadata,
            now=datetime.now(tz=UTC) + timedelta(hours=2),
            retention_hours=72,
            min_snapshots_to_keep=1,
            keep_all_minutes=60,
        )
        # All five are in one hour: the current one is kept for it; the others go.
        self.assertEqual(4, len(thinned))

    def test_expiry_removes_snapshots_and_leaves_the_table_readable(self) -> None:
        table = self.lake.load()
        ids = [item.snapshot_id for item in self.snapshots()[:3]]
        self.assertEqual(3, expire_snapshots(table, ids))
        table = self.lake.load()
        self.assertEqual(2, len(table.metadata.snapshots))
        self.assertEqual(5, len(table.scan().to_arrow()))
        self.assertEqual({0: 4}, committed_offsets(table.metadata).offsets)


class OrphanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lake = LakeFixture({"order_id": "O-0"})
        self.addCleanup(self.lake.close)
        self.lake.commit(self.lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1]))

    def stray(self, name: str, *, age_hours: float) -> str:
        uri = f"{LOCATION}/data/ts_day=2026-10-18/{name}"
        self.lake.store.clock = lambda: datetime.now(tz=UTC) - timedelta(hours=age_hours)
        try:
            self.lake.store.put(uri, b"orphan")
        finally:
            self.lake.store.clock = lambda: datetime.now(tz=UTC)
        return uri

    def scan(self, **overrides):
        values = dict(now=datetime.now(tz=UTC), min_age_hours=24)
        values.update(overrides)
        table = self.lake.load()
        return find_orphans(self.lake.client, LOCATION, referenced_files(table), **values)

    def test_only_unreferenced_files_older_than_the_guard_are_orphans(self) -> None:
        old = self.stray("old.parquet", age_hours=48)
        self.stray("young.parquet", age_hours=1)
        result = self.scan()
        self.assertEqual((old,), result.candidates)

    def test_referenced_files_the_pointer_and_pending_files_are_never_orphans(self) -> None:
        pending = self.stray("pending.parquet", age_hours=48)
        result = self.scan(now=datetime.now(tz=UTC) + timedelta(days=30), protected={pending})
        self.assertEqual((), result.candidates)
        self.assertNotIn(f"{LOCATION}/metadata/{POINTER_NAME}", result.candidates)
        self.assertGreater(result.listed, 3)

    def test_a_pass_stops_at_its_limit(self) -> None:
        for index in range(3):
            self.stray(f"old-{index}.parquet", age_hours=48)
        result = self.scan(limit=2)
        self.assertEqual(2, len(result.candidates))
        self.assertTrue(result.truncated)


if __name__ == "__main__":
    unittest.main()
