"""The maintenance app: compaction, expiry and orphan cleanup of tables a runtime writes (spec §7.9)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import LOCATION, MATERIALIZATION_ID, NAMESPACE  # noqa: E402
from runtime_support import RuntimeHarness, run  # noqa: E402

from materializer_control_plane.lakehouse.maintenance import live_data_files  # noqa: E402
from materializer_control_plane.lakehouse.offsets import committed_offsets  # noqa: E402
from materializer_control_plane.maintenance_app import worker as worker_module  # noqa: E402
from materializer_control_plane.maintenance_app.compact_worker import compaction_work_root  # noqa: E402
from materializer_control_plane.maintenance_app.config import MaintenanceAppConfig, get_maintenance_app_config  # noqa: E402
from materializer_control_plane.maintenance_app.handlers import advance_maintenance, handle_maintenance_message  # noqa: E402
from materializer_control_plane.maintenance_app.worker import (  # noqa: E402
    MaintenanceWorker,
    TableMaintenance,
    compact_in_process,
    job_settings,
    reset_worker,
)
from materializer_control_plane.models import MaintenanceMessage, MaintenanceTarget, RunningMaintenance  # noqa: E402
from materializer_control_plane.services.table_maintenance import maintenance_settings  # noqa: E402

TOPICS = ("orders", "payments")
SEEDS = {"orders": ({"order_id": "O-0", "amount": 1},), "payments": ({"payment_id": "P-0", "amount": 1.5},)}


def now_text() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class MaintenanceHarness:
    """A maintenance member over the tables a `RuntimeHarness` writes, ticked as the control plane does."""

    def __init__(self, runtime: RuntimeHarness, *, max_concurrent_jobs: int = 1) -> None:
        self.runtime = runtime
        self.worker = MaintenanceWorker(
            client=runtime.lake.client,
            app=MaintenanceAppConfig(max_concurrent_jobs=max_concurrent_jobs, app_name="test-maintenance"),
        )

    def target(self, name: str | None = None) -> MaintenanceTarget:
        spec = self.runtime.specs[0] if name is None else next(spec for spec in self.runtime.specs if spec.name == name)
        return MaintenanceTarget(
            materialization_id=MATERIALIZATION_ID,
            namespace=NAMESPACE,
            table=spec.name,
            location=self.runtime.lakes[spec.name].location,
            source_topic=spec.source_topic,
            partition_granularity=self.runtime.granularity,
        )

    def message(self, kind: str, name: str | None = None, **fields) -> MaintenanceMessage:
        return MaintenanceMessage(
            kind=kind,
            target=self.target(name),
            issued_at=now_text(),
            settings=maintenance_settings(self.runtime.config),
            **fields,
        )

    def table(self, name: str | None = None) -> TableMaintenance:
        return self.worker.tables[self.target(name).key]

    async def tick(self, name: str | None = None, **fields) -> list[dict]:
        """A tick of one table (the first), then a step: what it settles and starts."""
        outputs = self.worker.handle(self.message("maintenance_tick", name, **fields))
        return events(outputs + await self.worker.step())

    async def tick_all(self) -> list[dict]:
        outputs = []
        for spec in self.runtime.specs:
            outputs.extend(self.worker.handle(self.message("maintenance_tick", spec.name)))
        return events(outputs + await self.worker.step())

    async def request(self, job: str, name: str | None = None) -> list[dict]:
        outputs = self.worker.handle(self.message("maintenance_request", name, job=job, request_id="mnt-1", requested_by="ops"))
        return events(outputs + await self.worker.step())

    async def settle(self) -> list[dict]:
        """Let the running jobs end, then step: commit what they made, and start what is due."""
        tasks = [table.job.task for table in self.worker.tables.values() if table.job is not None and table.job.task is not None]
        if tasks:
            await asyncio.wait(tasks)
        return events(await self.worker.step())

    def only_compaction_due(self, name: str | None = None) -> None:
        """As if expiry and orphan cleanup had just run, so a step can only start a compaction."""
        table = self.table(name)
        table.last_runs["expire_snapshots"] = table.last_runs["orphan_cleanup"] = time.time()


def events(outputs: list[dict]) -> list[dict]:
    return [output["value"] for output in outputs if output.get("route") == "events"]


def completed(found: list[dict]) -> list[dict]:
    return [event["payload"] for event in found if event["kind"] == "maintenance_completed"]


class MaintenanceAppTestCase(unittest.TestCase):
    topics = ("orders",)

    def setUp(self) -> None:
        self.runtime = RuntimeHarness(
            {"order_id": "O-0"},
            topics=self.topics,
            seeds_by_topic=SEEDS if len(self.topics) > 1 else None,
            compaction_min_input_files=3,
            compaction_interval_minutes=15,
            orphan_min_age_hours=24,
            # Each test that wants it turns it on, so no listing runs on in the background.
            orphan_cleanup_enabled=False,
        )
        self.addCleanup(self.runtime.close)
        self.maintenance = MaintenanceHarness(self.runtime)
        self.addCleanup(self.maintenance.worker.stop)
        compactor = patch.object(TableMaintenance, "compactor", staticmethod(compact_in_process))
        compactor.start()
        self.addCleanup(compactor.stop)

    async def commit_batches(self, count: int, *, start: int = 0, at: datetime | None = None, topic: str | None = None) -> None:
        """Commit `count` batches of three records, one data file each, through the runtime."""
        offset = start
        for _ in range(count):
            for _ in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset, at=at, topic=topic)
                offset += 1
            await self.runtime.cycle()


class CompactionTests(MaintenanceAppTestCase):
    def test_a_requested_compaction_rewrites_small_files_and_keeps_every_row(self) -> None:
        async def scenario():
            await self.commit_batches(4)
            before = self.runtime.rows()
            started = await self.maintenance.request("compaction")
            finished = await self.maintenance.settle()
            return before, started, finished

        before, started, finished = run(scenario())
        (began,) = [event["payload"] for event in started if event["kind"] == "maintenance_started"]
        self.assertEqual(("compaction", "2026-10-18"), (began["job"], began["partition"]))
        self.assertEqual("test-maintenance", started[0]["runtime_app_ref"])
        (done,) = completed(finished)
        self.assertEqual(("compaction", "succeeded"), (done["job"], done["outcome"]))
        self.assertEqual(began["started_at"], done["started_at"])
        self.assertEqual((4, 1), (done["counts"]["inputs"], done["counts"]["outputs"]))
        self.assertEqual({"records": 12, "files": 1}, {key: done["totals"][key] for key in ("records", "files")})
        table = self.runtime.table()
        self.assertEqual(1, len(live_data_files(table)))
        self.assertEqual(before, self.runtime.rows())
        # The runtime's exactly-once accounting is carried through the replace.
        self.assertEqual({0: 11}, committed_offsets(table.metadata).offsets)
        self.assertEqual("test-maintenance", table.current_snapshot().summary.additional_properties["materializer.runtime"])

    def test_scheduled_compaction_leaves_the_partition_being_written_alone(self) -> None:
        async def scenario():
            await self.commit_batches(4, at=datetime.now(tz=UTC))
            return await self.maintenance.tick()

        found = run(scenario())
        self.assertIsNone(self.maintenance.table().job)
        self.assertEqual([], completed(found))
        self.assertEqual(4, len(live_data_files(self.runtime.table())))
        self.assertEqual(4, self.maintenance.table().small_files)

    def test_scheduled_compaction_takes_a_past_day_a_backfill_is_still_adding_to(self) -> None:
        async def scenario():
            # The day is long over, and every file was committed a moment ago.
            await self.commit_batches(4)
            await self.maintenance.tick()
            job = self.maintenance.table().job
            return job, await self.maintenance.settle()

        job, finished = run(scenario())
        self.assertEqual("2026-10-18", job.partition)
        (done,) = completed(finished)
        self.assertEqual("succeeded", done["outcome"])
        self.assertEqual(1, len(live_data_files(self.runtime.table())))

    def test_small_days_are_compacted_in_one_job(self) -> None:
        async def scenario():
            await self.commit_batches(3, at=datetime(2026, 9, 1, 12, tzinfo=UTC))
            await self.commit_batches(3, start=9, at=datetime(2026, 9, 2, 12, tzinfo=UTC))
            await self.maintenance.tick()
            return await self.maintenance.settle()

        (done,) = completed(run(scenario()))
        self.assertEqual("2026-09-01 … 2026-09-02 (2 partitions)", done["partition"])
        self.assertEqual((2, 6, 2), (done["counts"]["partitions"], done["counts"]["inputs"], done["counts"]["outputs"]))
        live = live_data_files(self.runtime.table())
        self.assertEqual(["2026-09-01", "2026-09-02"], sorted(item.partition_key for item in live))
        self.assertEqual(list(range(18)), [row["_kafka_offset"] for row in self.runtime.rows()])

    def test_a_backlog_is_worked_through_a_job_a_step_not_a_job_an_interval(self) -> None:
        async def scenario():
            await self.commit_batches(3, at=datetime(2026, 9, 1, 12, tzinfo=UTC))
            await self.commit_batches(3, start=9, at=datetime(2026, 9, 2, 12, tzinfo=UTC))
            # Each day fits a job, both together do not.
            days: dict[str, int] = {}
            for item in live_data_files(self.runtime.table()):
                days[item.partition_key] = days.get(item.partition_key, 0) + item.file_size
            self.runtime.configure(compaction_max_input_bytes=max(days.values()))
            await self.maintenance.tick()
            self.maintenance.only_compaction_due()
            # The step that commits the first starts the second, well inside the 15-minute interval.
            first = await self.maintenance.settle()
            second_job = self.maintenance.table().job
            second = await self.maintenance.settle()
            # Nothing is left, so the next step plans nothing and the backlog is over.
            third = await self.maintenance.tick()
            return first, second_job, second, third

        first, second_job, second, third = run(scenario())
        self.assertEqual(["2026-09-01"], [event["partition"] for event in completed(first)])
        self.assertIsNotNone(second_job)
        self.assertEqual(["2026-09-02"], [event["partition"] for event in completed(second)])
        self.assertEqual([], completed(third))
        self.assertIsNone(self.maintenance.table().job)
        self.assertFalse(self.maintenance.table().compaction_backlog)

    def test_a_compaction_backlog_does_not_hold_up_snapshot_expiry(self) -> None:
        async def scenario():
            await self.commit_batches(4)
            await self.maintenance.tick()
            await self.maintenance.settle()
            table = self.maintenance.table()
            table.compaction_backlog = True
            table.last_runs.pop("expire_snapshots", None)
            # Expiry is due, and goes before the backlog's next compaction.
            await self.maintenance.tick()
            return dict(table.last_runs)

        self.assertIn("expire_snapshots", run(scenario()))

    def test_a_compaction_writes_files_of_the_target_size_the_settings_name(self) -> None:
        jobs = []

        async def recording(job_file, job):
            jobs.append(json.loads(Path(job_file).read_text(encoding="utf-8")))
            return await compact_in_process(job_file, job)

        async def scenario():
            await self.commit_batches(3, at=datetime(2026, 9, 1, 12, tzinfo=UTC))
            with patch.object(TableMaintenance, "compactor", staticmethod(recording)):
                await self.maintenance.request("compaction")
                return await self.maintenance.settle()

        (done,) = completed(run(scenario()))
        self.assertEqual("succeeded", done["outcome"])
        self.assertEqual(self.runtime.config.compaction_target_file_size_bytes, jobs[0]["target_file_size_bytes"])

    def test_a_job_file_naming_a_directory_outside_the_work_root_is_refused(self) -> None:
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        escape = Path(outside.name) / "escaped"
        made = []

        async def tampered(job_file, job):
            made.append(job.work_dir)
            written = json.loads(Path(job_file).read_text(encoding="utf-8"))
            Path(job_file).write_text(json.dumps({**written, "work_dir": str(escape)}), encoding="utf-8")
            return await compact_in_process(job_file, job)

        async def scenario():
            await self.commit_batches(4)
            with patch.object(TableMaintenance, "compactor", staticmethod(tampered)):
                await self.maintenance.request("compaction")
                return await self.maintenance.settle()

        (failed,) = completed(run(scenario()))
        self.assertEqual("failed", failed["outcome"])
        self.assertIn("is not inside", failed["message"])
        self.assertFalse(escape.exists())
        self.assertEqual(4, len(live_data_files(self.runtime.table())))
        # The member's own directory lies inside the work root, and is gone once the job is.
        (work_dir,) = made
        self.assertEqual(os.path.realpath(compaction_work_root()), os.path.dirname(os.path.realpath(work_dir)))
        self.assertFalse(os.path.exists(work_dir))

    def test_a_failed_job_of_several_days_compacts_them_one_at_a_time(self) -> None:
        async def broken(job_file, job):
            raise RuntimeError("disk full")

        async def scenario():
            await self.commit_batches(3, at=datetime(2026, 9, 1, 12, tzinfo=UTC))
            await self.commit_batches(3, start=9, at=datetime(2026, 9, 2, 12, tzinfo=UTC))
            with patch.object(TableMaintenance, "compactor", staticmethod(broken)):
                await self.maintenance.request("compaction")
                failed = await self.maintenance.settle()
            solo = set(self.maintenance.table().solo)
            await self.maintenance.request("compaction")
            retried = await self.maintenance.settle()
            return failed, solo, retried

        failed, solo, retried = run(scenario())
        (failure,) = completed(failed)
        self.assertEqual("failed", failure["outcome"])
        self.assertIn("one at a time", failure["message"])
        self.assertEqual([], [event for event in failed if event["kind"] == "table_error"])
        self.assertEqual({"2026-09-01", "2026-09-02"}, solo)
        (retry,) = completed(retried)
        self.assertEqual(("succeeded", "2026-09-01"), (retry["outcome"], retry["partition"]))
        self.assertEqual({"2026-09-02"}, self.maintenance.table().solo)

    def test_a_requested_compaction_with_nothing_to_do_says_so(self) -> None:
        async def scenario():
            await self.commit_batches(1)
            return await self.maintenance.request("compaction")

        (skipped,) = completed(run(scenario()))
        self.assertEqual("skipped", skipped["outcome"])
        self.assertIn("3 or more files", skipped["message"])

    def test_the_runtime_keeps_committing_while_a_compaction_runs(self) -> None:
        """Two writers, one pointer: the runtime's append lands first, and the replace still commits."""

        async def scenario():
            await self.commit_batches(4)
            await self.maintenance.request("compaction")
            # More records arrive and commit before the compaction's replace lands.
            await self.commit_batches(1, start=12)
            finished = await self.maintenance.settle()
            return finished, self.runtime.rows()

        finished, rows = run(scenario())
        (done,) = completed(finished)
        self.assertEqual("succeeded", done["outcome"])
        self.assertEqual(list(range(15)), [row["_kafka_offset"] for row in rows])
        table = self.runtime.table()
        self.assertEqual(2, len(live_data_files(table)))
        self.assertEqual({0: 14}, committed_offsets(table.metadata).offsets)

    def test_failed_compactions_back_off_and_then_give_up_loudly(self) -> None:
        self.runtime.configure(compaction_max_attempts=1)

        async def broken(job_file, job):
            raise RuntimeError("disk full")

        async def scenario():
            await self.commit_batches(4)
            with patch.object(TableMaintenance, "compactor", staticmethod(broken)):
                await self.maintenance.request("compaction")
                return await self.maintenance.settle()

        found = run(scenario())
        (failed,) = completed(found)
        self.assertEqual("failed", failed["outcome"])
        self.assertIn("disk full", failed["message"])
        errors = [event["payload"] for event in found if event["kind"] == "table_error"]
        self.assertEqual(["compaction_failed"], [error["reason"] for error in errors])
        self.assertEqual(4, len(live_data_files(self.runtime.table())))

    def test_a_compaction_past_its_deadline_is_stopped(self) -> None:
        async def slow(job_file, job):
            await asyncio.sleep(3600)

        async def scenario():
            await self.commit_batches(4)
            with patch.object(TableMaintenance, "compactor", staticmethod(slow)):
                await self.maintenance.request("compaction")
                job = self.maintenance.table().job
                job.started_at -= 31 * 60
                found = events(await self.maintenance.worker.step())
                await asyncio.sleep(0)
                return found, job

        found, job = run(scenario())
        (failed,) = completed(found)
        self.assertIn("compaction_timeout_minutes", failed["message"])
        self.assertTrue(job.task.cancelled() or job.task.done())
        self.assertIsNone(self.maintenance.table().job)


class ExpiryAndOrphanTests(MaintenanceAppTestCase):
    def test_requested_expiry_reports_what_it_expired(self) -> None:
        async def scenario():
            await self.commit_batches(3)
            with patch.object(worker_module, "datetime") as clock:
                clock.now.return_value = datetime.now(tz=UTC) + timedelta(hours=100)
                self.runtime.configure(min_snapshots_to_keep=1)
                return await self.maintenance.request("expire_snapshots")

        (expired,) = completed(run(scenario()))
        self.assertEqual("succeeded", expired["outcome"])
        self.assertEqual(2, expired["counts"]["expired"])
        self.assertEqual(1, len(self.runtime.table().metadata.snapshots))
        self.assertEqual(9, len(self.runtime.rows()))

    def test_requested_expiry_with_nothing_old_enough_is_skipped(self) -> None:
        async def scenario():
            await self.commit_batches(1)
            return await self.maintenance.request("expire_snapshots")

        (skipped,) = completed(run(scenario()))
        self.assertEqual("skipped", skipped["outcome"])

    def test_orphan_cleanup_deletes_only_old_unreferenced_files(self) -> None:
        self.runtime.configure(orphan_cleanup_enabled=True)
        store = self.runtime.lake.store
        old = f"{LOCATION}/data/ts_day=2026-10-18/abandoned.parquet"
        young = f"{LOCATION}/data/ts_day=2026-10-18/uploading.parquet"
        store.clock = lambda: datetime.now(tz=UTC) - timedelta(hours=48)
        store.put(old, b"x")
        store.clock = lambda: datetime.now(tz=UTC)
        store.put(young, b"x")

        async def scenario():
            await self.commit_batches(1)
            started = await self.maintenance.request("orphan_cleanup")
            return started, await self.maintenance.settle()

        started, finished = run(scenario())
        self.assertEqual(["orphan_cleanup"], [event["payload"]["job"] for event in started if event["kind"] == "maintenance_started"])
        (cleaned,) = [run for run in completed(finished) if run["job"] == "orphan_cleanup"]
        self.assertEqual(1, cleaned["counts"]["deleted"])
        self.assertIsNone(store.get(old))
        self.assertIsNotNone(store.get(young))
        self.assertEqual(3, len(self.runtime.rows()))


class ScheduleTests(MaintenanceAppTestCase):
    def test_a_tick_says_when_each_job_last_ran(self) -> None:
        """A restarted member does not run again what its predecessor just did."""
        self.runtime.configure(orphan_cleanup_enabled=True)
        recently = now_text()

        async def scenario():
            await self.commit_batches(4, at=datetime.now(tz=UTC))
            return await self.maintenance.tick(
                last_runs={"compaction": recently, "expire_snapshots": recently, "orphan_cleanup": recently},
            )

        self.assertEqual([], run(scenario()))
        self.assertIsNone(self.maintenance.table().job)

    def test_a_job_the_record_names_that_nothing_runs_is_reported_abandoned(self) -> None:
        started = (datetime.now(tz=UTC) - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        member = worker_module.current_member_id()
        running = RunningMaintenance(job="compaction", started_at=started, partition="2026-10-18", member_id=member)

        async def scenario():
            await self.commit_batches(1)
            return await self.maintenance.tick(running=running)

        (abandoned,) = completed(run(scenario()))
        self.assertEqual(("compaction", "abandoned", started), (abandoned["job"], abandoned["outcome"], abandoned["started_at"]))

    def test_a_job_another_member_runs_is_left_alone_until_it_is_stale(self) -> None:
        async def scenario(started: str):
            running = RunningMaintenance(job="compaction", started_at=started, member_id="maintenance-1")
            return await self.maintenance.tick(running=running)

        recent = (datetime.now(tz=UTC) - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        self.assertEqual([], completed(run(scenario(recent))))
        stale = (datetime.now(tz=UTC) - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
        self.assertEqual(["abandoned"], [run["outcome"] for run in completed(run(scenario(stale)))])

    def test_a_job_this_member_ran_is_not_taken_for_an_abandoned_one(self) -> None:
        async def scenario():
            await self.commit_batches(4)
            started = await self.maintenance.request("compaction")
            began = next(event["payload"] for event in started if event["kind"] == "maintenance_started")
            await self.maintenance.settle()
            # The record still says it runs: its completion is on its way.
            running = RunningMaintenance(job="compaction", started_at=began["started_at"], member_id=worker_module.current_member_id())
            return await self.maintenance.tick(running=running)

        self.assertEqual([], completed(run(scenario())))

    def test_cancelling_stops_the_running_job_and_reports_it(self) -> None:
        async def slow(job_file, job):
            await asyncio.sleep(3600)

        async def scenario():
            await self.commit_batches(4)
            with patch.object(TableMaintenance, "compactor", staticmethod(slow)):
                await self.maintenance.request("compaction")
                job = self.maintenance.table().job
                outputs = self.maintenance.worker.handle(self.maintenance.message("maintenance_cancel", reason="table_dropping"))
                await asyncio.sleep(0)
                return events(outputs), job

        found, job = run(scenario())
        (cancelled,) = completed(found)
        self.assertEqual(("compaction", "cancelled", job.started_wall), (cancelled["job"], cancelled["outcome"], cancelled["started_at"]))
        self.assertTrue(job.task.cancelled() or job.task.done())
        self.assertEqual({}, self.maintenance.worker.tables)
        self.assertEqual(4, len(live_data_files(self.runtime.table())))

    def test_a_cancel_for_a_table_this_member_never_had_settles_the_record(self) -> None:
        started = (datetime.now(tz=UTC) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        running = RunningMaintenance(job="orphan_cleanup", started_at=started, member_id=worker_module.current_member_id())

        found = events(self.maintenance.worker.handle(self.maintenance.message("maintenance_cancel", running=running, reason="table_dropping")))

        self.assertEqual(["abandoned"], [run["outcome"] for run in completed(found)])

    def test_a_table_no_tick_names_any_more_is_forgotten(self) -> None:
        async def scenario():
            await self.commit_batches(1)
            await self.maintenance.tick()
            self.maintenance.table().last_seen -= get_maintenance_app_config().forget_after_seconds + 1
            await self.maintenance.worker.step()

        run(scenario())
        self.assertEqual({}, self.maintenance.worker.tables)

    def test_settings_that_do_not_fit_together_leave_the_previous_ones(self) -> None:
        previous = job_settings({"compaction_min_input_files": 4})

        settings = job_settings(
            {"compaction_small_file_bytes": 512 * 1024**2, "compaction_target_file_size_bytes": 256 * 1024**2},
            previous=previous,
        )

        self.assertIs(previous, settings)


class SharedSlotTests(MaintenanceAppTestCase):
    """One long job at a time across a member's tables, each table in its turn."""

    topics = TOPICS

    def setUp(self) -> None:
        super().setUp()
        self.gate: asyncio.Event | None = None

        async def gated(job_file, job):
            if self.gate is not None:
                await self.gate.wait()
            return await compact_in_process(job_file, job)

        compactor = patch.object(TableMaintenance, "compactor", staticmethod(gated))
        compactor.start()
        self.addCleanup(compactor.stop)

    async def commit_files(self, count: int) -> None:
        """Commit `count` files of three rows to every table."""
        offset = 0
        for _ in range(count):
            for _ in range(3):
                await self.runtime.write({"order_id": f"O-{offset}", "amount": offset}, offset=offset)
                await self.runtime.write({"payment_id": f"P-{offset}", "amount": 1.5}, offset=offset, topic="payments")
                offset += 1
            await self.runtime.cycle()

    def busy(self) -> list[str]:
        return [table.target.table for table in self.maintenance.worker.tables.values() if table.busy]

    async def finish(self) -> list[dict]:
        self.gate.set()
        found = await self.maintenance.settle()
        self.gate.clear()
        return completed(found)

    def test_a_compaction_of_every_table_runs_one_table_after_the_other(self) -> None:
        async def scenario():
            self.gate = asyncio.Event()
            await self.commit_files(4)
            await self.maintenance.tick_all()
            for name in TOPICS:
                self.maintenance.only_compaction_due(name)
            first = self.busy()
            # Ticked again while the first runs: the second table waits its turn.
            await self.maintenance.tick_all()
            still = self.busy()
            first_done = await self.finish()
            second = self.busy()
            second_done = await self.finish()
            return first, still, first_done, second, second_done

        first, still, first_done, second, second_done = run(scenario())
        self.assertEqual((["orders"], ["orders"]), (first, still))
        self.assertEqual([("compaction", "succeeded")], [(run["job"], run["outcome"]) for run in first_done])
        # The step that commits the first table's job starts the second's.
        self.assertEqual(["payments"], second)
        self.assertEqual(["succeeded"], [run["outcome"] for run in second_done])
        for name in TOPICS:
            self.assertEqual(1, len(live_data_files(self.runtime.table(name))))
            self.assertEqual(12, len(self.runtime.rows(name)))

    def test_a_request_naming_a_table_runs_on_that_table_only(self) -> None:
        async def scenario():
            self.gate = asyncio.Event()
            await self.commit_files(4)
            for name in TOPICS:
                self.maintenance.worker.handle(self.maintenance.message("maintenance_tick", name))
                # As if every scheduled job had just run: only the request can start one.
                self.maintenance.table(name).last_runs.update(dict.fromkeys(("compaction", "expire_snapshots", "orphan_cleanup"), time.time()))
            await self.maintenance.request("compaction", "payments")
            running = self.busy()
            done = await self.finish()
            return running, done, self.busy()

        running, done, after = run(scenario())
        self.assertEqual(["payments"], running)
        self.assertEqual(1, len(done))
        self.assertEqual([], after)
        self.assertEqual(4, len(live_data_files(self.runtime.table("orders"))))
        self.assertEqual(1, len(live_data_files(self.runtime.table("payments"))))

    def test_more_slots_run_more_tables_at_once(self) -> None:
        self.maintenance.worker = MaintenanceWorker(
            client=self.runtime.lake.client,
            app=MaintenanceAppConfig(max_concurrent_jobs=2, app_name="test-maintenance"),
        )

        async def scenario():
            self.gate = asyncio.Event()
            await self.commit_files(4)
            await self.maintenance.tick_all()
            busy = self.busy()
            await self.finish()
            return busy

        self.assertEqual(sorted(TOPICS), sorted(run(scenario())))


class HandlerTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_worker()

    def test_a_malformed_message_is_dropped(self) -> None:
        with self.assertLogs("materializer_control_plane.maintenance_app.handlers", level="WARNING"):
            self.assertEqual([], list(handle_maintenance_message({"kind": "maintenance_tick"})))

    def test_messages_reach_the_members_worker_and_the_step_drives_it(self) -> None:
        reset_worker(MaintenanceWorker(app=MaintenanceAppConfig(app_name="test-maintenance")))
        target = MaintenanceTarget(
            materialization_id=MATERIALIZATION_ID,
            namespace=NAMESPACE,
            table="orders",
            location=LOCATION,
            source_topic="orders",
        )
        message = MaintenanceMessage(kind="maintenance_cancel", target=target, issued_at=now_text(), reason="table_dropping")

        self.assertEqual([], list(handle_maintenance_message(message.to_dict())))
        self.assertEqual([], list(asyncio.run(advance_maintenance())))

    def test_a_stopping_member_stops_its_jobs(self) -> None:
        worker = MaintenanceWorker(app=MaintenanceAppConfig(app_name="test-maintenance"))
        reset_worker(worker)
        with patch.object(worker, "stop") as stop:
            asyncio.run(advance_maintenance(app=type("App", (), {"should_stop": True})()))
        stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
