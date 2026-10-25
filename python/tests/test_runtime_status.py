"""What each runtime member reports on its heartbeat (spec §7.12)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys
import unittest
from dataclasses import replace

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import MATERIALIZATION_ID  # noqa: E402
from runtime_support import FakeTable, RuntimeHarness, run  # noqa: E402

from materializer_control_plane.runtime_app.handlers.status import build_runtime_status_snapshot  # noqa: E402
from materializer_control_plane.runtime_app.state import get_partition_state, update_pending_batch  # noqa: E402


class RuntimeStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeHarness({"order_id": "O-0"})
        self.addCleanup(self.runtime.close)

    def snapshot(self, state_table=None) -> dict:
        return build_runtime_status_snapshot(
            self.runtime.state_table if state_table is None else state_table,
            materialization_id=MATERIALIZATION_ID,
            tables=["orders"],
        )

    def test_a_member_without_its_table_says_it_is_loading_it(self) -> None:
        snapshot = self.snapshot(FakeTable())
        self.assertEqual("waiting_for_table", snapshot["phase_reason"])
        self.assertEqual("unknown", snapshot["health"])
        self.assertTrue(snapshot["runtime_version"])

    def test_a_member_with_its_table_and_no_records_waits_for_them(self) -> None:
        run(self.runtime.deliver([], tick=True))
        snapshot = self.snapshot()
        self.assertEqual("waiting_for_source_records", snapshot["phase_reason"])
        self.assertEqual("healthy", snapshot["health"])
        self.assertEqual({"orders": 0}, snapshot["schema_versions"])

    def test_a_working_member_reports_its_counters(self) -> None:
        async def scenario():
            await self.runtime.write({"order_id": "O-0", "coupon": "X"}, offset=0)
            await self.runtime.write({"order_id": "O-1"}, offset=1)
            self.runtime.configure(seal_schema_wait_seconds=0)
            await self.runtime.write({"order_id": "O-2"}, offset=2)
            await self.runtime.cycle()

        run(scenario())
        snapshot = self.snapshot()
        self.assertEqual("processing", snapshot["phase_reason"])
        self.assertEqual({"orders": {"0": 2}}, snapshot["latest_offsets"])
        self.assertEqual({"orders": {"0": 2}}, snapshot["latest_committed_offsets"])
        self.assertEqual(3, snapshot["rows_written"])
        self.assertEqual(1, snapshot["unmapped_values"])
        self.assertEqual(0, snapshot["files_pending_commit"])
        self.assertEqual(3, snapshot["table_counters"]["orders"]["rows_written"])
        self.assertEqual(1, snapshot["table_counters"]["orders"]["unmapped_values"])

    def test_files_uploaded_long_ago_and_never_committed_report_stalled_commits(self) -> None:
        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            await self.runtime.pipeline()

        run(scenario())
        state = get_partition_state("orders", 0)
        (batch,) = state.pending_batches.values()
        long_ago = (datetime.now(tz=UTC) - timedelta(hours=1)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        update_pending_batch(replace(batch, rolled_at=long_ago))
        snapshot = self.snapshot()
        self.assertEqual("commits_stalled", snapshot["phase_reason"])
        self.assertEqual("degraded", snapshot["health"])
        self.assertEqual(1, snapshot["files_pending_commit"])

    def test_a_partition_error_degrades_the_member(self) -> None:
        async def scenario():
            await self.runtime.write({"order_id": "O-0"}, offset=0)

        run(scenario())
        get_partition_state("orders", 0).last_error = "Uploading failed: 503"
        snapshot = self.snapshot()
        self.assertEqual("degraded", snapshot["health"])
        self.assertEqual("Uploading failed: 503", snapshot["last_error"])

    def test_a_member_reports_only_the_partitions_it_owns(self) -> None:
        async def scenario():
            await self.runtime.write({"order_id": "O-0"}, offset=0)

        run(scenario())
        get_partition_state("orders", 0).owner_member_id = "member-9"
        self.assertEqual({}, self.snapshot()["latest_offsets"])


if __name__ == "__main__":
    unittest.main()
