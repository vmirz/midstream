from __future__ import annotations

import os
import asyncio
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import replace
import itertools
import threading
import time
import unittest
from unittest.mock import patch
import zlib

from materializer_control_plane.config import ControlPlaneConfig
from materializer_control_plane.handlers.maintenance import (
    build_consumer_lag_refresh_trigger,
    build_purge_deleted_trigger,
    build_table_maintenance_trigger,
    handle_maintenance_work_async,
)
from materializer_control_plane.handlers.resource_observation import (
    build_sweep_observation_trigger,
    handle_observation_work_for_output,
)
from materializer_control_plane.models import (
    ConsumerLag,
    Materialization,
    MaterializationSchema,
    MaterializationStatus,
    MaterializationWorkEvent,
    ResourceObservation,
    ResourceCondition,
    RuntimeBinding,
    RuntimeHealth,
    RuntimeProgress,
    RuntimeStatusEvent,
)
from materializer_control_plane.kafka.metadata import TopicMetadata
from materializer_control_plane.observation.progress import merge_member_progress
from materializer_control_plane.scale import (
    InvalidScaleError,
    ScaleSizingRules,
    assignable_members,
    normalize_replicas,
    normalize_storage_size,
    recommend_combined_scale,
    recommend_replicas,
    recommend_scale,
    storage_size_to_bytes,
)
from materializer_control_plane.services.consumer_lag import (
    record_consumer_lag,
    refresh_consumer_lag,
    refresh_consumer_lag_async,
)
from materializer_control_plane.services.observation import apply_runtime_status_event
from materializer_control_plane.services.retention import (
    expired_materialization_purges,
    find_purge_candidates,
    purge_materialization_records,
)
from materializer_control_plane.services.sweep_cursor import SweepCursor
from materializer_control_plane.services.topic_analysis import analyze_topics, analyze_topics_async
from materializer_control_plane.services.workflow import process_work_event
from materializer_control_plane.web.errors import WebError
from materializer_control_plane.web.topics import analyze_topics_view, analyze_topics_view_async
from materializer_control_plane.storage import (
    get_materialization,
    get_materialization_status,
    list_materializations,
    materialization_routing_key,
    materialization_status_key,
    save_materialization,
    save_materialization_schema,
    save_materialization_status,
    save_materialization_table,
    save_operation_record,
    save_runtime_binding,
)
from materializer_control_plane.storage import objects
from materializer_control_plane.storage.objects import (
    CLEANUP_PASS_MAX_OBJECTS,
    DELETE_BATCH_SIZE,
    DestinationDeleteError,
    drop_table_objects,
    drop_table_objects_pass,
    drop_table_objects_pass_async,
    retire_table_pointer,
    table_object_prefixes,
)
from table_records import build_table_record, table_specs
from materializer_control_plane import config as control_plane_config


class FakeTable:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}

    def get(self, key: str) -> dict | None:
        return self.values.get(key)

    def __setitem__(self, key: str, value: dict) -> None:
        self.values[key] = value

    def __delitem__(self, key: str) -> None:
        self.values.pop(key, None)

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict]]:
        for key in sorted(self.values):
            if key.startswith(prefix):
                yield key, self.values[key]


class ShardedTable:
    """The control plane's global `state` table as Kaspr keeps it, for routing tests.

    Every member reads every shard, but a write or a delete goes to the changelog shard of the event
    being handled: `handling(key)` stands for handling an event with that key. A write outside one
    fails, as Faust refuses one outside a stream. Deletes are tombstones in the handled shard only,
    which is what survives once the store is rebuilt from its changelog.
    """

    def __init__(self, shards: int = 3) -> None:
        self.shards: list[dict[str, dict]] = [{} for _ in range(shards)]
        self.current: int | None = None

    def shard_of(self, event_key: str) -> int:
        return zlib.crc32(event_key.encode()) % len(self.shards)

    @contextmanager
    def handling(self, event_key: str) -> Iterator[None]:
        previous, self.current = self.current, self.shard_of(event_key)
        try:
            yield
        finally:
            self.current = previous

    def get(self, key: str) -> dict | None:
        for shard in self.shards:
            if key in shard:
                return shard[key]
        return None

    def __setitem__(self, key: str, value: dict) -> None:
        self._handled()[key] = value

    def __delitem__(self, key: str) -> None:
        shard = self._handled()
        if key not in shard and self.get(key) is None:
            raise KeyError(key)
        shard.pop(key, None)

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict]]:
        for shard in self.shards:
            for key in sorted(shard):
                if key.startswith(prefix):
                    yield key, shard[key]

    def holders(self, key: str) -> list[int]:
        """The shards that hold a copy of `key`."""
        return [index for index, shard in enumerate(self.shards) if key in shard]

    def _handled(self) -> dict[str, dict]:
        if self.current is None:
            raise RuntimeError("Cannot modify table outside of agent/stream.")
        return self.shards[self.current]


def build_materialization(
    materialization_id: str,
    *,
    desired_phase: str = "running",
    source_topics: tuple[str, ...] = ("orders",),
    updated_at: str = "2026-07-12T00:00:00Z",
    storage_size: str = "1Gi",
    replicas: int = 1,
) -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        namespace="midstream",
        name="shop",
        tables=table_specs(*source_topics),
        destination_uri="gs://lake/materialized",
        desired_phase=desired_phase,
        created_at="2026-07-12T00:00:00Z",
        updated_at=updated_at,
        storage_size=storage_size,
        replicas=replicas,
    )


def build_status(
    materialization_id: str,
    *,
    desired_phase: str = "running",
    observed_phase: str = "running",
    last_transition_time: str = "2026-07-12T00:00:00Z",
    runtime_progress: RuntimeProgress | None = None,
) -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id=materialization_id,
        desired_phase=desired_phase,
        observed_phase=observed_phase,
        phase_reason=observed_phase,
        last_transition_time=last_transition_time,
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="healthy"),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name=f"kaspr-{materialization_id}",
            resource_exists=True,
            managed_agent_states={},
        ),
        runtime_progress=RuntimeProgress() if runtime_progress is None else runtime_progress,
    )


class ScaleRuleTests(unittest.TestCase):
    def test_topics_with_as_many_partitions_share_members(self) -> None:
        self.assertEqual(40, assignable_members([32, 32, 8]))
        self.assertEqual(12, assignable_members([4, 8]))
        self.assertEqual(6, assignable_members([6]))
        self.assertEqual(0, assignable_members([0]))

    def test_one_runtime_for_several_topics_is_sized_for_them_together(self) -> None:
        # 8 + 8 partitions: 8 replicas by the ratio, and 8 members are all the assignor keeps busy.
        combined = recommend_combined_scale([8, 8])
        self.assertEqual(8, combined.replicas)
        self.assertEqual(recommend_scale(partitions=16).replicas, 8)
        # 12 + 4 partitions: 8 by the ratio, within the 16 members that can be busy.
        self.assertEqual(8, recommend_combined_scale([12, 4]).replicas)
        # Small topics stay on one replica.
        small = recommend_combined_scale([1, 2])
        self.assertEqual(1, small.replicas)
        self.assertIn("2 topic(s), 3 partition(s) together", small.rationale)
        self.assertEqual({"replicas": 1, "rationale": small.rationale}, small.to_dict())

    def test_replica_ratio_matches_deployment_convention(self) -> None:
        self.assertEqual(recommend_replicas(partitions=32), 16)
        self.assertEqual(recommend_replicas(partitions=4), 1)
        self.assertEqual(recommend_replicas(partitions=3), 1)
        self.assertEqual(recommend_replicas(partitions=1), 1)
        self.assertEqual(recommend_replicas(partitions=8), 4)
        self.assertEqual(recommend_replicas(partitions=5), 3)

    def test_replica_recommendation_respects_max_replicas(self) -> None:
        rules = ScaleSizingRules(max_replicas=8)
        self.assertEqual(recommend_replicas(partitions=64, rules=rules), 8)

    def test_a_recommendation_is_replicas_alone(self) -> None:
        # A runtime member's volume holds its state table, whatever the topic holds: the
        # deployment sizes it, so there is no storage to recommend.
        recommendation = recommend_scale(partitions=32)
        self.assertEqual(recommendation.replicas, 16)
        self.assertEqual(["rationale", "replicas"], sorted(recommendation.to_dict()))
        self.assertEqual("32 partition(s) -> 16 replica(s) (1 replica per 2 partitions above 4 partitions)", recommendation.rationale)

    def test_storage_size_validation(self) -> None:
        self.assertEqual(normalize_storage_size(None), "64Mi")
        self.assertEqual(normalize_storage_size("512Mi"), "512Mi")
        self.assertEqual(storage_size_to_bytes("1Gi"), 1024**3)
        with self.assertRaises(InvalidScaleError):
            normalize_storage_size("1 gigabyte")

    def test_replica_validation(self) -> None:
        self.assertEqual(normalize_replicas(None), 1)
        self.assertEqual(normalize_replicas("6"), 6)
        with self.assertRaises(InvalidScaleError):
            normalize_replicas(0)
        with self.assertRaises(InvalidScaleError):
            normalize_replicas(10, maximum=8)


class ListStatusFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        for materialization_id, phase in (("mat-1", "running"), ("mat-2", "deleted"), ("mat-3", "paused")):
            save_materialization(self.table, build_materialization(materialization_id, desired_phase=phase))
            save_materialization_status(
                self.table,
                build_status(materialization_id, desired_phase=phase, observed_phase=phase),
            )

    def test_all_phases_returned_by_default(self) -> None:
        payload = list_materializations(self.table, limit="all")
        self.assertEqual(payload["returned"], 3)
        self.assertEqual(payload["status"], "all")

    def test_single_status_filter(self) -> None:
        payload = list_materializations(self.table, limit="all", status="deleted")
        self.assertEqual([item["materialization_id"] for item in payload["items"]], ["mat-2"])
        self.assertEqual(payload["status"], "deleted")

    def test_multiple_status_filter(self) -> None:
        payload = list_materializations(self.table, limit="all", status="running,paused")
        self.assertEqual(
            [item["materialization_id"] for item in payload["items"]],
            ["mat-1", "mat-3"],
        )

    def test_limit_applies_after_filtering(self) -> None:
        payload = list_materializations(self.table, limit="10", status="running,paused")
        self.assertEqual(payload["returned"], 2)

    def test_unknown_status_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            list_materializations(self.table, status="nonsense")

    def test_list_items_include_scale(self) -> None:
        payload = list_materializations(self.table, limit="all", status="running")
        self.assertEqual(payload["items"][0]["storage_size"], "1Gi")
        self.assertEqual(payload["items"][0]["replicas"], 1)


class CumulativeProgressTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))

    def _event(self, *, member_id: str, rows: int, byte_count: int, reported_at: str) -> RuntimeStatusEvent:
        return RuntimeStatusEvent(
            event_id=f"evt-{member_id}-{reported_at}",
            materialization_id="mat-1",
            runtime_app_ref="kaspr-mat-1",
            member_id=member_id,
            agent_name="writer",
            phase="running",
            health="healthy",
            reported_at=reported_at,
            rows_written=rows,
            bytes_written=byte_count,
            latest_offsets={"orders": {"0": rows}},
        )

    def test_cumulative_totals_accumulate_across_heartbeats(self) -> None:
        apply_runtime_status_event(
            self.table,
            self._event(member_id="pod-0", rows=100, byte_count=1000, reported_at="2026-07-12T00:01:00Z"),
        )
        status = apply_runtime_status_event(
            self.table,
            self._event(member_id="pod-0", rows=250, byte_count=2500, reported_at="2026-07-12T00:02:00Z"),
        )
        self.assertEqual(status.runtime_progress.cumulative_rows_written, 250)
        self.assertEqual(status.runtime_progress.cumulative_bytes_written, 2500)

    def test_counter_reset_after_restart_does_not_lose_progress(self) -> None:
        apply_runtime_status_event(
            self.table,
            self._event(member_id="pod-0", rows=500, byte_count=5000, reported_at="2026-07-12T00:01:00Z"),
        )
        status = apply_runtime_status_event(
            self.table,
            self._event(member_id="pod-0", rows=20, byte_count=200, reported_at="2026-07-12T00:02:00Z"),
        )
        self.assertEqual(status.runtime_progress.cumulative_rows_written, 520)
        self.assertEqual(status.runtime_progress.cumulative_bytes_written, 5200)
        self.assertEqual(status.runtime_progress.rows_written, 20)

    def test_multiple_members_contribute_independently(self) -> None:
        apply_runtime_status_event(
            self.table,
            self._event(member_id="pod-0", rows=100, byte_count=1000, reported_at="2026-07-12T00:01:00Z"),
        )
        status = apply_runtime_status_event(
            self.table,
            self._event(member_id="pod-1", rows=300, byte_count=3000, reported_at="2026-07-12T00:01:00Z"),
        )
        self.assertEqual(status.runtime_progress.cumulative_rows_written, 400)
        self.assertEqual(len(status.runtime_progress.member_progress), 2)

    def test_member_map_is_bounded(self) -> None:
        progress = RuntimeProgress()
        for index in range(10):
            progress = merge_member_progress(
                progress,
                member_id=f"pod-{index}",
                rows_written=index,
                bytes_written=index,
                files_written=0,
                records_skipped=0,
                latest_offsets={},
                reported_at=f"2026-07-12T00:0{index}:00Z",
                member_limit=3,
            )
        self.assertEqual(len(progress.member_progress), 3)

    def test_cumulative_totals_survive_round_trip(self) -> None:
        status = apply_runtime_status_event(
            self.table,
            self._event(member_id="pod-0", rows=100, byte_count=1000, reported_at="2026-07-12T00:01:00Z"),
        )
        restored = MaterializationStatus.from_dict(status.to_dict())
        self.assertEqual(restored.runtime_progress.cumulative_rows_written, 100)
        self.assertEqual(restored.runtime_progress.member_progress["pod-0"].rows_written, 100)

    def _mark_ready(self) -> None:
        status = build_status("mat-1")
        save_materialization_status(
            self.table,
            replace(
                status,
                resource_observation=replace(
                    status.resource_observation,
                    ready_condition=ResourceCondition(
                        status="True", reason="Healthy", message=None, last_transition_time=None
                    ),
                ),
            ),
        )

    def _state_event(self, member_id: str, *, phase_reason: str, reported_at: str) -> RuntimeStatusEvent:
        payload = self._event(member_id=member_id, rows=1, byte_count=1, reported_at=reported_at).to_dict()
        payload["phase_reason"] = phase_reason
        payload["phase_message"] = f"{member_id} is {phase_reason}"
        return RuntimeStatusEvent.from_dict(payload)

    def test_status_describes_every_member_rather_than_the_last_to_report(self) -> None:
        """A mixed fleet must read the same whichever member happened to heartbeat last."""
        self._mark_ready()
        apply_runtime_status_event(
            self.table,
            self._state_event("pod-0", phase_reason="waiting_for_source_records", reported_at="2026-07-12T00:01:00Z"),
        )
        apply_runtime_status_event(
            self.table,
            self._state_event("pod-1", phase_reason="processing", reported_at="2026-07-12T00:01:10Z"),
        )
        status = apply_runtime_status_event(
            self.table,
            self._state_event("pod-2", phase_reason="waiting_for_source_records", reported_at="2026-07-12T00:01:20Z"),
        )

        self.assertEqual(status.phase_reason, "processing")
        self.assertEqual(
            status.phase_message,
            "Runtime members: 1 of 3 processing source records; 2 of 3 idle, waiting for source records.",
        )

    def test_a_runtime_loads_its_table_only_while_every_member_does(self) -> None:
        self._mark_ready()
        apply_runtime_status_event(
            self.table,
            self._state_event("pod-0", phase_reason="waiting_for_table", reported_at="2026-07-12T00:01:00Z"),
        )
        loading = apply_runtime_status_event(
            self.table,
            self._state_event("pod-1", phase_reason="waiting_for_table", reported_at="2026-07-12T00:01:10Z"),
        )
        working = apply_runtime_status_event(
            self.table,
            self._state_event("pod-1", phase_reason="processing", reported_at="2026-07-12T00:01:20Z"),
        )

        self.assertEqual(("deploying", "waiting_for_table"), (loading.observed_phase, loading.phase_reason))
        self.assertEqual("All 2 runtime members are loading tables.", loading.phase_message)
        self.assertEqual("processing", working.phase_reason)

    def test_silent_members_drop_out_of_the_status(self) -> None:
        self._mark_ready()
        apply_runtime_status_event(
            self.table,
            self._state_event("pod-0", phase_reason="waiting_for_table", reported_at="2026-07-12T00:01:00Z"),
        )
        status = apply_runtime_status_event(
            self.table,
            self._state_event("pod-1", phase_reason="processing", reported_at="2026-07-12T00:10:00Z"),
        )

        self.assertEqual(status.phase_reason, "processing")
        self.assertEqual(status.phase_message, "pod-1 is processing")

    def test_skip_reasons_accumulate_across_members_and_restarts(self) -> None:
        def skipped(member_id: str, count: int, offset: int, reported_at: str) -> RuntimeStatusEvent:
            payload = self._event(member_id=member_id, rows=1, byte_count=1, reported_at=reported_at).to_dict()
            payload["records_skipped"] = count
            payload["skip_reasons"] = [
                {
                    "category": "payload.tags[] must be an array",
                    "count": count,
                    "last_reason": "payload.tags[] must be an array",
                    "last_seen_at": reported_at,
                    "last_partition": 3,
                    "last_offset": offset,
                    "last_schema_version": 9,
                }
            ]
            return RuntimeStatusEvent.from_dict(payload)

        apply_runtime_status_event(self.table, skipped("pod-0", 5, 100, "2026-07-12T00:01:00Z"))
        apply_runtime_status_event(self.table, skipped("pod-0", 8, 140, "2026-07-12T00:01:30Z"))
        apply_runtime_status_event(self.table, skipped("pod-1", 2, 7, "2026-07-12T00:01:40Z"))
        # pod-0 restarted: its counter starts over, and all of the new reading is fresh.
        status = apply_runtime_status_event(self.table, skipped("pod-0", 1, 150, "2026-07-12T00:02:00Z"))

        reasons = MaterializationStatus.from_dict(status.to_dict()).runtime_progress.skip_reasons
        reason = reasons["payload.tags[] must be an array"]
        self.assertEqual(reason.count, 11)
        self.assertEqual(status.runtime_progress.cumulative_records_skipped, 11)
        self.assertEqual((reason.last_member_id, reason.last_offset), ("pod-0", 150))


class RetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.config = ControlPlaneConfig(deleted_retention_hours=24, deleted_purge_batch_limit=10)

    def _save_deleted(self, materialization_id: str, deleted_at: str) -> None:
        save_materialization(
            self.table,
            build_materialization(materialization_id, desired_phase="deleted", updated_at=deleted_at),
        )
        save_materialization_status(
            self.table,
            build_status(
                materialization_id,
                desired_phase="deleted",
                observed_phase="deleted",
                last_transition_time=deleted_at,
            ),
        )
        save_materialization_schema(
            self.table,
            MaterializationSchema(materialization_id=materialization_id, table="orders", schema_id=0, since=deleted_at),
        )
        save_materialization_table(
            self.table,
            build_table_record(build_materialization(materialization_id, desired_phase="deleted", updated_at=deleted_at)),
        )
        save_operation_record(
            self.table,
            materialization_id=materialization_id,
            operation_id="op-1",
            updated_at=deleted_at,
            payload={"status": "applied"},
        )

    def _purged(self, purges: list) -> list[str]:
        """Run each purge the sweep requested, as the workflow agent does; the ids it purged."""
        for purge in purges:
            purge_materialization_records(self.table, purge.materialization_id)
        return [purge.materialization_id for purge in purges]

    def test_expired_records_are_purged(self) -> None:
        self._save_deleted("mat-old", "2026-07-10T00:00:00Z")
        purges = expired_materialization_purges(
            self.table,
            now="2026-07-12T00:00:00Z",
            config=self.config,
        )

        # The sweep runs on the `__system__` event: it requests the purge and deletes nothing itself.
        self.assertEqual([(purge.step, purge.materialization_id) for purge in purges], [("purge_materialization_records", "mat-old")])
        self.assertIsNotNone(get_materialization(self.table, "mat-old"))
        self.assertEqual(self._purged(purges), ["mat-old"])
        self.assertIsNone(get_materialization(self.table, "mat-old"))
        self.assertIsNone(get_materialization_status(self.table, "mat-old"))
        self.assertEqual(self.table.values, {})

    def test_recent_deletions_are_retained(self) -> None:
        self._save_deleted("mat-new", "2026-07-11T23:00:00Z")
        purges = expired_materialization_purges(
            self.table,
            now="2026-07-12T00:00:00Z",
            config=self.config,
        )
        self.assertEqual(purges, [])
        self.assertIsNotNone(get_materialization(self.table, "mat-new"))

    def _save_unreadable(self, materialization_id: str, *, desired_phase: str, updated_at: str) -> None:
        """A materialization as 1.0 stored it: one `source_topic`, which 2.0 cannot read."""
        self.table[f"materialization:{materialization_id}"] = {
            "entity_type": "materialization",
            "entity_id": materialization_id,
            "version": 1,
            "updated_at": updated_at,
            "payload": {
                "materialization_id": materialization_id,
                "name": "orders",
                "source_topic": "orders",
                "destination_uri": "gs://lake/materialized",
                "desired_phase": desired_phase,
                "created_at": updated_at,
                "updated_at": updated_at,
            },
        }

    def test_a_deleted_record_this_version_cannot_read_is_purged(self) -> None:
        """A copy an earlier release stranded can outlive its deletion; nothing else would remove it."""
        self._save_unreadable("mat-legacy", desired_phase="deleted", updated_at="2026-07-10T00:00:00Z")
        self._save_unreadable("mat-recent", desired_phase="deleted", updated_at="2026-07-11T23:00:00Z")
        self._save_unreadable("mat-running", desired_phase="running", updated_at="2026-07-01T00:00:00Z")

        purges = expired_materialization_purges(self.table, now="2026-07-12T00:00:00Z", config=self.config)

        self.assertEqual(self._purged(purges), ["mat-legacy"])
        self.assertEqual(
            sorted(self.table.values),
            ["materialization:mat-recent", "materialization:mat-running"],
        )

    def test_a_record_in_several_shards_is_requested_once(self) -> None:
        self._save_deleted("mat-old", "2026-07-10T00:00:00Z")
        stranded = dict(self.table.values["materialization:mat-old"])
        table = ShardedTable()
        table.shards[0]["materialization:mat-old"] = stranded
        table.shards[1]["materialization:mat-old"] = stranded

        candidates = find_purge_candidates(table, now="2026-07-12T00:00:00Z", retention_hours=24, limit=10)

        self.assertEqual([candidate.materialization_id for candidate in candidates], ["mat-old"])

    def test_active_materializations_are_never_purged(self) -> None:
        save_materialization(self.table, build_materialization("mat-live", updated_at="2026-01-01T00:00:00Z"))
        save_materialization_status(
            self.table,
            build_status("mat-live", last_transition_time="2026-01-01T00:00:00Z"),
        )
        candidates = find_purge_candidates(
            self.table,
            now="2026-07-12T00:00:00Z",
            retention_hours=1,
            limit=10,
        )
        self.assertEqual(candidates, [])

    def test_purge_batch_is_bounded(self) -> None:
        for index in range(5):
            self._save_deleted(f"mat-{index}", "2026-07-01T00:00:00Z")
        purges = expired_materialization_purges(
            self.table,
            now="2026-07-12T00:00:00Z",
            config=ControlPlaneConfig(deleted_retention_hours=1, deleted_purge_batch_limit=2),
        )
        self.assertEqual(len(purges), 2)

    def _save_destination_cleanup(self, materialization_id: str, *, status: str, last_pass_at: str) -> None:
        save_operation_record(
            self.table,
            materialization_id=materialization_id,
            operation_id="work-drop-table",
            updated_at=last_pass_at,
            payload={
                "operation_type": "drop_table",
                "status": status,
                "deleted_count": 250_000,
                "passes": 250,
            },
        )

    def test_a_destination_cleanup_still_making_progress_keeps_its_records(self) -> None:
        """A large cleanup outlasts the retention window; purging its records mid-way would strand
        every object it had not reached, since they are how each pass finds the destination."""
        self._save_deleted("mat-big", "2026-07-01T00:00:00Z")
        self._save_destination_cleanup("mat-big", status="in_progress", last_pass_at="2026-07-11T23:59:50Z")

        purges = expired_materialization_purges(self.table, now="2026-07-12T00:00:00Z", config=self.config)

        self.assertEqual(purges, [])
        self.assertIsNotNone(get_materialization(self.table, "mat-big"))

    def test_a_destination_cleanup_that_stopped_is_collected_a_window_after_its_last_pass(self) -> None:
        self._save_deleted("mat-stalled", "2026-07-01T00:00:00Z")
        self._save_destination_cleanup("mat-stalled", status="in_progress", last_pass_at="2026-07-10T00:00:00Z")

        purges = expired_materialization_purges(self.table, now="2026-07-12T00:00:00Z", config=self.config)

        self.assertEqual(self._purged(purges), ["mat-stalled"])
        self.assertEqual(self.table.values, {})

    def test_a_finished_destination_cleanup_does_not_hold_back_the_purge(self) -> None:
        self._save_deleted("mat-done", "2026-07-01T00:00:00Z")
        self._save_destination_cleanup("mat-done", status="applied", last_pass_at="2026-07-11T23:59:50Z")

        purges = expired_materialization_purges(self.table, now="2026-07-12T00:00:00Z", config=self.config)

        self.assertEqual(self._purged(purges), ["mat-done"])


class NotFound(Exception):
    """Named as google-cloud-storage's own, which is how a missing object is recognised."""


class FakeBlob:
    def __init__(self, name: str, bucket: "FakeBucket | None" = None) -> None:
        self.name = name
        self._bucket = bucket

    def delete(self) -> None:
        if self._bucket is None or self.name not in self._bucket.blobs:
            raise NotFound(self.name)
        self._bucket.blobs.remove(self.name)
        self._bucket.deleted.append(self.name)


class FakeBucket:
    def __init__(self, blobs: list[str], *, delete_seconds: float = 0.0) -> None:
        self.blobs = blobs
        self.deleted: list[str] = []
        self.delete_seconds = delete_seconds
        self.threads: set[str] = set()

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(name, self)

    def list_blobs(self, *, prefix: str):
        self.threads.add(threading.current_thread().name)
        return [FakeBlob(name) for name in self.blobs if name.startswith(prefix)]

    def delete_blobs(self, blobs, on_error=None):
        self.threads.add(threading.current_thread().name)
        # Blocks the calling thread the way an HTTP request to GCS does.
        time.sleep(self.delete_seconds * len(blobs))
        names = [blob.name for blob in blobs]
        self.deleted.extend(names)
        # A deleted object leaves the listing, as in GCS.
        removed = set(names)
        self.blobs = [name for name in self.blobs if name not in removed]


class FakeObjectStoreClient:
    def __init__(self, bucket: FakeBucket) -> None:
        self._bucket = bucket
        self.requested_buckets: list[str] = []

    def bucket(self, bucket_name: str) -> FakeBucket:
        self.requested_buckets.append(bucket_name)
        return self._bucket


class FakeHttpError(Exception):
    """An object-store error carrying its HTTP status, as google-cloud-storage's errors do."""

    def __init__(self, code: int) -> None:
        super().__init__(f"{code} from GCS")
        self.code = code


class OneByOneBlob:
    def __init__(self, bucket: OneByOneBucket, name: str) -> None:
        self.bucket = bucket
        self.name = name

    def delete(self) -> None:
        error = self.bucket.errors.get(self.name)
        if error is not None and error.code == 404:
            self.bucket.blobs.remove(self.name)  # someone else deleted it first
        if error is not None:
            raise error
        self.bucket.blobs.remove(self.name)


class OneByOneBucket:
    """A bucket with no `delete_blobs`: each blob deletes itself, one request apiece."""

    def __init__(self, blobs: list[str], errors: dict[str, FakeHttpError] | None = None) -> None:
        self.blobs = blobs
        self.errors = errors or {}

    def blob(self, name: str) -> OneByOneBlob:
        if name not in self.blobs:
            missing = OneByOneBlob(self, name)
            missing.delete = lambda: (_ for _ in ()).throw(NotFound(name))  # type: ignore[method-assign]
            return missing
        return OneByOneBlob(self, name)

    def list_blobs(self, *, prefix: str):
        return [OneByOneBlob(self, name) for name in self.blobs if name.startswith(prefix)]


class StreamingBucket(FakeBucket):
    """A listing that yields as it is read, recording how many listed objects await deletion."""

    def __init__(self, blobs: list[str]) -> None:
        super().__init__(blobs)
        self.outstanding = 0
        self.most_outstanding = 0

    def list_blobs(self, *, prefix: str):
        for name in [name for name in self.blobs if name.startswith(prefix)]:
            self.outstanding += 1
            self.most_outstanding = max(self.most_outstanding, self.outstanding)
            yield FakeBlob(name)

    def delete_blobs(self, blobs, on_error=None):
        super().delete_blobs(blobs, on_error=on_error)
        self.outstanding -= len(blobs)


LOCATION = "gs://lake/materializer/mat-1/orders"
DATA = "materializer/mat-1/orders/data/"
METADATA = "materializer/mat-1/orders/metadata/"
POINTER = "materializer/mat-1/orders/metadata/catalog-pointer.json"


class TableDropTests(unittest.TestCase):
    def test_prefixes_are_the_table_data_then_metadata_and_the_pointer(self) -> None:
        self.assertEqual(table_object_prefixes(LOCATION, "mat-1"), ("lake", (DATA, METADATA), POINTER))

    def test_a_location_that_is_not_the_materialization_table_is_refused(self) -> None:
        for location, materialization_id in (
            ("gs://lake/materializer/mat-2/orders", "mat-1"),
            # A directory that only ends in the id's last part, as the layout before datasets named them.
            ("gs://lake/materializer/shop-1/orders", "mat-1"),
            ("gs://lake/materializer/xmat-1/orders", "mat-1"),
            # Table names are lowercase.
            ("gs://lake/materializer/mat-1/Orders", "mat-1"),
            # The materialization's directory itself, which holds every one of its tables.
            ("gs://lake/materializer/mat-1", "mat-1"),
            ("gs://lake/materializer/mat-1/", "mat-1"),
            ("gs://lake/materializer/mat-1/orders/data", "mat-1"),
            ("gs://lake/materializer/mat-1/..", "mat-1"),
            ("gs://lake", "mat-1"),
            (LOCATION, ""),
            ("s3://lake/materializer/mat-1/orders", "mat-1"),
        ):
            with self.subTest(location=location, materialization_id=materialization_id), self.assertRaises(ValueError):
                table_object_prefixes(location, materialization_id)

    def test_only_the_table_objects_are_deleted_and_the_pointer_goes_last(self) -> None:
        other = "materializer/mat-1/payments/data/ts_day=2026-10-18/p00.parquet"
        bucket = FakeBucket(
            [
                f"{DATA}ts_day=2026-10-18/p00.parquet",
                f"{METADATA}00000-a.metadata.json",
                POINTER,
                other,
            ]
        )
        result = drop_table_objects(location=LOCATION, materialization_id="mat-1", client=FakeObjectStoreClient(bucket))
        self.assertTrue(result.complete)
        self.assertEqual(result.deleted_count, 3)
        self.assertEqual(bucket.blobs, [other])
        self.assertEqual(bucket.deleted[-1], POINTER)

    def _owned(self, count: int, *, prefix: str = DATA) -> list[str]:
        return [f"{prefix}part-{index:05d}.parquet" for index in range(count)]

    def _pass(self, bucket, **kwargs):
        return drop_table_objects_pass(location=LOCATION, materialization_id="mat-1", client=FakeObjectStoreClient(bucket), **kwargs)

    def test_a_pass_stops_at_its_object_budget_and_keeps_the_pointer_until_the_end(self) -> None:
        manifest = f"{METADATA}snap-1-a.avro"
        bucket = FakeBucket(self._owned(250) + [manifest, POINTER])

        passes = []
        while not passes or not passes[-1].complete:
            passes.append(self._pass(bucket, max_objects=100, max_seconds=None))
            if not passes[-1].complete:
                self.assertIn(POINTER, bucket.blobs)

        # The budget spans prefixes: the last pass finishes the data, the metadata and the pointer.
        self.assertEqual([(result.deleted_count, result.complete) for result in passes], [(100, False), (100, False), (52, True)])
        self.assertEqual(bucket.blobs, [])

    def test_a_pass_that_fills_its_budget_exactly_leaves_the_next_to_confirm_nothing_is_left(self) -> None:
        bucket = FakeBucket(self._owned(100))

        first = self._pass(bucket, max_objects=100, max_seconds=None)
        second = self._pass(bucket, max_objects=100, max_seconds=None)

        self.assertEqual((first.deleted_count, first.complete), (100, False))
        self.assertEqual((second.deleted_count, second.complete), (0, True))

    def test_a_pass_stops_at_its_deadline_once_it_has_deleted_a_batch(self) -> None:
        bucket = FakeBucket(self._owned(350))
        clock = iter([0.0] + [99.0] * 10).__next__  # the pass starts, then every reading is late

        result = self._pass(bucket, max_seconds=10, clock=clock)

        self.assertFalse(result.complete)
        self.assertEqual(result.deleted_count, DELETE_BATCH_SIZE)
        self.assertEqual(len(bucket.blobs), 350 - DELETE_BATCH_SIZE)

    def test_every_pass_makes_progress_however_slow_the_bucket(self) -> None:
        """A pass that runs out of time has always deleted something, so the chain terminates."""
        bucket = FakeBucket(self._owned(250))
        clock = itertools.count(0, 100).__next__  # every reading is past the previous deadline

        deleted = []
        while not deleted or deleted[-1][1] is False:
            result = self._pass(bucket, max_seconds=10, clock=clock)
            deleted.append((result.deleted_count, result.complete))
            self.assertLess(len(deleted), 10)

        self.assertEqual(deleted, [(100, False), (100, False), (50, False), (0, True)])
        self.assertEqual(bucket.blobs, [])

    def test_an_empty_table_is_complete_in_one_pass(self) -> None:
        result = self._pass(FakeBucket([]))

        self.assertTrue(result.complete)
        self.assertEqual(result.deleted_count, 0)
        self.assertEqual(result.bucket, "lake")

    def test_pass_budgets_must_be_positive(self) -> None:
        for kwargs in ({"max_objects": 0}, {"max_seconds": 0}):
            with self.subTest(**kwargs), self.assertRaises(ValueError):
                self._pass(FakeBucket([]), **kwargs)

    def test_the_async_pass_deletes_on_its_own_thread_while_the_loop_keeps_running(self) -> None:
        bucket = FakeBucket(self._owned(200), delete_seconds=0.002)

        async def scenario():
            ticks = 0
            done = asyncio.Event()

            async def ticker() -> None:
                nonlocal ticks
                while not done.is_set():
                    await asyncio.sleep(0.01)
                    ticks += 1

            ticking = asyncio.create_task(ticker())
            result = await drop_table_objects_pass_async(
                location=LOCATION,
                materialization_id="mat-1",
                client=FakeObjectStoreClient(bucket),
            )
            done.set()
            await ticking
            return result, ticks

        result, ticks = asyncio.run(scenario())

        self.assertTrue(result.complete)
        self.assertEqual(result.deleted_count, 200)
        # The deletes block their thread for 0.4 s; run on the loop, it could not tick meanwhile.
        self.assertGreaterEqual(ticks, 5)
        self.assertTrue(all(name.startswith("destination-cleanup") for name in bucket.threads), bucket.threads)

    def test_a_client_that_cannot_batch_keeps_the_per_object_budget(self) -> None:
        bucket = FakeBucket(self._owned(CLEANUP_PASS_MAX_OBJECTS + 1))

        result = self._pass(bucket, max_seconds=None)

        self.assertEqual((result.deleted_count, result.complete), (CLEANUP_PASS_MAX_OBJECTS, False))

    def test_the_listing_is_read_a_chunk_at_a_time(self) -> None:
        """However large the budget, a pass holds one chunk of listed objects, not the listing."""
        bucket = StreamingBucket(self._owned(2_500))

        result = self._pass(bucket, max_objects=2_000, max_seconds=None)

        self.assertEqual((result.deleted_count, result.complete), (2_000, False))
        self.assertEqual(bucket.most_outstanding, DELETE_BATCH_SIZE)

    def test_without_delete_blobs_an_object_already_gone_counts_as_deleted(self) -> None:
        names = self._owned(5)
        bucket = OneByOneBucket(list(names), errors={names[2]: FakeHttpError(404)})

        result = self._pass(bucket)

        self.assertEqual((result.deleted_count, result.complete), (5, True))
        self.assertEqual(bucket.blobs, [])

    def test_without_delete_blobs_a_refused_object_is_left_and_reported(self) -> None:
        """An object left behind must not let the chain call the table gone."""
        names = self._owned(150)
        bucket = OneByOneBucket(list(names) + [POINTER], errors={names[0]: FakeHttpError(403)})

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self._pass(bucket)

        self.assertIn("could not delete 1 object from gs://lake (403: 1)", logs.output[0])
        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (149, 1, False))
        # The pointer stays while anything else is left.
        self.assertIn(POINTER, bucket.blobs)
        with self.assertRaises(DestinationDeleteError) as raised:
            self._pass(bucket)
        self.assertIsInstance(raised.exception.__cause__, FakeHttpError)
        self.assertIn(names[0], str(raised.exception))

    def test_a_pass_that_fails_after_deleting_reports_what_it_deleted(self) -> None:
        manifest = f"{METADATA}snap-1-a.avro"

        class MetadataFailsToList(FakeBucket):
            def list_blobs(self, *, prefix: str):
                if prefix == METADATA:
                    raise FakeHttpError(503)
                return super().list_blobs(prefix=prefix)

        bucket = MetadataFailsToList(self._owned(150) + [manifest])

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self._pass(bucket)

        self.assertIn("stopped after deleting 150 objects", logs.output[0])
        self.assertEqual((result.deleted_count, result.complete), (150, False))
        with self.assertRaises(FakeHttpError):
            self._pass(bucket)
        self.assertEqual(bucket.blobs, [manifest])

    def test_a_table_kept_on_delete_only_leaves_the_catalog(self) -> None:
        from gcs_fakes import FakeGcs

        store = FakeGcs()
        store.put(f"{LOCATION}/metadata/catalog-pointer.json", b'{"format": 1}')
        store.put(f"{LOCATION}/data/ts_day=2026-10-18/p00.parquet", b"rows")

        result = retire_table_pointer(location=LOCATION, materialization_id="mat-1", client=store.client())

        self.assertEqual("destination_cleanup_disabled", result.skipped_reason)
        self.assertIsNone(store.get(f"{LOCATION}/metadata/catalog-pointer.json"))
        self.assertEqual(b'{"format": 1}', store.get(f"{LOCATION}/metadata/catalog-pointer.dropped.json"))
        self.assertEqual(b"rows", store.get(f"{LOCATION}/data/ts_day=2026-10-18/p00.parquet"))


class FakeMetadataClient:
    def __init__(
        self,
        topics: dict[str, TopicMetadata],
        group_offsets: dict[str, dict[str, dict[int, int]]] | None = None,
    ) -> None:
        self.topics = topics
        self.group_offsets = group_offsets or {}
        self.describe_calls: list[list[str]] = []
        self.group_calls: list[str] = []

    def describe_topics(self, topics: list[str]) -> dict[str, TopicMetadata]:
        self.describe_calls.append(list(topics))
        return {topic: self.topics[topic] for topic in topics if topic in self.topics}

    def consumer_group_offsets(self, group_id: str) -> dict[str, dict[int, int]]:
        self.group_calls.append(group_id)
        return self.group_offsets.get(group_id, {})


class TopicAnalysisTests(unittest.TestCase):
    def test_existing_topic_returns_partitions_and_recommendation(self) -> None:
        metadata_client = FakeMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=32,
                    beginning_offsets={index: 0 for index in range(32)},
                    end_offsets={index: 1000 for index in range(32)},
                )
            }
        )
        payload = analyze_topics(["orders"], metadata_client=metadata_client)
        item = payload["items"][0]
        self.assertTrue(item["exists"])
        self.assertEqual(item["partitions"], 32)
        self.assertEqual(item["estimated_records"], 32_000)
        self.assertEqual(item["recommendation"]["replicas"], 16)
        self.assertEqual(payload["valid"], 1)

    def test_missing_topic_is_reported_without_recommendation(self) -> None:
        metadata_client = FakeMetadataClient({})
        payload = analyze_topics(["ghost"], metadata_client=metadata_client)
        item = payload["items"][0]
        self.assertFalse(item["exists"])
        self.assertNotIn("recommendation", item)
        self.assertEqual(payload["valid"], 0)

    def test_topics_are_described_in_one_batched_call(self) -> None:
        metadata_client = FakeMetadataClient(
            {
                "a": TopicMetadata(name="a", exists=True, partitions=4, end_offsets={0: 1}),
                "b": TopicMetadata(name="b", exists=True, partitions=8, end_offsets={0: 1}),
            }
        )
        analyze_topics(["a", "b", "a"], metadata_client=metadata_client)
        self.assertEqual(metadata_client.describe_calls, [["a", "b"]])

    def test_several_topics_get_a_recommendation_for_one_runtime_reading_them_all(self) -> None:
        metadata_client = FakeMetadataClient(
            {
                "a": TopicMetadata(name="a", exists=True, partitions=8, end_offsets={0: 1}),
                "b": TopicMetadata(name="b", exists=True, partitions=8, end_offsets={0: 1}),
            }
        )

        payload = analyze_topics(["a", "b", "ghost"], metadata_client=metadata_client)

        self.assertEqual(8, payload["combined"]["replicas"])
        self.assertEqual(4, payload["items"][0]["recommendation"]["replicas"])
        self.assertNotIn("combined", analyze_topics(["a", "ghost"], metadata_client=metadata_client))

    def test_empty_topic_list_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            analyze_topics([], metadata_client=FakeMetadataClient({}))

    def test_topic_count_is_bounded(self) -> None:
        config = ControlPlaneConfig(topic_analysis_max_topics=2)
        with self.assertRaises(ValueError):
            analyze_topics(["a", "b", "c"], metadata_client=FakeMetadataClient({}), config=config)

    def _orders_client(self) -> AwaitingMetadataClient:
        return AwaitingMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=32,
                    beginning_offsets={index: 0 for index in range(32)},
                    end_offsets={index: 1000 for index in range(32)},
                )
            }
        )

    def test_async_analysis_answers_as_the_sync_one_does(self) -> None:
        metadata_client = self._orders_client()

        awaited = asyncio.run(analyze_topics_async(["orders", "ghost", "orders"], metadata_client=metadata_client))

        self.assertEqual(awaited, analyze_topics(["orders", "ghost"], metadata_client=metadata_client))
        self.assertEqual(metadata_client.describe_calls, [["orders", "ghost"], ["orders", "ghost"]])

    def test_async_analysis_refuses_bad_topics_before_asking_kafka(self) -> None:
        metadata_client = self._orders_client()
        config = ControlPlaneConfig(topic_analysis_max_topics=1)

        for topics in ([], ["a", "b"], "", [7]):
            with self.subTest(topics=topics), self.assertRaises(ValueError):
                asyncio.run(analyze_topics_async(topics, metadata_client=metadata_client, config=config))
        self.assertEqual(metadata_client.describe_calls, [])

    def test_an_analysis_kafka_does_not_answer_in_time_is_abandoned(self) -> None:
        metadata_client = self._orders_client()
        metadata_client.hang = True

        with patch("materializer_control_plane.services.topic_analysis.ANALYZE_TIMEOUT_SECONDS", 0.05):
            with self.assertRaisesRegex(TimeoutError, r"^Kafka did not answer within 0.05s$"):
                asyncio.run(analyze_topics_async(["orders"], metadata_client=metadata_client))
        self.assertTrue(metadata_client.cancelled, "the lookup was left running after the deadline")

    def test_the_metadata_clients_own_deadline_is_reported_as_it_is(self) -> None:
        metadata_client = self._orders_client()
        metadata_client.describe_error = TimeoutError("Kafka did not answer within 30s")

        with self.assertRaisesRegex(TimeoutError, r"^Kafka did not answer within 30s$"):
            asyncio.run(analyze_topics_async(["orders"], metadata_client=metadata_client))

    def test_the_view_refuses_a_bad_body_with_400_before_asking_kafka(self) -> None:
        metadata_client = self._orders_client()

        for body, field_name in (({}, "topics"), (None, None), ({"topics": []}, "topics"), ({"topics": 7}, "topics")):
            with self.subTest(body=body):
                with self.assertRaises(WebError) as raised:
                    asyncio.run(analyze_topics_view_async(body, metadata_client=metadata_client, config=ControlPlaneConfig()))
                self.assertEqual((raised.exception.code, raised.exception.status_code), ("invalid_request", 400))
                self.assertEqual(raised.exception.details.get("field"), field_name)
        self.assertEqual(metadata_client.describe_calls, [])

    def test_the_view_answers_503_when_kafka_fails_or_does_not_answer(self) -> None:
        """As `GET /topics` does: Kafka being unreachable is not the caller's fault, nor a 500."""
        failing = self._orders_client()
        failing.describe_error = RuntimeError("Unable to bootstrap from [('kafka', 9092)]")
        hanging = self._orders_client()
        hanging.hang = True

        with patch("materializer_control_plane.services.topic_analysis.ANALYZE_TIMEOUT_SECONDS", 0.05):
            for metadata_client, reason in (
                (failing, "Unable to bootstrap from [('kafka', 9092)]"),
                (hanging, "Kafka did not answer within 0.05s"),
            ):
                with self.subTest(reason=reason):
                    with self.assertRaises(WebError) as raised:
                        asyncio.run(
                            analyze_topics_view_async(
                                {"topics": ["orders"]},
                                metadata_client=metadata_client,
                                config=ControlPlaneConfig(),
                            )
                        )
                    self.assertEqual(raised.exception.status_code, 503)
                    self.assertEqual(
                        raised.exception.to_response(),
                        {
                            "error": {
                                "code": "unavailable",
                                "message": "Kafka topics cannot be analyzed right now",
                                "details": {"reason": reason},
                            }
                        },
                    )

    def test_the_sync_view_keeps_the_same_contract(self) -> None:
        metadata_client = self._orders_client()
        self.assertEqual(
            analyze_topics_view({"topic": "orders"}, metadata_client=metadata_client, config=ControlPlaneConfig()),
            asyncio.run(
                analyze_topics_view_async({"topic": "orders"}, metadata_client=metadata_client, config=ControlPlaneConfig())
            ),
        )
        metadata_client.describe_error = RuntimeError("broker down")
        with self.assertRaises(WebError) as raised:
            analyze_topics_view({"topics": ["orders"]}, metadata_client=metadata_client, config=ControlPlaneConfig())
        self.assertEqual((raised.exception.status_code, raised.exception.details), (503, {"reason": "broker down"}))


class AwaitingMetadataClient(FakeMetadataClient):
    """`FakeMetadataClient`, awaited the way the agents await the real one.

    Each lookup suspends; `while_waiting` runs then, standing in for what other agents write while
    the caller waits. `hang` makes a lookup wait until it is cancelled.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.while_waiting: Callable[[str], None] | None = None
        self.describe_error: Exception | None = None
        self.hang = False
        self.cancelled = False
        #: Shared with a `TimelineTable`, to see what happened in which order.
        self.timeline: list[str] | None = None

    def describe_topics(self, topics: list[str]) -> dict[str, TopicMetadata]:
        if self.describe_error is not None:
            raise self.describe_error
        return super().describe_topics(topics)

    async def describe_topics_async(self, topics: list[str]) -> dict[str, TopicMetadata]:
        await self._in_flight("describe")
        return self.describe_topics(topics)

    async def consumer_group_offsets_async(self, group_id: str) -> dict[str, dict[int, int]]:
        await self._in_flight("group")
        return self.consumer_group_offsets(group_id)

    async def _in_flight(self, kind: str) -> None:
        if self.timeline is not None:
            self.timeline.append(f"kafka:{kind}")
        try:
            # Far past any deadline under test, and bounded, so a missing deadline fails rather than hangs.
            await asyncio.sleep(10 if self.hang else 0)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self.while_waiting is not None:
            self.while_waiting(kind)


class TimelineTable(FakeTable):
    def __init__(self, timeline: list[str]) -> None:
        super().__init__()
        self.timeline = timeline

    def get(self, key: str) -> dict | None:
        self.timeline.append(f"read:{key}")
        return super().get(key)

    def __setitem__(self, key: str, value: dict) -> None:
        self.timeline.append(f"write:{key}")
        super().__setitem__(key, value)

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict]]:
        self.timeline.append(f"scan:{prefix}")
        return super().prefix_scan(prefix)


class ConsumerLagTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.config = ControlPlaneConfig()

    def test_lag_is_computed_from_committed_offsets(self) -> None:
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        metadata_client = FakeMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=2,
                    beginning_offsets={0: 0, 1: 0},
                    end_offsets={0: 100, 1: 200},
                )
            },
            group_offsets={"kaspr-mat-1-app": {"orders": {0: 90, 1: 150}}},
        )
        records = refresh_consumer_lag(
            self.table,
            metadata_client=metadata_client,
            measured_at="2026-07-12T00:10:00Z",
            config=self.config,
        )
        self.assertEqual(self._recorded(records), ["mat-1"])
        status = get_materialization_status(self.table, "mat-1")
        self.assertEqual(status.consumer_lag.total_lag, 60)
        self.assertEqual(status.consumer_lag.topics["orders"].partition_lag, {"0": 10, "1": 50})
        self.assertEqual(status.consumer_lag.topics["orders"].total_lag, 60)
        self.assertEqual(status.consumer_lag.measured_at, "2026-07-12T00:10:00Z")

    def test_lag_is_measured_on_each_topic_and_added_up(self) -> None:
        save_materialization(self.table, build_materialization("mat-1", source_topics=("orders", "payments")))
        save_materialization_status(
            self.table,
            build_status("mat-1", runtime_progress=RuntimeProgress(latest_offsets={"payments": {"0": 30}})),
        )
        metadata_client = FakeMetadataClient(
            {
                "orders": TopicMetadata(name="orders", exists=True, partitions=1, beginning_offsets={0: 0}, end_offsets={0: 100}),
                "payments": TopicMetadata(name="payments", exists=True, partitions=1, beginning_offsets={0: 0}, end_offsets={0: 50}),
            },
            group_offsets={"kaspr-mat-1-app": {"orders": {0: 90}}},
        )

        self._recorded(
            refresh_consumer_lag(self.table, metadata_client=metadata_client, measured_at="2026-07-12T00:10:00Z", config=self.config)
        )

        lag = get_materialization_status(self.table, "mat-1").consumer_lag
        self.assertEqual(metadata_client.describe_calls, [["orders", "payments"]])
        self.assertEqual({"orders": 10, "payments": 20}, {topic: item.total_lag for topic, item in lag.topics.items()})
        self.assertEqual(30, lag.total_lag)
        self.assertIsNone(lag.error)

    def test_a_missing_topic_is_named_and_the_others_still_measured(self) -> None:
        save_materialization(self.table, build_materialization("mat-1", source_topics=("orders", "gone")))
        save_materialization_status(self.table, build_status("mat-1"))
        metadata_client = FakeMetadataClient(
            {
                "orders": TopicMetadata(name="orders", exists=True, partitions=1, beginning_offsets={0: 0}, end_offsets={0: 100}),
                "gone": TopicMetadata(name="gone", exists=False, partitions=0),
            },
            group_offsets={"kaspr-mat-1-app": {"orders": {0: 60}}},
        )

        self._recorded(
            refresh_consumer_lag(self.table, metadata_client=metadata_client, measured_at="2026-07-12T00:10:00Z", config=self.config)
        )

        lag = get_materialization_status(self.table, "mat-1").consumer_lag
        self.assertEqual(40, lag.total_lag)
        self.assertEqual("gone: topic does not exist", lag.error)
        self.assertIsNone(lag.topics["gone"].total_lag)

    def test_heartbeat_offsets_are_used_when_group_has_no_commit(self) -> None:
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(
            self.table,
            build_status("mat-1", runtime_progress=RuntimeProgress(latest_offsets={"orders": {"0": 80}})),
        )
        metadata_client = FakeMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=1,
                    beginning_offsets={0: 0},
                    end_offsets={0: 100},
                )
            }
        )
        self._recorded(
            refresh_consumer_lag(
                self.table,
                metadata_client=metadata_client,
                measured_at="2026-07-12T00:10:00Z",
                config=self.config,
            )
        )
        status = get_materialization_status(self.table, "mat-1")
        self.assertEqual(status.consumer_lag.total_lag, 20)

    def test_topics_are_described_once_for_shared_source_topic(self) -> None:
        for materialization_id in ("mat-1", "mat-2"):
            save_materialization(
                self.table,
                build_materialization(materialization_id),
            )
            save_materialization_status(self.table, build_status(materialization_id))
        metadata_client = FakeMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=1,
                    beginning_offsets={0: 0},
                    end_offsets={0: 10},
                )
            }
        )
        refresh_consumer_lag(
            self.table,
            metadata_client=metadata_client,
            measured_at="2026-07-12T00:10:00Z",
            config=self.config,
        )
        self.assertEqual(metadata_client.describe_calls, [["orders"]])

    def test_deleted_materializations_are_skipped(self) -> None:
        save_materialization(
            self.table,
            build_materialization("mat-gone", desired_phase="deleted"),
        )
        save_materialization_status(
            self.table,
            build_status("mat-gone", desired_phase="deleted", observed_phase="deleted"),
        )
        metadata_client = FakeMetadataClient({})
        records = refresh_consumer_lag(
            self.table,
            metadata_client=metadata_client,
            measured_at="2026-07-12T00:10:00Z",
            config=self.config,
        )
        self.assertEqual(records, [])
        self.assertEqual(metadata_client.describe_calls, [])

    def _orders(self, *, group_offsets: dict | None = None) -> AwaitingMetadataClient:
        return AwaitingMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=2,
                    beginning_offsets={0: 0, 1: 0},
                    end_offsets={0: 100, 1: 200},
                )
            },
            group_offsets=group_offsets,
        )

    def _recorded(self, records: list, table: FakeTable | None = None) -> list[str]:
        """Record each measurement as the maintenance agent does on its own event; the ids measured."""
        for record in records:
            self.assertEqual(record.step, "record_consumer_lag")
            record_consumer_lag(self.table if table is None else table, record)
        return [record.materialization_id for record in records]

    def _refresh_async(self, metadata_client: AwaitingMetadataClient, table: FakeTable | None = None) -> list:
        return asyncio.run(
            refresh_consumer_lag_async(
                self.table if table is None else table,
                metadata_client=metadata_client,
                measured_at="2026-07-12T00:10:00Z",
                config=self.config,
            )
        )

    def _heartbeat(self, latest_offsets: dict[str, dict[str, int]], rows: int) -> RuntimeStatusEvent:
        return RuntimeStatusEvent(
            event_id=f"evt-pod-0-{rows}",
            materialization_id="mat-1",
            runtime_app_ref="kaspr-mat-1",
            member_id="pod-0",
            agent_name="writer",
            phase="running",
            health="healthy",
            reported_at="2026-07-12T00:10:01Z",
            rows_written=rows,
            bytes_written=rows * 10,
            latest_offsets=latest_offsets,
        )

    def test_async_refresh_measures_as_the_sync_refresh_does(self) -> None:
        for materialization_id in ("mat-1", "mat-2", "mat-3"):
            save_materialization(self.table, build_materialization(materialization_id))
            save_materialization_status(
                self.table,
                build_status(materialization_id, runtime_progress=RuntimeProgress(latest_offsets={"orders": {"1": 150}})),
            )
        save_materialization(self.table, build_materialization("mat-no-status"))
        sync_table = FakeTable()
        sync_table.values = {key: dict(value) for key, value in self.table.values.items()}
        group_offsets = {"kaspr-mat-1-app": {"orders": {0: 90, 1: 150}}, "kaspr-mat-2-app": {"orders": {0: 10}}}
        sync_client, async_client = self._orders(group_offsets=group_offsets), self._orders(group_offsets=group_offsets)

        sync_records = refresh_consumer_lag(
            sync_table,
            metadata_client=sync_client,
            measured_at="2026-07-12T00:10:00Z",
            config=self.config,
        )
        async_records = self._refresh_async(async_client)

        self.assertEqual(async_records, sync_records)
        self.assertEqual(self._recorded(async_records), ["mat-1", "mat-2", "mat-3"])
        self._recorded(sync_records, table=sync_table)
        self.assertEqual(self.table.values, sync_table.values)
        self.assertEqual(async_client.describe_calls, sync_client.describe_calls)
        self.assertEqual(async_client.group_calls, sync_client.group_calls)
        self.assertEqual(get_materialization_status(self.table, "mat-2").consumer_lag.total_lag, 140)

    def test_a_refresh_reads_the_table_around_kafka_and_writes_nothing(self) -> None:
        """It runs on the `__system__` event, whose shard is not the materialization's: a status
        written there would be a second copy that no delete of the materialization reaches."""
        timeline: list[str] = []
        self.table = TimelineTable(timeline)
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        timeline.clear()
        metadata_client = self._orders(group_offsets={"kaspr-mat-1-app": {"orders": {0: 90, 1: 150}}})
        metadata_client.timeline = timeline

        records = self._refresh_async(metadata_client)

        kafka = [index for index, entry in enumerate(timeline) if entry.startswith("kafka:")]
        self.assertEqual([timeline[index] for index in kafka], ["kafka:describe", "kafka:group"])
        self.assertTrue(timeline[0].startswith("scan:"))
        self.assertEqual([entry for entry in timeline if entry.startswith("write:")], [])
        # Measured against the status read again after Kafka answered, not carried across the waits.
        self.assertIn("read:materialization-status:mat-1", timeline[kafka[-1] + 1 :])

        timeline.clear()
        self._recorded(records)
        self.assertEqual([entry for entry in timeline if entry.startswith("write:")], ["write:materialization-status:mat-1"])

    def test_a_heartbeat_applied_during_the_lookup_is_kept(self) -> None:
        """Kafka is asked while runtime-status keeps applying heartbeats to the same record; saving
        the copy read before the lookup would silently drop them."""
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(
            self.table,
            build_status("mat-1", runtime_progress=RuntimeProgress(latest_offsets={"orders": {"0": 40, "1": 40}})),
        )
        metadata_client = self._orders()  # the group has committed nothing: lag comes from heartbeats

        def heartbeat(kind: str) -> None:
            if kind == "describe":
                apply_runtime_status_event(self.table, self._heartbeat({"orders": {"0": 95, "1": 180}}, rows=250))

        metadata_client.while_waiting = heartbeat

        self.assertEqual(self._recorded(self._refresh_async(metadata_client)), ["mat-1"])

        status = get_materialization_status(self.table, "mat-1")
        self.assertEqual(status.runtime_progress.latest_offsets, {"orders": {"0": 95, "1": 180}})
        self.assertEqual(status.runtime_progress.cumulative_rows_written, 250)
        # Measured against the heartbeat as it now stands: (100 - 95) + (200 - 180).
        self.assertEqual(status.consumer_lag.topics["orders"].partition_lag, {"0": 5, "1": 20})

    def test_a_purge_during_the_lookup_is_not_undone(self) -> None:
        """Writing the lag after a purge would recreate a status record nothing ever collects."""
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        metadata_client = self._orders()
        metadata_client.while_waiting = lambda kind: purge_materialization_records(self.table, "mat-1")

        self.assertEqual(self._refresh_async(metadata_client), [])
        self.assertEqual(self.table.values, {})

    def test_a_purge_before_the_measurement_is_recorded_is_not_undone(self) -> None:
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        records = self._refresh_async(self._orders())
        purge_materialization_records(self.table, "mat-1")

        self.assertFalse(record_consumer_lag(self.table, records[0]))
        self.assertEqual(self.table.values, {})

    def test_a_heartbeat_since_the_measurement_is_kept_when_it_is_recorded(self) -> None:
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        records = self._refresh_async(self._orders(group_offsets={"kaspr-mat-1-app": {"orders": {0: 90, 1: 150}}}))
        apply_runtime_status_event(self.table, self._heartbeat({"orders": {"0": 95, "1": 180}}, rows=250))

        self.assertTrue(record_consumer_lag(self.table, records[0]))

        status = get_materialization_status(self.table, "mat-1")
        self.assertEqual(status.runtime_progress.cumulative_rows_written, 250)
        self.assertEqual(status.consumer_lag.total_lag, 60)

    def test_an_older_measurement_does_not_replace_a_newer_one(self) -> None:
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        older = self._refresh_async(self._orders(group_offsets={"kaspr-mat-1-app": {"orders": {0: 90, 1: 150}}}))[0]
        newer = replace(older, consumer_lag=ConsumerLag(total_lag=0, measured_at="2026-07-12T00:11:00Z"))

        self.assertTrue(record_consumer_lag(self.table, newer))
        self.assertFalse(record_consumer_lag(self.table, older))
        self.assertEqual(get_materialization_status(self.table, "mat-1").consumer_lag.total_lag, 0)

    def test_a_materialization_that_stopped_being_tracked_meanwhile_gets_no_lag(self) -> None:
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        save_materialization(self.table, build_materialization("mat-2"))
        save_materialization_status(self.table, build_status("mat-2"))
        metadata_client = self._orders()

        def leave_tracking(kind: str) -> None:
            if kind != "describe":
                return
            status = get_materialization_status(self.table, "mat-1")
            save_materialization_status(self.table, replace(status, observed_phase="failed"))
            save_materialization(self.table, build_materialization("mat-2", desired_phase="deleting"))

        metadata_client.while_waiting = leave_tracking

        self.assertEqual(self._refresh_async(metadata_client), [])
        self.assertIsNone(get_materialization_status(self.table, "mat-1").consumer_lag)
        self.assertIsNone(get_materialization_status(self.table, "mat-2").consumer_lag)

    def test_a_topic_lookup_that_fails_writes_nothing(self) -> None:
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))
        before = {key: dict(value) for key, value in self.table.values.items()}
        metadata_client = self._orders()
        metadata_client.describe_error = TimeoutError("Kafka did not answer within 30s")

        with self.assertRaisesRegex(TimeoutError, "within 30s"):
            self._refresh_async(metadata_client)
        self.assertEqual(self.table.values, before)

    def _fleet(self, count: int) -> list[str]:
        ids = [f"mat-{index}" for index in range(1, count + 1)]
        for materialization_id in ids:
            save_materialization(self.table, build_materialization(materialization_id))
            save_materialization_status(self.table, build_status(materialization_id))
        return ids

    def test_successive_refreshes_measure_a_large_fleet_in_turn(self) -> None:
        """The batch limit once took the same first materializations every time."""
        ids = self._fleet(5)
        config = ControlPlaneConfig(consumer_lag_refresh_batch_limit=2)
        cursor = SweepCursor()

        rounds = []
        for refresh in range(3):
            if refresh % 2:
                records = refresh_consumer_lag(
                    self.table,
                    metadata_client=self._orders(),
                    measured_at="2026-07-12T00:10:00Z",
                    config=config,
                    cursor=cursor,
                )
            else:
                records = asyncio.run(
                    refresh_consumer_lag_async(
                        self.table,
                        metadata_client=self._orders(),
                        measured_at="2026-07-12T00:10:00Z",
                        config=config,
                        cursor=cursor,
                    )
                )
            rounds.append(self._recorded(records))

        self.assertEqual(rounds, [ids[0:2], ids[2:4], [ids[4], ids[0]]])

    def test_a_failed_refresh_measures_the_same_materializations_next_time(self) -> None:
        ids = self._fleet(3)
        config = ControlPlaneConfig(consumer_lag_refresh_batch_limit=2)
        cursor = SweepCursor()
        failing = self._orders()
        failing.describe_error = TimeoutError("Kafka did not answer within 30s")

        with self.assertRaises(TimeoutError):
            asyncio.run(
                refresh_consumer_lag_async(
                    self.table, metadata_client=failing, measured_at="2026-07-12T00:10:00Z", config=config, cursor=cursor
                )
            )
        records = asyncio.run(
            refresh_consumer_lag_async(
                self.table, metadata_client=self._orders(), measured_at="2026-07-12T00:11:00Z", config=config, cursor=cursor
            )
        )

        self.assertEqual(self._recorded(records), ids[0:2])


class AsyncMaintenanceHandlerTests(unittest.TestCase):
    """The maintenance agent's entry point dispatches each step to its non-blocking form."""

    def setUp(self) -> None:
        self.table = FakeTable()
        self.config = ControlPlaneConfig()
        save_materialization(self.table, build_materialization("mat-1"))
        save_materialization_status(self.table, build_status("mat-1"))

    def test_lag_refresh_awaits_kafka(self) -> None:
        metadata_client = AwaitingMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=1,
                    beginning_offsets={0: 0},
                    end_offsets={0: 100},
                )
            },
            group_offsets={"kaspr-mat-1-app": {"orders": {0: 60}}},
        )
        metadata_client.timeline = []

        outputs = list(
            asyncio.run(
                handle_maintenance_work_async(
                    build_consumer_lag_refresh_trigger(requested_at="2026-07-12T00:10:00Z"),
                    self.table,
                    metadata_client=metadata_client,
                    config=self.config,
                )
            )
        )

        # The lag goes back to the work topic keyed by its materialization, recorded by nothing yet.
        self.assertEqual([(output["route"], output["key"]) for output in outputs], [("work", "materialization:mat-1")])
        self.assertEqual(outputs[0]["value"]["step"], "record_consumer_lag")
        self.assertIsNone(get_materialization_status(self.table, "mat-1").consumer_lag)
        # Only the awaited lookups record themselves: the blocking ones were not used.
        self.assertEqual(metadata_client.timeline, ["kafka:describe", "kafka:group"])

        recorded = asyncio.run(handle_maintenance_work_async(outputs[0]["value"], self.table, config=self.config))

        self.assertEqual(list(recorded), [])
        self.assertEqual(get_materialization_status(self.table, "mat-1").consumer_lag.total_lag, 40)

    def test_the_agent_keeps_its_place_between_lag_refreshes(self) -> None:
        save_materialization(self.table, build_materialization("mat-2"))
        save_materialization_status(self.table, build_status("mat-2"))
        config = ControlPlaneConfig(consumer_lag_refresh_batch_limit=1)
        trigger = build_consumer_lag_refresh_trigger(requested_at="2026-07-12T00:10:00Z")
        metadata_client = AwaitingMetadataClient(
            {"orders": TopicMetadata(name="orders", exists=True, partitions=1, beginning_offsets={0: 0}, end_offsets={0: 100})}
        )

        with patch("materializer_control_plane.handlers.maintenance._CONSUMER_LAG_CURSOR", SweepCursor()):
            rounds = [
                [
                    output["value"]["materialization_id"]
                    for output in asyncio.run(
                        handle_maintenance_work_async(trigger, self.table, metadata_client=metadata_client, config=config)
                    )
                ]
                for _ in range(3)
            ]

        self.assertEqual(rounds, [["mat-1"], ["mat-2"], ["mat-1"]])

    def test_lag_refresh_needs_a_metadata_client(self) -> None:
        with self.assertRaisesRegex(ValueError, "metadata_client is required"):
            asyncio.run(
                handle_maintenance_work_async(
                    build_consumer_lag_refresh_trigger(requested_at="2026-07-12T00:10:00Z"),
                    self.table,
                    config=self.config,
                )
            )

    def test_the_retention_purge_runs_without_giving_up_the_loop(self) -> None:
        """A bounded table sweep: its reads and writes stay one uninterrupted unit."""
        coroutine = handle_maintenance_work_async(
            build_purge_deleted_trigger(requested_at="2026-07-12T00:10:00Z"),
            self.table,
            config=self.config,
        )

        with self.assertRaises(StopIteration) as finished:
            coroutine.send(None)

        self.assertEqual(list(finished.exception.value), [])

    def test_the_retention_purge_hands_each_purge_to_the_workflow_agent(self) -> None:
        save_materialization(self.table, build_materialization("mat-gone", desired_phase="deleted", updated_at="2026-07-01T00:00:00Z"))

        outputs = list(
            asyncio.run(
                handle_maintenance_work_async(
                    build_purge_deleted_trigger(requested_at="2026-07-12T00:10:00Z"),
                    self.table,
                    config=ControlPlaneConfig(deleted_retention_hours=24),
                )
            )
        )

        self.assertEqual([(output["route"], output["key"]) for output in outputs], [("work", "materialization:mat-gone")])
        self.assertEqual(outputs[0]["value"]["step"], "purge_materialization_records")
        self.assertIsNotNone(get_materialization(self.table, "mat-gone"))

    def test_an_unknown_step_is_ignored(self) -> None:
        value = {**build_purge_deleted_trigger(requested_at="2026-07-12T00:10:00Z"), "step": "sweep_observations"}

        self.assertEqual(list(asyncio.run(handle_maintenance_work_async(value, self.table, config=self.config))), [])



class ShardRoutingTests(unittest.TestCase):
    """Scheduled maintenance leaves each record in its materialization's own shard, and only there.

    The lag refresh and the retention purge run on `__system__` events. They once saved statuses and
    deleted records from there: every tracked materialization's status got a second copy in the
    `__system__` shard that nothing ever deleted, and a purge sent its tombstones to a shard that
    held none of the records it meant to remove.
    """

    SYSTEM_KEY = "materialization:__system__"

    def setUp(self) -> None:
        self.table = ShardedTable()
        self.config = ControlPlaneConfig(deleted_retention_hours=24, default_namespace="midstream")
        system_shard = self.table.shard_of(self.SYSTEM_KEY)
        # A materialization homed in the `__system__` shard would prove nothing.
        self.materialization_id = next(
            candidate
            for candidate in (f"mat-{index:012x}" for index in range(100))
            if self.table.shard_of(materialization_routing_key(candidate)) != system_shard
        )
        self.home_key = materialization_routing_key(self.materialization_id)
        self.metadata_client = AwaitingMetadataClient(
            {"orders": TopicMetadata(name="orders", exists=True, partitions=1, beginning_offsets={0: 0}, end_offsets={0: 100})},
            group_offsets={f"kaspr-{self.materialization_id}-app": {"orders": {0: 70}}},
        )

    def _handle(self, value: dict) -> list[dict]:
        """One work event, handled by the agent that consumes it, in the shard its key picks."""
        with self.table.handling(materialization_routing_key(value["materialization_id"])):
            if value["step"] == "purge_materialization_records":
                process_work_event(self.table, MaterializationWorkEvent.from_dict(value), None, config=self.config)
                return []
            return list(
                asyncio.run(
                    handle_maintenance_work_async(value, self.table, metadata_client=self.metadata_client, config=self.config)
                )
            )

    def _run(self, trigger: dict) -> None:
        """A scheduled trigger and all the work it hands on."""
        pending = [trigger]
        while pending:
            for output in self._handle(pending.pop(0)):
                self.assertEqual(output["route"], "work")
                self.assertEqual(output["key"], materialization_routing_key(output["value"]["materialization_id"]))
                pending.append(output["value"])

    def test_a_lag_refresh_records_the_lag_in_the_materializations_own_shard(self) -> None:
        with self.table.handling(self.home_key):
            save_materialization(self.table, build_materialization(self.materialization_id))
            save_materialization_status(self.table, build_status(self.materialization_id))

        self._run(build_consumer_lag_refresh_trigger(requested_at="2026-07-12T00:10:00Z"))

        status_key = materialization_status_key(self.materialization_id)
        self.assertEqual(self.table.holders(status_key), [self.table.shard_of(self.home_key)])
        self.assertEqual(self.table.shards[self.table.shard_of(self.SYSTEM_KEY)], {})
        self.assertEqual(get_materialization_status(self.table, self.materialization_id).consumer_lag.total_lag, 30)

    def test_the_table_maintenance_step_names_each_table_to_both_apps_and_writes_nothing(self) -> None:
        """Its maintenance ticks and its tail ticks are keyed by the table, for the apps, never written here."""
        tailed = build_materialization(self.materialization_id, source_topics=("orders", "payments"))
        with self.table.handling(self.home_key):
            save_materialization(self.table, tailed)
            for name in tailed.table_names:
                save_materialization_table(self.table, build_table_record(tailed, table=name))
        before = [dict(shard) for shard in self.table.shards]

        with self.table.handling(self.SYSTEM_KEY):
            outputs = list(
                asyncio.run(
                    handle_maintenance_work_async(
                        build_table_maintenance_trigger(requested_at="2026-07-12T00:10:00Z"),
                        self.table,
                        metadata_client=self.metadata_client,
                        config=self.config,
                    )
                )
            )

        self.assertEqual(before, self.table.shards)
        self.assertEqual(
            {
                ("maintenance", f"maintenance:{self.materialization_id}:orders"),
                ("maintenance", f"maintenance:{self.materialization_id}:payments"),
                ("tail", f"tail:{self.materialization_id}:orders"),
                ("tail", f"tail:{self.materialization_id}:payments"),
            },
            {(output["route"], output["key"]) for output in outputs},
        )

    def test_a_retention_purge_removes_the_records_from_the_materializations_own_shard(self) -> None:
        deleted = build_materialization(self.materialization_id, desired_phase="deleted", updated_at="2026-07-01T00:00:00Z")
        with self.table.handling(self.home_key):
            save_materialization(self.table, deleted)
            save_materialization_status(
                self.table,
                build_status(
                    self.materialization_id,
                    desired_phase="deleted",
                    observed_phase="deleted",
                    last_transition_time="2026-07-01T00:00:00Z",
                ),
            )
            save_materialization_table(self.table, build_table_record(deleted))

        self._run(build_purge_deleted_trigger(requested_at="2026-07-12T00:10:00Z"))

        self.assertEqual(self.table.shards, [{}, {}, {}])

    def test_the_directory_agent_reads_the_state_table_and_never_writes_it(self) -> None:
        """Claims, releases and the hourly sweep of claims all read materializations; every record they
        write is the directory's. A write to the state table outside a handled event would raise."""
        from materializer_control_plane.models.identity import Dataset, IdentityRequest
        from materializer_control_plane.services.identity import apply_identity_request
        from materializer_control_plane.services.table_claims import claim_request, release_request
        from materializer_control_plane.storage.identity import save_dataset, save_dataset_name

        directory = FakeTable()
        save_dataset(directory, Dataset(dataset_id="ds-000000000001", name="shop", created_at="t", created_by="a", updated_at="t", updated_by="a"))
        save_dataset_name(directory, "shop", "ds-000000000001", updated_at="t")
        materialization = build_materialization(self.materialization_id)
        with self.table.handling(self.home_key):
            save_materialization(self.table, materialization)
            save_materialization_table(self.table, build_table_record(materialization))
            save_materialization(self.table, build_materialization("mat-gone", desired_phase="deleting"))
        before = [dict(shard) for shard in self.table.shards]
        requests = (
            claim_request(request_id="idr-1", materialization_id="mat-gone", purpose="create", materialization_request_id="req-1", tables=table_specs("refunds"), actor_id=None, actor_email=None, submitted_at="2026-07-12T00:00:00Z"),
            release_request(request_id="idr-2", materialization_id="mat-other", everything=True, reason="deleted", actor_id=None, actor_email=None, submitted_at="2026-07-12T00:00:00Z"),
            IdentityRequest(request_id="idr-3", action="maintenance", submitted_at="2026-07-12T05:00:00Z", actor_id="system"),
        )

        for identity_request in requests:
            apply_identity_request(directory, None, identity_request, config=self.config, applied_at="2026-07-12T05:00:00Z", control_plane_table=self.table)

        self.assertEqual(before, self.table.shards)
        claims = sorted(key for key in directory.values if key.startswith("dataset-table:"))
        # The sweep released the deleted one's claim and claimed the live table's name again.
        self.assertEqual(["dataset-table:ds-000000000001:orders"], claims)

    def test_intake_and_the_workflow_write_only_the_materializations_records_and_no_directory_record(self) -> None:
        from materializer_control_plane.models import EventCause, MaterializationRequestEvent, MaterializationRequestSpec, TableClaimResult
        from materializer_control_plane.services import process_request_event

        create = MaterializationRequestEvent(
            request_id="req-1",
            materialization_id=self.materialization_id,
            action="create",
            submitted_at="2026-07-12T00:00:00Z",
            submitted_by="jane@example.com",
            spec=MaterializationRequestSpec(tables=table_specs("orders"), destination_uri="gs://lake"),
        )
        with self.table.handling(self.home_key):
            result = process_request_event(self.table, create, config=self.config)
        answer = MaterializationWorkEvent(
            work_id="work-claim-1",
            materialization_id=self.materialization_id,
            step="claim_rejected",
            requested_at="2026-07-12T00:00:01Z",
            caused_by=EventCause(kind="request", id="req-1"),
            dedupe_key="claim_rejected:1",
            claim=TableClaimResult(request_id="idr-claim-req-1", purpose="create", status="rejected", reason="table_name_taken", message="taken"),
        )
        with self.table.handling(self.home_key):
            process_work_event(self.table, answer, None, config=self.config)

        # The claim leaves for the directory as a request, on the directory's own key.
        self.assertEqual(["claim_tables"], [item.action for item in result.identity])
        keys = [key for shard in self.table.shards for key in shard]
        self.assertTrue(keys)
        self.assertEqual([], [key for key in keys if not key.startswith(f"materialization") or self.materialization_id not in key])
        self.assertEqual([self.table.shard_of(self.home_key)], sorted({index for index, shard in enumerate(self.table.shards) if shard}))

    def test_the_observation_sweep_writes_nothing_and_hands_on_work_keyed_by_materialization(self) -> None:
        with self.table.handling(self.home_key):
            save_materialization(self.table, build_materialization(self.materialization_id))
            save_materialization_status(self.table, build_status(self.materialization_id, observed_phase="deploying"))
            save_runtime_binding(
                self.table,
                RuntimeBinding(
                    materialization_id=self.materialization_id,
                    runtime_mode="dedicated",
                    kaspr_app_name=f"kaspr-{self.materialization_id}",
                    namespace="midstream",
                    managed_agent_names=["writer", "committer"],
                    template_version="v1",
                    last_applied_spec_hash="hash",
                    last_apply_operation_id="op-1",
                    updated_at="2026-07-12T00:00:00Z",
                ),
            )
        before = [dict(shard) for shard in self.table.shards]

        with self.table.handling(self.SYSTEM_KEY):
            outputs = list(
                handle_observation_work_for_output(
                    build_sweep_observation_trigger(requested_at="2026-07-12T00:10:00Z"),
                    self.table,
                    kaspr_app_client=None,
                    observed_at="2026-07-12T00:10:00Z",
                    config=self.config,
                )
            )

        self.assertEqual(self.table.shards, before)
        self.assertEqual([(output["route"], output["key"]) for output in outputs], [("work", self.home_key)])
        self.assertEqual(outputs[0]["value"]["step"], "observe_runtime")



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()
