from __future__ import annotations

import os
import unittest
from collections.abc import Iterable
from unittest.mock import patch

from midstream.runtime.templates import PackageSource
from midstream.config import ControlPlaneConfig
from midstream.models import Materialization, SettingOverride, SettingsDocument
from midstream.runtime import (
    build_dedicated_runtime_template_input,
    render_dedicated_runtime_resources,
    spec_hash,
)
from midstream.runtime_app.config import get_runtime_app_config
from midstream.runtime_app.handlers.runtime_settings import (
    apply_runtime_settings_message,
    handle_control_message,
)
from midstream.runtime_app.handlers.status import build_runtime_heartbeat, build_runtime_status_snapshot
from midstream.runtime_app.live_settings import (
    RUNTIME_SETTINGS_KEY,
    current_runtime_settings,
    current_settings_position,
    install_runtime_settings,
    refresh_runtime_settings,
    reset_runtime_settings,
    sanitize_runtime_settings,
)
from midstream.services.settings import build_runtime_settings_message
from midstream.settings import CONTROL_PLANE_SETTINGS, apply_control_plane_overrides
from table_records import build_table_records, table_specs


class FakeTable:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}
        self.reads = 0

    def get(self, key: str) -> dict | None:
        self.reads += 1
        return self.values.get(key)

    def __setitem__(self, key: str, value: dict) -> None:
        self.values[key] = value

    def __delitem__(self, key: str) -> None:
        self.values.pop(key, None)

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict]]:
        for key in sorted(self.values):
            if key.startswith(prefix):
                yield key, self.values[key]


#: Lineages as the control plane begins them: the time, then a random part.
EARLIER_LINEAGE = "20261023T090000Z-0a1b2c3d"
LINEAGE = "20261024T100000Z-4e5f6a7b"


def settings_message(revision: int, values: dict, *, target: str | None = None, lineage: str | None = None) -> dict:
    document = SettingsDocument(
        scope="runtime",
        revision=revision,
        values={
            key: SettingOverride(value=value, updated_at="2026-10-08T10:00:00Z", updated_by="jane")
            for key, value in values.items()
        },
        lineage=lineage,
    )
    return build_runtime_settings_message(
        document,
        published_at="2026-10-08T10:00:00Z",
        target_materialization_id=target,
    )


class LiveRuntimeSettingsTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_settings()
        self.addCleanup(reset_runtime_settings)

    def test_overrides_apply_over_the_environment(self) -> None:
        with patch.dict(os.environ, {"RUNTIME_MAX_OPEN_FILE_AGE_SECONDS": "600"}):
            self.assertEqual(get_runtime_app_config().max_open_file_age_seconds, 600)
            install_runtime_settings(1, {"max_open_file_age_seconds": 120, "protocol_logging_enabled": True})
            config = get_runtime_app_config()
        self.assertEqual(config.max_open_file_age_seconds, 120)
        self.assertTrue(config.protocol_logging_enabled)
        self.assertEqual(config.max_rows_per_file, 250_000)

    def test_an_explicit_environment_ignores_overrides(self) -> None:
        install_runtime_settings(1, {"max_rows_per_file": 10})
        self.assertEqual(get_runtime_app_config({}).max_rows_per_file, 250_000)

    def test_an_unworkable_combination_keeps_the_deployed_values(self) -> None:
        install_runtime_settings(1, {"max_open_file_age_seconds": 7200})
        with self.assertLogs("midstream.runtime_app.config", level="WARNING") as logs:
            first = get_runtime_app_config()
            second = get_runtime_app_config()
        self.assertEqual(first.max_open_file_age_seconds, 120)
        self.assertEqual(second.max_open_file_age_seconds, 120)
        self.assertEqual(len(logs.records), 1, "a refused revision is logged once, not on every call")

    def test_revisions_only_move_forward(self) -> None:
        self.assertTrue(install_runtime_settings(2, {"min_rows_per_file": 5}))
        self.assertFalse(install_runtime_settings(2, {"min_rows_per_file": 6}))
        self.assertFalse(install_runtime_settings(1, {"min_rows_per_file": 7}))
        self.assertEqual(current_runtime_settings(), (2, {"min_rows_per_file": 5}))

    def test_a_later_lineage_wins_whatever_revisions_the_earlier_one_reached(self) -> None:
        # An earlier installation's settings reached revision 14; a new one's start again at 0.
        self.assertTrue(install_runtime_settings(14, {"min_rows_per_file": 5}))
        self.assertTrue(install_runtime_settings(0, {}, LINEAGE))
        self.assertEqual(current_runtime_settings(), (0, {}))
        self.assertEqual(current_settings_position(), (LINEAGE, 0))
        self.assertTrue(install_runtime_settings(1, {"min_rows_per_file": 6}, LINEAGE))
        # Neither an earlier lineage nor settings without one take it back, whatever their revision.
        self.assertFalse(install_runtime_settings(99, {"min_rows_per_file": 7}, EARLIER_LINEAGE))
        self.assertFalse(install_runtime_settings(99, {"min_rows_per_file": 8}))
        self.assertFalse(install_runtime_settings(1, {"min_rows_per_file": 9}, LINEAGE))
        self.assertEqual(current_runtime_settings(), (1, {"min_rows_per_file": 6}))
        self.assertEqual(current_settings_position(), (LINEAGE, 1))

    def test_unknown_or_malformed_values_are_dropped(self) -> None:
        self.assertEqual(
            sanitize_runtime_settings(
                {
                    "max_rows_per_file": 10,
                    "min_rows_per_file": 0,
                    "max_open_file_age_seconds": "60",
                    "protocol_logging_enabled": 1,
                    "schema_ready_max_wait_seconds": True,
                    "upload_worker_count": 8,
                }
            ),
            {"max_rows_per_file": 10},
        )
        self.assertEqual(sanitize_runtime_settings(None), {})

    def test_members_pick_up_stored_settings_at_most_every_few_seconds(self) -> None:
        table = FakeTable()
        table[RUNTIME_SETTINGS_KEY] = {"revision": 3, "values": {"max_rows_per_file": 99}}
        clock = iter([100.0, 101.0, 106.0])
        self.assertEqual(refresh_runtime_settings(table, clock=lambda: next(clock)), 3)
        reads = table.reads
        table[RUNTIME_SETTINGS_KEY] = {"revision": 4, "values": {"max_rows_per_file": 77}}
        self.assertEqual(refresh_runtime_settings(table, clock=lambda: next(clock)), 3)
        self.assertEqual(table.reads, reads, "a refresh inside the interval does not read the table")
        self.assertEqual(refresh_runtime_settings(table, clock=lambda: next(clock)), 4)
        self.assertEqual(get_runtime_app_config().max_rows_per_file, 77)

    def test_a_missing_or_malformed_stored_record_changes_nothing(self) -> None:
        table = FakeTable()
        self.assertEqual(refresh_runtime_settings(table, force=True), 0)
        table[RUNTIME_SETTINGS_KEY] = {"revision": "three", "values": {"max_rows_per_file": 1}}
        self.assertEqual(refresh_runtime_settings(table, force=True), 0)
        self.assertEqual(refresh_runtime_settings(None, force=True), 0)
        self.assertEqual(current_settings_position(), ("", 0))

    def test_a_restarted_member_reads_the_lineage_back(self) -> None:
        table = FakeTable()
        table[RUNTIME_SETTINGS_KEY] = {"revision": 2, "lineage": LINEAGE, "values": {"max_rows_per_file": 20}}
        self.assertEqual(refresh_runtime_settings(table, force=True), 2)
        self.assertEqual(current_settings_position(), (LINEAGE, 2))
        # A record stored before lineages, or with a malformed one, reads as having none.
        for lineage in (None, "", 42, "x" * 65):
            with self.subTest(lineage=lineage):
                reset_runtime_settings()
                table[RUNTIME_SETTINGS_KEY] = {"revision": 2, "lineage": lineage, "values": {}}
                self.assertEqual(refresh_runtime_settings(table, force=True), 2)
                self.assertEqual(current_settings_position(), ("", 2))


class RuntimeSettingsDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_settings()
        self.addCleanup(reset_runtime_settings)
        self.table = FakeTable()

    def test_a_delivery_is_stored_and_installed(self) -> None:
        result = apply_runtime_settings_message(
            settings_message(1, {"max_open_file_age_seconds": 60}),
            self.table,
            materialization_id="mat-1",
            applied_at="2026-10-08T10:00:01Z",
        )
        self.assertEqual(result["revision"], 1)
        self.assertEqual(
            self.table.values[RUNTIME_SETTINGS_KEY],
            {
                "revision": 1,
                "lineage": None,
                "values": {"max_open_file_age_seconds": 60},
                "published_at": "2026-10-08T10:00:00Z",
                "applied_at": "2026-10-08T10:00:01Z",
            },
        )
        self.assertEqual(get_runtime_app_config().max_open_file_age_seconds, 60)

    def test_a_later_lineage_replaces_what_an_earlier_installation_left_on_the_topic(self) -> None:
        # Seen on a test cluster: a new runtime read an earlier installation's deliveries from the topic,
        # held their revision 14, and ignored the reinstalled control plane's revision 3.
        apply_runtime_settings_message(settings_message(14, {"max_rows_per_file": 14}), self.table)
        result = apply_runtime_settings_message(
            settings_message(3, {"max_rows_per_file": 3}, lineage=LINEAGE, target="mat-1"),
            self.table,
            materialization_id="mat-1",
        )
        self.assertEqual(result["lineage"], LINEAGE)
        self.assertEqual(self.table.values[RUNTIME_SETTINGS_KEY]["lineage"], LINEAGE)
        self.assertEqual(current_runtime_settings(), (3, {"max_rows_per_file": 3}))
        # The earlier deliveries, read again, change nothing, whatever their revision.
        self.assertIsNone(apply_runtime_settings_message(settings_message(14, {"max_rows_per_file": 14}), self.table))
        self.assertIsNone(
            apply_runtime_settings_message(settings_message(20, {"max_rows_per_file": 20}, lineage=EARLIER_LINEAGE), self.table)
        )
        self.assertEqual(self.table.values[RUNTIME_SETTINGS_KEY]["revision"], 3)
        self.assertEqual(current_runtime_settings(), (3, {"max_rows_per_file": 3}))

    def test_a_new_lineage_without_changes_drops_an_earlier_installations_overrides(self) -> None:
        apply_runtime_settings_message(settings_message(14, {"max_rows_per_file": 14}), self.table)
        self.assertEqual(get_runtime_app_config().max_rows_per_file, 14)
        apply_runtime_settings_message(settings_message(0, {}, lineage=LINEAGE), self.table)
        self.assertEqual(current_runtime_settings(), (0, {}))
        self.assertEqual(get_runtime_app_config().max_rows_per_file, 250_000)
        self.assertEqual(self.table.values[RUNTIME_SETTINGS_KEY]["values"], {})

    def test_a_member_installs_the_stored_lineage_when_its_delivery_is_older(self) -> None:
        # Another member stored this lineage's revision 2; this one is handed an older delivery.
        self.table[RUNTIME_SETTINGS_KEY] = {"revision": 2, "lineage": LINEAGE, "values": {"max_rows_per_file": 2}}
        self.assertIsNone(apply_runtime_settings_message(settings_message(9, {"max_rows_per_file": 9}), self.table))
        self.assertEqual(current_settings_position(), (LINEAGE, 2))
        self.assertEqual(current_runtime_settings(), (2, {"max_rows_per_file": 2}))

    def test_a_delivery_with_a_malformed_lineage_is_ignored(self) -> None:
        for lineage in (42, ["a"], "x" * 65):
            with self.subTest(lineage=lineage):
                message = {**settings_message(1, {"max_rows_per_file": 5}), "lineage": lineage}
                self.assertIsNone(apply_runtime_settings_message(message, self.table))
        self.assertEqual(self.table.values, {})
        self.assertEqual(current_settings_position(), ("", 0))

    def test_a_delivery_for_another_runtime_is_ignored(self) -> None:
        self.assertIsNone(
            apply_runtime_settings_message(
                settings_message(1, {"max_rows_per_file": 5}, target="mat-2"),
                self.table,
                materialization_id="mat-1",
            )
        )
        self.assertNotIn(RUNTIME_SETTINGS_KEY, self.table.values)

    def test_an_older_delivery_is_ignored_but_the_stored_one_is_installed(self) -> None:
        self.table[RUNTIME_SETTINGS_KEY] = {"revision": 5, "values": {"max_rows_per_file": 50}}
        self.assertIsNone(
            apply_runtime_settings_message(settings_message(4, {"max_rows_per_file": 40}), self.table)
        )
        self.assertEqual(self.table.values[RUNTIME_SETTINGS_KEY]["revision"], 5)
        self.assertEqual(current_runtime_settings(), (5, {"max_rows_per_file": 50}))

    def test_a_malformed_delivery_is_ignored_without_raising(self) -> None:
        self.assertIsNone(apply_runtime_settings_message({"kind": "runtime_settings", "revision": "x"}, self.table))
        self.assertIsNone(apply_runtime_settings_message({"kind": "runtime_settings"}, self.table))
        self.assertEqual(self.table.values, {})

    def test_the_settings_agent_applies_settings_and_drops_anything_else(self) -> None:
        self.assertIsNone(
            handle_control_message(settings_message(2, {"min_rows_per_file": 9}), self.table, materialization_id="mat-1")
        )
        self.assertEqual(current_runtime_settings()[0], 2)
        for ignored in (
            # An older control plane's: maintenance is the maintenance app's now.
            {"kind": "maintenance_request", "materialization_id": "mat-1", "job": "compaction", "request_id": "mnt-1"},
            {"materialization_id": "*", "kind": "something_newer"},
            "not a message",
        ):
            with self.subTest(message=ignored):
                self.assertIsNone(handle_control_message(ignored, self.table, materialization_id="mat-1"))

    def test_heartbeats_report_the_installed_revision(self) -> None:
        self.table[RUNTIME_SETTINGS_KEY] = {"revision": 7, "values": {}}
        snapshot = build_runtime_status_snapshot(self.table, materialization_id="mat-1")
        self.assertEqual(snapshot["settings_revision"], 7)
        # Sent though it has none, so the control plane can tell it from a runtime before lineages.
        self.assertEqual(snapshot["settings_lineage"], "")

    def test_heartbeats_report_the_installed_lineage(self) -> None:
        self.table[RUNTIME_SETTINGS_KEY] = {"revision": 0, "lineage": LINEAGE, "values": {}}
        snapshot = build_runtime_status_snapshot(self.table, materialization_id="mat-1")
        self.assertEqual((snapshot["settings_lineage"], snapshot["settings_revision"]), (LINEAGE, 0))


def build_materialization() -> Materialization:
    return Materialization(
        materialization_id="mat-1",
        namespace="midstream",
        name="shop",
        tables=table_specs("orders", "customers"),
        destination_uri="gs://lake/orders",
        desired_phase="running",
        created_at="2026-10-08T09:00:00Z",
        updated_at="2026-10-08T09:00:00Z",
    )


class RenderedRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_settings()
        self.addCleanup(reset_runtime_settings)
        patcher = patch("midstream.runtime.templates.installed_package_source", return_value=PackageSource(kind="git", reference="pkg-rev-1", requirement="git+https://github.com/vmirz/midstream.git@pkg-rev-1#egg=midstream-platform&subdirectory=python"))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.table_records = build_table_records(build_materialization())
        self.rendered = render_dedicated_runtime_resources(
            build_dedicated_runtime_template_input(build_materialization(), table_records=self.table_records)
        )

    def _settings_handler(self):
        agent = next(item for item in self.rendered["kaspr_agents"] if item["metadata"]["name"].endswith("-settings"))
        namespace: dict = {}
        exec(agent["spec"]["processors"]["init"]["python"], namespace)
        exec(agent["spec"]["processors"]["operations"][0]["map"]["python"], namespace)
        return namespace["handle"]

    def test_the_settings_agent_takes_broadcasts_and_its_own_messages_only(self) -> None:
        handle = self._settings_handler()
        table = FakeTable()
        self.assertIsNone(handle(settings_message(1, {"max_rows_per_file": 9}), table))
        self.assertEqual(current_runtime_settings()[0], 1)
        self.assertIsNone(handle(settings_message(2, {"max_rows_per_file": 8}, target="mat-2"), table))
        self.assertEqual(current_runtime_settings()[0], 1)
        self.assertIsNone(handle({"kind": "maintenance_request", "materialization_id": "mat-1", "job": "orphan_cleanup"}, table))

    def test_heartbeats_carry_the_settings_revision(self) -> None:
        task = next(item for item in self.rendered["kaspr_tasks"] if item["metadata"]["name"].endswith("-emit-status"))
        self.assertIn("build_runtime_heartbeat(", task["spec"]["processors"]["operations"][0]["map"]["python"])
        table = FakeTable()
        table[RUNTIME_SETTINGS_KEY] = {"revision": 4, "lineage": LINEAGE, "values": {}}
        heartbeat = build_runtime_heartbeat(table, materialization_id="mat-1", runtime_app_ref="kaspr-mat-1", agent_name="writer")
        self.assertEqual(4, heartbeat["settings_revision"])
        self.assertEqual(LINEAGE, heartbeat["settings_lineage"])

    def test_no_editable_setting_changes_the_rendered_runtime(self) -> None:
        # Settings must never reach the rendered spec: a change would alter every runtime's spec
        # hash, and the sweep would restart every runtime to apply it.
        base = ControlPlaneConfig()
        overrides = {
            "default_destination_uri": "gs://other",
            "default_runtime_replicas": 3,
            "max_runtime_replicas": 9,
            "runtime_autoscaling_enabled": False,
            "runtime_autoscale_max_replicas": 4,
            "runtime_autoscale_target_minutes": 5,
            "runtime_autoscale_min_lag_records": 50,
            "runtime_autoscale_cooldown_seconds": 120,
            "max_materialization_topics": 2,
            "default_runtime_storage_size": "9Gi",
            "destination_cleanup_enabled": False,
            "purge_records_on_delete": False,
            "deleted_retention_hours": 3,
            "deleted_purge_batch_limit": 3,
            "default_partition_granularity": "hour",
            "discovery_sample_records": 50,
            "discovery_sample_seconds": 3,
            "schema_history_limit": 3,
            "catalog_api_enabled": False,
            "catalog_metadata_cache_seconds": 30,
            "consumer_lag_refresh_batch_limit": 3,
            "topic_catalog_ttl_seconds": 3,
            "topic_catalog_min_refresh_seconds": 3,
            "topic_analysis_max_topics": 3,
            "protocol_logging_enabled": True,
            "session_idle_timeout_minutes": 30,
            "session_max_lifetime_hours": 2,
            "session_refresh_minutes": 5,
            "password_min_length": 16,
            "sign_in_max_failed_attempts": 5,
            "sign_in_lockout_minutes": 30,
            "audit_log_retention_days": 30,
            "sso_entra_enabled": True,
            "sso_github_enabled": True,
            "sso_google_enabled": True,
            "sso_oidc_enabled": True,
            "sso_first_sign_in": "invited",
            "sso_allowed_email_domains": "example.com",
            "password_sign_in_enabled": False,
            "live_tail_enabled": False,
            "live_tail_max_rows_per_table": 5000,
            "live_tail_max_bytes_per_table": 16 * 1024 * 1024,
            "live_tail_retention_seconds": 30,
            "live_tail_fetch_wait_ms": 100,
            "live_tail_max_records_per_second_per_table": 500,
            "live_tail_peek_max_rows": 50,
            "live_tail_idle_seconds": 60,
        }
        self.assertEqual(set(overrides), {item.key for item in CONTROL_PLANE_SETTINGS})
        overridden = apply_control_plane_overrides(
            base,
            SettingsDocument(
                scope="control-plane",
                revision=1,
                values={
                    key: SettingOverride(value=value, updated_at="2026-10-08T10:00:00Z", updated_by="jane")
                    for key, value in overrides.items()
                },
            ),
        )
        render = lambda config: spec_hash(
            render_dedicated_runtime_resources(
                build_dedicated_runtime_template_input(build_materialization(), table_records=self.table_records, config=config)
            )
        )
        self.assertEqual(render(base), render(overridden))


if __name__ == "__main__":
    unittest.main()
