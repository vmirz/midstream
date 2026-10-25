from __future__ import annotations

from dataclasses import fields, replace
import os
from pathlib import Path
import unittest
from collections.abc import Iterable
from typing import Any
from unittest.mock import patch

from materializer_control_plane.config import ControlPlaneConfig, get_config
from materializer_control_plane.handlers.settings import build_settings_bootstrap_request, handle_settings_request_for_output
from materializer_control_plane.identity.secrets import IdentitySecrets, identity_secrets_from_environment
from materializer_control_plane.models import (
    Materialization,
    MaterializationStatus,
    MemberProgress,
    ResourceObservation,
    RuntimeHealth,
    RuntimeProgress,
    SettingFallback,
    SettingOverride,
    SettingsChangeRequest,
    SettingsDocument,
    TableSpec,
)
from materializer_control_plane.runtime_app.config import RuntimeAppConfig
from materializer_control_plane.runtime_app.live_settings import LIVE_SETTING_ENV_VARS, LIVE_SETTING_FIELDS
from materializer_control_plane.services.settings import (
    apply_settings_change,
    build_runtime_settings_message,
    new_settings_lineage,
    runtime_settings_output_for_apply,
)
from materializer_control_plane.settings import (
    CONTROL_PLANE_SETTINGS,
    RUNTIME_SETTINGS,
    SettingValueError,
    coerce_setting_value,
    definition_for,
    describe_settings,
    resolve_config,
    runtime_environment,
    validate_scope_values,
)
from materializer_control_plane.storage import save_materialization, save_materialization_status
from materializer_control_plane.storage.settings import (
    SETTINGS_HISTORY_LIMIT,
    get_settings_document,
    list_settings_changes,
    save_settings_document,
)
from materializer_control_plane.web.errors import WebError
from materializer_control_plane.web.settings import (
    runtime_settings_rollout,
    settings_history_view,
    settings_scope_view,
    update_settings,
)
from materializer_control_plane.web.system import settings_view


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


RUNTIME_SPEC = {
    "resources": {"requests": {"cpu": 0.5, "memory": "1Gi"}, "limits": {"cpu": 1, "memory": "1Gi"}},
    "storage": {"type": "persistent-claim", "class": "fast", "size": "1Gi"},
    "template": {
        "kasprContainer": {
            "env": [
                {"name": "MATERIALIZER_RUNTIME_MEMBER_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                {"name": "MATERIALIZER_PROTOCOL_LOGGING_ENABLED", "value": "true"},
                {"name": "MATERIALIZER_RUNTIME_MAX_OPEN_FILE_AGE_SECONDS", "value": "120"},
            ]
        }
    },
}


#: A lineage as the control plane begins one: the time it began, then eight hex digits.
LINEAGE_PATTERN = r"^\d{8}T\d{6}Z-[0-9a-f]{8}$"
EARLIER_LINEAGE = "20261001T090000Z-0a1b2c3d"


def change(scope: str = "control-plane", *, request_id: str = "set-1", **fields: Any) -> SettingsChangeRequest:
    return SettingsChangeRequest(
        request_id=request_id,
        scope=scope,
        submitted_at=fields.pop("submitted_at", "2026-10-08T10:00:00Z"),
        submitted_by=fields.pop("submitted_by", "jane"),
        **fields,
    )


def build_materialization(materialization_id: str, *, name: str, desired_phase: str = "running") -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        name=name,
        tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
        destination_uri="gs://lake/orders",
        desired_phase=desired_phase,
        created_at="2026-10-08T09:00:00Z",
        updated_at="2026-10-08T09:00:00Z",
    )


def build_status(
    materialization_id: str,
    *,
    desired_phase: str = "running",
    members: dict[str, MemberProgress] | None = None,
) -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id=materialization_id,
        desired_phase=desired_phase,
        observed_phase=desired_phase,
        phase_reason="processing",
        last_transition_time="2026-10-08T09:00:00Z",
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="healthy"),
        resource_observation=ResourceObservation(
            namespace="kafka-sql",
            kaspr_app_name=f"kaspr-{materialization_id}",
            resource_exists=True,
            managed_agent_states={},
        ),
        runtime_progress=RuntimeProgress(member_progress=members or {}),
    )


class SettingsCatalogTests(unittest.TestCase):
    def test_every_control_plane_setting_is_a_config_field_read_from_its_environment_variable(self) -> None:
        samples = {
            "boolean": ("false", False),
            "integer": ("7", 7),
            "gcs_uri": ("gs://lake/root/", "gs://lake/root"),
            "quantity": ("4Gi", "4Gi"),
            "enum": ("hour", "hour"),
        }
        config_fields = {item.name for item in fields(ControlPlaneConfig)}
        for definition in CONTROL_PLANE_SETTINGS:
            with self.subTest(key=definition.key):
                self.assertIn(definition.key, config_fields)
                raw, expected = samples[definition.kind]
                if definition.kind == "integer" and definition.minimum is not None and definition.minimum > expected:
                    raw, expected = str(definition.minimum), definition.minimum
                get_config.cache_clear()
                with patch.dict(os.environ, {definition.env_var: raw}):
                    self.assertEqual(getattr(get_config(), definition.key), expected)
        get_config.cache_clear()

    def test_runtime_settings_are_exactly_the_live_runtime_fields(self) -> None:
        self.assertEqual({item.key for item in RUNTIME_SETTINGS}, set(LIVE_SETTING_FIELDS))
        runtime_fields = {item.name for item in fields(RuntimeAppConfig)}
        for definition in RUNTIME_SETTINGS:
            with self.subTest(key=definition.key):
                self.assertIn(definition.key, runtime_fields)
                self.assertEqual(definition.env_var, LIVE_SETTING_ENV_VARS[definition.key])
                expected_kind = "boolean" if LIVE_SETTING_FIELDS[definition.key] is bool else "integer"
                self.assertEqual(definition.kind, expected_kind)

    def test_every_setting_says_what_it_controls(self) -> None:
        for definition in (*CONTROL_PLANE_SETTINGS, *RUNTIME_SETTINGS):
            with self.subTest(key=definition.key):
                self.assertTrue(definition.label)
                self.assertTrue(definition.section)
                if not definition.advanced:
                    self.assertTrue(definition.description, "a primary setting needs a description")

    def test_coerces_and_bounds_values(self) -> None:
        replicas = definition_for("control-plane", "max_runtime_replicas")
        self.assertEqual(coerce_setting_value(replicas, 32), 32)
        self.assertEqual(coerce_setting_value(replicas, 32.0), 32)
        for bad in (0, 2000, "32", True, 1.5, None):
            with self.subTest(value=bad), self.assertRaises(SettingValueError) as caught:
                coerce_setting_value(replicas, bad)
            self.assertEqual(caught.exception.key, "max_runtime_replicas")

        cleanup = definition_for("control-plane", "destination_cleanup_enabled")
        self.assertIs(coerce_setting_value(cleanup, False), False)
        with self.assertRaises(SettingValueError):
            coerce_setting_value(cleanup, "false")

        destination = definition_for("control-plane", "default_destination_uri")
        self.assertEqual(coerce_setting_value(destination, " gs://lake/data/ "), "gs://lake/data")
        self.assertIsNone(coerce_setting_value(destination, ""))
        self.assertIsNone(coerce_setting_value(destination, None))
        for bad in ("s3://lake", "gs://", "gs://Lake", "lake/data", 5):
            with self.subTest(value=bad), self.assertRaises(SettingValueError):
                coerce_setting_value(destination, bad)

        storage = definition_for("control-plane", "default_runtime_storage_size")
        self.assertEqual(coerce_setting_value(storage, "512Mi"), "512Mi")
        for bad in ("lots", "", None, 5):
            with self.subTest(value=bad), self.assertRaises(SettingValueError):
                coerce_setting_value(storage, bad)

        granularity = definition_for("control-plane", "default_partition_granularity")
        self.assertEqual(["hour", "day", "month", "none"], granularity.describe()["options"])
        self.assertEqual(coerce_setting_value(granularity, "none"), "none")
        for bad in ("week", "", None, 1):
            with self.subTest(value=bad), self.assertRaises(SettingValueError):
                coerce_setting_value(granularity, bad)
        self.assertIsNone(replicas.describe()["options"])

    def test_values_that_cannot_work_together_are_refused(self) -> None:
        with self.assertRaises(SettingValueError) as caught:
            validate_scope_values("control-plane", {"default_runtime_replicas": 9, "max_runtime_replicas": 8})
        self.assertEqual(caught.exception.key, "default_runtime_replicas")
        with self.assertRaises(SettingValueError) as caught:
            validate_scope_values(
                "runtime",
                {"max_open_file_age_seconds": 600, "max_open_file_age_ceiling_seconds": 300},
            )
        self.assertEqual(caught.exception.key, "max_open_file_age_ceiling_seconds")
        with self.assertRaises(SettingValueError) as caught:
            validate_scope_values("runtime", {"min_rows_per_file": 10, "max_rows_per_file": 5})
        self.assertEqual(caught.exception.key, "min_rows_per_file")
        with self.assertRaises(SettingValueError) as caught:
            validate_scope_values(
                "runtime",
                {"compaction_small_file_bytes": 256 * 1024**2, "compaction_target_file_size_bytes": 256 * 1024**2},
            )
        self.assertEqual(caught.exception.key, "compaction_small_file_bytes")
        with self.assertRaises(SettingValueError) as caught:
            validate_scope_values(
                "runtime",
                {"seal_schema_wait_seconds": 120, "max_open_file_age_seconds": 60, "max_open_file_age_ceiling_seconds": 90},
            )
        self.assertEqual(caught.exception.key, "seal_schema_wait_seconds")
        validate_scope_values("control-plane", {"default_runtime_replicas": 8, "max_runtime_replicas": 8})

    def test_retired_settings_are_gone(self) -> None:
        keys = {item.key for item in (*CONTROL_PLANE_SETTINGS, *RUNTIME_SETTINGS)}
        for retired in (
            "schema_proposal_debounce_seconds",
            "object_manifest_enabled",
            "object_manifest_batch_limit",
            "object_manifest_marker_batch_limit",
            "schema_decision_max_wait_seconds",
            "schema_ready_max_wait_seconds",
        ):
            with self.subTest(key=retired):
                self.assertNotIn(retired, keys)


class EffectiveConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.base = ControlPlaneConfig(max_runtime_replicas=64, default_runtime_replicas=1)

    def save_overrides(self, scope: str, values: dict[str, Any], *, revision: int = 1) -> None:
        save_settings_document(
            self.table,
            SettingsDocument(
                scope=scope,
                revision=revision,
                values={
                    key: SettingOverride(value=value, updated_at="2026-10-08T10:00:00Z", updated_by="jane")
                    for key, value in values.items()
                },
                updated_at=f"2026-10-08T10:00:0{revision}Z",
                updated_by="jane",
            ),
            updated_at="2026-10-08T10:00:00Z",
        )

    def test_without_overrides_the_deployment_config_applies(self) -> None:
        self.assertIs(resolve_config(None, base=self.base), self.base)
        self.assertIs(resolve_config(self.table, base=self.base), self.base)

    def test_overrides_replace_deployment_values(self) -> None:
        self.save_overrides(
            "control-plane",
            {"max_runtime_replicas": 16, "default_destination_uri": "gs://lake", "destination_cleanup_enabled": False},
        )
        resolved = resolve_config(self.table, base=self.base)
        self.assertEqual(resolved.max_runtime_replicas, 16)
        self.assertEqual(resolved.default_destination_uri, "gs://lake")
        self.assertFalse(resolved.destination_cleanup_enabled)
        self.assertEqual(resolved.materialization_work_topic, self.base.materialization_work_topic)
        self.assertIs(resolve_config(self.table, base=self.base), resolved, "unchanged settings reuse the merge")

        self.save_overrides("control-plane", {"max_runtime_replicas": 8}, revision=2)
        self.assertEqual(resolve_config(self.table, base=self.base).max_runtime_replicas, 8)
        self.assertEqual(resolve_config(self.table, base=self.base).default_destination_uri, None)

    def test_an_invalid_stored_override_is_ignored(self) -> None:
        self.save_overrides("control-plane", {"max_runtime_replicas": "lots", "deleted_retention_hours": 12})
        resolved = resolve_config(self.table, base=self.base)
        self.assertEqual(resolved.max_runtime_replicas, 64)
        self.assertEqual(resolved.deleted_retention_hours, 12)

    def test_default_replicas_never_exceed_the_maximum(self) -> None:
        self.save_overrides("control-plane", {"default_runtime_replicas": 12})
        resolved = resolve_config(self.table, base=replace(self.base, max_runtime_replicas=4))
        self.assertEqual(resolved.default_runtime_replicas, 4)

    def test_describes_where_each_value_comes_from(self) -> None:
        self.save_overrides("control-plane", {"max_runtime_replicas": 16})
        base = ControlPlaneConfig(default_runtime_replicas=3)
        described = {
            setting.definition.key: setting
            for setting in describe_settings(
                "control-plane",
                get_settings_document(self.table, "control-plane"),
                config=base,
                environ={"MATERIALIZER_CONTROL_PLANE_DEFAULT_RUNTIME_REPLICAS": "3"},
            )
        }
        override = described["max_runtime_replicas"]
        self.assertEqual((override.value, override.source, override.default_value), (16, "override", 64))
        self.assertEqual(override.updated_by, "jane")
        deployment = described["default_runtime_replicas"]
        self.assertEqual((deployment.value, deployment.source, deployment.deployment_value), (3, "deployment", 3))
        default = described["schema_history_limit"]
        self.assertEqual((default.value, default.source, default.deployment_set), (50, "default", False))

    def test_runtime_values_come_from_the_runtime_spec_environment(self) -> None:
        config = ControlPlaneConfig(runtime_app_spec=RUNTIME_SPEC)
        self.assertEqual(
            runtime_environment(config),
            {
                "MATERIALIZER_PROTOCOL_LOGGING_ENABLED": "true",
                "MATERIALIZER_RUNTIME_MAX_OPEN_FILE_AGE_SECONDS": "120",
            },
        )
        described = {setting.definition.key: setting for setting in describe_settings("runtime", None, config=config)}
        self.assertEqual((described["max_open_file_age_seconds"].value, described["max_open_file_age_seconds"].source), (120, "deployment"))
        self.assertEqual((described["protocol_logging_enabled"].value, described["protocol_logging_enabled"].source), (True, "deployment"))
        self.assertEqual((described["max_rows_per_file"].value, described["max_rows_per_file"].source), (250_000, "default"))


class ApplySettingsChangeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.config = ControlPlaneConfig(runtime_app_spec=RUNTIME_SPEC)
        self.environ = {"MATERIALIZER_CONTROL_PLANE_MAX_RUNTIME_REPLICAS": "64"}

    def apply(self, request: SettingsChangeRequest, *, at: str = "2026-10-08T10:00:00Z") -> list[dict]:
        return apply_settings_change(self.table, request, config=self.config, environ=self.environ, applied_at=at)

    def test_applies_an_override_and_records_what_changed(self) -> None:
        outputs = self.apply(change(values={"max_runtime_replicas": 32}, base_revision=0))
        self.assertEqual(outputs, [])
        document = get_settings_document(self.table, "control-plane")
        self.assertEqual(document.revision, 1)
        self.assertEqual(document.values["max_runtime_replicas"].value, 32)
        self.assertEqual(document.values["max_runtime_replicas"].updated_by, "jane")
        self.assertEqual(document.recent_request_ids, ["set-1"])
        [record] = list_settings_changes(self.table, "control-plane")
        self.assertEqual(record.revision, 1)
        self.assertEqual(
            [item.to_dict() for item in record.changes],
            [{"key": "max_runtime_replicas", "before": 64, "after": 32,
              "before_source": "deployment", "after_source": "override"}],
        )

    def test_a_redelivered_request_changes_nothing(self) -> None:
        request = change(values={"max_runtime_replicas": 32})
        self.apply(request)
        self.apply(request, at="2026-10-08T10:05:00Z")
        self.assertEqual(get_settings_document(self.table, "control-plane").revision, 1)
        self.assertEqual(len(list_settings_changes(self.table, "control-plane")), 1)

    def test_a_request_read_against_an_older_revision_is_refused(self) -> None:
        self.apply(change(values={"max_runtime_replicas": 32}, base_revision=0))
        self.apply(change(request_id="set-2", values={"max_runtime_replicas": 8}, base_revision=0))
        document = get_settings_document(self.table, "control-plane")
        self.assertEqual(document.revision, 1)
        self.assertEqual(document.values["max_runtime_replicas"].value, 32)
        self.assertEqual(document.last_rejection.request_id, "set-2")
        self.assertEqual(document.last_rejection.reason, "settings_changed")
        self.assertNotIn("set-2", document.recent_request_ids)

    def test_a_request_that_no_longer_fits_is_refused(self) -> None:
        self.apply(change(values={"max_runtime_replicas": 4}))
        self.apply(change(request_id="set-2", values={"default_runtime_replicas": 6}))
        document = get_settings_document(self.table, "control-plane")
        self.assertEqual(document.revision, 1)
        self.assertEqual(document.last_rejection.reason, "invalid_setting")
        self.assertIn("cannot exceed maximum replicas", document.last_rejection.message)

    def test_reset_returns_a_setting_to_its_deployment_value(self) -> None:
        self.apply(change(values={"max_runtime_replicas": 32}))
        self.apply(change(request_id="set-2", reset=["max_runtime_replicas"]), at="2026-10-08T11:00:00Z")
        document = get_settings_document(self.table, "control-plane")
        self.assertEqual(document.revision, 2)
        self.assertNotIn("max_runtime_replicas", document.values)
        latest = list_settings_changes(self.table, "control-plane")[0]
        self.assertEqual(
            latest.changes[0].to_dict(),
            {"key": "max_runtime_replicas", "before": 32, "after": 64,
             "before_source": "override", "after_source": "deployment"},
        )

    def test_setting_the_current_value_is_applied_without_a_new_revision(self) -> None:
        self.apply(change(values={"max_runtime_replicas": 32}))
        self.apply(change(request_id="set-2", values={"max_runtime_replicas": 32}))
        document = get_settings_document(self.table, "control-plane")
        self.assertEqual(document.revision, 1)
        self.assertEqual(document.recent_request_ids, ["set-1", "set-2"])

    def test_pinning_the_deployment_value_is_recorded(self) -> None:
        self.apply(change(values={"max_runtime_replicas": 64}))
        [record] = list_settings_changes(self.table, "control-plane")
        self.assertEqual((record.changes[0].before_source, record.changes[0].after_source), ("deployment", "override"))

    def test_runtime_changes_are_delivered_to_every_runtime(self) -> None:
        outputs = list(
            handle_settings_request_for_output(
                change("runtime", values={"max_open_file_age_seconds": 60}).to_dict(),
                self.table,
                config=self.config,
            )
        )
        self.assertEqual(len(outputs), 1)
        [output] = outputs
        self.assertEqual(output["route"], "topic")
        self.assertEqual(output["topic"], self.config.schema_authority_updates_topic)
        self.assertEqual(output["key"], "runtime-settings")
        self.assertEqual(output["value"]["kind"], "runtime_settings")
        self.assertEqual(output["value"]["materialization_id"], "*")
        self.assertIsNone(output["value"]["target_materialization_id"])
        self.assertEqual(output["value"]["revision"], 1)
        self.assertEqual(output["value"]["values"], {"max_open_file_age_seconds": 60})
        self.assertEqual(output["value"]["lineage"], get_settings_document(self.table, "runtime").lineage)

    def test_a_first_runtime_change_begins_a_lineage_and_later_ones_keep_it(self) -> None:
        [first] = self.apply(change("runtime", values={"min_rows_per_file": 500}))
        document = get_settings_document(self.table, "runtime")
        lineage = document.lineage
        self.assertRegex(lineage, LINEAGE_PATTERN)
        self.assertTrue(lineage.startswith("20261008T100000Z-"), "a lineage begins with the time it began")
        # Revision 1 is the lineage's own; the revisions before it went out without one.
        self.assertEqual(document.lineage_revision, 0)
        self.assertEqual(first["value"]["lineage"], lineage)
        [second] = self.apply(change("runtime", request_id="set-2", values={"min_rows_per_file": 600}), at="2026-10-08T11:00:00Z")
        self.assertEqual((second["value"]["revision"], second["value"]["lineage"]), (2, lineage))
        # Only runtime settings are delivered, so only they have a lineage.
        self.apply(change(values={"max_runtime_replicas": 32}))
        self.assertIsNone(get_settings_document(self.table, "control-plane").lineage)

    def test_a_runtime_redeploy_gets_the_current_runtime_settings(self) -> None:
        self.assertIsNone(
            runtime_settings_output_for_apply(
                self.table, materialization_id="mat-1", published_at="2026-10-08T10:00:00Z", config=self.config
            )
        )
        self.apply(change("runtime", values={"min_rows_per_file": 500}))
        output = runtime_settings_output_for_apply(
            self.table, materialization_id="mat-1", published_at="2026-10-08T10:01:00Z", config=self.config
        )
        self.assertEqual(output["value"]["target_materialization_id"], "mat-1")
        self.assertEqual(output["value"]["materialization_id"], "*")
        self.assertEqual(output["value"]["values"], {"min_rows_per_file": 500})
        self.assertEqual(output["value"]["lineage"], get_settings_document(self.table, "runtime").lineage)

    def test_a_runtime_redeploy_gets_the_lineage_before_any_change(self) -> None:
        # A new runtime reads the delivery topic from its start; with no change made here, this
        # delivery is what tells it to drop whatever an earlier installation left there.
        list(handle_settings_request_for_output(build_settings_bootstrap_request(requested_at="2026-10-08T09:59:00Z"), self.table, config=self.config))
        output = runtime_settings_output_for_apply(
            self.table, materialization_id="mat-1", published_at="2026-10-08T10:01:00Z", config=self.config
        )
        lineage = get_settings_document(self.table, "runtime").lineage
        self.assertEqual(
            {key: output["value"][key] for key in ("target_materialization_id", "revision", "lineage", "values")},
            {"target_materialization_id": "mat-1", "revision": 0, "lineage": lineage, "values": {}},
        )

    def test_runtime_values_are_checked_against_the_runtime_deployment(self) -> None:
        # The runtime spec sets a 120 s seal age, so a ceiling below it cannot work.
        self.apply(change("runtime", values={"max_open_file_age_ceiling_seconds": 60}))
        document = get_settings_document(self.table, "runtime")
        self.assertEqual(document.revision, 0)
        self.assertEqual(document.last_rejection.reason, "invalid_setting")

    def test_history_keeps_the_newest_hundred_changes(self) -> None:
        for index in range(SETTINGS_HISTORY_LIMIT + 5):
            self.apply(
                change(request_id=f"set-{index}", values={"deleted_retention_hours": index + 1}),
                at=f"2026-10-08T10:{index // 60:02d}:{index % 60:02d}Z",
            )
        changes = list_settings_changes(self.table, "control-plane")
        self.assertEqual(len(changes), SETTINGS_HISTORY_LIMIT)
        self.assertEqual(changes[0].revision, SETTINGS_HISTORY_LIMIT + 5)
        self.assertEqual(changes[-1].revision, 6)

    def test_console_settings_are_stored_without_a_catalog(self) -> None:
        self.apply(
            change(
                "console",
                values={"listPollIntervalMs": 5000, "dataAccessEnabled": False, "logLevel": "debug"},
                fallbacks={"listPollIntervalMs": SettingFallback(value=2000, source="default")},
            )
        )
        document = get_settings_document(self.table, "console")
        self.assertEqual(document.override_values(), {"listPollIntervalMs": 5000, "dataAccessEnabled": False, "logLevel": "debug"})
        changes = {item.key: item for item in list_settings_changes(self.table, "console")[0].changes}
        self.assertEqual((changes["listPollIntervalMs"].before, changes["listPollIntervalMs"].before_source), (2000, "default"))

    def test_console_settings_must_be_plain_values_under_plain_names(self) -> None:
        for values in ({"bad key": 1}, {"nested": {"a": 1}}, {"list": [1]}, {"long": "x" * 3000}):
            with self.subTest(values=values):
                table = FakeTable()
                apply_settings_change(table, change("console", values=values), config=self.config)
                self.assertEqual(get_settings_document(table, "console").last_rejection.reason, "invalid_setting")

    def test_console_settings_reject_non_finite_numbers(self) -> None:
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                table = FakeTable()
                apply_settings_change(table, change("console", values={"number": value}), config=self.config)
                rejection = get_settings_document(table, "console").last_rejection
                self.assertEqual(rejection.reason, "invalid_setting")
                self.assertIn("finite number", rejection.message)


class SettingsBootstrapTests(unittest.TestCase):
    """At every start the control plane asks the settings agent to give the runtime settings a
    lineage if they have none; only a start on settings without one changes anything."""

    def setUp(self) -> None:
        self.table = FakeTable()
        self.config = ControlPlaneConfig(runtime_app_spec=RUNTIME_SPEC)

    def start(self, at: str = "2026-10-24T10:00:00Z") -> list[dict]:
        return list(
            handle_settings_request_for_output(build_settings_bootstrap_request(requested_at=at), self.table, config=self.config)
        )

    def test_a_first_start_begins_a_lineage_and_delivers_it_to_every_runtime(self) -> None:
        [output] = self.start()
        document = get_settings_document(self.table, "runtime")
        self.assertRegex(document.lineage, LINEAGE_PATTERN)
        self.assertTrue(document.lineage.startswith("20261024T100000Z-"))
        self.assertEqual((document.revision, document.lineage_revision, document.values, document.updated_by), (0, 0, {}, None))
        self.assertEqual((output["route"], output["topic"], output["key"]), ("topic", self.config.schema_authority_updates_topic, "runtime-settings"))
        self.assertEqual(
            {key: output["value"][key] for key in ("materialization_id", "target_materialization_id", "revision", "lineage", "values")},
            {"materialization_id": "*", "target_materialization_id": None, "revision": 0, "lineage": document.lineage, "values": {}},
        )

    def test_later_starts_change_nothing(self) -> None:
        self.start()
        lineage = get_settings_document(self.table, "runtime").lineage
        self.assertEqual(self.start("2026-10-25T10:00:00Z"), [])
        self.assertEqual(get_settings_document(self.table, "runtime").lineage, lineage)

    def test_an_upgrade_delivers_the_overrides_under_their_new_lineage(self) -> None:
        # Stored before lineages: revision 4, changed by jane.
        apply_settings_change(self.table, change("runtime", values={"min_rows_per_file": 500}), config=self.config)
        stored = get_settings_document(self.table, "runtime")
        save_settings_document(self.table, replace(stored, revision=4, lineage=None), updated_at="2026-10-08T10:00:00Z")
        [output] = self.start()
        document = get_settings_document(self.table, "runtime")
        self.assertRegex(document.lineage, LINEAGE_PATTERN)
        self.assertEqual((document.revision, document.override_values(), document.updated_by), (4, {"min_rows_per_file": 500}, "jane"))
        self.assertEqual(document.lineage_revision, 4, "revisions up to 4 went out without a lineage")
        self.assertEqual(
            (output["value"]["revision"], output["value"]["lineage"], output["value"]["values"]),
            (4, document.lineage, {"min_rows_per_file": 500}),
        )

    def test_changes_after_a_start_keep_its_lineage(self) -> None:
        self.start()
        lineage = get_settings_document(self.table, "runtime").lineage
        [output] = apply_settings_change(self.table, change("runtime", values={"min_rows_per_file": 500}), config=self.config)
        self.assertEqual((output["value"]["revision"], output["value"]["lineage"]), (1, lineage))
        self.assertEqual(get_settings_document(self.table, "runtime").lineage_revision, 0)

    def test_a_lost_settings_table_begins_a_later_lineage(self) -> None:
        self.start("2026-10-24T10:00:00Z")
        first = get_settings_document(self.table, "runtime").lineage
        self.table.values.clear()
        self.start("2026-10-26T08:00:00Z")
        second = get_settings_document(self.table, "runtime").lineage
        self.assertGreater(second, first, "runtimes follow the later lineage, whatever revisions the first reached")

    def test_only_the_runtime_scope_is_bootstrapped(self) -> None:
        request = {**build_settings_bootstrap_request(requested_at="2026-10-24T10:00:00Z"), "scope": "control-plane"}
        self.assertEqual(list(handle_settings_request_for_output(request, self.table, config=self.config)), [])
        self.assertEqual(self.table.values, {})

    def test_lineages_sort_by_when_they_began(self) -> None:
        moments = ["2026-10-24T09:59:59Z", "2026-10-24T10:00:00Z", "2026-10-24T10:00:00.500000Z", "2026-10-24T12:00:01+02:00", "2026-10-25T00:00:00Z"]
        lineages = [new_settings_lineage(moment) for moment in moments]
        for lineage in lineages:
            self.assertRegex(lineage, LINEAGE_PATTERN)
        self.assertEqual([lineage[:16] for lineage in lineages], ["20261024T095959Z", "20261024T100000Z", "20261024T100000Z", "20261024T100001Z", "20261025T000000Z"])
        self.assertEqual(sorted(lineages[:2] + lineages[3:]), lineages[:2] + lineages[3:])
        self.assertNotEqual(new_settings_lineage(moments[0]), new_settings_lineage(moments[0]))
        self.assertRegex(new_settings_lineage("not a time"), LINEAGE_PATTERN)

    def test_the_task_sends_what_the_settings_agent_applies(self) -> None:
        import yaml

        manifest = Path(__file__).resolve().parents[2] / "manifests" / "base" / "tasks" / "settings-bootstrap.yaml"
        task = yaml.safe_load(manifest.read_text())
        self.assertTrue(task["spec"]["onLeader"])
        self.assertNotIn("schedule", task["spec"], "it runs once, at startup")
        namespace: dict = {}
        exec(task["spec"]["processors"]["init"]["python"], namespace)
        operations = {operation["name"]: operation for operation in task["spec"]["processors"]["operations"]}
        exec(operations["build-bootstrap-request"]["map"]["python"], namespace)
        request = namespace["build_request"]()
        send = operations["send-bootstrap-request"]["topicSend"]
        self.assertEqual(send["name"], "materializer-settings-requests")
        exec(send["keySelector"]["python"], namespace)
        # The runtime scope's key, so it applies in order with that scope's changes, in its shard.
        self.assertEqual(namespace["select_key"](request), "settings:runtime")
        outputs = list(handle_settings_request_for_output(request, self.table, config=self.config))
        self.assertEqual(len(outputs), 1)
        self.assertRegex(get_settings_document(self.table, "runtime").lineage, LINEAGE_PATTERN)


class SettingsViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = FakeTable()
        self.state = FakeTable()
        self.config = ControlPlaneConfig(runtime_app_spec=RUNTIME_SPEC, default_destination_uri="gs://deployed")
        self.environ = {"MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI": "gs://deployed"}

    def accept(self, scope: str, body: dict, **kwargs: Any):
        return update_settings(self.settings, scope, body, submitted_by="jane", config=self.config, environ=self.environ, **kwargs)

    def test_reports_every_setting_with_its_provenance(self) -> None:
        view = settings_scope_view(self.settings, "control-plane", config=self.config, environ=self.environ)
        self.assertEqual(view["revision"], 0)
        by_key = {item["key"]: item for item in view["settings"]}
        self.assertEqual(set(by_key), {item.key for item in CONTROL_PLANE_SETTINGS})
        destination = by_key["default_destination_uri"]
        self.assertEqual(destination["value"], "gs://deployed")
        self.assertEqual(destination["source"], "deployment")
        self.assertEqual(destination["section"], "materializations")
        self.assertTrue(destination["description"])
        deployment = {item["key"]: item for item in view["deployment"]}
        self.assertEqual(deployment["materialization_work_topic"]["value"], "materialization-work")
        self.assertIn("settings_requests_topic", deployment)
        self.assertNotIn("rollout", view)

    def test_runtime_view_reports_runtime_deployment_and_rollout(self) -> None:
        view = settings_scope_view(self.settings, "runtime", control_plane_table=self.state, config=self.config, now="2026-10-08T10:00:00Z")
        deployment = {item["key"]: item["value"] for item in view["deployment"]}
        self.assertEqual(deployment["storage_class"], "fast")
        self.assertEqual(deployment["resources"], "requests: cpu 0.5, memory 1Gi; limits: cpu 1, memory 1Gi")
        self.assertEqual(deployment["vertical_autoscaling"], "off")
        self.assertEqual(deployment["heartbeat_interval"], "30s")
        self.assertEqual(view["rollout"]["total"], 0)
        self.assertIsNone(view["lineage"])
        list(handle_settings_request_for_output(build_settings_bootstrap_request(requested_at="2026-10-08T09:00:00Z"), self.settings, config=self.config))
        view = settings_scope_view(self.settings, "runtime", control_plane_table=self.state, config=self.config, now="2026-10-08T10:00:00Z")
        self.assertRegex(view["lineage"], LINEAGE_PATTERN)

    def test_runtime_view_reports_how_members_are_autoscaled(self) -> None:
        # What kaspr-operator makes of the runtime spec's verticalPodAutoscaler: whatever it leaves
        # out is the operator's default, the CPU requests resized in place.
        cases = (
            ({"enabled": False, "maxAllowed": {"cpu": "1"}}, "off"),
            ({"enabled": True}, "cpu requests, update mode InPlaceOrRecreate"),
            (
                {"enabled": True, "minAllowed": {"cpu": "100m"}, "maxAllowed": {"cpu": "1500m"}},
                "cpu requests 100m–1500m, update mode InPlaceOrRecreate",
            ),
            ({"enabled": True, "minAllowed": {"cpu": "100m"}}, "cpu requests from 100m, update mode InPlaceOrRecreate"),
            (
                {
                    "enabled": True,
                    "updateMode": "Initial",
                    "controlledResources": ["cpu", "memory"],
                    "controlledValues": "RequestsAndLimits",
                    "maxAllowed": {"memory": "2Gi"},
                },
                "cpu requests and limits, memory requests and limits up to 2Gi, update mode Initial",
            ),
        )
        for autoscaler, expected in cases:
            config = replace(self.config, runtime_app_spec={**RUNTIME_SPEC, "verticalPodAutoscaler": autoscaler})
            view = settings_scope_view(self.settings, "runtime", control_plane_table=self.state, config=config, now="2026-10-08T10:00:00Z")
            deployment = {item["key"]: item for item in view["deployment"]}
            self.assertEqual(deployment["vertical_autoscaling"]["value"], expected)
            self.assertEqual(deployment["vertical_autoscaling"]["env_var"], "MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC")

    def test_console_view_reports_stored_values(self) -> None:
        apply_settings_change(self.settings, change("console", values={"pageRowLimit": 500}), config=self.config)
        view = settings_scope_view(self.settings, "console")
        self.assertEqual(view["values"]["pageRowLimit"]["value"], 500)
        self.assertEqual(view["revision"], 1)
        self.assertEqual(view["recent_request_ids"], ["set-1"])

    def test_deployment_reports_credentials_only_as_configured(self) -> None:
        values = {
            "MATERIALIZER_AUTH_SIGNING_KEY": "k" * 40,
            "MATERIALIZER_AUTH_SERVICE_TOKEN": "t" * 40,
            "MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_EMAIL": "root@example.com",
            "MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_PASSWORD": "a bootstrap password",
        }
        cases = (
            (identity_secrets_from_environment(values), ("configured", "configured", "configured")),
            (IdentitySecrets(), ("not configured: nobody can sign in", "not configured", "not configured")),
        )
        for secrets, expected in cases:
            with patch("materializer_control_plane.web.settings.get_identity_secrets", return_value=secrets):
                view = settings_scope_view(self.settings, "control-plane", config=self.config, environ=self.environ)
            deployment = {item["key"]: item["value"] for item in view["deployment"]}
            self.assertEqual(
                (deployment["auth_signing_key"], deployment["auth_service_token"], deployment["bootstrap_admin_email"]),
                expected,
            )
            for value in values.values():
                self.assertNotIn(value, str(view))

    def test_an_unknown_scope_is_not_found(self) -> None:
        with self.assertRaises(WebError) as caught:
            settings_scope_view(self.settings, "secrets")
        self.assertEqual(caught.exception.status_code, 404)

    def test_accepts_a_valid_change(self) -> None:
        acceptance = self.accept("control-plane", {"values": {"max_runtime_replicas": 32}, "reset": [], "base_revision": 0})
        self.assertEqual(acceptance.response["scope"], "control-plane")
        self.assertEqual(acceptance.response["base_revision"], 0)
        self.assertEqual(acceptance.request_event.values, {"max_runtime_replicas": 32})
        self.assertEqual(acceptance.request_event.submitted_by, "jane")
        self.assertEqual(self.settings.values, {}, "the view must not write to the table")

    def test_refuses_invalid_changes_naming_the_setting(self) -> None:
        cases = [
            ({"values": {"max_runtime_replicas": 0}}, "max_runtime_replicas"),
            ({"values": {"default_namespace": "other"}}, "default_namespace"),
            ({"values": {"default_runtime_replicas": 99, "max_runtime_replicas": 10}}, "default_runtime_replicas"),
            ({"reset": ["materialization_work_topic"]}, "materialization_work_topic"),
            ({"values": {}, "reset": []}, None),
            ({"values": []}, "values"),
            ({"bogus": 1}, "bogus"),
            ({"values": {"max_runtime_replicas": 9}, "base_revision": -1}, "base_revision"),
        ]
        for body, field_name in cases:
            with self.subTest(body=body), self.assertRaises(WebError) as caught:
                self.accept("control-plane", body)
            self.assertEqual(caught.exception.status_code, 400)
            self.assertEqual(caught.exception.details.get("field"), field_name)

    def test_refuses_a_change_read_against_an_older_revision(self) -> None:
        apply_settings_change(self.settings, change(values={"max_runtime_replicas": 32}), config=self.config)
        with self.assertRaises(WebError) as caught:
            self.accept("control-plane", {"values": {"max_runtime_replicas": 8}, "base_revision": 0})
        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(caught.exception.details, {"reason": "settings_changed"})

    def test_history_lists_changes_newest_first(self) -> None:
        apply_settings_change(self.settings, change(values={"max_runtime_replicas": 32}), config=self.config, applied_at="2026-10-08T10:00:00Z")
        apply_settings_change(self.settings, change(request_id="set-2", values={"max_runtime_replicas": 16}), config=self.config, applied_at="2026-10-08T11:00:00Z")
        view = settings_history_view(self.settings, "control-plane")
        self.assertEqual([item["revision"] for item in view["changes"]], [2, 1])
        self.assertEqual(settings_history_view(self.settings, "control-plane", limit="1")["returned"], 1)
        with self.assertRaises(WebError):
            settings_history_view(self.settings, "control-plane", limit="0")

    def test_system_settings_report_effective_defaults(self) -> None:
        apply_settings_change(
            self.settings,
            change(values={"default_destination_uri": "gs://chosen", "max_runtime_replicas": 12}),
            config=self.config,
        )
        get_config.cache_clear()
        with patch.dict(os.environ, {"MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI": "gs://deployed"}):
            reported = settings_view(settings_table=self.settings)
        get_config.cache_clear()
        self.assertEqual(reported["default_destination_uri"], "gs://chosen")
        self.assertEqual(reported["max_replicas"], 12)


class RuntimeSettingsRolloutTests(unittest.TestCase):
    NOW = "2026-10-08T10:05:00Z"

    def setUp(self) -> None:
        self.state = FakeTable()

    def add(self, materialization_id: str, *, desired_phase: str = "running", members: dict[str, MemberProgress] | None = None) -> None:
        save_materialization(self.state, build_materialization(materialization_id, name=materialization_id.replace("-", "_"), desired_phase=desired_phase))
        save_materialization_status(self.state, build_status(materialization_id, desired_phase=desired_phase, members=members))

    def test_reports_how_far_a_revision_has_reached(self) -> None:
        fresh, stale = "2026-10-08T10:04:30Z", "2026-10-08T09:00:00Z"
        self.add("mat-current", members={"a": MemberProgress(reported_at=fresh, settings_revision=3), "b": MemberProgress(reported_at=fresh, settings_revision=4)})
        self.add("mat-pending", members={"a": MemberProgress(reported_at=fresh, settings_revision=3), "b": MemberProgress(reported_at=fresh, settings_revision=2)})
        self.add("mat-old", members={"a": MemberProgress(reported_at=fresh, settings_revision=None)})
        self.add("mat-silent", members={"a": MemberProgress(reported_at=stale, settings_revision=1)})
        self.add("mat-stopped", desired_phase="stopped")
        self.add("mat-gone", desired_phase="deleting")
        rollout = runtime_settings_rollout(self.state, revision=3, now=self.NOW)
        self.assertEqual(
            {key: rollout[key] for key in ("revision", "total", "current", "pending", "stopped", "not_reporting", "unsupported")},
            {"revision": 3, "total": 5, "current": 1, "pending": 1, "stopped": 1, "not_reporting": 1, "unsupported": 1},
        )
        states = {item["materialization_id"]: item["state"] for item in rollout["materializations"]}
        self.assertEqual(states["mat-pending"], "pending")
        self.assertEqual(rollout["materializations"][-1]["state"], "current", "current ones sort last")
        pending = next(item for item in rollout["materializations"] if item["materialization_id"] == "mat-pending")
        self.assertEqual((pending["revision"], pending["members"]), (2, 2))

    def test_a_member_on_settings_of_another_lineage_is_not_current(self) -> None:
        fresh = "2026-10-08T10:04:30Z"
        lineage = "20261008T090000Z-4e5f6a7b"
        self.add("mat-current", members={"a": MemberProgress(reported_at=fresh, settings_revision=3, settings_lineage=lineage)})
        # Seen on the lab: an earlier installation's revision 14, read back from the delivery topic.
        self.add("mat-earlier", members={"a": MemberProgress(reported_at=fresh, settings_revision=14, settings_lineage="")})
        self.add("mat-other", members={"a": MemberProgress(reported_at=fresh, settings_revision=20, settings_lineage=EARLIER_LINEAGE)})
        self.add("mat-mixed", members={
            "a": MemberProgress(reported_at=fresh, settings_revision=3, settings_lineage=lineage),
            "b": MemberProgress(reported_at=fresh, settings_revision=9, settings_lineage=""),
        })
        # A runtime from before lineages reports none: its revision is all there is to judge by.
        self.add("mat-before", members={"a": MemberProgress(reported_at=fresh, settings_revision=3)})
        rollout = runtime_settings_rollout(self.state, revision=3, lineage=lineage, now=self.NOW)
        items = {item["materialization_id"]: (item["state"], item["revision"]) for item in rollout["materializations"]}
        self.assertEqual(
            items,
            {
                "mat-current": ("current", 3),
                # A number from another history says nothing, so none is shown.
                "mat-earlier": ("pending", None),
                "mat-other": ("pending", None),
                "mat-mixed": ("pending", None),
                "mat-before": ("current", 3),
            },
        )
        self.assertEqual((rollout["current"], rollout["pending"]), (2, 3))

    def test_at_revision_zero_a_runtime_still_on_an_earlier_installations_settings_is_pending(self) -> None:
        fresh = "2026-10-08T10:04:30Z"
        lineage = "20261008T090000Z-4e5f6a7b"
        self.add("mat-earlier", members={"a": MemberProgress(reported_at=fresh, settings_revision=14, settings_lineage="")})
        self.add("mat-current", members={"a": MemberProgress(reported_at=fresh, settings_revision=0, settings_lineage=lineage)})
        rollout = runtime_settings_rollout(self.state, revision=0, lineage=lineage, now=self.NOW)
        self.assertEqual((rollout["current"], rollout["pending"]), (1, 1))
        # With no lineage begun yet, settings without one are this control plane's: as before.
        rollout = runtime_settings_rollout(self.state, revision=0, lineage=None, now=self.NOW)
        states = {item["materialization_id"]: item["state"] for item in rollout["materializations"]}
        self.assertEqual(states, {"mat-earlier": "current", "mat-current": "pending"})

    def test_a_runtime_upgraded_from_before_lineages_runs_the_settings_the_lineage_began_from(self) -> None:
        # The upgrade: the lineage begins at revision 4. A 4.0 member consumed its delivery and the
        # one re-applying the runtime, and ignored both; the 4.1 member that replaced it holds
        # revision 4 without a lineage — the very values the lineage began from.
        fresh = "2026-10-08T10:04:30Z"
        lineage = "20261008T090000Z-4e5f6a7b"
        self.add("mat-upgraded", members={"a": MemberProgress(reported_at=fresh, settings_revision=4, settings_lineage="")})
        self.add("mat-behind", members={"a": MemberProgress(reported_at=fresh, settings_revision=3, settings_lineage="")})
        self.add("mat-earlier", members={"a": MemberProgress(reported_at=fresh, settings_revision=14, settings_lineage="")})
        rollout = runtime_settings_rollout(self.state, revision=4, lineage=lineage, lineage_revision=4, now=self.NOW)
        items = {item["materialization_id"]: (item["state"], item["revision"]) for item in rollout["materializations"]}
        self.assertEqual(
            items,
            {"mat-upgraded": ("current", 4), "mat-behind": ("pending", 3), "mat-earlier": ("pending", None)},
        )
        # Once anything changes, revision 5 goes out with the lineage, and settings without one are behind.
        rollout = runtime_settings_rollout(self.state, revision=5, lineage=lineage, lineage_revision=4, now=self.NOW)
        items = {item["materialization_id"]: (item["state"], item["revision"]) for item in rollout["materializations"]}
        self.assertEqual(items["mat-upgraded"], ("pending", 4))

    def test_the_view_judges_upgraded_runtimes_by_where_the_lineage_began(self) -> None:
        settings = FakeTable()
        config = ControlPlaneConfig(runtime_app_spec=RUNTIME_SPEC)
        apply_settings_change(settings, change("runtime", values={"min_rows_per_file": 500}), config=config)
        stored = get_settings_document(settings, "runtime")
        save_settings_document(settings, replace(stored, lineage=None, lineage_revision=None), updated_at="2026-10-08T10:00:00Z")
        list(handle_settings_request_for_output(build_settings_bootstrap_request(requested_at="2026-10-08T10:01:00Z"), settings, config=config))
        self.add("mat-upgraded", members={"a": MemberProgress(reported_at="2026-10-08T10:04:30Z", settings_revision=1, settings_lineage="")})
        view = settings_scope_view(settings, "runtime", control_plane_table=self.state, config=config, now=self.NOW)
        self.assertEqual((view["rollout"]["current"], view["rollout"]["pending"]), (1, 0))

    def test_without_a_state_table_there_is_no_rollout(self) -> None:
        self.assertIsNone(runtime_settings_rollout(None, revision=1, now=self.NOW))


class RuntimeSettingsMessageTests(unittest.TestCase):
    def test_carries_every_override(self) -> None:
        document = SettingsDocument(
            scope="runtime",
            revision=5,
            values={
                "min_rows_per_file": SettingOverride(value=100, updated_at="2026-10-08T10:00:00Z", updated_by="jane"),
                "protocol_logging_enabled": SettingOverride(value=False, updated_at="2026-10-08T10:00:00Z", updated_by="jane"),
            },
        )
        self.assertEqual(
            build_runtime_settings_message(document, published_at="2026-10-08T10:00:00Z", target_materialization_id="mat-1"),
            {
                "kind": "runtime_settings",
                "materialization_id": "*",
                "target_materialization_id": "mat-1",
                "revision": 5,
                "lineage": None,
                "values": {"min_rows_per_file": 100, "protocol_logging_enabled": False},
                "published_at": "2026-10-08T10:00:00Z",
            },
        )
        named = replace(document, lineage="20261008T090000Z-4e5f6a7b")
        self.assertEqual(build_runtime_settings_message(named, published_at="2026-10-08T10:00:00Z")["lineage"], named.lineage)


if __name__ == "__main__":
    unittest.main()
