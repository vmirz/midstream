from __future__ import annotations

import os
import unittest

from materializer_control_plane.config import (
    DEFAULT_LIST_LIMIT,
    DEFAULT_NAMESPACE,
    DEFAULT_RUNTIME_NAME_PREFIX,
    MATERIALIZATION_REQUESTS_TOPIC,
    MATERIALIZATION_WORK_TOPIC,
    MATERIALIZATION_STATUS_TOPIC,
    SCHEMA_AUTHORITY_UPDATES_TOPIC,
    TABLE_EVENTS_TOPIC,
    get_config,
    normalize_list_limit,
)


class ControlPlaneConfigTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_config.cache_clear()
        for key in (
            "MATERIALIZER_CONTROL_PLANE_TABLE_NAME",
            "MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE",
            "MATERIALIZER_CONTROL_PLANE_DEFAULT_LIST_LIMIT",
            "MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX",
            "MATERIALIZER_CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION",
            "MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC",
            "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC",
            "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC",
            "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC",
            "MATERIALIZER_CONTROL_PLANE_TABLE_EVENTS_TOPIC",
            "MATERIALIZER_CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC",
            "MATERIALIZER_PROTOCOL_LOGGING_ENABLED",
            "MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS",
            "MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI",
            "MATERIALIZER_CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC",
            "MATERIALIZER_CONTROL_PLANE_STORAGE_PREFIX",
            "MATERIALIZER_CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY",
            "MATERIALIZER_CONTROL_PLANE_DISCOVERY_SAMPLE_RECORDS",
            "MATERIALIZER_CONTROL_PLANE_CATALOG_API_ENABLED",
            "MATERIALIZER_CONTROL_PLANE_CATALOG_BASE_URL",
        ):
            os.environ.pop(key, None)

    def test_normalize_list_limit_defaults_to_thirty(self) -> None:
        self.assertEqual(normalize_list_limit(None), DEFAULT_LIST_LIMIT)

    def test_normalize_list_limit_rejects_unbounded_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "limit must be one of"):
            normalize_list_limit("50")

    def test_get_config_uses_contract_defaults(self) -> None:
        config = get_config()

        self.assertEqual(config.default_namespace, DEFAULT_NAMESPACE)
        self.assertEqual(config.default_runtime_name_prefix, DEFAULT_RUNTIME_NAME_PREFIX)
        self.assertEqual(
            config.runtime_name_for_materialization("mat-123"),
            "kaspr-mat-123",
        )
        self.assertEqual(config.runtime_app_spec, {})
        self.assertEqual(config.materialization_requests_topic, MATERIALIZATION_REQUESTS_TOPIC)
        self.assertEqual(config.materialization_work_topic, MATERIALIZATION_WORK_TOPIC)
        self.assertEqual(config.materialization_status_topic, MATERIALIZATION_STATUS_TOPIC)
        self.assertEqual(config.table_events_topic, TABLE_EVENTS_TOPIC)
        self.assertEqual(config.schema_authority_updates_topic, SCHEMA_AUTHORITY_UPDATES_TOPIC)
        self.assertEqual(config.settings_requests_topic, "materializer-settings-requests")
        self.assertEqual(config.storage_prefix, "materializer")
        self.assertEqual(config.default_partition_granularity, "day")
        self.assertEqual(config.discovery_sample_records, 200)
        self.assertTrue(config.catalog_api_enabled)
        self.assertIsNone(config.catalog_base_url)

    def test_get_config_reads_the_settings_requests_topic(self) -> None:
        os.environ["MATERIALIZER_CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC"] = "develop-materializer-settings-requests"

        self.assertEqual(get_config().settings_requests_topic, "develop-materializer-settings-requests")

    def test_get_config_uses_environment_overrides(self) -> None:
        os.environ["MATERIALIZER_CONTROL_PLANE_TABLE_NAME"] = "develop-state"
        os.environ["MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE"] = "custom-ns"
        os.environ["MATERIALIZER_CONTROL_PLANE_DEFAULT_LIST_LIMIT"] = "100"
        os.environ["MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX"] = "dedicated-runtime"
        os.environ["MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC"] = "develop-materialization-requests"
        os.environ["MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC"] = "develop-materialization-work"
        os.environ["MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC"] = "develop-materialization-status"
        os.environ["MATERIALIZER_CONTROL_PLANE_TABLE_EVENTS_TOPIC"] = "develop-materialization-table-events"
        os.environ["MATERIALIZER_CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC"] = "develop-materialization-schema-authority-updates"
        os.environ["MATERIALIZER_CONTROL_PLANE_STORAGE_PREFIX"] = "develop"
        os.environ["MATERIALIZER_CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY"] = "hour"
        os.environ["MATERIALIZER_CONTROL_PLANE_CATALOG_BASE_URL"] = "https://materialize.example.com/catalog/v1/"

        config = get_config()

        self.assertEqual(config.control_plane_table_name, "develop-state")
        self.assertEqual(config.default_namespace, "custom-ns")
        self.assertEqual(config.default_list_limit, "100")
        self.assertEqual(
            config.runtime_name_for_materialization("mat-123"),
            "dedicated-runtime-mat-123",
        )
        self.assertEqual(config.runtime_app_spec, {})
        self.assertEqual(config.materialization_requests_topic, "develop-materialization-requests")
        self.assertEqual(config.materialization_work_topic, "develop-materialization-work")
        self.assertEqual(config.materialization_status_topic, "develop-materialization-status")
        self.assertEqual(config.table_events_topic, "develop-materialization-table-events")
        self.assertEqual(
            config.schema_authority_updates_topic,
            "develop-materialization-schema-authority-updates",
        )
        self.assertEqual(config.storage_prefix, "develop")
        self.assertEqual(config.default_partition_granularity, "hour")
        self.assertEqual(config.catalog_base_url, "https://materialize.example.com/catalog/v1")

    def test_get_config_parses_runtime_app_spec_mapping(self) -> None:
        os.environ["MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC"] = """
authentication:
  type: scram-sha-512
  username: runtime-user
template:
  pod:
    tolerations:
      - key: app
        operator: Equal
        value: twmapp
        effect: NoSchedule
"""

        config = get_config()

        self.assertEqual(config.runtime_app_spec["authentication"]["username"], "runtime-user")
        self.assertEqual(
            config.runtime_app_spec["template"]["pod"]["tolerations"][0]["key"],
            "app",
        )

    def test_get_config_reads_protocol_logging_settings(self) -> None:
        os.environ["MATERIALIZER_PROTOCOL_LOGGING_ENABLED"] = "true"
        os.environ["MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS"] = "45"

        config = get_config()

        self.assertTrue(config.protocol_logging_enabled)
        self.assertEqual(config.protocol_log_slow_wait_seconds, 45)

    def test_an_unknown_partition_granularity_is_refused(self) -> None:
        os.environ["MATERIALIZER_CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY"] = "week"

        with self.assertRaisesRegex(ValueError, "default_partition_granularity must be one of"):
            get_config()

    def test_get_config_has_no_default_destination_unless_one_is_set(self) -> None:
        self.assertIsNone(get_config().default_destination_uri)

        for blank in ("", "   "):
            with self.subTest(value=blank):
                get_config.cache_clear()
                os.environ["MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI"] = blank
                self.assertIsNone(get_config().default_destination_uri)

    def test_get_config_reads_the_default_destination_without_trailing_slashes(self) -> None:
        os.environ["MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI"] = " gs://kafka-materializations/lake// "

        self.assertEqual(get_config().default_destination_uri, "gs://kafka-materializations/lake")


if __name__ == "__main__":
    unittest.main()