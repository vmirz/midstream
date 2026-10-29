from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

from midstream import config as config_module
from midstream.config import (
    DEFAULT_LIST_LIMIT,
    DEFAULT_RUNTIME_NAME_PREFIX,
    MATERIALIZATION_REQUESTS_TOPIC,
    MATERIALIZATION_WORK_TOPIC,
    MATERIALIZATION_STATUS_TOPIC,
    SCHEMA_AUTHORITY_UPDATES_TOPIC,
    TABLE_EVENTS_TOPIC,
    _resolve_default_namespace,
    get_config,
    normalize_list_limit,
)


class ControlPlaneConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        get_config.cache_clear()
        # A deployment always names the runtimes' namespace; the namespace tests below take it away.
        os.environ["CONTROL_PLANE_DEFAULT_NAMESPACE"] = "midstream"

    def tearDown(self) -> None:
        get_config.cache_clear()
        for key in (
            "CONTROL_PLANE_TABLE_NAME",
            "CONTROL_PLANE_DEFAULT_NAMESPACE",
            "CONTROL_PLANE_DEFAULT_LIST_LIMIT",
            "CONTROL_PLANE_RUNTIME_NAME_PREFIX",
            "CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION",
            "CONTROL_PLANE_RUNTIME_APP_SPEC",
            "CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC",
            "CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC",
            "CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC",
            "CONTROL_PLANE_TABLE_EVENTS_TOPIC",
            "CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC",
            "PROTOCOL_LOGGING_ENABLED",
            "PROTOCOL_LOG_SLOW_WAIT_SECONDS",
            "CONTROL_PLANE_DEFAULT_DESTINATION_URI",
            "CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC",
            "CONTROL_PLANE_STORAGE_PREFIX",
            "CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY",
            "CONTROL_PLANE_DISCOVERY_SAMPLE_RECORDS",
            "CONTROL_PLANE_CATALOG_API_ENABLED",
            "CONTROL_PLANE_CATALOG_BASE_URL",
        ):
            os.environ.pop(key, None)

    def test_normalize_list_limit_defaults_to_thirty(self) -> None:
        self.assertEqual(normalize_list_limit(None), DEFAULT_LIST_LIMIT)

    def test_normalize_list_limit_rejects_unbounded_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "limit must be one of"):
            normalize_list_limit("50")

    def test_get_config_uses_contract_defaults(self) -> None:
        config = get_config()

        self.assertEqual(config.default_namespace, "midstream")
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

    def test_the_namespace_set_by_the_deployment_wins_over_the_pods_own(self) -> None:
        os.environ["CONTROL_PLANE_DEFAULT_NAMESPACE"] = " custom-ns "
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write("pod-namespace\n")
        try:
            self.assertEqual(_resolve_default_namespace(in_cluster_namespace_file=handle.name), "custom-ns")
        finally:
            os.unlink(handle.name)

    def test_without_the_variable_the_namespace_is_the_pods_own(self) -> None:
        os.environ.pop("CONTROL_PLANE_DEFAULT_NAMESPACE", None)
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write("pod-namespace\n")
        try:
            self.assertEqual(_resolve_default_namespace(in_cluster_namespace_file=handle.name), "pod-namespace")
            with mock.patch.object(config_module, "IN_CLUSTER_NAMESPACE_FILE", handle.name):
                self.assertEqual(get_config().default_namespace, "pod-namespace")
        finally:
            os.unlink(handle.name)

    def test_with_neither_the_configuration_refuses_to_load(self) -> None:
        """No namespace is ever assumed: outside a pod, the deployment has to name one."""
        os.environ.pop("CONTROL_PLANE_DEFAULT_NAMESPACE", None)
        missing = os.path.join(tempfile.gettempdir(), "midstream-no-such-namespace-file")
        with self.assertRaisesRegex(ValueError, "CONTROL_PLANE_DEFAULT_NAMESPACE is not set"):
            _resolve_default_namespace(in_cluster_namespace_file=missing)
        with mock.patch.object(config_module, "IN_CLUSTER_NAMESPACE_FILE", missing):
            with self.assertRaisesRegex(ValueError, "no in-cluster namespace file"):
                get_config()

    def test_get_config_reads_the_settings_requests_topic(self) -> None:
        os.environ["CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC"] = "develop-materializer-settings-requests"

        self.assertEqual(get_config().settings_requests_topic, "develop-materializer-settings-requests")

    def test_get_config_uses_environment_overrides(self) -> None:
        os.environ["CONTROL_PLANE_TABLE_NAME"] = "develop-state"
        os.environ["CONTROL_PLANE_DEFAULT_NAMESPACE"] = "custom-ns"
        os.environ["CONTROL_PLANE_DEFAULT_LIST_LIMIT"] = "100"
        os.environ["CONTROL_PLANE_RUNTIME_NAME_PREFIX"] = "dedicated-runtime"
        os.environ["CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC"] = "develop-materialization-requests"
        os.environ["CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC"] = "develop-materialization-work"
        os.environ["CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC"] = "develop-materialization-status"
        os.environ["CONTROL_PLANE_TABLE_EVENTS_TOPIC"] = "develop-materialization-table-events"
        os.environ["CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC"] = "develop-materialization-schema-authority-updates"
        os.environ["CONTROL_PLANE_STORAGE_PREFIX"] = "develop"
        os.environ["CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY"] = "hour"
        os.environ["CONTROL_PLANE_CATALOG_BASE_URL"] = "https://materialize.example.com/catalog/v1/"

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
        os.environ["CONTROL_PLANE_RUNTIME_APP_SPEC"] = """
authentication:
  type: scram-sha-512
  username: runtime-user
template:
  pod:
    tolerations:
      - key: app
        operator: Equal
        value: streaming
        effect: NoSchedule
"""

        config = get_config()

        self.assertEqual(config.runtime_app_spec["authentication"]["username"], "runtime-user")
        self.assertEqual(
            config.runtime_app_spec["template"]["pod"]["tolerations"][0]["key"],
            "app",
        )

    def test_get_config_reads_protocol_logging_settings(self) -> None:
        os.environ["PROTOCOL_LOGGING_ENABLED"] = "true"
        os.environ["PROTOCOL_LOG_SLOW_WAIT_SECONDS"] = "45"

        config = get_config()

        self.assertTrue(config.protocol_logging_enabled)
        self.assertEqual(config.protocol_log_slow_wait_seconds, 45)

    def test_an_unknown_partition_granularity_is_refused(self) -> None:
        os.environ["CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY"] = "week"

        with self.assertRaisesRegex(ValueError, "default_partition_granularity must be one of"):
            get_config()

    def test_get_config_has_no_default_destination_unless_one_is_set(self) -> None:
        self.assertIsNone(get_config().default_destination_uri)

        for blank in ("", "   "):
            with self.subTest(value=blank):
                get_config.cache_clear()
                os.environ["CONTROL_PLANE_DEFAULT_DESTINATION_URI"] = blank
                self.assertIsNone(get_config().default_destination_uri)

    def test_get_config_reads_the_default_destination_without_trailing_slashes(self) -> None:
        os.environ["CONTROL_PLANE_DEFAULT_DESTINATION_URI"] = " gs://example-bucket/lake// "

        self.assertEqual(get_config().default_destination_uri, "gs://example-bucket/lake")


if __name__ == "__main__":
    unittest.main()