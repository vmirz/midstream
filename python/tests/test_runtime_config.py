from __future__ import annotations

import os
import unittest

from materializer_control_plane.runtime_app.config import RuntimeAppConfig, get_runtime_app_config


class RuntimeAppConfigTests(unittest.TestCase):
    def tearDown(self) -> None:
        for key in (
            "MATERIALIZER_PROTOCOL_LOGGING_ENABLED",
            "MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS",
            "MATERIALIZER_RUNTIME_COMMIT_INTERVAL_SECONDS",
            "MATERIALIZER_RUNTIME_COMPACTION_ENABLED",
        ):
            os.environ.pop(key, None)

    def test_default_runtime_app_config_includes_the_processing_defaults(self) -> None:
        config = get_runtime_app_config()

        self.assertEqual(config.malformed_record_mode, "continue_with_error")
        self.assertEqual(config.max_open_file_age_seconds, 120)
        self.assertEqual(config.max_open_file_age_ceiling_seconds, 600)
        self.assertEqual(config.commit_interval_seconds, 30)
        self.assertEqual(config.commit_timer_interval, "5s")
        self.assertTrue(config.compaction_enabled)
        self.assertEqual(config.upload_interval, "5s")
        self.assertGreater(config.target_file_size_bytes, config.target_row_group_size_bytes)
        self.assertGreater(config.upload_worker_count, 0)
        self.assertGreater(config.max_pending_upload_batches, 0)
        self.assertGreater(config.max_pending_upload_bytes, 0)
        self.assertTrue(config.staging_root_path.startswith("/"))

    def test_runtime_app_config_reads_protocol_logging_settings(self) -> None:
        os.environ["MATERIALIZER_PROTOCOL_LOGGING_ENABLED"] = "true"
        os.environ["MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS"] = "45"

        config = get_runtime_app_config()

        self.assertTrue(config.protocol_logging_enabled)
        self.assertEqual(config.protocol_log_slow_wait_seconds, 45)

    def test_runtime_app_config_reads_commit_and_maintenance_settings(self) -> None:
        os.environ["MATERIALIZER_RUNTIME_COMMIT_INTERVAL_SECONDS"] = "12"
        os.environ["MATERIALIZER_RUNTIME_COMPACTION_ENABLED"] = "false"

        config = get_runtime_app_config()

        self.assertEqual(config.commit_interval_seconds, 12)
        self.assertFalse(config.compaction_enabled)

    def test_settings_that_cannot_work_together_are_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "compaction_small_file_bytes"):
            RuntimeAppConfig(compaction_small_file_bytes=512 * 1024 * 1024)
        with self.assertRaisesRegex(ValueError, "seal_schema_wait_seconds"):
            RuntimeAppConfig(seal_schema_wait_seconds=601)


if __name__ == "__main__":
    unittest.main()
