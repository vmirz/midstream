from __future__ import annotations

import importlib
import unittest


class PackageLayoutTests(unittest.TestCase):
    def test_expected_subpackages_are_importable(self) -> None:
        subpackages = [
            "handlers",
            "identity",
            "kubernetes",
            "models",
            "observation",
            "runtime",
            "runtime_app",
            "scheduler",
            "schema",
            "services",
            "settings",
            "storage",
            "tail_app",
            "utils",
            "web",
        ]

        for name in subpackages:
            with self.subTest(name=name):
                module = importlib.import_module(f"midstream.{name}")
                self.assertIsNotNone(module)

    def test_the_tail_app_imports_without_kafka_or_a_running_loop(self) -> None:
        """Its manifests import its handlers at load time, before any Kafka client or loop exists."""
        for name in ("handlers", "views", "worker", "table_tail", "buffer", "serve", "ingest", "routing", "auth", "peek", "events", "consumer", "config", "control_plane", "http"):
            with self.subTest(module=name):
                self.assertIsNotNone(importlib.import_module(f"midstream.tail_app.{name}"))


if __name__ == "__main__":
    unittest.main()