"""Midstream's own Kafka clients connect to a TLS listener the way Kaspr's do.

A KasprApp's `tls: {}` makes the operator set `K_KAFKA_SECURITY_PROTOCOL` to `SSL`, or `SASL_SSL` with
authentication. aiokafka refuses to make any client for those without an `ssl_context`, so before the
clients took one, TLS left the topic list, schema discovery, topic cleanup and the live tail failing
while Kaspr's own agents ran. Each client here is built as the apps build it, from the environment,
against a stand-in for aiokafka that holds it to that rule.
"""

from __future__ import annotations

import asyncio
from importlib.util import find_spec
import os
from pathlib import Path
import ssl
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_schema_discovery import build_materialization  # noqa: E402

from midstream.kafka.admin import AIOKafkaAdminClient  # noqa: E402
from midstream.kafka.metadata import AIOKafkaMetadataClient  # noqa: E402
from midstream.kafka.tls import kafka_ssl_context  # noqa: E402
from midstream.schema.discovery import KafkaTopicDiscoveryService  # noqa: E402
from midstream.tail_app.consumer import build_consumer, kafka_settings  # noqa: E402

#: What the operator sets for a listener with TLS, and without.
TLS_PROTOCOLS = ("SSL", "SASL_SSL")
PLAINTEXT_PROTOCOLS = ("PLAINTEXT", "SASL_PLAINTEXT")


def kaspr_environment(protocol: str) -> dict[str, str]:
    """The `K_KAFKA_*` the operator gives every pod of an app."""
    environment = {"K_KAFKA_BOOTSTRAP_SERVERS": "kafka-bootstrap:9093", "K_KAFKA_SECURITY_PROTOCOL": protocol}
    if protocol.startswith("SASL"):
        environment |= {
            "K_KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "K_KAFKA_AUTH_USERNAME": "midstream",
            "K_KAFKA_AUTH_PASSWORD": "not-a-real-password",
        }
    return environment


class StandInKafka:
    """aiokafka's client classes as far as these clients use them, each made under aiokafka's rule."""

    def __init__(self) -> None:
        self.made: list[dict] = []
        made = self.made

        class Client:
            def __init__(self, *topics: str, **options) -> None:
                protocol = options.get("security_protocol", "PLAINTEXT")
                if protocol in TLS_PROTOCOLS and options.get("ssl_context") is None:
                    # aiokafka/client.py, AIOKafkaClient.__init__
                    raise ValueError("`ssl_context` is mandatory if security_protocol=='SSL'")
                made.append(options)
                self._client = types.SimpleNamespace(fetch_all_metadata=self._metadata)

            async def _metadata(self):
                return types.SimpleNamespace(partitions_for_topic=lambda topic: None)

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            async def close(self) -> None:
                return None

            async def list_topics(self) -> list[str]:
                return []

            async def describe_topics(self, topics=None) -> list[dict]:
                return []

        self.module = types.SimpleNamespace(
            AIOKafkaAdminClient=Client,
            AIOKafkaConsumer=Client,
            TopicPartition=lambda topic, partition: (topic, partition),
        )

    def import_module(self, name: str):
        if name in ("aiokafka", "aiokafka.admin"):
            return self.module
        raise AssertionError(f"unexpected import {name}")


class KafkaSslContextTests(unittest.TestCase):
    def test_a_tls_protocol_gets_a_context_that_checks_the_brokers(self) -> None:
        for protocol in (*TLS_PROTOCOLS, " sasl_ssl "):
            with self.subTest(protocol=protocol):
                context = kafka_ssl_context(protocol)
                self.assertIsInstance(context, ssl.SSLContext)
                self.assertEqual(ssl.CERT_REQUIRED, context.verify_mode)
                self.assertTrue(context.check_hostname)
        self.assertIs(kafka_ssl_context("SSL"), kafka_ssl_context("SASL_SSL"), "one context per process")

    def test_it_is_the_context_kaspr_gives_its_own_clients(self) -> None:
        # Kaspr builds `ssl.create_default_context()` for SSL and SASL_SSL alike
        # (kaspr/types/settings.py, `_prepare_kafka_credentials`): the same checks, the same trust.
        ours, kasprs = kafka_ssl_context("SASL_SSL"), ssl.create_default_context()
        for setting in ("verify_mode", "check_hostname", "verify_flags", "minimum_version", "maximum_version", "options"):
            self.assertEqual(getattr(kasprs, setting), getattr(ours, setting), setting)
        self.assertEqual(kasprs.cert_store_stats(), ours.cert_store_stats())

    def test_a_plaintext_protocol_gets_none(self) -> None:
        for protocol in (*PLAINTEXT_PROTOCOLS, None, ""):
            with self.subTest(protocol=protocol):
                self.assertIsNone(kafka_ssl_context(protocol))


class ClientsOverTlsTests(unittest.TestCase):
    """Every client the control plane and the tail app make, for each protocol the operator sets."""

    def _check(self, protocol: str, made: list[dict]) -> None:
        self.assertTrue(made, "no Kafka client was made")
        for options in made:
            self.assertEqual(protocol, options["security_protocol"])
            if protocol in TLS_PROTOCOLS:
                self.assertIs(kafka_ssl_context(protocol), options["ssl_context"])
            else:
                self.assertIsNone(options.get("ssl_context"))

    def _each_protocol(self, run) -> None:
        for protocol in (*TLS_PROTOCOLS, *PLAINTEXT_PROTOCOLS):
            with self.subTest(protocol=protocol):
                kafka = StandInKafka()
                with patch.dict(os.environ, kaspr_environment(protocol)):
                    run(kafka)
                self._check(protocol, kafka.made)

    def test_the_topic_list_and_offsets_client(self) -> None:
        def run(kafka: StandInKafka) -> None:
            with patch("midstream.kafka.metadata.import_module", side_effect=kafka.import_module):
                client = AIOKafkaMetadataClient.from_environment()
                self.addCleanup(client._runner.close)
                asyncio.run(client.list_topics_async())

        self._each_protocol(run)

    def test_the_admin_client_that_deletes_topics_and_groups(self) -> None:
        def run(kafka: StandInKafka) -> None:
            with patch("midstream.kafka.admin.import_module", side_effect=kafka.import_module):
                client = AIOKafkaAdminClient.from_environment()
                self.addCleanup(client._runner.close)
                client.list_topics()

        self._each_protocol(run)

    def test_schema_discovery(self) -> None:
        def run(kafka: StandInKafka) -> None:
            with patch("midstream.schema.discovery.import_module", side_effect=kafka.import_module):
                service = KafkaTopicDiscoveryService.from_environment()
                self.addCleanup(service._runner.close)
                service.discover(build_materialization(), sample_records=1, sample_seconds=0.1)

        self._each_protocol(run)

    def test_the_tails_consumers(self) -> None:
        def run(kafka: StandInKafka) -> None:
            with patch("midstream.tail_app.consumer.import_module", side_effect=kafka.import_module):
                build_consumer(fetch_wait_ms=100)

        self._each_protocol(run)


@unittest.skipUnless(find_spec("aiokafka"), "aiokafka comes with the Kaspr image, not the package")
class RealAiokafkaTests(unittest.TestCase):
    """The stand-in's rule is aiokafka's: it refuses TLS without a context, and takes ours."""

    def test_aiokafka_takes_the_tails_settings_for_every_protocol(self) -> None:
        import aiokafka

        async def make(options: dict) -> None:
            consumer = aiokafka.AIOKafkaConsumer(**options)
            await consumer.stop()

        for protocol in (*TLS_PROTOCOLS, *PLAINTEXT_PROTOCOLS):
            with self.subTest(protocol=protocol):
                options = kafka_settings(kaspr_environment(protocol))
                asyncio.run(make(options))
                if protocol in TLS_PROTOCOLS:
                    with self.assertRaisesRegex(ValueError, "ssl_context"):
                        asyncio.run(make(options | {"ssl_context": None}))


if __name__ == "__main__":
    unittest.main()
