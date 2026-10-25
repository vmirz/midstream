"""The runtime a materialization is deployed as: nine resources rendered from templates (spec §4.4)."""

from __future__ import annotations

import os
import asyncio
from dataclasses import replace
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from midstream.config import ControlPlaneConfig  # noqa: E402
from midstream.models import Materialization  # noqa: E402
from midstream.scale import DEFAULT_STORAGE_SIZE, storage_size_to_bytes  # noqa: E402
from midstream.runtime import (  # noqa: E402
    build_dedicated_runtime_template_input,
    iter_rendered_resources,
    render_dedicated_runtime_resources,
    spec_hash,
)
from midstream.runtime.templates import STATE_STORE_WRITE_BUFFER_BYTES, STATE_STORE_WRITE_BUFFERS  # noqa: E402
from table_records import build_table_records, table_spec, table_specs  # noqa: E402
from midstream import config as control_plane_config


def build_materialization(**overrides) -> Materialization:
    values = dict(
        materialization_id="mat-1",
        name="shop",
        tables=table_specs("orders"),
        destination_uri="gs://lakehouse",
        runtime_mode="dedicated",
        resolved_runtime_name="kaspr-mat-1",
        namespace="midstream",
        desired_phase="running",
        created_at="2026-07-12T00:00:00Z",
        updated_at="2026-07-12T00:00:00Z",
        created_by="vmirzoyan",
    )
    values.update(overrides)
    return Materialization(**values)


def render(materialization: Materialization | None = None, *, config: ControlPlaneConfig | None = None, **record_overrides) -> dict:
    materialization = build_materialization() if materialization is None else materialization
    return render_dedicated_runtime_resources(
        build_dedicated_runtime_template_input(
            materialization,
            table_records=build_table_records(materialization, **record_overrides),
            config=config,
        )
    )


def shipped_runtime_app_spec() -> dict:
    """The runtime spec `manifests/base` deploys the control plane with."""
    import yaml

    manifest = Path(__file__).resolve().parents[2] / "manifests" / "base" / "apps" / "control-plane.yaml"
    env = yaml.safe_load(manifest.read_text())["spec"]["template"]["kasprContainer"]["env"]
    value = next(item["value"] for item in env if item["name"] == "MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC")
    return yaml.safe_load(value)


def by_suffix(resources: list[dict], suffix: str) -> dict:
    return next(item for item in resources if item["metadata"]["name"].endswith(suffix))


def map_python(resource: dict, index: int = 0) -> str:
    return resource["spec"]["processors"]["operations"][index]["map"]["python"]


def run_map(resource: dict, entrypoint: str, *args, index: int = 0, **namespace):
    """Run a rendered map operation the way Kaspr does: its init, then its function.

    Names given in `namespace` replace what the init defines, so a test can stand in for the
    package's entry point and still see everything else the init built.
    """
    scope: dict = {"context": {}}
    init = resource["spec"]["processors"].get("init", {}).get("python")
    if init:
        exec(init, scope)
    scope.update(namespace)
    exec(map_python(resource, index), scope)
    return scope[entrypoint](*args)


def init_python(resource: dict) -> str:
    return resource["spec"]["processors"]["init"]["python"]


class RuntimeTemplateTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch("midstream.runtime.templates.get_installed_package_revision", return_value="pkg-rev-1")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_runtime_is_an_app_its_state_table_three_agents_and_three_tasks(self) -> None:
        resources = iter_rendered_resources(render())

        self.assertEqual(
            [(item["kind"], item["metadata"]["name"]) for item in resources],
            [
                ("KasprApp", "kaspr-mat-1"),
                ("KasprTable", "kaspr-mat-1-state"),
                ("KasprAgent", "kaspr-mat-1-writer"),
                ("KasprAgent", "kaspr-mat-1-committer"),
                ("KasprAgent", "kaspr-mat-1-settings"),
                ("KasprTask", "kaspr-mat-1-emit-status"),
                ("KasprTask", "kaspr-mat-1-pipeline"),
                ("KasprTask", "kaspr-mat-1-commit-timer"),
            ],
        )
        # Table maintenance is the maintenance app's: no runtime ticks a maintenance clock.
        self.assertNotIn("maintenance", " ".join(item["metadata"]["name"] for item in resources))

    def test_the_app_is_pinned_to_the_package_and_names_its_table(self) -> None:
        rendered = render(
            config=ControlPlaneConfig(
                runtime_app_spec={
                    "bootstrapServers": "kafka:9092",
                    "storage": {"type": "persistent-claim", "class": "standard", "size": "1Gi", "deleteClaim": True},
                }
            )
        )
        app = rendered["kaspr_app"]
        spec = app["spec"]
        self.assertEqual(spec["bootstrapServers"], "kafka:9092")
        self.assertEqual(spec["storage"]["class"], "standard")
        self.assertEqual(spec["template"]["kasprContainer"]["env"][0]["name"], "MATERIALIZER_RUNTIME_MEMBER_ID")
        self.assertEqual(
            spec["pythonPackages"]["packages"][0],
            "git+https://github.com/vmirz/midstream.git@pkg-rev-1#egg=midstream&subdirectory=python",
        )
        annotations = app["metadata"]["annotations"]
        self.assertEqual(annotations["control-plane/package-revision"], "pkg-rev-1")
        self.assertEqual(annotations["control-plane/source-topics"], "orders")
        self.assertEqual(annotations["control-plane/tables"], "orders")
        self.assertNotIn("control-plane/schema-version", annotations)
        table = rendered["kaspr_tables"][0]
        self.assertTrue(table["spec"]["global"])
        self.assertEqual(table["spec"]["partitions"], 1)

    def test_the_writer_stages_records_holding_their_acknowledgements_and_publishes_nothing(self) -> None:
        rendered = render(config=ControlPlaneConfig())
        writer = by_suffix(rendered["kaspr_agents"], "-writer")

        self.assertTrue(writer["spec"]["noack"])
        self.assertTrue(writer["spec"]["isolatedPartitions"])
        self.assertNotIn("output", writer["spec"])
        self.assertEqual(writer["spec"]["input"]["topic"]["name"], "orders")
        for agent in rendered["kaspr_agents"]:
            if agent is not writer:
                self.assertNotIn("noack", agent["spec"])
        env = {item["name"]: item for item in rendered["kaspr_app"]["spec"]["template"]["kasprContainer"]["env"]}
        self.assertEqual(env["K_STREAM_WAIT_EMPTY"]["value"], "false")

        captured: dict = {}

        async def fake_stage(*args, **kwargs):
            captured.update(kwargs, value=args[0], state_table=args[1])

        asyncio.run(run_map(writer, "process_record", {"id": 1}, "state", stage_source_record=fake_stage))
        self.assertEqual(captured["value"], {"id": 1})
        self.assertEqual(captured["state_table"], "state")
        self.assertEqual((captured["materialization_id"], captured["runtime_app_ref"]), ("mat-1", "kaspr-mat-1"))
        (table,) = captured["tables"]
        self.assertEqual(
            (table.materialization_id, table.name, table.source_topic, table.location, table.namespace, table.partition_granularity),
            ("mat-1", "orders", "orders", "gs://lakehouse/materializer/mat-1/orders", "shop", "day"),
        )
        self.assertNotIn("skip_tombstones", captured)

    def test_one_runtime_reads_every_topic_into_its_own_table(self) -> None:
        materialization = build_materialization(tables=table_specs("orders", "order.items", "Order-Items"))
        rendered = render(materialization)
        writer = by_suffix(rendered["kaspr_agents"], "-writer")
        self.assertEqual(writer["spec"]["input"]["topic"]["name"], "orders,order.items,Order-Items")
        self.assertTrue(writer["spec"]["isolatedPartitions"])

        captured: dict = {}

        async def fake_stage(*args, **kwargs):
            captured.update(kwargs)

        asyncio.run(run_map(writer, "process_record", {"id": 1}, "state", stage_source_record=fake_stage))
        self.assertEqual(
            [(table.name, table.source_topic, table.location) for table in captured["tables"]],
            [
                ("orders", "orders", "gs://lakehouse/materializer/mat-1/orders"),
                ("order_items", "order.items", "gs://lakehouse/materializer/mat-1/order_items"),
                ("order_items_2", "Order-Items", "gs://lakehouse/materializer/mat-1/order_items_2"),
            ],
        )

        committer = by_suffix(rendered["kaspr_agents"], "-committer")
        commits: dict = {}

        async def fake_commit(*args, **kwargs):
            commits.update(kwargs)
            return iter(())

        asyncio.run(run_map(committer, "commit", {"kind": "commit_tick"}, "state", handle_commit_event=fake_commit))
        self.assertEqual([table.name for table in commits["tables"]], ["orders", "order_items", "order_items_2"])
        # Which incarnation of each name: a topic removed and added back gets a table of its own.
        self.assertEqual({"6b1c0000-0000-0000-0000-000000000000"}, {table.table_uuid for table in commits["tables"]})
        status = by_suffix(rendered["kaspr_tasks"], "-emit-status")
        reported: dict = {}

        def fake_heartbeat(*args, **kwargs):
            reported.update(kwargs)
            return {}

        run_map(status, "emit_snapshot", "state", build_runtime_heartbeat=fake_heartbeat)
        self.assertEqual(reported["tables"], ["orders", "order_items", "order_items_2"])
        # Still one app: one consumer group, one commits topic, one committer for every table.
        self.assertEqual(len(iter_rendered_resources(rendered)), 8)

    def test_each_table_is_in_its_own_dataset(self) -> None:
        """One runtime may write into several datasets: each table's Iceberg namespace is its dataset."""
        specs = (
            table_spec("orders", "orders"),
            table_spec("invoices", "billing.invoices", dataset="finance", dataset_id="ds-000000000002"),
        )
        rendered = render(build_materialization(tables=specs))

        writer, committer = by_suffix(rendered["kaspr_agents"], "-writer"), by_suffix(rendered["kaspr_agents"], "-committer")
        self.assertNotIn("namespace=", writer["spec"]["processors"]["init"]["python"])
        captured: dict = {}

        async def fake_commit(*args, **kwargs):
            captured.update(kwargs)
            return iter(())

        asyncio.run(run_map(committer, "commit", {"kind": "commit_tick"}, "state", handle_commit_event=fake_commit))
        self.assertEqual(
            [(table.namespace, table.name, table.location) for table in captured["tables"]],
            [
                ("shop", "orders", "gs://lakehouse/materializer/mat-1/orders"),
                ("finance", "invoices", "gs://lakehouse/materializer/mat-1/invoices"),
            ],
        )
        self.assertEqual(("finance", "invoices"), captured["tables"][1].identity.identifier)

    def test_a_runtime_is_not_rendered_before_every_table_exists(self) -> None:
        materialization = build_materialization(tables=table_specs("orders", "customers"))
        (orders, _) = build_table_records(materialization)
        with self.assertRaisesRegex(ValueError, "not created yet: customers"):
            build_dedicated_runtime_template_input(materialization, table_records=[orders])

    def test_the_writer_passes_skip_tombstones_only_when_enabled(self) -> None:
        default = render()
        enabled = render(build_materialization(skip_tombstones=True))
        self.assertNotIn("skip_tombstones", map_python(by_suffix(default["kaspr_agents"], "-writer")))
        self.assertIn("skip_tombstones=True,", map_python(by_suffix(enabled["kaspr_agents"], "-writer")))
        self.assertNotEqual(spec_hash(default), spec_hash(enabled))

    def test_the_committer_owns_the_one_partition_commits_topic_and_publishes_table_events(self) -> None:
        committer = by_suffix(render()["kaspr_agents"], "-committer")
        topic = committer["spec"]["input"]["topic"]
        self.assertTrue(committer["spec"]["input"]["declare"])
        self.assertEqual((topic["name"], topic["partitions"], topic["retention"]), ("kaspr-mat-1-commits", 1, 3600))
        (output,) = committer["spec"]["output"]["topics"]
        self.assertEqual(output["name"], "materialization-table-events")
        self.assertIn('value.get("route") == "events"', output["predicate"]["python"])

        captured: dict = {}

        async def fake_commit(*args, **kwargs):
            captured.update(kwargs)
            return iter(())

        asyncio.run(run_map(committer, "commit", {"kind": "commit_tick"}, "state", handle_commit_event=fake_commit))
        self.assertEqual(captured["commits_topic"], "kaspr-mat-1-commits")
        (table,) = captured["tables"]
        self.assertEqual(table.location, "gs://lakehouse/materializer/mat-1/orders")
        self.assertEqual((table.namespace, table.name), ("shop", "orders"))
        self.assertEqual(table.partition_granularity, "day")

    def test_the_settings_agent_takes_control_messages_for_its_runtime(self) -> None:
        settings = by_suffix(render()["kaspr_agents"], "-settings")
        self.assertEqual(settings["spec"]["input"]["topic"]["name"], "materialization-schema-authority-updates")
        # It sends nothing on: maintenance requests are the maintenance app's.
        self.assertNotIn("output", settings["spec"])

        seen: list = []

        def fake_handle(value, state_table, *, materialization_id):
            seen.append((value["kind"], materialization_id))
            return None

        for message in (
            {"kind": "runtime_settings", "materialization_id": "*"},
            {"kind": "maintenance_request", "materialization_id": "mat-1"},
            {"kind": "maintenance_request", "materialization_id": "mat-2"},
        ):
            run_map(settings, "handle", message, "state", handle_control_message=fake_handle)
        self.assertEqual(seen, [("runtime_settings", "mat-1"), ("maintenance_request", "mat-1")])

    def test_the_pipeline_task_drains_the_outbox_to_the_committer_with_acknowledged_sends(self) -> None:
        pipeline = by_suffix(render()["kaspr_tasks"], "-pipeline")
        self.assertEqual(pipeline["spec"]["schedule"]["interval"], "5s")
        self.assertIn("advance_pipeline", pipeline["spec"]["processors"]["init"]["python"])
        self.assertIn('return advance_pipeline(context["app"], state_table)', map_python(pipeline))
        send = pipeline["spec"]["processors"]["operations"][1]["topicSend"]
        self.assertEqual((send["name"], send["ack"]), ("kaspr-mat-1-commits", True))

    def test_the_timer_ticks_the_committer_from_one_member(self) -> None:
        task = by_suffix(render()["kaspr_tasks"], "-commit-timer")
        self.assertTrue(task["spec"]["onLeader"])
        self.assertEqual(task["spec"]["schedule"]["interval"], "5s")
        self.assertEqual(task["spec"]["processors"]["operations"][1]["topicSend"]["name"], "kaspr-mat-1-commits")
        self.assertEqual(run_map(task, "build")["kind"], "commit_tick")

    def test_the_heartbeat_task_reports_every_interval(self) -> None:
        status = by_suffix(render()["kaspr_tasks"], "-emit-status")
        self.assertEqual(status["spec"]["schedule"]["interval"], "30s")
        self.assertIn("build_runtime_heartbeat(", map_python(status))

    def test_child_names_derive_from_the_app_name_and_stay_within_kubernetes_bounds(self) -> None:
        rendered = render(build_materialization(resolved_runtime_name="develop-mat-mat-1"))
        self.assertEqual(rendered["kaspr_tables"][0]["metadata"]["name"], "develop-mat-mat-1-state")
        self.assertEqual(
            [item["metadata"]["name"] for item in rendered["kaspr_agents"]],
            ["develop-mat-mat-1-writer", "develop-mat-mat-1-committer", "develop-mat-mat-1-settings"],
        )

        long = render(build_materialization(resolved_runtime_name="develop-materializer-fusion-outbound-item-with-an-extraordinarily-long-suffix"))
        self.assertLessEqual(len(long["kaspr_app"]["metadata"]["name"]), 59)
        table_name = long["kaspr_tables"][0]["metadata"]["name"]
        self.assertLessEqual(len(table_name), 57)
        self.assertTrue(table_name.endswith("-state"))

    def test_the_deployment_runtime_spec_is_merged_under_the_materialization(self) -> None:
        rendered = render(
            config=ControlPlaneConfig(
                runtime_app_spec={
                    "authentication": {"type": "scram-sha-512", "username": "runtime-user"},
                    "template": {"pod": {"tolerations": [{"key": "app", "operator": "Equal", "value": "streaming", "effect": "NoSchedule"}]}},
                    "pythonPackages": {
                        "packages": [
                            "git+https://github.com/vmirz/midstream.git@main#egg=midstream&subdirectory=python",
                            "acme-runtime-support==1.2.3",
                        ]
                    },
                    "config": {"schedulerEnabled": True},
                }
            )
        )
        spec = rendered["kaspr_app"]["spec"]
        self.assertEqual(spec["authentication"]["username"], "runtime-user")
        self.assertEqual(spec["template"]["pod"]["tolerations"][0]["key"], "app")
        self.assertIn("@pkg-rev-1#", spec["pythonPackages"]["packages"][0])
        self.assertEqual(spec["pythonPackages"]["packages"][1], "acme-runtime-support==1.2.3")
        self.assertTrue(spec["config"]["schedulerEnabled"])

    def test_runtime_env_from_the_deployment_is_merged_with_the_defaults_by_name(self) -> None:
        env = render(
            config=ControlPlaneConfig(
                runtime_app_spec={
                    "template": {
                        "kasprContainer": {
                            "env": [
                                {"name": "MATERIALIZER_RUNTIME_MEMBER_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                                {"name": "GOOGLE_APPLICATION_CREDENTIALS", "value": "/app/gcp/key.json"},
                                {"name": "K_STREAM_WAIT_EMPTY", "value": "true"},
                            ]
                        }
                    }
                }
            )
        )["kaspr_app"]["spec"]["template"]["kasprContainer"]["env"]
        names = [item["name"] for item in env]
        self.assertEqual(
            sorted(names),
            [
                "GOOGLE_APPLICATION_CREDENTIALS",
                "K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER",
                "K_STORE_ROCKSDB_WRITE_BUFFER_SIZE",
                "K_STREAM_WAIT_EMPTY",
                "MATERIALIZER_RUNTIME_MEMBER_ID",
            ],
        )
        self.assertEqual([item for item in env if item["name"] == "K_STREAM_WAIT_EMPTY"], [{"name": "K_STREAM_WAIT_EMPTY", "value": "true"}])

    def test_scale_comes_from_the_materialization_and_a_stop_scales_to_zero(self) -> None:
        running = build_materialization(replicas=16, storage_size="8Gi")
        config = ControlPlaneConfig(runtime_app_spec={"replicas": 1, "storage": {"type": "persistent-claim", "size": "1Gi"}})
        rendered = render(running, config=config)
        stopped = render(replace(running, desired_phase="stopped"), config=config)
        self.assertEqual((rendered["kaspr_app"]["spec"]["replicas"], rendered["kaspr_app"]["spec"]["storage"]["size"]), (16, "8Gi"))
        self.assertEqual(stopped["kaspr_app"]["spec"]["replicas"], 0)
        for kind in ("kaspr_agents", "kaspr_tasks", "kaspr_tables"):
            self.assertEqual(stopped[kind], rendered[kind])
        self.assertNotEqual(spec_hash(stopped), spec_hash(rendered))
        defaults = render()["kaspr_app"]["spec"]
        self.assertEqual((defaults["replicas"], defaults["storage"]["size"]), (1, "64Mi"))

    def test_members_are_autoscaled_as_the_deployment_says(self) -> None:
        # The shipped runtime spec has a VerticalPodAutoscaler set each member's CPU request from its
        # use (handbook 02 §2.10c). The control plane renders it as written, and only from the
        # deployment, so a changed one reaches running runtimes as any changed spec does.
        shipped = shipped_runtime_app_spec()
        spec = render(config=ControlPlaneConfig(runtime_app_spec=shipped))["kaspr_app"]["spec"]
        self.assertEqual(spec["verticalPodAutoscaler"], shipped["verticalPodAutoscaler"])
        self.assertTrue(spec["verticalPodAutoscaler"]["enabled"])
        self.assertEqual(spec["verticalPodAutoscaler"]["controlledResources"], ["cpu"])
        stopped = render(build_materialization(desired_phase="stopped"), config=ControlPlaneConfig(runtime_app_spec=shipped))
        self.assertEqual(stopped["kaspr_app"]["spec"]["verticalPodAutoscaler"], shipped["verticalPodAutoscaler"])
        off = ControlPlaneConfig(runtime_app_spec={**shipped, "verticalPodAutoscaler": {"enabled": False}})
        self.assertNotEqual(spec_hash(render(config=off)), spec_hash(render(config=ControlPlaneConfig(runtime_app_spec=shipped))))
        self.assertNotIn("verticalPodAutoscaler", render()["kaspr_app"]["spec"])

    def test_the_state_store_flushes_its_writes_long_before_they_could_fill_a_members_volume(self) -> None:
        # RocksDB keeps a memtable's writes in its write-ahead log until it flushes, at the write
        # buffer's size; at Kaspr's 64 MiB a busy runtime's log alone would outgrow a 64Mi volume.
        # Set for the app: kaspr-operator's CRD drops a KasprTable's own options.
        deployment_env = [{"name": "K_STORE_ROCKSDB_WRITE_BUFFER_SIZE", "value": "1"}, {"name": "OTHER", "value": "x"}]
        for config, expected in (
            (None, "4194304"),
            # A deployment that sets one itself is taken at its word.
            (ControlPlaneConfig(runtime_app_spec={"template": {"kasprContainer": {"env": deployment_env}}}), "1"),
        ):
            env = {item["name"]: item.get("value") for item in render(config=config)["kaspr_app"]["spec"]["template"]["kasprContainer"]["env"]}
            self.assertEqual((env["K_STORE_ROCKSDB_WRITE_BUFFER_SIZE"], env["K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER"]), (expected, "2"))
        self.assertNotIn("options", render()["kaspr_tables"][0]["spec"])
        most_logged = STATE_STORE_WRITE_BUFFER_BYTES * STATE_STORE_WRITE_BUFFERS
        self.assertLessEqual(most_logged, storage_size_to_bytes(DEFAULT_STORAGE_SIZE) // 8)

    def test_the_spec_hash_ignores_schema_changes(self) -> None:
        first = build_materialization()
        later = replace(first, updated_at="2026-07-12T00:05:00Z")
        self.assertEqual(spec_hash(render(first)), spec_hash(render(later, schema_id=7, metadata_location="gs://elsewhere")))

    def test_an_hourly_table_is_written_hourly(self) -> None:
        rendered = render(build_materialization(partition_granularity="hour"))
        self.assertIn('partition_granularity="hour"', init_python(by_suffix(rendered["kaspr_agents"], "-writer")))
        self.assertIn('partition_granularity="hour"', init_python(by_suffix(rendered["kaspr_agents"], "-committer")))



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()
