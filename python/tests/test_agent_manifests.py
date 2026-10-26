"""The chart's agents, tasks and views duplicate knowledge that lives in code; keep the copies honest.

Everything here reads the chart as `helm template` renders it (`chart_support.py`), with the values
files under `charts/midstream/ci/`: one per installation shape, the way the deployment is made.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
from pathlib import Path
import re
import shutil
import subprocess
import sys
import types
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from chart_support import RELEASE, VALUES_FILES, bare, document, documents, python_sources, render  # noqa: E402

from midstream.handlers.maintenance import (
    MAINTENANCE_STEP_TRIGGERS,
    MAINTENANCE_STEPS,
)
from midstream.models import MaterializationWorkEvent
from midstream.models.events import WORK_STEPS
from midstream.services.workflow import WORKFLOW_APPLY_STEPS

def _filter_step_set(agent: dict) -> set[str]:
    """Extract the set literal from an agent's `select_work` input filter."""
    source = "\n".join(python_sources(agent))
    match = re.search(r"return value\.get\(\"step\"\) in (\{.*?\})", source, re.DOTALL)
    assert match is not None, f"no step filter found in {agent['metadata']['name']}"
    # The YAML block is indented; ast handles the literal once the indentation is gone.
    literal = "\n".join(line.strip() for line in match.group(1).splitlines())
    return set(ast.literal_eval(literal))


class AgentManifestTests(unittest.TestCase):
    def test_workflow_apply_agent_filter_matches_dispatcher(self) -> None:
        """Every step the dispatcher handles must survive the agent's input filter.

        The filter is a YAML predicate and cannot import the dispatcher, so the two lists are
        maintained by hand. When `cleanup_destination_objects` was added to the dispatcher but not
        to the filter, every delete published the event, consumed it, and dropped it silently:
        no error, no log, and destination objects were never removed.
        """
        declared = _filter_step_set(document("KasprAgent", "workflow-apply"))

        missing = WORKFLOW_APPLY_STEPS - declared
        self.assertEqual(
            missing,
            set(),
            f"steps the dispatcher handles but the agent filter drops: {sorted(missing)}",
        )

        extra = declared - WORKFLOW_APPLY_STEPS
        self.assertEqual(
            extra,
            set(),
            f"steps the agent accepts but the dispatcher cannot handle: {sorted(extra)}",
        )

    def test_maintenance_agent_filter_matches_handler(self) -> None:
        """Scheduled maintenance steps face the same silent-drop hazard as workflow steps.

        A cron task publishes the work event regardless of whether the agent accepts it, so a step
        missing from this filter fails as a task that appears to run and never does anything.
        """
        declared = _filter_step_set(document("KasprAgent", "maintenance"))

        missing = MAINTENANCE_STEPS - declared
        self.assertEqual(
            missing,
            set(),
            f"steps the maintenance handler handles but the agent filter drops: {sorted(missing)}",
        )

        extra = declared - MAINTENANCE_STEPS
        self.assertEqual(
            extra,
            set(),
            f"steps the maintenance agent accepts but the handler ignores: {sorted(extra)}",
        )

    def test_every_maintenance_step_is_scheduled_by_a_task(self) -> None:
        """A handler step that nothing schedules never runs, however correct the handler is."""
        task_sources = "\n".join(source for task in documents("KasprTask") for source in python_sources(task))
        unscheduled = {
            step
            for step, builder in MAINTENANCE_STEP_TRIGGERS.items()
            if builder not in task_sources
        }
        self.assertEqual(
            unscheduled,
            set(),
            f"maintenance steps no task schedules: {sorted(unscheduled)}",
        )


#: Entry points that resolve the effective configuration from the settings table. An operation
#: that calls one without passing `settings_table` silently runs on the environment alone, so an
#: administrator's change would apply everywhere except there.
CONFIG_RESOLVING_ENTRYPOINTS = (
    "handle_request_event_for_output",
    "handle_work_event_for_routed_output",
    "handle_work_event_for_routed_output_async",
    "handle_observation_work_for_output",
    "handle_observation_work_for_output_async",
    "handle_table_event",
    "handle_maintenance_work",
    "handle_maintenance_work_async",
    "handle_settings_request_for_output",
    "create_materialization",
    "update_materialization",
    "request_maintenance",
    "catalog_config_view",
    "catalog_namespaces_view",
    "catalog_namespace_view",
    "catalog_tables_view",
    "catalog_load_table_view",
    "catalog_commit_view",
    "catalog_metrics_view",
    "settings_view",
    "settings_scope_view",
    "update_settings",
    "settings_history_view",
    "list_topics_view",
    "analyze_topics_view",
    "analyze_topics_view_async",
    "handle_identity_request",
    "sign_in",
    "refresh_session",
    "session_view",
    "change_password",
    "create_user_request",
    "set_user_password_request",
    "audit_view",
)


class SettingsWiringTests(unittest.TestCase):
    """Settings reach every component through its own table binding; keep the bindings complete."""

    def _operations(self):
        for rendered in documents():
            if rendered.get("kind") not in ("KasprAgent", "KasprWebView"):
                continue
            for operation in rendered["spec"]["processors"].get("operations", []):
                yield rendered["metadata"]["name"], operation

    def test_every_operation_that_reads_configuration_binds_the_settings_table(self) -> None:
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        checked = 0
        for name, operation in self._operations():
            source = (operation.get("map") or {}).get("python", "")
            if not source:
                continue
            calls = [
                node
                for node in ast.walk(ast.parse(source))
                if isinstance(node, ast.Call) and _called_name(node) in CONFIG_RESOLVING_ENTRYPOINTS
            ]
            if not calls:
                continue
            checked += 1
            with self.subTest(resource=name, operation=operation["name"]):
                bindings = {table["paramName"]: table["name"] for table in operation.get("tables") or []}
                self.assertEqual(
                    bindings.get("settings_table"),
                    f"{RELEASE}-settings",
                    f"{name} calls {_called_name(calls[0])} but does not bind the settings table",
                )
                for call in calls:
                    passed = [arg for arg in call.args if isinstance(arg, ast.Name)] + [
                        keyword.value for keyword in call.keywords if isinstance(keyword.value, ast.Name)
                    ]
                    self.assertIn(
                        "settings_table",
                        {arg.id for arg in passed},
                        f"{name} calls {_called_name(call)} without passing settings_table",
                    )
        self.assertGreaterEqual(checked, 22, "the scan found fewer config-reading operations than exist")


async def _run_as_later_kaspr_operation(func, value, **tables) -> list:
    """What Kaspr 0.11 does with a map that is not the first operation of its pipeline.

    `AgentProcessorMapOperator.process` awaits the call when it returns an awaitable, then the
    processor runs `next_values.extend(ensure_generator(value, async_gen=isasyncgen(value)))`: a
    generator is iterated, an async generator raises `TypeError`, anything else is one value.
    """
    result = func(value, **tables)
    if inspect.isawaitable(result):
        result = await result
    if inspect.isasyncgen(result):
        raise TypeError("'async_generator' object is not iterable")
    if inspect.isgenerator(result):
        return list(result)
    return [result]


class WorkflowApplyEventLoopTests(unittest.TestCase):
    """The workflow agent shares its event loop with the API and every other agent.

    Its map once ran destination cleanup synchronously, so deleting a large bucket held the loop:
    the API accepted no connections until every object was gone. The map is a coroutine so that
    cleanup can wait on a worker thread instead — and it must stay one Kaspr can consume.
    """

    def _execute_work(self):
        try:
            import yaml
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        agent = document("KasprAgent", "workflow-apply")
        operations = agent["spec"]["processors"]["operations"]
        self.assertEqual(
            [operation["name"] for operation in operations],
            agent["spec"]["processors"]["pipeline"],
        )
        return operations[1]["map"]

    def test_execute_work_is_a_coroutine_and_not_an_async_generator(self) -> None:
        operation = self._execute_work()
        tree = ast.parse(operation["python"])
        entrypoint = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == operation["entrypoint"]
        )

        self.assertIsInstance(entrypoint, ast.AsyncFunctionDef, "a sync map runs cleanup on the event loop")
        self.assertFalse(
            any(isinstance(node, (ast.Yield, ast.YieldFrom)) for node in ast.walk(entrypoint)),
            "Kaspr cannot iterate an async generator returned by a later pipeline operation",
        )
        calls = {_called_name(node) for node in ast.walk(entrypoint) if isinstance(node, ast.Call)}
        self.assertIn("handle_work_event_for_routed_output_async", calls)

    def test_kaspr_sends_each_output_execute_work_returns(self) -> None:
        operation = self._execute_work()
        outputs = [{"route": "work", "value": {"step": "purge_materialization_records"}}, {"route": "topic"}]
        received = {}

        async def handle_work_event_for_routed_output_async(value, table, discovery_service, **kwargs):
            received.update(value=value, table=table, **kwargs)
            await asyncio.sleep(0)
            return (output for output in outputs)

        scope = {
            "workflow_apply_handler": types.SimpleNamespace(
                handle_work_event_for_routed_output_async=handle_work_event_for_routed_output_async,
            ),
            "discovery_service": object(),
            "kaspr_app_client": object(),
            "kafka_admin_client": object(),
        }
        exec(compile(operation["python"], "<workflow-apply>", "exec"), scope)
        tables = {"control_plane_table": object(), "settings_table": object()}

        sent = asyncio.run(
            _run_as_later_kaspr_operation(scope[operation["entrypoint"]], {"step": "x"}, **tables)
        )

        self.assertEqual(sent, outputs)
        self.assertIs(received["table"], tables["control_plane_table"])
        self.assertIs(received["settings_table"], tables["settings_table"])


def _entrypoint(operation: dict) -> ast.AST:
    tree = ast.parse(operation["python"])
    return next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == operation["entrypoint"]
    )


class MaintenanceEventLoopTests(unittest.TestCase):
    """The maintenance agent shares its event loop with the API and every other agent.

    Its map ran object-manifest compaction — about three GCS requests per commit marker, for up to
    a hundred destinations — and the consumer-lag lookups synchronously, holding the loop every
    five minutes and every minute. The map is a coroutine so both can wait off the loop. It hands
    the work it finds for each materialization back to the work topic, so it must stay one whose
    outputs Kaspr sends.
    """

    def _document(self) -> dict:
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        agent = document("KasprAgent", "maintenance")
        operations = agent["spec"]["processors"]["operations"]
        self.assertEqual(
            [operation["name"] for operation in operations],
            agent["spec"]["processors"]["pipeline"],
        )
        return agent

    def _execute_maintenance(self) -> dict:
        return self._document()["spec"]["processors"]["operations"][1]["map"]

    def test_execute_maintenance_is_a_coroutine_and_not_an_async_generator(self) -> None:
        operation = self._execute_maintenance()
        entrypoint = _entrypoint(operation)

        self.assertIsInstance(entrypoint, ast.AsyncFunctionDef, "a sync map compacts on the event loop")
        self.assertFalse(
            any(isinstance(node, (ast.Yield, ast.YieldFrom)) for node in ast.walk(entrypoint)),
            "Kaspr cannot iterate an async generator returned by a later pipeline operation",
        )
        calls = {_called_name(node) for node in ast.walk(entrypoint) if isinstance(node, ast.Call)}
        self.assertIn("handle_maintenance_work_async", calls)
        awaited = {
            _called_name(node.value)
            for node in ast.walk(entrypoint)
            if isinstance(node, ast.Await) and isinstance(node.value, ast.Call)
        }
        self.assertIn("handle_maintenance_work_async", awaited)

    def test_kaspr_sends_each_output_execute_maintenance_returns(self) -> None:
        operation = self._execute_maintenance()
        outputs = [
            {"route": "work", "key": "materialization:mat-1", "value": {"step": "record_consumer_lag"}},
            {"route": "work", "key": "materialization:mat-2", "value": {"step": "purge_materialization_records"}},
        ]
        received = {}

        async def handle_maintenance_work_async(value, table, **kwargs):
            received.update(value=value, table=table, **kwargs)
            await asyncio.sleep(0)
            return (output for output in outputs)

        metadata_client = object()
        scope = {
            "maintenance_handler": types.SimpleNamespace(
                handle_maintenance_work_async=handle_maintenance_work_async,
            ),
            "metadata_client": metadata_client,
        }
        exec(compile(operation["python"], "<maintenance>", "exec"), scope)
        tables = {"control_plane_table": object(), "settings_table": object()}

        sent = asyncio.run(
            _run_as_later_kaspr_operation(scope[operation["entrypoint"]], {"step": "x"}, **tables)
        )

        self.assertEqual(sent, outputs)
        self.assertIs(received["table"], tables["control_plane_table"])
        self.assertIs(received["settings_table"], tables["settings_table"])
        self.assertIs(received["metadata_client"], metadata_client)

    def test_the_agent_hands_work_back_to_the_topic_it_consumes_keyed_by_materialization(self) -> None:
        """A scheduled step arrives keyed `materialization:__system__`; anything it wrote would land
        in that event's shard of the state table instead of the materialization's own."""
        agent = self._document()
        topics = agent["spec"]["output"]["topics"]

        self.assertEqual(
            [topic["name"] for topic in topics],
            [agent["spec"]["input"]["topic"]["name"], f"{RELEASE}-maintenance-requests", f"{RELEASE}-tail-requests"],
        )
        scope: dict = {}
        for selector in ("keySelector", "valueSelector", "predicate"):
            exec(compile(topics[0][selector]["python"], f"<{selector}>", "exec"), scope)
        routed = {"route": "work", "key": "materialization:mat-1", "value": {"step": "record_consumer_lag"}}
        self.assertEqual(scope["select_key"](routed), "materialization:mat-1")
        self.assertEqual(scope["select_value"](routed), routed["value"])
        self.assertTrue(scope["should_send"](routed))
        self.assertFalse(scope["should_send"]({**routed, "route": "scheduler"}))
        self.assertFalse(scope["should_send"]({**routed, "route": "maintenance"}))
        self.assertFalse(scope["should_send"](None))

    def test_the_table_ticks_go_to_the_maintenance_app_keyed_by_table(self) -> None:
        topic = self._document()["spec"]["output"]["topics"][1]
        scope: dict = {}
        for selector in ("keySelector", "valueSelector", "predicate"):
            exec(compile(topic[selector]["python"], f"<{selector}>", "exec"), scope)
        routed = {"route": "maintenance", "key": "maintenance:mat-1:orders", "value": {"kind": "maintenance_tick"}}
        self.assertEqual(scope["select_key"](routed), "maintenance:mat-1:orders")
        self.assertEqual(scope["select_value"](routed), routed["value"])
        self.assertTrue(scope["should_send"](routed))
        self.assertFalse(scope["should_send"]({**routed, "route": "work"}))
        # Declared here as well as by the app, whichever starts first.
        self.assertEqual((True, 6), (topic["declare"], topic["partitions"]))

    def test_the_tail_ticks_go_to_the_tail_app_keyed_by_table_on_64_partitions(self) -> None:
        topic = self._document()["spec"]["output"]["topics"][2]
        scope: dict = {}
        for selector in ("keySelector", "valueSelector", "predicate"):
            exec(compile(topic[selector]["python"], f"<{selector}>", "exec"), scope)
        routed = {"route": "tail", "key": "tail:mat-1:orders", "value": {"kind": "tail_tick"}}
        self.assertEqual(f"{RELEASE}-tail-requests", topic["name"])
        self.assertEqual(scope["select_key"](routed), "tail:mat-1:orders")
        self.assertEqual(scope["select_value"](routed), routed["value"])
        self.assertTrue(scope["should_send"](routed))
        for route in ("work", "maintenance"):
            self.assertFalse(scope["should_send"]({**routed, "route": route}))
        self.assertFalse(scope["should_send"](None))
        # The partitions bound the tail app's members; declared here as well as by the app.
        self.assertEqual((True, 64), (topic["declare"], topic["partitions"]))


class TopicsAnalyzeEventLoopTests(unittest.TestCase):
    """`POST /topics/analyze` waited on Kafka on the event loop that serves every request."""

    def _analyze(self) -> dict:
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        view = document("KasprWebView", "topics-analyze")
        return view["spec"]["processors"]["operations"][0]["map"]

    def test_the_view_awaits_the_async_analysis(self) -> None:
        operation = self._analyze()
        entrypoint = _entrypoint(operation)

        self.assertIsInstance(entrypoint, ast.AsyncFunctionDef)
        self.assertFalse(any(isinstance(node, (ast.Yield, ast.YieldFrom)) for node in ast.walk(entrypoint)))
        awaited = {
            _called_name(node.value)
            for node in ast.walk(entrypoint)
            if isinstance(node, ast.Await) and isinstance(node.value, ast.Call)
        }
        self.assertIn("analyze_topics_view_async", awaited)
        calls = {_called_name(node) for node in ast.walk(entrypoint) if isinstance(node, ast.Call)}
        self.assertNotIn("analyze_topics_view", calls, "the sync view blocks the loop on Kafka")

    def test_kaspr_answers_with_what_the_view_returns(self) -> None:
        """A first WebView operation's coroutine is awaited, and its dict is the response body."""
        operation = self._analyze()
        payload = {"items": [], "requested": 1, "valid": 0}
        received = {}

        async def analyze_topics_view_async(body, **kwargs):
            received.update(body=body, **kwargs)
            await asyncio.sleep(0)
            return payload

        class Request:
            async def json(self):
                return {"topics": ["orders"]}

        metadata_client = object()
        scope = {"metadata_client": metadata_client}
        exec(compile(operation["python"], "<topics-analyze>", "exec"), scope)
        scope["authorize"] = lambda request, directory_table, permission: received.update(permission=permission)
        scope["analyze_topics_view_async"] = analyze_topics_view_async
        tables = {"settings_table": object(), "directory_table": object()}

        async def first_operation():
            result = scope[operation["entrypoint"]](Request(), **tables)
            return await result if inspect.isawaitable(result) else result

        self.assertIs(asyncio.run(first_operation()), payload)
        self.assertEqual(received["permission"], "topics.view")
        self.assertEqual(received["body"], {"topics": ["orders"]})
        self.assertIs(received["metadata_client"], metadata_client)
        self.assertIs(received["settings_table"], tables["settings_table"])


class ResourceObservationEventLoopTests(unittest.TestCase):
    """The observation agent reads KasprApps from the Kubernetes API on the shared event loop.

    A sync map would hold the loop for as long as the API server took to answer, and an
    unreachable one held it forever. The map is a coroutine so that the read waits on the
    client's own thread, and it must stay one Kaspr can consume.
    """

    def _execute_observation(self):
        try:
            import yaml
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        agent = document("KasprAgent", "resource-observation")
        operations = agent["spec"]["processors"]["operations"]
        self.assertEqual(
            [operation["name"] for operation in operations],
            agent["spec"]["processors"]["pipeline"],
        )
        return operations[1]["map"]

    def test_execute_observation_is_a_coroutine_and_not_an_async_generator(self) -> None:
        operation = self._execute_observation()
        tree = ast.parse(operation["python"])
        entrypoint = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == operation["entrypoint"]
        )

        self.assertIsInstance(entrypoint, ast.AsyncFunctionDef, "a sync map reads KasprApps on the event loop")
        self.assertFalse(
            any(isinstance(node, (ast.Yield, ast.YieldFrom)) for node in ast.walk(entrypoint)),
            "Kaspr cannot iterate an async generator returned by a later pipeline operation",
        )
        calls = {_called_name(node) for node in ast.walk(entrypoint) if isinstance(node, ast.Call)}
        self.assertIn("handle_observation_work_for_output_async", calls)

    def test_kaspr_sends_each_output_execute_observation_returns(self) -> None:
        operation = self._execute_observation()
        outputs = [{"route": "work", "value": {"step": "cleanup_runtime_kafka"}}, {"route": "scheduler"}]
        received = {}

        async def handle_observation_work_for_output_async(value, table, kaspr_app_client, **kwargs):
            received.update(value=value, table=table, kaspr_app_client=kaspr_app_client, **kwargs)
            await asyncio.sleep(0)
            return (output for output in outputs)

        scope = {
            "resource_observation_handler": types.SimpleNamespace(
                handle_observation_work_for_output_async=handle_observation_work_for_output_async,
            ),
            "kaspr_app_client": object(),
            "current_timestamp": lambda: "2026-10-15T00:00:00Z",
        }
        exec(compile(operation["python"], "<resource-observation>", "exec"), scope)
        tables = {"control_plane_table": object(), "settings_table": object()}

        sent = asyncio.run(
            _run_as_later_kaspr_operation(scope[operation["entrypoint"]], {"step": "x"}, **tables)
        )

        self.assertEqual(sent, outputs)
        self.assertIs(received["table"], tables["control_plane_table"])
        self.assertIs(received["kaspr_app_client"], scope["kaspr_app_client"])
        self.assertIs(received["settings_table"], tables["settings_table"])
        self.assertEqual(received["observed_at"], "2026-10-15T00:00:00Z")


#: Views anyone may call, signed in or not, that need no permission at all.
PUBLIC_WEBVIEWS = frozenset({"system-version"})

#: The tail app's views, which have no directory table: each hands the request to one of these, and
#: the view function behind it (`tail_app/views.py`) authorizes through the control plane first -
#: `authorize_tail` with this permission (handbook 19). `test_tail_security.py` proves it of each.
TAIL_APP_LABEL = f"{RELEASE}-tail"
TAIL_AUTHORIZED_HANDLERS = {
    "tail_rows": ("tail_rows_view", "data.query"),
    "tail_head": ("tail_head_view", "data.query"),
    "tail_health": ("tail_health_view", "materializations.view_details"),
    "tail_members": ("tail_members_view", "materializations.view_details"),
}

#: Entry points that authenticate the request themselves, from the identity table they are given.
SELF_AUTHENTICATING_ENTRYPOINTS = (
    "sign_in",
    "sign_out",
    "refresh_session",
    "session_view",
    "change_password",
    "my_sessions_view",
)


class AuthorizationWiringTests(unittest.TestCase):
    """Every view authorizes its caller first: a view that forgot would serve anyone.

    The control plane is reachable directly, not only through the console, so this is the check
    that keeps permissions meaningful. It reads each view's Python rather than trusting a list.
    """

    def _webviews(self):
        yield from documents("KasprWebView")

    def test_the_schema_views_pass_raw_on_to_the_view(self) -> None:
        """`?raw=true` reaches the view, which then adds the Iceberg schema: the tail app seeds a table from
        it. The manifests once dropped the query, so every seed fell back to reading the table from GCS."""
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        views = {bare(view["metadata"]["name"]): view for view in self._webviews()}
        cases = {
            "materializations-schema": ("get_materialization_schema_view", ("mat-1", "orders")),
            "materializations-schema-version": ("get_materialization_schema_version_view", ("mat-1", "orders", "2")),
        }

        class QueryParams:
            def __init__(self, raw):
                self.query_params = {} if raw is None else {"raw": raw}

        class Query:
            def __init__(self, raw):
                self.query = {} if raw is None else {"raw": raw}

        for name, (function, arguments) in cases.items():
            operation = views[name]["spec"]["processors"]["operations"][0]["map"]
            for request_type in (QueryParams, Query):
                for raw in ("true", None):
                    with self.subTest(view=name, request=request_type.__name__, raw=raw):
                        received = {}
                        scope = {}
                        exec(compile(operation["python"], f"<{name}>", "exec"), scope)
                        scope["authorize"] = lambda request, directory_table, permission: "principal"
                        scope[function] = lambda table, *args, **kwargs: received.update(kwargs) or {"ok": True}
                        result = scope[operation["entrypoint"]](request_type(raw), *arguments, control_plane_table=object(), directory_table=object())
                        self.assertEqual({"ok": True}, result)
                        self.assertEqual(raw, received.get("raw"))
                        self.assertEqual("principal", received.get("principal"))

    def test_every_webview_authorizes_its_caller(self) -> None:
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        from midstream.identity.permissions import PERMISSION_KEYS

        checked = 0
        for view in self._webviews():
            name = bare(view["metadata"]["name"])
            first = view["spec"]["processors"]["operations"][0]
            source = (first.get("map") or {}).get("python", "")
            with self.subTest(webview=name):
                if name in PUBLIC_WEBVIEWS:
                    self.assertNotIn("directory_table", source)
                    continue
                if view["metadata"].get("labels", {}).get("kaspr.io/app") == TAIL_APP_LABEL:
                    self._check_tail_view(name, first, source)
                    continue
                checked += 1
                bindings = {table["paramName"]: table["name"] for table in first.get("tables") or []}
                self.assertEqual(
                    bindings.get("directory_table"),
                    f"{RELEASE}-directory",
                    f"{name} must bind the directory table to authorize its caller",
                )
                calls = [node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Call)]
                authorizing = [call for call in calls if _called_name(call) == "authorize"]
                self_authenticating = [call for call in calls if _called_name(call) in SELF_AUTHENTICATING_ENTRYPOINTS]
                self.assertTrue(
                    authorizing or self_authenticating,
                    f"{name} neither calls authorize() nor an entry point that authenticates itself",
                )
                for call in authorizing:
                    self.assertGreaterEqual(len(call.args), 3, f"{name}: authorize(request, directory_table, permission)")
                    self.assertIsInstance(call.args[1], ast.Name)
                    self.assertEqual(call.args[1].id, "directory_table")
                    permission = call.args[2]
                    self.assertIsInstance(permission, ast.Constant, f"{name}: name the permission literally")
                    if permission.value is not None:
                        self.assertIn(permission.value, PERMISSION_KEYS, f"{name}: unknown permission {permission.value!r}")
                for call in self_authenticating:
                    passed = {arg.id for arg in call.args if isinstance(arg, ast.Name)}
                    self.assertIn("directory_table", passed, f"{name} must pass directory_table to {_called_name(call)}")
        self.assertGreaterEqual(checked, 43, "the scan found fewer views than exist")

    def _check_tail_view(self, name: str, operation: dict, source: str) -> None:
        """A tail app view binds no table, and hands the request to a handler that authorizes it."""
        from midstream.identity.permissions import PERMISSION_KEYS
        from midstream.tail_app import views as tail_views

        self.assertFalse(operation.get("tables"), f"{name}: the tail app has no tables to bind")
        called = {_called_name(node) for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Call)}
        handlers = called & set(TAIL_AUTHORIZED_HANDLERS)
        self.assertEqual(1, len(handlers), f"{name} must hand the request to one of {sorted(TAIL_AUTHORIZED_HANDLERS)}")
        (handler,) = handlers
        view_name, permission = TAIL_AUTHORIZED_HANDLERS[handler]
        self.assertIn(permission, PERMISSION_KEYS)
        module = ast.parse(Path(tail_views.__file__).read_text())
        view = next(node for node in module.body if isinstance(node, ast.AsyncFunctionDef) and node.name == view_name)
        calls = [node for node in ast.walk(view) if isinstance(node, ast.Call) and _called_name(node) == "authorize_tail"]
        self.assertTrue(calls, f"{view_name} must authorize through authorize_tail")
        for call in calls:
            self.assertIsInstance(call.args[1], ast.Constant, f"{view_name}: name the permission literally")
            self.assertEqual(permission, call.args[1].value, f"{view_name} authorizes the wrong permission")

    def test_the_tail_apps_parquet_views_answer_their_errors_as_json(self) -> None:
        """Rows and the head peek send Parquet; their refusals are the control plane's JSON error body."""
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        from midstream.tail_app.handlers import error_body

        views = {bare(view["metadata"]["name"]): view for view in self._webviews()}
        for name in ("tail-rows", "tail-head"):
            response = views[name]["spec"]["response"]
            self.assertEqual(("application/octet-stream", "application/json"), (response["contentType"], response.get("errorContentType")), name)
            self.assertIn("error_body(error)", response["bodySelector"]["onError"]["python"], name)
        for name in ("tail-health", "tail-members"):
            self.assertEqual("application/json", views[name]["spec"]["response"]["contentType"], name)
        refusal = {"error": {"code": "conflict", "message": "Moving.", "details": {"reason": "tail_rebalancing"}}}
        self.assertEqual(refusal, error_body({"cause_response": refusal}))
        self.assertEqual("internal_error", error_body({"cause": "boom"})["error"]["code"])

    def test_the_tail_app_views_are_all_known(self) -> None:
        """Each of the tail app's views is in `TAIL_AUTHORIZED_HANDLERS`, and no control-plane view uses one."""
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        tail_views = {
            bare(view["metadata"]["name"])
            for view in self._webviews()
            if view["metadata"].get("labels", {}).get("kaspr.io/app") == TAIL_APP_LABEL
        }
        self.assertEqual({"tail-rows", "tail-head", "tail-health", "tail-members"}, tail_views)
        for view in self._webviews():
            if bare(view["metadata"]["name"]) in tail_views:
                continue
            source = view["spec"]["processors"]["operations"][0].get("map", {}).get("python", "")
            self.assertNotIn("tail_app", source, f"{view['metadata']['name']} is the control plane's")

    def test_identity_requests_are_keyed_to_one_partition(self) -> None:
        """Every identity request shares one key, so the agent applies them strictly in order."""
        try:
            import yaml
        except ImportError:
            self.skipTest("pyyaml is required to read manifests")
        senders = 0
        for rendered in documents():
            if rendered.get("kind") not in ("KasprWebView", "KasprTask"):
                continue
            for operation in rendered["spec"]["processors"].get("operations", []):
                send = operation.get("topicSend") or {}
                if send.get("name") != f"{RELEASE}-identity-requests":
                    continue
                senders += 1
                with self.subTest(resource=rendered["metadata"]["name"]):
                    self.assertIn("identity_routing_key()", send["keySelector"]["python"])
        self.assertGreaterEqual(senders, 18)


class DirectoryWiringTests(unittest.TestCase):
    """Only the identity agent writes the directory: no other agent even binds it.

    Records live in the shard of the event being handled. Every materialization's work is keyed by
    the materialization, the directory's by `identity`; an agent of one writing the other's records
    would strand them (handbook 06 §6.4).
    """

    def test_no_agent_but_the_identity_agent_binds_the_directory(self) -> None:
        import yaml

        bound = {}
        for agent in documents("KasprAgent"):
            names = {table["name"] for operation in agent["spec"]["processors"]["operations"] for table in operation.get("tables") or []}
            bound[bare(agent["metadata"]["name"])] = names
        self.assertEqual(["identity-intake"], sorted(name for name, tables in bound.items() if f"{RELEASE}-directory" in tables))

    def test_the_identity_agent_answers_claims_on_the_work_topic_keyed_by_materialization(self) -> None:
        import yaml

        agent = document("KasprAgent", "identity-intake")
        (output,) = agent["spec"]["output"]["topics"]
        self.assertEqual(f"{RELEASE}-materialization-work", output["name"])
        self.assertIn('value.get("route") == "work"', output["predicate"]["python"])
        self.assertIn('value["key"]', output["keySelector"]["python"])

    def test_request_intake_sends_claims_on_the_identity_key(self) -> None:
        import yaml

        agent = document("KasprAgent", "request-intake")
        topics = {topic["name"]: topic for topic in agent["spec"]["output"]["topics"]}
        self.assertEqual({f"{RELEASE}-materialization-work", f"{RELEASE}-identity-requests"}, set(topics))
        self.assertIn("identity_routing_key()", topics[f"{RELEASE}-identity-requests"]["keySelector"]["python"])
        self.assertIn('value.get("route") == "identity"', topics[f"{RELEASE}-identity-requests"]["predicate"]["python"])


def _called_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


class ValuesWiringTests(unittest.TestCase):
    """Every installation shape must route scheduled work to the topic its agents actually consume.

    A `KasprTask` publishes its trigger regardless of whether anything is listening, so a task
    sending to another name than the one its agent consumes looks deployed and healthy while every
    tick vanishes. `compact-object-manifests` shipped with exactly that bug: the task went to
    `materialization-work` while the maintenance agent consumed `develop-materialization-work`.

    This walks the chart as rendered with each values file under `charts/midstream/ci/`, so it
    catches a template that names a topic or table differently from the one that declares it.
    """

    def test_task_outputs_match_agent_inputs(self) -> None:
        for values in VALUES_FILES:
            with self.subTest(values=values.name):
                rendered = render(values)

                consumed = {
                    document["spec"]["input"]["topic"]["name"]
                    for document in rendered
                    if document.get("kind") == "KasprAgent"
                    and document.get("spec", {}).get("input", {}).get("topic", {}).get("name")
                }

                for document in rendered:
                    if document.get("kind") != "KasprTask":
                        continue
                    for operation in document["spec"]["processors"].get("operations", []):
                        target = operation.get("topicSend", {}).get("name")
                        if target is None:
                            continue
                        self.assertIn(
                            target,
                            consumed,
                            f"{values.name}: task {document['metadata']['name']} publishes to "
                            f"{target!r}, which no agent in this installation consumes — its schedule "
                            f"would fire into nothing. Consumed topics: {sorted(consumed)}",
                        )

    def test_maintenance_hands_its_work_to_the_topic_it_consumes(self) -> None:
        """Its retention purges and lag measurements go to the work topic, for the workflow agent
        and for itself; on a name no agent consumes, every one would vanish."""
        for values in VALUES_FILES:
            with self.subTest(values=values.name):
                rendered = render(values)
                maintenance = next(
                    document
                    for document in rendered
                    if document.get("kind") == "KasprAgent" and document["metadata"]["name"].endswith("maintenance")
                )
                consumed = maintenance["spec"]["input"]["topic"]["name"]
                worker = next(
                    document
                    for document in rendered
                    if document.get("kind") == "KasprAgent" and document["metadata"]["name"].endswith("maintenance-worker")
                )
                tail_worker = next(
                    (
                        document
                        for document in rendered
                        if document.get("kind") == "KasprAgent" and document["metadata"]["name"].endswith("tail-worker")
                    ),
                    None,
                )
                tail_topic = maintenance["spec"]["output"]["topics"][2]["name"]
                if tail_worker is not None:
                    tail_topic = tail_worker["spec"]["input"]["topic"]["name"]
                else:
                    self.assertTrue(tail_topic.endswith("-tail-requests"), f"{values.name}: {tail_topic}")
                self.assertEqual(
                    [topic["name"] for topic in maintenance["spec"]["output"]["topics"]],
                    [consumed, worker["spec"]["input"]["topic"]["name"], tail_topic],
                    f"{values.name}: the maintenance agent consumes {consumed!r} but sends its work elsewhere, "
                    "or ticks tables on a topic the maintenance or tail app does not read",
                )

    def test_the_maintenance_app_owns_its_agent_and_task(self) -> None:
        """A blanket label once claimed every agent for the control plane: the
        maintenance app's agent would then run in the control plane, and the app would have none."""
        for values in VALUES_FILES:
            with self.subTest(values=values.name):
                rendered = render(values)
                apps = {document["metadata"]["name"] for document in rendered if document.get("kind") == "KasprApp"}
                maintenance_app = next(name for name in apps if name.endswith("-maintenance"))
                for document in rendered:
                    if document.get("kind") not in ("KasprAgent", "KasprTask", "KasprWebView", "KasprTable"):
                        continue
                    name = document["metadata"]["name"]
                    owner = document["metadata"].get("labels", {}).get("kaspr.io/app")
                    self.assertIn(owner, apps, f"{values.name}: {name} belongs to no app")
                    ours = name.endswith(("maintenance-worker", "maintenance-step"))
                    self.assertEqual(ours, owner == maintenance_app, f"{values.name}: {name} is {owner}'s")

    def test_the_tail_app_owns_its_agent_task_and_views_and_reaches_this_environment(self) -> None:
        """The tail app's resources are its own, it reads the topics the control plane ticks and the
        committers report on, and it calls this environment's control plane with its credential."""
        for values in VALUES_FILES:
            with self.subTest(values=values.name):
                rendered = render(values)
                apps = {document["metadata"]["name"]: document for document in rendered if document.get("kind") == "KasprApp"}
                tail_app = next((name for name in apps if name.endswith("-tail")), None)
                control_plane = next(name for name in apps if name.endswith("-control-plane"))
                ours = ("tail-worker", "tail-step", "tail-rows", "tail-head", "tail-health", "tail-members")
                if tail_app is None:
                    self.assertFalse([d["metadata"]["name"] for d in rendered if d["metadata"]["name"].endswith(ours)], f"{values.name}: tail resources without the tail app")
                    continue
                for document in rendered:
                    if document.get("kind") not in ("KasprAgent", "KasprTask", "KasprWebView", "KasprTable"):
                        continue
                    name = document["metadata"]["name"]
                    owner = document["metadata"].get("labels", {}).get("kaspr.io/app")
                    self.assertEqual(name.endswith(ours), owner == tail_app, f"{values.name}: {name} is {owner}'s")
                agents = {document["metadata"]["name"]: document for document in rendered if document.get("kind") == "KasprAgent"}
                tail_worker = next(document for name, document in agents.items() if name.endswith("tail-worker"))
                table_events = next(document for name, document in agents.items() if name.endswith("table-events"))
                env = {item["name"]: item for item in apps[tail_app]["spec"]["template"]["kasprContainer"]["env"]}
                self.assertEqual(tail_worker["spec"]["input"]["topic"]["name"], env["TAIL_REQUESTS_TOPIC"]["value"])
                self.assertEqual(64, tail_worker["spec"]["input"]["topic"]["partitions"])
                self.assertEqual(table_events["spec"]["input"]["topic"]["name"], env["TAIL_TABLE_EVENTS_TOPIC"]["value"])
                self.assertEqual(tail_app, env["TAIL_APP_NAME"]["value"])
                self.assertEqual(f"http://{control_plane}-app-api:6065", env["TAIL_CONTROL_PLANE_URL"]["value"])
                cp_env = {item["name"]: item for item in apps[control_plane]["spec"]["template"]["kasprContainer"]["env"]}
                self.assertEqual(
                    cp_env["AUTH_SERVICE_TOKEN"]["valueFrom"]["secretKeyRef"],
                    env["TAIL_SERVICE_TOKEN"]["valueFrom"]["secretKeyRef"],
                    f"{values.name}: the tail app reads another service credential than the control plane",
                )
                self.assertEqual(
                    apps[control_plane]["spec"]["pythonPackages"]["packages"],
                    apps[tail_app]["spec"]["pythonPackages"]["packages"],
                    f"{values.name}: the tail app installs another package than the control plane",
                )

    def test_webviews_use_the_installations_tables_and_request_topics(self) -> None:
        """A WebView on the wrong table or topic name reads an empty table and sends requests nobody handles.

        Every table and request topic derives from the release, so a template that spells one of them
        differently renders fine and accepts requests — every one of which then vanishes.
        """
        for values in VALUES_FILES:
            with self.subTest(values=values.name):
                rendered = render(values)

                tables = {
                    document["metadata"]["name"]
                    for document in rendered
                    if document.get("kind") == "KasprTable"
                }
                consumed = {
                    document["spec"]["input"]["topic"]["name"]
                    for document in rendered
                    if document.get("kind") == "KasprAgent"
                    and document.get("spec", {}).get("input", {}).get("topic", {}).get("name")
                }
                # The runtime control topic is consumed by every runtime's settings agent, which the
                # control plane renders itself: what it consumes is what the app's environment names.
                consumed |= _runtime_consumed_topics(rendered)
                for document in rendered:
                    if document.get("kind") not in ("KasprWebView", "KasprAgent"):
                        continue
                    name = document["metadata"]["name"]
                    kind = "webview" if document["kind"] == "KasprWebView" else "agent"
                    for operation in document["spec"]["processors"].get("operations", []):
                        for table in operation.get("tables", []):
                            self.assertIn(
                                table.get("name"),
                                tables,
                                f"{values.name}: {kind} {name} reads table {table.get('name')!r}, "
                                f"which this installation does not define. Tables: {sorted(tables)}",
                            )
                        if kind == "agent":
                            continue
                        target = operation.get("topicSend", {}).get("name")
                        if target is not None:
                            self.assertIn(
                                target,
                                consumed,
                                f"{values.name}: webview {name} sends to {target!r}, which no agent "
                                f"in this installation consumes. Consumed topics: {sorted(consumed)}",
                            )


def _runtime_consumed_topics(documents: list[dict]) -> set[str]:
    topics: set[str] = set()
    for document in documents:
        if document.get("kind") != "KasprApp":
            continue
        container = document.get("spec", {}).get("template", {}).get("kasprContainer", {})
        for entry in container.get("env") or []:
            if entry.get("name") == "CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC" and entry.get("value"):
                topics.add(entry["value"])
    return topics


class MaintenanceTriggerTests(unittest.TestCase):
    """Every scheduled step must survive the work-event model it is published through.

    `compact_object_manifests` shipped registered in the handler, the agent filter, and its task
    manifest, but missing from `WORK_STEPS` — so `MaterializationWorkEvent` rejected it. The cron
    task raised on every tick and took the whole control-plane app down with it. Nothing caught
    that because the service was tested directly, never through the event the task actually builds.
    """

    def test_every_maintenance_step_is_an_accepted_work_step(self) -> None:
        unregistered = MAINTENANCE_STEPS - set(WORK_STEPS)
        self.assertEqual(
            unregistered,
            set(),
            f"maintenance steps missing from WORK_STEPS: {sorted(unregistered)}",
        )

    def test_every_trigger_builder_produces_a_valid_work_event(self) -> None:
        """Runs the exact call the cron task makes, so a rejected step fails here, not in a pod."""
        from midstream.handlers import maintenance as maintenance_handler

        for step, builder_name in sorted(MAINTENANCE_STEP_TRIGGERS.items()):
            with self.subTest(step=step):
                builder = getattr(maintenance_handler, builder_name, None)
                self.assertIsNotNone(builder, f"{builder_name} is not defined on the handler")

                event = builder(requested_at="2026-01-15T10:00:00Z")

                self.assertEqual(event["step"], step)
                self.assertEqual(event["materialization_id"], maintenance_handler.SYSTEM_MATERIALIZATION_ID)
                # Round-trip through the model the workflow agent will decode it with.
                self.assertEqual(MaterializationWorkEvent.from_dict(event).step, step)


def _millicores(quantity: object) -> int:
    """A CPU quantity as Kubernetes reads it (`2`, `0.5`, `"1500m"`), in millicores."""
    text = str(quantity)
    return int(text[:-1]) if text.endswith("m") else round(float(text) * 1000)


class VerticalPodAutoscalingTests(unittest.TestCase):
    """Every Kaspr app of the deployment has its CPU request set from its use (handbook 02 §2.10c).

    That is the control plane, the maintenance and tail apps, and the runtime spec every runtime is
    rendered from, with every values file. Each app must be resizable in place: the CPU only, as
    kaspr-operator restarts the container on a memory resize; the requests only, so the limits stay
    the ceilings; and a ceiling under the CPU limit, since a request equal to every limit makes a pod
    Guaranteed, a QoS class no resize may change.
    """

    def test_every_app_has_its_cpu_request_set_from_use(self) -> None:
        import yaml

        for values in VALUES_FILES:
            specs = {}
            for document in render(values):
                if document.get("kind") != "KasprApp":
                    continue
                name = document["metadata"]["name"]
                specs[name] = document["spec"]
                for item in document["spec"]["template"].get("kasprContainer", {}).get("env", []):
                    if item["name"] == "CONTROL_PLANE_RUNTIME_APP_SPEC":
                        specs[f"{name}'s runtime spec"] = yaml.safe_load(item["value"])
            self.assertTrue(any(name.endswith("runtime spec") for name in specs), f"{values.name}: no runtime spec")
            for name, spec in specs.items():
                with self.subTest(values=values.name, app=name):
                    autoscaler = spec.get("verticalPodAutoscaler") or {}
                    self.assertIs(autoscaler.get("enabled"), True)
                    self.assertEqual(autoscaler.get("updateMode"), "InPlaceOrRecreate")
                    self.assertEqual(autoscaler.get("controlledResources"), ["cpu"])
                    self.assertEqual(autoscaler.get("controlledValues"), "RequestsOnly")
                    lowest = _millicores(autoscaler["minAllowed"]["cpu"])
                    highest = _millicores(autoscaler["maxAllowed"]["cpu"])
                    request = _millicores(spec["resources"]["requests"]["cpu"])
                    limit = _millicores(spec["resources"]["limits"]["cpu"])
                    self.assertLess(lowest, highest)
                    self.assertLessEqual(request, limit)
                    self.assertLess(
                        highest,
                        limit,
                        f"{values.name}: {name} may be given a CPU request of {highest}m against a limit of "
                        f"{limit}m; at the limit, beside a memory request at its own, the pod turns "
                        "Guaranteed and can no longer be resized in place",
                    )
