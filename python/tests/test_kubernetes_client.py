from __future__ import annotations

import asyncio
import threading
import unittest

from materializer_control_plane.kubernetes.kaspr_apps import (
    KubernetesKasprAppClient,
    _observation_from_resource,
    _run_config_loader,
)
from materializer_control_plane.utils.background_loop import AdapterTimeoutError


def finish_within(seconds: float, function):
    """Run `function` on a thread and return its result, failing if it is still running after `seconds`."""
    outcome: dict = {}

    def target() -> None:
        try:
            outcome["result"] = function()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the test's thread
            outcome["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    if thread.is_alive():
        raise AssertionError(f"still waiting after {seconds}s: the wait has no deadline")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


class KubernetesClientObservationTests(unittest.TestCase):
    def test_observation_from_resource_maps_kaspr_status_fields(self) -> None:
        observation = _observation_from_resource(
            {
                "metadata": {"generation": 4},
                "status": {
                    "observedGeneration": 4,
                    "rolloutInProgress": False,
                    "availableMembers": 1,
                    "rebalanceRequired": False,
                    "rebalancingMembers": 0,
                    "conditions": [
                        {
                            "type": "Ready",
                            "status": "True",
                            "reason": "Ready",
                            "message": "App is ready",
                            "lastTransitionTime": "2026-07-12T00:00:00Z",
                        }
                    ],
                    "linkedResources": {
                        "agents": [
                            {"name": "writer", "state": "running"},
                            {"name": "schema-proposer", "state": "paused"},
                        ]
                    },
                },
            },
            namespace="midstream",
            name="kaspr-materializer-orders",
        )

        self.assertTrue(observation.resource_exists)
        self.assertEqual(observation.metadata_generation, 4)
        self.assertEqual(observation.observed_generation, 4)
        self.assertEqual(observation.ready_condition.status, "True")
        self.assertEqual(observation.managed_agent_states["writer"], "running")

    def test_run_config_loader_accepts_sync_loader(self) -> None:
        called: list[str] = []

        def loader() -> None:
            called.append("sync")
            return None

        asyncio.run(_run_config_loader(loader))

        self.assertEqual(called, ["sync"])

    def test_run_config_loader_accepts_async_loader(self) -> None:
        called: list[str] = []

        async def loader() -> None:
            called.append("async")

        asyncio.run(_run_config_loader(loader))

        self.assertEqual(called, ["async"])



class NotFound(Exception):
    status = 404


class FakeCustomObjectsApi:
    """The CustomObjectsApi calls the client makes, answering after `delay` seconds."""

    def __init__(self, *, delay: float = 0.0, hang: bool = False) -> None:
        self.delay = delay
        self.hang = hang
        self.calls: list[tuple[str, str]] = []
        self.threads: set[str] = set()
        self.cancelled = threading.Event()

    async def patch_namespaced_custom_object(self, *, plural: str, name: str, **kwargs) -> dict:
        await self._wait("patch", f"{plural}/{name}")
        raise NotFound()

    async def create_namespaced_custom_object(self, *, plural: str, body: dict, **kwargs) -> dict:
        await self._wait("create", f"{plural}/{body['metadata']['name']}")
        return body

    async def delete_namespaced_custom_object(self, *, plural: str, name: str, **kwargs) -> dict:
        await self._wait("delete", f"{plural}/{name}")
        raise NotFound()

    async def get_namespaced_custom_object(self, *, plural: str, name: str, **kwargs) -> dict:
        await self._wait("get", f"{plural}/{name}")
        return {
            "metadata": {"generation": 2},
            "status": {"observedGeneration": 2, "linkedResources": {"agents": [{"name": "writer", "state": "running"}]}},
        }

    async def _wait(self, verb: str, target: str) -> None:
        self.calls.append((verb, target))
        self.threads.add(threading.current_thread().name)
        if not self.hang:
            await asyncio.sleep(self.delay)
            return
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


APP = {"kind": "KasprApp", "metadata": {"namespace": "midstream", "name": "kaspr-mat-1"}}


class KubernetesClientDeadlineTests(unittest.TestCase):
    """Each call runs on the client's own loop, under `call_timeout_seconds`."""

    def client(self, api: FakeCustomObjectsApi, **kwargs) -> KubernetesKasprAppClient:
        client = KubernetesKasprAppClient(**kwargs)
        self.addCleanup(client._runner.close)
        client._api = api
        return client

    def test_awaited_calls_answer_as_the_sync_ones_do(self) -> None:
        api = FakeCustomObjectsApi()
        client = self.client(api)

        async def awaited():
            await client.apply_async(APP)
            await client.delete_async(namespace="midstream", name="kaspr-mat-1-writer", kind="KasprAgent")
            return await client.observe_async(namespace="midstream", name="kaspr-mat-1")

        observation = asyncio.run(awaited())

        self.assertEqual(observation, client.observe(namespace="midstream", name="kaspr-mat-1"))
        self.assertEqual(observation.managed_agent_states, {"writer": "running"})
        self.assertEqual(
            api.calls[:4],
            [
                ("patch", "kasprapps/kaspr-mat-1"),
                ("create", "kasprapps/kaspr-mat-1"),
                ("delete", "kaspragents/kaspr-mat-1-writer"),
                ("get", "kasprapps/kaspr-mat-1"),
            ],
        )
        self.assertEqual(api.threads, {"kaspr-app-client"})
        self.assertEqual(client.call_timeout_seconds, 90.0)

    def test_an_awaited_call_leaves_the_callers_loop_running(self) -> None:
        client = self.client(FakeCustomObjectsApi(delay=0.2))
        turns = 0

        async def scenario():
            nonlocal turns
            done = False

            async def tick() -> None:
                nonlocal turns
                while not done:
                    await asyncio.sleep(0.005)
                    turns += 1

            ticking = asyncio.ensure_future(tick())
            observation = await client.observe_async(namespace="midstream", name="kaspr-mat-1")
            done = True
            await ticking
            return observation

        self.assertTrue(asyncio.run(scenario()).resource_exists)
        # 0.2 s at one turn per 5 ms is 40 turns; a loop held for the call would turn at most once.
        self.assertGreaterEqual(turns, 10)

    def test_every_call_is_cancelled_at_its_deadline(self) -> None:
        calls = {
            "Kubernetes apply of KasprApp midstream/kaspr-mat-1": ("apply", (APP,), {}),
            "Kubernetes delete of KasprTable midstream/kaspr-mat-1-schema-state": (
                "delete",
                (),
                {"namespace": "midstream", "name": "kaspr-mat-1-schema-state", "kind": "KasprTable"},
            ),
            "Kubernetes read of KasprApp midstream/kaspr-mat-1": (
                "observe",
                (),
                {"namespace": "midstream", "name": "kaspr-mat-1"},
            ),
        }
        for description, (method, args, kwargs) in calls.items():
            for awaited in (False, True):
                with self.subTest(call=method, awaited=awaited):
                    api = FakeCustomObjectsApi(hang=True)
                    client = self.client(api, call_timeout_seconds=0.05)

                    with self.assertRaises(AdapterTimeoutError) as raised:
                        if awaited:
                            finish_within(5, lambda: asyncio.run(getattr(client, f"{method}_async")(*args, **kwargs)))
                        else:
                            finish_within(5, lambda: getattr(client, method)(*args, **kwargs))

                    self.assertEqual(str(raised.exception), f"{description} timed out after 0.05s")
                    self.assertTrue(api.cancelled.wait(2), "the call was left running on the client's loop")

    def test_the_deadline_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            KubernetesKasprAppClient(call_timeout_seconds=0)


if __name__ == "__main__":
    unittest.main()

def pod(name: str, *, restarts: int = 0, terminated=None, waiting: str | None = None, container: str = "kaspr"):
    from types import SimpleNamespace

    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(
            container_statuses=[
                SimpleNamespace(
                    name=container,
                    restart_count=restarts,
                    last_state=SimpleNamespace(terminated=terminated),
                    state=SimpleNamespace(waiting=None if waiting is None else SimpleNamespace(reason=waiting)),
                )
            ]
        ),
    )


class Forbidden(Exception):
    status = 403


class FakeCoreV1Api:
    """The CoreV1Api call the client makes: the pods a label selects."""

    def __init__(self, pods: list | None = None, *, refuse: bool = False) -> None:
        self.pods = pods or []
        self.refuse = refuse
        self.calls: list[tuple[str, str]] = []

    async def list_namespaced_pod(self, namespace: str, *, label_selector: str, **kwargs):
        from types import SimpleNamespace

        self.calls.append((namespace, label_selector))
        if self.refuse:
            raise Forbidden("pods is forbidden")
        return SimpleNamespace(items=self.pods)


class KubernetesClientMemberTests(unittest.TestCase):
    """Each observation reads the app's member pods: how often each restarted, and why."""

    def client(self, core: FakeCoreV1Api | None) -> KubernetesKasprAppClient:
        client = KubernetesKasprAppClient()
        self.addCleanup(client._runner.close)
        client._api = FakeCustomObjectsApi()
        client._core_api = core
        return client

    def test_member_pods_come_with_their_restarts_and_last_exit(self) -> None:
        from datetime import UTC, datetime
        from types import SimpleNamespace

        killed = SimpleNamespace(reason="OOMKilled", exit_code=137, finished_at=datetime(2026, 10, 25, 2, 25, 10, 123000, tzinfo=UTC))
        core = FakeCoreV1Api(
            [
                pod("kaspr-mat-1-app-1", restarts=48, terminated=killed, waiting="CrashLoopBackOff"),
                pod("kaspr-mat-1-app-0", restarts=58, terminated=killed),
                pod("kaspr-mat-1-app-2"),
            ]
        )
        observation = self.client(core).observe(namespace="midstream", name="kaspr-mat-1")

        self.assertEqual([("midstream", "kaspr.io/app=kaspr-mat-1")], core.calls)
        self.assertEqual(["kaspr-mat-1-app-0", "kaspr-mat-1-app-1", "kaspr-mat-1-app-2"], [member.name for member in observation.members])
        first, looping, quiet = observation.members
        self.assertEqual((58, "OOMKilled", 137, "2026-10-25T02:25:10Z", None), (first.restarts, first.last_exit_reason, first.last_exit_code, first.last_exited_at, first.waiting_reason))
        self.assertEqual("CrashLoopBackOff", looping.waiting_reason)
        self.assertEqual((0, None, None), (quiet.restarts, quiet.last_exit_reason, quiet.last_exited_at))

    def test_a_pod_without_the_kaspr_container_is_a_member_with_nothing_to_say(self) -> None:
        observation = self.client(FakeCoreV1Api([pod("kaspr-mat-1-app-0", restarts=4, container="istio-proxy")])).observe(namespace="midstream", name="kaspr-mat-1")
        self.assertEqual(0, observation.members[0].restarts)

    def test_pods_it_may_not_read_leave_the_observation_without_members(self) -> None:
        client = self.client(FakeCoreV1Api(refuse=True))
        with self.assertLogs("materializer_control_plane.kubernetes.kaspr_apps", level="WARNING") as logs:
            first = client.observe(namespace="midstream", name="kaspr-mat-1")
            second = client.observe(namespace="midstream", name="kaspr-mat-1")
        self.assertEqual((None, None), (first.members, second.members))
        self.assertEqual({"writer": "running"}, first.managed_agent_states)
        self.assertEqual(1, len(logs.records), "reported once")

    def test_without_a_pod_reader_nothing_is_read(self) -> None:
        self.assertIsNone(self.client(None).observe(namespace="midstream", name="kaspr-mat-1").members)
