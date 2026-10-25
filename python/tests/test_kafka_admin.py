from __future__ import annotations

import asyncio
import threading
import time
import types
import unittest
from unittest.mock import patch

from midstream.kafka.admin import (
    AIOKafkaAdminClient,
    AsyncRunner,
    ConsumerGroupNotEmptyError,
    _build_admin_client,
)
from midstream.utils.background_loop import AdapterTimeoutError


class Hung:
    """A call that never answers, and notices when it is cancelled."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.cancelled = threading.Event()

    async def wait(self, *args, **kwargs):
        self.started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


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


async def count_turns_while(awaitable) -> tuple[object, int]:
    """Await `awaitable` and count how often this event loop turned meanwhile."""
    turns = 0
    done = False

    async def tick() -> None:
        nonlocal turns
        while not done:
            await asyncio.sleep(0.005)
            turns += 1

    ticking = asyncio.ensure_future(tick())
    try:
        result = await awaitable
    finally:
        done = True
        await ticking
    return result, turns


class KafkaAdminImportTests(unittest.TestCase):
    def test_build_admin_client_imports_from_aiokafka_admin_module(self) -> None:
        admin_ctor_calls: list[dict] = []

        class FakeAdminClient:
            def __init__(self, **kwargs) -> None:
                admin_ctor_calls.append(kwargs)

        def fake_import_module(name: str):
            if name == "aiokafka.admin":
                return types.SimpleNamespace(AIOKafkaAdminClient=FakeAdminClient)
            raise AssertionError(f"unexpected import {name}")

        with patch("midstream.kafka.admin.import_module", side_effect=fake_import_module):
            client = _build_admin_client(bootstrap_servers="kafka:9092")

        self.assertIsInstance(client, FakeAdminClient)
        self.assertEqual(admin_ctor_calls, [{"bootstrap_servers": "kafka:9092"}])

    def _client_over(self, admin) -> AIOKafkaAdminClient:
        client = AIOKafkaAdminClient(bootstrap_servers="kafka:9092", client_factory=lambda **kwargs: admin)
        self.addCleanup(client._runner.close)
        return client

    def test_consumer_group_state_reads_the_described_state(self) -> None:
        class FakeAdminClient:
            def __init__(self, groups) -> None:
                self.groups = groups

            async def start(self) -> None:
                return None

            async def describe_consumer_groups(self, group_ids):
                return [types.SimpleNamespace(groups=self.groups)]

        for groups, expected in (
            ([(0, "kaspr-app", "Stable", "consumer", "range", [])], "Stable"),
            ([(0, "kaspr-app", "Empty", "consumer", "", [])], "Empty"),
            ([(0, "kaspr-app", "", "", "", [])], "Dead"),
            ([], "Dead"),
        ):
            with self.subTest(expected=expected, groups=groups):
                self.assertEqual(expected, self._client_over(FakeAdminClient(groups)).consumer_group_state("kaspr-app"))

    def test_describe_consumer_group_reports_each_members_metadata_and_assignment(self) -> None:
        member_v2 = ("app-0-a22d4404-9a28-4818-893a-9e55fbe3a01b", "faust-1.17.23", "/10.0.0.1", b"metadata", b"assignment")
        # DescribeGroups v4 and later put the static member's instance id second.
        member_v4 = ("app-1-b22d4404-9a28-4818-893a-9e55fbe3a01b", "app-1", "faust-1.17.23", "/10.0.0.2", b"md", b"as")

        class FakeAdminClient:
            async def start(self) -> None:
                return None

            async def describe_consumer_groups(self, group_ids):
                return [types.SimpleNamespace(groups=[(0, "other", "Stable", "consumer", "faust", []), (0, "tail-app", "Stable", "consumer", "faust", [member_v2, member_v4])])]

        described = self._client_over(FakeAdminClient()).describe_consumer_group("tail-app")

        self.assertEqual(("tail-app", "Stable", "consumer", "faust"), (described.group_id, described.state, described.protocol_type, described.protocol))
        self.assertEqual(
            [("app-0-a22d4404-9a28-4818-893a-9e55fbe3a01b", "faust-1.17.23", "/10.0.0.1", b"metadata", b"assignment"), ("app-1-b22d4404-9a28-4818-893a-9e55fbe3a01b", "faust-1.17.23", "/10.0.0.2", b"md", b"as")],
            [(member.member_id, member.client_id, member.client_host, member.metadata, member.assignment) for member in described.members],
        )

    def test_describe_consumer_group_of_no_group_is_dead_and_refusals_raise(self) -> None:
        class FakeAdminClient:
            def __init__(self, groups) -> None:
                self.groups = groups

            async def start(self) -> None:
                return None

            async def describe_consumer_groups(self, group_ids):
                return [types.SimpleNamespace(groups=self.groups)]

        self.assertEqual(("Dead", ()), (lambda d: (d.state, d.members))(self._client_over(FakeAdminClient([])).describe_consumer_group("tail-app")))
        with patch("midstream.kafka.admin._error_for_code", return_value=RuntimeError):
            with self.assertRaises(RuntimeError) as caught:
                self._client_over(FakeAdminClient([(15, "tail-app", "", "", "", [])])).describe_consumer_group("tail-app")
        self.assertIn("tail-app", str(caught.exception))

    def test_commit_group_offsets_commits_for_no_member_and_raises_kafkas_refusal(self) -> None:
        sent = []

        class FakeAdminClient:
            def __init__(self, error_code: int) -> None:
                self.error_code = error_code

            async def start(self) -> None:
                return None

            async def find_coordinator(self, group_id: str) -> int:
                return 3

            async def _send_request(self, request, node_id: int):
                sent.append((request, node_id))
                return types.SimpleNamespace(topics=[("refunds", [(0, self.error_code), (1, self.error_code)])])

        with patch(
            "midstream.kafka.admin._offset_commit_request_type",
            return_value=lambda *fields: fields,
        ):
            self._client_over(FakeAdminClient(0)).commit_group_offsets("kaspr-app", {"refunds": {1: 0, 0: 5}})
            self.assertEqual(
                [(("kaspr-app", -1, "", -1, [("refunds", [(0, 5, ""), (1, 0, "")])]), 3)],
                sent,
            )
            # A group with members refuses the commit (UNKNOWN_MEMBER_ID): it never moves a running consumer.
            with self.assertRaises(Exception):
                self._client_over(FakeAdminClient(25)).commit_group_offsets("kaspr-app", {"refunds": {0: 5}})

    def test_delete_consumer_group_raises_consumer_group_not_empty_error(self) -> None:
        class FakeAdminClient:
            async def start(self) -> None:
                return None

            async def find_coordinator(self, group_id: str) -> int:
                return 1

            async def _send_request(self, request, coordinator_id: int):
                return types.SimpleNamespace(results=[("group-1", 68)])

        client = AIOKafkaAdminClient(
            bootstrap_servers="kafka:9092",
            client_factory=lambda **kwargs: FakeAdminClient(),
        )
        try:
            with patch(
                "midstream.kafka.admin._delete_groups_request_type",
                return_value=lambda groups: groups,
            ):
                with self.assertRaises(ConsumerGroupNotEmptyError):
                    client._runner.run(client._delete_consumer_group("group-1"))
        finally:
            client._runner.close()



class AsyncRunnerDeadlineTests(unittest.TestCase):
    """The loop every Kafka client here waits through. A deadline, when given, cancels an overrun.

    `kafka/metadata.py` uses `run()` and `submit()` too, so both keep their behaviour by default.
    """

    def setUp(self) -> None:
        self.runner = AsyncRunner()
        self.addCleanup(self.runner.close)

    def test_run_without_a_timeout_waits_for_the_answer(self) -> None:
        async def answer() -> str:
            await asyncio.sleep(0.05)
            return "answer"

        self.assertEqual(self.runner.run(answer()), "answer")
        self.assertEqual(self.runner.submit(answer()).result(timeout=5), "answer")
        self.assertEqual(self.runner._thread.name, "kafka-admin")

    def test_run_past_its_deadline_cancels_the_call(self) -> None:
        hung = Hung()

        with self.assertRaises(AdapterTimeoutError) as raised:
            finish_within(5, lambda: self.runner.run(hung.wait(), timeout=0.05, description="Kafka admin list_topics"))

        self.assertEqual(str(raised.exception), "Kafka admin list_topics timed out after 0.05s")
        self.assertIsInstance(raised.exception, TimeoutError)
        self.assertNotIsInstance(raised.exception, ValueError)
        self.assertTrue(hung.cancelled.wait(2), "the call was left running on the runner's loop")

    def test_run_async_past_its_deadline_cancels_the_call_without_holding_the_loop(self) -> None:
        hung = Hung()

        async def scenario():
            with self.assertRaises(AdapterTimeoutError) as raised:
                await count_turns_while(self.runner.run_async(hung.wait(), timeout=0.2, description="listing"))
            return raised.exception

        error = finish_within(5, lambda: asyncio.run(scenario()))

        self.assertEqual(str(error), "listing timed out after 0.2s")
        self.assertTrue(hung.cancelled.wait(2), "the call was left running on the runner's loop")

    def test_run_async_keeps_the_callers_loop_running(self) -> None:
        async def slow() -> str:
            await asyncio.sleep(0.2)
            return "answer"

        result, turns = asyncio.run(count_turns_while(self.runner.run_async(slow(), timeout=5)))

        self.assertEqual(result, "answer")
        # 0.2 s at one turn per 5 ms is 40 turns; a loop held for the call would turn at most once.
        self.assertGreaterEqual(turns, 10)

    def test_a_timeout_the_call_raises_itself_is_not_a_missed_deadline(self) -> None:
        async def refused() -> None:
            raise TimeoutError("the broker gave up")

        with self.assertRaises(TimeoutError) as raised:
            self.runner.run(refused(), timeout=5)
        self.assertNotIsInstance(raised.exception, AdapterTimeoutError)
        self.assertEqual(str(raised.exception), "the broker gave up")

        async def awaited() -> None:
            await self.runner.run_async(refused(), timeout=5)

        with self.assertRaises(TimeoutError) as raised:
            asyncio.run(awaited())
        self.assertNotIsInstance(raised.exception, AdapterTimeoutError)

    def test_cancelling_the_caller_cancels_the_call(self) -> None:
        """A stopping agent must not leave its call running on the runner's loop."""
        hung = Hung()

        async def scenario() -> None:
            waiting = asyncio.ensure_future(self.runner.run_async(hung.wait(), timeout=30))
            while not hung.started.is_set():
                await asyncio.sleep(0.005)
            waiting.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiting

        finish_within(5, lambda: asyncio.run(scenario()))

        self.assertTrue(hung.cancelled.wait(2))

    def test_a_closed_runner_refuses_work_instead_of_never_running_it(self) -> None:
        self.runner.close()

        async def answer() -> str:
            return "answer"

        call = answer()
        with self.assertRaises(RuntimeError):
            self.runner.run(call)
        call.close()


class HungAdmin:
    """An aiokafka admin client whose every request hangs; `start` hangs too when asked."""

    def __init__(self, *, hang_start: bool = False) -> None:
        self.hang_start = hang_start
        self.hung = Hung()
        self.closed = False

    async def start(self) -> None:
        if self.hang_start:
            await self.hung.wait()

    async def close(self) -> None:
        self.closed = True

    async def list_topics(self):
        await self.hung.wait()

    async def delete_topics(self, topics):
        await self.hung.wait()

    async def list_consumer_groups(self):
        await self.hung.wait()

    async def find_coordinator(self, group_id):
        await self.hung.wait()

    async def describe_consumer_groups(self, group_ids):
        await self.hung.wait()


class AnsweringAdmin:
    """An aiokafka admin client that answers after a short wait, as a broker does."""

    def __init__(self) -> None:
        self.threads: set[str] = set()
        self.deleted_topics: list[str] = []

    async def start(self) -> None:
        await self._answer()

    async def list_topics(self):
        await self._answer()
        return ["b-topic", "a-topic"]

    async def delete_topics(self, topics):
        await self._answer()
        self.deleted_topics.extend(topics)
        return types.SimpleNamespace(topic_error_codes=[(topic, 0) for topic in topics])

    async def list_consumer_groups(self):
        await self._answer()
        return [("group-b", "consumer"), types.SimpleNamespace(group="group-a")]

    async def _answer(self) -> None:
        self.threads.add(threading.current_thread().name)
        await asyncio.sleep(0.05)


class KafkaAdminDeadlineTests(unittest.TestCase):
    def client(self, admin, **kwargs) -> AIOKafkaAdminClient:
        client = AIOKafkaAdminClient(
            bootstrap_servers="kafka:9092",
            client_factory=lambda **ignored: admin,
            **kwargs,
        )
        self.addCleanup(client._runner.close)
        return client

    def test_every_call_has_a_deadline(self) -> None:
        self.assertEqual(self.client(AnsweringAdmin()).call_timeout_seconds, 120.0)
        calls = {
            "list_topics": lambda client: client.list_topics(),
            "delete_topics": lambda client: client.delete_topics(["topic"]),
            "list_consumer_groups": lambda client: client.list_consumer_groups(),
            "delete_consumer_group group-1": lambda client: client.delete_consumer_group("group-1"),
            "describe_consumer_group": lambda client: client.describe_consumer_group("group-1"),
        }
        for operation, call in calls.items():
            with self.subTest(operation=operation):
                admin = HungAdmin()
                client = self.client(admin, call_timeout_seconds=0.05)

                with patch(
                    "midstream.kafka.admin._delete_groups_request_type",
                    return_value=lambda groups: groups,
                ):
                    with self.assertRaises(AdapterTimeoutError) as raised:
                        finish_within(5, lambda: call(client))

                self.assertEqual(str(raised.exception), f"Kafka admin {operation} timed out after 0.05s")
                self.assertTrue(admin.hung.cancelled.wait(2))

    def test_awaited_calls_answer_as_the_sync_ones_do_without_holding_the_loop(self) -> None:
        admin = AnsweringAdmin()
        client = self.client(admin)

        async def scenario():
            topics, turns = await count_turns_while(client.list_topics_async())
            groups = await client.list_consumer_groups_async()
            await client.delete_topics_async(["a-topic"])
            return topics, groups, turns

        topics, groups, turns = asyncio.run(scenario())

        self.assertEqual(topics, ["a-topic", "b-topic"])
        self.assertEqual(groups, ["group-a", "group-b"])
        self.assertEqual(admin.deleted_topics, ["a-topic"])
        self.assertEqual(admin.threads, {"kafka-admin"})
        # Starting and listing wait 0.1 s on the client's loop; this loop turned meanwhile.
        self.assertGreaterEqual(turns, 5)
        self.assertEqual(client.list_topics(), topics)

    def test_an_awaited_call_has_the_deadline_too(self) -> None:
        admin = HungAdmin()
        client = self.client(admin, call_timeout_seconds=0.05)

        with self.assertRaises(AdapterTimeoutError) as raised:
            finish_within(5, lambda: asyncio.run(client.list_topics_async()))

        self.assertEqual(str(raised.exception), "Kafka admin list_topics timed out after 0.05s")
        self.assertTrue(admin.hung.cancelled.wait(2))

    def test_a_start_that_never_finishes_is_abandoned_and_closed(self) -> None:
        hung_admin = HungAdmin(hang_start=True)
        answering_admin = AnsweringAdmin()
        admins = [hung_admin, answering_admin]
        client = AIOKafkaAdminClient(
            bootstrap_servers="kafka:9092",
            client_factory=lambda **ignored: admins.pop(0),
            call_timeout_seconds=0.2,
        )
        self.addCleanup(client._runner.close)

        with self.assertRaises(AdapterTimeoutError):
            finish_within(5, client.list_topics)

        self.assertTrue(hung_admin.hung.cancelled.wait(2))
        deadline = time.monotonic() + 2
        while not hung_admin.closed and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(hung_admin.closed, "a client that never started kept its connections")
        # The next call starts a client of its own rather than reusing the one that never started.
        self.assertEqual(client.list_topics(), ["a-topic", "b-topic"])

    def test_a_call_past_its_deadline_gives_up_the_client_it_waited_on(self) -> None:
        """The connection that stopped answering may be why; the next call must not wait on it too."""
        hung_admin = HungAdmin()
        answering_admin = AnsweringAdmin()
        admins = [hung_admin, answering_admin]
        client = AIOKafkaAdminClient(
            bootstrap_servers="kafka:9092",
            client_factory=lambda **ignored: admins.pop(0),
            call_timeout_seconds=0.2,
        )
        self.addCleanup(client._runner.close)

        with self.assertRaises(AdapterTimeoutError):
            finish_within(5, lambda: asyncio.run(client.list_topics_async()))

        self.assertEqual(finish_within(5, client.list_topics), ["a-topic", "b-topic"])
        self.assertTrue(hung_admin.closed, "the client given up on kept its connections")
        self.assertEqual(admins, [])

    def test_the_deadline_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            AIOKafkaAdminClient(bootstrap_servers="kafka:9092", call_timeout_seconds=0)


if __name__ == "__main__":
    unittest.main()
