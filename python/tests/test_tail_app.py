"""The tail app's members, end to end over fakes: ticks, activation, seeding, ingest, answers, commits,
schema changes, bounds, backlogs, parking, peeks (handbook 19)."""

from __future__ import annotations

import asyncio
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest import mock

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tail_support import (  # noqa: E402
    BASE_MS,
    DATASET_ID,
    TailHarness,
    cancel,
    reader_on,
    schema_payload,
    target,
    tick,
)

from midstream.lakehouse.lifecycle import schema_to_json  # noqa: E402
from midstream.lakehouse.schema.infer import infer  # noqa: E402
from midstream.lakehouse.schema.policy import evolve  # noqa: E402
from midstream.tail_app import consumer as consumer_module  # noqa: E402
from midstream.tail_app.consumer import SOURCE_FETCH_MIN_BYTES  # noqa: E402
from midstream.tail_app.ingest import TailSchema  # noqa: E402
from midstream.tail_app.worker import KafkaClients  # noqa: E402
from midstream.tail_app.views import tail_head_view, tail_rows_view  # noqa: E402
from midstream.web.errors import WebError  # noqa: E402
from seal_cases import golden_evolution  # noqa: E402

FIELD_ID = b"PARQUET:field_id"


def rows_of(answer: dict) -> list[dict]:
    return pq.read_table(io.BytesIO(answer["body"])).to_pylist()


def offsets_of(answer: dict) -> list[tuple[int, int]]:
    return sorted((row["_kafka_partition"], row["_kafka_offset"]) for row in rows_of(answer))


def headers_json(answer: dict, name: str):
    return json.loads(answer["headers"][name])


class TrackingConsumer:
    """aiokafka's consumer as far as it tracks topics: a lookup of a partition its metadata does not
    know yet tracks the partition's topic, and an assignment tracks its partitions' topics. Every
    metadata refresh names the topics tracked; one tracking none asks for every topic."""

    def __init__(self) -> None:
        self._client = mock.Mock(_topics=set())
        self._client.set_topics.side_effect = lambda topics: setattr(self._client, "_topics", set(topics))
        self.assigned: set = set()

    def assign(self, partitions) -> None:
        self.assigned = set(partitions)
        self._client.set_topics([tp.topic for tp in partitions])

    def assignment(self) -> set:
        return set(self.assigned)

    async def beginning_offsets(self, partitions) -> dict:
        self._client._topics.update(tp.topic for tp in partitions)
        return {tp: 0 for tp in partitions}

    async def end_offsets(self, partitions) -> dict:
        self._client._topics.update(tp.topic for tp in partitions)
        return {tp: 10 for tp in partitions}


class TopicTrackingTests(unittest.IsolatedAsyncioTestCase):
    """A metadata request that names a topic makes a broker that creates topics on demand create it.
    On a test cluster, 52 topics deleted after their materializations came back at once: long-lived
    consumers that had looked up their offsets kept naming them in every refresh."""

    async def test_a_consumer_that_only_looks_up_offsets_tracks_no_topic(self) -> None:
        consumer = TrackingConsumer()
        adapter = consumer_module.ConsumerAdapter(consumer)
        await adapter.beginning_offsets([consumer_module.TP("fresh.orders", 0)])
        await adapter.end_offsets([consumer_module.TP("fresh.orders", 0), consumer_module.TP("fresh.orders", 1)])
        self.assertEqual(consumer._client._topics, set())

    async def test_a_consumer_that_reads_keeps_tracking_what_it_reads(self) -> None:
        consumer = TrackingConsumer()
        adapter = consumer_module.ConsumerAdapter(consumer)
        adapter.assign([consumer_module.TP("orders", 0)])
        await adapter.end_offsets([consumer_module.TP("orders", 0)])
        self.assertEqual(consumer._client._topics, {"orders"})
        # The peek lets its partitions go when it is done, and tracks none again.
        adapter.assign([])
        self.assertEqual(consumer._client._topics, set())

    async def test_a_lookup_that_fails_leaves_no_topic_tracked_either(self) -> None:
        consumer = TrackingConsumer()

        async def gone(partitions) -> dict:
            consumer._client._topics.update(tp.topic for tp in partitions)
            raise RuntimeError("the topic is being deleted")

        consumer.end_offsets = gone
        adapter = consumer_module.ConsumerAdapter(consumer)
        with self.assertRaises(RuntimeError):
            await adapter.end_offsets([consumer_module.TP("backlog.orders", 0)])
        self.assertEqual(consumer._client._topics, set())


class TailCase(unittest.IsolatedAsyncioTestCase):
    """One member, one table `mat-1/orders` on a 3-partition topic, and a reader of its dataset."""

    members = 1

    async def asyncSetUp(self) -> None:
        self.harness = TailHarness(members=self.members)
        self.target = target()
        self.harness.table(self.target)
        self.token = self.harness.control_plane.token("tok-rita", reader_on(DATASET_ID))

    @property
    def owner(self):
        return self.harness.owner_of("mat-1", "orders")

    async def tail(self, after: dict[int, int] | None = None, *, wait_ms: int = 0, token: str | None = None, worker=None, **query) -> dict:
        request = self.harness.request(token=token or self.token, after=after if after is not None else {}, wait_ms=wait_ms, **query)
        return await tail_rows_view(request, "mat-1", "orders", worker=worker or self.owner)

    async def refused(self, after: dict[int, int] | None = None, **kwargs) -> WebError:
        with self.assertRaises(WebError) as caught:
            await self.tail(after, **kwargs)
        return caught.exception

    async def activate(self, after: dict[int, int] | None = None) -> WebError | dict:
        """A first request, which activates the table, then the member catches up."""
        try:
            first = await self.tail(after if after is not None else {})
        except WebError as error:
            first = error
        await self.harness.settle()
        return first

    async def activate_other(self, materialization_id: str, table: str) -> None:
        """A first request for another table, whose answer does not matter: it activates it."""
        owner = self.harness.owner_of(materialization_id, table)
        try:
            await tail_rows_view(self.harness.request(token=self.token, after={}), materialization_id, table, worker=owner)
        except WebError:
            pass
        await self.harness.settle()


class SeedAndServeTests(TailCase):
    async def test_a_ticked_table_is_parked_until_asked_then_serves_what_is_past_its_committed_offsets(self) -> None:
        self.harness.produce("orders", 0, 5)
        self.harness.produce("orders", 1, 3)
        self.harness.deliver(tick(self.target, offsets={0: 1, 1: -1}))
        await self.harness.settle()
        table = self.owner.tables["tail:mat-1:orders"]
        self.assertEqual("parked", table.state)
        self.assertEqual([], self.harness.clients[0].sources[0].assigned if self.harness.clients[0].sources else [], "nothing read while parked")

        first = await self.activate({0: 1, 1: -1, 2: -1})

        self.assertIsInstance(first, WebError, "a parked table answers once its window is read")
        self.assertEqual("tail_not_ready", first.details["reason"])
        answer = await self.tail({0: 1, 1: -1, 2: -1})
        self.assertEqual([(0, 2), (0, 3), (0, 4), (1, 0), (1, 1), (1, 2)], offsets_of(answer))
        self.assertEqual({"0": 4, "1": 2, "2": -1}, headers_json(answer, "X-Tail-Through"))
        self.assertEqual("complete", answer["headers"]["X-Tail-Status"])
        self.assertEqual("live", answer["headers"]["X-Tail-State"])
        self.assertEqual({"0": 4, "1": 2, "2": -1}, headers_json(answer, "X-Tail-End-Offsets"))
        self.assertEqual(str(self.harness.schema.schema_id), answer["headers"]["X-Tail-Schema-Id"])
        self.assertEqual("6", answer["headers"]["X-Tail-Rows"])
        self.assertEqual(self.owner.config.member_id, answer["headers"]["X-Tail-Member"])
        self.assertEqual("control_plane", table.seed_source)

    async def test_an_answer_is_parquet_in_the_tables_physical_format(self) -> None:
        self.harness.produce("orders", 0, 2, value={"order_id": "A", "amount": 2.5, "v": {"z": 1}})
        self.harness.deliver(tick(self.target))
        await self.activate()

        answer = await self.tail({})

        table = pq.read_table(io.BytesIO(answer["body"]))
        self.assertEqual(self.harness.schema.arrow_schema.remove_metadata(), table.schema.remove_metadata())
        for column in table.schema:
            self.assertIn(FIELD_ID, column.metadata or {}, column.name)
        (row, _) = table.to_pylist()
        self.assertEqual((2.5, '{"z":1}'), (row["amount__v2"], row["v__json"]))
        self.assertEqual(str(BASE_MS), answer["headers"]["X-Tail-Time-Min"])
        self.assertEqual(pq.ParquetFile(io.BytesIO(answer["body"])).metadata.row_group(0).column(0).compression, "ZSTD")

    async def test_a_request_waits_for_a_parked_tables_window(self) -> None:
        self.harness.produce("orders", 2, 4)
        self.harness.deliver(tick(self.target))

        async def catch_up_meanwhile() -> None:
            for _ in range(10):
                await asyncio.sleep(0.02)
                await self.harness.step()
                await self.harness.fetch()

        waiting = asyncio.ensure_future(self.tail({}, wait_ms=2_500))
        await catch_up_meanwhile()
        answer = await waiting

        self.assertEqual([(2, 0), (2, 1), (2, 2), (2, 3)], offsets_of(answer))

    async def test_incremental_answers_continue_at_through_and_never_repeat_a_row(self) -> None:
        self.harness.deliver(tick(self.target))
        await self.activate()
        seen: list[tuple[int, int]] = []
        through = {0: -1, 1: -1, 2: -1}
        for round_ in range(5):
            for partition in range(3):
                self.harness.produce("orders", partition, round_ + 1)
            await self.harness.fetch()
            answer = await self.tail(through)
            seen.extend(offsets_of(answer))
            through = {int(partition): offset for partition, offset in headers_json(answer, "X-Tail-Through").items()}

        self.assertEqual(len(seen), len(set(seen)))
        self.assertEqual(sorted(seen), sorted((partition, offset) for partition in range(3) for offset in range(15)))
        self.assertEqual({0: 14, 1: 14, 2: 14}, through)

    async def test_format_json_answers_the_parquets_rows(self) -> None:
        self.harness.produce("orders", 0, 3)
        self.harness.deliver(tick(self.target))
        await self.activate()

        parquet = rows_of(await self.tail({}))
        as_json = json.loads((await self.tail({}, format="json"))["body"])["rows"]

        self.assertEqual([row["_kafka_offset"] for row in parquet], [row["_kafka_offset"] for row in as_json])
        self.assertEqual([row["order_id"] for row in parquet], [row["order_id"] for row in as_json])

    async def test_the_schema_comes_from_the_table_when_the_control_plane_cannot_give_it(self) -> None:
        from gcs_fakes import FakeGcs  # noqa: F401
        from lakehouse_support import LakeFixture

        from midstream.lakehouse.gcs import set_shared_storage_client

        lake = LakeFixture({"order_id": "O", "amount": 1})
        self.addCleanup(lake.close)
        lake.commit(lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1, 2]))
        set_shared_storage_client(lake.client)
        self.addCleanup(set_shared_storage_client, None)
        located = target(materialization_id=lake.materialization_id, table="orders")
        located = type(located)(**{**located.to_dict(), "location": lake.location, "dataset": "shop", "table_uuid": str(lake.load().metadata.table_uuid)})
        self.harness.table(located)
        self.harness.control_plane.schema_down = True
        self.harness.produce("orders", 0, 5)
        self.harness.deliver(tick(located))
        owner = self.harness.owner_of(lake.materialization_id, "orders")

        async def ask() -> dict:
            request = self.harness.request(token=self.token, after={0: 2})
            return await tail_rows_view(request, lake.materialization_id, "orders", worker=owner)

        try:
            await ask()
        except WebError:
            pass
        await self.harness.settle()
        answer = await ask()

        table = owner.tables[f"tail:{lake.materialization_id}:orders"]
        self.assertEqual("gcs", table.seed_source)
        self.assertEqual(2, table.committed[0], "the table's own committed offsets")
        self.assertEqual([(0, 3), (0, 4)], offsets_of(answer))


class FollowingTheTableTests(TailCase):
    async def test_a_commit_keeps_rows_for_the_retention_then_lets_them_go(self) -> None:
        self.harness.produce("orders", 0, 6)
        self.harness.deliver(tick(self.target, retention_seconds=30))
        await self.activate()
        table = self.owner.tables["tail:mat-1:orders"]
        for partition in list(table.buffer.partitions):
            table.close_segment(partition)

        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": {"0": 5}})
        self.assertEqual(6, (await self.tail({0: -1}))["headers"]["X-Tail-Rows"] and len(rows_of(await self.tail({0: -1}))))
        self.harness.clock.advance(31)
        await self.harness.step()

        error = await self.refused({0: -1})
        self.assertEqual("tail_unavailable", error.details["reason"])
        self.assertEqual({"0": "behind_retention"}, error.details["partitions"])
        self.assertEqual(0, len(rows_of(await self.tail({0: 5}))))

    async def test_commits_never_move_backwards_whatever_arrives_late(self) -> None:
        self.harness.deliver(tick(self.target, offsets={0: 9}))
        await self.activate()
        table = self.owner.tables["tail:mat-1:orders"]

        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": {"0": 4}})
        self.harness.deliver(tick(self.target, offsets={0: 2}))

        self.assertEqual(9, table.committed[0])

    async def test_a_schema_change_rebuilds_what_is_held_under_the_new_schema(self) -> None:
        first = golden_evolution()
        self.harness.produce("orders", 0, 1, value={"order_id": "A", "extra": "new"})
        self.harness.deliver(tick(self.target))
        await self.activate()
        before = rows_of(await self.tail({}))
        self.assertEqual({"extra": "new"}, json.loads(before[0]["_unmapped"]))
        self.assertEqual("1", (await self.tail({}))["headers"]["X-Tail-Settling"])
        evolved = evolve(first.schema, first.fork_map, [((), infer({"extra": "s"}))], last_column_id=first.last_column_id)

        self.harness.table_event(
            "schema_changed",
            "mat-1",
            "orders",
            {"schema_id": evolved.schema.schema_id, "schema": schema_to_json(evolved.schema), "fork_map": evolved.fork_map.to_dict(), "last_column_id": evolved.last_column_id, "rejected_paths": []},
        )
        answer = await self.tail({})

        (row,) = rows_of(answer)
        self.assertEqual(("new", None), (row["extra"], row["_unmapped"]))
        self.assertEqual((str(evolved.schema.schema_id), "0"), (answer["headers"]["X-Tail-Schema-Id"], answer["headers"]["X-Tail-Settling"]))

    async def test_a_tick_naming_a_newer_schema_fetches_it_when_no_event_said_so(self) -> None:
        first = golden_evolution()
        self.harness.deliver(tick(self.target, schema_id=first.schema.schema_id))
        await self.activate()
        evolved = evolve(first.schema, first.fork_map, [((), infer({"extra": "s"}))], last_column_id=first.last_column_id)
        newer = TailSchema(schema_id=evolved.schema.schema_id, iceberg_schema=schema_to_json(evolved.schema), fork_map=evolved.fork_map.to_dict())
        self.harness.control_plane.schemas[("mat-1", "orders")] = schema_payload(newer)

        self.harness.deliver(tick(self.target, schema_id=evolved.schema.schema_id))
        for _ in range(5):
            await asyncio.sleep(0)

        self.assertEqual(evolved.schema.schema_id, self.owner.tables["tail:mat-1:orders"].schema.schema_id)

    async def test_another_incarnation_of_the_table_is_seeded_afresh(self) -> None:
        self.harness.produce("orders", 0, 4)
        self.harness.deliver(tick(self.target, offsets={0: 1}))
        await self.activate()
        self.assertEqual([(0, 2), (0, 3)], offsets_of(await self.tail({0: 1})))

        reborn = type(self.target)(**{**self.target.to_dict(), "table_uuid": "uuid-2"})
        self.harness.deliver(tick(reborn, offsets={}))
        table = self.owner.tables["tail:mat-1:orders"]
        self.assertEqual("seeding", table.state, "watched, so it seeds again at once")
        await self.harness.settle()

        self.assertEqual([(0, 0), (0, 1), (0, 2), (0, 3)], offsets_of(await self.tail({})), "the new table holds nothing yet")

    async def test_a_cancel_ends_the_tail_at_once(self) -> None:
        self.harness.produce("orders", 0, 2)
        self.harness.deliver(tick(self.target))
        await self.activate()

        self.harness.deliver(cancel(self.target, "materialization_stopped"))
        await self.harness.step()

        self.assertNotIn("tail:mat-1:orders", self.owner.tables)
        self.harness.control_plane.materializations["mat-1"]["materialization"]["desired_phase"] = "stopped"
        error = await self.refused({})
        self.assertEqual((404, "live_tail_off"), (error.status_code, error.details["reason"]))

    async def test_a_4_1_control_planes_cancel_and_switch_are_still_honoured(self) -> None:
        # A control plane before 4.3 cancels a materialization whose live tail is off, and says so.
        self.harness.produce("orders", 0, 2)
        self.harness.deliver(tick(self.target))
        await self.activate()

        self.harness.deliver(cancel(self.target, "live_tail_off"))
        await self.harness.step()

        self.assertNotIn("tail:mat-1:orders", self.owner.tables)
        self.harness.control_plane.materializations["mat-1"]["materialization"]["live_tail"] = False
        error = await self.refused({})
        self.assertEqual((404, "live_tail_off"), (error.status_code, error.details["reason"]))

    async def test_a_table_no_tick_names_any_more_is_forgotten(self) -> None:
        self.harness.deliver(tick(self.target))
        await self.activate()

        self.harness.clock.advance(301)
        await self.harness.step()

        self.assertNotIn("tail:mat-1:orders", self.owner.tables)
        self.assertEqual(set(), self.harness.clients[0].sources[0].assignment())

    async def test_partitions_the_topic_gains_are_read_from_their_beginning(self) -> None:
        self.harness.deliver(tick(self.target))
        await self.activate()
        table = self.owner.tables["tail:mat-1:orders"]
        self.harness.kafka.create("orders", 4)
        self.harness.produce("orders", 3, 2)

        table.add_partitions([0, 1, 2, 3])
        self.owner.apply_plan()
        await self.harness.settle()

        self.assertEqual([(3, 0), (3, 1)], offsets_of(await self.tail({0: -1, 1: -1, 2: -1})))


class OneConsumerTests(TailCase):
    async def test_one_consumer_reads_every_active_table_and_reassigns_as_they_come_and_go(self) -> None:
        other = target(table="payments", topic="payments")
        self.harness.table(other)
        owner = self.harness.owner_of("mat-1", "payments")
        self.assertIs(owner, self.owner, "one member here")
        self.harness.deliver(tick(self.target))
        self.harness.deliver(tick(other))
        await self.activate()
        source = self.harness.clients[0].sources[0]
        self.assertEqual({("orders", 0), ("orders", 1), ("orders", 2)}, {(tp.topic, tp.partition) for tp in source.assignment()})

        await self.activate_other("mat-1", "payments")

        self.assertEqual(1, len(self.harness.clients[0].sources), "one source consumer")
        self.assertEqual(6, len(source.assignment()))
        self.harness.deliver(cancel(other))
        self.assertEqual(3, len(source.assignment()))

    async def test_two_tables_of_one_topic_share_its_partitions_from_where_each_is(self) -> None:
        twin = target(materialization_id="mat-2", table="orders", topic="orders", table_uuid="uuid-9")
        self.harness.table(twin)
        self.harness.produce("orders", 0, 6)
        self.harness.deliver(tick(self.target, offsets={0: 3}))
        self.harness.deliver(tick(twin, offsets={0: 0}))
        await self.activate()
        twin_owner = self.harness.owner_of("mat-2", "orders")
        try:
            await tail_rows_view(self.harness.request(token=self.token, after={}), "mat-2", "orders", worker=twin_owner)
        except WebError:
            pass
        await self.harness.settle()

        mine = await self.tail({0: 3})
        theirs = await tail_rows_view(self.harness.request(token=self.token, after={0: 0}), "mat-2", "orders", worker=twin_owner)

        self.assertEqual([(0, 4), (0, 5)], offsets_of(mine))
        self.assertEqual([(0, 1), (0, 2), (0, 3), (0, 4), (0, 5)], offsets_of(theirs))


    def test_the_source_waits_for_a_batch_of_records_and_the_others_answer_at_once(self) -> None:
        # The fetch loop goes over every active table after each fetch: answered for every record,
        # a member following fifty quiet tables on a test cluster spent most of a core doing it.
        built: dict[str, dict] = {}

        class Consumer:
            def __init__(self, **options) -> None:
                built.setdefault("options", []).append(options)

        module = type("aiokafka", (), {"AIOKafkaConsumer": Consumer})
        clients = KafkaClients()
        with mock.patch.dict(os.environ, {"K_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"}), mock.patch.object(consumer_module, "import_module", return_value=module):
            clients.source(fetch_wait_ms=200)
            clients.peek()
            clients.events()
            clients.metadata()

        source, *others = built["options"]
        self.assertEqual((SOURCE_FETCH_MIN_BYTES, 200), (source["fetch_min_bytes"], source["fetch_max_wait_ms"]))
        self.assertGreater(SOURCE_FETCH_MIN_BYTES, 1)
        self.assertEqual([1, 1, 1], [options["fetch_min_bytes"] for options in others], "a peek reads what is there")


class BoundsAndBacklogTests(TailCase):
    async def test_the_bound_pauses_only_that_table_and_answers_say_so(self) -> None:
        other = target(table="payments", topic="payments")
        self.harness.table(other)
        self.harness.deliver(tick(self.target, max_rows_per_table=1_000))
        self.harness.deliver(tick(other, max_rows_per_table=1_000))
        await self.activate()
        await self.activate_other("mat-1", "payments")
        # A burst between the bound and twice it, once both tables are live.
        self.harness.produce("orders", 0, 1_500)
        self.harness.produce("payments", 0, 10)
        await self.harness.settle(5)

        answer = await self.tail({})

        self.assertEqual("truncated", answer["headers"]["X-Tail-Status"])
        self.assertEqual("paused", answer["headers"]["X-Tail-State"])
        self.assertEqual([0, 1, 2], headers_json(answer, "X-Tail-Paused"))
        held = offsets_of(answer)
        self.assertEqual(list(range(len(held))), [offset for _, offset in held], "a contiguous prefix")
        self.assertGreaterEqual(len(held), 1_000)
        self.assertEqual("live", self.owner.tables["tail:mat-1:payments"].state, "the other table keeps up")
        # The runtime commits: the window shrinks under 75 % and the tail reads on to the head.
        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": {"0": 1_200}})
        await self.harness.settle(5)
        self.assertEqual("live", self.owner.tables["tail:mat-1:orders"].state)
        self.assertEqual(1_499, headers_json(await self.tail({0: 1_200}), "X-Tail-Through")["0"])

    async def test_a_runtime_far_behind_is_backlogged_reads_nothing_and_says_how_far(self) -> None:
        self.harness.produce("orders", 0, 3_000)
        self.harness.deliver(tick(self.target, offsets={}, max_rows_per_table=1_000))

        first = await self.activate()
        error = await self.refused({})

        self.assertIsInstance(first, WebError)
        self.assertEqual((409, "tail_backlogged"), (error.status_code, error.details["reason"]))
        # Offsets 0 to 2,999, none of them in the table yet.
        self.assertEqual(3_000, error.details["behind"])
        self.assertEqual({"0": 2_999, "1": -1, "2": -1}, error.details["end_offsets"])
        source = self.harness.clients[0].sources[0]
        self.assertEqual(set(), source.assignment(), "nothing read during a backlog")
        # The runtime catches up: within 75 % of the bound the tail catches up, and is live after.
        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": {"0": 2_500}})
        await self.harness.settle(4)
        answer = await self.tail({0: 2_500})
        self.assertEqual("live", answer["headers"]["X-Tail-State"])
        self.assertEqual(499, len(rows_of(answer)))

    async def test_a_burst_past_twice_the_bound_is_a_backlog_and_the_boundary_does_not_flap(self) -> None:
        self.harness.deliver(tick(self.target, max_rows_per_table=1_000))
        await self.activate()
        table = self.owner.tables["tail:mat-1:orders"]
        self.harness.produce("orders", 0, 1_900)
        await self.harness.settle(4)
        self.assertEqual("paused", table.state, "past the bound, under twice it")

        self.harness.produce("orders", 0, 200)
        await self.harness.settle(2)
        self.assertEqual("backlogged", table.state)
        self.assertEqual(0, table.buffer.rows(), "the buffer is dropped")
        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": {"0": 1_000}})
        await self.harness.settle(2)
        self.assertEqual("backlogged", table.state, "1,099 behind is still above 75 % of the bound")
        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": {"0": 1_400}})
        await self.harness.settle(4)
        self.assertEqual("live", table.state)

    async def test_a_table_above_the_ceiling_is_read_at_the_ceiling_and_the_others_keep_up(self) -> None:
        other = target(table="payments", topic="payments")
        self.harness.table(other)
        self.harness.deliver(tick(self.target, max_records_per_second_per_table=100))
        self.harness.deliver(tick(other))
        await self.activate()
        await self.activate_other("mat-1", "payments")
        self.harness.produce("orders", 0, 500)
        self.harness.produce("payments", 0, 50)

        await self.harness.fetch(3)

        table = self.owner.tables["tail:mat-1:orders"]
        self.assertLessEqual(table.buffer.rows(), 100, "read at most the ceiling in a second")
        self.assertEqual(50, self.owner.tables["tail:mat-1:payments"].buffer.rows())
        held = sorted(offset for _, offset in offsets_of(await self.tail({})))
        self.assertEqual(list(range(len(held))), held, "a contiguous prefix")
        self.harness.clock.advance(1.0)
        await self.harness.fetch(2)
        self.assertGreater(table.buffer.rows(), 100, "the next second reads on, from where it stopped")
        held = sorted(offset for _, offset in offsets_of(await self.tail({})))
        self.assertEqual(list(range(len(held))), held)


class RecordsTests(TailCase):
    async def test_tombstones_are_rows_unless_the_materialization_skips_them(self) -> None:
        self.harness.kafka.produce("orders", 0, None, key="K0")
        self.harness.produce("orders", 0, 1)
        self.harness.deliver(tick(self.target))
        await self.activate()
        rows = rows_of(await self.tail({}))
        self.assertEqual([0, 1], [row["_kafka_offset"] for row in rows])
        self.assertIsNone(rows[0]["order_id"])

        skipping = target(materialization_id="mat-3", skip_tombstones=True)
        self.harness.table(skipping)
        self.harness.kafka.create("orders", 3)
        self.harness.deliver(tick(skipping))
        owner = self.harness.owner_of("mat-3", "orders")
        try:
            await tail_rows_view(self.harness.request(token=self.token, after={}), "mat-3", "orders", worker=owner)
        except WebError:
            pass
        await self.harness.settle()
        answer = await tail_rows_view(self.harness.request(token=self.token, after={}), "mat-3", "orders", worker=owner)
        self.assertEqual([(0, 1)], offsets_of(answer))
        self.assertEqual("1", answer["headers"]["X-Tail-Rows"])
        self.assertEqual({"0": 1, "1": -1, "2": -1}, headers_json(answer, "X-Tail-Through"), "the tombstone's offset is covered")

    async def test_a_value_kaspr_cannot_decode_is_counted_and_gets_no_row(self) -> None:
        self.harness.kafka.produce("orders", 0, b"\xff not json", raw=True)
        self.harness.produce("orders", 0, 1)
        self.harness.deliver(tick(self.target))
        await self.activate()

        answer = await self.tail({})

        self.assertEqual([(0, 1)], offsets_of(answer))
        self.assertEqual(1, self.owner.tables["tail:mat-1:orders"].counters.undecodable)

    async def test_offsets_kafka_holds_no_record_of_are_covered(self) -> None:
        self.harness.produce("orders", 0, 2)
        self.harness.kafka.skip_offsets("orders", 0, 3)
        self.harness.produce("orders", 0, 1)
        self.harness.deliver(tick(self.target))
        await self.activate()

        answer = await self.tail({0: -1})

        self.assertEqual([(0, 0), (0, 1), (0, 5)], offsets_of(answer))
        self.assertEqual(5, headers_json(answer, "X-Tail-Through")["0"])

    async def test_records_retention_removed_are_served_from_kafkas_beginning_and_flagged(self) -> None:
        self.harness.produce("orders", 0, 10)
        self.harness.kafka.truncate_before("orders", 0, 6)
        self.harness.deliver(tick(self.target, offsets={0: 2}))
        await self.activate({0: 2})

        answer = await self.tail({0: 2})

        self.assertEqual([(0, 6), (0, 7), (0, 8), (0, 9)], offsets_of(answer))
        self.assertEqual([0], headers_json(answer, "X-Tail-Gap"))


class ParkingTests(TailCase):
    async def test_a_table_nobody_asks_for_is_parked_and_holds_nothing(self) -> None:
        self.harness.produce("orders", 0, 3)
        self.harness.deliver(tick(self.target, idle_seconds=600))
        await self.activate()
        table = self.owner.tables["tail:mat-1:orders"]
        self.assertTrue(table.active)

        self.harness.clock.advance(601)
        self.harness.deliver(tick(self.target, idle_seconds=600))
        await self.harness.step()

        self.assertEqual("parked", table.state)
        self.assertEqual(0, table.buffer.rows())
        self.assertEqual(set(), self.harness.clients[0].sources[0].assignment())

    async def test_idle_seconds_zero_keeps_a_table_tailed(self) -> None:
        self.harness.deliver(tick(self.target, idle_seconds=0))
        await self.activate()
        self.harness.clock.advance(10_000)
        self.harness.deliver(tick(self.target, idle_seconds=0))
        await self.harness.step()
        self.assertTrue(self.owner.tables["tail:mat-1:orders"].active)

    async def test_a_restarted_member_seeds_again_and_serves_the_same_prefix(self) -> None:
        self.harness.produce("orders", 0, 4)
        self.harness.deliver(tick(self.target, offsets={0: 0}))
        await self.activate()
        before = offsets_of(await self.tail({0: 0}))
        restarted = TailHarness(members=1)
        restarted.kafka = self.harness.kafka
        restarted.control_plane = self.harness.control_plane
        for worker in restarted.workers:
            worker.http = restarted.control_plane
            worker.control_plane.http = restarted.control_plane
            worker.sessions.control_plane.http = restarted.control_plane
            worker.clients.kafka = self.harness.kafka
        self.harness = restarted
        self.harness.deliver(tick(self.target, offsets={0: 0}))
        await self.activate({0: 0})

        self.assertEqual(before, offsets_of(await self.tail({0: 0})))


class PeekTests(TailCase):
    async def peek(self, *, rows: int = 200, token: str | None = None) -> dict:
        request = self.harness.request(token=token or self.token, rows=rows)
        return await tail_head_view(request, "mat-1", "orders", worker=self.owner)

    async def test_a_live_table_peeks_from_its_buffer_newest_first(self) -> None:
        for offset in range(6):
            self.harness.produce("orders", offset % 3, 1, timestamp=BASE_MS + offset * 1000)
        self.harness.deliver(tick(self.target))
        await self.activate()

        answer = await self.peek(rows=4)

        rows = rows_of(answer)
        self.assertEqual(4, len(rows))
        stamps = [row["_kafka_timestamp"] for row in rows]
        self.assertEqual(sorted(stamps, reverse=True), stamps)
        self.assertEqual("live", answer["headers"]["X-Tail-State"])
        self.assertEqual("4", answer["headers"]["X-Tail-Peek-Rows"])

    async def test_a_backlogged_table_peeks_at_kafkas_head_with_a_consumer_of_its_own(self) -> None:
        self.harness.produce("orders", 0, 3_000)
        self.harness.produce("orders", 1, 10)
        self.harness.deliver(tick(self.target, offsets={}, max_rows_per_table=1_000))
        await self.activate()
        self.assertEqual("backlogged", self.owner.tables["tail:mat-1:orders"].state)

        answer = await self.peek(rows=10)

        rows = rows_of(answer)
        self.assertEqual(10, len(rows))
        offsets = headers_json(answer, "X-Tail-Peek-Offsets")
        self.assertEqual(2_999, offsets["0"][1])
        self.assertEqual("backlogged", answer["headers"]["X-Tail-State"])
        self.assertEqual(set(), self.harness.clients[0].sources[0].assignment(), "the tail's own consumer reads nothing")
        self.assertGreater(int(answer["headers"]["X-Tail-Behind"]), 2_900)

    async def test_peeks_are_bounded_and_kept_a_second(self) -> None:
        self.harness.produce("orders", 0, 50)
        self.harness.deliver(tick(self.target, peek_max_rows=10))
        await self.activate()

        first = await self.peek(rows=500)
        self.harness.produce("orders", 0, 5)
        again = await self.peek(rows=500)

        self.assertEqual(10, len(rows_of(first)), "clamped to the setting")
        self.assertEqual(first["body"], again["body"], "the same answer within the second")
        with self.assertRaises(WebError) as caught:
            await self.peek(rows=0)
        self.assertEqual(400, caught.exception.status_code)


if __name__ == "__main__":
    unittest.main()
