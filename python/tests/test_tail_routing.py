"""Which member of the tail app owns a table, where it is, and how a request reaches it (tail_app/routing.py)."""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tail_support import (  # noqa: E402
    DATASET_ID,
    TICK_TOPIC,
    TailHarness,
    encode_member_assignment,
    encode_member_metadata,
    reader_on,
    target,
    tick,
)

from midstream.kafka.admin import ConsumerGroupDescription, GroupMemberDescription  # noqa: E402
from midstream.models import live_tail_routing_key  # noqa: E402
from midstream.tail_app.routing import (  # noqa: E402
    decode_member_assignment,
    decode_member_metadata,
    fallback_url,
    instance_name,
    member_map,
    murmur2,
    owner_partition,
)
from midstream.tail_app.views import tail_rows_view  # noqa: E402
from midstream.web.errors import WebError  # noqa: E402

#: Computed with aiokafka 0.14.0's `murmur2` and `DefaultPartitioner` - the partitioner Faust's
#: producer uses - and, for the two maintenance keys, the partitions the lab's ticks were found on.
PARTITIONER_VECTORS = [
    ("tail:mat-1:orders", 2865328073, 9, 3),
    ("tail:mat-1:payments", 1788429764, 4, 2),
    ("tail:mat-5c8ed527453d:refunds", 1568253468, 28, 0),
    ("tail:mat-bbc480ce42f0:orders", 2966315333, 5, 3),
    ("tail:mat-a1b2c3d4e5f6:orders", 2539246972, 60, 2),
    ("", 275646681, 25, 3),
    ("a", 2731586172, 60, 4),
    ("tail:mat-é:漢字", 642393582, 46, 0),
    ("maintenance:mat-5c8ed527453d:refunds", 197063692, 12, 4),
    ("maintenance:mat-5e7d3a7b9147:orders_by_month", 1654715455, 63, 1),
]

#: A member of the lab's maintenance app as DescribeGroups v2 reported it on 2026-10-23.
LAB_MEMBER = {
    "member_id": "develop-materializer-maintenance-app-0-a22d4404-9a28-4818-893a-9e55fbe3a01b",
    "metadata": "AAQAAAACAClkZXZlbG9wLW1hdGVyaWFsaXplci1tYWludGVuYW5jZS1yZXF1ZXN0cwA0ZGV2ZWxvcC1tYXRlcmlhbGl6ZXItbWFpbnRlbmFuY2UtYXBwLWFzc2lnbm9yLWxlYWRlcgAAAN97ImFzc2lnbm1lbnQiOiB7ImFjdGl2ZXMiOiB7fSwgInN0YW5kYnlzIjoge319LCAidXJsIjogImh0dHA6Ly9kZXZlbG9wLW1hdGVyaWFsaXplci1tYWludGVuYW5jZS1hcHAtMC5kZXZlbG9wLW1hdGVyaWFsaXplci1tYWludGVuYW5jZS1hcHAubWlkc3RyZWFtLnN2Yy5jbHVzdGVyLmxvY2FsOjYwNjUiLCAiY2hhbmdlbG9nX2Rpc3RyaWJ1dGlvbiI6IHt9LCAidG9waWNfZ3JvdXBzIjoge319",
    "assignment": "AAQAAAACADRkZXZlbG9wLW1hdGVyaWFsaXplci1tYWludGVuYW5jZS1hcHAtYXNzaWdub3ItbGVhZGVyAAAAAQAAAAAAKWRldmVsb3AtbWF0ZXJpYWxpemVyLW1haW50ZW5hbmNlLXJlcXVlc3RzAAAABgAAAAAAAAABAAAAAgAAAAMAAAAEAAAABQAAANV4nMVRy27CMBD8Fctn24S2cMivVBFanG1YYWzjXUdqUf69bvoBBXHobUfaeWjmpoGZpnjBKLpXNw1eaEZe7xFnDCnbCwgWgkBfWBqgKBgherSQs/3lp2IDwoilEd+7wai/yQWvFVl4ZRi1NerFqFej3ozaDUtTYIE4Hj+fy/JwlGH58a4lNKBPIrnfbO4y79w9b+4MH2ewfA2OZ+98qNyeXUgeQr/v9jvd3P0J4tSkpsNILIWOVSjFtYf/SNR811IkZfKHqaSanxqle3CT7bJ8A+HZ9jU=",
}


class OwnerPartitionTests(unittest.TestCase):
    def test_keys_hash_as_the_control_planes_producer_puts_them(self) -> None:
        for key, hashed, of_64, of_6 in PARTITIONER_VECTORS:
            with self.subTest(key=key):
                self.assertEqual(hashed, murmur2(key.encode("utf-8")))
                self.assertEqual(of_64, owner_partition(key, 64))
                self.assertEqual(of_6, owner_partition(key, 6))

    def test_a_table_has_one_owner_partition(self) -> None:
        self.assertEqual(owner_partition(live_tail_routing_key("mat-1", "orders"), 64), owner_partition("tail:mat-1:orders", 64))
        with self.assertRaises(ValueError):
            owner_partition("tail:mat-1:orders", 0)

    def test_hundreds_of_tables_spread_over_the_partitions(self) -> None:
        counts = [0] * 64
        for index in range(640):
            counts[owner_partition(f"tail:mat-{index:012x}:orders", 64)] += 1
        self.assertGreater(min(counts), 0)
        self.assertLess(max(counts), 30)


class MemberMapTests(unittest.TestCase):
    def test_a_lab_members_metadata_names_its_url_and_its_assignment_its_partitions(self) -> None:
        metadata = base64.b64decode(LAB_MEMBER["metadata"])
        assignment = base64.b64decode(LAB_MEMBER["assignment"])

        version, subscription, user_data = decode_member_metadata(metadata)
        self.assertEqual(4, version)
        self.assertEqual(["develop-materializer-maintenance-requests", "develop-materializer-maintenance-app-assignor-leader"], subscription)
        self.assertEqual(
            "http://develop-materializer-maintenance-app-0.develop-materializer-maintenance-app.midstream.svc.cluster.local:6065",
            json.loads(user_data)["url"],
        )
        _, assigned, _ = decode_member_assignment(assignment)
        self.assertIn(("develop-materializer-maintenance-requests", [0, 1, 2, 3, 4, 5]), assigned)

        mapped = member_map(
            ConsumerGroupDescription(
                group_id="develop-materializer-maintenance-app",
                state="Stable",
                members=(GroupMemberDescription(member_id=LAB_MEMBER["member_id"], client_id="faust-1.17.22", client_host="/10.42.0.144", metadata=metadata, assignment=assignment),),
            ),
            topic="develop-materializer-maintenance-requests",
        )
        (member,) = mapped.members
        self.assertEqual("develop-materializer-maintenance-app-0", member.name)
        self.assertEqual((0, 1, 2, 3, 4, 5), member.partitions)
        self.assertIs(member, mapped.owner(3))
        self.assertIsNone(mapped.owner(6))

    def test_a_member_whose_metadata_does_not_read_is_reached_by_its_pods_name(self) -> None:
        own = "http://materializer-tail-app-0.materializer-tail-app.midstream.svc.cluster.local:6065"
        description = ConsumerGroupDescription(
            group_id="materializer-tail-app",
            state="Stable",
            members=(
                GroupMemberDescription(
                    member_id="materializer-tail-app-1-0b0e2c4a-1111-2222-3333-444455556666",
                    client_id="faust",
                    client_host="/10.0.0.2",
                    metadata=encode_member_metadata([TICK_TOPIC], b"\x00 not json"),
                    assignment=encode_member_assignment([(TICK_TOPIC, [1, 3]), ("other", [0])]),
                ),
            ),
        )

        (member,) = member_map(description, topic=TICK_TOPIC, own_url=own).members

        self.assertEqual("http://materializer-tail-app-1.materializer-tail-app.midstream.svc.cluster.local:6065", member.url)
        self.assertEqual((1, 3), member.partitions)

    def test_names_and_addresses(self) -> None:
        self.assertEqual("app-0", instance_name("app-0-a22d4404-9a28-4818-893a-9e55fbe3a01b"))
        self.assertEqual("faust-1-dynamic", instance_name("faust-1-dynamic"))
        self.assertIsNone(fallback_url("pod-1", None))
        self.assertIsNone(fallback_url("pod-1", "no-scheme"))
        self.assertEqual({"state": "Dead", "members": [], "partitions": {}}, member_map(ConsumerGroupDescription(group_id="g", state="Dead"), topic=TICK_TOPIC).describe())

    def test_truncated_protocol_data_is_refused_not_misread(self) -> None:
        good = encode_member_assignment([(TICK_TOPIC, [1, 2])])
        with self.assertRaises(ValueError):
            decode_member_assignment(good[:-6])
        self.assertEqual((0, [], None), decode_member_assignment(b""))


class ForwardingTests(unittest.IsolatedAsyncioTestCase):
    """Two members; every request may reach either, and only the owner answers rows."""

    async def asyncSetUp(self) -> None:
        self.harness = TailHarness(members=2)
        self.target = target()
        self.harness.table(self.target)
        self.token = self.harness.control_plane.token("tok-rita", reader_on(DATASET_ID))
        self.owner = self.harness.owner_of("mat-1", "orders")
        self.other = next(worker for worker in self.harness.workers if worker is not self.owner)
        for worker in self.harness.workers:
            await worker.refresh_member_map()
        self.harness.produce("orders", 0, 3)
        self.harness.deliver(tick(self.target))

    async def ask(self, worker, after=None, **kwargs):
        return await tail_rows_view(self.harness.request(token=self.token, after=after if after is not None else {}, **kwargs), "mat-1", "orders", worker=worker)

    async def warm(self) -> None:
        try:
            await self.ask(self.owner)
        except WebError:
            pass
        await self.harness.settle()

    async def test_a_request_to_another_member_is_forwarded_to_the_owner_and_answered_unchanged(self) -> None:
        await self.warm()

        direct = await self.ask(self.owner)
        forwarded = await self.ask(self.other)

        self.assertEqual(direct["body"], forwarded["body"])
        self.assertEqual(self.owner.config.member_id, forwarded["headers"]["X-Tail-Member"])
        self.assertEqual("1", forwarded["headers"]["X-Tail-Forwarded"])
        path, headers = self.harness.control_plane.forwarded[-1]
        self.assertEqual("/tail/v1/materializations/mat-1/tables/orders", path)
        self.assertEqual(f"Bearer {self.token}", headers["authorization"])
        self.assertIn("x-materializer-console", headers)
        self.assertEqual(self.other.config.member_id, headers["X-Tail-Forwarded-By"])
        self.assertNotIn("tail:mat-1:orders", self.other.tables, "the forwarder holds nothing of it")

    async def test_a_forwarded_request_is_never_forwarded_again(self) -> None:
        request = self.harness.request(token=self.token, after={})
        request.headers["X-Tail-Forwarded-By"] = self.owner.config.member_id

        with self.assertRaises(WebError) as caught:
            await tail_rows_view(request, "mat-1", "orders", worker=self.other)

        self.assertEqual((409, "tail_rebalancing"), (caught.exception.status_code, caught.exception.details["reason"]))

    async def test_a_stale_map_is_read_again_once_then_answers_rebalancing(self) -> None:
        await self.warm()
        self.other.members = type(self.other.members)()
        self.harness.admin.members = []
        calls = self.harness.admin.calls

        with self.assertRaises(WebError) as caught:
            await self.ask(self.other)

        self.assertEqual("tail_rebalancing", caught.exception.details["reason"])
        self.assertEqual(calls + 1, self.harness.admin.calls, "one refresh, then the answer")

    async def test_an_owner_that_cannot_be_reached_answers_503(self) -> None:
        await self.warm()
        owner_url = self.harness.workers.index(self.owner)
        self.harness.control_plane.unreachable_members.add(self.owner.config.own_url)
        del owner_url

        with self.assertRaises(WebError) as caught:
            await self.ask(self.other)

        self.assertEqual((503, "tail_owner_unreachable"), (caught.exception.status_code, caught.exception.details["reason"]))

    async def test_the_owners_refusal_comes_back_as_it_is(self) -> None:
        with self.assertRaises(WebError) as caught:
            await self.ask(self.other, after={0: 9_999, 1: -1, 2: -1})
        self.assertEqual("tail_not_ready", caught.exception.details["reason"], "the owner's own answer")

    async def test_a_member_that_lost_the_tables_partition_lets_it_go_and_forwards(self) -> None:
        await self.warm()
        self.assertIn("tail:mat-1:orders", self.owner.tables)
        # A rebalance: every partition to the other member.
        owner_index = self.harness.workers.index(self.owner)
        other_index = 1 - owner_index
        self.harness.apps[other_index].partitions |= self.harness.apps[owner_index].partitions
        self.harness.apps[owner_index].partitions = set()
        self.harness.admin.members = [(self.other.config.member_id, self.other.config.own_url, sorted(self.harness.apps[other_index].partitions))]
        for worker in self.harness.workers:
            await worker.refresh_member_map()

        await self.harness.step()

        self.assertNotIn("tail:mat-1:orders", self.owner.tables, "its stale buffer is never served")
        with self.assertRaises(WebError):
            # The new owner has not been ticked yet: it looks the table up at the control plane.
            await self.ask(self.owner)
        await self.harness.settle()
        answer = await self.ask(self.owner)
        self.assertEqual(self.other.config.member_id, answer["headers"]["X-Tail-Member"])
        self.assertEqual(3, len(json.loads(json.dumps(answer["headers"]["X-Tail-Rows"]))) and int(answer["headers"]["X-Tail-Rows"]))


class RoutingSelfCheckTests(unittest.TestCase):
    def test_a_tick_on_another_partition_than_its_key_hashes_to_is_logged_as_an_error(self) -> None:
        harness = TailHarness(members=1)
        message = tick(target())
        right = owner_partition(message.key, harness.partitions)

        with self.assertLogs("midstream.tail_app.worker", level=logging.ERROR) as logged:
            harness.worker.handle(message, partition=(right + 1) % harness.partitions)
        self.assertIn("tail_routing_mismatch", logged.output[0])
        self.assertEqual(1, harness.worker.counters["routing_mismatches"])

        with self.assertNoLogs("midstream.tail_app.worker", level=logging.ERROR):
            harness.worker.handle(message, partition=right)


if __name__ == "__main__":
    unittest.main()
