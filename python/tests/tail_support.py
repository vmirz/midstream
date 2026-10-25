"""Doubles for the tail app's tests: Kafka records, a Kafka cluster in memory and consumers over it,
a control plane answering schemas and sessions, an HTTP client, and helpers to build a table's tail.

Fakes, not mocks, as the rest of the suite (handbook 13 §13.3): `FakeKafka` holds topics of
partitions of records with offsets, beginnings and ends, and `FakeConsumer` reads it as aiokafka's
consumer does - manual assignment, seeks, pauses, positions, high watermarks, `getmany` - with
failures injected per call.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass, field
import json
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from seal_cases import golden_evolution  # noqa: E402

from midstream.lakehouse.lifecycle import schema_to_json  # noqa: E402
from midstream.tail_app.ingest import TailSchema  # noqa: E402

#: 2026-10-18T10:00:00Z in epoch milliseconds.
BASE_MS = 1_760_781_600_000


from midstream.tail_app.consumer import TP  # noqa: E402  - the adapter's own key type


@dataclass(slots=True)
class Record:
    """A Kafka record as aiokafka hands it over: raw key and value bytes, headers as (name, bytes)."""

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]] = field(default_factory=list)
    timestamp_type: int = 0


def json_value(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


#: The golden evolution as JSON, worked out once: every harness needs it, and evolving is slow.
_GOLDEN_PARTS: str | None = None


def golden_schema(schema_id: int | None = None) -> TailSchema:
    """The seal's golden schema (`seal_cases.golden_evolution`) as the tail holds a table's schema.

    Each call builds a fresh `TailSchema` (it holds the drift detector), from parts computed once."""
    global _GOLDEN_PARTS
    if _GOLDEN_PARTS is None:
        evolution = golden_evolution()
        _GOLDEN_PARTS = json.dumps(
            {
                "schema_id": evolution.schema.schema_id,
                "iceberg_schema": schema_to_json(evolution.schema),
                "fork_map": evolution.fork_map.to_dict(),
                "last_column_id": evolution.last_column_id,
            }
        )
    parts = json.loads(_GOLDEN_PARTS)
    return TailSchema(
        schema_id=parts["schema_id"] if schema_id is None else schema_id,
        iceberg_schema=parts["iceberg_schema"],
        fork_map=parts["fork_map"],
        last_column_id=parts["last_column_id"],
    )


class FakeKafka:
    """Topics in memory: each partition a list of records with offsets that may skip (compaction)."""

    def __init__(self) -> None:
        self.topics: dict[str, dict[int, list[Record]]] = {}
        self.beginnings: dict[tuple[str, int], int] = {}
        self.ends: dict[tuple[str, int], int] = {}

    def create(self, topic: str, partitions: int) -> None:
        self.topics.setdefault(topic, {})
        for partition in range(partitions):
            self.topics[topic].setdefault(partition, [])
            self.beginnings.setdefault((topic, partition), 0)
            self.ends.setdefault((topic, partition), 0)

    def delete(self, topic: str) -> None:
        self.topics.pop(topic, None)

    def produce(
        self,
        topic: str,
        partition: int,
        value: Any,
        *,
        key: bytes | str | None = None,
        timestamp: int | None = None,
        headers: Iterable[tuple[str, bytes | None]] = (),
        raw: bool = False,
    ) -> Record:
        """Append a record at the partition's end; a JSON value unless `raw` (bytes or None as given)."""
        records = self.topics[topic][partition]
        offset = self.ends[(topic, partition)]
        encoded_key = key.encode("utf-8") if isinstance(key, str) else key
        payload = value if raw else (None if value is None else json_value(value))
        record = Record(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=BASE_MS + offset * 1000 if timestamp is None else timestamp,
            key=encoded_key,
            value=payload,
            headers=list(headers),
        )
        records.append(record)
        self.ends[(topic, partition)] = offset + 1
        return record

    def skip_offsets(self, topic: str, partition: int, count: int) -> None:
        """Offsets that hold no record: a transaction's markers, or records compaction removed."""
        self.ends[(topic, partition)] += count

    def truncate_before(self, topic: str, partition: int, offset: int) -> None:
        """Retention removed every record below `offset`."""
        self.topics[topic][partition] = [record for record in self.topics[topic][partition] if record.offset >= offset]
        self.beginnings[(topic, partition)] = offset

    def partitions_for(self, topic: str) -> list[int] | None:
        partitions = self.topics.get(topic)
        return None if partitions is None else sorted(partitions)


class FakeConsumer:
    """aiokafka's consumer over a `FakeKafka`, manually assigned, as the tail app uses it."""

    def __init__(self, kafka: FakeKafka, *, fetch_max_wait_ms: int = 200, max_poll_records: int = 2000) -> None:
        self.kafka = kafka
        self.fetch_max_wait_ms = fetch_max_wait_ms
        self.max_poll_records = max_poll_records
        self.assigned: list[TP] = []
        self.positions: dict[TP, int] = {}
        self.paused_tps: set[TP] = set()
        self.highwaters: dict[TP, int] = {}
        self.started = False
        self.stopped = False
        self.assign_calls = 0
        self.getmany_calls = 0
        #: Raised by the next `getmany` calls, one each, while it has any.
        self.getmany_failures: list[BaseException] = []
        #: Partitions whose fetches return nothing, as a partition without a leader does.
        self.stalled: set[TP] = set()
        #: When a fetch response last came for each partition: a count of fetches, as aiokafka's ms.
        self.polls: dict[TP, int] = {}

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def assign(self, partitions: Iterable[Any]) -> None:
        self.assign_calls += 1
        self.assigned = [TP(tp.topic, tp.partition) for tp in partitions]
        # aiokafka forgets every position and pause on assign; the caller seeks again.
        self.positions = {}
        self.paused_tps = set()
        self.highwaters = {}

    def assignment(self) -> set[TP]:
        return set(self.assigned)

    def seek(self, partition: Any, offset: int) -> None:
        tp = TP(partition.topic, partition.partition)
        assert tp in self.assigned, f"{tp} is not assigned"
        self.positions[tp] = offset

    def pause(self, *partitions: Any) -> None:
        self.paused_tps.update(TP(tp.topic, tp.partition) for tp in partitions)

    def resume(self, *partitions: Any) -> None:
        self.paused_tps.difference_update(TP(tp.topic, tp.partition) for tp in partitions)

    def paused(self) -> set[TP]:
        return set(self.paused_tps)

    async def position(self, partition: Any) -> int:
        return self.positions[TP(partition.topic, partition.partition)]

    def highwater(self, partition: Any) -> int | None:
        return self.highwaters.get(TP(partition.topic, partition.partition))

    async def beginning_offsets(self, partitions: Iterable[Any]) -> dict[TP, int]:
        return {TP(tp.topic, tp.partition): self.kafka.beginnings[(tp.topic, tp.partition)] for tp in partitions}

    async def end_offsets(self, partitions: Iterable[Any]) -> dict[TP, int]:
        return {TP(tp.topic, tp.partition): self.kafka.ends[(tp.topic, tp.partition)] for tp in partitions}

    async def partitions_for(self, topic: str) -> list[int] | None:
        return self.kafka.partitions_for(topic)

    async def getmany(self, timeout_ms: int = 0, max_records: int | None = None) -> dict[TP, list[Record]]:
        self.getmany_calls += 1
        if self.getmany_failures:
            raise self.getmany_failures.pop(0)
        limit = max_records or self.max_poll_records
        result: dict[TP, list[Record]] = {}
        for tp in self.assigned:
            if tp in self.paused_tps or tp not in self.positions or tp in self.stalled:
                continue
            records = self.kafka.topics.get(tp.topic, {}).get(tp.partition)
            if records is None:
                continue
            beginning = self.kafka.beginnings[(tp.topic, tp.partition)]
            end = self.kafka.ends[(tp.topic, tp.partition)]
            if self.positions[tp] < beginning:
                # auto_offset_reset="earliest": below the log start, read from it.
                self.positions[tp] = beginning
            batch = [record for record in records if record.offset >= self.positions[tp]][:limit]
            self.highwaters[tp] = end
            self.polls[tp] = self.getmany_calls
            if batch:
                result[tp] = batch
                # Past the last record returned, or past offsets holding none up to the end.
                self.positions[tp] = batch[-1].offset + 1 if len(batch) == limit else end
                limit -= len(batch)
            else:
                self.positions[tp] = max(self.positions[tp], end)
            if limit <= 0:
                break
        if not result:
            await asyncio.sleep(0)
        return result


# -- the rest of the member's world ---------------------------------------------------------------

import struct  # noqa: E402
from types import SimpleNamespace  # noqa: E402
from urllib.parse import parse_qs, unquote, urlparse  # noqa: E402

from midstream.identity.permissions import SERVICE_PERMISSIONS, expand_permissions  # noqa: E402
from midstream.identity.principals import Grants, Principal  # noqa: E402
from midstream.models import LiveTailHints, LiveTailMessage, LiveTailTarget, live_tail_routing_key  # noqa: E402
from midstream.tail_app.config import TailAppConfig  # noqa: E402
from midstream.tail_app.http import HttpResponse, HttpUnavailable  # noqa: E402
from midstream.tail_app.routing import owner_partition  # noqa: E402
from midstream.tail_app.worker import TailWorker  # noqa: E402

CONSOLE_TOKEN = "console-service-token-0123456789abcdef0123456789"
CONTROL_PLANE_URL = "http://control-plane:6065"
TICK_TOPIC = "materializer-tail-requests"
DATASET_ID = "ds-000000000001"
OTHER_DATASET_ID = "ds-000000000002"


def _head(self, tp):  # FakeConsumer.head: the high watermark, as read_committed would see a non-transactional topic
    return self.highwaters.get(TP(tp.topic, tp.partition))


def _set_fetch_wait(self, ms):
    self.fetch_max_wait_ms = ms


def _last_poll(self, tp):
    return self.polls.get(TP(tp.topic, tp.partition))


async def _topics_partitions(self, topics):
    return {topic: self.kafka.partitions_for(topic) for topic in topics}


FakeConsumer.head = _head
FakeConsumer.set_fetch_wait = _set_fetch_wait
FakeConsumer.last_poll = _last_poll
FakeConsumer.topics_partitions = _topics_partitions


class FakeClients:
    """The member's Kafka clients, every consumer over the same `FakeKafka`."""

    def __init__(self, kafka: FakeKafka, admin: "FakeAdmin | None" = None) -> None:
        self.kafka = kafka
        self.admin_client = admin or FakeAdmin()
        self.made: list[FakeConsumer] = []
        self.sources: list[FakeConsumer] = []
        self.fail_peek = False

    def source(self, *, fetch_wait_ms: int) -> FakeConsumer:
        consumer = FakeConsumer(self.kafka, fetch_max_wait_ms=fetch_wait_ms)
        self.made.append(consumer)
        self.sources.append(consumer)
        return consumer

    def events(self) -> FakeConsumer:
        consumer = FakeConsumer(self.kafka)
        self.made.append(consumer)
        return consumer

    def peek(self) -> FakeConsumer:
        if self.fail_peek:
            raise RuntimeError("the peek consumer cannot start")
        consumer = FakeConsumer(self.kafka)
        self.made.append(consumer)
        return consumer

    def metadata(self) -> FakeConsumer:
        consumer = FakeConsumer(self.kafka)
        self.made.append(consumer)
        return consumer

    def admin(self) -> "FakeAdmin":
        return self.admin_client


def encode_member_metadata(topics: list[str], user_data: bytes | None, version: int = 4) -> bytes:
    """A consumer group member's subscription, as the consumer protocol encodes it."""
    parts = [struct.pack(">h", version), struct.pack(">i", len(topics))]
    for topic in topics:
        encoded = topic.encode()
        parts += [struct.pack(">h", len(encoded)), encoded]
    parts.append(struct.pack(">i", -1) if user_data is None else struct.pack(">i", len(user_data)) + user_data)
    return b"".join(parts)


def encode_member_assignment(assignment: list[tuple[str, list[int]]], user_data: bytes | None = b"", version: int = 4) -> bytes:
    parts = [struct.pack(">h", version), struct.pack(">i", len(assignment))]
    for topic, partitions in assignment:
        encoded = topic.encode()
        parts += [struct.pack(">h", len(encoded)), encoded, struct.pack(">i", len(partitions))]
        parts += [struct.pack(">i", partition) for partition in partitions]
    parts.append(struct.pack(">i", -1) if user_data is None else struct.pack(">i", len(user_data)) + user_data)
    return b"".join(parts)


class FakeAdmin:
    """The admin client's `describe_consumer_group_async` over members set by the test:
    (name, url, tick partitions); a member without a URL joined with metadata that does not read."""

    def __init__(self) -> None:
        self.members: list[tuple[str, str | None, list[int]]] = []
        self.group_id = "materializer-tail-app"
        self.state = "Stable"
        self.calls = 0
        self.fail = False

    async def describe_consumer_group_async(self, group_id: str):
        from midstream.kafka.admin import ConsumerGroupDescription, GroupMemberDescription

        self.calls += 1
        if self.fail:
            raise RuntimeError("the coordinator is not available")
        members = []
        for name, url, partitions in self.members:
            user = json.dumps({"url": url, "assignment": {}}).encode() if url else b"not json"
            members.append(
                GroupMemberDescription(
                    member_id=f"{name}-a22d4404-9a28-4818-893a-9e55fbe3a01b",
                    client_id="faust-1.17.23",
                    client_host="/10.0.0.1",
                    metadata=encode_member_metadata([TICK_TOPIC], user),
                    assignment=encode_member_assignment([(TICK_TOPIC, partitions)]),
                )
            )
        return ConsumerGroupDescription(group_id=group_id, state=self.state, protocol_type="consumer", protocol="faust", members=tuple(members))


@dataclass(slots=True)
class FakeTP:
    topic: str
    partition: int


class FakeApp:
    """The Faust app as far as the worker asks it: its consumer's assignment of the tick topic."""

    def __init__(self, partitions: Iterable[int] = ()) -> None:
        self.partitions = set(partitions)
        self.should_stop = False
        self.consumer = SimpleNamespace(assignment=lambda: {FakeTP(TICK_TOPIC, partition) for partition in self.partitions})


def principal(kind: str = "user", *, permissions: Iterable[str] = (), datasets: dict[str, Iterable[str]] | None = None, limit: Iterable[str] | None = None, password_change: bool = False, email: str = "rita@example.com") -> Principal:
    granted = expand_permissions(list(permissions)) if kind != "service" else frozenset(SERVICE_PERMISSIONS)
    return Principal(
        kind=kind,
        grants=Grants(
            group_ids=("guest", "all-users") if kind == "user" else (),
            role_ids=(),
            permissions=frozenset(granted),
            datasets={dataset: frozenset(expand_permissions(list(perms))) for dataset, perms in (datasets or {}).items()},
            dataset_limit=None if limit is None else frozenset(limit),
        ),
        user_id="usr-000000000001" if kind in ("user", "token") else None,
        email=email if kind in ("user", "token") else None,
        password_change_required=password_change,
    )


def reader_on(dataset_id: str = DATASET_ID) -> Principal:
    """A person who may query one dataset, and view materializations."""
    return principal(permissions=["materializations.view_details"], datasets={dataset_id: ["data.query"]})


class FakeControlPlane:
    """The control plane over HTTP, as the tail app reaches it: sessions, schemas, tables. Also
    every member of the app, by URL, so a forward reaches the owner's views in-process."""

    def __init__(self) -> None:
        self.sessions: dict[str | None, Principal] = {None: principal("guest", permissions=["materializations.view"])}
        self.session_errors: dict[str, str] = {}
        self.schemas: dict[tuple[str, str], dict] = {}
        self.materializations: dict[str, dict] = {}
        self.tables: dict[tuple[str, str], dict] = {}
        self.calls: list[str] = []
        self.down = False
        self.schema_down = False
        self.members: dict[str, Any] = {}
        self.forwarded: list[tuple[str, dict[str, str]]] = []
        self.unreachable_members: set[str] = set()

    def token(self, name: str, who: Principal) -> str:
        self.sessions[name] = who
        return name

    async def get(self, url: str, *, headers: dict[str, str], timeout: float) -> HttpResponse:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base != CONTROL_PLANE_URL:
            return await self._member(base, parsed, headers)
        self.calls.append(parsed.path)
        if self.down:
            raise HttpUnavailable("connection refused")
        path = parsed.path
        if path == "/auth/session":
            token = (headers.get("Authorization") or "")[len("Bearer "):] or None
            if token in self.session_errors:
                guest = self.sessions[None]
                return _json_response(200, {"authenticated": False, "principal": guest.describe(), "session_error": self.session_errors[token]})
            who = self.sessions.get(token)
            if who is None:
                guest = self.sessions[None]
                return _json_response(200, {"authenticated": False, "principal": guest.describe(), "session_error": "invalid_session"})
            return _json_response(200, {"authenticated": who.kind == "user", "principal": who.describe()})
        if (headers.get("Authorization") or "") != f"Bearer {CONSOLE_TOKEN}":
            return _json_response(401, {"error": {"code": "unauthenticated", "message": "no", "details": {}}})
        parts = [unquote(part) for part in path.split("/") if part]
        if len(parts) == 5 and parts[0] == "materializations" and parts[2] == "tables" and parts[4] == "schema":
            if self.schema_down:
                raise HttpUnavailable("timed out")
            schema = self.schemas.get((parts[1], parts[3]))
            return _json_response(200, schema) if schema is not None else _json_response(404, {"error": {"code": "not_found", "message": "no", "details": {}}})
        if len(parts) == 2 and parts[0] == "materializations":
            found = self.materializations.get(parts[1])
            return _json_response(200, found) if found is not None else _json_response(404, {"error": {"code": "not_found", "message": "no", "details": {}}})
        if len(parts) == 4 and parts[0] == "materializations" and parts[2] == "tables":
            found = self.tables.get((parts[1], parts[3]))
            return _json_response(200, found) if found is not None else _json_response(404, {"error": {"code": "not_found", "message": "no", "details": {}}})
        return _json_response(404, {"error": {"code": "not_found", "message": "no route", "details": {}}})

    async def _member(self, base: str, parsed: Any, headers: dict[str, str]) -> HttpResponse:
        from midstream.tail_app import handlers
        from midstream.tail_app.views import tail_head_view, tail_health_view, tail_rows_view
        from midstream.web.errors import WebError

        if base in self.unreachable_members or base not in self.members:
            raise HttpUnavailable(f"{base}: connection refused")
        worker = self.members[base]
        self.forwarded.append((parsed.path, dict(headers)))
        request = FakeRequest(headers=headers, query={key: values[0] for key, values in parse_qs(parsed.query).items()}, path_qs=parsed.path + (f"?{parsed.query}" if parsed.query else ""))
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        try:
            if parts[-1] == "health":
                payload = await tail_health_view(request, worker=worker)
                return _json_response(200, payload)
            if parts[-1] == "head":
                answer = await tail_head_view(request, parts[3], parts[5], worker=worker)
            else:
                answer = await tail_rows_view(request, parts[3], parts[5], worker=worker)
        except WebError as error:
            return HttpResponse(status=error.status_code, headers={"Content-Type": "application/json"}, body=json.dumps(handlers.error_body({"cause_response": error.to_response()})).encode())
        return HttpResponse(status=answer["status"], headers={**answer["headers"], "Content-Type": answer["content_type"]}, body=answer["body"])


def _json_response(status: int, payload: Any) -> HttpResponse:
    return HttpResponse(status=status, headers={"Content-Type": "application/json"}, body=json.dumps(payload).encode())


@dataclass(slots=True)
class FakeRequest:
    headers: dict[str, str] = field(default_factory=dict)
    query: dict[str, str] = field(default_factory=dict)
    path_qs: str = "/"
    remote: str = "10.0.0.9"


def target(materialization_id: str = "mat-1", table: str = "orders", *, topic: str = "orders", dataset_id: str = DATASET_ID, table_uuid: str = "uuid-1", skip_tombstones: bool = False) -> LiveTailTarget:
    return LiveTailTarget(
        materialization_id=materialization_id,
        dataset="sales" if dataset_id == DATASET_ID else "finance",
        dataset_id=dataset_id,
        table=table,
        location=f"gs://lake/materializer/{materialization_id}/{table}",
        source_topic=topic,
        partition_granularity="day",
        table_uuid=table_uuid,
        skip_tombstones=skip_tombstones,
    )


def tick(tail_target: LiveTailTarget, *, offsets: dict[int, int] | None = None, schema_id: int | None = 3, issued_at: str = "2026-10-23T12:00:00Z", **settings: Any) -> LiveTailMessage:
    values = {
        "live_tail_max_rows_per_table": 250_000,
        "live_tail_max_bytes_per_table": 256 * 1024 * 1024,
        "live_tail_retention_seconds": 120,
        "live_tail_fetch_wait_ms": 200,
        "live_tail_max_records_per_second_per_table": 5_000,
        "live_tail_peek_max_rows": 1_000,
        "live_tail_idle_seconds": 600,
        "protocol_logging_enabled": False,
    }
    values.update({f"live_tail_{name}" if not name.startswith(("live_tail_", "protocol_")) else name: value for name, value in settings.items()})
    return LiveTailMessage(
        kind="tail_tick",
        target=tail_target,
        issued_at=issued_at,
        settings=values,
        hints=LiveTailHints(offsets={str(partition): offset for partition, offset in (offsets or {}).items()}, schema_id=schema_id, metadata_location=None),
    )


def cancel(tail_target: LiveTailTarget, reason: str = "table_dropping") -> LiveTailMessage:
    return LiveTailMessage(kind="tail_cancel", target=tail_target, issued_at="2026-10-23T12:00:00Z", reason=reason)


def schema_payload(schema: TailSchema, materialization_id: str = "mat-1", table: str = "orders") -> dict:
    return {
        "materialization_id": materialization_id,
        "table": table,
        "schema_id": schema.schema_id,
        "iceberg_schema": schema.iceberg_schema,
        "fork_map": schema.fork_map.to_dict(),
        "last_column_id": None,
        "rejected_paths": [list(path) for path in schema.rejected_paths],
    }


class Clock:
    """A monotonic clock the test moves."""

    def __init__(self, now: float = 10_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TailHarness:
    """The tail app's members over one Kafka and one control plane, driven step by step."""

    def __init__(self, *, members: int = 1, partitions: int = 64) -> None:
        self.kafka = FakeKafka()
        self.kafka.create(TICK_TOPIC, partitions)
        self.kafka.create("materialization-table-events", 3)
        self.control_plane = FakeControlPlane()
        self.clock = Clock()
        self.admin = FakeAdmin()
        self.admin.group_id = "materializer-tail-app"
        self.workers: list[TailWorker] = []
        self.apps: list[FakeApp] = []
        self.clients: list[FakeClients] = []
        self.partitions = partitions
        for index in range(members):
            name = f"materializer-tail-app-{index}"
            url = f"http://{name}.materializer-tail-app.midstream.svc.cluster.local:6065"
            clients = FakeClients(self.kafka, self.admin)
            worker = TailWorker(
                config=TailAppConfig(
                    control_plane_url=CONTROL_PLANE_URL,
                    member_id=name,
                    group_id="materializer-tail-app",
                    web_host=f"{name}.materializer-tail-app.midstream.svc.cluster.local",
                    web_port=6065,
                    requests_topic=TICK_TOPIC,
                ),
                console_token=CONSOLE_TOKEN,
                clients=clients,
                http=self.control_plane,
                clock=self.clock,
                background=False,
            )
            worker.tick_partitions = partitions
            worker.idle_sleep_seconds = 0
            app = FakeApp()
            worker.app = app
            self.workers.append(worker)
            self.apps.append(app)
            self.clients.append(clients)
            self.control_plane.members[url.rsplit(":", 1)[0] + ":6065"] = worker
        self.assign_evenly()
        self.schema = golden_schema()

    @property
    def worker(self) -> TailWorker:
        return self.workers[0]

    def assign_evenly(self) -> None:
        """Every tick partition to a member, round robin, and the group description to match."""
        self.admin.members = []
        count = len(self.workers)
        for index, (worker, app) in enumerate(zip(self.workers, self.apps)):
            app.partitions = {partition for partition in range(self.partitions) if partition % count == index}
            self.admin.members.append((worker.config.member_id, worker.config.own_url, sorted(app.partitions)))

    def owner_of(self, materialization_id: str, table: str) -> TailWorker:
        partition = owner_partition(live_tail_routing_key(materialization_id, table), self.partitions)
        return next(worker for worker, app in zip(self.workers, self.apps) if partition in app.partitions)

    def table(self, tail_target: LiveTailTarget, *, partitions: int = 3, schema: TailSchema | None = None) -> None:
        """A topic for the table, and the control plane's schema and records of it."""
        self.kafka.create(tail_target.source_topic, partitions)
        held = schema or self.schema
        self.control_plane.schemas[(tail_target.materialization_id, tail_target.table)] = schema_payload(held, tail_target.materialization_id, tail_target.table)
        self.control_plane.materializations[tail_target.materialization_id] = {
            "materialization": {
                "materialization_id": tail_target.materialization_id,
                "desired_phase": "running",
                "skip_tombstones": tail_target.skip_tombstones,
            }
        }
        self.control_plane.tables[(tail_target.materialization_id, tail_target.table)] = {
            "materialization_id": tail_target.materialization_id,
            "dataset": tail_target.dataset,
            "dataset_id": tail_target.dataset_id,
            "name": tail_target.table,
            "source_topic": tail_target.source_topic,
            "location": tail_target.location,
            "partition_granularity": tail_target.partition_granularity,
            "table_uuid": tail_target.table_uuid,
            "state": "active",
            "offsets": {},
            "schema_id": held.schema_id,
        }

    def deliver(self, message: LiveTailMessage) -> None:
        """A tick or cancel to the member that owns its table, on the partition its key hashes to."""
        partition = owner_partition(message.key, self.partitions)
        owner = self.owner_of(message.target.materialization_id, message.target.table)
        owner.handle(message, partition=partition)

    def produce(self, topic: str, partition: int, count: int = 1, **fields: Any) -> list[Record]:
        records = []
        for _ in range(count):
            offset = self.kafka.ends[(topic, partition)]
            value = fields.get("value", {"order_id": f"O{partition}-{offset}", "amount": offset})
            records.append(self.kafka.produce(topic, partition, value, key=fields.get("key", f"K{offset}"), timestamp=fields.get("timestamp"), headers=fields.get("headers", ()), raw=fields.get("raw", False)))
        return records

    async def step(self, rounds: int = 1) -> None:
        for _ in range(rounds):
            for worker in self.workers:
                await worker.step()
                await asyncio.sleep(0)
                # Seeds run as tasks: let them finish.
                for table in list(worker.tables.values()):
                    task = table._seed_task
                    if task is not None and not task.done():
                        await task
                worker.apply_plan()

    async def fetch(self, rounds: int = 1) -> None:
        for _ in range(rounds):
            for worker in self.workers:
                await worker.ensure_source()
                await worker.fetch_once()

    async def settle(self, rounds: int = 3) -> None:
        """Steps and fetches until the members have read what Kafka holds."""
        for _ in range(rounds):
            await self.step()
            await self.fetch(2)

    def request(self, *, token: str | None, after: dict[int, int] | None = None, console: bool = True, path: str | None = None, **query: Any) -> FakeRequest:
        headers = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if console:
            headers["X-Materializer-Console"] = CONSOLE_TOKEN
        values = {key: str(value) for key, value in query.items()}
        if after is not None:
            values["after"] = json.dumps({str(partition): offset for partition, offset in after.items()})
        return FakeRequest(headers=headers, query=values, path_qs=path or "/tail/v1/materializations/mat-1/tables/orders?" + "&".join(f"{key}={value}" for key, value in values.items()))

    def table_event(self, kind: str, materialization_id: str, table: str, payload: dict) -> None:
        """A table event to every member, as each reads the topic itself."""
        event = {"kind": kind, "materialization_id": materialization_id, "table": table, "payload": payload}
        for worker in self.workers:
            worker.apply_event(json.dumps(event).encode())
