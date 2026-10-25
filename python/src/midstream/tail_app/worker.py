"""Every table one member of the tail app owns, the member's Kafka consumers, and its requests.

- **Ticks** (`handle`): the `tail-worker` agent hands each message from the control plane here. A
  tick names a table and its settings; a cancel ends its tail at once. The member keeps a
  `TableTail` per table named, `parked` until someone asks for it.
- **Ownership**: the member owns the tables whose tick partitions Kaspr assigned it - the app's own
  assignment of the tick topic, read from Faust. A request for a table in another member's partition
  is forwarded there (`routing.py`); a table whose partition moved away is let go.
- **The step** (`step`, every 5 s from the `tail-step` task): keep the background loops running,
  forget tables no tick named for `forget_after_seconds`, park the idle, seed the activated, read the
  end offsets of backlogged tables, trim committed records past their retention, hold the member's
  memory budget, refresh the member map.
- **The fetch loop**: one consumer for every active table's partitions (`consumer.py`), each fetch
  handed to the tables, then their states re-evaluated and the fetch plan re-applied.
- **The events loop**: the table events the committers and the maintenance app publish, from the
  latest offset at start (`events.py`): commits and schema changes of the tables this member tails.

Nothing is persisted: a restarted member rebuilds everything from the ticks, the control plane, the
table events and Kafka (handbook 19).
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import time
from typing import Any

from midstream.models import LiveTailHints, LiveTailMessage, LiveTailTarget, live_tail_routing_key
from midstream.protocol_logging import log_protocol_event
from midstream.tail_app.auth import SessionChecker
from midstream.tail_app.config import TailAppConfig, TailSettings, default_settings, get_tail_app_config, service_token
from midstream.tail_app.consumer import SOURCE_FETCH_MIN_BYTES, TP, FetchPlan, build_consumer, plan_fetch
from midstream.tail_app.control_plane import ControlPlane, ControlPlaneError
from midstream.tail_app.events import apply_table_event
from midstream.tail_app.http import AiohttpClient, HttpClient
from midstream.tail_app.ingest import TailSchema
from midstream.tail_app.routing import MemberMap, member_map, owner_partition
from midstream.tail_app.table_tail import UNAVAILABLE_RETRY_SECONDS, TableTail

LOGGER = logging.getLogger(__name__)

#: Records one fetch takes at most, and how long the loop sleeps when nothing is assigned.
FETCH_MAX_RECORDS = 2_000
IDLE_SLEEP_SECONDS = 0.2
#: Waits between failures of a background loop.
LOOP_RETRY_SECONDS = (1, 2, 5, 10, 30)
#: A table not ticked yet on a partition this member owns is looked up at the control plane at most
#: this often - just after a start or a rebalance, before the next tick names it.
RESOLVE_RETRY_SECONDS = 10.0
#: Seeds of activated tables at once.
SEED_CONCURRENCY = 8
#: The fallback number of tick partitions, until the topic's own count is read.
DEFAULT_TICK_PARTITIONS = 64
#: How often the topics of active tables are looked up: a deleted one, or one with more partitions.
TOPIC_CHECK_SECONDS = 30.0


class KafkaClients:
    """The member's Kafka clients, made on first use on the app's loop from Kaspr's `K_KAFKA_*`."""

    def source(self, *, fetch_wait_ms: int) -> Any:
        return build_consumer(fetch_wait_ms=fetch_wait_ms, fetch_min_bytes=SOURCE_FETCH_MIN_BYTES)

    def events(self) -> Any:
        return build_consumer(fetch_wait_ms=500, isolation_level="read_uncommitted")

    def peek(self) -> Any:
        return build_consumer(fetch_wait_ms=100)

    def metadata(self) -> Any:
        return build_consumer(fetch_wait_ms=100)

    def admin(self) -> Any:
        from midstream.kafka.admin import AIOKafkaAdminClient

        # On a loop of its own, every call under a deadline (`kafka/admin.py`).
        return AIOKafkaAdminClient.from_environment()


class TailWorker:
    def __init__(
        self,
        *,
        config: TailAppConfig | None = None,
        console_token: str | None = None,
        clients: Any = None,
        control_plane: ControlPlane | None = None,
        http: HttpClient | None = None,
        clock: Any = time.monotonic,
        background: bool = True,
    ) -> None:
        self.config = config or get_tail_app_config()
        #: Run the fetch and events loops as tasks of their own; tests drive them by hand instead.
        self.background = background
        #: How long a fetch with nothing to read sleeps instead.
        self.idle_sleep_seconds = IDLE_SLEEP_SECONDS
        self.console_token = service_token() if console_token is None else (console_token or None)
        self.http = http or AiohttpClient()
        self.control_plane = control_plane or ControlPlane(self.config.control_plane_url, service_token=self.console_token, http=self.http)
        self.sessions = SessionChecker(
            self.control_plane,
            check_seconds=self.config.session_check_seconds,
            grace_seconds=self.config.session_grace_seconds,
            clock=clock,
        )
        self.clients = clients or KafkaClients()
        self.clock = clock
        self.started_at = clock()
        self.tables: dict[str, TableTail] = {}
        #: The settings the latest tick of any table carried: what a table not ticked yet takes.
        self.settings = default_settings()
        #: The tick topic's partitions this member holds, as Faust assigned them; None until known.
        self.owned: set[int] | None = None
        self._owned_changed_at = clock()
        self.tick_partitions: int | None = None
        self.members = MemberMap()
        self.app: Any = None
        self.counters = {"ticks": 0, "cancels": 0, "malformed": 0, "routing_mismatches": 0, "forwarded": 0, "served": 0}
        self._source: Any = None
        self._metadata: Any = None
        self._admin: Any = None
        self._assigned: set[TP] = set()
        self._paused: set[TP] = set()
        self._positions: dict[TP, int] = {}
        self._fetch_task: asyncio.Task | None = None
        self._events_task: asyncio.Task | None = None
        self._loop_failures = {"fetch": 0, "events": 0}
        self._serve_pool = ThreadPoolExecutor(max_workers=self.config.serve_concurrency, thread_name_prefix="tail-serve")
        self._serve_slots: asyncio.Semaphore | None = None
        self._seed_slots: asyncio.Semaphore | None = None
        self._resolved_at: dict[str, float] = {}
        self._member_map_at = 0.0
        self._topics_checked_at = clock()
        self._stopping = False
        self.peek: Any = None
        self._fetch_wait_ms = self.settings.fetch_wait_ms

    # -- ticks -------------------------------------------------------------------------------------

    def handle(self, message: LiveTailMessage, *, partition: int | None = None) -> None:
        """Take a tick or a cancel. `partition` is the tick topic's partition it arrived on."""
        key = message.key
        if partition is not None:
            self._check_routing(key, partition)
        if message.kind == "tail_cancel":
            self.counters["cancels"] += 1
            table = self.tables.pop(key, None)
            if table is not None:
                table.reset("parked")
                self._log("tail_table_forgotten", table=key, reason=message.reason)
                self.apply_plan()
            return
        self.counters["ticks"] += 1
        settings = TailSettings.from_tick(message.settings, previous=self.settings)
        self.settings = settings
        table = self.tables.get(key)
        if table is None:
            self.tables[key] = TableTail(message.target, settings, hints=message.hints, member_id=self.config.member_id, clock=self.clock)
            return
        reseeded = table.tick(message.target, settings, message.hints)
        if reseeded:
            self._log("tail_table_reseeded", table=key, reason="table_uuid", table_uuid=message.target.table_uuid)
            self.apply_plan()
        elif table.active and table.schema is not None and (table.hinted_schema_id or 0) > table.schema.schema_id:
            # A schema change this member missed: fetch the schema again.
            asyncio.ensure_future(self._refresh_schema(table))

    def _check_routing(self, key: str, partition: int) -> None:
        partitions = self.tick_partitions or DEFAULT_TICK_PARTITIONS
        expected = owner_partition(key, partitions)
        if expected != partition:
            self.counters["routing_mismatches"] += 1
            LOGGER.error(
                "tail_routing_mismatch: %s arrived on partition %s of %s but hashes to %s; requests for it will be forwarded to the wrong member",
                key,
                partition,
                partitions,
                expected,
            )

    # -- the step ----------------------------------------------------------------------------------

    async def step(self, *, app: Any = None) -> None:
        if app is not None:
            self.app = app
        if self._stopping or bool(getattr(app, "should_stop", False)):
            await self.stop()
            return
        self._refresh_owned()
        await self._ensure_loops()
        now = self.clock()
        for key, table in list(self.tables.items()):
            if now - table.last_ticked >= self.config.forget_after_seconds:
                del self.tables[key]
                table.reset("parked")
                self._log("tail_table_forgotten", table=key, reason="no_longer_ticked")
            elif table.idle():
                table.reset("parked")
                self._log("tail_table_parked", table=key)
        self._release_moved()
        for table in self.tables.values():
            if table.needs_seed():
                table.start_seed(self._seed(table))
        await self._refresh_ends()
        if now - self._topics_checked_at >= TOPIC_CHECK_SECONDS:
            await self._check_topics()
        for table in self.tables.values():
            if table.active:
                table.close_due()
                table.buffer.trim(table.settings.retention_seconds)
            table.evaluate()
        self._hold_budget()
        self.apply_plan()
        if now - self._member_map_at >= self.config.member_map_refresh_seconds:
            await self.refresh_member_map()

    async def stop(self) -> None:
        self._stopping = True
        for task in (self._fetch_task, self._events_task):
            if task is not None and not task.done():
                task.cancel()
        for table in self.tables.values():
            table.reset("parked")
        for client in (self._source, self._metadata):
            if client is not None:
                try:
                    await client.stop()
                except Exception:  # noqa: BLE001 - stopping
                    pass
        self._source = self._metadata = None

    def _refresh_owned(self) -> None:
        owned = assigned_partitions(self.app, self.config.requests_topic)
        if owned is None:
            return
        if owned != self.owned:
            gained = sorted(owned - (self.owned or set()))
            lost = sorted((self.owned or set()) - owned)
            if self.owned is not None:
                self._log("tail_rebalanced", gained=gained, lost=lost)
            self.owned = owned
            self._owned_changed_at = self.clock()

    def owns(self, key: str) -> bool | None:
        """Whether this member owns `key`'s table: by the app's own assignment of the tick topic, read
        now, else by the member map; None while neither says."""
        self._refresh_owned()
        partition = owner_partition(key, self.tick_partitions or DEFAULT_TICK_PARTITIONS)
        if self.owned is not None:
            return partition in self.owned
        member = self.members.owner(partition)
        if member is None:
            return None
        return member.name == self.config.member_id

    def _release_moved(self) -> None:
        """Let go of the tables whose tick partition another member holds now."""
        if self.owned is None:
            return
        released = False
        for key, table in list(self.tables.items()):
            if self.owns(key) is False:
                del self.tables[key]
                table.reset("parked")
                released = True
                self._log("tail_table_forgotten", table=key, reason="partition_moved")
        if released:
            self.apply_plan()

    # -- seeding ----------------------------------------------------------------------------------

    def activate(self, table: TableTail) -> None:
        """A request for a parked table - or one whose topic could not be read - seeds it now; the
        request waits for it."""
        table.ask()
        if table.state == "unavailable" and self.clock() - (table.unavailable_at or 0.0) < UNAVAILABLE_RETRY_SECONDS:
            return
        if table.state in ("parked", "unavailable"):
            table.state = "seeding"
            table._signal()
        if table.needs_seed():
            table.start_seed(self._seed(table))

    async def _seed(self, table: TableTail) -> None:
        slots = self._seed_slots or asyncio.Semaphore(SEED_CONCURRENCY)
        self._seed_slots = slots
        async with slots:
            try:
                plan = await self._seed_plan(table)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - every failure is retried
                table.seed_failed(error)
                return
            if plan is None:
                return
            if table.state != "seeding" or self.tables.get(table.key) is not table:
                return
            schema, source, partitions, beginnings, ends, committed = plan
            table.seeded(schema=schema, source=source, partitions=partitions, beginnings=beginnings, ends=ends, committed=committed)
            table.evaluate()
            self.apply_plan()

    async def _seed_plan(self, table: TableTail) -> tuple | None:
        target = table.target
        source = "control_plane"
        committed: dict[int, int] = {}
        try:
            payload = await self.control_plane.schema(target.materialization_id, target.table)
            schema = schema_from_payload(payload)
        except (ControlPlaneError, KeyError, TypeError, ValueError) as error:
            LOGGER.warning("Could not fetch the schema of %s from the control plane (%s); reading the table instead", table.key, error)
            schema, committed, table_uuid = await self._schema_from_table(target)
            source = "gcs"
            if table_uuid and target.table_uuid and table_uuid != target.table_uuid:
                raise RuntimeError(f"the table at {target.location} is another incarnation ({table_uuid}) than ticked ({target.table_uuid})")
        metadata = await self._metadata_consumer()
        partitions = await metadata.partitions_for(target.source_topic)
        if partitions is None:
            table.reset("unavailable", reason="topic_missing")
            table.last_error = f"topic {target.source_topic} does not exist"
            return None
        tps = [TP(target.source_topic, partition) for partition in partitions]
        beginnings = {tp.partition: offset for tp, offset in (await metadata.beginning_offsets(tps)).items()}
        ends = {tp.partition: offset for tp, offset in (await metadata.end_offsets(tps)).items()}
        return schema, source, partitions, beginnings, ends, committed

    async def _schema_from_table(self, target: LiveTailTarget) -> tuple[TailSchema, dict[int, int], str | None]:
        """The fallback seed: the table itself, through its pointer, as a runtime member reads it."""
        from midstream.lakehouse.offsets import committed_offsets
        from midstream.runtime_app.table_state import SchemaRecord, TableContext, load_table

        context = TableContext(
            materialization_id=target.materialization_id,
            location=target.location,
            namespace=target.dataset,
            name=target.table,
            partition_granularity=target.partition_granularity,
            source_topic=target.source_topic,
            table_uuid=target.table_uuid,
        )
        loop = asyncio.get_running_loop()
        loaded = await loop.run_in_executor(None, load_table, context)
        record = SchemaRecord.from_table(loaded)
        committed = {int(partition): int(offset) for partition, offset in committed_offsets(loaded.metadata).offsets.items()}
        schema = TailSchema(
            schema_id=record.schema_id,
            iceberg_schema=record.schema,
            fork_map=record.fork_map,
            last_column_id=record.last_column_id,
        )
        return schema, committed, record.table_uuid

    async def _refresh_schema(self, table: TableTail) -> None:
        try:
            payload = await self.control_plane.schema(table.target.materialization_id, table.target.table)
            if table.install_schema(schema_from_payload(payload)):
                self._log("tail_schema_applied", table=table.key, schema_id=table.schema.schema_id if table.schema else None, source="refresh")
        except Exception as error:  # noqa: BLE001 - the next tick tries again
            LOGGER.warning("Could not fetch the newer schema of %s: %s", table.key, error)

    async def _refresh_ends(self) -> None:
        """Read the end offsets of every table that is not fetching - backlogged, or paused at a bound
        while its topic keeps growing - in one request for them all, so a backlog is seen to end and
        a burst past twice the bound is seen to become one."""
        backlogged = [table for table in self.tables.values() if table.state in ("backlogged", "paused")]
        if not backlogged:
            return
        try:
            metadata = await self._metadata_consumer()
            tps = [tp for table in backlogged for tp in table.tps()]
            ends = await metadata.end_offsets(tps)
        except Exception as error:  # noqa: BLE001 - the next step tries again
            LOGGER.warning("Could not read the end offsets of backlogged tables: %s", error)
            return
        for table in backlogged:
            table.ends({tp.partition: offset for tp, offset in ends.items() if tp.topic == table.target.source_topic})

    async def _check_topics(self) -> None:
        """Every active table's topic, from one metadata read: a topic gone makes its table
        unavailable, which a later request seeds again; partitions it gained are read from their
        beginning."""
        self._topics_checked_at = self.clock()
        watched = [table for table in self.tables.values() if table.active]
        if not watched:
            return
        try:
            metadata = await self._metadata_consumer()
            partitions = await metadata.topics_partitions(sorted({table.target.source_topic for table in watched}))
        except Exception as error:  # noqa: BLE001 - the next check tries again
            LOGGER.warning("Could not read the tailed topics' metadata: %s", error)
            return
        changed = False
        for table in watched:
            found = partitions.get(table.target.source_topic)
            if found is None:
                LOGGER.warning("The topic of %s, %s, is gone: its tail is unavailable", table.key, table.target.source_topic)
                table.reset("unavailable", reason="topic_missing")
                table.last_error = f"topic {table.target.source_topic} does not exist"
                changed = True
            elif table.add_partitions(found):
                changed = True
        if changed:
            self.apply_plan()

    async def reset_source(self) -> None:
        """Let the source consumer go - after a failure - so the next fetch starts a new one, every
        partition sought again to where its tables are."""
        source, self._source = self._source, None
        self._assigned, self._paused, self._positions = set(), set(), {}
        if source is not None:
            try:
                await source.stop()
            except Exception:  # noqa: BLE001 - discarded either way
                pass

    async def _metadata_consumer(self) -> Any:
        if self._metadata is None:
            consumer = self.clients.metadata()
            await consumer.start()
            self._metadata = consumer
        return self._metadata

    # -- memory ------------------------------------------------------------------------------------

    def resident_bytes(self) -> int:
        return sum(table.buffer.resident_bytes() for table in self.tables.values())

    def _hold_budget(self) -> None:
        """Past the budget, drop what the tables already hold, then pause the largest; resume at 75 %."""
        budget = self.config.memory_budget_bytes
        total = self.resident_bytes()
        if total > budget:
            for table in self.tables.values():
                table.buffer.drop_retained()
            total = self.resident_bytes()
        if total > budget:
            largest = max((table for table in self.tables.values() if table.active and not table.budget_paused), key=lambda table: table.buffer.resident_bytes(), default=None)
            if largest is not None:
                largest.budget_paused = True
                largest.evaluate()
                LOGGER.warning("The tail's memory budget (%s bytes) is reached: pausing %s", budget, largest.key)
        elif total < 0.75 * budget:
            for table in self.tables.values():
                if table.budget_paused:
                    table.budget_paused = False
                    table.evaluate()

    # -- the fetch plan ----------------------------------------------------------------------------

    def apply_plan(self) -> FetchPlan | None:
        """Assign, seek, pause and resume the source consumer as the tables' needs say."""
        if self._source is None:
            return None
        plan = plan_fetch(need for table in self.tables.values() for need in table.needs())
        source = self._source
        if plan.assignment != self._assigned:
            source.assign(sorted(plan.assignment))
            self._assigned = set(plan.assignment)
            self._positions = {}
            self._paused = set()
            for tp in plan.assignment:
                position = plan.positions.get(tp)
                if position is None:
                    position = min((need.next_offset for table in self.tables.values() for need in table.needs() if need.tp == tp), default=0)
                source.seek(tp, position)
                self._positions[tp] = position
        else:
            for tp, position in plan.positions.items():
                current = self._positions.get(tp)
                if current is None or current > position:
                    # A table that resumed behind the partition's position: read it again for it.
                    source.seek(tp, position)
                    self._positions[tp] = position
        pause = plan.paused - self._paused
        resume = self._paused - plan.paused
        if pause:
            source.pause(*sorted(pause))
        if resume:
            source.resume(*sorted(resume))
        self._paused = set(plan.paused)
        return plan

    async def _ensure_loops(self) -> None:
        if not self.background:
            await self.ensure_source()
            return
        if self._fetch_task is None or self._fetch_task.done():
            self._fetch_task = asyncio.ensure_future(self._fetch_loop())
        if self._events_task is None or self._events_task.done():
            self._events_task = asyncio.ensure_future(self._events_loop())

    async def ensure_source(self) -> Any:
        """The member's source consumer, started and assigned as the plan says."""
        if self._source is None:
            source = self.clients.source(fetch_wait_ms=self.settings.fetch_wait_ms)
            await source.start()
            self._source = source
            self._fetch_wait_ms = self.settings.fetch_wait_ms
            self._assigned, self._paused, self._positions = set(), set(), {}
            self.apply_plan()
        return self._source

    async def _fetch_loop(self) -> None:
        while not self._stopping:
            try:
                await self.ensure_source()
                await self.fetch_once()
                self._loop_failures["fetch"] = 0
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - the loop goes on after a pause
                await self._loop_failed("fetch", error)

    async def fetch_once(self) -> int:
        """One fetch, handed to the tables; return how many records it brought.

        A table takes a partition's records only when the fetch started at or below its own next
        offset - else it would skip the records between, which another table sharing the partition
        already had: the plan seeks back for it instead. And only a table that took every record of
        the fetch moves its coverage to the consumer's position, past offsets that hold none.
        """
        source = self._source
        if source is None:
            return 0
        if self.settings.fetch_wait_ms != self._fetch_wait_ms:
            source.set_fetch_wait(self.settings.fetch_wait_ms)
            self._fetch_wait_ms = self.settings.fetch_wait_ms
        if not self._assigned or self._assigned == self._paused:
            await asyncio.sleep(self.idle_sleep_seconds)
            self.apply_plan()
            return 0
        starts = dict(self._positions)
        before: dict[tuple[str, int], int] = {}
        for table in self.tables.values():
            if table.active:
                for partition, buffer in table.buffer.partitions.items():
                    before[(table.key, partition)] = buffer.next_offset
        fetched_batch = await source.getmany(timeout_ms=self.settings.fetch_wait_ms, max_records=FETCH_MAX_RECORDS)
        # Keyed by this module's TP whatever the consumer keys by: a key that did not match would
        # read as a partition that brought nothing, and move its coverage past records never taken.
        batch = {TP(tp.topic, int(tp.partition)): records for tp, records in fetched_batch.items()}
        by_topic: dict[str, list[TableTail]] = {}
        for table in self.tables.values():
            if table.active:
                by_topic.setdefault(table.target.source_topic, []).append(table)
        records = 0
        took: set[tuple[str, int]] = set()
        for tp, fetched in batch.items():
            records += len(fetched)
            for table in by_topic.get(tp.topic, []):
                if not self._may_take(table, tp, starts, before):
                    continue
                if await table.ingest(tp.partition, fetched):
                    took.add((table.key, tp.partition))
        for tp in list(self._assigned):
            if tp in self._paused:
                continue
            try:
                position = await source.position(tp)
            except Exception:  # noqa: BLE001 - a partition being reassigned
                continue
            self._positions[tp] = position
            head = source.head(tp)
            polled = source.last_poll(tp)
            for table in by_topic.get(tp.topic, []):
                if tp.partition not in table.buffer.partitions:
                    continue
                complete = self._may_take(table, tp, starts, before) and (tp not in batch or (table.key, tp.partition) in took)
                table.fetched(tp.partition, position=position if complete and table.wanting() else None, head=head, last_poll=polled)
        for tables in by_topic.values():
            for table in tables:
                table.evaluate()
        self.apply_plan()
        return records

    @staticmethod
    def _may_take(table: TableTail, tp: TP, starts: dict[TP, int], before: dict[tuple[str, int], int]) -> bool:
        start = starts.get(tp)
        next_offset = before.get((table.key, tp.partition))
        return start is not None and next_offset is not None and start <= next_offset

    async def _events_loop(self) -> None:
        consumer = None
        while not self._stopping:
            try:
                if consumer is None:
                    consumer = self.clients.events()
                    await consumer.start()
                    partitions = await consumer.partitions_for(self.config.table_events_topic) or []
                    tps = [TP(self.config.table_events_topic, partition) for partition in partitions]
                    consumer.assign(tps)
                    ends = await consumer.end_offsets(tps)
                    for tp in tps:
                        # From now on: the seed covers anything earlier.
                        consumer.seek(tp, ends.get(tp, 0))
                batch = await consumer.getmany(timeout_ms=1000, max_records=500)
                for records in batch.values():
                    for record in records:
                        self.apply_event(record.value)
                self._loop_failures["events"] = 0
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - the loop goes on after a pause
                consumer = None
                await self._loop_failed("events", error)

    def apply_event(self, raw: Any) -> None:
        """One table event: a commit or a schema change of a table this member tails."""
        try:
            event = json.loads(raw) if isinstance(raw, (bytes, bytearray, str)) else raw
        except (TypeError, ValueError):
            LOGGER.warning("Ignoring a table event that is not JSON")
            return
        if not isinstance(event, dict):
            return
        key = live_tail_routing_key(str(event.get("materialization_id") or ""), str(event.get("table") or ""))
        table = self.tables.get(key)
        if table is None:
            return
        outcome = apply_table_event(table, event)
        if outcome == "schema":
            self._log("tail_schema_applied", table=key, schema_id=table.schema.schema_id if table.schema else None, source="event")
        if outcome is not None:
            table.evaluate()
            self.apply_plan()

    async def _loop_failed(self, name: str, error: BaseException) -> None:
        failures = self._loop_failures[name]
        delay = LOOP_RETRY_SECONDS[min(failures, len(LOOP_RETRY_SECONDS) - 1)]
        self._loop_failures[name] = failures + 1
        LOGGER.warning("The tail's %s loop failed (%s); trying again in %ss", name, error, delay)
        if name == "fetch":
            await self.reset_source()
        await asyncio.sleep(delay)

    # -- the member map --------------------------------------------------------------------------

    async def refresh_member_map(self) -> MemberMap:
        self._member_map_at = self.clock()
        group = self.config.group_id
        if not group:
            return self.members
        try:
            if self._admin is None:
                self._admin = self.clients.admin()
            description = await asyncio.wait_for(self._admin.describe_consumer_group_async(group), timeout=15)
            self.members = member_map(description, topic=self.config.requests_topic, own_url=self.config.own_url, now=self.clock())
            if self.tick_partitions is None:
                await self._count_tick_partitions()
        except Exception as error:  # noqa: BLE001 - the last map stays
            LOGGER.warning("Could not read the tail app's members: %s", error)
        return self.members

    async def _count_tick_partitions(self) -> None:
        try:
            metadata = await self._metadata_consumer()
            partitions = await metadata.partitions_for(self.config.requests_topic)
            if partitions:
                self.tick_partitions = len(partitions)
        except Exception as error:  # noqa: BLE001 - the default stays
            LOGGER.debug("Could not count the tick topic's partitions: %s", error)

    # -- lookups -----------------------------------------------------------------------------------

    async def resolve(self, materialization_id: str, table: str) -> TableTail | None:
        """The table, from its tick, or - on a partition this member owns but has not been ticked for
        yet - from the control plane: just after a start or a rebalance, before the next tick."""
        key = live_tail_routing_key(materialization_id, table)
        found = self.tables.get(key)
        if found is not None:
            return found
        if self.owns(key) is not True:
            # Only the owner looks a table up: another member would start a second tail of it.
            return None
        if self.clock() - self._resolved_at.get(key, -1e9) < RESOLVE_RETRY_SECONDS:
            return None
        self._resolved_at[key] = self.clock()
        try:
            detail = await self.control_plane.materialization(materialization_id)
            record = await self.control_plane.table(materialization_id, table)
        except ControlPlaneError as error:
            LOGGER.info("Could not look %s up at the control plane: %s", key, error)
            return None
        target = target_from_control_plane(detail, record)
        if target is None:
            return None
        found = self.tables.get(key)
        if found is None:
            found = TableTail(target, self.settings, hints=hints_from_record(record), member_id=self.config.member_id, clock=self.clock)
            # Not ticked: the forget window counts from now, and the next tick takes over.
            self.tables[key] = found
            self._log("tail_table_resolved", table=key)
        return found

    def serve_slots(self) -> asyncio.Semaphore:
        if self._serve_slots is None:
            self._serve_slots = asyncio.Semaphore(self.config.serve_concurrency)
        return self._serve_slots

    async def run_in_serve_pool(self, function: Any, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        async with self.serve_slots():
            return await loop.run_in_executor(self._serve_pool, lambda: function(*args, **kwargs))

    def _log(self, event: str, **fields: Any) -> None:
        log_protocol_event(LOGGER, enabled=self.settings.protocol_logging_enabled, event=event, member=self.config.member_id, **fields)

    # -- health ------------------------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        return {
            "member": self.config.member_id,
            "app": self.config.app_name,
            "uptime_seconds": round(self.clock() - self.started_at, 1),
            "owned_partitions": None if self.owned is None else sorted(self.owned),
            "tick_partitions": self.tick_partitions or DEFAULT_TICK_PARTITIONS,
            "tables": [table.describe() for table in sorted(self.tables.values(), key=lambda table: table.key)],
            "assigned": len(self._assigned),
            "paused": len(self._paused),
            "resident_bytes": self.resident_bytes(),
            "memory_budget_bytes": self.config.memory_budget_bytes,
            "counters": dict(self.counters),
            "loops": {
                "fetch": "running" if self._fetch_task is not None and not self._fetch_task.done() else "stopped",
                "events": "running" if self._events_task is not None and not self._events_task.done() else "stopped",
            },
        }


def assigned_partitions(app: Any, topic: str) -> set[int] | None:
    """The partitions of `topic` Faust assigned this member, or None when the app cannot say."""
    if app is None:
        return None
    consumer = getattr(app, "consumer", None)
    assignment = getattr(consumer, "assignment", None)
    if not callable(assignment):
        return None
    try:
        tps = assignment()
    except Exception:  # noqa: BLE001 - not started, or rebalancing
        return None
    return {int(tp.partition) for tp in tps if getattr(tp, "topic", None) == topic}


def schema_from_payload(payload: dict[str, Any]) -> TailSchema:
    """A table's schema from the control plane's `?raw=true` schema view."""
    if payload.get("schema_id") is None or not payload.get("iceberg_schema"):
        raise ValueError("the control plane holds no schema of the table yet")
    return TailSchema(
        schema_id=int(payload["schema_id"]),
        iceberg_schema=payload["iceberg_schema"],
        fork_map=payload.get("fork_map") or {},
        last_column_id=payload.get("last_column_id"),
        rejected_paths=payload.get("rejected_paths") or (),
    )


def target_from_control_plane(detail: dict[str, Any], record: dict[str, Any]) -> LiveTailTarget | None:
    """A table the control plane says is tailed - running and written - or None.

    Every materialization has a live tail; a control plane before 4.3 may still say one is off.
    """
    materialization = detail.get("materialization") or {}
    if materialization.get("live_tail") is False or materialization.get("desired_phase") != "running":
        return None
    if record.get("state") != "active":
        return None
    try:
        return LiveTailTarget(
            materialization_id=str(materialization["materialization_id"]),
            dataset=str(record["dataset"]),
            dataset_id=str(record["dataset_id"]),
            table=str(record["name"]),
            location=str(record["location"]),
            source_topic=str(record["source_topic"]),
            partition_granularity=str(record.get("partition_granularity") or "day"),
            table_uuid=record.get("table_uuid"),
            skip_tombstones=bool(materialization.get("skip_tombstones", False)),
        )
    except (KeyError, ValueError):
        return None


def hints_from_record(record: dict[str, Any]) -> LiveTailHints:
    return LiveTailHints.from_dict(
        {"offsets": record.get("offsets") or {}, "schema_id": record.get("schema_id"), "metadata_location": record.get("metadata_location")}
    )


_WORKER: TailWorker | None = None


def get_worker() -> TailWorker:
    """This member's worker: the agent hands it ticks, the task steps it, the views ask it."""
    global _WORKER
    if _WORKER is None:
        _WORKER = TailWorker()
    return _WORKER


def reset_worker(worker: TailWorker | None = None) -> None:
    """Start afresh. Tests use this between cases."""
    global _WORKER
    _WORKER = worker
