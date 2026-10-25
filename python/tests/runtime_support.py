"""A runtime in one process, for tests: writer, pipeline and committer over a fake bucket.

`RuntimeHarness` creates a materializer table per source topic on the fake GCS
(`lakehouse_support.LakeFixture`, one shared bucket) and runs the runtime's own code against them -
`stage_source_record` for each record, the batch pipeline (seals in this process, uploads on its
thread pool), and the committer - passing the outbox's messages to the committer the way the
pipeline task's `topicSend` and the commits topic would. Faust is modelled by `FakeApp`: a
rebalance count, a consumer that records acknowledgements, and `Message`s that acknowledge through it.
"""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
import os
from pathlib import Path
import sys
import tempfile
from typing import Any
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gcs_fakes import FakeGcs  # noqa: E402
from lakehouse_support import MATERIALIZATION_ID, MATERIALIZATION_LOCATION, NAMESPACE, LakeFixture  # noqa: E402
from table_records import table_specs  # noqa: E402

from midstream.lakehouse.gcs import set_shared_storage_client  # noqa: E402
from midstream.runtime_app import acks, flow  # noqa: E402
from midstream.runtime_app.committer import (  # noqa: E402
    _COMMITTERS,
    build_commit_tick,
    handle_commit_event,
    reset_committers,
)
from midstream.runtime_app.config import RuntimeAppConfig  # noqa: E402
from midstream.runtime_app.handlers.writer import reset_writer, stage_source_record  # noqa: E402
from midstream.runtime_app.live_settings import reset_runtime_settings  # noqa: E402
from midstream.runtime_app.outbox import outbox  # noqa: E402
from midstream.runtime_app.performance import reset_runtime_performance  # noqa: E402
from midstream.runtime_app.pipeline import batch_pipeline, seal_in_process  # noqa: E402
from midstream.runtime_app.state import reset_runtime_state  # noqa: E402
from midstream.runtime_app.table_state import reset_table_state, table_contexts  # noqa: E402

TOPIC = "orders"
RUNTIME_APP = "kaspr-mat-a1b2c3d4e5f6"
COMMITS_TOPIC = f"{RUNTIME_APP}-commits"
MEMBER_ID = "member-0"
BASE_TIMESTAMP = datetime(2026, 10, 18, 10, 0, tzinfo=UTC)

#: Every module that reads the runtime's configuration at its point of use.
CONFIG_READERS = (
    "midstream.runtime_app.handlers.writer",
    "midstream.runtime_app.pipeline",
    "midstream.runtime_app.committer",
    "midstream.runtime_app.handlers.status",
    "midstream.runtime_app.handlers.runtime_settings",
    # Read inside a function for the maintenance app's compaction work root, so patched where it
    # is defined too.
    "midstream.runtime_app.config",
)


class FakeTable:
    """The runtime's global state table: a dict with Kaspr's table methods."""

    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.values[key] = value

    def __delitem__(self, key: str) -> None:
        self.values.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self.values

    def prefix_scan(self, prefix: str):
        for key in sorted(self.values):
            if key.startswith(prefix):
                yield key, self.values[key]


@dataclass(frozen=True)
class TP:
    topic: str
    partition: int


class FakeConsumer:
    """Records what is acknowledged, paused and resumed."""

    def __init__(self, partitions=(0,), topics=(TOPIC,)) -> None:
        self.assigned = {TP(topic, partition) for topic in topics for partition in partitions}
        #: (partition, offset) of every acknowledged record, and the same with its topic.
        self.acked: list[tuple[int, int]] = []
        self.acked_by_topic: list[tuple[str, int, int]] = []
        self.paused: list[set] = []
        self.resumed: list[set] = []

    def assignment(self) -> set:
        return set(self.assigned)

    def pause_partitions(self, tps) -> None:
        self.paused.append(set(tps))

    def resume_partitions(self, tps) -> None:
        self.resumed.append(set(tps))


@dataclass(eq=False)
class Message:
    partition: int
    offset: int
    topic: str = TOPIC
    timestamp: float | None = None
    key: bytes | None = None
    headers: list | None = None
    acked: bool = False

    @property
    def tp(self) -> TP:
        return TP(self.topic, self.partition)

    def ack(self, consumer: FakeConsumer) -> bool:
        if self.acked:
            return False
        self.acked = True
        consumer.acked.append((self.partition, self.offset))
        consumer.acked_by_topic.append((self.topic, self.partition, self.offset))
        return True


@dataclass(eq=False)
class Event:
    message: Message


@dataclass
class FakeChannel:
    sent: list

    async def send(self, *, key=None, value=None, **_: Any) -> None:
        self.sent.append((key, value))


@dataclass
class FakeApp:
    """What the runtime reads from its Faust app."""

    consumer: FakeConsumer = field(default_factory=FakeConsumer)
    rebalancing_count: int = 1
    rebalancing: bool = False
    should_stop: bool = False
    sent: list = field(default_factory=list)

    def topic(self, name: str, **_: Any) -> FakeChannel:
        return FakeChannel(self.sent)


def runtime_config(staging_root: str, **overrides: Any) -> RuntimeAppConfig:
    """A configuration that rolls small batches at once and commits without waiting."""
    values = dict(
        staging_root_path=staging_root,
        max_rows_per_file=3,
        max_open_file_age_seconds=10,
        max_open_file_age_ceiling_seconds=600,
        min_rows_per_file=1,
        min_file_size_bytes=1,
        upload_worker_count=2,
        commit_interval_seconds=1,
        schema_evolution_debounce_seconds=1,
        seal_schema_wait_seconds=30,
        announce_retry_seconds=60,
        upload_initial_backoff_seconds=1,
    )
    values.update(overrides)
    return RuntimeAppConfig(**values)


class RuntimeHarness:
    """One runtime member, its committer and its tables - one per topic - in this process."""

    def __init__(
        self,
        *seed_records: dict,
        granularity: str = "day",
        partitions=(0,),
        topics=(TOPIC,),
        seeds_by_topic: dict[str, tuple[dict, ...]] | None = None,
        **config: Any,
    ) -> None:
        """`seed_records` give every table's schema 0; `seeds_by_topic` a topic's own instead."""
        seeds = seed_records or ({"order_id": "O-0", "amount": 1},)
        self.store = FakeGcs()
        self.specs = table_specs(*topics)
        #: Each table's lake, by table name; `lake` is the first table's.
        self.lakes = {
            spec.name: LakeFixture(
                *(seeds_by_topic or {}).get(spec.source_topic, seeds),
                granularity=granularity,
                location=f"{MATERIALIZATION_LOCATION}/{spec.name}",
                identifier=(NAMESPACE, spec.name),
                source_topic=spec.source_topic,
                store=self.store,
            )
            for spec in self.specs
        }
        self.lake = self.lakes[self.specs[0].name]
        self.tables = table_contexts(
            MATERIALIZATION_ID,
            [
                {"name": spec.name, "dataset": NAMESPACE, "source_topic": spec.source_topic, "location": self.lakes[spec.name].location}
                for spec in self.specs
            ],
            partition_granularity=granularity,
        )
        self.granularity = granularity
        self.state_table = FakeTable()
        self._assignment = (tuple(partitions), tuple(topics))
        self.app = FakeApp(consumer=FakeConsumer(partitions, topics=topics))
        self._tmp = tempfile.TemporaryDirectory()
        self.config = runtime_config(self._tmp.name, **config)
        self._stack = ExitStack()
        for module in CONFIG_READERS:
            self._stack.enter_context(patch(f"{module}.get_runtime_app_config", side_effect=lambda: self.config))
        self._stack.enter_context(patch.dict(os.environ, {"MATERIALIZER_RUNTIME_MEMBER_ID": MEMBER_ID}))
        reset_everything()
        set_shared_storage_client(self.lake.client)
        batch_pipeline.sealer = seal_in_process
        batch_pipeline.storage_client = self.lake.client
        self.events: list[dict] = []

    def close(self) -> None:
        reset_everything()
        set_shared_storage_client(None)
        self._stack.close()
        self._tmp.cleanup()
        for lake in self.lakes.values():
            lake.close()

    def configure(self, **overrides: Any) -> None:
        self.config = replace(self.config, **overrides)

    def restart_member(self) -> None:
        """Restart the member while its committer, on another member, runs on.

        The member's process state goes with its process. The new one has an epoch of its own, and
        its app counts rebalances from the first again. The state table stays, as its changelog
        does, and so does the committer, with everything it remembers.
        """
        reset_member()
        batch_pipeline.sealer = seal_in_process
        batch_pipeline.storage_client = self.lake.client
        self._stack.enter_context(patch("midstream.runtime_app.writer.context.PROCESS_EPOCH", uuid.uuid4().hex))
        partitions, topics = self._assignment
        self.app = FakeApp(consumer=FakeConsumer(partitions, topics=topics))

    # -- the writer ------------------------------------------------------------------------------

    def message(
        self,
        *,
        partition: int,
        offset: int,
        at: datetime | None = None,
        key: str | None = None,
        topic: str | None = None,
    ) -> Message:
        moment = at or BASE_TIMESTAMP
        return Message(
            partition=partition,
            offset=offset,
            topic=topic or self.specs[0].source_topic,
            timestamp=moment.timestamp(),
            key=None if key is None else key.encode(),
            headers=[("source", b"test")],
        )

    async def write(
        self,
        value,
        *,
        partition: int = 0,
        offset: int,
        at: datetime | None = None,
        key: str | None = None,
        topic: str | None = None,
    ) -> Message:
        """Hand one record of `topic` (the first) to the writer agent, as Kaspr would; return its message."""
        message = self.message(partition=partition, offset=offset, at=at, key=key, topic=topic)
        await self.write_message(value, message)
        return message

    async def write_message(self, value, message: Message, *, skip_tombstones: bool = False) -> None:
        await stage_source_record(
            value,
            self.state_table,
            context={"app": self.app, "event": Event(message)},
            materialization_id=MATERIALIZATION_ID,
            runtime_app_ref=RUNTIME_APP,
            tables=self.tables,
            skip_tombstones=skip_tombstones,
        )

    # -- the pipeline and the committer ----------------------------------------------------------

    async def pipeline(self, *, upload: bool = True) -> list[dict]:
        """Run the pipeline until it is idle, then drain the outbox as the pipeline task does."""
        await batch_pipeline.drain(self.app, self.state_table, upload=upload)
        return list(batch_pipeline.advance(self.app, self.state_table))

    async def deliver(self, messages: list[dict], *, tick: bool = True) -> list[dict]:
        """Hand messages to the committer (the commits topic), then a commit tick; return table events."""
        events: list[dict] = []
        for value in [*messages, *([build_commit_tick(materialization_id=MATERIALIZATION_ID)] if tick else [])]:
            outputs = await handle_commit_event(
                value,
                self.state_table,
                context={"app": self.app},
                materialization_id=MATERIALIZATION_ID,
                runtime_app_ref=RUNTIME_APP,
                tables=self.tables,
                commits_topic=COMMITS_TOPIC,
            )
            for output in outputs:
                if output.get("route") == "events":
                    events.append(output["value"])
        self.events.extend(events)
        return events

    async def cycle(self) -> list[dict]:
        """Seal, upload and announce whatever is due, commit it, and let members release what it covers."""
        self.allow_commit()
        events = await self.deliver(await self.pipeline())
        await batch_pipeline.drain(self.app, self.state_table)
        return events

    @property
    def runtime_committer(self):
        return _COMMITTERS.get(MATERIALIZATION_ID)

    @property
    def committer(self):
        """The first table's committer."""
        return self.table_committer(self.specs[0].name)

    def table_committer(self, name: str):
        runtime = self.runtime_committer
        return None if runtime is None else runtime.tables.get(name)

    def allow_commit(self) -> None:
        """Let the next tick commit every table at once rather than wait for the commit interval."""
        runtime = self.runtime_committer
        if runtime is None:
            return
        for committer in runtime.tables.values():
            committer.last_commit_at = None
            committer.next_attempt_at = 0.0

    def close_schema_window(self) -> None:
        """Make every open schema window due, as if its debounce had passed."""
        runtime = self.runtime_committer
        if runtime is None:
            return
        for committer in runtime.tables.values():
            if committer.window is not None:
                committer.window.first_at -= 3600

    # -- reading the tables ----------------------------------------------------------------------

    def table(self, name: str | None = None):
        return (self.lake if name is None else self.lakes[name]).load()

    def rows(self, name: str | None = None) -> list[dict]:
        """Every row of a table's current snapshot (the first table's), ordered by partition and offset."""
        table = self.table(name)
        if table.current_snapshot() is None:
            return []
        arrow = table.scan().to_arrow()
        return sorted(arrow.to_pylist(), key=lambda row: (row["_kafka_partition"], row["_kafka_offset"]))


def reset_member() -> None:
    """Forget what a member's process holds: everything but its committer."""
    reset_runtime_state()
    reset_runtime_performance()
    reset_table_state()
    reset_writer()
    reset_runtime_settings()
    acks.reset_acks()
    flow.reset_flow()
    outbox.clear()
    batch_pipeline.reset()


def reset_everything() -> None:
    reset_member()
    reset_committers()


def run(coroutine):
    return asyncio.run(coroutine)
