"""The head peek: a topic's newest records as they will look in its table, apart from the table.

While a table is backlogged - a new materialization's backfill, hours behind - its tail does not
apply, and the console shows the committed rows and how far behind they are. The peek shows what is
arriving now: the newest `rows` records across the topic's partitions, built with the shared row
builder under the table's current schema, newest first. They are in no run, count, statement or
export: the same records will reach the table, and maybe the tail, once the runtime gets there.

A live table answers from its buffer, with no read of Kafka. Any other answers from a dedicated
consumer - one of `peek_concurrency` - that is assigned the table's partitions, seeks each to
`ceil(rows / partitions)` before its end, fetches until every partition reached its end or the
deadline passed, and lets them go. Each answer is kept a second per table, so open tabs share one read.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
import logging
import math
import time
from typing import Any

import pyarrow as pa

from materializer_control_plane.tail_app.consumer import TP
from materializer_control_plane.tail_app.ingest import UNDECODABLE, TailSchema, build, decode_value, stage
from materializer_control_plane.tail_app.serve import parquet_bytes, rows_batch, segment_view
from materializer_control_plane.tail_app.table_tail import TableTail

LOGGER = logging.getLogger(__name__)

#: How long an answer is kept for the next request about the same table.
CACHE_SECONDS = 1.0
#: How long a peek fetches before it answers with what it has.
FETCH_DEADLINE_SECONDS = 2.0


@dataclass(slots=True)
class PeekAnswer:
    body: bytes
    rows: int
    #: Per partition, the first and last offset included.
    offsets: dict[int, tuple[int, int]] = field(default_factory=dict)
    ends: dict[int, int] = field(default_factory=dict)
    behind: int = 0
    state: str = "parked"
    schema_id: int = 0
    settling: int = 0
    generated_at: str = ""
    source: str = "kafka"


class Peeker:
    def __init__(self, *, clients: Any, concurrency: int, clock: Any = time.monotonic) -> None:
        self.clients = clients
        self.concurrency = concurrency
        self.clock = clock
        self._idle: asyncio.Queue | None = None
        self._made = 0
        self._cache: dict[tuple[str, int], tuple[float, PeekAnswer]] = {}
        self._lock: asyncio.Lock | None = None

    async def peek(self, table: TableTail, rows: int) -> PeekAnswer:
        """The newest `rows` records of the table's topic, newest first."""
        key = (table.key, rows)
        cached = self._cache.get(key)
        if cached is not None and self.clock() - cached[0] < CACHE_SECONDS:
            return cached[1]
        schema = table.schema
        if schema is None:
            raise RuntimeError("the table's schema is not known yet")
        if table.state == "live":
            built = self._from_buffer(table, rows)
            source = "buffer"
        else:
            built = await self._from_kafka(table, schema, rows)
            source = "kafka"
        answer = self._answer(table, schema, built, source=source)
        self._cache[key] = (self.clock(), answer)
        for stale in [item for item, (at, _) in self._cache.items() if self.clock() - at >= CACHE_SECONDS]:
            self._cache.pop(stale, None)
        return answer

    def _from_buffer(self, table: TableTail, rows: int) -> list[tuple[dict[str, Any], bool]]:
        """The newest rows a live table holds: the open segments, then its newest closed ones."""
        candidates: list[tuple[dict[str, Any], bool]] = []
        schema = table.schema
        assert schema is not None
        for partition, buffer in table.buffer.partitions.items():
            taken: list[tuple[dict[str, Any], bool]] = []
            if buffer.open is not None and buffer.open.schema_id == schema.schema_id:
                settling = set(buffer.open.settling)
                taken.extend((row, row["_kafka_offset"] in settling) for row in buffer.open.rows[-rows:])
            elif buffer.open is not None:
                for line in buffer.open.lines[-rows:]:
                    built = build(line, schema, default_partition=partition)
                    taken.append((built.built.row, built.settling))
            for segment in reversed(buffer.segments):
                if len(taken) >= rows:
                    break
                view = segment_view(segment, schema)
                settling = set(view.settling)
                newest = view.batch.slice(max(0, view.batch.num_rows - (rows - len(taken))))
                taken.extend((row, row["_kafka_offset"] in settling) for row in newest.to_pylist())
            candidates.extend(taken)
        return _newest(candidates, rows)

    async def _from_kafka(self, table: TableTail, schema: TailSchema, rows: int) -> list[tuple[dict[str, Any], bool]]:
        consumer = await self._consumer()
        try:
            partitions = table.partitions or (await consumer.partitions_for(table.target.source_topic) or [])
            tps = [TP(table.target.source_topic, partition) for partition in partitions]
            if not tps:
                return []
            beginnings = await consumer.beginning_offsets(tps)
            ends = await consumer.end_offsets(tps)
            table.ends({tp.partition: offset for tp, offset in ends.items()})
            # Shared out over the partitions that hold records: an empty one would waste its share.
            holding = [tp for tp in tps if ends.get(tp, 0) > beginnings.get(tp, 0)] or tps
            per_partition = max(1, math.ceil(rows / len(holding)))
            consumer.assign(tps)
            wanted: dict[TP, int] = {}
            for tp in tps:
                start = max(beginnings.get(tp, 0), ends.get(tp, 0) - per_partition)
                consumer.seek(tp, start)
                if ends.get(tp, 0) > start:
                    wanted[tp] = ends[tp]
            built: list[tuple[dict[str, Any], bool]] = []
            deadline = self.clock() + FETCH_DEADLINE_SECONDS
            while wanted and self.clock() < deadline:
                batch = await consumer.getmany(timeout_ms=200, max_records=max(rows, 100))
                for tp, records in batch.items():
                    for record in records:
                        if tp not in wanted or record.offset >= wanted[tp]:
                            continue
                        value = decode_value(record.value)
                        if value is UNDECODABLE or (value is None and table.target.skip_tombstones):
                            continue
                        line = stage(record, value, materialization_id=table.target.materialization_id, source_topic=table.target.source_topic)
                        row = build(line, schema, default_partition=tp.partition)
                        built.append((row.built.row, row.settling))
                for tp in list(wanted):
                    try:
                        if await consumer.position(tp) >= wanted[tp]:
                            del wanted[tp]
                    except Exception:  # noqa: BLE001 - give up on that partition
                        del wanted[tp]
            return _newest(built, rows)
        finally:
            try:
                consumer.assign([])
            except Exception:  # noqa: BLE001 - a consumer that cannot let go is replaced
                try:
                    await consumer.stop()
                except Exception:  # noqa: BLE001 - discarded either way
                    pass
                consumer = None
            await self._release(consumer)

    def _answer(self, table: TableTail, schema: TailSchema, built: list[tuple[dict[str, Any], bool]], *, source: str) -> PeekAnswer:
        rows = [row for row, _ in built]
        table_rows = pa.Table.from_batches([rows_batch(rows, schema)], schema=schema.arrow_schema) if rows else schema.arrow_schema.empty_table()
        offsets: dict[int, tuple[int, int]] = {}
        for row in rows:
            partition, offset = row["_kafka_partition"], row["_kafka_offset"]
            first, last = offsets.get(partition, (offset, offset))
            offsets[partition] = (min(first, offset), max(last, offset))
        ends = table.end_offsets()
        return PeekAnswer(
            body=parquet_bytes(table_rows),
            rows=len(rows),
            offsets=offsets,
            ends=ends,
            behind=table.lag(),
            state=table.state,
            schema_id=schema.schema_id,
            settling=sum(1 for _, settling in built if settling),
            generated_at=datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            source=source,
        )

    async def _consumer(self) -> Any:
        if self._idle is None:
            self._idle = asyncio.Queue()
            self._lock = asyncio.Lock()
        if self._idle.empty() and self._made < self.concurrency:
            async with self._lock:
                if self._made < self.concurrency:
                    self._made += 1
                    try:
                        consumer = self.clients.peek()
                        await consumer.start()
                    except BaseException:
                        self._made -= 1
                        raise
                    return consumer
        return await self._idle.get()

    async def _release(self, consumer: Any) -> None:
        if consumer is None:
            self._made -= 1
            return
        assert self._idle is not None
        self._idle.put_nowait(consumer)


def _newest(rows: list[tuple[dict[str, Any], bool]], limit: int) -> list[tuple[dict[str, Any], bool]]:
    """Newest first by Kafka timestamp - rows without one last - then offset; at most `limit`."""

    def order(item: tuple[dict[str, Any], bool]) -> tuple:
        row = item[0]
        moment = row.get("_kafka_timestamp")
        stamp = moment.timestamp() if isinstance(moment, datetime) else float("-inf")
        return (stamp, row["_kafka_offset"], row["_kafka_partition"])

    return sorted(rows, key=order, reverse=True)[:limit]
