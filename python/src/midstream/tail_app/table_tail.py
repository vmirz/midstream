"""One table's live tail on its owner: when it is read, from where, how much it holds, and its state.

**Tail what is watched.** A table arrives `parked`: known from its ticks, nothing assigned, nothing
read. The first request for it - a tail or a peek - activates it: its schema is fetched from the
control plane (or, failing that, read from the table through its pointer), its partitions and their
ends are read from the cluster's metadata, and, unless it is backlogged, its partitions are assigned
to the member's consumer from the table's committed offsets (`catching_up`) until every partition
has reached the head (`live`). A table nobody has asked about for `idle_seconds` is parked again.

**Backlogs.** A table whose runtime is more than `max_rows_per_table` records behind the topic's head
- a new materialization's backfill, above all - is `backlogged`: nothing is read but its end offsets,
and requests answer `409 tail_backlogged` with how far behind it is. Once the runtime is within 75 %
of the bound the table catches up, and is live seconds later. An active table whose window outgrows
twice the bound - a burst that keeps growing - drops its buffer and is backlogged too, so a table at
the boundary does not flap.

**Bounds.** Past `max_rows_per_table` uncommitted rows or `max_bytes_per_table` resident bytes the
table's partitions pause (`paused`) rather than evict, so what it holds stays a prefix; it resumes at
75 % of both. It is read at most `max_records_per_second_per_table` records a second, so one topic's
burst cannot slow the member's other tables; the member's memory budget may pause the largest table.

**Following the table.** Ticks carry hints - committed offsets, schema id - and the table events
the committer and the maintenance app publish carry the commits and schema changes as they happen
(`events.py`). Commits only raise the committed offsets; a schema change installs the new schema,
and rows built under the old one are built again from their staged lines when next served. A tick
naming another incarnation of the table (`table_uuid`) seeds it afresh.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import math
import time
from typing import Any

from midstream.models import LiveTailHints, LiveTailTarget
from midstream.protocol_logging import log_protocol_event
from midstream.tail_app.buffer import Selection, TableBuffer, Unavailable
from midstream.tail_app.config import TailSettings
from midstream.tail_app.consumer import TP, Need
from midstream.tail_app.ingest import UNDECODABLE, TailSchema, build, decode_value, epoch_ms, stage
from midstream.tail_app.serve import rebuild, rows_batch

LOGGER = logging.getLogger(__name__)

STATES = ("parked", "seeding", "backlogged", "catching_up", "live", "paused", "unavailable")
#: Leave a backlog below this share of the bound; resume a paused table below it too.
RESUME_FRACTION = 0.75
#: An active table whose window outgrows this many bounds is a backlog.
BACKLOG_ENTRY_BOUNDS = 2
#: Records a table builds at a time before the loop is let go to everyone else.
INGEST_CHUNK = 256
#: Waits between failed seeds: a table whose schema cannot be fetched answers `not ready` meanwhile.
SEED_RETRY_SECONDS = (1, 2, 5, 10, 30)
#: How often a request tries a table whose topic could not be read again.
UNAVAILABLE_RETRY_SECONDS = 30.0


@dataclass(slots=True)
class TableCounters:
    ingested: int = 0
    undecodable: int = 0
    tombstones_skipped: int = 0
    served: int = 0
    rows_served: int = 0
    bytes_served: int = 0
    forwarded: int = 0
    seeds: int = 0
    reconforms: int = 0


@dataclass(slots=True)
class PartitionProgress:
    """Where the consumer stands on one of the table's partitions."""

    #: The offset past the newest readable record, from the last fetch or end-offset read.
    head: int | None = None
    #: When the partition's position last reached its head.
    at_head_since: float | None = None
    caught_up: bool = False
    #: A fetch has answered for the partition since it was assigned.
    fetched: bool = False
    #: When a fetch response last came for it (the consumer's last poll, epoch ms), records or not.
    last_poll: int | None = None


class TableTail:
    def __init__(
        self,
        target: LiveTailTarget,
        settings: TailSettings,
        *,
        hints: LiveTailHints | None = None,
        member_id: str = "unknown-member",
        clock: Any = time.monotonic,
    ) -> None:
        self.target = target
        self.settings = settings
        self.hints = hints or LiveTailHints()
        self.member_id = member_id
        self._clock = clock
        now = clock()
        self.last_ticked = now
        self.last_asked: float | None = None
        self.state = "parked"
        self.reason: str | None = None
        self.schema: TailSchema | None = None
        self.seed_source: str | None = None
        self.buffer = TableBuffer(clock=clock)
        #: The table's committed offsets as this member knows them; they only rise.
        self.committed: dict[int, int] = dict(_offsets(self.hints.offsets))
        self.partitions: list[int] = []
        self.progress: dict[int, PartitionProgress] = {}
        self.bound_paused = False
        self.budget_paused = False
        self.rate_paused_until = 0.0
        self._rate_window = (0.0, 0)
        self.activated_at: float | None = None
        self.counters = TableCounters()
        self.changed = asyncio.Event() if _loop_running() else None
        self._seed_task: asyncio.Task | None = None
        self._seed_failures = 0
        self._next_seed_at = 0.0
        self.last_error: str | None = None
        #: The schema id a tick said the table has; a newer one than held means a change was missed.
        self.hinted_schema_id = self.hints.schema_id
        self._beginnings: dict[int, int] = {}
        #: When the table last became unavailable (its topic could not be read).
        self.unavailable_at: float | None = None

    # -- identity ------------------------------------------------------------------------------------

    @property
    def key(self) -> str:
        return self.target.key

    @property
    def active(self) -> bool:
        return self.state in ("catching_up", "live", "paused")

    @property
    def table_uuid(self) -> str | None:
        return self.target.table_uuid

    def tps(self) -> list[TP]:
        return [TP(self.target.source_topic, partition) for partition in self.partitions]

    # -- what the control plane says -------------------------------------------------------------

    def tick(self, target: LiveTailTarget, settings: TailSettings, hints: LiveTailHints | None) -> bool:
        """Take a tick; True when it names another incarnation of the table, which is seeded afresh."""
        reseed = bool(target.table_uuid and self.target.table_uuid and target.table_uuid != self.target.table_uuid)
        self.target = target
        self.settings = settings
        self.last_ticked = self._clock()
        if hints is not None:
            self.hints = hints
            self.hinted_schema_id = hints.schema_id
            if not reseed:
                self.commit(_offsets(hints.offsets))
        if reseed:
            # The table's name now belongs to a new table: nothing of the old one may be served.
            watched = self.state != "parked"
            self.reset("seeding" if watched else "parked")
            self.committed = dict(_offsets(hints.offsets if hints is not None else {}))
        return reseed

    def commit(self, offsets: dict[int, int]) -> None:
        """The table's new committed offsets, from a hint or a `table_committed` event: only ever up."""
        raised = False
        for partition, offset in offsets.items():
            if offset > self.committed.get(partition, -1):
                self.committed[partition] = offset
                raised = True
        if raised and self.buffer.partitions:
            self.buffer.commit(offsets)
        if raised:
            self._signal()

    def install_schema(self, schema: TailSchema) -> bool:
        """A newer schema of the table: rows built under the older one are built again when served."""
        if self.schema is not None and schema.schema_id <= self.schema.schema_id:
            return False
        self.schema = schema
        self.counters.reconforms += 1
        self._signal()
        return True

    # -- activation ------------------------------------------------------------------------------

    def ask(self) -> None:
        """A request for the table: it stays tailed for `idle_seconds` from now."""
        self.last_asked = self._clock()

    def idle(self) -> bool:
        if self.settings.idle_seconds == 0 or self.state == "parked":
            return False
        reference = self.last_asked if self.last_asked is not None else self.activated_at or self.last_ticked
        return self._clock() - reference >= self.settings.idle_seconds

    def reset(self, state: str = "parked", *, reason: str | None = None) -> None:
        """Let go of everything read: buffer, partitions, progress."""
        if self._seed_task is not None and not self._seed_task.done():
            self._seed_task.cancel()
        self._seed_task = None
        self.buffer.drop()
        self.progress = {}
        self.bound_paused = self.budget_paused = False
        self.rate_paused_until = 0.0
        self.state = state
        self.reason = reason
        if state == "parked":
            self.activated_at = None
            self.schema = None
        if state == "unavailable":
            self.unavailable_at = self._clock()
        self._signal()

    def needs_seed(self) -> bool:
        return self.state == "seeding" and (self._seed_task is None or self._seed_task.done()) and self._clock() >= self._next_seed_at

    def start_seed(self, coroutine: Any) -> None:
        self._seed_task = asyncio.ensure_future(coroutine)

    def seed_failed(self, error: BaseException) -> None:
        delay = SEED_RETRY_SECONDS[min(self._seed_failures, len(SEED_RETRY_SECONDS) - 1)]
        self._seed_failures += 1
        self._next_seed_at = self._clock() + delay
        self.last_error = str(error)
        LOGGER.warning("Could not seed the tail of %s (attempt %s): %s. Retrying in %ss.", self.key, self._seed_failures, error, delay)

    def seeded(
        self,
        *,
        schema: TailSchema,
        source: str,
        partitions: list[int],
        beginnings: dict[int, int],
        ends: dict[int, int],
        committed: dict[int, int] | None = None,
        table_uuid: str | None = None,
    ) -> None:
        """What a seed found: start the buffer from the committed offsets, or be backlogged."""
        if self.state != "seeding":
            return
        self._seed_failures = 0
        self.schema = schema if self.schema is None or schema.schema_id >= self.schema.schema_id else self.schema
        self.seed_source = source
        self.counters.seeds += 1
        if committed:
            for partition, offset in committed.items():
                self.committed[partition] = max(self.committed.get(partition, -1), offset)
        self.partitions = sorted(partitions)
        self.progress = {partition: PartitionProgress(head=ends.get(partition)) for partition in self.partitions}
        self._beginnings = dict(beginnings)
        self.activated_at = self.activated_at or self._clock()
        log_protocol_event(
            LOGGER,
            enabled=self.settings.protocol_logging_enabled,
            event="tail_table_seeded",
            table=self.key,
            source=source,
            offsets={str(partition): self.committed.get(partition, -1) for partition in self.partitions},
            schema_id=self.schema.schema_id,
            member=self.member_id,
        )
        if self.lag() > self.settings.max_rows_per_table:
            self.state = "backlogged"
            self._signal()
            return
        self._start_buffer()

    def _start_buffer(self) -> None:
        beginnings = getattr(self, "_beginnings", {})
        self.buffer.drop()
        for partition in self.partitions:
            committed = self.committed.get(partition, -1)
            start = committed + 1
            beginning = beginnings.get(partition, 0)
            gap_floor = None
            if start < beginning:
                # Retention removed records the table never took: the runtime cannot read them
                # either. Read from Kafka's beginning, and say so.
                start, gap_floor = beginning, beginning
            self.buffer.start(partition, committed=committed, start=start, gap_floor=gap_floor)
            progress = self.progress.setdefault(partition, PartitionProgress())
            progress.caught_up = False
            progress.fetched = False
        self.bound_paused = self.budget_paused = False
        self.state = "catching_up"
        self._signal()

    def add_partitions(self, partitions: list[int]) -> list[int]:
        """Partitions the topic gained: read from their beginning (committed -1)."""
        added = sorted(set(partitions) - set(self.partitions))
        if not added:
            return []
        self.partitions = sorted(set(self.partitions) | set(added))
        for partition in added:
            self.progress[partition] = PartitionProgress()
            if self.active:
                committed = self.committed.get(partition, -1)
                self.buffer.start(partition, committed=committed, start=committed + 1)
        return added

    # -- reading ---------------------------------------------------------------------------------

    def wanting(self) -> bool:
        """Whether the table wants records now: active, and paused by nothing."""
        return self.active and not self.bound_paused and not self.budget_paused and self._clock() >= self.rate_paused_until

    def needs(self) -> list[Need]:
        """What the table asks of the member's consumer, partition by partition."""
        if not self.active:
            return []
        wanting = self.wanting()
        return [
            Need(tp=TP(self.target.source_topic, partition), table=self.key, next_offset=buffer.next_offset, wanting=wanting)
            for partition, buffer in self.buffer.partitions.items()
        ]

    async def ingest(self, partition: int, records: list[Any], *, yield_every: int = INGEST_CHUNK) -> bool:
        """Records fetched for one of the table's partitions, in offset order. True when the table took
        every one of them - only then may its coverage move to the consumer's position.

        Records below the partition's next offset are already held (a shared partition read again
        for another table) and are skipped; nothing is taken while the table does not want records,
        and a table that reaches a bound mid-batch stops there, leaving the rest for later.
        """
        buffer = self.buffer.partitions.get(partition)
        schema = self.schema
        if buffer is None or schema is None or not self.wanting():
            return False
        added = 0
        handled = 0
        for record in records:
            offset = int(record.offset)
            if offset < buffer.next_offset:
                continue
            handled += 1
            value = decode_value(record.value)
            if value is UNDECODABLE:
                # Faust acknowledges what it cannot decode, and the writer never sees it.
                self.counters.undecodable += 1
                self.buffer.skip(partition, offset=offset)
            elif value is None and self.target.skip_tombstones:
                self.counters.tombstones_skipped += 1
                self.buffer.skip(partition, offset=offset)
            else:
                line = stage(record, value, materialization_id=self.target.materialization_id, source_topic=self.target.source_topic)
                row = build(line, schema, default_partition=partition)
                moment = row.built.moment
                self.buffer.append(
                    partition,
                    offset=offset,
                    line=line,
                    row=row.built.row,
                    moment=epoch_ms(moment),
                    schema_id=schema.schema_id,
                    settling=row.settling,
                )
                added += 1
                if self.buffer.due_to_close(partition):
                    self.close_segment(partition)
            self.counters.ingested += 1
            self._count_rate()
            if not self.wanting() and record is not records[-1]:
                # The ceiling was reached mid-batch: the rest stays unread, and is fetched again
                # once the table resumes.
                return False
            if handled % yield_every == 0:
                await asyncio.sleep(0)
                if self.buffer.partitions.get(partition) is not buffer or self.schema is None:
                    # Parked, cancelled or seeded afresh meanwhile.
                    return False
        return True

    def close_segment(self, partition: int) -> None:
        """Close a partition's open segment into an Arrow batch under the current schema."""
        buffer = self.buffer.partitions.get(partition)
        schema = self.schema
        if buffer is None or buffer.open is None or schema is None:
            return
        segment = buffer.open
        if segment.schema_id == schema.schema_id:
            batch, settling = rows_batch(segment.rows, schema), tuple(segment.settling)
        else:
            rebuilt, offsets = rebuild(segment.lines, schema, partition=partition)
            batch, settling = rebuilt, tuple(offsets)
        self.buffer.close(partition, batch=batch, schema_id=schema.schema_id, settling=settling)

    def close_due(self) -> None:
        for partition in list(self.buffer.partitions):
            if self.buffer.due_to_close(partition):
                self.close_segment(partition)

    def fetched(self, partition: int, *, position: int | None, head: int | None, last_poll: int | None = None) -> None:
        """Where a partition stands after a fetch: Kafka's position, when the table took everything
        below it, the readable head, and when a fetch response last came for it.

        A partition is at its head only as of a response that said so: one whose responses stopped
        - its leader gone, the broker unreachable - falls behind by the time since, though the
        head it last heard of is long reached.
        """
        if position is not None:
            self.buffer.advance(partition, position)
        progress = self.progress.setdefault(partition, PartitionProgress())
        fresh = last_poll is None or progress.last_poll is None or last_poll > progress.last_poll
        if last_poll is not None:
            progress.last_poll = last_poll if progress.last_poll is None else max(progress.last_poll, last_poll)
        if head is not None:
            progress.head = head
            progress.fetched = True
        buffer = self.buffer.partitions.get(partition)
        if buffer is not None and progress.head is not None and buffer.next_offset >= progress.head:
            if fresh:
                progress.at_head_since = self._clock()
            progress.caught_up = True
        elif buffer is not None and progress.head is not None:
            progress.caught_up = False

    def _count_rate(self) -> None:
        now = self._clock()
        window_start, count = self._rate_window
        if now - window_start >= 1.0:
            window_start, count = now, 0
        count += 1
        self._rate_window = (window_start, count)
        if count >= self.settings.max_records_per_second_per_table:
            self.rate_paused_until = window_start + 1.0

    # -- state -----------------------------------------------------------------------------------

    def lag(self, ends: dict[int, int] | None = None) -> int:
        """Records Kafka holds past the table's committed offsets: how far the runtime is behind."""
        total = 0
        for partition in self.partitions:
            head = (ends or {}).get(partition)
            if head is None:
                progress = self.progress.get(partition)
                head = None if progress is None else progress.head
            if head is None:
                continue
            total += max(0, head - 1 - self.committed.get(partition, -1))
        return total

    def ends(self, ends: dict[int, int]) -> None:
        """End offsets read for the table (a backlogged table's, or a refresh)."""
        for partition, head in ends.items():
            self.progress.setdefault(partition, PartitionProgress()).head = head

    def evaluate(self) -> str:
        """Re-evaluate the state from the committed offsets, the heads and the buffer's size."""
        previous = self.state
        settings = self.settings
        if self.state == "backlogged":
            if self.lag() < RESUME_FRACTION * settings.max_rows_per_table:
                self._start_buffer()
        elif self.active:
            if self.lag() > BACKLOG_ENTRY_BOUNDS * settings.max_rows_per_table:
                # A burst that keeps growing: the runtime is too far behind for a tail to be of use.
                self.buffer.drop()
                self.bound_paused = False
                self.state = "backlogged"
            else:
                rows = self.buffer.uncommitted_rows()
                size = self.buffer.resident_bytes()
                if not self.bound_paused and (rows >= settings.max_rows_per_table or size >= settings.max_bytes_per_table):
                    self.bound_paused = True
                elif self.bound_paused and rows < RESUME_FRACTION * settings.max_rows_per_table and size < RESUME_FRACTION * settings.max_bytes_per_table:
                    self.bound_paused = False
                if self.bound_paused or self.budget_paused:
                    self.state = "paused"
                elif self.buffer.partitions and all(self.progress.get(partition, PartitionProgress()).caught_up for partition in self.buffer.partitions):
                    self.state = "live"
                else:
                    self.state = "catching_up"
        if self.state != previous:
            if self.state == "paused" or previous == "paused":
                log_protocol_event(
                    LOGGER,
                    enabled=settings.protocol_logging_enabled,
                    event="tail_fetch_paused" if self.state == "paused" else "tail_fetch_resumed",
                    table=self.key,
                    rows=self.buffer.uncommitted_rows(),
                    bytes=self.buffer.resident_bytes(),
                    member=self.member_id,
                )
            self._signal()
        return self.state

    @property
    def ready(self) -> bool:
        """Whether an answer can be given: active, and every partition fetched at least once."""
        return self.active and all(self.progress.get(partition, PartitionProgress()).fetched for partition in self.buffer.partitions)

    def lag_ms(self) -> int | None:
        """0 when every partition's last fetch reached its head; else how long since one last did.

        A fetch answers within the fetch wait whether records came or not, so a partition last at
        its head longer ago than that, and a second of grace, has fallen behind or stopped hearing.
        """
        if not self.active:
            return None
        now = self._clock()
        grace = self.settings.fetch_wait_ms / 1000 + 1.0
        worst = 0
        for partition in self.buffer.partitions:
            progress = self.progress.get(partition, PartitionProgress())
            since = progress.at_head_since or self.activated_at or now
            behind = now - since
            if progress.caught_up and behind <= grace:
                continue
            worst = max(worst, int(behind * 1000))
        return worst

    def paused_partitions(self) -> list[int]:
        return sorted(self.buffer.partitions) if (self.bound_paused or self.budget_paused or self._clock() < self.rate_paused_until) else []

    def end_offsets(self) -> dict[int, int]:
        """Per partition, Kafka's newest readable offset (the head, less one), as last seen."""
        return {partition: progress.head - 1 for partition, progress in sorted(self.progress.items()) if progress.head is not None}

    def behind(self, through: dict[int, int]) -> int:
        return sum(max(0, head - through.get(partition, self.committed.get(partition, -1))) for partition, head in self.end_offsets().items())

    def gaps(self, selection: Selection) -> list[int]:
        return sorted(partition for partition, item in selection.partitions.items() if item.gap)

    def select(self, after: dict[int, int]) -> Selection | Unavailable:
        return self.buffer.select(after)

    async def wait(self, deadline: float, predicate: Any) -> None:
        """Wait until `predicate()` holds or `deadline` - real monotonic time - passes."""
        while not predicate():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            event = self._event()
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), timeout=min(remaining, 0.25))
            except asyncio.TimeoutError:
                pass

    def _event(self) -> asyncio.Event:
        if self.changed is None:
            self.changed = asyncio.Event()
        return self.changed

    def _signal(self) -> None:
        if self.changed is not None:
            self.changed.set()

    # -- health ----------------------------------------------------------------------------------

    def describe(self) -> dict[str, Any]:
        return {
            "table": self.key,
            "materialization_id": self.target.materialization_id,
            "dataset": self.target.dataset,
            "name": self.target.table,
            "source_topic": self.target.source_topic,
            "state": self.state,
            "reason": self.reason,
            "schema_id": None if self.schema is None else self.schema.schema_id,
            "seed_source": self.seed_source,
            "partitions": self.partitions,
            "committed": {str(partition): offset for partition, offset in sorted(self.committed.items())},
            "end_offsets": {str(partition): offset for partition, offset in self.end_offsets().items()},
            "next_offsets": {str(partition): buffer.next_offset for partition, buffer in sorted(self.buffer.partitions.items())},
            "rows": self.buffer.rows(),
            "uncommitted_rows": self.buffer.uncommitted_rows(),
            "bytes": self.buffer.resident_bytes(),
            "behind": self.lag(),
            "lag_ms": self.lag_ms(),
            "paused": self.paused_partitions(),
            "last_asked_seconds_ago": None if self.last_asked is None else round(self._clock() - self.last_asked, 1),
            "last_ticked_seconds_ago": round(self._clock() - self.last_ticked, 1),
            "last_error": self.last_error,
            "counters": {
                "ingested": self.counters.ingested,
                "undecodable": self.counters.undecodable,
                "tombstones_skipped": self.counters.tombstones_skipped,
                "served": self.counters.served,
                "rows_served": self.counters.rows_served,
                "bytes_served": self.counters.bytes_served,
                "seeds": self.counters.seeds,
                "reconforms": self.counters.reconforms,
            },
        }


def _offsets(offsets: dict[str, int] | dict[int, int]) -> dict[int, int]:
    return {int(partition): int(offset) for partition, offset in (offsets or {}).items()}


def _loop_running() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def peek_offsets(ends: dict[int, int], beginnings: dict[int, int], rows: int) -> dict[int, int]:
    """Where a peek at a topic's newest `rows` records starts reading each partition."""
    if not ends:
        return {}
    per_partition = max(1, math.ceil(rows / len(ends)))
    return {partition: max(beginnings.get(partition, 0), end - per_partition) for partition, end in ends.items()}


@dataclass(slots=True)
class SeedPlan:
    """What a seed reads: the schema, the partitions and their offsets."""

    schema: TailSchema
    source: str
    partitions: list[int]
    beginnings: dict[int, int] = field(default_factory=dict)
    ends: dict[int, int] = field(default_factory=dict)
    committed: dict[int, int] = field(default_factory=dict)
