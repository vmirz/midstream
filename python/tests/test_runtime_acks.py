"""The ack ledger never lets a partition's committed offset pass a row that is not durable.

`FaustCommitModel` reproduces the commit bookkeeping of twm-faust 1.17.22's `Consumer` - `ack`,
`_new_offset`, `_add_gap` (whose gap includes the record that revealed it) and the gap detection of
`_drain_messages` - so these tests run without Faust installed. Where twm-faust is importable, the
same scenarios run against its real `Consumer` methods too, which is what keeps the model honest.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import random
import unittest
from unittest.mock import patch

from midstream.runtime_app import acks

try:  # pragma: no cover - only where twm-faust is installed
    from faust.transport.consumer import Consumer as _FaustConsumer
    from intervaltree import IntervalTree as _IntervalTree
except Exception:  # pragma: no cover
    _FaustConsumer = None
    _IntervalTree = None

#: Whether the installed twm-faust has the commit-bookkeeping fixes (handbook 14 §14.2): gaps exclude
#: the record fetched, a read offset of 0 counts, and a partition with no committed offset starts at
#: the first offset read from it.
_FAUST_TRACKS_FIRST_READ = _FaustConsumer is not None and "_first_read_offset" in getattr(
    _FaustConsumer, "__annotations__", {}
)


TOPIC = "orders"


@dataclass(frozen=True)
class TP:
    topic: str
    partition: int


class _Gaps:
    """The parts of `intervaltree.IntervalTree` Faust's gap bookkeeping uses: [begin, end) spans."""

    def __init__(self) -> None:
        self.spans: list[list[int]] = []

    def __bool__(self) -> bool:
        return bool(self.spans)

    def addi(self, begin: int, end: int) -> None:
        if begin >= end:
            raise ValueError("null interval")
        self.spans.append([begin, end])

    def merge_overlaps(self) -> None:
        merged: list[list[int]] = []
        for begin, end in sorted(self.spans):
            if merged and begin < merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([begin, end])
        self.spans = merged

    def overlap(self, begin: int, end: int) -> list[tuple[int, int]]:
        return [(b, e) for b, e in self.spans if b < end and e > begin]

    def chop(self, begin: int, end: int) -> None:
        kept: list[list[int]] = []
        for b, e in self.spans:
            if e <= begin or b >= end:
                kept.append([b, e])
                continue
            if b < begin:
                kept.append([b, begin])
            if e > end:
                kept.append([end, e])
        self.spans = kept

    def at(self, point: int) -> set:
        return {(b, e) for b, e in self.spans if b <= point < e}


class FaustCommitModel:
    """One consumer's offset bookkeeping, as twm-faust 1.17.22 keeps it (one partition is enough)."""

    def __init__(self) -> None:
        self._committed_offset = defaultdict(lambda: None)
        self._acked = defaultdict(set)
        self._acked_index = defaultdict(set)
        self._gap = defaultdict(_Gaps)
        self._read_offset = defaultdict(lambda: None)
        self.kafka_committed: dict = {}

    # Consumer.ack
    def ack(self, message) -> bool:
        if not message.acked:
            message.acked = True
            tp = message.tp
            committed = self._committed_offset[tp]
            if committed is None or message.offset >= committed:
                if message.offset not in self._acked_index[tp]:
                    self._acked_index[tp].add(message.offset)
                    self._acked[tp].add(message.offset)
                    return True
        return False

    # Consumer._add_gap, called by _drain_messages with the fetched record's own offset as offset_to
    def _add_gap(self, tp, offset_from: int, offset_to: int) -> None:
        committed = self._committed_offset[tp]
        if committed is not None:
            offset_from = max(offset_from, committed)
        if offset_from <= offset_to:
            self._gap[tp].addi(offset_from, offset_to + 1)
            self._gap[tp].merge_overlaps()

    # The gap detection in Consumer._drain_messages, before the record is handed to the stream
    def fetch(self, tp, offset: int) -> None:
        r_offset = self._read_offset[tp]
        if r_offset is None or offset >= r_offset:
            gap = offset - (r_offset or 0)
            if gap > 1 and r_offset:
                self._add_gap(tp, r_offset + 1, offset)
            self._read_offset[tp] = offset

    # Consumer._new_offset
    def _new_offset(self, tp):
        acked = self._acked[tp]
        if acked:
            max_offset = max(acked)
            gaps = self._gap[tp]
            if gaps:
                candidates = sorted(gaps.overlap(0, max_offset))
                if candidates:
                    added: list[int] = []
                    for begin, end in candidates:
                        added.extend(range(begin, end))
                    new_max = max(added[-1], max_offset + 1)
                    acked.update(added)
                    gaps.chop(0, new_max)
            committed = self._committed_offset[tp]
            if committed:
                if min(acked) - committed > 0:
                    return None
            ordered = sorted(acked)
            run = [ordered[0]]
            for offset in ordered[1:]:
                if offset != run[-1] + 1:
                    break
                run.append(offset)
            acked.difference_update(run[:-1])
            self._acked_index[tp].difference_update(run)
            return run[-1] + 1
        return None

    # Consumer._should_commit and _commit_offsets, for one partition
    def commit(self, tp) -> None:
        offset = self._new_offset(tp)
        committed = self._committed_offset[tp]
        if offset is not None and (committed is None or (bool(offset) and offset > committed)):
            self._committed_offset[tp] = offset
            self.kafka_committed[tp] = offset

    # A rebalance: Consumer.on_partitions_revoked drops the partition's bookkeeping, and the
    # recovery that ends it seeks to the committed offset (perform_seek).
    def rebalance(self, tp) -> None:
        for mapping in (self._gap, self._acked, self._acked_index, self._read_offset, self._committed_offset):
            mapping.pop(tp, None)
        committed = self.kafka_committed.get(tp)
        self._read_offset[tp] = (committed - 1) if committed else None
        self._committed_offset[tp] = committed if committed else None

    def committed(self, tp):
        return self._committed_offset[tp]


class RealFaustCommits:  # pragma: no cover - only where twm-faust is installed
    """The same interface, driven by twm-faust's own Consumer methods.

    They need only the bookkeeping attributes, so they run on a plain namespace.
    """

    def __init__(self) -> None:
        import types
        import weakref

        from sortedcontainers import SortedSet

        consumer = types.SimpleNamespace(
            _committed_offset=defaultdict(lambda: None),
            _acked=defaultdict(SortedSet),
            _acked_index=defaultdict(set),
            _gap=defaultdict(_IntervalTree),
            _read_offset=defaultdict(lambda: None),
            _first_read_offset={},
            _unacked_messages=weakref.WeakSet(),
            _waiting_for_ack=None,
            _n_acked=0,
            app=types.SimpleNamespace(topics=types.SimpleNamespace(acks_enabled_for=lambda topic: True)),
        )
        consumer.ack = lambda message: _FaustConsumer.ack(consumer, message)
        self.consumer = consumer
        self.kafka_committed: dict = {}

    def ack(self, message) -> bool:
        return self.consumer.ack(message)

    def fetch(self, tp, offset: int) -> None:
        import asyncio

        # As `_drain_messages` does, in whichever twm-faust is installed.
        r_offset = self.consumer._read_offset[tp]
        if r_offset is None or offset >= r_offset:
            if _FAUST_TRACKS_FIRST_READ and r_offset is None:
                self.consumer._first_read_offset[tp] = offset
            gap = offset - (r_offset or 0)
            has_read = r_offset is not None if _FAUST_TRACKS_FIRST_READ else bool(r_offset)
            if gap > 1 and has_read:
                asyncio.run(_FaustConsumer._add_gap(self.consumer, tp, r_offset + 1, offset))
            self.consumer._read_offset[tp] = offset

    def commit(self, tp) -> None:
        offset = _FaustConsumer._new_offset(self.consumer, tp)
        if offset is not None and _FaustConsumer._should_commit(self.consumer, tp, offset):
            self.consumer._committed_offset[tp] = offset
            self.kafka_committed[tp] = offset

    def rebalance(self, tp) -> None:
        for name in ("_gap", "_acked", "_acked_index", "_read_offset", "_committed_offset", "_first_read_offset"):
            getattr(self.consumer, name).pop(tp, None)
        committed = self.kafka_committed.get(tp)
        self.consumer._read_offset[tp] = (committed - 1) if committed else None
        self.consumer._committed_offset[tp] = committed if committed else None

    def committed(self, tp):
        return self.consumer._committed_offset[tp]


@dataclass(eq=False)
class FakeMessage:
    tp: TP
    offset: int
    acked: bool = False

    @property
    def topic(self) -> str:
        return self.tp.topic

    @property
    def partition(self) -> int:
        return self.tp.partition

    def ack(self, consumer) -> bool:
        return consumer.ack(self)


@dataclass
class FakeEvent:
    message: FakeMessage


@dataclass
class FakeApp:
    consumer: object
    rebalancing: bool = False
    rebalancing_count: int = 1


@dataclass
class _Batch:
    batch_id: str
    offsets: list[int] = field(default_factory=list)
    rolled: bool = False
    durable: bool = False


class PartitionSimulation:
    """One partition's writer, staging, uploads and Faust commits, driven step by step."""

    def __init__(self, commits, *, seed: int, length: int, gap_rate: float, free_rate: float) -> None:
        self.rng = random.Random(seed)
        self.commits = commits
        self.tp = TP(TOPIC, 0)
        self.app = FakeApp(consumer=commits if not hasattr(commits, "consumer") else commits.consumer)
        # Offsets 0 and 1 always exist: twm-faust records no gap after offset 0 (see
        # `test_faust_records_no_gap_after_offset_zero`), which would hold the commit there whatever
        # the ledger does.
        self.offsets = [0, 1] + [offset for offset in range(2, length) if self.rng.random() >= gap_rate]
        self.free_rate = free_rate
        #: Offsets whose rows are in storage (written by any session), and offsets needing no row.
        self.durable: set[int] = set()
        self.free: set[int] = set()
        self.staged_by: dict[int, str] = {}
        self.batches: list[_Batch] = []
        self.open: _Batch | None = None
        self.next_index = 0
        self.batch_counter = 0
        self.ordered_cap = 12

    # -- the parts of the runtime being simulated -------------------------------------------------

    def _deliver(self) -> bool:
        committed = self.commits.kafka_committed.get(self.tp) or 0
        while self.next_index < len(self.offsets) and self.offsets[self.next_index] < committed:
            self.next_index += 1
        if self.next_index >= len(self.offsets):
            return False
        offset = self.offsets[self.next_index]
        self.next_index += 1
        self.commits.fetch(self.tp, offset)
        message = FakeMessage(self.tp, offset)
        generation = self.app.rebalancing_count
        if offset in self.durable or offset in self.free:
            # Already stored, or never needed a row: the writer passes it through.
            acks.register_record(self.app, FakeEvent(message), table="orders", partition=0, generation=generation)
            return True
        if self.rng.random() < self.free_rate:
            self.free.add(offset)
            acks.register_record(self.app, FakeEvent(message), table="orders", partition=0, generation=generation)
            return True
        starts = self.open is None
        if starts:
            self.batch_counter += 1
            self.open = _Batch(batch_id=f"b{self.batch_counter}")
            self.batches.append(self.open)
        self.open.offsets.append(offset)
        self.staged_by[offset] = self.open.batch_id
        acks.register_record(
            self.app,
            FakeEvent(message),
            table="orders",
            partition=0,
            generation=generation,
            batch_id=self.open.batch_id,
            starts_batch=starts,
        )
        ordered = acks.batch_is_ordered("orders", 0, self.open.batch_id)
        if (ordered and len(self.open.offsets) >= self.ordered_cap) or self.rng.random() < 0.08:
            self.open.rolled = True
            self.open = None
        return True

    def _make_oldest_durable(self) -> None:
        for batch in self.batches:
            if batch.durable:
                continue
            if not batch.rolled:
                return
            batch.durable = True
            self.durable.update(batch.offsets)
            acks.batch_durable(self.app, table="orders", partition=0, batch_id=batch.batch_id)
            return

    def _rebalance(self) -> None:
        # Sometimes the writer is part-way through a record when the rebalance begins, and only
        # finishes - as a record that needs no row - once it has ended.
        straddling = None
        if self.next_index < len(self.offsets) and self.rng.random() < 0.5:
            straddling = FakeMessage(self.tp, self.offsets[self.next_index])
            self.commits.fetch(self.tp, straddling.offset)
        received_generation = self.app.rebalancing_count
        self.app.rebalancing = True
        self.app.rebalancing_count += 1
        self.commits.rebalance(self.tp)
        self.app.rebalancing = False
        if straddling is not None:
            acks.register_record(
                self.app,
                FakeEvent(straddling),
                table="orders",
                partition=0,
                generation=received_generation,
            )
        # The pipeline abandons the session's batches and the ledger; what they staged is read again.
        acks.forget_partition("orders", 0)
        self.batches = [batch for batch in self.batches if batch.durable]
        self.open = None
        self.next_index = 0

    # -- the property -----------------------------------------------------------------------------

    def assert_nothing_committed_ahead_of_storage(self, case: unittest.TestCase) -> None:
        committed = self.commits.committed(self.tp)
        if committed is None:
            return
        for offset in self.offsets:
            if offset >= committed:
                break
            case.assertTrue(
                offset in self.durable or offset in self.free,
                f"committed offset {committed} passed offset {offset}, whose row is not durable",
            )

    def run(self, case: unittest.TestCase, *, rebalance_rate: float = 0.0) -> None:
        steps = 0
        while True:
            steps += 1
            case.assertLess(steps, 100_000, "simulation did not finish")
            if rebalance_rate and self.rng.random() < rebalance_rate:
                self._rebalance()
                self.assert_nothing_committed_ahead_of_storage(case)
            delivered = self._deliver()
            if self.rng.random() < 0.35:
                self._make_oldest_durable()
            if self.rng.random() < 0.3:
                self.commits.commit(self.tp)
            self.assert_nothing_committed_ahead_of_storage(case)
            if not delivered:
                break
        # Drain: everything rolls, is made durable and committed.
        if self.open is not None:
            self.open.rolled = True
            self.open = None
        for _ in range(len(self.batches) + 1):
            self._make_oldest_durable()
            self.commits.commit(self.tp)
            self.assert_nothing_committed_ahead_of_storage(case)
        self.commits.commit(self.tp)
        if self.offsets:
            case.assertEqual(
                self.commits.committed(self.tp),
                self.offsets[-1] + 1,
                "once every batch is durable, every offset commits",
            )


class AckLedgerPropertyTests(unittest.TestCase):
    commits_factory = FaustCommitModel

    def setUp(self) -> None:
        acks.reset_acks()
        self.addCleanup(acks.reset_acks)
        patcher = patch.object(acks, "ORDERED_BATCH_MAX_ROWS", 12)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _simulate(self, *, seeds, gap_rate: float, free_rate: float, rebalance_rate: float = 0.0, start_committed=None):
        for seed in seeds:
            with self.subTest(seed=seed):
                acks.reset_acks()
                commits = self.commits_factory()
                if start_committed is not None:
                    tp = TP(TOPIC, 0)
                    commits.kafka_committed[tp] = start_committed
                    commits.rebalance(tp)
                simulation = PartitionSimulation(
                    commits, seed=seed, length=400, gap_rate=gap_rate, free_rate=free_rate
                )
                if start_committed is not None:
                    simulation.durable.update(o for o in simulation.offsets if o < start_committed)
                simulation.run(self, rebalance_rate=rebalance_rate)

    def test_new_consumer_group_without_gaps(self) -> None:
        self._simulate(seeds=range(40), gap_rate=0.0, free_rate=0.1)

    def test_new_consumer_group_on_a_compacted_topic(self) -> None:
        """Gaps put the record after each one inside it, which a guard must never rely on."""
        self._simulate(seeds=range(40), gap_rate=0.3, free_rate=0.1)

    def test_existing_consumer_group_with_gaps_and_skipped_records(self) -> None:
        self._simulate(seeds=range(40), gap_rate=0.25, free_rate=0.3, start_committed=37)

    def test_rebalances_never_commit_what_a_session_staged(self) -> None:
        self._simulate(seeds=range(40), gap_rate=0.2, free_rate=0.1, rebalance_rate=0.02)

    def test_a_topic_of_records_that_need_no_row(self) -> None:
        self._simulate(seeds=range(10), gap_rate=0.2, free_rate=1.0)


@unittest.skipIf(_FaustConsumer is None, "twm-faust is not installed")
class AckLedgerAgainstFaustTests(AckLedgerPropertyTests):  # pragma: no cover
    """The same properties, against twm-faust's own Consumer methods."""

    commits_factory = RealFaustCommits


class AckLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        acks.reset_acks()
        self.addCleanup(acks.reset_acks)
        self.commits = FaustCommitModel()
        self.tp = TP(TOPIC, 0)
        self.app = FakeApp(consumer=self.commits)

    def _register(self, offset: int, *, batch_id=None, starts=False, generation=None, fetch=True) -> FakeMessage:
        if fetch:
            self.commits.fetch(self.tp, offset)
        message = FakeMessage(self.tp, offset)
        acks.register_record(
            self.app,
            FakeEvent(message),
            table="orders",
            partition=0,
            generation=self.app.rebalancing_count if generation is None else generation,
            batch_id=batch_id,
            starts_batch=starts,
        )
        return message

    def _with_committed(self, offset: int) -> None:
        self.commits.kafka_committed[self.tp] = offset
        self.commits.rebalance(self.tp)

    def test_a_guarded_batch_holds_only_its_first_record(self) -> None:
        self._with_committed(100)
        first = self._register(100, batch_id="b1", starts=True)
        rest = [self._register(offset, batch_id="b1") for offset in range(101, 110)]

        self.assertFalse(first.acked)
        self.assertTrue(all(message.acked for message in rest))
        self.assertFalse(acks.batch_is_ordered("orders", 0, "b1"))
        self.commits.commit(self.tp)
        self.assertEqual(self.commits.committed(self.tp), 100)

        acks.batch_durable(self.app, table="orders", partition=0, batch_id="b1")
        self.assertTrue(first.acked)
        self.commits.commit(self.tp)
        self.assertEqual(self.commits.committed(self.tp), 110)
        self.assertEqual(acks.ack_snapshot()["unacknowledged_batches"], 0)

    def test_a_batch_without_a_committed_offset_is_held_in_order(self) -> None:
        """Faust checks a run against the committed offset only when there is one."""
        messages = [self._register(0, batch_id="b1", starts=True)]
        messages += [self._register(offset, batch_id="b1") for offset in range(1, 6)]
        free = self._register(6)

        self.assertTrue(acks.batch_is_ordered("orders", 0, "b1"))
        self.assertFalse(any(message.acked for message in messages + [free]))
        self.commits.commit(self.tp)
        self.assertIsNone(self.commits.committed(self.tp))

        acks.batch_durable(self.app, table="orders", partition=0, batch_id="b1")
        self.assertTrue(all(message.acked for message in messages + [free]))
        self.commits.commit(self.tp)
        self.assertEqual(self.commits.committed(self.tp), 7)

    def test_a_guard_right_after_a_gap_is_not_trusted(self) -> None:
        """twm-faust records a gap with the fetched record inside it, as if acknowledged."""
        self._with_committed(10)
        self._register(10)
        self._register(14, batch_id="b1", starts=True)  # 11-13 compacted away

        self.assertTrue(acks.batch_is_ordered("orders", 0, "b1"))
        later = self._register(15, batch_id="b1")
        self.assertFalse(later.acked)
        self.commits.commit(self.tp)
        self.assertEqual(self.commits.committed(self.tp), 11)

    def test_free_records_behind_an_ordered_batch_wait_their_turn(self) -> None:
        guard = self._register(0, batch_id="b1", starts=True)
        free = self._register(1)
        self.assertFalse(free.acked)
        acks.batch_durable(self.app, table="orders", partition=0, batch_id="b1")
        self.assertTrue(guard.acked and free.acked)

    def test_nothing_is_acknowledged_while_the_app_rebalances(self) -> None:
        self._with_committed(5)
        guard = self._register(5, batch_id="b1", starts=True)
        self.app.rebalancing = True
        acks.batch_durable(self.app, table="orders", partition=0, batch_id="b1")
        self.assertFalse(guard.acked)
        late = self._register(6, generation=self.app.rebalancing_count)
        self.assertFalse(late.acked)

    def test_a_record_from_an_ended_session_is_dropped_unacknowledged(self) -> None:
        self._with_committed(5)
        self._register(5, batch_id="b1", starts=True)
        old_generation = self.app.rebalancing_count
        self.app.rebalancing_count += 1

        stale = self._register(6, generation=old_generation)
        self.assertFalse(stale.acked)
        # The old session's batch cannot release anything either: its ledger was dropped.
        acks.batch_durable(self.app, table="orders", partition=0, batch_id="b1")
        self.assertEqual(acks.ack_snapshot()["unacknowledged_batches"], 0)

        current = self._register(7)
        self.assertTrue(current.acked)

    def test_a_row_for_a_batch_the_ledger_never_saw_start_is_held(self) -> None:
        self._with_committed(3)
        orphan = self._register(3, batch_id="b9")
        self.assertFalse(orphan.acked)
        self.assertTrue(acks.batch_is_ordered("orders", 0, "b9"))

    def test_a_stream_that_acknowledges_on_its_own_switches_the_ledger_off(self) -> None:
        """Deployed without noack, Faust acknowledges each record itself; holding would only cost memory."""
        self._with_committed(3)
        guard = self._register(3, batch_id="b1", starts=True)
        guard.acked = True  # Faust's automatic acknowledgement
        with self.assertLogs(acks.LOGGER, level="WARNING"):
            self._register(4, batch_id="b1")
        self.assertEqual(acks.ack_snapshot()["held_guards"], 0)

    def test_forget_partition_reports_what_it_dropped(self) -> None:
        self._register(0, batch_id="b1", starts=True)
        self._register(1, batch_id="b1")
        self.assertEqual(acks.forget_partition("orders", 0), 2)
        self.assertEqual(acks.forget_partition("orders", 0), 0)

    def test_faust_records_no_gap_after_offset_zero(self) -> None:
        """A twm-faust limitation, not the ledger's: `_drain_messages` tests `r_offset` for truth.

        After offset 0 a gap is never recorded, so an offset compaction removed right after it is
        never acknowledged and the partition's commit stays at 1. Nothing is lost - the records after
        it are read again on restart - but the offset cannot advance.
        """
        first = self._register(0)
        second = self._register(2)  # offset 1 compacted away
        self.assertTrue(first.acked and second.acked)
        self.commits.commit(self.tp)
        self.commits.commit(self.tp)
        self.assertEqual(self.commits.committed(self.tp), 1)

    def test_guard_probe_reads_committed_offsets_and_gaps(self) -> None:
        self.assertFalse(acks.guard_is_honoured(self.commits, self.tp, 4))  # no committed offset
        self._with_committed(4)
        self.assertTrue(acks.guard_is_honoured(self.commits, self.tp, 4))
        self.commits._gap[self.tp].addi(6, 9)
        self.assertFalse(acks.guard_is_honoured(self.commits, self.tp, 8))
        self.assertTrue(acks.guard_is_honoured(self.commits, self.tp, 9))
        self.assertFalse(acks.guard_is_honoured(object(), self.tp, 9))  # unknown consumer


if __name__ == "__main__":
    unittest.main()
