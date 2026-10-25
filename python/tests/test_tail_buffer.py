"""A table's live tail in memory: contiguous runs of records per partition (tail_app/buffer.py, serve.py)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

import pyarrow.parquet as pq
import io

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tail_support import BASE_MS, Record, golden_schema, json_value  # noqa: E402

from materializer_control_plane.tail_app.buffer import (  # noqa: E402
    SEGMENT_MAX_AGE_SECONDS,
    SEGMENT_ROWS,
    SegmentView,
    Selection,
    TableBuffer,
    Unavailable,
    compress_lines,
    decompress_lines,
)
from materializer_control_plane.tail_app.ingest import build, stage  # noqa: E402
from materializer_control_plane.tail_app.serve import build_answer, rows_batch  # noqa: E402


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class BufferHarness:
    """A table's buffer fed as the tail feeds it: staged lines, built rows, segments closed when due."""

    def __init__(self, partitions: int = 2, *, committed: int = -1) -> None:
        self.clock = Clock()
        self.schema = golden_schema()
        self.buffer = TableBuffer(clock=self.clock)
        for partition in range(partitions):
            self.buffer.start(partition, committed=committed, start=committed + 1)

    def add(self, partition: int, offset: int, value: object = None, *, skip: bool = False) -> None:
        if skip:
            self.buffer.skip(partition, offset=offset)
            return
        record = Record(
            topic="orders",
            partition=partition,
            offset=offset,
            timestamp=BASE_MS + offset,
            key=f"K{offset}".encode(),
            value=json_value(value if value is not None else {"order_id": f"O{partition}-{offset}", "amount": offset}),
        )
        line = stage(record, {"order_id": f"O{partition}-{offset}", "amount": offset} if value is None else value, materialization_id="mat-1", source_topic="orders", observed_at="2026-10-18T10:00:01.000Z")
        row = build(line, self.schema, default_partition=partition)
        self.buffer.append(
            partition,
            offset=offset,
            line=line,
            row=row.built.row,
            moment=None if row.built.moment is None else int(row.built.moment.timestamp() * 1000),
            schema_id=self.schema.schema_id,
            settling=row.settling,
        )

    def close(self, partition: int) -> None:
        segment = self.buffer.partitions[partition].open
        self.buffer.close(partition, batch=rows_batch(segment.rows, self.schema), schema_id=segment.schema_id, settling=tuple(segment.settling))

    def answer(self, after: dict[int, int], *, max_rows: int = 1_000_000, max_bytes: int = 1 << 40):
        selection = self.buffer.select(after)
        if isinstance(selection, Unavailable):
            return selection
        return build_answer(selection, self.schema, max_rows=max_rows, max_bytes=max_bytes)


def offsets_of(answer) -> list[tuple[int, int]]:
    table = pq.read_table(io.BytesIO(answer.body))
    return sorted(zip(table.column("_kafka_partition").to_pylist(), table.column("_kafka_offset").to_pylist()))


class BufferTests(unittest.TestCase):
    def test_everything_past_after_comes_back_once_across_closed_and_open_segments(self) -> None:
        tail = BufferHarness()
        for offset in range(0, 5):
            tail.add(0, offset)
        tail.close(0)
        for offset in range(5, 8):
            tail.add(0, offset)
        tail.add(1, 0)

        answer = tail.answer({0: 2, 1: -1})

        self.assertEqual([(0, 3), (0, 4), (0, 5), (0, 6), (0, 7), (1, 0)], offsets_of(answer))
        self.assertEqual({0: 7, 1: 0}, answer.through)
        self.assertEqual(set(), answer.cut)
        # Asking again from `through` gets nothing, and `through` stays.
        again = tail.answer(answer.through)
        self.assertEqual((0, {0: 7, 1: 0}), (again.rows, again.through))

    def test_records_needing_no_row_and_skipped_offsets_are_covered_without_rows(self) -> None:
        tail = BufferHarness(partitions=1)
        tail.add(0, 0)
        tail.add(0, 1, skip=True)
        tail.buffer.advance(0, 5)

        answer = tail.answer({0: -1})

        self.assertEqual([(0, 0)], offsets_of(answer))
        # Offsets 1 to 4 hold no row Kafka gave; the answer covers them.
        self.assertEqual({0: 4}, answer.through)
        self.assertEqual(4, tail.answer({0: 0}).through[0])

    def test_an_after_older_than_what_is_held_is_refused_for_the_whole_table(self) -> None:
        tail = BufferHarness(committed=9)
        tail.add(0, 10)
        tail.add(1, 10)

        refused = tail.answer({0: 5, 1: 9})

        self.assertIsInstance(refused, Unavailable)
        self.assertEqual({0: "behind_retention"}, refused.partitions)
        self.assertIsInstance(tail.buffer.select({0: 9, 1: 9}), Selection)

    def test_an_after_past_the_buffer_answers_nothing_and_keeps_it(self) -> None:
        tail = BufferHarness(partitions=1)
        tail.add(0, 0)

        answer = tail.answer({0: 40})

        self.assertEqual((0, {0: 40}), (answer.rows, answer.through))

    def test_a_partition_the_caller_does_not_name_is_read_from_its_beginning(self) -> None:
        tail = BufferHarness()
        tail.add(0, 0)
        tail.add(1, 0)

        self.assertEqual([(0, 0), (1, 0)], offsets_of(tail.answer({})))

    def test_committed_segments_are_kept_for_the_retention_then_dropped_from_the_old_end(self) -> None:
        tail = BufferHarness(partitions=1)
        for offset in range(0, 4):
            tail.add(0, offset)
        tail.close(0)
        for offset in range(4, 6):
            tail.add(0, offset)
        tail.close(0)

        tail.buffer.commit({0: 3})
        tail.clock.now += 119
        self.assertEqual(0, tail.buffer.trim(120))
        self.assertEqual(6, tail.answer({0: -1}).rows, "a run pinned before the commit still finds its tail")
        tail.clock.now += 2
        self.assertEqual(1, tail.buffer.trim(120))

        self.assertEqual(4, tail.buffer.partitions[0].covered_from)
        self.assertEqual([(0, 4), (0, 5)], offsets_of(tail.answer({0: 3})))
        self.assertIsInstance(tail.answer({0: 2}), Unavailable)

    def test_commits_only_rise(self) -> None:
        tail = BufferHarness(partitions=1)
        tail.buffer.commit({0: 5})
        tail.buffer.commit({0: 3})
        self.assertEqual(5, tail.buffer.partitions[0].committed)

    def test_uncommitted_rows_count_only_what_the_table_does_not_hold(self) -> None:
        tail = BufferHarness(partitions=1)
        for offset in range(0, 6):
            tail.add(0, offset)
        tail.close(0)
        for offset in range(6, 9):
            tail.add(0, offset)

        tail.buffer.commit({0: 3})

        self.assertEqual(5, tail.buffer.uncommitted_rows())
        self.assertEqual(9, tail.buffer.rows())

    def test_memory_short_drops_every_retained_segment_at_once(self) -> None:
        tail = BufferHarness(partitions=1)
        for offset in range(0, 4):
            tail.add(0, offset)
        tail.close(0)
        tail.add(0, 4)
        tail.close(0)
        tail.buffer.commit({0: 3})
        retained = tail.buffer.retained_bytes()
        self.assertGreater(retained, 0)

        self.assertEqual(1, tail.buffer.drop_retained())

        self.assertEqual(0, tail.buffer.retained_bytes())
        self.assertEqual(4, tail.buffer.partitions[0].covered_from)

    def test_a_cap_cuts_each_partition_to_a_prefix_and_says_where_it_ends(self) -> None:
        tail = BufferHarness()
        for offset in range(0, 3):
            tail.add(0, offset)
            tail.add(1, offset)
        tail.close(0)
        tail.close(1)
        for offset in range(3, 6):
            tail.add(0, offset)
            tail.add(1, offset)

        answer = tail.answer({}, max_rows=4)

        self.assertEqual(4, answer.rows)
        self.assertEqual({0, 1}, answer.cut | {0})
        held = offsets_of(answer)
        for partition in (0, 1):
            mine = [offset for part, offset in held if part == partition]
            self.assertEqual(list(range(len(mine))), mine, "a prefix of what follows `after`")
            self.assertEqual(mine[-1] if mine else -1, answer.through[partition])
        # The next answer continues exactly where this one stopped.
        rest = tail.answer(answer.through)
        self.assertEqual(sorted(set((p, o) for p in (0, 1) for o in range(6)) - set(held)), offsets_of(rest))

    def test_segments_close_at_their_size_or_age(self) -> None:
        tail = BufferHarness(partitions=1)
        tail.add(0, 0)
        self.assertFalse(tail.buffer.due_to_close(0))
        tail.clock.now += SEGMENT_MAX_AGE_SECONDS
        self.assertTrue(tail.buffer.due_to_close(0))
        tail.close(0)
        for offset in range(1, SEGMENT_ROWS + 1):
            tail.add(0, offset)
        self.assertTrue(tail.buffer.due_to_close(0))

    def test_a_closed_segment_is_weighed_once_when_its_batch_is_built(self) -> None:
        tail = BufferHarness(partitions=1)
        for offset in range(10):
            tail.add(0, offset)
        tail.close(0)
        segment = tail.buffer.partitions[0].segments[0]
        self.assertEqual(segment.view.batch.nbytes + len(segment.raw), segment.nbytes)
        self.assertEqual(segment.nbytes, tail.buffer.resident_bytes())

        # pyarrow works a batch's size out from every buffer each time it is asked, and the fetch
        # loop weighs every segment of every active table after each fetch.
        class Weighed:
            times = 0

            @property
            def nbytes(self) -> int:
                Weighed.times += 1
                return 123

        view = SegmentView(batch=Weighed(), schema_id=1)
        self.assertEqual([123, 123, 123], [view.nbytes for _ in range(3)])
        self.assertEqual(1, Weighed.times)
        self.assertEqual(view, SegmentView(batch=view.batch, schema_id=1), "the size is not part of what a view is")

    def test_a_closed_segment_keeps_its_staged_lines_compressed(self) -> None:
        lines = [b'{"m":{"o":%d},"v":{"order_id":"O%d"}}' % (offset, offset) for offset in range(500)]
        raw, size = compress_lines(lines)
        self.assertLess(len(raw), size / 5)
        self.assertEqual(lines, decompress_lines(raw, size))
        self.assertEqual(([], (b"", 0)), (decompress_lines(b"", 0), compress_lines([])))

    def test_a_record_below_the_next_offset_is_refused(self) -> None:
        tail = BufferHarness(partitions=1)
        tail.add(0, 0)
        with self.assertRaises(ValueError):
            tail.add(0, 0)
        with self.assertRaises(ValueError):
            tail.buffer.skip(0, offset=0)

    def test_a_gap_kafka_made_is_served_from_its_beginning(self) -> None:
        tail = BufferHarness(partitions=1)
        # The table's next record was at 3, but retention removed everything below 10.
        tail.buffer.start(0, committed=2, start=10, gap_floor=10)
        tail.add(0, 10)

        selection = tail.buffer.select({0: 2})

        self.assertIsInstance(selection, Selection)
        self.assertTrue(selection.partitions[0].gap)
        tail.add(0, 11)
        tail.close(0)
        tail.buffer.commit({0: 11})
        tail.clock.now += 1000
        tail.buffer.trim(1)
        self.assertIsInstance(tail.buffer.select({0: 2}), Unavailable, "past Kafka's beginning it is a hole of ours")


if __name__ == "__main__":
    unittest.main()
