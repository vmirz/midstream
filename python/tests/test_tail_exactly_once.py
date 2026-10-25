"""R2 by a model: a run's pinned snapshot and the tails it fetches hold every record exactly once.

Random schedules drive the real tail app - its worker, buffer, fetch plan and answers - against a
simulated Kafka, runtime, control plane and console:

- records produced to 1-6 partitions, with offsets that hold no record (compaction, transaction
  markers) and values Kaspr cannot decode, which need no row;
- one or two tables of the topic (two materializations reading it), whose runtimes commit random
  prefixes, their offsets only ever rising; ticks whose hints lag the table; table events;
- bursts past small bounds, ingest ceilings, the clock moving past retentions and idle windows
  (trims and parks), members restarted (a fresh buffer seeded from a lagging hint), schema changes;
- a console that pins snapshots at random moments - the table's committed offsets then - and
  fetches each run's tail incrementally, `after` = the last answer's `through`.

After every answer: a refusal, or rows that are exactly the records Kafka holds with
`after < offset <= through` that need a row, each once, with `through` never below `after`; so a
run's snapshot and its tails together hold every record up to `through` exactly once. A parked or
backlogged table holds nothing. Schedules are seeded: a failure names its seed, which replays it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import random
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tail_support import DATASET_ID, TailHarness, reader_on, target, tick  # noqa: E402

from materializer_control_plane.lakehouse.lifecycle import schema_to_json  # noqa: E402
from materializer_control_plane.lakehouse.schema.infer import infer  # noqa: E402
from materializer_control_plane.lakehouse.schema.policy import evolve  # noqa: E402
from materializer_control_plane.tail_app.views import tail_rows_view  # noqa: E402
from materializer_control_plane.web.errors import WebError  # noqa: E402
from seal_cases import golden_evolution  # noqa: E402

#: Schedules one run of the suite tries; `TAIL_EXACTLY_ONCE_SCHEDULES` asks for more (or fewer).
SCHEDULES = int(os.environ.get("TAIL_EXACTLY_ONCE_SCHEDULES", "150"))
STEPS = 45


@dataclass(slots=True)
class Run:
    """A console run of one table: its pinned snapshot and how far its tails go."""

    table: str
    snapshot: dict[int, int]
    through: dict[int, int]
    rows: list[tuple[int, int]] = field(default_factory=list)


class Schedule:
    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.harness = TailHarness(members=1)
        self.partitions = self.rng.randint(1, 6)
        self.tables = ["mat-1"] + (["mat-2"] if self.rng.random() < 0.4 else [])
        self.targets = {materialization: target(materialization_id=materialization, table="orders", topic="orders", table_uuid=f"uuid-{materialization}") for materialization in self.tables}
        for tail_target in self.targets.values():
            self.harness.table(tail_target)
        self.harness.kafka.create("orders", self.partitions)
        self.token = self.harness.control_plane.token("tok", reader_on(DATASET_ID))
        #: Per partition, every offset Kafka holds a record at, and whether it needs a row.
        self.records: dict[int, dict[int, bool]] = {partition: {} for partition in range(self.partitions)}
        #: Per table, the committed offsets the table really holds.
        self.committed: dict[str, dict[int, int]] = {materialization: {partition: -1 for partition in range(self.partitions)} for materialization in self.tables}
        self.settings = {materialization: self._settings() for materialization in self.tables}
        self.runs: list[Run] = []
        self.answers = 0
        self.refusals = 0

    def _settings(self) -> dict:
        roll = self.rng.random()
        settings = {"retention_seconds": self.rng.choice([10, 30, 120]), "idle_seconds": self.rng.choice([0, 60, 600])}
        if roll < 0.3:
            settings["max_rows_per_table"] = 1_000
        if roll < 0.15:
            settings["max_records_per_second_per_table"] = 100
        return settings

    # -- the world -------------------------------------------------------------------------------

    def produce(self) -> None:
        for _ in range(self.rng.randint(1, 30)):
            partition = self.rng.randrange(self.partitions)
            if self.rng.random() < 0.1:
                self.harness.kafka.skip_offsets("orders", partition, self.rng.randint(1, 3))
            undecodable = self.rng.random() < 0.05
            value = b"\xff not json" if undecodable else {"order_id": f"O{partition}", "amount": self.rng.randint(0, 9)}
            record = self.harness.kafka.produce("orders", partition, value, key=f"K{partition}", raw=undecodable)
            self.records[partition][record.offset] = not undecodable

    def burst(self) -> None:
        partition = self.rng.randrange(self.partitions)
        for _ in range(self.rng.randint(800, 1_600)):
            record = self.harness.kafka.produce("orders", partition, {"order_id": "B"}, key="K")
            self.records[partition][record.offset] = True

    def commit(self, materialization: str) -> None:
        """The runtime commits a prefix of what Kafka holds: offsets only ever rise."""
        changed = {}
        for partition in range(self.partitions):
            end = self.harness.kafka.ends[("orders", partition)] - 1
            current = self.committed[materialization][partition]
            if end > current and self.rng.random() < 0.7:
                offset = self.rng.randint(current + 1, end)
                self.committed[materialization][partition] = offset
                changed[str(partition)] = offset
        if changed and self.rng.random() < 0.8:
            self.harness.table_event("table_committed", materialization, "orders", {"offsets": changed})

    def send_tick(self, materialization: str) -> None:
        """A tick whose hints may lag the table by an event: never ahead of it."""
        hints = {partition: offset if self.rng.random() < 0.7 else -1 for partition, offset in self.committed[materialization].items()}
        self.harness.deliver(tick(self.targets[materialization], offsets=hints, **self.settings[materialization]))

    def restart(self) -> None:
        """The member restarts: everything it held is gone; the ticks and the table bring it back."""
        old = self.harness
        fresh = TailHarness(members=1)
        fresh.kafka = old.kafka
        fresh.control_plane = old.control_plane
        fresh.clock = old.clock
        for worker in fresh.workers:
            worker.clock = old.clock
            worker.sessions._clock = old.clock
            worker.http = old.control_plane
            worker.control_plane.http = old.control_plane
            worker.clients.kafka = old.kafka
        fresh.clients[0].kafka = old.kafka
        self.harness = fresh
        for materialization in self.tables:
            self.send_tick(materialization)

    def schema_change(self, materialization: str) -> None:
        first = golden_evolution()
        evolved = evolve(first.schema, first.fork_map, [((), infer({"extra": self.rng.random()}))], last_column_id=first.last_column_id)
        self.harness.table_event(
            "schema_changed",
            materialization,
            "orders",
            {"schema_id": evolved.schema.schema_id, "schema": schema_to_json(evolved.schema), "fork_map": evolved.fork_map.to_dict(), "last_column_id": evolved.last_column_id, "rejected_paths": []},
        )

    # -- the console -----------------------------------------------------------------------------

    def pin(self, materialization: str) -> None:
        snapshot = dict(self.committed[materialization])
        self.runs.append(Run(table=materialization, snapshot=snapshot, through=dict(snapshot)))
        self.runs = self.runs[-4:]

    async def fetch_tail(self, run: Run) -> None:
        owner = self.harness.owner_of(run.table, "orders")
        request = self.harness.request(token=self.token, after=run.through, format="json")
        try:
            answer = await tail_rows_view(request, run.table, "orders", worker=owner)
        except WebError as error:
            self.refusals += 1
            assert error.status_code in (404, 409, 503), (self.seed, error.status_code, error.details)
            return
        self.answers += 1
        rows = [(row["_kafka_partition"], row["_kafka_offset"]) for row in json.loads(answer["body"])["rows"]]
        through = {int(partition): offset for partition, offset in json.loads(answer["headers"]["X-Tail-Through"]).items()}
        self.check_answer(run, rows, through)
        run.rows.extend(rows)
        for partition, offset in through.items():
            run.through[partition] = offset
        self.check_run(run)

    def check_answer(self, run: Run, rows: list[tuple[int, int]], through: dict[int, int]) -> None:
        assert len(rows) == len(set(rows)), (self.seed, "a row twice in one answer", rows)
        for partition in range(self.partitions):
            after = run.through.get(partition, -1)
            ends = through.get(partition, after)
            assert ends >= after, (self.seed, "through went back", partition, after, ends)
            expected = sorted(offset for offset, needs in self.records[partition].items() if needs and after < offset <= ends)
            got = sorted(offset for part, offset in rows if part == partition)
            assert got == expected, (self.seed, "not exactly the records past after", partition, after, ends, got, expected)

    def check_run(self, run: Run) -> None:
        """The snapshot's rows and the run's tails: every record up to `through`, each once."""
        assert len(run.rows) == len(set(run.rows)), (self.seed, "a row in two answers of a run")
        for partition in range(self.partitions):
            snapshot = run.snapshot.get(partition, -1)
            held = {offset for offset, needs in self.records[partition].items() if needs and offset <= snapshot}
            tails = {offset for part, offset in run.rows if part == partition}
            assert not held & tails, (self.seed, "a row in the snapshot and its tail", partition, sorted(held & tails))
            expected = {offset for offset, needs in self.records[partition].items() if needs and offset <= run.through[partition]}
            assert held | tails == expected, (self.seed, "a hole or an extra row", partition, sorted(expected - (held | tails)), sorted((held | tails) - expected))

    def check_holdings(self) -> None:
        for worker in self.harness.workers:
            for table in worker.tables.values():
                if table.state in ("parked", "backlogged"):
                    assert table.buffer.rows() == 0, (self.seed, "a parked or backlogged table holds rows", table.state)

    # -- the schedule ----------------------------------------------------------------------------

    async def play(self) -> None:
        for materialization in self.tables:
            self.send_tick(materialization)
            self.pin(materialization)
        for _ in range(STEPS):
            roll = self.rng.random()
            materialization = self.rng.choice(self.tables)
            if roll < 0.2:
                self.produce()
            elif roll < 0.23:
                self.burst()
            elif roll < 0.35:
                await self.harness.fetch(self.rng.randint(1, 3))
            elif roll < 0.43:
                await self.harness.step()
            elif roll < 0.53:
                self.commit(materialization)
            elif roll < 0.58:
                self.send_tick(materialization)
            elif roll < 0.63:
                self.harness.clock.advance(self.rng.choice([0.5, 1.5, 15, 70, 200]))
            elif roll < 0.7:
                self.pin(materialization)
            elif roll < 0.9:
                if self.runs:
                    await self.fetch_tail(self.rng.choice(self.runs))
            elif roll < 0.93:
                self.restart()
            elif roll < 0.96:
                self.schema_change(materialization)
            else:
                await self.harness.settle(1)
            self.check_holdings()
        # In the end: everything settles, and every run reads on to the head.
        await self.harness.settle(4)
        for run in self.runs:
            await self.fetch_tail(run)


class TailExactlyOnceTests(unittest.IsolatedAsyncioTestCase):
    async def test_random_schedules_never_duplicate_or_lose_a_record(self) -> None:
        answers = refusals = 0
        for seed in range(SCHEDULES):
            schedule = Schedule(seed)
            await schedule.play()
            answers += schedule.answers
            refusals += schedule.refusals
        # The schedules must exercise answers, not only refusals.
        self.assertGreater(answers, SCHEDULES)
        self.assertGreater(refusals, 0)


if __name__ == "__main__":
    unittest.main()
