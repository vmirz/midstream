"""Staged lines and a schema that exercise every way a seal turns a line into a row.

Shared by the seal's golden tests (`test_runtime_seal.py`), the row builder's (`test_row_builder.py`)
and the tail's parity tests (`test_tail_parity.py`), so all three judge the same rows. The schema has
a fork (`amount`, long then double), a collapsed family (`v`, long, string, then JSON), a struct, a
list of structs with a nested list, and the metadata columns. The lines cover: fitting values, the
fork's newer member, the collapse, unknown keys and unplaceable values (`_unmapped`), a value that is
not an object (`$`), a tombstone (a null payload), `b64:` keys and header values, a header without a
value, records without a Kafka timestamp, several days and months, non-ASCII text, an integer beyond
64 bits, and JSON numbers that render differently in other languages (`1.0`, `1e16`).
"""

from __future__ import annotations

from typing import Any

from materializer_control_plane.lakehouse.schema.forkmap import ForkMap
from materializer_control_plane.lakehouse.schema.infer import infer
from materializer_control_plane.lakehouse.schema.policy import Evolution, SchemaLimits, evolve
from materializer_control_plane.lakehouse.tables import build_schema

BASE_RECORD = {
    "order_id": "O1",
    "amount": 1,
    "customer": {"email": "e@example.com", "tier": 1},
    "items": [{"sku": "s", "qty": 1, "tags": ["a"]}],
    "flag": True,
    "note": "x",
    "v": 1,
    "big": 1,
}


def golden_evolution() -> Evolution:
    """Schema 3: the base record, `amount` forked to a double, `v` forked then collapsed to JSON."""
    first = evolve(build_schema(), ForkMap(), [((), infer(BASE_RECORD))], schema_id=0)
    forked = evolve(first.schema, first.fork_map, [((), infer({"amount": 1.5}))], last_column_id=first.last_column_id)
    one_fork = SchemaLimits(max_forks_per_field=1)
    string_fork = evolve(forked.schema, forked.fork_map, [((), infer({"v": "s"}))], limits=one_fork, last_column_id=forked.last_column_id)
    return evolve(string_fork.schema, string_fork.fork_map, [((), infer({"v": {"a": 1}}))], limits=one_fork, last_column_id=string_fork.last_column_id)


def base_evolution() -> Evolution:
    """Schema 0 of the same table: only the base record's shape."""
    return evolve(build_schema(), ForkMap(), [((), infer(BASE_RECORD))], schema_id=0)


#: A record Kafka gave no key.
NO_KEY = object()


def line(
    offset: int,
    value: Any,
    *,
    partition: int = 3,
    at: str | None = "2026-10-18T10:00:00.000Z",
    key: Any = None,
    headers: list[list[Any]] | None = None,
    read_at: str = "2026-10-18T10:00:01.250Z",
) -> dict[str, Any]:
    """A staged line as the writer builds it (`writer/batches.py::staged_line`): the key is `K<offset>`
    unless one is given, or none for `NO_KEY`."""
    return {
        "m": {
            "p": partition,
            "o": offset,
            "t": at,
            "k": None if key is NO_KEY else (f"K{offset}" if key is None else key),
            "h": headers,
            "a": read_at,
        },
        "v": value,
    }


def golden_lines(partition: int = 3) -> list[dict[str, Any]]:
    """Every kind of line the seal meets, in offset order, on one Kafka partition."""
    lines = [
        line(100, dict(BASE_RECORD), partition=partition, headers=[["event-type", "order.created"]]),
        line(101, {"order_id": "O2", "amount": 2.5, "customer": {"email": "f@example.com"}}, partition=partition),
        line(102, {"order_id": "O3", "v": {"z": [1, 2.0, 1e16]}, "note": "é漢字"}, partition=partition),
        line(103, {"order_id": "O4", "amount": "many", "v": 1.0, "extra": {"x": 1}, "empty": [], "gone": None}, partition=partition),
        line(104, [1, 2], partition=partition),
        line(105, "a bare string", partition=partition),
        line(106, None, partition=partition, headers=[["reason", "gdpr"]]),
        line(107, {"order_id": "O8"}, partition=partition, key="b64:AAEC", headers=[["bin", "b64:/w=="], ["no-value", None]]),
        line(108, {"order_id": "O9", "items": [{"sku": "a"}, {"sku": "b", "extra": 1}, None]}, partition=partition, at=None),
        line(109, {"order_id": "O10", "big": 2**70}, partition=partition, at="2026-10-19T00:00:00.000Z"),
        line(110, {"order_id": "O11", "v": "text", "flag": False}, partition=partition, at="2026-10-31T23:59:59.999Z"),
        line(111, {"order_id": "O12", "customer": {"email": None, "tier": 7}}, partition=partition, at="2026-11-01T00:00:00.000Z"),
        line(112, {"order_id": "O13", "items": [{"sku": "c", "qty": 3, "tags": []}]}, partition=partition, key=NO_KEY, at="2026-10-18T23:00:00.000Z"),
    ]
    return lines


def many_lines(count: int, *, start: int = 1000, partition: int = 3) -> list[dict[str, Any]]:
    """Enough fitting lines to fill several row groups at a small row-group size."""
    return [
        line(
            start + index,
            {"order_id": f"M{index}", "amount": index, "customer": {"email": f"m{index}@example.com", "tier": index % 3}, "note": "n" * (index % 17)},
            partition=partition,
            at=f"2026-10-{18 + index % 3:02d}T{index % 24:02d}:00:00.000Z",
        )
        for index in range(count)
    ]


#: (name, lines, granularity, row_group_bytes, after_offset): the seal jobs the golden tests run.
def golden_jobs() -> list[tuple[str, list[dict[str, Any]], str, int, int | None]]:
    return [
        ("every_kind_by_day", golden_lines(), "day", 32 * 1024 * 1024, None),
        ("every_kind_by_month", golden_lines(), "month", 32 * 1024 * 1024, None),
        ("every_kind_by_hour", golden_lines(), "hour", 32 * 1024 * 1024, None),
        ("every_kind_unpartitioned", golden_lines(), "none", 32 * 1024 * 1024, None),
        ("a_reseal_after_an_offset", golden_lines(), "day", 32 * 1024 * 1024, 104),
        ("small_row_groups", many_lines(60), "day", 2048, None),
    ]
