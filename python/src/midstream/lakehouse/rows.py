"""One table row from one staged line: the seal's row, shared with the live tail.

The writer stages each record as a JSON line - `{"m": {"p", "o", "t", "k", "h", "a"}, "v": value}`,
its partition, offset, Kafka timestamp, key, headers, the time it was read, and the value untouched
(`runtime_app/writer/batches.py::staged_line`). A seal turns each line into a row of the table
(`runtime_app/writer/seal.py`); so does the live tail, for records not committed yet
(`tail_app/`). Both call `build_row`, with a `Conformer` of the table's current schema, so a row the
tail serves is the row the seal will write: the same metadata columns, the same placement of every
value - forks, collapsed families, `_unmapped` - and, through `lakehouse/parquet.py`, the same Arrow
schema with its field ids.

The one column that may differ is `_materialized_at`, the time the record was read: the seal takes
the writer's (`a`), the tail its own read time.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from midstream.lakehouse.schema.conform import Conformer, unmapped_text


@dataclass(frozen=True, slots=True)
class BuiltRow:
    """A table row and what the seal and the tail need to know about it."""

    #: The metadata columns, then the payload columns the conformer placed values in; absent
    #: columns are null.
    row: dict[str, Any]
    #: How many values went to `_unmapped`: none, 0.
    unmapped_values: int
    #: The record's Kafka timestamp, parsed: what partitions the row and bounds its file; None
    #: when Kafka gave none.
    moment: datetime | None


def build_row(
    staged: dict[str, Any],
    *,
    conformer: Conformer,
    default_partition: int,
    now: Callable[[], datetime] | None = None,
) -> BuiltRow:
    """One table row from one staged line, exactly as the seal writes it.

    `default_partition` is the Kafka partition of a line that does not name its own; `now` gives the
    read time of a line that has none (`a`), the current time by default.
    """
    metadata = staged.get("m") or {}
    offset = int(metadata["o"])
    payload, unmapped = conformer.conform(staged.get("v"))
    moment = parse_timestamp(metadata.get("t"))
    row = {
        "_kafka_timestamp": moment,
        "_kafka_partition": int(metadata.get("p", default_partition)),
        "_kafka_offset": offset,
        "_kafka_key": metadata.get("k"),
        "_kafka_headers": [tuple(pair) for pair in metadata.get("h") or []] or None,
        "_materialized_at": parse_timestamp(metadata.get("a")) or (now or _utc_now)(),
        "_unmapped": unmapped_text(unmapped),
        **payload,
    }
    return BuiltRow(row=row, unmapped_values=len(unmapped) if unmapped else 0, moment=moment)


def parse_timestamp(value: Any) -> datetime | None:
    """A staged timestamp - UTC ISO-8601, as `writer/context.py::format_timestamp` renders it - or None."""
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)
