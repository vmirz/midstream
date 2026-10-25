"""Offsets reported per table and source partition, as the runtime's heartbeats carry them.

A materialization's runtime reads several topics, one table each, so an offset is named by the
table it belongs to and then by the source partition: `{"orders": {"0": 41, "1": 17}}`.
"""

from __future__ import annotations

from typing import Any


def _is_offset(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_table_offsets(name: str, offsets: dict[str, dict[str, int]]) -> None:
    """Offsets by table and then by source partition: numeric partition keys, values >= 0."""
    for table, partitions in offsets.items():
        if not isinstance(partitions, dict):
            raise ValueError(f"{name} must map each table to its partitions' offsets")
        for partition, offset in partitions.items():
            if not str(partition).isdigit():
                raise ValueError(f"{name} keys must be numeric strings")
            if offset < 0:
                raise ValueError(f"{name} values must be >= 0")


def table_offsets_from(value: Any) -> dict[str, dict[str, int]]:
    """Offsets by table and partition as another component reported them; anything malformed is left out."""
    if not isinstance(value, dict):
        return {}
    result: dict[str, dict[str, int]] = {}
    for table, partitions in value.items():
        if not isinstance(partitions, dict):
            continue
        kept = {
            str(partition): offset
            for partition, offset in partitions.items()
            if str(partition).isdigit() and _is_offset(offset)
        }
        if kept:
            result[str(table)] = kept
    return result
