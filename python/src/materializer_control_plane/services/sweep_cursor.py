"""Where a recurring maintenance sweep over the materializations resumes.

A sweep that covers only part of the fleet — capped by a batch limit, or stopped by a time budget
— must not start from the same materialization every time, or the ones beyond its reach are never
visited. Each sweep therefore starts just after the materialization the previous one reached, and
wraps around to the first.

Positions are by id, not by place in a listing: the table lists materializations partition by
partition, not in id order, and a materialization may be deleted between sweeps. Comparing ids
places the next sweep correctly either way.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class SweepCursor:
    """The id the next sweep starts after; None starts at the first."""

    after: str | None = None

    def order(self, items: Iterable[T], *, key: Callable[[T], str]) -> list[T]:
        """Every item, by id, starting with the first after the cursor and wrapping around."""
        ordered = sorted(items, key=key)
        if self.after is None:
            return ordered
        split = bisect_right([key(item) for item in ordered], self.after)
        return ordered[split:] + ordered[:split]

    def advance(self, reached: str) -> None:
        """Record the last id a sweep reached, for the next one to start after."""
        self.after = reached
