"""Pausing a runtime's source partitions for as long as any reason calls for it.

A full backlog stops a member fetching its source partitions: staging more rows would only grow the
local disk while uploads or commits fall behind. Each reason holds the pause under its own name; the
partitions resume when the last reason is released, so one ending never lets another's records
through. (Writers never wait for a schema in 1.0, so the backlog is the only reason today.)
"""

from __future__ import annotations

from dataclasses import dataclass, field

#: Reason held while the member's upload backlog is at its limits.
UPLOAD_BACKLOG = "upload_backlog"


@dataclass(slots=True)
class _SourcePause:
    holds: dict[str, int] = field(default_factory=dict)
    #: The partitions this module paused, to resume when the last hold is released.
    tps: set = field(default_factory=set)


_PAUSES: dict[str, _SourcePause] = {}


def reset_flow() -> None:
    """Forget every pause. Tests use this between cases."""
    _PAUSES.clear()


def acquire(consumer, source_topic: str, reason: str, tps: set) -> bool:
    """Hold the source partitions paused for `reason`; return whether this call paused them."""
    state = _PAUSES.setdefault(source_topic, _SourcePause())
    already_paused = bool(state.holds)
    state.holds[reason] = state.holds.get(reason, 0) + 1
    new_tps = set(tps) - state.tps
    if already_paused and not new_tps:
        return False
    state.tps |= set(tps)
    consumer.pause_partitions(set(tps) if not already_paused else new_tps)
    return not already_paused


def release(consumer, source_topic: str, reason: str) -> set | None:
    """Release one hold for `reason`; return the partitions resumed, if this was the last hold."""
    state = _PAUSES.get(source_topic)
    if state is None or state.holds.get(reason, 0) <= 0:
        return None
    remaining = state.holds[reason] - 1
    if remaining:
        state.holds[reason] = remaining
    else:
        del state.holds[reason]
    if state.holds:
        return None
    tps = _still_assigned(consumer, state.tps)
    state.tps = set()
    if tps:
        consumer.resume_partitions(tps)
    return tps


def held(source_topic: str, reason: str | None = None) -> bool:
    """Whether the source partitions are held paused, for `reason` or for any reason."""
    state = _PAUSES.get(source_topic)
    if state is None:
        return False
    if reason is None:
        return bool(state.holds)
    return state.holds.get(reason, 0) > 0


def reassert(consumer, source_topic: str, tps: set) -> None:
    """Pause the partitions again if anything still holds them.

    Faust resumes every assigned partition when a rebalance completes, which silently ends a pause
    taken before it; the pipeline calls this on each pass to restore one still called for.
    """
    state = _PAUSES.get(source_topic)
    if state is None or not state.holds or not tps:
        return
    state.tps |= set(tps)
    consumer.pause_partitions(set(tps))


def _still_assigned(consumer, tps: set) -> set:
    """The paused partitions this member still owns.

    A rebalance can move a paused partition away; handing Faust one it no longer owns to resume
    would add it to the partitions it fetches.
    """
    assignment = getattr(consumer, "assignment", None)
    if not callable(assignment):
        return set(tps)
    try:
        current = assignment()
    except Exception:
        return set(tps)
    return {tp for tp in tps if tp in (current or ())}
