"""Whether a runtime's members keep restarting, from their pods as the control plane last read them.

A heartbeat comes from a member that is up, so a runtime whose members are killed every few minutes
- out of memory, most often - still reports healthy between the kills. The pods say otherwise: the
container's restart count, its last termination and why, and Kubernetes' back-off. A member is
unstable while

- it is in `CrashLoopBackOff`: Kubernetes holds off restarting it after repeated failures;
- it was killed for running out of memory (`OOMKilled`) within `RESTART_WINDOW_SECONDS`: every
  such kill loses the member's uncommitted work, which it then reads and writes again;
- it restarted within the window having restarted `FREQUENT_RESTARTS` times since its pod began.

One restart for another reason - a node drain, a deploy - is not instability. Once no member has
restarted within the window, the runtime is stable again, whether or not its pods are read again.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime

from midstream.models import RuntimeMemberState

#: How far back a restart counts.
RESTART_WINDOW_SECONDS = 30 * 60
#: Restarts since a pod began that make one more within the window frequent.
FREQUENT_RESTARTS = 3
CRASH_LOOP = "CrashLoopBackOff"
OUT_OF_MEMORY = "OOMKilled"
#: The phase reason, and the runtime health's reason, of a runtime whose members keep restarting.
RESTARTING_PHASE_REASON = "runtime_members_restarting"
RESTARTING_HEALTH_REASON = "members_restarting"


@dataclass(frozen=True, slots=True)
class RestartInstability:
    """The members that keep restarting, and why, in words for a person."""

    unstable: int
    members: int
    cause: str
    message: str


def restart_instability(members: tuple[RuntimeMemberState, ...] | None, *, now: str) -> RestartInstability | None:
    """The runtime's restarting members as of `now`; None when none is, or the pods were not read."""
    if not members:
        return None
    unstable = [member for member in members if _unstable(member, now=now)]
    if not unstable:
        return None
    cause = _cause(unstable)
    crash_looping = sum(1 for member in unstable if member.waiting_reason == CRASH_LOOP)
    window = RESTART_WINDOW_SECONDS // 60
    count = f"{len(unstable)} of {len(members)} members" if len(members) > 1 else "The member"
    if crash_looping == len(unstable):
        what = "are crash-looping" if len(unstable) > 1 else "is crash-looping"
    else:
        what = f"restarted in the last {window} minutes"
    return RestartInstability(
        unstable=len(unstable),
        members=len(members),
        cause=cause,
        message=f"{count} {what}: {cause}.",
    )


def _unstable(member: RuntimeMemberState, *, now: str) -> bool:
    if member.waiting_reason == CRASH_LOOP:
        return True
    if member.last_exited_at is None:
        return False
    try:
        age = (_parse(now) - _parse(member.last_exited_at)).total_seconds()
    except ValueError:
        return False
    if age > RESTART_WINDOW_SECONDS:
        return False
    return member.last_exit_reason == OUT_OF_MEMORY or member.restarts >= FREQUENT_RESTARTS


def _cause(unstable: list[RuntimeMemberState]) -> str:
    """The most common reason among the unstable members' last exits, as a person would say it."""
    reasons = Counter(member.last_exit_reason for member in unstable if member.last_exit_reason)
    if reasons:
        reason, _ = reasons.most_common(1)[0]
        if reason == OUT_OF_MEMORY:
            return "out of memory"
        codes = Counter(
            member.last_exit_code for member in unstable if member.last_exit_reason == reason and member.last_exit_code is not None
        )
        if codes:
            code, _ = codes.most_common(1)[0]
            return f"exited with code {code}"
        return reason[0].lower() + reason[1:] if reason else "restarting"
    return "crash-looping"


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
