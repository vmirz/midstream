"""How the control plane scales a materialization's runtime past its own replica count."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: The lag measurements a materialization's autoscaling keeps, a minute apart: its last quarter hour.
LAG_POINTS_KEPT = 16


@dataclass(frozen=True, slots=True)
class LagPoint:
    """One consumer-lag measurement: records on the source topics beyond the committed offsets."""

    at: str
    lag: int

    def __post_init__(self) -> None:
        if not self.at:
            raise ValueError("at is required")
        if self.lag < 0:
            raise ValueError("lag must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        return {"at": self.at, "lag": self.lag}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "LagPoint":
        return cls(at=value["at"], lag=int(value["lag"]))


@dataclass(frozen=True, slots=True)
class RuntimeAutoscale:
    """A materialization's runtime as the control plane scaled it, and what it decided that from.

    `replicas` is the members its runtime runs while scaled up, more than the materialization's own
    `replicas`; None while it runs its own count. `reason` says why, in words for a person;
    `changed_at` is when the count last changed, up or back. `eta_seconds` is the time to catch up
    at the pace last measured, None while there is no telling. `points` are the lag measurements,
    oldest first.
    """

    materialization_id: str
    replicas: int | None = None
    reason: str | None = None
    changed_at: str | None = None
    eta_seconds: int | None = None
    points: tuple[LagPoint, ...] = ()
    updated_at: str | None = None

    def __post_init__(self) -> None:
        if not self.materialization_id:
            raise ValueError("materialization_id is required")
        if self.replicas is not None and self.replicas < 1:
            raise ValueError("replicas must be >= 1")
        if self.eta_seconds is not None and self.eta_seconds < 0:
            raise ValueError("eta_seconds must be >= 0")

    @property
    def scaled(self) -> bool:
        return self.replicas is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_id": self.materialization_id,
            "replicas": self.replicas,
            "reason": self.reason,
            "changed_at": self.changed_at,
            "eta_seconds": self.eta_seconds,
            "points": [point.to_dict() for point in self.points],
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeAutoscale":
        return cls(
            materialization_id=value["materialization_id"],
            replicas=value.get("replicas"),
            reason=value.get("reason"),
            changed_at=value.get("changed_at"),
            eta_seconds=value.get("eta_seconds"),
            points=tuple(LagPoint.from_dict(point) for point in value.get("points") or ()),
            updated_at=value.get("updated_at"),
        )
