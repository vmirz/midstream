"""Runtime scale inputs (storage size, replicas) and the replica recommendation rule.

The recommendation rule is intentionally isolated in this module and driven by the
``ScaleSizingRules`` dataclass so it can be retuned without touching call sites.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any

#: A runtime member's persistent volume holds only its state table (RocksDB): its tables' schemas
#: and committed offsets, its settings, kept small by its write buffer (`runtime/templates.py`).
#: The batches it stages are on the node's own disk.
DEFAULT_STORAGE_SIZE = "64Mi"
DEFAULT_REPLICAS = 1

# Kubernetes quantity suffixes expressed in bytes.
_BINARY_SUFFIXES = {"Ki": 1024**1, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4}
_DECIMAL_SUFFIXES = {"K": 1000**1, "M": 1000**2, "G": 1000**3, "T": 1000**4}
_QUANTITY_PATTERN = re.compile(r"^(?P<amount>\d+)(?P<suffix>Ki|Mi|Gi|Ti|K|M|G|T)?$")



class InvalidScaleError(ValueError):
    """Raised when a caller supplies an unusable storage size or replica count."""

    def __init__(self, message: str, *, field_name: str) -> None:
        self.field_name = field_name
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ScaleSizingRules:
    """Tunable inputs for the replica recommendation.

    Replica ratio is anchored on the observed convention that a 32-partition topic is
    served by 16 replicas, while topics with very few partitions stay at one replica.
    """

    partitions_per_replica: int = 2
    single_replica_partition_threshold: int = 4
    min_replicas: int = 1
    max_replicas: int = 64


DEFAULT_SIZING_RULES = ScaleSizingRules()


@dataclass(frozen=True, slots=True)
class ScaleRecommendation:
    """Recommended runtime scale for a source topic."""

    replicas: int
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "replicas": self.replicas,
            "rationale": self.rationale,
        }


def normalize_storage_size(value: Any, *, default: str = DEFAULT_STORAGE_SIZE) -> str:
    """Validate a Kubernetes storage quantity, falling back to the default when unset."""
    if value in (None, ""):
        return default
    if isinstance(value, bool) or not isinstance(value, str):
        raise InvalidScaleError("storage_size must be a string", field_name="storage_size")
    candidate = value.strip()
    if _QUANTITY_PATTERN.match(candidate) is None:
        raise InvalidScaleError(
            "storage_size must be a Kubernetes quantity such as 1Gi, 512Mi, or 10G",
            field_name="storage_size",
        )
    if storage_size_to_bytes(candidate) <= 0:
        raise InvalidScaleError("storage_size must be greater than zero", field_name="storage_size")
    return candidate


def normalize_replicas(
    value: Any,
    *,
    default: int = DEFAULT_REPLICAS,
    maximum: int | None = None,
) -> int:
    """Validate a replica count, falling back to the default when unset."""
    if value in (None, ""):
        return default
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise InvalidScaleError("replicas must be an integer", field_name="replicas")
    try:
        replicas = int(value)
    except (TypeError, ValueError) as error:
        raise InvalidScaleError("replicas must be an integer", field_name="replicas") from error
    if replicas < 1:
        raise InvalidScaleError("replicas must be >= 1", field_name="replicas")
    if maximum is not None and replicas > maximum:
        raise InvalidScaleError(f"replicas must be <= {maximum}", field_name="replicas")
    return replicas


def storage_size_to_bytes(value: str) -> int:
    """Convert a Kubernetes storage quantity into bytes."""
    match = _QUANTITY_PATTERN.match(value.strip())
    if match is None:
        raise InvalidScaleError(
            "storage_size must be a Kubernetes quantity such as 1Gi, 512Mi, or 10G",
            field_name="storage_size",
        )
    amount = int(match.group("amount"))
    suffix = match.group("suffix")
    if suffix is None:
        return amount
    multiplier = _BINARY_SUFFIXES.get(suffix) or _DECIMAL_SUFFIXES[suffix]
    return amount * multiplier


def recommend_scale(
    *,
    partitions: int,
    rules: ScaleSizingRules = DEFAULT_SIZING_RULES,
) -> ScaleRecommendation:
    """Recommend replicas for a source topic."""
    replicas = recommend_replicas(partitions=partitions, rules=rules)
    rationale = (
        f"{partitions} partition(s) -> {replicas} replica(s) "
        f"(1 replica per {rules.partitions_per_replica} partitions above "
        f"{rules.single_replica_partition_threshold} partitions)"
    )
    return ScaleRecommendation(replicas=replicas, rationale=rationale)


def assignable_members(partition_counts: list[int]) -> int:
    """The most members a runtime reading topics with these partition counts keeps busy.

    Faust's assignor co-partitions topics with the same number of partitions: partition `i` of
    each goes to the same member. So topics of 32, 32 and 8 partitions give 32 + 8 = 40 members
    work, not 72; a member beyond that reads nothing.
    """
    return sum(set(count for count in partition_counts if count > 0))


def recommend_combined_scale(
    partition_counts: list[int],
    *,
    rules: ScaleSizingRules = DEFAULT_SIZING_RULES,
) -> ScaleRecommendation:
    """Recommend replicas for one runtime reading topics with these partition counts.

    Every partition is work, so replicas follow the partitions of all the topics together, up to
    the members the assignor can keep busy (`assignable_members`).
    """
    total_partitions = sum(partition_counts)
    assignable = max(1, assignable_members(partition_counts))
    replicas = min(recommend_replicas(partitions=total_partitions, rules=rules), assignable)
    rationale = (
        f"{len(partition_counts)} topic(s), {total_partitions} partition(s) together -> {replicas} replica(s) "
        f"(1 replica per {rules.partitions_per_replica} partitions above "
        f"{rules.single_replica_partition_threshold} partitions, at most {assignable}: topics with as "
        f"many partitions share members)"
    )
    return ScaleRecommendation(replicas=replicas, rationale=rationale)


def recommend_replicas(
    *,
    partitions: int,
    rules: ScaleSizingRules = DEFAULT_SIZING_RULES,
) -> int:
    """Recommend a replica count for a partition count."""
    if partitions <= 0:
        return rules.min_replicas
    if partitions <= rules.single_replica_partition_threshold:
        return rules.min_replicas
    replicas = math.ceil(partitions / rules.partitions_per_replica)
    replicas = min(replicas, partitions, rules.max_replicas)
    return max(replicas, rules.min_replicas)
