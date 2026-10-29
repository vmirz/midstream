"""Which member of the tail app owns a table, and where it is.

**Owner.** Every tick about a table carries one key, `tail:<materialization id>:<table>`, so the
control plane's producer puts them all in one partition of `materializer-tail-requests`: aiokafka's
default partitioner, murmur2 over the key's UTF-8 bytes (Kaspr's default key serializer is `raw`),
masked positive, modulo the partition count (`owner_partition`; checked on a test cluster against the
maintenance app's ticks, which the same agent sends the same way). Kaspr gives that partition to one
member, and that member owns the table. Each tick's handler compares the partition it arrived on with
the computed one and logs `tail_routing_mismatch` should they ever differ.

**Members.** Any member may receive a request; one that does not own the table forwards it. Where
the owner is comes from the app's consumer group (`describe_consumer_groups`): each member's
assignment names the tick topic's partitions it holds, and the metadata it joined with - Faust's
`ClientMetadata` - names its URL, its `canonical_url`, which Kaspr sets to the pod's address behind
the app's headless service. Both are decoded here, in the Kafka consumer protocol's own layout, from
the group's description (`kafka/admin.py::describe_consumer_group`). When a
member's metadata cannot be read, its URL is built from its member id - a static member's id is its
instance id, the pod's name, then a dash and a UUID - and this member's own host suffix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
import struct
import time
from typing import Any

#: Kafka's murmur2 seed and constants (`org.apache.kafka.common.utils.Utils.murmur2`).
_SEED = 0x9747B28C
_M = 0x5BD1E995
_R = 24
#: A static member's id: its instance id (the pod's name), a dash, and a UUID the broker made.
_STATIC_MEMBER_ID = re.compile(r"^(?P<instance>.+)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def murmur2(data: bytes) -> int:
    """Kafka's murmur2 of `data`, as aiokafka's (and the Java client's) default partitioner hashes keys."""
    length = len(data)
    h = (_SEED ^ length) & 0xFFFFFFFF
    for index in range(length // 4):
        offset = index * 4
        k = data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24)
        k = (k * _M) & 0xFFFFFFFF
        k ^= k >> _R
        k = (k * _M) & 0xFFFFFFFF
        h = (h * _M) & 0xFFFFFFFF
        h ^= k
    extra = length % 4
    tail = length & ~3
    if extra >= 3:
        h ^= data[tail + 2] << 16
    if extra >= 2:
        h ^= data[tail + 1] << 8
    if extra >= 1:
        h ^= data[tail]
        h = (h * _M) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * _M) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def owner_partition(key: str, partitions: int) -> int:
    """The partition of the tick topic the control plane sends `key`'s messages to."""
    if partitions < 1:
        raise ValueError("partitions must be >= 1")
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % partitions


# -- the consumer group ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GroupMember:
    member_id: str
    #: The pod's name: the static member's instance id.
    name: str
    url: str | None
    #: The tick topic's partitions it holds.
    partitions: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class MemberMap:
    """The tick topic's partitions, by the member that holds each, as the group last reported them."""

    state: str = "Unknown"
    members: tuple[GroupMember, ...] = ()
    refreshed_at: float = field(default=0.0)

    def owner(self, partition: int) -> GroupMember | None:
        return next((member for member in self.members if partition in member.partitions), None)

    def describe(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "members": [
                {"member": member.name, "member_id": member.member_id, "url": member.url, "partitions": list(member.partitions)}
                for member in self.members
            ],
            "partitions": {str(partition): member.name for member in self.members for partition in member.partitions},
        }


def member_map(description: Any, *, topic: str, own_url: str | None = None, now: float | None = None) -> MemberMap:
    """The partition -> member map from a consumer group's description (`kafka/admin.py`).

    A member whose metadata does not decode gets the URL its instance id and `own_url` (this
    member's own address) imply.
    """
    stamp = time.monotonic() if now is None else now
    members = tuple(_member(member, topic=topic, own_url=own_url) for member in getattr(description, "members", ()) or ())
    return MemberMap(state=str(getattr(description, "state", "") or "Dead"), members=members, refreshed_at=stamp)


def _member(member: Any, *, topic: str, own_url: str | None) -> GroupMember:
    name = instance_name(member.member_id)
    url = None
    try:
        _version, _subscription, user_data = decode_member_metadata(member.metadata)
        url = (json.loads(user_data) if user_data else {}).get("url") or None
    except (ValueError, struct.error, UnicodeDecodeError, AttributeError):
        url = None
    if url is None:
        url = fallback_url(name, own_url)
    partitions: tuple[int, ...] = ()
    try:
        _version, assigned, _user_data = decode_member_assignment(member.assignment)
        partitions = tuple(sorted(partition for name_, parts in assigned if name_ == topic for partition in parts))
    except (ValueError, struct.error, UnicodeDecodeError):
        partitions = ()
    return GroupMember(member_id=member.member_id, name=name, url=url, partitions=partitions)


def instance_name(member_id: str) -> str:
    """The pod a static member's id names: the id without the dash and UUID the broker added."""
    match = _STATIC_MEMBER_ID.match(member_id)
    return match.group("instance") if match else member_id


def fallback_url(name: str, own_url: str | None) -> str | None:
    """A member's address from its pod's name and this member's own: the same service and port.

    `own_url` is `http://<pod>.<headless service>.<namespace>.svc.cluster.local:<port>`.
    """
    if not own_url or "://" not in own_url:
        return None
    scheme, _, address = own_url.partition("://")
    _pod, dot, rest = address.partition(".")
    if not dot:
        return None
    return f"{scheme}://{name}.{rest}"


def decode_member_metadata(data: bytes) -> tuple[int, list[str], bytes | None]:
    """A member's subscription as the consumer protocol encodes it: version, topics, user data."""
    reader = _Reader(data)
    version = reader.int16()
    topics = [reader.string() for _ in range(reader.int32_count())]
    user_data = reader.bytes()
    return version, topics, user_data


def decode_member_assignment(data: bytes) -> tuple[int, list[tuple[str, list[int]]], bytes | None]:
    """A member's assignment as the consumer protocol encodes it: version, topics' partitions, user data."""
    if not data:
        return 0, [], None
    reader = _Reader(data)
    version = reader.int16()
    assignment: list[tuple[str, list[int]]] = []
    for _ in range(reader.int32_count()):
        topic = reader.string()
        partitions = [reader.int32() for _ in range(reader.int32_count())]
        assignment.append((topic, partitions))
    user_data = reader.bytes() if reader.remaining else None
    return version, assignment, user_data


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = bytes(data)
        self.position = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.position

    def _take(self, size: int) -> bytes:
        if size < 0 or self.position + size > len(self.data):
            raise ValueError("truncated consumer protocol data")
        chunk = self.data[self.position : self.position + size]
        self.position += size
        return chunk

    def int16(self) -> int:
        return struct.unpack(">h", self._take(2))[0]

    def int32(self) -> int:
        return struct.unpack(">i", self._take(4))[0]

    def int32_count(self) -> int:
        count = self.int32()
        return max(count, 0)

    def string(self) -> str:
        size = self.int16()
        return "" if size < 0 else self._take(size).decode("utf-8")

    def bytes(self) -> bytes | None:
        size = self.int32()
        return None if size < 0 else self._take(size)
