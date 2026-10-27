"""Kafka admin helpers for deleting runtime-owned topics and consumer groups."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from importlib import import_module
import logging
import os
from typing import Any, Protocol, TypeVar

from midstream.kafka.tls import kafka_ssl_context
from midstream.utils.background_loop import AdapterTimeoutError, BackgroundLoop

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

NON_EMPTY_GROUP_ERROR_CODE = 68
#: Seconds an admin call may take, connecting included, before it is cancelled. aiokafka gives up
#: on each request after 40 s (`request_timeout_ms`), but not on a call that makes several: the
#: first call bootstraps the client, and deleting a group finds its coordinator first. Three
#: requests in a row is 120 s, so aiokafka's own timeouts fire first and this only ends a call
#: that would otherwise never finish.
ADMIN_CALL_TIMEOUT_SECONDS = 120.0
#: Seconds to wait for a client that failed to start to close its connections.
CLOSE_TIMEOUT_SECONDS = 5.0


class AsyncKafkaAdminClient(Protocol):
    async def list_topics(self) -> list[str]:
        ...

    async def delete_topics(self, topics: list[str]) -> None:
        ...

    async def list_consumer_groups(self) -> list[str]:
        ...

    async def delete_consumer_group(self, group_id: str) -> None:
        ...

    async def consumer_group_state(self, group_id: str) -> str:
        ...

    async def commit_group_offsets(self, group_id: str, offsets: dict[str, dict[int, int]]) -> None:
        ...

    async def describe_consumer_group(self, group_id: str) -> ConsumerGroupDescription:
        ...


class KafkaAdminClient(Protocol):
    """Synchronous facade used by workflow steps to delete runtime-owned Kafka resources.

    An implementation may also offer `<method>_async` variants, which the workflow agent awaits
    instead so that its event loop keeps running while Kafka answers.
    """

    def list_topics(self) -> list[str]:
        ...

    def delete_topics(self, topics: list[str]) -> None:
        ...

    def list_consumer_groups(self) -> list[str]:
        ...

    def delete_consumer_group(self, group_id: str) -> None:
        ...

    def consumer_group_state(self, group_id: str) -> str:
        """The group's state as Kafka reports it - `Empty`, `Stable`, `Dead`, ... - or `Dead` for none."""
        ...

    def commit_group_offsets(self, group_id: str, offsets: dict[str, dict[int, int]]) -> None:
        """Set where an empty group resumes each partition: `offsets[topic][partition]`, the next offset."""
        ...


#: The states in which a group has no member to hold an assignment.
EMPTY_GROUP_STATES = frozenset({"Empty", "Dead"})


@dataclass(frozen=True, slots=True)
class GroupMemberDescription:
    """One member of a consumer group, as DescribeGroups reports it: its metadata and assignment
    still encoded, in the group protocol's own format (the consumer protocol, for consumers)."""

    member_id: str
    client_id: str
    client_host: str
    metadata: bytes
    assignment: bytes


@dataclass(frozen=True, slots=True)
class ConsumerGroupDescription:
    group_id: str
    state: str
    protocol_type: str = ""
    protocol: str = ""
    members: tuple[GroupMemberDescription, ...] = ()


class ConsumerGroupNotEmptyError(RuntimeError):
    """Raised when Kafka refuses to delete a consumer group that still has members."""

    def __init__(self, group_id: str) -> None:
        self.group_id = group_id
        super().__init__(f"consumer group {group_id} is not empty")


class AsyncRunner(BackgroundLoop):
    """Run asyncio coroutines on a dedicated background loop for sync callers.

    `run()` waits as long as the coroutine takes unless given a `timeout`, and `submit()` never
    waits; `kafka/metadata.py` runs its client on one of these too.
    """

    def __init__(self, *, name: str = "kafka-admin") -> None:
        super().__init__(name=name)


class AIOKafkaAdminClient:
    """Synchronous facade over aiokafka admin operations used by workflow handlers.

    Each call waits at most `call_timeout_seconds` and is cancelled at that deadline with
    `AdapterTimeoutError`; the next call then connects afresh, since the connection that stopped
    answering may be why. The `*_async` variants await the same calls from another event loop.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        security_protocol: str | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        client_factory: Any | None = None,
        call_timeout_seconds: float = ADMIN_CALL_TIMEOUT_SECONDS,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers is required for kafka admin")
        if call_timeout_seconds <= 0:
            raise ValueError("call_timeout_seconds must be > 0")
        self.bootstrap_servers = bootstrap_servers.strip()
        self.security_protocol = None if security_protocol in (None, "") else security_protocol.strip()
        self.sasl_mechanism = None if sasl_mechanism in (None, "") else sasl_mechanism.strip()
        self.sasl_username = None if sasl_username in (None, "") else sasl_username
        self.sasl_password = None if sasl_password in (None, "") else sasl_password
        self._client_factory = _build_admin_client if client_factory is None else client_factory
        self.call_timeout_seconds = call_timeout_seconds
        self._runner = AsyncRunner()
        self._admin: Any = None

    @classmethod
    def from_environment(cls) -> "AIOKafkaAdminClient":
        bootstrap_servers = os.getenv("K_KAFKA_BOOTSTRAP_SERVERS", "")
        if not bootstrap_servers.strip():
            raise RuntimeError("K_KAFKA_BOOTSTRAP_SERVERS is required for kafka admin")
        return cls(
            bootstrap_servers=bootstrap_servers,
            security_protocol=os.getenv("K_KAFKA_SECURITY_PROTOCOL"),
            sasl_mechanism=os.getenv("K_KAFKA_SASL_MECHANISM"),
            sasl_username=os.getenv("K_KAFKA_AUTH_USERNAME"),
            sasl_password=os.getenv("K_KAFKA_AUTH_PASSWORD"),
        )

    def list_topics(self) -> list[str]:
        return self._run(self._list_topics(), "list_topics")

    def consumer_group_state(self, group_id: str) -> str:
        return self._run(self._consumer_group_state(group_id), "consumer_group_state")

    async def consumer_group_state_async(self, group_id: str) -> str:
        return await self._run_async(self._consumer_group_state(group_id), "consumer_group_state")

    def commit_group_offsets(self, group_id: str, offsets: dict[str, dict[int, int]]) -> None:
        self._run(self._commit_group_offsets(group_id, offsets), "commit_group_offsets")

    async def commit_group_offsets_async(self, group_id: str, offsets: dict[str, dict[int, int]]) -> None:
        await self._run_async(self._commit_group_offsets(group_id, offsets), "commit_group_offsets")

    def describe_consumer_group(self, group_id: str) -> ConsumerGroupDescription:
        """The group's state and members, each with its metadata and assignment (still encoded)."""
        return self._run(self._describe_consumer_group(group_id), "describe_consumer_group")

    async def describe_consumer_group_async(self, group_id: str) -> ConsumerGroupDescription:
        """`describe_consumer_group`, awaited without blocking the caller's event loop."""
        return await self._run_async(self._describe_consumer_group(group_id), "describe_consumer_group")

    def delete_topics(self, topics: list[str]) -> None:
        self._run(self._delete_topics(topics), "delete_topics")

    def list_consumer_groups(self) -> list[str]:
        return self._run(self._list_consumer_groups(), "list_consumer_groups")

    def delete_consumer_group(self, group_id: str) -> None:
        self._run(self._delete_consumer_group(group_id), f"delete_consumer_group {group_id}")

    async def list_topics_async(self) -> list[str]:
        """`list_topics`, awaited without blocking the caller's event loop."""
        return await self._run_async(self._list_topics(), "list_topics")

    async def delete_topics_async(self, topics: list[str]) -> None:
        """`delete_topics`, awaited without blocking the caller's event loop."""
        await self._run_async(self._delete_topics(topics), "delete_topics")

    async def list_consumer_groups_async(self) -> list[str]:
        """`list_consumer_groups`, awaited without blocking the caller's event loop."""
        return await self._run_async(self._list_consumer_groups(), "list_consumer_groups")

    async def delete_consumer_group_async(self, group_id: str) -> None:
        """`delete_consumer_group`, awaited without blocking the caller's event loop."""
        await self._run_async(self._delete_consumer_group(group_id), f"delete_consumer_group {group_id}")

    def _run(self, coroutine: Coroutine[Any, Any, T], operation: str) -> T:
        try:
            return self._runner.run(
                coroutine,
                timeout=self.call_timeout_seconds,
                description=f"Kafka admin {operation}",
            )
        except AdapterTimeoutError:
            self._runner.submit(self._abandon_admin())
            raise

    async def _run_async(self, coroutine: Coroutine[Any, Any, T], operation: str) -> T:
        try:
            return await self._runner.run_async(
                coroutine,
                timeout=self.call_timeout_seconds,
                description=f"Kafka admin {operation}",
            )
        except AdapterTimeoutError:
            self._runner.submit(self._abandon_admin())
            raise

    async def _abandon_admin(self) -> None:
        # On the runner's loop, after the abandoned call's cancellation and before the next call.
        admin, self._admin = self._admin, None
        if admin is not None:
            await _close_quietly(admin)

    async def _get_admin(self) -> Any:
        if self._admin is not None:
            return self._admin
        admin = self._client_factory(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol or "PLAINTEXT",
            ssl_context=kafka_ssl_context(self.security_protocol),
            sasl_mechanism=self.sasl_mechanism or "PLAIN",
            sasl_plain_username=self.sasl_username,
            sasl_plain_password=self.sasl_password,
        )
        try:
            await admin.start()
        except BaseException:
            # A start that failed, or was cancelled at its deadline, can leave connections open;
            # the next call starts a client of its own.
            await _close_quietly(admin)
            raise
        self._admin = admin
        return admin

    async def _list_topics(self) -> list[str]:
        admin = await self._get_admin()
        return sorted(await admin.list_topics())

    async def _delete_topics(self, topics: list[str]) -> None:
        if not topics:
            return
        admin = await self._get_admin()
        response = await admin.delete_topics(topics)
        for topic_name, error_code in getattr(response, "topic_error_codes", []):
            if error_code:
                raise _error_for_code(error_code)(f"failed to delete topic {topic_name}")

    async def _list_consumer_groups(self) -> list[str]:
        admin = await self._get_admin()
        groups = await admin.list_consumer_groups()
        result: list[str] = []
        for group in groups:
            if isinstance(group, tuple) and group:
                result.append(str(group[0]))
                continue
            group_name = getattr(group, "group", None)
            if isinstance(group_name, str):
                result.append(group_name)
        return sorted(result)

    async def _consumer_group_state(self, group_id: str) -> str:
        admin = await self._get_admin()
        for response in await admin.describe_consumer_groups([group_id]):
            for group in getattr(response, "groups", []):
                # (error_code, group_id, state, protocol_type, protocol, members, ...)
                error_code, described_id, state = group[0], group[1], group[2]
                if described_id != group_id:
                    continue
                if error_code:
                    raise _error_for_code(error_code)(f"failed to describe consumer group {group_id}")
                return str(state) or "Dead"
        return "Dead"

    async def _describe_consumer_group(self, group_id: str) -> ConsumerGroupDescription:
        admin = await self._get_admin()
        for response in await admin.describe_consumer_groups([group_id]):
            for group in getattr(response, "groups", []):
                # (error_code, group_id, state, protocol_type, protocol, members, ...)
                error_code, described_id = group[0], group[1]
                if described_id != group_id:
                    continue
                if error_code:
                    raise _error_for_code(error_code)(f"failed to describe consumer group {group_id}")
                members = tuple(
                    # (member_id, client_id, client_host, member_metadata, member_assignment); a later
                    # protocol version puts the instance id second, and is read the same way.
                    GroupMemberDescription(
                        member_id=str(member[0]),
                        client_id=str(member[-4]),
                        client_host=str(member[-3]),
                        metadata=bytes(member[-2] or b""),
                        assignment=bytes(member[-1] or b""),
                    )
                    for member in group[5]
                )
                return ConsumerGroupDescription(
                    group_id=group_id,
                    state=str(group[2]) or "Dead",
                    protocol_type=str(group[3] or ""),
                    protocol=str(group[4] or ""),
                    members=members,
                )
        return ConsumerGroupDescription(group_id=group_id, state="Dead")

    async def _commit_group_offsets(self, group_id: str, offsets: dict[str, dict[int, int]]) -> None:
        """Commit offsets for a group no member holds, as Kafka's admin tools do: generation -1, no member.

        Kafka refuses this for a group with members (`UNKNOWN_MEMBER_ID`), which is the guard that it
        never moves a running consumer.
        """
        if not any(offsets.values()):
            return
        admin = await self._get_admin()
        topics = [
            (topic, [(partition, offset, "") for partition, offset in sorted(partitions.items())])
            for topic, partitions in sorted(offsets.items())
            if partitions
        ]
        request = _offset_commit_request_type()(group_id, -1, "", -1, topics)
        coordinator_id = await admin.find_coordinator(group_id)
        response = await admin._send_request(request, coordinator_id)
        for topic, partitions in getattr(response, "topics", []):
            for partition, error_code in partitions:
                if error_code:
                    raise _error_for_code(error_code)(
                        f"failed to commit the offset of {topic} partition {partition} for consumer group {group_id}"
                    )

    async def _delete_consumer_group(self, group_id: str) -> None:
        admin = await self._get_admin()
        request = _delete_groups_request_type()([group_id])
        coordinator_id = await admin.find_coordinator(group_id)
        response = await admin._send_request(request, coordinator_id)
        for deleted_group_id, error_code in getattr(response, "results", []):
            if error_code == NON_EMPTY_GROUP_ERROR_CODE:
                raise ConsumerGroupNotEmptyError(str(deleted_group_id))
            if error_code:
                raise _error_for_code(error_code)(
                    f"failed to delete consumer group {deleted_group_id}"
                )


async def _close_quietly(admin: Any) -> None:
    close = getattr(admin, "close", None)
    if not callable(close):
        return
    try:
        await asyncio.wait_for(close(), CLOSE_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - closing is cleanup; the call's own error matters
        LOGGER.warning("Kafka admin client that was given up on did not close: %r", exc)


def _build_admin_client(**kwargs: Any) -> Any:
    try:
        aiokafka_admin_cls = import_module("aiokafka.admin").AIOKafkaAdminClient
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("aiokafka must be installed for kafka admin cleanup") from exc
    return aiokafka_admin_cls(**kwargs)


def _error_for_code(error_code: int):
    return import_module("aiokafka.errors").for_code(error_code)


def _delete_groups_request_type():
    return import_module("aiokafka.protocol.admin").DeleteGroupsRequest


def _offset_commit_request_type():
    # Group, generation, member, retention time, topics: sent as v2 or v3, whichever the broker takes.
    return import_module("aiokafka.protocol.commit").OffsetCommitRequest
