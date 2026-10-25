"""Runtime-local processing state models (process memory only; nothing here is persisted)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

#: A batch is `open` while rows are appended, `rolled` once closed and waiting for its seal,
#: `sealing` while its staged rows become Parquet files (one per partition day), `sealed`, then
#: `uploading`, `uploaded` once its files are in the bucket and announced to the committer (it waits
#: there until a commit covers it), or `failed` (an upload to retry). A batch leaves the member's
#: state when the table's committed offsets cover it.
BATCH_STATES = ("open", "rolled", "sealing", "sealed", "uploading", "uploaded", "failed")
BatchStateName = Literal["open", "rolled", "sealing", "sealed", "uploading", "uploaded", "failed"]
ERROR_KINDS = ("data_validation", "infrastructure")


def _validate_choice(name: str, value: str, choices: tuple[str, ...]) -> None:
    if value not in choices:
        allowed = ", ".join(choices)
        raise ValueError(f"{name} must be one of: {allowed}")


def _validate_non_negative(name: str, value: int | None) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} must be >= 0")


def _validate_numeric_offset_map(name: str, offsets: dict[str, dict[str, int]]) -> None:
    """Offsets by table, then by source partition."""
    for table, partitions in offsets.items():
        for partition, offset in partitions.items():
            if not partition.isdigit():
                raise ValueError(f"{name} keys must be numeric strings")
            if offset < -1:
                raise ValueError(f"{name} values must be >= -1")


@dataclass(frozen=True, slots=True)
class RuntimeMalformedRecord:
    partition: int
    offset: int
    schema_version: int | None
    reason: str
    raw_sample: Any = None
    recorded_at: str | None = None
    # Stable grouping key for skip accounting; `reason` may embed offsets and so never repeats.
    category: str | None = None

    def __post_init__(self) -> None:
        if self.partition < 0:
            raise ValueError("partition must be >= 0")
        if self.offset < 0:
            raise ValueError("offset must be >= 0")
        _validate_non_negative("schema_version", self.schema_version)
        if not self.reason:
            raise ValueError("reason is required")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "partition": self.partition,
            "offset": self.offset,
            "reason": self.reason,
        }
        if self.schema_version is not None:
            payload["schema_version"] = self.schema_version
        if self.raw_sample is not None:
            payload["raw_sample"] = self.raw_sample
        if self.recorded_at is not None:
            payload["recorded_at"] = self.recorded_at
        if self.category is not None:
            payload["category"] = self.category
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeMalformedRecord":
        return cls(
            partition=value["partition"],
            offset=value["offset"],
            schema_version=value.get("schema_version"),
            reason=value["reason"],
            raw_sample=value.get("raw_sample"),
            recorded_at=value.get("recorded_at"),
            category=value.get("category"),
        )


@dataclass(frozen=True, slots=True)
class RuntimeSkipReason:
    """How many records one member skipped for one reason, and where the latest one was."""

    category: str
    count: int
    last_reason: str
    last_seen_at: str | None = None
    last_partition: int | None = None
    last_offset: int | None = None
    last_schema_version: int | None = None

    def __post_init__(self) -> None:
        if not self.category:
            raise ValueError("category is required")
        _validate_non_negative("count", self.count)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "category": self.category,
            "count": self.count,
            "last_reason": self.last_reason,
        }
        optional_values = {
            "last_seen_at": self.last_seen_at,
            "last_partition": self.last_partition,
            "last_offset": self.last_offset,
            "last_schema_version": self.last_schema_version,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload[key] = value
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeSkipReason":
        return cls(
            category=value["category"],
            count=value.get("count", 0),
            last_reason=value.get("last_reason") or value["category"],
            last_seen_at=value.get("last_seen_at"),
            last_partition=value.get("last_partition"),
            last_offset=value.get("last_offset"),
            last_schema_version=value.get("last_schema_version"),
        )


@dataclass(frozen=True, slots=True)
class RuntimeWriteResult:
    rows_increment: int = 0
    bytes_increment: int = 0
    files_increment: int = 0
    records_skipped_increment: int = 0
    last_error: str | None = None
    last_error_kind: str | None = None
    last_file_path: str | None = None
    last_malformed_record: RuntimeMalformedRecord | None = None

    def __post_init__(self) -> None:
        _validate_non_negative("rows_increment", self.rows_increment)
        _validate_non_negative("bytes_increment", self.bytes_increment)
        _validate_non_negative("files_increment", self.files_increment)
        _validate_non_negative("records_skipped_increment", self.records_skipped_increment)
        if self.last_error_kind is not None:
            _validate_choice("last_error_kind", self.last_error_kind, ERROR_KINDS)


@dataclass(frozen=True, slots=True)
class RuntimeSourceRecordMetadata:
    materialization_id: str
    source_topic: str
    partition: int
    offset: int
    member_id: str
    observed_at: str
    #: The Kafka record timestamp, millisecond ISO-8601 UTC; None when Kafka reports none.
    timestamp: str | None = None
    #: The record key as text (`b64:` + base64 when it is not UTF-8); None for no key.
    key: str | None = None
    #: The record headers as `[name, value]` pairs, values decoded like the key.
    headers: tuple[tuple[str, str | None], ...] = ()

    def __post_init__(self) -> None:
        for name in ("materialization_id", "source_topic", "member_id", "observed_at"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        if self.partition < 0:
            raise ValueError("partition must be >= 0")
        if self.offset < 0:
            raise ValueError("offset must be >= 0")


@dataclass(frozen=True, slots=True)
class SealedFile:
    """One Parquet file a seal produced: all of a batch's rows for one partition value."""

    local_path: str
    uri: str
    partition_text: str | None
    partition_value: dict[str, str | None]
    record_count: int
    file_size: int
    event_time_min: str | None = None
    event_time_max: str | None = None
    uploaded: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "local_path": self.local_path,
            "uri": self.uri,
            "partition_text": self.partition_text,
            "partition_value": dict(self.partition_value),
            "record_count": self.record_count,
            "file_size": self.file_size,
            "event_time_min": self.event_time_min,
            "event_time_max": self.event_time_max,
            "uploaded": self.uploaded,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SealedFile":
        return cls(
            local_path=value["local_path"],
            uri=value["uri"],
            partition_text=value.get("partition_text"),
            partition_value=dict(value.get("partition_value") or {}),
            record_count=int(value.get("record_count") or 0),
            file_size=int(value.get("file_size") or 0),
            event_time_min=value.get("event_time_min"),
            event_time_max=value.get("event_time_max"),
            uploaded=bool(value.get("uploaded", False)),
        )


@dataclass(frozen=True, slots=True)
class RuntimeBatchState:
    batch_id: str
    materialization_id: str
    #: The table the batch's rows go to: its source topic's.
    table: str
    owner_member_id: str
    partition: int
    #: The staged rows (JSONL), kept until a commit covers the batch: a rejected seal is re-sealed
    #: from it.
    local_path: str
    state: BatchStateName
    start_offset: int
    end_offset: int
    #: Where this batch continues its partition from: every record in `(after_offset, end_offset]`
    #: is in this batch or needed no row (lakehouse/commits.py).
    after_offset: int = -1
    session_generation: int | None = None
    row_count: int = 0
    #: Staged JSONL bytes: the roll policy's and the backlog's measure.
    estimated_bytes: int = 0
    upload_attempts: int = 0
    seal_attempts: int = 0
    last_error: str | None = None
    event_time_min: str | None = None
    event_time_max: str | None = None
    created_at: str | None = None
    rolled_at: str | None = None
    updated_at: str | None = None
    #: Names one seal of this batch; a re-seal gets a new one.
    seal_id: str | None = None
    files: tuple[SealedFile, ...] = ()
    #: The schema the seal conformed to.
    schema_id: int | None = None
    unmapped_values: int = 0
    #: Drift observations among the staged rows (keys, bounded): the seal waits for the schema
    #: to take them, up to `seal_schema_wait_seconds`.
    observations: tuple[str, ...] = ()
    announced_at: str | None = None
    announce_attempts: int = 0
    reseals: int = 0

    def __post_init__(self) -> None:
        for name in ("batch_id", "materialization_id", "table", "owner_member_id", "local_path"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        if self.partition < 0:
            raise ValueError("partition must be >= 0")
        _validate_choice("state", self.state, BATCH_STATES)
        _validate_non_negative("start_offset", self.start_offset)
        _validate_non_negative("end_offset", self.end_offset)
        if self.end_offset < self.start_offset:
            raise ValueError("end_offset must be >= start_offset")
        if self.after_offset >= self.start_offset:
            raise ValueError("after_offset must be below start_offset")
        _validate_non_negative("row_count", self.row_count)
        _validate_non_negative("estimated_bytes", self.estimated_bytes)
        _validate_non_negative("upload_attempts", self.upload_attempts)
        _validate_non_negative("seal_attempts", self.seal_attempts)

    @property
    def sealed_bytes(self) -> int:
        return sum(item.file_size for item in self.files)

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "materialization_id": self.materialization_id,
            "table": self.table,
            "owner_member_id": self.owner_member_id,
            "partition": self.partition,
            "local_path": self.local_path,
            "state": self.state,
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "after_offset": self.after_offset,
            "session_generation": self.session_generation,
            "row_count": self.row_count,
            "estimated_bytes": self.estimated_bytes,
            "upload_attempts": self.upload_attempts,
            "seal_attempts": self.seal_attempts,
            "last_error": self.last_error,
            "event_time_min": self.event_time_min,
            "event_time_max": self.event_time_max,
            "created_at": self.created_at,
            "rolled_at": self.rolled_at,
            "updated_at": self.updated_at,
            "seal_id": self.seal_id,
            "files": [item.to_dict() for item in self.files],
            "schema_id": self.schema_id,
            "unmapped_values": self.unmapped_values,
            "observations": list(self.observations),
            "announced_at": self.announced_at,
            "announce_attempts": self.announce_attempts,
            "reseals": self.reseals,
        }


@dataclass(frozen=True, slots=True)
class RuntimeProcessingSummary:
    materialization_id: str
    member_id: str | None = None
    #: Offsets by table, then by source partition.
    latest_handled_offsets: dict[str, dict[str, int]] = field(default_factory=dict)
    latest_committed_offsets: dict[str, dict[str, int]] = field(default_factory=dict)
    #: Rows, bytes and files written and values unmapped, by table.
    table_counters: dict[str, dict[str, int]] = field(default_factory=dict)
    event_time: str | None = None
    rows_written: int | None = None
    bytes_written: int | None = None
    files_written: int | None = None
    records_skipped: int | None = None
    pending_upload_batches: int | None = None
    pending_upload_bytes: int | None = None
    files_pending_commit: int | None = None
    unmapped_values: int | None = None
    last_error: str | None = None
    last_error_kind: str | None = None
    last_file_path: str | None = None
    last_malformed_record: RuntimeMalformedRecord | None = None
    updated_at: str | None = None
    skip_reasons: tuple[RuntimeSkipReason, ...] = ()

    def __post_init__(self) -> None:
        if not self.materialization_id:
            raise ValueError("materialization_id is required")
        if self.member_id == "":
            raise ValueError("member_id must not be empty")
        _validate_numeric_offset_map("latest_handled_offsets", self.latest_handled_offsets)
        _validate_numeric_offset_map("latest_committed_offsets", self.latest_committed_offsets)
        for name in (
            "rows_written",
            "bytes_written",
            "files_written",
            "records_skipped",
            "pending_upload_batches",
            "pending_upload_bytes",
            "files_pending_commit",
            "unmapped_values",
        ):
            _validate_non_negative(name, getattr(self, name))
        if self.last_error_kind is not None:
            _validate_choice("last_error_kind", self.last_error_kind, ERROR_KINDS)

    @property
    def latest_offsets(self) -> dict[str, dict[str, int]]:
        return self.latest_handled_offsets

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "materialization_id": self.materialization_id,
            "latest_handled_offsets": {table: dict(offsets) for table, offsets in self.latest_handled_offsets.items()},
            "latest_committed_offsets": {table: dict(offsets) for table, offsets in self.latest_committed_offsets.items()},
            "latest_offsets": {table: dict(offsets) for table, offsets in self.latest_handled_offsets.items()},
            "table_counters": {table: dict(counters) for table, counters in self.table_counters.items()},
        }
        if self.member_id is not None:
            payload["member_id"] = self.member_id
        optional_values = {
            "event_time": self.event_time,
            "rows_written": self.rows_written,
            "bytes_written": self.bytes_written,
            "files_written": self.files_written,
            "records_skipped": self.records_skipped,
            "pending_upload_batches": self.pending_upload_batches,
            "pending_upload_bytes": self.pending_upload_bytes,
            "files_pending_commit": self.files_pending_commit,
            "unmapped_values": self.unmapped_values,
            "last_error": self.last_error,
            "last_error_kind": self.last_error_kind,
            "last_file_path": self.last_file_path,
            "updated_at": self.updated_at,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload[key] = value
        if self.last_malformed_record is not None:
            payload["last_malformed_record"] = self.last_malformed_record.to_dict()
        if self.skip_reasons:
            payload["skip_reasons"] = [reason.to_dict() for reason in self.skip_reasons]
        return payload
