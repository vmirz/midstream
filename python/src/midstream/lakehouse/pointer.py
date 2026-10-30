"""The per-table catalog entry: one object naming the table's current metadata file.

`<location>/metadata/catalog-pointer.json` is the whole catalog for a table. Committing writes a new
metadata file (create-only) and then replaces the pointer on the condition that it is still at the
version read: the store's compare-and-swap - a GCS generation precondition, an S3 or Azure If-Match
(`lakehouse/store/`). A pointer can therefore never move backwards, and two writers can never both
win.

A version is the store's opaque string, compared for equality only. No two versions of a pointer
can be equal even where a version is a hash of the content (S3's ETag): `TablePointer.sequence`
rises by one on every advance (`advanced`), and nothing but this module writes a pointer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
import re
from typing import Any

from midstream.lakehouse.store import ABSENT, ObjectNotFoundError, ObjectStore, PreconditionFailedError

LOGGER = logging.getLogger(__name__)

POINTER_NAME = "catalog-pointer.json"
DROPPED_POINTER_NAME = "catalog-pointer.dropped.json"
VERSION_HINT_NAME = "version-hint.text"
POINTER_FORMAT = 1
METADATA_FILE_PATTERN = re.compile(r"^(\d+)-([0-9a-fA-F-]+)\.metadata\.json$")


class PointerConflictError(RuntimeError):
    """The pointer moved since it was read: another commit won."""


@dataclass(frozen=True, slots=True)
class TablePointer:
    table_uuid: str
    identifier: tuple[str, ...]
    materialization_id: str
    location: str
    metadata_location: str
    sequence: int
    updated_at: str
    updated_by: str
    previous_metadata_location: str | None = None
    format: int = POINTER_FORMAT

    def __post_init__(self) -> None:
        for name in ("table_uuid", "materialization_id", "location", "metadata_location", "updated_at"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        if self.sequence < 0:
            raise ValueError("sequence must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "table_uuid": self.table_uuid,
            "identifier": list(self.identifier),
            "materialization_id": self.materialization_id,
            "location": self.location,
            "metadata_location": self.metadata_location,
            "previous_metadata_location": self.previous_metadata_location,
            "sequence": self.sequence,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TablePointer":
        return cls(
            format=int(value.get("format", POINTER_FORMAT)),
            table_uuid=str(value["table_uuid"]),
            identifier=tuple(value.get("identifier") or ()),
            materialization_id=str(value["materialization_id"]),
            location=str(value["location"]),
            metadata_location=str(value["metadata_location"]),
            previous_metadata_location=value.get("previous_metadata_location"),
            sequence=int(value.get("sequence", 0)),
            updated_at=str(value["updated_at"]),
            updated_by=str(value.get("updated_by") or ""),
        )

    def advanced(self, metadata_location: str, *, updated_by: str, updated_at: str | None = None) -> "TablePointer":
        """The pointer after a commit that wrote `metadata_location`."""
        return TablePointer(
            format=self.format,
            table_uuid=self.table_uuid,
            identifier=self.identifier,
            materialization_id=self.materialization_id,
            location=self.location,
            metadata_location=metadata_location,
            previous_metadata_location=self.metadata_location,
            sequence=self.sequence + 1,
            updated_at=updated_at or utc_now(),
            updated_by=updated_by,
        )


def pointer_uri(location: str) -> str:
    return f"{location.rstrip('/')}/metadata/{POINTER_NAME}"


def dropped_pointer_uri(location: str) -> str:
    return f"{location.rstrip('/')}/metadata/{DROPPED_POINTER_NAME}"


def version_hint_uri(location: str) -> str:
    return f"{location.rstrip('/')}/metadata/{VERSION_HINT_NAME}"


def read_pointer(store: ObjectStore, location: str) -> tuple[TablePointer | None, str | None]:
    """The table's pointer and the version it was read at; `(None, None)` when there is none.

    One request: the read carries the version of what it read, so the content and the version
    always belong together.
    """
    uri = pointer_uri(location)
    try:
        raw, version = store.get(uri)
    except ObjectNotFoundError:
        return None, None
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"table pointer {uri} is not a JSON object")
    return TablePointer.from_dict(payload), version


def pointer_bytes(pointer: TablePointer) -> bytes:
    """The pointer as it is stored: compact JSON with sorted keys."""
    return json.dumps(pointer.to_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")


def write_pointer(store: ObjectStore, location: str, pointer: TablePointer, *, expected_version: str | None) -> str:
    """Write the pointer if it is still at `expected_version`; return the version written.

    `expected_version=None` creates the pointer, only if the table has none yet. Raises
    `PointerConflictError` when the condition does not hold: another writer got there first.
    """
    try:
        return store.put(
            pointer_uri(location),
            pointer_bytes(pointer),
            if_version=ABSENT if expected_version is None else expected_version,
            content_type="application/json",
        )
    except PreconditionFailedError as exc:
        if expected_version is None:
            raise PointerConflictError(f"the table of {pointer.materialization_id} at {location} already has a pointer") from exc
        raise PointerConflictError(
            f"the pointer of table {pointer.materialization_id} moved past version {expected_version}"
        ) from exc


def delete_pointer(store: ObjectStore, location: str, *, version: str | None = None) -> bool:
    """Delete the pointer (only at `version` when given); False when the store knows it was already
    gone. Raises `PointerConflictError` when it exists at another version. A store without
    conditional deletes (S3) refuses `version` before any request."""
    try:
        return store.delete(pointer_uri(location), if_version=version)
    except PreconditionFailedError as exc:
        raise PointerConflictError(f"the pointer at {location} moved before it could be deleted") from exc


def write_version_hint(store: ObjectStore, location: str, metadata_location: str) -> None:
    """Best effort: name the current metadata file for readers that resolve a table by its root.

    DuckDB's `iceberg_scan('<table root>')` reads `metadata/version-hint.text` and opens
    `metadata/<hint>.metadata.json` (experiment E12). A failure here is logged, never raised: the
    pointer is the catalog, the hint only a convenience.
    """
    hint = metadata_location.rsplit("/", 1)[-1]
    if hint.endswith(".metadata.json"):
        hint = hint[: -len(".metadata.json")]
    try:
        store.put(version_hint_uri(location), hint.encode("utf-8"), content_type="text/plain")
    except Exception as exc:  # noqa: BLE001 - convenience only
        LOGGER.warning("Could not write the version hint of %s: %s", location, exc)


def metadata_version(metadata_location: str) -> int:
    """The version number a metadata file name starts with; -1 when it has none."""
    match = METADATA_FILE_PATTERN.match(metadata_location.rsplit("/", 1)[-1])
    return -1 if match is None else int(match.group(1))


def find_latest_metadata(store: ObjectStore, location: str, *, table_uuid: str | None = None) -> str | None:
    """The newest `*.metadata.json` of a table, optionally only one whose `table-uuid` matches.

    Used to repair a lost pointer (handbook 12 §12.13): metadata files are written before the pointer
    moves, so the newest one with the table's uuid is at least as new as the lost pointer's.
    """
    candidates: list[tuple[int, str]] = []
    for item in store.list(f"{location.rstrip('/')}/metadata/"):
        version = metadata_version(item.uri)
        if version >= 0:
            candidates.append((version, item.uri))
    for _, uri in sorted(candidates, reverse=True):
        if table_uuid is None:
            return uri
        try:
            payload = json.loads(store.get(uri)[0])
        except Exception:  # noqa: BLE001 - an unreadable file is skipped
            continue
        if isinstance(payload, dict) and str(payload.get("table-uuid")) == table_uuid:
            return uri
    return None


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
