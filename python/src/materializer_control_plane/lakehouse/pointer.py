"""The per-table catalog entry: one GCS object naming the table's current metadata file.

`<location>/metadata/catalog-pointer.json` is the whole catalog for a table. Committing writes a new
metadata file (create-only) and then replaces the pointer on the condition that its object
generation is still the one read: GCS's compare-and-swap. A pointer can therefore never move
backwards, and two writers can never both win.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
import re
from typing import Any

from materializer_control_plane.lakehouse.gcs import (
    gcs_uri,
    is_not_found,
    is_precondition_failed,
    parse_gcs_uri,
)

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


def read_pointer(client: Any, location: str) -> tuple[TablePointer | None, int]:
    """The table's pointer and its object generation; `(None, 0)` when there is none.

    One request: the download carries the generation it read, so the content and the generation
    always belong together.
    """
    bucket_name, name = parse_gcs_uri(pointer_uri(location))
    blob = client.bucket(bucket_name).blob(name)
    try:
        raw = blob.download_as_bytes()
    except Exception as exc:
        if is_not_found(exc):
            return None, 0
        raise
    generation = getattr(blob, "generation", None)
    if generation is None:
        # A client that did not record the generation of what it downloaded: ask for it.
        fresh = client.bucket(bucket_name).get_blob(name)
        if fresh is None:
            return None, 0
        generation = fresh.generation
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"table pointer {pointer_uri(location)} is not a JSON object")
    return TablePointer.from_dict(payload), int(generation)


def write_pointer(client: Any, location: str, pointer: TablePointer, *, generation: int) -> int:
    """Replace the pointer if its generation is still `generation` (0: create it); return the new one."""
    bucket_name, name = parse_gcs_uri(pointer_uri(location))
    blob = client.bucket(bucket_name).blob(name)
    try:
        blob.upload_from_string(
            json.dumps(pointer.to_dict(), sort_keys=True, separators=(",", ":")),
            content_type="application/json",
            if_generation_match=generation,
        )
    except Exception as exc:
        if is_precondition_failed(exc):
            raise PointerConflictError(
                f"the pointer of table {pointer.materialization_id} moved past generation {generation}"
            ) from exc
        raise
    new_generation = getattr(blob, "generation", None)
    return 0 if new_generation is None else int(new_generation)


def delete_pointer(client: Any, location: str, *, generation: int | None = None) -> bool:
    """Delete the pointer (optionally only at `generation`); False when it was already gone."""
    bucket_name, name = parse_gcs_uri(pointer_uri(location))
    blob = client.bucket(bucket_name).blob(name)
    try:
        if generation is None:
            blob.delete()
        else:
            blob.delete(if_generation_match=generation)
    except Exception as exc:
        if is_not_found(exc):
            return False
        if is_precondition_failed(exc):
            raise PointerConflictError(f"the pointer at {location} moved before it could be deleted") from exc
        raise
    return True


def write_version_hint(client: Any, location: str, metadata_location: str) -> None:
    """Best effort: name the current metadata file for readers that resolve a table by its root.

    DuckDB's `iceberg_scan('<table root>')` reads `metadata/version-hint.text` and opens
    `metadata/<hint>.metadata.json` (experiment E12). A failure here is logged, never raised: the
    pointer is the catalog, the hint only a convenience.
    """
    hint = metadata_location.rsplit("/", 1)[-1]
    if hint.endswith(".metadata.json"):
        hint = hint[: -len(".metadata.json")]
    bucket_name, name = parse_gcs_uri(version_hint_uri(location))
    try:
        client.bucket(bucket_name).blob(name).upload_from_string(hint, content_type="text/plain")
    except Exception as exc:  # noqa: BLE001 - convenience only
        LOGGER.warning("Could not write the version hint of %s: %s", location, exc)


def metadata_version(metadata_location: str) -> int:
    """The version number a metadata file name starts with; -1 when it has none."""
    match = METADATA_FILE_PATTERN.match(metadata_location.rsplit("/", 1)[-1])
    return -1 if match is None else int(match.group(1))


def find_latest_metadata(client: Any, location: str, *, table_uuid: str | None = None) -> str | None:
    """The newest `*.metadata.json` of a table, optionally only one whose `table-uuid` matches.

    Used to repair a lost pointer (handbook 12 §12.13): metadata files are written before the pointer
    moves, so the newest one with the table's uuid is at least as new as the lost pointer's.
    """
    bucket_name, prefix = parse_gcs_uri(f"{location.rstrip('/')}/metadata/")
    candidates: list[tuple[int, str]] = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        version = metadata_version(blob.name)
        if version >= 0:
            candidates.append((version, blob.name))
    for _, name in sorted(candidates, reverse=True):
        if table_uuid is None:
            return gcs_uri(bucket_name, name)
        try:
            payload = json.loads(client.bucket(bucket_name).blob(name).download_as_bytes())
        except Exception:  # noqa: BLE001 - an unreadable file is skipped
            continue
        if str(payload.get("table-uuid")) == table_uuid:
            return gcs_uri(bucket_name, name)
    return None


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
