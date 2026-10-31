"""An in-memory object store: the store behind the test suite.

Keyed by the full URI, so one instance can stand in for any scheme (`set_store("gs", MemoryStore())`);
URIs are still parsed as a real store parses them, so a malformed one fails here as it would there.
Conditions behave as the contract says, under one lock; versions come from a store-wide counter and
are never reused, even for an object deleted and written again. Like the GCS fake it replaces, it can
inject failures per operation and records every operation it is asked for; `read`, `write`, `names`
and `objects` inspect and arrange its contents without either.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
import threading

from midstream.lakehouse.store.base import (
    ABSENT,
    Absent,
    DeleteFailure,
    DeleteOutcome,
    ObjectInfo,
    ObjectNotFoundError,
    PreconditionFailedError,
    StoreError,
    byte_range,
)
from midstream.lakehouse.store.uris import require_object_uri, require_prefix_uri


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(slots=True)
class StoredObject:
    """One object: its bytes, version, content type, and when it was written (the store's clock)."""

    data: bytes
    version: str
    content_type: str
    created: datetime


class MemoryStore:
    """Objects in a dict, `uri -> StoredObject`.

    `fail(operation, uri)` is consulted before every operation and may return an exception to raise
    instead; operations are named `get`, `head`, `put`, `delete` and `list` (`get_to_file` counts as
    `get`, `put_file` as `put`, and `delete_many` as one `delete` per object, whose failure becomes
    that object's `DeleteFailure`). It runs outside the store's lock, as the GCS fake's did, so it
    may block or act on the store itself to stage a race. `calls` records `(operation, uri)` for
    each, the prefix for `list`. `clock` stamps what is written; tests move it to age objects.
    """

    scheme = "*"
    delete_batch_size = 100
    supports_conditional_delete = True

    def __init__(self) -> None:
        self.objects: dict[str, StoredObject] = {}
        self.fail: Callable[[str, str], BaseException | None] | None = None
        self.calls: list[tuple[str, str]] = []
        self.clock: Callable[[], datetime] = _utc_now
        self._lock = threading.RLock()
        self._counter = 0

    # The contract.

    def get(self, uri: str, *, start: int | None = None, end: int | None = None) -> tuple[bytes, str]:
        """The object's bytes, or the inclusive range `start..end`, and its version."""
        require_object_uri(uri)
        self._check("get", uri)
        with self._lock:
            stored = self._existing(uri)
            first, stop = byte_range(len(stored.data), start, end, uri=uri)
            return stored.data[first:stop], stored.version

    def head(self, uri: str) -> ObjectInfo | None:
        """The object's size, version and creation time; None when there is none."""
        require_object_uri(uri)
        self._check("head", uri)
        with self._lock:
            stored = self.objects.get(uri)
            return None if stored is None else _info(uri, stored)

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write the object if `if_version` holds; return its new version."""
        require_object_uri(uri)
        payload = _bytes(data)
        self._check("put", uri)
        with self._lock:
            return self._put(uri, payload, if_version=if_version, content_type=content_type)

    def put_file(
        self,
        uri: str,
        path: str,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """`put` with the content of the local file at `path`."""
        require_object_uri(uri)
        payload = Path(path).read_bytes()
        self._check("put", uri)
        with self._lock:
            return self._put(uri, payload, if_version=if_version, content_type=content_type)

    def get_to_file(self, uri: str, path: str) -> ObjectInfo:
        """Write the whole object to `path`; return what was read. Nothing is left at `path` on failure."""
        require_object_uri(uri)
        try:
            self._check("get", uri)
            with self._lock:
                stored = self._existing(uri)
                data, info = stored.data, _info(uri, stored)
            Path(path).write_bytes(data)
        except BaseException:
            _remove_quietly(path)
            raise
        return info

    def delete(self, uri: str, *, if_version: str | None = None) -> bool:
        """Delete the object (only at `if_version` when given); False when it was already missing."""
        require_object_uri(uri)
        if if_version is not None and not isinstance(if_version, str):
            raise TypeError(f"if_version must be a version string or None, not {if_version!r}")
        self._check("delete", uri)
        with self._lock:
            return self._delete(uri, if_version=if_version)

    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome:
        """Delete each object; a missing one counts as deleted, an injected failure is reported."""
        for uri in uris:
            require_object_uri(uri)
        deleted: list[str] = []
        failed: list[DeleteFailure] = []
        for uri in uris:
            try:
                self._check("delete", uri)
                with self._lock:
                    self._delete(uri, if_version=None)
            except Exception as exc:  # noqa: BLE001 - every object gets an outcome
                failed.append(DeleteFailure(uri, _status_of(exc), f"{type(exc).__name__}: {exc}", error=exc))
            else:
                deleted.append(uri)
        return DeleteOutcome(deleted=tuple(deleted), failed=tuple(failed))

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        """Every object whose URI starts with `prefix`, in URI order, as the store held them when asked."""
        require_prefix_uri(prefix)
        self._check("list", prefix)
        with self._lock:
            listed = [_info(uri, stored) for uri, stored in sorted(self.objects.items()) if uri.startswith(prefix)]
        return iter(listed)

    # Inspection, outside the contract: no failure injection, nothing recorded.

    def read(self, uri: str) -> bytes | None:
        """The object's bytes, or None."""
        with self._lock:
            stored = self.objects.get(uri)
            return None if stored is None else stored.data

    def write(self, uri: str, data: bytes | str, *, content_type: str = "application/octet-stream") -> str:
        """Write the object unconditionally (text as UTF-8); return its version."""
        payload = data.encode("utf-8") if isinstance(data, str) else _bytes(data)
        with self._lock:
            return self._put(uri, payload, if_version=None, content_type=content_type)

    def names(self, prefix: str = "") -> list[str]:
        """The URIs of the objects under `prefix`, sorted."""
        with self._lock:
            return sorted(uri for uri in self.objects if uri.startswith(prefix))

    # Internals; callers hold the lock, except around `_check`.

    def _check(self, operation: str, uri: str) -> None:
        self.calls.append((operation, uri))
        if self.fail is not None:
            error = self.fail(operation, uri)
            if error is not None:
                raise error

    def _existing(self, uri: str) -> StoredObject:
        stored = self.objects.get(uri)
        if stored is None:
            raise ObjectNotFoundError(f"no object at {uri}", uri=uri, code="NotFound")
        return stored

    def _put(self, uri: str, payload: bytes, *, if_version: str | Absent | None, content_type: str) -> str:
        current = self.objects.get(uri)
        if if_version is ABSENT:
            if current is not None:
                raise PreconditionFailedError(f"{uri} already exists", uri=uri, code="PreconditionFailed")
        elif if_version is not None:
            if not isinstance(if_version, str):
                raise TypeError(f"if_version must be a version string, ABSENT or None, not {if_version!r}")
            if current is None or current.version != if_version:
                raise PreconditionFailedError(f"{uri} is not at version {if_version}", uri=uri, code="PreconditionFailed")
        self._counter += 1
        version = str(self._counter)
        self.objects[uri] = StoredObject(data=payload, version=version, content_type=content_type, created=self.clock())
        return version

    def _delete(self, uri: str, *, if_version: str | None) -> bool:
        current = self.objects.get(uri)
        if current is None:
            return False
        if if_version is not None and current.version != if_version:
            raise PreconditionFailedError(f"{uri} is not at version {if_version}", uri=uri, code="PreconditionFailed")
        del self.objects[uri]
        return True


def _info(uri: str, stored: StoredObject) -> ObjectInfo:
    created = stored.created if stored.created.tzinfo is not None else stored.created.replace(tzinfo=UTC)
    return ObjectInfo(uri=uri, size=len(stored.data), version=stored.version, last_modified=created.astimezone(UTC))


def _bytes(data: bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, str):
        raise TypeError("object data must be bytes, not str: encode it first")
    return bytes(data)


def _status_of(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None) if isinstance(exc, StoreError) else None
    return status if isinstance(status, int) else None


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
