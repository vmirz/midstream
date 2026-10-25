"""An in-memory Google Cloud Storage with object generations, for the lakehouse tests.

Honours `if_generation_match` the way GCS does (0: the object must not exist; N: it must exist at
generation N), raises the library's own `PreconditionFailed` and `NotFound`, and supports range
downloads and prefix listings. Failures can be injected per operation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
import threading
from typing import Any

from google.api_core.exceptions import NotFound, PreconditionFailed


@dataclass
class StoredObject:
    data: bytes
    generation: int
    content_type: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    created: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


class FakeGcs:
    """The whole store: `(bucket, name) -> StoredObject`."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], StoredObject] = {}
        self._generation = 1_000
        self._lock = threading.RLock()
        #: `(operation, bucket, name) -> exception or None`; consulted before each operation.
        self.fail: Callable[[str, str, str], BaseException | None] | None = None
        self.calls: list[tuple[str, str, str]] = []
        #: When a written object is created; tests move it to age objects.
        self.clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC)

    def client(self) -> "FakeClient":
        return FakeClient(self)

    def next_generation(self) -> int:
        with self._lock:
            self._generation += 1
            return self._generation

    def check(self, operation: str, bucket: str, name: str) -> None:
        self.calls.append((operation, bucket, name))
        if self.fail is not None:
            error = self.fail(operation, bucket, name)
            if error is not None:
                raise error

    def put(self, uri: str, data: bytes) -> int:
        bucket, name = _split(uri)
        generation = self.next_generation()
        self.objects[(bucket, name)] = StoredObject(data=data, generation=generation, created=self.clock())
        return generation

    def get(self, uri: str) -> bytes | None:
        stored = self.objects.get(_split(uri))
        return None if stored is None else stored.data

    def names(self, prefix: str = "") -> list[str]:
        return sorted(f"gs://{bucket}/{name}" for bucket, name in self.objects if f"gs://{bucket}/{name}".startswith(prefix))


class FakeClient:
    def __init__(self, store: FakeGcs) -> None:
        self.store = store

    def bucket(self, name: str) -> "FakeBucket":
        return FakeBucket(self.store, name)

    def list_blobs(self, bucket_or_name, prefix: str | None = None, **_: Any):
        bucket = bucket_or_name if isinstance(bucket_or_name, str) else bucket_or_name.name
        return FakeBucket(self.store, bucket).list_blobs(prefix=prefix)


class FakeBucket:
    def __init__(self, store: FakeGcs, name: str) -> None:
        self.store = store
        self.name = name

    def blob(self, name: str) -> "FakeBlob":
        return FakeBlob(self, name)

    def get_blob(self, name: str) -> "FakeBlob | None":
        self.store.check("get_blob", self.name, name)
        stored = self.store.objects.get((self.name, name))
        if stored is None:
            return None
        blob = FakeBlob(self, name)
        blob._load(stored)
        return blob

    def list_blobs(self, prefix: str | None = None, **_: Any):
        self.store.check("list_blobs", self.name, prefix or "")
        result = []
        for (bucket, name), stored in sorted(self.store.objects.items()):
            if bucket == self.name and name.startswith(prefix or ""):
                blob = FakeBlob(self, name)
                blob._load(stored)
                result.append(blob)
        return iter(result)


class FakeBlob:
    def __init__(self, bucket: FakeBucket, name: str) -> None:
        self.bucket = bucket
        self.name = name
        self.generation: int | None = None
        self.size: int | None = None
        self.metadata: dict[str, str] | None = None
        self.content_type: str | None = None
        self.time_created: datetime | None = None
        self.updated: datetime | None = None

    def _load(self, stored: StoredObject) -> None:
        self.generation = stored.generation
        self.size = len(stored.data)
        self.content_type = stored.content_type
        self.time_created = stored.created
        self.updated = stored.created
        if self.metadata is None:
            self.metadata = dict(stored.metadata)

    @property
    def _key(self) -> tuple[str, str]:
        return (self.bucket.name, self.name)

    def _precondition(self, if_generation_match: int | None) -> None:
        stored = self.bucket.store.objects.get(self._key)
        if if_generation_match is None:
            return
        if if_generation_match == 0:
            if stored is not None:
                raise PreconditionFailed(f"{self.name} exists")
            return
        if stored is None or stored.generation != int(if_generation_match):
            raise PreconditionFailed(f"{self.name} is not at generation {if_generation_match}")

    def upload_from_string(self, data, content_type: str | None = None, if_generation_match: int | None = None, **_: Any) -> None:
        store = self.bucket.store
        store.check("upload", self.bucket.name, self.name)
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        with store._lock:
            self._precondition(if_generation_match)
            generation = store.next_generation()
            store.objects[self._key] = StoredObject(
                data=payload,
                generation=generation,
                content_type=content_type,
                metadata=dict(self.metadata or {}),
                created=store.clock(),
            )
        self.generation = generation
        self.size = len(payload)

    def upload_from_filename(self, filename: str, content_type: str | None = None, if_generation_match: int | None = None, **_: Any) -> None:
        self.upload_from_string(Path(filename).read_bytes(), content_type=content_type, if_generation_match=if_generation_match)

    def download_as_bytes(self, start: int | None = None, end: int | None = None, if_generation_match: int | None = None, **_: Any) -> bytes:
        store = self.bucket.store
        store.check("download", self.bucket.name, self.name)
        stored = store.objects.get(self._key)
        if stored is None:
            raise NotFound(self.name)
        if if_generation_match is not None and stored.generation != int(if_generation_match):
            raise PreconditionFailed(self.name)
        self._load(stored)
        data = stored.data
        if start is None and end is None:
            return data
        begin = 0 if start is None else start
        finish = len(data) - 1 if end is None else end
        return data[begin : finish + 1]

    def download_to_filename(self, filename: str, **kwargs: Any) -> None:
        Path(filename).write_bytes(self.download_as_bytes(**kwargs))

    def delete(self, if_generation_match: int | None = None, **_: Any) -> None:
        store = self.bucket.store
        store.check("delete", self.bucket.name, self.name)
        with store._lock:
            stored = store.objects.get(self._key)
            if stored is None:
                raise NotFound(self.name)
            if if_generation_match is not None and stored.generation != int(if_generation_match):
                raise PreconditionFailed(self.name)
            del store.objects[self._key]

    def reload(self, **_: Any) -> None:
        stored = self.bucket.store.objects.get(self._key)
        if stored is None:
            raise NotFound(self.name)
        self._load(stored)

    def exists(self, **_: Any) -> bool:
        return self._key in self.bucket.store.objects


def _split(uri: str) -> tuple[str, str]:
    without = uri.removeprefix("gs://")
    bucket, _, name = without.partition("/")
    return bucket, name
