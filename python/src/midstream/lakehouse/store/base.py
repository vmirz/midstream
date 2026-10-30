"""The object store contract: everything Midstream asks of the storage a table lives in.

Midstream touches a bucket through a handful of operations - read an object (whole or a byte range)
together with the version read, read its size, version and age, write it unconditionally,
create-only or as a compare-and-swap, delete one, delete many, list a prefix - and nothing else
(`docs/design/storage-backends.md` §3.1, §5.1). `ObjectStore` states them once; Google Cloud
Storage, Amazon S3, Azure Blob Storage, a filesystem and memory implement them, each store chosen per
object URI by its scheme (`registry.py`), so nothing above this package knows which backend it uses.

A version is an opaque string naming one state of one object: GCS's generation, S3's or Azure's
ETag, the file store's stamp, the memory store's counter. It is only ever compared for equality, and
only with versions the same store issued for the same object: never ordered, parsed or reused across
stores (Azure's ETags are random; S3's are content hashes, so equal content gives an equal ETag, which
the pointer's ever-rising sequence makes harmless, §3.3). Backends quote ETags in some answers and not
in others, so every version leaves a store without surrounding quotes (`normalize_version`), and the
store puts back whatever its backend needs when it sends one as a condition. The catalog pointer's
compare-and-swap rests on it (`lakehouse/pointer.py`).

Everything is blocking and every store is thread-safe: callers on an event loop run it on a worker
thread, and one store per scheme serves the whole process.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final, Protocol, runtime_checkable

#: HTTP statuses worth another try besides the 5xx: a timeout and throttling.
RETRYABLE_STATUSES: Final = frozenset({408, 429})


class Absent:
    """The type of `ABSENT`, the condition "the object must not exist yet" (a create-only write)."""

    __slots__ = ()
    _instance: Absent | None = None

    def __new__(cls) -> Absent:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "ABSENT"

    def __reduce__(self) -> tuple[type[Absent], tuple[()]]:
        return (Absent, ())


#: `put(..., if_version=ABSENT)`: create the object only if it does not exist.
ABSENT: Final = Absent()


def is_retryable_status(status: int | None) -> bool:
    """Whether a request that ended with `status` is worth another try.

    None - the request got no answer - 408, 429 and every 5xx are; anything else is an answer that
    would come back the same (GCS's retry strategy, which S3 and Azure share).
    """
    return status is None or status in RETRYABLE_STATUSES or 500 <= status <= 599


def normalize_version(value: str) -> str:
    """A version as it leaves a store: without the double quotes some backends put around an ETag in
    some answers but not in others (Azure's listings leave them out), so equal versions compare equal."""
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1]
    return text


def as_utc(value: datetime | None) -> datetime | None:
    """`value` as an aware UTC time; a naive time is taken to be UTC already."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def check_range(start: int | None, end: int | None) -> None:
    """Refuse a byte range no store can serve: negative offsets, or an end before the start."""
    for name, value in (("start", start), ("end", end)):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ValueError(f"{name} must be a non-negative integer, not {value!r}")
    if start is not None and end is not None and end < start:
        raise ValueError(f"the range {start}-{end} ends before it starts")


def byte_range(size: int, start: int | None, end: int | None, *, uri: str) -> tuple[int, int]:
    """The `[first, stop)` offsets that the inclusive range `start..end` selects in `size` bytes.

    No range is the whole object; `start` alone reads to the end, `end` alone from the first byte,
    and an `end` past the last byte is cut there. A range starting past the last byte raises
    `StoreError` with status 416, as the HTTP stores answer it.
    """
    check_range(start, end)
    if start is None and end is None:
        return 0, size
    first = 0 if start is None else start
    if first >= size:
        shown = f"{first}-" if end is None else f"{first}-{end}"
        raise StoreError(
            f"{uri}: the range {shown} starts past the end of the object ({size} bytes)",
            uri=uri,
            status=416,
            code="InvalidRange",
        )
    stop = size if end is None else min(end + 1, size)
    return first, stop


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    """One object as a store describes it.

    `last_modified` is aware UTC: when the object was written, which the orphan scan's age guard
    reads. None when the store cannot tell; such an object is never old enough to delete.
    """

    uri: str
    size: int
    version: str
    last_modified: datetime | None

    def __post_init__(self) -> None:
        if self.last_modified is not None and self.last_modified.tzinfo is None:
            raise ValueError(f"last_modified of {self.uri} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class DeleteFailure:
    """An object `delete_many` could not delete: it is still where it was.

    `status` is the HTTP status of the answer, None when the request got none. `error` is the
    exception behind the failure when there was one, kept for chaining; it takes no part in equality.
    """

    uri: str
    status: int | None
    message: str
    error: BaseException | None = field(default=None, compare=False, repr=False)

    @property
    def retryable(self) -> bool:
        """Whether deleting it again may succeed: no answer, 408, 429 or a 5xx."""
        return is_retryable_status(self.status)


@dataclass(frozen=True, slots=True)
class DeleteOutcome:
    """What `delete_many` did: each URI it was given is either deleted or failed."""

    deleted: tuple[str, ...] = ()
    failed: tuple[DeleteFailure, ...] = ()


class StoreError(RuntimeError):
    """A storage request failed.

    `status` is the HTTP status the store answered, None when it gave no answer (a connection that
    failed or timed out); `code` the backend's own name for the error when it gives one, such as
    `PreconditionFailed`, `NoSuchKey` or `ConnectionError`. The original exception is chained.
    """

    def __init__(self, message: str, *, uri: str | None = None, status: int | None = None, code: str | None = None) -> None:
        super().__init__(message)
        self.uri = uri
        self.status = status
        self.code = code

    @property
    def retryable(self) -> bool:
        """Whether the same request may succeed later: no answer, 408, 429 or a 5xx."""
        return is_retryable_status(self.status)


class ObjectNotFoundError(StoreError):
    """There is no object at the URI."""

    def __init__(self, message: str, *, uri: str | None = None, status: int | None = 404, code: str | None = None) -> None:
        super().__init__(message, uri=uri, status=status, code=code)

    @property
    def retryable(self) -> bool:
        """Never: a missing object is an answer."""
        return False


class PreconditionFailedError(StoreError):
    """A write's or delete's condition did not hold: the object exists, moved, or is missing.

    For the catalog pointer this is the news that another commit won; for a create-only upload, that
    the object is already there.
    """

    def __init__(self, message: str, *, uri: str | None = None, status: int | None = 412, code: str | None = None) -> None:
        super().__init__(message, uri=uri, status=status, code=code)

    @property
    def retryable(self) -> bool:
        """Never blindly: the caller re-reads and decides again."""
        return False


@runtime_checkable
class ObjectStore(Protocol):
    """One backend's objects, each addressed by its full URI (`gs://bucket/key`, `s3://bucket/key`,
    `abfss://container@account.dfs.core.windows.net/key`, `file:///path`).

    Every store implements these semantics exactly; `check.py` verifies them against a live store.

    Errors: a condition that does not hold raises `PreconditionFailedError`; a missing object raises
    `ObjectNotFoundError` where an operation says so; any other failure of the store raises
    `StoreError` with the HTTP status of the answer, None when there was none, the backend's
    exception chained. A URI of another scheme, a malformed URI or an impossible range raises
    `ValueError` before any request; a problem with a local file (`put_file`'s source,
    `get_to_file`'s target) raises the `OSError` as it is. Transient failures are retried inside each
    operation by the backend's own library, as before; `delete_many` retries nothing.

    Versions are opaque and compared for equality only (see the module's docstring); every version
    a store returns is one it accepts back as `if_version`.
    """

    #: The URI scheme served: `gs`, `s3`, `abfss` or `file`; `*` for a store that serves any.
    scheme: str
    #: The most deletes one request of the backend takes: what to hand `delete_many` at a time.
    delete_batch_size: int

    def get(self, uri: str, *, start: int | None = None, end: int | None = None) -> tuple[bytes, str]:
        """The object's bytes and the version they were read at, from one request.

        One request, so the content and the version always belong together: a compare-and-swap with
        that version succeeds only if nothing has written the object since. `start` and `end` select
        an inclusive byte range: `start` alone reads to the end, `end` alone from the first byte, an
        `end` past the last byte is cut there, and a `start` past it raises `StoreError` with status
        416. Raises `ObjectNotFoundError` when there is no such object.
        """
        ...

    def head(self, uri: str) -> ObjectInfo | None:
        """The object's size, version and last-modified time; None when there is no such object."""
        ...

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write `data` as the whole object, if `if_version` holds; return the version written.

        `if_version=None` writes unconditionally. `ABSENT` writes only if no object exists (create-
        only). A version string writes only if the object is still at that version (compare-and-swap:
        of writers holding the same version, exactly one succeeds). When the condition does not hold
        - the object exists, it has moved on, or it is missing under a version - nothing is written
        and `PreconditionFailedError` is raised.
        """
        ...

    def put_file(
        self,
        uri: str,
        path: str,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """`put` with the content of the local file at `path`, streamed rather than held in memory."""
        ...

    def get_to_file(self, uri: str, path: str) -> ObjectInfo:
        """Write the whole object to the local file `path`, replacing it; return what was read.

        Raises `ObjectNotFoundError` when there is no such object. After any failure nothing is left
        at `path`. `last_modified` is None when the download does not report it.
        """
        ...

    def delete(self, uri: str, *, if_version: str | None = None) -> bool:
        """Delete the object: True when it is gone after the call, False when the store knows it was
        already missing (a store that cannot tell the two apart, such as S3, answers True).

        With `if_version`, only while the object is at that version: when it exists at another,
        nothing is deleted and `PreconditionFailedError` is raised. A missing object is never an
        error, with or without `if_version`.
        """
        ...

    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome:
        """Delete every one of `uris` in as few requests as the backend allows; each URI is reported.

        A missing object counts as deleted. Nothing is retried: each failure carries its HTTP status
        (None when a request got no answer), and the caller retries what `DeleteFailure.retryable`
        allows (`storage/objects.py`). Up to `delete_batch_size` URIs go in one request.
        """
        ...

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        """Every object whose URI starts with `prefix` - a plain string prefix, not a directory -
        with its size, version and last-modified time, in URI order where the backend lists in order.
        Pagination is hidden.
        """
        ...
