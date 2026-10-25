"""Deletion of a materialization's tables - their data and metadata objects - from the bucket.

Each table lives under one location, `<destination>/<storage prefix>/<materialization id>/<table>/`:
`data/` and `metadata/`, the catalog pointer among the latter. Dropping a table deletes `data/`
first, then `metadata/`, and the pointer last, so a table that is half deleted stays recognisable
as the materialization's own until the very end. The workflow drops a materialization's tables one
after another.

A materialization can leave millions of objects behind. One request per delete takes hours, so
deletes go to GCS in batch requests of up to `DELETE_BATCH_SIZE` calls, the most the JSON API
takes in one: about a hundredth of the requests. A client that cannot batch — the test doubles,
which only list and delete — deletes one object per request, as before.

Deletion still runs in bounded passes: one pass lists and deletes at most a budget of objects
for at most about `CLEANUP_PASS_MAX_SECONDS`, and says whether anything is left. The workflow
queues another pass until nothing is, so a large destination costs time rather than the control
plane's attention.

Deleted objects drop out of the listing, so a pass needs no cursor: the next one lists whatever
remains. That also makes a pass that died half-way safe to run again, and a delete GCS answers
404 an object already gone.

The control plane serves its API and runs every agent on one event loop, which these calls would
stall for as long as they wait on the network. The workflow therefore runs each pass through
`drop_table_objects_pass_async`, on a thread of its own.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from functools import partial
from http import HTTPStatus
from itertools import islice
import json
import logging
import time
import re
from typing import Any, Protocol
from urllib.parse import urlparse

LOGGER = logging.getLogger(__name__)

#: Deletes in one GCS batch request — the JSON API takes at most 100 calls in one — and how many a
#: pass deletes between checks of its time budget.
DELETE_BATCH_SIZE = 100
#: Objects one cleanup pass lists and deletes at most when each delete is a request of its own: a
#: single GCS listing page.
CLEANUP_PASS_MAX_OBJECTS = 1000
#: The same budget when deletes go in batch requests: ten listing pages, read one at a time. It is
#: what a pass could delete in its time budget at a tenth of a second a batch request, so the time
#: budget is what usually ends a pass; this bounds one against a fast bucket.
CLEANUP_PASS_MAX_BATCHED_OBJECTS = 10_000
#: Seconds after which a pass stops and leaves the rest to the next one, so other workflow events
#: wait no longer than this behind a large cleanup. Checked between delete batches, so a pass can
#: run over by one batch.
CLEANUP_PASS_MAX_SECONDS = 10.0
#: What the last segment of a table's location may be: a table name (`table_names.py`).
_TABLE_DIRECTORY = re.compile(r"^[a-z][a-z0-9_]*$")
#: Waits before retrying the deletes of a batch request that GCS answered 408, 429 or 5xx, or did
#: not answer: google-cloud-storage retries no call in a batch. A retry that would start after the
#: pass's deadline is not made.
BATCH_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)
#: Seconds to wait for a batch request's answer, which GCS sends once it has run every call in it.
BATCH_REQUEST_TIMEOUT_SECONDS = 30.0

# One thread: the workflow agent handles one event at a time, and passes over the same prefixes
# must not overlap. A pool of its own keeps a slow bucket from occupying the loop's default
# executor, which the event loop also uses to resolve hostnames.
_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="destination-cleanup")


class BlobProtocol(Protocol):
    name: str

    def delete(self) -> None: ...


class BucketProtocol(Protocol):
    """What a pass needs of a bucket. A google-cloud-storage bucket also has `client` and
    `delete_blob`, through which its deletes are batched."""

    def list_blobs(self, *, prefix: str) -> Iterable[BlobProtocol]: ...

    def delete_blobs(self, blobs: list[BlobProtocol], on_error: Any = None) -> None: ...


class ObjectStoreClientProtocol(Protocol):
    def bucket(self, bucket_name: str) -> BucketProtocol: ...


class DestinationDeleteError(RuntimeError):
    """A cleanup pass deleted nothing: GCS refused or kept failing every delete it attempted.

    Raised so the workflow retries the pass after its backoff instead of running another at once.
    """


@dataclass(frozen=True, slots=True)
class DestinationCleanupResult:
    """Outcome of deleting a materialization's objects from its destination bucket.

    `complete` is False when a pass stopped at its budget, or left objects it could not delete;
    another pass continues from there. `deleted_count` counts only objects that are gone.
    `failed_count` counts the objects GCS refused or kept failing to delete, which stay listed for
    the next pass. Both stay out of `to_dict()`: the operation record's status says enough.
    """

    bucket: str
    prefixes: tuple[str, ...]
    deleted_count: int
    skipped_reason: str | None = None
    complete: bool = True
    failed_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "bucket": self.bucket,
            "prefixes": list(self.prefixes),
            "deleted_count": self.deleted_count,
        }
        if self.skipped_reason is not None:
            payload["skipped_reason"] = self.skipped_reason
        return payload


def parse_gcs_uri(value: str) -> tuple[str, str]:
    """Split a ``gs://bucket/prefix`` URI into its bucket and prefix parts."""
    parsed = urlparse(value)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ValueError(f"invalid GCS URI: {value}")
    return parsed.netloc, parsed.path.lstrip("/")


def table_object_prefixes(location: str, materialization_id: str) -> tuple[str, tuple[str, ...], str]:
    """The bucket, the prefixes to empty in order (`data/`, `metadata/`), and the pointer's name."""
    if not materialization_id:
        raise ValueError("materialization_id is required for destination cleanup")
    bucket_name, prefix = parse_gcs_uri(location)
    base = prefix.rstrip("/")
    # Guard against a malformed location wiping unrelated objects: a table's location is always a
    # directory named after the table inside one named after its materialization's id
    # (`lakehouse/tables.py::table_location`).
    parent, _, table = base.rpartition("/")
    if parent.rpartition("/")[2] != materialization_id or _TABLE_DIRECTORY.match(table) is None:
        raise ValueError(f"refusing to delete outside the table of {materialization_id}: {location}")
    return bucket_name, (f"{base}/data/", f"{base}/metadata/"), f"{base}/metadata/catalog-pointer.json"


def drop_table_objects(
    *,
    location: str,
    materialization_id: str,
    client: ObjectStoreClientProtocol | None = None,
) -> DestinationCleanupResult:
    """Delete every object of a table, blocking the caller throughout; for tests and tools.

    The workflow runs one pass per event instead, off the event loop.
    """
    table_object_prefixes(location, materialization_id)
    storage_client = build_storage_client() if client is None else client
    deleted_count = 0
    while True:
        result = drop_table_objects_pass(
            location=location,
            materialization_id=materialization_id,
            client=storage_client,
            max_seconds=None,
        )
        deleted_count += result.deleted_count
        if result.complete:
            return replace(result, deleted_count=deleted_count)


def drop_table_objects_pass(
    *,
    location: str,
    materialization_id: str,
    client: ObjectStoreClientProtocol | None = None,
    max_objects: int | None = None,
    max_seconds: float | None = CLEANUP_PASS_MAX_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> DestinationCleanupResult:
    """Delete up to `max_objects` of a table's objects, for about `max_seconds`.

    `max_objects` defaults to `CLEANUP_PASS_MAX_BATCHED_OBJECTS` when the client batches deletes
    and to `CLEANUP_PASS_MAX_OBJECTS` when it does not. `data/` is emptied, then `metadata/` but for
    the pointer, then the pointer goes. The result is `complete` only once the pointer is gone too.
    A pass stops early only after it has tried to delete something, and raises if it deleted
    nothing because those deletes failed - so a chain of passes either finishes, however slow the
    bucket, or fails where the workflow's retries can see it.
    """
    if max_objects is not None and max_objects < 1:
        raise ValueError("max_objects must be >= 1")
    if max_seconds is not None and max_seconds <= 0:
        raise ValueError("max_seconds must be > 0")
    bucket_name, prefixes, pointer_name = table_object_prefixes(location, materialization_id)
    storage_client = build_storage_client() if client is None else client
    bucket = storage_client.bucket(bucket_name)
    deadline = None if max_seconds is None else clock() + max_seconds
    batching_client = _batching_client(bucket)
    if batching_client is None:
        delete_chunk: Callable[[list[BlobProtocol]], _ChunkOutcome] = partial(_delete_one_by_one, bucket)
        budget = CLEANUP_PASS_MAX_OBJECTS if max_objects is None else max_objects
    else:
        delete_chunk = _BatchDeleter(batching_client, bucket, deadline=deadline, clock=clock, sleep=sleep)
        budget = CLEANUP_PASS_MAX_BATCHED_OBJECTS if max_objects is None else max_objects

    deleted_count = 0
    listed_count = 0
    attempted_count = 0
    failures = _Failures()
    complete = True
    try:
        for prefix in prefixes:
            # Read lazily, a chunk at a time: the listing pages by 1000, so memory holds one page
            # however large the budget, and the deadline is checked between chunks.
            listing = (blob for blob in bucket.list_blobs(prefix=prefix) if blob.name != pointer_name)
            while True:
                if attempted_count and _expired(deadline, clock):
                    complete = False
                    break
                wanted = min(DELETE_BATCH_SIZE, budget - listed_count)
                if wanted == 0:
                    # Out of budget inside a prefix that may hold more.
                    complete = False
                    break
                chunk = list(islice(listing, wanted))
                listed_count += len(chunk)
                if chunk:
                    outcome = delete_chunk(chunk)
                    attempted_count += len(chunk)
                    deleted_count += outcome.deleted
                    failures.add(outcome)
                    if outcome.stop:
                        complete = False
                        break
                if len(chunk) < wanted:
                    break  # the prefix is empty
            if not complete:
                break
        if complete and not failures.count:
            if _delete_pointer(bucket, pointer_name):
                deleted_count += 1
    except Exception as exc:
        if not deleted_count:
            raise
        # What was deleted is still deleted: report it, and let the next pass meet the error.
        LOGGER.warning(
            "destination cleanup pass stopped after deleting %d objects in gs://%s: %s: %s",
            deleted_count,
            bucket_name,
            type(exc).__name__,
            exc,
        )
        complete = False
    if failures.count:
        if not deleted_count:
            raise DestinationDeleteError(failures.describe(bucket_name)) from failures.cause()
        LOGGER.warning("destination cleanup pass left objects behind: %s", failures.describe(bucket_name))
        complete = False

    return DestinationCleanupResult(
        bucket=bucket_name,
        prefixes=prefixes,
        deleted_count=deleted_count,
        complete=complete,
        failed_count=failures.count,
    )


async def drop_table_objects_pass_async(
    *,
    location: str,
    materialization_id: str,
    client: ObjectStoreClientProtocol | None = None,
) -> DestinationCleanupResult:
    """`drop_table_objects_pass` on the cleanup thread, leaving the event loop free.

    Cancelling the caller does not stop a pass that has started: it finishes on its thread, and
    the redelivered event's pass runs after it.
    """
    return await asyncio.get_running_loop().run_in_executor(
        _EXECUTOR,
        partial(drop_table_objects_pass, location=location, materialization_id=materialization_id, client=client),
    )


def retire_table_pointer(
    *,
    location: str,
    materialization_id: str,
    client: ObjectStoreClientProtocol | None = None,
) -> DestinationCleanupResult:
    """Take a table out of the catalog but leave it readable by path (destination cleanup off).

    The pointer is copied to `catalog-pointer.dropped.json` and then deleted: no catalog lists the
    table any more, while `iceberg_scan('<metadata file>')` still reads it.
    """
    bucket_name, prefixes, pointer_name = table_object_prefixes(location, materialization_id)
    storage_client = build_storage_client() if client is None else client
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(pointer_name)
    if blob is not None:
        data = blob.download_as_bytes()
        retired = pointer_name.replace("catalog-pointer.json", "catalog-pointer.dropped.json")
        bucket.blob(retired).upload_from_string(data, content_type="application/json")
        _delete_pointer(bucket, pointer_name)
    return DestinationCleanupResult(bucket=bucket_name, prefixes=prefixes, deleted_count=0, skipped_reason="destination_cleanup_disabled")


async def retire_table_pointer_async(
    *,
    location: str,
    materialization_id: str,
    client: ObjectStoreClientProtocol | None = None,
) -> DestinationCleanupResult:
    return await asyncio.get_running_loop().run_in_executor(
        _EXECUTOR,
        partial(retire_table_pointer, location=location, materialization_id=materialization_id, client=client),
    )


def table_pointer_exists(*, location: str, materialization_id: str, client: ObjectStoreClientProtocol | None = None) -> bool:
    bucket_name, _, pointer_name = table_object_prefixes(location, materialization_id)
    storage_client = build_storage_client() if client is None else client
    return storage_client.bucket(bucket_name).get_blob(pointer_name) is not None


def _delete_pointer(bucket: Any, name: str) -> bool:
    try:
        bucket.blob(name).delete()
    except Exception as exc:  # noqa: BLE001 - classified below
        if _http_status(exc) == 404 or type(exc).__name__ == "NotFound":
            return False
        raise
    return True


def _expired(deadline: float | None, clock: Callable[[], float]) -> bool:
    return deadline is not None and clock() >= deadline


@dataclass(frozen=True, slots=True)
class _Failure:
    """A delete that did not happen: the object stays where it is."""

    name: str
    status: int | None  # None: the request got no answer
    message: str


@dataclass(frozen=True, slots=True)
class _ChunkOutcome:
    """What deleting one chunk of listed objects came to.

    `stop` ends the pass: deletes are failing in transit or being throttled, which more requests
    would not help. Objects GCS refuses one by one (403 and the like) do not stop it.
    """

    deleted: int
    failed: tuple[_Failure, ...] = ()
    stop: bool = False
    error: BaseException | None = None


@dataclass(slots=True)
class _Failures:
    """The failed deletes of one pass, summarised for a log line or an exception message."""

    count: int = 0
    statuses: Counter = field(default_factory=Counter)
    first: _Failure | None = None
    error: BaseException | None = None

    def add(self, outcome: _ChunkOutcome) -> None:
        for failure in outcome.failed:
            self.count += 1
            self.statuses[failure.status] += 1
            if self.first is None:
                self.first = failure
        if outcome.error is not None:
            self.error = outcome.error

    def cause(self) -> BaseException | None:
        """The last transport error seen, for chaining onto the exception raised."""
        return self.error

    def describe(self, bucket_name: str) -> str:
        statuses = ", ".join(
            f"{'no answer' if status is None else status}: {count}"
            for status, count in self.statuses.most_common()
        )
        noun = "object" if self.count == 1 else "objects"
        first = self.first
        detail = "" if first is None else f"; {first.name}: {first.message[:200]}"
        return f"could not delete {self.count} {noun} from gs://{bucket_name} ({statuses}){detail}"


class _BatchDeleter:
    """Deletes a chunk of objects in one GCS batch request, retrying what fails in transit.

    Each call in a batch gets its own answer, in order. 2xx and 404 mean the object is gone.
    408, 429 and 5xx — and a batch request with no answer — are retried after
    `BATCH_RETRY_DELAYS_SECONDS`, while the pass has time left; what still fails then stops the
    pass. Any other answer, such as 403 for an object under a hold, leaves that object and lets
    the pass go on.
    """

    def __init__(
        self,
        client: Any,
        bucket: Any,
        *,
        deadline: float | None,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._deadline = deadline
        self._clock = clock
        self._sleep = sleep

    def __call__(self, blobs: list[BlobProtocol]) -> _ChunkOutcome:
        pending = list(blobs)
        deleted = 0
        refused: list[_Failure] = []
        delays = iter(BATCH_RETRY_DELAYS_SECONDS)
        while True:
            retryable: list[tuple[BlobProtocol, _Failure]] = []
            error: BaseException | None = None
            try:
                responses = self._send(pending)
            except Exception as exc:  # noqa: BLE001 - classified below; the last one is kept
                status = _http_status(exc)
                failures = [_Failure(blob.name, status, f"{type(exc).__name__}: {exc}") for blob in pending]
                if status is not None and not _retryable(status):
                    # GCS turned the whole request away; every other one would fare the same.
                    return _ChunkOutcome(deleted, tuple(refused + failures), stop=True, error=exc)
                retryable = list(zip(pending, failures))
                error = exc
            else:
                for blob, response in zip(pending, responses):
                    status = response.status_code
                    if 200 <= status < 300 or status == 404:
                        deleted += 1
                        continue
                    failure = _Failure(blob.name, status, _error_message(response))
                    if _retryable(status):
                        retryable.append((blob, failure))
                    else:
                        refused.append(failure)
            if not retryable:
                return _ChunkOutcome(deleted, tuple(refused))
            delay = next(delays, None)
            if delay is None or (self._deadline is not None and self._clock() + delay >= self._deadline):
                failed = tuple(refused + [failure for _, failure in retryable])
                return _ChunkOutcome(deleted, failed, stop=True, error=error)
            self._sleep(delay)
            pending = [blob for blob, _ in retryable]

    def _send(self, blobs: list[BlobProtocol]) -> list[Any]:
        """One batch request deleting `blobs`; the answer to each call, in order."""
        with self._client.batch(raise_exception=False) as batch:
            for blob in blobs:
                # Deferred into the batch, so the library's retry would never fire: ours does.
                self._bucket.delete_blob(blob.name, timeout=BATCH_REQUEST_TIMEOUT_SECONDS, retry=None)
        # `Batch.finish()` returns these, but the context manager discards its return value; with
        # `raise_exception=False` this is the only place a failed call is reported.
        responses = getattr(batch, "_responses", None)
        if not isinstance(responses, list) or len(responses) != len(blobs):
            raise RuntimeError("GCS batch request returned no answer for each delete in it")
        return responses


def _batching_client(bucket: Any) -> Any | None:
    """The client to batch `bucket`'s deletes through, or None if it cannot batch them."""
    client = getattr(bucket, "client", None)
    if callable(getattr(client, "batch", None)) and callable(getattr(bucket, "delete_blob", None)):
        return client
    return None


def _delete_one_by_one(bucket: BucketProtocol, blobs: list[BlobProtocol]) -> _ChunkOutcome:
    """Delete without batching, one request per object, which the client retries itself.

    `delete_blobs` tolerates 404 and raises any other error, which ends the pass as it did before
    deletes were batched; what it deleted before the error goes uncounted. A bucket without it
    deletes blob by blob, and each blob's error is taken like a batched call's answer.
    """
    delete_blobs = getattr(bucket, "delete_blobs", None)
    if callable(delete_blobs):
        delete_blobs(blobs, on_error=lambda blob: None)
        return _ChunkOutcome(len(blobs))
    deleted = 0
    refused: list[_Failure] = []
    error: BaseException | None = None
    for blob in blobs:
        try:
            blob.delete()
        except Exception as exc:  # noqa: BLE001 - classified like a batched call's answer
            status = _http_status(exc)
            if status != 404:
                failure = _Failure(blob.name, status, f"{type(exc).__name__}: {exc}")
                if status is None or _retryable(status):
                    return _ChunkOutcome(deleted, tuple(refused + [failure]), stop=True, error=exc)
                refused.append(failure)
                error = exc
                continue
            LOGGER.debug("destination object already removed: %s", blob.name)
        deleted += 1
    return _ChunkOutcome(deleted, tuple(refused), error=error)


def _retryable(status: int) -> bool:
    # GCS's retry strategy: 408, 429 and 5xx are worth another try; anything else would fail again.
    return status in (408, 429) or status >= 500


def _http_status(exc: BaseException) -> int | None:
    """The HTTP status a google-cloud-storage error carries, None for one with no answer."""
    code = getattr(exc, "code", None)
    if isinstance(code, int) and not isinstance(code, bool) and 100 <= code < 600:
        return code
    return None


def _error_message(response: Any) -> str:
    """The status and message GCS gave for a failed call, the message from its JSON error body."""
    try:
        status = f"{response.status_code} {HTTPStatus(response.status_code).phrase}"
    except ValueError:
        status = str(response.status_code)
    try:
        message = json.loads(response.content or b"{}")["error"]["message"]
    except (ValueError, KeyError, TypeError):
        return status
    return f"{status}: {message}"


def build_storage_client() -> ObjectStoreClientProtocol:
    """Build a Google Cloud Storage client using application default credentials."""
    from google.cloud import storage

    return storage.Client()
