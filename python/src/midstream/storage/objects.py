"""Deletion of a materialization's tables - their data and metadata objects - from the destination.

Each table lives under one location, `<destination>/<storage prefix>/<materialization id>/<table>/`:
`data/` and `metadata/`, the catalog pointer among the latter. Dropping a table deletes `data/`
first, then `metadata/`, and the pointer last, so a table that is half deleted stays recognisable
as the materialization's own until the very end. The workflow drops a materialization's tables one
after another.

A materialization can leave millions of objects behind. One request per delete takes hours, so
deletes go to the store's `delete_many`, `delete_batch_size` objects at a time - GCS's batch
requests of 100, S3's `DeleteObjects` of 1,000, Azure's blob batches of 256 - a hundredth of the
requests or fewer. Nothing here knows which backend it deletes from (`lakehouse/store/`).

Deletion still runs in bounded passes: one pass lists and deletes at most a budget of objects
for at most about `CLEANUP_PASS_MAX_SECONDS`, and says whether anything is left. The workflow
queues another pass until nothing is, so a large destination costs time rather than the control
plane's attention. `delete_many` retries nothing: a delete that may succeed later - no answer, 408,
429, a 5xx - is tried again here after a backoff while the pass has time, and one the store refuses
(403 for an object under a hold, say) is left for the next pass.

Deleted objects drop out of the listing, so a pass needs no cursor: the next one lists whatever
remains. That also makes a pass that died half-way safe to run again, and a missing object counts
as deleted.

The control plane serves its API and runs every agent on one event loop, which these calls would
stall for as long as they wait on the network. The workflow therefore runs each pass through
`drop_table_objects_pass_async`, on a thread of its own.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from functools import partial
from itertools import islice
import logging
import re
import time
from typing import Any

from midstream.lakehouse.store import DeleteFailure, ObjectNotFoundError, ObjectStore, parse_object_uri, store_for

LOGGER = logging.getLogger(__name__)

#: Objects one cleanup pass lists and deletes at most when its store deletes one object per request:
#: a single listing page.
CLEANUP_PASS_MAX_OBJECTS = 1000
#: Delete requests one pass makes at most: what it could make in its time budget at a tenth of a
#: second a request, so the time budget is what usually ends a pass, and this bounds one against a
#: fast store. A pass's object budget is this many times its store's `delete_batch_size`, never less
#: than `CLEANUP_PASS_MAX_OBJECTS`: 10,000 objects in GCS's batches of 100.
CLEANUP_PASS_MAX_REQUESTS = 100
#: Seconds after which a pass stops and leaves the rest to the next one, so other workflow events
#: wait no longer than this behind a large cleanup. Checked between delete requests, so a pass can
#: run over by one.
CLEANUP_PASS_MAX_SECONDS = 10.0
#: What the last segment of a table's location may be: a table name (`table_names.py`).
_TABLE_DIRECTORY = re.compile(r"^[a-z][a-z0-9_]*$")
#: Waits before deleting again what failed in a way that may succeed later - no answer, 408, 429 or
#: a 5xx: `delete_many` retries nothing. A retry that would start after the pass's deadline is not
#: made.
BATCH_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)

# One thread: the workflow agent handles one event at a time, and passes over the same prefixes
# must not overlap. A pool of its own keeps a slow store from occupying the loop's default
# executor, which the event loop also uses to resolve hostnames.
_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="destination-cleanup")


class DestinationDeleteError(RuntimeError):
    """A cleanup pass deleted nothing: the store refused or kept failing every delete it attempted.

    Raised so the workflow retries the pass after its backoff instead of running another at once.
    """


@dataclass(frozen=True, slots=True)
class DestinationCleanupResult:
    """Outcome of deleting a materialization's objects from its destination.

    `bucket` is the bucket or container the table is in (`container@account` for Azure, empty for
    a local file store). `complete` is False when a pass stopped at its budget, or left objects it
    could not delete; another pass continues from there. `deleted_count` counts only objects that
    are gone. `failed_count` counts the objects the store refused or kept failing to delete, which
    stay listed for the next pass. Both stay out of `to_dict()`: the operation record's status says
    enough.
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


@dataclass(frozen=True, slots=True)
class _TableObjects:
    """Where a table's objects are: checked to be the table of its materialization."""

    #: The bucket or container (`container@account` for Azure, empty for a file store).
    container: str
    #: The store's root, for messages: `gs://lake`, `abfss://c@a.dfs.core.windows.net`, `file:///`.
    root: str
    #: The keys to empty in order, `data/` then `metadata/`, and the pointer's key.
    prefixes: tuple[str, ...]
    pointer_key: str
    #: The same as URIs, and the URI the pointer is copied to when the table is only retired.
    prefix_uris: tuple[str, ...]
    pointer_uri: str
    dropped_pointer_uri: str


def table_object_prefixes(location: str, materialization_id: str) -> tuple[str, tuple[str, ...], str]:
    """The bucket or container (`container@account` for Azure), the key prefixes to empty in order
    (`data/`, `metadata/`), and the pointer's key; ValueError for a location that is not a table of
    `materialization_id`."""
    objects = _table_objects(location, materialization_id)
    return objects.container, objects.prefixes, objects.pointer_key


def _table_objects(location: str, materialization_id: str) -> _TableObjects:
    if not materialization_id:
        raise ValueError("materialization_id is required for destination cleanup")
    parsed = parse_object_uri(location)
    base = parsed.key.rstrip("/")
    # Guard against a malformed location wiping unrelated objects: a table's location is always a
    # directory named after the table inside one named after its materialization's id
    # (`lakehouse/tables.py::table_location`), on every backend.
    parent, _, table = base.rpartition("/")
    if parent.rpartition("/")[2] != materialization_id or _TABLE_DIRECTORY.match(table) is None:
        raise ValueError(f"refusing to delete outside the table of {materialization_id}: {location}")
    prefixes = (f"{base}/data/", f"{base}/metadata/")
    pointer_key = f"{base}/metadata/catalog-pointer.json"
    root = replace(parsed, key="").uri
    return _TableObjects(
        container=parsed.container if parsed.account is None else f"{parsed.container}@{parsed.account}",
        root=root if parsed.scheme == "file" else root.rstrip("/"),
        prefixes=prefixes,
        pointer_key=pointer_key,
        prefix_uris=tuple(replace(parsed, key=prefix).uri for prefix in prefixes),
        pointer_uri=replace(parsed, key=pointer_key).uri,
        dropped_pointer_uri=replace(parsed, key=f"{base}/metadata/catalog-pointer.dropped.json").uri,
    )


def pass_object_budget(store: ObjectStore) -> int:
    """The most objects one pass lists and deletes through `store` when the caller sets no budget."""
    return max(CLEANUP_PASS_MAX_OBJECTS, CLEANUP_PASS_MAX_REQUESTS * _batch_size(store))


def drop_table_objects(
    *,
    location: str,
    materialization_id: str,
    store: ObjectStore | None = None,
) -> DestinationCleanupResult:
    """Delete every object of a table, blocking the caller throughout; for tests and tools.

    The workflow runs one pass per event instead, off the event loop.
    """
    _table_objects(location, materialization_id)
    object_store = store_for(location) if store is None else store
    deleted_count = 0
    while True:
        result = drop_table_objects_pass(
            location=location,
            materialization_id=materialization_id,
            store=object_store,
            max_seconds=None,
        )
        deleted_count += result.deleted_count
        if result.complete:
            return replace(result, deleted_count=deleted_count)


def drop_table_objects_pass(
    *,
    location: str,
    materialization_id: str,
    store: ObjectStore | None = None,
    max_objects: int | None = None,
    max_seconds: float | None = CLEANUP_PASS_MAX_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> DestinationCleanupResult:
    """Delete up to `max_objects` of a table's objects, for about `max_seconds`.

    `store` is the location's own store unless one is given; `max_objects` defaults to
    `pass_object_budget(store)`. Objects go to `store.delete_many` a `delete_batch_size` at a time.
    `data/` is emptied, then `metadata/` but for the pointer, then the pointer goes. The result is
    `complete` only once the pointer is gone too. A pass stops early only after it has tried to
    delete something, and raises if it deleted nothing because those deletes failed - so a chain of
    passes either finishes, however slow the store, or fails where the workflow's retries can see it.
    """
    if max_objects is not None and max_objects < 1:
        raise ValueError("max_objects must be >= 1")
    if max_seconds is not None and max_seconds <= 0:
        raise ValueError("max_seconds must be > 0")
    objects = _table_objects(location, materialization_id)
    object_store = store_for(location) if store is None else store
    deadline = None if max_seconds is None else clock() + max_seconds
    chunk_size = _batch_size(object_store)
    budget = pass_object_budget(object_store) if max_objects is None else max_objects
    delete_chunk = _ChunkDeleter(object_store, deadline=deadline, clock=clock, sleep=sleep)

    deleted_count = 0
    listed_count = 0
    attempted_count = 0
    failures = _Failures()
    complete = True
    try:
        for prefix in objects.prefix_uris:
            # Read lazily, a chunk at a time: a store's listing pages, so memory holds about one page
            # however large the budget, and the deadline is checked between chunks.
            listing = (item.uri for item in object_store.list(prefix) if item.uri != objects.pointer_uri)
            while True:
                if attempted_count and _expired(deadline, clock):
                    complete = False
                    break
                wanted = min(chunk_size, budget - listed_count)
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
            if object_store.delete(objects.pointer_uri):
                deleted_count += 1
    except Exception as exc:
        if not deleted_count:
            raise
        # What was deleted is still deleted: report it, and let the next pass meet the error.
        LOGGER.warning(
            "destination cleanup pass stopped after deleting %d objects in %s: %s: %s",
            deleted_count,
            objects.root,
            type(exc).__name__,
            exc,
        )
        complete = False
    if failures.count:
        if not deleted_count:
            raise DestinationDeleteError(failures.describe(objects.root)) from failures.cause()
        LOGGER.warning("destination cleanup pass left objects behind: %s", failures.describe(objects.root))
        complete = False

    return DestinationCleanupResult(
        bucket=objects.container,
        prefixes=objects.prefixes,
        deleted_count=deleted_count,
        complete=complete,
        failed_count=failures.count,
    )


async def drop_table_objects_pass_async(
    *,
    location: str,
    materialization_id: str,
    store: ObjectStore | None = None,
) -> DestinationCleanupResult:
    """`drop_table_objects_pass` on the cleanup thread, leaving the event loop free.

    Cancelling the caller does not stop a pass that has started: it finishes on its thread, and
    the redelivered event's pass runs after it.
    """
    return await asyncio.get_running_loop().run_in_executor(
        _EXECUTOR,
        partial(drop_table_objects_pass, location=location, materialization_id=materialization_id, store=store),
    )


def retire_table_pointer(
    *,
    location: str,
    materialization_id: str,
    store: ObjectStore | None = None,
) -> DestinationCleanupResult:
    """Take a table out of the catalog but leave it readable by path (destination cleanup off).

    The pointer is copied to `catalog-pointer.dropped.json` and then deleted: no catalog lists the
    table any more, while `iceberg_scan('<metadata file>')` still reads it. The pointer goes only
    once its copy is written.
    """
    objects = _table_objects(location, materialization_id)
    object_store = store_for(location) if store is None else store
    try:
        data, _ = object_store.get(objects.pointer_uri)
    except ObjectNotFoundError:
        pass
    else:
        object_store.put(objects.dropped_pointer_uri, data, content_type="application/json")
        object_store.delete(objects.pointer_uri)
    return DestinationCleanupResult(
        bucket=objects.container,
        prefixes=objects.prefixes,
        deleted_count=0,
        skipped_reason="destination_cleanup_disabled",
    )


async def retire_table_pointer_async(
    *,
    location: str,
    materialization_id: str,
    store: ObjectStore | None = None,
) -> DestinationCleanupResult:
    return await asyncio.get_running_loop().run_in_executor(
        _EXECUTOR,
        partial(retire_table_pointer, location=location, materialization_id=materialization_id, store=store),
    )


def table_pointer_exists(*, location: str, materialization_id: str, store: ObjectStore | None = None) -> bool:
    objects = _table_objects(location, materialization_id)
    object_store = store_for(location) if store is None else store
    return object_store.head(objects.pointer_uri) is not None


def _batch_size(store: ObjectStore) -> int:
    size = getattr(store, "delete_batch_size", 1)
    return size if isinstance(size, int) and not isinstance(size, bool) and size >= 1 else 1


def _expired(deadline: float | None, clock: Callable[[], float]) -> bool:
    return deadline is not None and clock() >= deadline


@dataclass(frozen=True, slots=True)
class _ChunkOutcome:
    """What deleting one chunk of listed objects came to.

    `stop` ends the pass: deletes are failing in transit or being throttled, which more requests
    would not help. Objects the store refuses one by one (403 and the like) do not stop it.
    """

    deleted: int
    failed: tuple[DeleteFailure, ...] = ()
    stop: bool = False
    error: BaseException | None = None


@dataclass(slots=True)
class _Failures:
    """The failed deletes of one pass, summarised for a log line or an exception message."""

    count: int = 0
    statuses: Counter = field(default_factory=Counter)
    first: DeleteFailure | None = None
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
        """The last error behind a failure, for chaining onto the exception raised."""
        return self.error

    def describe(self, root: str) -> str:
        statuses = ", ".join(
            f"{'no answer' if status is None else status}: {count}"
            for status, count in self.statuses.most_common()
        )
        noun = "object" if self.count == 1 else "objects"
        first = self.first
        detail = "" if first is None else f"; {first.uri}: {first.message[:200]}"
        return f"could not delete {self.count} {noun} from {root} ({statuses}){detail}"


class _ChunkDeleter:
    """Deletes a chunk of listed objects through `delete_many`, retrying what may succeed later.

    Each object gets its own outcome; a missing one counts as deleted. A failure with no answer,
    408, 429 or a 5xx is retried after `BATCH_RETRY_DELAYS_SECONDS`, while the pass has time left;
    what still fails then stops the pass. Any other failure, such as 403 for an object under a hold,
    leaves that object for the next pass and lets this one go on - unless the store turned a whole
    request away with it: every other request would fare the same, so the pass stops.
    """

    def __init__(
        self,
        store: ObjectStore,
        *,
        deadline: float | None,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
    ) -> None:
        self._store = store
        self._deadline = deadline
        self._clock = clock
        self._sleep = sleep

    def __call__(self, uris: list[str]) -> _ChunkOutcome:
        pending = list(uris)
        deleted = 0
        refused: list[DeleteFailure] = []
        delays = iter(BATCH_RETRY_DELAYS_SECONDS)
        while True:
            outcome = self._store.delete_many(pending)
            deleted += len(outcome.deleted)
            retryable: list[DeleteFailure] = []
            error: BaseException | None = None
            turned_away = False
            for failure in outcome.failed:
                if failure.error is not None:
                    error = failure.error
                if failure.retryable:
                    retryable.append(failure)
                else:
                    refused.append(failure)
                    turned_away = turned_away or failure.whole_request
            if turned_away:
                # The store turned a whole request away: every other one would fare the same.
                return _ChunkOutcome(deleted, tuple(refused + retryable), stop=True, error=error)
            if not retryable:
                return _ChunkOutcome(deleted, tuple(refused), error=error)
            delay = next(delays, None)
            if delay is None or (self._deadline is not None and self._clock() + delay >= self._deadline):
                return _ChunkOutcome(deleted, tuple(refused + retryable), stop=True, error=error)
            self._sleep(delay)
            pending = [failure.uri for failure in retryable]
