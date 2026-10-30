"""Destination cleanup's pass (`storage/objects.py`) over each kind of store it deletes through.

The pass lists a table's objects and hands them to `store.delete_many` a `delete_batch_size` at a
time, retrying what may succeed later; it knows nothing of the backend. The same scenarios run over:

- `GcsStore` and the real google-cloud-storage client, which talks to an in-process fake of the GCS
  JSON API - object listing, and the batch endpoint with multipart/mixed requests and responses - so
  these run the library's own batching: it sends up to 100 calls in one request, retries none of
  them, and reports each call's status only in the batch's responses.
- `MemoryStore`, which deletes one object at a time inside `delete_many` and reports each failure
  its hook injects as that object's: in chunks of 100 (its default, as GCS's batch requests) and of
  1,000 (as S3's `DeleteObjects`), counted in the store's own chunks.
- A `MemoryStore` deleting one object per request (`delete_batch_size` 1), whose passes keep to the
  per-object budget.

`MemoryStore` serves any scheme, so its tables sit on `abfss://`, `s3://` and `file://` locations:
the scheme changes only how the pass names the store in its results and messages.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Iterable, Sequence
from email.parser import BytesParser
import json
import threading
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlparse

try:
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage
    import requests

    from midstream.lakehouse.store.gcs import GcsStore
except ImportError:  # pragma: no cover - google-cloud-storage is a dependency of the package
    storage = None

from midstream.lakehouse.store import DeleteFailure, DeleteOutcome, MemoryStore, StoreError
from midstream.storage import objects
from midstream.storage.objects import (
    BATCH_RETRY_DELAYS_SECONDS,
    CLEANUP_PASS_MAX_OBJECTS,
    DestinationDeleteError,
    drop_table_objects,
    drop_table_objects_pass,
    drop_table_objects_pass_async,
    pass_object_budget,
)

ENDPOINT = "https://storage.test"
LOCATION = "gs://lake/materializer/mat-1/orders"
DATA = "materializer/mat-1/orders/data/"
METADATA = "materializer/mat-1/orders/metadata/"
POINTER = "materializer/mat-1/orders/metadata/catalog-pointer.json"
# Another table of the materialization whose location starts with this one's: it must be left alone.
SIBLING = "materializer/mat-1/orders_v2/data/part-00000.parquet"
REASONS = {204: "No Content", 403: "Forbidden", 404: "Not Found", 429: "Too Many Requests", 500: "Internal Server Error", 503: "Service Unavailable"}
BOUNDARY = "batch_pK7JBAk73-E=_AA5eFwv4m2Q="


class FakeGcsJsonApi:
    """Just enough of the GCS JSON API for destination cleanup, behind `requests.Session.request`.

    Lists objects by prefix, a page of up to 1000 at a time, and answers a batch request part by
    part, in order. `script[name]` holds the statuses the object's next deletes get (a 2xx or 404
    removes it); `batch_script` the outcome of the next batch requests themselves — a status, an
    exception to raise, or None to answer normally.

    google-cloud-storage 3.x also fetches the bucket's metadata, once per client and on a thread of
    its own, for its telemetry. That request is answered but kept out of `requests` and `threads`.
    """

    def __init__(self, names: list[str]) -> None:
        self.names = set(names)
        self.script: dict[str, list[int]] = {}
        self.batch_script: list[int | Exception | None] = []
        self.requests: Counter = Counter()
        self.batches: list[list[str]] = []
        self.threads: set[str] = set()

    def request(self, method, url, data=None, headers=None, timeout=None, **kwargs):
        parsed = urlparse(url)
        if method == "GET" and parsed.path == "/storage/v1/b/lake":
            bucket = {"kind": "storage#bucket", "name": "lake", "projectNumber": "1", "location": "US", "locationType": "multi-region"}
            return _response(method, url, 200, json.dumps(bucket).encode())
        self.threads.add(threading.current_thread().name)
        if method == "GET" and parsed.path == "/storage/v1/b/lake/o":
            self.requests["list"] += 1
            return self._list(url, parse_qs(parsed.query))
        if method == "POST" and parsed.path == "/batch/storage/v1":
            self.requests["batch"] += 1
            return self._batch(url, headers, data)
        if method == "DELETE":
            self.requests["delete"] += 1
            status = self._delete(_object_name(parsed.path))
            return _response(method, url, status, b"" if status == 204 else _error_body(status))
        raise AssertionError(f"unexpected request: {method} {url}")

    def _list(self, url: str, query: dict[str, list[str]]):
        prefix = query.get("prefix", [""])[0]
        token = query.get("pageToken", [""])[0]
        size = min(int(query.get("maxResults", ["1000"])[0]), 1000)
        names = sorted(name for name in self.names if name.startswith(prefix) and name > token)
        payload: dict = {"kind": "storage#objects", "items": [{"name": name, "bucket": "lake"} for name in names[:size]]}
        if len(names) > size:
            payload["nextPageToken"] = names[size - 1]
        return _response("GET", url, 200, json.dumps(payload).encode())

    def _batch(self, url: str, headers: dict, data: str):
        outcome = self.batch_script.pop(0) if self.batch_script else None
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is not None:
            return _response("POST", url, outcome, _error_body(outcome))
        message = BytesParser().parsebytes(f"Content-Type: {headers['Content-Type']}\r\n\r\n{data}".encode())
        names = []
        parts = []
        for call in message.get_payload():
            method, call_url, _ = call.get_payload().lstrip().split("\n", 1)[0].split(" ", 2)
            assert method == "DELETE", method
            name = _object_name(urlparse(call_url).path)
            names.append(name)
            status = self._delete(name)
            body = b"" if status == 204 else _error_body(status)
            inner = f"HTTP/1.1 {status} {REASONS[status]}\r\nContent-Type: application/json; charset=UTF-8\r\nContent-Length: {len(body)}\r\n\r\n"
            parts.append(f"--{BOUNDARY}\r\nContent-Type: application/http\r\nContent-ID: <response-{len(parts) + 1}>\r\n\r\n".encode() + inner.encode() + body + b"\r\n")
        self.batches.append(names)
        content = b"".join(parts) + f"--{BOUNDARY}--\r\n".encode()
        return _response("POST", url, 200, content, content_type=f"multipart/mixed; boundary={BOUNDARY}")

    def _delete(self, name: str) -> int:
        planned = self.script.get(name)
        status = planned.pop(0) if planned else (204 if name in self.names else 404)
        if 200 <= status < 300 or status == 404:
            self.names.discard(name)
        return status


def _object_name(path: str) -> str:
    return unquote(path.split("/o/", 1)[1])


def _error_text(status: int) -> str:
    """What GCS says of a delete it answered with `status`."""
    return {403: "Object is under active Event-Based hold and cannot be deleted."}.get(status, REASONS.get(status, "error"))


def _error_body(status: int) -> bytes:
    return json.dumps({"error": {"code": status, "message": _error_text(status)}}).encode()


def _response(method: str, url: str, status: int, content: bytes, content_type: str = "application/json; charset=UTF-8"):
    response = requests.Response()
    response.status_code = status
    response._content = content
    response.headers["Content-Type"] = content_type
    response.request = requests.Request(method, url).prepare()
    return response


def owned(count: int, prefix: str = DATA) -> list[str]:
    return [f"{prefix}part-{index:05d}.parquet" for index in range(count)]


@unittest.skipIf(storage is None, "google-cloud-storage is not installed")
class GcsStoreBatchRequestTests(unittest.TestCase):
    """The pass over `GcsStore`, whose `delete_many` sends GCS's batch requests of a hundred deletes."""

    def setUp(self) -> None:
        self.delays: list[float] = []

    def store(self, api: FakeGcsJsonApi) -> GcsStore:
        client = storage.Client(
            project="test",
            credentials=AnonymousCredentials(),
            _http=api,
            client_options={"api_endpoint": ENDPOINT},
        )
        return GcsStore(client=client)

    def run_pass(self, api: FakeGcsJsonApi, **kwargs):
        kwargs.setdefault("max_seconds", None)
        return drop_table_objects_pass(
            location=LOCATION,
            materialization_id="mat-1",
            store=self.store(api),
            sleep=self.delays.append,
            **kwargs,
        )

    def test_deletes_go_a_hundred_to_a_batch_request(self) -> None:
        self.assertEqual(GcsStore.delete_batch_size, 100)
        api = FakeGcsJsonApi(owned(250) + owned(1, METADATA) + [POINTER, SIBLING])

        result = self.run_pass(api)

        self.assertTrue(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (252, 0))
        self.assertEqual((result.bucket, result.prefixes), ("lake", (DATA, METADATA)))
        self.assertEqual(api.names, {SIBLING})
        # Two listings, one per prefix; four batch requests; one delete, the pointer's, last.
        self.assertEqual(api.requests, Counter({"list": 2, "batch": 4, "delete": 1}))
        self.assertEqual([len(names) for names in api.batches], [100, 100, 50, 1])
        self.assertEqual(self.delays, [])

    def test_a_batched_pass_is_held_to_its_own_budget_of_requests(self) -> None:
        api = FakeGcsJsonApi(owned(1_500))

        # More than a per-object pass may take: batched, one pass deletes it all.
        self.assertGreater(1_500, CLEANUP_PASS_MAX_OBJECTS)
        self.assertEqual(pass_object_budget(self.store(api)), 10_000)
        result = self.run_pass(api)
        self.assertEqual((result.deleted_count, result.complete), (1_500, True))

        api = FakeGcsJsonApi(owned(1_350))
        with patch.object(objects, "CLEANUP_PASS_MAX_REQUESTS", 12):
            result = self.run_pass(api)
        self.assertEqual((result.deleted_count, result.complete), (1_200, False))
        self.assertEqual(api.requests["batch"], 12)

    def test_a_missing_object_counts_as_gone(self) -> None:
        api = FakeGcsJsonApi(owned(5))
        api.script = {owned(5)[1]: [404], owned(5)[3]: [404]}

        result = self.run_pass(api)

        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (5, 0, True))
        self.assertEqual(api.requests["batch"], 1)
        self.assertEqual(self.delays, [])

    def test_throttled_and_failing_deletes_are_retried_after_a_backoff(self) -> None:
        names = owned(120)
        api = FakeGcsJsonApi(names)
        api.script = {names[3]: [429], names[7]: [500, 503], names[110]: [503]}

        result = self.run_pass(api)

        self.assertTrue(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (120, 0))
        # Only the failed calls go again: two of the first batch, then the one still failing.
        self.assertEqual(api.batches[1:3], [[names[3], names[7]], [names[7]]])
        self.assertEqual(api.batches[4], [names[110]])
        self.assertEqual(self.delays, [1.0, 2.0, 1.0])

    def test_deletes_that_keep_failing_end_the_pass_and_wait_for_the_next(self) -> None:
        names = owned(300)
        api = FakeGcsJsonApi(names)
        api.script = {names[0]: [503] * 4, names[1]: [429] * 4}

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self.run_pass(api)

        self.assertIn("could not delete 2 objects from gs://lake (503: 1, 429: 1)", logs.output[0])
        self.assertFalse(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (98, 2))
        self.assertEqual(self.delays, list(BATCH_RETRY_DELAYS_SECONDS))
        # The rest of the listing waits: more requests would not help a failing bucket.
        self.assertEqual(api.requests["batch"], 4)
        self.assertEqual(len(api.names), 202)

        result = self.run_pass(api)

        self.assertEqual((result.deleted_count, result.complete), (202, True))
        self.assertEqual(api.names, set())

    def test_an_object_gcs_refuses_is_left_and_the_pass_goes_on(self) -> None:
        names = owned(250)
        api = FakeGcsJsonApi(names)
        api.script = {names[120]: [403] * 3}

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self.run_pass(api)

        self.assertIn(names[120], logs.output[0])

        self.assertFalse(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (249, 1))
        self.assertEqual(api.requests["batch"], 3)
        self.assertEqual(self.delays, [], "a refusal is not retried")

        # Once only what GCS refuses is left, a pass deletes nothing: it raises, for the
        # workflow's backoff and, in the end, its failure, carrying GCS's reason.
        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(api)
        self.assertIn("403: 1", str(raised.exception))
        self.assertIn(names[120], str(raised.exception))
        self.assertIn("Event-Based hold", str(raised.exception))
        self.assertEqual(api.names, {names[120]})

    def test_a_pass_whose_deletes_all_fail_raises(self) -> None:
        names = owned(40)
        api = FakeGcsJsonApi(names)
        api.script = {name: [503] * 4 for name in names}

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(api)

        self.assertIn("could not delete 40 objects from gs://lake (503: 40)", str(raised.exception))
        self.assertEqual(len(api.names), 40)

    def test_a_batch_request_that_fails_in_transit_is_retried_whole(self) -> None:
        api = FakeGcsJsonApi(owned(100))
        api.batch_script = [503, requests.exceptions.ConnectionError("connection reset by peer")]

        result = self.run_pass(api)

        self.assertEqual((result.deleted_count, result.complete), (100, True))
        self.assertEqual(api.requests["batch"], 3)
        self.assertEqual(self.delays, [1.0, 2.0])

    def test_a_batch_request_gcs_turns_away_is_not_retried(self) -> None:
        api = FakeGcsJsonApi(owned(100))
        api.batch_script = [403]

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(api)

        self.assertEqual(type(raised.exception.__cause__).__name__, "Forbidden")
        # The request's refusal is every delete's in it.
        self.assertIn("could not delete 100 objects from gs://lake (403: 100)", str(raised.exception))
        self.assertEqual(api.requests["batch"], 1)
        self.assertEqual(self.delays, [])

    def test_a_batch_request_gcs_turns_away_ends_the_pass(self) -> None:
        # Every other batch request would be turned away alike: none is sent.
        api = FakeGcsJsonApi(owned(250))
        api.batch_script = [403]
        before = set(api.names)

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(api)

        self.assertEqual(type(raised.exception.__cause__).__name__, "Forbidden")
        self.assertEqual(api.requests["batch"], 1)
        self.assertEqual(self.delays, [])
        self.assertEqual(api.names, before)

    def test_a_pass_that_fails_after_deleting_reports_what_it_deleted(self) -> None:
        api = FakeGcsJsonApi(owned(300))
        api.batch_script = [None, 503, 503, 503, 503]

        with self.assertLogs(objects.LOGGER, "WARNING"):
            result = self.run_pass(api)

        self.assertFalse(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (100, 100))

    def test_retries_never_start_after_the_pass_deadline(self) -> None:
        names = owned(100)
        api = FakeGcsJsonApi(names)
        api.script = {names[0]: [503] * 4}
        # The pass starts at 0 with 10 s; the first failure reads 5 (retry in 1 s fits), the
        # second 8.5 (a retry in 2 s would start past the deadline).
        clock = iter([0.0, 5.0, 8.5]).__next__

        with self.assertLogs(objects.LOGGER, "WARNING"):
            result = self.run_pass(api, max_seconds=10, clock=clock)

        self.assertEqual(self.delays, [1.0])
        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (99, 1, False))

    def test_the_time_budget_ends_a_batched_pass_once_it_has_deleted_a_batch(self) -> None:
        api = FakeGcsJsonApi(owned(350))
        clock = iter([0.0] + [99.0] * 10).__next__  # the pass starts, then every reading is late

        result = self.run_pass(api, max_seconds=10, clock=clock)

        self.assertEqual((result.deleted_count, result.complete), (GcsStore.delete_batch_size, False))
        self.assertEqual(api.requests["batch"], 1)

    def test_a_full_delete_runs_batched_passes_until_nothing_is_left(self) -> None:
        api = FakeGcsJsonApi(owned(1_234) + [SIBLING])

        result = drop_table_objects(
            location=LOCATION,
            materialization_id="mat-1",
            store=self.store(api),
        )

        self.assertEqual((result.deleted_count, result.complete), (1_234, True))
        self.assertEqual(api.names, {SIBLING})
        # Data goes in batches; only the pointer is a request of its own (here already gone).
        self.assertEqual(api.requests["delete"], 1)

    def test_the_async_pass_batches_on_the_cleanup_thread(self) -> None:
        api = FakeGcsJsonApi(owned(150))

        result = asyncio.run(
            drop_table_objects_pass_async(
                location=LOCATION,
                materialization_id="mat-1",
                store=self.store(api),
            )
        )

        self.assertEqual((result.deleted_count, result.complete), (150, True))
        self.assertEqual(api.requests["batch"], 2)
        self.assertTrue(all(name.startswith("destination-cleanup") for name in api.threads), api.threads)


class RecordingStore(MemoryStore):
    """A `MemoryStore` deleting `delete_batch_size` objects per `delete_many`, which records each call.

    `script[uri]` holds what the object's next deletes meet, like the GCS fake's statuses, through the
    store's own failure hook: a 2xx deletes it, 404 finds it already gone (deleted by someone else
    after the listing), any other status fails that delete with a `StoreError` of that status, and an
    exception fails it with that exception. Each error in `turn_away` fails a whole `delete_many`
    call, as a store's answer to the request itself, before any delete in it. `batches` holds the
    URIs of each `delete_many` call, `deletes` those of each single `delete`, `threads` the threads
    `delete_many` ran on.
    """

    def __init__(self, delete_batch_size: int, uris: Iterable[str] = ()) -> None:
        super().__init__()
        self.delete_batch_size = delete_batch_size
        self.script: dict[str, list[int | BaseException]] = {}
        self.turn_away: list[StoreError] = []
        self.batches: list[list[str]] = []
        self.deletes: list[str] = []
        self.threads: set[str] = set()
        self.fail = self._scripted
        for uri in uris:
            self.write(uri, b"x")

    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome:
        self.batches.append(list(uris))
        self.threads.add(threading.current_thread().name)
        if self.turn_away:
            error = self.turn_away.pop(0)
            return DeleteOutcome(failed=tuple(DeleteFailure(uri, error.status, str(error), error=error, whole_request=True) for uri in uris))
        return super().delete_many(uris)

    def delete(self, uri: str, *, if_version: str | None = None) -> bool:
        self.deletes.append(uri)
        return super().delete(uri, if_version=if_version)

    def lists(self) -> list[str]:
        """The prefixes listed, in order."""
        return [prefix for operation, prefix in self.calls if operation == "list"]

    def _scripted(self, operation: str, uri: str) -> BaseException | None:
        planned = self.script.get(uri) if operation == "delete" else None
        if not planned:
            return None
        outcome = planned.pop(0)
        if isinstance(outcome, BaseException):
            return outcome
        if outcome == 404:
            self.objects.pop(uri, None)
            return None
        if 200 <= outcome < 300:
            return None
        return StoreError(f"{outcome} {REASONS[outcome]}: {_error_text(outcome)}", uri=uri, status=outcome, code=REASONS[outcome])


class MemoryPassHarness:
    """Passes over a `RecordingStore` of `batch_size`, holding a table of mat-1 at `location`."""

    batch_size: int
    location: str

    def setUp(self) -> None:
        self.delays: list[float] = []

    @property
    def pointer(self) -> str:
        return f"{self.location}/metadata/catalog-pointer.json"

    @property
    def sibling(self) -> str:
        """Another table of the materialization whose location starts with this one's: left alone."""
        return f"{self.location}_v2/data/part-00000.parquet"

    def uris(self, count: int, prefix: str = "data/") -> list[str]:
        return [f"{self.location}/{prefix}part-{index:05d}.parquet" for index in range(count)]

    def store(self, uris: Iterable[str] = ()) -> RecordingStore:
        return RecordingStore(self.batch_size, uris)

    def run_pass(self, store: RecordingStore, **kwargs):
        kwargs.setdefault("max_seconds", None)
        return drop_table_objects_pass(
            location=self.location,
            materialization_id="mat-1",
            store=store,
            sleep=self.delays.append,
            **kwargs,
        )


class ChunkedPassScenarios(MemoryPassHarness):
    """The GCS scenarios over a `MemoryStore`, counted in its chunks of `batch_size` (at least 8).

    A batch request that fails as a whole becomes a chunk whose every delete fails the same way.
    `budget` is a pass's object budget for the store; `root` and `container` are how the pass names
    the location's store in messages and in its result.
    """

    budget: int
    root: str
    container: str

    def test_deletes_go_to_the_store_a_chunk_at_a_time(self) -> None:
        b = self.batch_size
        data = self.uris(2 * b + b // 2)
        store = self.store(data + self.uris(1, "metadata/") + [self.pointer, self.sibling])

        result = self.run_pass(store)

        self.assertTrue(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (len(data) + 2, 0))
        self.assertEqual(result.bucket, self.container)
        self.assertEqual(store.names(), [self.sibling])
        # One listing per prefix; the pointer is a delete of its own, last.
        self.assertEqual(store.lists(), [f"{self.location}/data/", f"{self.location}/metadata/"])
        self.assertEqual([len(batch) for batch in store.batches], [b, b, b // 2, 1])
        self.assertEqual(store.deletes, [self.pointer])
        self.assertEqual(self.delays, [])

    def test_a_pass_is_held_to_its_own_budget_of_requests(self) -> None:
        b = self.batch_size
        self.assertEqual(pass_object_budget(self.store()), self.budget)

        # More than a per-object pass may take: in chunks, one pass deletes it all.
        count = CLEANUP_PASS_MAX_OBJECTS + b // 2
        result = self.run_pass(self.store(self.uris(count)))
        self.assertEqual((result.deleted_count, result.complete), (count, True))

        # A chunk per request: a pass of fewer requests stops at that many chunks.
        requests_per_pass = CLEANUP_PASS_MAX_OBJECTS // b + 2
        store = self.store(self.uris((requests_per_pass + 1) * b))
        with patch.object(objects, "CLEANUP_PASS_MAX_REQUESTS", requests_per_pass):
            result = self.run_pass(store)
        self.assertEqual((result.deleted_count, result.complete), (requests_per_pass * b, False))
        self.assertEqual([len(batch) for batch in store.batches], [b] * requests_per_pass)

    def test_a_missing_object_counts_as_gone(self) -> None:
        names = self.uris(5)
        store = self.store(names)
        store.script = {names[1]: [404], names[3]: [404]}

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (5, 0, True))
        self.assertEqual(len(store.batches), 1)
        self.assertEqual(self.delays, [])

    def test_throttled_and_failing_deletes_are_retried_after_a_backoff(self) -> None:
        b = self.batch_size
        names = self.uris(b + 20)
        store = self.store(names)
        store.script = {names[3]: [429], names[7]: [500, 503], names[b + 10]: [503]}

        result = self.run_pass(store)

        self.assertTrue(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (b + 20, 0))
        # Only the failed deletes go again: two of the first chunk, then the one still failing.
        self.assertEqual(store.batches[1:3], [[names[3], names[7]], [names[7]]])
        self.assertEqual(store.batches[4], [names[b + 10]])
        self.assertEqual(self.delays, [1.0, 2.0, 1.0])

    def test_deletes_that_keep_failing_end_the_pass_and_wait_for_the_next(self) -> None:
        b = self.batch_size
        names = self.uris(3 * b)
        store = self.store(names)
        store.script = {names[0]: [503] * 4, names[1]: [429] * 4}

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self.run_pass(store)

        self.assertIn(f"could not delete 2 objects from {self.root} (503: 1, 429: 1)", logs.output[0])
        self.assertFalse(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (b - 2, 2))
        self.assertEqual(self.delays, list(BATCH_RETRY_DELAYS_SECONDS))
        # The rest of the listing waits: more requests would not help a failing store.
        self.assertEqual(len(store.batches), 4)
        self.assertEqual(len(store.names()), 2 * b + 2)

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.complete), (2 * b + 2, True))
        self.assertEqual(store.names(), [])

    def test_an_object_the_store_refuses_is_left_and_the_pass_goes_on(self) -> None:
        b = self.batch_size
        names = self.uris(2 * b + b // 2)
        refused = names[b + 20]
        store = self.store(names)
        store.script = {refused: [403] * 3}

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self.run_pass(store)

        self.assertIn(refused, logs.output[0])

        self.assertFalse(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (len(names) - 1, 1))
        self.assertEqual(len(store.batches), 3)
        self.assertEqual(self.delays, [], "a refusal is not retried")

        # Once only what the store refuses is left, a pass deletes nothing: it raises, for the
        # workflow's backoff and, in the end, its failure, carrying the store's reason.
        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(store)
        self.assertIn("403: 1", str(raised.exception))
        self.assertIn(refused, str(raised.exception))
        self.assertIn("Event-Based hold", str(raised.exception))
        self.assertEqual(store.names(), [refused])

    def test_a_pass_whose_deletes_all_fail_raises(self) -> None:
        names = self.uris(40)
        store = self.store(names)
        store.script = {name: [503] * 4 for name in names}

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(store)

        self.assertIn(f"could not delete 40 objects from {self.root} (503: 40)", str(raised.exception))
        self.assertIsInstance(raised.exception.__cause__, StoreError)
        self.assertEqual(len(store.names()), 40)

    def test_a_chunk_whose_deletes_get_no_answer_is_retried_whole(self) -> None:
        names = self.uris(self.batch_size)
        store = self.store(names)
        lost = StoreError("connection reset by peer", status=None, code="ConnectionError")
        store.script = {name: [503, lost] for name in names}

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.complete), (len(names), True))
        self.assertEqual(store.batches, [names] * 3)
        self.assertEqual(self.delays, [1.0, 2.0])

    def test_a_chunk_the_store_turns_away_is_not_retried(self) -> None:
        names = self.uris(self.batch_size)
        store = self.store(names)
        forbidden = StoreError("403 Forbidden", status=403, code="Forbidden")
        store.script = {name: [forbidden] for name in names}

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(store)

        self.assertIs(raised.exception.__cause__, forbidden)
        self.assertIn(f"could not delete {len(names)} objects from {self.root} (403: {len(names)})", str(raised.exception))
        self.assertEqual(len(store.batches), 1)
        self.assertEqual(self.delays, [])

    def test_a_request_the_store_turns_away_ends_the_pass(self) -> None:
        # Every other request would be turned away alike: none is sent.
        names = self.uris(3 * self.batch_size)
        store = self.store(names)
        forbidden = StoreError("403 Forbidden", status=403, code="AccessDenied")
        store.turn_away = [forbidden]

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(store)

        self.assertIs(raised.exception.__cause__, forbidden)
        self.assertEqual(len(store.batches), 1)
        self.assertEqual(self.delays, [])
        self.assertEqual(store.names(self.location + "/data/"), sorted(names))

    def test_a_request_turned_away_in_transit_is_retried(self) -> None:
        names = self.uris(2 * self.batch_size)
        store = self.store(names)
        store.turn_away = [StoreError("503 Service Unavailable", status=503, code="ServiceUnavailable")]

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.complete), (len(names), True))
        self.assertEqual(self.delays, [BATCH_RETRY_DELAYS_SECONDS[0]])

    def test_a_pass_that_fails_after_deleting_reports_what_it_deleted(self) -> None:
        b = self.batch_size
        names = self.uris(3 * b)
        store = self.store(names)
        store.script = {name: [503] * 4 for name in names[b : 2 * b]}

        with self.assertLogs(objects.LOGGER, "WARNING"):
            result = self.run_pass(store)

        self.assertFalse(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (b, b))

    def test_retries_never_start_after_the_pass_deadline(self) -> None:
        names = self.uris(self.batch_size)
        store = self.store(names)
        store.script = {names[0]: [503] * 4}
        # The pass starts at 0 with 10 s; the first failure reads 5 (retry in 1 s fits), the
        # second 8.5 (a retry in 2 s would start past the deadline).
        clock = iter([0.0, 5.0, 8.5]).__next__

        with self.assertLogs(objects.LOGGER, "WARNING"):
            result = self.run_pass(store, max_seconds=10, clock=clock)

        self.assertEqual(self.delays, [1.0])
        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (len(names) - 1, 1, False))

    def test_the_time_budget_ends_a_pass_once_it_has_deleted_a_chunk(self) -> None:
        b = self.batch_size
        store = self.store(self.uris(3 * b + b // 2))
        clock = iter([0.0] + [99.0] * 10).__next__  # the pass starts, then every reading is late

        result = self.run_pass(store, max_seconds=10, clock=clock)

        self.assertEqual((result.deleted_count, result.complete), (b, False))
        self.assertEqual(len(store.batches), 1)

    def test_a_full_delete_runs_passes_until_nothing_is_left(self) -> None:
        store = self.store(self.uris(2_345) + [self.sibling])

        # One request a pass: each pass takes the per-object budget, so three passes delete it all.
        with patch.object(objects, "CLEANUP_PASS_MAX_REQUESTS", 1):
            result = drop_table_objects(location=self.location, materialization_id="mat-1", store=store)

        self.assertEqual((result.deleted_count, result.complete), (2_345, True))
        self.assertEqual(store.names(), [self.sibling])
        self.assertEqual(store.lists(), [f"{self.location}/data/"] * 3 + [f"{self.location}/metadata/"])
        # Data goes in chunks; only the pointer is a request of its own (here already gone).
        self.assertEqual(store.deletes, [self.pointer])

    def test_the_async_pass_deletes_on_the_cleanup_thread(self) -> None:
        b = self.batch_size
        store = self.store(self.uris(b + b // 2))

        result = asyncio.run(
            drop_table_objects_pass_async(location=self.location, materialization_id="mat-1", store=store)
        )

        self.assertEqual((result.deleted_count, result.complete), (b + b // 2, True))
        self.assertEqual(len(store.batches), 2)
        self.assertTrue(all(name.startswith("destination-cleanup") for name in store.threads), store.threads)


class MemoryStoreDeleteTests(ChunkedPassScenarios, unittest.TestCase):
    """`MemoryStore`'s own chunks of a hundred, one delete at a time inside each, on an Azure location."""

    batch_size = 100
    budget = 10_000
    location = "abfss://lake@acct.dfs.core.windows.net/materializer/mat-1/orders"
    root = "abfss://lake@acct.dfs.core.windows.net"
    container = "lake@acct"


class ThousandObjectChunkDeleteTests(ChunkedPassScenarios, unittest.TestCase):
    """A store taking a thousand deletes a request, as S3's `DeleteObjects`, on an S3 location."""

    batch_size = 1000
    budget = 100_000
    location = "s3://lake/materializer/mat-1/orders"
    root = "s3://lake"
    container = "lake"


class OneByOneDeleteTests(MemoryPassHarness, unittest.TestCase):
    """A store deleting one object per request (`delete_batch_size` 1): every chunk is one object, and a
    pass keeps to the per-object budget, `CLEANUP_PASS_MAX_OBJECTS`. On a local location, which the pass
    names `file:///`, in no container."""

    batch_size = 1
    location = "file:///lake/materializer/mat-1/orders"

    def test_each_delete_is_a_request_of_its_own(self) -> None:
        data = self.uris(3)
        metadata = self.uris(1, "metadata/")
        store = self.store(data + metadata + [self.pointer, self.sibling])
        store.script = {data[1]: [404]}

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (5, 0, True))
        self.assertEqual(result.bucket, "")
        self.assertEqual(store.batches, [[uri] for uri in data + metadata])
        self.assertEqual(store.deletes, [self.pointer])
        self.assertEqual(store.names(), [self.sibling])

    def test_a_pass_keeps_to_the_per_object_budget(self) -> None:
        store = self.store(self.uris(CLEANUP_PASS_MAX_OBJECTS + 500))
        self.assertEqual(pass_object_budget(store), CLEANUP_PASS_MAX_OBJECTS)

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.complete), (CLEANUP_PASS_MAX_OBJECTS, False))
        self.assertEqual(len(store.batches), CLEANUP_PASS_MAX_OBJECTS)
        self.assertEqual(store.deletes, [], "the pointer waits for the last pass")

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.complete), (500, True))
        self.assertEqual(store.names(), [])

    def test_each_failing_delete_is_retried_on_a_backoff_of_its_own(self) -> None:
        names = self.uris(3)
        store = self.store(names)
        store.script = {names[0]: [500, 429], names[2]: [503]}

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (3, 0, True))
        self.assertEqual(store.batches, [[names[0]]] * 3 + [[names[1]]] + [[names[2]]] * 2)
        self.assertEqual(self.delays, [1.0, 2.0, 1.0])

    def test_a_delete_that_keeps_failing_ends_the_pass_and_waits_for_the_next(self) -> None:
        names = self.uris(3)
        store = self.store(names)
        store.script = {names[1]: [503] * 4}

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self.run_pass(store)

        self.assertIn("could not delete 1 object from file:/// (503: 1)", logs.output[0])
        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (1, 1, False))
        self.assertEqual(self.delays, list(BATCH_RETRY_DELAYS_SECONDS))
        # The rest of the listing waits.
        self.assertEqual(store.names(), names[1:])

        result = self.run_pass(store)

        self.assertEqual((result.deleted_count, result.complete), (2, True))
        self.assertEqual(store.names(), [])

    def test_a_pass_whose_first_delete_keeps_failing_raises(self) -> None:
        names = self.uris(2)
        store = self.store(names)
        store.script = {names[0]: [503] * 4}

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(store)

        self.assertIn("could not delete 1 object from file:/// (503: 1)", str(raised.exception))
        self.assertEqual(store.names(), names)

    def test_an_object_the_store_refuses_is_left_and_the_pass_goes_on(self) -> None:
        names = self.uris(3)
        store = self.store(names)
        store.script = {names[1]: [403] * 2}

        with self.assertLogs(objects.LOGGER, "WARNING") as logs:
            result = self.run_pass(store)

        self.assertIn(names[1], logs.output[0])
        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (2, 1, False))
        self.assertEqual(self.delays, [], "a refusal is not retried")

        with self.assertRaises(DestinationDeleteError) as raised:
            self.run_pass(store)
        self.assertIn("403: 1", str(raised.exception))
        self.assertIn("Event-Based hold", str(raised.exception))
        self.assertEqual(store.names(), [names[1]])

    def test_retries_never_start_after_the_pass_deadline(self) -> None:
        names = self.uris(3)
        store = self.store(names)
        store.script = {names[1]: [503] * 4}
        # The pass starts at 0 with 10 s and reads the clock before each chunk after the first
        # (1: go on); the failure then reads 5 (a retry in 1 s fits), and 8.5 (one in 2 s would not).
        clock = iter([0.0, 1.0, 5.0, 8.5]).__next__

        with self.assertLogs(objects.LOGGER, "WARNING"):
            result = self.run_pass(store, max_seconds=10, clock=clock)

        self.assertEqual(self.delays, [1.0])
        self.assertEqual((result.deleted_count, result.failed_count, result.complete), (1, 1, False))
        self.assertEqual(store.names(), names[1:])

    def test_the_time_budget_ends_a_pass_once_it_has_deleted_an_object(self) -> None:
        store = self.store(self.uris(5))
        clock = iter([0.0] + [99.0] * 10).__next__  # the pass starts, then every reading is late

        result = self.run_pass(store, max_seconds=10, clock=clock)

        self.assertEqual((result.deleted_count, result.complete), (1, False))
        self.assertEqual(len(store.batches), 1)


if __name__ == "__main__":
    unittest.main()
