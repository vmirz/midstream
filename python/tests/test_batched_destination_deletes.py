"""Destination cleanup through the real google-cloud-storage client, which batches the deletes.

The client talks to an in-process fake of the GCS JSON API — object listing, and the batch
endpoint with multipart/mixed requests and responses — so these tests run the library's own
batching: it sends up to 100 calls in one request, retries none of them, and reports each call's
status only in the batch's responses.
"""

from __future__ import annotations

import asyncio
from collections import Counter
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
except ImportError:  # pragma: no cover - google-cloud-storage is a dependency of the package
    storage = None

from midstream.storage import objects
from midstream.storage.objects import (
    BATCH_RETRY_DELAYS_SECONDS,
    CLEANUP_PASS_MAX_OBJECTS,
    DELETE_BATCH_SIZE,
    DestinationDeleteError,
    drop_table_objects,
    drop_table_objects_pass,
    drop_table_objects_pass_async,
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


def _error_body(status: int) -> bytes:
    message = {403: "Object is under active Event-Based hold and cannot be deleted."}.get(status, REASONS.get(status, "error"))
    return json.dumps({"error": {"code": status, "message": message}}).encode()


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
class BatchedDestinationDeleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.delays: list[float] = []

    def client(self, api: FakeGcsJsonApi):
        return storage.Client(
            project="test",
            credentials=AnonymousCredentials(),
            _http=api,
            client_options={"api_endpoint": ENDPOINT},
        )

    def run_pass(self, api: FakeGcsJsonApi, **kwargs):
        kwargs.setdefault("max_seconds", None)
        return drop_table_objects_pass(
            location=LOCATION,
            materialization_id="mat-1",
            client=self.client(api),
            sleep=self.delays.append,
            **kwargs,
        )

    def test_deletes_go_a_hundred_to_a_batch_request(self) -> None:
        api = FakeGcsJsonApi(owned(250) + owned(1, METADATA) + [POINTER, SIBLING])

        result = self.run_pass(api)

        self.assertTrue(result.complete)
        self.assertEqual((result.deleted_count, result.failed_count), (252, 0))
        self.assertEqual(api.names, {SIBLING})
        # Two listings, one per prefix; four batch requests; one delete, the pointer's, last.
        self.assertEqual(api.requests, Counter({"list": 2, "batch": 4, "delete": 1}))
        self.assertEqual([len(names) for names in api.batches], [DELETE_BATCH_SIZE, DELETE_BATCH_SIZE, 50, 1])
        self.assertEqual(self.delays, [])

    def test_a_batched_pass_is_held_to_its_own_budget_of_listing_pages(self) -> None:
        api = FakeGcsJsonApi(owned(1_500))

        # More than a per-object pass may take: batched, one pass deletes it all.
        self.assertGreater(1_500, CLEANUP_PASS_MAX_OBJECTS)
        result = self.run_pass(api)
        self.assertEqual((result.deleted_count, result.complete), (1_500, True))

        api = FakeGcsJsonApi(owned(350))
        with patch.object(objects, "CLEANUP_PASS_MAX_BATCHED_OBJECTS", 300):
            result = self.run_pass(api)
        self.assertEqual((result.deleted_count, result.complete), (300, False))

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
        self.assertEqual(api.requests["batch"], 1)
        self.assertEqual(self.delays, [])

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

        self.assertEqual((result.deleted_count, result.complete), (DELETE_BATCH_SIZE, False))
        self.assertEqual(api.requests["batch"], 1)

    def test_a_full_delete_runs_batched_passes_until_nothing_is_left(self) -> None:
        api = FakeGcsJsonApi(owned(1_234) + [SIBLING])

        result = drop_table_objects(
            location=LOCATION,
            materialization_id="mat-1",
            client=self.client(api),
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
                client=self.client(api),
            )
        )

        self.assertEqual((result.deleted_count, result.complete), (150, True))
        self.assertEqual(api.requests["batch"], 2)
        self.assertTrue(all(name.startswith("destination-cleanup") for name in api.threads), api.threads)


if __name__ == "__main__":
    unittest.main()
