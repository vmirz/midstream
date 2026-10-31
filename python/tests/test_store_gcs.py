"""`GcsStore` (`lakehouse/store/gcs.py`): GCS behind the object store contract.

Over `gcs_fakes.FakeGcs` the conformance suite passes, conditional deletes included; the requests
carry exactly the arguments the code before the store sent (`if_generation_match` 0 for create-only,
the generation for a compare-and-swap, nothing for an unconditional write); every error of the
library maps to the contract's. Batch deletes run through the real google-cloud-storage client
against an in-process fake of the JSON API (as `test_batched_destination_deletes.py` does), so the
library's own batching is what is tested: a hundred calls per request, each answer classified, a
request that fails failing every call in it.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from email.parser import BytesParser
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gcs_fakes import FakeBlob, FakeGcs  # noqa: E402

from google.api_core import exceptions as api_exceptions  # noqa: E402
from google.auth import exceptions as auth_exceptions  # noqa: E402
from google.auth.credentials import AnonymousCredentials  # noqa: E402
from google.cloud import storage  # noqa: E402
import requests  # noqa: E402

from midstream.lakehouse.store import (  # noqa: E402
    ABSENT,
    ObjectNotFoundError,
    ObjectStore,
    PreconditionFailedError,
    StoreConfigurationError,
    StoreError,
)
from midstream.lakehouse.store.check import format_result, run_checks  # noqa: E402
from midstream.lakehouse.store.gcs import BATCH_REQUEST_TIMEOUT_SECONDS, BATCH_SIZE, GcsStore, _info  # noqa: E402

ENDPOINT = "https://storage.test"
#: What GCS says, in place of "No such object", when the bucket is not there.
NO_SUCH_BUCKET = "The specified bucket does not exist."
REASONS = {204: "No Content", 403: "Forbidden", 404: "Not Found", 429: "Too Many Requests", 500: "Internal Server Error", 503: "Service Unavailable"}
BOUNDARY = "batch_pK7JBAk73-E=_AA5eFwv4m2Q="
CREATED = "2026-10-30T12:00:00.123Z"


class FakeJsonApi:
    """Enough of the GCS JSON API behind `requests.Session.request` for listings and deletes.

    `objects` maps `(bucket, name)` to a size. Listings page 1,000 at a time; a batch request is
    answered part by part, in order; `script[name]` holds the statuses the object's next deletes get
    (a 2xx or 404 removes it), `batch_script` the outcome of the next batch requests themselves - a
    status, an exception to raise, `"not-multipart"` for the answer fake-gcs-server gives, or None to
    answer normally. The bucket metadata google-cloud-storage 3.x fetches for its telemetry is
    answered but not counted. A request for an object in one of `missing_buckets` - any operation,
    a batched delete too - is answered as GCS answers it: 404, "The specified bucket does not
    exist.", a JSON error but for a media download's plain text.
    """

    def __init__(self, objects: dict[tuple[str, str], int]) -> None:
        self.objects = dict(objects)
        self.missing_buckets: set[str] = set()
        self.script: dict[str, list[int]] = {}
        self.batch_script: list[int | Exception | str | None] = []
        self.requests: Counter = Counter()
        self.batches: list[list[tuple[str, str]]] = []
        self.lock = threading.Lock()

    def request(self, method, url, data=None, headers=None, timeout=None, **kwargs):
        parsed = urlparse(url)
        path = parsed.path
        if method == "GET" and path.startswith("/storage/v1/b/") and "/o" not in path:
            bucket = {"kind": "storage#bucket", "name": path.rsplit("/", 1)[1], "projectNumber": "1", "location": "US"}
            return _response(method, url, 200, json.dumps(bucket).encode())
        named = re.search(r"/b/([^/]+)/o(?:/|$)", path)
        if named is not None and named.group(1) in self.missing_buckets:
            with self.lock:
                self.requests["missing-bucket"] += 1
            if path.startswith("/download/"):
                return _response(method, url, 404, NO_SUCH_BUCKET.encode(), content_type="text/html; charset=UTF-8")
            return _response(method, url, 404, _error_body(404, NO_SUCH_BUCKET))
        with self.lock:
            if method == "GET" and path.endswith("/o"):
                self.requests["list"] += 1
                return self._list(url, path.split("/")[4], parse_qs(parsed.query))
            if method == "POST" and path == "/batch/storage/v1":
                self.requests["batch"] += 1
                return self._batch(url, headers, data)
            if method == "GET" and path.startswith("/storage/v1/b/") and "/o/" in path:
                self.requests["get"] += 1
                bucket, name = _bucket_and_name(path)
                if (bucket, name) not in self.objects:
                    return _response(method, url, 404, _error_body(404, f"No such object: {bucket}/{name}"))
                item = {"name": name, "bucket": bucket, "size": str(self.objects[(bucket, name)]), "generation": "1000", "timeCreated": CREATED, "updated": CREATED}
                return _response(method, url, 200, json.dumps(item).encode())
            if method == "DELETE":
                self.requests["delete"] += 1
                status = self._delete(*_bucket_and_name(path))
                return _response(method, url, status, b"" if status == 204 else _error_body(status))
        raise AssertionError(f"unexpected request: {method} {url}")

    def _list(self, url: str, bucket: str, query: dict[str, list[str]]):
        prefix = query.get("prefix", [""])[0]
        token = query.get("pageToken", [""])[0]
        size = min(int(query.get("maxResults", ["1000"])[0]), 1000)
        names = sorted(name for (owner, name) in self.objects if owner == bucket and name.startswith(prefix) and name > token)
        items = [
            {"name": name, "bucket": bucket, "size": str(self.objects[(bucket, name)]), "generation": str(1000 + index), "timeCreated": CREATED, "updated": CREATED}
            for index, name in enumerate(names[:size])
        ]
        payload: dict = {"kind": "storage#objects", "items": items}
        if len(names) > size:
            payload["nextPageToken"] = names[size - 1]
        return _response("GET", url, 200, json.dumps(payload).encode())

    def _batch(self, url: str, headers: dict, data: str):
        outcome = self.batch_script.pop(0) if self.batch_script else None
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == "not-multipart":
            return _response("POST", url, 200, b"{}")
        if outcome is not None:
            return _response("POST", url, outcome, _error_body(outcome))
        message = BytesParser().parsebytes(f"Content-Type: {headers['Content-Type']}\r\n\r\n{data}".encode())
        calls: list[tuple[str, str]] = []
        parts = []
        for call in message.get_payload():
            method, call_url, _ = call.get_payload().lstrip().split("\n", 1)[0].split(" ", 2)
            assert method == "DELETE", method
            bucket, name = _bucket_and_name(urlparse(call_url).path)
            calls.append((bucket, name))
            status = self._delete(bucket, name)
            body = b"" if status == 204 else _error_body(status, NO_SUCH_BUCKET if bucket in self.missing_buckets else None)
            inner = f"HTTP/1.1 {status} {REASONS[status]}\r\nContent-Type: application/json; charset=UTF-8\r\nContent-Length: {len(body)}\r\n\r\n"
            parts.append(f"--{BOUNDARY}\r\nContent-Type: application/http\r\nContent-ID: <response-{len(parts) + 1}>\r\n\r\n".encode() + inner.encode() + body + b"\r\n")
        self.batches.append(calls)
        content = b"".join(parts) + f"--{BOUNDARY}--\r\n".encode()
        return _response("POST", url, 200, content, content_type=f"multipart/mixed; boundary={BOUNDARY}")

    def _delete(self, bucket: str, name: str) -> int:
        if bucket in self.missing_buckets:
            return 404
        planned = self.script.get(name)
        status = planned.pop(0) if planned else (204 if (bucket, name) in self.objects else 404)
        if 200 <= status < 300 or status == 404:
            self.objects.pop((bucket, name), None)
        return status


def _bucket_and_name(path: str) -> tuple[str, str]:
    bucket_part, _, name = path.partition("/o/")
    return bucket_part.rsplit("/b/", 1)[1], unquote(name)


def _error_body(status: int, message: str | None = None) -> bytes:
    if message is None:
        message = {403: "Object is under active Event-Based hold and cannot be deleted."}.get(status, REASONS.get(status, "error"))
        return json.dumps({"error": {"code": status, "message": message}}).encode()
    return json.dumps({"error": {"code": status, "message": message, "errors": [{"message": message, "domain": "global", "reason": "notFound"}]}}).encode()


def _response(method: str, url: str, status: int, content: bytes, content_type: str = "application/json; charset=UTF-8"):
    response = requests.Response()
    response.status_code = status
    response._content = content
    response.headers["Content-Type"] = content_type
    response.request = requests.Request(method, url).prepare()
    return response


def real_client(api: FakeJsonApi) -> storage.Client:
    return storage.Client(project="test", credentials=AnonymousCredentials(), _http=api, client_options={"api_endpoint": ENDPOINT})


def uris(count: int, bucket: str = "lake", prefix: str = "t/data/") -> list[str]:
    return [f"gs://{bucket}/{prefix}part-{index:05d}.parquet" for index in range(count)]


def objects_of(uri_list: list[str]) -> dict[tuple[str, str], int]:
    result = {}
    for uri in uri_list:
        bucket, _, name = uri.removeprefix("gs://").partition("/")
        result[(bucket, name)] = 10
    return result


class GcsConformanceTests(unittest.TestCase):
    def test_every_check_passes_over_the_fake(self) -> None:
        store = GcsStore(FakeGcs().client())
        self.assertIsInstance(store, ObjectStore)
        results = run_checks(store, "gs://lake/check", writers=8, rounds=15, listing_objects=200, large_bytes=128 * 1024)
        self.assertEqual([format_result(result) for result in results if not result.passed], [])
        conditional = next(result for result in results if result.name == "conditional_delete")
        self.assertFalse(conditional.detail.startswith("skipped"))
        # The double cannot batch: delete_many sends one request per object, and says so.
        self.assertEqual((store.scheme, store.delete_batch_size), ("gs", 1))


class GcsRequestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeGcs()
        self.store = GcsStore(self.fake.client())

    def test_conditions_become_exactly_these_generation_matches(self) -> None:
        with patch.object(FakeBlob, "upload_from_string", autospec=True, side_effect=FakeBlob.upload_from_string) as upload:
            created = self.store.put("gs://lake/p", b"1", if_version=ABSENT, content_type="application/json")
            swapped = self.store.put("gs://lake/p", b"2", if_version=created)
            plain = self.store.put("gs://lake/p", b"3")
        sent = [call.kwargs for call in upload.call_args_list]
        self.assertEqual(sent[0], {"content_type": "application/json", "if_generation_match": 0})
        self.assertEqual(sent[1], {"content_type": "application/octet-stream", "if_generation_match": int(created)})
        self.assertIsInstance(sent[1]["if_generation_match"], int)
        self.assertEqual(sent[2], {"content_type": "application/octet-stream"})
        self.assertEqual([upload.call_args_list[index].args[1] for index in range(3)], [b"1", b"2", b"3"])
        generations = [stored.generation for stored in self.fake.objects.values()]
        self.assertEqual(plain, str(generations[0]))
        self.assertTrue(int(created) < int(swapped) < int(plain))

    def test_put_file_sends_the_same_conditions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "f")
            Path(source).write_bytes(b"file bytes")
            with patch.object(FakeBlob, "upload_from_filename", autospec=True, side_effect=FakeBlob.upload_from_filename) as upload:
                created = self.store.put_file("gs://lake/f", source, if_version=ABSENT)
                self.store.put_file("gs://lake/f", source, if_version=created, content_type="application/x-parquet")
                self.store.put_file("gs://lake/f", source)
        self.assertEqual([call.args[1] for call in upload.call_args_list], [source] * 3)
        self.assertEqual(
            [call.kwargs for call in upload.call_args_list],
            [
                {"content_type": "application/octet-stream", "if_generation_match": 0},
                {"content_type": "application/x-parquet", "if_generation_match": int(created)},
                {"content_type": "application/octet-stream"},
            ],
        )

    def test_deletes_go_by_name_with_the_generation_as_a_condition(self) -> None:
        version = self.store.put("gs://lake/d", b"x")
        self.store.get("gs://lake/d")  # a Blob that read the object knows its generation
        seen: list[tuple[int | None, dict]] = []
        original = FakeBlob.delete

        def record(blob, **kwargs):
            seen.append((blob.generation, kwargs))
            return original(blob, **kwargs)

        with patch.object(FakeBlob, "delete", autospec=True, side_effect=record):
            self.assertTrue(self.store.delete("gs://lake/d", if_version=version))
            self.store.put("gs://lake/e", b"x")
            self.assertTrue(self.store.delete("gs://lake/e"))
            self.assertFalse(self.store.delete("gs://lake/e"))
        self.assertEqual(seen, [(None, {"if_generation_match": int(version)}), (None, {}), (None, {})])

    def test_reads_ask_for_the_range_given_and_nothing_else(self) -> None:
        version = self.store.put("gs://lake/r", bytes(range(50)))
        with patch.object(FakeBlob, "download_as_bytes", autospec=True, side_effect=FakeBlob.download_as_bytes) as download:
            self.assertEqual(self.store.get("gs://lake/r"), (bytes(range(50)), version))
            self.assertEqual(self.store.get("gs://lake/r", start=5, end=9), (bytes(range(5, 10)), version))
            self.assertEqual(self.store.get("gs://lake/r", start=45)[0], bytes(range(45, 50)))
        self.assertEqual([call.kwargs for call in download.call_args_list], [{}, {"start": 5, "end": 9}, {"start": 45, "end": None}])
        self.assertEqual([operation for operation, _, _ in self.fake.calls].count("get_blob"), 0)

    def test_a_version_that_is_no_generation_is_refused_before_any_request(self) -> None:
        self.store.put("gs://lake/v", b"x")
        calls = len(self.fake.calls)
        for version in ("0", "-5", "abc", "1.5", ""):
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    self.store.put("gs://lake/v", b"y", if_version=version)
                with self.assertRaises(ValueError):
                    self.store.delete("gs://lake/v", if_version=version)
        for version in (7, b"7"):
            with self.subTest(version=version):
                with self.assertRaises(TypeError):
                    self.store.put("gs://lake/v", b"y", if_version=version)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            self.store.get("s3://lake/v")
        with self.assertRaises(ValueError):
            self.store.get("gs://lake/")
        self.assertEqual(len(self.fake.calls), calls)
        self.assertEqual(self.fake.get("gs://lake/v"), b"x")

    def test_a_download_that_records_no_generation_is_refused_not_asked_again(self) -> None:
        # Asking in a second request could pair these bytes with a newer generation: here another
        # writer commits right before that request, and a compare-and-swap with the generation it
        # names would overwrite that commit unseen. The read must fail instead.
        self.store.put("gs://lake/g", b"seq=1")
        original = FakeBlob.download_as_bytes

        def forgetful(blob, **kwargs):
            data = original(blob, **kwargs)
            blob.generation = None  # as when the answer carries no X-Goog-Generation
            return data

        def another_writer(operation, bucket, name):
            if operation == "get_blob":
                self.fake.fail = None
                self.store.put("gs://lake/g", b"seq=2, another writer's")
            return None

        self.fake.fail = another_writer
        with patch.object(FakeBlob, "download_as_bytes", autospec=True, side_effect=forgetful):
            for read in (lambda: self.store.get("gs://lake/g"), lambda: self.store.get("gs://lake/g", start=0, end=2)):
                with self.assertRaises(StoreError) as raised:
                    read()
                self.assertEqual(raised.exception.code, "NoGeneration")
                self.assertNotIsInstance(raised.exception, (ObjectNotFoundError, PreconditionFailedError))
            with tempfile.TemporaryDirectory() as directory:
                target = os.path.join(directory, "t")
                with self.assertRaises(StoreError) as raised:
                    self.store.get_to_file("gs://lake/g", target)
                self.assertEqual(raised.exception.code, "NoGeneration")
                self.assertFalse(os.path.exists(target))
        self.assertNotIn("get_blob", [operation for operation, _, _ in self.fake.calls])
        self.assertEqual(self.fake.get("gs://lake/g"), b"seq=1")

    def test_head_and_list_describe_objects(self) -> None:
        self.fake.clock = lambda: datetime(2026, 10, 30, 8, 0, tzinfo=UTC)
        version = self.store.put("gs://lake/h/one", b"12345")
        self.store.put("gs://lake/h/two", b"1")
        self.store.put("gs://lake/hx", b"1")
        info = self.store.head("gs://lake/h/one")
        self.assertEqual((info.uri, info.size, info.version, info.last_modified), ("gs://lake/h/one", 5, version, datetime(2026, 10, 30, 8, 0, tzinfo=UTC)))
        self.assertIsNone(self.store.head("gs://lake/h/none"))
        self.assertEqual([item.uri for item in self.store.list("gs://lake/h/")], ["gs://lake/h/one", "gs://lake/h/two"])
        self.assertEqual([item.uri for item in self.store.list("gs://lake/h")], ["gs://lake/h/one", "gs://lake/h/two", "gs://lake/hx"])
        self.assertIn(("list_blobs", "lake", "h/"), self.fake.calls)

    def test_object_times_fall_back_to_updated(self) -> None:
        updated = datetime(2026, 10, 1, 9, 30)
        info = _info("gs://lake/x", SimpleNamespace(generation=12, size="3", time_created=None, updated=updated))
        self.assertEqual((info.version, info.size, info.last_modified), ("12", 3, updated.replace(tzinfo=UTC)))
        self.assertIsNone(_info("gs://lake/x", SimpleNamespace(generation=1, size=0, time_created=None, updated=None)).last_modified)

    def test_the_client_is_built_once_on_first_use(self) -> None:
        built: list[object] = []

        def factory() -> object:
            time.sleep(0.05)
            client = FakeGcs().client()
            built.append(client)
            return client

        store = GcsStore()
        with patch("google.cloud.storage.Client", side_effect=factory):
            self.assertEqual(built, [])
            seen: list[object] = []
            threads = [threading.Thread(target=lambda: seen.append(store.client)) for _ in range(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(len(built), 1)
        self.assertTrue(all(client is built[0] for client in seen))
        given = FakeGcs().client()
        self.assertIs(GcsStore(given).client, given)


class GcsMissingBucketTests(unittest.TestCase):
    """A bucket that is not there is configuration, never a missing object - through the real client,
    for every operation - while a missing object in a bucket that is stays a missing object."""

    def setUp(self) -> None:
        self.api = FakeJsonApi(objects_of(uris(2)))
        self.api.missing_buckets = {"gone"}
        self.store = GcsStore(real_client(self.api))
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = directory.name

    def test_every_operation_on_a_missing_bucket_is_a_configuration_error(self) -> None:
        source = os.path.join(self.directory, "source")
        Path(source).write_bytes(b"x")
        target = os.path.join(self.directory, "target")
        for name, attempt in (
            ("get", lambda: self.store.get("gs://gone/t/x")),
            ("range", lambda: self.store.get("gs://gone/t/x", start=0, end=1)),
            ("head", lambda: self.store.head("gs://gone/t/x")),
            ("create-only put", lambda: self.store.put("gs://gone/t/x", b"x", if_version=ABSENT)),
            ("compare-and-swap", lambda: self.store.put("gs://gone/t/x", b"x", if_version="1001")),
            ("put_file", lambda: self.store.put_file("gs://gone/t/x", source, if_version=ABSENT)),
            ("get_to_file", lambda: self.store.get_to_file("gs://gone/t/x", target)),
            ("delete", lambda: self.store.delete("gs://gone/t/x")),
            ("conditional delete", lambda: self.store.delete("gs://gone/t/x", if_version="1001")),
            ("list", lambda: list(self.store.list("gs://gone/t/"))),
        ):
            with self.subTest(operation=name):
                with self.assertRaises(StoreConfigurationError) as caught:
                    attempt()
                error = caught.exception
                self.assertEqual((error.status, error.retryable, error.uri), (404, False, "gs://gone/t/" if name == "list" else "gs://gone/t/x"))
                self.assertIn("the bucket does not exist", str(error))
                self.assertIsNotNone(error.__cause__)
                self.assertFalse(os.path.exists(target))
        self.assertEqual(self.api.requests["missing-bucket"], 10)

    def test_a_missing_object_in_a_bucket_that_exists_is_still_missing(self) -> None:
        self.assertIsNone(self.store.head("gs://lake/t/missing"))
        self.assertFalse(self.store.delete("gs://lake/t/missing"))
        self.assertEqual(list(self.store.list("gs://lake/none/")), [])

    def test_deletes_in_a_missing_bucket_fail_for_good_batched_or_not(self) -> None:
        names = uris(2, bucket="gone") + uris(1)
        for batch_deletes, requests_made in ((True, Counter({"batch": 2})), (False, Counter({"delete": 1}))):
            with self.subTest(batch_deletes=batch_deletes):
                api = FakeJsonApi(objects_of(uris(1)))
                api.missing_buckets = {"gone"}
                outcome = GcsStore(real_client(api), batch_deletes=batch_deletes).delete_many(names)
                self.assertEqual(outcome.deleted, (names[2],))
                self.assertEqual([(failure.uri, failure.status, failure.retryable) for failure in outcome.failed], [(names[0], 404, False), (names[1], 404, False)])
                self.assertTrue(all("bucket does not exist" in failure.message for failure in outcome.failed))
                self.assertEqual(api.requests - Counter({"missing-bucket": api.requests["missing-bucket"]}), requests_made)

    def test_only_the_whole_message_says_the_bucket_is_missing(self) -> None:
        # An object's name may hold any text: a message that merely contains GCS's words is no answer
        # about the bucket.
        fake = FakeGcs()
        store = GcsStore(fake.client())
        for message, kind in (
            (NO_SUCH_BUCKET, StoreConfigurationError),
            ("the specified bucket does not exist", StoreConfigurationError),
            (f"No such object: lake/{NO_SUCH_BUCKET}", ObjectNotFoundError),
            ("Not Found", ObjectNotFoundError),
        ):
            with self.subTest(message=message):
                error = api_exceptions.NotFound(message, errors=[{"message": message, "reason": "notFound"}])
                fake.fail = lambda operation, bucket, name, error=error: error
                with self.assertRaises(kind):
                    store.get("gs://lake/x")
                if kind is ObjectNotFoundError:
                    self.assertIsNone(store.head("gs://lake/x"))
                else:
                    with self.assertRaises(kind):
                        store.head("gs://lake/x")


class GcsErrorMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeGcs()
        self.store = GcsStore(self.fake.client())
        self.version = self.store.put("gs://lake/x", b"x")

    def inject(self, operation: str, error: BaseException) -> None:
        self.fake.fail = lambda op, bucket, name: error if op == operation else None

    def assertMapped(self, call, kind: type[StoreError], status: int | None, cause: BaseException) -> StoreError:
        with self.assertRaises(kind) as raised:
            call()
        self.assertIs(type(raised.exception), kind)
        self.assertEqual(raised.exception.status, status)
        self.assertIs(raised.exception.__cause__, cause)
        self.assertEqual(raised.exception.uri, "gs://lake/x")
        return raised.exception

    def test_the_library_errors_map_to_the_contract(self) -> None:
        cases = [
            (api_exceptions.PreconditionFailed("moved"), PreconditionFailedError, 412),
            (api_exceptions.NotFound("gone"), ObjectNotFoundError, 404),
            (api_exceptions.ServiceUnavailable("busy"), StoreError, 503),
            (api_exceptions.TooManyRequests("slow down"), StoreError, 429),
            (api_exceptions.Forbidden("no"), StoreError, 403),
            (api_exceptions.InternalServerError("oops"), StoreError, 500),
            (api_exceptions.GoogleAPICallError("no status"), StoreError, None),
            (requests.exceptions.ConnectionError("reset by peer"), StoreError, None),
            (requests.exceptions.ReadTimeout("slow"), StoreError, None),
            (auth_exceptions.TransportError("metadata server"), StoreError, None),
            (ConnectionResetError("reset"), StoreError, None),
            (TimeoutError("timed out"), StoreError, None),
        ]
        for error, kind, status in cases:
            with self.subTest(error=type(error).__name__):
                self.inject("download", error)
                mapped = self.assertMapped(lambda: self.store.get("gs://lake/x"), kind, status, error)
                self.assertEqual(mapped.retryable, kind is StoreError and (status is None or status in (429, 500, 503)))

    def test_a_503_is_a_retryable_store_error_with_its_status(self) -> None:
        error = api_exceptions.ServiceUnavailable("backend error", errors=[{"reason": "backendError"}])
        self.inject("upload", error)
        mapped = self.assertMapped(lambda: self.store.put("gs://lake/x", b"y"), StoreError, 503, error)
        self.assertTrue(mapped.retryable)
        self.assertEqual(mapped.code, "backendError")

    def test_retries_that_gave_up_report_the_last_failure(self) -> None:
        last = api_exceptions.ServiceUnavailable("still busy")
        self.inject("download", api_exceptions.RetryError("deadline exceeded", cause=last))
        with self.assertRaises(StoreError) as raised:
            self.store.get("gs://lake/x")
        self.assertEqual((type(raised.exception), raised.exception.status), (StoreError, 503))
        self.inject("download", api_exceptions.RetryError("deadline exceeded", cause=ConnectionResetError("reset")))
        with self.assertRaises(StoreError) as raised:
            self.store.get("gs://lake/x")
        self.assertIsNone(raised.exception.status)

    def test_doubles_are_classified_by_name_and_status(self) -> None:
        class DataCorruption(Exception):
            pass

        class InvalidResponse(Exception):
            def __init__(self, status: int) -> None:
                super().__init__(f"status {status}")
                self.response = SimpleNamespace(status_code=status)

        class PreconditionFailed(Exception):
            pass

        class Answer(Exception):
            code = 502

        for error, kind, status in (
            (DataCorruption("checksum mismatch"), StoreError, None),
            (InvalidResponse(503), StoreError, 503),
            (InvalidResponse(412), PreconditionFailedError, 412),
            (PreconditionFailed("named so"), PreconditionFailedError, 412),
            (Answer("bad gateway"), StoreError, 502),
        ):
            with self.subTest(error=type(error).__name__):
                self.inject("download", error)
                self.assertMapped(lambda: self.store.get("gs://lake/x"), kind, status, error)

    def test_an_error_that_is_no_storage_failure_is_raised_as_it_is(self) -> None:
        for error in (ValueError("a bug"), KeyError("k"), FileNotFoundError("local")):
            with self.subTest(error=type(error).__name__):
                self.inject("download", error)
                with self.assertRaises(type(error)) as raised:
                    self.store.get("gs://lake/x")
                self.assertIs(raised.exception, error)

    def test_a_missing_object_under_a_compare_and_swap_is_a_lost_race(self) -> None:
        error = api_exceptions.NotFound("no such object")
        self.inject("upload", error)
        self.assertMapped(lambda: self.store.put("gs://lake/x", b"y", if_version=self.version), PreconditionFailedError, 404, error)
        self.assertMapped(lambda: self.store.put("gs://lake/x", b"y"), ObjectNotFoundError, 404, error)
        # And without injection, as the fake (and GCS) answer: 412.
        self.fake.fail = None
        self.store.delete("gs://lake/x")
        with self.assertRaises(PreconditionFailedError):
            self.store.put("gs://lake/x", b"y", if_version=self.version)

    def test_head_delete_and_list_answer_as_the_contract_says(self) -> None:
        self.inject("get_blob", api_exceptions.NotFound("gone"))
        self.assertIsNone(self.store.head("gs://lake/x"))
        busy = api_exceptions.ServiceUnavailable("busy")
        self.inject("get_blob", busy)
        self.assertMapped(lambda: self.store.head("gs://lake/x"), StoreError, 503, busy)
        self.inject("delete", api_exceptions.NotFound("gone"))
        self.assertFalse(self.store.delete("gs://lake/x"))
        moved = api_exceptions.PreconditionFailed("moved")
        self.inject("delete", moved)
        self.assertMapped(lambda: self.store.delete("gs://lake/x", if_version=self.version), PreconditionFailedError, 412, moved)
        held = api_exceptions.Forbidden("held")
        self.inject("delete", held)
        self.assertMapped(lambda: self.store.delete("gs://lake/x"), StoreError, 403, held)
        self.fake.fail = lambda op, bucket, name: busy if op == "list_blobs" else None
        listing = self.store.list("gs://lake/")
        with self.assertRaises(StoreError) as raised:
            list(listing)
        self.assertEqual((raised.exception.status, raised.exception.uri), (503, "gs://lake/"))

    def test_a_failed_download_to_a_file_leaves_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            for error, kind in ((api_exceptions.ServiceUnavailable("busy"), StoreError), (api_exceptions.NotFound("gone"), ObjectNotFoundError)):
                with self.subTest(error=type(error).__name__):
                    Path(target).write_bytes(b"stale")
                    self.inject("download", error)
                    with self.assertRaises(kind):
                        self.store.get_to_file("gs://lake/x", target)
                    self.assertFalse(os.path.exists(target))
            self.fake.fail = None
            info = self.store.get_to_file("gs://lake/x", target)
            self.assertEqual((info.size, info.version), (1, self.version))
            with self.assertRaises(FileNotFoundError):
                self.store.put_file("gs://lake/y", os.path.join(directory, "missing"))


class GcsBatchDeleteTests(unittest.TestCase):
    def store_over(self, api: FakeJsonApi, **kwargs) -> GcsStore:
        return GcsStore(real_client(api), **kwargs)

    def test_each_answer_in_a_batch_is_classified(self) -> None:
        names = uris(5)
        api = FakeJsonApi(objects_of(names[:4]))
        api.script = {
            "t/data/part-00001.parquet": [404],
            "t/data/part-00002.parquet": [403],
            "t/data/part-00003.parquet": [503],
        }
        original = storage.Bucket.delete_blob
        with patch.object(storage.Bucket, "delete_blob", autospec=True, side_effect=original) as delete_blob:
            outcome = self.store_over(api).delete_many(names)
        self.assertEqual(outcome.deleted, (names[0], names[1], names[4]))
        self.assertEqual([(failure.uri, failure.status, failure.retryable) for failure in outcome.failed], [(names[2], 403, False), (names[3], 503, True)])
        self.assertFalse(any(failure.whole_request for failure in outcome.failed))
        self.assertEqual(outcome.failed[0].message, "403 Forbidden: Object is under active Event-Based hold and cannot be deleted.")
        self.assertEqual(outcome.failed[1].message, "503 Service Unavailable: Service Unavailable")
        self.assertEqual(api.requests, Counter({"batch": 1}))
        self.assertEqual(
            [(call.args[1], call.kwargs) for call in delete_blob.call_args_list],
            [(name.removeprefix("gs://lake/"), {"timeout": BATCH_REQUEST_TIMEOUT_SECONDS, "retry": None}) for name in names],
        )
        self.assertEqual(sorted(api.objects), [("lake", "t/data/part-00002.parquet"), ("lake", "t/data/part-00003.parquet")])

    def test_a_request_that_fails_fails_every_delete_in_it(self) -> None:
        for outcome_of_request, status in (
            (requests.exceptions.ConnectionError("connection reset"), None),
            (503, 503),
            (403, 403),
            ("not-multipart", None),  # fake-gcs-server's answer: the library cannot read it
        ):
            with self.subTest(request=str(outcome_of_request)):
                names = uris(3)
                api = FakeJsonApi(objects_of(names))
                api.batch_script = [outcome_of_request]
                outcome = self.store_over(api).delete_many(names)
                self.assertEqual(outcome.deleted, ())
                self.assertEqual([failure.uri for failure in outcome.failed], names)
                self.assertTrue(all(failure.status == status for failure in outcome.failed))
                self.assertTrue(all(failure.retryable is (status is None or status >= 500) for failure in outcome.failed))
                self.assertTrue(all(failure.whole_request for failure in outcome.failed))
                self.assertEqual(len(api.objects), 3)
                self.assertTrue(outcome.failed[0].message)

    def test_more_than_a_hundred_go_a_hundred_to_a_request(self) -> None:
        names = uris(250)
        api = FakeJsonApi(objects_of(names))
        outcome = self.store_over(api).delete_many(names)
        self.assertEqual(outcome.deleted, tuple(names))
        self.assertEqual(outcome.failed, ())
        self.assertEqual([len(batch) for batch in api.batches], [BATCH_SIZE, BATCH_SIZE, 50])
        self.assertEqual(api.requests, Counter({"batch": 3}))
        self.assertEqual(api.objects, {})

    def test_one_failed_request_among_several_fails_only_its_own(self) -> None:
        names = uris(250)
        api = FakeJsonApi(objects_of(names))
        api.batch_script = [None, 503, None]
        outcome = self.store_over(api).delete_many(names)
        self.assertEqual(outcome.deleted, tuple(names[:100] + names[200:]))
        self.assertEqual([failure.uri for failure in outcome.failed], names[100:200])
        self.assertEqual(len(api.objects), 100)

    def test_deletes_are_grouped_by_bucket(self) -> None:
        lake, other = uris(120), uris(30, bucket="other-lake")
        mixed = [uri for pair in zip(lake, other) for uri in pair] + lake[30:]
        api = FakeJsonApi(objects_of(mixed))
        outcome = self.store_over(api).delete_many(mixed)
        self.assertEqual(outcome.deleted, tuple(mixed))
        self.assertEqual([(len(batch), batch[0][0]) for batch in api.batches], [(100, "lake"), (20, "lake"), (30, "other-lake")])

    def test_without_batches_each_object_gets_a_request_of_its_own(self) -> None:
        names = uris(4)
        api = FakeJsonApi(objects_of(names[:3]))
        api.script = {"t/data/part-00001.parquet": [403]}
        outcome = self.store_over(api, batch_deletes=False).delete_many(names)
        self.assertEqual(outcome.deleted, (names[0], names[2], names[3]))
        self.assertEqual([(failure.uri, failure.status) for failure in outcome.failed], [(names[1], 403)])
        self.assertEqual(api.requests, Counter({"delete": 4}))

    def test_a_client_that_cannot_batch_deletes_one_by_one(self) -> None:
        fake = FakeGcs()
        store = GcsStore(fake.client())
        for name in "abcde":
            fake.put(f"gs://lake/{name}", b"x")
        errors = {
            "b": api_exceptions.Forbidden("held"),
            "c": api_exceptions.ServiceUnavailable("busy"),
            "d": requests.exceptions.ConnectionError("reset"),
            "e": api_exceptions.NotFound("gone already"),
        }
        fake.fail = lambda operation, bucket, name: errors.get(name) if operation == "delete" else None
        with patch.object(FakeBlob, "delete", autospec=True, side_effect=FakeBlob.delete) as delete:
            outcome = store.delete_many(["gs://lake/a", "gs://lake/b", "gs://lake/c", "gs://lake/d", "gs://lake/e", "gs://lake/missing"])
        # No retry inside delete_many: the caller retries what may succeed.
        self.assertEqual([call.kwargs for call in delete.call_args_list], [{"retry": None}] * 6)
        self.assertEqual(outcome.deleted, ("gs://lake/a", "gs://lake/e", "gs://lake/missing"))
        self.assertEqual(
            [(failure.uri, failure.status, failure.retryable) for failure in outcome.failed],
            [("gs://lake/b", 403, False), ("gs://lake/c", 503, True), ("gs://lake/d", None, True)],
        )
        self.assertIs(outcome.failed[0].error, errors["b"])
        self.assertEqual(fake.names(), ["gs://lake/b", "gs://lake/c", "gs://lake/d", "gs://lake/e"])

    def test_the_batch_size_is_what_one_request_deletes(self) -> None:
        # A caller budgets its requests by it (storage/objects.py), so a store deleting one object per
        # request - by choice or through a client that cannot batch - says 1.
        api = FakeJsonApi(objects_of(uris(3)))
        self.assertEqual(self.store_over(api).delete_batch_size, BATCH_SIZE)
        unbatched = self.store_over(api, batch_deletes=False)
        self.assertEqual(unbatched.delete_batch_size, 1)
        self.assertEqual(GcsStore(FakeGcs().client()).delete_batch_size, 1)
        lazy = GcsStore()
        self.assertEqual(lazy.delete_batch_size, BATCH_SIZE)
        self.assertIsNone(lazy._client, "asking builds no client")
        self.assertEqual(GcsStore(batch_deletes=False).delete_batch_size, 1)
        unbatched.batch_deletes = True
        self.assertEqual(unbatched.delete_batch_size, BATCH_SIZE)

    def test_uris_are_checked_before_any_request(self) -> None:
        api = FakeJsonApi(objects_of(uris(2)))
        with self.assertRaises(ValueError):
            self.store_over(api).delete_many(uris(2) + ["s3://lake/x"])
        self.assertEqual(api.requests, Counter())
        self.assertEqual(self.store_over(api).delete_many([]).deleted, ())

    def test_a_listing_pages_through_the_real_client(self) -> None:
        names = uris(1_050) + uris(3, prefix="t/metadata/")
        api = FakeJsonApi(objects_of(names))
        listed = list(self.store_over(api).list("gs://lake/t/data/"))
        self.assertEqual([item.uri for item in listed], uris(1_050))
        self.assertEqual(api.requests, Counter({"list": 2}))
        first = listed[0]
        self.assertEqual((first.size, first.version), (10, "1000"))
        self.assertEqual(first.last_modified, datetime(2026, 10, 30, 12, 0, 0, 123000, tzinfo=UTC))


if __name__ == "__main__":
    unittest.main()
