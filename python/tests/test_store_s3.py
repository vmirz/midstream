"""`S3Store` (`lakehouse/store/s3.py`): S3 behind the object store contract, over botocore's `Stubber`.

Every request goes to a real boto3 client whose answers are stubbed, so nothing leaves the process: the
parameters of each call are exactly these (`IfNoneMatch='*'` for create-only, the quoted ETag as
`IfMatch` for a compare-and-swap, `Range` as `bytes=a-b`, `DeleteObjects` quiet and a thousand keys at
a time), versions leave the store without quotes, and every answer maps to the contract's error (design
§6.2) - 412 and 409 `ConditionalRequestConflict` as lost races, a 404 under `If-Match` too, a HEAD's
bare 404 as a missing object, `NoSuchBucket` as configuration, failures in transit with no status - and
404 and 412 are judged by the status whatever code comes with them. A download whose checksum does not
match, or an answer that cannot be parsed, is a failure in transit too, and `get_to_file` copies a
body a bounded chunk at a time. The clients keep botocore's own checksum and signing defaults on a
custom endpoint exactly as on AWS.
"""

from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import io
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import zlib

import boto3
from botocore.awsrequest import AWSResponse
from botocore.config import Config
from botocore.exceptions import (
    ChecksumError,
    ConnectionClosedError,
    EndpointConnectionError,
    FlexibleChecksumError,
    NoCredentialsError,
    ReadTimeoutError,
    ResponseStreamingError,
)
from botocore.parsers import ResponseParserError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
import urllib3

from midstream.lakehouse.store import (
    ABSENT,
    ObjectInfo,
    ObjectNotFoundError,
    ObjectStore,
    PreconditionFailedError,
    StoreError,
    StoreSettings,
)
from midstream.lakehouse.store.s3 import DELETE_BATCH_SIZE, S3Store, client_config, delete_error_status, store_error

BUCKET = "lake"
VERSION = "9b2cf535f27731c974343645a3985328"
ETAG = f'"{VERSION}"'
NEWER = "2c7a0f3e8d1b4c5a6e7f8091a2b3c4d5"
MODIFIED = datetime(2026, 10, 30, 12, 0, 1, tzinfo=UTC)
#: The environment the clients are built in: no profile, no config file, no checksum overrides.
CLEAN_ENVIRONMENT = {
    "AWS_ACCESS_KEY_ID": "test-key",
    "AWS_SECRET_ACCESS_KEY": "test-secret",
    "AWS_CONFIG_FILE": os.devnull,
    "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
}
UNSET = ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_SESSION_TOKEN")


def clean_environment() -> patch:
    environment = {name: value for name, value in os.environ.items() if name not in UNSET}
    environment.update(CLEAN_ENVIRONMENT)
    return patch.dict(os.environ, environment, clear=True)


_SESSIONS: list[boto3.session.Session] = []


def test_client(endpoint: str | None = None) -> object:
    """A real client that never retries, so a failure in transit surfaces at once. One session makes
    them all, so S3's service model is loaded once."""
    if not _SESSIONS:
        _SESSIONS.append(boto3.session.Session(aws_access_key_id="test-key", aws_secret_access_key="test-secret", region_name="us-east-1"))
    return _SESSIONS[0].client(
        "s3",
        endpoint_url=endpoint,
        config=Config(retries={"mode": "standard", "total_max_attempts": 1}, s3={"addressing_style": "path"}),
    )


def body(data: bytes) -> StreamingBody:
    return StreamingBody(io.BytesIO(data), len(data))


class StubbedStoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        environment = clean_environment()
        environment.start()
        self.addCleanup(environment.stop)
        self.client = test_client()
        self.stubber = Stubber(self.client)
        self.stubber.activate()
        self.addCleanup(self.stubber.deactivate)
        self.store = S3Store(client=self.client, settings=StoreSettings())

    def tearDown(self) -> None:
        self.stubber.assert_no_pending_responses()

    def stub_error(self, method: str, code: str, status: int, expected: dict | None = None, message: str = "") -> None:
        self.stubber.add_client_error(method, service_error_code=code, service_message=message or code, http_status_code=status, expected_params=expected)


class S3RequestTests(StubbedStoreTestCase):
    def test_the_store_is_an_object_store_of_the_s3_scheme(self) -> None:
        self.assertIsInstance(self.store, ObjectStore)
        self.assertEqual((self.store.scheme, self.store.delete_batch_size), ("s3", 1000))
        self.assertFalse(self.store.supports_conditional_delete)

    def test_reads_ask_for_the_range_given_and_return_the_unquoted_etag(self) -> None:
        data = bytes(range(256)) * 4
        cases = (
            ((None, None), None, data),
            ((10, 19), "bytes=10-19", data[10:20]),
            ((1000, None), "bytes=1000-", data[1000:]),
            ((None, 9), "bytes=0-9", data[:10]),
            ((0, 0), "bytes=0-0", data[:1]),
        )
        for (start, end), requested, answer in cases:
            expected = {"Bucket": BUCKET, "Key": "t/range"}
            if requested is not None:
                expected["Range"] = requested
            self.stubber.add_response("get_object", {"Body": body(answer), "ETag": ETAG, "ContentLength": len(answer)}, expected)
            self.assertEqual(self.store.get("s3://lake/t/range", start=start, end=end), (answer, VERSION))

    def test_an_impossible_range_is_refused_before_any_request(self) -> None:
        for start, end in ((-1, None), (5, 4), (None, -2)):
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                self.store.get("s3://lake/t/x", start=start, end=end)

    def test_conditions_become_exactly_these_headers(self) -> None:
        cases = (
            (None, {}),
            (ABSENT, {"IfNoneMatch": "*"}),
            (VERSION, {"IfMatch": ETAG}),
            (ETAG, {"IfMatch": ETAG}),  # a quoted version is quoted once
        )
        for condition, headers in cases:
            with self.subTest(condition=condition):
                expected = {"Bucket": BUCKET, "Key": "t/pointer", "Body": b"payload", "ContentType": "application/json", **headers}
                self.stubber.add_response("put_object", {"ETag": f'"{NEWER}"'}, expected)
                written = self.store.put("s3://lake/t/pointer", b"payload", if_version=condition, content_type="application/json")
                self.assertEqual(written, NEWER)

    def test_put_file_streams_the_open_file_in_one_put_object(self) -> None:
        bodies: list[object] = []
        self.client.meta.events.register_first("before-parameter-build.s3.PutObject", lambda params, **_: bodies.append(params["Body"]))
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "data.parquet")
            Path(path).write_bytes(b"x" * 4096)
            for condition, headers in ((ABSENT, {"IfNoneMatch": "*"}), (VERSION, {"IfMatch": ETAG}), (None, {})):
                expected = {"Bucket": BUCKET, "Key": "t/data.parquet", "Body": ANY, "ContentType": "application/octet-stream", **headers}
                self.stubber.add_response("put_object", {"ETag": ETAG}, expected)
                self.assertEqual(self.store.put_file("s3://lake/t/data.parquet", path, if_version=condition), VERSION)
            with self.assertRaises(FileNotFoundError):
                self.store.put_file("s3://lake/t/data.parquet", os.path.join(directory, "missing"))
        self.assertEqual(len(bodies), 3)
        for handle in bodies:
            # The open file itself, not its bytes read into memory, and closed once sent.
            self.assertEqual(getattr(handle, "name", None), path)
            self.assertTrue(handle.closed)

    def test_a_version_that_is_no_etag_is_refused_before_any_request(self) -> None:
        for version in ("*", "", '""', 'a"b'):
            with self.subTest(version=version), self.assertRaises(ValueError):
                self.store.put("s3://lake/t/v", b"x", if_version=version)
        for bad in (3, b"etag"):
            with self.subTest(bad=bad), self.assertRaises(TypeError):
                self.store.put("s3://lake/t/v", b"x", if_version=bad)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            self.store.put("s3://lake/t/v", "text")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            self.store.delete("s3://lake/t/v", if_version=ABSENT)  # type: ignore[arg-type]
        for uri in ("gs://lake/t/v", "s3://lake/", "s3:/lake/v"):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                self.store.head(uri)

    def test_head_describes_the_object_and_answers_none_when_it_is_missing(self) -> None:
        self.stubber.add_response(
            "head_object",
            {"ContentLength": 1234, "ETag": ETAG, "LastModified": MODIFIED},
            {"Bucket": BUCKET, "Key": "t/head"},
        )
        self.assertEqual(self.store.head("s3://lake/t/head"), ObjectInfo("s3://lake/t/head", 1234, VERSION, MODIFIED))
        # A HEAD's answer has no body: its code is the bare status.
        self.stub_error("head_object", "404", 404, {"Bucket": BUCKET, "Key": "t/missing"}, message="Not Found")
        self.assertIsNone(self.store.head("s3://lake/t/missing"))

    def test_get_to_file_streams_the_body_and_reports_what_was_read(self) -> None:
        data = os.urandom(3 * 1024 * 1024 + 17)
        recorded = RecordingBody(data)
        self.stubber.add_response(
            "get_object",
            {"Body": recorded, "ETag": ETAG, "ContentLength": len(data), "LastModified": MODIFIED},
            {"Bucket": BUCKET, "Key": "t/large"},
        )
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "large")
            info = self.store.get_to_file("s3://lake/t/large", target)
            self.assertEqual(Path(target).read_bytes(), data)
        self.assertEqual(info, ObjectInfo("s3://lake/t/large", len(data), VERSION, MODIFIED))
        # A mebibyte at a time, never the whole body in memory.
        self.assertGreater(len(recorded.reads), 3)
        self.assertLessEqual(max(recorded.reads), 1024 * 1024)

    def test_a_failed_download_leaves_nothing_at_the_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            Path(target).write_bytes(b"an older download")
            self.stub_error("get_object", "NoSuchKey", 404, {"Bucket": BUCKET, "Key": "t/missing"})
            with self.assertRaises(ObjectNotFoundError):
                self.store.get_to_file("s3://lake/t/missing", target)
            self.assertFalse(os.path.exists(target))
            self.stubber.add_response("get_object", {"Body": StreamingBody(BrokenStream(b"partial"), 100), "ETag": ETAG}, {"Bucket": BUCKET, "Key": "t/cut"})
            with self.assertRaises(StoreError) as caught:
                self.store.get_to_file("s3://lake/t/cut", target)
            self.assertIsNone(caught.exception.status)
            self.assertFalse(os.path.exists(target))

    def test_deletes_answer_true_and_conditional_deletes_are_not_offered(self) -> None:
        self.stubber.add_response("delete_object", {}, {"Bucket": BUCKET, "Key": "t/gone"})
        self.assertTrue(self.store.delete("s3://lake/t/gone"))
        # A server that does say the key was missing is believed.
        self.stub_error("delete_object", "NoSuchKey", 404, {"Bucket": BUCKET, "Key": "t/never"})
        self.assertFalse(self.store.delete("s3://lake/t/never"))
        # No request at all: the stubber would refuse an unexpected call.
        with self.assertRaises(NotImplementedError) as caught:
            self.store.delete("s3://lake/t/gone", if_version=VERSION)
        self.assertIn("supports_conditional_delete", str(caught.exception))

    def test_a_listing_pages_through_every_object(self) -> None:
        first = [{"Key": f"t/list/object-{index:04d}", "Size": index, "ETag": ETAG, "LastModified": MODIFIED} for index in range(3)]
        second = [{"Key": "t/list/object-0003", "Size": 3, "ETag": VERSION, "LastModified": MODIFIED}]
        self.stubber.add_response(
            "list_objects_v2",
            {"Contents": first, "IsTruncated": True, "NextContinuationToken": "page-2", "KeyCount": 3},
            {"Bucket": BUCKET, "Prefix": "t/list/"},
        )
        self.stubber.add_response(
            "list_objects_v2",
            {"Contents": second, "IsTruncated": False, "KeyCount": 1},
            {"Bucket": BUCKET, "Prefix": "t/list/", "ContinuationToken": "page-2"},
        )
        listed = list(self.store.list("s3://lake/t/list/"))
        self.assertEqual([item.uri for item in listed], [f"s3://lake/t/list/object-{index:04d}" for index in range(4)])
        self.assertEqual({item.version for item in listed}, {VERSION})  # quoted or not, the same version
        self.assertEqual([item.size for item in listed], [0, 1, 2, 3])
        self.assertTrue(all(item.last_modified == MODIFIED for item in listed))
        self.stubber.add_response("list_objects_v2", {"IsTruncated": False, "KeyCount": 0}, {"Bucket": BUCKET, "Prefix": ""})
        self.assertEqual(list(self.store.list("s3://lake/")), [])


class BrokenStream(io.RawIOBase):
    """A response body that fails after its first bytes: cut off, unless another `error` is given."""

    def __init__(self, first: bytes, error: Exception | None = None) -> None:
        self.first = first
        self.error = error or urllib3.exceptions.ProtocolError("Connection broken: IncompleteRead")

    def read(self, size: int = -1) -> bytes:
        if self.first:
            chunk, self.first = self.first, b""
            return chunk
        raise self.error


class RecordingBody(StreamingBody):
    """A response body that records how much each read asks for, and refuses to be read whole."""

    def __init__(self, data: bytes) -> None:
        super().__init__(io.BytesIO(data), len(data))
        self.reads: list[int] = []

    def read(self, amt: int | None = None) -> bytes:
        if amt is None or amt < 0:
            raise AssertionError("the whole body was read at once")
        self.reads.append(amt)
        return super().read(amt)


def answer(request: object, status: int, headers: dict[str, str], content: bytes) -> AWSResponse:
    """An HTTP answer to `request` as botocore receives one from the network, to parse and check."""
    headers = {"Content-Length": str(len(content)), **headers}
    raw = urllib3.HTTPResponse(body=io.BytesIO(content), headers=headers, status=status, preload_content=False)
    return AWSResponse(getattr(request, "url"), status, headers, raw)


class S3DeleteManyTests(StubbedStoreTestCase):
    def test_keys_go_a_thousand_to_a_quiet_request(self) -> None:
        uris = [f"s3://lake/t/object-{index:05d}" for index in range(1050)]
        keys = [uri.removeprefix("s3://lake/") for uri in uris]
        for chunk in (keys[:1000], keys[1000:]):
            expected = {"Bucket": BUCKET, "Delete": {"Objects": [{"Key": key} for key in chunk], "Quiet": True}}
            self.stubber.add_response("delete_objects", {}, expected)
        outcome = self.store.delete_many(uris)
        self.assertEqual(outcome.deleted, tuple(uris))
        self.assertEqual(outcome.failed, ())
        self.assertEqual(DELETE_BATCH_SIZE, 1000)

    def test_each_error_in_the_answer_fails_only_its_own_key(self) -> None:
        uris = [f"s3://lake/t/{name}" for name in ("ok", "denied", "internal", "slow", "missing", "odd", "throttled")]
        errors = [
            {"Key": "t/denied", "Code": "AccessDenied", "Message": "Access Denied"},
            {"Key": "t/internal", "Code": "InternalError", "Message": "We encountered an internal error."},
            {"Key": "t/slow", "Code": "SlowDown", "Message": "Please reduce your request rate."},
            {"Key": "t/missing", "Code": "NoSuchKey", "Message": "gone"},
            {"Key": "t/odd", "Code": "SomethingNew"},
            {"Key": "t/throttled", "Code": "RequestThrottledLater"},
            {"Key": "t/not-requested", "Code": "AccessDenied"},
        ]
        # moto ignores Quiet and lists what it deleted too: only the errors are read.
        deleted = [{"Key": "t/ok"}]
        expected = {"Bucket": BUCKET, "Delete": {"Objects": [{"Key": uri.removeprefix("s3://lake/")} for uri in uris], "Quiet": True}}
        self.stubber.add_response("delete_objects", {"Deleted": deleted, "Errors": errors}, expected)
        outcome = self.store.delete_many(uris)
        self.assertEqual(outcome.deleted, ("s3://lake/t/ok", "s3://lake/t/missing"))
        failed = {failure.uri: (failure.status, failure.retryable, failure.message) for failure in outcome.failed}
        self.assertEqual(
            failed,
            {
                "s3://lake/t/denied": (403, False, "AccessDenied: Access Denied"),
                "s3://lake/t/internal": (500, True, "InternalError: We encountered an internal error."),
                "s3://lake/t/slow": (503, True, "SlowDown: Please reduce your request rate."),
                "s3://lake/t/odd": (400, False, "SomethingNew"),
                "s3://lake/t/throttled": (503, True, "RequestThrottledLater"),
            },
        )

    def test_a_request_that_fails_fails_every_key_in_it_with_its_status(self) -> None:
        uris = [f"s3://lake/t/object-{index}" for index in range(3)]
        for code, status, retryable in (("AccessDenied", 403, False), ("SlowDown", 503, True), ("NoSuchBucket", 404, False)):
            with self.subTest(code=code):
                self.stub_error("delete_objects", code, status)
                outcome = self.store.delete_many(uris)
                self.assertEqual(outcome.deleted, ())
                self.assertEqual([(failure.uri, failure.status, failure.retryable) for failure in outcome.failed], [(uri, status, retryable) for uri in uris])
                self.assertTrue(all(code in failure.message for failure in outcome.failed))

    def test_keys_are_grouped_by_bucket_and_each_sent_once(self) -> None:
        uris = ["s3://lake/a", "s3://other/b", "s3://lake/c", "s3://lake/a"]
        self.stubber.add_response("delete_objects", {}, {"Bucket": "lake", "Delete": {"Objects": [{"Key": "a"}, {"Key": "c"}], "Quiet": True}})
        self.stubber.add_response(
            "delete_objects",
            {"Errors": [{"Key": "b", "Code": "AccessDenied", "Message": "no"}]},
            {"Bucket": "other", "Delete": {"Objects": [{"Key": "b"}], "Quiet": True}},
        )
        outcome = self.store.delete_many(uris)
        self.assertEqual(outcome.deleted, ("s3://lake/a", "s3://lake/c", "s3://lake/a"))
        self.assertEqual([(failure.uri, failure.status) for failure in outcome.failed], [("s3://other/b", 403)])

    def test_uris_are_checked_before_any_request(self) -> None:
        with self.assertRaises(ValueError):
            self.store.delete_many(["s3://lake/a", "gs://lake/b"])
        self.assertEqual(self.store.delete_many([]).deleted, ())


class S3TransportFailureTests(unittest.TestCase):
    """Failures in transit, raised where botocore sends a request: no answer, so no status."""

    def setUp(self) -> None:
        environment = clean_environment()
        environment.start()
        self.addCleanup(environment.stop)
        self.client = test_client("http://s3.test:9000")
        self.store = S3Store(client=self.client, settings=StoreSettings())
        self.failure: Exception | None = None
        self.answer: object = None
        self.client.meta.events.register("before-send.s3", self.send)

    def send(self, request: object, **_: object) -> AWSResponse:
        """Where botocore sends a request: raise `failure`, or give `answer(request)` back to parse."""
        if self.answer is not None:
            return self.answer(request)  # type: ignore[operator]
        assert self.failure is not None
        raise self.failure

    def test_a_download_whose_checksum_does_not_match_is_a_failure_in_transit(self) -> None:
        # botocore asks for the object's checksum and checks the body against it as it reads: what a
        # download corrupted on the way looks like. Worth another try, and nothing is left behind.
        data = b"the bytes that arrived"
        sent = base64.b64encode(zlib.crc32(b"the bytes that were sent").to_bytes(4, "big")).decode()
        modes: list[object] = []

        def corrupted(request: object) -> AWSResponse:
            modes.append(getattr(request, "headers").get("x-amz-checksum-mode"))
            return answer(request, 200, {"ETag": ETAG, "x-amz-checksum-crc32": sent}, data)

        self.answer = corrupted
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            for name, attempt in (("get", lambda: self.store.get("s3://lake/t/x")), ("get_to_file", lambda: self.store.get_to_file("s3://lake/t/x", target))):
                with self.subTest(operation=name), self.assertRaises(StoreError) as caught:
                    attempt()
                error = caught.exception
                self.assertEqual((type(error), error.status, error.code, error.retryable), (StoreError, None, "FlexibleChecksumError", True))
                self.assertIsInstance(error.__cause__, FlexibleChecksumError)
            self.assertEqual(os.listdir(directory), [])
        self.assertEqual([mode.decode() if isinstance(mode, bytes) else mode for mode in modes], ["ENABLED", "ENABLED"])

    def test_an_answer_that_cannot_be_read_is_worth_another_try(self) -> None:
        # botocore takes a 200 whose XML is cut off for a 500, as S3 sends one when a connection breaks
        # mid-answer: retried, then reported with that status.
        cut_off = b"<?xml version='1.0' encoding='UTF-8'?><ListBucketResult><Contents><Key>t/a</Key>"
        self.answer = lambda request: answer(request, 200, {"Content-Type": "application/xml"}, cut_off)
        with self.assertRaises(StoreError) as caught:
            list(self.store.list("s3://lake/t/"))
        self.assertEqual((type(caught.exception), caught.exception.status, caught.exception.retryable), (StoreError, 500, True))
        # A DeleteObjects answer cut off says nothing of any key: each one failed, worth another try.
        outcome = self.store.delete_many(["s3://lake/a", "s3://lake/b"])
        self.assertEqual(outcome.deleted, ())
        self.assertEqual([(failure.uri, failure.status, failure.retryable) for failure in outcome.failed], [("s3://lake/a", 500, True), ("s3://lake/b", 500, True)])
        # An answer botocore's parser refuses in any other way is a failure in transit.
        self.answer = None
        self.failure = ResponseParserError("Unable to parse response (no element found: line 1, column 75), invalid XML received.")
        for name, attempt in (("get", lambda: self.store.get("s3://lake/t/x")), ("head", lambda: self.store.head("s3://lake/t/x")), ("list", lambda: list(self.store.list("s3://lake/t/")))):
            with self.subTest(operation=name), self.assertRaises(StoreError) as caught:
                attempt()
            error = caught.exception
            self.assertEqual((type(error), error.status, error.code, error.retryable), (StoreError, None, "ResponseParserError", True))
            self.assertIs(error.__cause__, self.failure)

    def test_every_operation_maps_a_failure_in_transit_to_no_status(self) -> None:
        failures = (
            EndpointConnectionError(endpoint_url="http://s3.test:9000"),
            ConnectionClosedError(endpoint_url="http://s3.test:9000"),
            ReadTimeoutError(endpoint_url="http://s3.test:9000"),
        )
        operations = (
            lambda: self.store.get("s3://lake/t/x"),
            lambda: self.store.head("s3://lake/t/x"),
            lambda: self.store.put("s3://lake/t/x", b"x", if_version=ABSENT),
            lambda: self.store.delete("s3://lake/t/x"),
            lambda: list(self.store.list("s3://lake/t/")),
        )
        for failure in failures:
            self.failure = failure
            for operation in operations:
                with self.subTest(failure=type(failure).__name__), self.assertRaises(StoreError) as caught:
                    operation()
                self.assertNotIsInstance(caught.exception, (ObjectNotFoundError, PreconditionFailedError))
                self.assertIsNone(caught.exception.status)
                self.assertTrue(caught.exception.retryable)
                self.assertEqual(caught.exception.code, type(failure).__name__)
                self.assertIs(caught.exception.__cause__, failure)

    def test_a_delete_request_lost_in_transit_fails_every_key_with_no_status(self) -> None:
        self.failure = EndpointConnectionError(endpoint_url="http://s3.test:9000")
        outcome = self.store.delete_many(["s3://lake/a", "s3://lake/b"])
        self.assertEqual(outcome.deleted, ())
        self.assertEqual([(failure.uri, failure.status, failure.retryable) for failure in outcome.failed], [("s3://lake/a", None, True), ("s3://lake/b", None, True)])
        self.assertIs(outcome.failed[0].error, self.failure)

    def test_a_missing_credential_is_raised_as_it_is(self) -> None:
        self.failure = NoCredentialsError()
        with self.assertRaises(NoCredentialsError):
            self.store.get("s3://lake/t/x")


class S3ErrorMappingTests(StubbedStoreTestCase):
    def assertMapped(self, attempt, kind: type[StoreError], status: int | None, code: str | None) -> StoreError:
        with self.assertRaises(kind) as caught:
            attempt()
        error = caught.exception
        self.assertIs(type(error), kind)
        self.assertEqual((error.status, error.code), (status, code))
        self.assertIsNotNone(error.__cause__)
        return error

    def test_lost_races_are_precondition_failures(self) -> None:
        self.stub_error("put_object", "PreconditionFailed", 412)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=ABSENT), PreconditionFailedError, 412, "PreconditionFailed")
        self.stub_error("put_object", "PreconditionFailed", 412)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=VERSION), PreconditionFailedError, 412, "PreconditionFailed")
        # Another conditional write in flight: the same news for the pointer.
        self.stub_error("put_object", "ConditionalRequestConflict", 409)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=VERSION), PreconditionFailedError, 409, "ConditionalRequestConflict")
        self.stub_error("put_object", "ConditionalRequestConflict", 409)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=ABSENT), PreconditionFailedError, 409, "ConditionalRequestConflict")

    def test_a_missing_object_under_if_match_is_a_lost_race(self) -> None:
        self.stub_error("put_object", "NoSuchKey", 404)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=VERSION), PreconditionFailedError, 404, "NoSuchKey")
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "f")
            Path(path).write_bytes(b"x")
            self.stub_error("put_object", "NoSuchKey", 404)
            self.assertMapped(lambda: self.store.put_file("s3://lake/t/p", path, if_version=VERSION), PreconditionFailedError, 404, "NoSuchKey")

    def test_a_missing_object_elsewhere_is_not_found_whatever_its_code(self) -> None:
        self.stub_error("get_object", "NoSuchKey", 404)
        self.assertMapped(lambda: self.store.get("s3://lake/t/x"), ObjectNotFoundError, 404, "NoSuchKey")
        self.stub_error("get_object", "404", 404)
        self.assertMapped(lambda: self.store.get("s3://lake/t/x"), ObjectNotFoundError, 404, "404")
        self.stub_error("get_object", "NotFound", 404)
        error = self.assertMapped(lambda: self.store.get("s3://lake/t/x", start=3), ObjectNotFoundError, 404, "NotFound")
        self.assertFalse(error.retryable)

    def test_404_and_412_are_judged_by_the_status_whatever_the_code(self) -> None:
        self.stub_error("get_object", "", 404)
        self.assertMapped(lambda: self.store.get("s3://lake/t/x"), ObjectNotFoundError, 404, None)
        self.stub_error("get_object", "NoSuchObject", 404)
        self.assertMapped(lambda: self.store.get("s3://lake/t/x", start=2), ObjectNotFoundError, 404, "NoSuchObject")
        self.stub_error("head_object", "NoSuchObject", 404)
        self.assertIsNone(self.store.head("s3://lake/t/x"))
        self.stub_error("head_object", "", 404)
        self.assertIsNone(self.store.head("s3://lake/t/x"))
        with tempfile.TemporaryDirectory() as directory:
            self.stub_error("get_object", "", 404)
            self.assertMapped(lambda: self.store.get_to_file("s3://lake/t/x", os.path.join(directory, "target")), ObjectNotFoundError, 404, None)
            self.assertEqual(os.listdir(directory), [])
        # A 412 is a lost race whatever its code; under If-Match, so is a 404.
        self.stub_error("put_object", "", 412)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=VERSION), PreconditionFailedError, 412, None)
        self.stub_error("put_object", "ConditionNotMet", 412)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=ABSENT), PreconditionFailedError, 412, "ConditionNotMet")
        self.stub_error("put_object", "", 404)
        self.assertMapped(lambda: self.store.put("s3://lake/t/p", b"x", if_version=VERSION), PreconditionFailedError, 404, None)

    def test_a_body_that_fails_its_checksum_is_a_failure_in_transit(self) -> None:
        failures = (
            FlexibleChecksumError(error_msg="Expected checksum q1Wfpg== did not match calculated checksum: 3b8b7Q=="),
            ChecksumError(checksum_type="md5", expected_checksum="q1Wfpg==", actual_checksum="3b8b7Q=="),
        )
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            for failure in failures:
                for name, attempt in (("get", lambda: self.store.get("s3://lake/t/c")), ("get_to_file", lambda: self.store.get_to_file("s3://lake/t/c", target))):
                    with self.subTest(failure=type(failure).__name__, operation=name):
                        self.stubber.add_response("get_object", {"Body": StreamingBody(BrokenStream(b"", failure), 100), "ETag": ETAG}, {"Bucket": BUCKET, "Key": "t/c"})
                        error = self.assertMapped(attempt, StoreError, None, type(failure).__name__)
                        self.assertTrue(error.retryable)
                        self.assertIs(error.__cause__, failure)
            self.assertEqual(os.listdir(directory), [])

    def test_a_missing_bucket_is_configuration_not_a_missing_object(self) -> None:
        cases = (
            ("get_object", lambda: self.store.get("s3://lake/t/x")),
            ("put_object", lambda: self.store.put("s3://lake/t/x", b"x", if_version=VERSION)),
            ("delete_object", lambda: self.store.delete("s3://lake/t/x")),
            ("list_objects_v2", lambda: list(self.store.list("s3://lake/t/"))),
        )
        for method, attempt in cases:
            with self.subTest(method=method):
                self.stub_error(method, "NoSuchBucket", 404)
                error = self.assertMapped(attempt, StoreError, 404, "NoSuchBucket")
                self.assertIn("bucket does not exist", str(error))
                self.assertFalse(error.retryable)

    def test_other_answers_are_store_errors_with_their_status(self) -> None:
        cases = (
            ("AccessDenied", 403, False),
            ("InvalidRange", 416, False),
            ("SlowDown", 503, True),
            ("InternalError", 500, True),
            ("RequestTimeout", 400, False),
        )
        for code, status, retryable in cases:
            with self.subTest(code=code):
                self.stub_error("get_object", code, status)
                error = self.assertMapped(lambda: self.store.get("s3://lake/t/x", start=5000), StoreError, status, code)
                self.assertEqual(error.retryable, retryable)
                self.assertEqual(error.uri, "s3://lake/t/x")

    def test_a_body_cut_short_is_a_failure_in_transit(self) -> None:
        self.stubber.add_response("get_object", {"Body": StreamingBody(BrokenStream(b""), 100), "ETag": ETAG}, {"Bucket": BUCKET, "Key": "t/cut"})
        error = self.assertMapped(lambda: self.store.get("s3://lake/t/cut"), StoreError, None, "ResponseStreamingError")
        self.assertIsInstance(error.__cause__, ResponseStreamingError)
        # Fewer bytes than the answer announced.
        self.stubber.add_response("get_object", {"Body": StreamingBody(io.BytesIO(b"part"), 100), "ETag": ETAG}, {"Bucket": BUCKET, "Key": "t/short"})
        self.assertMapped(lambda: self.store.get("s3://lake/t/short"), StoreError, None, "IncompleteReadError")

    def test_an_error_that_is_no_storage_failure_is_none(self) -> None:
        self.assertIsNone(store_error(ValueError("bad argument"), "s3://lake/x"))
        self.assertIsNone(store_error(NoCredentialsError(), "s3://lake/x"))
        error = StoreError("already mapped")
        self.assertIs(store_error(error, "s3://lake/x"), error)

    def test_delete_error_codes_have_the_statuses_s3_documents(self) -> None:
        self.assertEqual(
            {code: delete_error_status(code) for code in ("AccessDenied", "NoSuchBucket", "InternalError", "SlowDown", "ServiceUnavailable", "RequestTimeout", "InvalidArgument")},
            {"AccessDenied": 403, "NoSuchBucket": 404, "InternalError": 500, "SlowDown": 503, "ServiceUnavailable": 503, "RequestTimeout": 408, "InvalidArgument": 400},
        )
        self.assertEqual((delete_error_status("BrandNew"), delete_error_status("BackendBusy")), (400, 503))


class S3ClientTests(unittest.TestCase):
    def setUp(self) -> None:
        environment = clean_environment()
        environment.start()
        self.addCleanup(environment.stop)

    def test_settings_come_from_the_environment(self) -> None:
        with patch.dict(os.environ, {"STORAGE_S3_ENDPOINT": "http://seaweedfs:8333/", "STORAGE_S3_REGION": "eu-west-1", "STORAGE_S3_PATH_STYLE": "true"}):
            store = S3Store.from_environment()
        self.assertEqual(store.settings, StoreSettings(s3_endpoint="http://seaweedfs:8333", s3_region="eu-west-1", s3_path_style=True))
        client = store.client
        self.assertEqual(client.meta.endpoint_url, "http://seaweedfs:8333")
        self.assertEqual(client.meta.region_name, "eu-west-1")

    def test_checksums_and_signing_stay_at_the_defaults_on_a_custom_endpoint_as_on_aws(self) -> None:
        custom = S3Store(settings=StoreSettings(s3_endpoint="http://localhost:8333", s3_path_style=True))
        aws = S3Store(settings=StoreSettings())
        self.assertEqual(custom.client.meta.endpoint_url, "http://localhost:8333")
        self.assertEqual(aws.client.meta.endpoint_url, "https://s3.amazonaws.com")
        self.assertEqual(custom.client.meta.config.s3, {"addressing_style": "path"})
        self.assertEqual(aws.client.meta.config.s3, {"addressing_style": "auto"})
        for name, store in (("custom endpoint", custom), ("AWS", aws)):
            with self.subTest(store=name):
                config = store.client.meta.config
                self.assertEqual(config.request_checksum_calculation, "when_supported")
                self.assertEqual(config.response_checksum_validation, "when_supported")
                self.assertEqual(config.signature_version, "s3v4")  # botocore's own choice for S3
                self.assertEqual(config.retries, {"mode": "standard", "total_max_attempts": 6})  # five retries
                self.assertEqual((config.max_pool_connections, config.connect_timeout, config.read_timeout), (32, 10, 120))
                self.assertEqual(store.delete_client.meta.config.retries, {"mode": "standard", "total_max_attempts": 1})
                self.assertEqual(store.delete_client.meta.config.request_checksum_calculation, "when_supported")
                self.assertEqual(self.sent_checksums(store), ("crc32", "ENABLED"))
        # The store sets nothing that would change them.
        config = client_config(StoreSettings(s3_endpoint="http://localhost:8333", s3_path_style=True))
        self.assertIsNone(config.request_checksum_calculation)
        self.assertIsNone(config.response_checksum_validation)
        self.assertIsNone(config.signature_version)

    def sent_checksums(self, store: S3Store) -> tuple[str | None, str | None]:
        """The checksum a put asks for and the checksum mode a get sends, as the client prepares them."""
        seen: dict[str, object] = {}

        def capture(params: dict, model: object, **_: object) -> None:
            name = getattr(model, "name")
            if name == "PutObject":
                seen["put"] = (params.get("context", {}).get("checksum", {}).get("request_algorithm") or {}).get("algorithm")
            elif name == "GetObject":
                seen["get"] = params.get("headers", {}).get("x-amz-checksum-mode")

        client = store.client
        # On the stubber's own wildcard and ahead of it: botocore calls wildcard handlers before
        # specific ones, and the stubber's answer ends the call.
        client.meta.events.register_first("before-call.*.*", capture)
        with Stubber(client) as stubber:
            stubber.add_response("put_object", {"ETag": ETAG})
            stubber.add_response("get_object", {"Body": body(b"x"), "ETag": ETAG})
            store.put("s3://lake/t/c", b"x", if_version=ABSENT)
            store.get("s3://lake/t/c")
        return seen.get("put"), seen.get("get")  # type: ignore[return-value]

    def test_the_clients_are_built_once_on_first_use_from_one_session(self) -> None:
        sessions: list[object] = []
        built: list[dict] = []
        lock = threading.Lock()

        class FakeSession:
            def __init__(self) -> None:
                sessions.append(self)

            def client(self, service: str, **kwargs: object) -> object:
                time.sleep(0.01)  # widen the window two threads could both build in
                with lock:
                    built.append({"service": service, **kwargs})
                return object()

        store = S3Store(settings=StoreSettings(s3_endpoint="http://localhost:8333", s3_region="eu-west-1"))
        with patch("boto3.session.Session", FakeSession):
            with ThreadPoolExecutor(max_workers=16) as pool:
                clients = list(pool.map(lambda _: store.client, range(64)))
            delete_clients = [store.delete_client for _ in range(4)]
        self.assertEqual(len({id(client) for client in clients}), 1)
        self.assertEqual(len({id(client) for client in delete_clients}), 1)
        self.assertIsNot(clients[0], delete_clients[0])
        self.assertEqual(len(sessions), 1)
        self.assertEqual([(entry["service"], entry["region_name"], entry["endpoint_url"]) for entry in built], [("s3", "eu-west-1", "http://localhost:8333")] * 2)
        self.assertEqual([entry["config"].retries for entry in built], [{"mode": "standard", "max_attempts": 5}, {"mode": "standard", "total_max_attempts": 1}])

    def test_a_given_client_serves_every_operation(self) -> None:
        client = test_client()
        store = S3Store(client=client, settings=StoreSettings())
        self.assertIs(store.client, client)
        self.assertIs(store.delete_client, client)


if __name__ == "__main__":
    unittest.main()
