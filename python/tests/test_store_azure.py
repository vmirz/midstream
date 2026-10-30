"""`AzureBlobStore` (`lakehouse/store/azure.py`): Azure Blob Storage behind the object store contract.

Over doubles of azure-storage-blob's clients, every call carries exactly these arguments: uploads always
`overwrite=True` with an explicit match condition (`IfMissing` for create-only, the quoted ETag with
`IfNotModified` for a compare-and-swap, none otherwise), reads `offset`/`length` for a range, batch
deletes 256 names at a time with `raise_on_any_failure=False` and no retries. Errors are classified by
the HTTP status and `x-ms-error-code` alone - never by the exception's class - and a missing container,
a refused credential or an unknown API version never reads as a missing blob (design §6.2).

Through the real library over a transport double (no network), the conditions arrive as the headers
Azure reads (`If-None-Match: *`, `If-Match` quoted, `x-ms-range`), the ETag leaves without its quotes,
deletes are sent once while other requests are retried, each delete in a batch names its blob by the
whole path (the account first behind an emulator's path-style endpoint, whatever its host is called),
and a body cut short is a failure in transit once the library stops reading it again. Real clients
show which credential each environment selects, the shared transport and the retry and size settings.
"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import io
import logging
import os
from pathlib import Path
import re
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from azure.core import MatchConditions
from azure.core.credentials import AccessToken, AzureNamedKeyCredential
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    IncompleteReadError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
    ServiceRequestError,
    ServiceResponseError,
)
from azure.core.pipeline.transport import RequestsTransport
from azure.identity import CredentialUnavailableError
from azure.storage.blob import BlobServiceClient, ExponentialRetry
import requests
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
from midstream.lakehouse.store import azure as azure_store
from midstream.lakehouse.store.azure import BATCH_SIZE, AzureBlobStore, AzureConfigurationError, store_error

ACCOUNT = "acct"
ROOT = f"abfss://box@{ACCOUNT}.dfs.core.windows.net"
ETAG = '"0x8DCE1F5A3B2C4D0"'
VERSION = "0x8DCE1F5A3B2C4D0"
NEWER = '"0x8DCE1F5A3B2C4D9"'
MODIFIED = datetime(2026, 10, 30, 12, 0, 1, tzinfo=UTC)
#: Microsoft's published development key (Azurite's), never a secret.
DEV_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CREDENTIAL_VARIABLES = ("AZURE_STORAGE_CONNECTION_STRING", "AZURE_STORAGE_ACCOUNT_KEY", "STORAGE_AZURE_ENDPOINT")


class FakeResponse:
    """What azure-core's errors read from an answer."""

    def __init__(self, status: int, code: str | None = None, reason: str = "") -> None:
        self.status_code = status
        self.reason = reason or {202: "Accepted", 403: "Forbidden", 404: "Not Found", 500: "Internal Server Error"}.get(status, "Error")
        self.headers = {"x-ms-error-code": code} if code else {}

    def text(self) -> str:
        return ""


def http_error(status: int, code: str | None, kind: type[HttpResponseError] = HttpResponseError, error_code: str | None = None) -> HttpResponseError:
    """An error as the library raises it; `kind` and `error_code` may say something else entirely."""
    error = kind(message=f"Operation returned an invalid status '{status}'\nRequestId:1\nErrorCode:{code}", response=FakeResponse(status, code))
    error.error_code = error_code or code
    return error


class Calls:
    """Every call the doubles receive, and what each method answers next: a value, an exception to
    raise, or a function of the call's arguments."""

    def __init__(self) -> None:
        self.log: list[tuple[str, str, tuple, dict]] = []
        self.script: dict[str, list[object]] = defaultdict(list)

    def answer(self, method: str, target: str, args: tuple, kwargs: dict, default: object) -> object:
        self.log.append((method, target, args, kwargs))
        queue = self.script.get(method)
        outcome = queue.pop(0) if queue else default
        if isinstance(outcome, BaseException):
            raise outcome
        if callable(outcome):
            return outcome(*args, **kwargs)
        return outcome

    def of(self, method: str) -> list[tuple[str, tuple, dict]]:
        return [(target, args, kwargs) for name, target, args, kwargs in self.log if name == method]


class FakeDownloader:
    def __init__(self, data: bytes, etag: str = ETAG, content_range: str | None = None) -> None:
        self.data = data
        self.properties = SimpleNamespace(etag=etag, last_modified=MODIFIED, size=len(data), content_range=content_range)

    def readall(self) -> bytes:
        return self.data

    def readinto(self, stream: io.IOBase) -> int:
        stream.write(self.data)
        return len(self.data)


class FakeBlobClient:
    def __init__(self, calls: Calls, container: str, name: str) -> None:
        self.calls, self.target = calls, f"{container}/{name}"

    def download_blob(self, **kwargs: object) -> object:
        return self.calls.answer("download_blob", self.target, (), kwargs, FakeDownloader(b"data"))

    def get_blob_properties(self, **kwargs: object) -> object:
        default = SimpleNamespace(name=self.target, size=4, etag=ETAG, last_modified=MODIFIED, metadata={})
        return self.calls.answer("get_blob_properties", self.target, (), kwargs, default)

    def upload_blob(self, data: object, **kwargs: object) -> object:
        content = data if isinstance(data, bytes) else getattr(data, "name", data)
        return self.calls.answer("upload_blob", self.target, (content,), kwargs, {"etag": NEWER, "last_modified": MODIFIED})

    def delete_blob(self, **kwargs: object) -> object:
        return self.calls.answer("delete_blob", self.target, (), kwargs, None)


class FakeContainerClient:
    def __init__(self, calls: Calls, container: str) -> None:
        self.calls, self.container = calls, container

    def delete_blobs(self, *names: str, **kwargs: object) -> object:
        default = [FakeResponse(202) for _ in names]
        return iter(self.calls.answer("delete_blobs", self.container, names, kwargs, default))

    def delete_blob(self, name: str, **kwargs: object) -> object:
        return self.calls.answer("delete_blob", f"{self.container}/{name}", (), kwargs, None)

    def list_blobs(self, **kwargs: object) -> object:
        return iter(self.calls.answer("list_blobs", self.container, (), kwargs, []))


class FakeServiceClient:
    def __init__(self, calls: Calls, account: str = ACCOUNT) -> None:
        self.calls, self.account_name = calls, account

    def get_blob_client(self, container: str, blob: str) -> FakeBlobClient:
        return FakeBlobClient(self.calls, container, blob)

    def get_container_client(self, container: str) -> FakeContainerClient:
        return FakeContainerClient(self.calls, container)


def blob_properties(name: str, size: int, etag: str, metadata: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(name=name, size=size, etag=etag, last_modified=MODIFIED, metadata=metadata or {})


class FakeStoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.calls = Calls()
        self.store = AzureBlobStore(settings=StoreSettings(), clients={ACCOUNT: FakeServiceClient(self.calls)})

    def uri(self, key: str) -> str:
        return f"{ROOT}/{key}"


class AzureRequestTests(FakeStoreTestCase):
    def test_the_store_is_an_object_store_of_the_abfss_scheme(self) -> None:
        self.assertIsInstance(self.store, ObjectStore)
        self.assertEqual((self.store.scheme, self.store.delete_batch_size, BATCH_SIZE), ("abfss", 256, 256))
        self.assertTrue(self.store.supports_conditional_delete)

    def test_writes_always_overwrite_with_an_explicit_match_condition(self) -> None:
        cases = (
            (None, {}),
            (ABSENT, {"match_condition": MatchConditions.IfMissing}),
            (VERSION, {"etag": ETAG, "match_condition": MatchConditions.IfNotModified}),
            (ETAG, {"etag": ETAG, "match_condition": MatchConditions.IfNotModified}),  # quoted once
        )
        for condition, expected in cases:
            with self.subTest(condition=condition):
                self.calls.log.clear()
                version = self.store.put(self.uri("t/pointer"), b"payload", if_version=condition, content_type="application/json")
                self.assertEqual(version, NEWER.strip('"'))
                [(target, args, kwargs)] = self.calls.of("upload_blob")
                self.assertEqual((target, args), ("box/t/pointer", (b"payload",)))
                settings = kwargs.pop("content_settings")
                self.assertEqual(settings.content_type, "application/json")
                self.assertEqual(kwargs, {"length": 7, "overwrite": True, **expected})

    def test_put_file_streams_the_open_file_with_four_transfers_in_flight(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "data.parquet")
            Path(path).write_bytes(b"x" * 4096)
            for condition, expected in ((ABSENT, {"match_condition": MatchConditions.IfMissing}), (None, {})):
                self.calls.log.clear()
                self.assertEqual(self.store.put_file(self.uri("t/data.parquet"), path, if_version=condition), NEWER.strip('"'))
                [(_, args, kwargs)] = self.calls.of("upload_blob")
                self.assertEqual(args, (path,))  # the open file itself, not its bytes
                kwargs.pop("content_settings")
                self.assertEqual(kwargs, {"length": 4096, "overwrite": True, "max_concurrency": 4, **expected})
            with self.assertRaises(FileNotFoundError):
                self.store.put_file(self.uri("t/data.parquet"), os.path.join(directory, "missing"))

    def test_a_conditional_upload_in_blocks_is_refused_before_any_block_when_the_condition_fails(self) -> None:
        large = b"x" * (8 * 1024 * 1024 + 1)
        with self.assertRaises(PreconditionFailedError) as caught:
            self.store.put(self.uri("t/big"), large, if_version=ABSENT)  # the blob exists
        self.assertEqual((caught.exception.status, caught.exception.code), (409, "BlobAlreadyExists"))
        self.calls.script["get_blob_properties"].append(SimpleNamespace(etag=NEWER, size=1, last_modified=MODIFIED))
        with self.assertRaises(PreconditionFailedError) as caught:
            self.store.put(self.uri("t/big"), large, if_version=VERSION)  # it moved on
        self.assertEqual((caught.exception.status, caught.exception.code), (412, "ConditionNotMet"))
        self.calls.script["get_blob_properties"].append(http_error(404, "BlobNotFound", ResourceNotFoundError))
        with self.assertRaises(PreconditionFailedError):
            self.store.put(self.uri("t/big"), large, if_version=VERSION)  # it is gone
        self.assertEqual(self.calls.of("upload_blob"), [])
        # When the condition holds, the upload goes ahead and its commit carries the condition.
        self.calls.script["get_blob_properties"].append(http_error(404, "BlobNotFound"))
        self.store.put(self.uri("t/big"), large, if_version=ABSENT)
        self.store.put(self.uri("t/big"), large, if_version=VERSION)
        conditions = [kwargs["match_condition"] for _, _, kwargs in self.calls.of("upload_blob")]
        self.assertEqual(conditions, [MatchConditions.IfMissing, MatchConditions.IfNotModified])
        # A small upload is one request whose condition Azure checks: nothing is asked first.
        self.calls.log.clear()
        self.store.put(self.uri("t/small"), b"x", if_version=ABSENT)
        self.assertEqual([name for name, *_ in self.calls.log], ["upload_blob"])

    def test_a_large_file_is_checked_before_its_blocks_too(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "big")
            with open(path, "wb") as handle:
                handle.truncate(8 * 1024 * 1024 + 1)
            with self.assertRaises(PreconditionFailedError):
                self.store.put_file(self.uri("t/big"), path, if_version=ABSENT)
        self.assertEqual(self.calls.of("upload_blob"), [])

    def test_reads_ask_for_offset_and_length_and_return_the_unquoted_etag(self) -> None:
        cases = (
            ((None, None), {}),
            ((10, 19), {"offset": 10, "length": 10}),
            ((1000, None), {"offset": 1000}),
            ((None, 9), {"offset": 0, "length": 10}),
            ((0, 0), {"offset": 0, "length": 1}),
        )
        for (start, end), expected in cases:
            with self.subTest(start=start, end=end):
                self.calls.log.clear()
                self.assertEqual(self.store.get(self.uri("t/r"), start=start, end=end), (b"data", VERSION))
                self.assertEqual(self.calls.of("download_blob"), [("box/t/r", (), expected)])
        for start, end in ((-1, None), (5, 4)):
            with self.assertRaises(ValueError):
                self.store.get(self.uri("t/r"), start=start, end=end)

    def test_a_range_that_comes_back_empty_starts_past_the_end(self) -> None:
        # Every range asks for one byte at least, so none back means it starts at or past the size:
        # Azurite answers a start at exactly the size with 206 and no bytes, where S3 answers 416.
        for start, end, content_range, size in ((10, None, "bytes 10-8388617/10", " (10 bytes)"), (10, 12, "bytes 10-12/10", " (10 bytes)"), (None, 0, None, "")):
            with self.subTest(start=start, end=end):
                self.calls.script["download_blob"].append(FakeDownloader(b"", content_range=content_range))
                with self.assertRaises(StoreError) as caught:
                    self.store.get(self.uri("t/ten"), start=start, end=end)
                error = caught.exception
                self.assertEqual((type(error), error.status, error.code, error.retryable), (StoreError, 416, "InvalidRange", False))
                self.assertTrue(str(error).endswith(f"starts past the end of the blob{size}"), str(error))
        # The whole of an empty blob is empty bytes.
        self.calls.script["download_blob"].append(FakeDownloader(b""))
        self.assertEqual(self.store.get(self.uri("t/empty")), (b"", VERSION))

    def test_a_blob_that_changes_while_it_is_read_is_read_again(self) -> None:
        changed = http_error(412, "ConditionNotMet", ResourceModifiedError)
        self.calls.script["download_blob"] = [changed, changed, FakeDownloader(b"settled", NEWER)]
        self.assertEqual(self.store.get(self.uri("t/moving")), (b"settled", NEWER.strip('"')))
        self.assertEqual(len(self.calls.of("download_blob")), 3)
        self.calls.script["download_blob"] = [changed, changed, changed]
        with self.assertRaises(StoreError) as caught:
            self.store.get(self.uri("t/moving"))
        # No condition of the caller's failed: not a lost race.
        self.assertNotIsInstance(caught.exception, PreconditionFailedError)
        self.assertEqual((caught.exception.status, caught.exception.code), (412, "ConditionNotMet"))
        self.assertIn("changed while it was read", str(caught.exception))

    def test_head_describes_the_blob_and_answers_none_when_it_is_missing(self) -> None:
        self.calls.script["get_blob_properties"].append(blob_properties("t/h", 1234, ETAG))
        self.assertEqual(self.store.head(self.uri("t/h")), ObjectInfo(self.uri("t/h"), 1234, VERSION, MODIFIED))
        self.calls.script["get_blob_properties"].append(http_error(404, "BlobNotFound", ResourceNotFoundError))
        self.assertIsNone(self.store.head(self.uri("t/missing")))
        # A missing container is no missing blob.
        self.calls.script["get_blob_properties"].append(http_error(404, "ContainerNotFound", ResourceNotFoundError))
        with self.assertRaises(AzureConfigurationError) as caught:
            self.store.head(self.uri("t/missing"))
        self.assertEqual((caught.exception.status, caught.exception.code), (404, "ContainerNotFound"))
        self.assertIn("container does not exist", str(caught.exception))

    def test_get_to_file_downloads_beside_the_target_and_renames(self) -> None:
        seen: list[str] = []

        def download(**kwargs: object) -> FakeDownloader:
            seen.append(str(kwargs))
            return FakeDownloader(b"content")

        self.calls.script["download_blob"].append(download)
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            info = self.store.get_to_file(self.uri("t/f"), target)
            self.assertEqual(Path(target).read_bytes(), b"content")
            self.assertEqual(os.listdir(directory), ["target"])
        self.assertEqual(info, ObjectInfo(self.uri("t/f"), 7, VERSION, MODIFIED))
        self.assertEqual(seen, [str({"max_concurrency": 4})])

    def test_a_failed_download_leaves_nothing_behind(self) -> None:
        class Broken(FakeDownloader):
            def readinto(self, stream: io.IOBase) -> int:
                stream.write(b"partial")
                raise ServiceResponseError("connection reset")

        for failure in (http_error(404, "BlobNotFound"), Broken(b"")):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as directory:
                target = os.path.join(directory, "target")
                Path(target).write_bytes(b"an older download")
                self.calls.script["download_blob"].append(failure)
                with self.assertRaises(StoreError):
                    self.store.get_to_file(self.uri("t/f"), target)
                self.assertEqual(os.listdir(directory), [])
        changed = http_error(412, "ConditionNotMet")
        self.calls.script["download_blob"] = [changed, FakeDownloader(b"second")]
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            self.assertEqual(self.store.get_to_file(self.uri("t/f"), target).size, 6)
            self.assertEqual(Path(target).read_bytes(), b"second")

    def test_deletes_say_whether_the_blob_was_there(self) -> None:
        self.assertTrue(self.store.delete(self.uri("t/d")))
        self.calls.script["delete_blob"].append(http_error(404, "BlobNotFound", ResourceNotFoundError))
        self.assertFalse(self.store.delete(self.uri("t/d")))
        self.assertTrue(self.store.delete(self.uri("t/d"), if_version=VERSION))
        self.assertEqual(
            [kwargs for _, _, kwargs in self.calls.of("delete_blob")],
            [{}, {}, {"etag": ETAG, "match_condition": MatchConditions.IfNotModified}],
        )
        self.calls.script["delete_blob"].append(http_error(404, "ContainerNotFound"))
        with self.assertRaises(AzureConfigurationError):
            self.store.delete(self.uri("t/d"))

    def test_a_refused_conditional_delete_asks_whether_the_blob_is_there(self) -> None:
        stale = http_error(412, "ConditionNotMet", ResourceModifiedError)
        # Azure answers a missing blob under If-Match with 412 as well: then it was already gone.
        self.calls.script["delete_blob"].append(stale)
        self.calls.script["get_blob_properties"].append(http_error(404, "BlobNotFound"))
        self.assertFalse(self.store.delete(self.uri("t/d"), if_version=VERSION))
        self.calls.script["delete_blob"].append(stale)
        with self.assertRaises(PreconditionFailedError) as caught:
            self.store.delete(self.uri("t/d"), if_version=VERSION)
        self.assertEqual((caught.exception.status, caught.exception.code), (412, "ConditionNotMet"))
        self.calls.script["delete_blob"].append(http_error(404, "BlobNotFound"))
        self.assertFalse(self.store.delete(self.uri("t/d"), if_version=VERSION))

    def test_a_version_that_is_no_etag_is_refused_before_any_request(self) -> None:
        for version in ("*", "", '""', 'a"b'):
            with self.subTest(version=version), self.assertRaises(ValueError):
                self.store.put(self.uri("t/v"), b"x", if_version=version)
        with self.assertRaises(TypeError):
            self.store.put(self.uri("t/v"), b"x", if_version=7)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            self.store.delete(self.uri("t/v"), if_version=ABSENT)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            self.store.put(self.uri("t/v"), "text")  # type: ignore[arg-type]
        for uri in ("gs://box/t/v", f"{ROOT}/", "abfss://box@acct.blob.core.windows.net/t/v", "abfss://box/t/v"):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                self.store.head(uri)
        self.assertEqual(self.calls.log, [])

    def test_a_listing_names_every_blob_once_and_leaves_out_directories(self) -> None:
        self.calls.script["list_blobs"].append(
            [
                blob_properties("t/list/a", 1, VERSION),  # listings leave the quotes out
                blob_properties("t/list/dir", 0, '"0x1"', {"hdi_isfolder": "true"}),
                blob_properties("t/list/b", 2, ETAG),
            ]
        )
        listed = list(self.store.list(self.uri("t/list/")))
        self.assertEqual(listed, [ObjectInfo(self.uri("t/list/a"), 1, VERSION, MODIFIED), ObjectInfo(self.uri("t/list/b"), 2, VERSION, MODIFIED)])
        self.assertEqual(self.calls.of("list_blobs"), [("box", (), {"name_starts_with": "t/list/", "include": ["metadata"]})])
        self.calls.log.clear()
        self.assertEqual(list(self.store.list(f"{ROOT}/")), [])
        self.assertEqual(self.calls.of("list_blobs"), [("box", (), {"name_starts_with": None, "include": ["metadata"]})])
        self.calls.script["list_blobs"].append(http_error(404, "ContainerNotFound"))
        with self.assertRaises(AzureConfigurationError):
            list(self.store.list(f"{ROOT}/t/"))


class AzureDeleteManyTests(FakeStoreTestCase):
    def test_blobs_go_256_to_a_batch_without_retries(self) -> None:
        uris = [self.uri(f"t/object-{index:04d}") for index in range(300)]
        outcome = self.store.delete_many(uris)
        self.assertEqual(outcome.deleted, tuple(uris))
        batches = self.calls.of("delete_blobs")
        self.assertEqual([len(names) for _, names, _ in batches], [256, 44])
        self.assertEqual({str(kwargs) for _, _, kwargs in batches}, {str({"raise_on_any_failure": False, "retry_total": 0})})
        self.assertEqual(list(batches[0][1][:2]), ["t/object-0000", "t/object-0001"])

    def test_each_answer_in_a_batch_is_classified(self) -> None:
        names = ["gone", "missing", "missing-no-code", "denied", "busy", "container"]
        answers = [FakeResponse(202), FakeResponse(404, "BlobNotFound"), FakeResponse(404), FakeResponse(403, "AuthorizationFailure"), FakeResponse(500, "InternalError"), FakeResponse(404, "ContainerNotFound")]
        self.calls.script["delete_blobs"].append(answers)
        outcome = self.store.delete_many([self.uri(f"t/{name}") for name in names])
        self.assertEqual(outcome.deleted, tuple(self.uri(f"t/{name}") for name in ("gone", "missing", "missing-no-code")))
        self.assertEqual(
            [(failure.uri, failure.status, failure.retryable, failure.message) for failure in outcome.failed],
            [
                (self.uri("t/denied"), 403, False, "403 Forbidden (AuthorizationFailure)"),
                (self.uri("t/busy"), 500, True, "500 Internal Server Error (InternalError)"),
                (self.uri("t/container"), 404, False, "404 Not Found (ContainerNotFound)"),
            ],
        )
        self.assertFalse(any(failure.whole_request for failure in outcome.failed))

    def test_a_batch_that_answers_for_too_few_blobs_fails_them_all(self) -> None:
        self.calls.script["delete_blobs"].append([FakeResponse(202)])
        outcome = self.store.delete_many([self.uri("t/a"), self.uri("t/b")])
        self.assertEqual(outcome.deleted, ())
        self.assertEqual([(failure.status, failure.retryable) for failure in outcome.failed], [(None, True), (None, True)])
        self.assertTrue(all(failure.whole_request for failure in outcome.failed))
        # How an emulator refuses a batch whose deletes name their blobs wrongly: one answer for all.
        self.calls.script["delete_blobs"].append([FakeResponse(400, "InvalidInput")])
        outcome = self.store.delete_many([self.uri("t/a"), self.uri("t/b")])
        self.assertEqual({failure.message for failure in outcome.failed}, {"the blob batch answered 1 of the 2 deletes it carried (the first: 400 InvalidInput)"})

    def test_a_batch_request_that_fails_fails_every_delete_in_it(self) -> None:
        uris = [self.uri("t/a"), self.uri("t/b")]
        for failure, status in (
            (http_error(403, "AuthorizationPermissionMismatch"), 403),
            (http_error(503, "ServerBusy"), 503),
            (ServiceRequestError("connection refused"), None),
        ):
            with self.subTest(failure=failure):
                self.calls.log.clear()
                self.calls.script["delete_blobs"].append(failure)
                outcome = self.store.delete_many(uris)
                self.assertEqual(outcome.deleted, ())
                self.assertEqual([(item.uri, item.status) for item in outcome.failed], [(uri, status) for uri in uris])
                self.assertTrue(all(item.whole_request for item in outcome.failed))
                self.assertIs(outcome.failed[0].error, failure)
                # A real failure never turns into a request per blob.
                self.assertEqual(self.calls.of("delete_blob"), [])

    def test_a_refused_batch_sends_the_account_one_request_per_blob_from_then_on(self) -> None:
        refusal = http_error(409, "FeatureNotYetSupportedForHierarchicalNamespaceAccounts")
        self.calls.script["delete_blobs"].append(refusal)
        self.calls.script["delete_blob"] = [None, http_error(404, "BlobNotFound"), http_error(403, "AuthorizationFailure"), ServiceResponseError("reset")]
        uris = [self.uri(f"t/{name}") for name in ("a", "b", "c", "d")]
        outcome = self.store.delete_many(uris)
        self.assertEqual(outcome.deleted, tuple(uris[:2]))
        self.assertEqual([(failure.uri, failure.status) for failure in outcome.failed], [(uris[2], 403), (uris[3], None)])
        self.assertEqual([(target, kwargs) for target, _, kwargs in self.calls.of("delete_blob")], [(f"box/t/{name}", {"retry_total": 0}) for name in "abcd"])
        self.calls.log.clear()
        self.store.delete_many([self.uri("t/e")])
        self.assertEqual(self.calls.of("delete_blobs"), [])
        self.assertEqual(len(self.calls.of("delete_blob")), 1)

    def test_every_way_a_service_refuses_batches_falls_back(self) -> None:
        for refusal in (http_error(501, "NotImplemented"), http_error(405, None), http_error(400, "InvalidQueryParameterValue"), http_error(400, "UnsupportedQueryParameter")):
            with self.subTest(refusal=refusal.status_code):
                store = AzureBlobStore(settings=StoreSettings(), clients={ACCOUNT: FakeServiceClient(self.calls)})
                self.calls.log.clear()
                self.calls.script["delete_blobs"].append(refusal)
                self.assertEqual(store.delete_many([self.uri("t/a")]).deleted, (self.uri("t/a"),))
                self.assertEqual(len(self.calls.of("delete_blob")), 1)

    def test_blobs_are_grouped_by_account_and_container_and_each_sent_once(self) -> None:
        other = Calls()
        store = AzureBlobStore(settings=StoreSettings(), clients={ACCOUNT: FakeServiceClient(self.calls), "other": FakeServiceClient(other, "other")})
        uris = [self.uri("t/a"), f"abfss://logs@{ACCOUNT}.dfs.core.windows.net/x", "abfss://box@other.dfs.core.windows.net/y", self.uri("t/a")]
        outcome = store.delete_many(uris)
        self.assertEqual(outcome.deleted, tuple(uris))
        self.assertEqual([(container, names) for container, names, _ in self.calls.of("delete_blobs")], [("box", ("t/a",)), ("logs", ("x",))])
        self.assertEqual([(container, names) for container, names, _ in other.of("delete_blobs")], [("box", ("y",))])

    def test_uris_are_checked_before_any_request(self) -> None:
        with self.assertRaises(ValueError):
            self.store.delete_many([self.uri("t/a"), "s3://box/t/b"])
        self.assertEqual(self.store.delete_many([]).deleted, ())
        self.assertEqual(self.calls.log, [])


class AzureErrorMappingTests(FakeStoreTestCase):
    def assertMapped(self, attempt, kind: type[StoreError], status: int | None, code: str | None) -> StoreError:
        with self.assertRaises(kind) as caught:
            attempt()
        error = caught.exception
        self.assertIs(type(error), kind)
        self.assertEqual((error.status, error.code), (status, code))
        self.assertIsNotNone(error.__cause__)
        return error

    def test_answers_are_classified_by_status_and_code_never_by_class(self) -> None:
        cases = (
            # The library's class says one thing, the answer another: the answer wins.
            (http_error(412, "ConditionNotMet", ResourceModifiedError), PreconditionFailedError),
            (http_error(409, "BlobAlreadyExists", ResourceExistsError), PreconditionFailedError),
            (http_error(409, "BlobAlreadyExists", HttpResponseError), PreconditionFailedError),
            (http_error(404, "BlobNotFound", HttpResponseError), ObjectNotFoundError),
            (http_error(404, "ContainerNotFound", ResourceNotFoundError, error_code="BlobNotFound"), AzureConfigurationError),
            (http_error(404, "ResourceNotFound", ResourceNotFoundError), AzureConfigurationError),
            (http_error(404, None, ResourceNotFoundError), StoreError),
            (http_error(403, "AuthorizationFailure", HttpResponseError), AzureConfigurationError),
            (http_error(403, "AuthenticationFailed", ClientAuthenticationError), AzureConfigurationError),
            (http_error(400, "InvalidHeaderValue", HttpResponseError), AzureConfigurationError),
            (http_error(409, "LeaseIdMissing", ResourceExistsError), StoreError),
            (http_error(412, "LeaseIdMismatchWithBlobOperation", ResourceModifiedError), StoreError),
            (http_error(416, "InvalidRange", HttpResponseError), StoreError),
            (http_error(503, "ServerBusy", HttpResponseError), StoreError),
        )
        for exc, kind in cases:
            with self.subTest(status=exc.status_code, code=exc.response.headers.get("x-ms-error-code"), library=type(exc).__name__):
                self.calls.script["upload_blob"].append(exc)
                error = self.assertMapped(lambda: self.store.put(self.uri("t/x"), b"x"), kind, exc.status_code, exc.response.headers.get("x-ms-error-code"))
                self.assertEqual(error.retryable, exc.status_code == 503)
                self.assertEqual(error.uri, self.uri("t/x"))

    def test_configuration_errors_say_what_to_fix(self) -> None:
        cases = (
            ("ContainerNotFound", 404, "container does not exist"),
            ("ResourceNotFound", 404, "STORAGE_AZURE_ENDPOINT"),
            ("AuthorizationFailure", 403, "role assignments"),
            ("AuthenticationFailed", 403, "refused the credentials"),
            ("AuthorizationPermissionMismatch", 403, "data role"),
            ("InvalidAuthenticationInfo", 401, "token"),
            ("NoAuthenticationInformation", 401, "no credentials"),
            ("InvalidHeaderValue", 400, "--skipApiVersionCheck"),
        )
        # Every configuration answer the store knows, each with its own hint.
        self.assertEqual({(status, code) for code, status, _ in cases}, set(azure_store._CONFIGURATION_ANSWERS))
        for code, status, hint in cases:
            with self.subTest(code=code):
                self.calls.script["upload_blob"].append(http_error(status, code))
                error = self.assertMapped(lambda: self.store.put(self.uri("t/x"), b"x", if_version=VERSION), AzureConfigurationError, status, code)
                self.assertIn(hint, str(error))
                self.assertFalse(error.retryable)

    def test_a_missing_blob_under_a_compare_and_swap_is_a_lost_race(self) -> None:
        self.calls.script["upload_blob"].append(http_error(404, "BlobNotFound", ResourceNotFoundError))
        self.assertMapped(lambda: self.store.put(self.uri("t/p"), b"x", if_version=VERSION), PreconditionFailedError, 404, "BlobNotFound")
        self.calls.script["upload_blob"].append(http_error(404, "BlobNotFound"))
        self.assertMapped(lambda: self.store.put(self.uri("t/p"), b"x"), ObjectNotFoundError, 404, "BlobNotFound")
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "data")
            Path(path).write_bytes(b"x")
            self.calls.script["upload_blob"].append(http_error(404, "BlobNotFound", ResourceNotFoundError))
            self.assertMapped(lambda: self.store.put_file(self.uri("t/p"), path, if_version=VERSION), PreconditionFailedError, 404, "BlobNotFound")
            self.calls.script["upload_blob"].append(http_error(404, "BlobNotFound"))
            self.assertMapped(lambda: self.store.put_file(self.uri("t/p"), path), ObjectNotFoundError, 404, "BlobNotFound")

    def test_a_missing_blob_is_not_found_when_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            for attempt in (
                lambda: self.store.get(self.uri("t/missing")),
                lambda: self.store.get(self.uri("t/missing"), start=5),
                lambda: self.store.get_to_file(self.uri("t/missing"), target),
            ):
                self.calls.script["download_blob"].append(http_error(404, "BlobNotFound", ResourceNotFoundError))
                error = self.assertMapped(attempt, ObjectNotFoundError, 404, "BlobNotFound")
                self.assertFalse(error.retryable)
            self.assertEqual(os.listdir(directory), [])
        # An answer, so never read again.
        self.assertEqual(len(self.calls.of("download_blob")), 3)

    def test_a_download_the_library_stopped_reading_is_a_failure_in_transit(self) -> None:
        # After three reads of a body cut short or reset, the library raises an HttpResponseError that
        # carries no answer, around the failure: no status, so worth another try, named after it.
        failures = (
            (IncompleteReadError(message="('Connection broken: IncompleteRead(10 bytes read, 990 more expected)')"), "IncompleteReadError"),
            (ServiceResponseError("Read timed out."), "ServiceResponseError"),
            (None, "HttpResponseError"),
        )

        def cut_short(failure: HttpResponseError) -> FakeDownloader:
            class CutShort(FakeDownloader):
                def readinto(self, stream: io.IOBase) -> int:
                    stream.write(b"partial")
                    raise failure

            return CutShort(b"")

        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            for inner, code in failures:
                failure = HttpResponseError("Download stream interrupted.") if inner is None else HttpResponseError(inner, error=inner)
                self.assertEqual((failure.status_code, failure.response), (None, None))
                for name, answer, attempt in (
                    ("get", failure, lambda: self.store.get(self.uri("t/cut"))),
                    ("get_to_file", cut_short(failure), lambda: self.store.get_to_file(self.uri("t/cut"), target)),
                ):
                    with self.subTest(failure=code, operation=name):
                        self.calls.script["download_blob"].append(answer)
                        error = self.assertMapped(attempt, StoreError, None, code)
                        self.assertTrue(error.retryable)
                        self.assertIs(error.__cause__, failure)
            self.assertEqual(os.listdir(directory), [])

    def test_failures_in_transit_have_no_status(self) -> None:
        for exc in (
            ServiceRequestError("connection refused"),
            ServiceResponseError("connection reset"),
            IncompleteReadError(message="cut short", response=FakeResponse(200)),
        ):
            with self.subTest(failure=type(exc).__name__):
                self.calls.script["get_blob_properties"].append(exc)
                error = self.assertMapped(lambda: self.store.head(self.uri("t/x")), StoreError, None, type(exc).__name__)
                self.assertTrue(error.retryable)
                self.assertIs(error.__cause__, exc)

    def test_an_error_that_is_no_storage_failure_is_raised_as_it_is(self) -> None:
        no_credential = ClientAuthenticationError(message="DefaultAzureCredential failed to retrieve a token")
        self.calls.script["get_blob_properties"].append(no_credential)
        with self.assertRaises(ClientAuthenticationError):
            self.store.head(self.uri("t/x"))
        self.assertIsNone(store_error(no_credential, self.uri("t/x")))
        # azure-identity's own refusal, a ClientAuthenticationError without an answer too.
        self.assertIsNone(store_error(CredentialUnavailableError(message="no managed identity endpoint"), self.uri("t/x")))
        self.assertIsNone(store_error(ValueError("bad argument"), self.uri("t/x")))
        mapped = StoreError("already mapped")
        self.assertIs(store_error(mapped, self.uri("t/x")), mapped)


class AzureClientTests(unittest.TestCase):
    """Which client and credential each environment builds: real clients, no request."""

    def setUp(self) -> None:
        environment = patch.dict(os.environ, {}, clear=False)
        environment.start()
        self.addCleanup(environment.stop)
        for name in CREDENTIAL_VARIABLES:
            os.environ.pop(name, None)

    def test_a_connection_string_serves_its_own_account(self) -> None:
        os.environ["AZURE_STORAGE_CONNECTION_STRING"] = (
            f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey={DEV_KEY};"
            "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
        )
        store = AzureBlobStore.from_environment()
        client = store.service_client("devstoreaccount1")
        self.assertIsInstance(client, BlobServiceClient)
        self.assertEqual((client.account_name, client.url), ("devstoreaccount1", "http://localhost:10000/devstoreaccount1/"))
        self.assertIs(store.service_client("devstoreaccount1"), client)
        self.assertSettings(client)

    def test_a_connection_string_for_another_account_is_refused_without_showing_it(self) -> None:
        connection_string = f"DefaultEndpointsProtocol=https;AccountName=other;AccountKey={DEV_KEY};EndpointSuffix=core.windows.net"
        os.environ["AZURE_STORAGE_CONNECTION_STRING"] = connection_string
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = DEV_KEY  # never a fallback for a mismatched string
        store = AzureBlobStore(settings=StoreSettings())
        with self.assertRaises(AzureConfigurationError) as caught:
            store.head(f"abfss://box@{ACCOUNT}.dfs.core.windows.net/t/x")
        message = str(caught.exception)
        self.assertIn("'other'", message)
        self.assertIn(f"'{ACCOUNT}'", message)
        self.assertNotIn(DEV_KEY, message)
        self.assertNotIn(connection_string, message)
        self.assertEqual((caught.exception.status, caught.exception.code, caught.exception.retryable), (None, "AccountMismatch", False))

    def test_an_account_key_signs_for_the_account_the_uri_names(self) -> None:
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = DEV_KEY
        emulator = AzureBlobStore(settings=StoreSettings(azure_endpoint="http://azurite:10000"))
        client = emulator.service_client(ACCOUNT)
        self.assertEqual(client.url, f"http://azurite:10000/{ACCOUNT}/")
        self.assertEqual(client.credential.account_name, ACCOUNT)
        self.assertEqual(client.credential.account_key, DEV_KEY)
        cloud = AzureBlobStore(settings=StoreSettings())
        self.assertEqual(cloud.service_client("lakeacct").url, "https://lakeacct.blob.core.windows.net/")
        self.assertEqual(cloud.service_client("lakeacct").credential.account_name, "lakeacct")
        self.assertSettings(client)

    def test_without_secrets_one_default_credential_serves_every_account(self) -> None:
        built: list[object] = []

        class FakeDefaultCredential:
            def __init__(self) -> None:
                built.append(self)

            def get_token(self, *scopes: str, **kwargs: object) -> AccessToken:
                return AccessToken("token", 2_000_000_000)

        with patch.object(azure_store, "_DEFAULT_CREDENTIAL", None), patch("azure.identity.DefaultAzureCredential", FakeDefaultCredential):
            first = AzureBlobStore(settings=StoreSettings())
            second = AzureBlobStore(settings=StoreSettings())
            with ThreadPoolExecutor(max_workers=8) as pool:
                clients = list(pool.map(first.service_client, ["one", "two"] * 8))
            other = second.service_client("three")
        self.assertEqual(len(built), 1)
        self.assertEqual(len({id(client) for client in clients}), 2)
        self.assertEqual({client.url for client in clients}, {"https://one.blob.core.windows.net/", "https://two.blob.core.windows.net/"})
        self.assertIs(other.credential, built[0])
        self.assertIs(clients[0].credential, built[0])

    def test_a_given_credential_wins_over_the_environment(self) -> None:
        os.environ["AZURE_STORAGE_CONNECTION_STRING"] = f"DefaultEndpointsProtocol=https;AccountName=other;AccountKey={DEV_KEY}"
        store = AzureBlobStore(settings=StoreSettings(), credential=AzureNamedKeyCredential(ACCOUNT, DEV_KEY))
        self.assertEqual(store.service_client(ACCOUNT).url, f"https://{ACCOUNT}.blob.core.windows.net/")

    def test_every_client_shares_one_transport_with_a_large_pool_and_no_urllib3_retries(self) -> None:
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = DEV_KEY
        store = AzureBlobStore(settings=StoreSettings())
        transports = {id(store.service_client(account)._pipeline._transport) for account in ("one", "two")}
        self.assertEqual(transports, {id(azure_store.shared_transport())})
        transport = azure_store.shared_transport()
        self.assertIsInstance(transport, RequestsTransport)
        self.assertFalse(transport._session_owner)
        for prefix in ("https://", "http://"):
            adapter = transport.session.get_adapter(prefix + "acct.blob.core.windows.net")
            self.assertEqual(adapter._pool_maxsize, 32)
            self.assertFalse(adapter.max_retries.total)
        self.assertEqual((transport.connection_config.timeout, transport.connection_config.read_timeout), (10, 120))
        # Blob clients made from a service client keep using it.
        blob = store.service_client("one").get_blob_client("box", "k")
        self.assertIs(blob._pipeline._transport._transport, transport)

    def assertSettings(self, client: BlobServiceClient) -> None:
        config = client._config
        self.assertEqual(
            (config.max_single_put_size, config.max_block_size, config.max_single_get_size, config.max_chunk_get_size),
            (8 * 1024 * 1024,) * 4,
        )
        self.assertIsInstance(config.retry_policy, ExponentialRetry)
        self.assertEqual((config.retry_policy.initial_backoff, config.retry_policy.increment_base, config.retry_policy.total_retries), (1, 2, 3))


class NoSleep:
    """The `time` module without its waits: the library sleeps a second between reads of a body."""

    def __getattr__(self, name: str) -> object:
        return getattr(time, name)

    @staticmethod
    def sleep(seconds: float) -> None:
        pass


class FakeBlobService(requests.adapters.BaseAdapter):
    """The Blob service's answers behind a requests session: each request recorded, the scripted
    answers given in order (status, headers, body), 200 with an ETag otherwise."""

    def __init__(self) -> None:
        super().__init__()
        self.requests: list[requests.PreparedRequest] = []
        self.answers: list[tuple[int, dict[str, str], bytes]] = []

    def send(self, request: requests.PreparedRequest, **kwargs: object) -> requests.Response:
        self.requests.append(request)
        status, headers, body = self.answers.pop(0) if self.answers else (200, {"ETag": NEWER}, b"")
        headers = {"Content-Length": str(len(body)), "Last-Modified": "Fri, 30 Oct 2026 12:00:01 GMT", "x-ms-request-id": "1", **headers}
        raw = urllib3.HTTPResponse(body=io.BytesIO(body), headers=headers, status=status, preload_content=False, decode_content=False)
        return requests.adapters.HTTPAdapter().build_response(request, raw)

    def close(self) -> None:
        pass


class AzureWireTests(unittest.TestCase):
    """The real library between the store and a transport double: what goes over the wire."""

    def setUp(self) -> None:
        self.service = FakeBlobService()
        session = requests.Session()
        session.mount("https://", self.service)
        client = BlobServiceClient(
            f"https://{ACCOUNT}.blob.core.windows.net",
            credential=AzureNamedKeyCredential(ACCOUNT, DEV_KEY),
            transport=RequestsTransport(session=session, session_owner=False),
            # Retries as the store's, without the waits.
            retry_policy=ExponentialRetry(initial_backoff=0, increment_base=0, retry_total=3, random_jitter_range=0),
        )
        self.store = AzureBlobStore(settings=StoreSettings(), clients={ACCOUNT: client})

    def test_conditions_arrive_as_the_headers_azure_reads(self) -> None:
        for condition, expected in ((ABSENT, ("*", None)), (VERSION, (None, ETAG)), (None, (None, None))):
            self.service.answers.append((201, {"ETag": NEWER}, b""))
            self.assertEqual(self.store.put(f"{ROOT}/t/p", b"payload", if_version=condition, content_type="application/json"), NEWER.strip('"'))
            request = self.service.requests[-1]
            self.assertEqual(request.method, "PUT")
            self.assertEqual((request.headers.get("If-None-Match"), request.headers.get("If-Match")), expected)
            self.assertEqual(request.headers["x-ms-blob-content-type"], "application/json")
            self.assertEqual(request.headers["x-ms-blob-type"], "BlockBlob")

    def test_a_range_is_asked_for_by_x_ms_range_and_the_etag_comes_back_bare(self) -> None:
        self.service.answers.append((206, {"ETag": ETAG, "Content-Range": "bytes 10-19/1024", "x-ms-blob-type": "BlockBlob"}, b"0123456789"))
        self.assertEqual(self.store.get(f"{ROOT}/t/r", start=10, end=19), (b"0123456789", VERSION))
        self.assertEqual(self.service.requests[-1].headers["x-ms-range"], "bytes=10-19")

    def test_a_conditional_delete_sends_the_quoted_etag(self) -> None:
        self.service.answers.append((202, {}, b""))
        self.assertTrue(self.store.delete(f"{ROOT}/t/d", if_version=VERSION))
        self.assertEqual(self.service.requests[-1].headers["If-Match"], ETAG)

    def test_a_body_cut_short_is_a_failure_in_transit_once_the_library_stops_reading_it(self) -> None:
        # 1,000 bytes announced and 10 sent, three times: the library reads the blob three times, a
        # second apart (not here), then raises an HttpResponseError that carries no answer.
        cut = (206, {"ETag": ETAG, "Content-Length": "1000", "Content-Range": "bytes 0-999/1000", "x-ms-blob-type": "BlockBlob"}, b"x" * 10)
        logger = logging.getLogger("azure.core")  # it warns "Incomplete download." on each read
        level = logger.level
        logger.setLevel(logging.ERROR)
        self.addCleanup(logger.setLevel, level)
        with tempfile.TemporaryDirectory() as directory, patch("azure.storage.blob._download.time", NoSleep()):
            target = os.path.join(directory, "target")
            for name, attempt in (("get", lambda: self.store.get(f"{ROOT}/t/cut")), ("get_to_file", lambda: self.store.get_to_file(f"{ROOT}/t/cut", target))):
                with self.subTest(operation=name):
                    self.service.requests.clear()
                    self.service.answers.extend([cut] * 3)
                    with self.assertRaises(StoreError) as caught:
                        attempt()
                    error = caught.exception
                    self.assertEqual((type(error), error.status, error.code, error.retryable), (StoreError, None, "IncompleteReadError", True))
                    self.assertEqual(len(self.service.requests), 3)
            self.assertEqual(os.listdir(directory), [])

    def test_each_delete_in_a_batch_names_its_blob_by_the_whole_path(self) -> None:
        # Behind an emulator's path-style endpoint the account comes first whatever the host is called
        # (the library adds it itself only for `localhost`); an account's own host needs none.
        session = requests.Session()
        session.mount("http://", self.service)
        emulator = BlobServiceClient(
            f"http://azurite.midstream.svc:10000/{ACCOUNT}",
            credential=AzureNamedKeyCredential(ACCOUNT, DEV_KEY),
            transport=RequestsTransport(session=session, session_owner=False),
        )
        stores = ((AzureBlobStore(settings=StoreSettings(), clients={ACCOUNT: emulator}), f"/{ACCOUNT}"), (self.store, ""))
        for store, prefix in stores:
            with self.subTest(prefix=prefix):
                self.service.requests.clear()
                self.service.answers.append((503, {"x-ms-error-code": "ServerBusy"}, b""))
                store.delete_many([f"{ROOT}/t/a", f"{ROOT}/t/b c"])
                [batch] = self.service.requests
                body = batch.body if isinstance(batch.body, bytes) else str(batch.body).encode()
                # Each sub-request's path, without its query (empty, or a SAS).
                paths = [path.decode() for path in re.findall(rb"DELETE ([^?\s]+)\S* HTTP/1\.1", body)]
                self.assertEqual(paths, [f"{prefix}/box/t/a", f"{prefix}/box/t/b%20c"])

    def test_deletes_are_sent_once_while_other_requests_are_retried(self) -> None:
        busy = (503, {"x-ms-error-code": "ServerBusy"}, b"")
        self.service.answers.extend([busy] * 4)
        with self.assertRaises(StoreError) as caught:
            self.store.head(f"{ROOT}/t/x")
        self.assertEqual((caught.exception.status, len(self.service.requests)), (503, 4))
        self.service.requests.clear()
        self.service.answers.append(busy)
        outcome = self.store.delete_many([f"{ROOT}/t/a", f"{ROOT}/t/b"])
        self.assertEqual([failure.status for failure in outcome.failed], [503, 503])
        self.assertEqual([request.method for request in self.service.requests], ["POST"])
        self.assertIn("comp=batch", self.service.requests[0].url)
        # One by one (a service that refuses batches), each delete is sent once as well.
        self.service.requests.clear()
        self.store._unbatched.add(ACCOUNT)
        self.service.answers.append(busy)
        outcome = self.store.delete_many([f"{ROOT}/t/a"])
        self.assertEqual([failure.status for failure in outcome.failed], [503])
        self.assertEqual([request.method for request in self.service.requests], ["DELETE"])


if __name__ == "__main__":
    unittest.main()
