"""The conformance suite against live stores: emulators in CI, real buckets by hand (design §11.2).

Skipped unless `STORAGE_LIVE_TESTS` names base URIs, comma-separated, each with the environment its
store reads - one run per S3 endpoint, since a process has one:

    STORAGE_EMULATOR_HOST=http://localhost:4443 STORAGE_LIVE_TESTS=gs://live-core-check \\
        python -m unittest discover -s python/tests -p 'test_store_live.py' -v
    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... STORAGE_S3_ENDPOINT=http://localhost:8333 \\
        STORAGE_S3_PATH_STYLE=true STORAGE_LIVE_TESTS=s3://live-s3-check python -m unittest ...
    AZURE_STORAGE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=...;BlobEndpoint=http://localhost:10000/devstoreaccount1;' \\
        STORAGE_LIVE_TESTS=abfss://live-check@devstoreaccount1.dfs.core.windows.net python -m unittest ...

Each URI's bucket or container is created first when it is missing, through the client of the store
`store_for` resolves (the same endpoint and credentials); then every check runs against that store,
16 writers racing for `STORAGE_LIVE_ROUNDS` rounds (20 unless set), with a 20 MiB streamed file - past
the 8 MiB that azure-storage-blob and google-cloud-storage still send in one request, so its race of
create-only uploads races Azure's blocks and GCS's resumable uploads - and each result is printed. A
store that declares no conditional deletes (S3's) has that check skipped, and says why. SeaweedFS
takes about 0.6 s per writer to settle create-only writes racing for one key (16 racers: ten seconds a
round), so CI checks it with fewer rounds.

Then each backend's own cases, under a fresh prefix they empty again: on both, ranges from the size
or past it refused with 416, and any range of an empty object; on S3, `DeleteObjects` over 1,050 keys
in two requests and a 64 MiB file in one `PutObject`; on Azure, a create-only upload in blocks refused
when the blob exists with no block left staged, the block-list commit carrying the condition for a
race past that check, six create-only uploads in blocks racing for each of four keys (the race the
azure-storage-blob pin is for), deletes of existing and missing blobs 256 to a batch - and again
through the emulator's address under a name other than `localhost`, as a cluster reaches it - and a
64 MiB file in 8 MiB blocks.

fake-gcs-server differs from GCS in two places the store does not bend to, so against it (when
`STORAGE_EMULATOR_HOST` is set) the run skips exactly those, and says so: conditional deletes, whose
precondition it ignores, and batch deletes, which the library cannot read back from it - deletes go
one per request there. `test_store_gcs.py` covers both against GCS's own answers.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.parse
import uuid

from midstream.lakehouse.store import (
    ABSENT,
    ObjectStore,
    PreconditionFailedError,
    StoreError,
    StoreSettings,
    parse_object_uri,
    reset_stores,
    scheme_of,
    set_store,
    store_for,
)
from midstream.lakehouse.store.check import format_result, run_checks, run_root

LIVE_URIS = [uri.strip() for uri in os.environ.get("STORAGE_LIVE_TESTS", "").split(",") if uri.strip()]
#: Rounds each racing check runs: keys raced by the creators, compare-and-swaps per writer.
ROUNDS = int(os.environ.get("STORAGE_LIVE_ROUNDS", "").strip() or 20)
_MIB = 1024 * 1024
#: The conformance suite's large file: three Azure blocks (8, 8 and 4 MiB) and a GCS resumable upload.
#: Never 8 MiB or less, which both libraries send in one request: then its race of create-only uploads
#: would race single PUTs, never the blocks whose mixing it exists to catch.
LARGE_BYTES = 20 * _MIB
#: The backend cases' large file: eight Azure blocks, one S3 PUT.
BIG_FILE_BYTES = 64 * _MIB
#: The Azure block race: uploads racing for each key, keys raced, and a file of three blocks.
BLOCK_RACERS = 6
BLOCK_RACE_ROUNDS = 4
BLOCK_RACE_BYTES = 20 * _MIB

#: What fake-gcs-server does differently from GCS, and so what a run against it leaves out.
GCS_EMULATOR_CONDITIONAL_DELETE = (
    "fake-gcs-server ignores preconditions on DELETE: a delete with a stale generation deletes"
)
GCS_EMULATOR_BATCH = (
    "fake-gcs-server answers the JSON API's batch requests in a form google-cloud-storage cannot read "
    "(ValueError: Bad response: not multi-part), having deleted nothing: delete_many sends one request per object"
)


def live_uri(scheme: str) -> str | None:
    """The first base URI of `scheme` in `STORAGE_LIVE_TESTS`."""
    return next((uri for uri in LIVE_URIS if scheme_of(uri) == scheme), None)


@unittest.skipUnless(LIVE_URIS, "set STORAGE_LIVE_TESTS to the base URIs of live stores to check")
class LiveConformanceTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_stores()
        self.addCleanup(reset_stores)

    def test_every_live_store_keeps_the_contract(self) -> None:
        for uri in LIVE_URIS:
            with self.subTest(uri=uri):
                self.check(uri)

    def check(self, uri: str) -> None:
        scheme = scheme_of(uri)
        skip: dict[str, str] = {}
        ensure_container(uri)
        if scheme == "gs" and os.environ.get("STORAGE_EMULATOR_HOST"):
            from midstream.lakehouse.store.gcs import GcsStore

            set_store("gs", GcsStore(batch_deletes=False))
            skip["conditional_delete"] = GCS_EMULATOR_CONDITIONAL_DELETE
            print(f"\n{uri}: against fake-gcs-server, {GCS_EMULATOR_BATCH}")
        store = store_for(uri)
        if getattr(store, "supports_conditional_delete", True) is False:
            skip["conditional_delete"] = f"{type(store).__name__} declares no conditional deletes (supports_conditional_delete is False)"
        results = run_checks(store, uri, writers=16, rounds=ROUNDS, listing_objects=1050, large_bytes=LARGE_BYTES, skip=skip)
        print(f"\n{type(store).__name__} under {uri}:")
        for result in results:
            print(f"  {format_result(result)}")
        self.assertEqual([format_result(result) for result in results if not result.passed], [])


class LiveBackendTestCase(unittest.TestCase):
    """One backend's own cases, under a fresh prefix of its live URI that each test empties again."""

    scheme = ""

    def setUp(self) -> None:
        reset_stores()
        self.addCleanup(reset_stores)
        base = live_uri(self.scheme)
        assert base is not None
        ensure_container(base)
        self.store: ObjectStore = store_for(base)
        self.root = run_root(base, f"backend-{uuid.uuid4().hex[:12]}")
        self.addCleanup(self.empty_root)
        directory = tempfile.TemporaryDirectory(prefix="midstream-live-")
        self.addCleanup(directory.cleanup)
        self.directory = directory.name

    def empty_root(self) -> None:
        uris = [item.uri for item in self.store.list(self.root)]
        batch = self.store.delete_batch_size
        for begin in range(0, len(uris), batch):
            outcome = self.store.delete_many(uris[begin : begin + batch])
            for failure in outcome.failed:
                self.store.delete(failure.uri)
        self.assertEqual(list(self.store.list(self.root)), [], f"objects were left under {self.root}")

    def random_file(self, name: str, size: int) -> tuple[str, str]:
        """A local file of `size` random bytes, and its SHA-256."""
        path = os.path.join(self.directory, name)
        digest = hashlib.sha256()
        with open(path, "wb") as handle:
            remaining = size
            while remaining:
                chunk = os.urandom(min(remaining, _MIB))
                handle.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
        return path, digest.hexdigest()

    def check_reads_past_the_end(self) -> None:
        """A range from the last byte reads it; one from the size or past it is 416, and so is any range
        of an empty object, whose whole is empty bytes (`check.py`'s range_read never starts at the size)."""
        ten = f"{self.root}ten-bytes"
        version = self.store.put(ten, b"0123456789")
        self.assertEqual(self.store.get(ten, start=9), (b"9", version))
        empty = f"{self.root}empty"
        nothing = self.store.put(empty, b"")
        self.assertEqual(self.store.get(empty), (b"", nothing))
        for uri, start, end in ((ten, 10, None), (ten, 10, 12), (ten, 11, None), (ten, 11, 20), (empty, 0, None), (empty, None, 0), (empty, 0, 5)):
            with self.subTest(object=uri.rsplit("/", 1)[1], start=start, end=end):
                with self.assertRaises(StoreError) as caught:
                    self.store.get(uri, start=start, end=end)
                self.assertEqual((type(caught.exception), caught.exception.status), (StoreError, 416))

    def round_trip(self, uri: str, size: int) -> str:
        """Upload a file of `size` create-only, read it back to a file, check both; the version."""
        source, digest = self.random_file("source", size)
        version = self.store.put_file(uri, source, if_version=ABSENT)
        described = self.store.head(uri)
        self.assertIsNotNone(described)
        assert described is not None
        self.assertEqual((described.size, described.version), (size, version))
        target = os.path.join(self.directory, "target")
        info = self.store.get_to_file(uri, target)
        self.assertEqual((info.size, info.version), (size, version))
        self.assertIsNotNone(info.last_modified)
        self.assertEqual(file_digest(target), digest)
        with self.assertRaises(PreconditionFailedError):
            self.store.put_file(uri, source, if_version=ABSENT)
        return version


@unittest.skipUnless(live_uri("s3"), "set STORAGE_LIVE_TESTS to an s3:// base URI to run the S3 store's own live cases")
class LiveS3Tests(LiveBackendTestCase):
    scheme = "s3"

    def test_a_range_from_the_size_or_past_it_is_refused(self) -> None:
        self.check_reads_past_the_end()

    def test_delete_many_sends_1050_keys_in_two_requests(self) -> None:
        uris = [f"{self.root}delete-many/object-{index:05d}" for index in range(1050)]
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(lambda uri: self.store.put(uri, b"x"), uris))
        requests: list[int] = []
        self.store.delete_client.meta.events.register(
            "before-parameter-build.s3.DeleteObjects",
            lambda params, **_: requests.append(len(params["Delete"]["Objects"])),
        )
        outcome = self.store.delete_many(uris + [f"{self.root}delete-many/never-written"])
        self.assertEqual(requests, [1000, 51])
        self.assertEqual(outcome.failed, ())
        self.assertEqual(len(outcome.deleted), 1051)
        self.assertEqual(list(self.store.list(f"{self.root}delete-many/")), [])

    def test_a_64_mib_file_goes_up_in_one_put_object_and_comes_back_whole(self) -> None:
        operations: list[str] = []
        for operation in ("PutObject", "CreateMultipartUpload", "UploadPart"):
            self.store.client.meta.events.register(f"before-parameter-build.s3.{operation}", lambda model, **_: operations.append(model.name))
        self.round_trip(f"{self.root}large", BIG_FILE_BYTES)
        # The create-only upload and the refused second one: never the transfer manager's parts.
        self.assertEqual(operations, ["PutObject", "PutObject"])


@unittest.skipUnless(live_uri("abfss"), "set STORAGE_LIVE_TESTS to an abfss:// base URI to run the Azure store's own live cases")
class LiveAzureTests(LiveBackendTestCase):
    scheme = "abfss"

    def blob_client(self, uri: str) -> object:
        parsed = parse_object_uri(uri)
        return self.store.service_client(parsed.account).get_blob_client(parsed.container, parsed.key)

    def test_a_range_from_the_size_or_past_it_is_refused(self) -> None:
        # Azurite answers a range from exactly the size with 206 and no bytes, not 416: the store refuses it.
        self.check_reads_past_the_end()

    def test_create_only_uploads_racing_in_blocks_commit_the_winners_file_whole(self) -> None:
        # The race the azure-storage-blob>=12.31 pin is for: up to 12.30 block ids came from offsets,
        # so racing uploads staged their blocks under the same ids and the winner's block-list commit
        # could take a loser's bytes. The conformance suite races three uploads once; this, more and longer.
        sources = [self.random_file(f"racer-{index}", BLOCK_RACE_BYTES) for index in range(BLOCK_RACERS)]
        target = os.path.join(self.directory, "raced")
        for round_number in range(BLOCK_RACE_ROUNDS):
            uri = f"{self.root}block-race/key-{round_number:02d}"
            barrier = threading.Barrier(len(sources))

            def create(path: str) -> bool:
                barrier.wait(timeout=60)
                try:
                    self.store.put_file(uri, path, if_version=ABSENT)
                except PreconditionFailedError:
                    return False
                return True

            with ThreadPoolExecutor(max_workers=len(sources)) as pool:
                won = list(pool.map(create, [path for path, _ in sources]))
            winners = [digest for (_, digest), outcome in zip(sources, won) if outcome]
            self.assertEqual(len(winners), 1, f"round {round_number}: {len(winners)} of {len(won)} create-only uploads won")
            self.store.get_to_file(uri, target)
            self.assertEqual(file_digest(target), winners[0], f"round {round_number}: the blob does not hold the winning upload's file whole")

    def test_a_create_only_upload_in_blocks_is_refused_with_no_block_staged(self) -> None:
        uri = f"{self.root}chunked-create-only"
        self.store.put(uri, b"first")
        source, _ = self.random_file("source", 20 * _MIB)
        with self.assertRaises(PreconditionFailedError):
            self.store.put_file(uri, source, if_version=ABSENT)
        self.assertEqual(self.store.get(uri)[0], b"first")
        committed, uncommitted = self.blob_client(uri).get_block_list("all")
        self.assertEqual((committed, uncommitted), ([], []))

    def test_the_block_list_commit_carries_the_condition(self) -> None:
        # A race past the early check: the commit itself must refuse, leaving the blob as it was.
        uri = f"{self.root}chunked-commit"
        first = self.store.put(uri, b"first")
        self.store.put(uri, b"second")
        source, _ = self.random_file("source", 20 * _MIB)
        with patch.object(self.store, "_refuse_early", lambda *args, **kwargs: None):
            with self.assertRaises(PreconditionFailedError) as created:
                self.store.put_file(uri, source, if_version=ABSENT)
            with self.assertRaises(PreconditionFailedError) as swapped:
                self.store.put_file(uri, source, if_version=first)
        self.assertEqual((created.exception.status, created.exception.code), (409, "BlobAlreadyExists"))
        self.assertEqual((swapped.exception.status, swapped.exception.code), (412, "ConditionNotMet"))
        self.assertEqual(self.store.get(uri)[0], b"second")
        # The staged blocks stay uncommitted until Azure discards them; they never become content.
        self.assertGreater(len(self.blob_client(uri).get_block_list("uncommitted")[1]), 0)

    def test_delete_many_deletes_existing_and_missing_blobs_256_to_a_batch(self) -> None:
        from azure.storage.blob import ContainerClient

        present = [f"{self.root}delete-many/object-{index:04d}" for index in range(300)]
        absent = [f"{self.root}delete-many/missing-{index:02d}" for index in range(20)]
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(lambda uri: self.store.put(uri, b"x"), present))
        batches: list[int] = []
        original = ContainerClient.delete_blobs

        def counted(client: object, *names: str, **kwargs: object) -> object:
            batches.append(len(names))
            return original(client, *names, **kwargs)

        with patch.object(ContainerClient, "delete_blobs", counted):
            outcome = self.store.delete_many(present + absent)
        self.assertEqual(batches, [256, 64])
        self.assertEqual(outcome.failed, ())
        self.assertEqual(outcome.deleted, tuple(present + absent))
        self.assertEqual(list(self.store.list(f"{self.root}delete-many/")), [])

    def test_delete_many_through_an_emulator_host_not_called_localhost(self) -> None:
        # The library names a batch's deletes by their whole path itself only behind `localhost`;
        # behind any other name - a cluster's `http://azurite.<namespace>.svc:10000` - the store must,
        # or the emulator answers the batch with one 400 and deletes nothing.
        from azure.core.credentials import AzureNamedKeyCredential
        from azure.storage.blob import ContainerClient

        from midstream.lakehouse.store.azure import AzureBlobStore

        uris = [f"{self.root}alias/object-{index}" for index in range(5)]
        account = parse_object_uri(uris[0]).account
        client = self.store.service_client(account)  # type: ignore[attr-defined]
        address = urllib.parse.urlsplit(client.url)
        key = getattr(client.credential, "account_key", None)
        if address.hostname not in ("localhost", "127.0.0.1") or not key:
            self.skipTest("needs an emulator on this machine and its account key, to reach it again as 0.0.0.0")
        alias = f"{address.scheme}://0.0.0.0" + (f":{address.port}" if address.port else "")
        store = AzureBlobStore(settings=StoreSettings(azure_endpoint=alias), credential=AzureNamedKeyCredential(account, key))
        for uri in uris:
            self.store.put(uri, b"x")
        batches: list[int] = []
        original = ContainerClient.delete_blobs

        def counted(container: object, *names: str, **kwargs: object) -> object:
            batches.append(len(names))
            return original(container, *names, **kwargs)

        with patch.object(ContainerClient, "delete_blobs", counted):
            outcome = store.delete_many(uris + [f"{self.root}alias/missing"])
        self.assertEqual(batches, [6])
        self.assertEqual(outcome.failed, ())
        self.assertEqual(len(outcome.deleted), 6)
        self.assertEqual(list(self.store.list(f"{self.root}alias/")), [])

    def test_a_64_mib_file_goes_up_in_8_mib_blocks_and_comes_back_whole(self) -> None:
        uri = f"{self.root}large"
        self.round_trip(uri, BIG_FILE_BYTES)
        committed, _ = self.blob_client(uri).get_block_list("committed")
        self.assertEqual([block.size for block in committed], [8 * _MIB] * 8)


def ensure_container(uri: str) -> None:
    """Create the URI's bucket or container when it is missing, through the client of the store that
    will serve it (the same endpoint and credentials)."""
    scheme = scheme_of(uri)
    if scheme == "gs":
        ensure_gcs_bucket(uri)
    elif scheme == "s3":
        ensure_s3_bucket(uri)
    elif scheme == "abfss":
        ensure_azure_container(uri)


def ensure_gcs_bucket(uri: str) -> None:
    """Create the URI's bucket when it is missing, as the store's own client would reach it."""
    from google.cloud import storage

    client = storage.Client()  # honours STORAGE_EMULATOR_HOST
    name = parse_object_uri(uri).container
    if client.lookup_bucket(name) is None:
        client.create_bucket(name)


def ensure_s3_bucket(uri: str) -> None:
    """Create the URI's bucket with the S3 store's client when a HEAD finds none."""
    from botocore.exceptions import ClientError

    client = store_for(uri).client  # type: ignore[attr-defined]
    bucket = parse_object_uri(uri).container
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
    params: dict[str, object] = {"Bucket": bucket}
    region = client.meta.region_name
    if region and region != "us-east-1":
        params["CreateBucketConfiguration"] = {"LocationConstraint": region}
    try:
        client.create_bucket(**params)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def ensure_azure_container(uri: str) -> None:
    """Create the URI's container with the Azure store's client for its account."""
    from azure.core.exceptions import HttpResponseError

    parsed = parse_object_uri(uri)
    store = store_for(uri)
    container = store.service_client(parsed.account).get_container_client(parsed.container)  # type: ignore[attr-defined]
    try:
        container.create_container()
    except HttpResponseError as exc:
        if (exc.status_code, exc.response.headers.get("x-ms-error-code")) != (409, "ContainerAlreadyExists"):
            raise


def file_digest(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(_MIB), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    unittest.main()
