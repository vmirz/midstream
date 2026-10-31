"""Conditional writes whose answer was lost: the store applied them, and the retry was refused.

Every storage library retries a conditional write whose answer it never got - google-cloud-storage
uploads that carry a generation precondition, boto3 and azure-core after a connection error - and
the retry of a write that landed is refused as a precondition failure (design §3.3); a write that
got no answer at all may have landed too. The catalog used to delete the staged metadata file on such
a refusal, though the pointer might name it already, leaving the table without its current metadata.
A failed pointer write is now checked against the pointer read back, and a refused write of the
staged metadata file against the object at its name: these tests stage the lost answer with a store
that applies a write and then fails it.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import IDENTIFIER, LOCATION, MATERIALIZATION_ID, LakeFixture  # noqa: E402

from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException  # noqa: E402

from midstream.lakehouse.catalog import PointerCatalog, StaticTableDirectory, TableEntry  # noqa: E402
from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.pointer import delete_pointer, pointer_uri, read_pointer, version_hint_uri, write_pointer  # noqa: E402
from midstream.lakehouse.store import MemoryStore, PreconditionFailedError, StoreError  # noqa: E402
from midstream.runtime_app.models import SealedFile  # noqa: E402
from midstream.runtime_app.writer.upload import upload_sealed_file  # noqa: E402

POINTER = pointer_uri(LOCATION)


class LostAnswerStore(MemoryStore):
    """A `MemoryStore` whose next conditional write of `lose` (a URI, or a test of one) lands and then
    fails with `answer(uri)`: refused by default, as the retry of a write whose answer was lost is.
    `after_landing` runs in between; `landed` holds the URIs written so."""

    def __init__(self) -> None:
        super().__init__()
        self.lose: str | Callable[[str], bool] | None = None
        self.answer: Callable[[str], BaseException] = lambda uri: PreconditionFailedError(f"{uri}: the retry of a write that landed", uri=uri)
        self.after_landing: Callable[[], None] | None = None
        self.landed: list[str] = []

    def put(self, uri, data, *, if_version=None, content_type="application/octet-stream"):
        version = super().put(uri, data, if_version=if_version, content_type=content_type)
        self._refuse_if_lost(uri, if_version)
        return version

    def put_file(self, uri, path, *, if_version=None, content_type="application/octet-stream"):
        version = super().put_file(uri, path, if_version=if_version, content_type=content_type)
        self._refuse_if_lost(uri, if_version)
        return version

    def _refuse_if_lost(self, uri: str, if_version: object) -> None:
        if self.lose is None or if_version is None:
            return
        if not (self.lose(uri) if callable(self.lose) else uri == self.lose):
            return
        self.lose = None
        self.landed.append(uri)
        if self.after_landing is not None:
            self.after_landing()
        raise self.answer(uri)


def no_answer(uri: str) -> StoreError:
    return StoreError(f"{uri}: the connection was reset", uri=uri, status=None, code="ConnectionError")


def is_staged_metadata(uri: str) -> bool:
    return uri.startswith(f"{LOCATION}/metadata/") and uri.endswith(".metadata.json")


def metadata_files(store: MemoryStore) -> list[str]:
    return [uri for uri in store.names(f"{LOCATION}/metadata/") if uri.endswith(".metadata.json")]


class LostPointerAnswerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = LostAnswerStore()
        self.lake = LakeFixture({"order_id": "O-0", "amount": 1}, store=self.store)
        self.addCleanup(self.lake.close)

    def seal(self, offsets: list[int], *, after: int = -1):
        return self.lake.write_seal(partition=0, after_offset=after, offsets=offsets)

    def assert_readable(self, rows: int) -> None:
        pointer, _ = read_pointer(self.store, LOCATION)
        self.assertIsNotNone(pointer)
        self.assertIsNotNone(self.store.read(pointer.metadata_location), "the pointer names a metadata file that exists")
        self.assertEqual(rows, len(self.lake.load().scan().to_arrow()))

    def test_a_pointer_write_that_landed_is_the_commit(self) -> None:
        before = read_pointer(self.store, LOCATION)[0]
        self.store.lose = POINTER

        result = self.lake.commit(self.seal([0, 1, 2]))

        self.assertEqual(3, result.records)
        pointer, _ = read_pointer(self.store, LOCATION)
        self.assertEqual(before.metadata_location, pointer.previous_metadata_location)
        self.assertEqual(before.sequence + 1, pointer.sequence)
        self.assert_readable(rows=3)
        self.assertEqual({0: 2}, committed_offsets(self.lake.load().metadata).offsets)
        hint = self.store.read(version_hint_uri(LOCATION)).decode()
        self.assertTrue(pointer.metadata_location.endswith(f"/{hint}.metadata.json"))
        # The committer goes on from the table as it is: the next seal continues it.
        self.lake.commit(self.seal([3, 4], after=2))
        self.assert_readable(rows=5)

    def test_a_commit_since_does_not_hide_that_the_write_landed(self) -> None:
        other = PointerCatalog(
            directory=StaticTableDirectory([TableEntry(identifier=IDENTIFIER, location=LOCATION, materialization_id=MATERIALIZATION_ID)]),
            store=self.store,
            writer="other",
        )

        def commit_on_top() -> None:
            with other.load_table(IDENTIFIER).transaction() as transaction:
                transaction.set_properties({"test.other": "yes"})

        self.store.lose = POINTER
        self.store.after_landing = commit_on_top

        result = self.lake.commit(self.seal([0, 1, 2]))

        self.assertEqual(3, result.records)
        table = self.lake.load()
        self.assertEqual("yes", table.metadata.properties["test.other"])
        self.assertEqual({0: 2}, committed_offsets(table.metadata).offsets)
        self.assert_readable(rows=3)
        # Every metadata file the pointers named is still there: the two commits and the first.
        self.assertEqual(3, len(metadata_files(self.store)))
        # The hint is the later commit's: this one does not take it back.
        pointer, _ = read_pointer(self.store, LOCATION)
        hint = self.store.read(version_hint_uri(LOCATION)).decode()
        self.assertTrue(pointer.metadata_location.endswith(f"/{hint}.metadata.json"))

    def test_a_lost_race_still_deletes_the_staged_metadata_file(self) -> None:
        other = PointerCatalog(
            directory=StaticTableDirectory([TableEntry(identifier=IDENTIFIER, location=LOCATION, materialization_id=MATERIALIZATION_ID)]),
            store=self.store,
            writer="other",
        )
        announced = self.seal([0, 1, 2])
        raced = False

        def race(operation: str, uri: str):
            nonlocal raced
            if operation == "put" and uri == POINTER and not raced:
                raced = True
                with other.load_table(IDENTIFIER).transaction() as transaction:
                    transaction.set_properties({"test.other": "yes"})
            return None

        self.store.fail = race

        with self.assertRaises(CommitFailedException):
            self.lake.commit(announced)

        self.store.fail = None
        # The other commit's metadata file and the first; this commit's staged file is gone.
        self.assertEqual(2, len(metadata_files(self.store)))
        self.assert_readable(rows=0)
        self.assertEqual("yes", self.lake.load().metadata.properties["test.other"])

    def test_a_pointer_that_cannot_be_read_back_leaves_the_outcome_unknown_and_deletes_nothing(self) -> None:
        def unreadable() -> None:
            self.store.fail = lambda operation, uri: StoreError("no answer", uri=uri, status=None) if (operation, uri) == ("get", POINTER) else None

        self.store.lose = POINTER
        self.store.after_landing = unreadable

        with self.assertRaises(CommitStateUnknownException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.store.fail = None
        self.assert_readable(rows=3)

    def other_catalog(self) -> PointerCatalog:
        return PointerCatalog(
            directory=StaticTableDirectory([TableEntry(identifier=IDENTIFIER, location=LOCATION, materialization_id=MATERIALIZATION_ID)]),
            store=self.store,
            writer="other",
        )

    def commit_other(self, properties: dict[str, str]) -> None:
        with self.other_catalog().load_table(IDENTIFIER).transaction() as transaction:
            transaction.set_properties(properties)

    def fail_pointer_writes_once(self, error: Callable[[str], BaseException], *, before: Callable[[], None] | None = None) -> None:
        """Fail the next pointer write with `error` before it is applied; `before` runs first."""
        failed = False

        def hook(operation: str, uri: str):
            nonlocal failed
            if operation != "put" or uri != POINTER or failed:
                return None
            failed = True
            if before is not None:
                before()
            return error(uri)

        self.store.fail = hook

    def test_a_pointer_write_with_no_answer_that_landed_is_the_commit(self) -> None:
        self.store.lose = POINTER
        self.store.answer = no_answer

        result = self.lake.commit(self.seal([0, 1, 2]))

        self.assertEqual(3, result.records)
        self.assert_readable(rows=3)
        self.assertEqual({0: 2}, committed_offsets(self.lake.load().metadata).offsets)

    def test_a_pointer_write_with_no_answer_and_an_unmoved_pointer_is_unknown_and_deletes_nothing(self) -> None:
        # The write may still be in flight, and land.
        self.fail_pointer_writes_once(no_answer)

        with self.assertRaises(CommitStateUnknownException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.store.fail = None
        self.assertEqual(2, len(metadata_files(self.store)), "the staged metadata file is kept")
        self.assert_readable(rows=0)

    def test_a_refusal_with_an_unmoved_pointer_is_unknown_and_deletes_nothing(self) -> None:
        # S3 refuses a conditional write that meets another in flight (409): maybe this one's own.
        self.fail_pointer_writes_once(lambda uri: PreconditionFailedError(f"{uri}: a conflicting write is in progress", uri=uri, status=409))

        with self.assertRaises(CommitStateUnknownException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.store.fail = None
        self.assertEqual(2, len(metadata_files(self.store)), "the staged metadata file is kept")
        self.assert_readable(rows=0)

    def test_a_pointer_write_with_no_answer_after_another_commit_moved_on_deletes_the_staged_file(self) -> None:
        # Its condition names a version gone for good: the write can never land.
        self.fail_pointer_writes_once(no_answer, before=lambda: self.commit_other({"test.other": "yes"}))

        with self.assertRaises(CommitFailedException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.store.fail = None
        self.assertEqual(2, len(metadata_files(self.store)), "the other commit's and the first")
        self.assert_readable(rows=0)

    def test_a_staged_file_in_the_metadata_log_counts_as_landed(self) -> None:
        def two_commits_on_top() -> None:
            self.commit_other({"test.first": "yes"})
            self.commit_other({"test.second": "yes"})

        self.store.lose = POINTER
        self.store.after_landing = two_commits_on_top

        result = self.lake.commit(self.seal([0, 1, 2]))

        self.assertEqual(3, result.records)
        self.assert_readable(rows=3)
        self.assertEqual(4, len(metadata_files(self.store)))

    def test_a_race_lost_further_back_still_deletes_the_staged_metadata_file(self) -> None:
        def two_commits_first() -> None:
            self.commit_other({"test.first": "yes"})
            self.commit_other({"test.second": "yes"})

        self.fail_pointer_writes_once(
            lambda uri: PreconditionFailedError(f"{uri} moved", uri=uri),
            before=two_commits_first,
        )

        with self.assertRaises(CommitFailedException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.store.fail = None
        self.assertEqual(3, len(metadata_files(self.store)), "the two other commits' and the first")
        self.assert_readable(rows=0)

    def test_a_log_that_no_longer_reaches_back_leaves_the_outcome_unknown_and_deletes_nothing(self) -> None:
        self.commit_other({"write.metadata.previous-versions-max": "1"})
        files: list[str] = []

        def two_commits_on_top() -> None:
            self.commit_other({"test.first": "yes"})
            self.commit_other({"test.second": "yes"})
            files.extend(metadata_files(self.store))

        self.store.lose = POINTER
        self.store.after_landing = two_commits_on_top

        with self.assertRaises(CommitStateUnknownException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.assert_readable(rows=3)
        self.assertEqual(files, metadata_files(self.store), "the commit deleted nothing")

    def test_a_pointer_naming_the_staged_file_after_another_start_is_unknown(self) -> None:
        def rewrite_previous() -> None:
            pointer, version = read_pointer(self.store, LOCATION)
            odd = pointer.advanced(pointer.metadata_location, updated_by="other", updated_at=pointer.updated_at)
            write_pointer(self.store, LOCATION, odd, expected_version=version)

        self.store.lose = POINTER
        self.store.after_landing = rewrite_previous

        with self.assertRaises(CommitStateUnknownException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.assert_readable(rows=3)

    def test_a_table_dropped_meanwhile_deletes_the_staged_file(self) -> None:
        self.fail_pointer_writes_once(
            lambda uri: PreconditionFailedError(f"{uri} is gone", uri=uri),
            before=lambda: delete_pointer(self.store, LOCATION),
        )

        with self.assertRaises(CommitFailedException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.store.fail = None
        self.assertEqual(1, len(metadata_files(self.store)), "only the first")
        self.assertIsNone(read_pointer(self.store, LOCATION)[0])

    def test_a_staged_metadata_write_that_landed_goes_on_to_the_pointer(self) -> None:
        self.store.lose = is_staged_metadata

        result = self.lake.commit(self.seal([0, 1, 2]))

        self.assertEqual(3, result.records)
        pointer, _ = read_pointer(self.store, LOCATION)
        self.assertEqual(self.store.landed, [pointer.metadata_location])
        self.assert_readable(rows=3)

    def test_another_object_at_the_staged_metadata_name_fails_the_commit(self) -> None:
        self.store.lose = is_staged_metadata
        self.store.after_landing = lambda: self.store.write(self.store.landed[-1], b"{}")

        with self.assertRaises(CommitFailedException):
            self.lake.commit(self.seal([0, 1, 2]))

        self.assert_readable(rows=0)

    def test_creating_a_table_whose_metadata_write_was_refused_without_writing_is_unknown(self) -> None:
        # S3 answers a create-only write that meets another in flight with 409 and writes nothing.
        store = MemoryStore()
        location = LOCATION.replace("/orders", "/payments")
        prefix = f"{location}/metadata/00000-"
        store.fail = lambda operation, uri: (
            PreconditionFailedError(f"{uri}: in progress", uri=uri, status=409) if operation == "put" and uri.startswith(prefix) else None
        )

        with self.assertRaises(CommitStateUnknownException):
            LakeFixture({"payment_id": "P-0"}, store=store, location=location, identifier=(IDENTIFIER[0], "payments"), source_topic="payments")

        store.fail = None
        self.assertIsNone(read_pointer(store, location)[0], "no pointer names a file that is not there")

    def test_creating_a_table_whose_metadata_write_landed_finds_it_created(self) -> None:
        store = LostAnswerStore()
        location = LOCATION.replace("/orders", "/payments")
        store.lose = lambda uri: uri.startswith(f"{location}/metadata/00000-")

        lake = LakeFixture({"payment_id": "P-0"}, store=store, location=location, identifier=(IDENTIFIER[0], "payments"), source_topic="payments")
        self.addCleanup(lake.close)

        pointer, _ = read_pointer(store, location)
        self.assertEqual(store.landed, [pointer.metadata_location])
        self.assertIsNotNone(store.read(pointer.metadata_location))

    def test_creating_a_table_refused_with_no_pointer_is_unknown(self) -> None:
        store = MemoryStore()
        location = LOCATION.replace("/orders", "/payments")
        refused = pointer_uri(location)
        store.fail = lambda operation, uri: PreconditionFailedError(f"{uri}: in progress", uri=uri, status=409) if (operation, uri) == ("put", refused) else None

        with self.assertRaises(CommitStateUnknownException):
            LakeFixture({"payment_id": "P-0"}, store=store, location=location, identifier=(IDENTIFIER[0], "payments"), source_topic="payments")

        store.fail = None
        self.assertIsNone(read_pointer(store, location)[0])

    def test_creating_a_table_whose_pointer_write_landed_finds_it_created(self) -> None:
        store = LostAnswerStore()
        location = LOCATION.replace("/orders", "/payments")
        store.lose = pointer_uri(location)

        lake = LakeFixture({"payment_id": "P-0"}, store=store, location=location, identifier=(IDENTIFIER[0], "payments"), source_topic="payments")
        self.addCleanup(lake.close)

        pointer, _ = read_pointer(store, location)
        self.assertEqual(lake.table.metadata_location, pointer.metadata_location)
        self.assertIsNotNone(store.read(pointer.metadata_location))
        self.assertIsNotNone(store.read(version_hint_uri(location)))
        lake.commit(lake.write_seal(partition=0, after_offset=-1, offsets=[0], values=[{"payment_id": "P-0"}]))
        self.assertEqual(1, len(lake.load().scan().to_arrow()))


class LostUploadAnswerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = LostAnswerStore()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "file.parquet"
        self.path.write_bytes(b"PAR1-rows-PAR1")
        self.uri = f"{LOCATION}/data/ts_day=2026-10-18/p00-o0000000000-0000000002-member-0-abc.parquet"
        self.item = SealedFile(
            local_path=str(self.path),
            uri=self.uri,
            partition_text="2026-10-18",
            partition_value={"ts_day": "2026-10-18"},
            record_count=3,
            file_size=self.path.stat().st_size,
        )

    def test_an_upload_that_landed_counts_as_done(self) -> None:
        self.store.lose = self.uri

        uploaded = upload_sealed_file(self.item, store=self.store)

        self.assertTrue(uploaded.uploaded)
        self.assertEqual(self.path.read_bytes(), self.store.read(self.uri))

    def test_another_object_at_the_name_is_refused(self) -> None:
        self.store.write(self.uri, b"something else entirely")

        with self.assertRaises(RuntimeError):
            upload_sealed_file(self.item, store=self.store)


if __name__ == "__main__":
    unittest.main()
