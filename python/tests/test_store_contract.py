"""The object store contract (`lakehouse/store/base.py`) over the memory and file stores.

The conformance suite (`check.py`, design §5.6) runs over both and every check passes; it fails a
store that breaks the contract; focused cases pin each store's own guarantees: create-only and
compare-and-swap refusals, conditional deletes, versions that are never issued twice, the memory
store's failure injection and call log, the file store's temporary files, versions and races between
instances and between processes. The registry (`registry.py`) builds one store per scheme, imports
no cloud SDK until a scheme needs it, and reads `StoreSettings` from the environment.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import types
import unittest
from unittest.mock import patch

from midstream.lakehouse.store import (
    ABSENT,
    Absent,
    DeleteFailure,
    DeleteOutcome,
    FileStore,
    MemoryStore,
    ObjectInfo,
    ObjectNotFoundError,
    ObjectStore,
    PreconditionFailedError,
    StoreError,
    StoreSettings,
    is_retryable_status,
    reset_stores,
    set_store,
    store_for,
    store_for_scheme,
)
from midstream.lakehouse.store import registry
from midstream.lakehouse.store.base import normalize_version
from midstream.lakehouse.store.check import CHECK_NAMES, format_result, main, run_checks
from midstream.lakehouse.store.file import LOCK_FILE_NAME, TEMPORARY_PATTERN

EVERY_CHECK = list(CHECK_NAMES)


def failures(results) -> list[str]:
    return [format_result(result) for result in results if not result.passed]


class ConformanceTests(unittest.TestCase):
    def test_the_memory_store_passes_every_check(self) -> None:
        results = run_checks(MemoryStore(), "gs://contract/check", large_bytes=256 * 1024)
        self.assertEqual(failures(results), [])
        self.assertEqual([result.name for result in results], EVERY_CHECK)

    def test_the_file_store_passes_every_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            results = run_checks(FileStore(), f"file://{directory}/check", large_bytes=256 * 1024)
            self.assertEqual(failures(results), [])
            self.assertEqual([result.name for result in results], EVERY_CHECK)
            # The run deleted everything it wrote; directories keep their lock files.
            left = [name for _, _, names in os.walk(directory) for name in names if name != LOCK_FILE_NAME]
            self.assertEqual(left, [])

    def test_large_object_is_reported_skipped_without_a_size(self) -> None:
        store = MemoryStore()
        results = run_checks(store, "gs://contract/check", writers=2, rounds=2, listing_objects=3)
        self.assertEqual([result.name for result in results], EVERY_CHECK)
        self.assertEqual(failures(results), [])
        large = next(result for result in results if result.name == "large_object")
        self.assertTrue(large.detail.startswith("skipped: no size given"), large.detail)
        self.assertFalse([uri for _, uri in store.calls if "/large" in uri])

    def test_a_run_works_under_a_fresh_prefix_and_leaves_nothing(self) -> None:
        store = MemoryStore()
        store.write("gs://contract/check-sibling", b"keep")
        store.write("gs://contract/check/other-run/x", b"keep")
        run_checks(store, "gs://contract/check", writers=2, rounds=3, listing_objects=5, run_id="0123456789ab")
        written = {uri for operation, uri in store.calls if operation == "put"}
        self.assertTrue(written)
        self.assertTrue(all(uri.startswith("gs://contract/check/0123456789ab/") for uri in written))
        self.assertEqual(store.names(), ["gs://contract/check-sibling", "gs://contract/check/other-run/x"])

    def test_the_suite_fails_a_store_whose_compare_and_swap_does_not_hold(self) -> None:
        results = {result.name: result for result in run_checks(UnconditionalStore(), "gs://contract/check", writers=8, rounds=20)}
        for name in ("compare_and_swap", "missing_under_condition", "concurrent_writers", "conditional_delete"):
            with self.subTest(check=name):
                self.assertFalse(results[name].passed, results[name].detail)
        self.assertIn("two writers won the same round", results["concurrent_writers"].detail)
        self.assertTrue(results["create_only"].passed)
        self.assertTrue(results["cleanup"].passed)

    def test_the_suite_fails_a_store_whose_reads_name_another_version(self) -> None:
        results = {result.name: result for result in run_checks(StaleVersionStore(), "gs://contract/check", writers=2, rounds=2)}
        self.assertFalse(results["read_with_version"].passed)
        self.assertFalse(results["create_only"].passed)

    def test_the_suite_fails_a_store_that_does_not_report_a_missing_object(self) -> None:
        # A missing pointer means "no table": a read must raise ObjectNotFoundError, whatever the
        # backend calls the miss, and must leave nothing behind.
        for store, expected in (
            (NotFoundAsPlainErrorStore(), "get of a missing object raised StoreError (status 404, code NoSuchKey), not ObjectNotFoundError"),
            (MissingReadsAsEmptyStore(), "get of a missing object answered (b'', '0') instead of raising ObjectNotFoundError"),
            (DownloadLeavesFileStore(), "get_to_file of a missing object left a file behind"),
        ):
            with self.subTest(store=type(store).__name__):
                results = run_checks(store, "gs://contract/check", writers=2, rounds=2, listing_objects=3)
                self.assertEqual([result.name for result in results if not result.passed], ["missing_object"])
                self.assertIn(expected, next(result for result in results if result.name == "missing_object").detail)

    def test_the_suite_fails_a_store_whose_racing_creators_can_all_win(self) -> None:
        results = run_checks(RacyCreateOnlyStore(), "gs://contract/check", writers=8, rounds=5, listing_objects=3)
        self.assertEqual([result.name for result in results if not result.passed], ["racing_creators"])
        detail = next(result for result in results if result.name == "racing_creators").detail
        self.assertIn("create-only writes succeeded", detail)

    def test_the_suite_fails_a_store_whose_put_file_drops_its_condition(self) -> None:
        # Without a size too: every run streams files up and down under conditions.
        results = run_checks(PutFileIgnoresConditionStore(), "gs://contract/check", writers=2, rounds=2, listing_objects=3)
        self.assertEqual([result.name for result in results if not result.passed], ["file_transfer"])
        detail = next(result for result in results if result.name == "file_transfer").detail
        self.assertIn("a second create-only put_file of the same key was not refused", detail)
        sized = run_checks(PutFileIgnoresConditionStore(), "gs://contract/check", writers=2, rounds=2, listing_objects=3, large_bytes=64 * 1024)
        self.assertEqual([result.name for result in sized if not result.passed], ["file_transfer", "large_object"])

    def test_the_suite_fails_racing_large_uploads_that_all_win(self) -> None:
        results = run_checks(RacyCreateOnlyFilesStore(), "gs://contract/check", writers=2, rounds=2, listing_objects=3, large_bytes=64 * 1024)
        self.assertEqual([result.name for result in results if not result.passed], ["large_object"])
        detail = next(result for result in results if result.name == "large_object").detail
        self.assertIn("create-only put_file uploads raced for one key", detail)

    def test_the_suite_fails_a_listing_that_gives_versions_another_form(self) -> None:
        results = run_checks(QuotedListingStore(), "gs://contract/check", writers=2, rounds=2, listing_objects=3)
        self.assertEqual([result.name for result in results if not result.passed], ["head_and_list"])
        self.assertIn("the listing reported version", next(result for result in results if result.name == "head_and_list").detail)

    def test_cleanup_does_not_trust_a_listing_that_hides_objects(self) -> None:
        store = BlindStore()
        results = {result.name: result for result in run_checks(store, "gs://contract/check", writers=2, rounds=2, listing_objects=20)}
        self.assertFalse(results["head_and_list"].passed)
        self.assertFalse(results["cleanup"].passed)
        self.assertIn("the listing no longer shows", results["cleanup"].detail)

    def test_cleanup_deletes_what_the_checks_named_even_when_the_listing_fails(self) -> None:
        store = MemoryStore()

        def broken_listing(operation: str, uri: str) -> BaseException | None:
            return StoreError("listing is down", status=503) if operation == "list" else None

        store.fail = broken_listing
        results = {result.name: result for result in run_checks(store, "gs://contract/check", writers=2, rounds=2, listing_objects=10)}
        self.assertFalse(results["cleanup"].passed)
        self.assertIn("listing", results["cleanup"].detail)
        self.assertEqual(store.names(), [])

    def test_a_skipped_check_is_reported_passed_with_its_reason(self) -> None:
        results = run_checks(MemoryStore(), "gs://contract/check", writers=2, rounds=2, listing_objects=3, skip={"conditional_delete": "the emulator ignores it"})
        skipped = next(result for result in results if result.name == "conditional_delete")
        self.assertTrue(skipped.passed)
        self.assertEqual(skipped.detail, "skipped: the emulator ignores it")
        with self.assertRaises(ValueError):
            run_checks(MemoryStore(), "gs://contract/check", skip={"no_such_check": "x"})
        with self.assertRaises(ValueError):
            run_checks(MemoryStore(), "gs://contract/check", skip={"cleanup": "x"})

    def test_a_store_may_declare_it_has_no_conditional_delete(self) -> None:
        store = MemoryStore()
        store.supports_conditional_delete = False
        results = run_checks(store, "gs://contract/check", writers=2, rounds=2, listing_objects=3)
        result = next(result for result in results if result.name == "conditional_delete")
        self.assertTrue(result.passed)
        self.assertTrue(result.detail.startswith("skipped"))
        self.assertNotIn(("delete", "gs://contract/check"), store.calls)

    def test_the_command_prints_a_line_per_check_and_exits_by_the_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            reset_stores()
            self.addCleanup(reset_stores)
            out = _capture(lambda: main([f"file://{directory}/cli", "--writers", "3", "--rounds", "4", "--list-objects", "12", "--large-mb", "0.25"]))
            code, text = out
            self.assertEqual(code, 0, text)
            lines = text.splitlines()
            self.assertTrue(lines[0].startswith("Checking FileStore under file://"))
            self.assertEqual(sum(line.startswith("PASS ") for line in lines), len(EVERY_CHECK))
            self.assertEqual(lines[-1], f"{len(EVERY_CHECK)} of {len(EVERY_CHECK)} checks passed")

            set_store("file", UnconditionalStore())
            code, text = _capture(lambda: main([f"file://{directory}/cli", "--writers", "4", "--rounds", "10", "--list-objects", "3"]))
            self.assertEqual(code, 1)
            self.assertIn("FAIL compare_and_swap", text)
            self.assertIn("failed: ", text.splitlines()[-1])

        code, _ = _capture(lambda: main(["ftp://lake/x"]))
        self.assertEqual(code, 2)


class ContractCases:
    """Focused cases every store passes; a subclass names the store and where its objects live."""

    def make_store(self) -> ObjectStore:
        raise NotImplementedError

    def base(self) -> str:
        raise NotImplementedError

    def setUp(self) -> None:  # noqa: D401 - unittest's hook
        self.store = self.make_store()
        self.uri = self.base() + "/object.json"

    def test_it_is_an_object_store(self) -> None:
        self.assertIsInstance(self.store, ObjectStore)

    def test_create_only_twice(self) -> None:
        version = self.store.put(self.uri, b"first", if_version=ABSENT)
        with self.assertRaises(PreconditionFailedError) as raised:
            self.store.put(self.uri, b"second", if_version=ABSENT)
        self.assertEqual(raised.exception.status, 412)
        self.assertFalse(raised.exception.retryable)
        self.assertEqual(self.store.get(self.uri), (b"first", version))

    def test_compare_and_swap_refuses_a_stale_version(self) -> None:
        first = self.store.put(self.uri, b"one")
        second = self.store.put(self.uri, b"two", if_version=first)
        self.assertNotEqual(first, second)
        with self.assertRaises(PreconditionFailedError):
            self.store.put(self.uri, b"three", if_version=first)
        self.assertEqual(self.store.get(self.uri), (b"two", second))
        third = self.store.put(self.uri, b"three", if_version=second)
        self.assertEqual(self.store.get(self.uri), (b"three", third))

    def test_compare_and_swap_on_a_missing_object_is_refused(self) -> None:
        version = self.store.put(self.base() + "/donor", b"donor")
        with self.assertRaises(PreconditionFailedError):
            self.store.put(self.uri, b"x", if_version=version)
        self.assertIsNone(self.store.head(self.uri))

    def test_an_unconditional_put_replaces(self) -> None:
        self.store.put(self.uri, b"one")
        version = self.store.put(self.uri, b"two")
        self.assertEqual(self.store.get(self.uri), (b"two", version))

    def test_conditional_delete(self) -> None:
        first = self.store.put(self.uri, b"one")
        second = self.store.put(self.uri, b"two", if_version=first)
        with self.assertRaises(PreconditionFailedError):
            self.store.delete(self.uri, if_version=first)
        self.assertEqual(self.store.get(self.uri)[0], b"two")
        self.assertTrue(self.store.delete(self.uri, if_version=second))
        self.assertIsNone(self.store.head(self.uri))
        # A missing object is never an error, with or without a version.
        self.assertFalse(self.store.delete(self.uri, if_version=second))

    def test_delete_missing(self) -> None:
        self.assertFalse(self.store.delete(self.uri))
        self.store.put(self.uri, b"x")
        self.assertTrue(self.store.delete(self.uri))
        self.assertFalse(self.store.delete(self.uri))

    def test_versions_are_never_reused_after_delete_and_recreate(self) -> None:
        seen: set[str] = set()
        for _ in range(25):
            version = self.store.put(self.uri, b"same bytes", if_version=ABSENT)
            self.assertNotIn(version, seen)
            seen.add(version)
            self.assertEqual(self.store.get(self.uri)[1], version)
            self.assertTrue(self.store.delete(self.uri, if_version=version))
        other = self.store.put(self.base() + "/other", b"same bytes")
        self.assertNotIn(other, seen)

    def test_versions_change_on_every_write_of_the_same_bytes(self) -> None:
        versions = [self.store.put(self.uri, b"identical") for _ in range(50)]
        self.assertEqual(len(set(versions)), len(versions))
        self.assertEqual(self.store.head(self.uri).version, versions[-1])

    def test_get_and_head_of_a_missing_object(self) -> None:
        with self.assertRaises(ObjectNotFoundError) as raised:
            self.store.get(self.uri)
        self.assertEqual(raised.exception.status, 404)
        self.assertFalse(raised.exception.retryable)
        self.assertIsNone(self.store.head(self.uri))

    def test_byte_ranges_are_inclusive(self) -> None:
        data = bytes(range(100))
        version = self.store.put(self.uri, data)
        self.assertEqual(self.store.get(self.uri, start=10, end=19), (data[10:20], version))
        self.assertEqual(self.store.get(self.uri, start=90)[0], data[90:])
        self.assertEqual(self.store.get(self.uri, end=4)[0], data[:5])
        self.assertEqual(self.store.get(self.uri, start=95, end=500)[0], data[95:])
        self.assertEqual(self.store.get(self.uri, start=99, end=99)[0], data[99:])
        with self.assertRaises(StoreError) as raised:
            self.store.get(self.uri, start=100)
        self.assertEqual(raised.exception.status, 416)
        for start, end in ((-1, None), (5, 4), (None, -2)):
            with self.subTest(start=start, end=end):
                with self.assertRaises(ValueError):
                    self.store.get(self.uri, start=start, end=end)

    def test_head_describes_the_object(self) -> None:
        version = self.store.put(self.uri, b"x" * 321)
        info = self.store.head(self.uri)
        self.assertEqual((info.uri, info.size, info.version), (self.uri, 321, version))
        self.assertEqual(info.last_modified.utcoffset(), timedelta(0))
        self.assertLess(abs((datetime.now(tz=UTC) - info.last_modified).total_seconds()), 60)

    def test_list_is_a_plain_prefix_in_uri_order(self) -> None:
        base = self.base()
        # No key is both an object and a directory of others: the file store keeps keys as paths.
        names = ["a/2", "a/1", "ab", "a/b/c", "b", "a-x"]
        versions = {f"{base}/{name}": self.store.put(f"{base}/{name}", name.encode()) for name in names}
        listed = list(self.store.list(f"{base}/a"))
        self.assertEqual([item.uri for item in listed], sorted(f"{base}/{name}" for name in ("a-x", "a/1", "a/2", "a/b/c", "ab")))
        for item in listed:
            self.assertEqual(item.version, versions[item.uri])
            self.assertEqual(item.size, len(item.uri[len(base) + 1 :]))
            self.assertIsNotNone(item.last_modified)
        self.assertEqual([item.uri for item in self.store.list(f"{base}/a/")], [f"{base}/a/1", f"{base}/a/2", f"{base}/a/b/c"])
        self.assertEqual(list(self.store.list(f"{base}/zzz")), [])
        self.assertEqual(list(self.store.list(f"{base}/nowhere/")), [])

    def test_delete_many_reports_every_object(self) -> None:
        base = self.base()
        present = [f"{base}/many/{index}" for index in range(5)]
        for uri in present:
            self.store.put(uri, b"x")
        outcome = self.store.delete_many(present + [f"{base}/many/missing"])
        self.assertIsInstance(outcome, DeleteOutcome)
        self.assertEqual(outcome.deleted, tuple(present + [f"{base}/many/missing"]))
        self.assertEqual(outcome.failed, ())
        self.assertEqual(list(self.store.list(f"{base}/many/")), [])
        self.assertEqual(self.store.delete_many([]), DeleteOutcome())

    def test_files_stream_both_ways(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "source")
            Path(source).write_bytes(os.urandom(70_000))
            version = self.store.put_file(self.uri, source, if_version=ABSENT)
            with self.assertRaises(PreconditionFailedError):
                self.store.put_file(self.uri, source, if_version=ABSENT)
            target = os.path.join(directory, "target")
            info = self.store.get_to_file(self.uri, target)
            self.assertEqual(Path(target).read_bytes(), Path(source).read_bytes())
            self.assertEqual((info.size, info.version), (70_000, version))
            with self.assertRaises(FileNotFoundError):
                self.store.put_file(self.base() + "/other", os.path.join(directory, "no-such-file"))

    def test_get_to_file_of_a_missing_object_leaves_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "target")
            Path(target).write_bytes(b"stale")
            with self.assertRaises(ObjectNotFoundError):
                self.store.get_to_file(self.uri, target)
            self.assertFalse(os.path.exists(target))

    def test_arguments_are_checked_before_any_request(self) -> None:
        with self.assertRaises(TypeError):
            self.store.put(self.uri, "text")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            self.store.put(self.uri, b"x", if_version=7)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            self.store.delete(self.uri, if_version=ABSENT)  # type: ignore[arg-type]
        for uri in ("not a uri", "ftp://lake/x"):
            with self.subTest(uri=uri):
                with self.assertRaises(ValueError):
                    self.store.get(uri)
        self.assertIsNone(self.store.head(self.uri))

    def test_racing_writers_have_exactly_one_winner_per_version(self) -> None:
        self.store.put(self.uri, b"0", if_version=ABSENT)
        wins: list[int] = []
        barrier = threading.Barrier(8)

        def writer() -> None:
            barrier.wait()
            won = 0
            for _ in range(30):
                data, version = self.store.get(self.uri)
                time.sleep(0.0002)
                try:
                    self.store.put(self.uri, str(int(data) + 1).encode(), if_version=version)
                except PreconditionFailedError:
                    continue
                won += 1
            wins.append(won)

        threads = [threading.Thread(target=writer) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(int(self.store.get(self.uri)[0]), sum(wins))
        self.assertGreater(sum(wins), 0)


class MemoryStoreTests(ContractCases, unittest.TestCase):
    def make_store(self) -> MemoryStore:
        return MemoryStore()

    def base(self) -> str:
        return "gs://lake/memory"

    def test_versions_come_from_one_counter_for_the_whole_store(self) -> None:
        first = self.store.put("gs://lake/a", b"x")
        second = self.store.put("s3://other/b", b"x")
        third = self.store.write("abfss://c@acct.dfs.core.windows.net/d", b"x")
        self.assertEqual([int(first), int(second), int(third)], [1, 2, 3])
        self.assertEqual(self.store.scheme, "*")
        self.assertEqual(self.store.delete_batch_size, 100)

    def test_failures_are_injected_per_operation_and_every_call_is_recorded(self) -> None:
        store = self.store
        store.write("gs://lake/x", b"x")
        store.fail = lambda operation, uri: StoreError("down", status=503) if operation == "get" else None
        with self.assertRaises(StoreError):
            store.get("gs://lake/x")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(StoreError):
                store.get_to_file("gs://lake/x", os.path.join(directory, "t"))
            self.assertEqual(os.listdir(directory), [])
            source = os.path.join(directory, "s")
            Path(source).write_bytes(b"from a file")
            store.put_file("gs://lake/y", source)
        store.head("gs://lake/x")
        store.put("gs://lake/z", b"z", if_version=ABSENT)
        list(store.list("gs://lake/"))
        store.delete("gs://lake/z")
        store.delete_many(["gs://lake/x", "gs://lake/missing"])
        self.assertEqual(
            store.calls,
            [
                ("get", "gs://lake/x"),
                ("get", "gs://lake/x"),
                ("put", "gs://lake/y"),
                ("head", "gs://lake/x"),
                ("put", "gs://lake/z"),
                ("list", "gs://lake/"),
                ("delete", "gs://lake/z"),
                ("delete", "gs://lake/x"),
                ("delete", "gs://lake/missing"),
            ],
        )
        # The inspection helpers neither fail nor record.
        store.fail = lambda operation, uri: RuntimeError("everything fails")
        calls = len(store.calls)
        self.assertEqual(store.read("gs://lake/y"), b"from a file")
        self.assertIsNone(store.read("gs://lake/x"))
        store.write("gs://lake/w", "text")
        self.assertEqual(store.names("gs://lake/"), ["gs://lake/w", "gs://lake/y"])
        self.assertEqual(sorted(store.objects), ["gs://lake/w", "gs://lake/y"])
        self.assertEqual(store.objects["gs://lake/w"].data, b"text")
        self.assertEqual(len(store.calls), calls)

    def test_delete_many_reports_an_injected_failure_per_object(self) -> None:
        store = self.store
        for name in ("a", "b", "c", "d"):
            store.write(f"gs://lake/{name}", b"x")
        injected = {
            "gs://lake/b": StoreError("busy", status=503),
            "gs://lake/c": StoreError("held", status=403),
            "gs://lake/d": ConnectionResetError("reset"),
        }
        store.fail = lambda operation, uri: injected.get(uri) if operation == "delete" else None
        outcome = store.delete_many(["gs://lake/a", "gs://lake/b", "gs://lake/c", "gs://lake/d"])
        self.assertEqual(outcome.deleted, ("gs://lake/a",))
        self.assertEqual([(failure.uri, failure.status, failure.retryable) for failure in outcome.failed], [
            ("gs://lake/b", 503, True),
            ("gs://lake/c", 403, False),
            ("gs://lake/d", None, True),
        ])
        self.assertIs(outcome.failed[0].error, injected["gs://lake/b"])
        self.assertEqual(store.names(), ["gs://lake/b", "gs://lake/c", "gs://lake/d"])

    def test_a_failure_hook_may_stage_a_race_from_another_thread(self) -> None:
        store = self.store
        version = store.put("gs://lake/pointer", b"1")

        def race(operation: str, uri: str) -> BaseException | None:
            if operation == "put" and uri == "gs://lake/pointer":
                store.fail = None
                rival = threading.Thread(target=lambda: store.put("gs://lake/pointer", b"rival", if_version=version))
                rival.start()
                rival.join(timeout=5)
                self.assertFalse(rival.is_alive(), "the hook ran under the store's lock")
            return None

        store.fail = race
        with self.assertRaises(PreconditionFailedError):
            store.put("gs://lake/pointer", b"mine", if_version=version)
        self.assertEqual(store.read("gs://lake/pointer"), b"rival")

    def test_the_clock_stamps_what_is_written(self) -> None:
        store = self.store
        store.clock = lambda: datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        store.put("gs://lake/old", b"x")
        store.clock = lambda: datetime(2026, 6, 1, 12, 0, 0)  # naive: taken as UTC
        store.write("gs://lake/new", b"y")
        self.assertEqual(store.head("gs://lake/old").last_modified, datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        listed = {item.uri: item.last_modified for item in store.list("gs://lake/")}
        self.assertEqual(listed["gs://lake/new"], datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC))
        self.assertEqual(store.objects["gs://lake/old"].content_type, "application/octet-stream")


class FileStoreTests(ContractCases, unittest.TestCase):
    def make_store(self) -> FileStore:
        return FileStore()

    def base(self) -> str:
        return f"file://{self.directory}/lake"

    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.directory = self._temporary.name
        super().setUp()

    def test_temporary_files_are_never_listed_and_never_left_behind(self) -> None:
        store = self.store
        version = store.put(self.uri, b"one", if_version=ABSENT)
        with self.assertRaises(PreconditionFailedError):
            store.put(self.uri, b"two", if_version=ABSENT)
        with self.assertRaises(PreconditionFailedError):
            store.put(self.uri, b"two", if_version="1-2-3")
        store.put(self.uri, b"three", if_version=version)
        lake = os.path.join(self.directory, "lake")
        self.assertEqual(sorted(os.listdir(lake)), [LOCK_FILE_NAME, "object.json"])
        # A temporary file a crashed writer left: neither listed nor in the way.
        leftover = os.path.join(lake, ".object.json.midstream-tmp-" + "0" * 32)
        Path(leftover).write_bytes(b"half")
        self.assertIsNotNone(TEMPORARY_PATTERN.match(os.path.basename(leftover)))
        self.assertEqual([item.uri for item in store.list(self.base() + "/")], [self.uri])
        self.assertEqual(store.get(self.uri)[0], b"three")

    def test_the_version_is_the_inode_size_and_stamp_of_the_file_read(self) -> None:
        version = self.store.put(self.uri, b"12345")
        status = os.stat(os.path.join(self.directory, "lake", "object.json"))
        self.assertEqual(version, f"{status.st_ino}-{status.st_size}-{status.st_mtime_ns}")
        data, read = self.store.get(self.uri)
        self.assertEqual((data, read), (b"12345", version))

    def test_a_reader_never_sees_a_half_written_file(self) -> None:
        payloads = [bytes([65 + index]) * 200_000 for index in range(4)]
        self.store.put(self.uri, payloads[0])
        stop = threading.Event()
        torn: list[str] = []

        def reader() -> None:
            while not stop.is_set():
                data, version = self.store.get(self.uri)
                if data not in payloads:
                    torn.append(f"{len(data)} bytes, {len(set(data))} distinct")

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            for index in range(60):
                self.store.put(self.uri, payloads[index % len(payloads)])
        finally:
            stop.set()
            thread.join()
        self.assertEqual(torn, [])

    def test_two_instances_race_compare_and_swap_and_create_only(self) -> None:
        uri = self.base() + "/counter"
        FileStore().put(uri, b"0", if_version=ABSENT)
        wins: list[int] = []
        created: list[str] = []
        barrier = threading.Barrier(6)

        def contender(index: int) -> None:
            store = FileStore()  # an instance of its own, as another process would have
            barrier.wait()
            for key in range(10):
                try:
                    store.put(f"{self.base()}/created-{key}", str(index).encode(), if_version=ABSENT)
                except PreconditionFailedError:
                    continue
                created.append(f"created-{key}")
            won = 0
            for _ in range(40):
                data, version = store.get(uri)
                time.sleep(0.0001)
                try:
                    store.put(uri, str(int(data) + 1).encode(), if_version=version)
                except PreconditionFailedError:
                    continue
                won += 1
            wins.append(won)

        threads = [threading.Thread(target=contender, args=(index,)) for index in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(int(FileStore().get(uri)[0]), sum(wins))
        self.assertEqual(sorted(created), sorted(f"created-{key}" for key in range(10)))

    def test_processes_agree_whatever_their_temporary_directories(self) -> None:
        # Each process has a temporary directory of its own, as under another user, sudo or in a
        # container: the lock lives with the objects, so they agree all the same.
        uri = self.base() + "/counter"
        self.store.put(uri, b"0", if_version=ABSENT)
        script = textwrap.dedent(
            """
            import json, random, sys, time
            from midstream.lakehouse.store import FileStore, PreconditionFailedError
            store, base, rounds = FileStore(), sys.argv[1], int(sys.argv[2])
            print("ready", flush=True)
            sys.stdin.readline()
            created = []
            for key in range(15):
                try:
                    store.put(f"{base}/created-{key}", b"x", if_version=__import__("midstream.lakehouse.store").lakehouse.store.ABSENT)
                except PreconditionFailedError:
                    continue
                created.append(key)
            won = 0
            for _ in range(rounds):
                data, version = store.get(f"{base}/counter")
                time.sleep(random.random() / 2000)
                try:
                    store.put(f"{base}/counter", str(int(data) + 1).encode(), if_version=version)
                except PreconditionFailedError:
                    continue
                won += 1
            print(json.dumps({"won": won, "created": created}), flush=True)
            """
        )
        private = [os.path.join(self.directory, f"tmp-{index}") for index in range(3)]
        for directory in private:
            os.makedirs(directory)
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", script, self.base(), "120"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
                env={**os.environ, "TMPDIR": directory},
            )
            for directory in private
        ]
        try:
            for process in processes:
                self.assertEqual(process.stdout.readline().strip(), "ready")
            for process in processes:
                process.stdin.write("go\n")
                process.stdin.flush()
            reports = [json.loads(process.communicate(timeout=60)[0]) for process in processes]
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
        self.assertEqual([process.returncode for process in processes], [0, 0, 0])
        self.assertEqual(int(self.store.get(uri)[0]), sum(report["won"] for report in reports))
        created = sorted(key for report in reports for key in report["created"])
        self.assertEqual(created, list(range(15)))
        self.assertEqual([os.listdir(directory) for directory in private], [[], [], []])

    def test_the_lock_lives_with_the_objects_it_guards(self) -> None:
        # Not in the temporary directory, which processes need not share, and where another user's
        # file or link could sit under the name.
        private = os.path.join(self.directory, "tmp")
        os.makedirs(private)
        nested = self.base() + "/nested/deeper/object"
        with patch.object(tempfile, "tempdir", private):
            version = self.store.put(self.uri, b"x")
            self.store.put(nested, b"y", if_version=ABSENT)
            self.assertTrue(self.store.delete(self.uri, if_version=version))
        self.assertEqual(os.listdir(private), [])
        lake = os.path.join(self.directory, "lake")
        self.assertTrue(os.path.isfile(os.path.join(lake, LOCK_FILE_NAME)))
        self.assertTrue(os.path.isfile(os.path.join(lake, "nested", "deeper", LOCK_FILE_NAME)))
        self.assertEqual([item.uri for item in self.store.list(self.base() + "/")], [nested])
        self.assertEqual([item.uri for item in self.store.list(self.base() + "/.midstream")], [])
        for name in (LOCK_FILE_NAME, ".object.json.midstream-tmp-" + "0" * 32):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.store.put(f"{self.base()}/{name}", b"z")
        # A delete in a directory that does not exist makes no lock there.
        self.assertFalse(self.store.delete(self.base() + "/absent/object"))
        self.assertFalse(os.path.exists(os.path.join(lake, "absent")))

    def test_a_symbolic_link_in_place_of_the_lock_is_never_followed(self) -> None:
        lake = os.path.join(self.directory, "lake")
        os.makedirs(lake)
        victim = os.path.join(self.directory, "victim.txt")
        Path(victim).write_bytes(b"must survive")
        os.symlink(victim, os.path.join(lake, LOCK_FILE_NAME))
        with self.assertRaises(StoreError) as raised:
            self.store.put(self.uri, b"x")
        self.assertIsNone(raised.exception.status)
        self.assertIsInstance(raised.exception.__cause__, OSError)
        self.assertEqual(Path(victim).read_bytes(), b"must survive")
        self.assertIsNone(self.store.head(self.uri))
        self.assertEqual(os.listdir(lake), [LOCK_FILE_NAME])

    def test_paths_two_uris_could_name_are_refused(self) -> None:
        for uri in (
            f"file://{self.directory}/a/../b",
            f"file://{self.directory}/a/./b",
            f"file://{self.directory}//b",
            f"file://{self.directory}/dir/",
            "file://host/tmp/x",
            "gs://lake/x",
        ):
            with self.subTest(uri=uri):
                with self.assertRaises(ValueError):
                    self.store.put(uri, b"x")

    def test_a_directory_is_no_object(self) -> None:
        os.makedirs(os.path.join(self.directory, "lake", "dir"))
        uri = self.base() + "/dir"
        self.assertIsNone(self.store.head(uri))
        with self.assertRaises(ObjectNotFoundError):
            self.store.get(uri)
        self.assertFalse(self.store.delete(uri))
        self.assertTrue(os.path.isdir(os.path.join(self.directory, "lake", "dir")))

    def test_symbolic_links_are_not_followed_by_a_listing(self) -> None:
        outside = os.path.join(self.directory, "outside")
        os.makedirs(outside)
        Path(outside, "secret").write_bytes(b"s")
        self.store.put(self.base() + "/real", b"r")
        os.symlink(outside, os.path.join(self.directory, "lake", "linked-dir"))
        os.symlink(os.path.join(outside, "secret"), os.path.join(self.directory, "lake", "linked-file"))
        self.assertEqual([item.uri for item in self.store.list(self.base() + "/")], [self.base() + "/real"])

    def test_a_failure_of_the_filesystem_is_a_store_error(self) -> None:
        self.store.put(self.base() + "/file", b"x")
        with self.assertRaises(StoreError) as raised:
            self.store.put(self.base() + "/file/child", b"y")
        self.assertIsNone(raised.exception.status)
        self.assertIsInstance(raised.exception.__cause__, OSError)
        self.assertNotIsInstance(raised.exception, (ObjectNotFoundError, PreconditionFailedError))
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            self.skipTest("permissions do not bind root")
        locked = os.path.join(self.directory, "lake", "locked")
        os.makedirs(locked)
        self.store.put(self.base() + "/locked/inside", b"x")
        os.chmod(locked, stat.S_IRUSR | stat.S_IXUSR)
        self.addCleanup(os.chmod, locked, stat.S_IRWXU)
        with self.assertRaises(StoreError) as raised:
            self.store.put(self.base() + "/locked/new", b"y")
        self.assertEqual(raised.exception.code, "EACCES")
        with self.assertRaises(StoreError):
            self.store.delete(self.base() + "/locked/inside")
        self.assertEqual(self.store.get(self.base() + "/locked/inside")[0], b"x")


class BaseTypesTests(unittest.TestCase):
    def test_absent_is_one_object(self) -> None:
        self.assertIs(Absent(), ABSENT)
        self.assertEqual(repr(ABSENT), "ABSENT")
        import pickle

        self.assertIs(pickle.loads(pickle.dumps(ABSENT)), ABSENT)

    def test_retryable_statuses(self) -> None:
        for status in (None, 408, 429, 500, 502, 503, 504, 599):
            with self.subTest(status=status):
                self.assertTrue(is_retryable_status(status))
                self.assertTrue(StoreError("x", status=status).retryable)
                self.assertTrue(DeleteFailure("gs://a/b", status, "x").retryable)
        for status in (400, 401, 403, 404, 409, 412, 416):
            with self.subTest(status=status):
                self.assertFalse(is_retryable_status(status))
                self.assertFalse(DeleteFailure("gs://a/b", status, "x").retryable)

    def test_errors_carry_their_answer(self) -> None:
        error = StoreError("boom", uri="gs://a/b", status=503, code="backendError")
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual((error.uri, error.status, error.code, str(error)), ("gs://a/b", 503, "backendError", "boom"))
        self.assertEqual(ObjectNotFoundError("gone").status, 404)
        self.assertEqual(PreconditionFailedError("moved").status, 412)
        self.assertFalse(PreconditionFailedError("conflict", status=409).retryable)
        self.assertTrue(issubclass(ObjectNotFoundError, StoreError) and issubclass(PreconditionFailedError, StoreError))

    def test_object_info_wants_an_aware_time(self) -> None:
        with self.assertRaises(ValueError):
            ObjectInfo("gs://a/b", 1, "1", datetime(2026, 1, 1))
        self.assertIsNone(ObjectInfo("gs://a/b", 1, "1", None).last_modified)

    def test_versions_leave_a_store_without_quotes(self) -> None:
        self.assertEqual(normalize_version('"0x8DCB1A2B3C4D5E6"'), "0x8DCB1A2B3C4D5E6")
        self.assertEqual(normalize_version("0x8DCB1A2B3C4D5E6"), "0x8DCB1A2B3C4D5E6")
        self.assertEqual(normalize_version(' "abc" '), "abc")
        self.assertEqual(normalize_version('"'), '"')


class RegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_stores()
        self.addCleanup(reset_stores)

    def test_one_store_per_scheme_built_on_first_use(self) -> None:
        store = store_for("file:///tmp/x")
        self.assertIsInstance(store, FileStore)
        self.assertIs(store_for("file:///other"), store)
        self.assertIs(store_for_scheme("file"), store)
        from midstream.lakehouse.store.gcs import GcsStore

        gcs = store_for("gs://lake/x")
        self.assertIsInstance(gcs, GcsStore)
        self.assertIs(store_for("gs://another-bucket/y"), gcs)
        reset_stores()
        self.assertIsNot(store_for("file:///tmp/x"), store)

    def test_an_installed_store_serves_its_scheme_until_removed(self) -> None:
        memory = MemoryStore()
        set_store("gs", memory)
        set_store("abfss", memory)
        self.assertIs(store_for("gs://lake/x"), memory)
        self.assertIs(store_for("abfss://c@acct.dfs.core.windows.net/x"), memory)
        set_store("gs", None)
        self.assertIsNot(store_for("gs://lake/x"), memory)
        with self.assertRaises(ValueError):
            set_store("s3", FileStore())
        with self.assertRaises(ValueError):
            set_store("ftp", memory)
        with self.assertRaises(ValueError):
            store_for("ftp://lake/x")
        with self.assertRaises(ValueError):
            store_for_scheme("gcs")

    def test_s3_and_azure_are_imported_only_when_their_scheme_is_resolved(self) -> None:
        built: list[str] = []

        def module(name: str, class_name: str) -> types.ModuleType:
            fake = types.ModuleType(name)

            class Store(MemoryStore):
                @classmethod
                def from_environment(cls) -> MemoryStore:
                    built.append(class_name)
                    return cls()

            setattr(fake, class_name, Store)
            return fake

        modules = {
            "midstream.lakehouse.store.s3": module("midstream.lakehouse.store.s3", "S3Store"),
            "midstream.lakehouse.store.azure": module("midstream.lakehouse.store.azure", "AzureBlobStore"),
        }
        with patch.dict(sys.modules, modules):
            store_for("file:///tmp/x")
            self.assertEqual(built, [])
            store_for("s3://lake/x")
            self.assertEqual(built, ["S3Store"])
            store_for("abfss://c@acct.dfs.core.windows.net/x")
            store_for("s3://lake/y")
            self.assertEqual(built, ["S3Store", "AzureBlobStore"])

    def test_concurrent_first_uses_build_one_store(self) -> None:
        built: list[FileStore] = []

        def slow_builder() -> FileStore:
            time.sleep(0.05)
            store = FileStore()
            built.append(store)
            return store

        with patch.dict(registry._BUILDERS, {"file": slow_builder}):
            seen: list[ObjectStore] = []
            threads = [threading.Thread(target=lambda: seen.append(store_for_scheme("file"))) for _ in range(12)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(len(built), 1)
        self.assertTrue(all(store is built[0] for store in seen))

    def test_a_builder_that_fails_installs_nothing(self) -> None:
        calls: list[int] = []

        def failing() -> FileStore:
            calls.append(1)
            raise RuntimeError("no credentials")

        with patch.dict(registry._BUILDERS, {"file": failing}):
            for _ in range(2):
                with self.assertRaises(RuntimeError):
                    store_for("file:///tmp/x")
        self.assertEqual(len(calls), 2)

    def test_importing_the_package_imports_no_cloud_sdk(self) -> None:
        probe = (
            "import sys, midstream.lakehouse.store as s; s.store_for('file:///tmp/x'); "
            "print(sorted(m for m in sys.modules if m.split('.')[0] in ('google', 'boto3', 'botocore', 'azure') "
            "or m in ('midstream.lakehouse.store.gcs', 'midstream.lakehouse.store.s3', 'midstream.lakehouse.store.azure')))"
        )
        result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=60, check=True)
        self.assertEqual(result.stdout.strip(), "[]")


class StoreSettingsTests(unittest.TestCase):
    def test_defaults(self) -> None:
        self.assertEqual(StoreSettings.from_environment({}), StoreSettings(None, "us-east-1", False, None))
        blank = {"STORAGE_S3_ENDPOINT": " ", "STORAGE_S3_REGION": "", "STORAGE_S3_PATH_STYLE": "", "STORAGE_AZURE_ENDPOINT": ""}
        self.assertEqual(StoreSettings.from_environment(blank), StoreSettings())

    def test_values(self) -> None:
        settings = StoreSettings.from_environment(
            {
                "STORAGE_S3_ENDPOINT": "http://seaweedfs.storage:8333/",
                "STORAGE_S3_REGION": "eu-west-1",
                "STORAGE_S3_PATH_STYLE": "true",
                "STORAGE_AZURE_ENDPOINT": "http://azurite:10000/",
            }
        )
        self.assertEqual(settings.s3_endpoint, "http://seaweedfs.storage:8333")
        self.assertEqual(settings.s3_region, "eu-west-1")
        self.assertTrue(settings.s3_path_style)
        self.assertEqual(settings.azure_endpoint, "http://azurite:10000")

    def test_path_style_is_a_boolean(self) -> None:
        for value, expected in (("true", True), ("TRUE", True), ("1", True), ("yes", True), ("false", False), ("0", False), ("No", False)):
            with self.subTest(value=value):
                self.assertIs(StoreSettings.from_environment({"STORAGE_S3_PATH_STYLE": value}).s3_path_style, expected)
        with self.assertRaisesRegex(ValueError, "STORAGE_S3_PATH_STYLE"):
            StoreSettings.from_environment({"STORAGE_S3_PATH_STYLE": "maybe"})

    def test_an_endpoint_is_a_url(self) -> None:
        for name in ("STORAGE_S3_ENDPOINT", "STORAGE_AZURE_ENDPOINT"):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, name):
                    StoreSettings.from_environment({name: "seaweedfs:8333"})

    def test_it_reads_the_process_environment_by_default(self) -> None:
        with patch.dict(os.environ, {"STORAGE_S3_REGION": "ap-south-1"}):
            self.assertEqual(StoreSettings.from_environment().s3_region, "ap-south-1")


class UnconditionalStore(MemoryStore):
    """A broken store: a version condition is ignored, so every compare-and-swap writes."""

    def put(self, uri, data, *, if_version=None, content_type="application/octet-stream"):
        return super().put(uri, data, if_version=if_version if if_version is ABSENT else None, content_type=content_type)

    def delete(self, uri, *, if_version=None):
        return super().delete(uri)


class StaleVersionStore(MemoryStore):
    """A broken store: a read names a version other than the one it read."""

    def get(self, uri, *, start=None, end=None):
        data, version = super().get(uri, start=start, end=end)
        return data, str(int(version) + 1000)


class NotFoundAsPlainErrorStore(MemoryStore):
    """A broken store: a read of a missing object raises a plain StoreError - as one that classified
    S3's GetObject `NoSuchKey` by its code - not ObjectNotFoundError."""

    def get(self, uri, *, start=None, end=None):
        try:
            return super().get(uri, start=start, end=end)
        except ObjectNotFoundError as exc:
            raise StoreError(str(exc), uri=uri, status=404, code="NoSuchKey") from None


class MissingReadsAsEmptyStore(MemoryStore):
    """A broken store: a read of a missing object answers no bytes at a made-up version."""

    def get(self, uri, *, start=None, end=None):
        try:
            return super().get(uri, start=start, end=end)
        except ObjectNotFoundError:
            return b"", "0"


class DownloadLeavesFileStore(MemoryStore):
    """A broken store: a download of a missing object leaves an empty file behind."""

    def get_to_file(self, uri, path):
        Path(path).touch()
        if self.read(uri) is None:
            raise ObjectNotFoundError(f"no object at {uri}", uri=uri)
        return super().get_to_file(uri, path)


class PutFileIgnoresConditionStore(MemoryStore):
    """A broken store: put_file drops its condition, as a transfer manager that cannot carry one."""

    def put_file(self, uri, path, *, if_version=None, content_type="application/octet-stream"):
        return super().put_file(uri, path, content_type=content_type)


class RacyCreateOnlyStore(MemoryStore):
    """A broken store: a create-only put checks, then writes unconditionally, so racing creators all win."""

    def put(self, uri, data, *, if_version=None, content_type="application/octet-stream"):
        if if_version is not ABSENT:
            return super().put(uri, data, if_version=if_version, content_type=content_type)
        if self.read(uri) is not None:
            raise PreconditionFailedError(f"{uri} already exists", uri=uri)
        time.sleep(0.001)
        return super().put(uri, data, content_type=content_type)


class RacyCreateOnlyFilesStore(MemoryStore):
    """A broken store: the same race in put_file alone, where large uploads go."""

    def put_file(self, uri, path, *, if_version=None, content_type="application/octet-stream"):
        if if_version is not ABSENT:
            return super().put_file(uri, path, if_version=if_version, content_type=content_type)
        if self.read(uri) is not None:
            raise PreconditionFailedError(f"{uri} already exists", uri=uri)
        time.sleep(0.01)
        return super().put_file(uri, path, content_type=content_type)


class QuotedListingStore(MemoryStore):
    """A broken store: its listing quotes the versions every other answer leaves bare (Azure's ETags,
    the other way around)."""

    def list(self, prefix):
        for info in super().list(prefix):
            yield ObjectInfo(info.uri, info.size, f'"{info.version}"', info.last_modified)


class BlindStore(MemoryStore):
    """A broken store: its listing shows nothing, and delete_many reports deletes it never made."""

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        return iter(())

    def delete_many(self, uris):
        return DeleteOutcome(deleted=tuple(uris))


def _capture(call) -> tuple[int, str]:
    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(io.StringIO()):
        code = call()
    return code, buffer.getvalue()


if __name__ == "__main__":
    unittest.main()
