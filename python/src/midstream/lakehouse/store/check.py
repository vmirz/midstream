"""The conformance suite: whether a live store keeps the contract Midstream's tables rely on.

Before a deployment is pointed at a new store, run

    python -m midstream.lakehouse.store.check s3://my-lake/_check

from a laptop or inside any Midstream pod (it uses the same environment and credentials). It checks
create-only writes and many creators racing for one key, reads of a missing object, compare-and-swap
and its refusals, many writers racing on one object with exactly one winner per round, byte ranges,
sizes, versions and times, listings across pages, streamed uploads and downloads under the same
conditions, and deletes; the unit tests run it over the memory and file stores, and the live tests
over emulators.

Everything happens under `<base uri>/<12 hex run id>/`, and the run deletes everything it wrote: the
last check, `cleanup`, passes only when nothing is left there.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
import random
import sys
import tempfile
import threading
import time
from typing import Final
import uuid

from midstream.lakehouse.store.base import ABSENT, ObjectNotFoundError, ObjectStore, PreconditionFailedError, StoreError
from midstream.lakehouse.store.uris import join

#: Every check, in the order they run; `large_object` runs only with a size, and is reported skipped
#: without one.
CHECK_NAMES: Final = (
    "create_only",
    "racing_creators",
    "read_with_version",
    "missing_object",
    "compare_and_swap",
    "missing_under_condition",
    "concurrent_writers",
    "pointer_never_repeats",
    "range_read",
    "head_and_list",
    "file_transfer",
    "delete",
    "delete_many",
    "conditional_delete",
    "large_object",
    "cleanup",
)
#: How far a store's object times may be from the local clock.
MAX_CLOCK_SKEW: Final = timedelta(minutes=15)
#: How many times the pointer check advances a table pointer.
POINTER_ADVANCES: Final = 1_000
#: Threads writing the listing check's objects.
LISTING_WRITERS: Final = 16
#: The longest pause a racing writer takes between its read and its write, so that writers in one
#: process interleave there as they do across a network.
WRITER_PAUSE_SECONDS: Final = 0.001
#: The size of the files `file_transfer` streams up and down.
FILE_TRANSFER_BYTES: Final = 256 * 1024
#: How many create-only uploads of large files `large_object` races for one key.
LARGE_RACERS: Final = 3
_MIB: Final = 1024 * 1024


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One check's outcome: what it found, and how long it took."""

    name: str
    passed: bool
    detail: str
    seconds: float


class CheckFailure(AssertionError):
    """A store answered against the contract."""


def run_root(base_uri: str, run_id: str) -> str:
    """The prefix one run works under: `<base uri>/<run id>/`."""
    return join(base_uri, run_id) + "/"


def run_checks(
    store: ObjectStore,
    base_uri: str,
    *,
    writers: int = 16,
    rounds: int = 25,
    listing_objects: int = 1050,
    large_bytes: int = 0,
    skip: Mapping[str, str] | None = None,
    run_id: str | None = None,
) -> list[CheckResult]:
    """Run every check against `store` under a fresh prefix of `base_uri`; one result per check.

    `writers` threads race `rounds` compare-and-swaps each on one object, and race to create each of
    `rounds` new keys; `listing_objects` objects are listed back (more than a page: 1,000 on GCS and
    S3); `large_bytes` > 0 adds a streamed upload and download of a file that size and a race of
    create-only uploads of such files, and is reported skipped without a size. `skip` maps a check's
    name to the reason it cannot run on this store - an emulator's known deviation - and reports it
    passed as skipped. The run deletes what it wrote whatever happens.
    """
    skipped = dict(skip or {})
    unknown = sorted(set(skipped) - set(CHECK_NAMES))
    if unknown:
        raise ValueError(f"unknown checks {unknown}: the checks are {', '.join(CHECK_NAMES)}")
    if "cleanup" in skipped:
        raise ValueError("cleanup cannot be skipped: it removes what the run wrote")
    if writers < 1 or rounds < 1 or listing_objects < 0 or large_bytes < 0:
        raise ValueError("writers and rounds must be at least 1, listing_objects and large_bytes at least 0")
    checks = _Checks(
        store,
        run_root(base_uri, run_id or uuid.uuid4().hex[:12]),
        writers=writers,
        rounds=rounds,
        listing_objects=listing_objects,
        large_bytes=large_bytes,
    )
    results: list[CheckResult] = []
    try:
        for name in CHECK_NAMES:
            if name == "cleanup":
                continue
            if name == "large_object" and large_bytes <= 0 and name not in skipped:
                skipped[name] = "no size given (large_bytes; --large-mb on the command line)"
            if name in skipped:
                results.append(CheckResult(name, True, f"skipped: {skipped[name]}", 0.0))
                continue
            results.append(_run(name, getattr(checks, name)))
    finally:
        results.append(_run("cleanup", checks.cleanup))
    return results


def format_result(result: CheckResult) -> str:
    """`PASS name  seconds  detail`, the line the command prints for a check."""
    return f"{'PASS' if result.passed else 'FAIL'} {result.name:<24} {result.seconds:7.2f}s  {result.detail}"


def main(argv: Sequence[str] | None = None) -> int:
    """The command: run every check against the store for a URI; 0 only when all pass."""
    parser = argparse.ArgumentParser(
        prog="python -m midstream.lakehouse.store.check",
        description="Check that an object store keeps the contract Midstream relies on, under a fresh "
        "prefix of URI that the run deletes again.",
    )
    parser.add_argument("uri", help="where the check may write, e.g. gs://my-lake/_check or s3://my-lake/_check")
    parser.add_argument("--writers", type=int, default=16, help="threads racing on one object (default 16)")
    parser.add_argument("--rounds", type=int, default=25, help="compare-and-swaps each writer attempts (default 25)")
    parser.add_argument("--list-objects", type=int, default=1050, help="objects written and listed back (default 1050)")
    parser.add_argument("--large-mb", type=float, default=0.0, help="also stream files of this many MiB up and down, and race create-only uploads of them")
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        metavar="CHECK",
        help="a check not to run, such as one an emulator is known to fail (repeatable)",
    )
    args = parser.parse_args(argv)
    if args.writers < 1 or args.rounds < 1 or args.list_objects < 0 or args.large_mb < 0:
        parser.error("--writers and --rounds must be at least 1, --list-objects and --large-mb at least 0")
    for name in args.skip:
        if name not in CHECK_NAMES or name == "cleanup":
            parser.error(f"cannot skip {name!r}: choose from {', '.join(name for name in CHECK_NAMES if name != 'cleanup')}")

    from midstream.lakehouse.store.registry import store_for

    try:
        store = store_for(args.uri)
    except (ValueError, ImportError) as exc:
        print(f"Cannot check {args.uri}: {exc}", file=sys.stderr)
        return 2
    run_id = uuid.uuid4().hex[:12]
    print(f"Checking {type(store).__name__} under {run_root(args.uri, run_id)}", flush=True)
    results = run_checks(
        store,
        args.uri,
        writers=args.writers,
        rounds=args.rounds,
        listing_objects=args.list_objects,
        large_bytes=int(args.large_mb * _MIB),
        skip={name: "skipped on request" for name in args.skip},
        run_id=run_id,
    )
    for result in results:
        print(format_result(result))
    failed = [result.name for result in results if not result.passed]
    summary = f"{len(results) - len(failed)} of {len(results)} checks passed"
    print(summary if not failed else f"{summary}; failed: {', '.join(failed)}")
    return 0 if not failed else 1


def _run(name: str, check: Callable[[], str]) -> CheckResult:
    started = time.monotonic()
    try:
        detail = check()
    except Exception as exc:  # noqa: BLE001 - a failure of the store is the finding
        detail = str(exc) if isinstance(exc, CheckFailure) else f"{type(exc).__name__}: {exc}"
        return CheckResult(name, False, _shorten(detail), time.monotonic() - started)
    return CheckResult(name, True, detail, time.monotonic() - started)


def _shorten(text: str, limit: int = 600) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _expect(condition: bool, failure: str) -> None:
    if not condition:
        raise CheckFailure(failure)


def _expect_refused(attempt: Callable[[], object], failure: str) -> None:
    """`attempt` must raise `PreconditionFailedError`."""
    try:
        attempt()
    except PreconditionFailedError:
        return
    raise CheckFailure(failure)


def _expect_missing(attempt: Callable[[], object], what: str) -> None:
    """`attempt` must raise `ObjectNotFoundError`: not a plain `StoreError`, and not an answer."""
    try:
        result = attempt()
    except ObjectNotFoundError:
        return
    except StoreError as exc:
        raise CheckFailure(
            f"{what} raised {type(exc).__name__} (status {exc.status}, code {exc.code}), not ObjectNotFoundError"
        ) from exc
    shown = repr(result)
    raise CheckFailure(f"{what} answered {shown[:80]}{'...' if len(shown) > 80 else ''} instead of raising ObjectNotFoundError")


def _attempt_create(create: Callable[[], object]) -> str:
    """Make a create-only write: `won`, `refused`, or `uncertain` when it failed in transit and may or
    may not have landed; any other failure is raised."""
    try:
        create()
    except PreconditionFailedError:
        return "refused"
    except StoreError as exc:
        if not exc.retryable:
            raise
        return "uncertain"
    return "won"


def _creation_problem(attempts: Sequence[tuple[str, object]], stored: object | None) -> str | None:
    """What is wrong with the create-only writes that raced for one new key; None when nothing is.

    `attempts` pairs each write's outcome (`_attempt_create`) with what it wrote, and `stored` is what
    the key holds afterwards, None when nothing. Exactly one write may win, and the key then holds
    what it wrote; with no winner, only writes that failed in transit may have landed.
    """
    winners = [content for outcome, content in attempts if outcome == "won"]
    uncertain = [content for outcome, content in attempts if outcome == "uncertain"]
    if len(winners) > 1:
        return f"{len(winners)} of {len(attempts)} create-only writes succeeded"
    if winners:
        return None if stored == winners[0] else "the key does not hold what the one successful write wrote"
    if not uncertain:
        return f"all {len(attempts)} create-only writes were refused, though the key was new"
    if stored is not None and stored not in uncertain:
        return "the key holds what no write that may have landed wrote"
    return None


class _Checks:
    """The checks, each a method returning its detail or raising; all under `root`."""

    def __init__(self, store: ObjectStore, root: str, *, writers: int, rounds: int, listing_objects: int, large_bytes: int) -> None:
        self.store = store
        self.root = root
        self.writers = writers
        self.rounds = rounds
        self.listing_objects = listing_objects
        self.large_bytes = large_bytes
        #: Every object URI a check named, so cleanup deletes them even if the listing misses some.
        self.named: set[str] = set()
        #: The listing check's objects; cleanup probes the first and the last.
        self.listed_objects: list[str] = []
        self._named_lock = threading.Lock()

    def key(self, name: str) -> str:
        uri = self.root + name
        if not uri.endswith("/"):
            with self._named_lock:
                self.named.add(uri)
        return uri

    def create_only(self) -> str:
        uri = self.key("create-only")
        first = self.store.put(uri, b"first", if_version=ABSENT)
        _expect(isinstance(first, str) and bool(first), f"a create-only put returned {first!r}, not a version")
        _expect_refused(
            lambda: self.store.put(uri, b"second", if_version=ABSENT),
            "a second create-only put of the same key was not refused",
        )
        data, version = self.store.get(uri)
        _expect(data == b"first", f"the refused put replaced the first content: {data[:40]!r}")
        _expect(version == first, f"get read version {version!r} of an object put returned {first!r} for")
        return f"created at version {first}; the second create was refused"

    def racing_creators(self) -> str:
        """`writers` threads at once create each of `rounds` new keys: per key exactly one wins - for a
        table's pointer, two winners would be two tables in one place - and the key holds its bytes."""
        keys = [self.key(f"racing-creators/key-{index:04d}") for index in range(self.rounds)]
        barrier = threading.Barrier(self.writers)
        lock = threading.Lock()
        attempts: list[list[tuple[str, bytes]]] = [[] for _ in keys]

        def creator(index: int) -> None:
            try:
                for number, uri in enumerate(keys):
                    payload = f"creator {index} of key {number}".encode("ascii")
                    try:
                        barrier.wait(timeout=60)
                    except threading.BrokenBarrierError:
                        pass
                    outcome = _attempt_create(lambda: self.store.put(uri, payload, if_version=ABSENT))
                    with lock:
                        attempts[number].append((outcome, payload))
            except BaseException:
                barrier.abort()  # the others need not wait for a creator that stopped
                raise

        with ThreadPoolExecutor(max_workers=self.writers, thread_name_prefix="check-creator") as pool:
            for future in [pool.submit(creator, index) for index in range(self.writers)]:
                future.result()
        problems: list[str] = []
        for number, uri in enumerate(keys):
            try:
                stored: bytes | None = self.store.get(uri)[0]
            except ObjectNotFoundError:
                stored = None
            problem = _creation_problem(attempts[number], stored)
            if problem is not None:
                problems.append(f"key {number + 1}: {problem}")
        _expect(not problems, f"{len(problems)} of {len(keys)} keys raced wrongly, e.g. " + "; ".join(problems[:3]))
        tally = Counter(outcome for round_attempts in attempts for outcome, _ in round_attempts)
        detail = f"{len(keys)} new keys each won by one of {self.writers} racing creators; {tally['refused']} creates were refused"
        if tally["uncertain"]:
            detail += f", {tally['uncertain']} failed in transit"
        return detail

    def read_with_version(self) -> str:
        uri = self.key("read-with-version")
        written = self.store.put(uri, b"one")
        data, version = self.store.get(uri)
        _expect(data == b"one", f"get returned {data[:40]!r}, not what was written")
        _expect(version == written, f"get read version {version!r}, put returned {written!r}")
        replaced = self.store.put(uri, b"two", if_version=version)
        _expect(replaced != version, f"a compare-and-swap returned the version it replaced ({version!r})")
        return f"read version {version}, then replaced it with {replaced}"

    def missing_object(self) -> str:
        """Reads of a key never written raise `ObjectNotFoundError`, which "no table" rests on (a
        missing pointer): not-found is the HTTP status, whatever the backend calls it - S3 answers a
        missing key's GetObject with `NoSuchKey` and its HeadObject with `404`."""
        uri = self.key("missing-object")
        _expect_missing(lambda: self.store.get(uri), "get of a missing object")
        _expect_missing(lambda: self.store.get(uri, start=0, end=9), "a ranged get of a missing object")
        with tempfile.TemporaryDirectory(prefix="midstream-check-") as directory:
            target = os.path.join(directory, "target")
            _expect_missing(lambda: self.store.get_to_file(uri, target), "get_to_file of a missing object")
            _expect(not os.path.lexists(target), "get_to_file of a missing object left a file behind")
        _expect(self.store.head(uri) is None, "head described an object never written")
        return "get, a ranged get and get_to_file raised ObjectNotFoundError; head answered None"

    def compare_and_swap(self) -> str:
        uri = self.key("compare-and-swap")
        first = self.store.put(uri, b"v1", if_version=ABSENT)
        second = self.store.put(uri, b"v2", if_version=first)
        _expect(second != first, f"a compare-and-swap returned the version it replaced ({first!r})")
        _expect_refused(
            lambda: self.store.put(uri, b"v3", if_version=first),
            "a compare-and-swap with a stale version was not refused",
        )
        data, version = self.store.get(uri)
        _expect(data == b"v2", f"the object holds {data[:40]!r}, not the second write")
        _expect(version == second, f"get read version {version!r}, the second write returned {second!r}")
        return f"{first} -> {second}; the stale write was refused"

    def missing_under_condition(self) -> str:
        donor = self.key("missing-under-condition-donor")
        version = self.store.put(donor, b"donor")
        uri = self.key("missing-under-condition")
        _expect_refused(
            lambda: self.store.put(uri, b"x", if_version=version),
            "a compare-and-swap on a missing key was not refused",
        )
        _expect(self.store.head(uri) is None, "a refused compare-and-swap created the object")
        return "refused, and nothing was created"

    def concurrent_writers(self) -> str:
        uri = self.key("counter")
        self.store.put(uri, b"0", if_version=ABSENT)
        barrier = threading.Barrier(self.writers)
        lock = threading.Lock()
        tally = {"won": 0, "conflicts": 0, "uncertain": 0, "read_errors": 0}
        problems: list[str] = []

        def count(name: str) -> None:
            with lock:
                tally[name] += 1

        def writer(index: int) -> None:
            last = -1
            try:
                barrier.wait(timeout=60)
            except threading.BrokenBarrierError:
                pass
            for _ in range(self.rounds):
                try:
                    data, version = self.store.get(uri)
                except StoreError as exc:
                    if not exc.retryable:
                        raise
                    count("read_errors")
                    continue
                value = int(data)
                if value < last:
                    with lock:
                        problems.append(f"writer {index} read {value} after reading {last}")
                    return
                last = value
                time.sleep(random.random() * WRITER_PAUSE_SECONDS)  # let the others race this write
                try:
                    self.store.put(uri, str(value + 1).encode("ascii"), if_version=version)
                except PreconditionFailedError:
                    count("conflicts")
                except StoreError as exc:
                    if not exc.retryable:
                        raise
                    # Failed in transit: the write may or may not have landed.
                    count("uncertain")
                else:
                    count("won")

        with ThreadPoolExecutor(max_workers=self.writers, thread_name_prefix="check-writer") as pool:
            for future in [pool.submit(writer, index) for index in range(self.writers)]:
                future.result()
        _expect(not problems, "; ".join(problems[:3]))
        final = int(self.store.get(uri)[0])
        won, uncertain = tally["won"], tally["uncertain"]
        _expect(won >= 1, f"every one of {self.writers * self.rounds} compare-and-swaps was refused")
        _expect(
            won <= final <= won + uncertain,
            f"the counter holds {final} after {won} compare-and-swaps succeeded"
            + (f" and {uncertain} failed in transit" if uncertain else "")
            + ": two writers won the same round, or a win was lost",
        )
        detail = f"{won} won and {tally['conflicts']} were refused, {self.writers} writers x {self.rounds} rounds"
        if uncertain or tally["read_errors"]:
            detail += f"; {uncertain} writes and {tally['read_errors']} reads failed in transit"
        return detail

    def pointer_never_repeats(self) -> str:
        from midstream.lakehouse.pointer import TablePointer, utc_now

        location = self.key("table")
        updated_at = utc_now()
        pointer = TablePointer(
            table_uuid=str(uuid.uuid4()),
            identifier=("check", "pointer"),
            materialization_id="mat-check",
            location=location,
            metadata_location=f"{location}/metadata/a.metadata.json",
            sequence=0,
            updated_at=updated_at,
            updated_by="check",
        )
        seen = {_serialize_pointer(pointer)}
        for index in range(POINTER_ADVANCES):
            # The same two metadata files and the same time throughout: only the sequence may differ.
            metadata = f"{location}/metadata/{'b' if index % 2 == 0 else 'a'}.metadata.json"
            advanced = pointer.advanced(metadata, updated_by="check", updated_at=updated_at)
            _expect(advanced.sequence == pointer.sequence + 1, f"advance {index + 1} moved the sequence to {advanced.sequence}")
            text = _serialize_pointer(advanced)
            _expect(text not in seen, f"advance {index + 1} serialized to bytes an earlier pointer had")
            seen.add(text)
            pointer = advanced
        return f"{POINTER_ADVANCES:,} advances, every serialization distinct"

    def range_read(self) -> str:
        uri = self.key("range")
        data = bytes(range(256)) * 4
        written = self.store.put(uri, data)
        cases = (
            ((10, 19), data[10:20]),
            ((0, 0), data[:1]),
            ((1000, None), data[1000:]),
            ((None, 9), data[:10]),
            ((1020, 5000), data[1020:]),
            ((0, None), data),
        )
        for (start, end), expected in cases:
            got, version = self.store.get(uri, start=start, end=end)
            shown = f"{'' if start is None else start}-{'' if end is None else end}"
            _expect(got == expected, f"bytes {shown} returned {len(got)} bytes, not the {len(expected)} expected")
            _expect(version == written, f"bytes {shown} were read at version {version!r}, not {written!r}")
        return f"{len(cases)} ranges of a {len(data):,}-byte object"

    def head_and_list(self) -> str:
        uri = self.key("head")
        payload = b"h" * 1234
        version = self.store.put(uri, payload)
        info = self.store.head(uri)
        _expect(info is not None, "head found no object just written")
        assert info is not None
        _expect(info.uri == uri, f"head described {info.uri!r}, not {uri!r}")
        _expect(info.size == len(payload), f"head reported {info.size} bytes, not {len(payload)}")
        _expect(info.version == version, f"head reported version {info.version!r}, put returned {version!r}")
        skew = self._skew(info.last_modified, "head")

        listing = self.key("listing/")
        expected = [f"{listing}object-{index:05d}" for index in range(self.listing_objects)]
        self.listed_objects = expected
        # Shares the text `listing` but not the prefix `listing/`: it must not be listed.
        self.store.put(self.key("listing-sibling"), b"s")
        written: dict[str, str] = {}
        if expected:
            with ThreadPoolExecutor(max_workers=LISTING_WRITERS, thread_name_prefix="check-lister") as pool:
                written = dict(zip(expected, pool.map(lambda name: self.store.put(name, name.encode("utf-8")), expected)))
        listed = list(self.store.list(listing))
        uris = [item.uri for item in listed]
        _expect(len(uris) == len(set(uris)), f"the listing returned {len(uris) - len(set(uris))} objects more than once")
        missing, extra = sorted(set(expected) - set(uris)), sorted(set(uris) - set(expected))
        _expect(not missing, f"the listing left out {len(missing)} of {len(expected)} objects, e.g. {missing[:1]}")
        _expect(not extra, f"the listing returned {len(extra)} objects outside the prefix, e.g. {extra[:1]}")
        for item in listed:
            _expect(item.size == len(item.uri.encode("utf-8")), f"the listing reported {item.size} bytes for {item.uri}")
            _expect(bool(item.version), f"the listing reported no version for {item.uri}")
            # In the very form every other answer gives it (Azure quotes ETags but not in listings):
            # versions are compared as they are.
            _expect(
                item.version == written[item.uri],
                f"the listing reported version {item.version!r} for {item.uri}, put returned {written[item.uri]!r}",
            )
            self._skew(item.last_modified, f"the listing of {item.uri}")
        order = "" if uris == sorted(uris) else ", not in URI order"
        return f"head: {info.size} bytes, {skew:.1f}s from the local clock; listed {len(uris):,} objects once each{order}"

    def file_transfer(self) -> str:
        """`put_file` and `get_to_file` - every upload's and compaction's calls - keep `put`'s
        conditions: a create, a refused second create, a compare-and-swap, a refused stale version;
        each download whole, at the version written, and as `get` reads it."""
        uri = self.key("file-transfer")
        size = FILE_TRANSFER_BYTES
        with tempfile.TemporaryDirectory(prefix="midstream-check-") as directory:
            sources = [os.path.join(directory, f"source-{index}") for index in range(3)]
            digests = [_random_file(source, size) for source in sources]
            target = os.path.join(directory, "target")
            first = self.store.put_file(uri, sources[0], if_version=ABSENT)
            _expect(isinstance(first, str) and bool(first), f"a create-only put_file returned {first!r}, not a version")
            _expect_refused(
                lambda: self.store.put_file(uri, sources[1], if_version=ABSENT),
                "a second create-only put_file of the same key was not refused",
            )
            self._expect_download(uri, target, size, first, digests[0], "after the refused create")
            second = self.store.put_file(uri, sources[1], if_version=first)
            _expect(second != first, f"a compare-and-swap put_file returned the version it replaced ({first!r})")
            _expect_refused(
                lambda: self.store.put_file(uri, sources[2], if_version=first),
                "a put_file with a stale version was not refused",
            )
            self._expect_download(uri, target, size, second, digests[1], "after the refused stale write")
            data, version = self.store.get(uri)
            _expect(
                version == second and hashlib.sha256(data).hexdigest() == digests[1],
                f"get read {len(data)} bytes at version {version!r}, not the file put_file wrote at {second!r}",
            )
        return f"{size // 1024} KiB files: a create and a compare-and-swap; the second create and the stale write were refused"

    def _expect_download(self, uri: str, target: str, size: int, version: str, digest: str, when: str) -> None:
        """`get_to_file` writes the whole object at `version` to `target`, replacing what is there."""
        info = self.store.get_to_file(uri, target)
        _expect(info.version == version, f"{when}, get_to_file read version {info.version!r}, not {version!r}")
        _expect(info.size == size, f"{when}, get_to_file reported {info.size} bytes, not {size}")
        _expect(_file_digest(target) == digest, f"{when}, the downloaded file is not the one written at {version!r}")

    def delete(self) -> str:
        missing = self.store.delete(self.key("delete-missing"))
        _expect(isinstance(missing, bool), f"deleting a missing object answered {missing!r}, not a boolean")
        uri = self.key("delete")
        self.store.put(uri, b"x")
        _expect(self.store.delete(uri) is True, "deleting an existing object did not answer True")
        _expect(self.store.head(uri) is None, "the object is still there after its delete")
        return f"a missing object answers {missing}"

    def delete_many(self) -> str:
        present = [self.key(f"delete-many/object-{index}") for index in range(5)]
        absent = [self.key(f"delete-many/missing-{index}") for index in range(3)]
        for uri in present:
            self.store.put(uri, b"x")
        outcome = self.store.delete_many(present + absent)
        if outcome.failed:
            first = outcome.failed[0]
            raise CheckFailure(f"{len(outcome.failed)} deletes failed, e.g. {first.uri}: {first.status} {first.message}")
        _expect(sorted(outcome.deleted) == sorted(present + absent), f"the outcome reported {len(outcome.deleted)} of {len(present + absent)} objects")
        left = [item.uri for item in self.store.list(self.key("delete-many/"))]
        _expect(not left, f"{len(left)} deleted objects are still listed, e.g. {left[0] if left else ''}")
        return f"{len(present)} existing and {len(absent)} missing objects all reported deleted"

    def conditional_delete(self) -> str:
        if getattr(self.store, "supports_conditional_delete", True) is False:
            return "skipped: the store does not support conditional deletes"
        uri = self.key("conditional-delete")
        first = self.store.put(uri, b"one", if_version=ABSENT)
        second = self.store.put(uri, b"two", if_version=first)
        _expect_refused(lambda: self.store.delete(uri, if_version=first), "a delete with a stale version was not refused")
        _expect(self.store.head(uri) is not None, "a refused conditional delete removed the object")
        _expect(self.store.delete(uri, if_version=second) is True, "a delete with the current version did not answer True")
        _expect(self.store.head(uri) is None, "the object is still there after its conditional delete")
        return "a stale version was refused, the current one deleted"

    def large_object(self) -> str:
        """A large file up and down, timed; a second create-only upload refused; and create-only
        uploads racing for one key: chunked uploads are where a store can mix two writers' chunks."""
        uri = self.key("large")
        size = self.large_bytes
        with tempfile.TemporaryDirectory(prefix="midstream-check-") as directory:
            sources = [os.path.join(directory, f"source-{index}") for index in range(LARGE_RACERS)]
            digests = [_random_file(source, size) for source in sources]
            started = time.monotonic()
            version = self.store.put_file(uri, sources[0], if_version=ABSENT)
            upload = time.monotonic() - started
            described = self.store.head(uri)
            _expect(described is not None and described.size == size, f"head reported {None if described is None else described.size} bytes, not {size}")
            target = os.path.join(directory, "target")
            started = time.monotonic()
            info = self.store.get_to_file(uri, target)
            download = time.monotonic() - started
            _expect(info.size == size, f"get_to_file reported {info.size} bytes, not {size}")
            _expect(info.version == version, f"get_to_file read version {info.version!r}, put_file returned {version!r}")
            _expect(_file_digest(target) == digests[0], "the downloaded file differs from the uploaded one")
            _expect_refused(
                lambda: self.store.put_file(uri, sources[1], if_version=ABSENT),
                "a second create-only put_file of the large object was not refused",
            )
            after = self.store.head(uri)
            _expect(after is not None and after.version == version, "a refused create-only put_file replaced the large object")
            race = self._race_large_creators(sources, digests, target)
        mib = size / _MIB
        return (
            f"{mib:.1f} MiB up in {upload:.2f}s ({mib / max(upload, 1e-9):.1f} MiB/s), down in {download:.2f}s "
            f"({mib / max(download, 1e-9):.1f} MiB/s); a second create was refused; {race}"
        )

    def _race_large_creators(self, sources: list[str], digests: list[str], target: str) -> str:
        """Upload every source create-only to one new key at once: one upload wins, and the key holds
        its file whole - not another's, and no mix of several uploads' chunks."""
        uri = self.key("large-race")
        barrier = threading.Barrier(len(sources))

        def create(source: str) -> str:
            try:
                barrier.wait(timeout=60)
            except threading.BrokenBarrierError:
                pass
            return _attempt_create(lambda: self.store.put_file(uri, source, if_version=ABSENT))

        with ThreadPoolExecutor(max_workers=len(sources), thread_name_prefix="check-large") as pool:
            outcomes = list(pool.map(create, sources))
        try:
            self.store.get_to_file(uri, target)
        except ObjectNotFoundError:
            stored = None
        else:
            stored = _file_digest(target)
        problem = _creation_problem(list(zip(outcomes, digests)), stored)
        _expect(problem is None, f"{len(sources)} create-only put_file uploads raced for one key: {problem}")
        return f"of {len(sources)} racing create-only uploads one won, its file whole"

    def cleanup(self) -> str:
        """Delete what the listing finds under the run's prefix and every object a check named, then
        confirm both by a listing and by asking for named objects one by one: a listing that misses
        objects can neither leave them behind nor pass the check."""
        try:
            found = [item.uri for item in self.store.list(self.root)]
        except Exception as exc:  # noqa: BLE001 - still delete what the checks named
            found, listing_error = [], f"{type(exc).__name__}: {exc}"
        else:
            listing_error = None
        targets = sorted(set(found) | self.named | set(self.listed_objects))
        batch = max(1, int(getattr(self.store, "delete_batch_size", 100)))
        one_by_one = 0
        errors: list[str] = []
        for begin in range(0, len(targets), batch):
            chunk = targets[begin : begin + batch]
            try:
                failed = [failure.uri for failure in self.store.delete_many(chunk).failed]
            except Exception:  # noqa: BLE001 - fall back to single deletes below
                failed = chunk
            for uri in failed:
                one_by_one += 1
                try:
                    self.store.delete(uri)
                except Exception as exc:  # noqa: BLE001 - keep deleting the rest
                    errors.append(f"{uri}: {type(exc).__name__}: {exc}")
        _expect(listing_error is None, f"the listing of {self.root} failed ({listing_error}); deleted what the checks named")
        _expect(not errors, f"{len(errors)} objects could not be deleted, e.g. {errors[0] if errors else ''}")
        remaining = [item.uri for item in self.store.list(self.root)]
        _expect(not remaining, f"{len(remaining)} objects are left under {self.root}, e.g. {remaining[:1]}")
        probes = sorted(self.named) + self.listed_objects[:1] + self.listed_objects[-1:]
        survivors = [uri for uri in probes if self.store.head(uri) is not None]
        _expect(not survivors, f"{len(survivors)} objects the listing no longer shows are still there, e.g. {survivors[0] if survivors else ''}")
        detail = f"deleted {len(found):,} objects; nothing is left under {self.root}"
        if one_by_one:
            detail += f" ({one_by_one:,} one at a time, after delete_many failed for them)"
        return detail

    def _skew(self, value: datetime | None, what: str) -> float:
        """Seconds between `value` and the local clock; fails when missing, naive or too far off."""
        _expect(value is not None, f"{what} reported no last-modified time: the orphan scan could never age the object")
        assert value is not None
        _expect(value.tzinfo is not None, f"{what} reported a last-modified time without a timezone")
        skew = abs((datetime.now(tz=UTC) - value).total_seconds())
        _expect(skew <= MAX_CLOCK_SKEW.total_seconds(), f"{what} reported a time {skew:.0f}s from the local clock")
        return skew


def _serialize_pointer(pointer: object) -> str:
    """A pointer's bytes as `lakehouse/pointer.py::write_pointer` writes them."""
    return json.dumps(pointer.to_dict(), sort_keys=True, separators=(",", ":"))  # type: ignore[attr-defined]


def _random_file(path: str, size: int) -> str:
    """Write `size` random bytes to `path`, a chunk at a time; return their SHA-256."""
    digest = hashlib.sha256()
    with open(path, "wb") as handle:
        remaining = size
        while remaining:
            chunk = os.urandom(min(remaining, _MIB))
            handle.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _file_digest(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(_MIB), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
