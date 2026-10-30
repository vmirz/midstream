"""An object store over the local filesystem, for `file:///` URIs: tests and laptops only.

One machine only: the chart refuses a `file://` destination (decision D5), since every pod would need
the same volume and a lock no network filesystem should be trusted with.

An object is a regular file; its key is its absolute path, so unlike in a bucket no key can name both
an object and a directory of others (`a` and `a/b`): the second write raises `StoreError`. Tables
never do. Every write fills a temporary file beside the target (`.<name>.midstream-tmp-<hex>`, never
listed) and then puts it in place in one step: create-only by `os.link`, which fails if the target
exists; unconditional and compare-and-swap by `os.replace`. Objects are never changed in place, so a
reader holding one open reads one version whole. Writes and deletes run under a process-wide lock
plus an `fcntl.flock` on the lock file of the object's directory (`.midstream-store.lock`: never
listed, never an object's name, and refused when it is a symbolic link). The lock lives with the
objects it guards, so every process that sees the files agrees on every condition, whatever its
user, temporary directory or container. Deleting objects leaves their directories and lock files.

A version is `<inode>-<size>-<mtime in ns>` of the file, read from the open file on reads so the
content and the version belong together. Inode numbers are reused once freed and kernels stamp files
from a coarse clock, so each write sets its file's mtime itself, under the lock, from a time kept in
the directory's lock file that rises at least a microsecond per write: no two writes in a directory
share an mtime, and a path never gets the same version twice on filesystems that keep
sub-microsecond times (ext4, XFS, Btrfs, tmpfs, APFS).

A failure of the filesystem under the store (a permission, a full disk) raises `StoreError` with no
status and the errno's name as its code; one with a caller's own file - `put_file`'s source,
`get_to_file`'s target - raises its `OSError` as it is, as every store does.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
import errno
import os
import re
import shutil
import stat
import threading
import time
from typing import BinaryIO, Final
import uuid

try:
    import fcntl
except ImportError:  # pragma: no cover - not POSIX: the process lock alone, one process at a time
    fcntl = None  # type: ignore[assignment]

from midstream.lakehouse.store.base import (
    ABSENT,
    Absent,
    DeleteFailure,
    DeleteOutcome,
    ObjectInfo,
    ObjectNotFoundError,
    PreconditionFailedError,
    StoreError,
    byte_range,
)
from midstream.lakehouse.store.uris import require_object_uri, require_prefix_uri

#: The lock file in each directory the store writes objects to: writers there lock it, and it holds
#: the last stamp given to a file written there. Never listed; no object may take its name.
LOCK_FILE_NAME: Final = ".midstream-store.lock"
#: How a lock file is opened: created when missing, never through a symbolic link.
_LOCK_FLAGS: Final = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
#: What a write's temporary file is called, beside its target.
TEMPORARY_PATTERN: Final = re.compile(r"^\..+\.midstream-tmp-[0-9a-f]{32}$")
#: The least two stamps differ by: a microsecond, kept by every filesystem that keeps sub-second times.
_STAMP_STEP_NS: Final = 1_000
#: A stamp in the lock file further ahead of the clock than this is not one of ours.
_STAMP_HORIZON_NS: Final = 3_600 * 1_000_000_000
_COPY_CHUNK_BYTES: Final = 1024 * 1024
#: Errors of `os.link` on filesystems without hard links.
_NO_HARD_LINKS: Final = frozenset({errno.EPERM, errno.ENOTSUP, errno.EOPNOTSUPP})
_MISSING: Final = (FileNotFoundError, NotADirectoryError, IsADirectoryError)

_PROCESS_LOCK = threading.Lock()


class FileStore:
    """Objects as files under `file:///` URIs. Content types are not kept."""

    scheme = "file"
    delete_batch_size = 100
    supports_conditional_delete = True

    def get(self, uri: str, *, start: int | None = None, end: int | None = None) -> tuple[bytes, str]:
        """The file's bytes, or the inclusive range `start..end`, and the version of the file read."""
        path = _object_path(uri)
        with _filesystem_errors(uri), _open_object(path, uri) as handle:
            status = os.fstat(handle.fileno())
            first, stop = byte_range(status.st_size, start, end, uri=uri)
            handle.seek(first)
            data = handle.read(stop - first)
        return data, _version(status)

    def head(self, uri: str) -> ObjectInfo | None:
        """The file's size, version and modification time; None when there is no such file."""
        path = _object_path(uri)
        with _filesystem_errors(uri):
            status = _regular_status(path)
        return None if status is None else _info(uri, status)

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write the file if `if_version` holds; return its new version."""
        payload = _bytes(data)
        return self._write(uri, if_version, lambda handle: handle.write(payload))

    def put_file(
        self,
        uri: str,
        path: str,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """`put` with the content of the local file at `path`, copied a chunk at a time."""
        _object_path(uri)
        with open(path, "rb") as source:  # a missing source is the caller's OSError
            return self._write(uri, if_version, lambda handle: shutil.copyfileobj(source, handle, _COPY_CHUNK_BYTES))

    def get_to_file(self, uri: str, path: str) -> ObjectInfo:
        """Copy the whole file to `path`; return what was read. Nothing is left at `path` on failure."""
        source_path = _object_path(uri)
        try:
            with _open_object(source_path, uri) as source:
                status = os.fstat(source.fileno())
                with open(path, "wb") as target:
                    shutil.copyfileobj(source, target, _COPY_CHUNK_BYTES)
        except BaseException:
            _remove_quietly(path)
            raise
        return _info(uri, status)

    def delete(self, uri: str, *, if_version: str | None = None) -> bool:
        """Delete the file (only at `if_version` when given); False when it was already missing."""
        path = _object_path(uri)
        _check_version_type(if_version, allow_absent=False)
        with _filesystem_errors(uri):
            if _regular_status(path) is None:
                return False  # nothing to delete: no lock to make, in a directory that may not exist
            try:
                with _exclusive(os.path.dirname(path)):
                    current = _current_version(path)
                    if current is None:
                        return False
                    if if_version is not None and current != if_version:
                        raise PreconditionFailedError(f"{uri} is not at version {if_version}", uri=uri, code="PreconditionFailed")
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        return False
            except (FileNotFoundError, NotADirectoryError):
                return False  # the directory went away, the file with it, before the lock was taken
        return True

    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome:
        """Delete each file in turn; a missing one counts as deleted, a failure is reported."""
        for uri in uris:
            _object_path(uri)
        deleted: list[str] = []
        failed: list[DeleteFailure] = []
        for uri in uris:
            try:
                self.delete(uri)
            except Exception as exc:  # noqa: BLE001 - every object gets an outcome
                status = exc.status if isinstance(exc, StoreError) else None
                failed.append(DeleteFailure(uri, status, f"{type(exc).__name__}: {exc}", error=exc))
            else:
                deleted.append(uri)
        return DeleteOutcome(deleted=tuple(deleted), failed=tuple(failed))

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        """Every file whose `file://` URI starts with `prefix`, in URI order; the store's temporary
        and lock files skipped.

        The walk starts at the deepest directory the prefix names - the prefix itself when it ends
        with a slash, else its parent - and does not follow symbolic links.
        """
        parsed = require_prefix_uri(prefix, scheme="file")
        _check_segments(parsed.key, prefix, last_may_be_partial=True)
        path_prefix = "/" + parsed.key
        start = path_prefix if path_prefix.endswith("/") else os.path.dirname(path_prefix)
        with _filesystem_errors(prefix):
            found = _walk(start, path_prefix)
        found.sort(key=lambda info: info.uri)
        return iter(found)

    def _write(self, uri: str, if_version: str | Absent | None, fill: Callable[[BinaryIO], object]) -> str:
        """Fill a temporary file beside the target, then put it in place if `if_version` holds."""
        target = _object_path(uri)
        _check_version_type(if_version, allow_absent=True)
        directory, name = os.path.split(target)
        temporary = os.path.join(directory, f".{name}.midstream-tmp-{uuid.uuid4().hex}")
        try:
            with _filesystem_errors(uri):
                os.makedirs(directory, exist_ok=True)
                with open(temporary, "xb") as handle:
                    fill(handle)
                with _exclusive(directory) as lock:
                    if if_version is ABSENT:
                        _stamp(temporary, lock)
                        _link_create_only(temporary, target, uri)
                    else:
                        if if_version is not None and _current_version(target) != if_version:
                            raise PreconditionFailedError(f"{uri} is not at version {if_version}", uri=uri, code="PreconditionFailed")
                        _stamp(temporary, lock)
                        os.replace(temporary, target)
                    return _version(os.stat(target))
        finally:
            _remove_quietly(temporary)


def _walk(start: str, path_prefix: str) -> list[ObjectInfo]:
    """The regular files at or below `start` whose paths start with `path_prefix`, unsorted; neither
    the store's temporary and lock files nor symbolic links."""
    found: list[ObjectInfo] = []
    pending = [start]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as scanned:
                entries = [*scanned]
        except _MISSING:
            continue
        for entry in entries:
            full = entry.path
            if entry.is_dir(follow_symlinks=False):
                branch = full + "/"
                if branch.startswith(path_prefix) or path_prefix.startswith(branch):
                    pending.append(full)
            elif entry.is_file(follow_symlinks=False) and full.startswith(path_prefix):
                if entry.name == LOCK_FILE_NAME or TEMPORARY_PATTERN.match(entry.name):
                    continue
                try:
                    status = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue  # deleted while the walk went on
                found.append(_info(f"file://{full}", status))
    return found


@contextmanager
def _filesystem_errors(uri: str) -> Iterator[None]:
    """Raise a failure of the filesystem under the store as `StoreError`: no status, the errno's name."""
    try:
        yield
    except StoreError:
        raise
    except OSError as exc:
        raise _store_error(uri, exc) from exc


def _store_error(uri: str, exc: OSError) -> StoreError:
    code = errno.errorcode.get(exc.errno or 0, type(exc).__name__)
    return StoreError(f"{uri}: {type(exc).__name__}: {exc}", uri=uri, status=None, code=code)


@contextmanager
def _exclusive(directory: str) -> Iterator[int]:
    """Hold the write lock of `directory`, shared by every process that sees its files; yields the
    lock file's descriptor. A symbolic link in its place, or anything but a regular file, is refused."""
    with _PROCESS_LOCK:
        path = os.path.join(directory, LOCK_FILE_NAME)
        descriptor = os.open(path, _LOCK_FLAGS, 0o666)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError(errno.EINVAL, "the store's lock is not a regular file", path)
            if fcntl is not None:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield descriptor
        finally:
            os.close(descriptor)  # releases the flock


def _stamp(path: str, lock: int) -> None:
    """Give `path` an mtime later than any stamp given before in its directory (its lock held)."""
    os.lseek(lock, 0, os.SEEK_SET)
    raw = os.read(lock, 64)
    now = time.time_ns()
    try:
        last = int(raw.decode("ascii").strip() or "0")
    except (UnicodeDecodeError, ValueError):
        last = 0
    if last > now + _STAMP_HORIZON_NS:
        last = 0
    stamp = max(now, last + _STAMP_STEP_NS)
    text = str(stamp).encode("ascii")
    os.lseek(lock, 0, os.SEEK_SET)
    os.write(lock, text)
    os.ftruncate(lock, len(text))
    os.utime(path, ns=(stamp, stamp))


def _link_create_only(temporary: str, target: str, uri: str) -> None:
    """Put `temporary` in place only if nothing is at `target` (lock held)."""
    try:
        os.link(temporary, target)
    except FileExistsError as exc:
        raise PreconditionFailedError(f"{uri} already exists", uri=uri, code="PreconditionFailed") from exc
    except OSError as exc:
        if exc.errno not in _NO_HARD_LINKS:
            raise
        # A filesystem without hard links: under the lock, checking and then renaming is as safe.
        if os.path.lexists(target):
            raise PreconditionFailedError(f"{uri} already exists", uri=uri, code="PreconditionFailed") from None
        os.replace(temporary, target)


@contextmanager
def _open_object(path: str, uri: str) -> Iterator[BinaryIO]:
    """The object's file open for reading: ObjectNotFoundError when there is none, StoreError when
    the filesystem refuses it."""
    try:
        handle = open(path, "rb")
    except _MISSING as exc:
        raise ObjectNotFoundError(f"no object at {uri}", uri=uri, code="NotFound") from exc
    except OSError as exc:
        raise _store_error(uri, exc) from exc
    with handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ObjectNotFoundError(f"no object at {uri}", uri=uri, code="NotFound")
        yield handle


def _object_path(uri: str) -> str:
    parsed = require_object_uri(uri, scheme="file")
    _check_segments(parsed.key, uri, last_may_be_partial=False)
    name = parsed.key.rsplit("/", 1)[-1]
    if name == LOCK_FILE_NAME or TEMPORARY_PATTERN.match(name):
        raise ValueError(f"a file URI cannot name the store's own lock or temporary files: {uri!r}")
    return "/" + parsed.key


def _check_segments(key: str, uri: str, *, last_may_be_partial: bool) -> None:
    """Refuse a path two URIs could name: empty, `.` or `..` segments (a prefix's last may be empty)."""
    if not key:
        return
    segments = key.split("/")
    checked = segments[:-1] if last_may_be_partial else segments
    for segment in checked:
        if segment in ("", ".", ".."):
            raise ValueError(f"a file URI cannot have empty, '.' or '..' path segments: {uri!r}")


def _regular_status(path: str) -> os.stat_result | None:
    try:
        status = os.stat(path)
    except _MISSING:
        return None
    return status if stat.S_ISREG(status.st_mode) else None


def _current_version(path: str) -> str | None:
    status = _regular_status(path)
    return None if status is None else _version(status)


def _version(status: os.stat_result) -> str:
    return f"{status.st_ino}-{status.st_size}-{status.st_mtime_ns}"


def _info(uri: str, status: os.stat_result) -> ObjectInfo:
    modified = datetime.fromtimestamp(status.st_mtime_ns / 1_000_000_000, tz=UTC)
    return ObjectInfo(uri=uri, size=status.st_size, version=_version(status), last_modified=modified)


def _check_version_type(if_version: object, *, allow_absent: bool) -> None:
    if if_version is None or isinstance(if_version, str) or (allow_absent and if_version is ABSENT):
        return
    raise TypeError(f"if_version must be a version string{', ABSENT' if allow_absent else ''} or None, not {if_version!r}")


def _bytes(data: bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, str):
        raise TypeError("object data must be bytes, not str: encode it first")
    return bytes(data)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
