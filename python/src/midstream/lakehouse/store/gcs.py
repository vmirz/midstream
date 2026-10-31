"""Google Cloud Storage behind the `ObjectStore` contract, through google-cloud-storage.

A version is the object's generation. Every operation makes the requests Midstream made to GCS
before the store layer existed, with the same arguments: a read is one `download_as_bytes`, whose
answer carries the generation read; a conditional write passes `if_generation_match` (0:
create-only); deletes go a hundred to a batch request of the JSON API. The library's own retries
apply - except inside a batch, which it never retries, and which `delete_many` leaves to its caller.
A download whose answer names no generation raises `StoreError` (`NoGeneration`) rather than asking
for the generation in a second request, whose answer could belong to a newer object than the bytes -
and a compare-and-swap with it would overwrite the write in between. GCS and fake-gcs-server always
name it.

Errors are mapped once, here: the library's `PreconditionFailed` (412) to `PreconditionFailedError`,
`NotFound` (404) to `ObjectNotFoundError` or the operation's answer for a missing object - unless GCS
says the bucket itself does not exist, a `StoreConfigurationError` that must not make a table look
missing - any other API error to `StoreError` with its HTTP status, and a failure in transit - a
connection reset, a timeout, a corrupted download - to `StoreError` with no status. Doubles of the
library are classified as the code before did: by class name, or by an HTTP status in an integer
`code`.

Credentials: `GOOGLE_APPLICATION_CREDENTIALS` (a mounted key), else application default credentials
(workload identity); `STORAGE_EMULATOR_HOST` points the library at an emulator. A credential that
cannot be found or refreshed raises google-auth's own error, as it did before the store.

fake-gcs-server, the emulator CI checks against, differs from GCS in three places the store does not
bend to: it ignores preconditions on DELETE (a stale conditional delete deletes), the library's batch
requests fail against it (`ValueError: Bad response: not multi-part`) having deleted nothing, and it
answers a missing bucket as it answers a missing object. `GcsStore(batch_deletes=False)` sends one
request per delete for such a server; the registry builds it so when `STORAGE_EMULATOR_HOST` is set.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from http import HTTPStatus
import http.client
import json
import os
import threading
from typing import Any, Final

from google.api_core import exceptions as api_exceptions
from google.auth import exceptions as auth_exceptions
import requests
import urllib3

from midstream.lakehouse.store.base import (
    ABSENT,
    Absent,
    DeleteFailure,
    DeleteOutcome,
    ObjectInfo,
    ObjectNotFoundError,
    PreconditionFailedError,
    StoreConfigurationError,
    StoreError,
    as_utc,
    check_range,
)
from midstream.lakehouse.store.uris import require_object_uri, require_prefix_uri

#: Deletes in one batch request: the most the JSON API takes in one.
BATCH_SIZE: Final = 100
#: Seconds to wait for a batch request's answer, which GCS sends once it has run every call in it.
BATCH_REQUEST_TIMEOUT_SECONDS: Final = 30.0
#: GCS's message - in place of "No such object: ..." - when the bucket itself does not exist,
#: compared without case or the closing period.
_NO_SUCH_BUCKET: Final = "the specified bucket does not exist"

#: Failures in transit, which got no answer: the types google-cloud-storage itself retries as such.
_TRANSPORT_ERRORS: Final = (
    ConnectionError,
    TimeoutError,
    requests.exceptions.RequestException,
    http.client.HTTPException,
    urllib3.exceptions.HTTPError,
    auth_exceptions.TransportError,
)


class GcsStore:
    """The GCS store: one client, built on first use unless one is given (tests give a double).

    `batch_deletes=False` makes `delete_many` send one request per object even through a client
    that can batch: for a server without the JSON API's batch endpoint, such as fake-gcs-server.
    """

    scheme = "gs"
    supports_conditional_delete = True

    def __init__(self, client: Any = None, *, batch_deletes: bool = True) -> None:
        self._client = client
        self._client_lock = threading.Lock()
        self.batch_deletes = batch_deletes

    @property
    def delete_batch_size(self) -> int:
        """The deletes `delete_many` sends in one request: a hundred in a batch, or 1 when it deletes
        one object per request - made with `batch_deletes=False`, or given a client that cannot batch
        (a test double) - so a caller budgeting requests by it counts what it sends
        (`storage/objects.py`). Asking builds no client: the library's own always batches."""
        if not self.batch_deletes:
            return 1
        client = self._client
        if client is not None and not callable(getattr(client, "batch", None)):
            return 1
        return BATCH_SIZE

    @property
    def client(self) -> Any:
        """The google-cloud-storage client; `google.cloud.storage.Client()` unless one was given."""
        client = self._client
        if client is None:
            with self._client_lock:
                if self._client is None:
                    from google.cloud import storage

                    self._client = storage.Client()
                client = self._client
        return client

    def get(self, uri: str, *, start: int | None = None, end: int | None = None) -> tuple[bytes, str]:
        """The object's bytes, or the inclusive range `start..end`, and the generation they were read at."""
        check_range(start, end)
        bucket_name, name = _bucket_and_name(uri)
        blob = self.client.bucket(bucket_name).blob(name)
        with _translated(uri):
            if start is None and end is None:
                data = blob.download_as_bytes()
            else:
                data = blob.download_as_bytes(start=start, end=end)
        return data, _read_generation(blob, uri)

    def head(self, uri: str) -> ObjectInfo | None:
        """The object's size, generation and creation time; None when there is no such object."""
        bucket_name, name = _bucket_and_name(uri)
        blob = self._get_blob(self.client.bucket(bucket_name), name, uri)
        return None if blob is None else _info(uri, blob)

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload the object if `if_version` holds; return its new generation."""
        payload = _bytes(data)
        bucket_name, name = _bucket_and_name(uri)
        condition = _generation_match(if_version)
        blob = self.client.bucket(bucket_name).blob(name)
        with _translated(uri, compare_and_swap=isinstance(if_version, str)):
            blob.upload_from_string(payload, content_type=content_type, **condition)
        return _written_generation(blob, uri)

    def put_file(
        self,
        uri: str,
        path: str,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload the local file at `path` if `if_version` holds (resumable when large); return the
        new generation."""
        bucket_name, name = _bucket_and_name(uri)
        condition = _generation_match(if_version)
        blob = self.client.bucket(bucket_name).blob(name)
        with _translated(uri, compare_and_swap=isinstance(if_version, str)):
            blob.upload_from_filename(path, content_type=content_type, **condition)
        return _written_generation(blob, uri)

    def get_to_file(self, uri: str, path: str) -> ObjectInfo:
        """Download the whole object to `path`; return what was read. Nothing is left at `path` on
        failure. `last_modified` is None: a download does not report it."""
        bucket_name, name = _bucket_and_name(uri)
        try:
            blob = self.client.bucket(bucket_name).blob(name)
            with _translated(uri):
                blob.download_to_filename(path)
            generation = _read_generation(blob, uri)
            size = os.path.getsize(path)
        except BaseException:
            _remove_quietly(path)
            raise
        return ObjectInfo(uri=uri, size=size, version=generation, last_modified=_blob_time(blob))

    def delete(self, uri: str, *, if_version: str | None = None) -> bool:
        """Delete the object (only at generation `if_version` when given); False when GCS answered 404."""
        bucket_name, name = _bucket_and_name(uri)
        if if_version is not None and not isinstance(if_version, str):
            raise TypeError(f"if_version must be a generation string or None, not {if_version!r}")
        # Always a fresh Blob, by name: one that carries a generation (from a read) would send it as
        # a selector, and GCS answers 404 for an older one, keeping the live object.
        blob = self.client.bucket(bucket_name).blob(name)
        try:
            with _translated(uri):
                if if_version is None:
                    blob.delete()
                else:
                    blob.delete(if_generation_match=_generation(if_version))
        except ObjectNotFoundError:
            return False
        return True

    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome:
        """Delete the objects, bucket by bucket, in batch requests of up to a hundred deletes.

        Each call in a batch gets its own answer: 2xx and 404 mean the object is gone - unless the 404
        says the bucket does not exist - anything else is that object's failure with GCS's status and
        message. A batch request that fails itself - no answer, or an error for the whole request -
        fails every delete in it. A client that cannot batch (a test double), or a store made with
        `batch_deletes=False`, deletes one object per request, each error taken like a call's answer.
        """
        targets = [(uri, *_bucket_and_name(uri)) for uri in uris]
        by_bucket: dict[str, list[tuple[int, str, str]]] = {}
        for index, (uri, bucket_name, name) in enumerate(targets):
            by_bucket.setdefault(bucket_name, []).append((index, uri, name))
        outcomes: dict[int, DeleteFailure | None] = {}
        client = self.client
        for bucket_name, items in by_bucket.items():
            bucket = client.bucket(bucket_name)
            if self.batch_deletes and _can_batch(client, bucket):
                for begin in range(0, len(items), BATCH_SIZE):
                    chunk = items[begin : begin + BATCH_SIZE]
                    for (index, _, _), failure in zip(chunk, _delete_batch(client, bucket, chunk)):
                        outcomes[index] = failure
            else:
                for index, uri, name in items:
                    outcomes[index] = _delete_one(bucket, uri, name)
        deleted = tuple(targets[index][0] for index in sorted(outcomes) if outcomes[index] is None)
        failed = tuple(failure for index in sorted(outcomes) if (failure := outcomes[index]) is not None)
        return DeleteOutcome(deleted=deleted, failed=failed)

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        """Every object whose URI starts with `prefix`, in name order, a listing page at a time."""
        parsed = require_prefix_uri(prefix, scheme="gs")
        return self._list(parsed.container, parsed.key, prefix)

    def _list(self, bucket_name: str, key_prefix: str, prefix: str) -> Iterator[ObjectInfo]:
        with _translated(prefix):
            for blob in self.client.list_blobs(bucket_name, prefix=key_prefix):
                yield _info(f"gs://{bucket_name}/{blob.name}", blob)

    def _get_blob(self, bucket: Any, name: str, uri: str) -> Any | None:
        # The request `bucket.get_blob(name)` makes, but its 404 judged here: the library's get_blob
        # answers None for any 404, a missing bucket's too, which must not make a table look missing.
        blob = bucket.blob(name)
        try:
            with _translated(uri):
                blob.reload()
        except ObjectNotFoundError:
            return None
        return blob


@contextmanager
def _translated(uri: str, *, compare_and_swap: bool = False) -> Iterator[None]:
    """Raise what an exception of the library amounts to under the contract; anything else as is.

    Under a compare-and-swap, a 404 is the news that the condition did not hold, as on S3.
    """
    try:
        yield
    except Exception as exc:
        error = store_error(exc, uri)
        if compare_and_swap and isinstance(error, ObjectNotFoundError):
            error = PreconditionFailedError(str(error), uri=uri, status=error.status, code=error.code)
        if error is None or error is exc:
            raise
        raise error from exc


def store_error(exc: BaseException, uri: str) -> StoreError | None:
    """The `StoreError` that an exception of google-cloud-storage amounts to; None for one that is no
    storage failure (a bad argument, a missing local file), which is raised as it is."""
    if isinstance(exc, StoreError):
        return exc
    if isinstance(exc, api_exceptions.RetryError):
        # The library retried until its deadline: the last failure is what to report.
        cause = exc.cause if isinstance(exc.cause, BaseException) else None
        inner = None if cause is None else store_error(cause, uri)
        if inner is None:
            return StoreError(f"{uri}: {exc}", uri=uri, status=None, code="RetryError")
        return type(inner)(f"{uri}: {exc}", uri=uri, status=inner.status, code=inner.code)
    status = http_status(exc)
    if status is None and type(exc).__name__ == "InvalidResponse":
        response_status = getattr(getattr(exc, "response", None), "status_code", None)
        status = response_status if isinstance(response_status, int) else None
    code = _error_code(exc)
    if status == 412 or _named(exc, "PreconditionFailed", api_exceptions.PreconditionFailed):
        return PreconditionFailedError(f"{uri}: {exc}", uri=uri, status=412, code=code)
    if status == 404 or _named(exc, "NotFound", api_exceptions.NotFound):
        if _bucket_missing(exc):
            return StoreConfigurationError(f"{uri}: the bucket does not exist ({exc})", uri=uri, status=404, code=code)
        return ObjectNotFoundError(f"{uri}: {exc}", uri=uri, status=404, code=code)
    if status is not None or isinstance(exc, api_exceptions.GoogleAPICallError):
        return StoreError(f"{uri}: {exc}", uri=uri, status=status, code=code)
    if isinstance(exc, _TRANSPORT_ERRORS) or type(exc).__name__ == "DataCorruption":
        return StoreError(f"{uri}: {type(exc).__name__}: {exc}", uri=uri, status=None, code=type(exc).__name__)
    return None


def http_status(exc: BaseException) -> int | None:
    """The HTTP status an error of google-cloud-storage (or a double of one) carries in `code`."""
    code = getattr(exc, "code", None)
    if isinstance(code, int) and not isinstance(code, bool) and 100 <= code < 600:
        return code
    return None


def _bucket_missing(exc: BaseException) -> bool:
    """Whether a 404 says the bucket itself does not exist, not the object: GCS's message is then "The
    specified bucket does not exist." - in the error list of a JSON API answer, or the message of a
    media download's body. Only the whole message counts, so an object's name never does."""
    messages: list[object] = []
    errors = getattr(exc, "errors", None)
    if isinstance(errors, (list, tuple)):
        messages.extend(item.get("message") for item in errors if isinstance(item, dict))
    text = getattr(getattr(exc, "response", None), "text", None)
    if isinstance(text, str):
        messages.append(_body_message(text))
    return any(isinstance(message, str) and _says_bucket_missing(message) for message in messages)


def _body_message(text: str) -> str:
    """The message of an error answer's body: its JSON error's message, else the text itself."""
    try:
        payload = json.loads(text)
    except ValueError:
        return text
    error = payload.get("error") if isinstance(payload, dict) else None
    message = error.get("message") if isinstance(error, dict) else None
    return message if isinstance(message, str) else text


def _says_bucket_missing(message: str) -> bool:
    return message.strip().rstrip(".").strip().lower() == _NO_SUCH_BUCKET


def _named(exc: BaseException, name: str, kind: type[BaseException]) -> bool:
    return isinstance(exc, kind) or type(exc).__name__ == name


def _error_code(exc: BaseException) -> str:
    """GCS's reason for an API error (`conditionNotMet`, `notFound`, ...), else the exception's class."""
    reason = getattr(exc, "reason", None)
    if isinstance(reason, str) and reason:
        return reason
    errors = getattr(exc, "errors", None)
    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
        first = errors[0].get("reason")
        if isinstance(first, str) and first:
            return first
    return type(exc).__name__


def _can_batch(client: Any, bucket: Any) -> bool:
    """Whether deletes in `bucket` can go in batch requests (a test double usually cannot)."""
    return callable(getattr(client, "batch", None)) and callable(getattr(bucket, "delete_blob", None))


def _delete_batch(client: Any, bucket: Any, chunk: list[tuple[int, str, str]]) -> list[DeleteFailure | None]:
    """One batch request deleting `chunk`; per object, None when it is gone or its failure."""
    try:
        responses = _send_batch(client, bucket, [name for _, _, name in chunk])
    except Exception as exc:  # noqa: BLE001 - the request failed as a whole: so did every delete in it
        error = store_error(exc, chunk[0][1])
        status = None if error is None else error.status
        message = f"{type(exc).__name__}: {exc}"
        return [DeleteFailure(uri, status, message, error=exc, whole_request=True) for _, uri, _ in chunk]
    failures: list[DeleteFailure | None] = []
    for (_, uri, _), response in zip(chunk, responses):
        status = response.status_code
        if 200 <= status < 300 or (status == 404 and not _answer_says_bucket_missing(response)):
            failures.append(None)
        else:
            # A missing bucket is configuration, as outside a batch: the delete failed, for good.
            failures.append(DeleteFailure(uri, status, _error_message(response)))
    return failures


def _answer_says_bucket_missing(response: Any) -> bool:
    """Whether one call's answer in a batch says the bucket does not exist."""
    content = getattr(response, "content", None)
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")
    return isinstance(content, str) and _says_bucket_missing(_body_message(content))


def _send_batch(client: Any, bucket: Any, names: list[str]) -> list[Any]:
    """One batch request deleting `names`; the answer to each call, in order."""
    with client.batch(raise_exception=False) as batch:
        for name in names:
            # Deferred into the batch, so the library's retry would never fire: the caller's does.
            bucket.delete_blob(name, timeout=BATCH_REQUEST_TIMEOUT_SECONDS, retry=None)
    # `Batch.finish()` returns these, but the context manager discards its return value; with
    # `raise_exception=False` this is the only place a failed call is reported.
    responses = getattr(batch, "_responses", None)
    if not isinstance(responses, list) or len(responses) != len(names):
        raise RuntimeError("GCS batch request returned no answer for each delete in it")
    return responses


def _delete_one(bucket: Any, uri: str, name: str) -> DeleteFailure | None:
    """Delete one object in a request of its own, never retried (the caller retries); None when it
    is gone, else its failure."""
    try:
        bucket.blob(name).delete(retry=None)
    except Exception as exc:  # noqa: BLE001 - classified like a batched call's answer
        error = store_error(exc, uri)
        if isinstance(error, ObjectNotFoundError):
            return None
        # A missing bucket (`StoreConfigurationError`) is a failure with its 404, never retried.
        status = None if error is None else error.status
        return DeleteFailure(uri, status, f"{type(exc).__name__}: {exc}", error=exc)
    return None


def _error_message(response: Any) -> str:
    """The status and message GCS gave for a failed call, the message from its JSON error body."""
    try:
        status = f"{response.status_code} {HTTPStatus(response.status_code).phrase}"
    except ValueError:
        status = str(response.status_code)
    try:
        message = json.loads(response.content or b"{}")["error"]["message"]
    except (ValueError, KeyError, TypeError):
        return status
    return f"{status}: {message}"


def _bucket_and_name(uri: str) -> tuple[str, str]:
    parsed = require_object_uri(uri, scheme="gs")
    return parsed.container, parsed.key


def _generation_match(if_version: str | Absent | None) -> dict[str, int]:
    """The `if_generation_match` argument for a condition: none, 0 (create-only), or a generation."""
    if if_version is None:
        return {}
    if if_version is ABSENT:
        return {"if_generation_match": 0}
    if not isinstance(if_version, str):
        raise TypeError(f"if_version must be a generation string, ABSENT or None, not {if_version!r}")
    return {"if_generation_match": _generation(if_version)}


def _generation(version: str) -> int:
    try:
        generation = int(version)
    except ValueError:
        raise ValueError(f"{version!r} is not a GCS generation") from None
    if generation <= 0:
        # 0 would ask for a create-only write; that is `ABSENT`, never a version.
        raise ValueError(f"{version!r} is not a GCS generation")
    return generation


def _read_generation(blob: Any, uri: str) -> str:
    """The generation a download recorded from its own answer (`X-Goog-Generation`).

    Never asked for in a second request: by then the object may have moved on, and these bytes paired
    with a newer generation would let a compare-and-swap overwrite a write its caller never saw.
    """
    generation = getattr(blob, "generation", None)
    if generation is None:
        raise StoreError(f"{uri}: GCS reported no generation for the object read", uri=uri, status=None, code="NoGeneration")
    return str(generation)


def _written_generation(blob: Any, uri: str) -> str:
    generation = getattr(blob, "generation", None)
    if generation is None:
        raise StoreError(f"{uri}: GCS reported no generation for the object written", uri=uri, status=None, code="NoGeneration")
    return str(generation)


def _info(uri: str, blob: Any) -> ObjectInfo:
    generation = getattr(blob, "generation", None)
    return ObjectInfo(
        uri=uri,
        size=int(getattr(blob, "size", None) or 0),
        version="" if generation is None else str(generation),
        last_modified=_blob_time(blob),
    )


def _blob_time(blob: Any) -> datetime | None:
    """When the object was created (else last updated), as the orphan scan's age guard reads it."""
    for name in ("time_created", "updated"):
        value = getattr(blob, name, None)
        if isinstance(value, datetime):
            return as_utc(value)
    return None


def _bytes(data: bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, str):
        raise TypeError("object data must be bytes, not str: encode it first")
    return bytes(data)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
