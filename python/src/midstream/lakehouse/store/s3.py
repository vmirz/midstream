"""Amazon S3, or an S3-compatible server, behind the `ObjectStore` contract, through boto3.

A version is the object's ETag without its quotes (`normalize_version`); it goes back as `If-Match`
quoted. A single-part ETag is the MD5 of the content, so equal content gives an equal version: the
catalog pointer never repeats its bytes (its sequence rises on every advance, design §3.3), so a stale
`If-Match` can never match again.

Conditional writes send only what S3 means one way: create-only is `IfNoneMatch='*'` - S3 accepts any
other `If-None-Match` value and writes - and a compare-and-swap is `IfMatch` with a real ETag, never
`*`, which an existing key answers 412. Reads are one `GetObject`, whose answer carries the ETag of the
bytes it holds; ranges go as `Range: bytes=a-b` (`bytes=0-b` for an end alone, never the suffix form
`bytes=-b`). `put_file` is one `PutObject` streaming the open file: boto3's transfer manager refuses
the conditions, and one PUT takes up to 5 GB, far above a seal's file.

Errors are mapped once, here (design §6.2): 412 `PreconditionFailed` and 409
`ConditionalRequestConflict` (another conditional write in flight) are `PreconditionFailedError`, and
so is a 404 under `If-Match`, where S3 answers `NoSuchKey` and GCS and Azure answer 412. Any other 404
is a missing object - judged by the status, since `HeadObject`'s answer has no body and its code is
just `404` - except `NoSuchBucket`, a configuration problem that must not make a table look missing;
a HEAD cannot tell it apart. Every other answer is a `StoreError` with its status and code, a failure
in transit one with no status.

Deletes: S3 does not say whether a key existed, so `delete` answers True. Conditional deletes are not
offered (`supports_conditional_delete = False`): moto and SeaweedFS 4.51 honour `If-Match` on
`DeleteObject`, but on AWS's general purpose buckets it is unverified, and a server that ignored it
would delete what it was asked to keep, so `delete(..., if_version=...)` raises `NotImplementedError`
before any request rather than emulating it. `delete_many` sends `DeleteObjects` of up to 1,000 keys,
quiet, and reads only the answer's `Errors` (moto ignores `Quiet`); those requests go through a client
without retries, as GCS's batch requests do, so the caller decides what to send again.

Checksums and signing are boto3's defaults (CRC32 request checksums and response validation "when
supported"), which SeaweedFS and moto accept as they are; nothing here weakens them. Credentials come
from boto3's chain (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, IRSA, EKS pod identity, an instance
role) and are never read or logged here; a credential that cannot be found raises botocore's own error.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
import http.client
import os
import threading
from typing import Any, Final

from botocore import exceptions as boto_errors
from botocore.parsers import ResponseParserError
import urllib3

from midstream.lakehouse.store.base import (
    ABSENT,
    Absent,
    DeleteFailure,
    DeleteOutcome,
    ObjectInfo,
    ObjectNotFoundError,
    PreconditionFailedError,
    StoreError,
    as_utc,
    check_range,
    normalize_version,
)
from midstream.lakehouse.store.registry import StoreSettings
from midstream.lakehouse.store.uris import require_object_uri, require_prefix_uri

#: Keys in one `DeleteObjects` request: the most S3 takes (moto accepts more; AWS does not).
DELETE_BATCH_SIZE: Final = 1000
#: Retries of a failed request under botocore's standard mode. A botocore `Config` counts its
#: `max_attempts` without the first attempt: six attempts in all.
MAX_RETRIES: Final = 5
#: Connections the client keeps per host: as many as the threads a runtime uploads with.
MAX_POOL_CONNECTIONS: Final = 32
CONNECT_TIMEOUT_SECONDS: Final = 10
READ_TIMEOUT_SECONDS: Final = 120
#: Bytes copied at a time from a download into a local file.
_COPY_CHUNK_BYTES: Final = 1024 * 1024

#: Failures in transit, which got no usable answer: the connection, a timeout, a body cut short or
#: corrupted on the way (a checksum that does not match), an answer that cannot be parsed.
_TRANSPORT_ERRORS: Final = (
    boto_errors.ConnectionError,
    boto_errors.HTTPClientError,
    boto_errors.IncompleteReadError,
    boto_errors.ChecksumError,
    boto_errors.FlexibleChecksumError,
    ResponseParserError,
    ConnectionError,
    TimeoutError,
    http.client.HTTPException,
    urllib3.exceptions.HTTPError,
)
#: Codes that mean a write's condition did not hold.
_PRECONDITION_CODES: Final = frozenset({"PreconditionFailed", "ConditionalRequestConflict"})
#: Codes that mean there is no such object.
_MISSING_CODES: Final = frozenset({"NoSuchKey", "NotFound", "404"})
#: The HTTP status behind each code a `DeleteObjects` answer may give a key, from S3's error
#: reference; a key's entry carries a code but no status.
_DELETE_ERROR_STATUSES: Final = {
    "AccessDenied": 403,
    "AccountProblem": 403,
    "AllAccessDisabled": 403,
    "InvalidAccessKeyId": 403,
    "InvalidObjectState": 403,
    "SignatureDoesNotMatch": 403,
    "NoSuchBucket": 404,
    "NoSuchVersion": 404,
    "MethodNotAllowed": 405,
    "ConditionalRequestConflict": 409,
    "OperationAborted": 409,
    "PreconditionFailed": 412,
    "InvalidArgument": 400,
    "InvalidRequest": 400,
    "InvalidURI": 400,
    "KeyTooLongError": 400,
    "MalformedXML": 400,
    # S3 sends RequestTimeout with a 400, but it is a timeout: worth another try, as 408 says.
    "RequestTimeout": 408,
    "TooManyRequests": 429,
    "InternalError": 500,
    "NotImplemented": 501,
    "ServiceUnavailable": 503,
    "SlowDown": 503,
}
#: Words in an unknown code that say the failure is passing (throttling, a busy or failing server).
_PASSING_FAILURE_WORDS: Final = ("Internal", "Unavailable", "SlowDown", "Throttl", "Timeout", "TooMany", "Busy")

#: Why one key of a `DeleteObjects` request was not deleted: the status, a message, the exception,
#: and whether the request failed as a whole.
_KeyFailure = tuple[int | None, str, BaseException | None, bool]


class S3Store:
    """The S3 store: one boto3 client for the process's endpoint and region, built on first use unless
    one is given (tests give a client under botocore's `Stubber`).

    `delete_many` uses a second client, built the same way from the same session but without retries;
    a given client serves both.
    """

    scheme = "s3"
    delete_batch_size = DELETE_BATCH_SIZE
    supports_conditional_delete = False

    def __init__(self, client: Any = None, settings: StoreSettings | None = None) -> None:
        self.settings = StoreSettings.from_environment() if settings is None else settings
        self._client = client
        self._delete_client = client
        self._session: Any = None
        self._lock = threading.Lock()

    @classmethod
    def from_environment(cls) -> S3Store:
        """The store for the `STORAGE_S3_*` settings in the environment."""
        return cls(settings=StoreSettings.from_environment())

    @property
    def client(self) -> Any:
        """The boto3 S3 client every operation but `delete_many` uses."""
        client = self._client
        if client is None:
            with self._lock:
                if self._client is None:
                    self._client = self._build_client(retries=True)
                client = self._client
        return client

    @property
    def delete_client(self) -> Any:
        """The client `delete_many` sends with: the same endpoint and credentials, no retries."""
        client = self._delete_client
        if client is None:
            with self._lock:
                if self._delete_client is None:
                    self._delete_client = self._build_client(retries=False)
                client = self._delete_client
        return client

    def _build_client(self, *, retries: bool) -> Any:
        """A client from the store's one boto3 session (sessions are not thread-safe: under the lock)."""
        if self._session is None:
            import boto3

            self._session = boto3.session.Session()
        return self._session.client(
            "s3",
            region_name=self.settings.s3_region,
            endpoint_url=self.settings.s3_endpoint,
            config=client_config(self.settings, retries=retries),
        )

    def get(self, uri: str, *, start: int | None = None, end: int | None = None) -> tuple[bytes, str]:
        """The object's bytes, or the inclusive range `start..end`, and the ETag they were read at."""
        check_range(start, end)
        bucket, key = _bucket_and_key(uri)
        params: dict[str, Any] = {"Bucket": bucket, "Key": key}
        requested = _range_header(start, end)
        if requested is not None:
            params["Range"] = requested
        with _translated(uri):
            response = self.client.get_object(**params)
            data = _read_body(response["Body"])
        return data, _version(response, uri)

    def head(self, uri: str) -> ObjectInfo | None:
        """The object's size, ETag and last-modified time; None when there is no such object."""
        bucket, key = _bucket_and_key(uri)
        try:
            with _translated(uri):
                response = self.client.head_object(Bucket=bucket, Key=key)
        except ObjectNotFoundError:
            return None
        return ObjectInfo(
            uri=uri,
            size=int(response.get("ContentLength") or 0),
            version=_version(response, uri),
            last_modified=as_utc(response.get("LastModified")),
        )

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write the object if `if_version` holds; return its new ETag."""
        payload = _bytes(data)
        bucket, key = _bucket_and_key(uri)
        condition = _write_condition(if_version)
        with _translated(uri, compare_and_swap=isinstance(if_version, str)):
            response = self.client.put_object(Bucket=bucket, Key=key, Body=payload, ContentType=content_type, **condition)
        return _version(response, uri)

    def put_file(
        self,
        uri: str,
        path: str,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write the local file at `path` as the object, if `if_version` holds, in one streamed
        `PutObject`; return its new ETag."""
        bucket, key = _bucket_and_key(uri)
        condition = _write_condition(if_version)
        with open(path, "rb") as handle:
            with _translated(uri, compare_and_swap=isinstance(if_version, str)):
                response = self.client.put_object(Bucket=bucket, Key=key, Body=handle, ContentType=content_type, **condition)
        return _version(response, uri)

    def get_to_file(self, uri: str, path: str) -> ObjectInfo:
        """Stream the whole object to `path`; return what was read. Nothing is left at `path` on failure."""
        bucket, key = _bucket_and_key(uri)
        try:
            with open(path, "wb") as handle:
                with _translated(uri):
                    response = self.client.get_object(Bucket=bucket, Key=key)
                    size = _copy_body(response["Body"], handle)
            version = _version(response, uri)
        except BaseException:
            _remove_quietly(path)
            raise
        return ObjectInfo(uri=uri, size=size, version=version, last_modified=as_utc(response.get("LastModified")))

    def delete(self, uri: str, *, if_version: str | None = None) -> bool:
        """Delete the object; True, since S3 does not say whether it existed.

        Conditional deletes are not offered (see the module's docstring): `if_version` raises
        `NotImplementedError` before any request.
        """
        bucket, key = _bucket_and_key(uri)
        if if_version is not None:
            if not isinstance(if_version, str):
                raise TypeError(f"if_version must be an ETag string or None, not {if_version!r}")
            raise NotImplementedError(
                f"{uri}: the S3 store does not offer conditional deletes: If-Match on DeleteObject is "
                "unverified on Amazon S3's general purpose buckets (supports_conditional_delete is False)"
            )
        try:
            with _translated(uri):
                self.client.delete_object(Bucket=bucket, Key=key)
        except ObjectNotFoundError:
            # S3 itself answers 204 for a missing key; a server that answers 404 has told us.
            return False
        return True

    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome:
        """Delete the objects, bucket by bucket, in `DeleteObjects` requests of up to 1,000 keys.

        The answer names only the keys it could not delete (quiet mode; moto lists the others too,
        which is ignored): each becomes a failure with the HTTP status its code stands for, and every
        other key is gone - a missing one included. A request that fails as a whole fails every key in
        it with the answer's status, or none when there was no answer. Nothing is retried.
        """
        targets = [(uri, *_bucket_and_key(uri)) for uri in uris]
        keys_by_bucket: dict[str, dict[str, None]] = {}
        for _, bucket, key in targets:
            keys_by_bucket.setdefault(bucket, {})[key] = None
        failures: dict[tuple[str, str], _KeyFailure] = {}
        for bucket, unique in keys_by_bucket.items():
            keys = list(unique)
            for begin in range(0, len(keys), DELETE_BATCH_SIZE):
                failures.update(self._delete_chunk(bucket, keys[begin : begin + DELETE_BATCH_SIZE]))
        deleted: list[str] = []
        failed: list[DeleteFailure] = []
        for uri, bucket, key in targets:
            failure = failures.get((bucket, key))
            if failure is None:
                deleted.append(uri)
            else:
                status, message, error, whole_request = failure
                failed.append(DeleteFailure(uri, status, message, error=error, whole_request=whole_request))
        return DeleteOutcome(deleted=tuple(deleted), failed=tuple(failed))

    def _delete_chunk(self, bucket: str, keys: list[str]) -> dict[tuple[str, str], _KeyFailure]:
        """One `DeleteObjects` request; the failure of each key it could not delete."""
        try:
            response = self.delete_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
            )
        except Exception as exc:  # noqa: BLE001 - the request failed as a whole: so did every delete in it
            error = store_error(exc, f"s3://{bucket}/{keys[0]}")
            status = None if error is None else error.status
            message = f"{type(exc).__name__}: {exc}"
            return {(bucket, key): (status, message, exc, True) for key in keys}
        requested = set(keys)
        failures: dict[tuple[str, str], _KeyFailure] = {}
        for entry in response.get("Errors") or ():
            key = entry.get("Key")
            code = str(entry.get("Code") or "")
            if key not in requested or code in _MISSING_CODES:
                continue  # a key this request did not send; a missing object counts as deleted
            message = f"{code}: {entry.get('Message')}" if entry.get("Message") else code or "no error code"
            failures[(bucket, key)] = (delete_error_status(code), message, None, False)
        return failures

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        """Every object whose URI starts with `prefix`, in key order, a listing page at a time."""
        parsed = require_prefix_uri(prefix, scheme="s3")
        return self._list(parsed.container, parsed.key, prefix)

    def _list(self, bucket: str, key_prefix: str, prefix: str) -> Iterator[ObjectInfo]:
        with _translated(prefix):
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
                for item in page.get("Contents") or ():
                    yield ObjectInfo(
                        uri=f"s3://{bucket}/{item['Key']}",
                        size=int(item.get("Size") or 0),
                        version=normalize_version(str(item.get("ETag") or "")),
                        last_modified=as_utc(item.get("LastModified")),
                    )


def client_config(settings: StoreSettings, *, retries: bool = True) -> Any:
    """The botocore configuration of the store's clients: path-style addressing when the settings ask
    for it, standard retries (or a single attempt, without `retries`), 32 pooled connections, 10 s to
    connect and 120 s to read. Checksums and signing stay at botocore's defaults: nothing here would
    change them."""
    from botocore.config import Config

    return Config(
        s3={"addressing_style": "path" if settings.s3_path_style else "auto"},
        retries={"mode": "standard", "max_attempts": MAX_RETRIES} if retries else {"mode": "standard", "total_max_attempts": 1},
        max_pool_connections=MAX_POOL_CONNECTIONS,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        read_timeout=READ_TIMEOUT_SECONDS,
    )


@contextmanager
def _translated(uri: str, *, compare_and_swap: bool = False) -> Iterator[None]:
    """Raise what an exception of boto3 amounts to under the contract; anything else as it is.

    Under a compare-and-swap (`If-Match`), a 404 is the news that the condition did not hold.
    """
    try:
        yield
    except Exception as exc:
        error = store_error(exc, uri, compare_and_swap=compare_and_swap)
        if error is None or error is exc:
            raise
        raise error from exc


def store_error(exc: BaseException, uri: str, *, compare_and_swap: bool = False) -> StoreError | None:
    """The `StoreError` an exception of boto3 amounts to; None for one that is no storage failure (a
    bad argument, a credential that cannot be found, a local file), which is raised as it is."""
    if isinstance(exc, StoreError):
        return exc
    if isinstance(exc, boto_errors.ClientError):
        status, code = _status_and_code(exc)
        if status == 412 or code in _PRECONDITION_CODES:
            return PreconditionFailedError(f"{uri}: {exc}", uri=uri, status=status or 412, code=code)
        if code == "NoSuchBucket":
            return StoreError(
                f"{uri}: the bucket does not exist, or STORAGE_S3_ENDPOINT or STORAGE_S3_REGION names the wrong "
                f"server ({status} NoSuchBucket)",
                uri=uri,
                status=status,
                code=code,
            )
        if status == 404 or code in _MISSING_CODES:
            if compare_and_swap:
                return PreconditionFailedError(
                    f"{uri}: there is no object to compare the version with ({exc})", uri=uri, status=404, code=code
                )
            return ObjectNotFoundError(f"{uri}: {exc}", uri=uri, status=404, code=code)
        return StoreError(f"{uri}: {exc}", uri=uri, status=status, code=code)
    if isinstance(exc, _TRANSPORT_ERRORS):
        return StoreError(f"{uri}: {type(exc).__name__}: {exc}", uri=uri, status=None, code=type(exc).__name__)
    return None


def delete_error_status(code: str) -> int:
    """The HTTP status a `DeleteObjects` error code stands for (an entry carries no status of its own).

    An unknown code is a refusal (400), unless its name says the failure is passing - throttling, a
    busy or failing server - which is a 503, worth another try.
    """
    status = _DELETE_ERROR_STATUSES.get(code)
    if status is not None:
        return status
    if any(word in code for word in _PASSING_FAILURE_WORDS):
        return 503
    return 400


def _status_and_code(exc: BaseException) -> tuple[int | None, str | None]:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None, None
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    code = (response.get("Error") or {}).get("Code")
    # An answer without a code (an empty one included) is judged by its status alone.
    return (status if isinstance(status, int) else None), (str(code) if code else None)


def _bucket_and_key(uri: str) -> tuple[str, str]:
    parsed = require_object_uri(uri, scheme="s3")
    return parsed.container, parsed.key


def _range_header(start: int | None, end: int | None) -> str | None:
    """`Range` for the inclusive `start..end`: `bytes=a-b`, `bytes=a-` for a start alone, `bytes=0-b`
    for an end alone (`bytes=-b` would ask for the last b bytes); None for the whole object."""
    if start is None and end is None:
        return None
    first = 0 if start is None else start
    return f"bytes={first}-" if end is None else f"bytes={first}-{end}"


def _write_condition(if_version: str | Absent | None) -> dict[str, str]:
    """The `PutObject` condition: none, `IfNoneMatch='*'` (create-only), or `IfMatch` with the ETag."""
    if if_version is None:
        return {}
    if if_version is ABSENT:
        return {"IfNoneMatch": "*"}
    if not isinstance(if_version, str):
        raise TypeError(f"if_version must be an ETag string, ABSENT or None, not {if_version!r}")
    return {"IfMatch": _quoted_etag(if_version)}


def _quoted_etag(version: str) -> str:
    """A version as `If-Match` carries it: the ETag in double quotes. `*` - any existing object - is
    never a version."""
    etag = normalize_version(version)
    if not etag or etag == "*" or '"' in etag:
        raise ValueError(f"{version!r} is not an S3 ETag")
    return f'"{etag}"'


def _version(response: dict[str, Any], uri: str) -> str:
    etag = response.get("ETag")
    if not etag:
        raise StoreError(f"{uri}: S3 reported no ETag for the object", uri=uri, status=None, code="NoETag")
    return normalize_version(str(etag))


def _read_body(body: Any) -> bytes:
    try:
        return body.read()
    finally:
        body.close()


def _copy_body(body: Any, handle: Any) -> int:
    """Copy a streaming body into an open file a chunk at a time; the bytes copied."""
    size = 0
    try:
        for chunk in body.iter_chunks(chunk_size=_COPY_CHUNK_BYTES):
            handle.write(chunk)
            size += len(chunk)
    finally:
        body.close()
    return size


def _bytes(data: bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, str):
        raise TypeError("object data must be bytes, not str: encode it first")
    return bytes(data)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
