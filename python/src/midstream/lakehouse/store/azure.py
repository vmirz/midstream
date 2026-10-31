"""Azure Blob Storage behind the `ObjectStore` contract, through azure-storage-blob.

URIs name the DFS endpoint (`abfss://<container>@<account>.dfs.core.windows.net/<key>`), the form
every engine reads; the store reaches the same account through its Blob endpoint, which every account
has - with or without a hierarchical namespace - and Azurite too. One `BlobServiceClient` per account,
built on first use: from `AZURE_STORAGE_CONNECTION_STRING` when it is set (it must name the URI's
account), else for the account's URL (`STORAGE_AZURE_ENDPOINT/<account>` for an emulator, else
`https://<account>.blob.core.windows.net`) with `AZURE_STORAGE_ACCOUNT_KEY` as the shared key when it
is set, else with `DefaultAzureCredential` (workload or managed identity, a service principal from
`AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_CLIENT_SECRET`), built once per process. Neither the string
nor the key is ever logged or put in a message. The Azure libraries are imported on first use.

A version is the blob's ETag without its quotes, compared for equality only: Azure's ETags are random,
change on every write - an identical one included - and come quoted from HEAD, GET and PUT but bare
from listings; a condition sends them quoted. Writes always go as `upload_blob(overwrite=True)` with an
explicit match condition - `IfMissing` (`If-None-Match: *`) for create-only, the ETag and
`IfNotModified` for a compare-and-swap, none for an unconditional write - because with
`overwrite=False` the library turns a 412 into `BlobAlreadyExists`. Past 8 MiB an upload goes in
blocks, and the block-list commit carries the condition; a conditional upload that large first asks
for the blob's ETag and refuses without staging a block when the condition already fails, so a lost
race is the only way blocks are left uncommitted (Azure discards them within a week). Requires
azure-storage-blob 12.31 or later: before it, block ids came from offsets and racing create-only
uploads committed mixed content.

Reads: a blob up to 8 MiB is one GET whose ETag belongs to the bytes; a larger one is read in chunks,
each later chunk under `If-Match`, so a blob that changes mid-read fails with 412 and the read starts
again, up to three times. `get_to_file` downloads into a temporary file beside the target and renames
it on success. Any range of an empty blob is 416, as on S3; the whole of one is empty bytes. A range
that starts at the blob's size is 416 as well, which Azurite answers with no bytes instead.

Errors are classified only by the HTTP status and the `x-ms-error-code` header, never by the
exception's class or the library's `error_code` (design §6.2): (412, `ConditionNotMet`) and (409,
`BlobAlreadyExists`) are `PreconditionFailedError`, and so is (404, `BlobNotFound`) under a
condition; (404, `BlobNotFound`) alone is a missing object, and nothing else is - a missing container,
a refused credential or an API version the server does not know is `AzureConfigurationError` (a
`StoreConfigurationError`, never retryable), which must never make a table look missing. A failure
in transit is a `StoreError` with no status - a download body cut short or reset included, which the
library reads three times before it gives up; a credential that cannot be obtained raises
azure-identity's own error.

Every client shares one transport per process - a requests session pooling 32 connections per host,
with urllib3's retries off - and retries a failed request three times, 1 s apart and growing (the
library's default first wait is 15 s). Deletes go 256 to a blob batch request, without retries (the
caller retries); where a service refuses the batch itself, the account's deletes go one by one. Each
delete in a batch names the blob by its whole path, the account included behind an emulator's path-
style endpoint whatever its host is called (the library adds it only for `localhost`). A
retried write whose first attempt landed answers 409 or 412 although it won: callers re-read and
compare, as `upload.py` and the catalog do.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
import os
import tempfile
import threading
from typing import Any, Final, TypeVar

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
    normalize_version,
)
from midstream.lakehouse.store.registry import StoreSettings
from midstream.lakehouse.store.uris import ObjectUri, require_object_uri, require_prefix_uri

#: Deletes in one blob batch request: the most Azure takes (257 is refused).
BATCH_SIZE: Final = 256
#: Blocks and download chunks of 8 MiB, and no single request larger: what a runtime holds per transfer.
TRANSFER_CHUNK_BYTES: Final = 8 * 1024 * 1024
#: Blocks or chunks in flight at once for one `put_file` or `get_to_file`.
TRANSFER_CONCURRENCY: Final = 4
#: Whole reads of a blob that keeps changing under a chunked download before giving up.
READ_ATTEMPTS: Final = 3
#: Retries after a failed request, the first after about a second, then growing.
RETRY_TOTAL: Final = 3
RETRY_INITIAL_BACKOFF_SECONDS: Final = 1
RETRY_INCREMENT_BASE: Final = 2
#: Pooled connections per host in the shared transport: room, with some to spare, for every thread of
#: a runtime that reaches the store at once - its uploads, the committer's footer reads, its table
#: reads - where urllib3 keeps 10, logging "Connection pool is full" and halving throughput.
POOL_SIZE: Final = 32
CONNECT_TIMEOUT_SECONDS: Final = 10
READ_TIMEOUT_SECONDS: Final = 120

#: The variables naming an account's credentials; read when a client is built, never logged.
CONNECTION_STRING_VARIABLE: Final = "AZURE_STORAGE_CONNECTION_STRING"
ACCOUNT_KEY_VARIABLE: Final = "AZURE_STORAGE_ACCOUNT_KEY"

#: (status, `x-ms-error-code`) answers that mean a condition did not hold.
_PRECONDITION_ANSWERS: Final = frozenset({(412, "ConditionNotMet"), (409, "BlobAlreadyExists")})
#: The one answer that means there is no such blob.
_BLOB_NOT_FOUND: Final = (404, "BlobNotFound")
#: Answers that mean the store's configuration is wrong, never that a blob is missing.
_CONFIGURATION_ANSWERS: Final = {
    (404, "ContainerNotFound"): "the container does not exist in this storage account",
    (404, "ResourceNotFound"): "the endpoint does not serve this account or container: check the account and STORAGE_AZURE_ENDPOINT",
    (403, "AuthorizationFailure"): "the account refuses this identity or network: check its role assignments and firewall",
    (403, "AuthorizationPermissionMismatch"): "the identity has no data role on the account, such as Storage Blob Data Contributor",
    (403, "AuthenticationFailed"): "the account refused the credentials: check the account key, connection string or identity",
    (401, "InvalidAuthenticationInfo"): "the account refused the identity's token: check the identity and its tenant",
    (401, "NoAuthenticationInformation"): "the request carried no credentials the account accepts",
    (400, "InvalidHeaderValue"): (
        "the service refused a request header, usually the API version: an emulator such as Azurite needs "
        "--skipApiVersionCheck"
    ),
}
#: How a service answers a blob batch it does not offer (an endpoint without the batch API, an account
#: whose namespace refuses it): only these send a batch's deletes one by one instead.
_BATCH_REFUSALS: Final = frozenset(
    {
        (400, "InvalidQueryParameterValue"),
        (400, "UnsupportedQueryParameter"),
        (409, "FeatureNotYetSupportedForHierarchicalNamespaceAccounts"),
    }
)
#: Statuses that refuse a batch request whatever the code: the method or the feature is not there.
_BATCH_REFUSAL_STATUSES: Final = frozenset({405, 501})

_T = TypeVar("_T")

_SHARED_LOCK = threading.Lock()
_TRANSPORT: Any = None
_DEFAULT_CREDENTIAL: Any = None


class AzureConfigurationError(StoreConfigurationError):
    """The Azure store cannot serve the URI as configured: a container that does not exist, refused
    credentials, a connection string for another account. A `StoreConfigurationError`: never
    retryable, since the same request fails until the configuration changes."""


class AzureBlobStore:
    """The Azure store: one `BlobServiceClient` per storage account, each built on first use.

    `credential`, when given, signs every account's requests instead of the environment's credentials
    (an `AzureNamedKeyCredential`, a token credential). `clients` maps an account to a ready client
    (tests give doubles); other accounts get clients built from the environment.
    """

    scheme = "abfss"
    delete_batch_size = BATCH_SIZE
    supports_conditional_delete = True

    def __init__(
        self,
        settings: StoreSettings | None = None,
        credential: Any = None,
        clients: Mapping[str, Any] | None = None,
    ) -> None:
        self.settings = StoreSettings.from_environment() if settings is None else settings
        self._credential = credential
        self._clients: dict[str, Any] = dict(clients or {})
        self._lock = threading.Lock()
        #: Accounts whose service refused a blob batch: their deletes go one by one.
        self._unbatched: set[str] = set()

    @classmethod
    def from_environment(cls) -> AzureBlobStore:
        """The store for the `STORAGE_AZURE_ENDPOINT` setting and the credentials in the environment."""
        return cls(settings=StoreSettings.from_environment())

    def account_url(self, account: str) -> str:
        """The Blob endpoint of `account`: `STORAGE_AZURE_ENDPOINT/<account>` for an emulator, else
        `https://<account>.blob.core.windows.net`."""
        if self.settings.azure_endpoint:
            return f"{self.settings.azure_endpoint}/{account}"
        return f"https://{account}.blob.core.windows.net"

    def service_client(self, account: str) -> Any:
        """The `BlobServiceClient` for `account`, built the first time it is asked for."""
        client = self._clients.get(account)
        if client is None:
            with self._lock:
                client = self._clients.get(account)
                if client is None:
                    client = self._build_service_client(account)
                    self._clients[account] = client
        return client

    def _build_service_client(self, account: str) -> Any:
        from azure.storage.blob import BlobServiceClient

        options = client_options()
        connection_string = _environment_value(CONNECTION_STRING_VARIABLE)
        if self._credential is None and connection_string:
            client = BlobServiceClient.from_connection_string(connection_string, **options)
            named = client.account_name
            if named != account:
                raise AzureConfigurationError(
                    f"{CONNECTION_STRING_VARIABLE} names the storage account {named!r}, but the store was asked "
                    f"for {account!r}: a connection string serves its own account only",
                    status=None,
                    code="AccountMismatch",
                )
            return client
        credential = self._credential
        if credential is None:
            key = _environment_value(ACCOUNT_KEY_VARIABLE)
            if key:
                from azure.core.credentials import AzureNamedKeyCredential

                credential = AzureNamedKeyCredential(account, key)
            else:
                credential = default_credential()
        return BlobServiceClient(account_url=self.account_url(account), credential=credential, **options)

    def _blob_client(self, parsed: ObjectUri) -> Any:
        return self.service_client(_account(parsed)).get_blob_client(parsed.container, parsed.key)

    def get(self, uri: str, *, start: int | None = None, end: int | None = None) -> tuple[bytes, str]:
        """The blob's bytes, or the inclusive range `start..end`, and the ETag they were read at.

        A blob larger than one chunk that changes while it is read is read again from the start, up to
        three times in all.
        """
        check_range(start, end)
        parsed = _object_uri(uri)
        options = _range_options(start, end)
        blob = self._blob_client(parsed)

        def read() -> tuple[bytes, str]:
            with _translated(uri):
                downloader = blob.download_blob(**options)
                data = downloader.readall()
            if options and not data:
                # Every range asks for at least one byte, so none came back because it starts at or past
                # the end: Azurite answers a start at exactly the size with 206 and no bytes, not 416.
                raise _range_past_the_end(uri, start, end, downloader.properties)
            return data, _etag(downloader.properties, uri)

        return _reading_again(uri, read)

    def head(self, uri: str) -> ObjectInfo | None:
        """The blob's size, ETag and last-modified time; None when there is no such blob."""
        parsed = _object_uri(uri)
        blob = self._blob_client(parsed)
        try:
            with _translated(uri):
                properties = blob.get_blob_properties()
        except ObjectNotFoundError:
            return None
        return _info(uri, properties)

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write the blob if `if_version` holds; return its new ETag."""
        payload = _bytes(data)
        parsed = _object_uri(uri)
        condition = _match_condition(if_version)
        blob = self._blob_client(parsed)
        if condition and len(payload) > TRANSFER_CHUNK_BYTES:
            self._refuse_early(blob, uri, if_version)
        with _translated(uri, conditional=isinstance(if_version, str)):
            response = blob.upload_blob(
                payload,
                length=len(payload),
                overwrite=True,
                content_settings=_content_settings(content_type),
                **condition,
            )
        return _written_etag(response, uri)

    def put_file(
        self,
        uri: str,
        path: str,
        *,
        if_version: str | Absent | None = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write the local file at `path` as the blob, if `if_version` holds, streamed in 8 MiB blocks
        when larger than one; return its new ETag."""
        parsed = _object_uri(uri)
        condition = _match_condition(if_version)
        with open(path, "rb") as handle:
            size = os.fstat(handle.fileno()).st_size
            blob = self._blob_client(parsed)
            if condition and size > TRANSFER_CHUNK_BYTES:
                self._refuse_early(blob, uri, if_version)
            with _translated(uri, conditional=isinstance(if_version, str)):
                response = blob.upload_blob(
                    handle,
                    length=size,
                    overwrite=True,
                    max_concurrency=TRANSFER_CONCURRENCY,
                    content_settings=_content_settings(content_type),
                    **condition,
                )
        return _written_etag(response, uri)

    def _refuse_early(self, blob: Any, uri: str, if_version: str | Absent | None) -> None:
        """Before an upload that goes in blocks: refuse it when its condition already fails, so that no
        block is staged on a blob the commit would not replace. The commit still carries the condition."""
        try:
            with _translated(uri):
                current: str | None = _etag(blob.get_blob_properties(), uri)
        except ObjectNotFoundError:
            current = None
        if if_version is ABSENT and current is not None:
            raise PreconditionFailedError(f"{uri} already exists", uri=uri, status=409, code="BlobAlreadyExists")
        if isinstance(if_version, str) and current != normalize_version(if_version):
            found = "it does not exist" if current is None else f"it is at {current}"
            raise PreconditionFailedError(f"{uri} is not at version {if_version}: {found}", uri=uri, status=412, code="ConditionNotMet")

    def get_to_file(self, uri: str, path: str) -> ObjectInfo:
        """Download the whole blob to `path` through a temporary file beside it; return what was read.
        Nothing is left at `path` on failure."""
        parsed = _object_uri(uri)
        try:
            blob = self._blob_client(parsed)
            return _reading_again(uri, lambda: _download_to_file(blob, uri, path))
        except BaseException:
            _remove_quietly(path)
            raise

    def delete(self, uri: str, *, if_version: str | None = None) -> bool:
        """Delete the blob (only at ETag `if_version` when given); False when it was already missing.

        Azure answers a stale ETag and an ETag on a missing blob alike (412 `ConditionNotMet`), so a
        refused conditional delete asks whether the blob is there: missing is False, present the
        `PreconditionFailedError`.
        """
        parsed = _object_uri(uri)
        if if_version is not None and not isinstance(if_version, str):
            raise TypeError(f"if_version must be an ETag string or None, not {if_version!r}")
        condition = {} if if_version is None else _match_condition(if_version)
        blob = self._blob_client(parsed)
        try:
            with _translated(uri):
                blob.delete_blob(**condition)
        except ObjectNotFoundError:
            return False
        except PreconditionFailedError:
            if self.head(uri) is None:
                return False
            raise
        return True

    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome:
        """Delete the blobs, container by container, in blob batch requests of up to 256 deletes.

        Each delete in a batch gets its own answer, in order: 2xx and 404 (`BlobNotFound`) mean the
        blob is gone, anything else is that blob's failure with its status. A batch request that fails
        itself fails every delete in it with its status (none when there was no answer) - unless the
        service refused the batch as a feature it does not offer, when the account's deletes go one
        request each from then on. Nothing is retried.
        """
        targets = [(uri, _object_uri(uri)) for uri in uris]
        groups: dict[tuple[str, str], dict[str, str]] = {}
        for uri, parsed in targets:
            groups.setdefault((_account(parsed), parsed.container), {}).setdefault(parsed.key, uri)
        failures: dict[tuple[str, str, str], DeleteFailure] = {}
        for (account, container), names in groups.items():
            container_client = self.service_client(account).get_container_client(container)
            items = list(names.items())
            for begin in range(0, len(items), BATCH_SIZE):
                chunk = items[begin : begin + BATCH_SIZE]
                for (name, _), failure in zip(chunk, self._delete_chunk(account, container_client, chunk)):
                    if failure is not None:
                        failures[(account, container, name)] = failure
        deleted: list[str] = []
        failed: list[DeleteFailure] = []
        for uri, parsed in targets:
            failure = failures.get((_account(parsed), parsed.container, parsed.key))
            if failure is None:
                deleted.append(uri)
            else:
                failed.append(failure)
        return DeleteOutcome(deleted=tuple(deleted), failed=tuple(failed))

    def _delete_chunk(self, account: str, container_client: Any, chunk: list[tuple[str, str]]) -> list[DeleteFailure | None]:
        """Delete `chunk` (name, URI pairs) in one batch request, or one by one where batches are
        refused; per blob, None when it is gone, else its failure."""
        if account not in self._unbatched:
            names = [name for name, _ in chunk]
            options: dict[str, Any] = {"raise_on_any_failure": False, "retry_total": 0}
            prefix = _account_path(container_client)
            if prefix:
                options["url_prepend"] = prefix
            try:
                responses = list(container_client.delete_blobs(*names, **options))
            except Exception as exc:  # noqa: BLE001 - classified below
                error = store_error(exc, chunk[0][1])
                if error is None or not _batch_refused(error):
                    status = None if error is None else error.status
                    message = f"{type(exc).__name__}: {_first_line(exc)}"
                    return [DeleteFailure(uri, status, message, error=exc, whole_request=True) for _, uri in chunk]
                with self._lock:
                    self._unbatched.add(account)
            else:
                if len(responses) != len(chunk):
                    message = f"the blob batch answered {len(responses)} of the {len(chunk)} deletes it carried"
                    if responses:
                        first = responses[0]
                        code = _header(first, "x-ms-error-code")
                        message += f" (the first: {first.status_code}{f' {code}' if code else ''})"
                    return [DeleteFailure(uri, None, message, whole_request=True) for _, uri in chunk]
                return [_batch_answer(uri, response) for (_, uri), response in zip(chunk, responses)]
        return [_delete_one(container_client, name, uri) for name, uri in chunk]

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        """Every blob whose URI starts with `prefix`, in name order, a listing page at a time; the
        directory entries of an account with a hierarchical namespace are not objects and are left out."""
        parsed = require_prefix_uri(prefix, scheme="abfss")
        return self._list(parsed, prefix)

    def _list(self, parsed: ObjectUri, prefix: str) -> Iterator[ObjectInfo]:
        account = _account(parsed)
        with _translated(prefix):
            container_client = self.service_client(account).get_container_client(parsed.container)
            for properties in container_client.list_blobs(name_starts_with=parsed.key or None, include=["metadata"]):
                if _is_directory(properties):
                    continue
                uri = ObjectUri(scheme="abfss", container=parsed.container, key=properties.name, account=account).uri
                yield _info(uri, properties)


def client_options() -> dict[str, Any]:
    """What every `BlobServiceClient` is built with: the shared transport, three quick retries, and
    8 MiB blocks and chunks so no single request holds more."""
    from azure.storage.blob import ExponentialRetry

    return {
        "transport": shared_transport(),
        "retry_policy": ExponentialRetry(
            initial_backoff=RETRY_INITIAL_BACKOFF_SECONDS,
            increment_base=RETRY_INCREMENT_BASE,
            retry_total=RETRY_TOTAL,
        ),
        "max_single_put_size": TRANSFER_CHUNK_BYTES,
        "max_block_size": TRANSFER_CHUNK_BYTES,
        "max_single_get_size": TRANSFER_CHUNK_BYTES,
        "max_chunk_get_size": TRANSFER_CHUNK_BYTES,
    }


def shared_transport() -> Any:
    """The process's one transport: a requests session pooling `POOL_SIZE` connections per host,
    with urllib3's own retries off (the library's retry policy retries), which no client closes."""
    global _TRANSPORT
    transport = _TRANSPORT
    if transport is None:
        with _SHARED_LOCK:
            if _TRANSPORT is None:
                _TRANSPORT = _build_transport()
            transport = _TRANSPORT
    return transport


def _build_transport() -> Any:
    from azure.core.pipeline.transport import RequestsTransport
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    adapter = HTTPAdapter(pool_maxsize=POOL_SIZE, max_retries=Retry(total=False))
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return RequestsTransport(
        session=session,
        session_owner=False,
        connection_timeout=CONNECT_TIMEOUT_SECONDS,
        read_timeout=READ_TIMEOUT_SECONDS,
    )


def default_credential() -> Any:
    """The process's one `DefaultAzureCredential`: slow on first use, so built once and shared."""
    global _DEFAULT_CREDENTIAL
    credential = _DEFAULT_CREDENTIAL
    if credential is None:
        with _SHARED_LOCK:
            if _DEFAULT_CREDENTIAL is None:
                from azure.identity import DefaultAzureCredential

                _DEFAULT_CREDENTIAL = DefaultAzureCredential()
            credential = _DEFAULT_CREDENTIAL
    return credential


@contextmanager
def _translated(uri: str, *, conditional: bool = False) -> Iterator[None]:
    """Raise what an exception of the Azure libraries amounts to under the contract; anything else as
    it is. Under a compare-and-swap (`conditional`), a missing blob is the condition failing."""
    try:
        yield
    except Exception as exc:
        error = store_error(exc, uri, conditional=conditional)
        if error is None or error is exc:
            raise
        raise error from exc


def store_error(exc: BaseException, uri: str, *, conditional: bool = False) -> StoreError | None:
    """The `StoreError` an exception of the Azure libraries amounts to, judged by the answer's status
    and `x-ms-error-code` alone; None for one that is no storage failure (a bad argument, a credential
    that cannot be obtained), which is raised as it is."""
    if isinstance(exc, StoreError):
        return exc
    from azure.core import exceptions as azure_errors

    if isinstance(exc, (azure_errors.ServiceRequestError, azure_errors.ServiceResponseError, azure_errors.DecodeError)):
        # No answer, or one that could not be read (cut short, undecodable).
        return _in_transit(exc, uri, type(exc).__name__)
    if not isinstance(exc, azure_errors.HttpResponseError):
        return None
    status = _status(exc)
    if status is None:
        if isinstance(exc, azure_errors.ClientAuthenticationError):
            # azure-identity could not obtain a token: no request was made, and none is worth repeating.
            return None
        # A download body cut short or reset, after the library's three reads of it, comes as
        # `HttpResponseError(error, error=error)` with no answer: named after the failure inside.
        inner = getattr(exc, "inner_exception", None)
        return _in_transit(exc, uri, type(inner).__name__ if isinstance(inner, BaseException) else type(exc).__name__)
    code = _error_code(exc)
    answer = (status, code)
    detail = f"{status} {code}" if code else str(status)
    if answer in _PRECONDITION_ANSWERS:
        return PreconditionFailedError(f"{uri}: the condition did not hold ({detail})", uri=uri, status=status, code=code)
    if answer == _BLOB_NOT_FOUND:
        if conditional:
            return PreconditionFailedError(f"{uri}: there is no blob to compare the version with ({detail})", uri=uri, status=status, code=code)
        return ObjectNotFoundError(f"{uri}: no such blob ({detail})", uri=uri, status=status, code=code)
    problem = _CONFIGURATION_ANSWERS.get(answer)
    if problem is not None:
        return AzureConfigurationError(f"{uri}: {problem} ({detail})", uri=uri, status=status, code=code)
    return StoreError(f"{uri}: {detail}: {_first_line(exc)}", uri=uri, status=status, code=code)


def _in_transit(exc: BaseException, uri: str, code: str) -> StoreError:
    """A failure in transit, named `code`: no answer, or none that could be read. No status: retryable."""
    return StoreError(f"{uri}: {code}: {_first_line(exc)}", uri=uri, status=None, code=code)


def _status(exc: BaseException) -> int | None:
    status = getattr(exc, "status_code", None)
    if not isinstance(status, int):
        status = getattr(getattr(exc, "response", None), "status_code", None)
    return status if isinstance(status, int) and not isinstance(status, bool) else None


def _error_code(exc: BaseException) -> str | None:
    """The answer's `x-ms-error-code` header (HEAD answers have no body but carry it)."""
    headers = getattr(getattr(exc, "response", None), "headers", None)
    if headers is None:
        return None
    try:
        code = headers.get("x-ms-error-code")
    except AttributeError:
        return None
    return str(code) if code else None


def _account_path(container_client: Any) -> str | None:
    """What a blob batch's deletes must carry before the container: the path of the account's URL when
    the client reaches it path-style (`http://azurite:10000/<account>`), None for an account's own host
    (`https://<account>.blob.core.windows.net`).

    The library adds it itself only behind a host named `localhost` or `127.0.0.1`: through any other
    name an emulator answers the whole batch with a single 400 `InvalidInput` and deletes nothing.
    """
    hostname = getattr(container_client, "primary_hostname", None)
    if not isinstance(hostname, str):
        return None
    return hostname.partition("/")[2].strip("/") or None


def _batch_refused(error: StoreError) -> bool:
    """Whether a failed batch request was refused as a feature the service does not offer."""
    if error.status in _BATCH_REFUSAL_STATUSES:
        return True
    return (error.status, error.code) in _BATCH_REFUSALS or "HierarchicalNamespace" in (error.code or "")


def _batch_answer(uri: str, response: Any) -> DeleteFailure | None:
    """One delete's answer in a batch: None when the blob is gone, else its failure."""
    status = int(response.status_code)
    code = _header(response, "x-ms-error-code")
    if 200 <= status < 300 or (status == 404 and code in (None, "BlobNotFound")):
        return None
    reason = getattr(response, "reason", None) or ""
    message = " ".join(part for part in (str(status), str(reason), f"({code})" if code else "") if part)
    return DeleteFailure(uri, status, message)


def _delete_one(container_client: Any, name: str, uri: str) -> DeleteFailure | None:
    """Delete one blob in a request of its own, never retried; None when it is gone, else its failure."""
    try:
        with _translated(uri):
            container_client.delete_blob(name, retry_total=0)
    except ObjectNotFoundError:
        return None
    except Exception as exc:  # noqa: BLE001 - classified like a batched delete's answer
        status = exc.status if isinstance(exc, StoreError) else None
        return DeleteFailure(uri, status, f"{type(exc).__name__}: {_first_line(exc)}", error=exc)
    return None


def _download_to_file(blob: Any, uri: str, path: str) -> ObjectInfo:
    """One download of the whole blob into a temporary file beside `path`, renamed onto it on success."""
    directory = os.path.dirname(os.path.abspath(path))
    descriptor, temporary = tempfile.mkstemp(prefix=f".{os.path.basename(path)}.", suffix=".part", dir=directory)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            with _translated(uri):
                downloader = blob.download_blob(max_concurrency=TRANSFER_CONCURRENCY)
                size = downloader.readinto(handle)
        properties = downloader.properties
        info = ObjectInfo(uri=uri, size=int(size), version=_etag(properties, uri), last_modified=as_utc(properties.last_modified))
        os.replace(temporary, path)
    except BaseException:
        _remove_quietly(temporary)
        raise
    return info


def _reading_again(uri: str, read: Callable[[], _T]) -> _T:
    """`read()`, started again when the blob changes under it, up to `READ_ATTEMPTS` reads in all.

    A chunked download sends its later chunks under `If-Match`: a 412 there is no condition of the
    caller's, so once the reads run out it is a `StoreError`, not a `PreconditionFailedError`.
    """
    attempt = 1
    while True:
        try:
            return read()
        except PreconditionFailedError as exc:
            if attempt >= READ_ATTEMPTS:
                raise StoreError(
                    f"{uri}: the blob changed while it was read, {READ_ATTEMPTS} times in a row ({exc})",
                    uri=uri,
                    status=exc.status,
                    code=exc.code,
                ) from exc
            attempt += 1


def _range_past_the_end(uri: str, start: int | None, end: int | None, properties: Any) -> StoreError:
    """The 416 for a range that starts at or past the blob's last byte, naming its size when known."""
    first = 0 if start is None else start
    shown = f"{first}-" if end is None else f"{first}-{end}"
    total = str(getattr(properties, "content_range", None) or "").rpartition("/")[2]
    size = f" ({int(total):,} bytes)" if total.isdigit() else ""
    return StoreError(f"{uri}: the range {shown} starts past the end of the blob{size}", uri=uri, status=416, code="InvalidRange")


def _object_uri(uri: str) -> ObjectUri:
    return require_object_uri(uri, scheme="abfss")


def _account(parsed: ObjectUri) -> str:
    assert parsed.account is not None  # every abfss URI names its account
    return parsed.account


def _range_options(start: int | None, end: int | None) -> dict[str, int]:
    """`download_blob`'s offset and length for the inclusive `start..end`; none for the whole blob."""
    if start is None and end is None:
        return {}
    first = 0 if start is None else start
    if end is None:
        return {"offset": first}
    return {"offset": first, "length": end - first + 1}


def _match_condition(if_version: str | Absent | None) -> dict[str, Any]:
    """`upload_blob`'s (and `delete_blob`'s) condition: none, `IfMissing` (create-only), or the
    quoted ETag with `IfNotModified`."""
    if if_version is None:
        return {}
    from azure.core import MatchConditions

    if if_version is ABSENT:
        return {"match_condition": MatchConditions.IfMissing}
    if not isinstance(if_version, str):
        raise TypeError(f"if_version must be an ETag string, ABSENT or None, not {if_version!r}")
    return {"etag": _quoted_etag(if_version), "match_condition": MatchConditions.IfNotModified}


def _quoted_etag(version: str) -> str:
    """A version as `If-Match` carries it: the ETag in double quotes. `*` is never a version."""
    etag = normalize_version(version)
    if not etag or etag == "*" or '"' in etag:
        raise ValueError(f"{version!r} is not an Azure ETag")
    return f'"{etag}"'


def _content_settings(content_type: str) -> Any:
    from azure.storage.blob import ContentSettings

    return ContentSettings(content_type=content_type)


def _etag(properties: Any, uri: str) -> str:
    etag = getattr(properties, "etag", None)
    if not etag:
        raise StoreError(f"{uri}: Azure reported no ETag for the blob", uri=uri, status=None, code="NoETag")
    return normalize_version(str(etag))


def _written_etag(response: Mapping[str, Any], uri: str) -> str:
    etag = response.get("etag") if response else None
    if not etag:
        raise StoreError(f"{uri}: Azure reported no ETag for the blob written", uri=uri, status=None, code="NoETag")
    return normalize_version(str(etag))


def _info(uri: str, properties: Any) -> ObjectInfo:
    etag = getattr(properties, "etag", None)
    return ObjectInfo(
        uri=uri,
        size=int(getattr(properties, "size", None) or 0),
        version="" if not etag else normalize_version(str(etag)),
        last_modified=as_utc(getattr(properties, "last_modified", None)),
    )


def _is_directory(properties: Any) -> bool:
    """A directory entry of an account with a hierarchical namespace, which flat listings return as a
    blob with the metadata `hdi_isfolder=true`."""
    metadata = getattr(properties, "metadata", None) or {}
    return str(metadata.get("hdi_isfolder", "")).lower() == "true"


def _header(response: Any, name: str) -> str | None:
    headers = getattr(response, "headers", None)
    value = None if headers is None else headers.get(name)
    return str(value) if value else None


def _environment_value(name: str) -> str | None:
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


def _first_line(exc: BaseException) -> str:
    """The first line of an error's message: the libraries' messages go on with request ids and XML."""
    text = str(getattr(exc, "message", None) or exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def _bytes(data: bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, str):
        raise TypeError("object data must be bytes, not str: encode it first")
    return bytes(data)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
