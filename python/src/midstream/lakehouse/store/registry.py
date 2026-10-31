"""One object store per URI scheme per process, built from the environment on first use.

`store_for(uri)` is how code above this package reaches storage: the scheme of the URI picks the
store, so one deployment may keep tables on several backends and nothing above the store branches on
one. Each store serves every bucket of its scheme with the process's one identity, as the shared GCS
client did; it is thread-safe and lives as long as the process.

The backends' libraries are imported only when a store of their scheme is first built: importing
this package imports no cloud SDK, and a scheme no table uses costs nothing. Credentials are the
SDKs' own variables (`GOOGLE_APPLICATION_CREDENTIALS`, `AWS_*`, `AZURE_*`), which workload identities
inject; Midstream's own knobs are the `STORAGE_*` variables `StoreSettings` reads. With
`STORAGE_EMULATOR_HOST` set (google-cloud-storage's own variable), the GCS store deletes one object per
request, as an emulator needs.

Tests install a store with `set_store()` - a `MemoryStore` may stand in for any scheme - and remove it
with `set_store(scheme, None)` or `reset_stores()`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import os
import threading

from midstream.lakehouse.store.base import ObjectStore
from midstream.lakehouse.store.uris import SCHEMES, scheme_of

_LOCK = threading.Lock()
_STORES: dict[str, ObjectStore] = {}

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


@dataclass(frozen=True, slots=True)
class StoreSettings:
    """Midstream's own storage settings, the `STORAGE_*` variables; the S3 and Azure stores read them.

    `s3_endpoint` is an S3-compatible server's URL (None: AWS), `s3_region` the region requests are
    signed for, `s3_path_style` whether the bucket goes in the path rather than the host name (most
    S3-compatible servers want it). `azure_endpoint` is an emulator's Blob endpoint, to which the
    account is appended (None: `https://<account>.blob.core.windows.net`).
    """

    s3_endpoint: str | None = None
    s3_region: str = "us-east-1"
    s3_path_style: bool = False
    azure_endpoint: str | None = None

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] = os.environ) -> StoreSettings:
        """The settings in `environ`; a blank variable counts as unset. ValueError names a variable
        whose value cannot be used."""
        return cls(
            s3_endpoint=_endpoint(environ, "STORAGE_S3_ENDPOINT"),
            s3_region=_text(environ, "STORAGE_S3_REGION") or "us-east-1",
            s3_path_style=_boolean(environ, "STORAGE_S3_PATH_STYLE", default=False),
            azure_endpoint=_endpoint(environ, "STORAGE_AZURE_ENDPOINT"),
        )


def store_for(uri: str) -> ObjectStore:
    """The process's store for the scheme of `uri`; ValueError for a URI of no known scheme."""
    return store_for_scheme(scheme_of(uri))


def store_for_scheme(scheme: str) -> ObjectStore:
    """The process's store for `scheme`, built from the environment the first time it is asked for."""
    _check_scheme(scheme)
    store = _STORES.get(scheme)
    if store is not None:
        return store
    with _LOCK:
        store = _STORES.get(scheme)
        if store is None:
            store = _BUILDERS[scheme]()
            _STORES[scheme] = store
        return store


def set_store(scheme: str, store: ObjectStore | None) -> None:
    """Install `store` as the process's store for `scheme` (the test seam); None removes the installed
    store, so the next use builds one from the environment."""
    _check_scheme(scheme)
    if store is not None and store.scheme not in (scheme, "*"):
        raise ValueError(f"a store of scheme {store.scheme!r} cannot serve {scheme}:// URIs")
    with _LOCK:
        if store is None:
            _STORES.pop(scheme, None)
        else:
            _STORES[scheme] = store


def reset_stores() -> None:
    """Forget every store, installed or built: each scheme's next use builds one again."""
    with _LOCK:
        _STORES.clear()


def _check_scheme(scheme: str) -> None:
    if scheme not in SCHEMES:
        raise ValueError(f"unknown storage scheme {scheme!r}: expected one of {', '.join(SCHEMES)}")


def _build_gcs() -> ObjectStore:
    from midstream.lakehouse.store.gcs import GcsStore

    # `STORAGE_EMULATOR_HOST` points google-cloud-storage at an emulator; fake-gcs-server cannot
    # answer the library's batch requests, so deletes go one per request there (`gcs.py`).
    return GcsStore(batch_deletes=_text(os.environ, "STORAGE_EMULATOR_HOST") is None)


def _build_s3() -> ObjectStore:
    from midstream.lakehouse.store.s3 import S3Store

    return S3Store.from_environment()


def _build_azure() -> ObjectStore:
    from midstream.lakehouse.store.azure import AzureBlobStore

    return AzureBlobStore.from_environment()


def _build_file() -> ObjectStore:
    from midstream.lakehouse.store.file import FileStore

    return FileStore()


_BUILDERS: dict[str, Callable[[], ObjectStore]] = {
    "gs": _build_gcs,
    "s3": _build_s3,
    "abfss": _build_azure,
    "file": _build_file,
}


def _text(environ: Mapping[str, str], name: str) -> str | None:
    value = environ.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _boolean(environ: Mapping[str, str], name: str, *, default: bool) -> bool:
    value = _text(environ, name)
    if value is None:
        return default
    normalized = value.lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    raise ValueError(f"{name} must be true or false (also 1/0, yes/no, on/off), not {value!r}")


def _endpoint(environ: Mapping[str, str], name: str) -> str | None:
    value = _text(environ, name)
    if value is None:
        return None
    endpoint = value.rstrip("/")
    scheme, separator, host = endpoint.partition("://")
    if not separator or scheme.lower() not in ("http", "https") or not host:
        raise ValueError(f"{name} must be an http:// or https:// URL, not {value!r}")
    return endpoint
