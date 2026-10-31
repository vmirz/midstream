"""The object store layer: one interface to every storage backend a table may live in.

`store_for(uri)` returns the process's store for the URI's scheme - `gs://` Google Cloud Storage,
`s3://` Amazon S3 or an S3-compatible server, `abfss://` Azure Blob Storage, `file:///` the local
filesystem (tests) - each implementing `ObjectStore` (`base.py`). `check.py` verifies a live store's
semantics: `python -m midstream.lakehouse.store.check <uri>`.

Importing this package imports no cloud SDK: each backend's store is imported when a store of its
scheme is first built (`registry.py`). `GcsStore` is exported from `midstream.lakehouse.store.gcs`.
"""

from __future__ import annotations

from midstream.lakehouse.store.base import (
    ABSENT,
    Absent,
    DeleteFailure,
    DeleteOutcome,
    ObjectInfo,
    ObjectNotFoundError,
    ObjectStore,
    PreconditionFailedError,
    StoreConfigurationError,
    StoreError,
    is_retryable_status,
)
from midstream.lakehouse.store.file import FileStore
from midstream.lakehouse.store.memory import MemoryStore
from midstream.lakehouse.store.registry import StoreSettings, reset_stores, set_store, store_for, store_for_scheme
from midstream.lakehouse.store.uris import join, parse_object_uri, scheme_of

__all__ = [
    "ABSENT",
    "Absent",
    "DeleteFailure",
    "DeleteOutcome",
    "FileStore",
    "MemoryStore",
    "ObjectInfo",
    "ObjectNotFoundError",
    "ObjectStore",
    "PreconditionFailedError",
    "StoreConfigurationError",
    "StoreError",
    "StoreSettings",
    "is_retryable_status",
    "join",
    "parse_object_uri",
    "reset_stores",
    "scheme_of",
    "set_store",
    "store_for",
    "store_for_scheme",
]
