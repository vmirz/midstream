"""Uploading a sealed batch's files to the table's `data/` location (spec §7.4).

Blocking calls, made from the batch pipeline's upload threads so the network round trips never
hold the runtime's event loop. Each file goes to its final object name, create-only (the store's
`if_version=ABSENT`); every name carries its seal's id, so an object already there is this very
file - from an earlier attempt, or this one's write that landed before its retry was refused - and
a matching size makes the upload done. A file is in the table only once a commit adds it: an
uploaded file no commit takes is an orphan, removed by orphan cleanup.
"""

from __future__ import annotations

from dataclasses import replace
import logging

from midstream.lakehouse.store import ABSENT, ObjectStore, PreconditionFailedError, store_for
from midstream.runtime_app.config import RuntimeAppConfig
from midstream.runtime_app.models import RuntimeBatchState, SealedFile

LOGGER = logging.getLogger(__name__)

#: Ceiling on the wait between two attempts at sealing or uploading the same batch.
MAX_RETRY_BACKOFF_SECONDS = 300


def upload_sealed_file(item: SealedFile, *, store: ObjectStore | None = None) -> SealedFile:
    """Upload one file through `store`, else its URI's own; returns it marked uploaded."""
    object_store = store_for(item.uri) if store is None else store
    try:
        object_store.put_file(item.uri, item.local_path, if_version=ABSENT)
    except PreconditionFailedError as exc:
        existing = object_store.head(item.uri)
        if existing is None or existing.size != item.file_size:
            raise RuntimeError(f"a different object already exists at {item.uri}") from exc
        LOGGER.info("Data file %s was already uploaded; counting it as done.", item.uri)
    return replace(item, uploaded=True)


def upload_batch_files(batch: RuntimeBatchState, *, store: ObjectStore | None = None) -> tuple[SealedFile, ...]:
    """Upload every file of a sealed batch not uploaded yet; returns them all, marked uploaded."""
    return tuple(item if item.uploaded else upload_sealed_file(item, store=store) for item in batch.files)


def retry_backoff_seconds(attempts: int, *, config: RuntimeAppConfig) -> int:
    """How long to wait after the `attempts`-th failed attempt before the next one.

    Doubles from `upload_initial_backoff_seconds` up to `MAX_RETRY_BACKOFF_SECONDS`, and never gives
    up: a batch that is not in the table holds its partition's committed offset.
    """
    exponent = max(attempts - 1, 0)
    if exponent >= 16:
        return MAX_RETRY_BACKOFF_SECONDS
    return min(config.upload_initial_backoff_seconds * (2**exponent), MAX_RETRY_BACKOFF_SECONDS)
