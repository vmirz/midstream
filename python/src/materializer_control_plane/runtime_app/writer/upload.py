"""Uploading a sealed batch's files to the table's `data/` location (spec §7.4).

Blocking calls, made from the batch pipeline's upload threads so the network round trips never
hold the runtime's event loop. Each file goes to its final object name, create-only
(`if_generation_match=0`); every name carries its seal's id, so an object already there is this very
file from an earlier attempt, and a matching size makes the upload done. A file is in the table only
once a commit adds it: an uploaded file no commit takes is an orphan, removed by orphan cleanup.
"""

from __future__ import annotations

from dataclasses import replace
import logging
from typing import Any

from materializer_control_plane.lakehouse.gcs import is_precondition_failed, parse_gcs_uri, shared_storage_client
from materializer_control_plane.runtime_app.config import RuntimeAppConfig
from materializer_control_plane.runtime_app.models import RuntimeBatchState, SealedFile

LOGGER = logging.getLogger(__name__)

#: Ceiling on the wait between two attempts at sealing or uploading the same batch.
MAX_RETRY_BACKOFF_SECONDS = 300


def upload_sealed_file(item: SealedFile, *, client: Any = None) -> SealedFile:
    """Upload one file; returns it marked uploaded."""
    storage_client = shared_storage_client() if client is None else client
    bucket_name, name = parse_gcs_uri(item.uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(name)
    try:
        blob.upload_from_filename(item.local_path, if_generation_match=0)
    except Exception as exc:
        if not is_precondition_failed(exc):
            raise
        existing = bucket.get_blob(name)
        if existing is None or int(existing.size or -1) != item.file_size:
            raise RuntimeError(f"a different object already exists at {item.uri}") from exc
        LOGGER.info("Data file %s was already uploaded; counting it as done.", item.uri)
    return replace(item, uploaded=True)


def upload_batch_files(batch: RuntimeBatchState, *, client: Any = None) -> tuple[SealedFile, ...]:
    """Upload every file of a sealed batch not uploaded yet; returns them all, marked uploaded."""
    return tuple(item if item.uploaded else upload_sealed_file(item, client=client) for item in batch.files)


def retry_backoff_seconds(attempts: int, *, config: RuntimeAppConfig) -> int:
    """How long to wait after the `attempts`-th failed attempt before the next one.

    Doubles from `upload_initial_backoff_seconds` up to `MAX_RETRY_BACKOFF_SECONDS`, and never gives
    up: a batch that is not in the table holds its partition's committed offset.
    """
    exponent = max(attempts - 1, 0)
    if exponent >= 16:
        return MAX_RETRY_BACKOFF_SECONDS
    return min(config.upload_initial_backoff_seconds * (2**exponent), MAX_RETRY_BACKOFF_SECONDS)
