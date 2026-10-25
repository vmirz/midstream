"""Google Cloud Storage helpers shared by the catalog, the runtime and the control plane.

Everything here is blocking: callers on an event loop run it on a worker thread.
"""

from __future__ import annotations

import threading
from typing import Any
from urllib.parse import urlparse

_SHARED_CLIENT: Any = None
_SHARED_CLIENT_LOCK = threading.Lock()


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Split `gs://bucket/path` into the bucket and the object name (no leading slash)."""
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ValueError(f"invalid GCS URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def gcs_uri(bucket: str, name: str) -> str:
    return f"gs://{bucket}/{name.lstrip('/')}"


def shared_storage_client() -> Any:
    """One GCS client per process, safe to share across threads."""
    global _SHARED_CLIENT
    with _SHARED_CLIENT_LOCK:
        if _SHARED_CLIENT is None:
            from google.cloud import storage

            _SHARED_CLIENT = storage.Client()
        return _SHARED_CLIENT


def set_shared_storage_client(client: Any) -> None:
    """Install the process's client; tests use it to put a fake in place."""
    global _SHARED_CLIENT
    with _SHARED_CLIENT_LOCK:
        _SHARED_CLIENT = client


def is_precondition_failed(exc: BaseException) -> bool:
    """Whether `exc` is GCS's 412: a generation precondition did not hold."""
    try:
        from google.api_core.exceptions import PreconditionFailed
    except Exception:  # pragma: no cover - the library is a hard dependency
        return type(exc).__name__ == "PreconditionFailed"
    return isinstance(exc, PreconditionFailed) or type(exc).__name__ == "PreconditionFailed"


def is_not_found(exc: BaseException) -> bool:
    """Whether `exc` is GCS's 404 (or a fake's `FileNotFoundError`)."""
    if isinstance(exc, FileNotFoundError):
        return True
    try:
        from google.api_core.exceptions import NotFound
    except Exception:  # pragma: no cover
        return type(exc).__name__ == "NotFound"
    return isinstance(exc, NotFound) or type(exc).__name__ == "NotFound"


def delete_object(client: Any, uri: str, *, if_generation_match: int | None = None) -> bool:
    """Delete one object; False when it was already gone."""
    bucket_name, name = parse_gcs_uri(uri)
    blob = client.bucket(bucket_name).blob(name)
    try:
        if if_generation_match is None:
            blob.delete()
        else:
            blob.delete(if_generation_match=if_generation_match)
    except Exception as exc:
        if is_not_found(exc):
            return False
        raise
    return True


def upload_bytes_create_only(
    client: Any,
    uri: str,
    data: bytes,
    *,
    content_type: str = "application/octet-stream",
) -> int | None:
    """Create an object that must not exist yet; return its generation.

    Raises the client's `PreconditionFailed` when the object is already there.
    """
    bucket_name, name = parse_gcs_uri(uri)
    blob = client.bucket(bucket_name).blob(name)
    blob.upload_from_string(data, content_type=content_type, if_generation_match=0)
    generation = getattr(blob, "generation", None)
    return None if generation is None else int(generation)


def download_bytes(client: Any, uri: str, *, start: int | None = None, end: int | None = None) -> bytes:
    """Download an object, or the inclusive byte range `start..end` of it."""
    bucket_name, name = parse_gcs_uri(uri)
    blob = client.bucket(bucket_name).blob(name)
    if start is None and end is None:
        return blob.download_as_bytes()
    return blob.download_as_bytes(start=start, end=end)


def object_size(client: Any, uri: str) -> int | None:
    """The object's size in bytes, or None when it does not exist."""
    bucket_name, name = parse_gcs_uri(uri)
    blob = client.bucket(bucket_name).get_blob(name)
    if blob is None:
        return None
    size = getattr(blob, "size", None)
    return None if size is None else int(size)
