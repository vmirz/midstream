"""A PyIceberg FileIO over the google-cloud-storage client.

PyIceberg reads table metadata, manifest lists and manifests through a FileIO, and writes manifests
and manifest lists through one when it commits. Using the same storage client as everything else
gives one set of credentials and one retry policy, and lets tests put an in-memory bucket behind
the whole catalog. Data files never go through here: the runtime uploads them itself.

Reads download the whole object, which suits metadata files (kilobytes to a few megabytes).
"""

from __future__ import annotations

import io
from types import TracebackType
from typing import Any

from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile, OutputStream
from pyiceberg.typedef import EMPTY_DICT, Properties

from midstream.lakehouse.gcs import (
    is_not_found,
    is_precondition_failed,
    parse_gcs_uri,
    shared_storage_client,
)


class _BytesInputStream(io.BytesIO):
    """An in-memory, seekable copy of an object (satisfies PyIceberg's InputStream)."""

    def __enter__(self) -> "_BytesInputStream":
        return self

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None,
    ) -> None:
        self.close()


class GcsInputFile(InputFile):
    def __init__(self, location: str, client: Any) -> None:
        super().__init__(location)
        self._client = client
        self._size: int | None = None

    def _blob(self):
        bucket, name = parse_gcs_uri(self.location)
        return self._client.bucket(bucket).blob(name)

    def __len__(self) -> int:
        if self._size is None:
            bucket, name = parse_gcs_uri(self.location)
            blob = self._client.bucket(bucket).get_blob(name)
            if blob is None:
                raise FileNotFoundError(self.location)
            self._size = int(blob.size)
        return self._size

    def exists(self) -> bool:
        bucket, name = parse_gcs_uri(self.location)
        return self._client.bucket(bucket).get_blob(name) is not None

    def open(self, seekable: bool = True) -> InputStream:
        try:
            data = self._blob().download_as_bytes()
        except Exception as exc:
            if is_not_found(exc):
                raise FileNotFoundError(self.location) from exc
            raise
        self._size = len(data)
        return _BytesInputStream(data)


class _UploadStream(io.BytesIO):
    """Buffers what PyIceberg writes and uploads it when closed."""

    def __init__(self, output: "GcsOutputFile", *, overwrite: bool) -> None:
        super().__init__()
        self._output = output
        self._overwrite = overwrite
        self._uploaded = False

    def close(self) -> None:
        if not self._uploaded and not self.closed:
            data = self.getvalue()
            self._output._upload(data, overwrite=self._overwrite)
            self._uploaded = True
        super().close()

    def __enter__(self) -> "_UploadStream":
        return self

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None,
    ) -> None:
        if exctype is not None:
            # Do not upload a half-written file.
            self._uploaded = True
        self.close()


class GcsOutputFile(OutputFile):
    def __init__(self, location: str, client: Any) -> None:
        super().__init__(location)
        self._client = client
        self._written: int | None = None

    def __len__(self) -> int:
        if self._written is not None:
            return self._written
        return len(self.to_input_file())

    def exists(self) -> bool:
        return self.to_input_file().exists()

    def to_input_file(self) -> InputFile:
        return GcsInputFile(self.location, self._client)

    def create(self, overwrite: bool = False) -> OutputStream:
        if not overwrite and self.exists():
            raise FileExistsError(self.location)
        return _UploadStream(self, overwrite=overwrite)

    def _upload(self, data: bytes, *, overwrite: bool) -> None:
        bucket, name = parse_gcs_uri(self.location)
        blob = self._client.bucket(bucket).blob(name)
        try:
            if overwrite:
                blob.upload_from_string(data, content_type=_content_type(name))
            else:
                blob.upload_from_string(data, content_type=_content_type(name), if_generation_match=0)
        except Exception as exc:
            if is_precondition_failed(exc):
                raise FileExistsError(self.location) from exc
            raise
        self._written = len(data)


class GcsFileIO(FileIO):
    """FileIO for `gs://` locations through one shared google-cloud-storage client."""

    def __init__(self, properties: Properties = EMPTY_DICT, client: Any = None) -> None:
        super().__init__(properties=properties)
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = shared_storage_client()
        return self._client

    def new_input(self, location: str) -> InputFile:
        return GcsInputFile(location, self.client)

    def new_output(self, location: str) -> OutputFile:
        return GcsOutputFile(location, self.client)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        uri = location if isinstance(location, str) else location.location
        bucket, name = parse_gcs_uri(uri)
        try:
            self.client.bucket(bucket).blob(name).delete()
        except Exception as exc:
            if is_not_found(exc):
                raise FileNotFoundError(uri) from exc
            raise


def _content_type(name: str) -> str:
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".avro"):
        return "avro/binary"
    return "application/octet-stream"
