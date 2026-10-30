"""A PyIceberg FileIO over the object store layer: one implementation for every backend.

PyIceberg reads table metadata, manifest lists, manifests and - in a scan - data files through a
FileIO, and writes manifests and manifest lists through one when it commits. Going through the same
store as everything else (`lakehouse/store/`) gives one set of credentials and one retry policy per
backend, and lets tests put an in-memory store behind the whole catalog. The runtime uploads its
data files itself.

Each location is served by the store the FileIO was given (tests), else by the process's store for
the location's scheme (`store_for`). Reads fetch the whole object, which suits metadata files
(kilobytes to a few megabytes).
"""

from __future__ import annotations

import io
from types import TracebackType

from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile, OutputStream
from pyiceberg.typedef import EMPTY_DICT, Properties

from midstream.lakehouse.store import ABSENT, ObjectNotFoundError, ObjectStore, PreconditionFailedError, store_for


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


class ObjectStoreInputFile(InputFile):
    """One object to read: its size and existence from the store's `head`, its bytes from `get`."""

    def __init__(self, location: str, store: ObjectStore) -> None:
        super().__init__(location)
        self._store = store
        self._size: int | None = None

    def __len__(self) -> int:
        if self._size is None:
            info = self._store.head(self.location)
            if info is None:
                raise FileNotFoundError(self.location)
            self._size = info.size
        return self._size

    def exists(self) -> bool:
        return self._store.head(self.location) is not None

    def open(self, seekable: bool = True) -> InputStream:
        try:
            data, _ = self._store.get(self.location)
        except ObjectNotFoundError as exc:
            raise FileNotFoundError(self.location) from exc
        self._size = len(data)
        return _BytesInputStream(data)


class _UploadStream(io.BytesIO):
    """Buffers what PyIceberg writes and uploads it when closed."""

    def __init__(self, output: "ObjectStoreOutputFile", *, overwrite: bool) -> None:
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


class ObjectStoreOutputFile(OutputFile):
    """One object to write, whole, when PyIceberg closes its stream.

    `create(overwrite=False)` writes create-only: `FileExistsError` when the object is there, before
    the write or by the time it lands. `overwrite=True` writes unconditionally.
    """

    def __init__(self, location: str, store: ObjectStore) -> None:
        super().__init__(location)
        self._store = store
        self._written: int | None = None

    def __len__(self) -> int:
        if self._written is not None:
            return self._written
        return len(self.to_input_file())

    def exists(self) -> bool:
        return self.to_input_file().exists()

    def to_input_file(self) -> InputFile:
        return ObjectStoreInputFile(self.location, self._store)

    def create(self, overwrite: bool = False) -> OutputStream:
        if not overwrite and self.exists():
            raise FileExistsError(self.location)
        return _UploadStream(self, overwrite=overwrite)

    def _upload(self, data: bytes, *, overwrite: bool) -> None:
        try:
            self._store.put(
                self.location,
                data,
                if_version=None if overwrite else ABSENT,
                content_type=_content_type(self.location),
            )
        except PreconditionFailedError as exc:
            raise FileExistsError(self.location) from exc
        self._written = len(data)


class ObjectStoreFileIO(FileIO):
    """FileIO for every location a store serves: `store` when given, else the process's store for
    each location's scheme."""

    def __init__(self, properties: Properties = EMPTY_DICT, store: ObjectStore | None = None) -> None:
        super().__init__(properties=properties)
        self._store = store

    def store_for(self, location: str) -> ObjectStore:
        """The store that serves `location`."""
        return store_for(location) if self._store is None else self._store

    def new_input(self, location: str) -> InputFile:
        return ObjectStoreInputFile(location, self.store_for(location))

    def new_output(self, location: str) -> OutputFile:
        return ObjectStoreOutputFile(location, self.store_for(location))

    def delete(self, location: str | InputFile | OutputFile) -> None:
        uri = location if isinstance(location, str) else location.location
        if not self.store_for(uri).delete(uri):
            raise FileNotFoundError(uri)


def _content_type(name: str) -> str:
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".avro"):
        return "avro/binary"
    return "application/octet-stream"
