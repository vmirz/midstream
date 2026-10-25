"""`GcsPointerCatalog`: a PyIceberg catalog whose only state is one pointer object per table.

Both planes embed it. The runtime's committer uses it for every write to its table; the control
plane uses it to create and drop tables and to serve the Iceberg REST facade. Which tables exist,
and where, comes from an injected `TableDirectory`: the control plane's state table, or, in a
runtime, its one table.

Commit protocol (spec §5.2): read the pointer and its generation; validate the requirements against
the metadata it names; write the new metadata file create-only; replace the pointer on the
condition that its generation has not moved. A moved pointer raises `CommitFailedException`, and
the metadata file just written is deleted.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import logging
from typing import Any, Protocol

from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    NoSuchViewError,
    TableAlreadyExistsError,
    ValidationException,
)
from pyiceberg.io import FileIO
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromByteStream
from pyiceberg.table import CommitTableResponse, Table, Transaction
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import AssertTableUUID
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties

from midstream.lakehouse.gcs import (
    delete_object,
    download_bytes,
    is_precondition_failed,
    shared_storage_client,
    upload_bytes_create_only,
)
from midstream.lakehouse.io import GcsFileIO
from midstream.lakehouse.pointer import (
    PointerConflictError,
    TablePointer,
    delete_pointer,
    read_pointer,
    utc_now,
    write_pointer,
    write_version_hint,
)
from midstream.lakehouse.tables import PROPERTY_MATERIALIZATION_ID

LOGGER = logging.getLogger(__name__)

DEFAULT_CATALOG_NAME = "materializer"


class ForeignTableError(RuntimeError):
    """The pointer at a materialization's location belongs to another materialization."""


@dataclass(frozen=True, slots=True)
class TableEntry:
    """What a directory knows about one table."""

    identifier: tuple[str, ...]
    location: str
    materialization_id: str


class TableDirectory(Protocol):
    """Which tables exist and where. The catalog itself only ever reads and writes pointers."""

    def entry(self, identifier: tuple[str, ...]) -> TableEntry | None: ...

    def entries(self, namespace: tuple[str, ...] | None = None) -> list[TableEntry]: ...


class StaticTableDirectory:
    """A fixed set of tables: a runtime's one table, or tests."""

    def __init__(self, entries: Iterable[TableEntry] = ()) -> None:
        self._entries: dict[tuple[str, ...], TableEntry] = {}
        for item in entries:
            self.add(item)

    def add(self, entry: TableEntry) -> None:
        self._entries[_normalized(entry.identifier)] = entry

    def remove(self, identifier: tuple[str, ...]) -> None:
        self._entries.pop(_normalized(identifier), None)

    def entry(self, identifier: tuple[str, ...]) -> TableEntry | None:
        return self._entries.get(_normalized(identifier))

    def entries(self, namespace: tuple[str, ...] | None = None) -> list[TableEntry]:
        values = list(self._entries.values())
        if namespace is None:
            return values
        wanted = _normalized(namespace)
        return [item for item in values if _normalized(item.identifier[:-1]) == wanted]


class GcsPointerCatalog(MetastoreCatalog):
    """A catalog over `<location>/metadata/catalog-pointer.json` objects."""

    def __init__(
        self,
        name: str = DEFAULT_CATALOG_NAME,
        *,
        directory: TableDirectory,
        client: Any = None,
        writer: str = "unknown",
        clock: Callable[[], str] = utc_now,
        **properties: str,
    ) -> None:
        super().__init__(name, **properties)
        self._directory = directory
        self._client = client
        self.writer = writer
        self._clock = clock

    # -- plumbing --------------------------------------------------------------------------------

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = shared_storage_client()
        return self._client

    @property
    def directory(self) -> TableDirectory:
        return self._directory

    def _load_file_io(self, properties: Properties = EMPTY_DICT, location: str | None = None) -> FileIO:
        return GcsFileIO(properties=dict(self.properties) | dict(properties), client=self.client)

    def _entry(self, identifier: str | Identifier) -> TableEntry:
        entry = self._directory.entry(_normalized(self.identifier_to_tuple(identifier)))
        if entry is None:
            raise NoSuchTableError(f"Table does not exist: {'.'.join(self.identifier_to_tuple(identifier))}")
        return entry

    def read_metadata(self, metadata_location: str) -> TableMetadata:
        import io as _io

        raw = download_bytes(self.client, metadata_location)
        return FromByteStream.table_metadata(_io.BytesIO(raw))

    def _table_from_pointer(self, entry: TableEntry, pointer: TablePointer) -> Table:
        if pointer.materialization_id != entry.materialization_id:
            raise ForeignTableError(
                f"the table at {entry.location} belongs to materialization {pointer.materialization_id}, "
                f"not {entry.materialization_id}"
            )
        metadata = self.read_metadata(pointer.metadata_location)
        return Table(
            identifier=entry.identifier,
            metadata=metadata,
            metadata_location=pointer.metadata_location,
            io=self._load_file_io(metadata.properties, pointer.metadata_location),
            catalog=self,
        )

    def load_pointer(self, identifier: str | Identifier) -> tuple[TablePointer | None, int]:
        """The table's pointer and its generation, without reading the metadata it names."""
        entry = self._entry(identifier)
        return read_pointer(self.client, entry.location)

    # -- reads -----------------------------------------------------------------------------------

    def load_table(self, identifier: str | Identifier) -> Table:
        entry = self._entry(identifier)
        pointer, _ = read_pointer(self.client, entry.location)
        if pointer is None:
            raise NoSuchTableError(f"Table does not exist: {'.'.join(entry.identifier)}")
        return self._table_from_pointer(entry, pointer)

    def table_exists(self, identifier: str | Identifier) -> bool:
        try:
            pointer, _ = self.load_pointer(identifier)
        except NoSuchTableError:
            return False
        return pointer is not None

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        wanted = self.identifier_to_tuple(namespace)
        if not self.namespace_exists(wanted):
            raise NoSuchNamespaceError(f"Namespace does not exist: {'.'.join(wanted)}")
        return sorted(item.identifier for item in self._directory.entries(wanted))

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        prefix = self.identifier_to_tuple(namespace)
        namespaces = {item.identifier[:-1] for item in self._directory.entries()}
        configured = self.properties.get("namespace")
        if configured:
            namespaces.add((configured,))
        if prefix:
            return sorted(ns for ns in namespaces if ns[: len(prefix)] == prefix and ns != prefix)
        return sorted(namespaces)

    def namespace_exists(self, namespace: str | Identifier) -> bool:
        wanted = _normalized(self.identifier_to_tuple(namespace))
        return any(_normalized(ns) == wanted for ns in self.list_namespaces())

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        if not self.namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")
        return {}

    # -- writes ----------------------------------------------------------------------------------

    def create_table_from_metadata(
        self,
        identifier: tuple[str, ...],
        *,
        location: str,
        materialization_id: str,
        metadata: TableMetadata,
    ) -> Table:
        """Create a table whose metadata version 0 is `metadata`, field ids kept as they are.

        Idempotent for one materialization: a pointer already naming `materialization_id` is that
        table, created by an earlier attempt, and is returned. A pointer naming any other
        materialization raises `TableAlreadyExistsError`.
        """
        entry = TableEntry(identifier=tuple(identifier), location=location, materialization_id=materialization_id)
        existing, _ = read_pointer(self.client, location)
        if existing is not None:
            if existing.materialization_id != materialization_id:
                raise TableAlreadyExistsError(
                    f"{location} already holds the table of materialization {existing.materialization_id}"
                )
            return self._table_from_pointer(entry, existing)
        metadata_location = f"{location.rstrip('/')}/metadata/00000-{metadata.table_uuid}.metadata.json"
        try:
            upload_bytes_create_only(
                self.client,
                metadata_location,
                metadata.model_dump_json(exclude_none=True).encode("utf-8"),
                content_type="application/json",
            )
        except Exception as exc:
            if not is_precondition_failed(exc):
                raise
            # An earlier attempt wrote this very file (same uuid) and died before the pointer.
        pointer = TablePointer(
            table_uuid=str(metadata.table_uuid),
            identifier=tuple(identifier),
            materialization_id=materialization_id,
            location=location.rstrip("/"),
            metadata_location=metadata_location,
            previous_metadata_location=None,
            sequence=0,
            updated_at=self._clock(),
            updated_by=self.writer,
        )
        try:
            write_pointer(self.client, location, pointer, generation=0)
        except PointerConflictError:
            winner, _ = read_pointer(self.client, location)
            if winner is None or winner.materialization_id != materialization_id:
                raise TableAlreadyExistsError(f"{location} was taken while creating its table") from None
            return self._table_from_pointer(entry, winner)
        write_version_hint(self.client, location, metadata_location)
        return self._table_from_pointer(entry, pointer)

    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema | Any,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        raise NotImplementedError("tables are created by creating a materialization")

    def commit_table(self, table: Table, requirements, updates) -> CommitTableResponse:
        entry = self._entry(table.name())
        pointer, generation = read_pointer(self.client, entry.location)
        if pointer is None:
            raise NoSuchTableError(f"Table does not exist: {'.'.join(entry.identifier)}")
        current = self._table_from_pointer(entry, pointer)
        staged = self._update_and_stage_table(current, entry.identifier, requirements, updates)
        if staged.metadata == current.metadata:
            return CommitTableResponse(metadata=current.metadata, metadata_location=current.metadata_location)
        try:
            upload_bytes_create_only(
                self.client,
                staged.metadata_location,
                staged.metadata.model_dump_json(exclude_none=True).encode("utf-8"),
                content_type="application/json",
            )
        except Exception as exc:
            if is_precondition_failed(exc):
                raise CommitFailedException(f"metadata file {staged.metadata_location} already exists") from exc
            raise
        try:
            write_pointer(
                self.client,
                entry.location,
                pointer.advanced(staged.metadata_location, updated_by=self.writer, updated_at=self._clock()),
                generation=generation,
            )
        except PointerConflictError as exc:
            delete_object(self.client, staged.metadata_location)
            raise CommitFailedException(str(exc)) from exc
        write_version_hint(self.client, entry.location, staged.metadata_location)
        return CommitTableResponse(metadata=staged.metadata, metadata_location=staged.metadata_location)

    def drop_table(self, identifier: str | Identifier) -> None:
        """Remove the table from the catalog: its pointer. Data and metadata are left in place."""
        entry = self._entry(identifier)
        if not delete_pointer(self.client, entry.location):
            raise NoSuchTableError(f"Table does not exist: {'.'.join(entry.identifier)}")

    def purge_table(self, identifier: str | Identifier) -> None:
        raise NotImplementedError("tables are dropped by deleting their materialization")

    def register_table(self, identifier: str | Identifier, metadata_location: str, overwrite: bool = False) -> Table:
        raise NotImplementedError("tables are created by creating a materialization")

    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        raise NotImplementedError("a materialization's table cannot be renamed")

    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        raise NamespaceAlreadyExistsError("namespaces are fixed by the deployment")

    def drop_namespace(self, namespace: str | Identifier) -> None:
        raise NotImplementedError("namespaces are fixed by the deployment")

    def update_namespace_properties(self, namespace, removals=None, updates=EMPTY_DICT):
        raise NotImplementedError("namespace properties are fixed by the deployment")

    # -- views (none) ----------------------------------------------------------------------------

    def list_views(self, namespace: str | Identifier) -> list[Identifier]:
        return []

    def load_view(self, identifier: str | Identifier):
        raise NoSuchViewError(f"View does not exist: {identifier}")

    def view_exists(self, identifier: str | Identifier) -> bool:
        return False

    def drop_view(self, identifier: str | Identifier) -> None:
        raise NoSuchViewError(f"View does not exist: {identifier}")

    def register_view(self, identifier: str | Identifier, metadata_location: str):
        raise NotImplementedError("views are not supported")


def commit_once(transaction: Transaction) -> Table:
    """Commit a transaction exactly once, without PyIceberg's automatic retry.

    PyIceberg retries a `CommitFailedException` by refreshing the table and running the snapshot
    producers again with what they were given. For an append that would add the same files a
    second time if the commit it lost to was another committer's that already added them, so the
    committer refreshes and decides again itself instead. On failure the manifests written for the
    attempt are deleted; the caller's data files are left alone.

    Uses PyIceberg's transaction internals (pinned to 0.12.x; `test_lakehouse_catalog` exercises
    them).
    """
    if getattr(transaction, "_failed", False):
        raise RuntimeError("this transaction failed to commit and cannot be reused")
    table = transaction._table
    if not transaction._updates:
        return table
    requirements = transaction._requirements + (AssertTableUUID(uuid=transaction.table_metadata.table_uuid),)
    producers = list(transaction._snapshot_producers)
    try:
        table._do_commit(updates=transaction._updates, requirements=requirements)
    except (CommitFailedException, ValidationException):
        for producer in producers:
            try:
                producer._clean_all_uncommitted()
            except Exception:  # noqa: BLE001 - cleanup only; orphan cleanup collects leftovers
                LOGGER.debug("could not clean up uncommitted manifests", exc_info=True)
        transaction._failed = True
        raise
    except Exception:
        # Outcome unknown: the files may be referenced already, so nothing is deleted.
        transaction._failed = True
        raise
    for producer in producers:
        try:
            producer._cleanup_uncommitted()
        except Exception:  # noqa: BLE001
            LOGGER.debug("could not clean up superseded manifests", exc_info=True)
    transaction._snapshot_producers = []
    transaction._updates = ()
    transaction._requirements = ()
    return table


def _normalized(identifier: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(part.lower() for part in identifier)
