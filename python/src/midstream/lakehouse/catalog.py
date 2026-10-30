"""`PointerCatalog`: a PyIceberg catalog whose only state is one pointer object per table.

Both planes embed it. The runtime's committer uses it for every write to its table; the control
plane uses it to create and drop tables and to serve the Iceberg REST facade. Which tables exist,
and where, comes from an injected `TableDirectory`: the control plane's state table, or, in a
runtime, its one table. Every table is read and written through the object store of its location
(`lakehouse/store/`), so the catalog works the same on every backend.

Commit protocol (spec §5.2): read the pointer and its version; validate the requirements against
the metadata it names; write the new metadata file create-only; replace the pointer on the
condition that its version has not moved (the store's compare-and-swap). A moved pointer raises
`CommitFailedException`, and the metadata file just written is deleted.

A failed pointer write is not always a lost race. Every storage library retries a conditional
write whose answer was lost, and the retry of a write that landed is refused (design §3.3); a write
that got no answer at all may have landed too. So a failed write is checked against the pointer as
it is read back: when it, or a commit since, names the staged file, the commit landed and is
reported as one; only when it provably did not is the staged file deleted. When nobody can tell,
the commit's state is unknown (`CommitStateUnknownException`) and nothing is deleted. The staged
metadata file's own create-only write is checked the same way: its name carries a fresh uuid, so an
object already there of the size just sent is that write, landed.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import io
import logging
from typing import Any, Protocol

from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
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

from midstream.lakehouse.io import ObjectStoreFileIO
from midstream.lakehouse.pointer import (
    PointerConflictError,
    TablePointer,
    delete_pointer,
    read_pointer,
    utc_now,
    write_pointer,
    write_version_hint,
)
from midstream.lakehouse.store import ABSENT, ObjectStore, PreconditionFailedError, store_for
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


class PointerCatalog(MetastoreCatalog):
    """A catalog over `<location>/metadata/catalog-pointer.json` objects.

    `store` serves every table when given (tests); otherwise each table goes through the process's
    store for the scheme of its location (`store_for`).
    """

    def __init__(
        self,
        name: str = DEFAULT_CATALOG_NAME,
        *,
        directory: TableDirectory,
        store: ObjectStore | None = None,
        writer: str = "unknown",
        clock: Callable[[], str] = utc_now,
        **properties: str,
    ) -> None:
        super().__init__(name, **properties)
        self._directory = directory
        self._store = store
        self.writer = writer
        self._clock = clock

    # -- plumbing --------------------------------------------------------------------------------

    @property
    def store(self) -> ObjectStore | None:
        """The store given to the catalog; None when each location's own store serves it."""
        return self._store

    def store_for(self, location: str) -> ObjectStore:
        """The store that serves `location`."""
        return store_for(location) if self._store is None else self._store

    @property
    def directory(self) -> TableDirectory:
        return self._directory

    def _load_file_io(self, properties: Properties = EMPTY_DICT, location: str | None = None) -> FileIO:
        return ObjectStoreFileIO(properties=dict(self.properties) | dict(properties), store=self._store)

    def _entry(self, identifier: str | Identifier) -> TableEntry:
        entry = self._directory.entry(_normalized(self.identifier_to_tuple(identifier)))
        if entry is None:
            raise NoSuchTableError(f"Table does not exist: {'.'.join(self.identifier_to_tuple(identifier))}")
        return entry

    def read_metadata(self, metadata_location: str) -> TableMetadata:
        raw, _ = self.store_for(metadata_location).get(metadata_location)
        return FromByteStream.table_metadata(io.BytesIO(raw))

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

    def load_pointer(self, identifier: str | Identifier) -> tuple[TablePointer | None, str | None]:
        """The table's pointer and its version, without reading the metadata it names."""
        entry = self._entry(identifier)
        return read_pointer(self.store_for(entry.location), entry.location)

    # -- reads -----------------------------------------------------------------------------------

    def load_table(self, identifier: str | Identifier) -> Table:
        entry = self._entry(identifier)
        pointer, _ = read_pointer(self.store_for(entry.location), entry.location)
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
        materialization raises `TableAlreadyExistsError`. A pointer write refused while there is
        still no pointer raises `CommitStateUnknownException`: a write in flight may land yet, and
        another attempt finds out.
        """
        entry = TableEntry(identifier=tuple(identifier), location=location, materialization_id=materialization_id)
        store = self.store_for(location)
        existing, _ = read_pointer(store, location)
        if existing is not None:
            if existing.materialization_id != materialization_id:
                raise TableAlreadyExistsError(
                    f"{location} already holds the table of materialization {existing.materialization_id}"
                )
            return self._table_from_pointer(entry, existing)
        metadata_location = f"{location.rstrip('/')}/metadata/00000-{metadata.table_uuid}.metadata.json"
        try:
            store.put(
                metadata_location,
                metadata.model_dump_json(exclude_none=True).encode("utf-8"),
                if_version=ABSENT,
                content_type="application/json",
            )
        except PreconditionFailedError:
            # An earlier attempt wrote this very file (same uuid) and died before the pointer, or
            # this attempt's write landed and its retry was refused.
            pass
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
            write_pointer(store, location, pointer, expected_version=None)
        except PointerConflictError as exc:
            # Another attempt created it first - or this one did, and its retry was refused.
            winner, _ = read_pointer(store, location)
            if winner is None:
                # Refused, yet there is none: a conflicting write is still in flight (S3 refuses a
                # conditional write meeting another, which may be this one's own first attempt).
                raise CommitStateUnknownException(
                    f"{location} refused the creation of its pointer ({exc}) but holds none: a conflicting write "
                    "may still be in flight"
                ) from exc
            if winner.materialization_id != materialization_id:
                raise TableAlreadyExistsError(f"{location} was taken while creating its table") from None
            if winner.metadata_location == metadata_location:
                write_version_hint(store, location, metadata_location)
            return self._table_from_pointer(entry, winner)
        write_version_hint(store, location, metadata_location)
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
        store = self.store_for(entry.location)
        pointer, version = read_pointer(store, entry.location)
        if pointer is None:
            raise NoSuchTableError(f"Table does not exist: {'.'.join(entry.identifier)}")
        current = self._table_from_pointer(entry, pointer)
        staged = self._update_and_stage_table(current, entry.identifier, requirements, updates)
        if staged.metadata == current.metadata:
            return CommitTableResponse(metadata=current.metadata, metadata_location=current.metadata_location)
        payload = staged.metadata.model_dump_json(exclude_none=True).encode("utf-8")
        try:
            store.put(staged.metadata_location, payload, if_version=ABSENT, content_type="application/json")
        except PreconditionFailedError as exc:
            # Only this commit's own write, retried after its answer was lost, can have put an
            # object at a name with a fresh uuid in it; one of another size is not that write.
            existing = store.head(staged.metadata_location)
            if existing is None or existing.size != len(payload):
                raise CommitFailedException(f"metadata file {staged.metadata_location} already exists") from exc
        try:
            write_pointer(
                store,
                entry.location,
                pointer.advanced(staged.metadata_location, updated_by=self.writer, updated_at=self._clock()),
                expected_version=version,
            )
        except Exception as exc:  # noqa: BLE001 - refused or unanswered: the pointer read back tells
            landed = self._landed(
                store,
                entry,
                staged.metadata_location,
                started_from=pointer.metadata_location,
                expected_version=version,
                failure=exc,
            )
            if landed is None:
                store.delete(staged.metadata_location)
                raise CommitFailedException(str(exc)) from exc
            LOGGER.info(
                "The pointer write of %s failed after it landed (%s); %s is committed",
                ".".join(entry.identifier),
                exc,
                staged.metadata_location,
            )
            if landed.metadata_location != staged.metadata_location:
                # A commit since has moved the pointer on, and named its own metadata in the hint.
                return CommitTableResponse(metadata=staged.metadata, metadata_location=staged.metadata_location)
        write_version_hint(store, entry.location, staged.metadata_location)
        return CommitTableResponse(metadata=staged.metadata, metadata_location=staged.metadata_location)

    def _landed(
        self,
        store: ObjectStore,
        entry: TableEntry,
        staged_location: str,
        *,
        started_from: str,
        expected_version: str,
        failure: BaseException,
    ) -> TablePointer | None:
        """The pointer as read back if a pointer write that failed landed all the same; None if it
        provably did not.

        The write's answer may have been lost and its retry refused, as the retry of a write that
        landed is, or the write may have got no answer at all. Every pointer moves on from the one
        before it, so the write landed if the pointer
        names the staged metadata file, having moved from the one this commit started from, or if a
        commit since has moved on from the staged file; and it did not if another commit moved the
        pointer on from where this one started. Further on, the current metadata's log tells, as far
        as it reaches back (`write.metadata.previous-versions-max` entries).

        When nobody can tell, `CommitStateUnknownException`, and nothing is deleted: the pointer or
        that metadata cannot be read, the log no longer reaches back to where this commit started,
        or the pointer has not moved at all - a write with no answer may still land, and S3 refuses
        a conditional write that meets another in flight (409), which may be this write's own first
        attempt.
        """
        table = ".".join(entry.identifier)
        try:
            now, version = read_pointer(store, entry.location)
        except Exception as error:
            raise CommitStateUnknownException(
                f"the pointer write of {table} failed ({failure}) and the pointer could not be read back: {error}"
            ) from error
        if now is None:
            # Dropped: nothing reads the staged file any more.
            return None
        if version == expected_version:
            raise CommitStateUnknownException(
                f"the pointer write of {table} failed ({failure}) and the pointer is still at version "
                f"{expected_version}: that write, or a conflicting one, may still be in flight"
            )
        if now.metadata_location == staged_location:
            if now.previous_metadata_location != started_from:
                # Only this commit names its staged file: never delete what the pointer names.
                raise CommitStateUnknownException(
                    f"the pointer of {table} names {staged_location} after {now.previous_metadata_location}, "
                    f"not {started_from}, where this commit started"
                )
            return now
        if now.previous_metadata_location == staged_location:
            return now
        if started_from in (now.metadata_location, now.previous_metadata_location):
            # Another commit moved on from where this one started: this write never landed.
            return None
        try:
            metadata = self.read_metadata(now.metadata_location)
        except Exception as error:
            raise CommitStateUnknownException(
                f"the pointer write of {table} failed ({failure}) and the metadata {now.metadata_location} "
                f"could not be read: {error}"
            ) from error
        log = [item.metadata_file for item in metadata.metadata_log]
        if staged_location in log:
            return now
        if started_from in log:
            # The log reaches back to where this commit started, and the staged file is not in it.
            return None
        raise CommitStateUnknownException(
            f"the pointer write of {table} failed ({failure}) and the pointer has moved on too far to tell "
            f"whether it landed: the metadata log of {now.metadata_location} no longer reaches back to {started_from}"
        )

    def drop_table(self, identifier: str | Identifier) -> None:
        """Remove the table from the catalog: its pointer. Data and metadata are left in place."""
        entry = self._entry(identifier)
        if not delete_pointer(self.store_for(entry.location), entry.location):
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
