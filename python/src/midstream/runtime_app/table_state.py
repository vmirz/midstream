"""The runtime's view of its tables: what the committer publishes in the state table (spec §7.10).

A runtime reads every source topic of its materialization, each into a table of its own, so its
state is per table. The state table is global: every member reads its replica locally, within one
changelog hop of the committer's write. Only the committer agent writes the keys below (an agent
writes tables; tasks and the pipeline only read them); `<table>` is the table's name:

| Key | Value |
|---|---|
| `schema:<table>` | the table's current schema, its fork map and metadata location |
| `committed-offsets:<table>` | the committed source offsets and valid-through as of the latest snapshot |
| `file-decisions:<table>` | what the committer decided about announced files its members must act on |
| `table:<table>` | table totals for the heartbeat |
| `runtime-settings` | live settings (`live_settings.py`, written by the settings agent) |
| `protection-policy` | the materialization's newest protection policy (`protection_state.py`, written by the settings agent) |

A fresh runtime's state table is empty until its committer has handled a first event, so a member
that finds no schema for a table there loads it from the table itself, once, off the event loop.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
import logging
from typing import Any

from pyiceberg.schema import Schema
from pyiceberg.table import Table

from midstream.lakehouse.commits import FileDecision, fork_map_of, protection_json_of
from midstream.lakehouse.lifecycle import schema_from_json, schema_to_json, table_catalog
from midstream.lakehouse.offsets import CommittedOffsets, committed_offsets
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.index import SchemaIndex, build_index
from midstream.lakehouse.tables import TableIdentity, partition_granularity_of
from midstream.protection.plan import ProtectionPlan
from midstream.protection.policy import TableProtection
from midstream.protection.secrets import get_protection_key
from midstream.runtime_app.schema.drift import DriftDetector

LOGGER = logging.getLogger(__name__)

SCHEMA_KEY = "schema"
COMMITTED_OFFSETS_KEY = "committed-offsets"
FILE_DECISIONS_KEY = "file-decisions"
TABLE_KEY = "table"


def schema_key(table: str) -> str:
    return f"{SCHEMA_KEY}:{table}"


def committed_offsets_key(table: str) -> str:
    return f"{COMMITTED_OFFSETS_KEY}:{table}"


def file_decisions_key(table: str) -> str:
    return f"{FILE_DECISIONS_KEY}:{table}"


def table_summary_key(table: str) -> str:
    return f"{TABLE_KEY}:{table}"

#: Waits between attempts to load the table when a member starts and finds no schema to work with.
LOAD_RETRY_SECONDS = (1, 2, 5, 10, 30)

# One thread for the table reads members make (startup, session starts): small and rare.
_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="table-read")


@dataclass(frozen=True, slots=True)
class TableContext:
    """One table this runtime writes, and the topic it holds: agents and tasks get them as arguments."""

    materialization_id: str
    location: str
    #: The table's Iceberg namespace: its dataset's name.
    namespace: str = "materializer"
    name: str = "table"
    partition_granularity: str = "day"
    source_topic: str | None = None
    #: The table's UUID, as the control plane recorded it when it created the table: which
    #: incarnation of the name this is (`same_incarnation`).
    table_uuid: str | None = None

    @property
    def identity(self) -> TableIdentity:
        return TableIdentity(
            namespace=self.namespace,
            name=self.name,
            materialization_id=self.materialization_id,
            location=self.location,
        )


@dataclass(frozen=True, slots=True)
class SchemaRecord:
    schema_id: int
    schema: dict[str, Any]
    fork_map: dict[str, Any] = field(default_factory=dict)
    metadata_location: str | None = None
    last_column_id: int | None = None
    rejected_paths: tuple[tuple[str, ...], ...] = ()
    updated_at: str | None = None
    #: The UUID of the table it was read from (`same_incarnation`).
    table_uuid: str | None = None
    #: The table's `materializer.protection` property, parsed: what the writers mask (design §5.6).
    #: Empty for a table that records none.
    protection: dict[str, Any] = field(default_factory=dict)

    @property
    def protection_revision(self) -> int:
        revision = self.protection.get("revision") if isinstance(self.protection, dict) else None
        return int(revision) if isinstance(revision, int) and not isinstance(revision, bool) else 0

    @property
    def position(self) -> tuple[int, int]:
        """Which of two records is newer: by schema, then by protection revision - a policy the
        committer records changes the protection and keeps the schema."""
        return (self.schema_id, self.protection_revision)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_id": self.schema_id,
            "schema": self.schema,
            "fork_map": self.fork_map,
            "metadata_location": self.metadata_location,
            "last_column_id": self.last_column_id,
            "rejected_paths": [list(item) for item in self.rejected_paths],
            "updated_at": self.updated_at,
            "table_uuid": self.table_uuid,
        }
        if self.protection:
            payload["protection"] = self.protection
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SchemaRecord":
        protection = value.get("protection")
        return cls(
            schema_id=int(value["schema_id"]),
            schema=dict(value["schema"]),
            fork_map=dict(value.get("fork_map") or {}),
            metadata_location=value.get("metadata_location"),
            last_column_id=value.get("last_column_id"),
            rejected_paths=tuple(tuple(item) for item in value.get("rejected_paths") or ()),
            updated_at=value.get("updated_at"),
            table_uuid=value.get("table_uuid"),
            protection=dict(protection) if isinstance(protection, dict) else {},
        )

    @classmethod
    def from_table(cls, table: Table, *, rejected_paths=(), updated_at: str | None = None) -> "SchemaRecord":
        stored = protection_json_of(table.metadata)
        return cls(
            schema_id=table.schema().schema_id,
            schema=schema_to_json(table.schema()),
            fork_map=fork_map_of(table.metadata).to_dict(),
            metadata_location=table.metadata_location,
            last_column_id=table.metadata.last_column_id,
            rejected_paths=tuple(tuple(item) for item in rejected_paths),
            updated_at=updated_at or _utc_now(),
            table_uuid=table_uuid_of(table),
            protection={} if not stored else TableProtection.from_json(stored).to_dict(),
        )


class SchemaView:
    """One schema ready for the hot path: parsed, indexed by record key, with its drift detector
    and its protection plan."""

    def __init__(self, record: SchemaRecord) -> None:
        self.record = record
        self.schema: Schema = schema_from_json(record.schema)
        self.fork_map = ForkMap.from_dict(record.fork_map)
        self.index: SchemaIndex = build_index(self.schema, self.fork_map, last_column_id=record.last_column_id)
        self.drift = DriftDetector(self.index, rejected_paths=record.rejected_paths)
        self.protection = TableProtection.from_dict(record.protection)
        self._plan: ProtectionPlan | None = None

    @property
    def schema_id(self) -> int:
        return self.record.schema_id

    @property
    def plan(self) -> ProtectionPlan:
        """What the writer masks each record with (design §5.4); built on first use.

        It is given this process's protection key; whether that key may hash - the one the
        deployment expects - is the writer's to check before it masks (`protection/secrets.py`).
        """
        if self._plan is None:
            key = get_protection_key()
            self._plan = ProtectionPlan(self.protection, self.index, secret=None if key is None else key.secret)
        return self._plan


def table_contexts(
    materialization_id: str,
    tables: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    partition_granularity: str,
) -> tuple[TableContext, ...]:
    """The runtime's tables, as its templates pass them:
    `[{name, dataset, source_topic, location, table_uuid}]`.

    Each table is in its own dataset, its Iceberg namespace. Each table's UUID is remembered as its
    name's current incarnation (`same_incarnation`).
    """
    contexts = tuple(
        TableContext(
            materialization_id=materialization_id,
            location=str(table["location"]),
            namespace=str(table["dataset"]),
            name=str(table["name"]),
            partition_granularity=partition_granularity,
            source_topic=str(table["source_topic"]),
            table_uuid=str(table["table_uuid"]) if table.get("table_uuid") else None,
        )
        for table in tables
    )
    for context in contexts:
        if context.table_uuid:
            _INCARNATIONS[context.name] = context.table_uuid
    return contexts


#: Each table's current incarnation, by name: its UUID, from the runtime's template. A table dropped
#: and created again under its name - its topic removed and added back - is a new table with a new
#: UUID, while the state table may still hold the dropped one's committed offsets and schema. Taken
#: for the new table's, those offsets would skip its records as already in it.
_INCARNATIONS: dict[str, str] = {}


def same_incarnation(table: str, value: dict[str, Any] | SchemaRecord | None) -> bool:
    """Whether a state-table entry of `table` belongs to the table this runtime writes under its name.

    An entry an earlier version wrote, without a UUID, counts as another table's: what it held is
    read again from the table itself.
    """
    expected = _INCARNATIONS.get(table)
    if expected is None or value is None:
        return True
    found = value.table_uuid if isinstance(value, SchemaRecord) else value.get("table_uuid")
    return found == expected


def table_uuid_of(table: Table) -> str:
    return str(table.metadata.table_uuid)


@lru_cache(maxsize=64)
def contexts_by_topic(tables: tuple[TableContext, ...]) -> dict[str, TableContext]:
    """The runtime's tables by the topic each holds. Read-only: it is cached for the writer's hot path."""
    return {table.source_topic: table for table in tables if table.source_topic}


_VIEWS: dict[str, SchemaView] = {}
_LOADED: dict[str, SchemaRecord] = {}


def reset_table_state() -> None:
    """Forget every cached view and incarnation. Tests use this between cases."""
    _VIEWS.clear()
    _LOADED.clear()
    _INCARNATIONS.clear()


def read_schema_record(state_table: Any, table: str) -> SchemaRecord | None:
    raw = None if state_table is None else state_table.get(schema_key(table))
    if not isinstance(raw, dict) or "schema" not in raw or not same_incarnation(table, raw):
        return None
    try:
        return SchemaRecord.from_dict(raw)
    except (KeyError, TypeError, ValueError):
        LOGGER.warning("Ignoring a malformed schema record of table %s in the runtime state table", table)
        return None


def read_committed_offsets(state_table: Any, table: str) -> CommittedOffsets:
    """The table's committed offsets as the committer last stored them; none for another incarnation's."""
    raw = None if state_table is None else state_table.get(committed_offsets_key(table))
    if not isinstance(raw, dict) or not same_incarnation(table, raw):
        return CommittedOffsets()
    return CommittedOffsets.from_dict(raw)


def committed_offsets_entry(offsets: CommittedOffsets, table: Table) -> dict[str, Any]:
    """What the state table keeps of a table's committed offsets: they, and whose they are."""
    return {**offsets.to_dict(), "table_uuid": table_uuid_of(table)}


def read_file_decisions(state_table: Any, table: str) -> dict[str, FileDecision]:
    raw = None if state_table is None else state_table.get(file_decisions_key(table))
    if not isinstance(raw, dict):
        return {}
    decisions: dict[str, FileDecision] = {}
    for path, value in raw.items():
        if isinstance(value, dict):
            try:
                decisions[path] = FileDecision.from_dict(path, value)
            except (KeyError, TypeError, ValueError):
                continue
    return decisions


def read_table_summary(state_table: Any, table: str) -> dict[str, Any] | None:
    raw = None if state_table is None else state_table.get(table_summary_key(table))
    return raw if isinstance(raw, dict) else None


def current_view(state_table: Any, table: str) -> SchemaView | None:
    """The newest schema of `table` this member knows - from the state table, or the one loaded at start."""
    loaded = _LOADED.get(table)
    if loaded is not None and not same_incarnation(table, loaded):
        loaded = None
    candidates = [record for record in (read_schema_record(state_table, table), loaded) if record is not None]
    if not candidates:
        return None
    newest = max(candidates, key=lambda record: record.position)
    cached = _VIEWS.get(table)
    if cached is not None and cached.record.position == newest.position and cached.record.table_uuid == newest.table_uuid:
        return cached
    view = SchemaView(newest)
    _VIEWS[table] = view
    return view


async def ensure_view(state_table: Any, context: TableContext, *, app: Any = None, client: Any = None) -> SchemaView:
    """`current_view` of the context's table, loading the table once when there is no schema anywhere yet.

    Retries until GCS answers: a record cannot be staged for a table this member cannot read.
    """
    view = current_view(state_table, context.name)
    if view is not None:
        return view
    loop = asyncio.get_running_loop()
    attempt = 0
    while True:
        try:
            table = await loop.run_in_executor(_EXECUTOR, load_table, context, client)
            break
        except Exception as exc:
            if getattr(app, "should_stop", False):
                raise
            delay = LOAD_RETRY_SECONDS[min(attempt, len(LOAD_RETRY_SECONDS) - 1)]
            attempt += 1
            LOGGER.warning(
                "Could not load table %s of materialization %s at %s (attempt %s): %s. Retrying in %ss.",
                context.name,
                context.materialization_id,
                context.location,
                attempt,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
    _LOADED[context.name] = SchemaRecord.from_table(table)
    view = current_view(state_table, context.name)
    assert view is not None
    return view


def load_table(context: TableContext, client: Any = None, *, writer: str = "runtime") -> Table:
    """The table, freshly loaded through its pointer. Blocking: call it off the event loop."""
    return table_catalog(context.identity, client=client, writer=writer).load_table(context.identity.identifier)


async def load_table_offsets(context: TableContext, *, app: Any = None, client: Any = None) -> CommittedOffsets:
    """The table's committed offsets as of its current snapshot, read from GCS (spec §7.8).

    A partition's session starts from this. Retries until GCS answers.
    """
    loop = asyncio.get_running_loop()
    attempt = 0
    while True:
        try:
            table = await loop.run_in_executor(_EXECUTOR, load_table, context, client)
            _LOADED[context.name] = SchemaRecord.from_table(table)
            return committed_offsets(table.metadata)
        except Exception as exc:
            if getattr(app, "should_stop", False):
                raise
            delay = LOAD_RETRY_SECONDS[min(attempt, len(LOAD_RETRY_SECONDS) - 1)]
            attempt += 1
            LOGGER.warning(
                "Could not read the committed offsets of table %s of materialization %s (attempt %s): %s. Retrying in %ss.",
                context.name,
                context.materialization_id,
                attempt,
                exc,
                delay,
            )
            await asyncio.sleep(delay)


def granularity_of(table: Table) -> str:
    return partition_granularity_of(table.metadata.properties)


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
