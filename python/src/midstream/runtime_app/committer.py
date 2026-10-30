"""The committer: the one member of a runtime that writes to its tables (spec §7.6).

The committer agent consumes `<runtime>-commits`, a topic of one partition, so exactly one member
runs it at a time and Kaspr moves it when that member goes. A runtime reads several source topics,
each into a table of its own; this one process commits to all of them. Everything that changes a
table goes through it, one event at a time, so the runtime never conflicts with itself:

- `data_file_ready`: a member's uploaded file joins the pending set;
- `schema_change_request`: observations join the open schema window, committed together
  `schema_evolution_debounce_seconds` after its first (a fixed window, so a stream of novel fields
  cannot starve it);
- `commit_tick` (every few seconds, from the runtime's leader) and `wakeup`: the clock. Pending files
  are committed once `commit_interval_seconds` have passed since the last commit, sooner at
  `max_files_per_commit` or `max_bytes_per_commit`.

It also owns each table's `materializer.protection` property (design §5.6): it records the protection
policy the control plane delivers - every field classified again, in one metadata commit - and
records the fields each schema change adds, in the schema's own commit. The writers mask by what the
table records.

Table maintenance - compaction, snapshot expiry, orphan cleanup - is not the runtime's: the
maintenance app does it for every materialization (`maintenance_app/`), committing through the same
pointer, so a commit here that loses to one of its commits refreshes and decides again.

Each table has its own `Committer` - its pending files and schema window - inside the runtime's
`RuntimeCommitter`, which routes every event to its table and keeps the work bounded whatever the
number of tables: one commit at a time, on one thread, the table waiting longest for its commit first.

Nothing is persisted here: pending files live in memory, and members announce every uncommitted
file again until a commit covers it, so a new committer rebuilds the set from nothing. What the
committer decides goes to the runtime's state table - each table's schema, committed offsets,
decisions members act on - and to `materialization-table-events` for the control plane.

The GCS work of each commit runs on a thread, so the member's event loop - its writer, its Kafka
consumer - keeps going. Commits refresh the table, decide, and commit once; a lost race refreshes
and decides again, which is what makes a stale committer harmless (spec §7.8).
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
import logging
import time
from typing import Any
import uuid

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table

from midstream.lakehouse.commits import (
    COVERED,
    ERROR,
    OVERLAP,
    AnnouncedFile,
    AppendPlan,
    AppendResult,
    FileDecision,
    MissingDataFileError,
    PendingFiles,
    Seal,
    commit_append,
    commit_protection,
    commit_schema,
    fork_map_of,
    plan_append,
    protection_json_of,
    table_totals,
)
from midstream.lakehouse.gcs import delete_object
from midstream.lakehouse.offsets import CommittedOffsets, committed_offsets
from midstream.lakehouse.schema.index import build_index
from midstream.lakehouse.schema.policy import Evolution, evolve, limits_of
from midstream.lakehouse.schema.render import render_schema
from midstream.protection.plan import record_new_fields, record_policy
from midstream.protection.policy import TableProtection
from midstream.protection.secrets import get_expected_key_id, get_protection_key
from midstream.protocol_logging import log_protocol_event
from midstream.runtime_app.config import RuntimeAppConfig, get_runtime_app_config
from midstream.runtime_app.performance import record_commit_processing
from midstream.runtime_app.protection_state import DeliveredPolicy, read_delivered_policy
from midstream.runtime_app.table_state import (
    SchemaRecord,
    TableContext,
    committed_offsets_entry,
    committed_offsets_key,
    file_decisions_key,
    load_table,
    read_file_decisions,
    schema_key,
    table_contexts,
    table_summary_key,
    table_uuid_of,
)
from midstream.runtime_app.writer.context import current_member_id
from midstream.storage.keys import materialization_routing_key

LOGGER = logging.getLogger(__name__)

#: GCS takes about one update a second to one object (experiment E11): commits are spaced by this.
COMMIT_SPACING_SECONDS = 1.0
#: Refresh-and-decide-again attempts within one commit cycle before it is retried at a later tick.
COMMIT_ATTEMPTS = 5
#: Backoff between failed commit cycles: doubling from the first, capped at the last.
FAILURE_BACKOFF_SECONDS = (5, 300)
#: Decisions members may still need are kept at most this many.
DECISIONS_LIMIT = 5_000
#: The committer counts as this member's while it handled an event this recently (heartbeat block).
ACTIVE_SECONDS = 30.0
#: A table error is published again for the same reason at most this often.
ERROR_REPEAT_SECONDS = 300.0
#: Stale announcements are logged at most this often per member and partition.
STALE_LOG_SECONDS = 60.0


@dataclass(slots=True)
class _SchemaWindow:
    first_at: float
    observations: list[tuple[tuple[str, ...], Any]] = field(default_factory=list)
    requests: int = 0


@dataclass(slots=True)
class _AppendCycle:
    plan: AppendPlan | None = None
    result: AppendResult | None = None
    decisions: list[FileDecision] = field(default_factory=list)
    table: Table | None = None
    error: BaseException | None = None
    attempts: int = 0


class Committer:
    """One table's committer, part of its runtime's `RuntimeCommitter`."""

    def __init__(self, context: TableContext, *, runtime: "RuntimeCommitter", client: Any = None) -> None:
        self.context = context
        self.runtime = runtime
        self.runtime_app_ref = runtime.runtime_app_ref
        self.client = client
        self.pending = PendingFiles()
        self.window: _SchemaWindow | None = None
        self.table: Table | None = None
        #: Held by a commit of this table: an append, or a schema change.
        self.lock = asyncio.Lock()
        self.last_commit_at: float | None = None
        self.last_commit_wall: str | None = None
        self.last_commit_duration_ms: int | None = None
        self.consecutive_failures = 0
        self.next_attempt_at = 0.0
        self.rejected_paths: set[tuple[str, ...]] = set()
        self.published: dict[str, Any] = {}
        self._errors_at: dict[str, float] = {}
        #: Announcements ignored as an older session of their member's process, since this one started.
        self.stale_announcements = 0
        self._stale_logged_at: dict[tuple[str, int], float] = {}
        #: The table's protection, parsed, with the metadata location it was read from.
        self._protection: tuple[str | None, TableProtection] | None = None

    # -- shared with the runtime's other tables --------------------------------------------------

    @property
    def pool(self) -> ThreadPoolExecutor:
        """The runtime's one commit thread: commits of all its tables run one at a time on it."""
        return self.runtime.pool

    @property
    def footer_pool(self) -> ThreadPoolExecutor:
        return self.runtime.footer_pool

    @property
    def last_event_at(self) -> float | None:
        return self.runtime.last_event_at

    def schema_window_due_in(self, now: float, *, config: RuntimeAppConfig) -> float | None:
        """Seconds until this table's open schema window should commit; None without one."""
        if self.window is None:
            return None
        return max(0.0, config.schema_evolution_debounce_seconds - (now - self.window.first_at))

    def _add_file(self, value: dict[str, Any]) -> None:
        if value.get("materialization_id") not in (None, "", self.context.materialization_id):
            return
        try:
            announced = AnnouncedFile.from_dict(value)
        except (KeyError, TypeError, ValueError) as error:
            LOGGER.warning("Ignoring a malformed file announcement: %s", error)
            return
        if not announced.file_path.startswith(f"{self.context.location.rstrip('/')}/data/"):
            LOGGER.warning("Ignoring an announced file outside the table: %s", announced.file_path)
            return
        if not self.pending.add(announced):
            self._ignore_stale(announced)

    def _ignore_stale(self, announced: AnnouncedFile) -> None:
        """Count an announcement of an older session of its member's process, and say so.

        Logged at most every `STALE_LOG_SECONDS` per member and partition: a member announces its
        files again every minute. Before session epochs, every announcement of a restarted member was
        ignored this way, in silence, for as long as the committer ran.
        """
        self.stale_announcements += 1
        key = (announced.member_id, announced.partition)
        now = time.monotonic()
        last = self._stale_logged_at.get(key)
        if last is not None and now - last < STALE_LOG_SECONDS:
            return
        self._stale_logged_at[key] = now
        epoch, generation = self.pending.session_of(announced.member_id, announced.partition) or (None, None)
        LOGGER.warning(
            "Ignoring a file announcement of %s table %s from an older session: member %s, partition %s, "
            "seal %s of session %s (process %s), while the member's newest is session %s (process %s). "
            "%s stale announcement(s) since this committer started.",
            self.context.materialization_id,
            self.context.name,
            announced.member_id,
            announced.partition,
            announced.seal_id,
            announced.session_generation,
            announced.session_epoch,
            generation,
            epoch,
            self.stale_announcements,
        )

    def _add_schema_request(self, value: dict[str, Any], *, config: RuntimeAppConfig) -> None:
        observations = []
        for item in value.get("observations") or []:
            if not isinstance(item, dict) or "path" not in item:
                continue
            path = tuple(str(segment) for segment in item.get("path") or ())
            if any(path[: len(rejected)] == rejected for rejected in self.rejected_paths):
                continue
            observations.append((path, item.get("observed")))
        if not observations:
            return
        if self.window is None:
            self.window = _SchemaWindow(first_at=time.monotonic())
        self.window.observations.extend(observations)
        self.window.requests += 1

    def _append_due(self, now: float, *, config: RuntimeAppConfig) -> bool:
        if not len(self.pending) or now < self.next_attempt_at:
            return False
        if self.last_commit_at is not None and now - self.last_commit_at < COMMIT_SPACING_SECONDS:
            return False
        if len(self.pending) >= config.max_files_per_commit or self.pending.bytes >= config.max_bytes_per_commit:
            return True
        return self.last_commit_at is None or now - self.last_commit_at >= config.commit_interval_seconds

    # -- loading and publishing ------------------------------------------------------------------

    async def _ensure_loaded(self, outputs: list[dict[str, Any]]) -> bool:
        if self.table is not None:
            return True
        try:
            self.table = await asyncio.get_running_loop().run_in_executor(self.pool, self._load)
        except Exception as error:
            LOGGER.warning(
                "The committer of %s could not load table %s: %s",
                self.context.materialization_id,
                self.context.name,
                error,
            )
            outputs.extend(self._table_error("table_unavailable", f"The table could not be loaded: {error}"))
            return False
        return True

    def _load(self) -> Table:
        return load_table(self.context, self.client, writer=f"runtime:{current_member_id()}")

    def _publish_state(self, state_table: Any) -> list[dict[str, Any]]:
        """Keep the state table's schema and offsets in step with the table (a new committer seeds them)."""
        table = self.table
        if table is None or state_table is None:
            return []
        name = self.context.name
        schema_id = table.schema().schema_id
        uuid_ = table_uuid_of(table)
        stored = state_table.get(schema_key(name))
        if (
            not isinstance(stored, dict)
            or stored.get("schema_id") != schema_id
            or stored.get("metadata_location") is None
            or stored.get("table_uuid") != uuid_
            or _stored_protection_revision(stored) != self.protection().revision
        ):
            state_table[schema_key(name)] = SchemaRecord.from_table(table, rejected_paths=sorted(self.rejected_paths)).to_dict()
        snapshot_id = table.metadata.current_snapshot_id
        offsets = state_table.get(committed_offsets_key(name))
        # A table dropped and created again under its name finds the dropped one's offsets here.
        if not isinstance(offsets, dict) or offsets.get("snapshot_id") != snapshot_id or offsets.get("table_uuid") != uuid_:
            state_table[committed_offsets_key(name)] = committed_offsets_entry(committed_offsets(table.metadata), table)
        if self.published.get("table") != snapshot_id:
            state_table[table_summary_key(name)] = self._table_summary()
            self.published["table"] = snapshot_id
        return []

    def _table_summary(self) -> dict[str, Any]:
        table = self.table
        if table is None:
            return {}
        snapshot = table.current_snapshot()
        offsets = committed_offsets(table.metadata)
        return {
            "table": self.context.name,
            "source_topic": self.context.source_topic,
            "metadata_location": table.metadata_location,
            "current_snapshot_id": None if snapshot is None else snapshot.snapshot_id,
            "snapshot_at": None if snapshot is None else _millis_text(snapshot.timestamp_ms),
            "sequence_number": None if snapshot is None else snapshot.sequence_number,
            **{f"total_{key}": value for key, value in table_totals(table).items()},
            "schema_id": table.schema().schema_id,
            "committed_offsets": {str(key): value for key, value in offsets.offsets.items()},
            "valid_through": offsets.valid_through,
            "snapshots": len(table.metadata.snapshots),
            "last_commit_at": self.last_commit_wall,
            "last_commit_duration_ms": self.last_commit_duration_ms,
            "pending_files": len(self.pending),
            "stale_announcements": self.stale_announcements,
        }

    # -- protection ------------------------------------------------------------------------------

    def protection(self) -> TableProtection:
        """The table's recorded protection, parsed once per metadata version."""
        table = self.table
        if table is None:
            return TableProtection()
        location = table.metadata_location
        if self._protection is None or self._protection[0] != location:
            self._protection = (location, TableProtection.from_json(protection_json_of(table.metadata)))
        return self._protection[1]

    async def _record_protection(self, state_table: Any, *, config: RuntimeAppConfig) -> list[dict[str, Any]]:
        """Record the delivered protection policy in the table when it is newer than the table's.

        Every field is classified again by the new policy and the current rules, and the property
        committed in one metadata commit (design §5.6). The writers follow once the new schema record
        reaches them through the state table, and cut their open batches there.
        """
        delivered = read_delivered_policy(state_table)
        if delivered is None or self.table is None or delivered.revision <= self.protection().revision:
            return []
        async with self.lock:
            started = time.monotonic()
            try:
                table, protection = await asyncio.get_running_loop().run_in_executor(
                    self.pool, self._protection_cycle, delivered
                )
            except Exception as error:
                LOGGER.exception(
                    "Recording protection revision %s of %s table %s failed",
                    delivered.revision,
                    self.context.materialization_id,
                    self.context.name,
                )
                return self._table_error("protection_commit_failed", f"Recording the protection policy failed: {error}")
        self.table = table
        self._protection = None
        state_table[schema_key(self.context.name)] = SchemaRecord.from_table(table, rejected_paths=sorted(self.rejected_paths)).to_dict()
        duration_ms = int((time.monotonic() - started) * 1000)
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="runtime_protection_recorded",
            materialization_id=self.context.materialization_id,
            table=self.context.name,
            revision=protection.revision,
            policy=protection.policy.to_dict(),
            protected_fields=len(protection.active_fields()),
            duration_ms=duration_ms,
        )
        LOGGER.info(
            "Recorded protection revision %s in %s table %s: %s field(s) protected",
            protection.revision,
            self.context.materialization_id,
            self.context.name,
            len(protection.active_fields()),
        )
        return [self._schema_changed_event(table, changes=[], duration_ms=duration_ms)]

    def _protection_cycle(self, delivered: DeliveredPolicy) -> tuple[Table, TableProtection]:
        last_error: BaseException | None = None
        for _ in range(COMMIT_ATTEMPTS):
            table = self._load()
            recorded = TableProtection.from_json(protection_json_of(table.metadata))
            if recorded.revision >= delivered.revision:
                return table, recorded
            index = build_index(table.schema(), fork_map_of(table.metadata), last_column_id=table.metadata.last_column_id)
            protection = record_policy(
                recorded,
                index,
                delivered.policy,
                revision=delivered.revision,
                at=_utc_now(),
                by=delivered.changed_by,
                key_id=_hashing_key_id(),
                sequence_number=table.metadata.last_sequence_number,
                reason=delivered.reason,
            )
            try:
                commit_protection(table, protection.to_json(), schema_id=index.schema_id)
            except CommitFailedException as error:
                last_error = error
                time.sleep(COMMIT_SPACING_SECONDS)
                continue
            return self._load(), protection
        raise RuntimeError(f"the protection commit kept losing to other commits: {last_error}")

    def _schema_changed_event(self, table: Table, *, changes: list[dict[str, Any]], duration_ms: int, requests: int | None = None) -> dict[str, Any]:
        rendered = render_schema(table.schema(), fork_map_of(table.metadata), schemas=table.metadata.schemas)
        protection = TableProtection.from_json(protection_json_of(table.metadata))
        payload = {
            "schema_id": table.schema().schema_id,
            "schema": SchemaRecord.from_table(table).schema,
            "fork_map": fork_map_of(table.metadata).to_dict(),
            "fields": rendered["fields"],
            "field_count": rendered["field_count"],
            "changes": changes,
            "metadata_location": table.metadata_location,
            "last_column_id": table.metadata.last_column_id,
            "rejected_paths": [list(item) for item in sorted(self.rejected_paths)],
            "duration_ms": duration_ms,
        }
        if protection_json_of(table.metadata) is not None:
            payload["protection"] = protection.to_dict()
        return self._event("schema_changed", payload)

    # -- schema ----------------------------------------------------------------------------------

    async def _commit_schema(self, state_table: Any, *, config: RuntimeAppConfig) -> list[dict[str, Any]]:
        window, self.window = self.window, None
        if window is None or not window.observations:
            return []
        async with self.lock:
            started = time.monotonic()
            try:
                table, evolution = await asyncio.get_running_loop().run_in_executor(
                    self.pool, self._schema_cycle, window.observations, config
                )
            except Exception as error:
                LOGGER.exception("Schema evolution of %s table %s failed", self.context.materialization_id, self.context.name)
                log_protocol_event(
                    LOGGER,
                    enabled=config.protocol_logging_enabled,
                    event="runtime_schema_commit_failed",
                    materialization_id=self.context.materialization_id,
                    table=self.context.name,
                    error=str(error),
                )
                return self._table_error("schema_evolution_failed", f"Schema evolution failed: {error}")
        self.table = table
        for change in evolution.rejected:
            self.rejected_paths.add(tuple(change.path))
        outputs: list[dict[str, Any]] = []
        if evolution.rejected:
            outputs.extend(
                self._table_error(
                    "schema_field_limit",
                    f"The schema is at its field limit ({config.max_schema_fields}): "
                    f"{len(evolution.rejected)} new field(s) go to _unmapped instead.",
                )
            )
        if not evolution.changed:
            if evolution.rejected:
                state_table[schema_key(self.context.name)] = SchemaRecord.from_table(table, rejected_paths=sorted(self.rejected_paths)).to_dict()
            return outputs
        state_table[schema_key(self.context.name)] = SchemaRecord.from_table(table, rejected_paths=sorted(self.rejected_paths)).to_dict()
        duration_ms = int((time.monotonic() - started) * 1000)
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="runtime_schema_committed",
            materialization_id=self.context.materialization_id,
            table=self.context.name,
            schema_id=table.schema().schema_id,
            changes=[change.to_dict() for change in evolution.changes],
            requests=window.requests,
            duration_ms=duration_ms,
        )
        outputs.append(
            self._schema_changed_event(table, changes=[change.to_dict() for change in evolution.changes], duration_ms=duration_ms)
        )
        return outputs

    def _schema_cycle(self, observations, config: RuntimeAppConfig) -> tuple[Table, Evolution]:
        limits = limits_of(config)
        last_error: BaseException | None = None
        for _ in range(COMMIT_ATTEMPTS):
            table = self._load()
            fork_map = fork_map_of(table.metadata)
            evolution = evolve(
                table.schema(),
                fork_map,
                observations,
                limits=limits,
                last_column_id=table.metadata.last_column_id,
            )
            if not evolution.changed:
                return table, evolution
            protection_json = self._protection_with_new_fields(table, fork_map, evolution)
            try:
                commit_schema(table, evolution, protection_json=protection_json)
            except CommitFailedException as error:
                last_error = error
                time.sleep(COMMIT_SPACING_SECONDS)
                continue
            return self._load(), evolution
        raise RuntimeError(f"the schema commit kept losing to other commits: {last_error}")

    @staticmethod
    def _protection_with_new_fields(table: Table, fork_map, evolution: Evolution) -> str | None:
        """The table's protection with the fields `evolution` adds recorded; None when it records
        nothing and nothing changes (a table without protection keeps no property)."""
        stored = protection_json_of(table.metadata)
        recorded = TableProtection.from_json(stored)
        if stored is None and recorded.policy.empty:
            return None
        before = build_index(table.schema(), fork_map, last_column_id=table.metadata.last_column_id)
        after = build_index(evolution.schema, evolution.fork_map, last_column_id=evolution.last_column_id)
        updated = record_new_fields(recorded, before, after)
        if updated is recorded:
            return None
        return updated.to_json()

    # -- appends ---------------------------------------------------------------------------------

    async def _commit_appends(self, state_table: Any, *, config: RuntimeAppConfig) -> list[dict[str, Any]]:
        async with self.lock:
            self._forget_covered_incomplete()
            seals = [seal for seal in self.pending.seals() if seal.complete]
            if not seals:
                return []
            started = time.monotonic()
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="runtime_commit_started",
                materialization_id=self.context.materialization_id,
                table=self.context.name,
                seals=len(seals),
                files=sum(len(seal.files) for seal in seals),
                pending_files=len(self.pending),
            )
            cycle = await asyncio.get_running_loop().run_in_executor(self.pool, self._append_cycle, seals, config)
        duration_ms = int((time.monotonic() - started) * 1000)
        outputs: list[dict[str, Any]] = []
        if cycle.table is not None:
            self.table = cycle.table
        self._record_decisions(state_table, cycle.decisions)
        decided = {decision.seal_id for decision in cycle.decisions}
        self.pending.remove(decided)
        self._forget_covered_incomplete()
        if cycle.error is not None:
            self.consecutive_failures += 1
            backoff = min(FAILURE_BACKOFF_SECONDS[0] * 2 ** (self.consecutive_failures - 1), FAILURE_BACKOFF_SECONDS[1])
            self.next_attempt_at = time.monotonic() + backoff
            record_commit_processing(
                materialization_id=self.context.materialization_id,
                observed_at=_utc_now(),
                commit_ms=duration_ms,
                files=0,
                records=0,
                failed=True,
            )
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="runtime_commit_failed",
                materialization_id=self.context.materialization_id,
                table=self.context.name,
                files=sum(len(seal.files) for seal in seals),
                attempts=cycle.attempts,
                retry_in_seconds=backoff,
                error=str(cycle.error),
            )
            LOGGER.warning(
                "Committing %s file(s) to table %s of %s failed (%s in a row): %s. Retrying in %ss; "
                "the files stay pending and their records unacknowledged.",
                sum(len(seal.files) for seal in seals),
                self.context.name,
                self.context.materialization_id,
                self.consecutive_failures,
                cycle.error,
                backoff,
            )
            outputs.extend(self._table_error("commit_failed", f"Commits are failing: {cycle.error}"))
            return outputs
        self.consecutive_failures = 0
        self.next_attempt_at = 0.0
        result = cycle.result
        if result is None:
            return outputs
        self.last_commit_at = time.monotonic()
        self.last_commit_wall = _utc_now()
        self.last_commit_duration_ms = duration_ms
        state_table[committed_offsets_key(self.context.name)] = committed_offsets_entry(result.offsets, cycle.table)
        state_table[table_summary_key(self.context.name)] = self._table_summary()
        self.published["table"] = result.snapshot_id
        record_commit_processing(
            materialization_id=self.context.materialization_id,
            observed_at=self.last_commit_wall,
            commit_ms=duration_ms,
            files=result.files,
            records=result.records,
        )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="runtime_commit_completed",
            materialization_id=self.context.materialization_id,
            table=self.context.name,
            snapshot_id=result.snapshot_id,
            sequence_number=result.sequence_number,
            files=result.files,
            records=result.records,
            duration_ms=duration_ms,
            attempts=cycle.attempts,
            pending_files=len(self.pending),
        )
        totals = table_totals(self.table) if self.table is not None else {}
        outputs.append(
            self._event(
                "table_committed",
                {
                    "snapshot_id": result.snapshot_id,
                    "sequence_number": result.sequence_number,
                    "committed_at": self.last_commit_wall,
                    "files_added": result.files,
                    "records_added": result.records,
                    "bytes_added": result.bytes,
                    "totals": totals,
                    "offsets": {str(key): value for key, value in result.offsets.offsets.items()},
                    "valid_through": result.offsets.valid_through,
                    "duration_ms": duration_ms,
                    "metadata_location": result.metadata_location,
                    "schema_id": None if self.table is None else self.table.schema().schema_id,
                    "snapshots": None if self.table is None else len(self.table.metadata.snapshots),
                },
            )
        )
        return outputs

    def _append_cycle(self, seals: list[Seal], config: RuntimeAppConfig) -> _AppendCycle:
        cycle = _AppendCycle()
        candidates = list(seals)
        while cycle.attempts < COMMIT_ATTEMPTS:
            cycle.attempts += 1
            try:
                table = self._load()
                cycle.table = table
                committed = committed_offsets(table.metadata)
                plan = plan_append(
                    candidates,
                    committed,
                    max_files=config.max_files_per_commit,
                    max_bytes=config.max_bytes_per_commit,
                )
                cycle.plan = plan
                # Only an overlapping seal's files are deleted at once. A covered one may be the very
                # file the table holds: a member re-announces what it has not yet seen committed.
                rejected = [item for item in plan.decisions if item.decision == OVERLAP]
                if not plan.accepted:
                    self._delete_rejected(rejected)
                    cycle.decisions.extend(plan.decisions)
                    return cycle
                result = commit_append(
                    table,
                    plan,
                    runtime=self.runtime_app_ref,
                    client=self.client or _shared_client(),
                    footer_pool=self.footer_pool,
                )
            except CommitFailedException as error:
                cycle.error = error
                time.sleep(COMMIT_SPACING_SECONDS)
                continue
            except MissingDataFileError as missing:
                # Announced but not there: its member seals the rows again under a new name.
                bad = next((seal for seal in candidates if missing.file_path in seal.files), None)
                if bad is None:
                    cycle.error = missing
                    return cycle
                candidates.remove(bad)
                cycle.decisions.extend(
                    FileDecision(
                        file_path=path,
                        decision=ERROR,
                        partition=bad.partition,
                        seal_id=bad.seal_id,
                        batch_id=bad.batch_id,
                        member_id=bad.member_id,
                        offset_end=bad.offset_end,
                        reason=f"data file missing: {missing.file_path}",
                        decided_at=_utc_now(),
                    )
                    for path in bad.files
                )
                if not candidates:
                    return cycle
                continue
            except Exception as error:  # noqa: BLE001 - a GCS outage, say: retried at a later tick
                cycle.error = error
                return cycle
            cycle.error = None
            cycle.result = result
            cycle.table = table
            self._delete_rejected(rejected)
            cycle.decisions.extend(plan.decisions)
            return cycle
        return cycle

    def _delete_rejected(self, decisions: list[FileDecision]) -> None:
        """Delete an overlapping seal's files: no commit takes them, and its member seals the rest again.

        Never a covered seal's: its file is either the table's own, announced again after its commit
        (a member re-announces until it sees one, and a new committer hears every pending file again),
        or a duplicate of rows the table holds under another name, which orphan cleanup removes once
        no snapshot references it and it is older than `orphan_min_age_hours`.
        """
        client = self.client or _shared_client()
        for decision in decisions:
            try:
                delete_object(client, decision.file_path)
            except Exception as error:  # noqa: BLE001 - orphan cleanup collects what is left
                LOGGER.info("Could not delete the rejected file %s: %s", decision.file_path, error)

    def _forget_covered_incomplete(self) -> None:
        """Forget incomplete seals the table already covers.

        A seal is decided once all its files are announced. A member that loses the partition after
        announcing only some of them never completes it, while the partition's new owner reads and
        commits the same records; kept, the seal would count as pending for as long as this
        committer runs. Its files stay, like a covered seal's: orphan cleanup removes them.
        """
        if self.table is None:
            return
        committed = committed_offsets(self.table.metadata)
        covered = [
            seal.seal_id
            for seal in self.pending.seals()
            if not seal.complete and seal.offset_end <= committed.get(seal.partition)
        ]
        if covered:
            self.pending.remove(covered)
            LOGGER.info(
                "Forgot %d incomplete seal(s) of %s table %s the table already covers",
                len(covered),
                self.context.materialization_id,
                self.context.name,
            )

    def _record_decisions(self, state_table: Any, decisions: list[FileDecision]) -> None:
        """Store the decisions members act on (not acceptances: members release by offsets)."""
        if state_table is None:
            return
        actionable = [item for item in decisions if item.decision in (COVERED, OVERLAP, ERROR)]
        current = read_file_decisions(state_table, self.context.name)
        offsets = committed_offsets(self.table.metadata) if self.table is not None else CommittedOffsets()
        kept = {
            path: decision
            for path, decision in current.items()
            # A decision about a range the table now covers is moot: its batch is released.
            if decision.decision != COVERED and decision.offset_end > offsets.get(decision.partition)
        }
        if not actionable and len(kept) == len(current):
            return
        for item in actionable:
            if item.decision == COVERED:
                continue
            kept[item.file_path] = item
        if len(kept) > DECISIONS_LIMIT:
            newest = sorted(kept.values(), key=lambda item: item.decided_at or "", reverse=True)[:DECISIONS_LIMIT]
            kept = {item.file_path: item for item in newest}
        state_table[file_decisions_key(self.context.name)] = {path: decision.to_dict() for path, decision in kept.items()}
        for item in actionable:
            log_protocol_event(
                LOGGER,
                enabled=get_runtime_app_config().protocol_logging_enabled,
                event="runtime_file_decision",
                materialization_id=self.context.materialization_id,
                table=self.context.name,
                file_path=item.file_path,
                decision=item.decision,
                partition=item.partition,
                covered_through=item.covered_through,
                member_id=item.member_id,
            )

    # -- events ----------------------------------------------------------------------------------

    def _event(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = {
            "event_id": f"evt-table-{kind}-{uuid.uuid4().hex[:16]}",
            "kind": kind,
            "materialization_id": self.context.materialization_id,
            "table": self.context.name,
            "occurred_at": _utc_now(),
            "runtime_app_ref": self.runtime_app_ref,
            "member_id": current_member_id(),
            "payload": payload,
        }
        return {"route": "events", "key": materialization_routing_key(self.context.materialization_id), "value": event}

    def _table_error(self, reason: str, message: str) -> list[dict[str, Any]]:
        now = time.monotonic()
        last = self._errors_at.get(reason)
        if last is not None and now - last < ERROR_REPEAT_SECONDS:
            return []
        self._errors_at[reason] = now
        return [self._event("table_error", {"reason": reason, "message": message})]

    # -- reporting -------------------------------------------------------------------------------

    def active(self) -> bool:
        return self.runtime.active()

    def table_block(self) -> dict[str, Any] | None:
        if not self.active() or self.table is None:
            return None
        return self._table_summary()


class RuntimeCommitter:
    """Every table's committer of one runtime, in the process of the member that owns `<runtime>-commits`.

    It routes each event to its table's `Committer` and keeps the work bounded however many tables
    there are: their commits run one at a time on one thread, the table waiting longest first.
    """

    def __init__(self, contexts: tuple[TableContext, ...], *, runtime_app_ref: str, client: Any = None) -> None:
        if not contexts:
            raise ValueError("a runtime commits to at least one table")
        self.contexts = tuple(contexts)
        self.materialization_id = contexts[0].materialization_id
        self.runtime_app_ref = runtime_app_ref
        self.client = client
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="committer")
        self.footer_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="committer-footers")
        self.last_event_at: float | None = None
        self._wakeup: asyncio.TimerHandle | None = None
        self.tables: dict[str, Committer] = {
            context.name: Committer(context, runtime=self, client=client) for context in self.contexts
        }

    def committer(self, name: str | None) -> Committer | None:
        """The committer of table `name`; a runtime of one table takes a message that names none."""
        if not name:
            return next(iter(self.tables.values())) if len(self.tables) == 1 else None
        return self.tables.get(name)

    # -- entry point -----------------------------------------------------------------------------

    async def handle(self, value: dict[str, Any], state_table: Any, *, app: Any = None, commits_topic: str | None = None) -> list[dict[str, Any]]:
        self.last_event_at = time.monotonic()
        config = get_runtime_app_config()
        kind = value.get("kind") if isinstance(value, dict) else None
        if kind in ("data_file_ready", "schema_change_request"):
            committer = self.committer(value.get("table"))
            if committer is None:
                LOGGER.warning("Ignoring a %s for table %r, which this runtime does not write", kind, value.get("table"))
            elif kind == "data_file_ready":
                committer._add_file(value)
            else:
                committer._add_schema_request(value, config=config)
        outputs: list[dict[str, Any]] = []
        ready: list[Committer] = []
        for committer in self.tables.values():
            if await committer._ensure_loaded(outputs):
                outputs.extend(committer._publish_state(state_table))
                ready.append(committer)
        for committer in ready:
            outputs.extend(await committer._record_protection(state_table, config=config))
        now = time.monotonic()
        for committer in ready:
            due_in = committer.schema_window_due_in(now, config=config)
            if due_in is not None and due_in <= 0:
                outputs.extend(await committer._commit_schema(state_table, config=config))
        # One table's commit at a time; whichever has waited longest for one goes first.
        due = [committer for committer in ready if committer._append_due(time.monotonic(), config=config)]
        due.sort(key=lambda committer: committer.last_commit_at or 0.0)
        for committer in due:
            outputs.extend(await committer._commit_appends(state_table, config=config))
        self._schedule_wakeup(app, commits_topic, config=config)
        return outputs

    # -- wakeups ---------------------------------------------------------------------------------

    def _schedule_wakeup(self, app: Any, topic: str | None, *, config: RuntimeAppConfig) -> None:
        """Ask for an event when the next schema window ends, so it commits without waiting for a tick."""
        if app is None or topic is None or self._wakeup is not None:
            return
        now = time.monotonic()
        waits = [
            due_in
            for due_in in (committer.schema_window_due_in(now, config=config) for committer in self.tables.values())
            if due_in is not None
        ]
        if not waits:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        def fire() -> None:
            self._wakeup = None
            loop.create_task(self._send_wakeup(app, topic))

        self._wakeup = loop.call_later(max(0.1, min(waits)) + 0.1, fire)

    async def _send_wakeup(self, app: Any, topic: str) -> None:
        try:
            channel = app.topic(topic, value_serializer="json")
            await channel.send(
                key="commits",
                value={"kind": "wakeup", "materialization_id": self.materialization_id, "sent_at": _utc_now()},
            )
        except Exception as error:  # noqa: BLE001 - the next tick does the same
            LOGGER.debug("Could not send the committer its wakeup: %s", error)

    # -- reporting -------------------------------------------------------------------------------

    def active(self) -> bool:
        return self.last_event_at is not None and time.monotonic() - self.last_event_at < ACTIVE_SECONDS

    def table_blocks(self) -> dict[str, dict[str, Any]] | None:
        """Each table's heartbeat block, while this member runs the committer."""
        if not self.active():
            return None
        blocks = {name: block for name, block in ((name, committer.table_block()) for name, committer in self.tables.items()) if block is not None}
        return blocks or None

    def stop(self) -> None:
        self.pool.shutdown(wait=False)
        self.footer_pool.shutdown(wait=False)


_COMMITTERS: dict[str, RuntimeCommitter] = {}


def get_runtime_committer(contexts: tuple[TableContext, ...], *, runtime_app_ref: str) -> RuntimeCommitter:
    materialization_id = contexts[0].materialization_id
    committer = _COMMITTERS.get(materialization_id)
    if committer is None or committer.contexts != tuple(contexts):
        if committer is not None:
            committer.stop()
        committer = RuntimeCommitter(tuple(contexts), runtime_app_ref=runtime_app_ref)
        _COMMITTERS[materialization_id] = committer
    return committer


def reset_committers() -> None:
    """Forget every committer. Tests use this between cases."""
    for committer in _COMMITTERS.values():
        committer.stop()
    _COMMITTERS.clear()


async def handle_commit_event(
    value: dict[str, Any],
    state_table: Any,
    *,
    context: dict[str, Any] | None = None,
    materialization_id: str,
    runtime_app_ref: str,
    tables: tuple[TableContext, ...],
    commits_topic: str | None = None,
) -> Iterator[dict[str, Any]]:
    """The committer agent's map: handle one event, return its routed outputs as a plain generator.

    `tables` is the runtime's table contexts (`table_state.table_contexts`), built once by the
    agent's init block.
    """
    committer = get_runtime_committer(tuple(tables), runtime_app_ref=runtime_app_ref)
    app = None if context is None else context.get("app")
    try:
        outputs = await committer.handle(value, state_table, app=app, commits_topic=commits_topic)
    except Exception:  # noqa: BLE001 - one bad event must not stop the committer; ticks follow
        LOGGER.exception("The committer of %s failed to handle a %r event", materialization_id, value.get("kind") if isinstance(value, dict) else None)
        outputs = []
    computed = list(outputs)
    return (item for item in computed)


def committer_table_blocks(materialization_id: str) -> dict[str, dict[str, Any]] | None:
    """The heartbeat's `tables` blocks, when this member runs the committer."""
    committer = _COMMITTERS.get(materialization_id)
    return None if committer is None else committer.table_blocks()


def build_commit_tick(*, materialization_id: str) -> dict[str, Any]:
    return {"kind": "commit_tick", "materialization_id": materialization_id, "sent_at": _utc_now()}


def _stored_protection_revision(stored: dict[str, Any]) -> int:
    protection = stored.get("protection")
    revision = protection.get("revision") if isinstance(protection, dict) else None
    return revision if isinstance(revision, int) and not isinstance(revision, bool) else 0


def _hashing_key_id() -> str | None:
    """The id of the key the members hash with: the one the deployment expects, else this process's."""
    expected = get_expected_key_id()
    if expected is not None:
        return expected
    key = get_protection_key()
    return None if key is None else key.key_id


def _shared_client():
    from midstream.lakehouse.gcs import shared_storage_client

    return shared_storage_client()


def _millis_text(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
