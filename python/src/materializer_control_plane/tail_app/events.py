"""Following a table: the table events its committer and the maintenance app publish (06 §6.6).

Each member reads `materialization-table-events` with a plain consumer of no group, from the latest
offset at its start - the seed covers anything earlier - and applies, to the tables it tails:

- `table_committed`: `payload.offsets` are the table's new committed offsets. Each partition's only
  ever rises, which starts the retention clock on the rows it now holds. A commit never changes rows.
- `schema_changed`: `payload.schema`, `payload.fork_map`, `payload.schema_id`, `last_column_id`,
  `rejected_paths` are the table's new schema. Rows built under an older one are built again from
  their staged lines when next served.
- `maintenance_completed` after a compaction carries the offsets forward unchanged: nothing to do.

Both are idempotent and may arrive late or twice: an event whose offsets or schema id are not newer
changes nothing. A parked table's events are ignored - activating it fetches its schema afresh.
"""

from __future__ import annotations

import logging
from typing import Any

from materializer_control_plane.tail_app.ingest import TailSchema

LOGGER = logging.getLogger(__name__)


def apply_table_event(table: Any, event: dict[str, Any]) -> str | None:
    """Apply one table event to `table`'s tail; return `commit`, `schema`, or None for nothing."""
    if not table.active and table.state not in ("backlogged", "seeding"):
        return None
    kind = event.get("kind")
    payload = event.get("payload") or {}
    if not isinstance(payload, dict):
        return None
    event_uuid = payload.get("table_uuid")
    if event_uuid and table.table_uuid and event_uuid != table.table_uuid:
        return None
    if kind == "table_committed":
        offsets = payload.get("offsets") or {}
        try:
            parsed = {int(partition): int(offset) for partition, offset in offsets.items()}
        except (AttributeError, TypeError, ValueError):
            LOGGER.warning("Ignoring a table_committed event of %s whose offsets do not read", table.key)
            return None
        table.commit(parsed)
        return "commit"
    if kind == "schema_changed":
        if not table.active and table.state != "backlogged":
            return None
        try:
            schema = TailSchema(
                schema_id=int(payload["schema_id"]),
                iceberg_schema=payload["schema"],
                fork_map=payload.get("fork_map") or {},
                last_column_id=payload.get("last_column_id"),
                rejected_paths=payload.get("rejected_paths") or (),
            )
        except (KeyError, TypeError, ValueError) as error:
            LOGGER.warning("Ignoring a schema_changed event of %s that does not read: %s", table.key, error)
            return None
        return "schema" if table.install_schema(schema) else None
    return None
