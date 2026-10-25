"""Shard-stable reads for records that may exist in more than one table shard."""

from __future__ import annotations

from materializer_control_plane.models import RecordEnvelope
from materializer_control_plane.storage.envelopes import decode_envelope
from materializer_control_plane.storage.table import TableProtocol


def resolve_envelope(
    table: TableProtocol,
    *,
    key: str,
    entity_type: str,
    entity_id: str,
) -> RecordEnvelope | None:
    """Read one record, tolerating copies stranded in more than one table shard.

    The table is co-partitioned with the streams that write it, so a record written while handling
    a message keyed differently from the record's own key lands in another shard. A plain keyed read
    then returns whichever shard answers — and that answer is **not stable across restarts**, which
    is how a deleted materialization came back as running after a redeploy.

    Scanning the record's exact key finds every copy; the newest `updated_at` is the current state,
    because the latest write is by definition the most recent truth about that record. This is
    deterministic where a keyed read is not, and it costs a single-key prefix scan.

    Falls back to the plain read when the scan finds nothing, so a table whose scan cannot match a
    complete key still behaves exactly as before.
    """
    newest: RecordEnvelope | None = None
    for scanned_key, raw_value in table.prefix_scan(key):
        if scanned_key != key:
            continue
        envelope = decode_envelope(raw_value, expected_entity_type=entity_type)
        if envelope is None or envelope.entity_id != entity_id:
            continue
        if newest is None or (envelope.updated_at or "") > (newest.updated_at or ""):
            newest = envelope
    if newest is not None:
        return newest
    return decode_envelope(table.get(key), expected_entity_type=entity_type)
