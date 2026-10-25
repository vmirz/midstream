"""Persistence for settings overrides and their change history, in the `settings` table.

The settings table is separate from the reconciliation `state` table: it holds configuration, is
written only by the settings agent, and every component that reads configuration binds it.
"""

from __future__ import annotations

from typing import Any

from materializer_control_plane.models import SettingsChangeRecord, SettingsDocument
from materializer_control_plane.storage.envelopes import decode_envelope, encode_payload_envelope
from materializer_control_plane.storage.table import TableProtocol

SETTINGS_KEY_PREFIX = "settings:"
SETTINGS_CHANGE_KEY_PREFIX = "settings-change:"
SETTINGS_ENTITY_TYPE = "settings"
SETTINGS_CHANGE_ENTITY_TYPE = "settings-change"

#: Changes kept per scope; the oldest is dropped as each new one is recorded.
SETTINGS_HISTORY_LIMIT = 100

# Revisions are zero padded so a prefix scan lists a scope's changes in order.
_REVISION_KEY_WIDTH = 10


def settings_key(scope: str) -> str:
    return f"{SETTINGS_KEY_PREFIX}{scope}"


def settings_routing_key(scope: str) -> str:
    """Kafka key for a scope's change requests: one partition, so they apply in order."""
    return settings_key(scope)


def settings_change_prefix(scope: str) -> str:
    return f"{SETTINGS_CHANGE_KEY_PREFIX}{scope}:"


def settings_change_key(scope: str, revision: int) -> str:
    return f"{settings_change_prefix(scope)}{revision:0{_REVISION_KEY_WIDTH}d}"


def get_settings_document(table: TableProtocol | None, scope: str) -> SettingsDocument | None:
    """Load a scope's overrides, or None when nothing in it was ever changed.

    Scans the exact key and keeps the highest revision, so a stale copy in another table shard
    can never stand in for the current one.
    """
    if table is None:
        return None
    key = settings_key(scope)
    newest: SettingsDocument | None = None
    for scanned_key, raw_value in table.prefix_scan(key):
        if scanned_key != key:
            continue
        document = _decode_document(raw_value, scope)
        if document is not None and (newest is None or document.revision > newest.revision):
            newest = document
    if newest is not None:
        return newest
    return _decode_document(table.get(key), scope)


def save_settings_document(table: TableProtocol, document: SettingsDocument, *, updated_at: str) -> dict[str, Any]:
    """Persist a scope's overrides."""
    stored = encode_payload_envelope(
        entity_type=SETTINGS_ENTITY_TYPE,
        entity_id=document.scope,
        updated_at=updated_at,
        payload=document.to_dict(),
    )
    table[settings_key(document.scope)] = stored
    return stored


def save_settings_change(
    table: TableProtocol,
    record: SettingsChangeRecord,
    *,
    limit: int = SETTINGS_HISTORY_LIMIT,
) -> None:
    """Record an applied change, keeping only the newest `limit` changes of its scope."""
    table[settings_change_key(record.scope, record.revision)] = encode_payload_envelope(
        entity_type=SETTINGS_CHANGE_ENTITY_TYPE,
        entity_id=f"{record.scope}:{record.revision}",
        updated_at=record.updated_at,
        payload=record.to_dict(),
    )
    keys = sorted(key for key, _ in table.prefix_scan(settings_change_prefix(record.scope)))
    for key in keys[: max(len(keys) - limit, 0)]:
        del table[key]


def list_settings_changes(
    table: TableProtocol | None,
    scope: str,
    *,
    limit: int | None = None,
) -> list[SettingsChangeRecord]:
    """A scope's recorded changes, newest first."""
    if table is None:
        return []
    records: dict[int, SettingsChangeRecord] = {}
    for _, raw_value in table.prefix_scan(settings_change_prefix(scope)):
        envelope = decode_envelope(raw_value, expected_entity_type=SETTINGS_CHANGE_ENTITY_TYPE)
        if envelope is None:
            continue
        record = SettingsChangeRecord.from_dict(envelope.payload)
        records[record.revision] = record
    ordered = [records[revision] for revision in sorted(records, reverse=True)]
    return ordered if limit is None else ordered[:limit]


def _decode_document(raw_value: dict[str, Any] | None, scope: str) -> SettingsDocument | None:
    envelope = decode_envelope(raw_value, expected_entity_type=SETTINGS_ENTITY_TYPE)
    if envelope is None or envelope.entity_id != scope:
        return None
    return SettingsDocument.from_dict(envelope.payload)
