"""Persistence of `materialization-table:<id>:<table>`: where each of a materialization's tables is, and its state."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from midstream.models import MaterializationTable
from midstream.storage.envelopes import decode_envelope, encode_payload_envelope, read_payload
from midstream.storage.keys import (
    MATERIALIZATION_TABLE_KEY_PREFIX,
    materialization_table_key,
    materialization_table_prefix,
)
from midstream.storage.table import TableProtocol

ENTITY_TYPE = "materialization-table"


def save_materialization_table(table: TableProtocol, record: MaterializationTable) -> dict[str, Any]:
    stored = encode_payload_envelope(
        entity_type=ENTITY_TYPE,
        entity_id=f"{record.materialization_id}:{record.name}",
        updated_at=record.updated_at,
        payload=record.to_dict(),
    )
    table[materialization_table_key(record.materialization_id, record.name)] = stored
    return stored


def get_materialization_table(table: TableProtocol, materialization_id: str, name: str) -> MaterializationTable | None:
    envelope = decode_envelope(
        table.get(materialization_table_key(materialization_id, name)),
        expected_entity_type=ENTITY_TYPE,
    )
    return None if envelope is None else read_payload(envelope, MaterializationTable.from_dict)


def list_materialization_tables(table: TableProtocol, materialization_id: str) -> list[MaterializationTable]:
    """A materialization's table records, in no particular order; `tables_in_order` orders them."""
    records: dict[str, MaterializationTable] = {}
    for _, raw in table.prefix_scan(materialization_table_prefix(materialization_id)):
        envelope = decode_envelope(raw, expected_entity_type=ENTITY_TYPE)
        if envelope is None:
            continue
        record = read_payload(envelope, MaterializationTable.from_dict)
        if record is None or record.materialization_id != materialization_id:
            continue
        current = records.get(record.name)
        # A record stranded in more than one shard: the newest write is the current state.
        if current is None or record.updated_at > current.updated_at:
            records[record.name] = record
    return list(records.values())


def iter_materialization_tables(table: TableProtocol) -> Iterator[MaterializationTable]:
    """Every table record of every materialization; the catalog lists them."""
    for _, raw in table.prefix_scan(MATERIALIZATION_TABLE_KEY_PREFIX):
        envelope = decode_envelope(raw, expected_entity_type=ENTITY_TYPE)
        record = None if envelope is None else read_payload(envelope, MaterializationTable.from_dict)
        if record is not None:
            yield record


def delete_materialization_table(table: TableProtocol, materialization_id: str, name: str) -> None:
    del table[materialization_table_key(materialization_id, name)]
