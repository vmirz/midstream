"""Creating a materialization's table: what the control plane's `create_table` step does (spec §9.1).

Blocking GCS calls throughout; the workflow runs this on a worker thread.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from pyiceberg.schema import Schema
from pyiceberg.table import Table

from midstream.lakehouse.catalog import GcsPointerCatalog, StaticTableDirectory, TableEntry
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.tables import TableIdentity, new_table_metadata


@dataclass(frozen=True, slots=True)
class CreatedTable:
    location: str
    metadata_location: str
    table_uuid: str
    schema_id: int
    last_column_id: int


def table_catalog(identity: TableIdentity, *, client: Any = None, writer: str = "control-plane") -> GcsPointerCatalog:
    """A catalog that knows exactly one table: `identity`'s."""
    directory = StaticTableDirectory(
        [TableEntry(identifier=identity.identifier, location=identity.location, materialization_id=identity.materialization_id)]
    )
    return GcsPointerCatalog(directory=directory, client=client, writer=writer)


def create_materializer_table(
    identity: TableIdentity,
    *,
    schema: Schema,
    fork_map: ForkMap,
    granularity: str,
    properties: dict[str, str],
    client: Any = None,
    writer: str = "control-plane",
) -> CreatedTable:
    """Create the table, or find it created by an earlier attempt for the same materialization.

    Raises `TableAlreadyExistsError` when the location holds another materialization's table.
    """
    catalog = table_catalog(identity, client=client, writer=writer)
    resolved_properties = dict(properties)
    resolved_properties.setdefault("materializer.fork-map", fork_map.to_json())
    metadata = new_table_metadata(
        location=identity.location,
        schema=schema,
        granularity=granularity,
        properties=resolved_properties,
    )
    table: Table = catalog.create_table_from_metadata(
        identity.identifier,
        location=identity.location,
        materialization_id=identity.materialization_id,
        metadata=metadata,
    )
    return CreatedTable(
        location=identity.location,
        metadata_location=table.metadata_location,
        table_uuid=str(table.metadata.table_uuid),
        schema_id=table.schema().schema_id,
        last_column_id=table.metadata.last_column_id,
    )


def schema_to_json(schema: Schema) -> dict[str, Any]:
    """The schema as Iceberg serializes it, as a plain dict."""
    return json.loads(schema.model_dump_json())


def schema_from_json(value: dict[str, Any]) -> Schema:
    return Schema.model_validate_json(json.dumps(value))
