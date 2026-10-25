"""A materialization's table records, as `create_table` stores them, for tests that need them."""

from __future__ import annotations

from typing import Any

from midstream.lakehouse.tables import table_identity
from midstream.models import Materialization, MaterializationTable, TableSpec
from midstream.table_names import table_names_for_topics

#: The dataset tests put tables in unless they say otherwise.
DATASET = "shop"
DATASET_ID = "ds-000000000001"


def table_spec(name: str, topic: str, *, dataset: str = DATASET, dataset_id: str = DATASET_ID) -> TableSpec:
    """One table, in `DATASET` unless another is named."""
    return TableSpec(name=name, source_topic=topic, dataset=dataset, dataset_id=dataset_id)


def table_specs(*topics: str, dataset: str = DATASET, dataset_id: str = DATASET_ID) -> tuple[TableSpec, ...]:
    """The tables a materialization of `topics` gets, named after them as the create view names
    them when no names are chosen, all in one dataset."""
    return tuple(
        table_spec(name, topic, dataset=dataset, dataset_id=dataset_id)
        for name, topic in zip(table_names_for_topics(topics), topics)
    )


def build_table_record(
    materialization: Materialization,
    *,
    storage_prefix: str = "materializer",
    table: str | None = None,
    **overrides: Any,
) -> MaterializationTable:
    """The record of one of `materialization`'s tables, kept ones included: `table`, or its first."""
    spec = materialization.tables[0] if table is None else materialization.any_table(table)
    assert spec is not None, f"materialization has no table {table}"
    identity = table_identity(
        destination_uri=materialization.destination_uri,
        storage_prefix=storage_prefix,
        dataset=spec.dataset,
        materialization_id=materialization.materialization_id,
        table=spec.name,
    )
    values: dict[str, Any] = dict(
        materialization_id=materialization.materialization_id,
        dataset=spec.dataset,
        dataset_id=spec.dataset_id,
        name=identity.name,
        source_topic=spec.source_topic,
        location=identity.location,
        table_uuid="6b1c0000-0000-0000-0000-000000000000",
        partition_granularity=materialization.partition_granularity,
        created_at=materialization.created_at,
        updated_at=materialization.updated_at,
        metadata_location=f"{identity.location}/metadata/00000-6b1c0000.metadata.json",
        schema_id=0,
        totals={"records": 0, "files": 0, "bytes": 0},
        snapshots=0,
    )
    values.update(overrides)
    return MaterializationTable(**values)


def build_table_records(materialization: Materialization, *, storage_prefix: str = "materializer", **overrides: Any) -> list[MaterializationTable]:
    """The records of every table of `materialization`, in its order."""
    return [
        build_table_record(materialization, storage_prefix=storage_prefix, table=spec.name, **overrides)
        for spec in materialization.tables
    ]
