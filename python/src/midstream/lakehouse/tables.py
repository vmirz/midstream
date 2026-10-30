"""The shape of a materializer table: metadata columns, partition spec, sort order, properties.

A materialization holds one Iceberg v2 table per source topic, each at
`<destination>/<storage prefix>/<materialization id>/<table>` and identified as `<dataset>.<table>`:
its files are grouped by the materialization that writes it, and its dataset exists only in the
catalog, so a table's name and its location do not depend on each other. A table's first seven
columns describe the Kafka record and are fixed
at creation with fixed field ids, so readers can rely on them (the console reads the bounds of ids
1-3 from manifests). Payload fields follow from id 10 on, top-level fields keeping the record's own
key names.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import uuid

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import DayTransform, HourTransform, IdentityTransform, MonthTransform
from pyiceberg.types import (
    IntegerType,
    LongType,
    MapType,
    NestedField,
    StringType,
    TimestamptzType,
)

FORMAT_VERSION = 2

TIMESTAMP_FIELD_ID = 1
PARTITION_FIELD_ID = 2
OFFSET_FIELD_ID = 3
KEY_FIELD_ID = 4
HEADERS_FIELD_ID = 5
MATERIALIZED_AT_FIELD_ID = 6
UNMAPPED_FIELD_ID = 7
HEADERS_KEY_ID = 8
HEADERS_VALUE_ID = 9
#: The first id a payload field gets. Ids 1-9 are the metadata columns, the header map's key and
#: value included.
FIRST_PAYLOAD_FIELD_ID = 10
#: Partition fields have ids of their own, from 1000.
PARTITION_SPEC_FIELD_ID = 1000

TIMESTAMP_COLUMN = "_kafka_timestamp"
PARTITION_COLUMN = "_kafka_partition"
OFFSET_COLUMN = "_kafka_offset"
KEY_COLUMN = "_kafka_key"
HEADERS_COLUMN = "_kafka_headers"
MATERIALIZED_AT_COLUMN = "_materialized_at"
UNMAPPED_COLUMN = "_unmapped"

METADATA_COLUMN_NAMES = (
    TIMESTAMP_COLUMN,
    PARTITION_COLUMN,
    OFFSET_COLUMN,
    KEY_COLUMN,
    HEADERS_COLUMN,
    MATERIALIZED_AT_COLUMN,
    UNMAPPED_COLUMN,
)
METADATA_FIELD_IDS = frozenset(range(1, FIRST_PAYLOAD_FIELD_ID))

PARTITION_GRANULARITIES = ("hour", "day", "month", "none")
DEFAULT_PARTITION_GRANULARITY = "day"
PARTITION_NAMES = {"day": "ts_day", "hour": "ts_hour", "month": "ts_month"}
#: Each granularity's Iceberg transform of `_kafka_timestamp`. Iceberg has none for a week.
PARTITION_TRANSFORMS = {"hour": HourTransform, "day": DayTransform, "month": MonthTransform}

#: Table properties every materializer table is created with (spec §6.3). The ones that depend on
#: settings are added by `table_properties`.
BASE_PROPERTIES = {
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": str(256 * 1024 * 1024),
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
    "commit.manifest-merge.enabled": "true",
    "commit.manifest.min-count-to-merge": "50",
    "commit.manifest.target-size-bytes": str(8 * 1024 * 1024),
    "commit.retry.num-retries": "6",
}

PROPERTY_MATERIALIZATION_ID = "materializer.materialization-id"
PROPERTY_SOURCE_TOPIC = "materializer.source-topic"
PROPERTY_CREATED_BY = "materializer.created-by"
PROPERTY_CREATED_AT = "materializer.created-at"
PROPERTY_PACKAGE_VERSION = "materializer.package-version"
PROPERTY_PARTITION_GRANULARITY = "materializer.partition-granularity"
PROPERTY_FORK_MAP = "materializer.fork-map"
#: What the table protects: its policy, its protected fields and their history (`protection/policy.py`).
PROPERTY_PROTECTION = "materializer.protection"


def metadata_fields() -> list[NestedField]:
    """The seven Kafka metadata columns, with their fixed ids."""
    return [
        NestedField(TIMESTAMP_FIELD_ID, TIMESTAMP_COLUMN, TimestamptzType(), required=False,
                    doc="Kafka record timestamp; null when Kafka reports none"),
        NestedField(PARTITION_FIELD_ID, PARTITION_COLUMN, IntegerType(), required=True,
                    doc="Kafka partition"),
        NestedField(OFFSET_FIELD_ID, OFFSET_COLUMN, LongType(), required=True,
                    doc="Kafka offset"),
        NestedField(KEY_FIELD_ID, KEY_COLUMN, StringType(), required=False,
                    doc="Kafka record key, UTF-8; base64 prefixed 'b64:' when not UTF-8"),
        NestedField(
            HEADERS_FIELD_ID,
            HEADERS_COLUMN,
            MapType(HEADERS_KEY_ID, StringType(), HEADERS_VALUE_ID, StringType(), value_required=False),
            required=False,
            doc="Kafka record headers; values UTF-8, base64 prefixed 'b64:' when not UTF-8",
        ),
        NestedField(MATERIALIZED_AT_FIELD_ID, MATERIALIZED_AT_COLUMN, TimestamptzType(), required=True,
                    doc="When the materializer read the record"),
        NestedField(UNMAPPED_FIELD_ID, UNMAPPED_COLUMN, StringType(), required=False,
                    doc="JSON object of the values no column could take; null otherwise"),
    ]


def build_schema(payload_fields: list[NestedField] | tuple[NestedField, ...] = (), *, schema_id: int = 0) -> Schema:
    """A table schema: the metadata columns followed by `payload_fields` (ids already assigned)."""
    return Schema(*metadata_fields(), *payload_fields, schema_id=schema_id)


def payload_fields(schema: Schema) -> list[NestedField]:
    """The schema's top-level payload fields, in order."""
    return [field for field in schema.fields if field.field_id not in METADATA_FIELD_IDS]


def partition_spec(granularity: str) -> PartitionSpec:
    """The partition spec for `granularity`: the hour, day or month of `_kafka_timestamp`, or none."""
    if granularity not in PARTITION_GRANULARITIES:
        raise ValueError(f"partition granularity must be one of: {', '.join(PARTITION_GRANULARITIES)}")
    if granularity == "none":
        return PartitionSpec(spec_id=0)
    transform = PARTITION_TRANSFORMS[granularity]()
    return PartitionSpec(
        PartitionField(
            source_id=TIMESTAMP_FIELD_ID,
            field_id=PARTITION_SPEC_FIELD_ID,
            transform=transform,
            name=PARTITION_NAMES[granularity],
        ),
        spec_id=0,
    )


def sort_order() -> SortOrder:
    """`_kafka_timestamp` ascending: advisory, honoured by compaction."""
    return SortOrder(
        SortField(
            source_id=TIMESTAMP_FIELD_ID,
            transform=IdentityTransform(),
            direction=SortDirection.ASC,
            null_order=NullOrder.NULLS_FIRST,
        ),
        order_id=1,
    )


@dataclass(frozen=True, slots=True)
class TableIdentity:
    """Where one of a materialization's tables lives and what it is called."""

    #: The table's Iceberg namespace: its dataset's name.
    namespace: str
    #: The table's name in its dataset.
    name: str
    materialization_id: str
    location: str

    @property
    def identifier(self) -> tuple[str, str]:
        return (self.namespace, self.name)


def materialization_location(destination_uri: str, *, storage_prefix: str, materialization_id: str) -> str:
    """`<destination>/<storage prefix>/<materialization id>`: every table of it is below.

    The id never names anything else, so a table dropped and created again elsewhere never meets a
    half-deleted location; the storage prefix keeps deployments sharing a bucket apart. The drop's
    guard recognises the directory by the id (`storage/objects.py`).
    """
    return f"{destination_uri.rstrip('/')}/{storage_prefix}/{materialization_id}"


def table_location(destination_uri: str, *, storage_prefix: str, materialization_id: str, table: str) -> str:
    """`<materialization location>/<table>`: one directory per table."""
    base = materialization_location(destination_uri, storage_prefix=storage_prefix, materialization_id=materialization_id)
    return f"{base}/{table}"


def table_identity(
    *,
    destination_uri: str,
    storage_prefix: str,
    dataset: str,
    materialization_id: str,
    table: str,
) -> TableIdentity:
    """A table's identifier, `(dataset, table)`, and its location under its materialization."""
    return TableIdentity(
        namespace=dataset,
        name=table,
        materialization_id=materialization_id,
        location=table_location(
            destination_uri,
            storage_prefix=storage_prefix,
            materialization_id=materialization_id,
            table=table,
        ),
    )


def table_properties(
    *,
    materialization_id: str,
    source_topic: str,
    partition_granularity: str,
    created_by: str | None,
    created_at: str,
    package_version: str,
    snapshot_retention_hours: int,
    min_snapshots_to_keep: int,
    fork_map_json: str | None = None,
    protection_json: str | None = None,
) -> dict[str, str]:
    properties = dict(BASE_PROPERTIES)
    properties.update(
        {
            "history.expire.max-snapshot-age-ms": str(snapshot_retention_hours * 3_600_000),
            "history.expire.min-snapshots-to-keep": str(min_snapshots_to_keep),
            PROPERTY_MATERIALIZATION_ID: materialization_id,
            PROPERTY_SOURCE_TOPIC: source_topic,
            PROPERTY_CREATED_BY: created_by or "",
            PROPERTY_CREATED_AT: created_at,
            PROPERTY_PACKAGE_VERSION: package_version,
            PROPERTY_PARTITION_GRANULARITY: partition_granularity,
        }
    )
    if fork_map_json is not None:
        properties[PROPERTY_FORK_MAP] = fork_map_json
    if protection_json is not None:
        properties[PROPERTY_PROTECTION] = protection_json
    return properties


def new_table_metadata(
    *,
    location: str,
    schema: Schema,
    granularity: str,
    properties: dict[str, str],
    table_uuid: uuid.UUID | None = None,
) -> TableMetadataV2:
    """Metadata version 0 of a new table, keeping `schema`'s field ids exactly as given.

    PyIceberg's `new_table_metadata` re-assigns every field id (all fields of a struct first, then
    nested ones), which would move the metadata columns off their fixed ids.
    """
    spec = partition_spec(granularity)
    order = sort_order()
    return TableMetadataV2(
        location=location.rstrip("/"),
        table_uuid=table_uuid or uuid.uuid4(),
        last_updated_ms=int(datetime.now(tz=UTC).timestamp() * 1000),
        last_column_id=max(schema.highest_field_id, FIRST_PAYLOAD_FIELD_ID - 1),
        schemas=[schema],
        current_schema_id=schema.schema_id,
        partition_specs=[spec],
        default_spec_id=spec.spec_id,
        last_partition_id=PARTITION_SPEC_FIELD_ID if spec.fields else PARTITION_SPEC_FIELD_ID - 1,
        sort_orders=[order],
        default_sort_order_id=order.order_id,
        properties=properties,
    )


def partition_granularity_of(properties: dict[str, str]) -> str:
    value = properties.get(PROPERTY_PARTITION_GRANULARITY, DEFAULT_PARTITION_GRANULARITY)
    return value if value in PARTITION_GRANULARITIES else DEFAULT_PARTITION_GRANULARITY
