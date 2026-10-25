"""Data files: their names, their partition values, and their `DataFile` entries (spec §7.5).

The runtime writes Iceberg-conformant Parquet itself - field ids in the schema, one file per
partition value - so committing a file is a matter of describing it: its size, record count,
partition value and per-column statistics, read from the Parquet footer alone (two small range
requests). PyIceberg's `add_files` is not used: it is meant for files without field ids and would
set a name mapping on the table.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
import io
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import (
    MetricModeTypes,
    MetricsMode,
    StatisticsCollector,
    compute_statistics_plan,
    data_file_statistics_from_parquet_metadata,
    match_metrics_mode,
    parquet_path_to_id_mapping,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import BinaryType, IcebergType, ListType, MapType, StringType, StructType

from materializer_control_plane.lakehouse.gcs import is_not_found, parse_gcs_uri
from materializer_control_plane.lakehouse.tables import PARTITION_NAMES

EPOCH_DATE = date(1970, 1, 1)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
NULL_PARTITION_TEXT = "__null__"
#: Footer bytes fetched speculatively with the length: most footers fit, saving a request.
FOOTER_PREFETCH_BYTES = 256 * 1024


def partition_text(granularity: str, moment: datetime | None) -> str | None:
    """The partition value a row with Kafka timestamp `moment` belongs to, as text.

    `2026-10-18` for day, `2026-10-18-10` for hour, `2026-10` for month, None when unpartitioned;
    `__null__` for a row without a timestamp in a partitioned table.
    """
    if granularity == "none":
        return None
    if moment is None:
        return NULL_PARTITION_TEXT
    moment = moment.astimezone(UTC)
    if granularity == "day":
        return moment.date().isoformat()
    if granularity == "month":
        return f"{moment.year:04d}-{moment.month:02d}"
    return f"{moment.date().isoformat()}-{moment.hour:02d}"


def partition_values(granularity: str, text: str | None) -> dict[str, str | None]:
    """The event's `partition_value`: `{"ts_day": "2026-10-18"}`, `{}` when unpartitioned."""
    if granularity == "none":
        return {}
    return {PARTITION_NAMES[granularity]: None if text in (None, NULL_PARTITION_TEXT) else text}


def partition_record(granularity: str, values: dict[str, str | None]) -> Record:
    """The DataFile partition tuple: days, hours or months since the epoch."""
    if granularity == "none":
        return Record()
    text = values.get(PARTITION_NAMES[granularity])
    if text is None:
        return Record(None)
    if granularity == "day":
        return Record((date.fromisoformat(text) - EPOCH_DATE).days)
    if granularity == "month":
        return Record((int(text[:4]) - EPOCH_DATE.year) * 12 + int(text[5:7]) - 1)
    day, hour = text[:10], int(text[11:13])
    moment = datetime.fromisoformat(day).replace(tzinfo=UTC, hour=hour)
    return Record(int((moment - EPOCH).total_seconds() // 3600))


def partition_text_of(granularity: str, partition: tuple) -> str | None:
    """A partition tuple (days, hours or months since the epoch) as the text `partition_text` gives."""
    if granularity == "none" or not partition:
        return None
    value = partition[0]
    if value is None:
        return NULL_PARTITION_TEXT
    if granularity == "day":
        return (EPOCH_DATE.fromordinal(EPOCH_DATE.toordinal() + int(value))).isoformat()
    if granularity == "month":
        years, month = divmod(int(value), 12)
        return f"{EPOCH_DATE.year + years:04d}-{month + 1:02d}"
    moment = datetime.fromtimestamp(int(value) * 3600, tz=UTC)
    return f"{moment.date().isoformat()}-{moment.hour:02d}"


def partition_directory(granularity: str, text: str | None) -> str:
    if granularity == "none" or text is None:
        return ""
    return f"{PARTITION_NAMES[granularity]}={text}/"


def data_file_uri(
    location: str,
    *,
    granularity: str,
    partition_value_text: str | None,
    kafka_partition: int,
    offset_start: int,
    offset_end: int,
    member_id: str,
    unique: str,
) -> str:
    """`L/data/ts_day=2026-10-18/p03-o0000000100-0000000199-<member>-<uuid>.parquet`."""
    name = (
        f"p{kafka_partition:02d}-o{offset_start:010d}-{offset_end:010d}-"
        f"{_safe(member_id)}-{unique}.parquet"
    )
    return f"{location.rstrip('/')}/data/{partition_directory(granularity, partition_value_text)}{name}"


def compacted_file_uri(location: str, *, granularity: str, partition_value_text: str | None, unique: str) -> str:
    """`L/data/ts_day=2026-10-18/compact-<unique>.parquet`: a compaction's output."""
    return f"{location.rstrip('/')}/data/{partition_directory(granularity, partition_value_text)}compact-{_safe(unique)}.parquet"


def read_parquet_footer(client: Any, uri: str) -> tuple[pq.FileMetaData, int]:
    """The Parquet footer of an object and the object's size, in one or two range requests."""
    bucket_name, name = parse_gcs_uri(uri)
    blob = client.bucket(bucket_name).get_blob(name)
    if blob is None:
        raise FileNotFoundError(uri)
    size = int(blob.size)
    if size < 12:
        raise ValueError(f"{uri} is too small to be a Parquet file ({size} bytes)")
    start = max(0, size - FOOTER_PREFETCH_BYTES)
    try:
        tail = blob.download_as_bytes(start=start, end=size - 1)
        if tail[-4:] != b"PAR1":
            raise ValueError(f"{uri} is not a Parquet file")
        footer_length = int.from_bytes(tail[-8:-4], "little")
        if footer_length + 8 > len(tail):
            footer_start = size - 8 - footer_length
            tail = blob.download_as_bytes(start=footer_start, end=size - 1)
    except Exception as exc:
        if is_not_found(exc):
            raise FileNotFoundError(uri) from exc
        raise
    footer = tail[-(footer_length + 8):]
    # A footer is readable alone once it is preceded by the leading magic bytes.
    metadata = pq.read_metadata(pa.BufferReader(pa.py_buffer(b"PAR1" + footer)))
    return metadata, size


def footer_from_bytes(data: bytes) -> pq.FileMetaData:
    return pq.read_metadata(io.BytesIO(data))


def build_data_file(
    table_metadata: TableMetadata,
    *,
    file_path: str,
    file_size: int,
    parquet_metadata: pq.FileMetaData,
    granularity: str,
    partition_value: dict[str, str | None] | None = None,
    partition: Record | None = None,
) -> DataFile:
    """The manifest entry for a data file, its statistics from the footer, its partition as announced.

    The partition is `partition` when given (a compaction's, copied from its inputs), else derived
    from the announced `partition_value`.
    """
    schema = table_metadata.schema()
    statistics = data_file_statistics_from_parquet_metadata(
        parquet_metadata=parquet_metadata,
        stats_columns=statistics_plan(schema, table_metadata.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(schema),
    )
    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=partition if partition is not None else partition_record(granularity, partition_value or {}),
        file_size_in_bytes=file_size,
        sort_order_id=None,
        spec_id=table_metadata.default_spec_id,
        equality_ids=None,
        key_metadata=None,
        **statistics.to_serialized_dict(),
    )


def statistics_plan(schema: Schema, properties: dict[str, str]) -> dict[int, StatisticsCollector]:
    """PyIceberg's statistics plan, with bounds kept for fields nested only in structs.

    PyIceberg collects only counts for every nested column. Java Iceberg keeps lower and upper
    bounds for a struct's fields and drops them only inside lists and maps, whose Parquet statistics
    mix every element; this does the same, so a filter on `customer.email` can prune files too.
    """
    plan = compute_statistics_plan(schema, properties)
    default = match_metrics_mode(properties.get("write.metadata.metrics.default", "truncate(16)"))
    for field_id in _struct_only_leaves(schema):
        collector = plan.get(field_id)
        if collector is None or collector.mode.type != MetricModeTypes.COUNTS:
            continue
        column_mode = properties.get(f"write.metadata.metrics.column.{collector.column_name}")
        mode = default if column_mode is None else match_metrics_mode(column_mode)
        if mode.type == MetricModeTypes.TRUNCATE and not isinstance(collector.iceberg_type, (StringType, BinaryType)):
            mode = MetricsMode(MetricModeTypes.FULL)
        plan[field_id] = StatisticsCollector(
            field_id=collector.field_id,
            iceberg_type=collector.iceberg_type,
            mode=mode,
            column_name=collector.column_name,
        )
    return plan


def _struct_only_leaves(schema: Schema) -> list[int]:
    leaves: list[int] = []

    def visit(iceberg_type: IcebergType, nested: bool) -> None:
        if isinstance(iceberg_type, StructType):
            for item in iceberg_type.fields:
                if isinstance(item.field_type, StructType):
                    visit(item.field_type, True)
                elif isinstance(item.field_type, (ListType, MapType)):
                    continue
                elif nested:
                    leaves.append(item.field_id)

    visit(schema.as_struct(), False)
    return leaves


def _safe(text: str) -> str:
    return "".join(character if character.isalnum() or character in "-_." else "_" for character in text)
