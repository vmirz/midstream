"""Writing Iceberg-conformant Parquet: field ids in the file schema, zstd, statistics on.

Readers map columns by field id (`PARQUET:field_id`), which is what makes a renamed key, a fork or
a field added later read correctly from every file ever written.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io.fileformat import FileFormatFactory
from pyiceberg.io.pyarrow import ArrowAccessor, ArrowProjectionVisitor, schema_to_pyarrow
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema, visit_with_partner
from pyiceberg.types import ListType, MapType

DEFAULT_COMPRESSION = "zstd"
#: Rows per row group are chosen so a row group holds about this much staged JSON.
DEFAULT_ROW_GROUP_BYTES = 32 * 1024 * 1024


def arrow_schema(schema: Schema) -> pa.Schema:
    """The Arrow schema for `schema`, every field carrying its Iceberg field id."""
    return schema_to_pyarrow(schema, include_field_ids=True)


def project_batch(schema: Schema, file_schema: Schema, batch: pa.RecordBatch) -> pa.RecordBatch:
    """`batch`, read from a data file of `file_schema`, projected onto `schema` by field id.

    A field the file lacks comes back null, and every field carries its field id, as PyIceberg's own
    projection does (`_to_requested_schema`) - but a null list or map of structs stays null, where
    PyIceberg 0.12 returns it empty (`_NullKeepingProjection`).
    """
    visitor = _NullKeepingProjection(file_schema, include_field_ids=True, format_model=FileFormatFactory.get(FileFormat.PARQUET))
    return pa.RecordBatch.from_struct_array(visit_with_partner(schema, batch, visitor, ArrowAccessor(file_schema)))


class _NullKeepingProjection(ArrowProjectionVisitor):
    """PyIceberg's projection, keeping the nulls of lists and maps whose elements are structs.

    PyIceberg 0.12 rebuilds such an array from its offsets alone (to reorder struct fields, which
    Arrow's cast cannot: apache/arrow#38809), and offsets carry no nulls: a record without the key
    would read as an empty list. Rebuilt with the array's own null mask, it reads as it was written.
    """

    def list(self, list_type: ListType, list_array: pa.Array | None, value_array: pa.Array | None) -> pa.Array | None:
        if isinstance(list_array, (pa.ListArray, pa.LargeListArray)) and isinstance(value_array, pa.StructArray):
            mask = list_array.is_null() if list_array.null_count else None
            rebuilt = pa.LargeListArray.from_arrays(list_array.offsets, value_array, mask=mask)
            element = self._cast_if_needed(list_type.element_field, value_array)
            initializer = pa.large_list if isinstance(list_array, pa.LargeListArray) else pa.list_
            return rebuilt.cast(initializer(self._construct_field(list_type.element_field, element.type)))
        return super().list(list_type, list_array, value_array)

    def map(
        self, map_type: MapType, map_array: pa.Array | None, key_result: pa.Array | None, value_result: pa.Array | None
    ) -> pa.Array | None:
        if isinstance(map_array, pa.MapArray) and key_result is not None and isinstance(value_result, pa.StructArray):
            keys = self._cast_if_needed(map_type.key_field, key_result)
            values = self._cast_if_needed(map_type.value_field, value_result)
            arrow_type = pa.map_(
                self._construct_field(map_type.key_field, keys.type),
                self._construct_field(map_type.value_field, values.type),
            )
            mask = map_array.is_null() if map_array.null_count else None
            return pa.MapArray.from_arrays(map_array.offsets, keys, values, arrow_type, mask=mask)
        return super().map(map_type, map_array, key_result, value_result)


def table_from_rows(rows: list[dict[str, Any]], schema: Schema | pa.Schema) -> pa.Table:
    target = schema if isinstance(schema, pa.Schema) else arrow_schema(schema)
    return pa.Table.from_pylist(rows, schema=target)


def write_table(
    table: pa.Table,
    path: str | Path,
    *,
    row_group_size: int | None = None,
    compression: str = DEFAULT_COMPRESSION,
) -> int:
    """Write `table` to `path`; return the file's size."""
    pq.write_table(
        table,
        str(path),
        compression=compression,
        write_statistics=True,
        row_group_size=row_group_size,
    )
    return Path(path).stat().st_size


def rows_per_group(row_count: int, *, staged_bytes: int, target_bytes: int = DEFAULT_ROW_GROUP_BYTES) -> int:
    """Rows per row group so a group holds about `target_bytes` of the staged rows."""
    if row_count < 1:
        return 1
    if staged_bytes <= 0:
        return row_count
    per_row = max(staged_bytes / row_count, 1)
    return max(1, min(row_count, int(target_bytes // per_row)))
