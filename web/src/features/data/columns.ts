import { fieldName, isMetadataColumn } from '@shared/fieldPaths';
import type { QueryColumn } from '@shared/types';

export type ColumnGroupId = 'payload' | 'kafka';

export interface ColumnGroup {
  id: ColumnGroupId;
  label: string;
  hint: string;
  columns: QueryColumn[];
}

/** The record's Kafka timestamp: the axis every time range filters on, and the column read first. */
export const TIME_COLUMN = '_kafka_timestamp';

/**
 * A column is Kafka metadata only if it is one the runtime writes. A field of the record that
 * happens to start with an underscore — `_id` — is still a field.
 */
export function groupOf(column: string): ColumnGroupId {
  return isMetadataColumn(column) ? 'kafka' : 'payload';
}

const GROUP_ORDER: ColumnGroupId[] = ['payload', 'kafka'];

const GROUP_META: Record<ColumnGroupId, { label: string; hint: string }> = {
  payload: { label: 'Fields', hint: 'Materialized fields from your schema' },
  kafka: { label: 'Kafka metadata', hint: 'Per-record provenance written by the runtime' },
};

/**
 * Split columns into their groups, dropping empty ones.
 *
 * Payload order follows the schema rather than the Parquet order, so fields read in the order the
 * materialization declares them; the Kafka timestamp leads its group.
 */
export function groupColumns(columns: QueryColumn[], schemaOrder: string[]): ColumnGroup[] {
  const rank = new Map(schemaOrder.map((name, index) => [name, index]));
  const buckets = new Map<ColumnGroupId, QueryColumn[]>(GROUP_ORDER.map((id) => [id, []]));

  for (const column of columns) buckets.get(groupOf(column.name))?.push(column);

  const payload = buckets.get('payload');
  if (payload && rank.size > 0) {
    payload.sort((a, b) => {
      const left = rank.get(a.name);
      const right = rank.get(b.name);
      if (left !== undefined && right !== undefined) return left - right;
      if (left !== undefined) return -1;
      if (right !== undefined) return 1;
      return 0;
    });
  }
  const kafka = buckets.get('kafka');
  if (kafka) kafka.sort((a, b) => Number(b.name === TIME_COLUMN) - Number(a.name === TIME_COLUMN));

  return GROUP_ORDER.filter((id) => (buckets.get(id)?.length ?? 0) > 0).map((id) => ({
    id,
    label: GROUP_META[id].label,
    hint: GROUP_META[id].hint,
    columns: buckets.get(id) ?? [],
  }));
}

/**
 * Columns shown before anyone changes them: the record's timestamp, then the payload.
 *
 * The other metadata columns are provenance — partition, offset, key, headers — and showing them
 * by default puts infrastructure ahead of the data. A result with no payload columns at all, such
 * as a statement's, shows everything, so the grid is never mysteriously empty.
 */
export function defaultVisibleColumns(columns: QueryColumn[]): string[] {
  const payload = columns.filter((column) => groupOf(column.name) === 'payload');
  if (payload.length === 0) return columns.map((column) => column.name);
  const timestamp = columns.some((column) => column.name === TIME_COLUMN) ? [TIME_COLUMN] : [];
  return [...timestamp, ...payload.map((column) => column.name)];
}

/** Grid order: the timestamp first when shown, then the payload, then the rest of the metadata. */
export function gridOrder(columns: QueryColumn[], schemaOrder: string[]): QueryColumn[] {
  const groups = groupColumns(columns, schemaOrder);
  const timestamp = columns.filter((column) => column.name === TIME_COLUMN);
  const rest = groups.flatMap((group) => group.columns).filter((column) => column.name !== TIME_COLUMN);
  return [...timestamp, ...rest];
}

/**
 * How one column is written in the rail and the grid header.
 *
 * Split into a `prefix` that may be truncated and a `leaf` that must not be. Materialized fields
 * are deep paths that differ only at the end — `caused_by.id` / `caused_by.kind`,
 * `observation_policy.attempt` / `.max_attempts` / `.mode` / `.scheduled_for` — so ordinary
 * end-truncation renders whole groups of columns identical and unusable.
 */
export interface ColumnLabel {
  full: string;
  display: string;
  prefix: string;
  leaf: string;
}

/**
 * Display labels for a column set.
 *
 * Columns arrive already named the way people should read them — the server drops the schema's
 * `payload.` root — so a label is the name itself, split where it may be truncated.
 */
export function buildColumnLabels(columns: QueryColumn[]): Map<string, ColumnLabel> {
  return new Map(columns.map((column) => [column.name, splitLabel(column.name)]));
}

function splitLabel(name: string): ColumnLabel {
  const cut = name.lastIndexOf('.');
  return {
    full: name,
    display: name,
    prefix: cut === -1 ? '' : name.slice(0, cut + 1),
    leaf: cut === -1 ? name : name.slice(cut + 1),
  };
}

/** The label for a column, including one absent from the map — a hand-written SQL projection, say. */
export function labelFor(labels: Map<string, ColumnLabel>, name: string): ColumnLabel {
  return labels.get(name) ?? splitLabel(name);
}

/**
 * The column a name from an older link means.
 *
 * Columns used to carry the schema's `payload.` root, and links made then still say
 * `payload.status`. A name that is a column is that column; otherwise one that drops the root and
 * then is a column means that one. Null when it names nothing in `columns`.
 */
export function resolveColumnName(name: string, columns: ReadonlyArray<{ name: string }>): string | null {
  if (columns.some((column) => column.name === name)) return name;
  const renamed = fieldName(name);
  return renamed !== name && columns.some((column) => column.name === renamed) ? renamed : null;
}

const NUMERIC_TYPES = /^(TINY|SMALL|BIG|HUGE)?INT|^DECIMAL|^DOUBLE|^FLOAT|^REAL|^NUMERIC|^UINTEGER|^UBIGINT/i;
const TEMPORAL_TYPES = /^DATE|^TIME|^TIMESTAMP/i;

/** A list column holds many values per row, so it can be matched but not ordered. */
export function isListType(type: string): boolean {
  return /\[\]$/.test(type) || /^LIST/i.test(type);
}

export function isNumericType(type: string): boolean {
  return !isListType(type) && NUMERIC_TYPES.test(type);
}

export function isComparableType(type: string): boolean {
  return !isListType(type) && (isNumericType(type) || TEMPORAL_TYPES.test(type));
}

/** `VARCHAR` → `str`, `BIGINT` → `int`: enough to scan a list without eating the width. */
export function compactType(type: string): string {
  const upper = type.toUpperCase();
  // Containers first: `MAP(VARCHAR, VARCHAR)` and `STRUCT(a VARCHAR)` hold text, but are not text.
  if (upper.startsWith('STRUCT')) return 'obj';
  if (upper.startsWith('MAP')) return 'map';
  if (upper.includes('CHAR') || upper.includes('TEXT') || upper.includes('STRING')) return 'str';
  if (upper.startsWith('TIMESTAMP') || upper.startsWith('DATE') || upper.startsWith('TIME')) return 'time';
  if (upper.startsWith('BOOL')) return 'bool';
  if (/INT|DECIMAL|DOUBLE|FLOAT|REAL|NUMERIC/.test(upper)) return 'num';
  if (upper.includes('[]') || upper.startsWith('LIST')) return 'list';
  return upper.slice(0, 4).toLowerCase();
}
