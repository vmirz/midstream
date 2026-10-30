/**
 * The columns a materialization presents, and the SQL that produces them from its Iceberg table.
 *
 * The table is native Iceberg: `customer` is a struct, `items` a list of structs, and a field whose
 * values changed type has forks — `total` and `total__v2` — while one that forked too often holds
 * its later values as JSON text in `__json`. People should see none of that: they see flat columns
 * named by path — `customer.email`, `items[].sku` — and `total` once, carrying its whole history.
 * Folding every fork with `COALESCE`, newest first, is what keeps a forked field from reading NULL
 * for every row written before the fork.
 *
 * Columns come from the table's current schema and fork map (`iceberg/schema.ts`), which is the
 * same walk the control plane's schema projection makes, so a column is named exactly as the
 * Schema tab names the field. Every data file is read through a schema map keyed by field id
 * (`readSchemaMap`), so a file written before a field existed reads it as NULL rather than failing
 * to bind, and every projection here has the same shape whichever files it reads.
 */

import { badRequest } from '../errors.ts';
import { quoteIdentifier, quoteLiteral } from './sql.ts';
import { METADATA_COLUMNS, PAYLOAD_ROOT, isMetadataColumn } from '../../shared/fieldPaths.ts';
import { buildIndex, isMetadataFieldId, logicalName, type ElementIndex, type FieldProtection, type ForkMap, type ProtectionMap, type StructIndex } from '../iceberg/schema.ts';
import type { IcebergSchema, IcebergType } from '../iceberg/metadata.ts';
import type { BrowseFilter, QueryColumn, TimeRange } from '../../shared/types.ts';

export type ColumnGroup = 'payload' | 'kafka';

/** One Iceberg field carrying (part of) a logical column. */
export interface ColumnFork {
  fieldId: number;
  /** SQL reading the field from a row of the native table. */
  expression: string;
  /** The DuckDB type the expression has. */
  type: string;
  json: boolean;
}

export interface LogicalColumn {
  /** What people see and type: a field path (`customer.email`, `items[].sku`) or a metadata column. */
  name: string;
  /** The type the column presents as. */
  type: string;
  /** Fields that carry it, newest first. */
  forks: ColumnFork[];
  /** SQL producing the column from a row of the native table. */
  expression: string;
  group: ColumnGroup;
  /**
   * The column's values are JSON text: a field collapsed to JSON, or `_unmapped`.
   *
   * Grids and flat exports show that text as it is; a nested export decodes it back into the
   * structure it holds.
   */
  jsonText: boolean;
  /** Whether the schema still writes it, as opposed to a field only superseded forks hold. */
  current: boolean;
  /** How its values are masked (handbook 22), when a field carrying it is protected. */
  protection?: FieldProtection;
}

export const TIMESTAMP_COLUMN = '_kafka_timestamp';
const TIMESTAMP_TYPE = 'TIMESTAMP WITH TIME ZONE';
const JSON_METADATA = new Set(['_unmapped']);

/** The DuckDB type of an Iceberg type. */
export function duckType(type: IcebergType): string {
  if (typeof type === 'string') {
    const decimal = /^decimal\((\d+),\s*(\d+)\)$/.exec(type);
    if (decimal) return `DECIMAL(${decimal[1]}, ${decimal[2]})`;
    switch (type) {
      case 'boolean':
        return 'BOOLEAN';
      case 'int':
        return 'INTEGER';
      case 'long':
        return 'BIGINT';
      case 'float':
        return 'FLOAT';
      case 'double':
        return 'DOUBLE';
      case 'date':
        return 'DATE';
      case 'time':
        return 'TIME';
      case 'timestamp':
        return 'TIMESTAMP';
      case 'timestamptz':
      case 'timestamptz_ns':
        return TIMESTAMP_TYPE;
      case 'timestamp_ns':
        return 'TIMESTAMP_NS';
      case 'uuid':
        return 'UUID';
      case 'binary':
        return 'BLOB';
      default:
        return type.startsWith('fixed') ? 'BLOB' : 'VARCHAR';
    }
  }
  if (type.type === 'struct') {
    return `STRUCT(${type.fields.map((field) => `${quoteIdentifier(field.name)} ${duckType(field.type)}`).join(', ')})`;
  }
  if (type.type === 'list') return `${duckType(type.element)}[]`;
  return `MAP(${duckType(type.key)}, ${duckType(type.value)})`;
}

/**
 * The `schema` argument of `read_parquet` that reads data files as `schema` describes them.
 *
 * Keyed by field id, which Iceberg data files carry on every column: a file written before a field
 * existed reads it as NULL, and nested struct fields a file lacks read as NULL too.
 */
export function readSchemaMap(schema: IcebergSchema): string {
  const entries = schema.fields.map(
    (field) => `${field.id}: {name: ${quoteLiteral(field.name)}, type: ${quoteLiteral(duckType(field.type))}, default_value: NULL}`,
  );
  return `map {${entries.join(', ')}}`;
}

/** `read_parquet` over data files, presenting `schema`'s top-level columns by field id. */
export function readParquet(sources: readonly string[], schema: IcebergSchema, options: { rowNumbers?: boolean } = {}): string {
  const list = sources.length === 1 ? quoteLiteral(sources[0] as string) : `[${sources.map(quoteLiteral).join(', ')}]`;
  return (
    `read_parquet(${list}, schema = ${readSchemaMap(schema)}, hive_partitioning = false` +
    `${options.rowNumbers ? ', file_row_number = true' : ''})`
  );
}

interface Leaf {
  name: string;
  fork: ColumnFork;
  current: boolean;
}

interface WalkContext {
  /** SQL reading child `name` of the struct value in hand. */
  child: (name: string) => string;
  /** Wraps an expression over the value in hand into one over a whole row (the enclosing lists). */
  wrap: (inner: string) => string;
  /** Lists the value in hand sits inside. */
  depth: number;
  current: boolean;
}

const ROOT: WalkContext = {
  child: (name) => quoteIdentifier(name),
  wrap: (inner) => inner,
  depth: 0,
  current: true,
};

function listSuffix(depth: number): string {
  return '[]'.repeat(depth);
}

function walkStruct(struct: StructIndex, context: WalkContext, leaves: Leaf[]): void {
  for (const family of struct.families.values()) {
    for (const member of family.members) {
      const current = context.current && member.state !== 'superseded';
      const relative = context.child(member.name);
      const name = logicalName(member.logicalPath);
      if (member.json) {
        leaves.push({ name, current, fork: { fieldId: member.fieldId, expression: context.wrap(relative), type: `VARCHAR${listSuffix(context.depth)}`, json: true } });
      } else if (member.struct) {
        walkStruct(member.struct, { ...context, child: (key) => `(${relative}).${quoteIdentifier(key)}`, current }, leaves);
      } else if (member.element) {
        walkList(member.element, relative, name, member.fieldId, member.type, { ...context, current }, leaves);
      } else {
        leaves.push({
          name,
          current,
          fork: { fieldId: member.fieldId, expression: context.wrap(relative), type: `${duckType(member.type)}${listSuffix(context.depth)}`, json: false },
        });
      }
    }
  }
}

/** Whether a list's elements hold a struct anywhere below, so its fields become columns of their own. */
function holdsStruct(element: ElementIndex): boolean {
  if (element.json) return false;
  if (element.struct) return true;
  return element.element ? holdsStruct(element.element) : false;
}

function innermostJson(element: ElementIndex): boolean {
  return element.json || (element.element ? innermostJson(element.element) : false);
}

function walkList(
  element: ElementIndex,
  list: string,
  name: string,
  fieldId: number,
  listType: IcebergType,
  context: WalkContext,
  leaves: Leaf[],
): void {
  if (!holdsStruct(element)) {
    // A list of values (or of lists of values) is one column, typed as the list it is.
    const json = innermostJson(element);
    const type = json ? `VARCHAR${listSuffix(listDepth(listType))}` : duckType(listType);
    leaves.push({ name, current: context.current, fork: { fieldId, expression: context.wrap(list), type: `${type}${listSuffix(context.depth)}`, json } });
    return;
  }
  const variable = `__e${context.depth + 1}`;
  const inner: WalkContext = {
    child: (key) => `(${variable}).${quoteIdentifier(key)}`,
    wrap: (expression) => context.wrap(`list_transform(${list}, lambda ${variable}: ${expression})`),
    depth: context.depth + 1,
    current: context.current,
  };
  if (element.struct) walkStruct(element.struct, inner, leaves);
  else if (element.element) {
    walkList(element.element, variable, logicalName([...element.logicalPath]), fieldId, element.type, inner, leaves);
  }
}

function listDepth(type: IcebergType): number {
  return typeof type !== 'string' && type.type === 'list' ? 1 + listDepth(type.element) : 0;
}

function typeDepth(type: string): number {
  return (type.match(/(\[\])+$/)?.[0].length ?? 0) / 2;
}

/** The logical columns of a schema: payload fields in schema order, then the Kafka metadata. */
export function logicalColumns(schema: IcebergSchema | null, forkMap: ForkMap = new Map(), protection: ProtectionMap = new Map()): LogicalColumn[] {
  if (!schema) return metadataColumns(null);
  const leaves: Leaf[] = [];
  walkStruct(buildIndex(schema, forkMap), ROOT, leaves);

  // One column per name. A field whose forks differ in shape — a value, later a list of values —
  // keeps one column per shape, the list one named with its `[]`.
  const groups = new Map<string, Leaf[]>();
  for (const leaf of leaves) {
    const group = groups.get(leaf.name) ?? [];
    group.push(leaf);
    groups.set(leaf.name, group);
  }
  const shaped = new Map<string, Leaf[]>();
  for (const [name, group] of groups) {
    const shallowest = Math.min(...group.map((leaf) => typeDepth(leaf.fork.type)));
    for (const leaf of group) {
      const key = `${name}${listSuffix(typeDepth(leaf.fork.type) - shallowest)}`;
      shaped.set(key, [...(shaped.get(key) ?? []), leaf]);
    }
  }

  const payload: LogicalColumn[] = [];
  for (const [name, group] of shaped) {
    const forks = group.map((leaf) => leaf.fork).sort((a, b) => b.fieldId - a.fieldId);
    const column = fold(isMetadataColumn(name) ? `${PAYLOAD_ROOT}${name}` : name, forks, group.some((leaf) => leaf.current));
    // The newest protected fork says how the column is masked: a type fork of a protected field is.
    const masked = forks.map((fork) => protection.get(fork.fieldId)).find((entry) => entry !== undefined);
    payload.push(masked ? { ...column, protection: masked } : column);
  }
  // A field only superseded forks hold still has history worth showing; it sorts after the fields
  // the schema currently writes.
  payload.sort((a, b) => Number(b.current) - Number(a.current));
  return [...payload, ...metadataColumns(schema)];
}

function fold(name: string, forks: ColumnFork[], current: boolean): LogicalColumn {
  const json = forks.some((fork) => fork.json);
  const castTo = commonType(forks, json);
  const read = (fork: ColumnFork): string => {
    // Older values of a field now held as JSON are written as JSON too, so the column decodes alike.
    if (json && !fork.json && forks.length > 1 && typeDepth(fork.type) === 0) return `CAST(to_json(${fork.expression}) AS VARCHAR)`;
    if (castTo === null || fork.type === castTo) return fork.expression;
    return `TRY_CAST(${fork.expression} AS ${castTo})`;
  };
  const expression = forks.length === 1 ? read(forks[0] as ColumnFork) : `COALESCE(${forks.map(read).join(', ')})`;
  return {
    name,
    type: castTo ?? forks[0]?.type ?? 'VARCHAR',
    forks,
    expression,
    group: 'payload',
    jsonText: json,
    current,
  };
}

const NUMERIC = /^(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|FLOAT|DOUBLE|DECIMAL\(\d+, \d+\))$/;

/**
 * One type every fork of a field can be read as, or null when they already agree.
 *
 * Forks exist because types conflicted. Numbers widen to DOUBLE so the column stays comparable and
 * sortable; anything else becomes text, keeping list nesting so a list of mixed encodings still
 * reads as a list.
 */
function commonType(forks: ColumnFork[], json: boolean): string | null {
  const distinct = new Set(forks.map((fork) => fork.type));
  if (distinct.size <= 1) return null;
  const suffix = listSuffix(typeDepth(forks[0]?.type ?? ''));
  const bases = forks.map((fork) => fork.type.slice(0, fork.type.length - suffix.length));
  if (!json && bases.every((base) => NUMERIC.test(base))) return `DOUBLE${suffix}`;
  return `VARCHAR${suffix}`;
}

function metadataColumns(schema: IcebergSchema | null): LogicalColumn[] {
  const types = new Map<string, string>();
  for (const field of schema?.fields ?? []) if (isMetadataFieldId(field.id)) types.set(field.name, duckType(field.type));
  return METADATA_COLUMNS.filter((name) => schema === null || types.has(name)).map((name): LogicalColumn => {
    const type = types.get(name) ?? DEFAULT_METADATA_TYPES[name];
    const expression = quoteIdentifier(name);
    return {
      name,
      type,
      forks: [{ fieldId: 0, expression, type, json: JSON_METADATA.has(name) }],
      expression,
      group: 'kafka',
      jsonText: JSON_METADATA.has(name),
      current: true,
    };
  });
}

const DEFAULT_METADATA_TYPES: Record<(typeof METADATA_COLUMNS)[number], string> = {
  _kafka_timestamp: TIMESTAMP_TYPE,
  _kafka_partition: 'INTEGER',
  _kafka_offset: 'BIGINT',
  _kafka_key: 'VARCHAR',
  _kafka_headers: 'MAP(VARCHAR, VARCHAR)',
  _materialized_at: TIMESTAMP_TYPE,
  _unmapped: 'VARCHAR',
};

export function toQueryColumns(columns: LogicalColumn[]): QueryColumn[] {
  return columns.map((column) => ({
    name: column.name,
    type: column.type,
    ...(column.jsonText ? { json: true } : {}),
    ...(column.protection ? { protection: column.protection } : {}),
  }));
}

/**
 * The column a filter or a link names.
 *
 * Names are matched exactly first. Links made before the `payload.` root was dropped still name
 * `payload.status`, which is the column now called `status`.
 */
export function findColumn(columns: LogicalColumn[], name: string): LogicalColumn | undefined {
  return (
    columns.find((column) => column.name === name) ??
    (name.startsWith(PAYLOAD_ROOT) ? columns.find((column) => column.name === name.slice(PAYLOAD_ROOT.length)) : undefined)
  );
}

/**
 * The SELECT list presenting `columns` from a row of the native table.
 *
 * `only` restricts it to the columns a count needs; the result always has the same shape whichever
 * files were read, because every file is read through the schema map.
 */
export function selectList(columns: LogicalColumn[], only?: ReadonlySet<string>): string {
  const parts: string[] = [];
  for (const column of columns) {
    if (only && !only.has(column.name)) continue;
    parts.push(`${column.expression} AS ${quoteIdentifier(column.name)}`);
  }
  return parts.length > 0 ? parts.join(', ') : 'NULL AS "__empty"';
}

/**
 * The predicate restricting rows to `range`, or null when the range is unbounded.
 *
 * It is written against the native timestamp column, ahead of any projection, so DuckDB pushes it
 * into the Parquet reader and skips whole row groups on their min/max statistics.
 */
export function timePredicate(range: TimeRange): string | null {
  if (range.from === null && range.to === null) return null;
  const column = quoteIdentifier(TIMESTAMP_COLUMN);
  const bounds: string[] = [];
  if (range.from !== null) bounds.push(`${column} >= ${timestampLiteral(range.from)}`);
  if (range.to !== null) bounds.push(`${column} < ${timestampLiteral(range.to)}`);
  return bounds.join(' AND ');
}

function timestampLiteral(epochMs: number): string {
  return `TIMESTAMPTZ ${quoteLiteral(new Date(epochMs).toISOString())}`;
}

/** Columns a set of filters reads, so a count reads nothing else. */
export function filterColumns(filters: BrowseFilter[]): Set<string> {
  return new Set(filters.map((filter) => filter.column));
}

/**
 * Filters as the run will apply them: each naming its column the way the view does.
 *
 * Rejects a filter on a column the view does not have before it reaches SQL, and renames one that
 * used a column's older, `payload.`-rooted name, so everything downstream sees one spelling.
 */
export function resolveFilters(filters: BrowseFilter[], columns: LogicalColumn[]): BrowseFilter[] {
  return filters.map((filter) => {
    const column = findColumn(columns, filter.column);
    if (!column) throw badRequest(`there is no column named "${filter.column}"`, { field: 'column' });
    return column.name === filter.column ? filter : { ...filter, column: column.name };
  });
}

function likeLiteral(value: string, pattern: (escaped: string) => string): string {
  const escaped = value.replace(/[\\%_]/g, (match) => `\\${match}`);
  return `${quoteLiteral(pattern(escaped))} ESCAPE '\\'`;
}

const COMPARISON_SQL: Record<string, string> = { eq: '=', neq: '<>', gt: '>', gte: '>=', lt: '<', lte: '<=' };

/**
 * One filter as SQL over the logical column it names.
 *
 * On a list column — `items[].sku` — "is" means "any element is", which is what someone
 * filtering orders by SKU means, and the ordering comparisons have no meaning at all.
 */
export function renderFilter(filter: BrowseFilter, columns: LogicalColumn[]): string {
  const column = findColumn(columns, filter.column);
  if (!column) throw badRequest(`there is no column named "${filter.column}"`, { field: 'column' });
  const identifier = quoteIdentifier(column.name);
  const depth = typeDepth(column.type);

  switch (filter.operator) {
    case 'is_null':
      return `${identifier} IS NULL`;
    case 'is_not_null':
      return `${identifier} IS NOT NULL`;
    case 'contains':
      return `CAST(${identifier} AS VARCHAR) ILIKE ${likeLiteral(filter.value ?? '', (v) => `%${v}%`)}`;
    case 'starts_with':
      return `CAST(${identifier} AS VARCHAR) ILIKE ${likeLiteral(filter.value ?? '', (v) => `${v}%`)}`;
    case 'eq':
    case 'neq':
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte': {
      if (filter.value === undefined || filter.value === '') {
        throw badRequest(`filter on ${filter.column} requires a value`, { field: 'value' });
      }
      if (depth > 0) {
        if (filter.operator !== 'eq' && filter.operator !== 'neq') {
          throw badRequest(`${filter.column} is a list; it can only be matched with "is" or "contains"`, {
            field: 'operator',
          });
        }
        const element = column.type.replace(/(\[\])+$/, '');
        const flattened = depth > 1 ? `flatten(${identifier})` : identifier;
        const value = element === 'VARCHAR' ? quoteLiteral(filter.value) : `TRY_CAST(${quoteLiteral(filter.value)} AS ${element})`;
        const test = `list_contains(${flattened}, ${value})`;
        return filter.operator === 'eq' ? `coalesce(${test}, false)` : `NOT coalesce(${test}, false)`;
      }
      return `${identifier} ${COMPARISON_SQL[filter.operator]} ${quoteLiteral(filter.value)}`;
    }
    default:
      throw badRequest(`unsupported filter operator "${String(filter.operator)}"`, { field: 'operator' });
  }
}

export function whereClause(filters: BrowseFilter[], columns: LogicalColumn[]): string {
  if (filters.length === 0) return '';
  return ` WHERE ${filters.map((filter) => renderFilter(filter, columns)).join(' AND ')}`;
}
