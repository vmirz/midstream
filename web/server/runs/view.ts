/**
 * The views over snapshots' data files that statements, profiles and exports read.
 *
 * A statement reads each of the materialization's tables by its canonical name, its dataset and the
 * table — `SELECT * FROM sales.orders` — as flat logical columns, and its native Iceberg table as
 * `iceberg.sales.orders`: structs and lists as they are, which is what an external engine attached to
 * the catalog sees. A materialization's tables may be in several datasets, and a statement can read
 * several tables at once, joining them like any others; only the tables it names are planned and
 * read. Profiles and exports read a run's rows under `ROWS_VIEW`, a name of the console's own.
 * Internal names start with an underscore, which neither a dataset nor a table name ever does, so
 * nothing can be named onto one of them.
 */

import { badRequest } from '../errors.ts';
import { duckType, readParquet, selectList, timePredicate, type LogicalColumn } from '../data/logical.ts';
import { quoteIdentifier, stripStringsAndComments } from '../data/sql.ts';
import type { IcebergSchema } from '../iceberg/metadata.ts';
import type { TimeRange } from '../../shared/types.ts';

/** A run's rows, as profiles and exports read them. */
export const ROWS_VIEW = '__run_rows';

/** The native table, before its columns are named and folded into logical ones. */
const NATIVE_VIEW = '__table';

/** A table's logical view, before it is given its canonical name. */
const LOGICAL_VIEW = '__view';

/** The database statements read the native tables from: `FROM iceberg.sales.orders`. */
export const NATIVE_CATALOG = 'iceberg';

/** How SQL names one of a materialization's tables, and an engine on the catalog does too: `sales.orders`. */
export function qualifiedName(namespace: string, table: string): string {
  return `${namespace}.${table}`;
}

/** One table a statement reads: its name, its snapshot's data files, and how they are read. */
export interface StatementTable {
  /** The table's dataset: its namespace in SQL. */
  namespace: string;
  name: string;
  /** The data files of the run's snapshot inside the time range, as DuckDB opens them. */
  sources: string[];
  schema: IcebergSchema | null;
  columns: LogicalColumn[];
}

/**
 * The statements that expose `name` over `sources`, restricted to `range`.
 *
 * The range is applied to the native scan, ahead of the projection, so DuckDB pushes it into the
 * Parquet reader and skips row groups outside it on their statistics. A range no file overlaps
 * still yields views with every column, so a statement binds and simply returns nothing.
 */
export function viewSetup(
  sources: string[],
  schema: IcebergSchema | null,
  columns: LogicalColumn[],
  range: TimeRange,
  { name, filtersSql = '', nativeName = NATIVE_VIEW }: { name: string; filtersSql?: string; nativeName?: string },
): string[] {
  const view = quoteIdentifier(name);
  const native = quoteIdentifier(nativeName);
  const statements: string[] = [];
  if (sources.length === 0 || schema === null) {
    const nulls = (fields: Array<{ name: string; type: string }>): string =>
      fields.map((field) => `NULL::${field.type} AS ${quoteIdentifier(field.name)}`).join(', ') || 'NULL AS "__empty"';
    statements.push(
      `CREATE OR REPLACE TEMP VIEW ${native} AS SELECT ${nulls((schema?.fields ?? []).map((field) => ({ name: field.name, type: duckType(field.type) })))} WHERE FALSE`,
    );
    statements.push(`CREATE OR REPLACE TEMP VIEW ${view} AS SELECT ${nulls(columns)} WHERE FALSE`);
  } else {
    const predicate = timePredicate(range);
    statements.push(
      `CREATE OR REPLACE TEMP VIEW ${native} AS SELECT * FROM ${readParquet(sources, schema)}${predicate ? ` WHERE ${predicate}` : ''}`,
    );
    statements.push(`CREATE OR REPLACE TEMP VIEW ${view} AS SELECT * FROM (SELECT ${selectList(columns)} FROM ${native}) AS logical${filtersSql}`);
  }
  return statements;
}

/**
 * Every view a statement reads, inside its sandbox: each table as `<dataset>.<table>`, and its
 * native table as `iceberg.<dataset>.<table>`.
 *
 * A sandbox is a DuckDB instance of the statement's own (`data/duckdb.ts::withSandbox`), so the
 * schemas are real — `sales.orders` resolves as DuckDB resolves any qualified name — and gone with
 * the statement. `iceberg` is an in-memory database attached for the native views: the sandbox allows it,
 * as it refuses any file. The views are defined over temporary ones, which only `temp` can hold.
 */
export function statementViews(tables: StatementTable[], range: TimeRange): string[] {
  const statements = [`ATTACH ':memory:' AS ${NATIVE_CATALOG}`];
  for (const namespace of new Set(tables.map((table) => table.namespace))) {
    statements.push(`CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(namespace)}`);
    statements.push(`CREATE SCHEMA IF NOT EXISTS ${NATIVE_CATALOG}.${quoteIdentifier(namespace)}`);
  }
  tables.forEach((table, index) => {
    const logicalName = `${LOGICAL_VIEW}_${index}`;
    const nativeName = `${NATIVE_VIEW}_${index}`;
    const canonical = `${quoteIdentifier(table.namespace)}.${quoteIdentifier(table.name)}`;
    statements.push(...viewSetup(table.sources, table.schema, table.columns, range, { name: logicalName, nativeName }));
    statements.push(`CREATE OR REPLACE VIEW ${canonical} AS SELECT * FROM temp.main.${quoteIdentifier(logicalName)}`);
    statements.push(`CREATE OR REPLACE VIEW ${NATIVE_CATALOG}.${canonical} AS SELECT * FROM temp.main.${quoteIdentifier(nativeName)}`);
  });
  return statements;
}

/** An identifier: quoted, with `""` for a quote inside, or bare. */
const IDENTIFIER = /"(?:[^"]|"")+"|[A-Za-z_][\w$]*/g;

function unquote(token: string): string {
  return token.startsWith('"') ? token.slice(1, -1).replace(/""/g, '"') : token;
}

/**
 * The materialization's tables a statement names, in the materialization's order.
 *
 * Any identifier spelled as a table's name counts — `sales.orders`, `"Orders"`,
 * `iceberg.sales.orders` — since SQL compares names without regard to case; a table's name is unique
 * in its materialization, whatever its dataset. A column that happens to share a table's name only
 * costs that table's planning; a table the statement reads is never missed.
 */
export function referencedTables(statement: string, tableNames: readonly string[]): string[] {
  const byName = new Map(tableNames.map((name) => [name.toLowerCase(), name]));
  const found = new Set<string>();
  for (const match of stripStringsAndComments(statement, { keepLength: true }).matchAll(IDENTIFIER)) {
    const table = byName.get(unquote(match[0]).toLowerCase());
    if (table !== undefined) found.add(table);
  }
  return tableNames.filter((name) => found.has(name));
}

/**
 * Refuse `iceberg.…` for anything but one of the materialization's native tables,
 * `iceberg.<dataset>.<table>`, saying which there are; DuckDB's own message would suggest a system
 * table, or none. The tables may be in several datasets.
 */
export function assertNativeReferences(statement: string, tables: ReadonlyArray<{ dataset: string; name: string }>): void {
  const stripped = stripStringsAndComments(statement, { keepLength: true });
  const part = '("(?:[^"]|"")+"|[A-Za-z_][\\w$]*)';
  const pattern = new RegExp(`(^|[^\\w."])${NATIVE_CATALOG}\\.${part}(?:\\.${part})?`, 'gi');
  const known = new Set(tables.map((table) => qualifiedName(table.dataset, table.name).toLowerCase()));
  for (const match of stripped.matchAll(pattern)) {
    const schema = unquote(match[2] as string);
    const table = match[3] === undefined ? null : unquote(match[3]);
    if (table !== null && known.has(qualifiedName(schema, table).toLowerCase())) continue;
    const named = table === null ? `${NATIVE_CATALOG}.${schema}` : `${NATIVE_CATALOG}.${schema}.${table}`;
    const natives = tables.map((entry) => `${NATIVE_CATALOG}.${qualifiedName(entry.dataset, entry.name)}`);
    throw badRequest(`there is no table ${named}; this materialization's native tables are ${listOf(natives)}`, {
      field: 'sql',
    });
  }
}

/** `a`, `a and b`, `a, b and c`. */
export function listOf(names: readonly string[]): string {
  if (names.length <= 1) return names.join('');
  return `${names.slice(0, -1).join(', ')} and ${names.at(-1) as string}`;
}
