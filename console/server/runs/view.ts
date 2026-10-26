/**
 * The views over snapshots' data files that statements, profiles and exports read.
 *
 * A statement reads tables by their full names, `<domain>.<dataset>.<table>` (handbook 20), as flat
 * logical columns, and their native Iceberg tables as `<domain>_iceberg.<dataset>.<table>`: structs
 * and lists as they are, which is what an external engine attached to the catalog sees. It may read
 * tables of several materializations and domains, joining them like any others; only the tables it
 * names are planned and read. Profiles and exports read a run's rows under `ROWS_VIEW`, a name of
 * the console's own. Internal names start with an underscore, which neither a dataset nor a table
 * name ever does, so nothing can be named onto one of them.
 */

import { duckType, readParquet, selectList, timePredicate, type LogicalColumn } from '../data/logical.ts';
import { quoteIdentifier } from '../data/sql.ts';
import { nativeCatalog } from './names.ts';
import type { IcebergSchema } from '../iceberg/metadata.ts';
import type { TimeRange } from '../../shared/types.ts';

/** A run's rows, as profiles and exports read them. */
export const ROWS_VIEW = '__run_rows';

/** The native table, before its columns are named and folded into logical ones. */
const NATIVE_VIEW = '__table';

/** A table's logical view, before it is given its canonical name. */
const LOGICAL_VIEW = '__view';

/** How a browse run names its table, as its materialization's page does: `sales.orders`. */
export function qualifiedName(namespace: string, table: string): string {
  return `${namespace}.${table}`;
}

/** One table a statement reads: its name, its snapshot's data files, and how they are read. */
export interface StatementTable {
  /** The table's domain: its catalog in SQL. */
  domain: string;
  /** The table's dataset: its schema in SQL. */
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
 * Every view a statement reads, inside its sandbox: each table as `<domain>.<dataset>.<table>`, and
 * its native table as `<domain>_iceberg.<dataset>.<table>`.
 *
 * A sandbox is a DuckDB instance of the statement's own (`data/duckdb.ts::withSandbox`), so the
 * catalogs and schemas are real - `domain_a.sales.orders` resolves as DuckDB resolves any qualified
 * name - and gone with the statement. Each domain, and its native catalog, is an in-memory database
 * attached for the statement: the sandbox allows that, as it refuses any file. The views are defined
 * over temporary ones, which only `temp` can hold.
 */
export function statementViews(tables: StatementTable[], range: TimeRange): string[] {
  const statements: string[] = [];
  for (const domain of new Set(tables.map((table) => table.domain))) {
    statements.push(`ATTACH ':memory:' AS ${quoteIdentifier(domain)}`);
    statements.push(`ATTACH ':memory:' AS ${quoteIdentifier(nativeCatalog(domain))}`);
  }
  const schemas = new Set<string>();
  for (const table of tables) {
    const key = `${table.domain}.${table.namespace}`;
    if (schemas.has(key)) continue;
    schemas.add(key);
    statements.push(`CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(table.domain)}.${quoteIdentifier(table.namespace)}`);
    statements.push(`CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(nativeCatalog(table.domain))}.${quoteIdentifier(table.namespace)}`);
  }
  tables.forEach((table, index) => {
    const logicalName = `${LOGICAL_VIEW}_${index}`;
    const nativeName = `${NATIVE_VIEW}_${index}`;
    const inSchema = `${quoteIdentifier(table.namespace)}.${quoteIdentifier(table.name)}`;
    statements.push(...viewSetup(table.sources, table.schema, table.columns, range, { name: logicalName, nativeName }));
    statements.push(`CREATE OR REPLACE VIEW ${quoteIdentifier(table.domain)}.${inSchema} AS SELECT * FROM temp.main.${quoteIdentifier(logicalName)}`);
    statements.push(`CREATE OR REPLACE VIEW ${quoteIdentifier(nativeCatalog(table.domain))}.${inSchema} AS SELECT * FROM temp.main.${quoteIdentifier(nativeName)}`);
  });
  return statements;
}

/** `a`, `a and b`, `a, b and c`. */
export function listOf(names: readonly string[]): string {
  if (names.length <= 1) return names.join('');
  return `${names.slice(0, -1).join(', ')} and ${names.at(-1) as string}`;
}
