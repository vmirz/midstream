/**
 * How a statement names tables (handbook 20): always with their domain, `<domain>.<dataset>.<table>`,
 * so a statement reads the same wherever it is pasted; the native Iceberg table is
 * `<domain>_iceberg.<dataset>.<table>`. In DuckDB the domain is a catalog, the dataset a schema.
 */

import { stripStringsAndComments } from '../data/sql.ts';
import { NATIVE_CATALOG_SUFFIX } from '../../shared/tableNames.ts';

/** A table a statement names. */
export interface TableReference {
  domain: string;
  dataset: string;
  table: string;
  /** Named as its native Iceberg table, `<domain>_iceberg.…`. */
  native: boolean;
  /** As the statement wrote it, for a message. */
  written: string;
}

/** A table's name in SQL: `<domain>.<dataset>.<table>`. */
export function canonicalName(domain: string, dataset: string, table: string): string {
  return `${domain}.${dataset}.${table}`;
}

/** The catalog a domain's native tables are in: `<domain>_iceberg`. */
export function nativeCatalog(domain: string): string {
  return `${domain}${NATIVE_CATALOG_SUFFIX}`;
}

/** An identifier: quoted, with `""` for a quote inside, or bare. */
const PART = String.raw`("(?:[^"]|"")+"|[A-Za-z_][\w$]*)`;
const DOT = String.raw`\s*\.\s*`;

function unquote(token: string): string {
  return token.startsWith('"') ? token.slice(1, -1).replace(/""/g, '"') : token;
}

/**
 * Every table a statement names in one of `domains`, each once, in the order they first appear:
 * three dotted names whose first is a domain id, or its native catalog. SQL compares names without
 * regard to case, so they are read lowercase; a fourth name - a column - after them is not part of
 * the table's.
 */
export function namedTables(statement: string, domains: readonly string[]): TableReference[] {
  const known = new Map<string, { domain: string; native: boolean }>();
  for (const domain of domains) {
    known.set(domain.toLowerCase(), { domain, native: false });
    known.set(nativeCatalog(domain).toLowerCase(), { domain, native: true });
  }
  const pattern = new RegExp(`(?<![\\w$."])${PART}${DOT}${PART}${DOT}${PART}`, 'g');
  const found = new Map<string, TableReference>();
  for (const match of stripStringsAndComments(statement, { keepLength: true }).matchAll(pattern)) {
    const catalog = known.get(unquote(match[1] as string).toLowerCase());
    if (!catalog) continue;
    const dataset = unquote(match[2] as string).toLowerCase();
    const table = unquote(match[3] as string).toLowerCase();
    const key = `${catalog.native ? 'n' : 'l'}:${canonicalName(catalog.domain, dataset, table)}`;
    if (!found.has(key)) found.set(key, { domain: catalog.domain, dataset, table, native: catalog.native, written: match[0] });
  }
  return [...found.values()];
}

/**
 * The tables of `tables` a statement names without their domain - `sales.orders`, or the native
 * `iceberg.sales.orders` of before - each once: a statement must name its domain.
 */
export function unqualifiedReferences(statement: string, tables: ReadonlyArray<{ dataset: string; table: string }>): string[] {
  const byName = new Set(tables.map((entry) => `${entry.dataset}.${entry.table}`.toLowerCase()));
  // Two dotted names not after a dot - nor before a third, which makes them a table's own name - or
  // three that start with the old native catalog.
  const two = new RegExp(`(?<![\\w$."])${PART}${DOT}${PART}(?!${DOT}[A-Za-z_"])`, 'g');
  const native = new RegExp(`(?<![\\w$."])iceberg${DOT}${PART}${DOT}${PART}`, 'gi');
  const stripped = stripStringsAndComments(statement, { keepLength: true });
  const found = new Set<string>();
  for (const match of stripped.matchAll(two)) {
    const name = `${unquote(match[1] as string)}.${unquote(match[2] as string)}`.toLowerCase();
    if (byName.has(name)) found.add(name);
  }
  for (const match of stripped.matchAll(native)) {
    const name = `${unquote(match[1] as string)}.${unquote(match[2] as string)}`.toLowerCase();
    if (byName.has(name)) found.add(`iceberg.${name}`);
  }
  return [...found];
}
