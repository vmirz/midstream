/**
 * Run tests: a statement's tables found among the tables given, as `routes/data.ts` finds them among
 * the domains' fleets. Not used by the running console.
 */

import { canonicalName, namedTables } from './names.ts';
import type { ResolvedTable, RunTableSource } from './store.ts';
import type { TailAccess } from './tail.ts';

/** The tables of `tables` a statement names, each once, in the order it names them. */
export function sourcesFor(
  sql: string,
  tables: readonly RunTableSource[],
  tail: TailAccess | null = null,
): { sources: ResolvedTable[]; tableNames: string[] } {
  const byName = new Map(tables.map((table) => [canonicalName(table.domain, table.dataset, table.name).toLowerCase(), table]));
  const sources: ResolvedTable[] = [];
  for (const reference of namedTables(sql, [...new Set(tables.map((table) => table.domain))])) {
    const table = byName.get(canonicalName(reference.domain, reference.dataset, reference.table));
    if (table && !sources.some((source) => source === table || (source.domain === table.domain && source.dataset === table.dataset && source.name === table.name))) {
      sources.push({ ...table, tail });
    }
  }
  return { sources, tableNames: tables.map((table) => canonicalName(table.domain, table.dataset, table.name)) };
}
