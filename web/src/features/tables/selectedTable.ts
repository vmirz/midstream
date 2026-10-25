import { useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';

import { readableTables, type CpDetail, type CpTableSpec } from '@shared/types';

/** The query parameter naming the table the Data, Schema and Table tabs show. */
export const TABLE_PARAM = 'table';

/**
 * The materialization's table `name` refers to, compared as SQL compares names; its first table
 * when `name` names none of them - or names one `allowed` refuses, when the first allowed one is.
 * Null for a materialization without tables, or without one allowed.
 */
export function resolveTable(
  tables: readonly CpTableSpec[],
  name: string | null | undefined,
  allowed: (table: CpTableSpec) => boolean = () => true,
): CpTableSpec | null {
  const wanted = name?.trim().toLowerCase();
  const named = wanted ? tables.find((table) => table.name.toLowerCase() === wanted) : undefined;
  if (named && allowed(named)) return named;
  return tables.find(allowed) ?? null;
}

/**
 * The table a tab shows, from `?table=`, and a way to choose another.
 *
 * The parameter is shared by the Data, Schema and Table tabs, so moving between them keeps looking
 * at the same table. The materialization's first table needs no parameter. Kept tables are there
 * too, after the written ones: they stay readable. A tab that needs access to a table's dataset
 * passes `allowed`: a table it refuses is never the one shown.
 */
export function useSelectedTable(
  detail: CpDetail | null,
  allowed?: (table: CpTableSpec) => boolean,
): {
  table: CpTableSpec | null;
  tables: CpTableSpec[];
  select: (name: string) => void;
} {
  const [params, setParams] = useSearchParams();
  const tables = detail ? readableTables(detail.materialization) : [];
  const table = resolveTable(tables, params.get(TABLE_PARAM), allowed);
  const first = tables[0]?.name ?? null;
  const select = useCallback(
    (name: string) => {
      setParams(
        (current) => {
          const next = new URLSearchParams(current);
          if (name === first) next.delete(TABLE_PARAM);
          else next.set(TABLE_PARAM, name);
          return next;
        },
        { replace: true },
      );
    },
    [setParams, first],
  );
  return { table, tables, select };
}

/** `sales.orders`, or `sales.orders · orders.v2` when the table's name is not its topic's. */
export function tableLabel(table: CpTableSpec): string {
  const name = `${table.dataset}.${table.name}`;
  return table.name === table.source_topic ? name : `${name} · ${table.source_topic}`;
}
