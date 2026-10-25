/**
 * Column profiles, over the rows of one run.
 *
 * A profile describes what the user is looking at — the run's time range and filters, or a
 * statement's result — so its top values are usable as filter suggestions rather than trivia about
 * the dataset as a whole. It is work the console volunteers, so it runs under a short budget and
 * reports itself `partial` rather than holding the panel open for the length of a real query.
 */

import { badRequest } from '../errors.ts';
import { runBudgetedQuery } from '../data/duckdb.ts';
import { quoteIdentifier } from '../data/sql.ts';
import { ROWS_VIEW } from './view.ts';
import type { Run } from './run.ts';
import type { ColumnProfile, ColumnProfileValue } from '../../shared/types.ts';

const TOP_VALUES = 12;

export async function profileColumn(run: Run, column: string): Promise<ColumnProfile> {
  if (!run.columns?.some((candidate) => candidate.name === column)) {
    throw badRequest(`there is no column named "${column}" in this result`, { field: 'column' });
  }
  if (run.state === 'failed') {
    throw badRequest('this run failed, so it has no rows to profile', { state: run.state });
  }

  const setup = await run.rowsView();
  const quoted = quoteIdentifier(column);
  const [aggregate, top] = await Promise.all([
    runBudgetedQuery(
      `SELECT count(*)::BIGINT AS total, count(${quoted})::BIGINT AS non_null, ` +
        `approx_count_distinct(${quoted})::BIGINT AS distinct_count, ` +
        `CAST(min(${quoted}) AS VARCHAR) AS min_value, CAST(max(${quoted}) AS VARCHAR) AS max_value ` +
        `FROM ${ROWS_VIEW}`,
      { setup },
    ),
    runBudgetedQuery(
      `SELECT CAST(${quoted} AS VARCHAR) AS value, count(*)::BIGINT AS count ` +
        `FROM ${ROWS_VIEW} GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT ${TOP_VALUES}`,
      { setup },
    ),
  ]);

  const summary = aggregate?.rows[0] ?? {};
  const numeric = (value: unknown): number => (typeof value === 'number' && Number.isFinite(value) ? value : Number(value ?? 0) || 0);
  const text = (value: unknown): string | null => (typeof value === 'string' ? value : null);

  return {
    column,
    partial: aggregate === null || top === null,
    total: numeric(summary.total),
    nonNull: numeric(summary.non_null),
    distinct: numeric(summary.distinct_count),
    min: text(summary.min_value),
    max: text(summary.max_value),
    topValues: (top?.rows ?? []).map<ColumnProfileValue>((row) => ({
      value: row.value === null || row.value === undefined ? null : String(row.value),
      count: numeric(row.count),
    })),
  };
}
