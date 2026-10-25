// Type-only, so `node --test` can load this module without the bundler's path aliases.
import type { QueryColumn } from '@shared/types';

import { matchName, searchTerms } from '../../lib/nameSearch.ts';

export interface FieldMatch {
  column: QueryColumn;
  /** Ranges of `column.name` that matched, for emphasis. */
  ranges: Array<[number, number]>;
}

/**
 * Columns matching a query, best first: every term must appear in the name, and an exact name,
 * then a name that starts with a term, then a term starting a path segment (`customer.email`,
 * `email_hash`) rank ahead of a plain substring; shorter names break ties. A blank query returns
 * every column in the order given.
 */
export function searchFields(columns: QueryColumn[], query: string): FieldMatch[] {
  const terms = searchTerms(query);
  if (terms.length === 0) return columns.map((column) => ({ column, ranges: [] }));

  const scored: Array<FieldMatch & { score: number; order: number }> = [];
  columns.forEach((column, order) => {
    const match = matchName(column.name.toLowerCase(), terms);
    if (match) scored.push({ column, ranges: match.ranges, score: match.score, order });
  });
  scored.sort(
    (left, right) =>
      left.score - right.score || left.column.name.length - right.column.name.length || left.order - right.order,
  );
  return scored.map(({ column, ranges }) => ({ column, ranges }));
}
