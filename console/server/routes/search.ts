import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { authIn } from '../auth/guard.ts';
import { can } from '../auth/principal.ts';
import { domains } from '../domains/index.ts';
import { parseBody } from './materializations.ts';
import type { SearchResult, SearchResults } from '../../shared/types.ts';

const searchQuery = z.object({ q: z.string().trim().min(1).max(200), limit: z.coerce.number().int().min(1).max(200).default(50) });

/** How well `text` matches `query`: 3 the whole text, 2 its start, 1 inside it, 0 not at all. */
function score(query: string, ...texts: Array<string | null | undefined>): number {
  let best = 0;
  for (const text of texts) {
    if (!text) continue;
    const lower = text.toLowerCase();
    if (lower === query) return 3;
    if (lower.startsWith(query)) best = Math.max(best, 2);
    else if (lower.includes(query)) best = Math.max(best, 1);
  }
  return best;
}

/**
 * Search across every domain (handbook 20): materializations by label, id or topic, tables by
 * `<dataset>.<table>` or `<domain>.<dataset>.<table>`, and datasets by name or description - in each
 * domain where the person may see them. Answered from what the console holds already: each domain's
 * fleet, and its datasets as read with its service token, kept half a minute.
 */
export async function registerSearchRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/search', async (request): Promise<SearchResults> => {
    const { q, limit } = parseBody(searchQuery, request.query ?? {});
    const query = q.toLowerCase();
    const found: Array<SearchResult & { rank: number }> = [];
    for (const domain of domains().served()) {
      const principal = authIn(request, domain).state.principal;
      if (can(principal, 'materializations.view')) {
        for (const item of domain.live.fleetSnapshot().items) {
          if (item.desired_phase === 'deleted' && item.observed_phase === 'deleted') continue;
          const rank = score(query, item.name, item.materialization_id, ...item.source_topics);
          if (rank > 0) {
            found.push({
              domain: domain.id,
              kind: 'materialization',
              id: item.materialization_id,
              label: item.name,
              detail: item.source_topics.join(', '),
              materializationId: item.materialization_id,
              rank: rank + 0.2,
            });
          }
          item.tables.forEach((table, index) => {
            const rank = score(query, table, `${domain.id}.${table}`, table.split('.').at(-1));
            if (rank > 0) {
              found.push({
                domain: domain.id,
                kind: 'table',
                id: table,
                label: `${domain.id}.${table}`,
                detail: item.source_topics[index] ?? null,
                materializationId: item.materialization_id,
                rank: rank + 0.1,
              });
            }
          });
        }
      }
      if (can(principal, 'datasets.view')) {
        for (const dataset of await domain.readDatasets()) {
          const rank = score(query, dataset.name, `${domain.id}.${dataset.name}`, dataset.description);
          if (rank > 0) {
            found.push({
              domain: domain.id,
              kind: 'dataset',
              id: dataset.name,
              label: `${domain.id}.${dataset.name}`,
              detail: dataset.description || null,
              materializationId: null,
              rank,
            });
          }
        }
      }
    }
    found.sort((left, right) => right.rank - left.rank || left.label.localeCompare(right.label));
    return {
      query: q,
      results: found.slice(0, limit).map(({ rank: _rank, ...result }) => result),
      truncated: found.length > limit,
    };
  });
}
