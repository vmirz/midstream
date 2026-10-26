import type { SearchResult } from '@shared/types';

import { domainPath } from '../../lib/paths.ts';

/** Where a result opens: in its domain, a materialization's page, a table on its Table tab, a dataset's page. */
export function resultPath(result: SearchResult): string {
  switch (result.kind) {
    case 'materialization':
      return domainPath(result.domain, `/materializations/${encodeURIComponent(result.id)}`);
    case 'table': {
      const table = result.id.split('.').at(-1) ?? result.id;
      return domainPath(
        result.domain,
        `/materializations/${encodeURIComponent(result.materializationId ?? '')}/table?table=${encodeURIComponent(table)}`,
      );
    }
    case 'dataset':
      return domainPath(result.domain, `/datasets/${encodeURIComponent(result.id)}`);
  }
}
