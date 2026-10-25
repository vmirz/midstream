import { ALL_TIME, decodeRange, encodeRange, type TimeRangeSpec } from './timeRange';
import { TABLE_PARAM } from '../tables/selectedTable';
import type { BrowseFilter, RunOrder } from '@shared/types';

/**
 * Everything that defines what the Data tab shows, as it lives in the address bar.
 *
 * The query string is the view: copying it shares it, and reloading restores it. `run` names the
 * run on screen, so a reload in the middle of a long query picks the same run back up — progress
 * and all — instead of starting it again.
 */
export interface ViewState {
  /** The table shown, by name; null for the materialization's first. */
  table: string | null;
  range: TimeRangeSpec;
  filters: BrowseFilter[];
  order: RunOrder;
  /** A hand-written statement, when the view is one. */
  sql: string | null;
  page: number;
  pageSize: number;
  visibleColumns: string[] | null;
  pinnedColumn: string | null;
  runId: string | null;
  /**
   * Live mode: the browse view follows the live tail's newest records. On unless turned off
   * (`live=0`); it only acts on a materialization whose live tail is on.
   */
  live: boolean;
}

export const PAGE_SIZES = [50, 100, 250, 500, 1000];
export const DEFAULT_PAGE_SIZE = 100;

export function readViewState(params: URLSearchParams): ViewState {
  const pageSize = Number.parseInt(params.get('rows') ?? '', 10);
  const page = Number.parseInt(params.get('page') ?? '', 10);
  const columns = params.get('cols');
  const run = params.get('run');
  const table = params.get(TABLE_PARAM);
  return {
    table: table && /^[A-Za-z][A-Za-z0-9_]{0,127}$/.test(table) ? table : null,
    range: decodeRange(params.get('t')),
    filters: readFilters(params.get('f')),
    order: params.get('order') === 'oldest' ? 'oldest' : 'newest',
    sql: params.get('sql'),
    // Pages are 1-based in the address bar, where a person might edit one.
    page: Number.isFinite(page) && page > 1 ? page - 1 : 0,
    pageSize: PAGE_SIZES.includes(pageSize) ? pageSize : DEFAULT_PAGE_SIZE,
    visibleColumns: columns ? columns.split(',').filter((name) => name.length > 0) : null,
    pinnedColumn: params.get('pin'),
    // A link from before snapshots left the Data tab may name a run pinned to an older one
    // (`asof`): the view opens on the latest data instead.
    runId: run && /^[0-9a-f-]{36}$/i.test(run) && !params.has('asof') ? run : null,
    live: params.get('live') !== '0',
  };
}

function readFilters(raw: string | null): BrowseFilter[] {
  if (!raw) return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (entry): entry is BrowseFilter =>
        typeof entry === 'object' &&
        entry !== null &&
        typeof (entry as BrowseFilter).column === 'string' &&
        typeof (entry as BrowseFilter).operator === 'string',
    );
  } catch {
    return [];
  }
}

/** Serialise a view, leaving out anything at its default so an untouched tab has a bare URL. */
export function writeViewState(state: ViewState): URLSearchParams {
  const params = new URLSearchParams();
  if (state.table) params.set(TABLE_PARAM, state.table);
  if (encodeRange(state.range) !== encodeRange(ALL_TIME)) params.set('t', encodeRange(state.range));
  if (state.filters.length > 0) params.set('f', JSON.stringify(state.filters));
  if (state.order !== 'newest') params.set('order', state.order);
  if (state.sql !== null) params.set('sql', state.sql);
  if (state.page > 0) params.set('page', String(state.page + 1));
  if (state.pageSize !== DEFAULT_PAGE_SIZE) params.set('rows', String(state.pageSize));
  if (state.visibleColumns !== null) params.set('cols', state.visibleColumns.join(','));
  if (state.pinnedColumn) params.set('pin', state.pinnedColumn);
  if (state.runId) params.set('run', state.runId);
  if (!state.live) params.set('live', '0');
  return params;
}
