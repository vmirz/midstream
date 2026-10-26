import { useMemo } from 'react';

import { useFleetStream, type LiveStatus } from './live';
import type { ConsoleFleet, DomainFleet, FleetTotals, MaterializationSummary, ThroughputSample } from '@shared/types';

/** A materialization of the fleet, with the domain it is in. */
export interface DomainMaterialization extends MaterializationSummary {
  domain: string;
  domainName: string;
}

/**
 * The fleet a page shows: every domain's, merged, or one domain's (handbook 20). Totals and the
 * throughput series are summed over the domains shown; each item says its domain.
 */
export interface Fleet {
  generatedAt: string;
  /** Whether every domain shown is read now. */
  connected: boolean;
  /** Why a domain cannot be read, in words, naming it. */
  controlPlaneError: string | null;
  totals: FleetTotals;
  items: DomainMaterialization[];
  series: ThroughputSample[];
  /** Each domain shown, as the stream sent it. */
  domains: DomainFleet[];
  /** Domains whose materializations this person may not see, or cannot be told about now. */
  hidden: DomainFleet[];
}

/** Every domain's fleet merged, or `domain`'s alone, live. */
export function useFleet(domain: string | null = null): { data: Fleet | null; status: LiveStatus } {
  const { data, status } = useFleetStream();
  const merged = useMemo(() => (data ? mergeFleet(data, domain) : null), [data, domain]);
  return { data: merged, status };
}

const EMPTY_TOTALS: FleetTotals = {
  materializations: 0,
  running: 0,
  stopped: 0,
  failing: 0,
  pending: 0,
  deleted: 0,
  rowsCommitted: 0,
  tableBytes: 0,
  tableFiles: 0,
  rowsWritten: 0,
  bytesWritten: 0,
  filesWritten: 0,
  recordsSkipped: 0,
  rowsPerSecond: 0,
  bytesPerSecond: 0,
};

/** Points of different domains' series within this long are one point of the merged series. */
const SERIES_BUCKET_MS = 5_000;

/** The fleet of the domains a page shows. Pure: what `useFleet` computes, for tests. */
export function mergeFleet(fleet: ConsoleFleet, domain: string | null): Fleet {
  const chosen = fleet.domains.filter((entry) => domain === null || entry.domain === domain);
  const shown = chosen.filter((entry) => entry.access === 'ok' && entry.snapshot !== null);
  const hidden = chosen.filter((entry) => entry.access !== 'ok' || entry.snapshot === null);
  const items = shown.flatMap((entry) =>
    (entry.snapshot?.items ?? []).map((item) => ({ ...item, domain: entry.domain, domainName: entry.name })),
  );
  items.sort((left, right) => right.updated_at.localeCompare(left.updated_at));
  const totals = shown.reduce<FleetTotals>((sum, entry) => addTotals(sum, entry.snapshot!.totals), { ...EMPTY_TOTALS });
  const errors = shown
    .filter((entry) => entry.snapshot?.controlPlaneError)
    .map((entry) => (shown.length > 1 || domain === null ? `${entry.name}: ${entry.snapshot!.controlPlaneError}` : entry.snapshot!.controlPlaneError!));
  return {
    generatedAt: fleet.generatedAt,
    connected: shown.every((entry) => entry.snapshot!.connected),
    controlPlaneError: errors.length > 0 ? errors.join(' · ') : null,
    totals,
    items,
    series: shown.length === 1 ? shown[0]!.snapshot!.series : mergeSeries(shown.map((entry) => entry.snapshot!.series)),
    domains: shown,
    hidden,
  };
}

function addTotals(sum: FleetTotals, more: FleetTotals): FleetTotals {
  return {
    materializations: sum.materializations + more.materializations,
    running: sum.running + more.running,
    stopped: sum.stopped + more.stopped,
    failing: sum.failing + more.failing,
    pending: sum.pending + more.pending,
    deleted: sum.deleted + more.deleted,
    rowsCommitted: sum.rowsCommitted + more.rowsCommitted,
    tableBytes: sum.tableBytes + more.tableBytes,
    tableFiles: sum.tableFiles + more.tableFiles,
    rowsWritten: sum.rowsWritten + more.rowsWritten,
    bytesWritten: sum.bytesWritten + more.bytesWritten,
    filesWritten: sum.filesWritten + more.filesWritten,
    recordsSkipped: sum.recordsSkipped + more.recordsSkipped,
    // A rate summed over the domains is unknown while any domain's is.
    rowsPerSecond: sum.rowsPerSecond === null || more.rowsPerSecond === null ? null : sum.rowsPerSecond + more.rowsPerSecond,
    bytesPerSecond: sum.bytesPerSecond === null || more.bytesPerSecond === null ? null : sum.bytesPerSecond + more.bytesPerSecond,
  };
}

/**
 * Several domains' throughput series as one: points within the same few seconds are summed, each
 * domain's latest point carried forward into later buckets, so a domain sampled a second later does
 * not read as a dip.
 */
export function mergeSeries(series: ThroughputSample[][]): ThroughputSample[] {
  const buckets = [...new Set(series.flatMap((points) => points.map((point) => Math.floor(point.t / SERIES_BUCKET_MS))))].sort((a, b) => a - b);
  const cursors = series.map(() => 0);
  const latest: Array<ThroughputSample | null> = series.map(() => null);
  return buckets.map((bucket) => {
    series.forEach((points, index) => {
      while ((cursors[index] ?? 0) < points.length && Math.floor(points[cursors[index]!]!.t / SERIES_BUCKET_MS) <= bucket) {
        latest[index] = points[cursors[index]!]!;
        cursors[index] = (cursors[index] ?? 0) + 1;
      }
    });
    const present = latest.filter((point): point is ThroughputSample => point !== null);
    const sum = (key: 'rows' | 'bytes' | 'files' | 'skipped') => present.reduce((total, point) => total + point[key], 0);
    const rate = (key: 'rowsPerSecond' | 'bytesPerSecond' | 'lag') =>
      present.some((point) => point[key] === null) ? null : present.reduce((total, point) => total + (point[key] as number), 0);
    return {
      t: bucket * SERIES_BUCKET_MS,
      rows: sum('rows'),
      bytes: sum('bytes'),
      files: sum('files'),
      skipped: sum('skipped'),
      rowsPerSecond: rate('rowsPerSecond'),
      bytesPerSecond: rate('bytesPerSecond'),
      lag: rate('lag'),
    };
  });
}
