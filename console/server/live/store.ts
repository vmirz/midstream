import { SERVICE, type ControlPlaneClient } from '../controlPlane/client.ts';
import { config } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { logger, type Logger } from '../logger.ts';
import { RateTracker, type CurrentRates } from './rates.ts';
import { can } from '../../shared/access.ts';
import { isDeletedPhase, readableTables } from '../../shared/types.ts';
import type {
  CpConsumerLag,
  CpDetail,
  CpListItem,
  CpPrincipal,
  CpRuntimeProgress,
  CpSchema,
  CpTableSpec,
  DetailSnapshot,
  FleetSnapshot,
  FleetTotals,
  LagSample,
  LifecyclePhase,
  MaterializationSummary,
  ThroughputSample,
} from '../../shared/types.ts';

type Listener<T> = (payload: T) => void;

const PENDING_PHASES = new Set<LifecyclePhase>(['requested', 'discovering', 'discovered', 'deploying']);
const FAILING_PHASES = new Set<LifecyclePhase>(['failed', 'degraded']);

/** Schemas of one materialization's tables read at once. */
const SCHEMA_CONCURRENCY = 4;

/** Points kept per consumer-lag history; older points thin out first once it is full. */
const LAG_HISTORY_POINTS = 720;
/** The most recent span of lag history, which is never thinned. */
const LAG_FULL_RESOLUTION_MS = 60 * 60 * 1000;

const NO_RATES: CurrentRates = { rowsPerSecond: null, bytesPerSecond: null, members: {}, warming: false };

/**
 * Polls the control plane and turns its point-in-time projections into a live, rate-aware
 * snapshot stream.
 *
 * The control plane only exposes cumulative counters, so throughput is derived here: each runtime
 * member's counters are differenced between that member's own heartbeats (`rates.ts`), which is
 * what keeps the rate steady however the polls and the heartbeats happen to line up. Polling backs
 * off to an idle cadence when no browser is connected so an open console tab is the only thing
 * that costs the control plane anything.
 */
export class LiveStore {
  /** The control plane of the domain this store follows; every domain has a store of its own. */
  private readonly controlPlane: ControlPlaneClient;
  /** Its log, naming the domain, so a failure says whose it is. */
  private readonly log: Logger;
  /** Told when the control plane stops answering the store's polls, and when it answers again. */
  private readonly onConnectionChange: ((connected: boolean) => void) | null;

  constructor(controlPlane: ControlPlaneClient, log: Logger = logger, onConnectionChange: ((connected: boolean) => void) | null = null) {
    this.controlPlane = controlPlane;
    this.log = log;
    this.onConnectionChange = onConnectionChange;
  }

  private readonly list = new Map<string, CpListItem>();
  private readonly details = new Map<string, CpDetail>();
  /** Each table's current schema, by materialization and then table name. */
  private readonly schemas = new Map<string, Map<string, CpSchema>>();
  private readonly schemaErrors = new Map<string, Map<string, string>>();
  private readonly series = new Map<string, ThroughputSample[]>();
  private readonly rateTrackers = new Map<string, RateTracker>();
  private readonly rates = new Map<string, CurrentRates>();
  private readonly lagHistory = new Map<string, LagSample[]>();
  private readonly lastDetailFetch = new Map<string, number>();

  private fleetSeries: ThroughputSample[] = [];
  private fleetListeners = new Set<Listener<FleetSnapshot>>();
  private detailListeners = new Map<string, Set<Listener<DetailSnapshot>>>();

  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private inFlight: Promise<void> | null = null;
  private lastRefreshAt = 0;
  private controlPlaneError: string | null = null;
  private generatedAt = new Date(0).toISOString();

  start(): void {
    if (this.running) return;
    this.running = true;
    void this.tick();
  }

  stop(): void {
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }

  get subscriberCount(): number {
    let total = this.fleetListeners.size;
    for (const listeners of this.detailListeners.values()) total += listeners.size;
    return total;
  }

  subscribeFleet(listener: Listener<FleetSnapshot>): () => void {
    this.fleetListeners.add(listener);
    listener(this.fleetSnapshot());
    this.wake();
    return () => {
      this.fleetListeners.delete(listener);
    };
  }

  subscribeDetail(materializationId: string, listener: Listener<DetailSnapshot>): () => void {
    const listeners = this.detailListeners.get(materializationId) ?? new Set();
    listeners.add(listener);
    this.detailListeners.set(materializationId, listeners);
    listener(this.detailSnapshot(materializationId));
    this.wake();
    return () => {
      const current = this.detailListeners.get(materializationId);
      if (!current) return;
      current.delete(listener);
      if (current.size === 0) this.detailListeners.delete(materializationId);
    };
  }

  /** Force an immediate refresh, e.g. right after a mutation is accepted. */
  wake(): void {
    if (!this.running || this.inFlight) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => void this.tick(), 50);
  }

  /**
   * Guarantee the fleet snapshot is no older than `maxAgeMs`.
   *
   * Polling backs off when nobody is streaming, so a plain REST reader would otherwise see
   * whatever the idle loop last observed.
   */
  async ensureFresh(maxAgeMs: number): Promise<void> {
    if (Date.now() - this.lastRefreshAt <= maxAgeMs) return;
    await this.runRefresh(true);
  }

  fleetSnapshot(): FleetSnapshot {
    const items = [...this.list.values()]
      .map((item) => this.summarize(item))
      .sort((a, b) => (b.updated_at ?? '').localeCompare(a.updated_at ?? ''));

    const warming = items.some((item) => !isDeletedPhase(item) && this.rates.get(item.materialization_id)?.warming === true);
    return {
      generatedAt: this.generatedAt,
      connected: this.controlPlaneError === null,
      controlPlaneError: this.controlPlaneError,
      totals: aggregate(items, warming),
      items,
      series: this.fleetSeries,
    };
  }

  detailSnapshot(materializationId: string): DetailSnapshot {
    const rates = this.rates.get(materializationId) ?? NO_RATES;
    return {
      generatedAt: this.generatedAt,
      materializationId,
      detail: this.details.get(materializationId) ?? null,
      schemas: Object.fromEntries(this.schemas.get(materializationId) ?? []),
      error: this.controlPlaneError,
      schemaErrors: Object.fromEntries(this.schemaErrors.get(materializationId) ?? []),
      series: this.series.get(materializationId) ?? [],
      rowsPerSecond: rates.rowsPerSecond,
      bytesPerSecond: rates.bytesPerSecond,
      memberRates: rates.members,
      lagHistory: this.lagHistory.get(materializationId) ?? [],
    };
  }

  /** Cached detail, refreshed on demand once it exceeds the detail poll interval. */
  async getDetail(materializationId: string): Promise<CpDetail> {
    const age = Date.now() - (this.lastDetailFetch.get(materializationId) ?? 0);
    const cached = this.details.get(materializationId);
    if (cached && age <= config.detailPollIntervalMs) return cached;

    const detail = await this.refreshDetail(materializationId, Date.now());
    if (detail === null) {
      throw new ConsoleError(
        'not_found',
        `materialization ${materializationId} was not found`,
        404,
        { resource: 'materialization', id: materializationId },
      );
    }
    return detail;
  }

  /**
   * The current schema of one of a materialization's tables, by its name in any case.
   *
   * Reading the detail first lets the drift check pick up a schema the table evolved to.
   */
  async getSchema(materializationId: string, table: string): Promise<CpSchema> {
    const detail = await this.getDetail(materializationId);
    const spec = tableSpec(detail, table);
    const cached = this.schemas.get(materializationId)?.get(spec.name);
    if (cached && !schemaDrifted(cached, detail)) return cached;
    return this.fetchSchema(materializationId, spec.name);
  }

  private async fetchSchema(id: string, table: string): Promise<CpSchema> {
    try {
      const schema = await this.controlPlane.schema(SERVICE, id, table);
      mapOf(this.schemas, id).set(table, schema);
      this.schemaErrors.get(id)?.delete(table);
      return schema;
    } catch (cause) {
      this.recordSchemaError(id, table, cause);
      throw cause;
    }
  }

  /** Schema reads fail independently of the detail poll, so the failure is kept per table. */
  private recordSchemaError(id: string, table: string, cause: unknown): void {
    const message = cause instanceof Error ? cause.message : String(cause);
    const errors = mapOf(this.schemaErrors, id);
    const previous = errors.get(table);
    errors.set(table, message);
    if (previous === message) {
      this.log.debug({ err: cause, materializationId: id, table }, 'schema refresh still failing');
      return;
    }
    this.log.warn({ err: cause, materializationId: id, table }, 'schema refresh failed');
  }

  private summarize(item: CpListItem): MaterializationSummary {
    const detail = this.details.get(item.materialization_id);
    const progress = detail?.status.runtime_progress;
    const observation = detail?.status.resource_observation;
    const rates = this.rates.get(item.materialization_id) ?? NO_RATES;

    return {
      ...item,
      runtimeHealth: detail?.status.runtime_health.level ?? 'unknown',
      lastHeartbeat: detail?.status.last_heartbeat ?? null,
      ...committedColumns(committedTotals(progress)),
      rowsWritten: progress?.cumulative_rows_written ?? progress?.rows_written ?? null,
      bytesWritten: progress?.cumulative_bytes_written ?? progress?.bytes_written ?? null,
      filesWritten: progress?.cumulative_files_written ?? progress?.files_written ?? null,
      recordsSkipped: progress?.cumulative_records_skipped ?? progress?.records_skipped ?? null,
      rowsPerSecond: rates.rowsPerSecond,
      bytesPerSecond: rates.bytesPerSecond,
      agentsRunning: observation?.managed_agents_running ?? null,
      agentsTotal: observation?.managed_agents_total ?? null,
      lag: detail?.status.consumer_lag?.total_lag ?? item.consumer_lag ?? null,
    };
  }

  private nextInterval(): number {
    if (this.subscriberCount === 0) return config.idlePollIntervalMs;
    if (this.detailListeners.size > 0) return config.detailPollIntervalMs;
    return config.listPollIntervalMs;
  }

  private async tick(): Promise<void> {
    if (!this.running) return;
    await this.runRefresh(this.subscriberCount > 0);
    if (this.running) {
      this.timer = setTimeout(() => void this.tick(), this.nextInterval());
    }
  }

  /** Serialize refreshes so concurrent callers share one pass over the control plane. */
  private async runRefresh(includeStaleDetails: boolean): Promise<void> {
    this.inFlight ??= (async () => {
      try {
        await this.refresh(includeStaleDetails);
      } catch (cause) {
        const wasConnected = this.controlPlaneError === null;
        this.controlPlaneError = cause instanceof Error ? cause.message : String(cause);
        this.log.warn({ err: cause }, 'live refresh failed');
        if (wasConnected) this.onConnectionChange?.(false);
        this.emitAll();
      } finally {
        this.inFlight = null;
      }
    })();
    await this.inFlight;
  }

  private async refresh(includeStaleDetails: boolean): Promise<void> {
    const now = Date.now();
    const listing = await this.controlPlane.list(SERVICE, 'all');
    if (this.controlPlaneError !== null) {
      this.controlPlaneError = null;
      this.onConnectionChange?.(true);
    }

    const seen = new Set<string>();
    for (const item of listing.items ?? []) {
      seen.add(item.materialization_id);
      this.list.set(item.materialization_id, item);
    }
    for (const id of [...this.list.keys()]) {
      if (!seen.has(id)) this.forget(id);
    }

    const targets = this.detailRefreshTargets(now, includeStaleDetails);
    await mapWithConcurrency(targets, config.detailConcurrency, async (id) => {
      // One unhealthy materialization must not stall the rest of the fleet refresh.
      await this.refreshDetail(id, now).catch(() => undefined);
    });

    this.recordFleetSample(now);
    this.lastRefreshAt = now;
    this.generatedAt = new Date(now).toISOString();
    this.emitAll();
  }

  /** Fetch one materialization's detail, sample it, and reconcile its schemas while it is watched. */
  private async refreshDetail(id: string, now: number): Promise<CpDetail | null> {
    try {
      const detail = await this.controlPlane.detail(SERVICE, id);
      this.details.set(id, detail);
      this.lastDetailFetch.set(id, now);
      this.recordSample(id, detail, now);
      // Only an open detail page shows schemas; anything else asks for one (`getSchema`).
      if (this.detailListeners.has(id)) await this.refreshSchemas(id, detail);
      return detail;
    } catch (cause) {
      if (isNotFound(cause)) {
        this.forget(id);
        return null;
      }
      this.log.debug({ err: cause, materializationId: id }, 'detail refresh failed');
      throw cause;
    }
  }

  /** Watched materializations refresh every tick; the rest rotate on the slower fleet cadence. */
  private detailRefreshTargets(now: number, includeStale: boolean): string[] {
    const watched = [...this.detailListeners.keys()].filter((id) => this.list.has(id));
    if (!includeStale) return watched;

    const stale = [...this.list.keys()]
      .filter((id) => !this.detailListeners.has(id))
      .filter((id) => now - (this.lastDetailFetch.get(id) ?? 0) >= config.fleetDetailPollIntervalMs)
      .sort((a, b) => (this.lastDetailFetch.get(a) ?? 0) - (this.lastDetailFetch.get(b) ?? 0));

    return [...new Set([...watched, ...stale])];
  }

  /** Read each table's schema the console does not have yet, or whose table has evolved past it. */
  private async refreshSchemas(id: string, detail: CpDetail): Promise<void> {
    const known = this.schemas.get(id);
    const due = readableTables(detail.materialization)
      .map((spec) => spec.name)
      .filter((name) => {
        const schema = known?.get(name);
        return schema === undefined || schemaDrifted(schema, detail);
      });
    await mapWithConcurrency(due, SCHEMA_CONCURRENCY, async (table) => {
      await this.fetchSchema(id, table).catch(() => undefined);
    });
  }

  private recordSample(id: string, detail: CpDetail, now: number): void {
    const progress = detail.status.runtime_progress;
    let tracker = this.rateTrackers.get(id);
    if (!tracker) {
      tracker = new RateTracker(rateWindows(), now);
      this.rateTrackers.set(id, tracker);
    }
    tracker.observe(progress, detail.status.last_heartbeat, now);
    const rates = tracker.current(now);
    this.rates.set(id, rates);
    this.recordLag(id, detail.status.consumer_lag);

    // A runtime that has never reported progress has nothing to chart.
    if (!progress) return;
    const sample: ThroughputSample = {
      t: now,
      rows: progress.cumulative_rows_written ?? progress.rows_written ?? 0,
      bytes: progress.cumulative_bytes_written ?? progress.bytes_written ?? 0,
      files: progress.cumulative_files_written ?? progress.files_written ?? 0,
      skipped: progress.cumulative_records_skipped ?? progress.records_skipped ?? 0,
      rowsPerSecond: rates.rowsPerSecond,
      bytesPerSecond: rates.bytesPerSecond,
      lag: detail.status.consumer_lag?.total_lag ?? null,
    };
    this.series.set(id, appendSample(this.series.get(id) ?? [], sample));
  }

  /** One point per lag measurement, kept by when the control plane measured it rather than when it was polled. */
  private recordLag(id: string, consumerLag: CpConsumerLag | undefined): void {
    if (!consumerLag?.measured_at || typeof consumerLag.total_lag !== 'number') return;
    const t = Date.parse(consumerLag.measured_at);
    if (!Number.isFinite(t)) return;
    const history = this.lagHistory.get(id) ?? [];
    const last = history.at(-1);
    if (last && t <= last.t) return;
    this.lagHistory.set(id, appendLag(history, { t, lag: consumerLag.total_lag }));
  }

  private recordFleetSample(now: number): void {
    const sample: ThroughputSample = {
      t: now,
      rows: 0,
      bytes: 0,
      files: 0,
      skipped: 0,
      rowsPerSecond: null,
      bytesPerSecond: null,
      lag: null,
    };
    for (const detail of this.details.values()) {
      const progress = detail.status.runtime_progress;
      if (!progress) continue;
      sample.rows += progress.cumulative_rows_written ?? progress.rows_written ?? 0;
      sample.bytes += progress.cumulative_bytes_written ?? progress.bytes_written ?? 0;
      sample.files += progress.cumulative_files_written ?? progress.files_written ?? 0;
      sample.skipped += progress.cumulative_records_skipped ?? progress.records_skipped ?? 0;
    }

    // The fleet's rate is the sum of each materialization's own, never a difference of summed
    // counters: details refresh on a rotating cadence, so a summed counter only moves for the
    // subset refreshed this tick. While any of them is still warming up the sum would understate
    // it, so there is none yet.
    const rates = [...this.rates.values()];
    if (!rates.some((entry) => entry.warming)) {
      for (const entry of rates) {
        if (entry.rowsPerSecond !== null) sample.rowsPerSecond = (sample.rowsPerSecond ?? 0) + entry.rowsPerSecond;
        if (entry.bytesPerSecond !== null) sample.bytesPerSecond = (sample.bytesPerSecond ?? 0) + entry.bytesPerSecond;
      }
    }
    this.fleetSeries = appendSample(this.fleetSeries, sample);
  }

  /** Drop everything the console holds for a materialization that has left the fleet. */
  private forget(id: string): void {
    this.list.delete(id);
    this.details.delete(id);
    this.schemas.delete(id);
    this.schemaErrors.delete(id);
    this.series.delete(id);
    this.rateTrackers.delete(id);
    this.rates.delete(id);
    this.lagHistory.delete(id);
    this.lastDetailFetch.delete(id);
  }

  private emitAll(): void {
    if (this.fleetListeners.size > 0) {
      const snapshot = this.fleetSnapshot();
      for (const listener of this.fleetListeners) listener(snapshot);
    }
    for (const [id, listeners] of this.detailListeners) {
      const snapshot = this.detailSnapshot(id);
      for (const listener of listeners) listener(snapshot);
    }
  }
}

function rateWindows(): { rowsMs: number; bytesMs: number; staleMs: number } {
  return {
    rowsMs: config.ingestRateWindowMs,
    bytesMs: config.writeRateWindowMs,
    // Mirrors the control plane's member freshness, and never less than two row windows.
    staleMs: Math.max(120_000, config.ingestRateWindowMs * 2),
  };
}

/**
 * Add `sample` to a series whose points are at least `seriesSampleMs` apart.
 *
 * The newest point follows every poll: it is replaced until it is far enough from the one before
 * it, and only then kept. The series therefore spans the same time whatever the poll cadence, and
 * its end is never older than the last poll.
 */
export function appendSample(
  series: ThroughputSample[],
  sample: ThroughputSample,
  spacingMs: number = config.seriesSampleMs,
  retention: number = config.seriesRetention,
): ThroughputSample[] {
  const settled = series.at(-2);
  const next = settled && sample.t - settled.t < spacingMs ? [...series.slice(0, -1), sample] : [...series, sample];
  return next.length > retention ? next.slice(next.length - retention) : next;
}

/**
 * Add a lag measurement to a history kept for `lagHistoryMs`.
 *
 * Measurements arrive about once a minute, or more often where the control plane is set to measure
 * more often, so a full history thins its older points rather than shortening: the last hour stays
 * at full resolution, and every other older point goes each time it fills.
 */
export function appendLag(
  history: LagSample[],
  sample: LagSample,
  keepMs: number = config.lagHistoryMs,
  maxPoints: number = LAG_HISTORY_POINTS,
): LagSample[] {
  const next = [...history.filter((point) => sample.t - point.t <= keepMs), sample];
  while (next.length > maxPoints) {
    const cutoff = sample.t - LAG_FULL_RESOLUTION_MS;
    const older = next.filter((point) => point.t < cutoff);
    if (older.length < 2) return next.slice(next.length - maxPoints);
    const drop = new Set(older.filter((_, index) => index % 2 === 1));
    const thinned = next.filter((point) => !drop.has(point));
    next.splice(0, next.length, ...thinned);
  }
  return next;
}

/**
 * What a materialization's tables hold: the rows, bytes and files of each table's current snapshot
 * as its committer last reported them, together - the figures the Overview's *Rows committed* and
 * *Table size* show. Not the runtime's `cumulative_*_written`, which also count whatever a member
 * wrote and lost to a restart before it was committed, and wrote again. Null before any table is
 * reported.
 */
export function committedTotals(progress: CpRuntimeProgress | undefined): { rows: number; bytes: number; files: number } | null {
  const tables = Object.values(progress?.tables ?? {});
  if (tables.length === 0) return null;
  return tables.reduce(
    (sum, table) => ({ rows: sum.rows + (table.total_records ?? 0), bytes: sum.bytes + (table.total_bytes ?? 0), files: sum.files + (table.total_files ?? 0) }),
    { rows: 0, bytes: 0, files: 0 },
  );
}

function committedColumns(totals: { rows: number; bytes: number; files: number } | null): Pick<MaterializationSummary, 'rowsCommitted' | 'tableBytes' | 'tableFiles'> {
  return { rowsCommitted: totals?.rows ?? null, tableBytes: totals?.bytes ?? null, tableFiles: totals?.files ?? null };
}

function aggregate(items: MaterializationSummary[], warming: boolean): FleetTotals {
  // Deleted materializations stay in `items` for the recently-deleted view but must not
  // distort fleet counts or throughput.
  const active = items.filter((item) => !isDeletedPhase(item));
  const totals: FleetTotals = {
    materializations: active.length,
    running: 0,
    stopped: 0,
    failing: 0,
    pending: 0,
    deleted: items.length - active.length,
    rowsCommitted: 0,
    tableBytes: 0,
    tableFiles: 0,
    rowsWritten: 0,
    bytesWritten: 0,
    filesWritten: 0,
    recordsSkipped: 0,
    rowsPerSecond: warming ? null : 0,
    bytesPerSecond: warming ? null : 0,
  };

  for (const item of active) {
    if (item.observed_phase === 'running') totals.running += 1;
    else if (item.observed_phase === 'stopped' || item.observed_phase === 'paused') totals.stopped += 1;
    else if (FAILING_PHASES.has(item.observed_phase)) totals.failing += 1;
    else if (PENDING_PHASES.has(item.observed_phase)) totals.pending += 1;

    totals.rowsCommitted += item.rowsCommitted ?? 0;
    totals.tableBytes += item.tableBytes ?? 0;
    totals.tableFiles += item.tableFiles ?? 0;
    totals.rowsWritten += item.rowsWritten ?? 0;
    totals.bytesWritten += item.bytesWritten ?? 0;
    totals.filesWritten += item.filesWritten ?? 0;
    totals.recordsSkipped += item.recordsSkipped ?? 0;
    if (totals.rowsPerSecond !== null) totals.rowsPerSecond += item.rowsPerSecond ?? 0;
    if (totals.bytesPerSecond !== null) totals.bytesPerSecond += item.bytesPerSecond ?? 0;
  }
  return totals;
}

async function mapWithConcurrency<T>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<void>,
): Promise<void> {
  if (items.length === 0) return;
  let cursor = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length) {
      const item = items[cursor++];
      if (item === undefined) return;
      await worker(item);
    }
  });
  await Promise.all(runners);
}

/**
 * A detail snapshot as one person may see it.
 *
 * The store reads with the console's own credential, which sees the schemas of every dataset; what
 * goes to a person holds only the schemas of the tables whose datasets they may see the schemas of,
 * and each table says whether they may read and query it. Every stream and read of a detail goes
 * through here before it leaves the server.
 */
export function visibleDetail(snapshot: DetailSnapshot, principal: CpPrincipal): DetailSnapshot {
  const detail = snapshot.detail;
  if (!detail) return { ...snapshot, schemas: {}, schemaErrors: {} };
  const datasetOf = new Map(readableTables(detail.materialization).map((spec) => [spec.name, spec.dataset_id]));
  for (const table of detail.tables) if (!datasetOf.has(table.name)) datasetOf.set(table.name, table.dataset_id);
  const readable = (table: string, fallback?: string): boolean => {
    const datasetId = datasetOf.get(table) ?? fallback;
    return datasetId !== undefined && can(principal, 'dataset.view_schema', datasetId);
  };
  return {
    ...snapshot,
    detail: {
      ...detail,
      tables: detail.tables.map((table) => ({
        ...table,
        readable: can(principal, 'dataset.view_schema', table.dataset_id),
        queryable: can(principal, 'data.query', table.dataset_id),
      })),
    },
    schemas: Object.fromEntries(Object.entries(snapshot.schemas).filter(([table, schema]) => readable(table, schema.dataset_id))),
    schemaErrors: Object.fromEntries(Object.entries(snapshot.schemaErrors).filter(([table]) => readable(table))),
  };
}

/** The materialization's table called `table`, written or kept, compared without regard to case as SQL does; 404 when none is. */
export function tableSpec(detail: CpDetail, table: string): CpTableSpec {
  const wanted = table.trim().toLowerCase();
  const tables = readableTables(detail.materialization);
  const spec = tables.find((candidate) => candidate.name.toLowerCase() === wanted);
  if (spec) return spec;
  const known = tables.map((candidate) => candidate.name).join(', ');
  throw new ConsoleError('not_found', `materialization ${detail.materialization.materialization_id} has no table ${table}; its tables are ${known}`, 404, {
    resource: 'table',
    id: table,
  });
}

/** Whether the table record names a newer schema than `schema`: the table evolved since it was read. */
function schemaDrifted(schema: CpSchema, detail: CpDetail): boolean {
  const reported = detail.tables.find((record) => record.name === schema.table)?.schema_id ?? null;
  return reported !== null && schema.schema_id !== reported;
}

function mapOf<V>(maps: Map<string, Map<string, V>>, id: string): Map<string, V> {
  let map = maps.get(id);
  if (!map) {
    map = new Map();
    maps.set(id, map);
  }
  return map;
}

function isNotFound(cause: unknown): boolean {
  return (
    typeof cause === 'object' &&
    cause !== null &&
    'statusCode' in cause &&
    (cause as { statusCode: unknown }).statusCode === 404
  );
}

