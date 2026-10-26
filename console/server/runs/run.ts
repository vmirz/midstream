/**
 * A run: the console's only unit of query work.
 *
 * Pressing Run creates one. The first page of a table, a time range, a filter and a hand-written
 * statement are all runs; paging and exporting read from one. There is no other way a query is
 * executed, so there is nothing for a client to choose between and no second path to drift from
 * the first.
 *
 * Every run is pinned to one snapshot of each table it reads — the current one when it started, or
 * the one it was asked for — which is what keeps paging stable while new commits keep arriving:
 * page 40 always shows the same rows, and Refresh is an explicit new run rather than something that
 * happens to whoever pages next.
 *
 * A run of a table also reads the records past its snapshot from the tail app, its live tail
 * (`runs/tail.ts`), as further files in the run's own directory; a browse run's refresh appends
 * those that arrive later, and bumps the run's `revision` so pages read before it are read again.
 */

import { randomUUID } from 'node:crypto';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';

import { config } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import { cancelledError, isCancelled } from '../data/duckdb.ts';
import type { Catalog, DataObject } from '../data/catalog.ts';
import type { LogicalColumn } from '../data/logical.ts';
import { TableTail, type TailAccess } from './tail.ts';
import type { IcebergSchema } from '../iceberg/metadata.ts';
import type {
  QueryColumn,
  RunError,
  RunPage,
  RunPhase,
  RunProgress,
  RunRequest,
  RunSnapshot,
  RunState,
  RunTableRead,
} from '../../shared/types.ts';

export interface RunInit {
  /** The domain the run was started in: its materialization's (handbook 20). */
  domain: string;
  materializationId: string;
  /** Who started the run, as its domain knows them; only they can read it (`runs/store.ts::getRun`). */
  ownerId: string;
  /** The run's table: the one browsed, or the statement's (`RunRequest.table`). */
  tableName: string;
  /**
   * The datasets of the tables it reads, each in its domain: reading its rows needs `data.query` on
   * each, again, as the person is in that domain.
   */
  datasets: RunDataset[];
  request: RunRequest;
  /** The run's table at the snapshot it reads. */
  catalog: Catalog;
  columns: LogicalColumn[];
  /** Admission for the scanning phase, so a burst of runs cannot saturate DuckDB. */
  admit: (signal: AbortSignal) => Promise<() => void>;
  /** How the run reads its tables' live tails; absent, it reads none (`liveTail: false`, `asOf`). */
  tail?: TailAccess | null;
}

type Listener = (snapshot: RunSnapshot) => void;

/** How often listeners hear about progress while a run works. Terminal changes go out at once. */
const EMIT_INTERVAL_MS = 200;

/** A dataset a run reads, in the domain it belongs to. */
export interface RunDataset {
  domain: string;
  id: string;
  name: string;
}

export abstract class Run {
  readonly id = randomUUID();
  readonly domain: string;
  readonly materializationId: string;
  readonly ownerId: string;
  readonly tableName: string;
  readonly datasets: ReadonlyArray<RunDataset>;
  readonly request: RunRequest;
  readonly startedAt = Date.now();
  protected readonly catalog: Catalog;
  /** The table schema every data file is read through. Null when there is no table. */
  protected readonly schema: IcebergSchema | null;
  protected readonly logical: LogicalColumn[];
  protected readonly admit: RunInit['admit'];
  /** The run's own directory: its tail files and a statement's result, removed when it expires. */
  protected readonly workDir: string;
  protected readonly tailAccess: TailAccess | null;
  /** The live tail of each table the run reads, by table name, once fetched. */
  protected readonly tails = new Map<string, TableTail>();
  /** Bumped whenever the run's rows change after it finished: a refresh appended the tail's newest. */
  revision = 0;
  protected disposed = false;

  state: RunState = 'running';
  phase: RunPhase = 'planning';
  finishedAt: number | null = null;
  scope = { objects: 0, bytes: 0, rows: 0 };
  progress: RunProgress = {
    rowsTotal: 0,
    rowsDone: 0,
    objectsTotal: 0,
    objectsDone: 0,
    objectsRead: 0,
    bytesTotal: 0,
    fraction: null,
  };
  rows = 0;
  totalExact = false;
  rowsReady = 0;
  columns: QueryColumn[] | null = null;
  truncated = false;
  rowsRetained: number | null = null;
  error: RunError | null = null;
  lastAccess = Date.now();

  private readonly abortController = new AbortController();
  private rowHolds = 0;
  private cancelRequested = false;
  private readonly listeners = new Set<Listener>();
  private emitTimer: NodeJS.Timeout | null = null;
  private lastEmitAt = 0;

  constructor(init: RunInit) {
    this.domain = init.domain;
    this.materializationId = init.materializationId;
    this.ownerId = init.ownerId;
    this.tableName = init.tableName;
    this.datasets = init.datasets;
    this.request = init.request;
    this.catalog = init.catalog;
    this.schema = init.catalog.schema;
    this.logical = init.columns;
    this.admit = init.admit;
    this.workDir = join(config.runDir, this.id);
    this.tailAccess = init.tail ?? null;
  }

  /**
   * Fetch the live tail of each table, past its snapshot, all at once. Every failure is the tail's
   * status, never the run's: a run whose tail cannot be had reads committed rows and says why.
   */
  protected async fetchTails(tables: Array<{ name: string; catalog: Catalog }>): Promise<void> {
    const access = this.tailAccess;
    if (access === null) return;
    await Promise.all(
      tables.map(async ({ name, catalog }) => {
        const tail = new TableTail(name, catalog, access.off);
        this.tails.set(name, tail);
        await tail.fetch(access, this.workDir, this.signal, () => !this.disposed);
      }),
    );
  }

  /** The files a table's live tail holds for this run, oldest first; none without a tail. */
  protected tailObjects(table: string): DataObject[] {
    return [...(this.tails.get(table)?.objects ?? [])];
  }

  /** The live tail of a table the run reads — its own by default — or undefined when it read none. */
  tailOf(table: string = this.tableName): TableTail | undefined {
    return this.tails.get(table);
  }

  /** The snapshot of its own table the run is pinned to; null for a table with none yet. */
  get snapshotId(): string | null {
    return this.catalog.snapshot?.snapshotId ?? null;
  }

  /** Aborted on cancel, and on the first failure, so every scan this run started stops at once. */
  protected get signal(): AbortSignal {
    return this.abortController.signal;
  }

  /** The work. Resolving leaves the run to be marked succeeded; throwing marks it failed. */
  protected abstract execute(): Promise<void>;

  /** Rows `[offset, offset + limit)` in run order, as far as they are ready now. */
  abstract readPage(offset: number, limit: number): Promise<RunPage>;

  /** The same rows as `readPage`, bypassing its cache. For exports, which read everything once. */
  abstract readRows(offset: number, limit: number): Promise<Record<string, unknown>[]>;

  /** Rows an export of this run contains: the whole result, up to the export limit. */
  abstract exportableRows(): number;

  /** Statements that expose this run's rows as the view `ROWS_VIEW`, in no particular order. */
  abstract rowsView(): Promise<string[]>;

  /** Files this run keeps on disk — its tail files, a statement's result — deleted when it expires. */
  protected async releaseStorage(): Promise<void> {
    await rm(this.workDir, { recursive: true, force: true });
  }

  /** Every table the run reads, at the snapshot it is pinned to. A browse run reads its own. */
  protected tableReads(): RunTableRead[] {
    return [tableRead(this.tableName, this.catalog, this.tails.get(this.tableName))];
  }

  /**
   * Result columns that carry a `json` field's text, by name.
   *
   * A statement keeps a column's name unless it renames it, so a field selected as itself is still
   * recognised; an alias or an expression is just text.
   */
  jsonTextColumns(): Set<string> {
    return new Set((this.columns ?? []).filter((column) => column.json).map((column) => column.name));
  }

  async start(): Promise<void> {
    try {
      await this.execute();
      if (this.state === 'running') this.finish('succeeded');
    } catch (cause) {
      if (this.cancelRequested || isCancelled(cause)) {
        if (this.state === 'running') this.finish('cancelled');
        return;
      }
      const error = toRunError(cause);
      // A statement that does not parse is the user's to fix, not the console's to report.
      if (error.code === 'query_failed') logger.debug({ err: cause, runId: this.id }, 'run statement failed');
      else logger.warn({ err: cause, runId: this.id }, 'run failed');
      this.finish('failed', error);
    }
  }

  cancel(): void {
    if (this.state !== 'running') return;
    this.cancelRequested = true;
    this.abortController.abort();
    this.finish('cancelled');
  }

  /** Stop every scan this run has in flight, because one of them failed. */
  protected abortAfterFailure(): void {
    this.abortController.abort();
  }

  protected throwIfCancelled(): void {
    if (this.signal.aborted) throw cancelledError();
  }

  protected finish(state: Exclude<RunState, 'running'>, error: RunError | null = null): void {
    if (this.state !== 'running') return;
    this.state = state;
    this.phase = 'done';
    this.error = error;
    this.finishedAt = Date.now();
    this.changed(true);
  }

  isTerminal(): boolean {
    return this.state !== 'running';
  }

  touch(): void {
    this.lastAccess = Date.now();
  }

  /**
   * Keep the run's rows as they are until the release is called: an export reads them a page at a
   * time, and a refresh appending the tail's newest meanwhile would move every row it has yet to
   * read. A refresh while rows are held fetches nothing; the next one after the release does.
   */
  holdRows(): () => void {
    this.rowHolds += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.rowHolds -= 1;
    };
  }

  protected get rowsHeld(): boolean {
    return this.rowHolds > 0;
  }

  /** Let the run expire `ms` from now unless it is read again: another run has taken its place. */
  expireWithin(ms: number): void {
    this.lastAccess = Math.min(this.lastAccess, Date.now() - config.runIdleRetentionMs + ms);
  }

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Tell listeners something changed, at most every `EMIT_INTERVAL_MS` unless `immediate`. */
  protected changed(immediate = false): void {
    if (immediate) {
      if (this.emitTimer) {
        clearTimeout(this.emitTimer);
        this.emitTimer = null;
      }
      this.emit();
      return;
    }
    if (this.emitTimer) return;
    const wait = Math.max(0, EMIT_INTERVAL_MS - (Date.now() - this.lastEmitAt));
    this.emitTimer = setTimeout(() => {
      this.emitTimer = null;
      this.emit();
    }, wait);
  }

  private emit(): void {
    this.lastEmitAt = Date.now();
    const snapshot = this.snapshot();
    for (const listener of this.listeners) {
      try {
        listener(snapshot);
      } catch (cause) {
        logger.debug({ err: cause, runId: this.id }, 'run listener failed');
      }
    }
  }

  snapshot(): RunSnapshot {
    const until = this.finishedAt ?? Date.now();
    return {
      id: this.id,
      domain: this.domain,
      materializationId: this.materializationId,
      request: this.request,
      state: this.state,
      phase: this.phase,
      startedAt: new Date(this.startedAt).toISOString(),
      finishedAt: this.finishedAt === null ? null : new Date(this.finishedAt).toISOString(),
      elapsedMs: Math.max(0, until - this.startedAt),
      table: this.tableName,
      tables: this.tableReads(),
      asOf: new Date(this.catalog.loadedAt).toISOString(),
      snapshotId: this.catalog.snapshot?.snapshotId ?? null,
      snapshotAt: this.catalog.snapshot?.committedAt == null ? null : new Date(this.catalog.snapshot.committedAt).toISOString(),
      pinned: this.catalog.snapshot !== null && this.catalog.snapshot.snapshotId !== this.catalog.currentSnapshotId,
      validThrough: this.catalog.snapshot?.validThrough ?? null,
      scope: { ...this.scope },
      progress: { ...this.progress },
      rows: this.rows,
      totalExact: this.totalExact,
      rowsReady: this.rowsReady,
      columns: this.columns,
      truncated: this.truncated,
      rowsRetained: this.rowsRetained,
      error: this.error,
      expiresAt: new Date(this.lastAccess + config.runIdleRetentionMs).toISOString(),
      revision: this.revision,
      ...(this.tails.has(this.tableName) ? { tail: (this.tails.get(this.tableName) as TableTail).view() } : {}),
    };
  }

  async dispose(): Promise<void> {
    this.disposed = true;
    this.cancel();
    this.listeners.clear();
    try {
      await this.releaseStorage();
    } catch (cause) {
      logger.debug({ err: cause, runId: this.id }, 'run storage already released');
    }
  }
}

/** A table as a run reads it: the snapshot it is pinned to, and the live tail past it. */
export function tableRead(name: string, catalog: Catalog, tail?: TableTail): RunTableRead {
  const snapshot = catalog.snapshot;
  return {
    name,
    snapshotId: snapshot?.snapshotId ?? null,
    snapshotAt: snapshot?.committedAt == null ? null : new Date(snapshot.committedAt).toISOString(),
    pinned: snapshot !== null && snapshot.snapshotId !== catalog.currentSnapshotId,
    validThrough: snapshot?.validThrough ?? null,
    ...(tail ? { tail: tail.view() } : {}),
  };
}

export function toRunError(cause: unknown): RunError {
  if (cause instanceof ConsoleError) {
    const position = cause.details.position as RunError['position'] | undefined;
    return { code: cause.code, message: cause.message, position: position ?? null };
  }
  return { code: 'run_failed', message: cause instanceof Error ? cause.message : String(cause) };
}
