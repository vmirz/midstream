/**
 * The data files behind one of a materialization's tables at one snapshot, and the cheap questions
 * they answer.
 *
 * Everything here is metadata: the table's manifests record every data file's path, exact row
 * count and Kafka-timestamp span (`iceberg/snapshot.ts`), so "how many rows", "what does this time
 * range cover" and "which files can a range skip" are all answered without opening a Parquet file.
 * That is what makes the first page of a table instant at any size, and what makes a narrow time
 * range cheap — the files outside it are never read at all.
 */

import { dataAccessMode } from '../config.ts';
import { ConsoleError, unavailable } from '../errors.ts';
import { logger } from '../logger.ts';
import { ObjectMissingError } from '../iceberg/io.ts';
import { currentSchema, type IcebergSchema } from '../iceberg/metadata.ts';
import { FORK_MAP_PROPERTY, PROTECTION_PROPERTY, parseForkMap, parseProtection, type ForkMap, type ProtectionMap } from '../iceberg/schema.ts';
import { snapshotCatalog, type DataObject, type SnapshotCatalog } from '../iceberg/snapshot.ts';
import { loadTable } from '../iceberg/table.ts';
import { warmFooter } from './objects.ts';
import type { DataSummary, ScopeEstimate, TimeRange } from '../../shared/types.ts';

export type { DataObject } from '../iceberg/snapshot.ts';

export interface Catalog {
  materializationId: string;
  /** The table's name: what statements read it by. */
  table: string | null;
  /** The table's location; null before the control plane has created it. */
  location: string | null;
  metadataLocation: string | null;
  /** The table's current schema, which every snapshot is read through. Null without a table. */
  schema: IcebergSchema | null;
  forkMap: ForkMap;
  /** The table's protected fields (handbook 22); empty for a table that records none. */
  protection: ProtectionMap;
  /** The snapshot the catalog describes. Null without a table. */
  snapshot: SnapshotCatalog | null;
  /** The table's current snapshot when the catalog was read; differs from `snapshot` when pinned as of an older one. */
  currentSnapshotId: string | null;
  /** Live data files of the snapshot, sorted by name. */
  objects: DataObject[];
  /** Why there is nothing to read, when there is not. */
  reason: string | null;
  /** When the table's pointer was read, epoch milliseconds. */
  loadedAt: number;
}

/**
 * The files of one of a materialization's tables, at its current snapshot or at `snapshotId`.
 *
 * `fresh` asks for a table read no older than `snapshotCacheMs`, for a run that should include
 * whatever was committed moments before Run was pressed (`iceberg/table.ts`).
 */
export async function loadCatalog(
  materializationId: string,
  location: string | null,
  options: { fresh?: boolean; snapshotId?: string | null; table?: string | null } = {},
): Promise<Catalog> {
  assertQueryable();
  const name = options.table ?? null;
  if (!location) return emptyCatalog(materializationId, name, null, 'the table has not been created yet');
  let table: Awaited<ReturnType<typeof loadTable>>;
  let snapshot: SnapshotCatalog;
  try {
    table = await loadTable(location, { fresh: options.fresh });
    if (!table) return emptyCatalog(materializationId, name, location, 'the table has not been created yet');
    snapshot = await snapshotCatalog(table.metadata, options.snapshotId ?? table.metadata.currentSnapshotId);
  } catch (cause) {
    if (!(cause instanceof ConsoleError)) logger.warn({ err: cause, location }, 'table could not be read');
    throw unreadableTable(location, cause);
  }
  const catalog: Catalog = {
    materializationId,
    table: name,
    location,
    metadataLocation: table.metadataLocation,
    schema: currentSchema(table.metadata),
    forkMap: parseForkMap(table.metadata.properties[FORK_MAP_PROPERTY]),
    protection: parseProtection(table.metadata.properties[PROTECTION_PROPERTY]),
    snapshot,
    currentSnapshotId: table.metadata.currentSnapshotId,
    objects: snapshot.objects,
    reason: snapshot.objects.length > 0 ? null : 'nothing has been committed to the table yet',
    loadedAt: table.readAt,
  };
  if (snapshot.snapshotId === table.metadata.currentSnapshotId) scheduleFooterWarming(catalog);
  return catalog;
}

/**
 * Why a table that exists cannot be read — storage unreachable or refusing, a metadata file or
 * manifest missing — as a 503 that says which, not as a failure of the request. Storage's own
 * message is left to the log: it can name the console's service account.
 */
export function unreadableTable(location: string, cause: unknown): ConsoleError {
  if (cause instanceof ConsoleError) return cause;
  const status = (cause as { code?: unknown } | null)?.code;
  const why =
    cause instanceof ObjectMissingError
      ? cause.message
      : typeof status === 'number'
        ? `object storage answered ${status}`
        : 'object storage could not be reached';
  return unavailable(`the table at ${location} could not be read: ${why}`, { location });
}

function emptyCatalog(materializationId: string, table: string | null, location: string | null, reason: string): Catalog {
  return {
    materializationId,
    table,
    location,
    metadataLocation: null,
    schema: null,
    forkMap: new Map(),
    protection: new Map(),
    snapshot: null,
    currentSnapshotId: null,
    objects: [],
    reason,
    loadedAt: Date.now(),
  };
}

function assertQueryable(): void {
  if (dataAccessMode() === 'disabled') throw unavailable('data access is turned off for this console (Settings › Data explorer)');
}

export function parseInstant(text: string | null): number | null {
  if (!text) return null;
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? parsed : null;
}

/** Whether an object may hold rows inside `range`. Never rules out what it cannot see. */
export function overlapsRange(object: DataObject, range: TimeRange): boolean {
  if (range.from === null && range.to === null) return true;
  if (!object.timeKnown) return true;
  if (object.timeMin === null || object.timeMax === null) return false;
  if (range.from !== null && object.timeMax < range.from) return false;
  if (range.to !== null && object.timeMin >= range.to) return false;
  return true;
}

/** Whether every row of an object lies inside `range`, so its manifest count is the answer. */
export function insideRange(object: DataObject, range: TimeRange): boolean {
  if (range.from === null && range.to === null) return true;
  if (!object.timeKnown || object.timeMin === null || object.timeMax === null) return false;
  return (range.from === null || object.timeMin >= range.from) && (range.to === null || object.timeMax < range.to);
}

/** Everything the console can say about a table's data without reading any of it. */
export function summarize(catalog: Catalog): DataSummary {
  let bytes = 0;
  let rows = 0;
  let rowsExact = true;
  let untimedRows = 0;
  let timeMin: number | null = null;
  let timeMax: number | null = null;
  let newestCommit: number | null = null;

  for (const object of catalog.objects) {
    bytes += object.bytes;
    if (object.rows === null) rowsExact = false;
    else rows += object.rows;
    if (object.timeKnown && object.timeMin === null) untimedRows += object.rows ?? 0;
    if (object.timeMin !== null && (timeMin === null || object.timeMin < timeMin)) timeMin = object.timeMin;
    if (object.timeMax !== null && (timeMax === null || object.timeMax > timeMax)) timeMax = object.timeMax;
    if (object.committedAt !== null && (newestCommit === null || object.committedAt > newestCommit)) {
      newestCommit = object.committedAt;
    }
  }

  const snapshot = catalog.snapshot;
  const available = catalog.objects.length > 0;
  const iso = (value: number | null): string | null => (value === null ? null : new Date(value).toISOString());
  return {
    materializationId: catalog.materializationId,
    table: catalog.table,
    available,
    reason: catalog.reason,
    objects: catalog.objects.length,
    bytes,
    rows,
    rowsExact,
    untimedRows,
    timeMin: iso(timeMin),
    timeMax: iso(timeMax),
    newestCommit: iso(snapshot?.committedAt ?? newestCommit),
    source: 'iceberg',
    asOf: new Date(catalog.loadedAt).toISOString(),
    snapshotId: snapshot?.snapshotId ?? null,
    snapshotAt: iso(snapshot?.committedAt ?? null),
    currentSnapshotId: catalog.currentSnapshotId,
    validThrough: snapshot?.validThrough ?? null,
    deleteFiles: snapshot?.deleteFiles ?? 0,
  };
}

/** What a run over `range` would read: an upper bound unless every object lies wholly inside. */
export function estimateRange(catalog: Catalog, range: TimeRange): ScopeEstimate {
  let objects = 0;
  let bytes = 0;
  let rows = 0;
  let rowsExact = true;
  for (const object of catalog.objects) {
    if (!overlapsRange(object, range)) continue;
    objects += 1;
    bytes += object.bytes;
    rows += object.rows ?? 0;
    if (object.rows === null || !insideRange(object, range)) rowsExact = false;
  }
  return { range, objects, bytes, rows, rowsExact };
}

/*
 * Background footer warming.
 *
 * Reading a data file starts with its footer: a HEAD and two ranged reads, ~150 ms from outside the
 * cloud. DuckDB keeps every footer it has read (`parquet_metadata_cache`), so reading them ahead of
 * the first scan that needs them takes that cost off the first filter or statement someone runs.
 * Newest first and capped, because a long-lived table can hold more files than are worth holding in
 * memory, and recent data is what people look at. A few at a time, and held back while any run
 * scans, so it never competes with a question someone is waiting on.
 */

const WARM_LIMIT = 5_000;
const WARM_CONCURRENCY = 8;
const warmed = new Set<string>();
let warming: Promise<void> | null = null;
const warmQueue: DataObject[] = [];
let warmingHolds = 0;
const warmingResumes: Array<() => void> = [];

/** Hold footer warming back, e.g. while a run scans. Returns the release. */
export function pauseWarming(): () => void {
  warmingHolds += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    warmingHolds -= 1;
    if (warmingHolds === 0) for (const resume of warmingResumes.splice(0)) resume();
  };
}

function scheduleFooterWarming(catalog: Catalog): void {
  if (dataAccessMode() !== 'direct') return;
  const pending = [...catalog.objects]
    .filter((object) => !warmed.has(object.uri))
    .sort((a, b) => (b.timeMax ?? 0) - (a.timeMax ?? 0))
    .slice(0, Math.max(0, WARM_LIMIT - warmed.size));
  if (pending.length === 0) return;
  for (const object of pending) warmed.add(object.uri);
  warmQueue.push(...pending);
  warming ??= drainWarmQueue().finally(() => {
    warming = null;
  });
}

async function drainWarmQueue(): Promise<void> {
  const worker = async (): Promise<void> => {
    while (warmQueue.length > 0) {
      while (warmingHolds > 0) await new Promise<void>((resume) => warmingResumes.push(resume));
      const object = warmQueue.shift();
      if (!object) return;
      try {
        await warmFooter(object.uri);
      } catch (cause) {
        warmed.delete(object.uri);
        logger.debug({ err: cause, uri: object.uri }, 'footer warming failed');
      }
    }
  };
  await Promise.all(Array.from({ length: WARM_CONCURRENCY }, () => worker()));
}
