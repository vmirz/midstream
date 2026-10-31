/**
 * What one snapshot of a table holds: its data files, as the run engine reads them.
 *
 * Every live data file becomes a `DataObject` carrying its exact row count and, from the manifest's
 * column bounds, its Kafka-timestamp span, partition and offsets. That is what lets a browse run
 * know its total and the position of every row without opening a file, and a time range skip the
 * files outside it. A snapshot never changes, so its catalog is built once and kept; the manifests
 * behind it are kept too, so the next snapshot reads only the manifests it added.
 */

import type { PartitionGranularity } from '../../shared/types.ts';
import { notFound } from '../errors.ts';
import { objectKey } from '../storage/uris.ts';
import { assertUnderLocation, isUnderLocation } from './io.ts';
import { ENTRY_DELETED, decodeBound, readManifest, readManifestList, type ManifestEntry, type ManifestFile } from './manifests.ts';
import { partitionGranularity, snapshotById, type IcebergSnapshot, type TableMetadata } from './metadata.ts';
import { OFFSET_FIELD_ID, PARTITION_FIELD_ID, TIMESTAMP_FIELD_ID } from './schema.ts';

export interface DataObject {
  uri: string;
  /** The object's key within its bucket or container. */
  name: string;
  bytes: number;
  /** Exact rows, from the manifest. */
  rows: number | null;
  /** When the snapshot that added the file was committed, epoch milliseconds; null once expired. */
  committedAt: number | null;
  /** The Kafka partition every row came from; null for a compacted file spanning several. */
  partition: number | null;
  offsetStart: number | null;
  offsetEnd: number | null;
  /**
   * Inclusive span of the file's Kafka timestamps, epoch milliseconds.
   *
   * Null when the file holds no timestamped row, or when the span is unknown — `timeKnown` tells
   * the two apart: a file known to hold no timestamps can never match a bounded range, while one
   * whose span is unknown can never be ruled out.
   */
  timeMin: number | null;
  timeMax: number | null;
  timeKnown: boolean;
  /** The table partition the file is in: `2026-10-17` (day), `2026-10-17-10` (hour), `__null__`, or null. */
  partitionKey: string | null;
  /** A file of the live tail's: records newer than the snapshot, on the console's own disk (`runs/tail.ts`). */
  tail?: boolean;
}

export interface SnapshotCatalog {
  tableUuid: string;
  /** Null for a table with no snapshot yet: created, nothing committed. */
  snapshotId: string | null;
  sequenceNumber: number | null;
  /** When the snapshot was committed, epoch milliseconds. */
  committedAt: number | null;
  operation: string | null;
  schemaId: number | null;
  /** Live data files, sorted by name. */
  objects: DataObject[];
  /** Live delete files; the materializer writes none, but an external engine with write access may. */
  deleteFiles: number;
  /**
   * Live data files outside the table's location. The materializer writes none; an engine that may
   * commit can add one, and the console never reads it, so they are counted and left out of `objects`.
   */
  outsideFiles: number;
  summary: Record<string, string>;
  /** Committed Kafka offsets by partition, from the snapshot summary. */
  offsets: Record<string, number>;
  /** Every partition's records up to this instant are in this snapshot (`materializer.valid-through`). */
  validThrough: string | null;
}

const MANIFEST_CACHE_LIMIT = 5_000;
const SNAPSHOT_CACHE_LIMIT = 64;
const manifestLists = new Map<string, Promise<ManifestFile[]>>();
const manifests = new Map<string, Promise<ManifestEntry[]>>();
const snapshots = new Map<string, Promise<SnapshotCatalog>>();

function cached<T>(cache: Map<string, Promise<T>>, key: string, limit: number, load: () => Promise<T>): Promise<T> {
  const hit = cache.get(key);
  if (hit) {
    // Re-inserted, so eviction drops the least recently used.
    cache.delete(key);
    cache.set(key, hit);
    return hit;
  }
  const loading = load();
  cache.set(key, loading);
  loading.catch(() => cache.delete(key));
  while (cache.size > limit) cache.delete(cache.keys().next().value as string);
  return loading;
}

/** The catalog of `snapshotId`, or of the current snapshot when it is not given. */
export function snapshotCatalog(metadata: TableMetadata, snapshotId: string | null = metadata.currentSnapshotId): Promise<SnapshotCatalog> {
  if (snapshotId === null) return Promise.resolve(emptyCatalog(metadata));
  const snapshot = snapshotById(metadata, snapshotId);
  if (!snapshot) {
    return Promise.reject(
      notFound(`snapshot ${snapshotId} is not in this table; it may have expired`, { resource: 'snapshot', id: snapshotId }),
    );
  }
  return cached(snapshots, `${metadata.tableUuid}:${snapshot.snapshotId}`, SNAPSHOT_CACHE_LIMIT, () => buildCatalog(metadata, snapshot));
}

function emptyCatalog(metadata: TableMetadata): SnapshotCatalog {
  return {
    tableUuid: metadata.tableUuid,
    snapshotId: null,
    sequenceNumber: null,
    committedAt: null,
    operation: null,
    schemaId: metadata.currentSchemaId,
    objects: [],
    deleteFiles: 0,
    outsideFiles: 0,
    summary: {},
    offsets: {},
    validThrough: null,
  };
}

/** One of a snapshot's manifests and its entries. */
export interface SnapshotManifest {
  manifest: ManifestFile;
  entries: ManifestEntry[];
}

/**
 * A snapshot's manifests with their entries, read through the caches its catalog is built from: a
 * manifest never changes, so each is read once whatever snapshots list it. Refused, before anything
 * is read, when the snapshot names a manifest list or manifest outside the table's location.
 */
export async function snapshotManifests(metadata: TableMetadata, snapshot: IcebergSnapshot): Promise<SnapshotManifest[]> {
  const location = metadata.location;
  const manifestList = assertUnderLocation(snapshot.manifestList, location, 'a manifest list');
  const list = await cached(manifestLists, manifestList, MANIFEST_CACHE_LIMIT, () => readManifestList(manifestList));
  for (const manifest of list) assertUnderLocation(manifest.path, location, 'a manifest');
  const entriesPerManifest = await Promise.all(
    list.map((manifest) => cached(manifests, manifest.path, MANIFEST_CACHE_LIMIT, () => readManifest(manifest))),
  );
  return list.map((manifest, index) => ({ manifest, entries: entriesPerManifest[index] ?? [] }));
}

async function buildCatalog(metadata: TableMetadata, snapshot: IcebergSnapshot): Promise<SnapshotCatalog> {
  const location = metadata.location;
  const committedAtBySnapshot = new Map(metadata.snapshots.map((item) => [item.snapshotId, item.timestampMs]));
  const granularity = partitionGranularity(metadata);
  const objects: DataObject[] = [];
  let deleteFiles = 0;
  let outsideFiles = 0;
  for (const { manifest, entries } of await snapshotManifests(metadata, snapshot)) {
    for (const entry of entries) {
      if (entry.status === ENTRY_DELETED) continue;
      if (manifest.content !== 0 || entry.content !== 0) {
        deleteFiles += 1;
        continue;
      }
      if (!isUnderLocation(entry.filePath, location)) {
        outsideFiles += 1;
        continue;
      }
      objects.push(toDataObject(entry, committedAtBySnapshot, granularity));
    }
  }
  objects.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  return {
    tableUuid: metadata.tableUuid,
    snapshotId: snapshot.snapshotId,
    sequenceNumber: snapshot.sequenceNumber,
    committedAt: snapshot.timestampMs,
    operation: snapshot.summary.operation ?? null,
    schemaId: snapshot.schemaId,
    objects,
    deleteFiles,
    outsideFiles,
    summary: snapshot.summary,
    offsets: committedOffsets(metadata, snapshot),
    validThrough: snapshot.summary['materializer.valid-through'] ?? null,
  };
}

/** The offsets a snapshot carries, walking back past snapshots that carry none (an external commit). */
export function committedOffsets(metadata: TableMetadata, snapshot: IcebergSnapshot): Record<string, number> {
  let current: IcebergSnapshot | undefined = snapshot;
  for (let walked = 0; current && walked < 10_000; walked += 1) {
    const raw = current.summary['materializer.offsets'];
    if (raw !== undefined) {
      try {
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        return Object.fromEntries(Object.entries(parsed).map(([partition, offset]) => [partition, Number(offset)]));
      } catch {
        return {};
      }
    }
    current = current.parentSnapshotId === null ? undefined : snapshotById(metadata, current.parentSnapshotId);
  }
  return {};
}

function toDataObject(
  entry: ManifestEntry,
  committedAtBySnapshot: ReadonlyMap<string, number>,
  granularity: PartitionGranularity,
): DataObject {
  const timeLower = entry.lowerBounds.get(TIMESTAMP_FIELD_ID);
  const timeUpper = entry.upperBounds.get(TIMESTAMP_FIELD_ID);
  const partitionLower = bound(entry.lowerBounds.get(PARTITION_FIELD_ID), 'int');
  const partitionUpper = bound(entry.upperBounds.get(PARTITION_FIELD_ID), 'int');
  const single = partitionLower !== null && partitionLower === partitionUpper;
  const timeMin = timestampMs(timeLower);
  const timeMax = timestampMs(timeUpper);
  // No bounds and every value null is a file of untimed rows; no bounds otherwise is an unknown span.
  const allNull = entry.nullValueCounts.get(TIMESTAMP_FIELD_ID) === entry.recordCount;
  return {
    uri: entry.filePath,
    name: objectKey(entry.filePath),
    bytes: entry.fileSizeInBytes,
    rows: entry.recordCount,
    committedAt: entry.snapshotId === null ? null : (committedAtBySnapshot.get(entry.snapshotId) ?? null),
    partition: single ? Number(partitionLower) : null,
    offsetStart: single ? numberBound(entry.lowerBounds.get(OFFSET_FIELD_ID)) : null,
    offsetEnd: single ? numberBound(entry.upperBounds.get(OFFSET_FIELD_ID)) : null,
    timeMin,
    timeMax,
    timeKnown: (timeMin !== null && timeMax !== null) || allNull,
    partitionKey: partitionKey(entry.partition, granularity),
  };
}

function bound(buffer: Buffer | undefined, type: string): number | bigint | null {
  if (!buffer) return null;
  const value = decodeBound(type, buffer);
  return typeof value === 'number' || typeof value === 'bigint' ? value : null;
}

function numberBound(buffer: Buffer | undefined): number | null {
  const value = bound(buffer, 'long');
  return value === null ? null : Number(value);
}

/** A timestamp bound (microseconds) as epoch milliseconds, rounded down. */
export function timestampMs(buffer: Buffer | undefined): number | null {
  const value = bound(buffer, 'timestamptz');
  if (value === null) return null;
  const big = BigInt(value);
  const millis = big >= 0n ? big / 1000n : (big - 999n) / 1000n;
  return Number(millis);
}


/** The partition of rows that have no Kafka timestamp, as the runtime names it. */
export const NULL_PARTITION = '__null__';

/**
 * A partition as the runtime names it in its file paths and maintenance reports:
 * `{ ts_day: 20743 }` → `2026-10-17`, `{ ts_hour: 497842 }` → `2026-10-17-10`, `{ ts_month: 681 }` →
 * `2026-10`, a null value → `__null__`. Null for an unpartitioned table.
 */
export function partitionKey(partition: Record<string, number | null>, granularity: PartitionGranularity): string | null {
  if (granularity === 'none') return null;
  const value = partition[`ts_${granularity}`];
  if (value === null || value === undefined) return NULL_PARTITION;
  if (granularity === 'day') return new Date(value * 86_400_000).toISOString().slice(0, 10);
  if (granularity === 'month') {
    // Months since January 1970; `Math.floor` keeps months before it right.
    const year = 1970 + Math.floor(value / 12);
    return `${String(year).padStart(4, '0')}-${String(value - (year - 1970) * 12 + 1).padStart(2, '0')}`;
  }
  const moment = new Date(value * 3_600_000).toISOString();
  return `${moment.slice(0, 10)}-${moment.slice(11, 13)}`;
}

/** Test seam: forget every cached snapshot and manifest. */
export function resetSnapshotCaches(): void {
  manifestLists.clear();
  manifests.clear();
  snapshots.clear();
}
