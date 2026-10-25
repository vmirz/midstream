/**
 * A materialization's table as the console last read it: pointer → metadata file.
 *
 * Reading the pointer is the only thing that can tell the console there is newer data, so it is
 * what freshness is about. A run asks for a `fresh` table — a pointer read no older than
 * `snapshotCacheMs` — so it includes whatever was committed moments before Run was pressed. The
 * summary and estimates the Data tab polls are served from a read up to `tableMetadataFreshMs`
 * old, and a stale one, up to `catalogMaxStaleMs`, is returned at once while a fresh read happens
 * in the background. Metadata files never change once written, so each one is parsed once.
 */

import { config } from '../config.ts';
import { unavailable } from '../errors.ts';
import { logger } from '../logger.ts';
import { assertUnderLocation } from './io.ts';
import { readMetadata, type TableMetadata } from './metadata.ts';
import { readPointer, type TablePointer } from './pointer.ts';

export interface LoadedTable {
  location: string;
  pointer: TablePointer;
  metadataLocation: string;
  metadata: TableMetadata;
  /** When the pointer was read, epoch milliseconds. */
  readAt: number;
}

interface Entry {
  /** Null once read: the table has no pointer (not created yet, or dropped). */
  table: LoadedTable | null;
  readAt: number;
  refreshing: Promise<LoadedTable | null> | null;
}

const tables = new Map<string, Entry>();
const metadataFiles = new Map<string, Promise<TableMetadata>>();
const METADATA_CACHE_LIMIT = 256;

/** The table at `location`, or null when it has no pointer. */
export async function loadTable(location: string, options: { fresh?: boolean } = {}): Promise<LoadedTable | null> {
  let entry = tables.get(location);
  if (!entry) {
    entry = { table: null, readAt: Number.NEGATIVE_INFINITY, refreshing: null };
    tables.set(location, entry);
  }
  const age = Date.now() - entry.readAt;
  const read = entry.readAt !== Number.NEGATIVE_INFINITY;

  if (options.fresh) {
    if (read && age <= config.snapshotCacheMs) return entry.table;
    return refresh(location, entry);
  }
  if (read && age <= config.tableMetadataFreshMs) return entry.table;
  if (read && age <= config.catalogMaxStaleMs) {
    void refresh(location, entry).catch((cause) => logger.warn({ err: cause, location }, 'table refresh failed'));
    return entry.table;
  }
  return refresh(location, entry);
}

function refresh(location: string, entry: Entry): Promise<LoadedTable | null> {
  entry.refreshing ??= (async () => {
    const readAt = Date.now();
    const pointer = await readPointer(location);
    const table = pointer ? { location, pointer, metadataLocation: pointer.metadataLocation, metadata: await confinedMetadata(location, pointer), readAt } : null;
    entry.table = table;
    entry.readAt = readAt;
    return table;
  })().finally(() => {
    entry.refreshing = null;
  });
  return entry.refreshing;
}

/** The metadata file the pointer names, when it and the table it describes are at `location`. */
async function confinedMetadata(location: string, pointer: TablePointer): Promise<TableMetadata> {
  const metadata = await metadataAt(assertUnderLocation(pointer.metadataLocation, location, 'its metadata'));
  const trim = (value: string): string => value.replace(/\/+$/, '');
  if (trim(metadata.location) !== trim(location)) {
    throw unavailable(`the table at ${location} says it is at ${metadata.location}; the console reads only files under ${location}`, {
      location,
    });
  }
  return metadata;
}

/** One metadata file, parsed once. */
export function metadataAt(metadataLocation: string): Promise<TableMetadata> {
  const hit = metadataFiles.get(metadataLocation);
  if (hit) return hit;
  const loading = readMetadata(metadataLocation);
  metadataFiles.set(metadataLocation, loading);
  loading.catch(() => metadataFiles.delete(metadataLocation));
  while (metadataFiles.size > METADATA_CACHE_LIMIT) metadataFiles.delete(metadataFiles.keys().next().value as string);
  return loading;
}

/** Test seam: forget every table read. */
export function resetTables(): void {
  tables.clear();
  metadataFiles.clear();
}
