/**
 * Where DuckDB reads a table's data files from.
 *
 * Data files are immutable — the runtime uploads with `if_generation_match=0`, and a file is never
 * rewritten, only replaced in a later snapshot — so anything learned about one can be kept for the
 * life of the process. That is what makes per-file reads cheap enough to page a large table one
 * file at a time.
 */

import { config, dataAccessMode } from '../config.ts';
import { badRequest } from '../errors.ts';
import { ensureLocalCopies } from './gcs.ts';
import { runQuery } from './duckdb.ts';
import { quoteLiteral } from './sql.ts';
import type { DataObject } from '../iceberg/snapshot.ts';

/** Maps a data file's URI to a local path DuckDB can open; tests use it to serve fixtures. */
export type ObjectPathResolver = (uri: string) => string | null;

let resolver: ObjectPathResolver | null = null;

/** Test seam: read data files from wherever `next` says, or from storage again when null. */
export function setObjectPathResolver(next: ObjectPathResolver | null): void {
  resolver = next;
}

/**
 * Where DuckDB should read each object from: `gs://` in place, or a local mirror in cached mode.
 *
 * Cached mode exists for deployments without HMAC credentials, where DuckDB cannot read object
 * storage itself. An object is downloaded once and kept; the cache evicts by age past its budget.
 */
export async function objectSources(objects: DataObject[]): Promise<Map<string, string>> {
  const sources = new Map<string, string>();
  const remote: DataObject[] = [];
  for (const object of objects) {
    const local = resolver?.(object.uri) ?? null;
    // Only object storage needs mirroring; anything else is already a path DuckDB can open.
    if (local !== null) sources.set(object.uri, local);
    else if (dataAccessMode() === 'direct' || !object.uri.startsWith('gs://')) sources.set(object.uri, object.uri);
    else remote.push(object);
  }
  if (remote.length > 0) {
    for (const [uri, path] of await ensureLocalCopies(remote.map(({ uri, name }) => ({ uri, name })))) sources.set(uri, path);
  }
  return sources;
}

export async function objectSource(object: DataObject): Promise<string> {
  return (await objectSources([object])).get(object.uri) ?? object.uri;
}

/**
 * Refuse a scan cached mode would have to download wholesale.
 *
 * Only scans across many objects can hit it; a page reads one object at a time.
 */
export function assertWithinCachedBudget(candidates: DataObject[]): void {
  if (dataAccessMode() !== 'cached') return;
  const objects = candidates.filter((object) => object.uri.startsWith('gs://') && resolver?.(object.uri) == null);
  const bytes = objects.reduce((sum, object) => sum + object.bytes, 0);
  if (objects.length > config.cachedModeMaxFiles || bytes > config.cachedModeMaxBytes) {
    throw badRequest(
      `This time range covers ${objects.length.toLocaleString('en-US')} files (${Math.round(bytes / 1e6).toLocaleString('en-US')} MB), ` +
        `more than this console can download for one query. Narrow the time range, or configure GCS ` +
        `HMAC credentials so files are read in place.`,
      { objects: objects.length, bytes },
    );
  }
}

/** Read one data file's footer into DuckDB's metadata cache, so the next scan of it starts at once. */
export async function warmFooter(source: string): Promise<void> {
  await runQuery(`DESCRIBE SELECT * FROM read_parquet(${quoteLiteral(source)}, hive_partitioning = false)`);
}
