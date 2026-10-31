/**
 * Where DuckDB reads a table's data files from.
 *
 * Every object resolves to the storage profile whose scope covers it (`storage/registry.ts`): a
 * direct profile's objects are read in place, by the path DuckDB is given for them
 * (`duckdbSource`, the `az://` form for Azure), with the profile's secret; a cached profile's are
 * mirrored to local disk first. An object no profile covers never reaches DuckDB — it would send an
 * uncovered `s3://` path anonymously to AWS — and is refused with `storage_not_configured`.
 *
 * Data files are immutable — written create-only, never rewritten, only replaced in a later
 * snapshot — so anything learned about one can be kept for the life of the process. That is what
 * makes per-file reads cheap enough to page a large table one file at a time.
 */

import { config } from '../config.ts';
import { badRequest } from '../errors.ts';
import { ensureLocalCopies, type MirrorTarget } from '../storage/cache.ts';
import type { StorageProfile } from '../storage/profile.ts';
import { storage } from '../storage/registry.ts';
import { isLocalUri } from '../storage/uris.ts';
import { prepareStorageAccess, runQuery } from './duckdb.ts';
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
 * Where DuckDB should read each object from: in place for a direct profile, from the local mirror for
 * a cached one, as it is for a local file (the live tail's, in the run's directory). Before any path
 * is handed out, the direct profiles' secrets exist on the shared instance.
 */
export async function objectSources(objects: DataObject[]): Promise<Map<string, string>> {
  const sources = new Map<string, string>();
  const mirror: MirrorTarget[] = [];
  const direct = new Set<StorageProfile>();
  for (const object of objects) {
    const local = resolver?.(object.uri) ?? null;
    if (local !== null) {
      sources.set(object.uri, local);
      continue;
    }
    if (isLocalUri(object.uri)) {
      sources.set(object.uri, object.uri);
      continue;
    }
    const profile = storage().profileFor(object.uri);
    if (profile.mode === 'direct') {
      sources.set(object.uri, profile.duckdbSource(object.uri));
      direct.add(profile);
    } else {
      mirror.push({ uri: object.uri, profile });
    }
  }
  if (direct.size > 0) await prepareStorageAccess([...direct]);
  if (mirror.length > 0) {
    for (const [uri, path] of await ensureLocalCopies(mirror)) sources.set(uri, path);
  }
  return sources;
}

export async function objectSource(object: DataObject): Promise<string> {
  const source = (await objectSources([object])).get(object.uri);
  if (source === undefined) throw badRequest(`no source for ${object.uri}`);
  return source;
}

/**
 * Refuse a scan that would download more of cached profiles' files than one query may.
 *
 * Only scans across many objects can hit it; a page reads one object at a time. Direct profiles'
 * objects, local files and objects no profile covers (refused when they are read) do not count.
 */
export function assertWithinCachedBudget(candidates: DataObject[]): void {
  const profiles = new Set<StorageProfile>();
  let files = 0;
  let bytes = 0;
  for (const object of candidates) {
    if (resolver?.(object.uri) != null || isLocalUri(object.uri)) continue;
    const profile = storage().find(object.uri);
    if (!profile || profile.mode !== 'cached') continue;
    profiles.add(profile);
    files += 1;
    bytes += object.bytes;
  }
  if (files > config.cachedModeMaxFiles || bytes > config.cachedModeMaxBytes) {
    const names = [...profiles].map((profile) => profile.name);
    const named = names.length === 1 ? `profile ${names[0]}` : `profiles ${names.slice(0, -1).join(', ')} and ${names.at(-1)}`;
    throw badRequest(
      `This time range covers ${files.toLocaleString('en-US')} files (${Math.round(bytes / 1e6).toLocaleString('en-US')} MB), ` +
        `more than this console can download for one query. Narrow the time range, or configure direct access for ` +
        `storage ${named} so files are read in place.`,
      { objects: files, bytes, profiles: names },
    );
  }
}

/**
 * The path DuckDB reads one object by, when its profile reads in place, with the profile's secret in
 * place; null for anything else (cached, local, or no profile at all).
 */
export async function directSource(uri: string): Promise<string | null> {
  if (isLocalUri(uri)) return null;
  const profile = storage().find(uri);
  if (!profile || profile.mode !== 'direct') return null;
  await prepareStorageAccess([profile]);
  return profile.duckdbSource(uri);
}

/** Read one data file's footer into DuckDB's metadata cache, so the next scan of it starts at once. */
export async function warmFooter(source: string): Promise<void> {
  await runQuery(`DESCRIBE SELECT * FROM read_parquet(${quoteLiteral(source)}, hive_partitioning = false)`);
}
