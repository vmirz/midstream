/**
 * The local mirror of cached profiles' data files: `<CONSOLE_PARQUET_CACHE_DIR>/<profile>/<container>/<key>`.
 *
 * Objects behind a table's pointer are immutable once committed, so a mirrored file is never
 * downloaded again; the cache evicts by age past its byte budget. Every path segment of the key is
 * kept as it is except empty, `.` and `..` segments, which are dropped, and null bytes, which are
 * removed: the join cannot leave the cache directory even for a name that passed the location check
 * (console 12 §12.6). Profile and container keep two destinations' objects apart.
 */

import { mkdir, readdir, rm, stat } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import { config } from '../config.ts';
import { badRequest } from '../errors.ts';
import { logger } from '../logger.ts';
import type { StorageProfile } from './profile.ts';
import { parseStorageUri } from './uris.ts';

/** A data file to mirror, and the profile it is read with. */
export interface MirrorTarget {
  uri: string;
  profile: StorageProfile;
}

const DOWNLOAD_CONCURRENCY = 6;

function safeSegment(segment: string): string {
  return segment.replace(/[\0]/g, '');
}

/** Where one object is mirrored. */
export function cachePathFor({ uri, profile }: MirrorTarget): string {
  const location = parseStorageUri(uri);
  if (!location) throw badRequest(`not an object in storage: ${uri}`);
  const container = safeSegment(location.container);
  // Null bytes are removed BEFORE empty/`.`/`..` segments are dropped: a segment like `.\0.` must not
  // survive the drop and then become `..` once the null byte is gone, which would escape the cache dir.
  const segments = location.key
    .split('/')
    .map(safeSegment)
    .filter((segment) => segment.length > 0 && segment !== '.' && segment !== '..');
  if (segments.length === 0 || !container || container === '.' || container === '..') {
    throw badRequest(`object name is not usable as a cache path: ${uri}`);
  }
  return join(config.parquetCacheDir, profile.name, container, ...segments);
}

/**
 * Mirror the objects into the local cache and return their paths, by URI. Already-cached objects
 * count towards the progress's total: the question is how much of a scan is on disk, not how much
 * this call downloaded.
 */
export async function ensureLocalCopies(
  targets: MirrorTarget[],
  options: { onProgress?: (done: number, total: number) => void } = {},
): Promise<Map<string, string>> {
  await mkdir(config.parquetCacheDir, { recursive: true });
  const resolved = new Map<string, string>();
  const missing: MirrorTarget[] = [];
  for (const target of targets) {
    const path = cachePathFor(target);
    resolved.set(target.uri, path);
    try {
      if ((await stat(path)).size > 0) continue;
    } catch {
      /* not cached yet */
    }
    missing.push(target);
  }

  let done = targets.length - missing.length;
  options.onProgress?.(done, targets.length);
  if (missing.length > 0) {
    let cursor = 0;
    const workers = Array.from({ length: Math.min(DOWNLOAD_CONCURRENCY, missing.length) }, async () => {
      while (cursor < missing.length) {
        const target = missing[cursor++];
        if (!target) return;
        const path = resolved.get(target.uri) as string;
        await mkdir(dirname(path), { recursive: true });
        await target.profile.downloadToFile(target.uri, path);
        done += 1;
        options.onProgress?.(done, targets.length);
      }
    });
    await Promise.all(workers);
    void evictCache();
  }
  return resolved;
}

/** Trim the cache back under its byte budget, oldest access first. */
async function evictCache(): Promise<void> {
  try {
    const entries: { path: string; size: number; atime: number }[] = [];
    const walk = async (dir: string): Promise<void> => {
      for (const entry of await readdir(dir, { withFileTypes: true })) {
        const path = join(dir, entry.name);
        if (entry.isDirectory()) await walk(path);
        else {
          const stats = await stat(path);
          entries.push({ path, size: stats.size, atime: stats.atimeMs });
        }
      }
    };
    await walk(config.parquetCacheDir);
    let total = entries.reduce((sum, entry) => sum + entry.size, 0);
    if (total <= config.parquetCacheMaxBytes) return;
    for (const entry of entries.sort((a, b) => a.atime - b.atime)) {
      if (total <= config.parquetCacheMaxBytes) break;
      await rm(entry.path, { force: true });
      total -= entry.size;
    }
  } catch (cause) {
    logger.warn({ err: cause }, 'parquet cache eviction failed');
  }
}
