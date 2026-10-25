import { randomUUID } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { mkdir, readdir, rename, rm, stat } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type { ReadableStream as NodeReadableStream } from 'node:stream/web';
import { setTimeout as sleep } from 'node:timers/promises';

import { CRC32C, Storage } from '@google-cloud/storage';

import { config } from '../config.ts';
import { badRequest } from '../errors.ts';
import { logger } from '../logger.ts';

let storageClient: Storage | null = null;

function client(): Storage {
  storageClient ??= new Storage();
  return storageClient;
}

/** Test seam: read through `next`, or through a client made from the environment again when null. */
export function setStorageClient(next: Storage | null): void {
  storageClient = next;
}

/** Answers GCS asks a reader to retry, as the client library retries them. */
const RETRYABLE_STATUSES = new Set([408, 429, 500, 502, 503, 504]);

/** Attempts at one read: the first, then retries 0.5 s and 1 s later. */
const READ_ATTEMPTS = 3;
const RETRY_DELAY_MS = 500;

/** Longest one attempt may take: metadata objects are small, data files can be large. */
const METADATA_TIMEOUT_MS = 30_000;
const DATA_FILE_TIMEOUT_MS = 10 * 60_000;

/** A read GCS answered with an error: `code` is the HTTP status, 404 when there is no such object. */
export class GcsReadError extends Error {
  readonly code: number;

  constructor(uri: string, status: number, detail: string) {
    super(`reading ${uri} failed with HTTP ${status}${detail ? `: ${detail}` : ''}`);
    this.name = 'GcsReadError';
    this.code = status;
  }
}

/** The bytes read do not have the checksum GCS stored for the object. */
class ChecksumMismatchError extends Error {
  constructor(uri: string) {
    super(`the bytes read from ${uri} do not match its stored CRC32C checksum`);
    this.name = 'ChecksumMismatchError';
  }
}

/**
 * Read one object through the JSON API with the client library's endpoint and credentials, and hand
 * the answer to `consume`, retrying a failed attempt as the library would.
 *
 * Not the library's `download()`: its stream plumbing (teeny-request) gives every download a stream
 * with more listeners than Node allows by default, so it printed `MaxListenersExceededWarning` twice
 * for every object — every few seconds while a Data tab follows a table, whose pointer it re-reads.
 * Retried: 408, 429 and 5xx answers, failed or timed-out connections, and bytes that fail their
 * checksum. A custom endpoint (`STORAGE_EMULATOR_HOST`, an emulator) is called without credentials,
 * as the library calls it.
 */
async function readFromGcs<T>(
  bucket: string,
  name: string,
  timeoutMs: number,
  consume: (response: Response, uri: string) => Promise<T>,
): Promise<T> {
  const storage = client();
  const uri = `gs://${bucket}/${name}`;
  const url = `${storage.baseUrl}/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(name)}?alt=media`;
  const authenticate = !storage.customEndpoint || storage.useAuthWithCustomEndpoint === true;
  for (let attempt = 1; ; attempt += 1) {
    const headers = new Headers(authenticate ? await storage.authClient.getRequestHeaders(url) : undefined);
    // A table's pointer changes in place; never take a cached copy of it.
    headers.set('Cache-Control', 'no-store');
    try {
      const response = await fetch(url, { headers, signal: AbortSignal.timeout(timeoutMs) });
      if (!response.ok) throw new GcsReadError(uri, response.status, await errorDetail(response));
      return await consume(response, uri);
    } catch (cause) {
      if (attempt >= READ_ATTEMPTS || !isRetryable(cause)) throw cause;
      logger.debug({ err: cause, uri, attempt }, 'GCS read failed; retrying');
    }
    await sleep(RETRY_DELAY_MS * 2 ** (attempt - 1));
  }
}

function isRetryable(cause: unknown): boolean {
  if (cause instanceof GcsReadError) return RETRYABLE_STATUSES.has(cause.code);
  if (cause instanceof ChecksumMismatchError) return true;
  // fetch fails a refused, reset or cut-off connection with a TypeError, and a timeout as an abort.
  if (cause instanceof TypeError) return true;
  return cause instanceof Error && (cause.name === 'TimeoutError' || cause.name === 'AbortError');
}

/** GCS's own message for a refused read, from its JSON error body. */
async function errorDetail(response: Response): Promise<string> {
  const text = await response.text().catch(() => '');
  try {
    const message = (JSON.parse(text) as { error?: { message?: unknown } }).error?.message;
    if (typeof message === 'string') return message;
  } catch {
    /* not JSON */
  }
  return text.slice(0, 300);
}

/**
 * The CRC32C GCS stored for the object, when the bytes read are the stored bytes.
 *
 * Not when the object is stored compressed: fetch hands back the bytes decompressed, as the library
 * did, and the stored checksum is of the compressed ones. The library skipped those too.
 */
function storedChecksum(response: Response): string | null {
  if (response.headers.get('x-goog-stored-content-encoding') !== 'identity') return null;
  for (const entry of (response.headers.get('x-goog-hash') ?? '').split(',')) {
    const [kind, value] = entry.trim().split(/=(.*)/s);
    if (kind === 'crc32c' && value) return value;
  }
  return null;
}

function verifyChecksum(response: Response, computed: CRC32C, uri: string): void {
  const expected = storedChecksum(response);
  if (expected !== null && !computed.validate(expected)) throw new ChecksumMismatchError(uri);
}

export function parseGcsUri(uri: string): { bucket: string; prefix: string } {
  if (!uri.startsWith('gs://')) {
    throw badRequest(`only gs:// destinations can be queried, received "${uri}"`);
  }
  const remainder = uri.slice('gs://'.length);
  const separator = remainder.indexOf('/');
  const bucket = separator === -1 ? remainder : remainder.slice(0, separator);
  const prefix = separator === -1 ? '' : remainder.slice(separator + 1).replace(/\/+$/, '');
  if (!bucket) throw badRequest(`invalid GCS URI: ${uri}`);
  return { bucket, prefix };
}

/** A data file to mirror: its URI, and its name within the bucket. */
export interface MirrorTarget {
  uri: string;
  name: string;
}

/** Local cache path for one object, mirroring its name within the bucket. */
function localPathFor(file: MirrorTarget): string {
  const { bucket } = parseGcsUri(file.uri);
  const segments = file.name
    .split('/')
    .filter((segment) => segment.length > 0 && segment !== '.' && segment !== '..')
    .map((segment) => segment.replace(/[\0]/g, ''));
  if (segments.length === 0) {
    throw badRequest(`object name is not usable as a cache path: ${file.name}`);
  }
  return join(config.parquetCacheDir, bucket, ...segments);
}

/**
 * Mirror the requested objects into the local cache and return their paths.
 *
 * Used when no GCS HMAC credentials are configured, so DuckDB cannot read `gs://` directly.
 * Objects are immutable once committed, so a cached file is never re-downloaded.
 */
export async function ensureLocalCopies(
  files: MirrorTarget[],
  options: { onProgress?: (done: number, total: number) => void } = {},
): Promise<Map<string, string>> {
  await mkdir(config.parquetCacheDir, { recursive: true });
  const resolved = new Map<string, string>();

  const missing: MirrorTarget[] = [];
  for (const file of files) {
    const target = localPathFor(file);
    resolved.set(file.uri, target);
    try {
      const stats = await stat(target);
      if (stats.size > 0) continue;
    } catch {
      /* not cached yet */
    }
    missing.push(file);
  }

  // Already-cached objects count toward the denominator: from the caller's point of view the
  // question is "how much of this scan is on disk", not "how much did this call download".
  let done = files.length - missing.length;
  options.onProgress?.(done, files.length);

  if (missing.length > 0) {
    let cursor = 0;
    const workers = Array.from({ length: Math.min(6, missing.length) }, async () => {
      while (cursor < missing.length) {
        const file = missing[cursor++];
        if (!file) return;
        const target = localPathFor(file);
        const { bucket } = parseGcsUri(file.uri);
        await mkdir(dirname(target), { recursive: true });
        await downloadToFile(bucket, file.name, target);
        done += 1;
        options.onProgress?.(done, files.length);
      }
    });
    await Promise.all(workers);
    void evictCache();
  }

  return resolved;
}

/** Trim the parquet cache back under its byte budget, oldest access first. */
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

/**
 * Write one object to `target`, whole or not at all.
 *
 * Written beside it and renamed into place once its checksum holds, so a download cut short never
 * leaves a part of a file that the next scan would take for the whole of it.
 */
async function downloadToFile(bucket: string, name: string, target: string): Promise<void> {
  await readFromGcs(bucket, name, DATA_FILE_TIMEOUT_MS, async (response, uri) => {
    const partial = `${target}.${randomUUID()}.part`;
    const checksum = new CRC32C();
    try {
      await pipeline(
        Readable.fromWeb((response.body ?? new Blob().stream()) as NodeReadableStream<Uint8Array>),
        new Transform({
          transform(chunk: Buffer, _encoding, callback) {
            checksum.update(chunk);
            callback(null, chunk);
          },
        }),
        createWriteStream(partial),
      );
      verifyChecksum(response, checksum, uri);
      await rename(partial, target);
    } catch (cause) {
      await rm(partial, { force: true });
      throw cause;
    }
  });
}

/** Download one object's bytes into memory: the table's pointer, metadata files and manifests. */
export async function downloadObject(bucket: string, name: string): Promise<Buffer> {
  return readFromGcs(bucket, name, METADATA_TIMEOUT_MS, async (response, uri) => {
    const contents = Buffer.from(await response.arrayBuffer());
    const checksum = new CRC32C();
    checksum.update(contents);
    verifyChecksum(response, checksum, uri);
    return contents;
  });
}
