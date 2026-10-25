/**
 * Reading a table's metadata objects: the pointer, metadata files, manifest lists and manifests.
 *
 * These are small and are read whole, through the console's own GCS credentials (ADC), whatever
 * the data access mode: cached mode mirrors data files because DuckDB cannot read `gs://` without
 * HMAC keys, but the console itself always can. A path that is not a `gs://` URI is read from
 * local disk, and tests swap the reader for one over checked-in fixtures (`setObjectReader`).
 */

import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import { downloadObject, parseGcsUri } from '../data/gcs.ts';
import { unavailable } from '../errors.ts';

export type ObjectReader = (uri: string) => Promise<Buffer>;

/** No object at that location: a table not created yet, or a file a newer commit removed. */
export class ObjectMissingError extends Error {
  readonly uri: string;

  constructor(uri: string) {
    super(`no object at ${uri}`);
    this.name = 'ObjectMissingError';
    this.uri = uri;
  }
}

async function readStoredObject(uri: string): Promise<Buffer> {
  try {
    if (uri.startsWith('gs://')) {
      const { bucket, prefix } = parseGcsUri(uri);
      return await downloadObject(bucket, prefix);
    }
    return await readFile(uri.startsWith('file://') ? fileURLToPath(uri) : uri);
  } catch (cause) {
    if (isNotFound(cause)) throw new ObjectMissingError(uri);
    throw cause;
  }
}

function isNotFound(cause: unknown): boolean {
  if (!cause || typeof cause !== 'object') return false;
  const record = cause as { code?: unknown };
  return record.code === 404 || record.code === 'ENOENT';
}

let reader: ObjectReader = readStoredObject;

/** Read one metadata object whole. */
export function readObject(uri: string): Promise<Buffer> {
  return reader(uri);
}

/** Test seam: read objects through `next`, or through storage again when null. */
export function setObjectReader(next: ObjectReader | null): void {
  reader = next ?? readStoredObject;
}

/**
 * Whether `uri` names an object under `location`.
 *
 * A table's pointer, metadata and manifests name every file the console reads for it, and an engine
 * that may commit to the table — or anyone who can write its bucket — chooses those names. The
 * console reads only what lies under the location the control plane recorded, so a table can never
 * make it read another bucket, another table, or a file on the console's own disk.
 */
export function isUnderLocation(uri: string, location: string): boolean {
  const base = location.replace(/\/+$/, '');
  if (!base || !uri.startsWith(`${base}/`)) return false;
  return !uri
    .slice(base.length + 1)
    .split('/')
    .some((segment) => segment === '..' || segment === '.');
}

/** `uri`, when it lies under `location`; otherwise a 503 naming what the table pointed at. */
export function assertUnderLocation(uri: string, location: string, what: string): string {
  if (isUnderLocation(uri, location)) return uri;
  throw unavailable(`the table at ${location} names ${what} outside its location (${uri}); the console reads only files under it`, {
    location,
    uri,
  });
}

/** `gs://bucket/a/b` + `c/d.json` → `gs://bucket/a/b/c/d.json`. */
export function joinLocation(location: string, path: string): string {
  return `${location.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}
