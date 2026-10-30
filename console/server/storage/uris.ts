/**
 * Object locations, one form per backend (design 4.7): `gs://bucket/key`, `s3://bucket/key` and
 * `abfss://container@account.dfs.core.windows.net/key`. A table's location and every path its
 * metadata names are in one of these forms, or a local path (`/…` or `file://…`) — the test fixtures.
 *
 * Anything else with a scheme is neither: no storage profile can serve it, so it is refused, never
 * read from disk and never handed to DuckDB.
 */

export type StorageScheme = 'gs' | 's3' | 'abfss';

/** One object (or prefix) in object storage. */
export interface StorageLocation {
  scheme: StorageScheme;
  /** The bucket, or for Azure the container. */
  container: string;
  /** Everything after the container's `/`: empty for the container itself; a prefix ends with `/`. */
  key: string;
  /** Azure only: the storage account. */
  account?: string;
}

/** The DNS suffix of an Azure storage account's Data Lake endpoint, the only Azure form accepted. */
export const AZURE_DFS_SUFFIX = '.dfs.core.windows.net';

const SCHEME = /^([a-z][a-z0-9+.-]*):\/\//i;
const BUCKET = /^[^/@\s]+$/;
const AZURE_AUTHORITY = /^([^/@\s]+)@([a-z0-9]+)\.dfs\.core\.windows\.net$/;

/** Whether `uri` is a path on the console's own disk rather than an object in storage. */
export function isLocalUri(uri: string): boolean {
  return uri.startsWith('file://') || !SCHEME.test(uri);
}

/**
 * `uri` as a location, or null when it is local or in no form of the three. A container of `.` or
 * `..` is no container: it could never be one, and it must never become a path segment.
 */
export function parseStorageUri(uri: string): StorageLocation | null {
  const scheme = SCHEME.exec(uri)?.[1];
  if (scheme !== 'gs' && scheme !== 's3' && scheme !== 'abfss') return null;
  const rest = uri.slice(scheme.length + 3);
  const slash = rest.indexOf('/');
  const authority = slash === -1 ? rest : rest.slice(0, slash);
  const key = slash === -1 ? '' : rest.slice(slash + 1);
  if (scheme === 'abfss') {
    const match = AZURE_AUTHORITY.exec(authority);
    if (!match) return null;
    const container = match[1] as string;
    if (container === '.' || container === '..') return null;
    return { scheme, container, key, account: match[2] as string };
  }
  if (!BUCKET.test(authority) || authority === '.' || authority === '..') return null;
  return { scheme, container: authority, key };
}

/** The canonical URI of a location: the inverse of `parseStorageUri`. */
export function formatStorageUri(location: StorageLocation): string {
  if (location.scheme === 'abfss') {
    return `abfss://${location.container}@${location.account ?? ''}${AZURE_DFS_SUFFIX}/${location.key}`;
  }
  return `${location.scheme}://${location.container}/${location.key}`;
}

/** `s3://bucket/a/b` + `c/d.json` → `s3://bucket/a/b/c/d.json`, whatever the scheme. */
export function joinStorageUri(base: string, path: string): string {
  return `${base.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}

/** An object's key within its container: what a cache path mirrors. Local paths are their own. */
export function objectKey(uri: string): string {
  return parseStorageUri(uri)?.key ?? uri;
}
