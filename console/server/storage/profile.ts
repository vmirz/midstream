/**
 * A storage profile: how the console reads one destination prefix (its scope) — the SDK it reads
 * metadata objects and cached data files with, and the secret DuckDB reads data files in place with.
 * One profile per backend kind (`gcs.ts`, `s3.ts`, `azure.ts`); `registry.ts` picks the one whose scope
 * is the longest prefix of an object's URI, as DuckDB picks among scoped secrets.
 *
 * Credentials come from the environment, never from the registry file, and never leave the process:
 * not in a log, an answer or an error. A profile's description names its variables, not their values.
 */

import { randomUUID } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { rename, rm } from 'node:fs/promises';
import { Transform, type Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as sleep } from 'node:timers/promises';

import { logger } from '../logger.ts';
import { quoteLiteral } from '../data/sql.ts';
import type { StorageKind, StorageMode, StorageProfileView } from '../../shared/types.ts';

export type { StorageKind, StorageMode } from '../../shared/types.ts';

/** The URI scheme each kind serves. */
export const SCHEME_OF_KIND: Readonly<Record<StorageKind, string>> = { gcs: 'gs://', s3: 's3://', azure: 'abfss://' };

/** The credential variables each kind reads, by suffix: `CONSOLE_STORAGE_<NAME>_<SUFFIX>`. */
export const CREDENTIAL_SUFFIXES: Readonly<Record<StorageKind, readonly string[]>> = {
  gcs: ['GCS_HMAC_KEY_ID', 'GCS_HMAC_SECRET'],
  s3: ['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY', 'SESSION_TOKEN'],
  azure: ['AZURE_CONNECTION_STRING', 'AZURE_ACCOUNT_KEY', 'AZURE_CLIENT_ID', 'AZURE_TENANT_ID', 'AZURE_CLIENT_SECRET'],
};

/** `CONSOLE_STORAGE_<NAME>_<SUFFIX>`: the name in upper case, `-` as `_`. */
export function credentialVariable(name: string, suffix: string): string {
  return `CONSOLE_STORAGE_${name.toUpperCase().replace(/-/g, '_')}_${suffix}`;
}

/** One profile as the registry describes it, with its credentials as the environment holds them. */
export interface StorageProfileConfig {
  name: string;
  /** The destination prefix it serves, ending with `/`. */
  scope: string;
  kind: StorageKind;
  /** An S3-compatible server, an emulator: its URL, without a trailing slash. Null: the cloud itself. */
  endpoint: string | null;
  region: string | null;
  pathStyle: boolean | null;
  /** `cached` makes the console download data files even where DuckDB could read them in place. */
  access: StorageMode | null;
  /** gcs: a service-account key file; absent or missing, application default credentials. */
  keyFile: string | null;
  /** By variable suffix (`ACCESS_KEY_ID`), only the ones set. */
  credentials: Readonly<Record<string, string>>;
}

/** Test seams: read objects without any network. */
export interface ProfileSeams {
  readObject?: (uri: string) => Promise<Buffer>;
  downloadToFile?: (uri: string, target: string) => Promise<void>;
}

export interface StorageProfile {
  readonly name: string;
  readonly scope: string;
  readonly kind: StorageKind;
  readonly mode: StorageMode;
  /** The DuckDB secret's name: `console_<name>`, `-` as `_`. */
  readonly secretName: string;
  /** A metadata object, whole: retried on 408, 429, 5xx and failed connections; a 404 is final. */
  readObject(uri: string): Promise<Buffer>;
  /** A data file into `target`, whole or not at all. */
  downloadToFile(uri: string, target: string): Promise<void>;
  /** The statement creating the profile's DuckDB secret; null for a cached profile. */
  duckdbSecret(): string | null;
  /** The DuckDB extensions its secret and paths need. */
  duckdbExtensions(): string[];
  /** What DuckDB is given for a data file under the scope. */
  duckdbSource(uri: string): string;
  /** The scope as DuckDB matches it against sources: spelled as `duckdbSource` spells paths. */
  duckdbScope(): string;
  /** Everything the Settings page and the Connect panel may show: never a credential. */
  describe(): StorageProfileView;
  /** What is wrong with the profile's configuration, worth a warning at start. Names variables only. */
  warnings(): string[];
}

/** Answers object storage asks a reader to retry. */
const RETRYABLE_STATUSES = new Set([408, 429, 500, 502, 503, 504]);
/** Failed connections, as Node and the SDKs report them. */
const RETRYABLE_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'EAI_AGAIN', 'ENOTFOUND', 'UND_ERR_SOCKET', 'REQUEST_SEND_ERROR']);

/** Attempts at one read: the first, then retries 0.5 s and 1 s later. */
export const READ_ATTEMPTS = 3;
const RETRY_DELAY_MS = 500;

/** Longest one attempt may take: metadata objects are small, data files can be large. */
export const METADATA_TIMEOUT_MS = 30_000;
export const DATA_FILE_TIMEOUT_MS = 10 * 60_000;

/** A read storage answered with an error: `code` is the HTTP status, 404 when there is no such object. */
export class StorageReadError extends Error {
  readonly code: number;

  constructor(uri: string, status: number, detail: string) {
    super(`reading ${uri} failed with HTTP ${status}${detail ? `: ${detail}` : ''}`);
    this.name = 'StorageReadError';
    this.code = status;
  }
}

/** The bytes read do not have the checksum storage keeps for the object. */
export class ChecksumMismatchError extends Error {
  constructor(uri: string, kind: string) {
    super(`the bytes read from ${uri} do not match its stored ${kind} checksum`);
    this.name = 'ChecksumMismatchError';
  }
}

export function isRetryable(cause: unknown): boolean {
  if (cause instanceof StorageReadError) return RETRYABLE_STATUSES.has(cause.code);
  if (cause instanceof ChecksumMismatchError) return true;
  // fetch fails a refused, reset or cut-off connection with a TypeError, and a timeout as an abort.
  if (cause instanceof TypeError) return true;
  if (!(cause instanceof Error)) return false;
  if (cause.name === 'TimeoutError' || cause.name === 'AbortError') return true;
  const code = (cause as { code?: unknown }).code;
  return typeof code === 'string' && RETRYABLE_CODES.has(code);
}

/**
 * One read, attempted up to three times: each attempt has `timeoutMs`, and only what storage asks to
 * be retried, a failed connection or a checksum that does not hold is tried again.
 */
export async function withRetries<T>(uri: string, timeoutMs: number, attempt: (signal: AbortSignal) => Promise<T>): Promise<T> {
  for (let tries = 1; ; tries += 1) {
    try {
      return await attempt(AbortSignal.timeout(timeoutMs));
    } catch (cause) {
      if (tries >= READ_ATTEMPTS || !isRetryable(cause)) throw cause;
      logger.debug({ err: cause, uri, attempt: tries }, 'storage read failed; retrying');
    }
    await sleep(RETRY_DELAY_MS * 2 ** (tries - 1));
  }
}

/**
 * Write `body` to `target`, whole or not at all: beside it first, renamed into place once `verify`
 * holds, so a download cut short never leaves a part of a file the next scan would take for all of it.
 */
export async function writeWhole(target: string, body: Readable, observe?: (chunk: Buffer) => void, verify?: () => void): Promise<void> {
  const partial = `${target}.${randomUUID()}.part`;
  try {
    await pipeline(
      body,
      new Transform({
        transform(chunk: Buffer, _encoding, callback) {
          observe?.(chunk);
          callback(null, chunk);
        },
      }),
      createWriteStream(partial),
    );
    verify?.();
    await rename(partial, target);
  } catch (cause) {
    await rm(partial, { force: true });
    throw cause;
  }
}

/** `http://host:8333/` → `host:8333`, as DuckDB's ENDPOINT takes it: never with the scheme. */
export function endpointHost(endpoint: string): string {
  const url = new URL(endpoint);
  const path = url.pathname.replace(/\/+$/, '');
  return `${url.host}${path}`;
}

/** Whether an endpoint is reached over TLS. */
export function endpointUsesTls(endpoint: string): boolean {
  return new URL(endpoint).protocol === 'https:';
}

/** `name 'value'` pairs of a CREATE SECRET statement. */
export function secretOption(name: string, value: string | boolean): string {
  return `${name} ${typeof value === 'boolean' ? String(value) : quoteLiteral(value)}`;
}

/** What every profile shares: its identity, its credentials and the seams. */
export abstract class BaseProfile implements StorageProfile {
  readonly name: string;
  readonly scope: string;
  readonly kind: StorageKind;
  readonly secretName: string;
  protected readonly config: StorageProfileConfig;
  private readonly seams: ProfileSeams;

  constructor(config: StorageProfileConfig, seams: ProfileSeams = {}) {
    this.name = config.name;
    this.scope = config.scope;
    this.kind = config.kind;
    this.secretName = `console_${config.name.replace(/-/g, '_')}`;
    this.config = config;
    this.seams = seams;
  }

  abstract get mode(): StorageMode;
  abstract duckdbSecret(): string | null;
  abstract duckdbExtensions(): string[];
  abstract duckdbSource(uri: string): string;
  abstract duckdbScope(): string;
  /** What the console reads the profile with, in words: `access key`, `default credential chain`. */
  protected abstract credentialsInUse(): string;
  /** Whether the environment gave the profile credentials of its own. */
  protected abstract credentialsFound(): boolean;
  protected abstract read(uri: string): Promise<Buffer>;
  protected abstract download(uri: string, target: string): Promise<void>;

  readObject(uri: string): Promise<Buffer> {
    return this.seams.readObject ? this.seams.readObject(uri) : this.read(uri);
  }

  downloadToFile(uri: string, target: string): Promise<void> {
    return this.seams.downloadToFile ? this.seams.downloadToFile(uri, target) : this.download(uri, target);
  }

  warnings(): string[] {
    return [];
  }

  /** A credential the environment set, by suffix. */
  protected credential(suffix: string): string | null {
    return this.config.credentials[suffix] ?? null;
  }

  protected variable(suffix: string): string {
    return credentialVariable(this.name, suffix);
  }

  protected account(): string | null {
    return null;
  }

  describe(): StorageProfileView {
    return {
      name: this.name,
      kind: this.kind,
      scope: this.scope,
      endpoint: this.config.endpoint,
      region: this.config.region,
      pathStyle: this.config.pathStyle,
      mode: this.mode,
      account: this.account(),
      credentials: this.credentialsInUse(),
      credentialsFound: this.credentialsFound(),
      credentialVariables: CREDENTIAL_SUFFIXES[this.kind].map((suffix) => this.variable(suffix)),
    };
  }
}
