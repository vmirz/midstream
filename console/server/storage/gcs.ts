/**
 * A GCS profile: metadata objects and cached data files through GCS's JSON API, data files in place
 * through DuckDB with an HMAC key pair.
 *
 * Objects are read with a plain GET (`?alt=media`) with the client library's endpoint and credentials
 * — the profile's key file when it exists, else application default credentials — and, as the
 * library does, without credentials for a custom endpoint (an emulator). Not the library's
 * `download()`: its stream plumbing (teeny-request) gives every download a stream with more
 * listeners than Node allows by default, so it printed `MaxListenersExceededWarning` twice for every
 * object — every few seconds while a Data tab follows a table, whose pointer it re-reads. Every read
 * is checked against the CRC32C GCS stored.
 */

import { existsSync } from 'node:fs';
import { Readable } from 'node:stream';
import type { ReadableStream as NodeReadableStream } from 'node:stream/web';

import { CRC32C, Storage, type StorageOptions } from '@google-cloud/storage';

import { logger } from '../logger.ts';
import {
  BaseProfile,
  ChecksumMismatchError,
  DATA_FILE_TIMEOUT_MS,
  METADATA_TIMEOUT_MS,
  StorageReadError,
  endpointHost,
  endpointUsesTls,
  secretOption,
  withRetries,
  writeWhole,
  type ProfileSeams,
  type StorageMode,
  type StorageProfileConfig,
} from './profile.ts';
import { parseStorageUri } from './uris.ts';

/**
 * A GCS HMAC access ID: `GOOG1E…` for a service account, `GOOG…` for a user. Deliberately permissive
 * about length — Google has shipped more than one size — but the prefix is fixed, and it is the part
 * people get wrong (a service-account email or a project id pasted in its place).
 */
const HMAC_KEY_ID_PATTERN = /^GOOG[0-9A-Z]{16,}$/;
/** A GCS HMAC secret is 40 characters of base64. */
const HMAC_SECRET_LENGTH = 40;

export interface GcsProfileOptions extends ProfileSeams {
  /** Test seam: read through this client instead of one made from the profile. */
  client?: Storage;
}

export class GcsProfile extends BaseProfile {
  private storageClient: Storage | null;

  constructor(config: StorageProfileConfig, options: GcsProfileOptions = {}) {
    super(config, options);
    this.storageClient = options.client ?? null;
  }

  private hmacPair(): { keyId: string; secret: string } | null {
    const keyId = this.credential('GCS_HMAC_KEY_ID');
    const secret = this.credential('GCS_HMAC_SECRET');
    return keyId && secret ? { keyId, secret } : null;
  }

  /** Direct with an HMAC key pair, which only the deployment can give; otherwise cached. */
  get mode(): StorageMode {
    return this.config.access !== 'cached' && this.hmacPair() ? 'direct' : 'cached';
  }

  duckdbSecret(): string | null {
    const pair = this.hmacPair();
    if (this.mode !== 'direct' || !pair) return null;
    const options = ['TYPE gcs', secretOption('KEY_ID', pair.keyId), secretOption('SECRET', pair.secret)];
    const endpoint = this.config.endpoint;
    if (endpoint) {
      // An emulator, or a private endpoint: reached by path, as DuckDB cannot resolve bucket hosts there.
      options.push(secretOption('ENDPOINT', endpointHost(endpoint)), secretOption('USE_SSL', endpointUsesTls(endpoint)), "URL_STYLE 'path'");
    }
    options.push(secretOption('SCOPE', this.scope));
    return `CREATE OR REPLACE TEMPORARY SECRET ${this.secretName} (${options.join(', ')})`;
  }

  duckdbExtensions(): string[] {
    return ['httpfs'];
  }

  duckdbSource(uri: string): string {
    return uri;
  }

  duckdbScope(): string {
    return this.scope;
  }

  protected credentialsInUse(): string {
    const identity = this.config.endpoint
      ? 'no credentials (a custom endpoint)'
      : this.keyFileExists()
        ? 'its key file'
        : 'application default credentials';
    return `${identity}; ${this.hmacPair() ? 'an HMAC key pair for DuckDB' : 'no HMAC key pair, so data files are downloaded'}`;
  }

  protected credentialsFound(): boolean {
    return this.keyFileExists() || this.hmacPair() !== null;
  }

  private keyFileExists(): boolean {
    return this.config.keyFile !== null && existsSync(this.config.keyFile);
  }

  warnings(): string[] {
    const warnings: string[] = [];
    const keyId = this.credential('GCS_HMAC_KEY_ID');
    const secret = this.credential('GCS_HMAC_SECRET');
    if (Boolean(keyId) !== Boolean(secret)) {
      warnings.push(
        `storage profile ${this.name}: ${this.variable(keyId ? 'GCS_HMAC_SECRET' : 'GCS_HMAC_KEY_ID')} is not set, so its HMAC key pair is incomplete and data files are downloaded`,
      );
    }
    if (keyId && !HMAC_KEY_ID_PATTERN.test(keyId)) {
      warnings.push(
        `storage profile ${this.name}: ${this.variable('GCS_HMAC_KEY_ID')} does not look like a GCS HMAC access ID (GOOG1E…); it is not a service account email or a key file. ` +
          'Create one with `gcloud storage hmac create <service-account-email>`; every gs:// scan fails until it is corrected',
      );
    }
    if (secret && secret.length !== HMAC_SECRET_LENGTH) {
      warnings.push(
        `storage profile ${this.name}: ${this.variable('GCS_HMAC_SECRET')} is not ${HMAC_SECRET_LENGTH} characters long, as a GCS HMAC secret is; every gs:// scan fails to authenticate until it is corrected`,
      );
    }
    if (this.config.access === 'direct' && !this.hmacPair()) {
      warnings.push(`storage profile ${this.name}: access is direct, but DuckDB needs an HMAC key pair to read gs:// in place; data files are downloaded`);
    }
    if (this.config.keyFile && !this.keyFileExists()) {
      warnings.push(`storage profile ${this.name}: its key file ${this.config.keyFile} does not exist; it reads with application default credentials`);
    }
    return warnings;
  }

  /** What to check when GCS refuses DuckDB's signature: this profile's variables, never their values. */
  hmacAdvice(): string {
    const shape = this.warnings().find((warning) => warning.includes('GCS_HMAC'));
    if (shape) return `${shape.replace(`storage profile ${this.name}: `, '')}.`;
    return (
      `Check that ${this.variable('GCS_HMAC_KEY_ID')} and ${this.variable('GCS_HMAC_SECRET')} are a matching, active GCS HMAC ` +
      `key pair whose service account can read ${this.scope}.`
    );
  }

  private client(): Storage {
    if (this.storageClient) return this.storageClient;
    const options: StorageOptions = {};
    if (this.config.endpoint) options.apiEndpoint = this.config.endpoint;
    else if (this.keyFileExists()) options.keyFilename = this.config.keyFile as string;
    else logger.info({ profile: this.name, keyFile: this.config.keyFile }, 'storage profile reads GCS with application default credentials');
    this.storageClient = new Storage(options);
    return this.storageClient;
  }

  /**
   * One GET of the JSON API, retried as the library would retry it; `consume` reads the answer.
   * A custom endpoint (an emulator) is called without credentials, as the library calls it.
   */
  private async get<T>(uri: string, timeoutMs: number, consume: (response: Response) => Promise<T>): Promise<T> {
    const location = parseStorageUri(uri);
    if (!location || location.scheme !== 'gs') throw new Error(`storage profile ${this.name} reads gs:// objects, not ${uri}`);
    const storage = this.client();
    const url = `${storage.baseUrl}/b/${encodeURIComponent(location.container)}/o/${encodeURIComponent(location.key)}?alt=media`;
    const authenticate = !storage.customEndpoint || storage.useAuthWithCustomEndpoint === true;
    return withRetries(uri, timeoutMs, async (signal) => {
      const headers = new Headers(authenticate ? await storage.authClient.getRequestHeaders(url) : undefined);
      // A table's pointer changes in place; never take a cached copy of it.
      headers.set('Cache-Control', 'no-store');
      const response = await fetch(url, { headers, signal });
      if (!response.ok) throw new StorageReadError(uri, response.status, await errorDetail(response));
      return consume(response);
    });
  }

  protected async read(uri: string): Promise<Buffer> {
    return this.get(uri, METADATA_TIMEOUT_MS, async (response) => {
      const contents = Buffer.from(await response.arrayBuffer());
      const checksum = new CRC32C();
      checksum.update(contents);
      verifyChecksum(response, checksum, uri);
      return contents;
    });
  }

  protected async download(uri: string, target: string): Promise<void> {
    await this.get(uri, DATA_FILE_TIMEOUT_MS, async (response) => {
      const checksum = new CRC32C();
      await writeWhole(
        target,
        Readable.fromWeb((response.body ?? new Blob().stream()) as NodeReadableStream<Uint8Array>),
        (chunk) => checksum.update(chunk),
        () => verifyChecksum(response, checksum, uri),
      );
    });
  }
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
  if (expected !== null && !computed.validate(expected)) throw new ChecksumMismatchError(uri, 'CRC32C');
}
