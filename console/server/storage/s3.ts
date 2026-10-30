/**
 * An S3 profile: Amazon S3, or any S3-compatible server at `endpoint` (SeaweedFS on a test cluster).
 * Metadata objects and cached data files through `@aws-sdk/client-s3`, data files in place through
 * DuckDB's `s3` secret — with the profile's keys, or the default credential chain when it has none.
 *
 * The SDK's own retries are off: `withRetries` retries every backend alike. GetObject asks for the
 * object's checksum (`ChecksumMode`), which the SDK verifies as the body is read whenever the object
 * carries one.
 */

import type { Readable } from 'node:stream';

import { GetObjectCommand, S3Client, type GetObjectCommandOutput } from '@aws-sdk/client-s3';

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

/** The region DuckDB and the SDK sign for when a profile names none. */
export const DEFAULT_S3_REGION = 'us-east-1';

export interface S3ProfileOptions extends ProfileSeams {
  /** Test seam: read through this client instead of one made from the profile. */
  client?: S3Client;
}

export class S3Profile extends BaseProfile {
  private s3: S3Client | null;

  constructor(config: StorageProfileConfig, options: S3ProfileOptions = {}) {
    super(config, options);
    this.s3 = options.client ?? null;
  }

  private keys(): { accessKeyId: string; secretAccessKey: string; sessionToken?: string } | null {
    const accessKeyId = this.credential('ACCESS_KEY_ID');
    const secretAccessKey = this.credential('SECRET_ACCESS_KEY');
    if (!accessKeyId || !secretAccessKey) return null;
    const sessionToken = this.credential('SESSION_TOKEN');
    return sessionToken ? { accessKeyId, secretAccessKey, sessionToken } : { accessKeyId, secretAccessKey };
  }

  /** Always able to read in place: with keys, or with the chain. */
  get mode(): StorageMode {
    return this.config.access === 'cached' ? 'cached' : 'direct';
  }

  /**
   * `TYPE s3` with the keys, or `PROVIDER credential_chain` with `REFRESH 'auto'` (DuckDB 1.5.5 takes it
   * for the chain, and fetches new credentials when the ones it holds expire). An S3-compatible server's
   * `ENDPOINT` is `host:port` without the scheme — DuckDB adds `http://` itself — with `USE_SSL` from the
   * scheme and `URL_STYLE 'path'` as the profile says; AWS needs neither.
   */
  duckdbSecret(): string | null {
    if (this.mode !== 'direct') return null;
    const options = ['TYPE s3'];
    const keys = this.keys();
    if (keys) {
      options.push(secretOption('KEY_ID', keys.accessKeyId), secretOption('SECRET', keys.secretAccessKey));
      if (keys.sessionToken) options.push(secretOption('SESSION_TOKEN', keys.sessionToken));
    } else {
      options.push('PROVIDER credential_chain', secretOption('REFRESH', 'auto'));
    }
    options.push(secretOption('REGION', this.config.region ?? DEFAULT_S3_REGION));
    const endpoint = this.config.endpoint;
    if (endpoint) {
      options.push(secretOption('ENDPOINT', endpointHost(endpoint)), secretOption('USE_SSL', endpointUsesTls(endpoint)));
      if (this.config.pathStyle) options.push("URL_STYLE 'path'");
    }
    options.push(secretOption('SCOPE', this.scope));
    return `CREATE OR REPLACE TEMPORARY SECRET ${this.secretName} (${options.join(', ')})`;
  }

  /** The credential chain is the `aws` extension's. */
  duckdbExtensions(): string[] {
    return this.keys() ? ['httpfs'] : ['httpfs', 'aws'];
  }

  duckdbSource(uri: string): string {
    return uri;
  }

  duckdbScope(): string {
    return this.scope;
  }

  protected credentialsInUse(): string {
    const keys = this.keys();
    if (!keys) return 'the default credential chain';
    return keys.sessionToken ? 'an access key with a session token' : 'an access key';
  }

  protected credentialsFound(): boolean {
    return this.keys() !== null;
  }

  warnings(): string[] {
    const id = this.credential('ACCESS_KEY_ID');
    const secret = this.credential('SECRET_ACCESS_KEY');
    if (Boolean(id) === Boolean(secret)) return [];
    return [
      `storage profile ${this.name}: ${this.variable(id ? 'SECRET_ACCESS_KEY' : 'ACCESS_KEY_ID')} is not set, so its access key is incomplete; it reads with the default credential chain`,
    ];
  }

  /** Made once: the profile's keys, or the SDK's default chain when it has none. */
  private client(): S3Client {
    if (this.s3) return this.s3;
    this.s3 = new S3Client({
      region: this.config.region ?? DEFAULT_S3_REGION,
      endpoint: this.config.endpoint ?? undefined,
      forcePathStyle: this.config.pathStyle ?? false,
      maxAttempts: 1,
      credentials: this.keys() ?? undefined,
    });
    return this.s3;
  }

  private async getObject(uri: string, signal: AbortSignal): Promise<GetObjectCommandOutput> {
    const location = parseStorageUri(uri);
    if (!location || location.scheme !== 's3') throw new Error(`storage profile ${this.name} reads s3:// objects, not ${uri}`);
    try {
      return await this.client().send(new GetObjectCommand({ Bucket: location.container, Key: location.key, ChecksumMode: 'ENABLED' }), {
        abortSignal: signal,
      });
    } catch (cause) {
      throw readError(uri, cause);
    }
  }

  protected async read(uri: string): Promise<Buffer> {
    return withRetries(uri, METADATA_TIMEOUT_MS, async (signal) => {
      const response = await this.getObject(uri, signal);
      try {
        return Buffer.from((await response.Body?.transformToByteArray()) ?? new Uint8Array());
      } catch (cause) {
        throw readError(uri, cause);
      }
    });
  }

  protected async download(uri: string, target: string): Promise<void> {
    await withRetries(uri, DATA_FILE_TIMEOUT_MS, async (signal) => {
      const response = await this.getObject(uri, signal);
      try {
        await writeWhole(target, response.Body as Readable);
      } catch (cause) {
        throw readError(uri, cause);
      }
    });
  }
}

/** The SDK's failure as the console's: an HTTP answer by its status, a checksum by its kind. */
function readError(uri: string, cause: unknown): unknown {
  if (cause instanceof StorageReadError || cause instanceof ChecksumMismatchError) return cause;
  if (!(cause instanceof Error)) return cause;
  if (/checksum mismatch/i.test(cause.message)) return new ChecksumMismatchError(uri, 'S3');
  const status = (cause as { $metadata?: { httpStatusCode?: unknown } }).$metadata?.httpStatusCode;
  if (typeof status === 'number' && status >= 300) return new StorageReadError(uri, status, `${cause.name}: ${cause.message}`);
  return cause;
}
