/**
 * An Azure Blob Storage profile: metadata objects and cached data files through
 * `@azure/storage-blob`, data files in place through DuckDB's `azure` secret.
 *
 * Locations keep the one Azure form everywhere — `abfss://<container>@<account>.dfs.core.windows.net/…`
 * (design 4.7) — except where they are handed to DuckDB: there every path, scope and allowed path is
 * `az://<account>.blob.core.windows.net/<container>/…`, spelled identically, because DuckDB sends that
 * form to the connection string's BlobEndpoint when it has one and to the account's own blob host
 * otherwise, and checks the account it names against the connection string's. The short `az://c/k`
 * form could not tell two accounts apart.
 *
 * DuckDB 1.5.5's `config` provider takes a CONNECTION_STRING and refuses ACCOUNT_KEY, SAS_TOKEN and
 * BLOB_ENDPOINT, so an account key reaches DuckDB inside a connection string the console builds, with
 * the emulator's endpoint as its BlobEndpoint. The secret's ENDPOINT attribute is a DNS suffix for
 * sovereign clouds, always https, and is never used for an emulator.
 */

import { createHash } from 'node:crypto';
import type { Readable } from 'node:stream';

import { ClientSecretCredential, DefaultAzureCredential, type TokenCredential } from '@azure/identity';
import { BlobServiceClient, StorageSharedKeyCredential, type BlobDownloadResponseParsed, type StoragePipelineOptions } from '@azure/storage-blob';

import {
  BaseProfile,
  ChecksumMismatchError,
  DATA_FILE_TIMEOUT_MS,
  METADATA_TIMEOUT_MS,
  StorageReadError,
  secretOption,
  withRetries,
  writeWhole,
  type ProfileSeams,
  type StorageMode,
  type StorageProfileConfig,
} from './profile.ts';
import { parseStorageUri, type StorageLocation } from './uris.ts';

/** `abfss://c@a.dfs.core.windows.net/k` → `az://a.blob.core.windows.net/c/k`: what DuckDB is given. */
export function azurePathForDuckdb(uri: string): string {
  const location = parseStorageUri(uri);
  if (!location || location.scheme !== 'abfss') throw new Error(`not an abfss:// location: ${uri}`);
  return `az://${location.account}.blob.core.windows.net/${location.container}/${location.key}`;
}

/** One `Key=Value` pair per entry, keys compared without case, in their order. */
function connectionStringParts(text: string): Array<[string, string]> {
  return text
    .split(';')
    .filter((part) => part.trim().length > 0)
    .map((part) => {
      const at = part.indexOf('=');
      return at === -1 ? [part.trim(), ''] : [part.slice(0, at).trim(), part.slice(at + 1)];
    });
}

function connectionStringValue(text: string, key: string): string | null {
  return connectionStringParts(text).find(([name]) => name.toLowerCase() === key.toLowerCase())?.[1] ?? null;
}

/**
 * Why an Azure profile cannot work as configured, or null: a connection string naming another account
 * than the scope reads objects of the wrong account, and DuckDB refuses it outright.
 */
export function azureConfigurationProblem(config: StorageProfileConfig, variable: string): string | null {
  const text = config.credentials.AZURE_CONNECTION_STRING;
  if (!text) return null;
  const named = connectionStringValue(text, 'AccountName');
  const account = parseStorageUri(config.scope)?.account;
  if (named !== null && account && named !== account) {
    return `${variable} names another storage account than the profile's scope (${account})`;
  }
  return null;
}

let defaultCredential: TokenCredential | null = null;

/** `DefaultAzureCredential`, made once for the process. */
function defaultAzureCredential(): TokenCredential {
  defaultCredential ??= new DefaultAzureCredential();
  return defaultCredential;
}

export interface AzureProfileOptions extends ProfileSeams {
  /** Test seam: read through this client instead of one made from the profile. */
  client?: BlobServiceClient;
}

export class AzureProfile extends BaseProfile {
  private service: BlobServiceClient | null;
  private readonly location: StorageLocation;

  constructor(config: StorageProfileConfig, options: AzureProfileOptions = {}) {
    super(config, options);
    this.service = options.client ?? null;
    const location = parseStorageUri(config.scope);
    if (!location || location.scheme !== 'abfss' || !location.account) throw new Error(`not an Azure scope: ${config.scope}`);
    this.location = location;
  }

  protected account(): string {
    return this.location.account as string;
  }

  private servicePrincipal(): { tenantId: string; clientId: string; clientSecret: string } | null {
    const tenantId = this.credential('AZURE_TENANT_ID');
    const clientId = this.credential('AZURE_CLIENT_ID');
    const clientSecret = this.credential('AZURE_CLIENT_SECRET');
    return tenantId && clientId && clientSecret ? { tenantId, clientId, clientSecret } : null;
  }

  /**
   * The connection string DuckDB and the SDK read with: the deployment's, with the account named and,
   * for an emulator, its endpoint, or one built from the account key. Null without either.
   */
  connectionString(): string | null {
    const account = this.account();
    const endpoint = this.config.endpoint;
    const given = this.credential('AZURE_CONNECTION_STRING');
    if (given) {
      const parts = connectionStringParts(given);
      const has = (key: string): boolean => parts.some(([name]) => name.toLowerCase() === key.toLowerCase());
      if (!has('AccountName')) parts.push(['AccountName', account]);
      if (endpoint && !has('BlobEndpoint')) parts.push(['BlobEndpoint', `${endpoint}/${account}`], ['DfsEndpoint', `${endpoint}/${account}`]);
      return `${parts.map(([name, value]) => `${name}=${value}`).join(';')};`;
    }
    const key = this.credential('AZURE_ACCOUNT_KEY');
    if (!key) return null;
    if (!endpoint) return `DefaultEndpointsProtocol=https;AccountName=${account};AccountKey=${key};EndpointSuffix=core.windows.net`;
    const protocol = new URL(endpoint).protocol === 'https:' ? 'https' : 'http';
    return `DefaultEndpointsProtocol=${protocol};AccountName=${account};AccountKey=${key};BlobEndpoint=${endpoint}/${account};DfsEndpoint=${endpoint}/${account};`;
  }

  /**
   * Direct unless the profile says cached, or an emulator would be reached without a connection string:
   * DuckDB sends a token-authenticated read to the account's real host, never to an endpoint.
   */
  get mode(): StorageMode {
    if (this.config.access === 'cached') return 'cached';
    if (this.config.endpoint && this.connectionString() === null) return 'cached';
    return 'direct';
  }

  duckdbSecret(): string | null {
    if (this.mode !== 'direct') return null;
    const options = ['TYPE azure'];
    const connection = this.connectionString();
    const principal = this.servicePrincipal();
    if (connection) {
      options.push(secretOption('CONNECTION_STRING', connection));
    } else if (principal) {
      options.push(
        'PROVIDER service_principal',
        secretOption('TENANT_ID', principal.tenantId),
        secretOption('CLIENT_ID', principal.clientId),
        secretOption('CLIENT_SECRET', principal.clientSecret),
        secretOption('ACCOUNT_NAME', this.account()),
      );
    } else {
      options.push('PROVIDER credential_chain', secretOption('ACCOUNT_NAME', this.account()));
    }
    options.push(secretOption('SCOPE', this.duckdbScope()));
    return `CREATE OR REPLACE TEMPORARY SECRET ${this.secretName} (${options.join(', ')})`;
  }

  duckdbExtensions(): string[] {
    return ['azure'];
  }

  duckdbSource(uri: string): string {
    return azurePathForDuckdb(uri);
  }

  duckdbScope(): string {
    return azurePathForDuckdb(this.scope);
  }

  protected credentialsInUse(): string {
    if (this.credential('AZURE_CONNECTION_STRING')) return 'a connection string';
    if (this.credential('AZURE_ACCOUNT_KEY')) return 'the account key';
    if (this.servicePrincipal()) return 'a service principal';
    return 'the default Azure credential';
  }

  protected credentialsFound(): boolean {
    return Boolean(this.credential('AZURE_CONNECTION_STRING') || this.credential('AZURE_ACCOUNT_KEY') || this.servicePrincipal());
  }

  warnings(): string[] {
    const warnings: string[] = [];
    const principal = ['AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET'];
    const set = principal.filter((suffix) => this.credential(suffix));
    if (set.length > 0 && set.length < principal.length && !this.credential('AZURE_CONNECTION_STRING') && !this.credential('AZURE_ACCOUNT_KEY')) {
      const missing = principal.filter((suffix) => !this.credential(suffix)).map((suffix) => this.variable(suffix));
      warnings.push(`storage profile ${this.name}: its service principal lacks ${missing.join(' and ')}; it reads with the default Azure credential`);
    }
    if (this.config.access !== 'cached' && this.config.endpoint && this.connectionString() === null) {
      warnings.push(
        `storage profile ${this.name}: DuckDB reaches the endpoint ${this.config.endpoint} only through a connection string or the account key ` +
          `(${this.variable('AZURE_CONNECTION_STRING')}, ${this.variable('AZURE_ACCOUNT_KEY')}); data files are downloaded`,
      );
    }
    return warnings;
  }

  private client(): BlobServiceClient {
    if (this.service) return this.service;
    // `withRetries` retries every backend alike; the SDK's own policy would multiply its attempts.
    const options: StoragePipelineOptions = { retryOptions: { maxTries: 1 } };
    const connection = this.credential('AZURE_CONNECTION_STRING') ? this.connectionString() : null;
    if (connection) {
      this.service = BlobServiceClient.fromConnectionString(connection, options);
      return this.service;
    }
    const account = this.account();
    const url = this.config.endpoint ? `${this.config.endpoint}/${account}` : `https://${account}.blob.core.windows.net`;
    const key = this.credential('AZURE_ACCOUNT_KEY');
    const principal = this.servicePrincipal();
    const credential = key
      ? new StorageSharedKeyCredential(account, key)
      : principal
        ? new ClientSecretCredential(principal.tenantId, principal.clientId, principal.clientSecret)
        : defaultAzureCredential();
    this.service = new BlobServiceClient(url, credential, options);
    return this.service;
  }

  private async open(uri: string, signal: AbortSignal): Promise<BlobDownloadResponseParsed> {
    const location = parseStorageUri(uri);
    if (!location || location.scheme !== 'abfss' || location.account !== this.account()) {
      throw new Error(`storage profile ${this.name} reads abfss:// objects of account ${this.account()}, not ${uri}`);
    }
    try {
      return await this.client().getContainerClient(location.container).getBlobClient(location.key).download(0, undefined, { abortSignal: signal });
    } catch (cause) {
      throw readError(uri, cause);
    }
  }

  protected async read(uri: string): Promise<Buffer> {
    return withRetries(uri, METADATA_TIMEOUT_MS, async (signal) => {
      const response = await this.open(uri, signal);
      const chunks: Buffer[] = [];
      try {
        for await (const chunk of response.readableStreamBody ?? []) chunks.push(Buffer.from(chunk as Uint8Array));
      } catch (cause) {
        throw readError(uri, cause);
      }
      const contents = Buffer.concat(chunks);
      verifyMd5(response, createHash('md5').update(contents).digest(), uri);
      return contents;
    });
  }

  protected async download(uri: string, target: string): Promise<void> {
    await withRetries(uri, DATA_FILE_TIMEOUT_MS, async (signal) => {
      const response = await this.open(uri, signal);
      const hash = createHash('md5');
      try {
        await writeWhole(
          target,
          response.readableStreamBody as Readable,
          (chunk) => hash.update(chunk),
          () => verifyMd5(response, hash.digest(), uri),
        );
      } catch (cause) {
        throw readError(uri, cause);
      }
    });
  }
}

/** The blob's stored MD5, when it carries one, against the bytes read. */
function verifyMd5(response: BlobDownloadResponseParsed, computed: Buffer, uri: string): void {
  const stored = response.contentMD5 ?? response.blobContentMD5;
  if (stored && stored.length > 0 && !Buffer.from(stored).equals(computed)) throw new ChecksumMismatchError(uri, 'MD5');
}

/** The SDK's failure as the console's: an HTTP answer by its status. */
function readError(uri: string, cause: unknown): unknown {
  if (cause instanceof StorageReadError || cause instanceof ChecksumMismatchError) return cause;
  const status = (cause as { statusCode?: unknown } | null)?.statusCode;
  if (typeof status === 'number' && status >= 300) {
    const code = (cause as { code?: unknown }).code;
    return new StorageReadError(uri, status, typeof code === 'string' ? code : cause instanceof Error ? cause.message.split('\n')[0] ?? '' : '');
  }
  return cause;
}
