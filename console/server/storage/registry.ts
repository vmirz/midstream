/**
 * The console's storage profiles (design 5.8), the twin of the domains registry: one per destination
 * prefix, matched to every object by its longest scope, as DuckDB matches scoped secrets.
 *
 * The chart renders the profiles into a JSON file (`CONSOLE_STORAGE_FILE`); development may set the
 * same JSON inline (`CONSOLE_STORAGE`). Each profile's credentials come from its own variables,
 * `CONSOLE_STORAGE_<NAME>_…`, never from the file. Without a registry the console reads no object
 * storage at all: every table answers that no storage is configured for its location.
 *
 * The registry is checked as the chart checks it, and against what DuckDB cannot tell apart (an
 * `s3://` path names no endpoint), so a console started by hand refuses what the chart would.
 */

import { readFileSync } from 'node:fs';

import { config } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { AzureProfile, azureConfigurationProblem } from './azure.ts';
import { GcsProfile } from './gcs.ts';
import { CREDENTIAL_SUFFIXES, SCHEME_OF_KIND, credentialVariable, type ProfileSeams, type StorageKind, type StorageMode, type StorageProfile, type StorageProfileConfig } from './profile.ts';
import { S3Profile } from './s3.ts';
import { isLocalUri, parseStorageUri } from './uris.ts';

export class StorageRegistryError extends Error {}

const NAME_PATTERN = /^[a-z][a-z0-9-]{0,39}$/;
const FIELDS = ['name', 'scope', 'kind', 'endpoint', 'region', 'pathStyle', 'access', 'keyFile'];
const FORMS: Readonly<Record<StorageKind, string>> = {
  gcs: 'gs://bucket/[prefix/]',
  s3: 's3://bucket/[prefix/]',
  azure: 'abfss://container@account.dfs.core.windows.net/[prefix/]',
};

/**
 * Read the profiles: the file, else the inline JSON, else none; then every rule. `env` is where each
 * profile's credentials are read from.
 */
export function loadStorageRegistry(
  source: { file?: string | null; json?: string | null },
  env: Record<string, string | undefined> = process.env,
): StorageProfileConfig[] {
  let raw: unknown;
  if (source.file) {
    let text: string;
    try {
      text = readFileSync(source.file, 'utf8');
    } catch (cause) {
      throw new StorageRegistryError(`cannot read the storage file ${source.file}: ${cause instanceof Error ? cause.message : String(cause)}`);
    }
    raw = parse(text, source.file);
  } else if (source.json) {
    raw = parse(source.json, 'CONSOLE_STORAGE');
  } else {
    return [];
  }
  return validateStorageRegistry(raw, env);
}

function parse(text: string, where: string): unknown {
  try {
    return JSON.parse(text);
  } catch (cause) {
    throw new StorageRegistryError(`${where} is not JSON: ${cause instanceof Error ? cause.message : String(cause)}`);
  }
}

/** Every rule of the registry, with each profile's credentials read from `env`. */
export function validateStorageRegistry(raw: unknown, env: Record<string, string | undefined> = process.env): StorageProfileConfig[] {
  if (!Array.isArray(raw)) throw new StorageRegistryError('the storage registry must be a list of profiles: [{name, scope, kind, ...}]');
  const names = new Set<string>();
  const scopes = new Map<string, string>();
  const profiles = raw.map((entry, index): StorageProfileConfig => {
    let where = `storage profile ${index + 1}`;
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw new StorageRegistryError(`${where} must be a profile: {name, scope, kind, ...}`);
    const value = entry as Record<string, unknown>;
    const name = typeof value.name === 'string' ? value.name : '';
    if (!NAME_PATTERN.test(name)) {
      throw new StorageRegistryError(`${where}: name "${name}" must be a lower case letter followed by up to 39 lower case letters, digits or hyphens`);
    }
    where = `storage profile ${index + 1} (${name})`;
    if (names.has(name)) throw new StorageRegistryError(`${where}: the name is used twice; every profile's name must be unique`);
    names.add(name);
    for (const key of Object.keys(value)) {
      if (!FIELDS.includes(key)) throw new StorageRegistryError(`${where}: unknown field "${key}"; a profile takes ${FIELDS.join(', ')}`);
    }
    const kind = value.kind;
    if (kind !== 'gcs' && kind !== 's3' && kind !== 'azure') throw new StorageRegistryError(`${where}: kind "${String(kind ?? '')}" must be gcs, s3 or azure`);
    const scheme = SCHEME_OF_KIND[kind];
    const scope = typeof value.scope === 'string' ? value.scope : '';
    if (!scope.endsWith('/')) throw new StorageRegistryError(`${where}: scope "${scope}" must end with '/': it is the destination prefix the profile serves`);
    if (!scope.startsWith(scheme)) throw new StorageRegistryError(`${where}: scope "${scope}" does not start with ${scheme}, the scheme of kind ${kind}`);
    if (/\s/.test(scope) || parseStorageUri(scope) === null) {
      throw new StorageRegistryError(
        `${where}: scope "${scope}" must name a ${kind === 'azure' ? 'container and its account' : 'bucket'} and hold no whitespace: ${FORMS[kind]}`,
      );
    }
    const served = scopes.get(scope);
    if (served) throw new StorageRegistryError(`${where}: scope "${scope}" is already served by profile ${served}`);
    scopes.set(scope, name);
    const access = value.access ?? null;
    if (access !== null && access !== 'direct' && access !== 'cached') throw new StorageRegistryError(`${where}: access "${String(access)}" must be direct or cached`);
    if (value.pathStyle !== undefined && value.pathStyle !== null && typeof value.pathStyle !== 'boolean') {
      throw new StorageRegistryError(`${where}: pathStyle must be true or false`);
    }
    const credentials: Record<string, string> = {};
    for (const suffix of CREDENTIAL_SUFFIXES[kind]) {
      const set = (env[credentialVariable(name, suffix)] ?? '').trim();
      if (set) credentials[suffix] = set;
    }
    const profile: StorageProfileConfig = {
      name,
      scope,
      kind,
      endpoint: endpointOf(value.endpoint, where),
      region: optionalText(value.region, `${where}: region`),
      pathStyle: typeof value.pathStyle === 'boolean' ? value.pathStyle : null,
      access: access as StorageMode | null,
      keyFile: optionalText(value.keyFile, `${where}: keyFile`),
      credentials,
    };
    if (kind === 'azure') {
      const problem = azureConfigurationProblem(profile, credentialVariable(name, 'AZURE_CONNECTION_STRING'));
      if (problem) throw new StorageRegistryError(`${where}: ${problem}`);
    }
    return profile;
  });
  assertEndpointsDistinguishable(profiles);
  return profiles;
}

function optionalText(value: unknown, what: string): string | null {
  if (value === undefined || value === null || value === '') return null;
  if (typeof value !== 'string') throw new StorageRegistryError(`${what} must be text`);
  return value.trim() || null;
}

function endpointOf(value: unknown, where: string): string | null {
  const text = optionalText(value, `${where}: endpoint`);
  if (text === null) return null;
  let parsed: URL;
  try {
    parsed = new URL(text);
  } catch {
    throw new StorageRegistryError(`${where}: endpoint is not a URL: ${text}`);
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') throw new StorageRegistryError(`${where}: endpoint must be http or https`);
  // Name the server alone, as the chart's midstream.endpoint helper requires: no userinfo (which would
  // carry a credential), no path, query or fragment. The endpoint is shown on Settings and in the
  // Connect snippets, so a credential embedded here would be disclosed to anyone who can see the table.
  if (parsed.username || parsed.password || parsed.search || parsed.hash || (parsed.pathname !== '' && parsed.pathname !== '/')) {
    throw new StorageRegistryError(`${where}: endpoint must name the server alone, http(s)://host[:port], with no user, path, query or fragment: ${text}`);
  }
  return text.replace(/\/+$/, '');
}

/**
 * An `s3://` path names a bucket, not an endpoint, so DuckDB sends it to the endpoint of the secret
 * whose scope matches best. Two profiles serving one bucket through different endpoints would read
 * one endpoint's objects with the other's: refused. Nested scopes on one endpoint are fine.
 */
function assertEndpointsDistinguishable(profiles: StorageProfileConfig[]): void {
  const byBucket = new Map<string, StorageProfileConfig>();
  for (const profile of profiles) {
    if (profile.kind !== 's3') continue;
    const bucket = parseStorageUri(profile.scope)?.container as string;
    const other = byBucket.get(bucket);
    if (!other) {
      byBucket.set(bucket, profile);
      continue;
    }
    const endpoint = (value: string | null): string => (value === null ? 'AWS' : new URL(value).href.replace(/\/+$/, ''));
    if (endpoint(other.endpoint) !== endpoint(profile.endpoint)) {
      throw new StorageRegistryError(
        `storage profiles ${other.name} and ${profile.name} both serve the bucket s3://${bucket}/ through different endpoints ` +
          `(${endpoint(other.endpoint)}, ${endpoint(profile.endpoint)}); an s3:// path names no endpoint, so neither DuckDB nor the console ` +
          'could tell their objects apart',
      );
    }
  }
}

/** A profile of its kind from its configuration. */
export function createProfile(profile: StorageProfileConfig, seams: ProfileSeams = {}): StorageProfile {
  if (profile.kind === 'gcs') return new GcsProfile(profile, seams);
  if (profile.kind === 's3') return new S3Profile(profile, seams);
  return new AzureProfile(profile, seams);
}

/** The 503 a location no profile covers answers with. */
export function storageNotConfigured(uri: string): ConsoleError {
  return new ConsoleError('storage_not_configured', `no storage is configured for ${uri}; add a storage profile whose scope covers it`, 503, { uri });
}

/** Every profile the console has, and which of them serves an object. */
export class StorageRegistry {
  readonly profiles: readonly StorageProfile[];
  /** Longest scope first, so the first match is the longest. */
  private readonly byScope: readonly StorageProfile[];

  constructor(profiles: readonly StorageProfile[]) {
    this.profiles = [...profiles];
    this.byScope = [...profiles].sort((a, b) => b.scope.length - a.scope.length);
  }

  /** The profile whose scope is the longest prefix of `uri`; null for none, and for a local path. */
  find(uri: string): StorageProfile | null {
    if (isLocalUri(uri)) return null;
    return this.byScope.find((profile) => uri.startsWith(profile.scope)) ?? null;
  }

  /** The profile serving `uri`, or the 503 saying none does: a hard gate before any read. */
  profileFor(uri: string): StorageProfile {
    const profile = this.find(uri);
    if (!profile) throw storageNotConfigured(uri);
    return profile;
  }

  /** The profile serving everything under a table's location. */
  forLocation(location: string): StorageProfile | null {
    return this.find(`${location.replace(/\/+$/, '')}/`);
  }

  /**
   * The direct profiles DuckDB reads `sources` with — for each source, the one whose scope matches it
   * best, as DuckDB picks among their secrets. Local paths have none.
   */
  forDuckdbSources(sources: Iterable<string>): StorageProfile[] {
    const direct = [...this.byScope]
      .filter((profile) => profile.mode === 'direct')
      .sort((a, b) => b.duckdbScope().length - a.duckdbScope().length);
    const found = new Set<StorageProfile>();
    for (const source of sources) {
      const profile = direct.find((candidate) => source.startsWith(candidate.duckdbScope()));
      if (profile) found.add(profile);
    }
    return [...found];
  }

  get size(): number {
    return this.profiles.length;
  }
}

let current: StorageRegistry | null = null;

/** The console's storage profiles, read from the registry the first time they are needed. */
export function storage(): StorageRegistry {
  current ??= new StorageRegistry(loadStorageRegistry({ file: config.storageFile, json: config.storageJson }).map((profile) => createProfile(profile)));
  return current;
}

/** Use these profiles instead of the registry's: tests. Null reads the registry again. */
export function useStorage(replacement: StorageRegistry | readonly StorageProfile[] | null): StorageRegistry | null {
  current = replacement === null ? null : replacement instanceof StorageRegistry ? replacement : new StorageRegistry(replacement);
  return current;
}
