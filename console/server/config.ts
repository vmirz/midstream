import { readFileSync } from 'node:fs';
import { hostname, tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Every environment variable the deployment set to a non-empty value, for settings provenance. */
const environmentSet = new Set<string>();

/** What each environment variable falls back to when the deployment leaves it unset. */
const environmentDefaults = new Map<string, string | number | boolean | null>();

function raw(name: string): string | undefined {
  const value = process.env[name];
  if (value === undefined || value === '') return undefined;
  environmentSet.add(name);
  return value;
}

function str(name: string, fallback: string): string {
  environmentDefaults.set(name, fallback);
  return raw(name) ?? fallback;
}

function optional(name: string): string | null {
  environmentDefaults.set(name, null);
  return raw(name) ?? null;
}

function int(name: string, fallback: number): number {
  environmentDefaults.set(name, fallback);
  const value = raw(name);
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be an integer, received "${value}"`);
  }
  return parsed;
}

function bool(name: string, fallback: boolean): boolean {
  environmentDefaults.set(name, fallback);
  const value = raw(name);
  if (value === undefined) return fallback;
  return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
}

/** Whether the deployment sets an environment variable, as opposed to leaving its default. */
export function isEnvironmentSet(name: string): boolean {
  return environmentSet.has(name);
}

/** The value an environment variable falls back to when the deployment leaves it unset. */
export function environmentDefault(name: string): string | number | boolean | null | undefined {
  return environmentDefaults.get(name);
}

const dataDir = str('CONSOLE_DATA_DIR', join(tmpdir(), 'midstream-console'));

/**
 * The console's own version, read from `package.json`.
 *
 * Declared in exactly one place so the shipped version cannot drift from the package: a literal
 * here would be a second source of truth that nothing forces anyone to bump.
 */
function packageVersion(): string {
  try {
    const here = dirname(fileURLToPath(import.meta.url));
    const manifest = readFileSync(join(here, '..', 'package.json'), 'utf8');
    return (JSON.parse(manifest) as { version?: string }).version ?? '0.0.0+unknown';
  } catch {
    return '0.0.0+unknown';
  }
}

/**
 * Settings fixed by the deployment: where the console runs, what it connects to, what it may
 * store, and how big a pod it is sized for. They change only with the deployment.
 */
const deployment = {
  port: int('PORT', 8787),
  host: str('HOST', '0.0.0.0'),
  version: str('CONSOLE_VERSION', packageVersion()),
  memberId: str('HOSTNAME', hostname()),

  /**
   * The URL people use for this console, without a trailing slash: where the sign-in providers send
   * people back to (`<url>/api/auth/sso/<provider>/callback`) and the old hostnames redirect to.
   * Unset, the console takes the URL each request arrived at, which suits development only.
   */
  consoleUrl: optional('CONSOLE_URL')?.replace(/\/+$/, '') || null,
  /**
   * The domains this console serves (handbook 20): a JSON file the chart renders, or the same JSON
   * inline for development. With neither, one domain at http://localhost:6065 (`domains/registry.ts`).
   * Each domain's service token comes from `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`.
   */
  domainsFile: optional('CONSOLE_DOMAINS_FILE'),
  domainsJson: optional('CONSOLE_DOMAINS'),

  dataDir,
  parquetCacheDir: str('CONSOLE_PARQUET_CACHE_DIR', join(dataDir, 'parquet-cache')),
  parquetCacheMaxBytes: int('CONSOLE_PARQUET_CACHE_MAX_BYTES', 8 * 1024 * 1024 * 1024),

  /** HMAC credentials enable zero-copy `gs://` scans straight from DuckDB. */
  gcsHmacKeyId: optional('CONSOLE_GCS_HMAC_KEY_ID'),
  gcsHmacSecret: optional('CONSOLE_GCS_HMAC_SECRET'),

  duckdbMemoryLimit: str('CONSOLE_DUCKDB_MEMORY_LIMIT', '4GB'),
  /**
   * DuckDB worker threads.
   *
   * Reading object storage is bound by round-trip latency, not CPU: a remote Parquet read blocks
   * its thread for the length of each range request, so threads are also how many requests are in
   * flight. Measured from a laptop against GCS, a cold filtered scan of 459 objects took 41 s on
   * 8 threads and 10.5 s on 64. The default leans towards I/O; a CPU-starved pod can lower it.
   */
  duckdbThreads: int('CONSOLE_DUCKDB_THREADS', 32),

  /**
   * Runs: the console's single unit of query work.
   *
   * `runConcurrency` bounds runs scanning at once; more wait their turn. `runScanConcurrency` is
   * how many objects one run reads in parallel, which is what makes a cold scan over object
   * storage fast — each object is four round trips, and they overlap. Together they size Node's
   * worker pool (`workerPool.ts`) when the process starts, which is why they are deployment
   * settings rather than ones the Settings page can change.
   */
  runConcurrency: int('CONSOLE_RUN_CONCURRENCY', 4),
  runScanConcurrency: int('CONSOLE_RUN_SCAN_CONCURRENCY', 48),
  runDir: str('CONSOLE_RUN_DIR', join(dataDir, 'runs')),

  serveClient: bool('CONSOLE_SERVE_CLIENT', true),
  corsOrigin: optional('CONSOLE_CORS_ORIGIN'),

  /**
   * The cookies holding a browser's sessions, one per domain: `<name>_<domain id>`. Renaming it
   * signs everyone out.
   */
  sessionCookieName: str('CONSOLE_SESSION_COOKIE', 'midstream_session'),
  /**
   * Whether the session cookie is marked Secure: `auto` follows the request's scheme, as reported
   * by the ingress, so it is Secure behind TLS and still works on http://localhost.
   */
  cookieSecure: str('CONSOLE_COOKIE_SECURE', 'auto'),

  /**
   * Single sign-on (handbook 20): this console's registration with each provider. A provider with a
   * client id and secret is offered on the sign-in page; each domain decides whether it accepts
   * people from it. The secrets come from Secrets and are never shown.
   */
  ssoEntraTenantId: optional('CONSOLE_SSO_ENTRA_TENANT_ID'),
  ssoEntraClientId: optional('CONSOLE_SSO_ENTRA_CLIENT_ID'),
  ssoEntraClientSecret: optional('CONSOLE_SSO_ENTRA_CLIENT_SECRET'),
  ssoGithubClientId: optional('CONSOLE_SSO_GITHUB_CLIENT_ID'),
  ssoGithubClientSecret: optional('CONSOLE_SSO_GITHUB_CLIENT_SECRET'),
  ssoGoogleClientId: optional('CONSOLE_SSO_GOOGLE_CLIENT_ID'),
  ssoGoogleClientSecret: optional('CONSOLE_SSO_GOOGLE_CLIENT_SECRET'),
  /** A Google Workspace domain the account chooser is limited to; each domain still checks its own. */
  ssoGoogleHostedDomain: optional('CONSOLE_SSO_GOOGLE_HOSTED_DOMAIN'),
  ssoOidcIssuer: optional('CONSOLE_SSO_OIDC_ISSUER')?.replace(/\/+$/, '') || null,
  ssoOidcClientId: optional('CONSOLE_SSO_OIDC_CLIENT_ID'),
  ssoOidcClientSecret: optional('CONSOLE_SSO_OIDC_CLIENT_SECRET'),
  ssoOidcLabel: optional('CONSOLE_SSO_OIDC_LABEL'),
  ssoOidcScopes: str('CONSOLE_SSO_OIDC_SCOPES', 'openid email profile'),
};

/**
 * Settings an administrator may change from the Settings page while the console runs.
 *
 * These are the deployment's values; an override stored with the control plane replaces one until
 * it is reset (`settings/store.ts`). Every read goes through `config`, whose getters return the
 * override when there is one, so a change applies to the next poll, query or request — each of
 * these is read where it is used, never captured once at startup.
 */
const tunableDeployment = {
  controlPlaneTimeoutMs: int('CONTROL_PLANE_TIMEOUT_MS', 10_000),
  logLevel: str('LOG_LEVEL', 'info'),

  /**
   * How long the console trusts a session it checked with the control plane before checking it
   * again. Deactivating someone or ending their sessions reaches this console within this long.
   */
  sessionCheckMs: int('CONSOLE_SESSION_CHECK_MS', 5_000),
  /** Sign-in attempts one client address may make per minute, before the control plane sees them. */
  signInAttemptsPerMinute: int('CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE', 10),

  /** Fleet list refresh cadence while at least one client is connected. */
  listPollIntervalMs: int('CONSOLE_LIST_POLL_MS', 2_000),
  /** Detail refresh cadence for materializations with an open detail stream. */
  detailPollIntervalMs: int('CONSOLE_DETAIL_POLL_MS', 1_500),
  /** Background detail refresh used to keep fleet-wide throughput metrics warm. */
  fleetDetailPollIntervalMs: int('CONSOLE_FLEET_DETAIL_POLL_MS', 6_000),
  /** Slow cadence used when no browser is connected. */
  idlePollIntervalMs: int('CONSOLE_IDLE_POLL_MS', 30_000),
  detailConcurrency: int('CONSOLE_DETAIL_CONCURRENCY', 6),
  /** How long the topic list is served before a read refreshes it in the background. */
  topicCatalogTtlMs: int('CONSOLE_TOPIC_CATALOG_TTL_MS', 15_000),
  /** Floor between topic-list refreshes, however many browsers ask for one. */
  topicCatalogMinRefreshMs: int('CONSOLE_TOPIC_CATALOG_MIN_REFRESH_MS', 5_000),
  seriesRetention: int('CONSOLE_SERIES_POINTS', 240),
  /**
   * Least spacing between points of a throughput series.
   *
   * Polls come every 1.5–6 s depending on who is watching; spacing the points keeps the series'
   * span steady across them — 240 points at 5 s is 20 minutes — while its newest point still
   * follows every poll.
   */
  seriesSampleMs: int('CONSOLE_SERIES_SAMPLE_MS', 5_000),
  /**
   * The spans ingest and write rates are measured over, per runtime member.
   *
   * Members report their counters on their own heartbeats (30 s by default). A member's rate is
   * measured over about this long, timed by the runtime's own clock, and a materialization's rate
   * is the sum over its members. Rows arrive steadily, so a minute is both live and smooth; bytes
   * land a sealed Parquet object at a time, in waves every few minutes, and read as a rate only as
   * a slope over several waves. See `live/rates.ts`.
   */
  ingestRateWindowMs: int('CONSOLE_INGEST_RATE_WINDOW_MS', 60_000),
  writeRateWindowMs: int('CONSOLE_WRITE_RATE_WINDOW_MS', 15 * 60 * 1000),
  /** How long consumer-lag history is kept, one point per measurement. */
  lagHistoryMs: int('CONSOLE_LAG_HISTORY_MS', 6 * 60 * 60 * 1000),

  dataAccessEnabled: bool('CONSOLE_DATA_ACCESS_ENABLED', true),
  /** Budget for work the console volunteers rather than work a user asked for: column profiles. */
  queryStatsTimeoutMs: int('CONSOLE_QUERY_STATS_TIMEOUT_MS', 8_000),
  /** Guard rail against pulling an unbounded dataset into the local cache. */
  cachedModeMaxBytes: int('CONSOLE_CACHED_MODE_MAX_BYTES', 2 * 1024 * 1024 * 1024),
  cachedModeMaxFiles: int('CONSOLE_CACHED_MODE_MAX_FILES', 400),
  /**
   * How fresh a table read must be.
   *
   * Reading the table's pointer is the only way to learn of newer data. A run insists on a read no
   * older than `snapshotCacheMs`, so it includes what was committed moments before Run was pressed;
   * the summary and estimates an open Data tab polls are served from a read up to
   * `tableMetadataFreshMs` old; and an older read, up to `catalogMaxStaleMs`, is served at once
   * while a fresh one happens in the background. A run pins the snapshot it started with, so these
   * only decide how quickly new commits reach the *next* run.
   */
  snapshotCacheMs: int('CONSOLE_SNAPSHOT_CACHE_MS', 2_000),
  tableMetadataFreshMs: int('CONSOLE_TABLE_METADATA_FRESH_MS', 5_000),
  catalogMaxStaleMs: int('CONSOLE_CATALOG_MAX_STALE_MS', 5 * 60 * 1000),

  /** How long a run is kept after it was last used. Its stored result is deleted with it. */
  runIdleRetentionMs: int('CONSOLE_RUN_RETENTION_MS', 30 * 60 * 1000),
  /**
   * Rows a statement's result keeps for paging and export.
   *
   * Its total is always counted in full; past this, only the first rows are kept, because a
   * `SELECT *` over a large range would otherwise copy the dataset onto the console's disk.
   */
  resultRowLimit: int('CONSOLE_RESULT_ROW_LIMIT', 1_000_000),
  /** Most rows one export may contain. */
  exportRowLimit: int('CONSOLE_EXPORT_ROW_LIMIT', 1_000_000),
  /** Most rows one page request may return. */
  pageRowLimit: int('CONSOLE_PAGE_ROW_LIMIT', 1_000),

  /**
   * The live tail: a run of a table, every materialization's, also reads the records newer than its
   * snapshot, from its domain's tail app (the registry's `tailUrl`), and the Data tab's Live mode
   * appends what arrives every `liveTailRefreshMs`. `liveTailMaxRows` bounds one fetch;
   * `liveTailTimeoutMs` is how long a run waits for its tail before it goes on without it.
   */
  liveTailEnabled: bool('CONSOLE_LIVE_TAIL_ENABLED', true),
  liveTailRefreshMs: int('CONSOLE_LIVE_TAIL_REFRESH_MS', 1_500),
  liveTailMaxRows: int('CONSOLE_LIVE_TAIL_MAX_ROWS', 100_000),
  liveTailTimeoutMs: int('CONSOLE_LIVE_TAIL_TIMEOUT_MS', 3_000),
};

export type TunableSettings = typeof tunableDeployment;
export type TunableKey = keyof TunableSettings;
export type Config = typeof deployment & TunableSettings;

/** Every setting the Settings page may override, in declaration order. */
export const TUNABLE_KEYS = Object.keys(tunableDeployment) as TunableKey[];

/** The deployment's value of every overridable setting, before any override. */
export const deploymentSettings: Readonly<TunableSettings> = Object.freeze({ ...tunableDeployment });

let overrides: Partial<TunableSettings> = {};

/**
 * Replace the overrides in force. Values must already be validated against the settings catalog;
 * a key left out returns to its deployment value.
 */
export function applySettingOverrides(values: Partial<TunableSettings>): void {
  overrides = { ...values };
}

/** The overrides in force, by key. */
export function currentSettingOverrides(): Partial<TunableSettings> {
  return { ...overrides };
}

/**
 * The console's configuration. Deployment settings are plain values; overridable settings are
 * getters that return the override in force, or the deployment's value when there is none.
 */
export const config: Readonly<Config> = (() => {
  const target = { ...deployment } as Config;
  for (const key of TUNABLE_KEYS) {
    Object.defineProperty(target, key, {
      enumerable: true,
      get: () => overrides[key] ?? tunableDeployment[key],
    });
  }
  return Object.freeze(target);
})();

/**
 * How the Data tab reads object storage, from the settings in force now.
 *
 * `direct` needs both HMAC credentials, which only the deployment can provide; `disabled` follows
 * the Data explorer setting, which an administrator may turn off and on while the console runs.
 */
export function dataAccessMode(): 'direct' | 'cached' | 'disabled' {
  if (!config.dataAccessEnabled) return 'disabled';
  return config.gcsHmacKeyId && config.gcsHmacSecret ? 'direct' : 'cached';
}

/**
 * A GCS HMAC access ID: `GOOG1E…` for a service account, `GOOG…` for a user.
 *
 * Deliberately permissive about length — Google has shipped more than one size — but the prefix is
 * fixed, and it is the part people get wrong.
 */
const HMAC_KEY_ID_PATTERN = /^GOOG[0-9A-Z]{16,}$/;

/** A GCS HMAC secret is 40 characters of base64. */
const HMAC_SECRET_LENGTH = 40;

/**
 * Complain about HMAC credentials that cannot possibly sign a request.
 *
 * Worth checking at boot because the runtime failure is so misleading: DuckDB rejects a malformed
 * access ID with `MalformedSecurityHeader` and appends its own boilerplate blaming an unset S3
 * region, which sends you looking in entirely the wrong place. The shape is cheap to verify and
 * the mistake — pasting a service-account email or a project id into the key id — is common.
 *
 * Never returns the credential itself, only its length and prefix.
 */
export function hmacCredentialWarning(): string | null {
  if (dataAccessMode() !== 'direct') return null;

  const keyId = config.gcsHmacKeyId ?? '';
  if (!HMAC_KEY_ID_PATTERN.test(keyId)) {
    return (
      `CONSOLE_GCS_HMAC_KEY_ID does not look like a GCS HMAC access ID: expected "GOOG1E…", got ` +
      `${keyId.length} characters starting "${keyId.slice(0, 6)}". This is an HMAC access ID, not a ` +
      `service account email or a JSON key file — create one with ` +
      `\`gcloud storage hmac create <service-account-email>\`. Every gs:// scan will fail with ` +
      `MalformedSecurityHeader until it is corrected.`
    );
  }

  const secret = config.gcsHmacSecret ?? '';
  if (secret.length !== HMAC_SECRET_LENGTH) {
    return (
      `CONSOLE_GCS_HMAC_SECRET is ${secret.length} characters; a GCS HMAC secret is ` +
      `${HMAC_SECRET_LENGTH}. Every gs:// scan will fail to authenticate until it is corrected.`
    );
  }

  return null;
}
