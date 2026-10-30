import {
  DuckDBArrayValue,
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBInstance,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  type DuckDBConnection,
} from '@duckdb/node-api';

import { config } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import { GcsProfile } from '../storage/gcs.ts';
import type { StorageProfile } from '../storage/profile.ts';
import { storage } from '../storage/registry.ts';
import { quoteLiteral } from './sql.ts';
import { fieldName } from '../../shared/fieldPaths.ts';
import type { QueryColumn } from '../../shared/types.ts';

export interface RawQueryResult {
  columns: QueryColumn[];
  rows: Record<string, unknown>[];
  elapsedMs: number;
}

let instancePromise: Promise<DuckDBInstance> | null = null;

/**
 * Settings that make repeat reads of immutable objects nearly free.
 *
 * Committed Parquet objects never change, so everything DuckDB learns about one can be kept for the
 * life of the process. Measured from a laptop against GCS over 438 objects:
 *
 * | | cold | with these |
 * |---|---|---|
 * | bind a `union_by_name` view | 9.6 s | 69 ms |
 * | read one 100-row page | ~250 ms | ~20 ms |
 *
 * `parquet_metadata_cache` keeps footers, `enable_http_metadata_cache` keeps the HEAD responses
 * every remote open starts with, and the external file cache (on by default) keeps the byte
 * ranges themselves, within the memory limit. Times render in UTC so a value never depends on
 * where the console happens to run.
 */
const INSTANCE_SETTINGS = [
  'SET GLOBAL parquet_metadata_cache = true',
  'SET GLOBAL enable_http_metadata_cache = true',
  'SET GLOBAL enable_external_file_cache = true',
  "SET GLOBAL TimeZone = 'UTC'",
];

async function getInstance(): Promise<DuckDBInstance> {
  instancePromise ??= (async () => {
    const instance = await DuckDBInstance.create(':memory:', {
      memory_limit: config.duckdbMemoryLimit,
      threads: String(config.duckdbThreads),
    });
    const connection = await instance.connect();
    try {
      for (const setting of INSTANCE_SETTINGS) {
        try {
          await connection.run(setting);
        } catch (cause) {
          logger.warn({ err: cause, setting }, 'duckdb setting unavailable');
        }
      }
    } finally {
      connection.closeSync();
    }
    logger.info(
      { memoryLimit: config.duckdbMemoryLimit, threads: config.duckdbThreads },
      'duckdb instance ready',
    );
    return instance;
  })();
  return instancePromise;
}

/** The extensions a storage profile may need; nothing else is ever installed by name. */
const STORAGE_EXTENSIONS = new Set(['httpfs', 'aws', 'azure']);

const installs = new Map<string, Promise<void>>();
const profilesReady = new WeakMap<StorageProfile, Promise<void>>();
let setupQueue: Promise<unknown> = Promise.resolve();

/** One instance-wide change at a time: two at once can fail with `Catalog write-write conflict`. */
function serially<T>(work: () => Promise<T>): Promise<T> {
  const next = setupQueue.then(work, work);
  setupQueue = next.catch(() => undefined);
  return next;
}

/**
 * Install an extension once per process, through the shared instance; every instance — a sandbox
 * too — then only loads it. A failed attempt is not cached, so one transient error cannot stick.
 */
function ensureInstalled(extension: string): Promise<void> {
  if (!STORAGE_EXTENSIONS.has(extension)) return Promise.reject(new Error(`not a storage extension: ${extension}`));
  let pending = installs.get(extension);
  if (!pending) {
    pending = serially(async () => {
      const connection = await (await getInstance()).connect();
      try {
        await connection.run(`INSTALL ${extension}`);
      } finally {
        connection.closeSync();
      }
    });
    installs.set(extension, pending);
    pending.catch(() => installs.delete(extension));
  }
  return pending;
}

/**
 * Create a profile's secret on `connection`, failing with what went wrong but never the statement:
 * DuckDB echoes a statement it cannot parse (`LINE 1: … KEY_ID '…'`), credentials and all, and the
 * error reaches the run's status and the log. Every quoted value of the statement is kept out of the
 * message too.
 */
async function createSecret(connection: DuckDBConnection, profile: StorageProfile, statement: string): Promise<void> {
  try {
    await connection.run(statement);
  } catch (cause) {
    const raw = cause instanceof Error ? cause.message : String(cause);
    let message = raw.split('\nLINE')[0]?.trim() || 'the secret could not be created';
    for (const [, quoted] of statement.matchAll(/'((?:[^']|'')+)'/g)) {
      const value = (quoted as string).replace(/''/g, "'");
      if (value.length >= 4) message = message.split(value).join('<redacted>');
    }
    throw new ConsoleError('storage_unavailable', `DuckDB could not create the secret of storage profile ${profile.name}: ${message}`, 503, {
      profile: profile.name,
    });
  }
}

/**
 * Give the shared instance what DuckDB needs to read each direct profile's objects in place: its
 * extensions, loaded once (a load is the instance's, not a connection's), and its secret, created
 * once — a secret is a catalog object owned by the instance. A profile is prepared the first time its
 * objects are read, so an Azure extension is loaded only where an Azure profile is read. A failed
 * attempt is not cached.
 */
export async function prepareStorageAccess(profiles: readonly StorageProfile[]): Promise<void> {
  await Promise.all(
    profiles.map((profile) => {
      let pending = profilesReady.get(profile);
      if (!pending) {
        pending = (async () => {
          const statement = profile.duckdbSecret();
          if (statement === null) return;
          for (const extension of profile.duckdbExtensions()) await ensureInstalled(extension);
          await serially(async () => {
            const connection = await (await getInstance()).connect();
            try {
              for (const extension of profile.duckdbExtensions()) await connection.run(`LOAD ${extension}`);
              await createSecret(connection, profile, statement);
            } finally {
              connection.closeSync();
            }
          });
          logger.debug({ profile: profile.name, scope: profile.scope }, 'duckdb storage secret ready');
        })();
        profilesReady.set(profile, pending);
        pending.catch(() => profilesReady.delete(profile));
      }
      return pending;
    }),
  );
}

/**
 * Run `work` on a fresh connection that can read the destination, closing it afterwards.
 *
 * One connection per unit of work keeps an interrupt local: aborting `signal` stops whatever this
 * connection is executing and nothing else. That is how a cancelled run stops scanning at once
 * rather than when its current query happens to finish.
 */
export async function withConnection<T>(
  work: (connection: DuckDBConnection) => Promise<T>,
  options: { signal?: AbortSignal } = {},
): Promise<T> {
  const { signal } = options;
  if (signal?.aborted) throw cancelledError();

  const instance = await getInstance();
  const connection = await instance.connect();
  const interrupt = (): void => {
    try {
      connection.interrupt();
    } catch (cause) {
      logger.debug({ err: cause }, 'duckdb interrupt failed');
    }
  };
  signal?.addEventListener('abort', interrupt, { once: true });

  try {
    return await work(connection);
  } catch (cause) {
    if (signal?.aborted) throw cancelledError();
    throw toConsoleError(cause);
  } finally {
    signal?.removeEventListener('abort', interrupt);
    try {
      connection.closeSync();
    } catch (cause) {
      logger.debug({ err: cause }, 'duckdb connection close failed');
    }
  }
}

/** What a sandbox may open: whole directories (with a trailing `/`) and exact files. */
export interface SandboxAccess {
  directories: string[];
  paths: string[];
}

/**
 * Run `work` on a DuckDB instance of its own that can open nothing but `access`.
 *
 * For hand-written statements, which the read-only guard checks only by keyword — and keywords can
 * be dodged (`"read_csv"(…)`, `read_csv_auto(…)`, `parquet_scan('gs://…')`). Here every file and
 * object outside `access` is refused by DuckDB itself: external access is off and the
 * configuration is locked before the statement runs, which DuckDB enforces instance-wide and will
 * not undo, so the sandbox lives exactly as long as the work. It costs ~30 ms to set up and starts
 * with cold caches, which only statement runs pay; browsing uses the shared instance.
 *
 * Its storage access is only what its paths need: the extensions of the direct profiles whose scopes
 * serve them, loaded before the lock (nothing can load after it), and only those profiles' secrets,
 * so a sandbox holds no secret its statement does not read with. `access.paths` are the sources exactly as
 * DuckDB opens them, the `az://` form for Azure, so they match the secrets' scopes. The sandbox does
 * not tell reads from writes — `COPY … TO` an allowed object path would succeed — so the SQL guard's
 * refusal of COPY is what stops a write, and profiles should hold read-only credentials.
 */
export async function withSandbox<T>(
  access: SandboxAccess,
  work: (connection: DuckDBConnection) => Promise<T>,
  options: { signal?: AbortSignal } = {},
): Promise<T> {
  const { signal } = options;
  if (signal?.aborted) throw cancelledError();
  const instance = await DuckDBInstance.create(':memory:', {
    memory_limit: config.duckdbMemoryLimit,
    threads: String(config.duckdbThreads),
  });
  const connection = await instance.connect();
  const interrupt = (): void => {
    try {
      connection.interrupt();
    } catch (cause) {
      logger.debug({ err: cause }, 'duckdb interrupt failed');
    }
  };
  signal?.addEventListener('abort', interrupt, { once: true });
  try {
    for (const setting of INSTANCE_SETTINGS) {
      try {
        await connection.run(setting);
      } catch (cause) {
        logger.debug({ err: cause, setting }, 'duckdb setting unavailable');
      }
    }
    await enableProgress(connection);
    const profiles = storage().forDuckdbSources(access.paths);
    for (const extension of new Set(profiles.flatMap((profile) => profile.duckdbExtensions()))) {
      await ensureInstalled(extension);
      await connection.run(`LOAD ${extension}`);
    }
    for (const profile of profiles) {
      const statement = profile.duckdbSecret();
      if (statement !== null) await createSecret(connection, profile, statement);
    }
    const list = (values: string[]): string => `[${[...new Set(values)].map(quoteLiteral).join(', ')}]`;
    await connection.run(`SET allowed_directories = ${list(access.directories)}`);
    await connection.run(`SET allowed_paths = ${list(access.paths)}`);
    await connection.run('SET enable_external_access = false');
    await connection.run('SET lock_configuration = true');
    return await work(connection);
  } catch (cause) {
    if (signal?.aborted) throw cancelledError();
    throw toConsoleError(cause);
  } finally {
    signal?.removeEventListener('abort', interrupt);
    try {
      connection.closeSync();
      instance.closeSync();
    } catch (cause) {
      logger.debug({ err: cause }, 'duckdb sandbox close failed');
    }
  }
}


/** Execute one statement and return its rows, converted to JSON-safe values. */
export async function runQuery(
  sql: string,
  options: { signal?: AbortSignal; setup?: string[] } = {},
): Promise<RawQueryResult> {
  return withConnection(async (connection) => {
    for (const statement of options.setup ?? []) await connection.run(statement);
    return readAll(connection, sql);
  }, options);
}

/** Run `sql` on an open connection and collect the converted result. */
export async function readAll(connection: DuckDBConnection, sql: string): Promise<RawQueryResult> {
  const startedAt = performance.now();
  const reader = await connection.runAndReadAll(sql);
  const names = reader.columnNames();
  const types = reader.columnTypes().map((type) => String(type));
  const rows = reader.getRowObjects().map(convertRow);
  return {
    columns: names.map((name, index) => ({ name, type: types[index] ?? 'UNKNOWN' })),
    rows,
    elapsedMs: Math.round(performance.now() - startedAt),
  };
}

/**
 * Run a statement under a budget, returning `null` when it overruns instead of throwing.
 *
 * For work the console volunteers — column profiles — which can fan out across a whole range and
 * must never hold a panel open for the length of a real query.
 */
export async function runBudgetedQuery(
  sql: string,
  options: { budgetMs?: number; setup?: string[] } = {},
): Promise<RawQueryResult | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.budgetMs ?? config.queryStatsTimeoutMs);
  try {
    return await runQuery(sql, { signal: controller.signal, setup: options.setup });
  } catch (cause) {
    logger.debug({ err: cause }, 'budgeted query did not complete');
    return null;
  } finally {
    clearTimeout(timer);
  }
}

/** One row with every value converted to something JSON can carry without loss. */
export function convertRow(row: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) output[key] = toJsonValue(value);
  return output;
}

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);

/**
 * Convert a DuckDB value into a plain JSON value.
 *
 * Timestamps with a zone become ISO-8601 in UTC — `2026-10-05T12:00:00.500Z` — whatever the
 * console's own zone, so the browser can render them in the reader's. Integers beyond 2^53 and
 * decimals stay strings rather than being rounded through a double. Lists and structs keep their
 * shape, so a nested value reads as data rather than as DuckDB's display text.
 */
export function toJsonValue(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  if (typeof value === 'bigint') return value >= MIN_SAFE && value <= MAX_SAFE ? Number(value) : value.toString();
  if (typeof value !== 'object') return value;

  if (value instanceof DuckDBTimestampTZValue) return isoFromMicros(value.micros);
  if (value instanceof DuckDBTimestampMillisecondsValue) return naiveTimestamp(value.toString());
  if (value instanceof DuckDBTimestampSecondsValue) return naiveTimestamp(value.toString());
  if (value instanceof DuckDBTimestampNanosecondsValue) return naiveTimestamp(value.toString());
  if (value instanceof DuckDBTimestampValue) return naiveTimestamp(value.toString());
  if (value instanceof DuckDBDateValue) return value.toString();
  if (value instanceof DuckDBDecimalValue) return value.toString();
  if (value instanceof DuckDBListValue || value instanceof DuckDBArrayValue) {
    return value.items.map(toJsonValue);
  }
  if (value instanceof DuckDBStructValue) {
    return Object.fromEntries(Object.entries(value.entries).map(([key, item]) => [key, toJsonValue(item)]));
  }
  if (value instanceof DuckDBMapValue) {
    return value.entries.map((entry) => ({ key: toJsonValue(entry.key), value: toJsonValue(entry.value) }));
  }
  if (value instanceof DuckDBBlobValue) return `0x${Buffer.from(value.bytes).toString('hex')}`;
  if (value instanceof Uint8Array) return `0x${Buffer.from(value).toString('hex')}`;
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(toJsonValue);

  const record = value as Record<string, unknown>;
  if (Object.getPrototypeOf(record) !== Object.prototype && typeof record.toString === 'function') {
    return record.toString();
  }
  return Object.fromEntries(Object.entries(record).map(([key, item]) => [key, toJsonValue(item)]));
}

function isoFromMicros(micros: bigint): string | null {
  const millis = Number(micros / 1000n);
  if (!Number.isFinite(millis)) return null;
  const date = new Date(millis);
  return Number.isNaN(date.getTime()) ? String(micros) : date.toISOString();
}

/** A zoneless timestamp renders as written, with `T`, but never gains a `Z` it does not have. */
function naiveTimestamp(text: string): string {
  return text.replace(' ', 'T');
}

/** Where in the submitted statement an error was reported. Both are 1-based, as an editor counts. */
export interface StatementPosition {
  line: number;
  column: number;
}

/**
 * Split a DuckDB error into a message and the place it points at.
 *
 * DuckDB appends the offending source line and a caret beneath it. The echo is dropped — it repeats
 * the statement back at someone who just wrote it — but its coordinates are kept, so the editor
 * can mark the spot instead of describing it.
 */
export function parseDuckDbError(
  cause: unknown,
  { tableNames }: { tableNames?: readonly string[] } = {},
): { message: string; position: StatementPosition | null } {
  const raw = cause instanceof Error ? cause.message : String(cause);
  const message = suggestTableName(
    suggestFieldName(gcsSignatureHint(explainMissingDataFile(raw.split('\nLINE')[0]?.trim() || raw))),
    tableNames,
  );

  const match = /\nLINE (\d+):(.*)\n( *)\^/.exec(raw);
  if (!match) return { message, position: null };

  const line = Number.parseInt(match[1] as string, 10);
  // `LINE 3:` then a separator space, so the source line's first character sits at `prefix + 1`.
  const prefix = `LINE ${match[1]}:`.length;
  const caret = (match[3] as string).length;
  if (!Number.isFinite(line) || caret <= prefix) return { message, position: null };

  return { message, position: { line, column: caret - prefix } };
}

/**
 * Point a statement written against the old column names at the new ones.
 *
 * Columns used to carry the schema's `payload.` root — `"payload.status"` — and statements saved
 * from then still say so. DuckDB's own candidates may not include the right column, so name it.
 */
function suggestFieldName(message: string): string {
  const missing = /Referenced column "(payload\.[^"]+)" not found/.exec(message)?.[1];
  if (!missing) return message;
  const name = fieldName(missing);
  if (name === missing) return message;
  return `${message}\nColumns are named without the "payload." root: write "${name}".`;
}

/**
 * Say which tables there are when a statement names one that is not.
 *
 * `tableNames` are full names, `domain_a.shop.orders`. DuckDB guesses the nearest names it knows.
 * When a guess is one of the tables, the guess is the answer; otherwise it is a system table such as
 * `pg_am`, which only misleads, so the tables are named instead.
 */
function suggestTableName(message: string, tableNames: readonly string[] | undefined): string {
  if (!tableNames || tableNames.length === 0) return message;
  if (!/Table with name ([^\s!]+) does not exist/i.test(message)) return message;
  const guesses = (/Did you mean "([^"]*)"\?/.exec(message)?.[1] ?? '')
    .split(/\s+or\s+/)
    .map((guess) => guess.trim().replace(/^memory\./i, '').toLowerCase())
    .filter(Boolean);
  // DuckDB names a table in a schema the statement already named without its catalog and schema:
  // `domain_a.shop.orderz` gets "orders".
  const known = new Set(
    tableNames.flatMap((name) => {
      const [domain, ...rest] = name.toLowerCase().split('.');
      const inDomain = rest.join('.');
      return [name.toLowerCase(), `${domain}_iceberg.${inDomain}`, inDomain, rest.at(-1) as string];
    }),
  );
  if (guesses.some((guess) => known.has(guess))) return message;
  const withoutGuess = message.replace(/\s*Did you mean "[^"]*"\?/, '');
  const list = tableNames.length === 1 ? `table here is ${tableNames[0]}` : `tables here are ${tableNames.slice(0, -1).join(', ')} and ${tableNames.at(-1)}`;
  return `${withoutGuess}\nThe ${list}.`;
}

function toConsoleError(cause: unknown): ConsoleError {
  if (cause instanceof ConsoleError) return cause;
  const { message, position } = parseDuckDbError(cause);
  return new ConsoleError('query_failed', message, 400, position ? { position } : {});
}

export function cancelledError(): ConsoleError {
  return new ConsoleError('cancelled', 'cancelled', 409);
}

export function isCancelled(cause: unknown): boolean {
  return cause instanceof ConsoleError && cause.code === 'cancelled';
}

/**
 * Say which of the console's variables to check when GCS refuses DuckDB's signature.
 *
 * Only for an object a gcs profile serves: DuckDB's own S3 messages are accurate, and HMAC advice
 * would send someone reading S3 to the wrong variables. DuckDB names the object by the URL it asked
 * (`https://storage.googleapis.com/<bucket>/<key>`, or the profile's endpoint), so that is mapped back
 * to its `gs://` URI and its profile. Where DuckDB blames an unset region, the region is dropped: the
 * cause is credentials that cannot sign the request.
 */
function gcsSignatureHint(message: string): string {
  if (!/GCS authentication failed|MalformedSecurityHeader|SignatureDoesNotMatch|InvalidAccessKeyId/.test(message)) return message;
  const profile = gcsProfileNamedIn(message);
  if (!profile) return message;
  const blamedRegion = /No region is provided|S3 region being set incorrectly/i.test(message);
  const cleaned = message
    .replace(/\s*\*?\s*No region is provided\.?/gi, '')
    .replace(/\s*-?\s*this can be caused by the S3 region being set incorrectly\.?/gi, '')
    .trim();
  return `${cleaned}${blamedRegion ? ' — GCS rejected the request signature, not the region.' : ''}\n${profile.hmacAdvice()}`;
}

/** The gcs profile serving an object a DuckDB message names, by `gs://` URI or by the URL DuckDB asked. */
function gcsProfileNamedIn(message: string): GcsProfile | null {
  const registry = storage();
  const gcs = registry.profiles.filter((profile): profile is GcsProfile => profile instanceof GcsProfile);
  if (gcs.length === 0) return null;
  for (const token of message.match(/(?:gs|https?):\/\/[^\s'"()]+/g) ?? []) {
    const candidates = [token];
    const url = /^https?:\/\/(?:([^/.]+)\.)?storage\.googleapis\.com\/(.*)$/.exec(token);
    if (url) candidates.push(url[1] ? `gs://${url[1]}/${url[2]}` : `gs://${url[2]}`);
    for (const profile of gcs) {
      const endpoint = profile.describe().endpoint;
      if (endpoint && token.startsWith(`${endpoint}/`)) candidates.push(`gs://${token.slice(endpoint.length + 1)}`);
    }
    for (const candidate of candidates) {
      const found = registry.find(candidate);
      if (found instanceof GcsProfile) return found;
    }
  }
  return null;
}

/** DuckDB's sandbox wording for an allowed path it retried as a directory: the file is not there. */
const MISSING_IN_SANDBOX = /Permission Error: Cannot access file "([^"]+?)\/\*\*\/\*\.parquet"[^\n]*/;
const REMOTE_PATH = /(?:gs|s3|az|abfss|https?):\/\//;

/**
 * Say plainly that a data file is gone, whatever the backend.
 *
 * A snapshot expiry removes the files of snapshots no longer kept, so a run pinned to an older
 * snapshot can find one gone. DuckDB words it per scheme: `HTTP 404 Not Found` for `gs://` and
 * `s3://`, `No files found that match the pattern` for `az://`, and in a statement's sandbox a
 * Permission Error on the path followed by `/**\/*.parquet` — DuckDB retried the missing file as a
 * directory, which the sandbox refuses. Local files are not explained: the cache can evict one.
 */
export function explainMissingDataFile(message: string): string {
  const hint = 'The data file is no longer in storage: a snapshot expiry may have removed it. Run the query again to read the table’s current snapshot.';
  const sandboxed = MISSING_IN_SANDBOX.exec(message);
  if (sandboxed && REMOTE_PATH.test(sandboxed[1] as string)) {
    return message.replace(MISSING_IN_SANDBOX, `IO Error: ${sandboxed[1]} could not be read.\n${hint}`);
  }
  if (!REMOTE_PATH.test(message)) return message;
  if (/No files found that match the pattern/.test(message) || /HTTP 404\b/.test(message)) return `${message}\n${hint}`;
  return message;
}

/**
 * DuckDB's own estimate of how far one statement has got, 0–1, or `null` while it cannot say.
 *
 * Its row counters are deliberately not used: `rows_processed` counts tuples crossing pipeline
 * operators, not rows of the answer, and overshot a 63,438-row dataset by more than ten times.
 */
export function statementProgress(connection: DuckDBConnection): number | null {
  try {
    const percentage = Number(connection.progress?.percentage ?? -1);
    return Number.isFinite(percentage) && percentage >= 0 ? Math.min(1, percentage / 100) : null;
  } catch {
    return null;
  }
}

/** Turn on DuckDB's progress tracking for one connection, from the first chunk. */
export async function enableProgress(connection: DuckDBConnection): Promise<void> {
  for (const setting of ['SET enable_progress_bar = true', 'SET enable_progress_bar_print = false', 'SET progress_bar_time = 0']) {
    try {
      await connection.run(setting);
    } catch (cause) {
      logger.debug({ err: cause, setting }, 'progress setting unavailable');
    }
  }
}

/** Parse and bind a statement without running it. Cheap, and with no side effects. */
export async function assertStatementBinds(sql: string, setup: string[]): Promise<void> {
  await runQuery(`PREPARE console_bind_check AS ${sql}`, { setup });
}
