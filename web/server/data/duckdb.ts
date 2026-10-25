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

import { config, hmacCredentialWarning } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
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

let httpfsReady: Promise<void> | null = null;
let secretReady: Promise<void> | null = null;

/**
 * Install httpfs and register the GCS secret, for direct `gs://` scans.
 *
 * The secret is a catalog object owned by the instance, so it is created exactly once per process;
 * creating it per connection fails as soon as two queries start together (`Catalog write-write
 * conflict`). A failed attempt is not cached, so one transient error cannot poison direct mode.
 */
async function ensureRemoteAccess(connection: DuckDBConnection): Promise<void> {
  if (!config.gcsHmacKeyId || !config.gcsHmacSecret) return;

  httpfsReady ??= (async () => {
    await connection.run('INSTALL httpfs');
    await connection.run('LOAD httpfs');
  })().catch((cause) => {
    httpfsReady = null;
    throw cause;
  });
  await httpfsReady;

  // Extensions load per connection; the secret does not.
  await connection.run('LOAD httpfs');

  secretReady ??= connection
    .run(
      `CREATE OR REPLACE SECRET console_gcs (TYPE GCS, KEY_ID ${quoteLiteral(config.gcsHmacKeyId)}, SECRET ${quoteLiteral(config.gcsHmacSecret)})`,
    )
    .then(() => undefined)
    .catch((cause) => {
      secretReady = null;
      throw cause;
    });
  await secretReady;
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
    await ensureRemoteAccess(connection);
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
    if (config.gcsHmacKeyId && config.gcsHmacSecret) {
      await ensureHttpfsInstalled();
      await connection.run('LOAD httpfs');
      await connection.run(
        `CREATE SECRET console_gcs (TYPE GCS, KEY_ID ${quoteLiteral(config.gcsHmacKeyId)}, SECRET ${quoteLiteral(config.gcsHmacSecret)})`,
      );
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

/** httpfs is installed once, through the shared instance; a sandbox only loads it. */
async function ensureHttpfsInstalled(): Promise<void> {
  const instance = await getInstance();
  const connection = await instance.connect();
  try {
    await ensureRemoteAccess(connection);
  } finally {
    connection.closeSync();
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
    suggestFieldName(replaceMisleadingRegionHint(raw.split('\nLINE')[0]?.trim() || raw)),
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
 * `tableNames` are the canonical names, `shop.orders`. DuckDB guesses the nearest names it knows —
 * `"iceberg.shop.orders or shop.orders"` for a bare `orders`. When a guess is one of the
 * materialization's tables, the guess is the answer; otherwise it is a system table such as `pg_am`,
 * which only misleads, so the materialization's tables are named instead.
 */
function suggestTableName(message: string, tableNames: readonly string[] | undefined): string {
  if (!tableNames || tableNames.length === 0) return message;
  if (!/Table with name ([^\s!]+) does not exist/i.test(message)) return message;
  const guesses = (/Did you mean "([^"]*)"\?/.exec(message)?.[1] ?? '')
    .split(/\s+or\s+/)
    .map((guess) => guess.trim().replace(/^memory\./i, '').toLowerCase())
    .filter(Boolean);
  // DuckDB names a table in a schema the statement already named without its schema: `shop.orderz`
  // gets "orders".
  const known = new Set(
    tableNames.flatMap((name) => [name.toLowerCase(), `iceberg.${name.toLowerCase()}`, name.toLowerCase().split('.').at(-1) as string]),
  );
  if (guesses.some((guess) => known.has(guess))) return message;
  const withoutGuess = message.replace(/\s*Did you mean "[^"]*"\?/, '');
  const list = tableNames.length === 1 ? `table is ${tableNames[0]}` : `tables are ${tableNames.slice(0, -1).join(', ')} and ${tableNames.at(-1)}`;
  return `${withoutGuess}\nThis materialization's ${list}.`;
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
 * Correct DuckDB's boilerplate when a `gs://` read fails to authenticate.
 *
 * Every S3-layer HTTP error blames an unset region, which is wrong for GCS and sends people to
 * debug the wrong thing; the real cause is credentials that cannot sign the request.
 */
function replaceMisleadingRegionHint(message: string): string {
  const authFailure = /MalformedSecurityHeader|SignatureDoesNotMatch|InvalidAccessKeyId/.test(message);
  if (!authFailure) return message;

  const withoutRegionHint = message
    .replace(/\s*\*?\s*No region is provided\.?/gi, '')
    .replace(/\s*-?\s*this can be caused by the S3 region being set incorrectly\.?/gi, '')
    .trim();

  const detail =
    hmacCredentialWarning() ??
    'Check that CONSOLE_GCS_HMAC_KEY_ID and CONSOLE_GCS_HMAC_SECRET are a matching, active GCS ' +
      'HMAC key pair whose service account can read this bucket.';

  return `${withoutRegionHint} — GCS rejected the request signature, not the region. ${detail}`;
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
