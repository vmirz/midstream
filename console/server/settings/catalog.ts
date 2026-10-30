/**
 * Every console setting an administrator can change from the Settings page, and what it means.
 *
 * The deployment's environment sets each one (`config.ts`); an override chosen on the Settings
 * page wins until it is reset. Settings that must match the deployment itself — ports, paths,
 * credentials, the control plane's address, and anything sized with the pod — are not here and
 * stay environment-only. The labels and descriptions are shown as written, so they say what a
 * setting controls in the reader's terms and, where it helps, when to change it.
 */

import { TUNABLE_KEYS, type TunableKey, type TunableSettings } from '../config.ts';
import type { SettingKind, SettingUnit, SettingValue } from '../../shared/types.ts';

export interface ConsoleSettingDefinition {
  key: TunableKey;
  envVar: string;
  section: string;
  label: string;
  description: string | null;
  kind: SettingKind;
  unit: SettingUnit | null;
  minimum?: number;
  maximum?: number;
  options?: readonly string[];
  maxLength?: number;
  advanced?: boolean;
  danger?: boolean;
  effect?: string;
}

const SECOND = 1_000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;
const GIB = 1024 ** 3;

export const LOG_LEVELS = ['trace', 'debug', 'info', 'warn', 'error', 'fatal'] as const;

export const CONSOLE_SETTINGS: readonly ConsoleSettingDefinition[] = [
  {
    key: 'listPollIntervalMs',
    envVar: 'CONSOLE_LIST_POLL_MS',
    section: 'live',
    label: 'Fleet refresh',
    description: 'How often the console asks the control plane for the fleet while someone is watching.',
    kind: 'integer',
    unit: 'ms',
    minimum: 500,
    maximum: 10 * MINUTE,
  },
  {
    key: 'detailPollIntervalMs',
    envVar: 'CONSOLE_DETAIL_POLL_MS',
    section: 'live',
    label: 'Open materialization refresh',
    description: 'How often a materialization someone has open is refreshed.',
    kind: 'integer',
    unit: 'ms',
    minimum: 500,
    maximum: 10 * MINUTE,
  },
  {
    key: 'fleetDetailPollIntervalMs',
    envVar: 'CONSOLE_FLEET_DETAIL_POLL_MS',
    section: 'live',
    label: 'Background refresh',
    description:
      'How often each materialization nobody has open is refreshed, which keeps fleet-wide rates ' +
      'current. Raise it first to cut the load the console puts on the control plane.',
    kind: 'integer',
    unit: 'ms',
    minimum: SECOND,
    maximum: HOUR,
  },
  {
    key: 'idlePollIntervalMs',
    envVar: 'CONSOLE_IDLE_POLL_MS',
    section: 'live',
    label: 'Idle refresh',
    description: 'Refresh interval while no browser is connected.',
    kind: 'integer',
    unit: 'ms',
    minimum: 5 * SECOND,
    maximum: HOUR,
  },
  {
    key: 'detailConcurrency',
    envVar: 'CONSOLE_DETAIL_CONCURRENCY',
    section: 'live',
    label: 'Parallel detail requests',
    description: 'Most materialization details fetched from the control plane at once.',
    kind: 'integer',
    unit: 'requests',
    minimum: 1,
    maximum: 64,
    advanced: true,
  },
  {
    key: 'seriesRetention',
    envVar: 'CONSOLE_SERIES_POINTS',
    section: 'live',
    label: 'Chart points',
    description: 'Points kept in each throughput chart. Points × spacing is the span a chart covers.',
    kind: 'integer',
    unit: 'points',
    minimum: 10,
    maximum: 10_000,
    effect: 'charts as they gain points',
  },
  {
    key: 'seriesSampleMs',
    envVar: 'CONSOLE_SERIES_SAMPLE_MS',
    section: 'live',
    label: 'Chart point spacing',
    description: 'Least time between two points of a throughput chart.',
    kind: 'integer',
    unit: 'ms',
    minimum: SECOND,
    maximum: 10 * MINUTE,
    effect: 'charts as they gain points',
  },
  {
    key: 'ingestRateWindowMs',
    envVar: 'CONSOLE_INGEST_RATE_WINDOW_MS',
    section: 'live',
    label: 'Ingest rate window',
    description:
      'Span each runtime member’s row rate is measured over. Keep it at least twice the runtime ' +
      'heartbeat (30 s); longer is steadier, shorter is livelier.',
    kind: 'integer',
    unit: 'ms',
    minimum: 10 * SECOND,
    maximum: HOUR,
  },
  {
    key: 'writeRateWindowMs',
    envVar: 'CONSOLE_WRITE_RATE_WINDOW_MS',
    section: 'live',
    label: 'Write rate window',
    description:
      'Span the Parquet write rate is fitted over. Bytes land in waves as batches seal, so a ' +
      'window shorter than a few waves swings between zero and a whole wave.',
    kind: 'integer',
    unit: 'ms',
    minimum: MINUTE,
    maximum: DAY,
  },
  {
    key: 'lagHistoryMs',
    envVar: 'CONSOLE_LAG_HISTORY_MS',
    section: 'live',
    label: 'Consumer lag history',
    description: 'How long lag measurements are kept for the consumer-lag chart.',
    kind: 'integer',
    unit: 'ms',
    minimum: 10 * MINUTE,
    maximum: 7 * DAY,
  },
  {
    key: 'dataAccessEnabled',
    envVar: 'CONSOLE_DATA_ACCESS_ENABLED',
    section: 'data',
    label: 'Data explorer',
    description:
      'Let people query materialized data from each materialization’s Data tab. Off, the tab ' +
      'explains it is turned off and the console reads no object storage.',
    kind: 'boolean',
    unit: null,
    effect: 'the next query',
  },
  {
    key: 'pageRowLimit',
    envVar: 'CONSOLE_PAGE_ROW_LIMIT',
    section: 'data',
    label: 'Rows per page',
    description: 'Most rows one page of results may hold.',
    kind: 'integer',
    unit: 'rows',
    minimum: 10,
    maximum: 10_000,
  },
  {
    key: 'resultRowLimit',
    envVar: 'CONSOLE_RESULT_ROW_LIMIT',
    section: 'data',
    label: 'Statement rows kept',
    description:
      'Rows a SQL statement’s result keeps for paging and export; its total is always counted. ' +
      'Results are stored on the console’s disk, so this bounds how much one statement can store.',
    kind: 'integer',
    unit: 'rows',
    minimum: 1_000,
    maximum: 100_000_000,
    effect: 'the next statement',
  },
  {
    key: 'exportRowLimit',
    envVar: 'CONSOLE_EXPORT_ROW_LIMIT',
    section: 'data',
    label: 'Export rows',
    description: 'Most rows one export may contain.',
    kind: 'integer',
    unit: 'rows',
    minimum: 1_000,
    maximum: 100_000_000,
  },
  {
    key: 'queryStatsTimeoutMs',
    envVar: 'CONSOLE_QUERY_STATS_TIMEOUT_MS',
    section: 'data',
    label: 'Column profile time limit',
    description: 'Time a column profile may take before it is shown as partial.',
    kind: 'integer',
    unit: 'ms',
    minimum: SECOND,
    maximum: 5 * MINUTE,
  },
  {
    key: 'runIdleRetentionMs',
    envVar: 'CONSOLE_RUN_RETENTION_MS',
    section: 'data',
    label: 'Run retention',
    description: 'How long a run and its stored result are kept after anyone last looked at them.',
    kind: 'integer',
    unit: 'ms',
    minimum: MINUTE,
    maximum: DAY,
  },
  {
    key: 'snapshotCacheMs',
    envVar: 'CONSOLE_SNAPSHOT_CACHE_MS',
    section: 'data',
    label: 'Snapshot freshness for runs',
    description:
      'How old a read of a table’s current snapshot may be when someone presses Run. Lower means a ' +
      'run includes commits made a moment before; every read is one small request to object storage.',
    kind: 'integer',
    unit: 'ms',
    minimum: 0,
    maximum: MINUTE,
    advanced: true,
  },
  {
    key: 'tableMetadataFreshMs',
    envVar: 'CONSOLE_TABLE_METADATA_FRESH_MS',
    section: 'data',
    label: 'Table summary freshness',
    description: 'How long a table’s row counts, time span and estimates are shown as current before the table is read again.',
    kind: 'integer',
    unit: 'ms',
    minimum: 0,
    maximum: 10 * MINUTE,
    advanced: true,
  },
  {
    key: 'catalogMaxStaleMs',
    envVar: 'CONSOLE_CATALOG_MAX_STALE_MS',
    section: 'data',
    label: 'Stale table limit',
    description: 'How long an older read of a table may still be shown while a fresh one is made.',
    kind: 'integer',
    unit: 'ms',
    minimum: SECOND,
    maximum: HOUR,
    advanced: true,
  },
  {
    key: 'cachedModeMaxFiles',
    envVar: 'CONSOLE_CACHED_MODE_MAX_FILES',
    section: 'data',
    label: 'Cached-mode file budget',
    description: 'Most data files one scan may download from storage profiles read in cached mode; a wider time range is refused.',
    kind: 'integer',
    unit: 'objects',
    minimum: 1,
    maximum: 100_000,
    advanced: true,
  },
  {
    key: 'cachedModeMaxBytes',
    envVar: 'CONSOLE_CACHED_MODE_MAX_BYTES',
    section: 'data',
    label: 'Cached-mode byte budget',
    description: 'Most bytes one scan may download from storage profiles read in cached mode.',
    kind: 'integer',
    unit: 'bytes',
    minimum: 1024 * 1024,
    maximum: 1024 * GIB,
    advanced: true,
  },
  {
    key: 'topicCatalogTtlMs',
    envVar: 'CONSOLE_TOPIC_CATALOG_TTL_MS',
    section: 'topics',
    label: 'Topic list cache',
    description: 'How long the console serves the topic list before refreshing it in the background.',
    kind: 'integer',
    unit: 'ms',
    minimum: SECOND,
    maximum: HOUR,
  },
  {
    key: 'topicCatalogMinRefreshMs',
    envVar: 'CONSOLE_TOPIC_CATALOG_MIN_REFRESH_MS',
    section: 'topics',
    label: 'Minimum topic list refresh',
    description: 'Least time between two topic-list refreshes, however many browsers ask.',
    kind: 'integer',
    unit: 'ms',
    minimum: 500,
    maximum: HOUR,
  },
  {
    key: 'logLevel',
    envVar: 'LOG_LEVEL',
    section: 'diagnostics',
    label: 'Console log level',
    description: 'Lowest severity the console logs. Debug shows per-materialization refresh failures.',
    kind: 'enum',
    unit: null,
    options: LOG_LEVELS,
  },
  {
    key: 'controlPlaneTimeoutMs',
    envVar: 'CONTROL_PLANE_TIMEOUT_MS',
    section: 'diagnostics',
    label: 'Control-plane request timeout',
    description: 'How long the console waits for the control plane to answer one request.',
    kind: 'integer',
    unit: 'ms',
    minimum: SECOND,
    maximum: 2 * MINUTE,
  },
  {
    key: 'sessionCheckMs',
    envVar: 'CONSOLE_SESSION_CHECK_MS',
    section: 'security',
    label: 'Session check interval',
    description:
      'How long the console trusts a session it checked with the control plane before checking it ' +
      'again. Deactivating someone or ending their sessions reaches the console within this long.',
    kind: 'integer',
    unit: 'ms',
    minimum: 0,
    maximum: MINUTE,
    advanced: true,
  },
  {
    key: 'signInAttemptsPerMinute',
    envVar: 'CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE',
    section: 'security',
    label: 'Sign-in attempts per minute',
    description:
      'Sign-in attempts one browser address may make in a minute before the console turns it away. ' +
      'Accounts are also locked out after repeated wrong passwords, whatever the address.',
    kind: 'integer',
    unit: 'attempts',
    minimum: 1,
    maximum: 1_000,
  },
  {
    key: 'liveTailEnabled',
    envVar: 'CONSOLE_LIVE_TAIL_ENABLED',
    section: 'live-tail',
    label: 'Live tail in this console',
    description:
      'Show the newest records of every materialization, before they are committed, ' +
      'and let the Data tab follow them live. Off, this console shows committed rows only; the control ' +
      'plane’s switch above turns the tail off everywhere.',
    kind: 'boolean',
    unit: null,
    effect: 'the next query',
  },
  {
    key: 'liveTailRefreshMs',
    envVar: 'CONSOLE_LIVE_TAIL_REFRESH_MS',
    section: 'live-tail',
    label: 'Live refresh',
    description: 'How often the Data tab’s Live mode asks for records that arrived since its last look.',
    kind: 'integer',
    unit: 'ms',
    minimum: 500,
    maximum: MINUTE,
  },
  {
    key: 'liveTailMaxRows',
    envVar: 'CONSOLE_LIVE_TAIL_MAX_ROWS',
    section: 'live-tail',
    label: 'Most tail rows per fetch',
    description:
      'Most uncommitted records one fetch of a table’s tail takes. Past it the run says the newest ' +
      'records are not shown yet, and Live takes the rest on its next look.',
    kind: 'integer',
    unit: 'rows',
    minimum: 1_000,
    maximum: 10_000_000,
    advanced: true,
  },
  {
    key: 'liveTailTimeoutMs',
    envVar: 'CONSOLE_LIVE_TAIL_TIMEOUT_MS',
    section: 'live-tail',
    label: 'Tail wait',
    description:
      'How long a run waits for a table’s tail. Past it the run shows committed rows only and says ' +
      'the live tail is unavailable; Live tries again on its next look.',
    kind: 'integer',
    unit: 'ms',
    minimum: 500,
    maximum: 30 * SECOND,
    advanced: true,
  },
];

/**
 * Settings earlier consoles had. A stored override for one is ignored without complaint, since it
 * can only have been written by a console from before the setting was retired.
 */
const RETIRED_SETTINGS = new Set([
  'defaultSubmittedBy',
  // 1.0 reads Iceberg manifests; the control plane's object manifest and object listing are gone.
  'objectManifestEnabled',
  'manifestTailMaxMarkers',
  'maxListedObjects',
  'catalogFreshMs',
]);

const BY_KEY = new Map(CONSOLE_SETTINGS.map((definition) => [definition.key as string, definition]));

export function consoleSettingDefinition(key: string): ConsoleSettingDefinition | undefined {
  return BY_KEY.get(key);
}

/** A reason a value cannot be stored for a setting, or null when it can. */
export function consoleSettingProblem(definition: ConsoleSettingDefinition, value: unknown): string | null {
  switch (definition.kind) {
    case 'boolean':
      return typeof value === 'boolean' ? null : `${definition.label} must be on or off`;
    case 'integer': {
      if (typeof value !== 'number' || !Number.isInteger(value)) return `${definition.label} must be a whole number`;
      if (definition.minimum !== undefined && value < definition.minimum) {
        return `${definition.label} must be at least ${definition.minimum}`;
      }
      if (definition.maximum !== undefined && value > definition.maximum) {
        return `${definition.label} must be at most ${definition.maximum}`;
      }
      return null;
    }
    case 'enum':
      return typeof value === 'string' && (definition.options ?? []).includes(value)
        ? null
        : `${definition.label} must be one of: ${(definition.options ?? []).join(', ')}`;
    case 'string': {
      if (typeof value !== 'string' || !value.trim()) return `${definition.label} cannot be empty`;
      if (definition.maxLength !== undefined && value.length > definition.maxLength) {
        return `${definition.label} must be at most ${definition.maxLength} characters`;
      }
      return null;
    }
    default:
      return `${definition.label} cannot be set here`;
  }
}

/**
 * The stored overrides that are valid against this console's catalog.
 *
 * Overrides are stored by the control plane, which does not know the console's settings; one saved
 * by a newer or older console may not fit this one, and is dropped here rather than applied.
 */
export function validConsoleOverrides(
  values: Record<string, SettingValue>,
): { valid: Partial<TunableSettings>; rejected: string[] } {
  const valid: Partial<Record<TunableKey, SettingValue>> = {};
  const rejected: string[] = [];
  for (const [key, value] of Object.entries(values)) {
    if (RETIRED_SETTINGS.has(key)) continue;
    const definition = BY_KEY.get(key);
    if (!definition || consoleSettingProblem(definition, value) !== null) {
      rejected.push(key);
      continue;
    }
    valid[definition.key] = definition.kind === 'string' ? (value as string).trim() : value;
  }
  return { valid: valid as Partial<TunableSettings>, rejected };
}

/** Every overridable key has a definition; a test holds this, so a new key cannot go undescribed. */
export function undescribedTunableKeys(): TunableKey[] {
  return TUNABLE_KEYS.filter((key) => !BY_KEY.has(key));
}
