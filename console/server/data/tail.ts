/**
 * The live tail: a table's records newer than its committed snapshot, from the tail app.
 *
 * The tail app (handbook 19) reads each table's topic and keeps the records its runtime has not
 * committed yet, built into rows exactly as the seal will write them. A run asks it for the records
 * past its snapshot's offsets — `materializer.offsets`, the last offset of each partition the
 * snapshot holds — and gets one Parquet file in the table's own physical format, which reads like
 * any data file. Snapshot and tail are disjoint and contiguous, so together they hold every record
 * up to the tail's `through` exactly once; each later fetch continues at the last one's `through`.
 *
 * Every call carries the person's own session, which the tail app checks with the control plane,
 * and the console's credential in `X-Console-Token`: the endpoint is this console's alone.
 * Nothing here throws for the tail's sake. Every answer, refusal and failure is a `TailOutcome`, so a
 * run that cannot have its tail goes on with committed rows and says why.
 */

import { randomUUID } from 'node:crypto';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { config } from '../config.ts';
import { logger } from '../logger.ts';
import { readAll, withConnection } from './duckdb.ts';
import { logicalColumns, readParquet, selectList, toQueryColumns } from './logical.ts';
import type { UserCaller } from '../controlPlane/client.ts';
import type { IcebergSchema } from '../iceberg/metadata.ts';
import type { ForkMap } from '../iceberg/schema.ts';
import type { QueryColumn, RunTailState } from '../../shared/types.ts';

/** Records past `after`, as one Parquet file, and what the tail app's headers say of them. */
export interface TailRows {
  kind: 'rows';
  bytes: Buffer;
  rows: number;
  /** `truncated`: a cap or a paused partition stopped the answer short of what the tail has read. */
  status: 'complete' | 'truncated' | 'empty';
  after: Record<string, number>;
  /** Per partition, the last offset the answer covers: every record past `after` up to it is in it. */
  through: Record<string, number>;
  /** Per partition, the topic's last offset as the tail last saw it. */
  endOffsets: Record<string, number>;
  paused: number[];
  /** Partitions whose records past `after` retention had already removed: served from Kafka's beginning. */
  gap: number[];
  schemaId: number | null;
  settling: number;
  /** Epoch milliseconds of the rows' Kafka timestamps; null when no row has one. */
  timeMin: number | null;
  timeMax: number | null;
  lagMs: number | null;
  state: RunTailState | null;
  member: string | null;
  tableUuid: string | null;
  generatedAt: string | null;
}

/** The runtime is too far behind the topic for the tail to apply: it reads nothing until it catches up. */
export interface TailBacklogged {
  kind: 'backlogged';
  behind: number;
  endOffsets: Record<string, number>;
  committed: Record<string, number>;
  message: string;
}

/**
 * Why there is no tail just now:
 *
 * - `not_ready`: the tail is warming up for the table (seeding, catching up); try again shortly.
 * - `rebalancing`: the table is moving between the tail app's members.
 * - `unavailable_for_snapshot`: the tail no longer holds records this far back; a newer snapshot
 *   (a new run) has its tail.
 * - `topic_unavailable`: the tail cannot read the table's topic.
 * - `unreachable`: the tail app, its member that owns the table, or the control plane it checks
 *   sessions with could not be reached; `timeout` when it did not answer in time.
 * - `forbidden`: the tail app refused the person, or this console's credential.
 * - `unreadable`: it answered something that is not a tail: a short body, missing headers, not Parquet.
 * - `rejected`: it refused the request itself, which is the console's bug.
 */
export interface TailUnavailable {
  kind: 'unavailable';
  reason: string;
  message: string;
  /** The tail app's status code; null when it did not answer. */
  status: number | null;
  state: RunTailState | null;
}

/** No tail can be asked for from this console: no tail app, or no credential to call it with, is configured. */
export interface TailOff {
  kind: 'off';
  reason: string;
  message: string;
}

export type TailOutcome = TailRows | TailBacklogged | TailUnavailable | TailOff;

/** The newest records on a table's topic, committed or not, for a peek while the table is backlogged. */
export interface HeadRows {
  kind: 'rows';
  bytes: Buffer;
  rows: number;
  offsets: Record<string, [number, number]>;
  endOffsets: Record<string, number>;
  behind: number;
  state: RunTailState | null;
  schemaId: number | null;
  settling: number;
  member: string | null;
  generatedAt: string | null;
}

export type HeadOutcome = HeadRows | TailUnavailable | TailOff;

export interface TailRequest {
  materializationId: string;
  table: string;
  /** Per partition, the last offset the caller has; a partition left out is read from its beginning. */
  after: Record<string, number>;
  maxRows: number;
  timeoutMs: number;
  signal?: AbortSignal;
}

/** The header the tail app requires the console's credential in. */
export const CONSOLE_HEADER = 'x-console-token';

/** How much sooner than the console's own timeout the tail app is asked to answer. */
const WAIT_MARGIN_MS = 500;
/** The longest the tail app waits for a table that is warming up (`tail_app/views.py::MAX_WAIT_MS`). */
const MAX_WAIT_MS = 5_000;

const STATES: ReadonlySet<string> = new Set(['seeding', 'catching_up', 'live', 'paused', 'backlogged']);

/**
 * Where a domain's tail app is, and the domain's service token it requires in `X-Console-Token`
 * (handbook 19 §19.10): every domain has its own (handbook 20).
 */
export interface TailTarget {
  /** The domain, for what is said when it has no tail app. */
  name: string;
  tailUrl: string | null;
  serviceToken: string | null;
}

/** Records of a table past `after`. Never throws: every failure is an outcome. */
export async function fetchTail(target: TailTarget, caller: UserCaller, request: TailRequest): Promise<TailOutcome> {
  const query = new URLSearchParams({
    after: JSON.stringify(request.after),
    max_rows: String(Math.max(1, Math.floor(request.maxRows))),
    wait_ms: String(waitMs(request.timeoutMs)),
  });
  const answer = await call(target, caller, `${tablePath(request.materializationId, request.table)}?${query}`, request.timeoutMs, request.signal);
  if (answer.kind !== 'answer') return answer;
  if (answer.status !== 200) return refusal(answer.status, answer.body);
  return readRows(answer.headers, answer.body, request.after);
}

/** The newest `rows` records on a table's topic. Never throws: every failure is an outcome. */
export async function fetchHead(
  target: TailTarget,
  caller: UserCaller,
  request: { materializationId: string; table: string; rows: number; timeoutMs: number; signal?: AbortSignal },
): Promise<HeadOutcome> {
  const query = new URLSearchParams({ rows: String(Math.max(1, Math.floor(request.rows))) });
  const answer = await call(target, caller, `${tablePath(request.materializationId, request.table)}/head?${query}`, request.timeoutMs, request.signal);
  if (answer.kind !== 'answer') return answer;
  if (answer.status !== 200) {
    const outcome = refusal(answer.status, answer.body);
    // A backlogged table is what a peek is for; the tail app answers it with rows, never this.
    return outcome.kind === 'backlogged' ? unavailableOutcome('unreadable', outcome.message, answer.status) : outcome;
  }
  const { headers, body } = answer;
  const problem = parquetProblem(body, headers);
  if (problem) return unavailableOutcome('unreadable', problem, 200);
  const offsets = jsonHeader(headers, 'x-tail-peek-offsets');
  return {
    kind: 'rows',
    bytes: body,
    rows: intHeader(headers, 'x-tail-peek-rows') ?? 0,
    offsets: isRecord(offsets) ? peekOffsets(offsets) : {},
    endOffsets: offsetsHeader(headers, 'x-tail-end-offsets') ?? {},
    behind: intHeader(headers, 'x-tail-behind') ?? 0,
    state: stateHeader(headers),
    schemaId: intHeader(headers, 'x-tail-schema-id'),
    settling: intHeader(headers, 'x-tail-settling') ?? 0,
    member: headers.get('x-tail-member'),
    generatedAt: headers.get('x-tail-generated-at'),
  };
}

/**
 * A peek's records as the table's columns — forks folded, nested fields named as the grid names
 * them — newest first. Read on the shared DuckDB instance from a file of its own, removed after.
 */
export async function readPeek(
  bytes: Buffer,
  table: { schema: IcebergSchema | null; forkMap: ForkMap },
  limit: number,
): Promise<{ rows: Record<string, unknown>[]; columns: QueryColumn[] }> {
  const columns = logicalColumns(table.schema, table.forkMap);
  if (table.schema === null || bytes.length === 0) return { rows: [], columns: toQueryColumns(columns) };
  const directory = join(config.runDir, 'peeks');
  const path = join(directory, `${randomUUID()}.parquet`);
  await mkdir(directory, { recursive: true });
  await writeFile(path, bytes);
  try {
    const schema = table.schema;
    const result = await withConnection((connection) =>
      readAll(
        connection,
        `SELECT ${selectList(columns)} FROM ${readParquet([path], schema)} ` +
          `ORDER BY "_kafka_timestamp" DESC NULLS LAST, "_kafka_offset" DESC, "_kafka_partition" LIMIT ${Math.max(0, Math.floor(limit))}`,
      ),
    );
    return { rows: result.rows, columns: toQueryColumns(columns) };
  } finally {
    await rm(path, { force: true }).catch(() => undefined);
  }
}

/* ------------------------------------------------------------------------------------- calling */

type Answer = { kind: 'answer'; status: number; headers: Headers; body: Buffer };

async function call(target: TailTarget, caller: UserCaller, path: string, timeoutMs: number, signal?: AbortSignal): Promise<Answer | TailUnavailable | TailOff> {
  const base = target.tailUrl;
  if (!base) return { kind: 'off', reason: 'not_configured', message: `No live tail is configured for ${target.name}.` };
  const credential = target.serviceToken;
  if (!credential) {
    return { kind: 'off', reason: 'not_configured', message: `This console has no service token for ${target.name}, which its live tail requires.` };
  }
  const timeout = AbortSignal.timeout(Math.max(1, timeoutMs));
  const composed = signal ? AbortSignal.any([signal, timeout]) : timeout;
  const headers: Record<string, string> = { accept: 'application/octet-stream, application/json', [CONSOLE_HEADER]: credential };
  if (caller.token) headers.authorization = `Bearer ${caller.token}`;
  try {
    const response = await fetch(`${base}${path}`, { headers, signal: composed });
    const body = Buffer.from(await response.arrayBuffer());
    return { kind: 'answer', status: response.status, headers: response.headers, body };
  } catch (cause) {
    if (timeout.aborted) return unavailableOutcome('timeout', `The live tail did not answer within ${formatMs(timeoutMs)}.`, null);
    if (signal?.aborted) return unavailableOutcome('cancelled', 'The run was cancelled.', null);
    logger.debug({ err: cause, path }, 'live tail unreachable');
    return unavailableOutcome('unreachable', 'The live tail could not be reached.', null);
  }
}

function tablePath(materializationId: string, table: string): string {
  return `/tail/v1/materializations/${encodeURIComponent(materializationId)}/tables/${encodeURIComponent(table)}`;
}

/** How long the tail app may wait for a warming table: answering before the console gives up on it. */
function waitMs(timeoutMs: number): number {
  return Math.max(0, Math.min(MAX_WAIT_MS, Math.floor(timeoutMs) - WAIT_MARGIN_MS));
}

/* ------------------------------------------------------------------------------------ answers */

function readRows(headers: Headers, body: Buffer, after: Record<string, number>): TailRows | TailUnavailable {
  const through = offsetsHeader(headers, 'x-tail-through');
  if (through === null) return unavailableOutcome('unreadable', 'The live tail answered without saying how far its rows go.', 200);
  const rows = intHeader(headers, 'x-tail-rows');
  if (rows === null || rows < 0) return unavailableOutcome('unreadable', 'The live tail answered without a row count.', 200);
  const problem = parquetProblem(body, headers);
  if (problem) return unavailableOutcome('unreadable', problem, 200);
  // An answer ending before what the caller has would leave a record counted twice, or never.
  for (const [partition, offset] of Object.entries(after)) {
    if (through[partition] !== undefined && through[partition] < offset) {
      return unavailableOutcome('unreadable', `The live tail answered partition ${partition} through ${through[partition]}, before ${offset}.`, 200);
    }
  }
  const reported = headers.get('x-tail-status');
  const status = reported === 'complete' || reported === 'truncated' || reported === 'empty' ? reported : rows === 0 ? 'empty' : 'complete';
  return {
    kind: 'rows',
    bytes: body,
    rows,
    status,
    after: offsetsHeader(headers, 'x-tail-after') ?? { ...after },
    through,
    endOffsets: offsetsHeader(headers, 'x-tail-end-offsets') ?? {},
    paused: intListHeader(headers, 'x-tail-paused'),
    gap: intListHeader(headers, 'x-tail-gap'),
    schemaId: intHeader(headers, 'x-tail-schema-id'),
    settling: intHeader(headers, 'x-tail-settling') ?? 0,
    timeMin: intHeader(headers, 'x-tail-time-min'),
    timeMax: intHeader(headers, 'x-tail-time-max'),
    lagMs: intHeader(headers, 'x-tail-lag-ms'),
    state: stateHeader(headers),
    member: headers.get('x-tail-member'),
    tableUuid: headers.get('x-tail-table-uuid') || null,
    generatedAt: headers.get('x-tail-generated-at'),
  };
}

/**
 * A non-2xx answer as an outcome. The body is the control plane's error shape whatever its content
 * type — `{"error": {"code", "message", "details"}}` — which Kaspr may send as bytes.
 */
export function refusal(status: number, body: Buffer): TailBacklogged | TailUnavailable {
  const error = errorBody(body);
  const reason = typeof error.details.reason === 'string' ? error.details.reason : error.code;
  const message = error.message ?? `The live tail answered ${status}.`;
  const state = typeof error.details.state === 'string' && STATES.has(error.details.state) ? (error.details.state as RunTailState) : null;
  if (status === 409 && reason === 'tail_backlogged') {
    return {
      kind: 'backlogged',
      behind: integer(error.details.behind) ?? 0,
      endOffsets: offsetsOf(error.details.end_offsets) ?? {},
      committed: offsetsOf(error.details.committed) ?? {},
      message,
    };
  }
  // Not followed there now - the materialization stopped (before 4.3, also its live tail off), or
  // not ticked yet - which a resume or the next tick changes: asked again next refresh.
  if (status === 404 && reason === 'live_tail_off') return unavailableOutcome('not_tailed', message, status);
  if (status === 404) {
    // Not the tail app's own answer: CONSOLE_TAIL_URL points somewhere that is not it.
    return unavailableOutcome('unreachable', `The live tail answered 404 (${message}); is CONSOLE_TAIL_URL the tail app?`, status);
  }
  if (status === 409 && reason === 'tail_rebalancing') return unavailableOutcome('rebalancing', message, status);
  if (status === 409 && reason === 'tail_unavailable') return unavailableOutcome('unavailable_for_snapshot', message, status);
  if (status === 503 && reason === 'tail_not_ready') return { ...unavailableOutcome('not_ready', message, status), state: state ?? 'seeding' };
  if (status === 503 && reason === 'topic_unavailable') return unavailableOutcome('topic_unavailable', message, status);
  if (status === 503 && reason === 'peek_failed') return unavailableOutcome('peek_failed', message, status);
  if (status === 401 || status === 403) return unavailableOutcome('forbidden', message, status);
  if (status === 400) return unavailableOutcome('rejected', message, status);
  return unavailableOutcome('unreachable', message, status);
}

function unavailableOutcome(reason: string, message: string, status: number | null): TailUnavailable {
  return { kind: 'unavailable', reason, message, status, state: null };
}

function errorBody(body: Buffer): { code: string; message: string | null; details: Record<string, unknown> } {
  try {
    const parsed = JSON.parse(body.toString('utf8')) as { error?: { code?: unknown; message?: unknown; details?: unknown } };
    const error = parsed?.error;
    if (error && typeof error === 'object') {
      return {
        code: typeof error.code === 'string' ? error.code : 'error',
        message: typeof error.message === 'string' ? error.message : null,
        details: isRecord(error.details) ? error.details : {},
      };
    }
  } catch {
    /* not JSON: said by the status alone */
  }
  return { code: 'error', message: null, details: {} };
}

/** Why a body is not the Parquet file it claims to be, or null. */
function parquetProblem(body: Buffer, headers: Headers): string | null {
  const declared = intHeader(headers, 'x-tail-bytes');
  if (declared !== null && declared !== body.length) {
    return `The live tail's answer was cut short: ${body.length} of ${declared} bytes arrived.`;
  }
  const magic = Buffer.from('PAR1');
  if (body.length < 12 || !body.subarray(0, 4).equals(magic) || !body.subarray(body.length - 4).equals(magic)) {
    return 'The live tail answered something that is not a Parquet file.';
  }
  return null;
}

/* ------------------------------------------------------------------------------------ headers */

function jsonHeader(headers: Headers, name: string): unknown {
  const raw = headers.get(name);
  if (raw === null || raw === '') return undefined;
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
}

function offsetsHeader(headers: Headers, name: string): Record<string, number> | null {
  return offsetsOf(jsonHeader(headers, name));
}

/** `{"0": 41, "3": -1}`: a partition's offset, -1 for none; anything else is not offsets. */
function offsetsOf(value: unknown): Record<string, number> | null {
  if (!isRecord(value)) return null;
  const offsets: Record<string, number> = {};
  for (const [partition, offset] of Object.entries(value)) {
    if (!/^\d{1,9}$/.test(partition) || !Number.isSafeInteger(offset) || (offset as number) < -1) return null;
    offsets[String(Number(partition))] = offset as number;
  }
  return offsets;
}

function peekOffsets(value: Record<string, unknown>): Record<string, [number, number]> {
  const offsets: Record<string, [number, number]> = {};
  for (const [partition, bounds] of Object.entries(value)) {
    if (Array.isArray(bounds) && bounds.length === 2 && bounds.every((item) => Number.isSafeInteger(item))) {
      offsets[partition] = [bounds[0] as number, bounds[1] as number];
    }
  }
  return offsets;
}

function intHeader(headers: Headers, name: string): number | null {
  const raw = headers.get(name);
  if (raw === null || !/^-?\d{1,18}$/.test(raw.trim())) return null;
  return Number(raw.trim());
}

function intListHeader(headers: Headers, name: string): number[] {
  const value = jsonHeader(headers, name);
  return Array.isArray(value) ? value.filter((item): item is number => Number.isSafeInteger(item)) : [];
}

function stateHeader(headers: Headers): RunTailState | null {
  const raw = headers.get('x-tail-state');
  return raw !== null && STATES.has(raw) ? (raw as RunTailState) : null;
}

function integer(value: unknown): number | null {
  return Number.isSafeInteger(value) ? (value as number) : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function formatMs(ms: number): string {
  return ms % 1000 === 0 ? `${ms / 1000} s` : `${(ms / 1000).toFixed(1)} s`;
}
