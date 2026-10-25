/**
 * A run's live tail of one table: the records past the table's snapshot, fetched from the tail app
 * and kept as Parquet files in the run's own directory, each one more `DataObject` of the run.
 *
 * The first fetch asks for the records past the snapshot's offsets — the last offset of each
 * partition the snapshot holds — and each refresh asks past the last fetch's `through`. The files
 * are therefore disjoint and follow on from each other and from the snapshot, so the snapshot and
 * the files together hold every record up to `through` exactly once (design §7). A fetch that
 * fails changes nothing the run holds: only the tail's status says so, and the next refresh asks
 * again from where the run stands.
 */

import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { logger } from '../logger.ts';
import { readAll, withConnection } from '../data/duckdb.ts';
import { quoteLiteral } from '../data/sql.ts';
import type { DataObject } from '../data/catalog.ts';
import type { Catalog } from '../data/catalog.ts';
import type { TailOutcome } from '../data/tail.ts';
import { RUN_TAIL_SWITCHES, type RunTail, type RunTailState, type RunTailStatus } from '../../shared/types.ts';

/** How a run reads tails: why it reads none, or how it fetches one table's. */
export interface TailAccess {
  /** Why the run reads no tail of any table — live tail off somewhere, or not configured — or null. */
  off: { reason: string; message: string } | null;
  /** A table's records past `after`. Never throws: every failure is an outcome (`data/tail.ts`). */
  fetch: (table: string, after: Record<string, number>, signal: AbortSignal) => Promise<TailOutcome>;
}

/** Why a tail is off by a switch that can be turned back on (`routes/system.ts::liveTailOff`). */
const SWITCHES: ReadonlySet<string> = new Set(RUN_TAIL_SWITCHES);

/** What one fetch did to a tail. */
export type TailFetchResult =
  /** It found records, now a file of the run's. */
  | { kind: 'appended'; object: DataObject }
  /** Nothing new, or nothing could be fetched; the tail's status says which. */
  | { kind: 'unchanged' }
  /** The tail no longer holds records this far back: only a newer snapshot — a new run — has a tail. */
  | { kind: 'stale' };

export class TableTail {
  readonly table: string;
  /** Files of this tail, oldest first: one per fetch that found records. */
  readonly objects: DataObject[] = [];
  /** Per partition, the last offset the run holds: the snapshot's, then each fetch's `through`. */
  private through: Record<string, number>;
  private status: RunTailStatus;
  private state: RunTailState | null = null;
  private reason: string | null = null;
  private message: string | null = null;
  private endOffsets: Record<string, number> = {};
  private behindBacklog: number | null = null;
  private settling = 0;
  private lagMs: number | null = null;
  private member: string | null = null;
  private generatedAt: string | null = null;
  private fetches = 0;
  private lastStatus: 'complete' | 'truncated' | 'empty' | null = null;

  constructor(table: string, catalog: Catalog, off: { reason: string; message: string } | null) {
    this.table = table;
    this.through = { ...(catalog.snapshot?.offsets ?? {}) };
    if (off) {
      this.status = 'off';
      this.reason = off.reason;
      this.message = off.message;
    } else if (catalog.objects.length > 0 && Object.keys(this.through).length === 0) {
      // Rows with no offsets cannot be told apart from the tail's: showing both could repeat them.
      this.status = 'unavailable';
      this.reason = 'unknown_offsets';
      this.message = 'This snapshot does not record the Kafka offsets it holds, so its tail cannot be placed after it.';
    } else {
      this.status = 'empty';
    }
  }

  /** Whether the tail is done fetching: off, or never placeable after this snapshot. */
  get stopped(): boolean {
    return this.status === 'off' || this.reason === 'unknown_offsets';
  }

  /**
   * Off by a switch — this console's, the control plane's, the materialization's, or the
   * materialization stopped — that can be turned back on; not because no tail app is configured.
   */
  get switchedOff(): boolean {
    return this.status === 'off' && this.reason !== null && SWITCHES.has(this.reason);
  }

  /** Rows the run holds from this tail. */
  get rows(): number {
    return this.objects.reduce((sum, object) => sum + (object.rows ?? 0), 0);
  }

  /** Turn the tail off: nothing more is fetched; what the run holds stays. */
  turnOff(reason: string, message: string): void {
    this.status = 'off';
    this.reason = reason;
    this.message = message;
  }

  /**
   * Forget every file of this tail — one went missing — so the run pages its committed rows alone.
   * The next refresh would not be contiguous with what is left, so the tail stops too.
   */
  lose(): DataObject[] {
    const lost = this.objects.splice(0);
    this.status = 'unavailable';
    this.reason = 'lost';
    this.message = 'The live tail’s rows of this run are gone from the console’s disk; run the query again for them.';
    this.through = { ...this.through };
    return lost;
  }

  /**
   * Fetch the records past `through` and keep them as a further file in `directory`.
   *
   * `wanted` is asked once the answer is in, before anything is written: a run disposed meanwhile
   * keeps nothing. A file DuckDB cannot read, or reads a different number of rows from, is dropped
   * and the tail says so; the run goes on without it.
   */
  async fetch(access: TailAccess, directory: string, signal: AbortSignal, wanted: () => boolean = () => true): Promise<TailFetchResult> {
    if (this.stopped) return { kind: 'unchanged' };
    const after = { ...this.through };
    const outcome = await access.fetch(this.table, after, signal);
    if (!wanted()) return { kind: 'unchanged' };

    if (outcome.kind === 'off') {
      this.turnOff(outcome.reason, outcome.message);
      return { kind: 'unchanged' };
    }
    if (outcome.kind === 'backlogged') {
      this.status = 'backlogged';
      this.state = 'backlogged';
      this.reason = null;
      this.message = outcome.message;
      this.endOffsets = outcome.endOffsets;
      this.behindBacklog = outcome.behind;
      return { kind: 'unchanged' };
    }
    if (outcome.kind === 'unavailable') {
      this.status = 'unavailable';
      this.state = outcome.state;
      this.reason = outcome.reason;
      this.message = outcome.message;
      return outcome.reason === 'unavailable_for_snapshot' ? { kind: 'stale' } : { kind: 'unchanged' };
    }

    let object: DataObject | null = null;
    if (outcome.rows > 0) {
      const path = join(directory, `tail-${this.table}-${this.fetches}.parquet`);
      try {
        await mkdir(directory, { recursive: true });
        await writeFile(path, outcome.bytes);
        const read = await countRows(path);
        if (read !== outcome.rows) throw new Error(`the file holds ${read} rows, the tail said ${outcome.rows}`);
      } catch (cause) {
        logger.warn({ err: cause, table: this.table }, 'live tail file unreadable');
        await rm(path, { force: true }).catch(() => undefined);
        this.status = 'unavailable';
        this.reason = 'unreadable';
        this.message = 'The live tail’s answer could not be read.';
        return { kind: 'unchanged' };
      }
      object = {
        uri: path,
        // Zero-padded, so newer fetches sort first among files of equal time (`browse.ts::orderObjects`).
        name: `tail-${this.table}-${String(this.fetches).padStart(6, '0')}`,
        bytes: outcome.bytes.length,
        rows: outcome.rows,
        committedAt: null,
        partition: null,
        offsetStart: null,
        offsetEnd: null,
        timeMin: outcome.timeMin,
        timeMax: outcome.timeMax,
        // The tail reports the bounds of every timestamp it served; none means no row has one.
        timeKnown: true,
        partitionKey: null,
        tail: true,
      };
      this.objects.push(object);
      this.fetches += 1;
      this.settling += outcome.settling;
    }

    // Contiguous with what the run holds, whatever the answer held: `through` only moves forward.
    for (const [partition, offset] of Object.entries(outcome.through)) {
      this.through[partition] = Math.max(this.through[partition] ?? -1, offset);
    }
    this.endOffsets = outcome.endOffsets;
    this.behindBacklog = null;
    this.lastStatus = outcome.status;
    this.state = outcome.state;
    this.reason = null;
    this.message = null;
    this.lagMs = outcome.lagMs;
    this.member = outcome.member;
    this.generatedAt = outcome.generatedAt;
    this.status = outcome.status === 'truncated' ? 'truncated' : this.objects.length > 0 ? 'complete' : 'empty';
    return object === null ? { kind: 'unchanged' } : { kind: 'appended', object };
  }

  /** Per partition, the last offset the run holds of the table. */
  offsets(): Record<string, number> {
    return { ...this.through };
  }

  view(): RunTail {
    return {
      status: this.status,
      state: this.state,
      reason: this.reason,
      message: this.message,
      rows: this.rows,
      segments: this.objects.length,
      through: { ...this.through },
      endOffsets: { ...this.endOffsets },
      behind: this.behindBacklog ?? behind(this.endOffsets, this.through),
      settling: this.settling,
      lagMs: this.lagMs,
      member: this.member,
      generatedAt: this.generatedAt,
    };
  }

  /** The answer before the last, for tests: what the last fetch reported. */
  get lastAnswerStatus(): 'complete' | 'truncated' | 'empty' | null {
    return this.lastStatus;
  }
}

/** Records on the topic past what the run holds: the sum over partitions of `end − through`. */
export function behind(endOffsets: Record<string, number>, through: Record<string, number>): number {
  let total = 0;
  for (const [partition, end] of Object.entries(endOffsets)) {
    total += Math.max(0, end - (through[partition] ?? -1));
  }
  return total;
}

async function countRows(path: string): Promise<number> {
  return withConnection(async (connection) => {
    const result = await readAll(connection, `SELECT count(*)::BIGINT AS n FROM read_parquet(${quoteLiteral(path)})`);
    return Number(result.rows[0]?.n ?? -1);
  });
}
