/**
 * Live mode, decided from what is on screen: whether the Data tab asks for the live tail's newest
 * records now, and what its status line says of them.
 *
 * Kept free of React so it can be tested as plain functions: the hooks in `useRun.ts` and the
 * Data tab only wire these to timers and to the page's visibility.
 */

import type { CpMaterialization, RunSnapshot, RunTail, TimeRange } from '@shared/types';

import { RUN_TAIL_SWITCHES } from '../../../shared/types.ts';

/**
 * Where a page stands, as part of its cache key: `full` and `final` pages never change; a partial
 * page is keyed by how many of its rows are ready, so it is fetched again as more arrive.
 */
export function pageStage(run: RunSnapshot | null, offset: number, limit: number): string {
  if (run === null) return 'none';
  const ready = Math.min(run.rowsReady, offset + limit);
  if (ready >= offset + limit) return 'full';
  return run.state !== 'running' ? 'final' : String(ready);
}

/**
 * A page's cache key. A refresh that appends the tail's newest records moves every row of the run
 * down, so the run's `revision` is part of it: a page read before is read again.
 */
export function pageQueryKey(run: RunSnapshot | null, offset: number, limit: number) {
  return ['run-page', run?.id ?? null, run?.revision ?? 0, offset, limit, pageStage(run, offset, limit)] as const;
}

/**
 * Whether the materialization's runs can follow the live tail in this console: on here and in the
 * control plane, and the materialization running - the control plane tails a running one only.
 * Every materialization has a live tail; a control plane before 4.3 may still say one is off.
 */
export function liveTailAvailable(
  configEnabled: boolean | undefined,
  materialization: Pick<CpMaterialization, 'live_tail' | 'desired_phase'> | undefined,
): boolean {
  if (configEnabled !== true || materialization === undefined) return false;
  return materialization.live_tail !== false && (materialization.desired_phase ?? 'running') === 'running';
}

/** A tail turned off by a switch that can be turned back on (`RUN_TAIL_SWITCHES`). */
function switchedOff(tail: RunTail | undefined): boolean {
  return tail?.status === 'off' && RUN_TAIL_SWITCHES.includes(tail.reason ?? '');
}

/**
 * Whether the Data tab can run a query on a table: once something is committed, or - with live tail
 * on - as soon as the table exists, its records coming from the tail until the first commit.
 */
export function tableQueryable(input: {
  /** The table's summary says it holds committed data. */
  available: boolean | undefined;
  liveTail: boolean;
  /** The control plane has created the table and writes it. */
  created: boolean;
}): boolean {
  return input.available === true || (input.liveTail && input.created);
}

/** A snapshot as a run read it, or as the table's summary describes it. */
interface SnapshotRead {
  snapshotId: string | null;
  snapshotAt: string | null;
}

/**
 * Whether the table has a snapshot newer than the one a run read: committed after it. Live replaces
 * a run as soon as a commit lands, so the run can be ahead of the table's summary, which the Data tab
 * reads every 30 s; a different snapshot is newer only when it was committed later.
 */
export function newerSnapshot(read: SnapshotRead | undefined, summary: SnapshotRead | null): boolean {
  if (!read || !summary?.snapshotId || summary.snapshotId === read.snapshotId) return false;
  const summaryAt = Date.parse(summary.snapshotAt ?? '');
  const readAt = Date.parse(read.snapshotAt ?? '');
  // Without both times there is no telling which is the newer: a different one is offered.
  if (Number.isNaN(summaryAt) || Number.isNaN(readAt)) return true;
  return summaryAt > readAt;
}

/**
 * Whether the head peek has done its work: a finished run's tail is neither backlogged nor short of
 * the head, so the run shows the newest records itself. A run still running is no answer - Live
 * starts one at every commit, and its tail reads `empty` until it is fetched - or a backfill's
 * peek would close within seconds of being opened.
 */
export function peekDone(run: RunSnapshot | null): boolean {
  if (run === null || run.state === 'running') return false;
  const status = run.tail?.status;
  return status !== undefined && status !== 'backlogged' && status !== 'truncated';
}

/** A range whose end has passed: nothing that arrives now has a timestamp inside it. */
export function rangeEnded(range: TimeRange, now: number = Date.now()): boolean {
  return range.to !== null && range.to <= now;
}

/** Why Live is not asking for the tail just now, or null when it is. */
export type LiveHold =
  /** Live is off, or the materialization has no live tail. */
  | 'off'
  /** The tab is hidden: Live waits until it is seen again. */
  | 'hidden'
  /** No run yet, or the run is still working: the next look comes once it has finished. */
  | 'waiting'
  /** A statement: Live never refreshes one; its Auto re-run does. */
  | 'statement'
  /** The run reads an older snapshot: nothing newer belongs in it. */
  | 'pinned'
  /** The run failed or was cancelled: Run starts again. */
  | 'stopped'
  /** The tail is off for this run for good: no tail app is configured for the console. */
  | 'tail_off'
  /** The run's range has ended: nothing new can arrive in it. */
  | 'range_ended'
  /** A row is open in the drawer: new rows would move it from under the reader. */
  | 'inspecting'
  /** Past the first page, newest first: new rows would shift every page being read. */
  | 'paged'
  /** The session is gone or no longer allows the query: the sign-in prompt takes over. */
  | 'signed_out';

export interface LiveInput {
  /** The toggle, and whether the materialization can follow its tail at all. */
  enabled: boolean;
  visible: boolean;
  run: RunSnapshot | null;
  /** A refresh was refused for the person's session or access. */
  authRefused: boolean;
  /** A row of the grid is open in the drawer. */
  inspecting?: boolean;
  /** Which page is on screen, 0 for the first. */
  page?: number;
  now?: number;
}

/** What holds Live back from asking for the tail, or null when it should ask. */
export function liveHold({ enabled, visible, run, authRefused, inspecting = false, page = 0, now = Date.now() }: LiveInput): LiveHold | null {
  if (!enabled) return 'off';
  if (authRefused) return 'signed_out';
  if (run === null || run.state === 'running') return 'waiting';
  if (run.request.kind === 'sql') return 'statement';
  if (run.request.asOf) return 'pinned';
  if (run.state !== 'succeeded') return 'stopped';
  // Switched off for this run, on again as far as this tab knows: asking lets a new run follow it.
  if (run.tail?.status === 'off' && !switchedOff(run.tail)) return 'tail_off';
  if (rangeEnded(run.request.range, now)) return 'range_ended';
  if (!visible) return 'hidden';
  if (inspecting) return 'inspecting';
  // Oldest first, new rows go at the end and every page keeps its rows.
  if (page > 0 && (run.request.order ?? 'newest') === 'newest') return 'paged';
  return null;
}

/** The Auto re-run of a statement waits this long after the previous run finished. */
export const AUTO_RERUN_DELAY_MS = 5_000;

/**
 * How long until a statement's Auto re-run starts a new run, or null when it should not: only a
 * statement that succeeded, never while one runs, never for a draft that differs from what ran.
 */
export function autoRerunDelay(input: {
  enabled: boolean;
  visible: boolean;
  run: RunSnapshot | null;
  /** The editor holds a statement other than the one that ran. */
  dirty: boolean;
  now?: number;
}): number | null {
  const { enabled, visible, run, dirty, now = Date.now() } = input;
  if (!enabled || !visible || dirty || run === null) return null;
  if (run.request.kind !== 'sql' || run.state !== 'succeeded' || run.finishedAt === null) return null;
  return Math.max(0, Date.parse(run.finishedAt) + AUTO_RERUN_DELAY_MS - now);
}

/**
 * What the Data tab says of a run's live tail, in plain words, and how loudly: a `warning` is in
 * amber at the status line's right; anything `muted` is a few words beside it.
 */
export interface TailNotice {
  tone: 'muted' | 'warning';
  text: string;
  /** More of the reason, for a tooltip. */
  detail: string | null;
  /** The table is far behind its newest records: a peek at them helps. */
  peek: boolean;
  /** The view is short of the newest records, as opposed to Live having stopped. */
  behind?: boolean;
}

/** A lag past this is said: Live is a few seconds behind the newest records. */
export const LAG_NOTICE_MS = 5_000;

/**
 * What the Data tab says of a run's tail under Live, or of its tail alone when Live is off.
 *
 * Nothing while Live simply follows: the toggle's pulsing dot says so. The words are the reader's,
 * never the system's: how far behind the view is, not which part of the system is behind.
 * `hold` is why Live is not following (`liveHold`), which decides what to say when the tail
 * itself is fine: an ended range, a hidden tab. `lag` is how far the table's topic is ahead of what
 * the table holds, as the Overview measures it (once a minute, streamed to every open page): while
 * the table is backlogged, the view holds nothing past the table, so that is how far behind it is,
 * kept current whether or not Live refreshes the run.
 */
export function tailNotice(
  tail: RunTail | undefined,
  options: { live: boolean; hold: LiveHold | null; maxRows: number | null; lag?: number | null },
): TailNotice | null {
  const { live, hold } = options;
  if (!tail || tail.status === 'off') {
    // Switched off while Live follows it: said until the toggle goes too (the config is read again each minute).
    if (live && switchedOff(tail)) return { tone: 'muted', text: 'Live is off', detail: tail?.message ?? null, peek: false };
    return null;
  }
  switch (tail.status) {
    case 'backlogged':
      return {
        tone: 'warning',
        text: `${formatCount(options.lag !== null && options.lag !== undefined && options.lag > 0 ? options.lag : tail.behind)} records behind`,
        detail: 'The newest records show here once the table catches up.',
        peek: true,
        behind: true,
      };
    case 'unavailable':
      if (tail.reason === 'not_tailed' || tail.reason === 'not_ready') {
        return { tone: 'muted', text: 'Live is starting', detail: tail.message ?? null, peek: false };
      }
      if (tail.reason === 'rebalancing') return { tone: 'muted', text: 'Live is reconnecting', detail: tail.message ?? null, peek: false };
      return { tone: 'warning', text: 'Live is unavailable', detail: tail.message ?? 'Showing the rows saved so far.', peek: false };
    case 'truncated':
      return {
        tone: 'warning',
        text: tail.behind > 0 ? `${formatCount(tail.behind)} records behind` : 'Catching up',
        detail: tail.message ?? null,
        peek: options.maxRows !== null && tail.behind > options.maxRows,
        behind: true,
      };
    case 'complete':
    case 'empty': {
      if (tail.settling > 0) {
        return {
          tone: 'muted',
          text: `${formatCount(tail.settling)} ${tail.settling === 1 ? 'row has' : 'rows have'} new fields`,
          detail: 'Their new fields show under _unmapped until the table adds them.',
          peek: false,
        };
      }
      if (tail.lagMs !== null && tail.lagMs >= LAG_NOTICE_MS) {
        return { tone: 'muted', text: `Live is ${Math.round(tail.lagMs / 1000)} s behind`, detail: null, peek: false };
      }
      if (!live) return null;
      if (hold === 'range_ended') return { tone: 'muted', text: 'This time range has ended', detail: 'Nothing new can arrive in it.', peek: false };
      if (hold === 'hidden') return { tone: 'muted', text: 'Live paused while this tab is hidden', detail: null, peek: false };
      if (hold === 'inspecting') return { tone: 'muted', text: 'Live paused while a row is open', detail: null, peek: false };
      if (hold === 'paged') return { tone: 'muted', text: 'Live paused past the first page', detail: 'Go back to the first page to follow new records.', peek: false };
      if (hold === 'signed_out') return { tone: 'warning', text: 'Live stopped: sign in again to follow new records', detail: null, peek: false };
      return null;
    }
  }
}

function formatCount(value: number): string {
  return value.toLocaleString('en-US');
}
