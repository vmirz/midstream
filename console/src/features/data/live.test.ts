import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { AUTO_RERUN_DELAY_MS, autoRerunDelay, liveHold, liveTailAvailable, newerSnapshot, pageQueryKey, peekDone, rangeEnded, tableQueryable, tailNotice } from './live.ts';
import { resolveRange } from './timeRange.ts';
import type { RunSnapshot, RunTail } from '@shared/types';

const NOW = Date.parse('2026-10-23T12:00:00.000Z');

function run(overrides: Partial<RunSnapshot> = {}, request: Partial<RunSnapshot['request']> = {}): RunSnapshot {
  return {
    id: 'run-1',
    domain: 'domain_a',
    materializationId: 'mat-1',
    request: { kind: 'browse', range: { from: null, to: null }, ...request },
    state: 'succeeded',
    phase: 'done',
    startedAt: '2026-10-23T11:59:58.000Z',
    finishedAt: '2026-10-23T11:59:59.000Z',
    elapsedMs: 1_000,
    table: 'orders',
    tables: [],
    asOf: '2026-10-23T11:59:58.000Z',
    snapshotId: '7',
    snapshotAt: null,
    pinned: false,
    validThrough: null,
    scope: { objects: 1, bytes: 1, rows: 1 },
    progress: { rowsTotal: 1, rowsDone: 1, objectsTotal: 1, objectsDone: 1, objectsRead: 0, bytesTotal: 1, fraction: 1 },
    rows: 10,
    totalExact: true,
    rowsReady: 10,
    columns: [],
    truncated: false,
    rowsRetained: null,
    error: null,
    expiresAt: null,
    revision: 0,
    ...overrides,
  };
}

function tail(overrides: Partial<RunTail> = {}): RunTail {
  return {
    status: 'complete',
    state: 'live',
    reason: null,
    message: null,
    rows: 3,
    segments: 1,
    through: { '0': 12 },
    endOffsets: { '0': 12 },
    behind: 0,
    settling: 0,
    lagMs: 0,
    member: 'materializer-tail-app-0',
    generatedAt: null,
    ...overrides,
  };
}

const following = { enabled: true, visible: true, authRefused: false, now: NOW };

describe('page keys', () => {
  it('change with the run’s revision, so a page read before a refresh is read again', () => {
    const before = pageQueryKey(run(), 0, 100);
    const after = pageQueryKey(run({ revision: 1 }), 0, 100);
    assert.notDeepEqual(before, after);
    assert.deepEqual(before, pageQueryKey(run(), 0, 100), 'and nothing else changes them for a finished run');
    assert.equal(pageQueryKey(null, 0, 100)[2], 0);
  });
});

describe('when Live asks for the tail', () => {
  it('asks only for a finished browse run that is seen, on its first page, with no row open', () => {
    assert.equal(liveHold({ ...following, run: run() }), null);
    assert.equal(liveHold({ ...following, run: run(), visible: false }), 'hidden');
    assert.equal(liveHold({ ...following, run: run({ state: 'running' }) }), 'waiting', 'never while a run is running');
    assert.equal(liveHold({ ...following, run: null }), 'waiting');
    assert.equal(liveHold({ ...following, run: run(), inspecting: true }), 'inspecting');
    assert.equal(liveHold({ ...following, run: run(), page: 2 }), 'paged');
    assert.equal(liveHold({ ...following, run: run({}, { order: 'oldest' }), page: 2 }), null, 'oldest first, new rows go at the end');
  });

  it('never refreshes a statement, a pinned run, or a run that failed', () => {
    assert.equal(liveHold({ ...following, run: run({}, { kind: 'sql', sql: 'SELECT 1' }) }), 'statement');
    assert.equal(liveHold({ ...following, run: run({}, { asOf: '6' }) }), 'pinned');
    assert.equal(liveHold({ ...following, run: run({ state: 'failed' }) }), 'stopped');
    assert.equal(liveHold({ ...following, run: run({ state: 'cancelled' }) }), 'stopped');
  });

  it('stops when the tail is off for good, when the session is refused, or when Live is off', () => {
    assert.equal(liveHold({ ...following, run: run({ tail: tail({ status: 'off', reason: 'not_configured' }) }) }), 'tail_off');
    assert.equal(liveHold({ ...following, run: run(), authRefused: true }), 'signed_out');
    assert.equal(liveHold({ ...following, run: run(), enabled: false }), 'off');
  });

  it('asks again for a tail switched off since, once the switches are on as far as the tab knows', () => {
    for (const reason of ['console', 'control_plane', 'materialization', 'stopped']) {
      assert.equal(liveHold({ ...following, run: run({ tail: tail({ status: 'off', reason }) }) }), null, `${reason}: a new run follows it`);
    }
    assert.equal(liveHold({ ...following, run: run({ tail: tail({ status: 'off', reason: 'control_plane' }) }), enabled: false }), 'off');
    assert.equal(liveHold({ ...following, run: run({ tail: tail({ status: 'unavailable', reason: 'not_tailed', rows: 0 }) }) }), null, 'a resume or a tick brings it back');
  });

  it('polls nothing for a range that has ended, and keeps a relative range open', () => {
    assert.equal(liveHold({ ...following, run: run({}, { range: { from: NOW - 86_400_000, to: NOW - 1 } }) }), 'range_ended');
    assert.equal(rangeEnded({ from: null, to: NOW + 1 }, NOW), false);
    const lastQuarter = { kind: 'last', amount: 15, unit: 'minute' } as const;
    const first = resolveRange(lastQuarter, NOW);
    const later = resolveRange(lastQuarter, NOW + 60_000);
    assert.equal(first.to, null, 'the rows the tail appends are never outside it');
    assert.equal(later.from, (first.from as number) + 60_000, 'the run that replaces it slides');
    assert.equal(liveHold({ ...following, run: run({}, { range: first }), now: NOW + 3_600_000 }), null);
  });

  it('is available for every materialization while it runs, where this console has live tail', () => {
    assert.equal(liveTailAvailable(true, { live_tail: true }), true);
    assert.equal(liveTailAvailable(true, {}), true, 'whether or not the control plane says so');
    assert.equal(liveTailAvailable(true, { live_tail: true, desired_phase: 'running' }), true);
    assert.equal(liveTailAvailable(true, { live_tail: false }), false, 'a control plane before 4.3 may still say one is off');
    assert.equal(liveTailAvailable(true, undefined), false, 'not before the materialization is known');
    assert.equal(liveTailAvailable(false, { live_tail: true }), false);
    assert.equal(liveTailAvailable(undefined, undefined), false);
    assert.equal(liveTailAvailable(true, { live_tail: true, desired_phase: 'stopped' }), false, 'the control plane tails a running one only');
    assert.equal(liveTailAvailable(true, { live_tail: true, desired_phase: 'deleting' }), false);
  });
});

describe('the head peek', () => {
  it('stays open through the runs Live starts during a backfill, and closes once one has caught up', () => {
    assert.equal(peekDone(run({ tail: tail({ status: 'backlogged', state: 'backlogged', rows: 0 }) })), false);
    assert.equal(peekDone(run({ tail: tail({ status: 'truncated' }) })), false);
    // Live's run after a commit, its tail still being fetched: `empty` says nothing yet.
    assert.equal(peekDone(run({ state: 'running', tail: tail({ status: 'empty', rows: 0 }) })), false);
    assert.equal(peekDone(null), false);
    assert.equal(peekDone(run({ tail: tail() })), true, 'the table caught up: the run shows the newest records itself');
    assert.equal(peekDone(run({ tail: tail({ status: 'empty', rows: 0 }) })), true);
    assert.equal(peekDone(run({ tail: undefined })), false, 'a run that asked for no tail leaves an open peek alone');
  });
});

describe('newer data', () => {
  const at = (id: string, time: string) => ({ snapshotId: id, snapshotAt: time });

  it('is a snapshot committed after the one the run read', () => {
    assert.equal(newerSnapshot(at('7', '2026-10-24T03:30:00Z'), at('8', '2026-10-24T03:30:40Z')), true);
    assert.equal(newerSnapshot(at('7', '2026-10-24T03:30:00Z'), at('7', '2026-10-24T03:30:00Z')), false);
  });

  it('is not the summary lagging behind a run Live replaced after a commit', () => {
    assert.equal(newerSnapshot(at('8', '2026-10-24T03:30:40Z'), at('7', '2026-10-24T03:30:00Z')), false);
  });

  it('is any other snapshot when a time is missing, and nothing without a run or a snapshot', () => {
    assert.equal(newerSnapshot(at('7', '2026-10-24T03:30:00Z'), { snapshotId: '8', snapshotAt: null }), true);
    assert.equal(newerSnapshot(undefined, at('8', '2026-10-24T03:30:40Z')), false);
    assert.equal(newerSnapshot(at('7', '2026-10-24T03:30:00Z'), null), false);
    assert.equal(newerSnapshot(at('7', '2026-10-24T03:30:00Z'), { snapshotId: null, snapshotAt: null }), false);
  });
});

describe('when the Data tab can query a table', () => {
  it('once it holds committed rows, or with live tail as soon as it exists', () => {
    assert.equal(tableQueryable({ available: true, liveTail: false, created: true }), true);
    assert.equal(tableQueryable({ available: false, liveTail: true, created: true }), true, 'its records come from the tail until the first commit');
    assert.equal(tableQueryable({ available: false, liveTail: false, created: true }), false);
    assert.equal(tableQueryable({ available: false, liveTail: true, created: false }), false, 'not before the table exists');
    assert.equal(tableQueryable({ available: undefined, liveTail: true, created: false }), false);
  });
});

describe('a statement’s Auto re-run', () => {
  const statement = run({ finishedAt: new Date(NOW - 2_000).toISOString() }, { kind: 'sql', sql: 'SELECT 1' });

  it('runs again five seconds after the last run finished', () => {
    assert.equal(autoRerunDelay({ enabled: true, visible: true, run: statement, dirty: false, now: NOW }), AUTO_RERUN_DELAY_MS - 2_000);
    assert.equal(autoRerunDelay({ enabled: true, visible: true, run: statement, dirty: false, now: NOW + 10_000 }), 0);
  });

  it('never while one runs, for a draft that differs, out of sight, or when off', () => {
    assert.equal(autoRerunDelay({ enabled: true, visible: true, run: { ...statement, state: 'running', finishedAt: null }, dirty: false, now: NOW }), null);
    assert.equal(autoRerunDelay({ enabled: true, visible: true, run: statement, dirty: true, now: NOW }), null);
    assert.equal(autoRerunDelay({ enabled: true, visible: false, run: statement, dirty: false, now: NOW }), null);
    assert.equal(autoRerunDelay({ enabled: false, visible: true, run: statement, dirty: false, now: NOW }), null);
    assert.equal(autoRerunDelay({ enabled: true, visible: true, run: { ...statement, state: 'failed' }, dirty: false, now: NOW }), null);
    assert.equal(autoRerunDelay({ enabled: true, visible: true, run: run(), dirty: false, now: NOW }), null, 'a browse run is Live’s');
  });
});

describe('what the Data tab says of the tail', () => {
  const live = { live: true, hold: null, maxRows: null };

  it('says nothing while Live follows, and why it is not following otherwise', () => {
    assert.equal(tailNotice(tail(), live), null, 'the toggle’s pulsing dot says it');
    assert.equal(tailNotice(tail(), { ...live, hold: 'hidden' })?.text, 'Live paused while this tab is hidden');
    assert.equal(tailNotice(tail(), { ...live, hold: 'inspecting' })?.text, 'Live paused while a row is open');
    assert.equal(tailNotice(tail(), { ...live, hold: 'paged' })?.text, 'Live paused past the first page');
    assert.equal(tailNotice(tail(), { ...live, hold: 'range_ended' })?.text, 'This time range has ended');
    assert.deepEqual(tailNotice(tail(), { ...live, hold: 'signed_out' }), {
      tone: 'warning',
      text: 'Live stopped: sign in again to follow new records',
      detail: null,
      peek: false,
    });
    assert.equal(tailNotice(tail(), { ...live, live: false }), null, 'how many rows the tail added is not said');
    assert.equal(tailNotice(tail({ rows: 0, status: 'empty' }), { ...live, live: false }), null);
  });

  it('tells each reason Live is short apart, in plain words', () => {
    assert.deepEqual([tailNotice(tail({ status: 'unavailable', reason: 'not_ready', rows: 0 }), live)?.tone, tailNotice(tail({ status: 'unavailable', reason: 'not_ready', rows: 0 }), live)?.text], ['muted', 'Live is starting']);
    assert.equal(tailNotice(tail({ status: 'unavailable', reason: 'rebalancing', rows: 0 }), live)?.text, 'Live is reconnecting');
    const unreachable = tailNotice(tail({ status: 'unavailable', reason: 'unreachable', rows: 0 }), live);
    assert.deepEqual([unreachable?.tone, unreachable?.text], ['warning', 'Live is unavailable']);
    const truncated = tailNotice(tail({ status: 'truncated', behind: 4_000, rows: 1_000 }), live);
    assert.deepEqual([truncated?.tone, truncated?.text], ['warning', '4,000 records behind'], 'the records not shown');
    assert.equal(tailNotice(tail({ settling: 2 }), live)?.text, '2 rows have new fields');
    assert.equal(tailNotice(tail({ settling: 1 }), live)?.text, '1 row has new fields');
    assert.equal(tailNotice(tail({ lagMs: 7_400 }), live)?.text, 'Live is 7 s behind');
  });

  it('never names the parts of the system', () => {
    const tails = [
      tail({ status: 'backlogged', state: 'backlogged', behind: 2_500_000, rows: 0 }),
      tail({ status: 'truncated', behind: 4_000, rows: 1_000 }),
      tail({ status: 'unavailable', reason: 'not_ready', rows: 0 }),
      tail({ status: 'unavailable', reason: 'rebalancing', rows: 0 }),
      tail({ status: 'unavailable', reason: 'unreachable', rows: 3 }),
      tail({ status: 'unavailable', reason: 'not_tailed', rows: 0 }),
      tail({ settling: 2 }),
      tail({ lagMs: 7_400 }),
      tail({ status: 'off', reason: 'control_plane' }),
    ];
    const holds = [null, 'hidden', 'inspecting', 'paged', 'range_ended', 'signed_out'] as const;
    for (const each of tails) {
      for (const hold of holds) {
        const text = tailNotice(each, { ...live, hold })?.text ?? '';
        assert.doesNotMatch(text, /runtime|tail|snapshot|commit|schema|topic|member/i, text);
      }
    }
  });

  it('counts a backlog by the topic’s measured lag, which stays current, and a short tail by what it does not show', () => {
    const backlogged = tail({ status: 'backlogged', state: 'backlogged', behind: 2_500_000, rows: 0 });
    assert.equal(tailNotice(backlogged, { ...live, lag: 2_412_000 })?.text, '2,412,000 records behind');
    assert.equal(tailNotice(backlogged, { ...live, lag: null })?.text, '2,500,000 records behind', 'no measurement yet');
    assert.equal(tailNotice(backlogged, { ...live, lag: 0 })?.text, '2,500,000 records behind', 'a lag of nothing is no measurement of a backlog');
    assert.equal(tailNotice(tail({ status: 'truncated', behind: 4_000, rows: 1_000 }), { ...live, lag: 5_000 })?.text, '4,000 records behind', 'the tail’s rows are on screen');
  });

  it('offers a peek while the table is backlogged, or far short of the head', () => {
    const backlogged = tailNotice(tail({ status: 'backlogged', state: 'backlogged', behind: 2_500_000, rows: 0 }), live);
    assert.deepEqual([backlogged?.tone, backlogged?.peek, backlogged?.text], ['warning', true, '2,500,000 records behind']);
    assert.equal(tailNotice(tail({ status: 'truncated', behind: 500 }), { ...live, maxRows: 100_000 })?.peek, false);
    assert.equal(tailNotice(tail({ status: 'truncated', behind: 500_000 }), { ...live, maxRows: 100_000 })?.peek, true);
  });

  it('says a tail is off only while Live follows one a switch turned off, and when the tail app does not follow it', () => {
    assert.equal(tailNotice(undefined, live), null);
    assert.equal(tailNotice(tail({ status: 'off', reason: 'not_configured' }), live), null);
    assert.equal(tailNotice(tail({ status: 'off', reason: 'materialization' }), { ...live, live: false }), null);
    const switched = tailNotice(tail({ status: 'off', reason: 'control_plane', message: 'Live tail is off for every materialization (Settings › Live tail).' }), live);
    assert.deepEqual([switched?.tone, switched?.text, switched?.detail], ['muted', 'Live is off', 'Live tail is off for every materialization (Settings › Live tail).']);
    const notTailed = tailNotice(tail({ status: 'unavailable', reason: 'not_tailed', rows: 0 }), live);
    assert.deepEqual([notTailed?.tone, notTailed?.text], ['muted', 'Live is starting']);
  });
});
