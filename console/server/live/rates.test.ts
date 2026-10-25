/**
 * Rates from runtime heartbeats, against a simulated runtime.
 *
 * The simulation reproduces what the console actually sees: members working at steady rates,
 * each reporting cumulative counters on its own 30-second heartbeat (timestamps truncated to the
 * second, as the runtime writes them), and the console polling the control plane's latest copy of
 * every member every 1.5 seconds.
 */

import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

const dataDir = await mkdtemp(join(tmpdir(), 'console-rates-'));
process.env.CONSOLE_DATA_DIR = dataDir;

const { RateTracker, windowRate, windowSlope } = await import('./rates.ts');
const { appendLag, appendSample } = await import('./store.ts');

import type { CpMemberProgress, CpRuntimeProgress, LagSample, ThroughputSample } from '../../shared/types.ts';

after(async () => {
  await rm(dataDir, { recursive: true, force: true });
});

const WINDOWS = { rowsMs: 60_000, bytesMs: 900_000, staleMs: 120_000 };
const T0 = Date.parse('2026-10-08T00:00:00.000Z');
const HEARTBEAT_MS = 30_000;

interface SimMember {
  id: string;
  rowsPerSecond: number;
  /** When in the heartbeat cycle this member reports. */
  phaseMs: number;
  /** When the member's process started; its counters count from here. */
  startedAt: number;
  /** Stops reporting from here on. */
  stopsAt?: number;
}

/** What the control plane holds for each member at `now`: its latest heartbeat. */
function progressAt(members: SimMember[], now: number): CpRuntimeProgress {
  const memberProgress: Record<string, CpMemberProgress> = {};
  for (const member of members) {
    const last = Math.min(now, member.stopsAt ?? Number.POSITIVE_INFINITY);
    const beats = Math.floor((last - member.startedAt - member.phaseMs) / HEARTBEAT_MS);
    if (beats < 0) continue;
    const reportedAt = member.startedAt + member.phaseMs + beats * HEARTBEAT_MS;
    const rows = Math.round(((reportedAt - member.startedAt) / 1000) * member.rowsPerSecond);
    memberProgress[member.id] = {
      rows_written: rows,
      bytes_written: rows * 250,
      files_written: 0,
      records_skipped: 0,
      // The runtime truncates its timestamps to the second.
      reported_at: new Date(Math.floor(reportedAt / 1000) * 1000).toISOString().replace('.000Z', 'Z'),
    };
  }
  return { member_progress: memberProgress };
}

function fleet(count: number, rowsPerSecond: number): SimMember[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `develop-mat-x-app-${index}`,
    rowsPerSecond: rowsPerSecond + index * 7,
    phaseMs: Math.round((index * HEARTBEAT_MS) / count) + 137 * index,
    startedAt: T0 - 600_000,
  }));
}

/** Poll every 1.5 s from `from` to `to`, returning the total rate after each poll. */
function poll(tracker: InstanceType<typeof RateTracker>, members: SimMember[], from: number, to: number): Array<number | null> {
  const rates: Array<number | null> = [];
  for (let now = from; now <= to; now += 1_500) {
    tracker.observe(progressAt(members, now), null, now);
    rates.push(tracker.current(now).rowsPerSecond);
  }
  return rates;
}

describe('RateTracker', () => {
  it('reads a steady rate from staggered heartbeats, polled far faster than they arrive', () => {
    const members = fleet(8, 230);
    const truth = members.reduce((sum, member) => sum + member.rowsPerSecond, 0);
    const tracker = new RateTracker(WINDOWS, T0);
    const rates = poll(tracker, members, T0, T0 + 10 * 60_000);

    // Every member has reported twice within a heartbeat or so; from then on the total is known.
    const settled = rates.slice(Math.ceil(70_000 / 1_500));
    assert.ok(settled.every((rate) => rate !== null), 'a total once every member has reported twice');
    for (const rate of settled) {
      assert.ok(Math.abs((rate as number) - truth) / truth < 0.04, `${rate} is within 4% of ${truth}`);
    }
  });

  it('has no total until every member has reported twice, rather than one that climbs', () => {
    const members = fleet(8, 200);
    const tracker = new RateTracker(WINDOWS, T0);
    const rates = poll(tracker, members, T0, T0 + 20_000);
    assert.ok(rates.every((rate) => rate === null));
    assert.equal(tracker.current(T0 + 20_000).warming, true, 'and says it is warming up, not stopped');
    poll(tracker, members, T0 + 21_500, T0 + 90_000);
    assert.equal(tracker.current(T0 + 90_000).warming, false);
  });

  it('measures each member on its own', () => {
    const members = fleet(4, 100);
    const tracker = new RateTracker(WINDOWS, T0);
    poll(tracker, members, T0, T0 + 3 * 60_000);
    const current = tracker.current(T0 + 3 * 60_000);
    for (const member of members) {
      const measured = current.members[member.id]?.rowsPerSecond ?? 0;
      assert.ok(Math.abs(measured - member.rowsPerSecond) / member.rowsPerSecond < 0.05, `${member.id}: ${measured}`);
    }
    // Bytes follow rows here, at 250 bytes a row.
    const bytes = current.bytesPerSecond ?? 0;
    assert.ok(Math.abs(bytes - (current.rowsPerSecond ?? 0) * 250) / bytes < 0.05);
  });

  it('treats a member restart as a counter reset, not a negative rate or a spike', () => {
    const members = fleet(2, 300);
    const tracker = new RateTracker(WINDOWS, T0);
    poll(tracker, members, T0, T0 + 120_000);
    // Member 0 restarts: its counters start again from zero.
    const restarted = [{ ...members[0]!, startedAt: T0 + 125_000, phaseMs: 3_000 }, members[1]!];
    const rates = poll(tracker, restarted, T0 + 121_500, T0 + 300_000);
    const truth = members[0]!.rowsPerSecond + members[1]!.rowsPerSecond;
    for (const rate of rates) {
      assert.ok(rate !== null && rate >= 0 && rate <= truth * 1.05, `${rate} never spikes past ${truth}`);
    }
    assert.ok(Math.abs((rates.at(-1) as number) - truth) / truth < 0.04, 'and settles back at the true rate');
  });

  it('drops a member that stops reporting', () => {
    const members = fleet(2, 300);
    members[1]!.stopsAt = T0 + 90_000;
    const tracker = new RateTracker(WINDOWS, T0);
    const rates = poll(tracker, members, T0, T0 + 400_000);
    assert.ok(Math.abs((rates.at(-1) as number) - members[0]!.rowsPerSecond) < 15);
    assert.deepEqual(Object.keys(tracker.current(T0 + 400_000).members), [members[0]!.id]);
  });

  it('follows a control plane that reports only totals as one member', () => {
    const tracker = new RateTracker(WINDOWS, T0);
    for (let beat = 0; beat <= 10; beat += 1) {
      const at = T0 + beat * 30_000;
      tracker.observe({ cumulative_rows_written: beat * 30_000, cumulative_bytes_written: beat * 3_000_000 }, new Date(at).toISOString(), at);
    }
    const current = tracker.current(T0 + 300_000);
    assert.equal(current.rowsPerSecond, 1_000);
    assert.equal(current.bytesPerSecond, 100_000);
    assert.deepEqual(current.members, {}, 'the totals are not a member');
  });

  it('reads bytes that land in waves of sealed objects as a steady rate', () => {
    // One member whose partitions seal together every 300–330 s, 48 MB a wave, heartbeating every 30 s.
    const waves: number[] = [];
    for (let t = 0, step = 0; t < 7_200_000; step += 1) waves.push((t += 300_000 + ((step * 7_919) % 30_000)));
    const sealedBy = (t: number): number => waves.filter((at) => at <= t).length * 48_000_000;
    const rate = (48_000_000 / 315_000) * 1000;
    const readings = Array.from({ length: 240 }, (_, beat) => ({ t: 7_000 + beat * 30_000, rows: 0, bytes: sealedBy(7_000 + beat * 30_000) }));
    for (let end = 60; end <= readings.length; end += 1) {
      const measured = windowSlope(readings.slice(0, end), 'bytes', 900_000) as number;
      assert.ok(measured > rate * 0.85 && measured < rate * 1.15, `${measured} within 15% of ${rate}`);
    }
  });

  it('measures over the reading closest to the window, whatever the heartbeat jitter', () => {
    const readings = [
      { t: 0, rows: 0, bytes: 0 },
      { t: 31_000, rows: 3_100, bytes: 0 },
      { t: 60_000, rows: 6_000, bytes: 0 },
      { t: 89_000, rows: 8_900, bytes: 0 },
    ];
    assert.equal(windowRate(readings, 'rows', 60_000), 100);
    assert.equal(windowRate(readings.slice(0, 2), 'rows', 60_000), 100, 'less history than the window: what there is');
    assert.equal(windowRate(readings.slice(0, 1), 'rows', 60_000), null);
  });
});

describe('series', () => {
  const sample = (t: number): ThroughputSample => ({ t, rows: 0, bytes: 0, files: 0, skipped: 0, rowsPerSecond: t, bytesPerSecond: null, lag: null });

  it('keeps points at least the sample spacing apart, with the newest following every poll', () => {
    let series: ThroughputSample[] = [];
    for (let t = 0; t <= 30_000; t += 1_500) series = appendSample(series, sample(t), 5_000, 240);
    const times = series.map((point) => point.t);
    assert.equal(times.at(-1), 30_000);
    for (let index = 1; index < times.length - 1; index += 1) {
      assert.ok((times[index] as number) - (times[index - 1] as number) >= 4_500);
    }
  });

  it('keeps every lag measurement for the last hour and thins older ones', () => {
    let history: LagSample[] = [];
    // Every 15 seconds for six hours: 1,440 measurements.
    for (let t = 0; t <= 6 * 3_600_000; t += 15_000) history = appendLag(history, { t, lag: t }, 6 * 3_600_000, 720);
    assert.ok(history.length <= 720);
    const lastHour = history.filter((point) => point.t >= 5 * 3_600_000);
    assert.equal(lastHour.length, 241, 'the newest hour is at full resolution');
    assert.equal(history[0]?.t, 0, 'and the oldest point is still there');
  });
});
