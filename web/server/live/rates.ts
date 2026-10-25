/**
 * Ingest and write rates from runtime heartbeats, measured on the runtime's own clock.
 *
 * Each runtime member reports its counters on its own heartbeat — every 30 seconds by default —
 * and the control plane folds every report into one materialization-wide total. Differencing that
 * total at the console's poll interval, which is how rates used to be derived, divides whatever
 * heartbeats happen to land between two polls by the 1.5 seconds between them: 30 seconds of one
 * member's rows over 1.5 seconds on one poll, nothing on the next dozen. A steady 2,000 rows/s
 * then draws as a picket fence of zeros and 20,000 rows/s spikes, however evenly the members are
 * actually working.
 *
 * Here every member is followed on its own: its counter's change over roughly `rowsMs`, between
 * two of its own heartbeats and timed by their `reported_at`, is its rate, and a materialization's
 * rate is the sum over the members still reporting. That is what the members are doing, averaged
 * over a window short enough to stay live — and polling faster or slower, or a slow control plane
 * delivering heartbeats late, no longer changes the answer.
 *
 * Bytes are different. They are counted when a batch is sealed into a Parquet object, a whole
 * object at a time, and a materialization's partitions tend to seal together — every five minutes
 * or so, by the runtime's batching policy — so per member they rise as a staircase. A difference
 * between two points flips between nothing and a whole wave as a wave enters or leaves its window.
 * The byte rate is instead the least-squares slope through every reading in the last `bytesMs`
 * (15 minutes by default), which stays within about a tenth of the true rate as waves come and go.
 */

import type { CpRuntimeProgress, MemberRate } from '../../shared/types.ts';

export interface RateWindows {
  /** Span one member's row rate is measured over. */
  rowsMs: number;
  /** Span one member's byte rate is measured over. */
  bytesMs: number;
  /** A member silent this long has been scaled away, replaced or stopped, and no longer counts. */
  staleMs: number;
}

export interface CurrentRates {
  rowsPerSecond: number | null;
  bytesPerSecond: number | null;
  /** Each member still reporting, with its own rates. */
  members: Record<string, MemberRate>;
  /**
   * The rates are unknown only because members have not all reported twice since the console
   * started watching — not because nothing is running. A total over the fleet waits too.
   */
  warming: boolean;
}

/** A member's counters at one heartbeat, carried on across its restarts so they never go backwards. */
export interface Reading {
  t: number;
  rows: number;
  bytes: number;
}

interface MemberTrack {
  readings: Reading[];
  /** The counters as last reported, to tell a restart (a drop) from progress. */
  reported: { rows: number; bytes: number };
  /** When the console last saw a new heartbeat from this member, on the console's clock. */
  seenAt: number;
}

/** Older control planes report only materialization totals; they are followed as one member. */
const WHOLE_RUNTIME = '';

/** A reading this far behind the member's last is a different clock, not a late heartbeat. */
const CLOCK_JUMP_MS = 60_000;

export class RateTracker {
  private readonly members = new Map<string, MemberTrack>();
  private readonly windows: RateWindows;
  private readonly createdAt: number;

  constructor(windows: RateWindows, now: number) {
    this.windows = windows;
    this.createdAt = now;
  }

  /** Take in one detail observation: every member's latest heartbeat, as the control plane holds it. */
  observe(progress: CpRuntimeProgress | undefined, lastHeartbeat: string | null, now: number): void {
    const members = Object.entries(progress?.member_progress ?? {});
    if (members.length > 0) {
      for (const [id, member] of members) {
        this.record(id, Date.parse(member.reported_at ?? ''), member.rows_written ?? 0, member.bytes_written ?? 0, now);
      }
    } else if (progress && lastHeartbeat) {
      this.record(
        WHOLE_RUNTIME,
        Date.parse(lastHeartbeat),
        progress.cumulative_rows_written ?? progress.rows_written ?? 0,
        progress.cumulative_bytes_written ?? progress.bytes_written ?? 0,
        now,
      );
    }

    const forgetAfter = Math.max(this.windows.staleMs, this.windows.bytesMs) * 2;
    for (const [id, track] of this.members) {
      if (now - track.seenAt > forgetAfter) this.members.delete(id);
    }
  }

  private record(id: string, reportedAt: number, rows: number, bytes: number, now: number): void {
    if (!Number.isFinite(reportedAt)) return;
    const track = this.members.get(id);
    const last = track?.readings.at(-1);
    if (!track || !last || reportedAt < last.t - CLOCK_JUMP_MS) {
      this.members.set(id, { readings: [{ t: reportedAt, rows, bytes }], reported: { rows, bytes }, seenAt: now });
      return;
    }
    // The same heartbeat, seen again on a later poll.
    if (reportedAt <= last.t) return;

    // A counter below its last report means the member restarted and counts from zero again, so
    // everything it reports now is new — the same rule the control plane folds totals with.
    const rowsDelta = rows >= track.reported.rows ? rows - track.reported.rows : rows;
    const bytesDelta = bytes >= track.reported.bytes ? bytes - track.reported.bytes : bytes;
    track.readings.push({ t: reportedAt, rows: last.rows + rowsDelta, bytes: last.bytes + bytesDelta });
    track.reported = { rows, bytes };
    track.seenAt = now;

    // Keep what the longer window needs, and one reading before it.
    const horizon = reportedAt - Math.max(this.windows.rowsMs, this.windows.bytesMs) * 2;
    while (track.readings.length > 2 && (track.readings[1] as Reading).t <= horizon) track.readings.shift();
  }

  /** The rates as of `now`: every member still reporting, and their sum. */
  current(now: number): CurrentRates {
    const members: Record<string, MemberRate> = {};
    let rowsPerSecond: number | null = null;
    let bytesPerSecond: number | null = null;
    let waiting = false;

    for (const [id, track] of this.members) {
      if (now - track.seenAt > this.windows.staleMs) continue;
      const rows = windowRate(track.readings, 'rows', this.windows.rowsMs);
      const bytes = windowSlope(track.readings, 'bytes', this.windows.bytesMs);
      if (id !== WHOLE_RUNTIME) members[id] = { rowsPerSecond: rows, bytesPerSecond: bytes };
      if (rows === null) {
        waiting = true;
        continue;
      }
      rowsPerSecond = (rowsPerSecond ?? 0) + rows;
      bytesPerSecond = (bytesPerSecond ?? 0) + (bytes ?? 0);
    }

    // Right after the console starts watching, members get their second heartbeat one at a time; a
    // total over those so far would climb for half a minute as if throughput were ramping up. So
    // there is no total until they all have one. A member that joins later is counted from its
    // second heartbeat on.
    if (waiting && now - this.createdAt < this.windows.staleMs) {
      return { rowsPerSecond: null, bytesPerSecond: null, members, warming: true };
    }
    return { rowsPerSecond, bytesPerSecond, members, warming: false };
  }
}

/**
 * A counter's rate over about `windowMs`, ending at the latest reading.
 *
 * The far end is the reading closest to `windowMs` before the latest, so heartbeat jitter moves
 * the span by a second, not by a whole heartbeat. With less history than that, it is whatever
 * history there is.
 */
export function windowRate(readings: Reading[], field: 'rows' | 'bytes', windowMs: number): number | null {
  const latest = readings.at(-1);
  if (!latest || readings.length < 2) return null;
  const target = latest.t - windowMs;
  let base = readings[0] as Reading;
  for (const reading of readings) {
    if (reading === latest) break;
    if (Math.abs(reading.t - target) < Math.abs(base.t - target)) base = reading;
  }
  const seconds = (latest.t - base.t) / 1000;
  return seconds > 0 ? Math.max(0, latest[field] - base[field]) / seconds : null;
}

/**
 * A counter's rate as the least-squares slope through every reading in the last `windowMs`.
 *
 * For a counter that rises in steps — bytes, which land a sealed object at a time — a slope through
 * all the readings moves gradually as steps enter and leave the window, where a difference between
 * two readings jumps by a whole step. With fewer than two readings in the window it falls back to
 * the last two there are.
 */
export function windowSlope(readings: Reading[], field: 'rows' | 'bytes', windowMs: number): number | null {
  const latest = readings.at(-1);
  if (!latest || readings.length < 2) return null;
  let points = readings.filter((reading) => reading.t >= latest.t - windowMs);
  if (points.length < 2) points = readings.slice(-2);

  const meanT = points.reduce((sum, point) => sum + point.t, 0) / points.length;
  const meanV = points.reduce((sum, point) => sum + point[field], 0) / points.length;
  let covariance = 0;
  let variance = 0;
  for (const point of points) {
    covariance += (point.t - meanT) * (point[field] - meanV);
    variance += (point.t - meanT) ** 2;
  }
  return variance > 0 ? Math.max(0, (covariance / variance) * 1000) : null;
}
