import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ALL_TIME, decodeRange, encodeRange, rangeLabel, resolveRange, type TimeRangeSpec } from './timeRange.ts';

// Wednesday 7 October 2026, 14:30 local time.
const now = new Date(2026, 9, 7, 14, 30).getTime();
const local = (year: number, month: number, day: number): number => new Date(year, month - 1, day).getTime();

describe('resolveRange', () => {
  it('measures relative ranges back from the moment Run is pressed', () => {
    assert.deepEqual(resolveRange({ kind: 'last', amount: 15, unit: 'minute' }, now), { from: now - 15 * 60_000, to: null });
    assert.deepEqual(resolveRange({ kind: 'last', amount: 7, unit: 'day' }, now), { from: now - 7 * 86_400_000, to: null });
  });

  it('anchors calendar presets to the local day, with weeks starting on Sunday', () => {
    const cases: [TimeRangeSpec, number | null, number | null][] = [
      [{ kind: 'calendar', preset: 'today' }, local(2026, 10, 7), null],
      [{ kind: 'calendar', preset: 'yesterday' }, local(2026, 10, 6), local(2026, 10, 7)],
      [{ kind: 'calendar', preset: 'week_to_date' }, local(2026, 10, 4), null],
      [{ kind: 'calendar', preset: 'previous_week' }, local(2026, 9, 27), local(2026, 10, 4)],
      [{ kind: 'calendar', preset: 'month_to_date' }, local(2026, 10, 1), null],
      [{ kind: 'calendar', preset: 'previous_month' }, local(2026, 9, 1), local(2026, 10, 1)],
      [{ kind: 'calendar', preset: 'year_to_date' }, local(2026, 1, 1), null],
      [{ kind: 'calendar', preset: 'previous_year' }, local(2025, 1, 1), local(2026, 1, 1)],
    ];
    for (const [spec, from, to] of cases) {
      assert.deepEqual(resolveRange(spec, now), { from, to }, rangeLabel(spec));
    }
  });

  it('includes the whole last day of a date range, across a daylight-saving change', () => {
    assert.deepEqual(resolveRange({ kind: 'dates', from: '2026-11-01', to: '2026-11-01' }, now), {
      from: local(2026, 11, 1),
      to: local(2026, 11, 2),
    });
  });

  it('leaves All time unbounded', () => {
    assert.deepEqual(resolveRange(ALL_TIME, now), { from: null, to: null });
  });
});

describe('encodeRange', () => {
  it('round-trips every kind through the address bar', () => {
    const specs: TimeRangeSpec[] = [
      ALL_TIME,
      { kind: 'last', amount: 4, unit: 'hour' },
      { kind: 'calendar', preset: 'previous_month' },
      { kind: 'dates', from: '2026-10-01', to: '2026-10-05' },
      { kind: 'dates', from: '2026-10-01', to: null },
      { kind: 'between', from: 1_759_665_296_000, to: 1_759_751_696_000 },
    ];
    for (const spec of specs) assert.deepEqual(decodeRange(encodeRange(spec)), spec);
  });

  it('opens anything it cannot read as All time rather than failing', () => {
    for (const text of ['', 'last-0m', 'last-5y', 'tomorrow', 'dates:2026-1-1..', 'between:x..y']) {
      assert.deepEqual(decodeRange(text), ALL_TIME, text);
    }
  });
});

describe('rangeLabel', () => {
  it('says what was chosen', () => {
    assert.equal(rangeLabel({ kind: 'last', amount: 60, unit: 'minute' }), 'Last 60 minutes');
    assert.equal(rangeLabel({ kind: 'last', amount: 1, unit: 'day' }), 'Last 1 day');
    assert.equal(rangeLabel({ kind: 'calendar', preset: 'week_to_date' }), 'Week to date');
  });
});
