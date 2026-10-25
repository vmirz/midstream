import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { formatEta, lagTrend } from './lagTrend.ts';

const minute = 60_000;
const series = (lags: number[], every = minute) => lags.map((lag, index) => ({ t: index * every, lag }));

describe('lagTrend', () => {
  it('reads the drain rate and the time left from the measured lags', () => {
    const trend = lagTrend(series([107_357_698, 106_938_384, 106_543_644, 106_130_762]));
    assert.equal(trend.state, 'draining');
    assert.ok(Math.abs((trend.perSecond as number) + 6_820) < 50, `${trend.perSecond}`);
    assert.ok(Math.abs((trend.etaMs as number) / 3_600_000 - 4.32) < 0.05, `${trend.etaMs}`);
  });

  it('says when the topic is outrunning the runtime', () => {
    assert.equal(lagTrend(series([1_000, 5_000, 9_000])).state, 'growing');
  });

  it('calls a lag that barely moves steady, and zero caught up', () => {
    assert.equal(lagTrend(series([1_000_000, 1_000_100, 999_950, 1_000_020])).state, 'steady');
    assert.equal(lagTrend(series([30, 0])).state, 'caught-up');
  });

  it('needs two measurements', () => {
    assert.equal(lagTrend(series([5_000])).state, 'unknown');
    assert.equal(lagTrend([]).state, 'unknown');
  });

  it('weighs the last ten minutes', () => {
    // Growing an hour ago, draining now.
    const lags = [...Array.from({ length: 50 }, (_, index) => 1_000 + index * 100), ...Array.from({ length: 10 }, (_, index) => 5_900 - index * 300)];
    assert.equal(lagTrend(series(lags)).state, 'draining');
  });
});

describe('formatEta', () => {
  it('rounds to a readable unit', () => {
    assert.equal(formatEta(45_000), '45s');
    assert.equal(formatEta(12 * minute), '12m');
    assert.equal(formatEta(190 * minute), '3h 10m');
    assert.equal(formatEta(60 * 60 * minute), '2d 12h');
  });
});
