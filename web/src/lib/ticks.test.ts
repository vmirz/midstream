import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { niceStep, timeTicks, valueAxis } from './ticks.ts';

describe('niceStep', () => {
  it('rounds to 1, 2, 2.5 or 5 times a power of ten', () => {
    assert.equal(niceStep(10, 4), 2.5);
    assert.equal(niceStep(6_200, 4), 2_000);
    assert.equal(niceStep(0.9, 4), 0.25);
    assert.equal(niceStep(0, 4), 1);
  });
});

describe('valueAxis', () => {
  it('keeps zero on a rate axis', () => {
    const axis = valueAxis([5_970, 6_245], { baseline: 'zero' });
    assert.equal(axis.min, 0);
    assert.ok(axis.max >= 6_245);
    assert.deepEqual(axis.ticks, [0, 2_000, 4_000, 6_000, 8_000]);
  });

  it('fits a large, slowly changing value so its slope shows', () => {
    const axis = valueAxis([108_000_000, 104_200_000], { baseline: 'auto' });
    assert.ok(axis.min > 100_000_000 && axis.min <= 104_200_000, `min ${axis.min}`);
    assert.ok(axis.max >= 108_000_000 && axis.max < 112_000_000, `max ${axis.max}`);
    assert.ok(axis.ticks.length >= 3);
  });

  it('keeps zero when the values come near it', () => {
    assert.equal(valueAxis([1_000, 90_000], { baseline: 'auto' }).min, 0);
  });

  it('gives a flat series room', () => {
    const zero = valueAxis([0, 0], { baseline: 'auto' });
    assert.deepEqual([zero.min, zero.max > 0], [0, true]);
    const flat = valueAxis([500, 500], { baseline: 'auto' });
    assert.ok(flat.min < 500 && flat.max > 500);
  });
});

describe('timeTicks', () => {
  it('steps in round minutes and stays inside the range', () => {
    const from = Date.parse('2026-10-08T03:01:17Z');
    const to = from + 20 * 60_000;
    const { ticks, step } = timeTicks(from, to, 6);
    assert.equal(step, 300_000);
    assert.ok(ticks.every((tick) => tick >= from && tick <= to));
    assert.ok(ticks.every((tick) => new Date(tick).getSeconds() === 0 && new Date(tick).getMinutes() % 5 === 0));
  });

  it('uses seconds only for short spans', () => {
    const from = Date.parse('2026-10-08T03:00:00Z');
    assert.equal(timeTicks(from, from + 60_000, 6).step, 10_000);
    assert.equal(timeTicks(from, from + 6 * 3_600_000, 6).step, 3_600_000);
  });
});
