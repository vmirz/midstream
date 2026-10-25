/**
 * Which partitions the next compaction pass takes, as the maintenance app's planner decides it
 * (`python/tests/test_lakehouse_maintenance.py` holds the planner's own cases).
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { compactionDue, partitionWindowMs, settledFiles, type CompactionThresholds } from './compaction.ts';

const MINUTE = 60_000;
const NOW = Date.parse('2026-10-23T15:00:00Z');
const THRESHOLDS: CompactionThresholds = { smallFileBytes: 64 * 1024 * 1024, minInputFiles: 2, hotPartitionDelayMinutes: 60 };
/** A small file added `minutesAgo` before NOW. */
const file = (minutesAgo: number | null, bytes = 1024) => ({ bytes, committedAt: minutesAgo === null ? null : NOW - minutesAgo * MINUTE });

describe('which partitions the next compaction pass takes', () => {
  it('reads a partition’s span from its key', () => {
    assert.deepEqual(partitionWindowMs('day', '2026-10-23'), { from: Date.parse('2026-10-23T00:00:00Z'), to: Date.parse('2026-10-24T00:00:00Z') });
    assert.deepEqual(partitionWindowMs('hour', '2026-10-23-14'), { from: Date.parse('2026-10-23T14:00:00Z'), to: Date.parse('2026-10-23T15:00:00Z') });
    assert.deepEqual(partitionWindowMs('month', '2026-12'), { from: Date.parse('2026-12-01T00:00:00Z'), to: Date.parse('2027-01-01T00:00:00Z') });
    assert.equal(partitionWindowMs('day', '__null__'), null);
    assert.equal(partitionWindowMs('none', null), null);
  });

  it('takes a past partition whole, however recently a backfill added to it', () => {
    assert.equal(settledFiles([file(1), file(2)], '2026-10-21', 'day', NOW, 60 * MINUTE).length, 2);
    assert.equal(compactionDue([file(1), file(2)], '2026-09', 'month', THRESHOLDS, NOW), true);
  });

  it('waits for today until no commit has added to it for the delay', () => {
    assert.equal(compactionDue([file(300), file(10)], '2026-10-23', 'day', THRESHOLDS, NOW), false);
    assert.equal(compactionDue([file(300), file(61)], '2026-10-23', 'day', THRESHOLDS, NOW), true);
    // The hour that just ended is still within the delay.
    assert.equal(compactionDue([file(90), file(70)], '2026-10-23-14', 'hour', THRESHOLDS, NOW), true);
    assert.equal(compactionDue([file(90), file(30)], '2026-10-23-14', 'hour', THRESHOLDS, NOW), false);
  });

  it('lets this month’s files settle one by one as they age', () => {
    const month = [file(300), file(120), file(10)];
    assert.equal(settledFiles(month, '2026-10', 'month', NOW, 60 * MINUTE).length, 2);
    assert.equal(compactionDue(month, '2026-10', 'month', THRESHOLDS, NOW), true);
    assert.equal(compactionDue([file(300), file(10)], '2026-10', 'month', THRESHOLDS, NOW), false);
  });

  it('settles an unpartitioned table’s files, and those without a timestamp, by age', () => {
    assert.equal(settledFiles([file(90), file(5)], null, 'none', NOW, 60 * MINUTE).length, 1);
    assert.equal(settledFiles([file(90), file(5), file(null)], '__null__', 'day', NOW, 60 * MINUTE).length, 2);
  });

  it('counts only small files, and takes everything when there is no delay', () => {
    assert.equal(compactionDue([file(300), file(300, 128 * 1024 * 1024)], '2026-09', 'month', THRESHOLDS, NOW), false);
    assert.equal(compactionDue([file(1), file(1)], '2026-10-23', 'day', { ...THRESHOLDS, hotPartitionDelayMinutes: 0 }, NOW), true);
  });
});
