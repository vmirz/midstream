/**
 * Which partitions the maintenance app's next compaction pass takes, as its planner decides it
 * (`lakehouse/maintenance.py::plan_compaction`), so the Table tab says what will happen rather
 * than what would if nothing were still being written.
 *
 * A partition qualifies once it holds enough small files that compaction may take now. What live
 * traffic still writes waits for the hot partition delay, so it is not rewritten again and again
 * while it grows: a partition whose time reaches into that last stretch - today, this hour - waits
 * as a whole until no commit has added to it for that long; where nothing ends a partition soon -
 * this month, an unpartitioned table, the rows without a timestamp - each file waits until it is
 * that old. A past partition is taken however recently a backfill added to it.
 */

import type { PartitionGranularity } from '../../shared/types.ts';

const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;
/** A partition longer than this is never waited for as a whole: a month is not. */
const LONG_PARTITION_MS = DAY_MS;

export interface CompactionThresholds {
  smallFileBytes: number;
  minInputFiles: number;
  hotPartitionDelayMinutes: number;
}

/** A data file as the rule reads it: its size, and when the snapshot that added it was committed. */
export interface CompactionInput {
  bytes: number;
  /** Epoch milliseconds; null once that snapshot expired, which makes the file old enough. */
  committedAt: number | null;
}

/** The span of Kafka timestamps a partition holds, from its key: `2026-10-17`, `2026-10-17-10`, `2026-10`. */
export function partitionWindowMs(granularity: PartitionGranularity, key: string | null): { from: number; to: number } | null {
  if (key === null || key === '__null__') return null;
  const parts = key.split('-').map(Number);
  if (!parts.every((part) => Number.isInteger(part))) return null;
  const [year = 0, month = 1, day = 1, hour = 0] = parts;
  if (granularity === 'month' && parts.length === 2) return { from: Date.UTC(year, month - 1, 1), to: Date.UTC(year, month, 1) };
  if (granularity === 'day' && parts.length === 3) {
    const from = Date.UTC(year, month - 1, day);
    return { from, to: from + DAY_MS };
  }
  if (granularity === 'hour' && parts.length === 4) {
    const from = Date.UTC(year, month - 1, day, hour);
    return { from, to: from + HOUR_MS };
  }
  return null;
}

/** The files of one partition that compaction may take now. */
export function settledFiles<T extends CompactionInput>(
  files: readonly T[],
  partitionKey: string | null,
  granularity: PartitionGranularity,
  nowMs: number,
  hotMs: number,
): T[] {
  if (hotMs <= 0) return [...files];
  const window = partitionWindowMs(granularity, partitionKey);
  if (window && window.to <= nowMs - hotMs) return [...files];
  if (!window || window.to - window.from > LONG_PARTITION_MS) {
    return files.filter((file) => file.committedAt === null || nowMs - file.committedAt >= hotMs);
  }
  const latestAdd = files.reduce((latest, file) => Math.max(latest, file.committedAt ?? 0), 0);
  return nowMs - latestAdd < hotMs ? [] : [...files];
}

/** Whether the next compaction pass takes the partition: it has enough small files compaction may take now. */
export function compactionDue(
  files: readonly CompactionInput[],
  partitionKey: string | null,
  granularity: PartitionGranularity,
  thresholds: CompactionThresholds,
  nowMs: number,
): boolean {
  const settled = settledFiles(files, partitionKey, granularity, nowMs, thresholds.hotPartitionDelayMinutes * 60_000);
  return settled.filter((file) => file.bytes < thresholds.smallFileBytes).length >= thresholds.minInputFiles;
}
