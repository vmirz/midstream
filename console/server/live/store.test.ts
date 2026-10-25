import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { committedTotals } from './store.ts';

describe('what a materialization’s tables hold', () => {
  it('sums each table’s current snapshot as its committer reported it, never what the runtime wrote', () => {
    // As the lab reported a runtime that lost writes to restarts: written 290 M, held 95 M.
    const progress = {
      cumulative_rows_written: 290_457_253,
      cumulative_bytes_written: 18_151_374_776,
      cumulative_files_written: 9_120,
      tables: {
        orders: { total_records: 95_024_992, total_bytes: 16_070_606_014, total_files: 2_751 },
        payments: { total_records: 8, total_bytes: 9_000, total_files: 2 },
      },
    };
    assert.deepEqual(committedTotals(progress), { rows: 95_025_000, bytes: 16_070_615_014, files: 2_753 });
  });

  it('is unknown before any table is reported, and a table without a snapshot holds nothing', () => {
    assert.equal(committedTotals(undefined), null);
    assert.equal(committedTotals({ cumulative_rows_written: 10, cumulative_bytes_written: 100 }), null);
    assert.deepEqual(committedTotals({ tables: { orders: {} } }), { rows: 0, bytes: 0, files: 0 });
  });
});
