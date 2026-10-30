import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { describesSchema, maintenanceSchema } from './table.ts';

describe('table properties', () => {
  it('keeps what describes the fields from people who may not see the schemas', () => {
    assert.equal(describesSchema('materializer.fork-map'), true);
    assert.equal(describesSchema('materializer.protection'), true, 'it names the protected fields and their kinds');
    assert.equal(describesSchema('schema.name-mapping.default'), true);
    assert.equal(describesSchema('materializer.partition-granularity'), false);
    assert.equal(describesSchema('write.parquet.compression-codec'), false);
  });
});

describe('maintenance requests', () => {
  it('takes a reprotect of every table or one, purging only when asked', () => {
    assert.deepEqual(maintenanceSchema.parse({ job: 'reprotect', purge: true }), { job: 'reprotect', purge: true });
    assert.deepEqual(maintenanceSchema.parse({ job: 'reprotect', table: 'orders' }), { job: 'reprotect', table: 'orders' });
  });

  it('refuses a purge with any other job, and one that is not a boolean', () => {
    assert.throws(() => maintenanceSchema.parse({ job: 'compaction', purge: true }));
    assert.throws(() => maintenanceSchema.parse({ job: 'reprotect', purge: 'yes' }));
    assert.throws(() => maintenanceSchema.parse({ job: 'vacuum' }));
  });
});
