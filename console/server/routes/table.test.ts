import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { describesSchema } from './table.ts';

describe('table properties', () => {
  it('keeps what describes the fields from people who may not see the schemas', () => {
    assert.equal(describesSchema('materializer.fork-map'), true);
    assert.equal(describesSchema('materializer.protection'), true, 'it names the protected fields and their kinds');
    assert.equal(describesSchema('schema.name-mapping.default'), true);
    assert.equal(describesSchema('materializer.partition-granularity'), false);
    assert.equal(describesSchema('write.parquet.compression-codec'), false);
  });
});
