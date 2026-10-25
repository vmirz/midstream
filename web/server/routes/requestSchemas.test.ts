import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { filtersSchema } from './requestSchemas.ts';

describe('request schemas', () => {
  it('keeps the filter list short, since filters are hand-built one at a time', () => {
    const filter = { column: '_kafka_partition', operator: 'eq' as const, value: '3' };
    assert.doesNotThrow(() => filtersSchema.parse(Array.from({ length: 20 }, () => filter)));
    assert.throws(() => filtersSchema.parse(Array.from({ length: 21 }, () => filter)));
  });

  it('accepts logical paths through lists', () => {
    assert.doesNotThrow(() =>
      filtersSchema.parse([{ column: 'payload.items[].sku', operator: 'eq', value: 'SKU-1' }]),
    );
  });

  it('refuses an operator it does not know', () => {
    assert.throws(() => filtersSchema.parse([{ column: 'payload.status', operator: 'like', value: 'x' }]));
  });
});
