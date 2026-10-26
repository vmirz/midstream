import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { SearchResult } from '@shared/types';

import { resultPath } from './results.ts';

function result(overrides: Partial<SearchResult>): SearchResult {
  return { domain: 'domain_a', kind: 'materialization', id: 'orders', label: 'Orders', detail: null, materializationId: 'orders', ...overrides };
}

describe('resultPath', () => {
  it('opens a materialization in its domain', () => {
    assert.equal(resultPath(result({ domain: 'domain_b' })), '/d/domain_b/materializations/orders');
  });

  it('opens a table on its materialization’s Table tab, chosen by its name', () => {
    const table = result({ kind: 'table', id: 'sales.orders_v2', label: 'domain_a.sales.orders_v2', materializationId: 'shop' });
    assert.equal(resultPath(table), '/d/domain_a/materializations/shop/table?table=orders_v2');
  });

  it('opens a dataset’s page in its domain', () => {
    assert.equal(resultPath(result({ kind: 'dataset', id: 'sales', label: 'domain_a.sales', materializationId: null })), '/d/domain_a/datasets/sales');
  });
});
