import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DEFAULT_TABLE_NAME_RULES } from '../../../shared/tableNames.ts';
import type { CpDataset } from '../../../shared/types.ts';
import { claimRow, defaultDataset, draftProblems, draftTables, tableRequests, type TableDraft } from './tableDrafts.ts';

const rules = DEFAULT_TABLE_NAME_RULES;

function dataset(name: string, permissions: string[], tables: string[] = []): CpDataset {
  return {
    dataset_id: `ds-${name}`,
    name,
    description: '',
    revision: 1,
    created_at: '',
    created_by: '',
    updated_at: '',
    updated_by: '',
    owners: [],
    tables,
    table_count: tables.length,
    permissions,
    locked: !permissions.includes('dataset.view_schema'),
  };
}

const WRITE = ['dataset.view_schema', 'data.query', 'dataset.write'];
const DATASETS = [dataset('sales', WRITE, ['orders']), dataset('finance', ['dataset.view_schema']), dataset('secret', [])];

describe('table drafts', () => {
  it('give a new topic the dataset last chosen and a name derived from it, unique among the rows', () => {
    const first = draftTables(['sales.orders', 'eu.orders'], [], { rules, dataset: 'sales' });
    assert.deepEqual(
      first.map((draft) => [draft.topic, draft.dataset, draft.name, draft.edited]),
      [
        ['sales.orders', 'sales', 'sales_orders', false],
        ['eu.orders', 'sales', 'eu_orders', false],
      ],
    );
    const renamed: TableDraft[] = [{ ...first[0]!, name: 'orders', edited: true }, first[1]!];
    const next = draftTables(['sales.orders', 'eu.orders', 'orders'], renamed, { rules, dataset: 'finance' });
    assert.deepEqual(
      next.map((draft) => [draft.dataset, draft.name]),
      [
        ['sales', 'orders'],
        ['sales', 'eu_orders'],
        ['finance', 'orders_2'],
      ],
      'a typed name is kept, and a derived one steps around it',
    );
  });

  it('keep clear of the names the materialization already has', () => {
    const drafts = draftTables(['orders'], [], { rules, dataset: 'sales', taken: ['orders'] });
    assert.equal(drafts[0]?.name, 'orders_2');
  });

  it('say what is wrong with each row, the dataset first', () => {
    const rows: TableDraft[] = [
      { topic: 'a', dataset: '', name: 'a', edited: false },
      { topic: 'b', dataset: 'nowhere', name: 'b', edited: false },
      { topic: 'c', dataset: 'finance', name: 'c', edited: false },
      { topic: 'd', dataset: 'sales', name: 'Order', edited: true },
      { topic: 'e', dataset: 'sales', name: 'orders', edited: true },
      { topic: 'f', dataset: 'sales', name: 'dup', edited: true },
      { topic: 'g', dataset: 'sales', name: 'dup', edited: true },
      { topic: 'h', dataset: 'sales', name: 'fine', edited: true },
    ];
    assert.deepEqual(draftProblems(rows, { rules, datasets: DATASETS }), [
      { field: 'dataset', message: 'Choose the dataset its table goes in' },
      { field: 'dataset', message: 'There is no dataset nowhere' },
      { field: 'dataset', message: 'You cannot add tables to finance' },
      { field: 'name', message: 'Lowercase only: "order"' },
      { field: 'name', message: 'sales.orders is taken; choose another name' },
      { field: 'name', message: 'Another table of this materialization is named dup' },
      { field: 'name', message: 'Another table of this materialization is named dup' },
      null,
    ]);
    assert.deepEqual(draftProblems([{ topic: 'x', dataset: 'sales', name: 'payments', edited: false }], { rules, datasets: DATASETS, taken: ['Payments'] }), [
      { field: 'name', message: 'Another table of this materialization is named payments' },
    ]);
    assert.deepEqual(draftProblems([{ topic: 'x', dataset: 'any', name: 'x', edited: false }], { rules, datasets: null }), [null], 'unknown until the list loads');
  });

  it('become the request’s tables, each with the name shown', () => {
    assert.deepEqual(tableRequests([{ topic: 'sales.orders', dataset: 'sales', name: ' orders ', edited: true }]), [
      { source_topic: 'sales.orders', dataset: 'sales', name: 'orders' },
    ]);
  });

  it('start a new row in the dataset last chosen, or the only one the person may write', () => {
    assert.equal(defaultDataset([{ topic: 'a', dataset: 'finance', name: 'a', edited: false }], DATASETS), 'finance');
    assert.equal(defaultDataset([], DATASETS), 'sales');
    assert.equal(defaultDataset([], [...DATASETS, dataset('ops', WRITE)]), '');
    assert.equal(defaultDataset([], null), '');
  });
});

describe('a refused claim', () => {
  it('names its row by field, or by index', () => {
    assert.deepEqual(claimRow({ field: 'tables.2.name', reason: 'table_name_taken' }), { index: 2, field: 'name' });
    assert.deepEqual(claimRow({ reason: 'forbidden', index: 1 }), { index: 1, field: 'dataset' });
    assert.deepEqual(claimRow({ upstream_details: { field: 'tables.0.dataset', reason: 'dataset_not_found' } }), { index: 0, field: 'dataset' });
    assert.deepEqual(claimRow({ field: 'tables.3' }), { index: 3, field: null });
    assert.equal(claimRow({ field: 'replicas' }), null);
    assert.equal(claimRow({}), null);
  });
});
