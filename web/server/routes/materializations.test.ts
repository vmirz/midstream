import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { createMaterializationSchema, updateMaterializationSchema } from './materializations.ts';

const base = { tables: [{ source_topic: 'orders', dataset: 'sales' }] };

describe('create materialization schema', () => {
  it('takes no live_tail: every materialization has a live tail', () => {
    for (const live_tail of [true, false]) assert.throws(() => createMaterializationSchema.parse({ ...base, live_tail }));
    assert.equal('live_tail' in createMaterializationSchema.parse(base), false);
  });

  it('passes skip_tombstones through to the control plane', () => {
    assert.equal(createMaterializationSchema.parse({ ...base, skip_tombstones: true }).skip_tombstones, true);
  });

  it('leaves skip_tombstones unset when omitted so the control plane default applies', () => {
    assert.equal(createMaterializationSchema.parse(base).skip_tombstones, undefined);
  });

  it('rejects a non-boolean skip_tombstones', () => {
    assert.throws(() => createMaterializationSchema.parse({ ...base, skip_tombstones: 'true' }));
  });

  it('takes each topic to read with the dataset its table goes in, in their order, each once', () => {
    const tables = [
      { source_topic: ' orders', dataset: 'sales' },
      { source_topic: 'invoices.v2', dataset: 'finance', name: 'invoices' },
    ];
    assert.deepEqual(createMaterializationSchema.parse({ ...base, tables }).tables, [
      { source_topic: 'orders', dataset: 'sales' },
      { source_topic: 'invoices.v2', dataset: 'finance', name: 'invoices' },
    ]);
    for (const topics of [[], ['orders', 'orders'], ['orders topic'], ['.'], ['x'.repeat(250)]]) {
      const requested = topics.map((source_topic) => ({ source_topic, dataset: 'sales' }));
      assert.throws(() => createMaterializationSchema.parse({ ...base, tables: requested }), JSON.stringify(topics));
    }
    assert.throws(() => createMaterializationSchema.parse({ ...base, tables: 'orders' }));
    const { tables: _omitted, ...withoutTables } = base;
    assert.throws(() => createMaterializationSchema.parse({ ...withoutTables, source_topics: ['orders'] }), 'source_topics is gone');
  });

  it('needs a dataset for every table, named as SQL reads it', () => {
    assert.throws(() => createMaterializationSchema.parse({ ...base, tables: [{ source_topic: 'orders' }] }));
    for (const dataset of ['', 'Sales', 'sales-eu', '1sales', 'select', 'main', 'a'.repeat(64)]) {
      assert.throws(() => createMaterializationSchema.parse({ ...base, tables: [{ source_topic: 'orders', dataset }] }), dataset);
    }
  });

  it('takes an optional table name, which two tables cannot share', () => {
    const named = (name: string) => ({ ...base, tables: [{ source_topic: 'orders', dataset: 'sales', name }] });
    assert.equal(createMaterializationSchema.parse(named(' orders_eu ')).tables[0]?.name, 'orders_eu');
    for (const name of ['Orders', 'orders-eu', 'order', 'a'.repeat(64)]) {
      assert.throws(() => createMaterializationSchema.parse(named(name)), name);
    }
    const twice = [
      { source_topic: 'orders', dataset: 'sales', name: 'orders' },
      { source_topic: 'orders.eu', dataset: 'finance', name: 'orders' },
    ];
    assert.throws(() => createMaterializationSchema.parse({ ...base, tables: twice }), 'unique in the materialization, whatever the dataset');
  });

  it('takes an optional label of free text', () => {
    assert.equal(createMaterializationSchema.parse(base).name, undefined);
    assert.equal(createMaterializationSchema.parse({ ...base, name: 'Orders — EU, 2026!' }).name, 'Orders — EU, 2026!');
    assert.throws(() => createMaterializationSchema.parse({ ...base, name: 'a'.repeat(129) }));
    assert.throws(() => createMaterializationSchema.parse({ ...base, name: 'line\nbreak' }));
  });

  /**
   * The destination, the runtime's namespace and name, and its storage are the deployment's. A body
   * naming one is refused, not stripped: stripping it would report success for a choice not made.
   */
  it('refuses what the deployment chooses, and anything it does not know', () => {
    for (const [field, value] of [
      ['destination_uri', 'gs://elsewhere'],
      ['namespace', 'team-a'],
      ['runtime_name', 'orders-runtime'],
      ['storage_size', '8Gi'],
      ['colour', 'red'],
    ] as const) {
      const result = createMaterializationSchema.safeParse({ ...base, [field]: value });
      assert.equal(result.success, false, field);
    }
  });

  it('takes the month as a partitioning, and no week', () => {
    assert.equal(createMaterializationSchema.parse({ ...base, partition_granularity: 'month' }).partition_granularity, 'month');
    assert.throws(() => createMaterializationSchema.parse({ ...base, partition_granularity: 'week' }));
  });

  it('may turn autoscaling off; omitted, the control plane turns it on', () => {
    assert.equal(createMaterializationSchema.parse({ ...base, autoscaling: false }).autoscaling, false);
    assert.equal(createMaterializationSchema.parse(base).autoscaling, undefined);
    assert.throws(() => createMaterializationSchema.parse({ ...base, autoscaling: 'off' }));
  });
});

describe('update materialization schema', () => {
  it('takes no live_tail, and refuses a change that says nothing', () => {
    assert.throws(() => updateMaterializationSchema.parse({ live_tail: true }));
    assert.throws(() => updateMaterializationSchema.parse({ live_tail: false, replicas: 2 }));
    assert.throws(() => updateMaterializationSchema.parse({}), 'something must change');
  });

  it('accepts a replica count', () => {
    assert.deepEqual(updateMaterializationSchema.parse({ replicas: 8 }), { replicas: 8 });
  });

  it('accepts turning autoscaling on or off, alone or with a replica count', () => {
    assert.deepEqual(updateMaterializationSchema.parse({ autoscaling: false }), { autoscaling: false });
    assert.deepEqual(updateMaterializationSchema.parse({ autoscaling: true, replicas: 4 }), { autoscaling: true, replicas: 4 });
    for (const autoscaling of ['true', 1, null]) {
      assert.throws(() => updateMaterializationSchema.parse({ autoscaling }), String(autoscaling));
    }
  });

  it('rejects a count that is not a positive whole number', () => {
    for (const replicas of [0, -1, 1.5, '8', null]) {
      assert.throws(() => updateMaterializationSchema.parse({ replicas }), String(replicas));
    }
  });

  it('accepts a new label, an empty one going back to the id', () => {
    assert.deepEqual(updateMaterializationSchema.parse({ name: 'Shop orders' }), { name: 'Shop orders' });
    assert.deepEqual(updateMaterializationSchema.parse({ name: '' }), { name: '' });
  });

  /** A setting that cannot change is refused, not dropped: dropping it would report success. */
  it('refuses any other setting', () => {
    assert.throws(() => updateMaterializationSchema.parse({ replicas: 8, destination_uri: 'gs://elsewhere' }));
    assert.throws(() => updateMaterializationSchema.parse({ source_topics: ['orders'] }));
  });

  it('accepts a change of topics: new ones with their dataset, with what becomes of each removed table', () => {
    const body = {
      tables: [{ source_topic: 'orders' }, { source_topic: 'refunds', dataset: 'finance', name: 'refunds' }],
      removed_tables: { payments: 'keep' },
    };
    assert.deepEqual(updateMaterializationSchema.parse(body), body);
    // Dropping a kept table changes no topic.
    assert.deepEqual(updateMaterializationSchema.parse({ removed_tables: { payments: 'drop' } }), { removed_tables: { payments: 'drop' } });
  });

  it('refuses a change that says nothing, or topics that are not topics', () => {
    assert.throws(() => updateMaterializationSchema.parse({}));
    assert.throws(() => updateMaterializationSchema.parse({ tables: [] }));
    assert.throws(() => updateMaterializationSchema.parse({ tables: [{ source_topic: 'orders' }, { source_topic: 'orders' }] }));
    assert.throws(() => updateMaterializationSchema.parse({ tables: [{ source_topic: 'bad topic' }] }));
    assert.throws(() => updateMaterializationSchema.parse({ tables: [{ source_topic: 'orders', dataset: 'Sales' }] }));
    assert.throws(() => updateMaterializationSchema.parse({ removed_tables: { payments: 'archive' } }));
  });
});
