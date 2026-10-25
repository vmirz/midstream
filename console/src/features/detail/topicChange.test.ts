import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CpDetail, CpTableSummary } from '@shared/types';

import { keepOrder, planTopicChange } from './topicChange.ts';

function summary(name: string, state: CpTableSummary['state'] = 'active'): CpTableSummary {
  return {
    dataset: 'shop',
    dataset_id: 'ds-000000000001',
    name,
    source_topic: name,
    identifier: `shop.${name}`,
    location: `gs://lake/materializer/mat-123456789abc/${name}`,
    partition_granularity: 'day',
    schema_id: 0,
    current_snapshot_id: null,
    last_commit_at: null,
    valid_through: null,
    totals: {},
    last_error: null,
    state,
  };
}

function detail(): CpDetail {
  return {
    materialization: {
      materialization_id: 'mat-123456789abc',
      name: 'shop',
      tables: [
        { name: 'orders', source_topic: 'orders', dataset: 'shop', dataset_id: 'ds-000000000001' },
        { name: 'payments_2', source_topic: 'payments', dataset: 'shop', dataset_id: 'ds-000000000001' },
      ],
      retired_tables: [{ name: 'refunds', source_topic: 'refunds', dataset: 'shop', dataset_id: 'ds-000000000001' }],
      source_topics: ['orders', 'payments'],
      destination_uri: 'gs://lake',
      runtime_mode: 'dedicated',
      namespace: 'midstream',
      datasets: ['shop'],
    },
    status: {
      desired_phase: 'running',
      observed_phase: 'running',
      phase_reason: 'runtime_ready',
      phase_message: null,
      last_transition_time: '2026-10-19T00:00:00Z',
      last_heartbeat: null,
      last_resource_observation_time: null,
      runtime_health: { level: 'healthy', reason: null, message: null },
    },
    runtime_binding: null,
    tables: [summary('orders'), summary('payments_2'), summary('refunds', 'retired'), summary('shipments', 'dropped')],
  };
}

describe('planTopicChange', () => {
  it('asks about the tables of removed topics, and offers to drop the kept ones', () => {
    const change = planTopicChange(detail(), ['orders']);

    assert.deepEqual(change.removed.map((spec) => spec.name), ['payments_2']);
    assert.deepEqual(change.stillKept.map((spec) => spec.name), ['refunds']);
    assert.equal(change.changesTopics, true);
  });

  it('writes a kept table again when its topic is added back, and names new topics apart', () => {
    const change = planTopicChange(detail(), ['orders', 'payments', 'refunds', 'shipments']);

    assert.deepEqual(change.resumed.map((spec) => spec.name), ['refunds']);
    assert.deepEqual(change.added, ['shipments']);
    assert.equal(change.fixed.get('payments'), 'payments_2');
    assert.equal(change.fixed.get('refunds'), 'refunds');
    // Dropped with its files kept: the name is not used again.
    assert.deepEqual(change.taken, ['shipments']);
    assert.equal(change.notes.get('shipments'), 'new table');
    assert.equal(change.notes.get('refunds'), 'kept table, written again');
  });

  it('changes nothing for the topics as they are', () => {
    const change = planTopicChange(detail(), ['orders', 'payments']);

    assert.equal(change.changesTopics, false);
    assert.deepEqual([change.removed, change.resumed, change.added], [[], [], []]);
  });
});

describe('keepOrder', () => {
  it('keeps the topics read already in their order, with the others after them as chosen', () => {
    assert.deepEqual(keepOrder(['orders', 'payments'], ['payments', 'refunds', 'orders']), ['orders', 'payments', 'refunds']);
    assert.deepEqual(keepOrder(['orders', 'payments'], ['refunds']), ['refunds']);
  });
});
