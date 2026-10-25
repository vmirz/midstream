import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { TopicSummary } from '@shared/types';

import { indexTopics, searchTopics, segmentName } from './topicSearch.ts';

function topic(name: string, category?: TopicSummary['category']): TopicSummary {
  return { name, partitions: 3, replicationFactor: 2, ...(category ? { category } : {}) };
}

// The topics on the cluster this was built against, in catalog (alphabetical) order.
const CLUSTER = indexTopics(
  [
    topic('__consumer_offsets', 'kafka'),
    topic('develop-materialization-requests', 'materializer'),
    topic('joiner-app-orders-changelog', 'stream'),
    topic('materializer-loadtest-orders'),
    topic('mimir-ingest'),
    topic('orders'),
    topic('orders.cdc.v2'),
    topic('products'),
    topic('reorders'),
  ].sort((left, right) => left.name.localeCompare(right.name)),
);

const names = (query: string, includeHidden = false): string[] =>
  searchTopics(CLUSTER, query, { includeHidden }).matches.map((match) => match.topic.name);

describe('searchTopics', () => {
  it('lists every visible topic alphabetically when there is no query', () => {
    assert.deepEqual(names(''), [
      'materializer-loadtest-orders',
      'mimir-ingest',
      'orders',
      'orders.cdc.v2',
      'products',
      'reorders',
    ]);
  });

  it('ranks the exact name, then prefixes, then word starts, then substrings', () => {
    assert.deepEqual(names('orders'), ['orders', 'orders.cdc.v2', 'materializer-loadtest-orders', 'reorders']);
  });

  it('requires every term, in any order', () => {
    assert.deepEqual(names('v2 orders'), ['orders.cdc.v2']);
    assert.deepEqual(names('load orders'), ['materializer-loadtest-orders']);
    assert.deepEqual(names('orders nothing'), []);
  });

  it('ignores case and surrounding whitespace', () => {
    assert.deepEqual(names('  MIMIR  '), ['mimir-ingest']);
  });

  it('keeps hidden topics out but counts the ones that match', () => {
    const result = searchTopics(CLUSTER, 'orders', { includeHidden: false });
    assert.equal(result.hiddenMatches, 1);
    assert.ok(names('orders', true).includes('joiner-app-orders-changelog'));
    assert.equal(CLUSTER.hiddenTotal, 3);
  });

  it('marks the occurrence that starts a word rather than the first one', () => {
    const [match] = searchTopics(indexTopics([topic('reorders-orders')]), 'orders', { includeHidden: false }).matches;
    assert.deepEqual(match!.ranges, [[9, 15]]);
  });

  it('merges overlapping term ranges', () => {
    const [match] = searchTopics(CLUSTER, 'order ders', { includeHidden: false }).matches;
    assert.equal(match!.topic.name, 'orders');
    assert.deepEqual(match!.ranges, [[0, 6]]);
  });

  it('stays interactive on a fifty-thousand topic cluster', () => {
    const big = indexTopics(
      Array.from({ length: 50_000 }, (_, index) => topic(`team-${index % 400}.service-${index}.events.v${index % 7}`)),
    );
    const started = performance.now();
    const result = searchTopics(big, 'service-4 events v3', { includeHidden: false });
    const elapsed = performance.now() - started;

    assert.ok(result.matches.length > 0);
    assert.ok(elapsed < 100, `search took ${elapsed.toFixed(1)}ms`);
  });
});

describe('segmentName', () => {
  it('splits a name into plain and matched runs', () => {
    assert.deepEqual(segmentName('orders.cdc.v2', [[0, 6], [11, 13]]), [
      { text: 'orders', match: true },
      { text: '.cdc.', match: false },
      { text: 'v2', match: true },
    ]);
  });

  it('returns the whole name when nothing matched', () => {
    assert.deepEqual(segmentName('orders', []), [{ text: 'orders', match: false }]);
  });
});
