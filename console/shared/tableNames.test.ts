import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  domainIdProblem,
  DATASET_NAME_MAX_LENGTH,
  LABEL_MAX_LENGTH,
  TABLE_NAME_MAX_LENGTH,
  datasetNameProblem,
  labelProblem,
  normalizeLabel,
  qualifiedTableName,
  sameTableName,
  tableNameForTopic,
  tableNameProblem,
  tableNamesForTopics,
} from './tableNames.ts';

/** (topic, table name): the control plane's own vectors, `python/tests/test_table_names.py`. */
const TABLE_NAME_VECTORS: Array<[string, string]> = [
  ['orders', 'orders'],
  ['Orders.V2', 'orders_v2'],
  ['orders-v2', 'orders_v2'],
  ['orders__v2', 'orders_v2'],
  ['materializer-loadtest-orders', 'materializer_loadtest_orders'],
  ['2026-orders', 't_2026_orders'],
  ['order', 'order_topic'],
  ['SELECT', 'select_topic'],
  ['iceberg', 'iceberg_topic'],
  ['__consumer_offsets', 'consumer_offsets'],
  ['---', 'topic'],
  ['_', 'topic'],
  ['x'.repeat(80), 'x'.repeat(63)],
  [`${'a'.repeat(62)}-b`, 'a'.repeat(62)],
];

describe('dataset and table names', () => {
  it('accept what can be typed unquoted, in lowercase', () => {
    for (const name of ['orders', 'orders_2026', 'o', 'materialized', 'data', 'a'.repeat(DATASET_NAME_MAX_LENGTH)]) {
      assert.equal(datasetNameProblem(name), null, name);
      assert.equal(tableNameProblem(name), null, name);
    }
  });

  it('refuse anything else, and say why as the control plane does', () => {
    for (const problem of [datasetNameProblem, tableNameProblem]) {
      assert.equal(problem('  '), 'A name is required');
      assert.equal(problem('1orders'), 'Start with a lowercase letter');
      assert.equal(problem('_orders'), 'Start with a lowercase letter');
      assert.equal(problem('orders-lake'), 'Use only lowercase letters, digits and underscores');
      assert.equal(problem('orders lake'), 'Use only lowercase letters, digits and underscores');
      assert.equal(problem('orders$'), 'Use only lowercase letters, digits and underscores');
      assert.equal(problem('Orders_2026'), 'Lowercase only: "orders_2026"');
      assert.equal(problem('a'.repeat(64)), 'At most 63 characters');
      assert.equal(problem('order'), '"order" is a reserved SQL word');
      assert.equal(problem('read_parquet'), '"read_parquet" is a reserved SQL word');
      assert.equal(problem('lake'), '"lake" is a reserved SQL word');
    }
  });

  it('keep DuckDB\'s own catalogs and schemas from datasets, which are namespaces in SQL', () => {
    assert.equal(datasetNameProblem('memory'), '"memory" is a name DuckDB gives its own catalogs and schemas');
    assert.equal(datasetNameProblem('main'), '"main" is a name DuckDB gives its own catalogs and schemas');
    assert.equal(datasetNameProblem('information_schema'), '"information_schema" is a name DuckDB gives its own catalogs and schemas');
    assert.equal(tableNameProblem('memory'), null, 'a table may take one');
    assert.equal(tableNameForTopic('memory'), 'memory');
  });

  it('follow the control plane\'s own limits and reserved words when it says them', () => {
    const rules = { maxLength: 10, reserved: new Set(['orders']) };
    assert.equal(tableNameProblem('orders', rules), '"orders" is a reserved SQL word');
    assert.equal(tableNameProblem('select', rules), null);
    assert.equal(datasetNameProblem('a'.repeat(11), rules), 'At most 10 characters');
  });

  it('compare without regard to case, as SQL does', () => {
    assert.equal(sameTableName('Orders', 'orders '), true);
    assert.equal(sameTableName('orders', 'orders_2'), false);
    assert.equal(sameTableName(null, 'orders'), false);
  });

  it('read as `<dataset>.<table>`', () => {
    assert.equal(qualifiedTableName('sales', 'orders'), 'sales.orders');
  });
});

describe('materialization labels', () => {
  it('are free text, at most 128 characters once trimmed', () => {
    for (const label of ['', 'Shop', 'Orders — EU, 2026!', `  ${'a'.repeat(LABEL_MAX_LENGTH)}  `]) {
      assert.equal(labelProblem(label), null, label);
    }
    assert.equal(labelProblem('a'.repeat(LABEL_MAX_LENGTH + 1)), 'At most 128 characters');
    assert.equal(labelProblem('line\nbreak'), 'Use no control characters');
  });

  it('are kept trimmed, with runs of space as one', () => {
    assert.equal(normalizeLabel('  Shop   orders \t EU '), 'Shop orders EU');
    assert.equal(normalizeLabel('   '), '');
  });
});

describe('table names', () => {
  it('are derived from the topic as the control plane derives them', () => {
    for (const [topic, expected] of TABLE_NAME_VECTORS) {
      const name = tableNameForTopic(topic);
      assert.equal(name, expected, topic);
      assert.ok(name.length <= TABLE_NAME_MAX_LENGTH);
      assert.equal(tableNameProblem(name), null, `${name} is a name SQL reads unquoted`);
    }
  });

  it('take suffixes, in the topics’ order, when two would be the same', () => {
    assert.deepEqual(tableNamesForTopics(['orders.v2', 'orders-v2', 'ORDERS_V2']), ['orders_v2', 'orders_v2_2', 'orders_v2_3']);
    assert.deepEqual(tableNamesForTopics(['orders_2', 'orders', 'orders-2']), ['orders_2', 'orders', 'orders_2_2']);
    assert.deepEqual(tableNamesForTopics(['x'.repeat(80), 'x'.repeat(81)]), ['x'.repeat(63), `${'x'.repeat(61)}_2`]);
    assert.deepEqual(tableNamesForTopics([]), []);
  });

  it('keep the tables an edit already has, and give new topics names nothing else uses', () => {
    // As `python/tests/test_topic_changes.py`: a kept table and one dropped with its files hold their names.
    const fixed = new Map([
      ['orders', 'orders'],
      ['orders-v2', 'orders_v2'],
    ]);
    assert.deepEqual(tableNamesForTopics(['orders', 'orders.v2', 'orders.v3', 'Orders'], undefined, { fixed, taken: ['ORDERS_V3'] }), [
      'orders',
      'orders_v2_2',
      'orders_v3_2',
      'orders_2',
    ]);
    // A topic already read keeps its name even where it would be derived otherwise now.
    assert.deepEqual(tableNamesForTopics(['payments'], undefined, { fixed: new Map([['payments', 'payments_2']]) }), ['payments_2']);
  });
});

describe('domain ids', () => {
  it('follow a dataset name\'s rule, at most 32 characters', () => {
    for (const good of ['domain_a', 'sales', 'a', 'scm_2', 'x'.repeat(32)]) assert.equal(domainIdProblem(good), null, good);
    for (const bad of ['', 'Domain_A', 'domain-a', '1domain', '_a', 'x'.repeat(33)]) assert.notEqual(domainIdProblem(bad), null, bad);
  });

  it('are not words the console\'s URLs or SQL use, as the control plane says', () => {
    for (const reserved of ['api', 'd', 'catalog', 'iceberg', 'main', 'memory', 'temp', 'system', 'select', 'lake']) {
      assert.notEqual(domainIdProblem(reserved), null, reserved);
    }
    assert.match(domainIdProblem('orders_iceberg') ?? '', /SQL/);
  });
});

