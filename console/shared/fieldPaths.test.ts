import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { fieldName, isMetadataColumn, pathSteps, rowNester, withFieldNames } from './fieldPaths.ts';

describe('fieldName', () => {
  it('drops the payload root', () => {
    assert.equal(fieldName('payload.customer.email'), 'customer.email');
    assert.equal(fieldName('payload.items[].sku'), 'items[].sku');
    assert.equal(fieldName('payload.value'), 'value');
  });

  it('keeps a field that is itself called payload', () => {
    assert.equal(fieldName('payload.payload.after.id'), 'payload.after.id');
  });

  it('keeps the root where a field would take a metadata column name', () => {
    assert.equal(fieldName('payload._kafka_offset'), 'payload._kafka_offset');
    assert.equal(fieldName('payload._kafka_offset.note'), '_kafka_offset.note');
    assert.equal(fieldName('payload._id'), '_id');
  });

  it('leaves metadata and other names alone', () => {
    assert.equal(fieldName('_kafka_timestamp'), '_kafka_timestamp');
    assert.equal(fieldName('count'), 'count');
    assert.equal(fieldName('payload'), 'payload');
    assert.ok(isMetadataColumn('_kafka_partition'));
    assert.ok(!isMetadataColumn('_id'));
  });
});

describe('withFieldNames', () => {
  it('renames paths quoted in runtime messages', () => {
    assert.equal(withFieldNames('payload.total must be a floating-point value'), 'total must be a floating-point value');
    assert.equal(
      withFieldNames('Schema proposal rejected: payload.customer.phone expected=string observed=object'),
      'Schema proposal rejected: customer.phone expected=string observed=object',
    );
    assert.equal(withFieldNames('payload.items[].qty must be an integer.'), 'items[].qty must be an integer.');
  });

  it('leaves prose and physical names alone', () => {
    assert.equal(
      withFieldNames('decoded source payload must be a dictionary-like object'),
      'decoded source payload must be a dictionary-like object',
    );
    assert.equal(withFieldNames('column payload__total__v2'), 'column payload__total__v2');
    assert.equal(withFieldNames('payload._kafka_offset must be an integer'), 'payload._kafka_offset must be an integer');
  });
});

describe('pathSteps', () => {
  it('reads list depth from each segment', () => {
    assert.deepEqual(pathSteps('items[].attributes[].name'), [
      { key: 'items', lists: 1 },
      { key: 'attributes', lists: 1 },
      { key: 'name', lists: 0 },
    ]);
    assert.deepEqual(pathSteps('matrix[][]'), [{ key: 'matrix', lists: 2 }]);
    assert.deepEqual(pathSteps('_kafka_offset'), [{ key: '_kafka_offset', lists: 0 }]);
  });
});

/** Nested output compares as plain data, whatever prototype its objects were built with. */
const plain = (value: unknown): unknown => JSON.parse(JSON.stringify(value));

describe('rowNester', () => {
  it('rebuilds objects from dotted names and keeps metadata at the top', () => {
    const nest = rowNester(['order_id', 'customer.email', 'customer.address.city', '_kafka_offset']);
    assert.deepEqual(
      plain(nest({ order_id: 'o-1', 'customer.email': 'a@b.c', 'customer.address.city': 'Austin', _kafka_offset: 7 })),
      { order_id: 'o-1', customer: { email: 'a@b.c', address: { city: 'Austin' } }, _kafka_offset: 7 },
    );
  });

  it('zips list columns that share an array into one list of objects', () => {
    const nest = rowNester(['items[].sku', 'items[].qty', 'items[].tags']);
    assert.deepEqual(
      plain(nest({ 'items[].sku': ['A', 'B'], 'items[].qty': [1, 2], 'items[].tags': [['x'], []] })),
      { items: [{ sku: 'A', qty: 1, tags: ['x'] }, { sku: 'B', qty: 2, tags: [] }] },
    );
  });

  it('follows lists inside lists', () => {
    const nest = rowNester(['items[].attributes[].name', 'items[].sku']);
    assert.deepEqual(
      plain(nest({ 'items[].attributes[].name': [['color', 'size'], ['fit']], 'items[].sku': ['A', 'B'] })),
      { items: [{ attributes: [{ name: 'color' }, { name: 'size' }], sku: 'A' }, { attributes: [{ name: 'fit' }], sku: 'B' }] },
    );
  });

  it('keeps nulls without inventing structure for a null list', () => {
    const nest = rowNester(['customer.email', 'items[].sku', 'items[].qty']);
    assert.deepEqual(plain(nest({ 'customer.email': null, 'items[].sku': null, 'items[].qty': null })), {
      customer: { email: null },
      items: null,
    });
    assert.deepEqual(plain(nest({ 'customer.email': null, 'items[].sku': ['A'], 'items[].qty': null })), {
      customer: { email: null },
      items: [{ sku: 'A' }],
    });
  });

  it('lets whichever shape a row filled win when a field changed shape', () => {
    // `customer.phone` was text, then became an object; a row fills one or the other.
    const nest = rowNester(['customer.phone.number', 'customer.phone']);
    assert.deepEqual(plain(nest({ 'customer.phone.number': '555', 'customer.phone': null })), {
      customer: { phone: { number: '555' } },
    });
    assert.deepEqual(plain(nest({ 'customer.phone.number': null, 'customer.phone': '555-1234' })), {
      customer: { phone: '555-1234' },
    });
  });

  it('keeps a value under its flat name rather than lose it to a conflict', () => {
    const nest = rowNester(['customer.email', 'customer.email.domain', 'a.b', 'a']);
    assert.deepEqual(plain(nest({ 'customer.email': 'x@y.z', 'customer.email.domain': 'y.z', 'a.b': 1, a: 2 })), {
      customer: { email: 'x@y.z' },
      'customer.email.domain': 'y.z',
      a: { b: 1 },
      'a (2)': 2,
    });
  });

  it('merges a whole array with its elements\' own columns instead of repeating them', () => {
    // `items[].discounts` was materialized whole before its elements had columns; both hold the data.
    const nest = rowNester(['items[].discounts', 'items[].discounts[].code', 'items[].discounts[].amount']);
    const row = {
      'items[].discounts': [[{ amount: 3.01, code: 'FLASH40' }], null],
      'items[].discounts[].code': [['FLASH40'], null],
      'items[].discounts[].amount': [['3.01'], null],
    };
    assert.deepEqual(plain(nest(row)), { items: [{ discounts: [{ amount: 3.01, code: 'FLASH40' }] }, { discounts: null }] });

    const leavesFirst = rowNester(['items[].discounts[].amount', 'items[].discounts']);
    assert.deepEqual(plain(leavesFirst({ 'items[].discounts[].amount': [['3.0']], 'items[].discounts': [[{ amount: 3, note: 'x' }]] })), {
      items: [{ discounts: [{ amount: '3.0', note: 'x' }] }],
    });
  });

  it('decodes JSON text columns into the values they hold', () => {
    const nest = rowNester(['customer.phone', 'items[].attributes'], { jsonColumns: new Set(['customer.phone', 'items[].attributes']) });
    assert.deepEqual(
      plain(nest({ 'customer.phone': '{"number":"555","ext":null}', 'items[].attributes': ['{"color":"red"}', '"plain"'] })),
      { customer: { phone: { number: '555', ext: null } }, items: [{ attributes: { color: 'red' } }, { attributes: 'plain' }] },
    );
    assert.deepEqual(plain(nest({ 'customer.phone': 'not json', 'items[].attributes': null })), {
      customer: { phone: 'not json' },
      items: null,
    });
  });

  it('treats __proto__ as an ordinary key', () => {
    const nest = rowNester(['__proto__.polluted']);
    const nested = nest({ '__proto__.polluted': true });
    assert.equal(({} as Record<string, unknown>).polluted, undefined);
    assert.equal(JSON.stringify(nested), '{"__proto__":{"polluted":true}}');
  });
});
