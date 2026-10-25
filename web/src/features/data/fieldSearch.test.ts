import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { QueryColumn } from '@shared/types';

import { searchFields } from './fieldSearch.ts';

const columns: QueryColumn[] = [
  'order_id',
  'customer.id',
  'customer.email_hash',
  'customer.preferences.email_opt_in',
  'items[].sku',
  'items[].category.name',
  'status',
  '_kafka_timestamp',
  '_kafka_offset',
].map((name) => ({ name, type: 'VARCHAR' }));

const names = (query: string): string[] => searchFields(columns, query).map((match) => match.column.name);

describe('searchFields', () => {
  it('returns every column, in order, for a blank query', () => {
    assert.deepEqual(names('  '), columns.map((column) => column.name));
  });

  it('ranks a term starting a path segment ahead of a plain substring', () => {
    assert.deepEqual(names('email'), ['customer.email_hash', 'customer.preferences.email_opt_in']);
    assert.deepEqual(names('id'), ['order_id', 'customer.id']);
  });

  it('puts the exact name first', () => {
    assert.equal(names('status')[0], 'status');
  });

  it('needs every term, in any order', () => {
    assert.deepEqual(names('name items'), ['items[].category.name']);
    assert.deepEqual(names('kafka off'), ['_kafka_offset']);
    assert.deepEqual(names('nothing like it'), []);
  });

  it('marks what matched', () => {
    const [match] = searchFields(columns, 'sku');
    assert.deepEqual(match?.ranges, [[8, 11]]);
  });
});
