import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { diffSchemas } from './diff.ts';
import type { CpSchema, CpSchemaField } from '../../shared/types.ts';

function field(overrides: Partial<CpSchemaField> & Pick<CpSchemaField, 'field_id' | 'name'>): CpSchemaField {
  return {
    path: [overrides.name],
    type: 'string',
    required: false,
    kind: 'value',
    state: 'current',
    fork_version: 1,
    ...overrides,
  };
}

function schema(schemaId: number, fields: CpSchemaField[]): CpSchema {
  return {
    materialization_id: 'mat-1',
    table: 'orders',
    dataset: 'shop',
    dataset_id: 'ds-000000000001',
    source_topic: 'orders',
    schema_id: schemaId,
    since: null,
    metadata_location: null,
    field_count: fields.length,
    fields,
    changes: [],
  };
}

const KAFKA = field({ field_id: 1, name: '_kafka_timestamp', type: 'timestamptz', kind: 'metadata' });

describe('schema diffs', () => {
  it('match fields by id and name every kind of change', () => {
    const before = schema(0, [
      KAFKA,
      field({ field_id: 10, name: 'id' }),
      field({ field_id: 11, name: 'total', type: 'long' }),
      field({ field_id: 12, name: 'phone' }),
    ]);
    const after = schema(1, [
      KAFKA,
      field({ field_id: 10, name: 'id' }),
      field({ field_id: 11, name: 'total', type: 'long', state: 'superseded' }),
      field({ field_id: 20, name: 'total', path: ['total__v2'], type: 'double', fork_version: 2, fork_of: 11 }),
      field({ field_id: 12, name: 'phone', state: 'superseded' }),
      field({ field_id: 21, name: 'phone', path: ['phone__json'], kind: 'json', state: 'collapsed', fork_version: 3, fork_of: 12 }),
      field({ field_id: 22, name: 'coupon' }),
    ]);
    const diff = diffSchemas('mat-1', before, after);
    assert.deepEqual([diff.from, diff.to], [0, 1]);
    const kinds = Object.fromEntries(diff.entries.map((entry) => [entry.field_id, entry.kind]));
    assert.deepEqual(kinds, { 22: 'added', 20: 'forked', 21: 'collapsed', 11: 'superseded', 12: 'superseded', 10: 'unchanged' });
    assert.equal(diff.summary.added, 1);
    assert.equal(diff.summary.forked, 1);
    assert.equal(diff.summary.collapsed, 1);
    assert.equal(diff.summary.superseded, 2);
    assert.ok(!diff.entries.some((entry) => entry.field_id === 1), 'metadata columns are never part of a diff');
  });

  it('report a type promotion and a nullability change on the same field id', () => {
    const before = schema(2, [field({ field_id: 10, name: 'n', type: 'int', required: true })]);
    const after = schema(3, [field({ field_id: 10, name: 'n', type: 'long', required: true }), field({ field_id: 11, name: 'm', required: false })]);
    const widened = diffSchemas('mat-1', before, after).entries.find((entry) => entry.field_id === 10);
    assert.equal(widened?.kind, 'type_widened');
    assert.deepEqual(widened?.changes, [{ attribute: 'type', from: 'int', to: 'long' }]);

    const relaxed = diffSchemas('mat-1', schema(3, [field({ field_id: 11, name: 'm', required: true })]), after);
    assert.equal(relaxed.entries.find((entry) => entry.field_id === 11)?.kind, 'required_changed');
  });

  it('diff the first schema against nothing', () => {
    const diff = diffSchemas('mat-1', null, schema(0, [KAFKA, field({ field_id: 10, name: 'id' })]));
    assert.equal(diff.from, null);
    assert.deepEqual(diff.entries.map((entry) => entry.kind), ['added']);
  });
});
