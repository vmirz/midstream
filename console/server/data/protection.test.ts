/**
 * The console's reader of a table's `materializer.protection` property (handbook 22), against what the
 * control plane writes: `testdata/protection.json` is made by `python/tests/console_fixture.py` with
 * the control plane's own code - a schema inferred from masked samples, and its recorded protection.
 */

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { currentSchema, parseMetadata } from '../iceberg/metadata.ts';
import { parseForkMap, parseProtection } from '../iceberg/schema.ts';
import { logicalColumns, toQueryColumns } from './logical.ts';

const FIXTURE = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'testdata', 'protection.json');

interface ProtectionFixture {
  schema: Record<string, unknown>;
  fork_map: unknown;
  protection_property: string;
  expected: Record<string, { kind: string; mask: string }>;
}

async function fixture(): Promise<ProtectionFixture> {
  return JSON.parse(await readFile(FIXTURE, 'utf8')) as ProtectionFixture;
}

describe('a table’s recorded protection', () => {
  it('reads each protected field by id, and not those written plain again', async () => {
    const { protection_property: property } = await fixture();
    const protection = parseProtection(property);
    const recorded = JSON.parse(property) as { fields: Record<string, { until?: number; kind: string; mask: string }> };
    const active = Object.entries(recorded.fields).filter(([, entry]) => entry.until === undefined);
    assert.equal(protection.size, active.length);
    for (const [id, entry] of active) assert.deepEqual(protection.get(Number(id)), { kind: entry.kind, mask: entry.mask });
    assert.ok(Object.values(recorded.fields).some((entry) => entry.until !== undefined), 'the fixture has a field written plain again');
  });

  it('marks exactly the columns the control plane protects, under the Schema tab’s names', async () => {
    const { schema, fork_map: forkMap, protection_property: property, expected } = await fixture();
    const metadata = parseMetadata(JSON.stringify({ 'format-version': 2, schemas: [schema], 'current-schema-id': schema['schema-id'] }));
    const columns = logicalColumns(currentSchema(metadata), parseForkMap(JSON.stringify(forkMap)), parseProtection(property));
    const marked = Object.fromEntries(columns.filter((column) => column.protection).map((column) => [column.name, column.protection]));
    assert.deepEqual(marked, expected);
    const query = toQueryColumns(columns);
    assert.deepEqual(query.find((column) => column.name === 'ssn')?.protection, { kind: 'national_id', mask: 'hash' });
    assert.equal(query.find((column) => column.name === 'order_id')?.protection, undefined);
  });

  it('reads nothing protected from a table without the property, or with one it cannot read', () => {
    for (const text of [undefined, null, '', 'not json', '{"fields": {"x": {}}}', '{"fields": {"7": {"kind": 1}}}']) {
      assert.equal(parseProtection(text).size, 0, String(text));
    }
  });
});
