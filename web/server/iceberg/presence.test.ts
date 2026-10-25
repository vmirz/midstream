/**
 * When each field holds values, read from the fixture table's manifests (`testdata/iceberg/orders`).
 *
 * The fixture's day one (2026-10-17, offsets 0-6 at 10:00-10:06) is compacted into one file; day two
 * holds offsets 7-9 (10:07-10:09) in the first schema and, after `total` forked to a double,
 * `customer.phone` to a struct and `coupon` was added, offsets 10-11 (10:10-10:11) in the second.
 */

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';

import { setObjectReader } from './io.ts';
import { currentSchema, readMetadata, type IcebergType, type TableMetadata } from './metadata.ts';
import { readPointer } from './pointer.ts';
import { tablePresence, type FieldPresence } from './presence.ts';
import { resetSnapshotCaches } from './snapshot.ts';
import { FIXTURE_LOCATION, fixtureReader } from './testing.ts';

const at = (iso: string): number => Date.parse(iso);
const DAY1_FIRST = at('2026-10-17T10:00:00Z');
const DAY1_LAST = at('2026-10-17T10:06:00Z');
const DAY2_V1_FIRST = at('2026-10-18T10:07:00Z');
const DAY2_V1_LAST = at('2026-10-18T10:09:00Z');
const V2_FIRST = at('2026-10-18T10:10:00Z');
const V2_LAST = at('2026-10-18T10:11:00Z');

let metadata: TableMetadata;

before(async () => {
  setObjectReader(fixtureReader());
  const pointer = await readPointer(FIXTURE_LOCATION);
  metadata = await readMetadata(pointer!.metadataLocation);
});

after(() => {
  setObjectReader(null);
  resetSnapshotCaches();
});

/** The id of the field at `path` in the current schema: `['customer', 'phone__v2']`. */
function idOf(path: string[]): number {
  let fields = currentSchema(metadata).fields;
  let id = -1;
  for (const name of path) {
    const field = fields.find((candidate) => candidate.name === name);
    assert.ok(field, `no field ${path.join('.')}`);
    id = field.id;
    const type: IcebergType = field.type;
    fields = typeof type !== 'string' && type.type === 'struct' ? type.fields : [];
  }
  return id;
}

async function presenceOf(path: string[]): Promise<FieldPresence | undefined> {
  return (await tablePresence(metadata)).fields.get(idOf(path));
}

describe('when each field holds values', () => {
  it('spans the Kafka timestamps the table holds', async () => {
    const presence = await tablePresence(metadata);
    assert.equal(presence.snapshotId, metadata.currentSnapshotId);
    assert.equal(presence.dataFrom, DAY1_FIRST);
    assert.equal(presence.dataThrough, V2_LAST);
  });

  it('dates a field written from the first records by its first and last files', async () => {
    // Day one is one compacted file, so the first record lies within its span; the last in the newest file.
    assert.deepEqual(await presenceOf(['customer', 'email']), {
      first: { from: DAY1_FIRST, to: DAY1_LAST },
      last: { from: V2_FIRST, to: V2_LAST },
      files: 3,
      values: 12,
    });
  });

  it('dates a fork by its own values, and the field it took over from by where they stop', async () => {
    // `total` held longs until the fork; its doubles are in `total__v2`, from the second schema on.
    assert.deepEqual(await presenceOf(['total']), {
      first: { from: DAY1_FIRST, to: DAY1_LAST },
      last: { from: DAY2_V1_FIRST, to: DAY2_V1_LAST },
      files: 2,
      values: 10,
    });
    assert.deepEqual(await presenceOf(['total__v2']), {
      first: { from: V2_FIRST, to: V2_LAST },
      last: { from: V2_FIRST, to: V2_LAST },
      files: 1,
      values: 2,
    });
  });

  it('dates an added field by the first file that holds it, whatever the schema it arrived in', async () => {
    const coupon = await presenceOf(['coupon']);
    assert.deepEqual(coupon?.first, { from: V2_FIRST, to: V2_LAST });
  });

  it('dates a struct by its fields, and leaves its counts to them', async () => {
    const phone = await presenceOf(['customer', 'phone__v2']);
    assert.deepEqual(phone, { first: { from: V2_FIRST, to: V2_LAST }, last: { from: V2_FIRST, to: V2_LAST }, files: null, values: null });
    const customer = await presenceOf(['customer']);
    assert.deepEqual(customer?.first, { from: DAY1_FIRST, to: DAY1_LAST });
    assert.deepEqual(customer?.last, { from: V2_FIRST, to: V2_LAST });
  });

  it('leaves out a field no record holds a value of', async () => {
    // `_unmapped` is null on every row of the fixture.
    assert.equal(await presenceOf(['_unmapped']), undefined);
  });

  it('answers nothing for a table with no snapshot', async () => {
    const empty = await tablePresence({ ...metadata, currentSnapshotId: null });
    assert.deepEqual(empty, { snapshotId: null, dataFrom: null, dataThrough: null, fields: new Map() });
  });
});
