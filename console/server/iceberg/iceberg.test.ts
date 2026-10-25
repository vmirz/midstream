/**
 * The Iceberg reader, against a table the control plane's own library wrote (`testdata/iceberg`).
 *
 * The fixture is three appends over two days, a schema change that forks `total` and
 * `customer.phone` and adds `coupon`, a fourth append under the new schema, and a compaction of the
 * first day. `expected.json` records what PyIceberg says the current snapshot holds and what the
 * control plane's schema projection renders, so the reader is checked against the reference
 * implementation rather than against itself.
 */

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { after, before, describe, it } from 'node:test';

import { setObjectReader } from './io.ts';
import { currentSchema, partitionGranularity, readMetadata } from './metadata.ts';
import { readPointer } from './pointer.ts';
import { FORK_MAP_PROPERTY, firstSeenSchemaIds, parseForkMap, projectFields } from './schema.ts';
import { partitionKey, resetSnapshotCaches, snapshotCatalog } from './snapshot.ts';
import { loadTable, resetTables } from './table.ts';
import { FIXTURE_DIR, fixtureReader } from './testing.ts';

interface Expected {
  location: string;
  metadata_location: string;
  current_snapshot_id: number | string;
  current_schema_id: number;
  files: Array<{ path: string; record_count: number; file_size: number; partition_key: string; timestamp_min: number; timestamp_max: number }>;
  snapshots: Array<{ snapshot_id: number | string; operation: string; schema_id: number; sequence_number: number }>;
  offsets: Record<string, number>;
  rows: number;
  projection: Record<string, { schema_id: number; field_count: number; fields: Array<Record<string, unknown>> }>;
}

/** Read the fixture's raw text so snapshot ids keep every digit. */
async function expected(): Promise<Expected> {
  const { parseJson } = await import('./json.ts');
  return parseJson(await readFile(join(FIXTURE_DIR, 'expected.json'), 'utf8')) as Expected;
}

let want: Expected;

before(async () => {
  want = await expected();
  setObjectReader(fixtureReader(want.location));
});

after(() => {
  setObjectReader(null);
  resetSnapshotCaches();
  resetTables();
});

describe('the table pointer', () => {
  it('names the metadata file the last commit wrote', async () => {
    const pointer = await readPointer(want.location);
    assert.equal(pointer?.metadataLocation, want.metadata_location);
    assert.equal(pointer?.materializationId, 'mat-a1b2c3d4e5f6');
    assert.deepEqual(pointer?.identifier, ['shop', 'orders']);
  });

  it('is absent for a table that does not exist', async () => {
    assert.equal(await readPointer(`${want.location}-missing`), null);
  });
});

describe('table metadata', () => {
  it('keeps 64-bit snapshot ids exact', async () => {
    const metadata = await readMetadata(want.metadata_location);
    assert.equal(metadata.currentSnapshotId, String(want.current_snapshot_id));
    assert.deepEqual(
      metadata.snapshots.map((snapshot) => [snapshot.snapshotId, snapshot.summary.operation, snapshot.schemaId]),
      want.snapshots.map((snapshot) => [String(snapshot.snapshot_id), snapshot.operation, snapshot.schema_id]),
    );
  });

  it('reads the schemas, the partition spec and the fork map', async () => {
    const metadata = await readMetadata(want.metadata_location);
    assert.equal(metadata.currentSchemaId, want.current_schema_id);
    assert.equal(metadata.schemas.length, 2);
    assert.equal(partitionGranularity(metadata), 'day');
    const forkMap = parseForkMap(metadata.properties[FORK_MAP_PROPERTY]);
    assert.equal(forkMap.get(20)?.forkOf, 11);
    assert.equal(forkMap.get(11)?.state, 'superseded');
  });
});

describe('the snapshot catalog (E5)', () => {
  it('lists exactly the live data files PyIceberg lists, with their counts and sizes', async () => {
    const catalog = await snapshotCatalog(await readMetadata(want.metadata_location));
    assert.deepEqual(
      catalog.objects.map((object) => [object.uri, object.rows, object.bytes]),
      [...want.files].sort((a, b) => a.path.localeCompare(b.path)).map((file) => [file.path, file.record_count, file.file_size]),
    );
    assert.equal(catalog.objects.reduce((sum, object) => sum + (object.rows ?? 0), 0), want.rows);
    assert.equal(catalog.deleteFiles, 0);
    assert.equal(catalog.operation, 'replace');
  });

  it('decodes each file’s timestamp span, partition and offsets from the manifest bounds', async () => {
    const catalog = await snapshotCatalog(await readMetadata(want.metadata_location));
    for (const file of want.files) {
      const object = catalog.objects.find((candidate) => candidate.uri === file.path);
      assert.ok(object, file.path);
      // PyIceberg reports microseconds; the reader works in milliseconds.
      assert.equal(object.timeMin, Math.floor(file.timestamp_min / 1000), `${file.path} timeMin`);
      assert.equal(object.timeMax, Math.floor(file.timestamp_max / 1000), `${file.path} timeMax`);
      assert.equal(object.timeKnown, true);
      assert.equal(object.partitionKey, file.partition_key);
    }
    const compacted = catalog.objects.find((object) => object.uri.includes('/compact-'));
    assert.equal(compacted?.partition, 0, 'a compaction of one Kafka partition still knows it');
    const fresh = catalog.objects.find((object) => object.uri.includes('/p01-'));
    assert.deepEqual([fresh?.partition, fresh?.offsetStart, fresh?.offsetEnd], [1, 10, 11]);
  });

  it('carries the committed offsets and valid-through from the snapshot summary', async () => {
    const catalog = await snapshotCatalog(await readMetadata(want.metadata_location));
    assert.deepEqual(catalog.offsets, want.offsets);
    assert.equal(catalog.validThrough, '2026-10-18T10:09:00.000Z');
  });

  it('reads an older snapshot as it was', async () => {
    const metadata = await readMetadata(want.metadata_location);
    const third = metadata.snapshots[2];
    assert.ok(third);
    const catalog = await snapshotCatalog(metadata, third.snapshotId);
    assert.equal(catalog.objects.length, 3);
    assert.equal(catalog.objects.reduce((sum, object) => sum + (object.rows ?? 0), 0), 10);
    assert.deepEqual(catalog.offsets, { '0': 9 });
  });

  it('names partitions the way the runtime does', () => {
    assert.equal(partitionKey({ ts_day: 20743 }, 'day'), '2026-10-17');
    assert.equal(partitionKey({ ts_hour: 20743 * 24 + 10 }, 'hour'), '2026-10-17-10');
    assert.equal(partitionKey({ ts_day: null }, 'day'), '__null__');
    assert.equal(partitionKey({ ts_month: 681 }, 'month'), '2026-10');
    assert.equal(partitionKey({ ts_month: 0 }, 'month'), '1970-01');
    assert.equal(partitionKey({ ts_month: 11 }, 'month'), '1970-12');
    assert.equal(partitionKey({ ts_month: -1 }, 'month'), '1969-12');
    assert.equal(partitionKey({ ts_month: null }, 'month'), '__null__');
    assert.equal(partitionKey({}, 'none'), null);
  });

  it('refuses a snapshot the table does not hold', async () => {
    const metadata = await readMetadata(want.metadata_location);
    await assert.rejects(() => snapshotCatalog(metadata, '42'), /snapshot 42 is not in this table/);
  });
});

describe('the schema projection', () => {
  it('names, types and states every field exactly as the control plane does', async () => {
    const metadata = await readMetadata(want.metadata_location);
    const forkMap = parseForkMap(metadata.properties[FORK_MAP_PROPERTY]);
    const since = firstSeenSchemaIds(metadata.schemas);
    for (const schema of metadata.schemas) {
      const reference = want.projection[String(schema.schemaId)];
      assert.ok(reference);
      const fields = projectFields(schema, forkMap, { since });
      assert.deepEqual(
        fields.map((field) => ({ ...field, doc: field.doc ?? undefined })),
        reference.fields.map((field) => ({ ...field, doc: field.doc ?? undefined })),
        `schema ${schema.schemaId}`,
      );
    }
    assert.equal(currentSchema(metadata).schemaId, 1);
  });
});

describe('table freshness', () => {
  it('reads the pointer once while a read is fresh', async () => {
    resetTables();
    let reads = 0;
    setObjectReader(async (uri) => {
      if (uri.endsWith('catalog-pointer.json')) reads += 1;
      return fixtureReader(want.location)(uri);
    });
    try {
      const first = await loadTable(want.location, { fresh: true });
      const second = await loadTable(want.location, { fresh: true });
      assert.equal(first?.metadataLocation, want.metadata_location);
      assert.equal(second, first);
      assert.equal(reads, 1);
    } finally {
      setObjectReader(fixtureReader(want.location));
    }
  });
});
