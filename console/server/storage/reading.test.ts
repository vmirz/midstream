/**
 * How the console reads through its profiles, with each profile's reader swapped for a seam so no
 * test touches a network: metadata objects go to the profile that covers them, data files reach
 * DuckDB in place or from the cache by their profile's mode, the cached budget counts only cached
 * profiles' files, and nothing outside every profile is read or handed to DuckDB.
 */

import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, afterEach, describe, it } from 'node:test';

const dataDir = await mkdtemp(join(tmpdir(), 'console-reading-'));
process.env.CONSOLE_DATA_DIR = dataDir;

const { createProfile, useStorage, validateStorageRegistry } = await import('./registry.ts');
const { cachePathFor } = await import('./cache.ts');
const { readObject, ObjectMissingError, setObjectReader } = await import('../iceberg/io.ts');
const { objectSources, objectSource, assertWithinCachedBudget, directSource } = await import('../data/objects.ts');
const { tableStorage } = await import('../routes/table.ts');
const { config } = await import('../config.ts');
const { ConsoleError } = await import('../errors.ts');
const { StorageReadError } = await import('./profile.ts');
import type { DataObject } from '../iceberg/snapshot.ts';
import type { StorageProfile } from './profile.ts';

const read: string[] = [];
const downloaded: string[] = [];

const profiles: StorageProfile[] = validateStorageRegistry(
  [
    { name: 'lake', scope: 's3://example-lake/', kind: 's3', endpoint: 'http://seaweedfs:8333', pathStyle: true, region: 'us-east-1' },
    { name: 'lake-cold', scope: 's3://example-lake/cold/', kind: 's3', endpoint: 'http://seaweedfs:8333', access: 'cached' },
    { name: 'archive', scope: 'gs://example-archive/', kind: 'gcs' },
    { name: 'azure-lake', scope: 'abfss://lake@exampleacct.dfs.core.windows.net/', kind: 'azure' },
  ],
  { CONSOLE_STORAGE_LAKE_ACCESS_KEY_ID: 'console-key-id', CONSOLE_STORAGE_LAKE_SECRET_ACCESS_KEY: 'console-secret' },
).map((profile) =>
  createProfile(profile, {
    readObject: async (uri) => {
      read.push(`${profile.name} ${uri}`);
      if (uri.endsWith('missing.json')) throw new StorageReadError(uri, 404, 'NoSuchKey');
      return Buffer.from(profile.name);
    },
    downloadToFile: async (uri, target) => {
      downloaded.push(`${profile.name} ${uri}`);
      await writeFile(target, `${profile.name}:${uri}`);
    },
  }),
);
useStorage(profiles);

const object = (uri: string, bytes = 1_000): DataObject => ({
  uri,
  name: uri,
  bytes,
  rows: 1,
  committedAt: null,
  partition: null,
  offsetStart: null,
  offsetEnd: null,
  timeMin: null,
  timeMax: null,
  timeKnown: false,
  partitionKey: null,
});

after(() => rm(dataDir, { recursive: true, force: true }));
afterEach(() => {
  read.length = 0;
  downloaded.length = 0;
  setObjectReader(null);
});

describe('metadata objects', () => {
  it('are read through the profile that covers each, by the longest scope', async () => {
    assert.deepEqual(await readObject('s3://example-lake/sales/orders/metadata/catalog-pointer.json'), Buffer.from('lake'));
    assert.deepEqual(await readObject('s3://example-lake/cold/orders/metadata/v1.metadata.json'), Buffer.from('lake-cold'));
    assert.deepEqual(await readObject('gs://example-archive/x/metadata/catalog-pointer.json'), Buffer.from('archive'));
    assert.deepEqual(await readObject('abfss://lake@exampleacct.dfs.core.windows.net/x/metadata/catalog-pointer.json'), Buffer.from('azure-lake'));
    assert.deepEqual(read, [
      'lake s3://example-lake/sales/orders/metadata/catalog-pointer.json',
      'lake-cold s3://example-lake/cold/orders/metadata/v1.metadata.json',
      'archive gs://example-archive/x/metadata/catalog-pointer.json',
      'azure-lake abfss://lake@exampleacct.dfs.core.windows.net/x/metadata/catalog-pointer.json',
    ]);
  });

  it('are missing when the profile says so, as a table not created yet', async () => {
    await assert.rejects(readObject('s3://example-lake/sales/orders/metadata/missing.json'), ObjectMissingError);
  });

  it('are read from disk when local, and refused when no profile covers them', async () => {
    const file = join(dataDir, 'pointer.json');
    await writeFile(file, '{}');
    assert.deepEqual(await readObject(file), Buffer.from('{}'));
    assert.deepEqual(await readObject(`file://${file}`), Buffer.from('{}'));
    for (const uri of ['s3://other-lake/x/metadata/catalog-pointer.json', 'az://lake/x.json', 'https://example.com/x.json']) {
      await assert.rejects(readObject(uri), (error: unknown) => error instanceof ConsoleError && error.code === 'storage_not_configured' && error.statusCode === 503);
    }
    assert.deepEqual(read, []);
  });
});

describe('data files', () => {
  it('reach DuckDB in place for a direct profile, from the cache for a cached one, as they are when local', async () => {
    const tail = join(dataDir, 'tail-0.parquet');
    const sources = await objectSources([
      object('s3://example-lake/sales/orders/data/a.parquet'),
      object('s3://example-lake/cold/orders/data/b.parquet'),
      object('gs://example-archive/x/data/c.parquet'),
      object('abfss://lake@exampleacct.dfs.core.windows.net/x/data/d.parquet'),
      object(tail),
    ]);
    assert.deepEqual(Object.fromEntries(sources), {
      's3://example-lake/sales/orders/data/a.parquet': 's3://example-lake/sales/orders/data/a.parquet',
      's3://example-lake/cold/orders/data/b.parquet': join(config.parquetCacheDir, 'lake-cold', 'example-lake', 'cold', 'orders', 'data', 'b.parquet'),
      'gs://example-archive/x/data/c.parquet': join(config.parquetCacheDir, 'archive', 'example-archive', 'x', 'data', 'c.parquet'),
      'abfss://lake@exampleacct.dfs.core.windows.net/x/data/d.parquet': 'az://exampleacct.blob.core.windows.net/lake/x/data/d.parquet',
      [tail]: tail,
    });
    assert.deepEqual(downloaded.sort(), ['archive gs://example-archive/x/data/c.parquet', 'lake-cold s3://example-lake/cold/orders/data/b.parquet']);
    assert.equal(await readFile(join(config.parquetCacheDir, 'archive', 'example-archive', 'x', 'data', 'c.parquet'), 'utf8'), 'archive:gs://example-archive/x/data/c.parquet');
  });

  it('never reach DuckDB without a profile covering them', async () => {
    await assert.rejects(objectSource(object('s3://other-lake/x/data/a.parquet')), (error: unknown) => error instanceof ConsoleError && error.code === 'storage_not_configured');
    await assert.rejects(objectSources([object('s3://example-lake/a.parquet'), object('az://lake/x.parquet')]), /no storage is configured for az:\/\/lake\/x\.parquet/);
    assert.equal(await directSource('s3://other-lake/x.parquet'), null);
    assert.equal(await directSource('gs://example-archive/x.parquet'), null, 'a cached profile is not read in place');
  });

  it('are cached under their profile and container, with no way out of the cache', () => {
    const cold = profiles[1] as StorageProfile;
    assert.equal(cachePathFor({ uri: 's3://example-lake/cold/a/../../b/./c.parquet', profile: cold }), join(config.parquetCacheDir, 'lake-cold', 'example-lake', 'cold', 'a', 'b', 'c.parquet'));
    assert.equal(cachePathFor({ uri: 's3://example-lake/cold//x\0y.parquet', profile: cold }), join(config.parquetCacheDir, 'lake-cold', 'example-lake', 'cold', 'xy.parquet'));
    // A `.\0.` segment must not survive the drop and turn into `..`: null bytes are stripped first.
    assert.equal(
      cachePathFor({ uri: 's3://example-lake/cold/.\0./.\0./a/b.parquet', profile: cold }),
      join(config.parquetCacheDir, 'lake-cold', 'example-lake', 'cold', 'a', 'b.parquet'),
    );
    // However many `.\0.` segments, every one is dropped, so the path cannot climb out of the cache dir.
    assert.equal(
      cachePathFor({ uri: 's3://example-lake/cold/.\0./.\0./.\0./.\0./escaped.parquet', profile: cold }),
      join(config.parquetCacheDir, 'lake-cold', 'example-lake', 'cold', 'escaped.parquet'),
    );
    assert.throws(() => cachePathFor({ uri: 's3://example-lake/../..', profile: cold }), /not usable as a cache path/);
  });
});

describe('the cached budget', () => {
  it('counts only cached profiles’ files, and names the profiles to read in place', () => {
    const over = config.cachedModeMaxFiles + 1;
    const direct = Array.from({ length: over }, (_, index) => object(`s3://example-lake/sales/data/${index}.parquet`));
    assert.doesNotThrow(() => assertWithinCachedBudget(direct));
    const local = Array.from({ length: over }, (_, index) => object(join(dataDir, `${index}.parquet`)));
    assert.doesNotThrow(() => assertWithinCachedBudget(local));
    const cold = Array.from({ length: over }, (_, index) => object(`s3://example-lake/cold/data/${index}.parquet`));
    assert.throws(() => assertWithinCachedBudget(cold), /configure direct access for storage profile lake-cold so files are read in place/);
    const mixed = [...cold.slice(0, over - 1), object('gs://example-archive/x/data/1.parquet')];
    assert.throws(() => assertWithinCachedBudget(mixed), /storage profiles lake-cold and archive so files/);
    assert.throws(() => assertWithinCachedBudget([object('gs://example-archive/big.parquet', config.cachedModeMaxBytes + 1)]), /storage profile archive/);
  });
});

describe('the Table tab', () => {
  it('describes the storage of a table’s location, without its credentials', () => {
    assert.deepEqual(tableStorage('s3://example-lake/sales/orders'), {
      name: 'lake',
      kind: 's3',
      scope: 's3://example-lake/',
      endpoint: 'http://seaweedfs:8333',
      region: 'us-east-1',
      pathStyle: true,
      mode: 'direct',
      account: null,
    });
    assert.equal(tableStorage('abfss://lake@exampleacct.dfs.core.windows.net/sales/orders')?.account, 'exampleacct');
    assert.equal(tableStorage('s3://other-lake/sales/orders'), null);
    assert.equal(tableStorage(null), null);
    assert.ok(!JSON.stringify(tableStorage('s3://example-lake/sales/orders')).includes('console-'));
  });
});
