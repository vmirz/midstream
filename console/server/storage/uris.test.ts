import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { formatStorageUri, isLocalUri, joinStorageUri, objectKey, parseStorageUri } from './uris.ts';

describe('storage URIs', () => {
  it('take the three forms apart', () => {
    assert.deepEqual(parseStorageUri('gs://lake/sales/orders/metadata/v1.metadata.json'), {
      scheme: 'gs',
      container: 'lake',
      key: 'sales/orders/metadata/v1.metadata.json',
    });
    assert.deepEqual(parseStorageUri('s3://example-lake/'), { scheme: 's3', container: 'example-lake', key: '' });
    assert.deepEqual(parseStorageUri('s3://example-lake'), { scheme: 's3', container: 'example-lake', key: '' });
    assert.deepEqual(parseStorageUri('abfss://lake@exampleacct.dfs.core.windows.net/sales/orders/data/a.parquet'), {
      scheme: 'abfss',
      container: 'lake',
      account: 'exampleacct',
      key: 'sales/orders/data/a.parquet',
    });
  });

  it('put them back together, and join paths onto them', () => {
    for (const uri of ['gs://lake/a/b.json', 's3://example-lake/p/', 'abfss://lake@exampleacct.dfs.core.windows.net/x/y.parquet']) {
      assert.equal(formatStorageUri(parseStorageUri(uri)!), uri);
    }
    assert.equal(joinStorageUri('s3://lake/sales/orders/', '/metadata/catalog-pointer.json'), 's3://lake/sales/orders/metadata/catalog-pointer.json');
    assert.equal(objectKey('abfss://lake@exampleacct.dfs.core.windows.net/sales/a.parquet'), 'sales/a.parquet');
    assert.equal(objectKey('/tmp/run/tail-0.parquet'), '/tmp/run/tail-0.parquet');
  });

  it('keep local paths local, and recognise no other form', () => {
    assert.equal(isLocalUri('/var/lib/midstream-console/runs/r1/tail-0.parquet'), true);
    assert.equal(isLocalUri('file:///tmp/lake/a.parquet'), true);
    assert.equal(isLocalUri('relative/path.parquet'), true);
    for (const uri of ['s3://lake/a', 'gs://lake/a', 'abfss://c@a.dfs.core.windows.net/k', 'az://c/k', 'https://lake.s3.amazonaws.com/a', 'gcs://lake/a']) {
      assert.equal(isLocalUri(uri), false, uri);
    }
    for (const uri of [
      'az://lake/a',
      'wasbs://lake@exampleacct.blob.core.windows.net/a',
      'abfss://lake@exampleacct.blob.core.windows.net/a',
      'abfss://lake/a',
      's3:///a',
      'gs://../a',
      'abfss://..@exampleacct.dfs.core.windows.net/a',
      '/tmp/a.parquet',
    ]) {
      assert.equal(parseStorageUri(uri), null, uri);
    }
  });
});
