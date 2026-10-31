/**
 * The storage registry: every rule the chart checks, and what DuckDB cannot tell apart; credentials
 * from the environment only; the longest scope serving an object; and the refusal of a location no
 * profile covers.
 */

import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

import { ConsoleError } from '../errors.ts';
import { StorageRegistry, StorageRegistryError, createProfile, loadStorageRegistry, validateStorageRegistry } from './registry.ts';

const lake = { name: 'lake', scope: 's3://example-lake/', kind: 's3', endpoint: 'http://seaweedfs-s3.seaweedfs.svc:8333', region: 'us-east-1', pathStyle: true };
const archive = { name: 'archive', scope: 'gs://example-archive/', kind: 'gcs', keyFile: '/etc/midstream-storage/archive/service-account.json' };
const azure = { name: 'azure-lake', scope: 'abfss://lake@exampleacct.dfs.core.windows.net/', kind: 'azure' };

const refuses = (raw: unknown, pattern: RegExp, env: Record<string, string> = {}): void => {
  assert.throws(() => validateStorageRegistry(raw, env), (error: unknown) => error instanceof StorageRegistryError && pattern.test(error.message), String(pattern));
};

const dir = await mkdtemp(join(tmpdir(), 'console-storage-registry-'));
after(() => rm(dir, { recursive: true, force: true }));

describe('the storage registry', () => {
  it('reads the design’s example, each profile as it says', () => {
    const profiles = validateStorageRegistry([lake, archive, azure], {});
    assert.deepEqual(
      profiles.map(({ name, scope, kind, endpoint, region, pathStyle, access, keyFile }) => ({ name, scope, kind, endpoint, region, pathStyle, access, keyFile })),
      [
        { name: 'lake', scope: 's3://example-lake/', kind: 's3', endpoint: 'http://seaweedfs-s3.seaweedfs.svc:8333', region: 'us-east-1', pathStyle: true, access: null, keyFile: null },
        { name: 'archive', scope: 'gs://example-archive/', kind: 'gcs', endpoint: null, region: null, pathStyle: null, access: null, keyFile: archive.keyFile },
        { name: 'azure-lake', scope: azure.scope, kind: 'azure', endpoint: null, region: null, pathStyle: null, access: null, keyFile: null },
      ],
    );
  });

  it('is read from the file, else the inline JSON, else is empty', async () => {
    const file = join(dir, 'storage.json');
    await writeFile(file, JSON.stringify([lake]));
    assert.deepEqual(loadStorageRegistry({ file, json: JSON.stringify([archive]) }, {}).map((profile) => profile.name), ['lake']);
    assert.deepEqual(loadStorageRegistry({ json: JSON.stringify([archive]) }, {}).map((profile) => profile.name), ['archive']);
    assert.deepEqual(loadStorageRegistry({}, {}), []);
    assert.throws(() => loadStorageRegistry({ file: join(dir, 'missing.json') }, {}), /cannot read the storage file/);
    assert.throws(() => loadStorageRegistry({ json: '[{' }, {}), /CONSOLE_STORAGE is not JSON/);
  });

  it('refuses what the chart refuses', () => {
    refuses({ profiles: [lake] }, /must be a list of profiles/);
    refuses(['lake'], /must be a profile/);
    refuses([{ ...lake, name: 'Lake' }], /name "Lake" must be a lower case letter/);
    refuses([{ ...lake, name: 'a'.repeat(41) }], /must be a lower case letter/);
    refuses([{ ...lake, name: 'lake_eu' }], /must be a lower case letter/);
    refuses([lake, { ...lake, scope: 's3://other/' }], /\(lake\): the name is used twice/);
    refuses([{ ...lake, credentialsSecret: 'lake-credentials' }], /unknown field "credentialsSecret"/);
    refuses([{ ...lake, kind: 'minio' }], /kind "minio" must be gcs, s3 or azure/);
    refuses([{ ...lake, scope: 's3://example-lake' }], /must end with '\/'/);
    refuses([{ ...lake, scope: 'gs://example-lake/' }], /does not start with s3:\/\/, the scheme of kind s3/);
    refuses([{ ...azure, scope: 'abfss://exampleacct.dfs.core.windows.net/' }], /must name a container and its account/);
    refuses([{ ...azure, scope: 'abfss://lake@exampleacct.blob.core.windows.net/' }], /must name a container and its account/);
    refuses([{ ...lake, scope: 's3://example lake/' }], /hold no whitespace/);
    refuses([lake, { ...lake, name: 'lake-two' }], /scope "s3:\/\/example-lake\/" is already served by profile lake/);
    refuses([{ ...lake, access: 'lazy' }], /access "lazy" must be direct or cached/);
    refuses([{ ...lake, pathStyle: 'yes' }], /pathStyle must be true or false/);
    refuses([{ ...lake, endpoint: 'seaweedfs-s3 on port 8333' }], /endpoint is not a URL/);
    refuses([{ ...lake, endpoint: 'seaweedfs:8333' }], /endpoint must be http or https/);
    refuses([{ ...lake, endpoint: 'ftp://seaweedfs:8333' }], /endpoint must be http or https/);
    // The endpoint names the server alone, as the chart requires: no embedded credential, path or query.
    refuses([{ ...lake, endpoint: 'http://keyid:secret@seaweedfs:8333' }], /endpoint must name the server alone/);
    refuses([{ ...lake, endpoint: 'https://acct.blob.core.windows.net/?sv=2024&sig=ABC' }], /endpoint must name the server alone/);
    refuses([{ ...lake, endpoint: 'http://seaweedfs:8333/some/path' }], /endpoint must name the server alone/);
    refuses([{ ...lake, endpoint: 'http://seaweedfs:8333#frag' }], /endpoint must name the server alone/);
  });

  it('refuses one bucket served through two endpoints, which an s3:// path cannot tell apart', () => {
    refuses([lake, { ...lake, name: 'lake-archive', scope: 's3://example-lake/archive/', endpoint: 'https://s3.example.com' }], /lake and lake-archive both serve the bucket s3:\/\/example-lake\//);
    refuses([lake, { name: 'aws', scope: 's3://example-lake/aws/', kind: 's3' }], /through different endpoints \(http:\/\/seaweedfs-s3\.seaweedfs\.svc:8333, AWS\)/);
    // Nested scopes on one endpoint are fine, and so are other buckets elsewhere.
    const nested = validateStorageRegistry([lake, { ...lake, name: 'lake-sales', scope: 's3://example-lake/sales/', endpoint: `${lake.endpoint}/` }], {});
    assert.equal(nested.length, 2);
    assert.equal(validateStorageRegistry([lake, { name: 'aws', scope: 's3://other-lake/', kind: 's3' }], {}).length, 2);
  });

  it('reads each profile’s credentials from its own variables, never from the file', () => {
    const env = {
      CONSOLE_STORAGE_LAKE_ACCESS_KEY_ID: 'console-test-key-id',
      CONSOLE_STORAGE_LAKE_SECRET_ACCESS_KEY: ' console-test-secret ',
      CONSOLE_STORAGE_LAKE_GCS_HMAC_KEY_ID: 'not read for an s3 profile',
      CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_KEY_ID: 'GOOG1ECONSOLETESTKEYID0123',
      CONSOLE_STORAGE_AZURE_LAKE_AZURE_ACCOUNT_KEY: 'console-test-account-key',
      CONSOLE_STORAGE_AZURE_LAKE_AZURE_CLIENT_ID: '',
    };
    const [s3, gcs, az] = validateStorageRegistry([lake, archive, azure], env);
    assert.deepEqual(s3?.credentials, { ACCESS_KEY_ID: 'console-test-key-id', SECRET_ACCESS_KEY: 'console-test-secret' });
    assert.deepEqual(gcs?.credentials, { GCS_HMAC_KEY_ID: 'GOOG1ECONSOLETESTKEYID0123' });
    assert.deepEqual(az?.credentials, { AZURE_ACCOUNT_KEY: 'console-test-account-key' });
  });

  it('refuses an Azure connection string for another account, naming the variable only', () => {
    const env = { CONSOLE_STORAGE_AZURE_LAKE_AZURE_CONNECTION_STRING: 'DefaultEndpointsProtocol=https;AccountName=otheracct;AccountKey=console-test-key' };
    assert.throws(
      () => validateStorageRegistry([azure], env),
      (error: unknown) =>
        error instanceof StorageRegistryError &&
        /CONSOLE_STORAGE_AZURE_LAKE_AZURE_CONNECTION_STRING names another storage account than the profile's scope \(exampleacct\)/.test(error.message) &&
        !error.message.includes('console-test-key') &&
        !error.message.includes('otheracct'),
    );
  });
});

describe('which profile serves an object', () => {
  const registry = new StorageRegistry(
    validateStorageRegistry(
      [lake, { ...lake, name: 'lake-sales', scope: 's3://example-lake/sales/' }, archive, azure],
      {},
    ).map((profile) => createProfile(profile)),
  );

  it('is the one whose scope is the longest prefix', () => {
    assert.equal(registry.find('s3://example-lake/sales/orders/data/a.parquet')?.name, 'lake-sales');
    assert.equal(registry.find('s3://example-lake/finance/payments/data/a.parquet')?.name, 'lake');
    assert.equal(registry.find('gs://example-archive/x/metadata/catalog-pointer.json')?.name, 'archive');
    assert.equal(registry.find('abfss://lake@exampleacct.dfs.core.windows.net/sales/a.parquet')?.name, 'azure-lake');
    // A plain prefix, compared with case: no profile serves these.
    assert.equal(registry.find('s3://example-lakehouse/a.parquet'), null);
    assert.equal(registry.find('S3://example-lake/a.parquet'), null);
    assert.equal(registry.find('abfss://other@exampleacct.dfs.core.windows.net/a.parquet'), null);
    assert.equal(registry.find('/tmp/runs/r1/tail-0.parquet'), null);
  });

  it('serves a table by its location, which has no trailing slash', () => {
    assert.equal(registry.forLocation('s3://example-lake/sales/orders')?.name, 'lake-sales');
    assert.equal(registry.forLocation('s3://example-lake/sales')?.name, 'lake-sales');
    assert.equal(registry.forLocation('s3://example-lake/finance/payments/')?.name, 'lake');
    assert.equal(registry.forLocation('s3://elsewhere/sales/orders'), null);
  });

  it('refuses a location no profile covers with a 503 that names it', () => {
    assert.throws(
      () => registry.profileFor('s3://elsewhere/sales/orders/metadata/catalog-pointer.json'),
      (error: unknown) =>
        error instanceof ConsoleError &&
        error.code === 'storage_not_configured' &&
        error.statusCode === 503 &&
        error.message === 'no storage is configured for s3://elsewhere/sales/orders/metadata/catalog-pointer.json; add a storage profile whose scope covers it',
    );
  });

  it('gives a sandbox only the direct profiles its sources need', () => {
    const direct = new StorageRegistry(
      validateStorageRegistry(
        [lake, { ...lake, name: 'lake-sales', scope: 's3://example-lake/sales/' }, { ...azure, access: 'cached' }, { ...azure, name: 'azure-two', scope: 'abfss://two@exampleacct.dfs.core.windows.net/' }],
        {},
      ).map((profile) => createProfile(profile)),
    );
    const names = (sources: string[]): string[] => direct.forDuckdbSources(sources).map((profile) => profile.name).sort();
    assert.deepEqual(names(['s3://example-lake/sales/orders/data/a.parquet']), ['lake-sales']);
    assert.deepEqual(names(['s3://example-lake/sales/a.parquet', 's3://example-lake/finance/b.parquet', '/tmp/r/tail-0.parquet']), ['lake', 'lake-sales']);
    assert.deepEqual(names(['az://exampleacct.blob.core.windows.net/two/sales/a.parquet']), ['azure-two']);
    // A cached profile's files reach DuckDB as local copies: it needs no secret.
    assert.deepEqual(names(['az://exampleacct.blob.core.windows.net/lake/sales/a.parquet']), []);
    assert.deepEqual(names([]), []);
  });
});
