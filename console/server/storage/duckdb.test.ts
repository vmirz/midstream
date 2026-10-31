/**
 * The profiles' secrets in DuckDB itself (1.5.5): a sandbox holds only the secrets its sources need,
 * each source matches the profile with the longest scope (`which_secret`, a table function), the Azure
 * extension is loaded only for an Azure source, and nothing in DuckDB's settings names a profile.
 * Creating a secret opens no connection, so no storage is reached; DuckDB's httpfs and azure extensions
 * are installed on first use, as the console installs them.
 */

import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

const dataDir = await mkdtemp(join(tmpdir(), 'console-storage-duckdb-'));
process.env.CONSOLE_DATA_DIR = dataDir;

const { createProfile, useStorage, validateStorageRegistry } = await import('./registry.ts');
const { prepareStorageAccess, runQuery, withSandbox, parseDuckDbError, explainMissingDataFile } = await import('../data/duckdb.ts');
import type { DuckDBConnection } from '@duckdb/node-api';

const env = {
  CONSOLE_STORAGE_LAKE_ACCESS_KEY_ID: 'consolekeyid',
  CONSOLE_STORAGE_LAKE_SECRET_ACCESS_KEY: 'consolesecretvalue',
  CONSOLE_STORAGE_LAKE_SALES_ACCESS_KEY_ID: 'consolesaleskeyid',
  CONSOLE_STORAGE_LAKE_SALES_SECRET_ACCESS_KEY: 'consolesalessecret',
  CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_KEY_ID: 'GOOG1ECONSOLETESTKEY0123456789',
  CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_SECRET: 'c'.repeat(40),
  CONSOLE_STORAGE_AZURITE_AZURE_ACCOUNT_KEY: Buffer.from('console-account-key').toString('base64'),
};
const registry = useStorage(
  validateStorageRegistry(
    [
      { name: 'lake', scope: 's3://example-lake/', kind: 's3', endpoint: 'http://seaweedfs:8333', pathStyle: true },
      { name: 'lake-sales', scope: 's3://example-lake/sales/', kind: 's3', endpoint: 'http://seaweedfs:8333', pathStyle: true },
      { name: 'archive', scope: 'gs://example-archive/', kind: 'gcs' },
      { name: 'azurite', scope: 'abfss://lake@devstoreaccount1.dfs.core.windows.net/', kind: 'azure', endpoint: 'http://azurite:10000' },
    ],
    env,
  ).map((profile) => createProfile(profile)),
);
assert.ok(registry);

after(() => rm(dataDir, { recursive: true, force: true }));

async function rows(connection: DuckDBConnection, sql: string): Promise<Record<string, unknown>[]> {
  return (await connection.runAndReadAll(sql)).getRowObjects();
}

/** Whether the azure extension is loaded: its settings exist. (`duckdb_extensions()` lists a directory, which a sandbox refuses.) */
async function azureLoaded(connection: DuckDBConnection): Promise<boolean> {
  const found = await rows(connection, "SELECT count(*)::INTEGER AS n FROM duckdb_settings() WHERE name LIKE 'azure%'");
  return Number(found[0]?.n ?? 0) > 0;
}

const S3_SALES = 's3://example-lake/sales/orders/data/a.parquet';
const S3_FINANCE = 's3://example-lake/finance/payments/data/b.parquet';
const AZ = 'az://devstoreaccount1.blob.core.windows.net/lake/sales/orders/data/c.parquet';

describe('a sandbox’s secrets', () => {
  it('are only those of the profiles serving its sources, each source by its longest scope', async () => {
    await withSandbox({ directories: [], paths: [S3_SALES, S3_FINANCE] }, async (connection) => {
      const names = (await rows(connection, 'SELECT name FROM duckdb_secrets() ORDER BY name')).map((row) => row.name);
      assert.deepEqual(names, ['console_lake', 'console_lake_sales']);
      assert.deepEqual(await rows(connection, `SELECT name FROM which_secret('${S3_SALES}', 's3')`), [{ name: 'console_lake_sales' }]);
      assert.deepEqual(await rows(connection, `SELECT name FROM which_secret('${S3_FINANCE}', 's3')`), [{ name: 'console_lake' }]);
      // An s3:// path no profile covers would go to AWS without credentials; none of these secrets matches it.
      assert.deepEqual(await rows(connection, "SELECT name FROM which_secret('s3://other-lake/a.parquet', 's3')"), []);
      assert.equal(await azureLoaded(connection), false, 'no Azure source, no azure extension');
    });
  });

  it('give an Azure source the azure extension and its secret, in the az:// form the scope is in', async () => {
    await withSandbox({ directories: [], paths: [AZ] }, async (connection) => {
      assert.deepEqual((await rows(connection, 'SELECT name FROM duckdb_secrets()')).map((row) => row.name), ['console_azurite']);
      assert.deepEqual(await rows(connection, `SELECT name FROM which_secret('${AZ}', 'azure')`), [{ name: 'console_azurite' }]);
      assert.equal(await azureLoaded(connection), true);
    });
  });

  it('are none for local files, and the sandbox stays locked', async () => {
    const local = join(dataDir, 'tail-0.parquet');
    assert.deepEqual(registry.forDuckdbSources([local]), []);
    await withSandbox({ directories: [`${dataDir}/`], paths: [local] }, async (connection) => {
      assert.equal(await azureLoaded(connection), false);
      assert.deepEqual(await rows(connection, "SELECT current_setting('lock_configuration') AS locked"), [{ locked: true }]);
    });
  });

  it('leave no trace of any profile in DuckDB’s settings', async () => {
    await withSandbox({ directories: [], paths: [S3_SALES, S3_FINANCE, AZ] }, async (connection) => {
      // `allowed_paths` names the run's own sources, by design; nothing names a profile's endpoint or keys.
      const settings = JSON.stringify(await rows(connection, 'SELECT name, value FROM duckdb_settings()'));
      for (const secret of ['seaweedfs', 'azurite', 'consolekeyid', 'consolesaleskeyid', 'consolesecretvalue', 'GOOG1E', 'example-archive']) {
        assert.ok(!settings.includes(secret), `duckdb_settings() names ${secret}`);
      }
    });
  });
});

describe('the shared instance', () => {
  it('creates each direct profile’s secret once, when its objects are first read', async () => {
    await prepareStorageAccess(registry.profiles.filter((profile) => profile.mode === 'direct'));
    await prepareStorageAccess(registry.profiles.filter((profile) => profile.mode === 'direct'));
    const found = await runQuery('SELECT name FROM duckdb_secrets() ORDER BY name');
    assert.deepEqual(found.rows.map((row) => row.name), ['console_archive', 'console_azurite', 'console_lake', 'console_lake_sales']);
    const gcs = await runQuery("SELECT name FROM which_secret('gs://example-archive/x/data/a.parquet', 'gcs')");
    assert.deepEqual(gcs.rows, [{ name: 'console_archive' }]);
  });
});

describe('a secret DuckDB refuses', () => {
  it('fails without the statement or any value in it, on the shared instance and in a sandbox', async () => {
    const { BaseProfile } = await import('./profile.ts');
    const { ConsoleError } = await import('../errors.ts');
    /** A profile whose statement DuckDB cannot parse, and so echoes back after `LINE 1:`. */
    class Broken extends BaseProfile {
      get mode(): 'direct' {
        return 'direct';
      }
      duckdbSecret(): string {
        return `CREATE OR REPLACE TEMPORARY SECRET ${this.secretName} (TYPE s3, KEY_ID 'console-broken-key-id', SECRET 'console-broken-secret' oops, SCOPE '${this.scope}')`;
      }
      duckdbExtensions(): string[] {
        return ['httpfs'];
      }
      duckdbSource(uri: string): string {
        return uri;
      }
      duckdbScope(): string {
        return this.scope;
      }
      protected credentialsInUse(): string {
        return 'an access key';
      }
      protected credentialsFound(): boolean {
        return true;
      }
      protected read(): Promise<Buffer> {
        return Promise.reject(new Error('not read'));
      }
      protected download(): Promise<void> {
        return Promise.reject(new Error('not read'));
      }
    }
    const broken = new Broken({ name: 'broken', scope: 's3://broken-lake/', kind: 's3', endpoint: null, region: null, pathStyle: null, access: null, keyFile: null, credentials: {} });
    const refused = (error: unknown): boolean =>
      error instanceof ConsoleError &&
      error.code === 'storage_unavailable' &&
      /DuckDB could not create the secret of storage profile broken: Parser Error/.test(error.message) &&
      !error.message.includes('console-broken') &&
      !error.message.includes('LINE');
    await assert.rejects(prepareStorageAccess([broken]), refused);
    useStorage([...registry.profiles, broken]);
    try {
      await assert.rejects(withSandbox({ directories: [], paths: ['s3://broken-lake/a.parquet'] }, async () => undefined), refused);
    } finally {
      useStorage(registry);
    }
  });
});

describe('DuckDB’s errors', () => {
  it('name the gcs profile’s variables when GCS refuses the signature, and only for gcs', () => {
    const gcs = parseDuckDbError(
      new Error(
        "HTTP Error: HTTP error on 'https://storage.googleapis.com/example-archive/x/data/a.parquet' (HTTP 403 Forbidden)\n\n" +
          'Authentication Failure - GCS authentication failed.\n* HMAC credentials were provided but authentication failed.',
      ),
    ).message;
    assert.match(gcs, /GCS authentication failed/);
    assert.match(gcs, /Check that CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_KEY_ID and CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_SECRET are a matching, active GCS HMAC key pair/);
    const s3 =
      "HTTP Error: HTTP GET error reading 'http://seaweedfs:8333/example-lake/x/a.parquet' in region 'us-east-1' (HTTP 403 Forbidden)\n\n" +
      'InvalidAccessKeyId: The access key ID you provided does not exist in our records.';
    assert.equal(parseDuckDbError(new Error(s3)).message, s3);
    assert.doesNotMatch(parseDuckDbError(new Error(s3)).message, /HMAC/);
  });

  it('say a data file is gone in each backend’s words', () => {
    const hint = /The data file is no longer in storage: a snapshot expiry may have removed it/;
    assert.match(explainMissingDataFile(`HTTP Error: HTTP GET error reading '${S3_SALES}' in region 'us-east-1' (HTTP 404 Not Found)`), hint);
    assert.match(explainMissingDataFile("HTTP Error: HTTP GET error reading 'gs://example-archive/x/a.parquet' in region '' (HTTP 404 Not Found)"), hint);
    assert.match(explainMissingDataFile(`IO Error: No files found that match the pattern "${AZ}"`), hint);
    const sandboxed = explainMissingDataFile(`Permission Error: Cannot access file "${AZ}/**/*.parquet" - file system operations are disabled by configuration`);
    assert.equal(sandboxed, `IO Error: ${AZ} could not be read.\nThe data file is no longer in storage: a snapshot expiry may have removed it. Run the query again to read the table’s current snapshot.`);
    // What is not a removed data file stays as DuckDB said it.
    for (const message of [
      'Permission Error: Cannot access file "/etc/hosts" - file system operations are disabled by configuration',
      `Permission Error: Cannot access file "${AZ}" - file system operations are disabled by configuration`,
      'IO Error: No files found that match the pattern "/var/lib/midstream-console/parquet-cache/x.parquet"',
    ]) {
      assert.equal(explainMissingDataFile(message), message);
    }
  });
});
