/**
 * Each backend's profile: the DuckDB secret it creates, the path DuckDB is given for a data file, its
 * mode, and what it says about itself — never a credential.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { AzureProfile, azurePathForDuckdb } from './azure.ts';
import { GcsProfile } from './gcs.ts';
import type { StorageProfile, StorageProfileConfig } from './profile.ts';
import { S3Profile } from './s3.ts';

function config(overrides: Partial<StorageProfileConfig> & Pick<StorageProfileConfig, 'name' | 'scope' | 'kind'>): StorageProfileConfig {
  return { endpoint: null, region: null, pathStyle: null, access: null, keyFile: null, credentials: {}, ...overrides };
}

const AZURITE_KEY = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==';

/** Nothing a profile shows may hold a credential's value. */
function assertShowsNoCredential(profile: StorageProfile, values: string[]): void {
  const shown = JSON.stringify({ describe: profile.describe(), warnings: profile.warnings() });
  for (const value of values) assert.ok(!shown.includes(value), `${profile.name} shows a credential`);
}

describe('an S3 profile', () => {
  const seaweed = config({
    name: 'lake',
    scope: 's3://example-lake/',
    kind: 's3',
    endpoint: 'http://seaweedfs-s3.seaweedfs.svc:8333',
    pathStyle: true,
    credentials: { ACCESS_KEY_ID: 'console-key-id', SECRET_ACCESS_KEY: "console-secret'quoted" },
  });

  it('gives an S3-compatible server its endpoint without the scheme, its TLS and path style', () => {
    const profile = new S3Profile(seaweed);
    assert.equal(profile.mode, 'direct');
    assert.equal(
      profile.duckdbSecret(),
      "CREATE OR REPLACE TEMPORARY SECRET console_lake (TYPE s3, KEY_ID 'console-key-id', SECRET 'console-secret''quoted', " +
        "REGION 'us-east-1', ENDPOINT 'seaweedfs-s3.seaweedfs.svc:8333', USE_SSL false, URL_STYLE 'path', SCOPE 's3://example-lake/')",
    );
    assert.deepEqual(profile.duckdbExtensions(), ['httpfs']);
    assert.equal(profile.duckdbSource('s3://example-lake/sales/orders/data/a.parquet'), 's3://example-lake/sales/orders/data/a.parquet');
    assert.equal(profile.duckdbScope(), 's3://example-lake/');
    assertShowsNoCredential(profile, ['console-key-id', 'console-secret']);
  });

  it('uses TLS for an https endpoint, and a session token when there is one', () => {
    const profile = new S3Profile({
      ...seaweed,
      name: 'r2-lake',
      endpoint: 'https://s3.example.com/',
      pathStyle: false,
      region: 'eu-west-1',
      credentials: { ...seaweed.credentials, SESSION_TOKEN: 'console-session' },
    });
    assert.equal(
      profile.duckdbSecret(),
      "CREATE OR REPLACE TEMPORARY SECRET console_r2_lake (TYPE s3, KEY_ID 'console-key-id', SECRET 'console-secret''quoted', " +
        "SESSION_TOKEN 'console-session', REGION 'eu-west-1', ENDPOINT 's3.example.com', USE_SSL true, SCOPE 's3://example-lake/')",
    );
    assert.equal(profile.describe().credentials, 'an access key with a session token');
  });

  it('leaves AWS its own endpoint and addressing, and reads with the credential chain without keys', () => {
    const profile = new S3Profile(config({ name: 'aws', scope: 's3://example-lake/midstream/', kind: 's3', region: 'eu-central-1', pathStyle: true }));
    assert.equal(
      profile.duckdbSecret(),
      "CREATE OR REPLACE TEMPORARY SECRET console_aws (TYPE s3, PROVIDER credential_chain, REFRESH 'auto', REGION 'eu-central-1', SCOPE 's3://example-lake/midstream/')",
    );
    assert.deepEqual(profile.duckdbExtensions(), ['httpfs', 'aws']);
    assert.deepEqual(
      { credentials: profile.describe().credentials, found: profile.describe().credentialsFound },
      { credentials: 'the default credential chain', found: false },
    );
  });

  it('is cached only when it says so, with no secret', () => {
    const profile = new S3Profile({ ...seaweed, access: 'cached' });
    assert.equal(profile.mode, 'cached');
    assert.equal(profile.duckdbSecret(), null);
  });

  it('warns of half an access key by the variable it lacks', () => {
    const profile = new S3Profile({ ...seaweed, credentials: { ACCESS_KEY_ID: 'console-key-id' } });
    assert.deepEqual(profile.warnings(), [
      'storage profile lake: CONSOLE_STORAGE_LAKE_SECRET_ACCESS_KEY is not set, so its access key is incomplete; it reads with the default credential chain',
    ]);
    assertShowsNoCredential(profile, ['console-key-id']);
  });
});

describe('a GCS profile', () => {
  const archive = config({ name: 'archive', scope: 'gs://example-archive/', kind: 'gcs' });
  const pair = { GCS_HMAC_KEY_ID: 'GOOG1ECONSOLETESTKEY0123456789', GCS_HMAC_SECRET: 'c'.repeat(40) };

  it('reads in place with an HMAC key pair', () => {
    const profile = new GcsProfile({ ...archive, credentials: pair });
    assert.equal(profile.mode, 'direct');
    assert.equal(
      profile.duckdbSecret(),
      `CREATE OR REPLACE TEMPORARY SECRET console_archive (TYPE gcs, KEY_ID 'GOOG1ECONSOLETESTKEY0123456789', SECRET '${'c'.repeat(40)}', SCOPE 'gs://example-archive/')`,
    );
    assert.deepEqual(profile.duckdbExtensions(), ['httpfs']);
    assert.equal(profile.duckdbSource('gs://example-archive/a/b.parquet'), 'gs://example-archive/a/b.parquet');
    assert.deepEqual(profile.warnings(), []);
    assertShowsNoCredential(profile, ['GOOG1ECONSOLETESTKEY', 'c'.repeat(40)]);
  });

  it('is cached without the pair, or when it says so', () => {
    for (const profile of [new GcsProfile(archive), new GcsProfile({ ...archive, credentials: pair, access: 'cached' })]) {
      assert.equal(profile.mode, 'cached');
      assert.equal(profile.duckdbSecret(), null);
    }
  });

  it('reaches an emulator by path, as it does an S3-compatible server', () => {
    const profile = new GcsProfile({ ...archive, endpoint: 'http://localhost:4443', credentials: pair });
    assert.match(profile.duckdbSecret() ?? '', /, ENDPOINT 'localhost:4443', USE_SSL false, URL_STYLE 'path', SCOPE 'gs:\/\/example-archive\/'\)$/);
  });

  it('warns of credentials that cannot sign, naming the variables only', () => {
    const profile = new GcsProfile({ ...archive, access: 'direct', credentials: { GCS_HMAC_KEY_ID: 'svc@project.iam.gserviceaccount.com', GCS_HMAC_SECRET: 'short' } });
    const warnings = profile.warnings();
    assert.equal(warnings.length, 2);
    assert.match(warnings[0] ?? '', /CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_KEY_ID does not look like a GCS HMAC access ID/);
    assert.match(warnings[1] ?? '', /CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_SECRET is not 40 characters long/);
    assert.match(profile.hmacAdvice(), /CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_KEY_ID does not look like/);
    assertShowsNoCredential(profile, ['svc@project', 'short']);
    const half = new GcsProfile({ ...archive, access: 'direct', credentials: { GCS_HMAC_SECRET: 'c'.repeat(40) } });
    assert.deepEqual(half.warnings(), [
      'storage profile archive: CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_KEY_ID is not set, so its HMAC key pair is incomplete and data files are downloaded',
      'storage profile archive: access is direct, but DuckDB needs an HMAC key pair to read gs:// in place; data files are downloaded',
    ]);
    assert.match(new GcsProfile({ ...archive, credentials: pair }).hmacAdvice(), /^Check that CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_KEY_ID and CONSOLE_STORAGE_ARCHIVE_GCS_HMAC_SECRET are a matching, active GCS HMAC key pair whose service account can read gs:\/\/example-archive\/\.$/);
  });
});

describe('an Azure profile', () => {
  const real = config({ name: 'azure-lake', scope: 'abfss://lake@exampleacct.dfs.core.windows.net/midstream/', kind: 'azure' });
  const azurite = config({
    name: 'azurite',
    scope: 'abfss://lake@devstoreaccount1.dfs.core.windows.net/',
    kind: 'azure',
    endpoint: 'http://azurite.azurite.svc:10000',
    credentials: { AZURE_ACCOUNT_KEY: AZURITE_KEY },
  });

  it('gives DuckDB every path and scope as az://<account>.blob.core.windows.net/<container>/…, emulator or not', () => {
    for (const profile of [new AzureProfile({ ...real, credentials: { AZURE_ACCOUNT_KEY: 'console-key' } }), new AzureProfile(azurite)]) {
      const account = profile.describe().account as string;
      assert.equal(profile.duckdbSource(`abfss://lake@${account}.dfs.core.windows.net/midstream/sales/orders/data/a.parquet`), `az://${account}.blob.core.windows.net/lake/midstream/sales/orders/data/a.parquet`);
      assert.deepEqual(profile.duckdbExtensions(), ['azure']);
    }
    assert.equal(new AzureProfile(real).duckdbScope(), 'az://exampleacct.blob.core.windows.net/lake/midstream/');
    assert.equal(new AzureProfile(azurite).duckdbScope(), 'az://devstoreaccount1.blob.core.windows.net/lake/');
    assert.equal(azurePathForDuckdb('abfss://c@a1.dfs.core.windows.net/'), 'az://a1.blob.core.windows.net/c/');
  });

  it('hands an account key to DuckDB inside a connection string: the cloud’s, or the emulator’s endpoint', () => {
    assert.equal(
      new AzureProfile({ ...real, credentials: { AZURE_ACCOUNT_KEY: 'console-key' } }).duckdbSecret(),
      "CREATE OR REPLACE TEMPORARY SECRET console_azure_lake (TYPE azure, CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=exampleacct;" +
        "AccountKey=console-key;EndpointSuffix=core.windows.net', SCOPE 'az://exampleacct.blob.core.windows.net/lake/midstream/')",
    );
    assert.equal(
      new AzureProfile(azurite).duckdbSecret(),
      `CREATE OR REPLACE TEMPORARY SECRET console_azurite (TYPE azure, CONNECTION_STRING 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;` +
        `AccountKey=${AZURITE_KEY};BlobEndpoint=http://azurite.azurite.svc:10000/devstoreaccount1;DfsEndpoint=http://azurite.azurite.svc:10000/devstoreaccount1;', ` +
        "SCOPE 'az://devstoreaccount1.blob.core.windows.net/lake/')",
    );
  });

  it('names the account in a connection string that lacks it, and the emulator’s endpoint', () => {
    const profile = new AzureProfile({
      ...azurite,
      credentials: { AZURE_CONNECTION_STRING: 'SharedAccessSignature=sv=2024&sig=console-sas' },
    });
    assert.equal(
      profile.connectionString(),
      'SharedAccessSignature=sv=2024&sig=console-sas;AccountName=devstoreaccount1;BlobEndpoint=http://azurite.azurite.svc:10000/devstoreaccount1;DfsEndpoint=http://azurite.azurite.svc:10000/devstoreaccount1;',
    );
    const given = 'DefaultEndpointsProtocol=https;AccountName=exampleacct;AccountKey=console-key;EndpointSuffix=core.windows.net';
    assert.equal(new AzureProfile({ ...real, credentials: { AZURE_CONNECTION_STRING: given } }).connectionString(), `${given};`);
    assertShowsNoCredential(profile, ['console-sas']);
  });

  it('reads with a service principal, or the credential chain, naming the account', () => {
    const principal = new AzureProfile({ ...real, credentials: { AZURE_TENANT_ID: 'tenant-1', AZURE_CLIENT_ID: 'client-1', AZURE_CLIENT_SECRET: 'console-client-secret' } });
    assert.equal(
      principal.duckdbSecret(),
      "CREATE OR REPLACE TEMPORARY SECRET console_azure_lake (TYPE azure, PROVIDER service_principal, TENANT_ID 'tenant-1', CLIENT_ID 'client-1', " +
        "CLIENT_SECRET 'console-client-secret', ACCOUNT_NAME 'exampleacct', SCOPE 'az://exampleacct.blob.core.windows.net/lake/midstream/')",
    );
    assert.equal(principal.describe().credentials, 'a service principal');
    assertShowsNoCredential(principal, ['console-client-secret']);
    assert.equal(
      new AzureProfile(real).duckdbSecret(),
      "CREATE OR REPLACE TEMPORARY SECRET console_azure_lake (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'exampleacct', SCOPE 'az://exampleacct.blob.core.windows.net/lake/midstream/')",
    );
  });

  it('downloads first where DuckDB could only reach an emulator by token, which it would send to the real account', () => {
    const profile = new AzureProfile({ ...azurite, credentials: {} });
    assert.equal(profile.mode, 'cached');
    assert.equal(profile.duckdbSecret(), null);
    assert.match(profile.warnings()[0] ?? '', /reaches the endpoint http:\/\/azurite\.azurite\.svc:10000 only through a connection string or the account key/);
  });
});
