import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CatalogConnection, TableStorage } from '@shared/types';

import { ENGINES, snippet, type Engine } from './connect.ts';

const connection: CatalogConnection = {
  uri: 'https://console.example.com/d/domain_a/catalog',
  federatedUri: 'https://console.example.com/catalog',
  domain: 'domain_a',
  configured: true,
  enabled: true,
};

const seaweed: TableStorage = {
  name: 'lake',
  kind: 's3',
  scope: 's3://example-lake/',
  endpoint: 'http://seaweedfs-s3.seaweedfs.svc:8333',
  region: 'us-east-1',
  pathStyle: true,
  mode: 'direct',
  account: null,
};
const aws: TableStorage = { ...seaweed, name: 'aws', endpoint: null, region: 'eu-west-1', pathStyle: null };
const azure: TableStorage = { name: 'azure-lake', kind: 'azure', scope: 'abfss://lake@exampleacct.dfs.core.windows.net/', endpoint: null, region: null, pathStyle: null, mode: 'direct', account: 'exampleacct' };
const archive: TableStorage = { name: 'archive', kind: 'gcs', scope: 'gs://example-archive/', endpoint: null, region: null, pathStyle: null, mode: 'cached', account: null };

const S3 = 's3://example-lake/sales/orders';
const AZ = 'abfss://lake@exampleacct.dfs.core.windows.net/sales/orders';
const GS = 'gs://example-archive/sales/orders';
const engines = ENGINES.map((engine) => engine.value);

const all = (location: string, storage: TableStorage | null): Record<Engine, string> =>
  Object.fromEntries(engines.map((engine) => [engine, snippet(engine, connection, 'sales', 'orders', location, storage)])) as Record<Engine, string>;

describe('the Connect snippets', () => {
  it('read an S3-compatible server with its endpoint, path style and region', () => {
    const text = all(S3, seaweed);
    assert.match(
      text.duckdb,
      /CREATE SECRET s3 \(TYPE S3, KEY_ID '<access key id>', SECRET '<secret access key>', REGION 'us-east-1', ENDPOINT 'seaweedfs-s3\.seaweedfs\.svc:8333', USE_SSL false, URL_STYLE 'path'\);/,
    );
    assert.match(text.duckdb, /ATTACH 'warehouse' AS lake \(TYPE ICEBERG, SECRET midstream, ENDPOINT 'https:\/\/console\.example\.com\/d\/domain_a\/catalog'\);/);
    assert.match(text['duckdb-path'], /CREATE SECRET s3 \(TYPE S3, .*URL_STYLE 'path'\);\n\nSELECT count\(\*\) FROM iceberg_scan\('s3:\/\/example-lake\/sales\/orders'\);/);
    assert.match(text.pyiceberg, /"s3\.endpoint": "http:\/\/seaweedfs-s3\.seaweedfs\.svc:8333",\n {8}"s3\.path-style-access": "true",\n {8}"s3\.region": "us-east-1",/);
    assert.match(text.pyiceberg, /table = catalog\.load_table\("domain_a\.sales\.orders"\)/);
    assert.match(text.spark, /spark\.sql\.catalog\.lake\.io-impl=org\.apache\.iceberg\.aws\.s3\.S3FileIO\nspark\.sql\.catalog\.lake\.s3\.endpoint=http:\/\/seaweedfs-s3\.seaweedfs\.svc:8333\nspark\.sql\.catalog\.lake\.s3\.path-style-access=true\nspark\.sql\.catalog\.lake\.client\.region=us-east-1/);
    assert.match(text.trino, /fs\.native-s3\.enabled=true\ns3\.endpoint=http:\/\/seaweedfs-s3\.seaweedfs\.svc:8333\ns3\.path-style-access=true\ns3\.region=us-east-1/);
  });

  it('leave AWS its own endpoint and addressing', () => {
    const text = all(S3, aws);
    assert.match(text.duckdb, /CREATE SECRET s3 \(TYPE S3, KEY_ID '<access key id>', SECRET '<secret access key>', REGION 'eu-west-1'\);/);
    for (const engine of engines) assert.doesNotMatch(text[engine], /s3\.endpoint|URL_STYLE|USE_SSL|path-style/, engine);
    assert.match(text.trino, /s3\.region=eu-west-1/);
  });

  it('read Azure with the account and an Azure secret', () => {
    const text = all(AZ, azure);
    assert.match(text.duckdb, /INSTALL azure; LOAD azure;\nCREATE SECRET azure \(TYPE AZURE, CONNECTION_STRING '<connection string>'\);/);
    assert.match(text.duckdb, /PROVIDER credential_chain, ACCOUNT_NAME 'exampleacct'/);
    assert.match(text['duckdb-path'], /iceberg_scan\('abfss:\/\/lake@exampleacct\.dfs\.core\.windows\.net\/sales\/orders'\)/);
    assert.match(text.pyiceberg, /"adls\.account-name": "exampleacct",\n {8}"adls\.account-key": "<account key>",/);
    assert.match(text.spark, /io-impl=org\.apache\.iceberg\.azure\.adlsv2\.ADLSFileIO\nspark\.sql\.catalog\.lake\.adls\.auth\.shared-key\.account\.name=exampleacct\nspark\.sql\.catalog\.lake\.adls\.auth\.shared-key\.account\.key=<account key>/);
    assert.match(text.trino, /fs\.native-azure\.enabled=true\nazure\.auth-type=ACCESS_KEY\nazure\.access-key=<account key>/);
  });

  it('read GCS with an HMAC key for DuckDB and Google credentials for the rest', () => {
    const text = all(GS, archive);
    assert.match(text.duckdb, /CREATE SECRET gcs \(TYPE GCS, KEY_ID '<HMAC key id>', SECRET '<HMAC secret>'\);/);
    assert.match(text.pyiceberg, /# Every domain in one catalog\. Data files are read with your Google credentials \(ADC\)\.\ncatalog = RestCatalog\("lake", uri="https:\/\/console\.example\.com\/catalog", token="<access token>"\)/);
    assert.match(text.spark, /io-impl=org\.apache\.iceberg\.gcp\.gcs\.GCSFileIO/);
    assert.match(text.trino, /fs\.native-gcs\.enabled=true/);
    assert.match(text['duckdb-path'], /CREATE SECRET gcs .*\n\nSELECT count\(\*\) FROM iceberg_scan\('gs:\/\/example-archive\/sales\/orders'\);/);
  });

  it('follow the location’s scheme when no profile covers it', () => {
    assert.match(snippet('duckdb', connection, 'sales', 'orders', S3, null), /CREATE SECRET s3 \(TYPE S3, KEY_ID '<access key id>', SECRET '<secret access key>', REGION 'us-east-1'\);/);
    assert.match(snippet('pyiceberg', connection, 'sales', 'orders', AZ, null), /"adls\.account-name": "exampleacct"/);
    assert.match(snippet('trino', connection, 'sales', 'orders', GS, null), /fs\.native-gcs\.enabled=true/);
    assert.match(snippet('spark', connection, 'sales', 'orders', '<table location>', seaweed), /S3FileIO/);
  });

  it('hold a placeholder for every credential, never a value', () => {
    for (const [location, storage] of [[S3, seaweed], [S3, aws], [AZ, azure], [GS, archive]] as const) {
      const text = Object.values(all(location, storage)).join('\n');
      for (const line of text.split('\n')) {
        if (/(KEY_ID|SECRET|TOKEN|access-key|secret-access-key|account-key|account\.key|aws-secret-key|aws-access-key|token=|CONNECTION_STRING)\b/i.test(line) && !/CREATE SECRET midstream|SECRET midstream/.test(line)) {
          assert.match(line, /<[^>]+>/, `${location}: ${line}`);
        }
      }
    }
  });
});
