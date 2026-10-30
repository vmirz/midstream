// Type-only, so `node --test` can load this module without the bundler's path aliases.
import type { CatalogConnection, TableStorage } from '@shared/types';

/**
 * The Connect panel's snippets: how each engine reads a table through the catalog, which attaches as
 * `lake` (handbook 20), and reads its data files itself from object storage, with credentials for
 * the table's location.
 *
 * PyIceberg and Spark read nested namespaces, so they attach the federated catalog and name the
 * table in full, `lake.<domain>.<dataset>.<table>`: one catalog for every domain. DuckDB and Trino
 * read one level of namespaces, so they attach the domain's own catalog, one per domain, and name
 * the table `lake.<dataset>.<table>`.
 *
 * The data-file half follows the location's scheme — `gs://`, `s3://` or `abfss://` — filled from the
 * console's own storage profile for it: an S3-compatible server's endpoint, the region, path-style
 * addressing, the Azure account. Every credential is a placeholder; none is ever filled in.
 */

export type Engine = 'duckdb' | 'duckdb-path' | 'pyiceberg' | 'spark' | 'trino';

export const ENGINES: Array<{ value: Engine; label: string }> = [
  { value: 'duckdb', label: 'DuckDB' },
  { value: 'pyiceberg', label: 'PyIceberg' },
  { value: 'spark', label: 'Spark' },
  { value: 'trino', label: 'Trino' },
  { value: 'duckdb-path', label: 'By path' },
];

type Scheme = 'gs' | 's3' | 'abfss';

/** What the snippets need of the storage: from the profile, or the location alone without one. */
interface Target {
  scheme: Scheme;
  /** S3: an S3-compatible server's URL; GCS: an emulator's. Null for the cloud itself. */
  endpoint: string | null;
  region: string;
  pathStyle: boolean;
  /** Azure: the storage account. */
  account: string;
}

function targetOf(location: string, storage: TableStorage | null): Target {
  const scheme: Scheme = location.startsWith('s3://')
    ? 's3'
    : location.startsWith('abfss://')
      ? 'abfss'
      : location.startsWith('gs://')
        ? 'gs'
        : storage?.kind === 's3'
          ? 's3'
          : storage?.kind === 'azure'
            ? 'abfss'
            : 'gs';
  const account = storage?.account ?? /^abfss:\/\/[^@/]+@([^./]+)\./.exec(location)?.[1] ?? '<account>';
  return {
    scheme,
    endpoint: storage?.endpoint ?? null,
    region: storage?.region ?? 'us-east-1',
    pathStyle: storage?.pathStyle === true,
    account,
  };
}

/** `http://host:8333` → `host:8333`: DuckDB's ENDPOINT never takes the scheme. */
function hostOf(endpoint: string): string {
  return endpoint.replace(/^[a-z]+:\/\//i, '').replace(/\/+$/, '');
}

function usesTls(endpoint: string): boolean {
  return /^https:/i.test(endpoint);
}

/** The DuckDB statements that read the data files, with a comment saying what they need. */
function duckdbStorage(target: Target): string[] {
  switch (target.scheme) {
    case 's3': {
      const options = ["TYPE S3", "KEY_ID '<access key id>'", "SECRET '<secret access key>'", `REGION '${target.region}'`];
      if (target.endpoint) {
        options.push(`ENDPOINT '${hostOf(target.endpoint)}'`, `USE_SSL ${usesTls(target.endpoint)}`);
        if (target.pathStyle) options.push("URL_STYLE 'path'");
      }
      return ['-- DuckDB reads the data files itself, with an access key that can read the bucket:', `CREATE SECRET s3 (${options.join(', ')});`];
    }
    case 'abfss':
      return [
        '-- DuckDB reads the data files itself, with a connection string for the storage account',
        `-- (or PROVIDER credential_chain, ACCOUNT_NAME '${target.account}' to use your Azure sign-in):`,
        'INSTALL azure; LOAD azure;',
        "CREATE SECRET azure (TYPE AZURE, CONNECTION_STRING '<connection string>');",
      ];
    case 'gs': {
      const options = ['TYPE GCS', "KEY_ID '<HMAC key id>'", "SECRET '<HMAC secret>'"];
      if (target.endpoint) options.push(`ENDPOINT '${hostOf(target.endpoint)}'`, `USE_SSL ${usesTls(target.endpoint)}`, "URL_STYLE 'path'");
      return ['-- DuckDB reads the data files itself, with a GCS HMAC key that can read the bucket:', `CREATE SECRET gcs (${options.join(', ')});`];
    }
  }
}

/** PyIceberg's FileIO properties for the data files, and what reads them. */
function pyicebergStorage(target: Target): { comment: string; properties: Array<[string, string]> } {
  switch (target.scheme) {
    case 's3': {
      const properties: Array<[string, string]> = [];
      if (target.endpoint) properties.push(['s3.endpoint', target.endpoint]);
      if (target.endpoint && target.pathStyle) properties.push(['s3.path-style-access', 'true']);
      properties.push(['s3.region', target.region], ['s3.access-key-id', '<access key id>'], ['s3.secret-access-key', '<secret access key>']);
      return { comment: 'Data files are read with an access key that can read the bucket.', properties };
    }
    case 'abfss':
      return {
        comment: 'Data files are read with the storage account’s key.',
        properties: [
          ['adls.account-name', target.account],
          ['adls.account-key', '<account key>'],
        ],
      };
    case 'gs':
      return { comment: 'Data files are read with your Google credentials (ADC).', properties: [] };
  }
}

/** Spark's FileIO for the data files, as catalog properties. */
function sparkStorage(target: Target): string[] {
  const prefix = 'spark.sql.catalog.lake';
  switch (target.scheme) {
    case 's3':
      return [
        `${prefix}.io-impl=org.apache.iceberg.aws.s3.S3FileIO`,
        ...(target.endpoint ? [`${prefix}.s3.endpoint=${target.endpoint}`] : []),
        ...(target.endpoint && target.pathStyle ? [`${prefix}.s3.path-style-access=true`] : []),
        `${prefix}.client.region=${target.region}`,
        `${prefix}.s3.access-key-id=<access key id>`,
        `${prefix}.s3.secret-access-key=<secret access key>`,
      ];
    case 'abfss':
      return [
        `${prefix}.io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO`,
        `${prefix}.adls.auth.shared-key.account.name=${target.account}`,
        `${prefix}.adls.auth.shared-key.account.key=<account key>`,
      ];
    case 'gs':
      return [`${prefix}.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO`];
  }
}

/** Trino's native file system for the data files. */
function trinoStorage(target: Target): string[] {
  switch (target.scheme) {
    case 's3':
      return [
        'fs.native-s3.enabled=true',
        ...(target.endpoint ? [`s3.endpoint=${target.endpoint}`] : []),
        ...(target.endpoint && target.pathStyle ? ['s3.path-style-access=true'] : []),
        `s3.region=${target.region}`,
        's3.aws-access-key=<access key id>',
        's3.aws-secret-key=<secret access key>',
      ];
    case 'abfss':
      return ['fs.native-azure.enabled=true', 'azure.auth-type=ACCESS_KEY', 'azure.access-key=<account key>'];
    case 'gs':
      return ['fs.native-gcs.enabled=true'];
  }
}

/**
 * How `engine` reads `<namespace>.<table>` at `location`, with the console's storage profile for
 * that location (null when none covers it: the snippet follows the location's scheme alone).
 */
export function snippet(
  engine: Engine,
  connection: CatalogConnection,
  namespace: string,
  table: string,
  location: string,
  storage: TableStorage | null = null,
): string {
  const federated = `${connection.domain}.${namespace}`;
  const target = targetOf(location, storage);
  switch (engine) {
    case 'duckdb':
      return [
        'INSTALL iceberg; LOAD iceberg;',
        "CREATE SECRET midstream (TYPE ICEBERG, TOKEN '<access token>');",
        ...duckdbStorage(target),
        `-- One level of namespaces: this domain's own catalog; attach another domain's beside it.`,
        `ATTACH 'warehouse' AS lake (TYPE ICEBERG, SECRET midstream, ENDPOINT '${connection.uri}');`,
        '',
        `SELECT * FROM lake.${namespace}.${table} LIMIT 10;`,
      ].join('\n');
    case 'pyiceberg': {
      const { comment, properties } = pyicebergStorage(target);
      const open =
        properties.length === 0
          ? [`catalog = RestCatalog("lake", uri="${connection.federatedUri}", token="<access token>")`]
          : [
              'catalog = RestCatalog(',
              '    "lake",',
              `    uri="${connection.federatedUri}",`,
              '    token="<access token>",',
              '    **{',
              ...properties.map(([key, value]) => `        "${key}": "${value}",`),
              '    },',
              ')',
            ];
      return [
        'from pyiceberg.catalog.rest import RestCatalog',
        '',
        `# Every domain in one catalog. ${comment}`,
        ...open,
        `table = catalog.load_table("${federated}.${table}")`,
        'rows = table.scan(row_filter="_kafka_timestamp >= \'2026-01-01T00:00:00+00:00\'").to_arrow()',
      ].join('\n');
    }
    case 'spark':
      return [
        '# Every domain in one catalog: tables are lake.<domain>.<dataset>.<table>.',
        'spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.lake.type=rest',
        `spark.sql.catalog.lake.uri=${connection.federatedUri}`,
        'spark.sql.catalog.lake.token=<access token>',
        ...sparkStorage(target),
        '',
        `SELECT * FROM lake.${federated}.${table} LIMIT 10;`,
      ].join('\n');
    case 'trino':
      return [
        `# etc/catalog/lake.properties: this domain's own catalog, one level of namespaces.`,
        'connector.name=iceberg',
        'iceberg.catalog.type=rest',
        `iceberg.rest-catalog.uri=${connection.uri}`,
        'iceberg.rest-catalog.security=OAUTH2',
        'iceberg.rest-catalog.oauth2.token=<access token>',
        ...trinoStorage(target),
        '',
        `SELECT * FROM lake.${namespace}."${table}" LIMIT 10;`,
      ].join('\n');
    case 'duckdb-path':
      return [
        '-- No catalog: read the newest snapshot straight from object storage.',
        'INSTALL iceberg; LOAD iceberg;',
        ...duckdbStorage(target),
        '',
        `SELECT count(*) FROM iceberg_scan('${location}');`,
      ].join('\n');
  }
}
