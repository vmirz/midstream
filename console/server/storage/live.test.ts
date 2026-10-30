/**
 * The storage profiles against real servers: SeaweedFS for S3, Azurite for Azure, fake-gcs-server
 * for GCS. Skipped unless CONSOLE_STORAGE_LIVE is set.
 *
 *   CONSOLE_STORAGE_LIVE=1 node --test server/storage/live.test.ts
 *
 * Endpoints default to the local emulators — SeaweedFS at http://localhost:8333 (keys
 * midstreamdev / midstreamdevsecret, path style), Azurite's devstoreaccount1 at http://localhost:10000
 * (started with --skipApiVersionCheck: the SDKs send API versions newer than it accepts), and
 * fake-gcs-server at http://localhost:4443 — and each may be overridden:
 * CONSOLE_STORAGE_LIVE_S3_ENDPOINT, _S3_ACCESS_KEY_ID, _S3_SECRET_ACCESS_KEY, _AZURE_ENDPOINT,
 * _AZURE_ACCOUNT, _AZURE_ACCOUNT_KEY, _GCS_ENDPOINT. Every backend gets a direct profile and a cached
 * one nested in it. A Parquet fixture is uploaded with each SDK, read back through each profile's
 * readObject and downloadToFile, and queried with DuckDB on the shared instance and in a sandbox; one
 * statement joins two backends. Everything written goes under a key of the run's own, removed after.
 */

import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import type { DataObject } from '../iceberg/snapshot.ts';
import type { StorageProfile } from './profile.ts';

const LIVE = Boolean(process.env.CONSOLE_STORAGE_LIVE);
const env = (name: string, fallback: string): string => process.env[`CONSOLE_STORAGE_LIVE_${name}`] || fallback;
const S3_ENDPOINT = env('S3_ENDPOINT', 'http://localhost:8333');
const S3_KEY_ID = env('S3_ACCESS_KEY_ID', 'midstreamdev');
const S3_SECRET = env('S3_SECRET_ACCESS_KEY', 'midstreamdevsecret');
const AZURE_ENDPOINT = env('AZURE_ENDPOINT', 'http://localhost:10000');
const AZURE_ACCOUNT = env('AZURE_ACCOUNT', 'devstoreaccount1');
const AZURE_KEY = env('AZURE_ACCOUNT_KEY', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==');
const GCS_ENDPOINT = env('GCS_ENDPOINT', 'http://localhost:4443');

/** Every backend's bucket or container is this one; each run writes under a key of its own. */
const CONTAINER = 'console-live';
const RUN = `run-${Date.now()}-${process.pid}`;
const FIXTURE = fileURLToPath(new URL('../../testdata/iceberg/orders/data/ts_day=2026-10-17/compact-49556ce56b7c4e06-0-5da6cd5f.parquet', import.meta.url));
const FIXTURE_ROWS = 7;

const S3_SCOPE = `s3://${CONTAINER}/`;
const AZURE_SCOPE = `abfss://${CONTAINER}@${AZURE_ACCOUNT}.dfs.core.windows.net/`;
const GCS_SCOPE = `gs://${CONTAINER}/`;

/** One profile under test and the object it was given. */
interface Case {
  profile: string;
  uri: string;
}
const CASES: Case[] = [
  { profile: 'live-s3', uri: `${S3_SCOPE}${RUN}/orders/data/a.parquet` },
  { profile: 'live-s3-cached', uri: `${S3_SCOPE}cached/${RUN}/orders/data/a.parquet` },
  { profile: 'live-azure', uri: `${AZURE_SCOPE}${RUN}/orders/data/a.parquet` },
  { profile: 'live-azure-cached', uri: `${AZURE_SCOPE}cached/${RUN}/orders/data/a.parquet` },
  { profile: 'live-gcs', uri: `${GCS_SCOPE}${RUN}/orders/data/a.parquet` },
  { profile: 'live-gcs-cached', uri: `${GCS_SCOPE}cached/${RUN}/orders/data/a.parquet` },
];
/** The direct profile of each backend. */
const DIRECT: Case[] = CASES.filter((testCase) => !testCase.profile.endsWith('-cached'));

describe('storage profiles against the emulators', { skip: LIVE ? false : 'set CONSOLE_STORAGE_LIVE=1 to run against SeaweedFS, Azurite and fake-gcs-server' }, async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'console-storage-live-'));
  process.env.CONSOLE_DATA_DIR = dataDir;

  const { S3Client, PutObjectCommand, CreateBucketCommand, DeleteObjectCommand } = await import('@aws-sdk/client-s3');
  const { BlobServiceClient, StorageSharedKeyCredential } = await import('@azure/storage-blob');
  const { Storage } = await import('@google-cloud/storage');
  const { createProfile, useStorage, validateStorageRegistry } = await import('./registry.ts');
  const { objectSources } = await import('../data/objects.ts');
  const { runQuery, withSandbox, parseDuckDbError } = await import('../data/duckdb.ts');
  const { readObject, ObjectMissingError } = await import('../iceberg/io.ts');

  const bytes = await readFile(FIXTURE);
  const s3 = new S3Client({ region: 'us-east-1', endpoint: S3_ENDPOINT, forcePathStyle: true, credentials: { accessKeyId: S3_KEY_ID, secretAccessKey: S3_SECRET } });
  const blobs = new BlobServiceClient(`${AZURE_ENDPOINT}/${AZURE_ACCOUNT}`, new StorageSharedKeyCredential(AZURE_ACCOUNT, AZURE_KEY)).getContainerClient(CONTAINER);
  const gcs = new Storage({ apiEndpoint: GCS_ENDPOINT, projectId: 'console-live' });
  const keyOf = (uri: string): string => uri.replace(/^[a-z0-9]+:\/\/[^/]+\//, '');

  const registry = useStorage(
    validateStorageRegistry(
      [
        { name: 'live-s3', scope: S3_SCOPE, kind: 's3', endpoint: S3_ENDPOINT, pathStyle: true, region: 'us-east-1' },
        { name: 'live-s3-cached', scope: `${S3_SCOPE}cached/`, kind: 's3', endpoint: S3_ENDPOINT, pathStyle: true, access: 'cached' },
        { name: 'live-azure', scope: AZURE_SCOPE, kind: 'azure', endpoint: AZURE_ENDPOINT },
        { name: 'live-azure-cached', scope: `${AZURE_SCOPE}cached/`, kind: 'azure', endpoint: AZURE_ENDPOINT, access: 'cached' },
        { name: 'live-gcs', scope: GCS_SCOPE, kind: 'gcs', endpoint: GCS_ENDPOINT },
        { name: 'live-gcs-cached', scope: `${GCS_SCOPE}cached/`, kind: 'gcs', endpoint: GCS_ENDPOINT, access: 'cached' },
      ],
      {
        CONSOLE_STORAGE_LIVE_S3_ACCESS_KEY_ID: S3_KEY_ID,
        CONSOLE_STORAGE_LIVE_S3_SECRET_ACCESS_KEY: S3_SECRET,
        CONSOLE_STORAGE_LIVE_S3_CACHED_ACCESS_KEY_ID: S3_KEY_ID,
        CONSOLE_STORAGE_LIVE_S3_CACHED_SECRET_ACCESS_KEY: S3_SECRET,
        CONSOLE_STORAGE_LIVE_AZURE_AZURE_ACCOUNT_KEY: AZURE_KEY,
        CONSOLE_STORAGE_LIVE_AZURE_CACHED_AZURE_ACCOUNT_KEY: AZURE_KEY,
        // fake-gcs-server does not check signatures: any well-formed pair lets DuckDB read in place.
        CONSOLE_STORAGE_LIVE_GCS_GCS_HMAC_KEY_ID: 'GOOG1ECONSOLELIVETESTKEY0123456',
        CONSOLE_STORAGE_LIVE_GCS_GCS_HMAC_SECRET: 'c'.repeat(40),
      },
    ).map((profile) => createProfile(profile)),
  );
  assert.ok(registry);
  const profile = (name: string): StorageProfile => registry.profiles.find((candidate) => candidate.name === name) as StorageProfile;

  const object = (uri: string): DataObject => ({
    uri,
    name: keyOf(uri),
    bytes: bytes.length,
    rows: FIXTURE_ROWS,
    committedAt: null,
    partition: null,
    offsetStart: null,
    offsetEnd: null,
    timeMin: null,
    timeMax: null,
    timeKnown: false,
    partitionKey: null,
  });
  const count = (source: string): string => `SELECT count(*)::INTEGER AS n FROM read_parquet('${source}')`;

  before(async () => {
    await s3.send(new CreateBucketCommand({ Bucket: CONTAINER })).catch((cause: { name?: string }) => {
      if (cause.name !== 'BucketAlreadyOwnedByYou' && cause.name !== 'BucketAlreadyExists') throw cause;
    });
    await blobs.createIfNotExists();
    await gcs.createBucket(CONTAINER).catch((cause: { code?: number }) => {
      if (cause.code !== 409) throw cause;
    });
    for (const { uri } of CASES) {
      if (uri.startsWith('s3://')) await s3.send(new PutObjectCommand({ Bucket: CONTAINER, Key: keyOf(uri), Body: bytes }));
      else if (uri.startsWith('abfss://')) await blobs.getBlockBlobClient(keyOf(uri)).uploadData(bytes);
      else await gcs.bucket(CONTAINER).file(keyOf(uri)).save(bytes);
    }
  });

  after(async () => {
    for (const { uri } of CASES) {
      if (uri.startsWith('s3://')) await s3.send(new DeleteObjectCommand({ Bucket: CONTAINER, Key: keyOf(uri) })).catch(() => undefined);
      else if (uri.startsWith('abfss://')) await blobs.getBlockBlobClient(keyOf(uri)).deleteIfExists();
      else await gcs.bucket(CONTAINER).file(keyOf(uri)).delete({ ignoreNotFound: true }).catch(() => undefined);
    }
    await rm(dataDir, { recursive: true, force: true });
  });

  for (const testCase of CASES) {
    describe(testCase.profile, () => {
      it('serves its object, by the longest scope', () => {
        assert.equal(registry.find(testCase.uri)?.name, testCase.profile);
        assert.equal(profile(testCase.profile).mode, testCase.profile.endsWith('-cached') ? 'cached' : 'direct');
      });

      it('reads it back whole through readObject and downloadToFile', async () => {
        assert.ok((await readObject(testCase.uri)).equals(bytes));
        const target = join(dataDir, `${testCase.profile}.parquet`);
        await profile(testCase.profile).downloadToFile(testCase.uri, target);
        assert.ok((await readFile(target)).equals(bytes));
        await assert.rejects(readObject(testCase.uri.replace('a.parquet', 'missing.json')), ObjectMissingError);
      });

      it('queries it with DuckDB on the shared instance and in a sandbox', async () => {
        const source = (await objectSources([object(testCase.uri)])).get(testCase.uri) as string;
        assert.ok(source.startsWith(testCase.profile.endsWith('-cached') ? join(dataDir, 'parquet-cache', testCase.profile) : testCase.uri.startsWith('abfss://') ? 'az://' : testCase.uri.slice(0, 5)), source);
        assert.deepEqual((await runQuery(count(source))).rows, [{ n: FIXTURE_ROWS }]);
        const sandboxed = await withSandbox({ directories: [], paths: [source] }, async (connection) => (await connection.runAndReadAll(count(source))).getRowObjects());
        assert.deepEqual(sandboxed, [{ n: FIXTURE_ROWS }]);
      });
    });
  }

  describe('across backends', () => {
    it('joins S3, Azure and GCS in one statement, on the shared instance and in a sandbox', async () => {
      const sources = await objectSources(DIRECT.map((testCase) => object(testCase.uri)));
      const [s3Source, azureSource, gcsSource] = DIRECT.map((testCase) => sources.get(testCase.uri) ?? assert.fail(`no source for ${testCase.uri}`));
      const sql =
        `SELECT count(*)::INTEGER AS n FROM read_parquet('${s3Source}') AS s ` +
        `JOIN read_parquet('${azureSource}') AS z USING (_kafka_partition, _kafka_offset) ` +
        `JOIN read_parquet('${gcsSource}') AS g USING (_kafka_partition, _kafka_offset)`;
      assert.deepEqual((await runQuery(sql)).rows, [{ n: FIXTURE_ROWS }]);
      const paths = [...sources.values()];
      const sandboxed = await withSandbox({ directories: [], paths }, async (connection) => (await connection.runAndReadAll(sql)).getRowObjects());
      assert.deepEqual(sandboxed, [{ n: FIXTURE_ROWS }]);
    });

    it('says a data file is gone in each backend’s words', async () => {
      for (const testCase of DIRECT) {
        const missing = profile(testCase.profile).duckdbSource(testCase.uri.replace('a.parquet', 'gone.parquet'));
        await assert.rejects(runQuery(count(missing)), /The data file is no longer in storage/, testCase.profile);
        await assert.rejects(
          withSandbox({ directories: [], paths: [missing] }, (connection) => connection.run(count(missing))),
          (error: unknown) => /The data file is no longer in storage/.test(parseDuckDbError(error).message),
          `${testCase.profile} in a sandbox`,
        );
      }
    });
  });
});
