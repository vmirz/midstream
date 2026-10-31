/**
 * The S3 and Azure profiles' reads through their SDKs, each against a local fake of its HTTP API
 * (path-style S3; an emulator-style Azure endpoint): a missing object is missing at once, what the
 * service asks to be retried is, a refusal is not, bytes that fail the checksum the object carries
 * are refused after their retries, and a download is whole or not at all.
 */

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, beforeEach, describe, it } from 'node:test';
import { crc32 } from 'node:zlib';

interface StoredObject {
  bytes: Buffer;
  failures?: number[];
  /** Send a checksum of other bytes. */
  corrupt?: boolean;
}

const objects = new Map<string, StoredObject>();
const requests: string[] = [];

function crc32Header(bytes: Buffer): string {
  const value = Buffer.alloc(4);
  value.writeUInt32BE(crc32(bytes) >>> 0);
  return value.toString('base64');
}

/** S3, path style: GET /<bucket>/<key>. */
function serveS3(request: IncomingMessage, response: ServerResponse): void {
  const path = decodeURIComponent(new URL(request.url ?? '/', 'http://s3').pathname.slice(1));
  requests.push(path);
  const xml = (status: number, code: string, message: string): void => {
    response.writeHead(status, { 'content-type': 'application/xml' }).end(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>${code}</Code><Message>${message}</Message></Error>`);
  };
  const stored = objects.get(path);
  if (!stored) return xml(404, 'NoSuchKey', 'The specified key does not exist.');
  const failure = stored.failures?.shift();
  if (failure === 403) return xml(403, 'AccessDenied', 'Access Denied');
  if (failure !== undefined) return xml(failure, 'SlowDown', 'Please reduce your request rate.');
  response
    .writeHead(200, {
      'content-type': 'application/octet-stream',
      'content-length': String(stored.bytes.length),
      etag: '"0123456789abcdef"',
      'x-amz-checksum-crc32': crc32Header(stored.corrupt ? Buffer.from('other bytes') : stored.bytes),
    })
    .end(stored.bytes);
}

/** Azure, as an emulator answers: GET /<account>/<container>/<blob>. */
function serveAzure(request: IncomingMessage, response: ServerResponse): void {
  const path = decodeURIComponent(new URL(request.url ?? '/', 'http://azure').pathname.slice(1));
  requests.push(path);
  const fail = (status: number, code: string): void => {
    response
      .writeHead(status, { 'content-type': 'application/xml', 'x-ms-error-code': code, 'x-ms-request-id': 'r' })
      .end(`<?xml version="1.0" encoding="utf-8"?><Error><Code>${code}</Code><Message>${code}</Message></Error>`);
  };
  const stored = objects.get(path);
  if (!stored) return fail(404, 'BlobNotFound');
  const failure = stored.failures?.shift();
  if (failure === 403) return fail(403, 'AuthorizationFailure');
  if (failure !== undefined) return fail(failure, 'ServerBusy');
  response
    .writeHead(200, {
      'content-type': 'application/octet-stream',
      'content-length': String(stored.bytes.length),
      'content-md5': createHash('md5').update(stored.corrupt ? Buffer.from('other bytes') : stored.bytes).digest('base64'),
      'x-ms-blob-type': 'BlockBlob',
      'x-ms-request-id': 'r',
      'x-ms-version': '2025-01-05',
      etag: '"0x8D0"',
      'last-modified': new Date().toUTCString(),
    })
    .end(stored.bytes);
}

async function listen(handler: (request: IncomingMessage, response: ServerResponse) => void): Promise<{ url: string; close: () => Promise<void> }> {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

const s3 = await listen(serveS3);
const azure = await listen(serveAzure);
const dataDir = await mkdtemp(join(tmpdir(), 'console-sdk-'));
process.env.CONSOLE_DATA_DIR = dataDir;

const { S3Profile } = await import('./s3.ts');
const { AzureProfile } = await import('./azure.ts');
const { StorageReadError } = await import('./profile.ts');
const { useStorage } = await import('./registry.ts');
const { readObject, ObjectMissingError } = await import('../iceberg/io.ts');

const base = { region: null, pathStyle: null, access: null, keyFile: null } as const;
const lake = new S3Profile({
  ...base,
  name: 'lake',
  scope: 's3://example-lake/',
  kind: 's3',
  endpoint: s3.url,
  pathStyle: true,
  credentials: { ACCESS_KEY_ID: 'console-key-id', SECRET_ACCESS_KEY: 'console-secret' },
});
const blobs = new AzureProfile({
  ...base,
  name: 'azurite',
  scope: 'abfss://lake@devstoreaccount1.dfs.core.windows.net/',
  kind: 'azure',
  endpoint: azure.url,
  credentials: { AZURE_ACCOUNT_KEY: Buffer.from('console-account-key').toString('base64') },
});
useStorage([lake, blobs]);

after(async () => {
  await s3.close();
  await azure.close();
  await rm(dataDir, { recursive: true, force: true });
});

beforeEach(() => {
  objects.clear();
  requests.length = 0;
});

const cases = [
  { name: 'S3', profile: lake, uri: (key: string) => `s3://example-lake/${key}`, stored: (key: string) => `example-lake/${key}`, checksum: /stored S3 checksum/ },
  {
    name: 'Azure',
    profile: blobs,
    uri: (key: string) => `abfss://lake@devstoreaccount1.dfs.core.windows.net/${key}`,
    stored: (key: string) => `devstoreaccount1/lake/${key}`,
    checksum: /stored MD5 checksum/,
  },
];

for (const backend of cases) {
  describe(`${backend.name} reads`, () => {
    it('read an object whole', async () => {
      objects.set(backend.stored('sales/orders/metadata/catalog-pointer.json'), { bytes: Buffer.from('{"metadata-location":"x"}') });
      assert.deepEqual(await readObject(backend.uri('sales/orders/metadata/catalog-pointer.json')), Buffer.from('{"metadata-location":"x"}'));
      assert.deepEqual(requests, [backend.stored('sales/orders/metadata/catalog-pointer.json')]);
    });

    it('say a missing object is missing, at once', async () => {
      await assert.rejects(backend.profile.readObject(backend.uri('nothing.json')), (error: unknown) => error instanceof StorageReadError && error.code === 404);
      assert.equal(requests.length, 1, 'a missing object is not retried');
      await assert.rejects(readObject(backend.uri('nothing.json')), ObjectMissingError);
    });

    it('retry what the service asks to be retried, and not a refusal', async () => {
      objects.set(backend.stored('manifest.avro'), { bytes: Buffer.from('manifest'), failures: [503, 500] });
      assert.deepEqual(await backend.profile.readObject(backend.uri('manifest.avro')), Buffer.from('manifest'));
      assert.equal(requests.length, 3);
      requests.length = 0;
      objects.set(backend.stored('refused.avro'), { bytes: Buffer.from('manifest'), failures: [403] });
      await assert.rejects(backend.profile.readObject(backend.uri('refused.avro')), (error: unknown) => error instanceof StorageReadError && error.code === 403);
      assert.equal(requests.length, 1);
    });

    it('refuse bytes that fail the checksum the object carries, after retrying them', async () => {
      objects.set(backend.stored('manifest.avro'), { bytes: Buffer.from('manifest'), corrupt: true });
      await assert.rejects(backend.profile.readObject(backend.uri('manifest.avro')), backend.checksum);
      assert.equal(requests.length, 3);
    });

    it('download a data file whole, or leave nothing behind', async () => {
      const target = join(dataDir, backend.name, 'a.parquet');
      await rm(join(dataDir, backend.name), { recursive: true, force: true });
      const { mkdir } = await import('node:fs/promises');
      await mkdir(join(dataDir, backend.name), { recursive: true });
      const bytes = Buffer.alloc(2 * 1024 * 1024, 3);
      objects.set(backend.stored('data/a.parquet'), { bytes, failures: [503] });
      await backend.profile.downloadToFile(backend.uri('data/a.parquet'), target);
      assert.deepEqual(await readFile(target), bytes);
      await rm(target);
      objects.set(backend.stored('data/a.parquet'), { bytes, corrupt: true });
      await assert.rejects(backend.profile.downloadToFile(backend.uri('data/a.parquet'), target), backend.checksum);
      assert.deepEqual(await readdir(join(dataDir, backend.name)), [], 'neither the file nor its partial copy');
    });
  });
}
