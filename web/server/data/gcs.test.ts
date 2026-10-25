/**
 * The console's GCS reads: a table's metadata objects into memory, its data files into the cache.
 *
 * A fake of GCS's JSON API answers, reached the way the client library reaches an emulator
 * (`STORAGE_EMULATOR_HOST`, called without credentials). It serves each object with the checksum
 * headers GCS sends, and can refuse, fail for a while, or send bytes that fail their checksum.
 *
 * The client library's download stream printed `MaxListenersExceededWarning` twice for every object
 * read from GCS. It does not against a local fake, so the tests make it throw instead: no read may
 * go through it.
 */

import assert from 'node:assert/strict';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, beforeEach, describe, it } from 'node:test';

import { CRC32C, File, Storage, type StorageOptions } from '@google-cloud/storage';

File.prototype.createReadStream = () => {
  throw new Error("read through the client library's download stream, which warns on every object");
};

interface StoredObject {
  bytes: Buffer;
  /** Answers to give before the object: one status per request. */
  failures?: number[];
  /** Send a checksum that does not match the bytes. */
  corrupt?: boolean;
}

const objects = new Map<string, StoredObject>();
const requests: { path: string; authorization: string | undefined; cacheControl: string | undefined }[] = [];

function key(bucket: string, name: string): string {
  return `${bucket}/${name}`;
}

function serve(request: IncomingMessage, response: ServerResponse): void {
  const url = new URL(request.url ?? '/', 'http://fake-gcs');
  requests.push({
    path: url.pathname,
    authorization: request.headers.authorization,
    cacheControl: request.headers['cache-control'],
  });
  const match = /^\/storage\/v1\/b\/([^/]+)\/o\/([^/]+)$/.exec(url.pathname);
  if (request.method !== 'GET' || !match || url.searchParams.get('alt') !== 'media') {
    response.writeHead(400).end();
    return;
  }
  const name = decodeURIComponent(match[2] ?? '');
  const stored = objects.get(key(decodeURIComponent(match[1] ?? ''), name));
  if (!stored) {
    response
      .writeHead(404, { 'content-type': 'application/json' })
      .end(JSON.stringify({ error: { code: 404, message: `No such object: ${name}` } }));
    return;
  }
  const failure = stored.failures?.shift();
  if (failure !== undefined) {
    response.writeHead(failure, { 'content-type': 'application/json' }).end(JSON.stringify({ error: { code: failure, message: 'Try again' } }));
    return;
  }
  const checksum = new CRC32C();
  checksum.update(stored.corrupt ? Buffer.from('other bytes') : stored.bytes);
  response
    .writeHead(200, {
      'content-type': 'application/octet-stream',
      'x-goog-hash': `crc32c=${checksum.toString()},md5=not-checked`,
      'x-goog-stored-content-encoding': 'identity',
    })
    .end(stored.bytes);
}

const server = createServer(serve);
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
const port = (server.address() as AddressInfo).port;

const dataDir = await mkdtemp(join(tmpdir(), 'console-gcs-'));
process.env.CONSOLE_DATA_DIR = dataDir;
process.env.STORAGE_EMULATOR_HOST = `http://127.0.0.1:${port}/storage/v1`;

const { downloadObject, ensureLocalCopies, GcsReadError, setStorageClient } = await import('./gcs.ts');
const { readObject, ObjectMissingError } = await import('../iceberg/io.ts');
const { config } = await import('../config.ts');

const warnings: Error[] = [];
process.on('warning', (warning) => warnings.push(warning));

after(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
  await rm(dataDir, { recursive: true, force: true });
});

beforeEach(() => {
  objects.clear();
  requests.length = 0;
  warnings.length = 0;
});

async function filesUnder(dir: string): Promise<string[]> {
  const found: string[] = [];
  for (const entry of await readdir(dir, { withFileTypes: true, recursive: true })) {
    if (entry.isFile()) found.push(join(entry.parentPath, entry.name));
  }
  return found.sort();
}

describe('metadata reads', () => {
  it('reads an object whole, fresh each time, without warnings', async () => {
    const pointer = Buffer.from(JSON.stringify({ 'metadata-location': 'gs://lake/shop/orders/metadata/00001.metadata.json' }));
    objects.set(key('lake', 'shop/orders/metadata/catalog-pointer.json'), { bytes: pointer });
    for (let read = 0; read < 12; read += 1) {
      assert.deepEqual(await downloadObject('lake', 'shop/orders/metadata/catalog-pointer.json'), pointer);
    }
    const together = await Promise.all(
      Array.from({ length: 12 }, () => downloadObject('lake', 'shop/orders/metadata/catalog-pointer.json')),
    );
    assert.ok(together.every((bytes) => bytes.equals(pointer)));
    await new Promise((resolve) => setImmediate(resolve));
    // The client library's download() printed MaxListenersExceededWarning twice for each of these.
    assert.deepEqual(warnings.map((warning) => warning.name), []);
    assert.equal(requests.length, 24);
    assert.equal(requests[0]?.path, '/storage/v1/b/lake/o/shop%2Forders%2Fmetadata%2Fcatalog-pointer.json');
    assert.equal(requests[0]?.cacheControl, 'no-store');
    // An emulator is called without credentials, as the client library calls it.
    assert.equal(requests[0]?.authorization, undefined);
  });

  it('reads with the credentials of the client library', async () => {
    objects.set(key('lake', 'pointer.json'), { bytes: Buffer.from('{}') });
    const credentials = { getRequestHeaders: async () => ({ Authorization: 'Bearer console-test-token' }) };
    setStorageClient(
      new Storage({
        apiEndpoint: `http://127.0.0.1:${port}`,
        useAuthWithCustomEndpoint: true,
        authClient: credentials as unknown as StorageOptions['authClient'],
      }),
    );
    try {
      assert.deepEqual(await downloadObject('lake', 'pointer.json'), Buffer.from('{}'));
    } finally {
      setStorageClient(null);
    }
    assert.equal(requests[0]?.authorization, 'Bearer console-test-token');
  });

  it('says a missing object is missing, at once', async () => {
    await assert.rejects(downloadObject('lake', 'shop/orders/metadata/catalog-pointer.json'), (error: unknown) => {
      assert.ok(error instanceof GcsReadError);
      assert.equal(error.code, 404);
      assert.match(error.message, /No such object: shop\/orders\/metadata\/catalog-pointer\.json/);
      return true;
    });
    assert.equal(requests.length, 1, 'a missing object is not retried');
    // What the table reader makes of it: a table not created yet.
    await assert.rejects(readObject('gs://lake/shop/orders/metadata/catalog-pointer.json'), ObjectMissingError);
  });

  it('retries what GCS asks to be retried', async () => {
    objects.set(key('lake', 'manifest.avro'), { bytes: Buffer.from('manifest'), failures: [503, 429] });
    assert.deepEqual(await downloadObject('lake', 'manifest.avro'), Buffer.from('manifest'));
    assert.equal(requests.length, 3);
  });

  it('does not retry a refusal', async () => {
    objects.set(key('lake', 'manifest.avro'), { bytes: Buffer.from('manifest'), failures: [403] });
    await assert.rejects(downloadObject('lake', 'manifest.avro'), (error: unknown) => error instanceof GcsReadError && error.code === 403);
    assert.equal(requests.length, 1);
  });

  it('refuses bytes that fail their checksum, after retrying them', async () => {
    objects.set(key('lake', 'manifest.avro'), { bytes: Buffer.from('manifest'), corrupt: true });
    await assert.rejects(downloadObject('lake', 'manifest.avro'), /do not match its stored CRC32C checksum/);
    assert.equal(requests.length, 3);
  });
});

describe('data files', () => {
  const file = { uri: 'gs://lake/shop/orders/data/part-0.parquet', name: 'shop/orders/data/part-0.parquet' };
  const target = join(config.parquetCacheDir, 'lake', 'shop', 'orders', 'data', 'part-0.parquet');

  it('mirrors a file into the cache once, without warnings', async () => {
    const bytes = Buffer.alloc(3 * 1024 * 1024, 7);
    objects.set(key('lake', file.name), { bytes, failures: [500] });
    const progress: [number, number][] = [];
    const paths = await ensureLocalCopies([file], { onProgress: (done, total) => progress.push([done, total]) });
    assert.equal(paths.get(file.uri), target);
    assert.deepEqual(await readFile(target), bytes);
    assert.deepEqual(progress, [[0, 1], [1, 1]]);
    assert.equal(requests.length, 2, 'the failed attempt and the one that read it');
    // Already cached: not read again.
    await ensureLocalCopies([file]);
    assert.equal(requests.length, 2);
    assert.deepEqual(await filesUnder(config.parquetCacheDir), [target]);
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(warnings.map((warning) => warning.name), []);
  });

  it('never leaves part of a file in the cache', async () => {
    await rm(config.parquetCacheDir, { recursive: true, force: true });
    objects.set(key('lake', file.name), { bytes: Buffer.alloc(1024, 1), corrupt: true });
    await assert.rejects(ensureLocalCopies([file]), /do not match its stored CRC32C checksum/);
    assert.deepEqual(await filesUnder(config.parquetCacheDir), [], 'neither the file nor its partial copy');
  });
});
