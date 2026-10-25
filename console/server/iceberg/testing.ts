/**
 * Test support: the checked-in fixture tables (`testdata/iceberg/{orders,payments}`), written by the
 * control plane's own library, and a reader serving their `gs://` objects from disk.
 *
 * They are the two tables of one materialization, `shop`: each lives in a directory named after it
 * below the materialization's location, and so does its local copy.
 */

import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

import { ObjectMissingError } from './io.ts';

const here = dirname(fileURLToPath(import.meta.url));

/** Where the tables' copies are: one directory per table. */
export const FIXTURE_ROOT = join(here, '..', '..', 'testdata', 'iceberg');
/** The materialization's location; each table is a directory below it. */
export const FIXTURE_MATERIALIZATION_LOCATION = 'gs://lake/materializer/mat-a1b2c3d4e5f6';
export const FIXTURE_DIR = join(FIXTURE_ROOT, 'orders');
export const FIXTURE_LOCATION = `${FIXTURE_MATERIALIZATION_LOCATION}/orders`;
export const PAYMENTS_DIR = join(FIXTURE_ROOT, 'payments');
export const PAYMENTS_LOCATION = `${FIXTURE_MATERIALIZATION_LOCATION}/payments`;

/** Objects under the fixture tables' `gs://` locations are read from the checked-in copies. */
export function fixtureReader(location: string = FIXTURE_MATERIALIZATION_LOCATION): (uri: string) => Promise<Buffer> {
  return async (uri) => {
    const path = fixturePath(uri, location);
    if (path === null) throw new ObjectMissingError(uri);
    try {
      return await readFile(path);
    } catch {
      throw new ObjectMissingError(uri);
    }
  };
}

/** The local copy of one of the fixture tables' objects, for DuckDB to open. */
export function fixturePath(uri: string, location: string = FIXTURE_MATERIALIZATION_LOCATION): string | null {
  if (!uri.startsWith(`${location}/`)) return null;
  const relative = uri.slice(location.length + 1);
  // Under one table's location the path is the table's own; under the materialization's, it names the table first.
  return location === FIXTURE_MATERIALIZATION_LOCATION ? join(FIXTURE_ROOT, relative) : join(FIXTURE_ROOT, location.slice(FIXTURE_MATERIALIZATION_LOCATION.length + 1), relative);
}

/**
 * An Avro container file with every occurrence of `from` replaced by `to`, which must be as long, so
 * every record's encoding stays valid. Each block is inflated, rewritten and deflated again: enough
 * to give a fixture manifest a data file somewhere else.
 */
export function rewriteAvro(buffer: Buffer, from: string, to: string): Buffer {
  if (Buffer.byteLength(from) !== Buffer.byteLength(to)) throw new Error('rewriteAvro replaces text with text as long');
  let position = 4;
  for (;;) {
    let count: number;
    [count, position] = readLong(buffer, position);
    if (count === 0) break;
    if (count < 0) {
      count = -count;
      [, position] = readLong(buffer, position);
    }
    for (let field = 0; field < count * 2; field += 1) {
      let length: number;
      [length, position] = readLong(buffer, position);
      position += length;
    }
  }
  const sync = buffer.subarray(position, position + 16);
  const parts = [buffer.subarray(0, position + 16)];
  position += 16;
  while (position < buffer.length) {
    const [count, afterCount] = readLong(buffer, position);
    const [size, start] = readLong(buffer, afterCount);
    const block = zlib.inflateRawSync(buffer.subarray(start, start + size)).toString('latin1');
    const deflated = zlib.deflateRawSync(Buffer.from(block.split(from).join(to), 'latin1'));
    parts.push(writeLong(count), writeLong(deflated.length), deflated, sync);
    position = start + size + 16;
  }
  return Buffer.concat(parts);
}

/** Avro's zigzag variable-length `long`: its value, and the offset after it. */
function readLong(buffer: Buffer, offset: number): [number, number] {
  let raw = 0n;
  let shift = 0n;
  let position = offset;
  for (;;) {
    const byte = buffer[position] as number;
    position += 1;
    raw |= BigInt(byte & 0x7f) << shift;
    shift += 7n;
    if ((byte & 0x80) === 0) break;
  }
  return [Number((raw >> 1n) ^ -(raw & 1n)), position];
}

function writeLong(value: number): Buffer {
  let raw = BigInt(value) >= 0n ? BigInt(value) << 1n : (-BigInt(value) << 1n) - 1n;
  const bytes: number[] = [];
  do {
    let byte = Number(raw & 0x7fn);
    raw >>= 7n;
    if (raw > 0n) byte |= 0x80;
    bytes.push(byte);
  } while (raw > 0n);
  return Buffer.from(bytes);
}
