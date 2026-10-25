/**
 * Manifest lists and manifests: the Avro files that say which data files a snapshot holds.
 *
 * A snapshot names one manifest list; each entry of it names a manifest; each manifest entry is
 * one data file with its row count, size, partition and per-column bounds. Manifests never change
 * once written, so a decoded one is kept for the life of the process and a new snapshot costs only
 * the manifests it added — usually one.
 *
 * The bounds are why this reader exists rather than asking DuckDB: they give every data file's
 * Kafka-timestamp span (field id 1), partition (2) and offsets (3), which is what lets a time range
 * skip files and a browse run know its exact total without opening any of them.
 */

import { promisify } from 'node:util';
import zlib from 'node:zlib';

import avro from 'avsc';

import { readObject } from './io.ts';
import { snapshotIdText } from './json.ts';

const inflateRaw = promisify(zlib.inflateRaw);

/** Avro longs decode as BigInt: file sizes fit a double, snapshot ids do not. */
const LONG = avro.types.LongType.__with({
  fromBuffer: (buffer: Buffer) => buffer.readBigInt64LE(),
  toBuffer: (value: bigint | number) => {
    const buffer = Buffer.alloc(8);
    buffer.writeBigInt64LE(BigInt(value));
    return buffer;
  },
  fromJSON: (value: unknown) => BigInt(value as string),
  toJSON: (value: bigint) => Number(value),
  isValid: (value: unknown) => typeof value === 'bigint',
  compare: (a: bigint, b: bigint) => (a === b ? 0 : a < b ? -1 : 1),
});

type Codec = (buffer: Buffer, callback: (cause: Error | null, inflated?: Buffer) => void) => void;

const CODECS: Record<string, Codec> = {
  null: (buffer, callback) => callback(null, buffer),
  deflate: (buffer, callback) => {
    inflateRaw(buffer).then((inflated) => callback(null, inflated), (cause: Error) => callback(cause));
  },
  ...('zstdDecompress' in zlib
    ? {
        zstandard: (buffer: Buffer, callback: (cause: Error | null, inflated?: Buffer) => void) => {
          const decompress = promisify((zlib as unknown as { zstdDecompress: typeof zlib.inflateRaw }).zstdDecompress);
          decompress(buffer).then((inflated) => callback(null, inflated), (cause: Error) => callback(cause));
        },
      }
    : {}),
};

/** Every record of an Avro object container file. */
export async function decodeAvro(buffer: Buffer): Promise<Record<string, unknown>[]> {
  return new Promise((resolve, reject) => {
    const records: Record<string, unknown>[] = [];
    const decoder = new avro.streams.BlockDecoder({
      parseHook: (schema: unknown) => avro.Type.forSchema(schema as avro.Schema, { registry: { long: LONG } } as never),
      codecs: CODECS as never,
    } as never);
    decoder.on('data', (record: Record<string, unknown>) => records.push(record));
    decoder.on('end', () => resolve(records));
    decoder.on('error', reject);
    decoder.end(buffer);
  });
}

/** One entry of a manifest list: a manifest, and what it holds. */
export interface ManifestFile {
  path: string;
  length: number;
  specId: number;
  /** 0: data files; 1: delete files. */
  content: number;
  sequenceNumber: number;
  minSequenceNumber: number;
  addedSnapshotId: string | null;
  addedFiles: number;
  existingFiles: number;
  deletedFiles: number;
  addedRows: number;
  existingRows: number;
  deletedRows: number;
}

/** Entry status in a manifest. A deleted entry records a file the snapshot no longer holds. */
export const ENTRY_EXISTING = 0;
export const ENTRY_ADDED = 1;
export const ENTRY_DELETED = 2;

/** One entry of a manifest: a data (or delete) file. */
export interface ManifestEntry {
  status: number;
  snapshotId: string | null;
  sequenceNumber: number | null;
  /** 0: data; 1: position deletes; 2: equality deletes. */
  content: number;
  filePath: string;
  fileFormat: string;
  partition: Record<string, number | null>;
  recordCount: number;
  fileSizeInBytes: number;
  valueCounts: Map<number, number>;
  nullValueCounts: Map<number, number>;
  lowerBounds: Map<number, Buffer>;
  upperBounds: Map<number, Buffer>;
}

export async function readManifestList(uri: string): Promise<ManifestFile[]> {
  const records = await decodeAvro(await readObject(uri));
  return records.map((record) => ({
    path: String(record.manifest_path),
    length: toNumber(record.manifest_length),
    specId: toNumber(record.partition_spec_id),
    content: toNumber(record.content ?? 0),
    sequenceNumber: toNumber(record.sequence_number ?? 0),
    minSequenceNumber: toNumber(record.min_sequence_number ?? 0),
    addedSnapshotId: snapshotIdText(record.added_snapshot_id),
    addedFiles: toNumber(record.added_files_count ?? record.added_data_files_count ?? 0),
    existingFiles: toNumber(record.existing_files_count ?? record.existing_data_files_count ?? 0),
    deletedFiles: toNumber(record.deleted_files_count ?? record.deleted_data_files_count ?? 0),
    addedRows: toNumber(record.added_rows_count ?? 0),
    existingRows: toNumber(record.existing_rows_count ?? 0),
    deletedRows: toNumber(record.deleted_rows_count ?? 0),
  }));
}

/**
 * The entries of one manifest, with sequence numbers and snapshot ids inherited from the manifest
 * list where the entry leaves them out (the v2 spec's inheritance rule).
 */
export async function readManifest(manifest: ManifestFile): Promise<ManifestEntry[]> {
  const records = await decodeAvro(await readObject(manifest.path));
  return records.map((record) => {
    const file = (record.data_file ?? {}) as Record<string, unknown>;
    const status = toNumber(record.status);
    const explicitSequence = record.sequence_number ?? null;
    return {
      status,
      snapshotId: snapshotIdText(record.snapshot_id) ?? manifest.addedSnapshotId,
      sequenceNumber:
        explicitSequence !== null ? toNumber(explicitSequence) : status === ENTRY_ADDED ? manifest.sequenceNumber : null,
      content: toNumber(file.content ?? 0),
      filePath: String(file.file_path),
      fileFormat: String(file.file_format ?? 'PARQUET'),
      partition: partitionValues(file.partition),
      recordCount: toNumber(file.record_count),
      fileSizeInBytes: toNumber(file.file_size_in_bytes),
      valueCounts: countMap(file.value_counts),
      nullValueCounts: countMap(file.null_value_counts),
      lowerBounds: boundMap(file.lower_bounds),
      upperBounds: boundMap(file.upper_bounds),
    };
  });
}

function toNumber(value: unknown): number {
  if (typeof value === 'bigint') return Number(value);
  if (typeof value === 'number') return value;
  if (value === null || value === undefined) return 0;
  return Number(value);
}

function partitionValues(value: unknown): Record<string, number | null> {
  if (!value || typeof value !== 'object') return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, item === null || item === undefined ? null : toNumber(item)]),
  );
}

/** Iceberg writes an int-keyed map as an array of `{ key, value }` records. */
function countMap(value: unknown): Map<number, number> {
  const result = new Map<number, number>();
  if (!Array.isArray(value)) return result;
  for (const item of value as Array<{ key: unknown; value: unknown }>) result.set(toNumber(item.key), toNumber(item.value));
  return result;
}

function boundMap(value: unknown): Map<number, Buffer> {
  const result = new Map<number, Buffer>();
  if (!Array.isArray(value)) return result;
  for (const item of value as Array<{ key: unknown; value: unknown }>) {
    if (Buffer.isBuffer(item.value)) result.set(toNumber(item.key), item.value);
  }
  return result;
}

/**
 * A bound's value, by the column's Iceberg type (the spec's single-value serialization).
 *
 * Integers and timestamps are little-endian; a `long` or a timestamp comes back as a BigInt, since
 * microseconds since the epoch do not fit a double exactly.
 */
export function decodeBound(type: string, buffer: Buffer): number | bigint | string | boolean | null {
  switch (type) {
    case 'boolean':
      return buffer.length > 0 ? buffer[0] !== 0 : null;
    case 'int':
    case 'date':
      return buffer.length >= 4 ? buffer.readInt32LE(0) : null;
    case 'long':
    case 'time':
    case 'timestamp':
    case 'timestamptz':
      // A bound written by a v1 writer for an int column promoted to long is 4 bytes.
      if (buffer.length >= 8) return buffer.readBigInt64LE(0);
      return buffer.length >= 4 ? BigInt(buffer.readInt32LE(0)) : null;
    case 'float':
      return buffer.length >= 4 ? buffer.readFloatLE(0) : null;
    case 'double':
      return buffer.length >= 8 ? buffer.readDoubleLE(0) : null;
    case 'string':
      return buffer.toString('utf8');
    default:
      return null;
  }
}
