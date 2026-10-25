/**
 * When a table holds values of each of its fields, by the records' Kafka timestamps, read from the
 * statistics its manifests keep for every data file - no data file is opened.
 *
 * A manifest entry counts each column's values and nulls, a list's elements included, and bounds
 * the file's `_kafka_timestamp`. A file holding a value of a field therefore says the field had a
 * value at some instant between the file's bounds. Over every file that holds one, the earliest
 * record with a value lies between the smallest lower bound and the smallest upper bound, and the
 * latest between the largest lower and the largest upper bound: as tight as the files are, a day for
 * a day-partitioned table, an hour for an hour's. It does not matter in what order the runtime read
 * the records, which is what makes it the date a change happened rather than the date the runtime
 * met it: a backfill reads partitions at their own pace, and discovery samples the newest records
 * beside the oldest.
 */

import { isUnderLocation } from './io.ts';
import { ENTRY_DELETED, type ManifestEntry } from './manifests.ts';
import { currentSchema, snapshotById, type IcebergType, type TableMetadata } from './metadata.ts';
import { TIMESTAMP_FIELD_ID } from './schema.ts';
import { snapshotManifests, timestampMs } from './snapshot.ts';

/** Two instants, epoch milliseconds: a record's Kafka timestamp lies between them, both included. */
export interface Span {
  from: number;
  to: number;
}

export interface FieldPresence {
  /** When the earliest record holding a value of the field was written. */
  first: Span | null;
  /** When the latest one was. */
  last: Span | null;
  /** Data files holding a value; null for a struct, list or map, whose values are its fields'. */
  files: number | null;
  /** Its values, a list's elements each counted; null for a struct, list or map. */
  values: number | null;
}

export interface TablePresence {
  snapshotId: string | null;
  /** The span of Kafka timestamps the snapshot holds. */
  dataFrom: number | null;
  dataThrough: number | null;
  /** By field id; a field no file holds a value of is absent. */
  fields: Map<number, FieldPresence>;
}

interface Accumulator {
  files: number;
  values: number;
  firstFrom: number;
  firstTo: number;
  lastFrom: number;
  lastTo: number;
}

interface ManifestPresence {
  dataFrom: number;
  dataThrough: number;
  fields: Map<number, Accumulator>;
}

/** What one manifest's live data files say, kept with its entries: a manifest never changes. */
const byManifest = new WeakMap<ManifestEntry[], ManifestPresence>();

/** When the table held values of each field, as of `snapshotId` (the current snapshot when not given). */
export async function tablePresence(
  metadata: TableMetadata,
  snapshotId: string | null = metadata.currentSnapshotId,
): Promise<TablePresence> {
  const snapshot = snapshotId === null ? undefined : snapshotById(metadata, snapshotId);
  if (!snapshot) return { snapshotId: null, dataFrom: null, dataThrough: null, fields: new Map() };
  const total: ManifestPresence = { dataFrom: Infinity, dataThrough: -Infinity, fields: new Map() };
  for (const { manifest, entries } of await snapshotManifests(metadata, snapshot)) {
    // Delete manifests list no rows; the console refuses a snapshot that has any.
    if (manifest.content !== 0) continue;
    let part = byManifest.get(entries);
    if (!part) {
      part = manifestPresence(entries, metadata.location);
      byManifest.set(entries, part);
    }
    merge(total, part);
  }
  const leaves = new Map<number, FieldPresence>();
  for (const [fieldId, accumulator] of total.fields) leaves.set(fieldId, presenceOf(accumulator));
  return {
    snapshotId: snapshot.snapshotId,
    dataFrom: Number.isFinite(total.dataFrom) ? total.dataFrom : null,
    dataThrough: Number.isFinite(total.dataThrough) ? total.dataThrough : null,
    fields: withContainers(leaves, metadata),
  };
}

function manifestPresence(entries: ManifestEntry[], location: string): ManifestPresence {
  const result: ManifestPresence = { dataFrom: Infinity, dataThrough: -Infinity, fields: new Map() };
  for (const entry of entries) {
    // The snapshot's own live data files, as its catalog reads them: never a file outside the table.
    if (entry.status === ENTRY_DELETED || entry.content !== 0 || !isUnderLocation(entry.filePath, location)) continue;
    const lower = timestampMs(entry.lowerBounds.get(TIMESTAMP_FIELD_ID));
    const upper = timestampMs(entry.upperBounds.get(TIMESTAMP_FIELD_ID));
    const timed = lower !== null && upper !== null;
    if (timed) {
      result.dataFrom = Math.min(result.dataFrom, lower);
      result.dataThrough = Math.max(result.dataThrough, upper);
    }
    for (const [fieldId, count] of entry.valueCounts) {
      const present = count - (entry.nullValueCounts.get(fieldId) ?? 0);
      if (present <= 0) continue;
      let accumulator = result.fields.get(fieldId);
      if (!accumulator) {
        accumulator = { files: 0, values: 0, firstFrom: Infinity, firstTo: Infinity, lastFrom: -Infinity, lastTo: -Infinity };
        result.fields.set(fieldId, accumulator);
      }
      accumulator.files += 1;
      accumulator.values += present;
      // A file of rows without a Kafka timestamp says nothing of when.
      if (!timed) continue;
      accumulator.firstFrom = Math.min(accumulator.firstFrom, lower);
      accumulator.firstTo = Math.min(accumulator.firstTo, upper);
      accumulator.lastFrom = Math.max(accumulator.lastFrom, lower);
      accumulator.lastTo = Math.max(accumulator.lastTo, upper);
    }
  }
  return result;
}

function merge(into: ManifestPresence, part: ManifestPresence): void {
  into.dataFrom = Math.min(into.dataFrom, part.dataFrom);
  into.dataThrough = Math.max(into.dataThrough, part.dataThrough);
  for (const [fieldId, accumulator] of part.fields) {
    const current = into.fields.get(fieldId);
    if (!current) {
      into.fields.set(fieldId, { ...accumulator });
      continue;
    }
    current.files += accumulator.files;
    current.values += accumulator.values;
    current.firstFrom = Math.min(current.firstFrom, accumulator.firstFrom);
    current.firstTo = Math.min(current.firstTo, accumulator.firstTo);
    current.lastFrom = Math.max(current.lastFrom, accumulator.lastFrom);
    current.lastTo = Math.max(current.lastTo, accumulator.lastTo);
  }
}

function presenceOf(accumulator: Accumulator): FieldPresence {
  const dated = Number.isFinite(accumulator.firstFrom);
  return {
    first: dated ? { from: accumulator.firstFrom, to: accumulator.firstTo } : null,
    last: dated ? { from: accumulator.lastFrom, to: accumulator.lastTo } : null,
    files: accumulator.files,
    values: accumulator.values,
  };
}

/**
 * A struct, list or map has no statistics of its own: it holds a value when one of its fields does,
 * so its first record is the earliest of theirs and its last the latest. Walked over the current
 * schema, which holds every field the table has had - nothing is ever dropped.
 */
function withContainers(leaves: Map<number, FieldPresence>, metadata: TableMetadata): Map<number, FieldPresence> {
  const fields = new Map(leaves);
  const visit = (fieldId: number, type: IcebergType): FieldPresence | null => {
    if (typeof type === 'string') return fields.get(fieldId) ?? null;
    const children: FieldPresence[] = [];
    const child = (id: number, childType: IcebergType): void => {
      const presence = visit(id, childType);
      if (presence) children.push(presence);
    };
    if (type.type === 'struct') for (const field of type.fields) child(field.id, field.type);
    else if (type.type === 'list') child(type['element-id'], type.element);
    else {
      child(type['key-id'], type.key);
      child(type['value-id'], type.value);
    }
    const combined = combine(children);
    if (combined) fields.set(fieldId, combined);
    return combined;
  };
  for (const field of currentSchema(metadata).fields) visit(field.id, field.type);
  return fields;
}

function combine(children: FieldPresence[]): FieldPresence | null {
  if (children.length === 0) return null;
  return {
    first: bound(children.map((child) => child.first), Math.min),
    last: bound(children.map((child) => child.last), Math.max),
    files: null,
    values: null,
  };
}

/** The earliest (`Math.min`) or the latest (`Math.max`) of several spans, bound by bound. */
function bound(spans: (Span | null)[], pick: (a: number, b: number) => number): Span | null {
  return spans.reduce<Span | null>((result, span) => {
    if (!span) return result;
    return result ? { from: pick(result.from, span.from), to: pick(result.to, span.to) } : { ...span };
  }, null);
}
