/**
 * An Iceberg table metadata file (`metadata/<n>-<uuid>.metadata.json`), as far as the console reads it.
 *
 * One file holds everything about the table at one commit: every schema it has had, its partition
 * spec and sort order, its properties (the fork map among them), and its snapshots, each naming the
 * manifest list that says which data files it holds. Snapshot ids are kept as decimal text: they
 * are 64-bit and would not survive a double (`json.ts`).
 */

import type { PartitionGranularity } from '../../shared/types.ts';
import { readObject } from './io.ts';
import { parseJson, snapshotIdText } from './json.ts';

export type IcebergType = string | IcebergStruct | IcebergList | IcebergMap;

export interface IcebergStruct {
  type: 'struct';
  fields: IcebergField[];
}

export interface IcebergList {
  type: 'list';
  'element-id': number;
  element: IcebergType;
  'element-required': boolean;
}

export interface IcebergMap {
  type: 'map';
  'key-id': number;
  key: IcebergType;
  'value-id': number;
  value: IcebergType;
  'value-required': boolean;
}

export interface IcebergField {
  id: number;
  name: string;
  required: boolean;
  type: IcebergType;
  doc?: string;
}

export interface IcebergSchema {
  schemaId: number;
  fields: IcebergField[];
}

export interface PartitionField {
  sourceId: number;
  fieldId: number;
  transform: string;
  name: string;
}

export interface SortField {
  sourceId: number;
  transform: string;
  direction: string;
  nullOrder: string;
}

export interface IcebergSnapshot {
  snapshotId: string;
  parentSnapshotId: string | null;
  sequenceNumber: number;
  timestampMs: number;
  manifestList: string;
  summary: Record<string, string>;
  schemaId: number | null;
}

export interface TableMetadata {
  formatVersion: number;
  tableUuid: string;
  location: string;
  lastUpdatedMs: number;
  lastColumnId: number;
  lastSequenceNumber: number;
  currentSchemaId: number;
  schemas: IcebergSchema[];
  defaultSpecId: number;
  partitionSpecs: Array<{ specId: number; fields: PartitionField[] }>;
  defaultSortOrderId: number;
  sortOrders: Array<{ orderId: number; fields: SortField[] }>;
  properties: Record<string, string>;
  currentSnapshotId: string | null;
  /** Oldest first. */
  snapshots: IcebergSnapshot[];
  /** Earlier metadata files, oldest first. */
  metadataLog: Array<{ timestampMs: number; metadataFile: string }>;
}

export async function readMetadata(metadataLocation: string): Promise<TableMetadata> {
  return parseMetadata((await readObject(metadataLocation)).toString('utf8'));
}

export function parseMetadata(text: string): TableMetadata {
  const raw = parseJson(text) as Record<string, unknown>;
  const formatVersion = Number(raw['format-version'] ?? 1);
  const schemas = asArray(raw.schemas).map((schema) => parseSchema(schema as Record<string, unknown>));
  if (schemas.length === 0 && raw.schema) schemas.push(parseSchema(raw.schema as Record<string, unknown>));
  const snapshots = asArray(raw.snapshots)
    .map((snapshot) => parseSnapshot(snapshot as Record<string, unknown>))
    .sort((a, b) => a.sequenceNumber - b.sequenceNumber || a.timestampMs - b.timestampMs);
  const currentSnapshotId = snapshotIdText(raw['current-snapshot-id']);
  return {
    formatVersion,
    tableUuid: String(raw['table-uuid'] ?? ''),
    location: String(raw.location ?? ''),
    lastUpdatedMs: Number(raw['last-updated-ms'] ?? 0),
    lastColumnId: Number(raw['last-column-id'] ?? 0),
    lastSequenceNumber: Number(raw['last-sequence-number'] ?? 0),
    currentSchemaId: Number(raw['current-schema-id'] ?? schemas[0]?.schemaId ?? 0),
    schemas,
    defaultSpecId: Number(raw['default-spec-id'] ?? 0),
    partitionSpecs: asArray(raw['partition-specs']).map((spec) => {
      const record = spec as Record<string, unknown>;
      return {
        specId: Number(record['spec-id'] ?? 0),
        fields: asArray(record.fields).map((field) => {
          const item = field as Record<string, unknown>;
          return {
            sourceId: Number(item['source-id']),
            fieldId: Number(item['field-id']),
            transform: String(item.transform),
            name: String(item.name),
          };
        }),
      };
    }),
    defaultSortOrderId: Number(raw['default-sort-order-id'] ?? 0),
    sortOrders: asArray(raw['sort-orders']).map((order) => {
      const record = order as Record<string, unknown>;
      return {
        orderId: Number(record['order-id'] ?? 0),
        fields: asArray(record.fields).map((field) => {
          const item = field as Record<string, unknown>;
          return {
            sourceId: Number(item['source-id']),
            transform: String(item.transform),
            direction: String(item.direction ?? 'asc'),
            nullOrder: String(item['null-order'] ?? 'nulls-first'),
          };
        }),
      };
    }),
    properties: Object.fromEntries(
      Object.entries((raw.properties as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [key, String(value)]),
    ),
    currentSnapshotId: currentSnapshotId === '-1' ? null : currentSnapshotId,
    snapshots,
    metadataLog: asArray(raw['metadata-log']).map((entry) => {
      const record = entry as Record<string, unknown>;
      return { timestampMs: Number(record['timestamp-ms'] ?? 0), metadataFile: String(record['metadata-file'] ?? '') };
    }),
  };
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function parseSchema(raw: Record<string, unknown>): IcebergSchema {
  return { schemaId: Number(raw['schema-id'] ?? 0), fields: asArray(raw.fields).map(parseField) };
}

function parseField(raw: unknown): IcebergField {
  const record = raw as Record<string, unknown>;
  return {
    id: Number(record.id),
    name: String(record.name),
    required: Boolean(record.required),
    type: parseType(record.type),
    ...(typeof record.doc === 'string' ? { doc: record.doc } : {}),
  };
}

function parseType(raw: unknown): IcebergType {
  if (typeof raw === 'string') return raw;
  const record = raw as Record<string, unknown>;
  switch (record.type) {
    case 'struct':
      return { type: 'struct', fields: asArray(record.fields).map(parseField) };
    case 'list':
      return {
        type: 'list',
        'element-id': Number(record['element-id']),
        element: parseType(record.element),
        'element-required': Boolean(record['element-required']),
      };
    case 'map':
      return {
        type: 'map',
        'key-id': Number(record['key-id']),
        key: parseType(record.key),
        'value-id': Number(record['value-id']),
        value: parseType(record.value),
        'value-required': Boolean(record['value-required']),
      };
    default:
      throw new Error(`unknown Iceberg type ${JSON.stringify(raw)}`);
  }
}

function parseSnapshot(raw: Record<string, unknown>): IcebergSnapshot {
  const summary = Object.fromEntries(
    Object.entries((raw.summary as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [key, String(value)]),
  );
  return {
    snapshotId: snapshotIdText(raw['snapshot-id']) ?? '0',
    parentSnapshotId: snapshotIdText(raw['parent-snapshot-id']),
    sequenceNumber: Number(raw['sequence-number'] ?? 0),
    timestampMs: Number(raw['timestamp-ms'] ?? 0),
    manifestList: String(raw['manifest-list'] ?? ''),
    summary,
    schemaId: raw['schema-id'] === undefined || raw['schema-id'] === null ? null : Number(raw['schema-id']),
  };
}

export function schemaById(metadata: TableMetadata, schemaId: number): IcebergSchema | undefined {
  return metadata.schemas.find((schema) => schema.schemaId === schemaId);
}

export function currentSchema(metadata: TableMetadata): IcebergSchema {
  const schema = schemaById(metadata, metadata.currentSchemaId) ?? metadata.schemas.at(-1);
  if (!schema) throw new Error('the table metadata holds no schema');
  return schema;
}

export function snapshotById(metadata: TableMetadata, snapshotId: string): IcebergSnapshot | undefined {
  return metadata.snapshots.find((snapshot) => snapshot.snapshotId === snapshotId);
}

export function currentSnapshot(metadata: TableMetadata): IcebergSnapshot | null {
  return metadata.currentSnapshotId === null ? null : (snapshotById(metadata, metadata.currentSnapshotId) ?? null);
}

/** The partition granularity a materializer table was created with: hour, day, month, or none. */
export function partitionGranularity(metadata: TableMetadata): PartitionGranularity {
  const spec = metadata.partitionSpecs.find((candidate) => candidate.specId === metadata.defaultSpecId);
  const transform = spec?.fields.find((field) => field.sourceId === 1)?.transform;
  if (transform === 'day') return 'day';
  if (transform === 'hour') return 'hour';
  if (transform === 'month') return 'month';
  return 'none';
}

/** An Iceberg type as text, the way the control plane writes it: `long`, `struct`, `list<string>`. */
export function typeText(type: IcebergType): string {
  if (typeof type === 'string') return type;
  if (type.type === 'struct') return 'struct';
  if (type.type === 'list') return `list<${typeText(type.element)}>`;
  return `map<${typeText(type.key)}, ${typeText(type.value)}>`;
}
