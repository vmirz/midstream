/**
 * A table schema read the way the materializer writes it: record keys, fork families and JSON
 * collapses, from the Iceberg schema and the fork map (`materializer.fork-map`).
 *
 * The control plane's `lakehouse/schema/index.py` and `render.py` are the reference; this is the
 * same walk, so the console names every field exactly as the API's schema projection does. Payload
 * fields are indexed by the record key they hold: usually one field named after its key; a key
 * whose values changed type has a *family* — `total`, `total__v2` — and one whose family forked too
 * often has a `__json` member holding its values as JSON text from then on.
 */

import type { IcebergField, IcebergList, IcebergSchema, IcebergType } from './metadata.ts';
import { typeText } from './metadata.ts';

/** Path segment standing for a list's element. */
export const ELEMENT = '[]';

/** Ids 1-9 are the Kafka metadata columns, the header map's key and value included. */
export const FIRST_PAYLOAD_FIELD_ID = 10;
export const TIMESTAMP_FIELD_ID = 1;
export const PARTITION_FIELD_ID = 2;
export const OFFSET_FIELD_ID = 3;

export function isMetadataFieldId(id: number): boolean {
  return id >= 1 && id < FIRST_PAYLOAD_FIELD_ID;
}

export type ForkState = 'current' | 'superseded' | 'collapsed';

export interface ForkEntry {
  fieldId: number;
  path: string[];
  /** The record key the field holds, when it is not the field's own name. */
  key: string | null;
  forkOf: number | null;
  forkVersion: number;
  state: ForkState;
}

export type ForkMap = ReadonlyMap<number, ForkEntry>;

export const FORK_MAP_PROPERTY = 'materializer.fork-map';

export function parseForkMap(text: string | undefined | null): ForkMap {
  const map = new Map<number, ForkEntry>();
  if (!text) return map;
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return map;
  }
  const fields = (raw as { fields?: Record<string, Record<string, unknown>> } | null)?.fields ?? {};
  for (const [id, entry] of Object.entries(fields)) {
    const fieldId = Number(id);
    if (!Number.isInteger(fieldId)) continue;
    const state = entry.state === 'superseded' || entry.state === 'collapsed' ? entry.state : 'current';
    map.set(fieldId, {
      fieldId,
      path: Array.isArray(entry.path) ? entry.path.map(String) : [],
      key: typeof entry.key === 'string' ? entry.key : null,
      forkOf: entry.fork_of === null || entry.fork_of === undefined ? null : Number(entry.fork_of),
      forkVersion: Number(entry.fork_version ?? 1) || 1,
      state,
    });
  }
  return map;
}

export interface ElementIndex {
  elementId: number;
  type: IcebergType;
  required: boolean;
  json: boolean;
  path: string[];
  logicalPath: string[];
  struct: StructIndex | null;
  element: ElementIndex | null;
}

export interface Member {
  fieldId: number;
  name: string;
  type: IcebergType;
  required: boolean;
  forkVersion: number;
  state: ForkState;
  /** A collapse field: its values are JSON text. */
  json: boolean;
  path: string[];
  logicalPath: string[];
  forkOf: number | null;
  struct: StructIndex | null;
  element: ElementIndex | null;
  doc: string | null;
}

export interface Family {
  key: string;
  /** Oldest first. */
  members: Member[];
}

export interface StructIndex {
  path: string[];
  logicalPath: string[];
  families: Map<string, Family>;
}

/** The payload fields of `schema`, indexed by record key with `forkMap`'s families and renames. */
export function buildIndex(schema: IcebergSchema, forkMap: ForkMap): StructIndex {
  const payload = schema.fields.filter((field) => !isMetadataFieldId(field.id));
  return indexStruct(payload, [], [], forkMap);
}

function indexStruct(fields: IcebergField[], path: string[], logicalPath: string[], forkMap: ForkMap): StructIndex {
  const index: StructIndex = { path, logicalPath, families: new Map() };
  const ids = new Set(fields.map((field) => field.id));
  const roots = new Map<number, string>();
  const pending: Array<{ field: IcebergField; entry: ForkEntry }> = [];
  const familyOf = (key: string): Family => {
    let family = index.families.get(key);
    if (!family) {
      family = { key, members: [] };
      index.families.set(key, family);
    }
    return family;
  };
  for (const field of fields) {
    const entry = forkMap.get(field.id);
    if (entry && entry.forkOf !== null && ids.has(entry.forkOf)) {
      pending.push({ field, entry });
      continue;
    }
    const key = entry?.key ?? field.name;
    roots.set(field.id, key);
    familyOf(key).members.push(member(field, entry, key, path, logicalPath, forkMap));
  }
  for (const { field, entry } of pending) {
    const key = roots.get(entry.forkOf as number) ?? field.name;
    familyOf(key).members.push(member(field, entry, key, path, logicalPath, forkMap));
  }
  for (const family of index.families.values()) {
    family.members.sort((a, b) => a.forkVersion - b.forkVersion || a.fieldId - b.fieldId);
    const only = family.members[0];
    if (family.members.length === 1 && only && only.forkOf === null && only.state === 'superseded') only.state = 'current';
  }
  return index;
}

function member(
  field: IcebergField,
  entry: ForkEntry | undefined,
  key: string,
  path: string[],
  logicalPath: string[],
  forkMap: ForkMap,
): Member {
  const memberPath = [...path, field.name];
  const memberLogical = [...logicalPath, key];
  const json = entry?.state === 'collapsed';
  const result: Member = {
    fieldId: field.id,
    name: field.name,
    type: field.type,
    required: field.required,
    forkVersion: entry?.forkVersion ?? 1,
    state: entry?.state ?? 'current',
    json,
    path: memberPath,
    logicalPath: memberLogical,
    forkOf: entry?.forkOf ?? null,
    struct: null,
    element: null,
    doc: field.doc ?? null,
  };
  if (!json && typeof field.type !== 'string') {
    if (field.type.type === 'struct') result.struct = indexStruct(field.type.fields, memberPath, memberLogical, forkMap);
    else if (field.type.type === 'list') result.element = indexElement(field.type, memberPath, memberLogical, forkMap);
  }
  return result;
}

function indexElement(list: IcebergList, path: string[], logicalPath: string[], forkMap: ForkMap): ElementIndex {
  const elementPath = [...path, ELEMENT];
  const elementLogical = [...logicalPath, ELEMENT];
  const json = forkMap.get(list['element-id'])?.state === 'collapsed';
  const element: ElementIndex = {
    elementId: list['element-id'],
    type: list.element,
    required: list['element-required'],
    json,
    path: elementPath,
    logicalPath: elementLogical,
    struct: null,
    element: null,
  };
  if (!json && typeof list.element !== 'string') {
    if (list.element.type === 'struct') element.struct = indexStruct(list.element.fields, elementPath, elementLogical, forkMap);
    else if (list.element.type === 'list') element.element = indexElement(list.element, elementPath, elementLogical, forkMap);
  }
  return element;
}

/** `["items", "[]", "sku"]` → `items[].sku`. */
export function logicalName(path: readonly string[]): string {
  let text = '';
  for (const segment of path) {
    if (segment === ELEMENT) text += '[]';
    else text = text ? `${text}.${segment}` : segment;
  }
  return text;
}

export type FieldKind = 'value' | 'container' | 'json' | 'metadata';

/** One field as the Schema tab and the API show it (the control plane's `render_fields`). */
export interface ProjectedField {
  field_id: number;
  path: string[];
  name: string;
  type: string;
  required: boolean;
  kind: FieldKind;
  state: ForkState;
  fork_version: number;
  fork_of?: number | null;
  since_schema_id?: number | null;
  doc?: string | null;
}

/** For each field id, the lowest schema id that has it. */
export function firstSeenSchemaIds(schemas: readonly IcebergSchema[]): Map<number, number> {
  const since = new Map<number, number>();
  for (const schema of [...schemas].sort((a, b) => a.schemaId - b.schemaId)) {
    for (const id of fieldIds(schema.fields)) if (!since.has(id)) since.set(id, schema.schemaId);
  }
  return since;
}

function fieldIds(fields: readonly IcebergField[]): number[] {
  const ids: number[] = [];
  for (const field of fields) {
    ids.push(field.id);
    ids.push(...typeIds(field.type));
  }
  return ids;
}

function typeIds(type: IcebergType): number[] {
  if (typeof type === 'string') return [];
  if (type.type === 'struct') return fieldIds(type.fields);
  if (type.type === 'list') return [type['element-id'], ...typeIds(type.element)];
  return [type['key-id'], type['value-id'], ...typeIds(type.value)];
}

export function projectFields(
  schema: IcebergSchema,
  forkMap: ForkMap,
  options: { since?: ReadonlyMap<number, number>; includeMetadata?: boolean } = {},
): ProjectedField[] {
  const since = options.since;
  const fields: ProjectedField[] = [];
  if (options.includeMetadata ?? true) {
    for (const field of schema.fields) {
      if (!isMetadataFieldId(field.id)) continue;
      fields.push({
        field_id: field.id,
        path: [field.name],
        name: field.name,
        type: typeText(field.type),
        required: field.required,
        since_schema_id: since?.get(field.id) ?? 0,
        fork_version: 1,
        state: 'current',
        kind: 'metadata',
        doc: field.doc ?? null,
      });
    }
  }
  renderStruct(buildIndex(schema, forkMap), fields, since);
  return fields;
}

function renderStruct(struct: StructIndex, out: ProjectedField[], since: ReadonlyMap<number, number> | undefined): void {
  for (const family of struct.families.values()) {
    for (const item of family.members) {
      out.push(memberEntry(item, since));
      if (item.struct) renderStruct(item.struct, out, since);
      else if (item.element) renderElement(item.element, out, since);
    }
  }
}

function renderElement(element: ElementIndex, out: ProjectedField[], since: ReadonlyMap<number, number> | undefined): void {
  if (element.struct) renderStruct(element.struct, out, since);
  else if (element.element) renderElement(element.element, out, since);
}

function memberEntry(item: Member, since: ReadonlyMap<number, number> | undefined): ProjectedField {
  const elementJson = item.element?.json ?? false;
  const container = typeof item.type !== 'string';
  const kind: FieldKind = item.json ? 'json' : container ? (elementJson ? 'json' : 'container') : 'value';
  const entry: ProjectedField = {
    field_id: item.fieldId,
    path: [...item.path],
    name: logicalName(item.logicalPath),
    type: elementJson ? 'list<json>' : typeText(item.type),
    required: item.required,
    fork_version: item.forkVersion,
    state: item.state,
    kind,
  };
  const first = since?.get(item.fieldId);
  if (first !== undefined) entry.since_schema_id = first;
  if (item.forkOf !== null) entry.fork_of = item.forkOf;
  return entry;
}
