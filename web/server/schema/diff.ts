/**
 * What changed between two schema versions, field by field.
 *
 * Fields are matched by Iceberg field id, which never changes for the life of a field: a rename
 * would keep it, a fork is a new field with an id of its own, and nothing is ever dropped. So a
 * field only `after` has is new — `added`, a new fork of an existing path (`forked`), or the JSON
 * field a family collapsed into (`collapsed`) — and a field both have can only have changed state
 * (`superseded`, `collapsed`), type (an Iceberg promotion) or nullability.
 */

import type { CpSchema, CpSchemaField, SchemaChangeKind, SchemaDiff, SchemaDiffEntry, SchemaFieldChange } from '../../shared/types.ts';

const COMPARED_ATTRIBUTES = ['type', 'required', 'state', 'kind'] as const satisfies readonly (keyof CpSchemaField)[];

const RANK: Record<SchemaChangeKind, number> = {
  added: 0,
  forked: 1,
  collapsed: 2,
  type_widened: 3,
  required_changed: 4,
  superseded: 5,
  removed: 6,
  unchanged: 7,
};

function newFieldKind(field: CpSchemaField): SchemaChangeKind {
  if (field.kind === 'json' && field.state === 'collapsed') return 'collapsed';
  if (field.fork_of != null || (field.fork_version ?? 1) >= 2) return 'forked';
  return 'added';
}

function changedKind(changes: SchemaFieldChange[], after: CpSchemaField): SchemaChangeKind {
  const changed = new Set(changes.map((change) => change.attribute));
  if (changed.has('state') && after.state === 'collapsed') return 'collapsed';
  if (changed.has('type')) return 'type_widened';
  if (changed.has('required')) return 'required_changed';
  if (changed.has('state') && after.state === 'superseded') return 'superseded';
  return changes.length > 0 ? 'type_widened' : 'unchanged';
}

export function diffSchemas(materializationId: string, before: CpSchema | null, after: CpSchema | null): SchemaDiff {
  const payload = (schema: CpSchema | null): CpSchemaField[] => (schema?.fields ?? []).filter((field) => field.kind !== 'metadata');
  const beforeFields = new Map(payload(before).map((field) => [field.field_id, field]));
  const afterFields = new Map(payload(after).map((field) => [field.field_id, field]));
  const ids = [...new Set([...beforeFields.keys(), ...afterFields.keys()])];

  const entries: SchemaDiffEntry[] = ids.map((id) => {
    const from = beforeFields.get(id) ?? null;
    const to = afterFields.get(id) ?? null;
    if (from === null && to !== null) {
      return { field_id: id, name: to.name, path: to.path, kind: newFieldKind(to), before: null, after: to, changes: [] };
    }
    if (from !== null && to === null) {
      return { field_id: id, name: from.name, path: from.path, kind: 'removed', before: from, after: null, changes: [] };
    }
    const left = from as CpSchemaField;
    const right = to as CpSchemaField;
    const changes: SchemaFieldChange[] = COMPARED_ATTRIBUTES.filter((attribute) => left[attribute] !== right[attribute]).map(
      (attribute) => ({ attribute, from: left[attribute], to: right[attribute] }),
    );
    return { field_id: id, name: right.name, path: right.path, kind: changedKind(changes, right), before: left, after: right, changes };
  });

  entries.sort((a, b) => RANK[a.kind] - RANK[b.kind] || a.name.localeCompare(b.name) || a.field_id - b.field_id);

  const summary = Object.fromEntries(Object.keys(RANK).map((kind) => [kind, 0])) as Record<SchemaChangeKind, number>;
  for (const entry of entries) summary[entry.kind] += 1;
  return {
    materialization_id: materializationId,
    from: before?.schema_id ?? null,
    to: after?.schema_id ?? null,
    summary,
    entries,
  };
}
