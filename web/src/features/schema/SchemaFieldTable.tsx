import type { ReactNode } from 'react';

import { formatSpan, fromTheStart, stoppedArriving } from './timeline';
import { Badge } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import type { CpSchemaField, SchemaTimeline } from '@shared/types';

/** Iceberg types by family, so a column of them reads at a glance. */
function typeTone(type: string): string {
  if (type === 'string') return 'var(--color-phase-pending)';
  if (['int', 'long', 'float', 'double'].includes(type) || type.startsWith('decimal')) return 'var(--color-phase-running)';
  if (type === 'boolean') return 'var(--color-phase-paused)';
  if (type.startsWith('timestamp') || type === 'date' || type === 'time') return 'var(--color-accent)';
  return 'var(--color-ink-faint)';
}

const GRID = 'grid grid-cols-[minmax(0,1fr)_150px_84px_120px_64px] items-center gap-4';

/** A field's name, its leading segments dimmed so deep paths stay scannable. */
export function FieldName({ name }: { name: string }): ReactNode {
  const head = name.slice(0, name.lastIndexOf('.') + 1);
  return (
    <span className="numeric truncate text-[12px]">
      {head ? <span className="text-[var(--color-ink-faint)]">{head}</span> : null}
      <span className="text-[var(--color-ink)]">{name.slice(head.length)}</span>
    </span>
  );
}

export function TypeChip({ type, json = false }: { type: string | null; json?: boolean }): ReactNode {
  const text = type ?? 'unknown';
  return (
    <span className="numeric inline-flex min-w-0 items-center gap-1.5 text-[12px] text-[var(--color-ink-muted)]" title={text}>
      <span className="size-1.5 shrink-0 rounded-full" style={{ backgroundColor: json ? 'var(--color-ink-faint)' : typeTone(text) }} />
      <span className="truncate">{json && text === 'string' ? 'json' : text}</span>
    </span>
  );
}

/** What a field is now: written to, replaced by a newer fork, or collapsed to JSON text. */
export function FieldState({ field }: { field: CpSchemaField }): ReactNode {
  if (field.kind === 'metadata') return <Badge>Kafka</Badge>;
  if (field.state === 'superseded') return <Badge>superseded</Badge>;
  if (field.kind === 'json') return <Badge tone="paused">JSON</Badge>;
  if (field.fork_version > 1) return <Badge tone="accent">fork v{field.fork_version}</Badge>;
  return <span className="text-[12px] text-[var(--color-ink-faint)]">—</span>;
}

/** The physical path, when it is not simply the field's name: a fork, a collapse, a renamed key. */
function physicalNote(field: CpSchemaField): string | null {
  const physical = field.path.join('.').replace(/\.\[\]/g, '[]');
  return physical === field.name ? null : physical;
}

/**
 * When a field held values, as a row says it: when it first did, unless it was there from the table's
 * first records, and when it last did, once it has stopped - by the records' Kafka timestamps, in UTC.
 */
export function fieldDates(field: CpSchemaField, timeline: SchemaTimeline | null): string | null {
  const times = timeline?.fields[String(field.field_id)];
  if (!timeline || !times || field.kind === 'metadata') return null;
  const parts: string[] = [];
  if (times.first && !fromTheStart(times, timeline)) parts.push(`first seen ${formatSpan(times.first)}`);
  if (times.last && (field.state === 'superseded' || stoppedArriving(times, timeline.dataThrough))) {
    parts.push(`last seen ${formatSpan(times.last)}`);
  }
  return parts.length > 0 ? parts.join(' · ') : null;
}

export function SchemaFieldTable({ fields, timeline = null }: { fields: CpSchemaField[]; timeline?: SchemaTimeline | null }): ReactNode {
  return (
    <div className="overflow-x-auto">
      <div
        className={cn(
          GRID,
          'min-w-[640px] border-b border-[var(--color-line)] bg-[var(--color-raised)] px-5 py-2',
          'text-[11px] font-medium tracking-wide text-[var(--color-ink-muted)] uppercase',
        )}
      >
        <span>Field</span>
        <span>Type</span>
        <span>Required</span>
        <span>State</span>
        <span className="text-right" title="Iceberg field id">
          Id
        </span>
      </div>
      <ul className="min-w-[640px] divide-y divide-[var(--color-line)]">
        {fields.map((field) => {
          const note = physicalNote(field);
          const dates = fieldDates(field, timeline);
          const parts = [
            note ? `written to ${note}` : null,
            (field.since_schema_id ?? 0) > 0 ? `since schema ${field.since_schema_id}` : null,
            dates,
          ].filter((part): part is string => part !== null);
          return (
            <li key={field.field_id} className={cn(GRID, 'px-5 py-2.5', field.state === 'superseded' && 'opacity-55')}>
              <span className="flex min-w-0 flex-col">
                <FieldName name={field.name} />
                {parts.length > 0 ? (
                  <span className="numeric truncate text-[11px] text-[var(--color-ink-faint)]" title={`${parts.join(' · ')}${dates ? ' (UTC)' : ''}`}>
                    {parts.join(' · ')}
                  </span>
                ) : null}
              </span>
              <TypeChip type={field.type} json={field.kind === 'json'} />
              <span className="text-[12px] text-[var(--color-ink-muted)]">{field.required ? 'required' : 'optional'}</span>
              <span>
                <FieldState field={field} />
              </span>
              <span className="numeric text-right text-[11px] text-[var(--color-ink-faint)]">{field.field_id}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
