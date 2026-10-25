import { useQuery } from '@tanstack/react-query';
import { ArrowUpRight, Braces, GitFork, Minus, Plus, Replace, ShieldCheck } from 'lucide-react';
import { useState, type ReactNode } from 'react';

import { FieldName, TypeChip } from './SchemaFieldTable';
import { EmptyState, Panel, PanelHeader, SegmentedControl, Skeleton } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { api } from '@/lib/api';
import { humanize } from '@/lib/format';
import type { SchemaChangeKind, SchemaDiffEntry } from '@shared/types';

const KIND_STYLE: Record<SchemaChangeKind, { icon: ReactNode; label: string; accent: string; hint: string }> = {
  added: {
    icon: <Plus className="size-3" />,
    label: 'Added',
    accent: 'var(--color-phase-running)',
    hint: 'A new field, for a key the topic started sending.',
  },
  forked: {
    icon: <GitFork className="size-3" />,
    label: 'Forked',
    accent: 'var(--color-accent)',
    hint: 'A new field for an existing key, whose values changed to a type the old field cannot hold. Both stay readable as one column.',
  },
  collapsed: {
    icon: <Braces className="size-3" />,
    label: 'Collapsed',
    accent: 'var(--color-phase-paused)',
    hint: 'From here on the key’s values are kept as JSON text: its type changed too often, or its object grew too wide.',
  },
  type_widened: {
    icon: <ArrowUpRight className="size-3" />,
    label: 'Widened',
    accent: 'var(--color-phase-pending)',
    hint: 'The field’s type was promoted in place, which readers handle without a fork.',
  },
  required_changed: {
    icon: <ShieldCheck className="size-3" />,
    label: 'Required changed',
    accent: 'var(--color-phase-pending)',
    hint: 'The field became optional or required.',
  },
  superseded: {
    icon: <Replace className="size-3" />,
    label: 'Superseded',
    accent: 'var(--color-ink-muted)',
    hint: 'A newer fork took the key’s values over; this field keeps the rows written before.',
  },
  removed: {
    icon: <Minus className="size-3" />,
    label: 'Removed',
    accent: 'var(--color-phase-failed)',
    hint: 'The field is gone from the schema. The materializer never removes one; another engine did.',
  },
  unchanged: {
    icon: null,
    label: 'Unchanged',
    accent: 'var(--color-ink-faint)',
    hint: '',
  },
};

const SUMMARY_ORDER: SchemaChangeKind[] = ['added', 'forked', 'collapsed', 'type_widened', 'required_changed', 'superseded', 'removed'];

function renderValue(value: unknown): string {
  if (value === null || value === undefined) return '—';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  return String(value);
}

export function SchemaDiffView({
  materializationId,
  table,
  from,
  to,
}: {
  materializationId: string;
  /** The table whose schema versions are compared. */
  table: string;
  from: number | null;
  to: number | null;
}): ReactNode {
  const [showUnchanged, setShowUnchanged] = useState<'changed' | 'all'>('changed');

  const diffQuery = useQuery({
    queryKey: ['schema-diff', materializationId, table, from, to],
    queryFn: () => api.schemaDiff(materializationId, table, from, to),
  });

  if (diffQuery.isPending) {
    return (
      <Panel className="space-y-2 p-5">
        <Skeleton className="h-8" />
        <Skeleton className="h-8" />
        <Skeleton className="h-8" />
      </Panel>
    );
  }

  if (diffQuery.isError || !diffQuery.data) {
    return (
      <Panel>
        <EmptyState
          title="Comparison unavailable"
          description={diffQuery.error instanceof Error ? diffQuery.error.message : 'One of these schemas could not be read.'}
        />
      </Panel>
    );
  }

  const diff = diffQuery.data;
  const entries = diff.entries.filter((entry) => showUnchanged === 'all' || entry.kind !== 'unchanged');

  return (
    <Panel className="overflow-hidden">
      <PanelHeader
        title={diff.from === null ? `Schema ${diff.to ?? 0}, as first created` : `Schema ${diff.from} → ${diff.to}`}
        description={
          <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
            {SUMMARY_ORDER.map((kind) => (
              <Summary key={kind} kind={kind} count={diff.summary[kind] ?? 0} />
            ))}
            <span className="text-[var(--color-ink-faint)]">{diff.summary.unchanged} unchanged</span>
          </span>
        }
        actions={
          <SegmentedControl
            value={showUnchanged}
            onChange={setShowUnchanged}
            options={[
              { value: 'changed', label: 'Changes' },
              { value: 'all', label: 'All' },
            ]}
          />
        }
      />

      {entries.length === 0 ? (
        <EmptyState title="No differences" description="These two schemas have the same fields." />
      ) : (
        <ul className="divide-y divide-[var(--color-line)]">
          {entries.map((entry) => (
            <DiffRow key={entry.field_id} entry={entry} />
          ))}
        </ul>
      )}
    </Panel>
  );
}

function Summary({ kind, count }: { kind: SchemaChangeKind; count: number }): ReactNode {
  if (count === 0) return null;
  return (
    <span className="inline-flex items-center gap-1" style={{ color: KIND_STYLE[kind].accent }} title={KIND_STYLE[kind].hint}>
      {KIND_STYLE[kind].icon}
      {count} {KIND_STYLE[kind].label.toLowerCase()}
    </span>
  );
}

function DiffRow({ entry }: { entry: SchemaDiffEntry }): ReactNode {
  const style = KIND_STYLE[entry.kind];
  const field = entry.after ?? entry.before;
  const physical = entry.path.join('.').replace(/\.\[\]/g, '[]');

  return (
    <li className="flex items-start gap-3 px-5 py-3">
      <span
        className="mt-0.5 grid size-5 shrink-0 place-items-center rounded-md"
        style={{
          color: style.accent,
          backgroundColor: `color-mix(in srgb, ${style.accent} 14%, transparent)`,
        }}
        title={style.hint || style.label}
      >
        {style.icon}
      </span>

      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <FieldName name={entry.name} />
          {physical !== entry.name ? <span className="numeric text-[11px] text-[var(--color-ink-faint)]">{physical}</span> : null}
          {field ? <TypeChip type={field.type} json={field.kind === 'json'} /> : null}
          <span className="numeric text-[11px] text-[var(--color-ink-faint)]" title="Iceberg field id">
            id {entry.field_id}
          </span>
        </div>

        {entry.changes.length > 0 ? (
          <ul className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1">
            {entry.changes.map((change) => (
              <li key={change.attribute} className="text-[12px] text-[var(--color-ink-muted)]">
                {humanize(change.attribute)}:{' '}
                <span className="numeric text-[var(--color-phase-failed)] line-through">{renderValue(change.from)}</span>{' '}
                <span className="numeric text-[var(--color-phase-running)]">{renderValue(change.to)}</span>
              </li>
            ))}
          </ul>
        ) : null}
      </div>

      <span className={cn('shrink-0 text-[11px] font-medium')} style={{ color: style.accent }} title={style.hint || undefined}>
        {style.label}
      </span>
    </li>
  );
}
