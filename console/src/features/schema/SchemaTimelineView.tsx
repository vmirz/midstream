import { Braces, CircleSlash, GitFork, Inbox, Plus, TriangleAlert } from 'lucide-react';
import { useState, type ReactNode } from 'react';

import { FieldName, TypeChip } from './SchemaFieldTable';
import { buildTimeline, dayOf, formatSpan, type TimelineEntry, type TimelineGroup } from './timeline';
import { Button, EmptyState, Panel, PanelHeader, Skeleton } from '@/components/ui/primitives';
import { formatCount } from '@/lib/format';
import type { CpSchemaField, PartitionGranularity, SchemaTimeline, TimeSpan } from '@shared/types';

/** How precise a date read from the table's files can be: as wide as its partitions. */
const PRECISION: Record<PartitionGranularity, string> = {
  hour: 'to the hour or better: the table is partitioned by hour',
  day: 'to the day: the table is partitioned by day',
  month: 'as wide as its files, up to a month: the table is partitioned by month',
  none: 'as wide as its files: the table is not partitioned',
};

/**
 * The table's schema history by its data: on what UTC day each field first held a value, and last
 * did, newest first - whatever schema version holds the change, since versions follow the order the
 * runtime met each shape in, not the order the topic's records had them.
 */
export function SchemaTimelineView({
  fields,
  timeline,
  loading,
  error,
  onSelectField,
}: {
  /** The current schema's fields: they include every field the table has had. */
  fields: readonly CpSchemaField[];
  timeline: SchemaTimeline | null;
  loading: boolean;
  error: string | null;
  /** Show one field in the Fields view. */
  onSelectField: (name: string) => void;
}): ReactNode {
  const description = timeline
    ? `When each field first and last held a value, by the records’ Kafka timestamps, in UTC days as the table’s partitions are. Read from the table’s files, so a date is precise ${PRECISION[timeline.granularity]}.`
    : 'When each field first and last held a value, by the records’ Kafka timestamps, in UTC days.';
  let body: ReactNode;
  if (loading) {
    body = (
      <div className="space-y-2 p-5">
        <Skeleton className="h-8" />
        <Skeleton className="h-8" />
        <Skeleton className="h-8" />
      </div>
    );
  } else if (error || !timeline || !timeline.available) {
    body = (
      <EmptyState
        icon={<TriangleAlert className="size-6" />}
        title="No dates to show"
        description={`The console could not read when the table’s fields held values: ${error ?? timeline?.reason ?? 'the table could not be read'}.`}
      />
    );
  } else {
    const groups = buildTimeline(fields, timeline);
    body =
      groups.length === 0 ? (
        <EmptyState title="No values yet" description="Nothing committed to the table holds a value yet." />
      ) : (
        <ol className="divide-y divide-[var(--color-line)]">
          {groups.map((group) => (
            <TimelineDay key={group.day} group={group} onSelectField={onSelectField} />
          ))}
        </ol>
      );
  }
  return (
    <Panel className="overflow-hidden">
      <PanelHeader title="Timeline" description={description} />
      {body}
    </Panel>
  );
}

function TimelineDay({ group, onSelectField }: { group: TimelineGroup; onSelectField: (name: string) => void }): ReactNode {
  const [open, setOpen] = useState(false);
  const collapsed = group.initial && !open;
  return (
    <li className="px-5 py-3.5">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
        <span className="numeric text-[13px] font-medium text-[var(--color-ink)]">{formatSpan(group.span)}</span>
        {group.initial ? (
          <span className="text-[12px] text-[var(--color-ink-muted)]">
            · first records: {formatCount(group.entries.length)} {group.entries.length === 1 ? 'field' : 'fields'} held values
            from the start
          </span>
        ) : null}
        {group.initial ? (
          <Button variant="ghost" size="sm" className="ml-auto h-7" onClick={() => setOpen((current) => !current)} aria-expanded={open}>
            {open ? 'Hide' : 'Show'}
          </Button>
        ) : null}
      </div>
      {collapsed ? null : (
        <ul className="mt-2 space-y-1.5">
          {group.entries.map((entry) => (
            <TimelineRow key={`${entry.kind}:${entry.field.field_id}`} entry={entry} group={group.span} onSelectField={onSelectField} />
          ))}
        </ul>
      )}
    </li>
  );
}

const ICONS: Record<TimelineEntry['kind'], ReactNode> = {
  added: <Plus className="size-3.5" />,
  forked: <GitFork className="size-3.5" />,
  collapsed: <Braces className="size-3.5" />,
  stopped: <CircleSlash className="size-3.5" />,
  unmapped: <Inbox className="size-3.5" />,
};

const TONES: Record<TimelineEntry['kind'], string> = {
  added: 'var(--color-phase-running)',
  forked: 'var(--color-accent)',
  collapsed: 'var(--color-phase-paused)',
  stopped: 'var(--color-ink-faint)',
  unmapped: 'var(--color-phase-pending)',
};

function TimelineRow({
  entry,
  group,
  onSelectField,
}: {
  entry: TimelineEntry;
  group: TimeSpan;
  onSelectField: (name: string) => void;
}): ReactNode {
  // An entry its group's dates do not tell tightly - its files end sooner than another's - says its own.
  const own = dayOf(entry.span.to) !== dayOf(group.to);
  return (
    <li className="flex min-w-0 items-start gap-2.5 text-[12px]">
      <span className="mt-[3px] shrink-0" style={{ color: TONES[entry.kind] }} aria-hidden>
        {ICONS[entry.kind]}
      </span>
      <span className="flex min-w-0 flex-1 flex-wrap items-baseline gap-x-2 gap-y-0.5">
        <button
          type="button"
          className="min-w-0 max-w-full rounded-sm text-left hover:underline focus-visible:outline-2"
          onClick={() => onSelectField(entry.field.name)}
          title={`Show ${entry.field.name} in the fields`}
        >
          <FieldName name={entry.field.name} />
        </button>
        <span className="text-[var(--color-ink-muted)]">{describe(entry)}</span>
        {own ? <span className="numeric text-[11px] text-[var(--color-ink-faint)]">{formatSpan(entry.span)}</span> : null}
      </span>
    </li>
  );
}

function describe(entry: TimelineEntry): ReactNode {
  switch (entry.kind) {
    case 'added':
      return (
        <span className="inline-flex items-center gap-1.5">
          first value
          <TypeChip type={entry.field.type} json={entry.field.kind === 'json'} />
        </span>
      );
    case 'forked': {
      const previous = entry.previous;
      return (
        <span>
          changed type{previous ? ` from ${previous.field.type}` : ''} to {entry.field.type}
          {previous?.last ? ` · its ${previous.field.type} values run until ${formatSpan(previous.last)}` : ''}
        </span>
      );
    }
    case 'collapsed':
      return 'kept as JSON text from here: its values changed type too often, or its object grew too wide';
    case 'stopped':
      return 'last value';
    case 'unmapped':
      return `values first went to _unmapped${entry.values ? ` (${formatCount(entry.values)} in all)` : ''}: the schema had no field for them`;
  }
}
