import { useQuery } from '@tanstack/react-query';
import { ChevronDown, ChevronRight, Clock } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import {
  ALL_TIME,
  CALENDAR_PRESETS,
  LAST_PRESETS,
  localDateTimeValue,
  localDateValue,
  rangeLabel,
  resolveRange,
  sameRange,
  type TimeRangeSpec,
  type TimeUnit,
} from './timeRange';
import { Popover } from '@/components/ui/Popover';
import { Button, Input, Select } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { formatCount, formatTimestamp, localZone } from '@/lib/format';
import { useApi } from '@/lib/domains';
import type { DataSummary, ScopeEstimate } from '@shared/types';

/**
 * Chooses the time range every run is bounded by, the way Splunk's time picker does.
 *
 * Presets cover the common questions in one click. Each is annotated with how many rows it covers of
 * the table on screen, read from its manifests, so the cost of a question is visible before it is
 * asked — a
 * narrower range reads fewer files and is faster, and the numbers say so. Choosing a range
 * does not run anything: Run does.
 */
export function TimeRangePicker({
  materializationId,
  table,
  value,
  onChange,
  summary,
}: {
  materializationId: string;
  /** The table whose rows the estimates count. */
  table: string;
  value: TimeRangeSpec;
  onChange: (spec: TimeRangeSpec) => void;
  summary: DataSummary | null;
}): ReactNode {
  const scoped = !sameRange(value, ALL_TIME);
  return (
    <Popover
      width={600}
      trigger={({ toggle, open, id }) => (
        <button
          type="button"
          onClick={toggle}
          aria-expanded={open}
          aria-controls={id}
          aria-label={`Time range: ${rangeLabel(value)}`}
          className={cn(
            'inline-flex h-8 shrink-0 items-center gap-2 rounded-[var(--radius-control)] border px-2.5 text-[12px] transition-colors',
            scoped
              ? 'border-[var(--color-accent)] bg-[var(--color-accent-soft)] text-[var(--color-ink)]'
              : 'border-[var(--color-line-strong)] bg-[var(--color-surface)] text-[var(--color-ink)] hover:bg-[var(--color-raised)]',
          )}
        >
          <Clock className="size-3.5 shrink-0 text-[var(--color-ink-muted)]" />
          <span className="font-medium whitespace-nowrap">{rangeLabel(value)}</span>
          <ChevronDown className="size-3.5 shrink-0 text-[var(--color-ink-faint)]" />
        </button>
      )}
    >
      {({ close }) => (
        <PickerBody
          materializationId={materializationId}
          table={table}
          value={value}
          summary={summary}
          onChoose={(spec) => {
            onChange(spec);
            close();
          }}
        />
      )}
    </Popover>
  );
}

function PickerBody({
  materializationId,
  table,
  value,
  summary,
  onChoose,
}: {
  materializationId: string;
  table: string;
  value: TimeRangeSpec;
  summary: DataSummary | null;
  onChoose: (spec: TimeRangeSpec) => void;
}): ReactNode {
  const api = useApi();
  const [open, setOpen] = useState<'relative' | 'dates' | 'datetimes' | null>(null);
  const presets = useMemo(() => [...CALENDAR_PRESETS, ...LAST_PRESETS, ALL_TIME], []);

  // Resolved once per opening, so the figures describe the ranges as they would run now.
  const estimates = useQuery({
    queryKey: ['range-estimates', materializationId, table, summary?.asOf ?? null],
    queryFn: () => {
      const now = Date.now();
      return api.dataEstimate(materializationId, table, presets.map((spec) => resolveRange(spec, now)));
    },
    staleTime: 30_000,
    retry: 0,
  });

  const estimateFor = (spec: TimeRangeSpec): ScopeEstimate | null => {
    const index = presets.findIndex((preset) => sameRange(preset, spec));
    return index >= 0 ? (estimates.data?.estimates[index] ?? null) : null;
  };

  const option = (spec: TimeRangeSpec): ReactNode => (
    <PresetOption
      key={rangeLabel(spec)}
      spec={spec}
      selected={sameRange(spec, value)}
      estimate={estimateFor(spec)}
      onChoose={onChoose}
    />
  );

  return (
    <div className="max-h-[min(640px,calc(100dvh-220px))] overflow-y-auto">
      <Section title="Presets" open>
        <div className="grid grid-cols-[1.15fr_1fr_0.85fr] gap-x-4 px-3 pb-3">
          <PresetColumn label="Relative">{CALENDAR_PRESETS.map(option)}</PresetColumn>
          <PresetColumn label="Last">{LAST_PRESETS.map(option)}</PresetColumn>
          <PresetColumn label="Other">{option(ALL_TIME)}</PresetColumn>
        </div>
      </Section>

      <Section
        title="Relative"
        open={open === 'relative'}
        onToggle={() => setOpen((current) => (current === 'relative' ? null : 'relative'))}
      >
        <RelativeForm value={value} onChoose={onChoose} />
      </Section>
      <Section
        title="Date range"
        open={open === 'dates'}
        onToggle={() => setOpen((current) => (current === 'dates' ? null : 'dates'))}
      >
        <DateRangeForm value={value} summary={summary} onChoose={onChoose} />
      </Section>
      <Section
        title="Date & time range"
        open={open === 'datetimes'}
        onToggle={() => setOpen((current) => (current === 'datetimes' ? null : 'datetimes'))}
      >
        <DateTimeRangeForm value={value} summary={summary} onChoose={onChoose} />
      </Section>

      <SpanNote summary={summary} />
    </div>
  );
}

function Section({
  title,
  open,
  onToggle,
  children,
}: {
  title: string;
  open: boolean;
  onToggle?: () => void;
  children: ReactNode;
}): ReactNode {
  return (
    <section className="border-b border-[var(--color-line)] last:border-b-0">
      <button
        type="button"
        onClick={onToggle}
        disabled={!onToggle}
        aria-expanded={open}
        className="flex w-full items-center gap-1.5 px-3 py-2 text-left text-[12px] font-medium text-[var(--color-ink)] enabled:hover:bg-[var(--color-raised)]"
      >
        {open ? (
          <ChevronDown className="size-3.5 text-[var(--color-ink-faint)]" />
        ) : (
          <ChevronRight className="size-3.5 text-[var(--color-ink-faint)]" />
        )}
        {title}
      </button>
      {open ? children : null}
    </section>
  );
}

function PresetColumn({ label, children }: { label: string; children: ReactNode }): ReactNode {
  return (
    <div>
      <p className="px-1.5 pb-1 text-[10px] font-semibold tracking-wide text-[var(--color-ink-faint)] uppercase">
        {label}
      </p>
      <ul className="space-y-px">{children}</ul>
    </div>
  );
}

function PresetOption({
  spec,
  selected,
  estimate,
  onChoose,
}: {
  spec: TimeRangeSpec;
  selected: boolean;
  estimate: ScopeEstimate | null;
  onChoose: (spec: TimeRangeSpec) => void;
}): ReactNode {
  return (
    <li>
      <button
        type="button"
        onClick={() => onChoose(spec)}
        className={cn(
          'flex w-full items-baseline justify-between gap-2 rounded-[var(--radius-control)] px-1.5 py-1 text-left text-[12px] transition-colors',
          selected
            ? 'bg-[var(--color-accent-soft)] font-medium text-[var(--color-accent)]'
            : 'text-[var(--color-accent)] hover:bg-[var(--color-raised)]',
        )}
      >
        <span className="whitespace-nowrap">{rangeLabel(spec)}</span>
        {estimate ? (
          <span
            className="numeric shrink-0 text-[10px] text-[var(--color-ink-faint)]"
            title={
              estimate.rowsExact
                ? `${estimate.rows.toLocaleString()} rows in ${estimate.objects.toLocaleString()} files`
                : `At most ${estimate.rows.toLocaleString()} rows: ${estimate.objects.toLocaleString()} files overlap this range`
            }
          >
            {estimate.objects === 0 ? 'no data' : `${estimate.rowsExact ? '' : '≤'}${formatCount(estimate.rows)}`}
          </span>
        ) : null}
      </button>
    </li>
  );
}

const UNITS: TimeUnit[] = ['minute', 'hour', 'day', 'week'];

function RelativeForm({
  value,
  onChoose,
}: {
  value: TimeRangeSpec;
  onChoose: (spec: TimeRangeSpec) => void;
}): ReactNode {
  const [amount, setAmount] = useState(value.kind === 'last' ? String(value.amount) : '2');
  const [unit, setUnit] = useState<TimeUnit>(value.kind === 'last' ? value.unit : 'hour');
  const parsed = Number.parseInt(amount, 10);
  const valid = Number.isFinite(parsed) && parsed > 0 && parsed <= 999_999;

  return (
    <form
      className="flex items-center gap-2 px-3 pb-3"
      onSubmit={(event) => {
        event.preventDefault();
        if (valid) onChoose({ kind: 'last', amount: parsed, unit });
      }}
    >
      <span className="text-[12px] text-[var(--color-ink-muted)]">Last</span>
      <Input
        value={amount}
        onChange={(event) => setAmount(event.target.value)}
        inputMode="numeric"
        aria-label="Amount"
        className="numeric h-8 w-20 text-[12px]"
      />
      <Select
        value={unit}
        onChange={(event) => setUnit(event.target.value as TimeUnit)}
        aria-label="Unit"
        className="h-8 w-28 text-[12px]"
      >
        {UNITS.map((entry) => (
          <option key={entry} value={entry}>
            {entry}s
          </option>
        ))}
      </Select>
      <Button type="submit" size="sm" variant="secondary" disabled={!valid} className="ml-auto">
        Apply
      </Button>
    </form>
  );
}

function DateRangeForm({
  value,
  summary,
  onChoose,
}: {
  value: TimeRangeSpec;
  summary: DataSummary | null;
  onChoose: (spec: TimeRangeSpec) => void;
}): ReactNode {
  const latest = summary?.timeMax ? Date.parse(summary.timeMax) : Date.now();
  const [from, setFrom] = useState(value.kind === 'dates' ? (value.from ?? '') : localDateValue(latest - 6 * 86_400_000));
  const [to, setTo] = useState(value.kind === 'dates' ? (value.to ?? '') : localDateValue(latest));
  const valid = (from !== '' || to !== '') && (from === '' || to === '' || from <= to);

  return (
    <form
      className="flex flex-wrap items-center gap-2 px-3 pb-3"
      onSubmit={(event) => {
        event.preventDefault();
        if (valid) onChoose({ kind: 'dates', from: from || null, to: to || null });
      }}
    >
      <span className="text-[12px] text-[var(--color-ink-muted)]">Between</span>
      <Input type="date" value={from} onChange={(event) => setFrom(event.target.value)} aria-label="From date" className="numeric h-8 w-[150px] text-[12px]" />
      <span className="text-[12px] text-[var(--color-ink-muted)]">and</span>
      <Input type="date" value={to} onChange={(event) => setTo(event.target.value)} aria-label="To date" className="numeric h-8 w-[150px] text-[12px]" />
      <Button type="submit" size="sm" variant="secondary" disabled={!valid} className="ml-auto">
        Apply
      </Button>
      <p className="w-full text-[11px] text-[var(--color-ink-faint)]">Whole days in {localZone().name}, both ends included.</p>
    </form>
  );
}

function DateTimeRangeForm({
  value,
  summary,
  onChoose,
}: {
  value: TimeRangeSpec;
  summary: DataSummary | null;
  onChoose: (spec: TimeRangeSpec) => void;
}): ReactNode {
  const latest = summary?.timeMax ? Date.parse(summary.timeMax) : Date.now();
  const [from, setFrom] = useState(
    value.kind === 'between' && value.from !== null ? localDateTimeValue(value.from) : localDateTimeValue(latest - 3_600_000),
  );
  const [to, setTo] = useState(
    value.kind === 'between' && value.to !== null ? localDateTimeValue(value.to) : localDateTimeValue(latest),
  );
  const fromMs = from ? new Date(from).getTime() : null;
  const toMs = to ? new Date(to).getTime() : null;
  const valid =
    (fromMs !== null || toMs !== null) &&
    (fromMs === null || Number.isFinite(fromMs)) &&
    (toMs === null || Number.isFinite(toMs)) &&
    (fromMs === null || toMs === null || fromMs < toMs);

  return (
    <form
      className="flex flex-wrap items-center gap-2 px-3 pb-3"
      onSubmit={(event) => {
        event.preventDefault();
        if (valid) onChoose({ kind: 'between', from: fromMs, to: toMs });
      }}
    >
      <span className="text-[12px] text-[var(--color-ink-muted)]">Between</span>
      <Input type="datetime-local" value={from} onChange={(event) => setFrom(event.target.value)} aria-label="From" className="numeric h-8 w-[196px] text-[12px]" />
      <span className="text-[12px] text-[var(--color-ink-muted)]">and</span>
      <Input type="datetime-local" value={to} onChange={(event) => setTo(event.target.value)} aria-label="To" className="numeric h-8 w-[196px] text-[12px]" />
      <Button type="submit" size="sm" variant="secondary" disabled={!valid} className="ml-auto">
        Apply
      </Button>
      <p className="w-full text-[11px] text-[var(--color-ink-faint)]">Times in {localZone().name}. The end is exclusive.</p>
    </form>
  );
}

/** Where the data sits on the time axis, so an empty range is explained rather than mysterious. */
function SpanNote({ summary }: { summary: DataSummary | null }): ReactNode {
  if (!summary) return null;
  return (
    <div className="space-y-0.5 border-t border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2 text-[11px] text-[var(--color-ink-muted)]">
      <p>
        Ranges filter on each record&apos;s Kafka timestamp, in your time zone ({localZone().name}).
        {summary.timeMin && summary.timeMax ? (
          <>
            {' '}
            Records span <span className="numeric">{formatTimestamp(summary.timeMin)}</span> to{' '}
            <span className="numeric">{formatTimestamp(summary.timeMax)}</span>.
          </>
        ) : null}
      </p>
      {summary.untimedRows > 0 ? (
        <p className="text-[var(--color-phase-paused)]">
          {summary.untimedRows.toLocaleString()} rows were written without a Kafka timestamp and only
          appear under All time.
        </p>
      ) : null}
    </div>
  );
}
