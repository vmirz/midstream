import type { TimeRange } from '@shared/types';

/**
 * The time range a view asks for, as the user chose it.
 *
 * Kept as a choice — "Last 15 minutes", "Yesterday" — rather than as instants, because a choice is
 * what a link should carry and what a label should say. It becomes instants only when Run is
 * pressed, which pins the run to the window the user meant at that moment.
 *
 * Calendar presets follow the browser's time zone: "Today" is the reader's today. Weeks start on
 * Sunday, as Splunk's do.
 */
export type TimeUnit = 'minute' | 'hour' | 'day' | 'week';

export type CalendarPreset =
  | 'today'
  | 'week_to_date'
  | 'month_to_date'
  | 'year_to_date'
  | 'yesterday'
  | 'previous_week'
  | 'previous_month'
  | 'previous_year';

export type TimeRangeSpec =
  | { kind: 'all' }
  | { kind: 'last'; amount: number; unit: TimeUnit }
  | { kind: 'calendar'; preset: CalendarPreset }
  /** Whole local days, both ends inclusive, as `YYYY-MM-DD`. */
  | { kind: 'dates'; from: string | null; to: string | null }
  /** Instants in epoch milliseconds, `[from, to)`. */
  | { kind: 'between'; from: number | null; to: number | null };

export const ALL_TIME: TimeRangeSpec = { kind: 'all' };

export const LAST_PRESETS: TimeRangeSpec[] = [
  { kind: 'last', amount: 15, unit: 'minute' },
  { kind: 'last', amount: 60, unit: 'minute' },
  { kind: 'last', amount: 4, unit: 'hour' },
  { kind: 'last', amount: 24, unit: 'hour' },
  { kind: 'last', amount: 7, unit: 'day' },
  { kind: 'last', amount: 30, unit: 'day' },
];

export const CALENDAR_PRESETS: TimeRangeSpec[] = (
  [
    'today',
    'week_to_date',
    'month_to_date',
    'year_to_date',
    'yesterday',
    'previous_week',
    'previous_month',
    'previous_year',
  ] as const
).map((preset) => ({ kind: 'calendar', preset }));

const UNIT_MS: Record<TimeUnit, number> = {
  minute: 60_000,
  hour: 3_600_000,
  day: 86_400_000,
  week: 604_800_000,
};

const CALENDAR_LABELS: Record<CalendarPreset, string> = {
  today: 'Today',
  week_to_date: 'Week to date',
  month_to_date: 'Month to date',
  year_to_date: 'Year to date',
  yesterday: 'Yesterday',
  previous_week: 'Previous week',
  previous_month: 'Previous month',
  previous_year: 'Previous year',
};

function startOfDay(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

/** Calendar arithmetic, so a day across a daylight-saving change is still one day. */
function addDays(date: Date, days: number): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate() + days);
}

function startOfWeek(date: Date): Date {
  const day = startOfDay(date);
  return addDays(day, -day.getDay());
}

function parseLocalDate(text: string | null): Date | null {
  if (!text) return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (!match) return null;
  const date = new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  return Number.isNaN(date.getTime()) ? null : date;
}

/** The instants a range stands for at `now`. */
export function resolveRange(spec: TimeRangeSpec, now: number = Date.now()): TimeRange {
  const today = startOfDay(new Date(now));
  switch (spec.kind) {
    case 'all':
      return { from: null, to: null };
    case 'last':
      return { from: now - spec.amount * UNIT_MS[spec.unit], to: null };
    case 'dates': {
      const from = parseLocalDate(spec.from);
      const to = parseLocalDate(spec.to);
      return { from: from ? from.getTime() : null, to: to ? addDays(to, 1).getTime() : null };
    }
    case 'between':
      return { from: spec.from, to: spec.to };
    case 'calendar':
      switch (spec.preset) {
        case 'today':
          return { from: today.getTime(), to: null };
        case 'yesterday':
          return { from: addDays(today, -1).getTime(), to: today.getTime() };
        case 'week_to_date':
          return { from: startOfWeek(today).getTime(), to: null };
        case 'previous_week': {
          const week = startOfWeek(today);
          return { from: addDays(week, -7).getTime(), to: week.getTime() };
        }
        case 'month_to_date':
          return { from: new Date(today.getFullYear(), today.getMonth(), 1).getTime(), to: null };
        case 'previous_month':
          return {
            from: new Date(today.getFullYear(), today.getMonth() - 1, 1).getTime(),
            to: new Date(today.getFullYear(), today.getMonth(), 1).getTime(),
          };
        case 'year_to_date':
          return { from: new Date(today.getFullYear(), 0, 1).getTime(), to: null };
        case 'previous_year':
          return {
            from: new Date(today.getFullYear() - 1, 0, 1).getTime(),
            to: new Date(today.getFullYear(), 0, 1).getTime(),
          };
      }
  }
}

const DATE = new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
const DATE_TIME = new Intl.DateTimeFormat(undefined, {
  month: 'short',
  day: 'numeric',
  year: 'numeric',
  hour: 'numeric',
  minute: '2-digit',
});

/** What the picker's trigger says: the choice, in words. */
export function rangeLabel(spec: TimeRangeSpec): string {
  switch (spec.kind) {
    case 'all':
      return 'All time';
    case 'last':
      return `Last ${spec.amount} ${spec.unit}${spec.amount === 1 ? '' : 's'}`;
    case 'calendar':
      return CALENDAR_LABELS[spec.preset];
    case 'dates': {
      const from = parseLocalDate(spec.from);
      const to = parseLocalDate(spec.to);
      if (from && to) return `${DATE.format(from)} – ${DATE.format(to)}`;
      if (from) return `Since ${DATE.format(from)}`;
      if (to) return `Through ${DATE.format(to)}`;
      return 'All time';
    }
    case 'between':
      if (spec.from !== null && spec.to !== null) return `${DATE_TIME.format(spec.from)} – ${DATE_TIME.format(spec.to)}`;
      if (spec.from !== null) return `Since ${DATE_TIME.format(spec.from)}`;
      if (spec.to !== null) return `Before ${DATE_TIME.format(spec.to)}`;
      return 'All time';
  }
}

/** The instants a run covered, for the status line: `Oct 6, 2:15 PM → now`. */
export function describeInstants(range: TimeRange): string {
  if (range.from === null && range.to === null) return 'all time';
  const from = range.from === null ? 'the beginning' : DATE_TIME.format(range.from);
  const to = range.to === null ? 'now' : DATE_TIME.format(range.to);
  return `${from} → ${to}`;
}

const UNIT_CODE: Record<TimeUnit, string> = { minute: 'm', hour: 'h', day: 'd', week: 'w' };
const CODE_UNIT: Record<string, TimeUnit> = { m: 'minute', h: 'hour', d: 'day', w: 'week' };

/** A compact, readable form for the address bar: `last-15m`, `yesterday`, `dates:2026-10-01..2026-10-05`. */
export function encodeRange(spec: TimeRangeSpec): string {
  switch (spec.kind) {
    case 'all':
      return 'all';
    case 'last':
      return `last-${spec.amount}${UNIT_CODE[spec.unit]}`;
    case 'calendar':
      return spec.preset;
    case 'dates':
      return `dates:${spec.from ?? ''}..${spec.to ?? ''}`;
    case 'between':
      return `between:${spec.from ?? ''}..${spec.to ?? ''}`;
  }
}

/** The inverse of `encodeRange`; anything unrecognisable reads as All time. */
export function decodeRange(text: string | null): TimeRangeSpec {
  if (!text || text === 'all') return ALL_TIME;
  const last = /^last-(\d{1,6})([mhdw])$/.exec(text);
  if (last) {
    const amount = Number(last[1]);
    const unit = CODE_UNIT[last[2] as string];
    if (amount > 0 && unit) return { kind: 'last', amount, unit };
  }
  if (text in CALENDAR_LABELS) return { kind: 'calendar', preset: text as CalendarPreset };
  const dates = /^dates:(\d{4}-\d{2}-\d{2})?\.\.(\d{4}-\d{2}-\d{2})?$/.exec(text);
  if (dates) return { kind: 'dates', from: dates[1] ?? null, to: dates[2] ?? null };
  const between = /^between:(\d{1,15})?\.\.(\d{1,15})?$/.exec(text);
  if (between) {
    return {
      kind: 'between',
      from: between[1] ? Number(between[1]) : null,
      to: between[2] ? Number(between[2]) : null,
    };
  }
  return ALL_TIME;
}

export function sameRange(a: TimeRangeSpec, b: TimeRangeSpec): boolean {
  return encodeRange(a) === encodeRange(b);
}

/** `YYYY-MM-DD` for a local date input. */
export function localDateValue(epochMs: number): string {
  const date = new Date(epochMs);
  const pad = (value: number): string => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** `YYYY-MM-DDTHH:MM` for a local datetime input. */
export function localDateTimeValue(epochMs: number): string {
  const date = new Date(epochMs);
  const pad = (value: number): string => String(value).padStart(2, '0');
  return `${localDateValue(epochMs)}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}
