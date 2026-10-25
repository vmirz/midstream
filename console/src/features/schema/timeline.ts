/**
 * When a table's schema changed, by its data: no React, so it is unit-tested.
 *
 * A schema version is numbered in the order the runtime met each shape, which is not the order the
 * shapes appeared in the topic: a backfill reads partitions at their own pace, and schema 0 already
 * holds what discovery sampled from the topic's newest records beside its oldest. So a change is
 * dated by the records themselves - each field's first and last value, by Kafka timestamp, which the
 * console reads from the table's manifests (`GET …/schema/timeline`).
 *
 * Dates are UTC, the zone the table's partitions are cut in: a day-partitioned table's file holds one
 * UTC day, so a date read from it is a single day in UTC where in most other zones it would straddle
 * two.
 */

import type { CpSchemaField, FieldTimes, SchemaTimeline, TimeSpan } from '@shared/types';

const DAY_MS = 86_400_000;

/** The zone dates from the data are told in: the table's partitions'. */
export const DATA_ZONE = 'UTC';

export interface DateOptions {
  /** `DATA_ZONE` by default. */
  timeZone?: string;
  /** The reader's by default. */
  locale?: string;
}

/**
 * The UTC dates a span covers, as tight as its bounds: `Apr 15, 2025` for one day, `Apr 3 – 28, 2025`,
 * `Mar 30 – Apr 2, 2025`. A span is as wide as the table's files are: a day in a day-partitioned table.
 */
export function formatSpan(span: TimeSpan, options: DateOptions = {}): string {
  const format = new Intl.DateTimeFormat(options.locale, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    timeZone: options.timeZone ?? DATA_ZONE,
  });
  return format.formatRange(Date.parse(span.from), Date.parse(span.to));
}

/** The UTC day an instant falls on, `2025-04-15`: how changes are grouped. */
export function dayOf(iso: string, options: DateOptions = {}): string {
  return new Intl.DateTimeFormat('en-CA', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    timeZone: options.timeZone ?? DATA_ZONE,
  }).format(Date.parse(iso));
}

/** Whether a field's last value came a day or more before the table's last record: it stopped arriving. */
export function stoppedArriving(times: FieldTimes | undefined, dataThrough: string | null): boolean {
  if (!times?.last || !dataThrough) return false;
  return Date.parse(times.last.to) < Date.parse(dataThrough) - DAY_MS;
}

/** Whether a field held a value in the table's first records: it was there from the start. */
export function fromTheStart(times: FieldTimes | undefined, timeline: SchemaTimeline, options: DateOptions = {}): boolean {
  if (!times?.first || !timeline.dataFrom) return false;
  return dayOf(times.first.from, options) === dayOf(timeline.dataFrom, options);
}

/**
 * When the data first needed each schema version: the earliest first value among the fields the
 * version added, forked or collapsed. Absent for a version none of whose fields hold a value yet.
 */
export function versionDataSpans(fields: readonly CpSchemaField[], times: Record<string, FieldTimes>): Map<number, TimeSpan> {
  const spans = new Map<number, TimeSpan>();
  for (const field of fields) {
    if (field.kind === 'metadata' || field.since_schema_id === null || field.since_schema_id === undefined) continue;
    const first = times[String(field.field_id)]?.first;
    if (!first) continue;
    const current = spans.get(field.since_schema_id);
    spans.set(field.since_schema_id, current ? earlier(current, first) : first);
  }
  return spans;
}

function earlier(a: TimeSpan, b: TimeSpan): TimeSpan {
  return {
    from: Date.parse(a.from) <= Date.parse(b.from) ? a.from : b.from,
    to: Date.parse(a.to) <= Date.parse(b.to) ? a.to : b.to,
  };
}

export type TimelineKind = 'forked' | 'collapsed' | 'added' | 'stopped' | 'unmapped';

export interface TimelineEntry {
  kind: TimelineKind;
  field: CpSchemaField;
  /** When it happened: the first value of the field, or for `stopped` its last. */
  span: TimeSpan;
  /** A fork's: the member it took over from, and when that one last held a value. */
  previous?: { field: CpSchemaField; last: TimeSpan | null };
  /** `unmapped`: how many values went there in all. */
  values?: number | null;
}

export interface TimelineGroup {
  /** The UTC day, `2025-04-15`, its entries' spans start on. */
  day: string;
  /** What its entries' spans cover together: a day in a day-partitioned table, up to a month in a month's. */
  span: TimeSpan;
  /** The table's first records: what every later change is measured against. */
  initial: boolean;
  entries: TimelineEntry[];
}

const KIND_ORDER: Record<TimelineKind, number> = { forked: 0, collapsed: 1, added: 2, stopped: 3, unmapped: 4 };

/**
 * The table's schema history by its data: for each day a field first held a value, or last did,
 * what changed - newest first. A field that arrived inside a struct or list added that same day is
 * folded into it, so a new object is one change rather than one per key. The first records' fields
 * are one group, marked `initial`. A group is dated by the span its entries cover: a change read
 * from a month's file may lie anywhere in it, so the group starting on its first day is not dated
 * by that day alone.
 */
export function buildTimeline(fields: readonly CpSchemaField[], timeline: SchemaTimeline, options: DateOptions = {}): TimelineGroup[] {
  const entries: TimelineEntry[] = [];
  const payload = fields.filter((field) => field.kind !== 'metadata');
  for (const field of payload) {
    const times = timeline.fields[String(field.field_id)];
    if (times?.first) {
      const kind: TimelineKind = field.kind === 'json' ? 'collapsed' : field.fork_version > 1 ? 'forked' : 'added';
      const entry: TimelineEntry = { kind, field, span: times.first };
      if (kind === 'forked') {
        const previous = payload.find((other) => other.name === field.name && other.fork_version === field.fork_version - 1);
        if (previous) entry.previous = { field: previous, last: timeline.fields[String(previous.field_id)]?.last ?? null };
      }
      entries.push(entry);
    }
    // A superseded member's last value is part of its fork's entry; a field still current says when it stopped.
    if (field.state !== 'superseded' && times?.last && stoppedArriving(times, timeline.dataThrough)) {
      entries.push({ kind: 'stopped', field, span: times.last });
    }
  }
  const unmapped = fields.find((field) => field.kind === 'metadata' && field.name === '_unmapped');
  const unmappedTimes = unmapped ? timeline.fields[String(unmapped.field_id)] : undefined;
  if (unmapped && unmappedTimes?.first) {
    entries.push({ kind: 'unmapped', field: unmapped, span: unmappedTimes.first, values: unmappedTimes.values });
  }

  const firstDay = timeline.dataFrom ? dayOf(timeline.dataFrom, options) : null;
  const byDay = new Map<string, TimelineEntry[]>();
  for (const entry of entries) {
    const day = dayOf(entry.span.from, options);
    const group = byDay.get(day) ?? [];
    group.push(entry);
    byDay.set(day, group);
  }
  return [...byDay.entries()]
    .map(([day, group]) => ({
      day,
      span: covering(group.map((entry) => entry.span)),
      initial: day === firstDay,
      entries: folded(group).sort(
        (a, b) => KIND_ORDER[a.kind] - KIND_ORDER[b.kind] || (a.field.name < b.field.name ? -1 : a.field.name > b.field.name ? 1 : 0),
      ),
    }))
    .sort((a, b) => (a.day < b.day ? 1 : a.day > b.day ? -1 : 0));
}

/** The span from the earliest start to the latest end of several. */
function covering(spans: TimeSpan[]): TimeSpan {
  return spans.reduce((result, span) => ({
    from: Date.parse(span.from) < Date.parse(result.from) ? span.from : result.from,
    to: Date.parse(span.to) > Date.parse(result.to) ? span.to : result.to,
  }));
}

/** One day's entries without those an entry of an enclosing struct or list already says. */
function folded(entries: TimelineEntry[]): TimelineEntry[] {
  return entries.filter(
    (entry) =>
      !entries.some(
        (other) =>
          other !== entry &&
          other.kind !== 'unmapped' &&
          (other.kind === 'stopped') === (entry.kind === 'stopped') &&
          encloses(other.field.path, entry.field.path),
      ),
  );
}

/** Whether `outer` is a field `inner` is nested in: its path a strict prefix of the other's. */
function encloses(outer: readonly string[], inner: readonly string[]): boolean {
  return outer.length < inner.length && outer.every((segment, index) => inner[index] === segment);
}
