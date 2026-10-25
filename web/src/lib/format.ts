const COMPACT = new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 });
const FULL = new Intl.NumberFormat();

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return Math.abs(value) >= 10_000 ? COMPACT.format(value) : FULL.format(value);
}

export function formatExact(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return FULL.format(value);
}

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];

export function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  if (value === 0) return '0 B';
  const exponent = Math.min(Math.floor(Math.log10(Math.abs(value)) / 3), BYTE_UNITS.length - 1);
  const scaled = value / 1000 ** exponent;
  return `${scaled.toFixed(scaled >= 100 || exponent === 0 ? 0 : 1)} ${BYTE_UNITS[exponent]}`;
}

export function formatRate(value: number | null | undefined, unit: string): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  if (value < 0.05) return `0 ${unit}`;
  return `${formatCount(Math.round(value * 10) / 10)} ${unit}`;
}

export function formatBytesRate(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return `${formatBytes(value)}/s`;
}

export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms)) return '—';
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  return `${minutes}m ${seconds}s`;
}

export function formatRelativeTime(iso: string | null | undefined, now = Date.now()): string {
  if (!iso) return '—';
  const timestamp = Date.parse(iso);
  if (!Number.isFinite(timestamp)) return '—';
  const deltaSeconds = Math.round((timestamp - now) / 1000);
  const absolute = Math.abs(deltaSeconds);

  if (absolute < 5) return 'just now';
  const steps: [number, Intl.RelativeTimeFormatUnit][] = [
    [60, 'second'],
    [3600, 'minute'],
    [86_400, 'hour'],
    [2_592_000, 'day'],
    [31_536_000, 'month'],
    [Number.POSITIVE_INFINITY, 'year'],
  ];
  const divisors = [1, 60, 3600, 86_400, 2_592_000, 31_536_000];
  const index = steps.findIndex(([limit]) => absolute < limit);
  const unit = steps[index]?.[1] ?? 'year';
  const divisor = divisors[index] ?? 1;
  return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(
    Math.round(deltaSeconds / divisor),
    unit,
  );
}

export function formatTimestamp(iso: string | null | undefined): string {
  if (!iso) return '—';
  const timestamp = Date.parse(iso);
  if (!Number.isFinite(timestamp)) return iso;
  return new Date(timestamp).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

/** Turn `waiting_for_source_records` into `Waiting for source records`. */
export function humanize(value: string | null | undefined): string {
  if (!value) return '—';
  const spaced = value.replace(/[_-]+/g, ' ').trim();
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

/** `orders`, `orders and payments`, `orders, payments, refunds and 2 more`. */
export function topicsLabel(topics: readonly string[], shown = 3): string {
  if (topics.length <= 1) return topics[0] ?? '';
  if (topics.length <= shown) return `${topics.slice(0, -1).join(', ')} and ${topics.at(-1) as string}`;
  return `${topics.slice(0, shown).join(', ')} and ${topics.length - shown} more`;
}

export function displayName(item: { name: string | null; materialization_id: string }): string {
  return item.name?.trim() || item.materialization_id;
}

export function formatCell(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

/** DuckDB's name for an instant: a timestamp that carries its zone, sent as UTC ISO-8601. */
export function isInstantType(type: string | null | undefined): boolean {
  return typeof type === 'string' && /^TIMESTAMP(?:TZ| WITH TIME ZONE)$/i.test(type.trim());
}

const pad = (value: number, width = 2): string => String(value).padStart(width, '0');

/**
 * An instant in the reader's time zone, as `2026-10-04 16:59:58.604`.
 *
 * Local because the time picker resolves ranges in local time, so a row inside "Today" should read
 * as today; fixed-width and ordered like ISO so a column of them lines up and sorts by eye.
 */
export function formatInstant(iso: string): string {
  const time = Date.parse(iso);
  if (!Number.isFinite(time)) return iso;
  const date = new Date(time);
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`
  );
}

/** The reader's time zone: a short label for headers (`PT`) and its full name (`America/Los_Angeles`). */
export function localZone(): { label: string; name: string } {
  const name = Intl.DateTimeFormat().resolvedOptions().timeZone;
  for (const style of ['shortGeneric', 'short'] as const) {
    try {
      const label = new Intl.DateTimeFormat(undefined, { timeZoneName: style })
        .formatToParts(new Date())
        .find((part) => part.type === 'timeZoneName')?.value;
      if (label) return { label, name };
    } catch {
      /* `shortGeneric` is newer than some engines */
    }
  }
  return { label: name, name };
}

/** A cell as shown: instants in local time, everything else as `formatCell` renders it. */
export function displayCell(value: unknown, type?: string | null): string {
  if (typeof value === 'string' && isInstantType(type)) return formatInstant(value);
  return formatCell(value);
}

/** `3729562493443214175` → `3729…4175`: enough to tell snapshots apart at a glance. */
export function shortSnapshotId(snapshotId: string): string {
  return snapshotId.length > 10 ? `${snapshotId.slice(0, 4)}…${snapshotId.slice(-4)}` : snapshotId;
}
