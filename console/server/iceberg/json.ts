/**
 * JSON parsing that keeps 64-bit integers exact.
 *
 * Iceberg snapshot ids are random 64-bit integers, written into metadata files and API answers as
 * plain JSON numbers. `JSON.parse` would round them through a double — 3729562493443214175 reads
 * back as 3729562493443214000 — and a rounded id names no snapshot at all. Any integer a double
 * cannot hold exactly is kept as its decimal text instead, which is how the console carries
 * snapshot ids everywhere: in run snapshots, in links, and back to the control plane.
 */

type Reviver = (key: string, value: unknown, context?: { source?: string }) => unknown;

/** Keys holding a snapshot id, which is text wherever the console carries it, small or not. */
const SNAPSHOT_ID_KEY = /snapshot[_-]id$/i;

const exact: Reviver = (key, value, context) => {
  if (typeof value === 'number' && Number.isInteger(value)) {
    const source = context?.source;
    const text = source && /^-?\d+$/.test(source) ? source : null;
    if (text !== null && (!Number.isSafeInteger(value) || SNAPSHOT_ID_KEY.test(key))) return text;
  }
  return value;
};

/** `JSON.parse`, with integers beyond 2^53 — and every snapshot id — kept as decimal strings. */
export function parseJson(text: string): unknown {
  return JSON.parse(text, exact as Parameters<typeof JSON.parse>[1]);
}

/** A snapshot id as the console carries it: decimal text, or null for none. */
export function snapshotIdText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string' && /^-?\d+$/.test(value)) return value;
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'number' && Number.isInteger(value)) return String(value);
  return null;
}
