/**
 * How materialized fields are named wherever a person reads or types one.
 *
 * The control plane roots every inferred field at `payload.` — `payload.customer.email` — so a
 * record's own fields can never collide with the metadata columns the runtime writes beside them
 * (`_kafka_offset`, …). That root is bookkeeping rather than anything a producer wrote, so the
 * console drops it: the column is `customer.email` in the grid, in filters, in SQL and in exports
 * alike. The one exception is a top-level field that would otherwise take a metadata column's name,
 * which keeps its prefix so the two stay distinct.
 *
 * Dependency-free, like everything in `shared/`: the server and the browser both import it, under
 * different module resolution modes.
 */

/**
 * The Kafka metadata columns every table has beside the payload, in the order a reader wants them.
 *
 * `_unmapped` holds, as a JSON object, any value no column could take; it is null on almost every row.
 */
export const METADATA_COLUMNS = [
  '_kafka_timestamp',
  '_kafka_partition',
  '_kafka_offset',
  '_kafka_key',
  '_kafka_headers',
  '_materialized_at',
  '_unmapped',
] as const;

const METADATA = new Set<string>(METADATA_COLUMNS);

/** The root every inferred field path starts with. */
export const PAYLOAD_ROOT = 'payload.';

export function isMetadataColumn(name: string): boolean {
  return METADATA.has(name);
}

/**
 * The name a field goes by: its logical path without the `payload.` root.
 *
 * `payload.customer.email` is `customer.email`; `payload._kafka_offset`, a field that would take a
 * metadata column's name, keeps its root. Anything not under the root is returned as it is.
 */
export function fieldName(path: string): string {
  if (!path.startsWith(PAYLOAD_ROOT)) return path;
  const name = path.slice(PAYLOAD_ROOT.length);
  return name === '' || METADATA.has(name) ? path : name;
}

/** Logical paths written into prose — `payload.total must be a number` — and where they end. */
const PATH_IN_TEXT = /(^|[^\w.$-])payload\.([^\s"'`,;:()]+)/g;

/**
 * A message from the runtime or the control plane, with the field paths it quotes renamed the way
 * the console names them: `payload.total must be a floating-point value` reads
 * `total must be a floating-point value`.
 */
export function withFieldNames(text: string): string {
  return text.replace(PATH_IN_TEXT, (_match, lead: string, rest: string) => lead + fieldName(`${PAYLOAD_ROOT}${rest}`));
}

/** One step of a field path: a key, and how many lists deep its value sits (`items[]` is one). */
export interface PathStep {
  key: string;
  lists: number;
}

/** `items[].attributes[].name` → `items` (1 list), `attributes` (1 list), `name`. */
export function pathSteps(name: string): PathStep[] {
  return name.split('.').map((part) => {
    const match = /^(.+?)((?:\[\])+)$/.exec(part);
    return match ? { key: match[1] as string, lists: (match[2] as string).length / 2 } : { key: part, lists: 0 };
  });
}

type Holder = Record<string, unknown> | unknown[];

function slotOf(holder: Holder, slot: string | number): unknown {
  return Array.isArray(holder) ? holder[slot as number] : holder[slot as string];
}

function assign(holder: Holder, slot: string | number, value: unknown): void {
  if (Array.isArray(holder)) holder[slot as number] = value;
  else holder[slot as string] = value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * The container at `slot`, creating it when the slot is empty or holds a null.
 *
 * Null when the slot already holds something of another shape — a value, or the other kind of
 * container — which is a field whose shape changed over its history.
 */
function claim(holder: Holder, slot: string | number, kind: 'object' | 'array'): Holder | null {
  const existing = slotOf(holder, slot);
  if (existing === undefined || existing === null) {
    // Without a prototype, a field literally named `__proto__` is just another key.
    const created: Holder = kind === 'array' ? [] : (Object.create(null) as Record<string, unknown>);
    assign(holder, slot, created);
    return created;
  }
  if (kind === 'array') return Array.isArray(existing) ? existing : null;
  return isRecord(existing) ? existing : null;
}

/**
 * Whether two scalars are one value in two encodings.
 *
 * The same field can reach a row through more than one column: an array the schema materialized
 * whole, before its elements were given columns of their own, carries `{ "amount": 3.01 }` while
 * the element's own column, widened to text across a fork, carries `"3.01"`.
 */
function sameValue(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (typeof left === 'object' || typeof right === 'object') return false;
  if (String(left) === String(right)) return true;
  const numbers = [Number(left), Number(right)];
  return String(left).trim() !== '' && String(right).trim() !== '' && numbers.every(Number.isFinite) && numbers[0] === numbers[1];
}

/**
 * Put `value` at `slot`, merging with whatever is already there.
 *
 * A null never displaces anything. Containers merge key by key and element by element, and equal
 * values in different encodings agree; only two different values at one place conflict, and then
 * the existing one stays.
 */
function setLeaf(holder: Holder, slot: string | number, value: unknown): boolean {
  const existing = slotOf(holder, slot);
  if (existing === undefined || existing === null) {
    if (existing === undefined || value !== null) assign(holder, slot, value);
    return true;
  }
  if (value === null || value === undefined) return true;
  if (Array.isArray(existing) && Array.isArray(value)) {
    let merged = true;
    value.forEach((element, index) => {
      merged = setLeaf(existing, index, element) && merged;
    });
    return merged;
  }
  if (isRecord(existing) && isRecord(value)) {
    let merged = true;
    for (const [key, item] of Object.entries(value)) merged = setLeaf(existing, key, item) && merged;
    return merged;
  }
  return sameValue(existing, value);
}

function put(holder: Holder, slot: string | number, lists: number, steps: PathStep[], at: number, value: unknown): boolean {
  if (lists > 0) {
    // A list column holds one entry per element; anything else — a null, in practice — stands
    // where the list would be.
    if (!Array.isArray(value)) return setLeaf(holder, slot, value);
    const list = claim(holder, slot, 'array');
    if (list === null) return false;
    let placed = true;
    value.forEach((element, index) => {
      placed = put(list, index, lists - 1, steps, at, element) && placed;
    });
    return placed;
  }
  if (at === steps.length - 1) return setLeaf(holder, slot, value);
  // A null creates the objects above it, so `{ "customer": { "email": null } }` reads as the record
  // did — unless one of them already holds a value, and then there is nothing to place.
  const parent = slotOf(holder, slot);
  if (value === null && parent !== undefined && parent !== null && !isRecord(parent)) return true;
  const object = claim(holder, slot, 'object');
  if (object === null) return false;
  const next = steps[at + 1] as PathStep;
  return put(object, next.key, next.lists, steps, at + 1, value);
}

/** Text a `json` column holds, as the value it encodes; text that does not parse stays text. */
function decodeJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(decodeJson);
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

export interface NestOptions {
  /** Columns that hold JSON text, decoded into the values they encode. */
  jsonColumns?: ReadonlySet<string>;
}

/**
 * A function turning one flat row — `{ "customer.email": …, "items[].sku": [ … ] }` — into the
 * record it came from: `{ customer: { email: … }, items: [ { sku: … } ] }`.
 *
 * Columns are field paths, so the structure is in the names: a dot is a nested object and `[]` a
 * list whose elements line up across every column beneath it. Columns whose names hold no path,
 * such as the Kafka metadata or an aggregate's alias, stay at the top level.
 *
 * A field whose shape changed over its history can have columns for both shapes — `customer.phone`
 * as text, and later `customer.phone.number` — and one row only ever fills one of them. Values are
 * placed before nulls so the filled shape wins wherever they meet. Columns that overlap — a whole
 * array materialized beside its elements' own columns — merge, equal values agreeing whatever
 * their encoding. Should two different values ever claim one place, the later keeps its flat name
 * rather than being lost.
 */
export function rowNester(columns: readonly string[], options: NestOptions = {}): (row: Record<string, unknown>) => Record<string, unknown> {
  const plan = columns.map((name) => ({ name, steps: pathSteps(name), json: options.jsonColumns?.has(name) ?? false }));
  return (row) => {
    const nested = Object.create(null) as Record<string, unknown>;
    for (const nulls of [false, true]) {
      for (const { name, steps, json } of plan) {
        const raw = row[name] ?? null;
        if ((raw === null) !== nulls) continue;
        const value = json ? decodeJson(raw) : raw;
        const first = steps[0] as PathStep;
        if (!put(nested, first.key, first.lists, steps, 0, value) && value !== null) nested[freeKey(nested, name)] = value;
      }
    }
    return nested;
  };
}

/** `name`, or `name (2)`, `name (3)`… when a single-step name is already the key of something else. */
function freeKey(holder: Record<string, unknown>, name: string): string {
  if (!(name in holder)) return name;
  let suffix = 2;
  while (`${name} (${suffix})` in holder) suffix += 1;
  return `${name} (${suffix})`;
}
