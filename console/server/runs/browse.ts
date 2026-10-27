/**
 * Browse runs: a time range and filters over every column, newest rows first.
 *
 * The opening view of a table is one of these with no filters and no range, and it is the reason
 * this works the way it does. The table's manifests record every data file's exact row count, so a
 * run can know its total, and the exact position of every row, without reading anything:
 *
 *   objects, newest first:  [ A: 66,655 ][ B: 66,654 ][ C: 54,119 ] …
 *   row 70,000 is row 3,345 of B
 *
 * A page is then one read of one object's row range — `file_row_number BETWEEN …`, which DuckDB
 * answers from a single row group — so page 1 and page 40,000 cost the same, and neither sorts
 * or re-scans anything. That is BigQuery's preview, built on what the runtime already records.
 *
 * Narrowing changes only how many of those counts must be read rather than looked up:
 *
 * - an object wholly inside the time range, with no filters, is counted from its manifest entry;
 * - an object straddling the range, or any object under a filter, is counted by reading only the
 *   columns that decide it — the timestamp and the filtered columns — with row groups outside the
 *   range skipped on their statistics.
 *
 * Objects are counted newest first and in parallel, so the first page is readable as soon as the
 * first objects are counted, long before the total is known. Progress is exact: rows of the
 * objects counted so far, out of rows of every object the range selected.
 *
 * The live tail's files are objects like any other, on local disk, newest of all: the first page of
 * a table, with live tail on, is the records its runtime has not committed yet. A refresh of a
 * finished run appends what the tail has read since as a further file, and every row moves down by
 * the rows it adds (`revision`): the snapshot stays pinned, only its tail grows.
 */

import { access as accessFile } from 'node:fs/promises';

import { config } from '../config.ts';
import { readAll, withConnection } from '../data/duckdb.ts';
import { objectSource, objectSources, assertWithinCachedBudget } from '../data/objects.ts';
import { insideRange, overlapsRange, type DataObject } from '../data/catalog.ts';
import { filterColumns, readParquet, selectList, timePredicate, toQueryColumns, whereClause } from '../data/logical.ts';
import { Run, type RunInit } from './run.ts';
import { ROWS_VIEW, viewSetup } from './view.ts';
import type { TailAccess, TailFetchResult } from './tail.ts';
import type { BrowseFilter, RunOrder, RunPage } from '../../shared/types.ts';

interface Segment {
  object: DataObject;
  /** Every row of the object matches: no filters, and it lies wholly inside the range. */
  whole: boolean;
  /** Rows of this object the run matches, once known. */
  count: number | null;
}

const PAGE_CACHE_SIZE = 32;

/**
 * Objects one page reads at once.
 *
 * A screenful of rows comes from one or two objects. An export reads five thousand rows a page, and
 * under a sparse filter those can be spread over dozens of objects; reading every one of them at
 * once decompresses all of their columns together, which ran DuckDB out of memory on a 33,805-row
 * export from 171 objects of 81 columns. A few at a time keeps a page's memory bounded.
 */
const PAGE_READ_CONCURRENCY = 4;

/** Workers that start a scan alone, on the newest objects. */
const FIRST_WAVE = 8;
/** When the rest join if no object has been counted yet. */
const WIDEN_AFTER_MS = 500;

export class BrowseRun extends Run {
  private segments: Segment[] = [];
  /** `starts[i]` is the run position of segment `i`'s first row, for every counted leading segment. */
  private readonly starts: number[] = [];
  private readyIndex = 0;
  private readonly filters: BrowseFilter[];
  private readonly order: RunOrder;
  private readonly pages = new Map<string, Promise<Record<string, unknown>[]>>();
  /** Page reads someone is waiting on; while there are any, counting starts no new object. */
  private pageReads = 0;
  /** Pages already served while partial, whose later reads wait their turn. */
  private readonly servedPartial = new Set<string>();
  private readonly resumeCounting: Array<() => void> = [];
  /** A refresh in flight: a second one waits for it rather than fetching the same records again. */
  private refreshing: Promise<TailFetchResult> | null = null;

  constructor(init: RunInit) {
    super(init);
    this.filters = init.request.filters ?? [];
    this.order = init.request.order ?? 'newest';
  }

  protected async execute(): Promise<void> {
    const range = this.request.range;
    await this.fetchTails([{ key: this.tableName, table: this.tableName, catalog: this.catalog }]);
    this.throwIfCancelled();
    const selected = [...this.tailObjects(this.tableName), ...this.catalog.objects].filter((object) => overlapsRange(object, range));
    this.segments = orderObjects(selected, this.order).map((object) => {
      const whole = this.filters.length === 0 && object.rows !== null && insideRange(object, range);
      return { object, whole, count: whole ? object.rows : null };
    });

    let rowsTotal = 0;
    let bytes = 0;
    for (const { object } of this.segments) {
      rowsTotal += object.rows ?? 0;
      bytes += object.bytes;
    }
    this.scope = { objects: this.segments.length, bytes, rows: rowsTotal };
    this.columns = toQueryColumns(this.logical);
    this.progress = {
      rowsTotal,
      rowsDone: 0,
      objectsTotal: this.segments.length,
      objectsDone: 0,
      objectsRead: 0,
      bytesTotal: bytes,
      fraction: null,
    };
    for (const segment of this.segments) {
      if (segment.count !== null) this.recordCounted(segment);
    }
    this.advanceReady();

    const pending = this.segments.filter((segment) => segment.count === null);
    if (pending.length === 0) return;

    assertWithinCachedBudget(pending.map((segment) => segment.object));
    this.phase = 'scanning';
    this.changed(true);

    const release = await this.admit(this.signal);
    try {
      await this.countSegments(pending);
    } finally {
      release();
    }
  }

  /**
   * Count every pending segment, newest first, a bounded number at a time.
   *
   * The newest objects decide the first page, so they start with the network to themselves: a
   * first wave of workers begins at once, and the rest join as soon as one object is counted, or
   * after `WIDEN_AFTER_MS` if none is yet.
   */
  private async countSegments(pending: Segment[]): Promise<void> {
    let cursor = 0;
    let failure: unknown = null;
    let widen = (): void => {};
    const widened = new Promise<void>((resolve) => {
      widen = resolve;
    });
    const timer = setTimeout(widen, WIDEN_AFTER_MS);

    const worker = async (): Promise<void> => {
      try {
        while (cursor < pending.length && failure === null && !this.signal.aborted) {
          while (this.pageReads > 0) await new Promise<void>((resume) => this.resumeCounting.push(resume));
          if (cursor >= pending.length || failure !== null || this.signal.aborted) break;
          const segment = pending[cursor++] as Segment;
          try {
            segment.count = await this.countObject(segment.object);
            this.progress.objectsRead += 1;
          } catch (cause) {
            if (failure === null && !this.signal.aborted) {
              failure = cause;
              this.abortAfterFailure();
            }
            return;
          }
          const firstRows = this.rowsReady === 0;
          this.recordCounted(segment);
          this.advanceReady();
          // The first readable rows are what the person running this is waiting for.
          this.changed(firstRows && this.rowsReady > 0);
          widen();
        }
      } finally {
        widen();
      }
    };

    const width = Math.min(config.runScanConcurrency, pending.length);
    const first = Math.min(width, FIRST_WAVE);
    const workers = Array.from({ length: first }, () => worker());
    workers.push(
      widened.then(async () => {
        clearTimeout(timer);
        await Promise.all(Array.from({ length: width - first }, () => worker()));
      }),
    );
    await Promise.all(workers);
    if (failure !== null) throw failure;
    this.throwIfCancelled();
  }

  /** Rows of one object matching the range and filters, reading only the columns that decide it. */
  private async countObject(object: DataObject): Promise<number> {
    const schema = this.schema;
    if (schema === null) return 0;
    const source = await objectSource(object);
    return withConnection(
      async (connection) => {
        const predicate = timePredicate(this.request.range);
        const inner =
          `SELECT ${selectList(this.logical, filterColumns(this.filters))} ` +
          `FROM ${readParquet([source], schema)}${predicate ? ` WHERE ${predicate}` : ''}`;
        const result = await readAll(
          connection,
          `SELECT count(*)::BIGINT AS n FROM (${inner}) AS o${whereClause(this.filters, this.logical)}`,
        );
        return Number(result.rows[0]?.n ?? 0);
      },
      { signal: this.signal },
    );
  }

  private recordCounted(segment: Segment): void {
    this.progress.objectsDone += 1;
    this.progress.rowsDone += segment.object.rows ?? 0;
    this.progress.fraction =
      this.progress.rowsTotal > 0
        ? this.progress.rowsDone / this.progress.rowsTotal
        : this.progress.objectsDone / Math.max(1, this.progress.objectsTotal);
    this.rows += segment.count ?? 0;
  }

  /** Extend the run of leading counted segments, which is exactly what can be paged. */
  private advanceReady(): void {
    while (this.readyIndex < this.segments.length) {
      const segment = this.segments[this.readyIndex] as Segment;
      if (segment.count === null) break;
      this.starts[this.readyIndex] = this.rowsReady;
      this.rowsReady += segment.count;
      this.readyIndex += 1;
    }
    if (this.readyIndex === this.segments.length) {
      this.totalExact = true;
      this.rows = this.rowsReady;
    }
  }

  /**
   * One page, answered at once with whatever of it is counted.
   *
   * Only a complete page is cached: a partial one will read differently once counting reaches it.
   * A page read while counting goes first, because from outside the cloud the two share one
   * network path: measured cold, a first page took 1.4 s beside a full-width count and 0.86 s
   * ahead of it. That holds for the first read of a page and for the read that completes it; the
   * reads in between, as a partial page fills, wait their turn, so a sparse filter — whose first
   * page fills only as counting proceeds — cannot stall the count it is waiting on.
   */
  async readPage(offset: number, limit: number): Promise<RunPage> {
    this.touch();
    const complete = offset + limit <= this.rowsReady || this.isTerminal();
    const key = `${offset}:${limit}`;
    let rows: Promise<Record<string, unknown>[]> | undefined = complete ? this.pages.get(key) : undefined;
    if (!rows) {
      const first = !this.servedPartial.has(key);
      if (!complete) this.servedPartial.add(key);
      const read = (): Promise<Record<string, unknown>[]> => this.readRange(offset, limit);
      rows = (complete || first) && !this.isTerminal() ? this.ahead(read) : read();
      if (complete) this.remember(key, rows);
    }
    const page: RunPage = {
      runId: this.id,
      offset,
      rows: await rows,
      columns: this.columns ?? [],
      complete,
      total: this.rows,
      totalExact: this.totalExact,
    };
    if (complete) this.prefetch(offset + limit, limit);
    return page;
  }

  /** Run a read with counting held back until it finishes. */
  private async ahead<T>(read: () => Promise<T>): Promise<T> {
    this.pageReads += 1;
    try {
      return await read();
    } finally {
      this.pageReads -= 1;
      if (this.pageReads === 0) for (const resume of this.resumeCounting.splice(0)) resume();
    }
  }

  /**
   * Read the next page into DuckDB's caches while this one is being looked at.
   *
   * The rows are kept too, so the click that asks for them is answered from memory.
   */
  private prefetch(offset: number, limit: number): void {
    const key = `${offset}:${limit}`;
    if (this.pages.has(key) || offset >= this.rowsReady || offset + limit > this.rowsReady) return;
    const rows = this.readRange(offset, limit);
    this.remember(key, rows);
    rows.catch(() => undefined);
  }

  private remember(key: string, rows: Promise<Record<string, unknown>[]>): void {
    this.pages.set(key, rows);
    rows.catch(() => this.pages.delete(key));
    while (this.pages.size > PAGE_CACHE_SIZE) this.pages.delete(this.pages.keys().next().value as string);
  }

  readRows(offset: number, limit: number): Promise<Record<string, unknown>[]> {
    return this.readRange(offset, limit);
  }

  exportableRows(): number {
    return Math.min(this.rows, config.exportRowLimit);
  }

  /** Every object that may hold a matching row, with the range and filters applied. */
  async rowsView(): Promise<string[]> {
    const objects = this.segments.filter((segment) => segment.count !== 0).map((segment) => segment.object);
    const sources = await objectSources(objects);
    return viewSetup(
      objects.map((object) => sources.get(object.uri) ?? object.uri),
      this.schema,
      this.logical,
      this.request.range,
      { name: ROWS_VIEW, filtersSql: whereClause(this.filters, this.logical) },
    );
  }

  /**
   * Append the records the live tail has read since the run's last look, as one more file.
   *
   * Only for a finished run whose table has a tail: the snapshot stays pinned, its tail grows. A
   * refresh while another is in flight waits for that one instead of fetching the same records
   * twice. `stale` means the tail no longer holds records this far back, and only a new run - pinned
   * to a newer snapshot - has a tail.
   */
  async refreshTail(access: TailAccess): Promise<TailFetchResult> {
    this.refreshing ??= this.appendTail(access).finally(() => {
      this.refreshing = null;
    });
    return this.refreshing;
  }

  private async appendTail(access: TailAccess): Promise<TailFetchResult> {
    const tail = this.tails.get(this.tableName);
    // An export is reading the rows as they are: what arrived meanwhile waits for the next refresh.
    if (this.state !== 'succeeded' || !tail || tail.stopped || this.rowsHeld) return { kind: 'unchanged' };
    const result = await tail.fetch(access, this.workDir, this.signal, () => !this.disposed && !this.rowsHeld);
    if (result.kind !== 'appended') {
      this.changed(true);
      return result;
    }
    const object = result.object;
    if (!overlapsRange(object, this.request.range)) {
      this.changed(true);
      return result;
    }
    const whole = this.filters.length === 0 && object.rows !== null && insideRange(object, this.request.range);
    const segment: Segment = { object, whole, count: whole ? object.rows : await this.countObject(object) };
    this.segments = orderSegments([...this.segments, segment], this.order);
    this.scope = { objects: this.scope.objects + 1, bytes: this.scope.bytes + object.bytes, rows: this.scope.rows + (object.rows ?? 0) };
    this.progress = {
      ...this.progress,
      rowsTotal: this.progress.rowsTotal + (object.rows ?? 0),
      rowsDone: this.progress.rowsDone + (object.rows ?? 0),
      objectsTotal: this.progress.objectsTotal + 1,
      objectsDone: this.progress.objectsDone + 1,
      objectsRead: this.progress.objectsRead + (whole ? 0 : 1),
      bytesTotal: this.progress.bytesTotal + object.bytes,
    };
    this.reposition();
    return result;
  }

  /**
   * Every row's position again, after the run's objects changed: the run is finished, so every
   * segment is counted, and pages read before are forgotten (`revision`).
   */
  private reposition(): void {
    this.starts.length = 0;
    this.readyIndex = 0;
    this.rowsReady = 0;
    this.totalExact = false;
    this.advanceReady();
    this.pages.clear();
    this.servedPartial.clear();
    this.revision += 1;
    this.changed(true);
  }

  /**
   * Drop a tail whose file has gone from disk, so the run pages its committed rows alone. Its files
   * are consecutive, so they go together; the tail says so, and stops.
   */
  private async dropMissingTail(): Promise<boolean> {
    const tail = this.tails.get(this.tableName);
    if (!tail || tail.objects.length === 0) return false;
    const missing = await Promise.all(tail.objects.map((object) => accessFile(object.uri).then(() => false, () => true)));
    if (!missing.some(Boolean)) return false;
    const lost = new Set(tail.lose().map((object) => object.uri));
    const dropped = this.segments.filter((segment) => lost.has(segment.object.uri));
    this.segments = this.segments.filter((segment) => !lost.has(segment.object.uri));
    for (const segment of dropped) {
      this.scope = { objects: this.scope.objects - 1, bytes: this.scope.bytes - segment.object.bytes, rows: this.scope.rows - (segment.object.rows ?? 0) };
    }
    if (this.isTerminal()) this.reposition();
    return true;
  }

  /** Rows `[offset, offset + limit)` in run order, without the page cache. */
  private async readRange(offset: number, limit: number, retried = false): Promise<Record<string, unknown>[]> {
    try {
      return await this.readRangeOnce(offset, limit);
    } catch (cause) {
      // A tail file can only go missing from under a run by hand; its committed rows still page.
      if (!retried && this.isTerminal() && (await this.dropMissingTail())) return this.readRange(offset, limit, true);
      throw cause;
    }
  }

  private async readRangeOnce(offset: number, limit: number): Promise<Record<string, unknown>[]> {
    const end = Math.min(offset + limit, this.rowsReady);
    if (offset >= end) return [];

    const reads: Array<() => Promise<Record<string, unknown>[]>> = [];
    let index = this.segmentAt(offset);
    let position = offset;
    while (position < end && index < this.readyIndex) {
      const segment = this.segments[index] as Segment;
      const start = this.starts[index] as number;
      const count = segment.count ?? 0;
      const from = position - start;
      const to = Math.min(count, end - start);
      if (to > from) reads.push(() => this.readSegment(segment, from, to));
      position = start + count;
      index += 1;
    }

    const results: Record<string, unknown>[][] = new Array(reads.length);
    let cursor = 0;
    const worker = async (): Promise<void> => {
      while (cursor < reads.length) {
        const at = cursor++;
        results[at] = await (reads[at] as () => Promise<Record<string, unknown>[]>)();
      }
    };
    await Promise.all(Array.from({ length: Math.min(PAGE_READ_CONCURRENCY, reads.length) }, () => worker()));
    return results.flat();
  }

  /** The first counted segment whose rows reach past `offset`. */
  private segmentAt(offset: number): number {
    let low = 0;
    let high = this.readyIndex - 1;
    let found = this.readyIndex;
    while (low <= high) {
      const middle = (low + high) >> 1;
      const end = (this.starts[middle] as number) + ((this.segments[middle] as Segment).count ?? 0);
      if (end > offset) {
        found = middle;
        high = middle - 1;
      } else {
        low = middle + 1;
      }
    }
    return found;
  }

  /**
   * Matching rows `[from, to)` of one object, in run order.
   *
   * A whole object maps run positions straight onto file row numbers, so DuckDB reads one row
   * group. Otherwise the object's matching rows are numbered in file order and the page taken from
   * them; DuckDB still skips row groups the range rules out.
   */
  private async readSegment(segment: Segment, from: number, to: number): Promise<Record<string, unknown>[]> {
    const { object } = segment;
    const schema = this.schema;
    if (schema === null) return [];
    const newest = this.order === 'newest';
    const source = await objectSource(object);

    return withConnection(async (connection) => {
      const projection = selectList(this.logical);
      const scan = readParquet([source], schema, { rowNumbers: true });
      const direction = newest ? 'DESC' : 'ASC';

      let sql: string;
      if (segment.whole) {
        const count = segment.count ?? 0;
        const [low, high] = newest ? [count - to, count - 1 - from] : [from, to - 1];
        sql =
          `SELECT ${projection}, file_row_number AS "__row" FROM ${scan} ` +
          `WHERE file_row_number BETWEEN ${low} AND ${high} ORDER BY "__row" ${direction}`;
      } else {
        const predicate = timePredicate(this.request.range);
        const inner = `SELECT ${projection}, file_row_number AS "__row" FROM ${scan}${predicate ? ` WHERE ${predicate}` : ''}`;
        sql =
          `SELECT * FROM (${inner}) AS o${whereClause(this.filters, this.logical)} ` +
          `ORDER BY "__row" ${direction} LIMIT ${to - from} OFFSET ${from}`;
      }

      const result = await readAll(connection, sql);
      for (const row of result.rows) delete row.__row;
      return result.rows;
    });
  }
}

/**
 * Newest first: by the latest Kafka timestamp an object holds, then by commit time.
 *
 * Objects holding no timestamped row — records Kafka gave no timestamp — have no place on the time
 * axis and sort after every object that does, by commit time among themselves. The live tail's
 * files are newer than anything committed, so they come first among objects of the same time.
 */
export function orderObjects(objects: DataObject[], order: RunOrder): DataObject[] {
  const sorted = [...objects].sort((a, b) => {
    const timed = Number(b.timeMax !== null) - Number(a.timeMax !== null);
    if (timed !== 0) return timed;
    if (a.timeMax !== null && b.timeMax !== null && a.timeMax !== b.timeMax) return b.timeMax - a.timeMax;
    const tail = Number(b.tail === true) - Number(a.tail === true);
    if (tail !== 0) return tail;
    const committed = (b.committedAt ?? 0) - (a.committedAt ?? 0);
    if (committed !== 0) return committed;
    return b.name.localeCompare(a.name);
  });
  return order === 'newest' ? sorted : sorted.reverse();
}

/** Segments in run order, by their objects (`orderObjects`). */
function orderSegments(segments: Segment[], order: RunOrder): Segment[] {
  const byUri = new Map(segments.map((segment) => [segment.object.uri, segment]));
  return orderObjects(segments.map((segment) => segment.object), order).map((object) => byUri.get(object.uri) as Segment);
}
