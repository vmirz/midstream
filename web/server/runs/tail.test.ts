/**
 * Runs with a live tail: the tail app's real answers — the fixture's tail files, built by the tail
 * app's own code (`python/tests/console_fixture.py`) — served by a stub of its HTTP API, over the
 * fixture tables, read through the console's real client (`data/tail.ts`).
 *
 * What matters is that a run's snapshot and its tail together hold every record up to the tail's
 * `through` exactly once, however the tail is fetched, refreshed, refused or lost; and that a run
 * whose tail cannot be had still answers with its committed rows and says why.
 */

import assert from 'node:assert/strict';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, readFile, readdir, rm, unlink } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, afterEach, before, beforeEach, describe, it } from 'node:test';

/* ------------------------------------------------------------------------- the stub tail app */

interface StubRequest {
  table: string;
  head: boolean;
  after: Record<string, number>;
  headers: IncomingMessage['headers'];
  query: URLSearchParams;
}

interface StubAnswer {
  status: number;
  headers?: Record<string, string>;
  body?: Buffer | string;
  delayMs?: number;
  /** Never answer: the request hangs until the client gives up. */
  hang?: boolean;
}

let respond: (request: StubRequest) => StubAnswer | Promise<StubAnswer> = () => ({ status: 500 });
const seen: StubRequest[] = [];

const server = createServer((request, response) => {
  void handle(request, response);
});
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
const port = (server.address() as AddressInfo).port;

async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? '/', `http://127.0.0.1:${port}`);
  const match = /^\/tail\/v1\/materializations\/([^/]+)\/tables\/([^/]+)(\/head)?$/.exec(url.pathname);
  if (!match) {
    response.writeHead(404).end('no such route');
    return;
  }
  const stub: StubRequest = {
    table: decodeURIComponent(match[2] as string),
    head: match[3] !== undefined,
    after: JSON.parse(url.searchParams.get('after') ?? '{}') as Record<string, number>,
    headers: request.headers,
    query: url.searchParams,
  };
  seen.push(stub);
  const answer = await respond(stub);
  if (answer.hang) return;
  if (answer.delayMs) await new Promise((resolve) => setTimeout(resolve, answer.delayMs));
  response.writeHead(answer.status, { 'content-type': 'application/octet-stream', ...(answer.headers ?? {}) });
  response.end(answer.body ?? '');
}

const dataDir = await mkdtemp(join(tmpdir(), 'console-tail-runs-'));
process.env.CONSOLE_DATA_DIR = dataDir;
process.env.CONSOLE_TAIL_URL = `http://127.0.0.1:${port}/`;
process.env.CONSOLE_SERVICE_TOKEN = 'console-service-credential-0123456789abcdef';

const { setObjectReader } = await import('../iceberg/io.ts');
const { setObjectPathResolver } = await import('../data/objects.ts');
const { runQuery } = await import('../data/duckdb.ts');
const { fetchTail } = await import('../data/tail.ts');
const { resetTables } = await import('../iceberg/table.ts');
const { resetSnapshotCaches } = await import('../iceberg/snapshot.ts');
const { FIXTURE_DIR, PAYMENTS_DIR, FIXTURE_LOCATION, PAYMENTS_LOCATION, fixturePath, fixtureReader } = await import('../iceberg/testing.ts');
const { evictIdleRuns, getRun, refreshRun, resetRuns, startRun } = await import('./store.ts');
const { openExport } = await import('./export.ts');
const { config } = await import('../config.ts');

import type { Run } from './run.ts';
import type { TailAccess } from './tail.ts';
import type { RunRequest, TimeRange } from '../../shared/types.ts';

/* ---------------------------------------------------------------------------- the fixture */

interface FixtureAnswer {
  file: string;
  after: Record<string, number>;
  through: Record<string, number>;
  end_offsets: Record<string, number>;
  rows: number;
  bytes: number;
  schema_id: number;
  settling: number;
  time_min: number | null;
  time_max: number | null;
  served: Array<Record<string, unknown>>;
}

async function tailOf(dir: string): Promise<{ committed: Record<string, number>; answers: FixtureAnswer[] }> {
  const expected = JSON.parse(await readFile(join(dir, 'expected.json'), 'utf8')) as { tail: { committed: Record<string, number>; answers: FixtureAnswer[] } };
  return expected.tail;
}

const ORDERS_TAIL = await tailOf(FIXTURE_DIR);
const PAYMENTS_TAIL = await tailOf(PAYMENTS_DIR);
const DIRS: Record<string, string> = { orders: FIXTURE_DIR, payments: PAYMENTS_DIR };
const TAILS: Record<string, FixtureAnswer[]> = { orders: ORDERS_TAIL.answers, payments: PAYMENTS_TAIL.answers };
const MATERIALIZATION = 'mat-a1b2c3d4e5f6';
const PERSON = { kind: 'user' as const, token: 'tok-person-0001' };
const TABLES = [
  { name: 'orders', dataset: 'shop', datasetId: 'ds-000000000001', location: FIXTURE_LOCATION },
  { name: 'payments', dataset: 'finance', datasetId: 'ds-000000000002', location: PAYMENTS_LOCATION },
];
/** Rows the orders table holds at its current snapshot. */
const COMMITTED_ROWS = 12;
const ALL_TIME: TimeRange = { from: null, to: null };

const answer = (table: string, index: number): FixtureAnswer => TAILS[table]?.[index] as FixtureAnswer;
const offsetsOf = (fixture: FixtureAnswer): string[] => fixture.served.map((row) => `${row._kafka_partition}:${row._kafka_offset}`);

function headersOf(fixture: FixtureAnswer): Record<string, string> {
  return {
    'x-tail-rows': String(fixture.rows),
    'x-tail-bytes': String(fixture.bytes),
    'x-tail-after': JSON.stringify(fixture.after),
    'x-tail-through': JSON.stringify(fixture.through),
    'x-tail-end-offsets': JSON.stringify(fixture.end_offsets),
    'x-tail-status': fixture.rows === 0 ? 'empty' : 'complete',
    'x-tail-paused': '[]',
    'x-tail-gap': '[]',
    'x-tail-schema-id': String(fixture.schema_id),
    'x-tail-settling': String(fixture.settling),
    'x-tail-lag-ms': '0',
    'x-tail-member': 'materializer-tail-app-0',
    'x-tail-state': 'live',
    'x-tail-generated-at': '2026-10-23T12:00:00.000Z',
    ...(fixture.time_min === null ? {} : { 'x-tail-time-min': String(fixture.time_min), 'x-tail-time-max': String(fixture.time_max) }),
  };
}

/** The tail app as it answers the fixture: the answer past exactly `after`, or that it holds nothing so far back. */
async function fixtureTail(request: StubRequest): Promise<StubAnswer> {
  const found = (TAILS[request.table] ?? []).find((candidate) => sameOffsets(candidate.after, request.after));
  if (!found) return refusalAnswer(409, 'tail_unavailable', 'The live tail no longer holds records this far back.');
  return { status: 200, headers: headersOf(found), body: await readFile(join(DIRS[request.table] as string, found.file)) };
}

/** An error as the tail app sends it before Kaspr has an error content type: JSON, as bytes. */
function refusalAnswer(status: number, reason: string, message: string, details: Record<string, unknown> = {}): StubAnswer {
  return { status, body: Buffer.from(JSON.stringify({ error: { code: 'error', message, details: { reason, ...details } } })) };
}

function sameOffsets(a: Record<string, number>, b: Record<string, number>): boolean {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...keys].every((key) => (a[key] ?? -1) === (b[key] ?? -1));
}

function access(options: { off?: { reason: string; message: string } | null; timeoutMs?: number } = {}): TailAccess {
  return {
    off: options.off ?? null,
    fetch: (table, after, signal) =>
      fetchTail(PERSON, { materializationId: MATERIALIZATION, table, after, maxRows: 100_000, timeoutMs: options.timeoutMs ?? 3_000, signal }),
  };
}

async function settled(run: Run): Promise<Run> {
  if (!run.isTerminal()) {
    await new Promise<void>((resolve) => {
      const stop = run.subscribe((snapshot) => {
        if (snapshot.state === 'running') return;
        stop();
        resolve();
      });
      if (run.isTerminal()) resolve();
    });
  }
  return run;
}

async function started(request: Partial<RunRequest> = {}, tail: TailAccess | null = access()): Promise<Run> {
  const full: RunRequest = { kind: 'browse', range: ALL_TIME, ...request } as RunRequest;
  return settled(await startRun({ materializationId: MATERIALIZATION, ownerId: 'usr-test', tables: TABLES, request: full, tail }));
}

async function refreshed(run: Run, tail: TailAccess | null = access(), range: TimeRange | null = null): Promise<{ replaced: boolean; run: Run }> {
  const outcome = await refreshRun({ run, ownerId: 'usr-test', tables: TABLES, range, tail });
  await settled(outcome.run);
  return outcome;
}

/** Every row of a run, a page at a time, as `partition:offset`. */
async function everyOffset(run: Run, pageSize = 5): Promise<string[]> {
  const offsets: string[] = [];
  for (let offset = 0; offset < run.rows; offset += pageSize) {
    for (const row of (await run.readPage(offset, pageSize)).rows) offsets.push(`${row._kafka_partition}:${row._kafka_offset}`);
  }
  return offsets;
}

/** The orders table's committed rows, read straight from its data files. */
async function committedOffsets(): Promise<string[]> {
  const files = (await readdir(join(FIXTURE_DIR, 'data'), { recursive: true })).filter((name) => name.endsWith('.parquet'));
  const result = await runQuery(
    `SELECT _kafka_partition AS p, _kafka_offset AS o FROM read_parquet([${files.map((name) => `'${join(FIXTURE_DIR, 'data', name)}'`).join(', ')}])`,
  );
  // The compacted file and the files it replaced both sit in the directory: the snapshot holds each record once.
  return [...new Set(result.rows.map((row) => `${row.p}:${row.o}`))].sort();
}

const COMMITTED = await committedOffsets();

/** The pointer of the orders table, as the console reads it: its current one, or the one before. */
let ordersPointer: 'current' | 'previous' = 'current';

before(() => {
  const reader = fixtureReader();
  setObjectReader(async (uri) => {
    const bytes = await reader(uri);
    if (ordersPointer === 'previous' && uri === `${FIXTURE_LOCATION}/metadata/catalog-pointer.json`) {
      const pointer = JSON.parse(bytes.toString('utf8')) as Record<string, unknown>;
      return Buffer.from(JSON.stringify({ ...pointer, metadata_location: pointer.previous_metadata_location }));
    }
    return bytes;
  });
  setObjectPathResolver((uri) => fixturePath(uri));
});

beforeEach(() => {
  respond = fixtureTail;
  seen.length = 0;
  ordersPointer = 'current';
  resetTables();
});

afterEach(async () => {
  await resetRuns();
});

after(async () => {
  setObjectReader(null);
  setObjectPathResolver(null);
  resetTables();
  resetSnapshotCaches();
  server.closeAllConnections();
  server.close();
  await rm(dataDir, { recursive: true, force: true });
});

/* ---------------------------------------------------------------------------------- browse */

describe('a browse run with a live tail', () => {
  it('reads the records past its snapshot first, with the person’s session and the console’s credential', async () => {
    const run = await started();

    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.equal(run.rows, COMMITTED_ROWS + answer('orders', 0).rows);
    assert.equal(run.totalExact, true);
    const first = (await run.readPage(0, answer('orders', 0).rows)).rows.map((row) => `${row._kafka_partition}:${row._kafka_offset}`);
    assert.deepEqual(first.sort(), offsetsOf(answer('orders', 0)).sort(), 'the tail is the newest of all');
    const tail = run.snapshot().tail;
    assert.deepEqual(
      [tail?.status, tail?.rows, tail?.segments, tail?.through, tail?.behind, tail?.member],
      ['complete', 4, 1, answer('orders', 0).through, 0, 'materializer-tail-app-0'],
    );
    assert.deepEqual(run.snapshot().tables[0]?.tail?.through, answer('orders', 0).through);
    assert.equal(run.snapshot().revision, 0);

    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0]?.after, ORDERS_TAIL.committed, 'past the snapshot’s offsets');
    assert.equal(seen[0]?.headers.authorization, `Bearer ${PERSON.token}`);
    assert.equal(seen[0]?.headers['x-materializer-console'], config.serviceToken);
    assert.equal(seen[0]?.query.get('max_rows'), '100000');
    assert.equal(seen[0]?.query.get('wait_ms'), '2500', 'the tail app answers before the console gives up');
  });

  it('holds every record once, its snapshot’s and its tail’s, paged either way', async () => {
    const expected = [...COMMITTED, ...offsetsOf(answer('orders', 0))].sort();
    for (const order of ['newest', 'oldest'] as const) {
      const run = await started({ order });
      const offsets = await everyOffset(run, 3);
      assert.equal(new Set(offsets).size, offsets.length, `${order}: a row twice`);
      assert.deepEqual([...offsets].sort(), expected, order);
      const tailRows = new Set(offsetsOf(answer('orders', 0)));
      const positions = offsets.map((offset, index) => (tailRows.has(offset) ? index : -1)).filter((index) => index >= 0);
      assert.deepEqual(
        positions,
        order === 'newest' ? [0, 1, 2, 3] : [12, 13, 14, 15],
        `${order}: the tail's rows are the newest`,
      );
    }
  });

  it('prunes the tail by its time bounds like any file, and filters count it', async () => {
    const tail = answer('orders', 0);
    const inTail = await started({ range: { from: tail.time_min, to: null } });
    assert.equal(inTail.rows, tail.rows);
    assert.equal(inTail.progress.objectsRead, 0, 'wholly inside: its row count is the answer');
    const before = await started({ range: { from: null, to: tail.time_min } });
    assert.equal(before.rows, COMMITTED_ROWS);
    assert.equal(before.snapshot().tail?.rows, 4, 'fetched, and outside the range');

    const filtered = await started({ filters: [{ column: 'order_id', operator: 'eq', value: 'O-20' }] });
    assert.equal(filtered.rows, 1);
    assert.equal((await filtered.readPage(0, 5)).rows[0]?.order_id, 'O-20');
  });

  it('reads no tail as of an older snapshot, nor when the request says no', async () => {
    const pinned = await startRun({
      materializationId: MATERIALIZATION,
      ownerId: 'usr-test',
      tables: TABLES,
      request: { kind: 'browse', range: ALL_TIME, asOf: await currentSnapshot() },
      tail: access(),
    });
    await settled(pinned);
    const refused = await started({ liveTail: false });
    for (const run of [pinned, refused]) {
      assert.equal(run.rows, COMMITTED_ROWS);
      assert.equal(run.snapshot().tail, undefined);
    }
    assert.equal(seen.length, 0);
  });

  it('keeps its tail files in the run’s own directory, gone with the run', async () => {
    const run = await started();
    const directory = join(config.runDir, run.id);
    assert.deepEqual(await readdir(directory), ['tail-orders-0.parquet']);
    await run.dispose();
    await assert.rejects(readdir(directory));
  });
});

/* ------------------------------------------------------------------------------- statements */

describe('a statement with live tails', () => {
  it('reads each table with its tail, natively too', async () => {
    const run = await started({ kind: 'sql', sql: 'SELECT count(*) AS n FROM shop.orders' });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.equal(Number((await run.readPage(0, 1)).rows[0]?.n), COMMITTED_ROWS + 4);
    const native = await started({ kind: 'sql', sql: "SELECT count(*) AS n FROM iceberg.shop.orders WHERE order_id = 'O-20'" });
    assert.equal(Number((await native.readPage(0, 1)).rows[0]?.n), 1);
  });

  it('joins two tables each with a tail', async () => {
    const run = await started({
      kind: 'sql',
      sql: 'SELECT o.order_id, p.payment_id FROM shop.orders o JOIN finance.payments p ON p.order_id = o.order_id ORDER BY 1',
    });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    const rows = (await run.readPage(0, 20)).rows.map((row) => `${row.order_id}/${row.payment_id}`);
    // O-0 to O-5 are committed in both; O-12 is in both tails; O-13's payment is in the tail, its order not yet.
    assert.deepEqual(rows, ['O-0/P-0', 'O-1/P-1', 'O-12/P-12', 'O-2/P-2', 'O-3/P-3', 'O-4/P-4', 'O-5/P-5']);
    assert.deepEqual(run.snapshot().tables.map((table) => [table.name, table.tail?.rows]), [
      ['orders', 4],
      ['payments', 2],
    ]);
    assert.deepEqual((await readdir(join(config.runDir, run.id))).sort(), ['result.parquet', 'tail-orders-0.parquet', 'tail-payments-0.parquet']);
  });

  it('is never refreshed in place: a refresh is a new run', async () => {
    const run = await started({ kind: 'sql', sql: 'SELECT count(*) AS n FROM shop.orders' });
    const outcome = await refreshed(run);
    assert.equal(outcome.replaced, true);
    assert.notEqual(outcome.run.id, run.id);
    assert.equal(Number((await outcome.run.readPage(0, 1)).rows[0]?.n), COMMITTED_ROWS + 4);
  });
});

/* --------------------------------------------------------------------------------- refresh */

describe('refreshing a run', () => {
  it('appends only the records past its tail’s through, and bumps its revision', async () => {
    const run = await started();
    const outcome = await refreshed(run);

    assert.equal(outcome.replaced, false);
    assert.equal(outcome.run, run);
    assert.equal(seen.at(-1)?.query.get('after'), JSON.stringify(answer('orders', 0).through));
    assert.equal(run.rows, COMMITTED_ROWS + 4 + 2);
    assert.equal(run.snapshot().revision, 1);
    const tail = run.snapshot().tail;
    assert.deepEqual([tail?.segments, tail?.rows, tail?.through], [2, 6, answer('orders', 1).through]);
    const first = (await run.readPage(0, 2)).rows.map((row) => `${row._kafka_partition}:${row._kafka_offset}`);
    assert.deepEqual(first.sort(), offsetsOf(answer('orders', 1)).sort(), 'the newest records first');
    const offsets = await everyOffset(run, 4);
    assert.deepEqual([...offsets].sort(), [...COMMITTED, ...offsetsOf(answer('orders', 0)), ...offsetsOf(answer('orders', 1))].sort());
    assert.equal(new Set(offsets).size, offsets.length);

    // Nothing new: the run, its rows and its pages stay as they are.
    await refreshed(run);
    assert.deepEqual([run.rows, run.snapshot().revision, run.snapshot().tail?.status], [18, 1, 'complete']);
  });

  it('starts a new run once the table has a newer snapshot, with its range resolved again', async () => {
    ordersPointer = 'previous';
    const old = await started({ range: { from: 1_000, to: null }, rangeLabel: 'Last 15 minutes' });
    assert.notEqual(old.snapshotId, await currentSnapshot());
    ordersPointer = 'current';
    resetTables();

    const outcome = await refreshed(old, access(), { from: 2_000, to: null });

    assert.equal(outcome.replaced, true);
    assert.equal(outcome.run.snapshotId, await currentSnapshot());
    assert.deepEqual(outcome.run.request.range, { from: 2_000, to: null });
    assert.equal(outcome.run.request.rangeLabel, 'Last 15 minutes');
    assert.equal(outcome.run.rows, COMMITTED_ROWS + 4);
  });

  it('says why when the tail cannot be had, and takes it once it can', async () => {
    respond = () => refusalAnswer(503, 'tail_not_ready', 'The live tail is warming up for this table.', { state: 'seeding' });
    const run = await started();
    assert.equal(run.state, 'succeeded');
    assert.equal(run.rows, COMMITTED_ROWS);
    assert.deepEqual([run.snapshot().tail?.status, run.snapshot().tail?.reason, run.snapshot().tail?.state], ['unavailable', 'not_ready', 'seeding']);

    respond = () => refusalAnswer(409, 'tail_rebalancing', 'This table is moving between the tail’s members.');
    await refreshed(run);
    assert.deepEqual([run.snapshot().tail?.reason, run.rows], ['rebalancing', COMMITTED_ROWS]);

    respond = fixtureTail;
    const outcome = await refreshed(run);
    assert.equal(outcome.replaced, false);
    assert.equal(seen.at(-1)?.query.get('after'), JSON.stringify(ORDERS_TAIL.committed), 'from the snapshot still');
    assert.deepEqual([run.rows, run.snapshot().tail?.status, run.snapshot().revision], [COMMITTED_ROWS + 4, 'complete', 1]);
  });

  it('keeps a tail it cannot read off the run, which answers with its committed rows', async () => {
    respond = () => ({
      status: 200,
      headers: { ...headersOf(answer('orders', 0)), 'x-tail-bytes': '72' },
      body: Buffer.concat([Buffer.from('PAR1'), Buffer.alloc(64, 7), Buffer.from('PAR1')]),
    });
    const run = await started();
    assert.equal(run.state, 'succeeded');
    assert.equal(run.rows, COMMITTED_ROWS);
    assert.deepEqual([run.snapshot().tail?.status, run.snapshot().tail?.reason], ['unavailable', 'unreadable']);
    assert.deepEqual(await readdir(join(config.runDir, run.id)).catch(() => []), [], 'no file kept');
  });

  it('goes on with committed rows when the tail does not answer in time', async () => {
    respond = () => ({ status: 200, hang: true });
    const startedAt = Date.now();
    const run = await started({}, access({ timeoutMs: 400 }));
    assert.ok(Date.now() - startedAt < 2_500, 'within the timeout');
    assert.equal(run.state, 'succeeded');
    assert.deepEqual([run.rows, run.snapshot().tail?.status, run.snapshot().tail?.reason], [COMMITTED_ROWS, 'unavailable', 'timeout']);
  });

  it('serializes refreshes of one run rather than fetching twice', async () => {
    const run = await started();
    respond = async (request) => ({ ...(await fixtureTail(request)), delayMs: 150 });
    const [one, two] = await Promise.all([refreshed(run), refreshed(run)]);
    assert.equal(one.run, run);
    assert.equal(two.run, run);
    assert.equal(seen.filter((request) => sameOffsets(request.after, answer('orders', 0).through)).length, 1);
    assert.equal(run.rows, COMMITTED_ROWS + 6);
  });

  it('waits while an export reads the run, so the download holds every row once', async () => {
    const run = await started();
    const download = await openExport(run, 'csv');
    const before = seen.length;
    const held = await refreshed(run);
    assert.equal(held.replaced, false);
    assert.equal(seen.length, before, 'no fetch while the export reads');
    let text = '';
    for await (const chunk of download.stream) text += String(chunk);
    await download.cleanup();
    const lines = text.trim().split('\n');
    assert.equal(lines.length, 1 + COMMITTED_ROWS + 4);
    assert.equal(new Set(lines).size, lines.length, 'no row twice');
    await refreshed(run);
    assert.equal(run.rows, COMMITTED_ROWS + 6, 'and Live goes on after it');
  });

  it('turns the tail off when live tail goes off, keeps what it read, and fetches no more', async () => {
    const run = await started();
    const before = seen.length;
    const off = access({ off: { reason: 'materialization', message: 'Live tail is off for this materialization.' } });
    const outcome = await refreshed(run, off);
    assert.equal(outcome.replaced, false);
    assert.deepEqual([run.snapshot().tail?.status, run.snapshot().tail?.reason, run.rows], ['off', 'materialization', COMMITTED_ROWS + 4]);
    await refreshed(run, off);
    assert.equal(seen.length, before, 'no fetch while it is off');
  });

  it('starts a new run when a tail switched off is on again', async () => {
    for (const reason of ['console', 'control_plane', 'materialization', 'stopped']) {
      const switched = await started({}, access({ off: { reason, message: 'Live tail is off.' } }));
      assert.equal(switched.snapshot().tail?.status, 'off');
      const still = await refreshed(switched, access({ off: { reason, message: 'Live tail is off.' } }));
      assert.equal(still.replaced, false, `${reason}: not while it is off`);
      const outcome = await refreshed(switched, access());
      assert.equal(outcome.replaced, true, reason);
      assert.equal(outcome.run.rows, COMMITTED_ROWS + 4);
      assert.equal(outcome.run.snapshot().tail?.status, 'complete');
    }
  });

  it('never starts one for a tail no tail app is configured for', async () => {
    const run = await started({}, access({ off: { reason: 'not_configured', message: 'No live tail is configured for this console.' } }));
    const before = seen.length;
    const outcome = await refreshed(run, access());
    assert.equal(outcome.replaced, false);
    assert.equal(seen.length, before);
  });

  it('asks again for a table the tail app does not follow yet, and follows it once it does', async () => {
    respond = () => refusalAnswer(404, 'live_tail_off', 'This table is not tailed.');
    const run = await started();
    assert.deepEqual([run.snapshot().tail?.status, run.snapshot().tail?.reason, run.rows], ['unavailable', 'not_tailed', COMMITTED_ROWS]);
    const before = seen.length;
    await refreshed(run, access());
    assert.equal(seen.length, before + 1, 'asked again: a resume or a tick brings the table back');
    respond = fixtureTail;
    const outcome = await refreshed(run, access());
    assert.equal(outcome.replaced, false, 'the same snapshot, so the same run');
    assert.deepEqual([run.snapshot().tail?.status, run.rows], ['complete', COMMITTED_ROWS + 4]);
  });

  it('replaces a run that was cancelled or failed', async () => {
    respond = () => ({ status: 200, hang: true });
    const run = await startRun({ materializationId: MATERIALIZATION, ownerId: 'usr-test', tables: TABLES, request: { kind: 'browse', range: ALL_TIME }, tail: access() });
    run.cancel();
    await settled(run);
    assert.equal(run.state, 'cancelled');
    respond = fixtureTail;
    const outcome = await refreshed(run);
    assert.equal(outcome.replaced, true);
    assert.equal(outcome.run.rows, COMMITTED_ROWS + 4);
  });

  it('pages its committed rows alone when a tail file is gone from disk', async () => {
    const run = await started();
    await unlink(join(config.runDir, run.id, 'tail-orders-0.parquet'));
    const page = await run.readPage(0, 20);
    assert.equal(page.rows.length, COMMITTED_ROWS);
    assert.deepEqual([run.rows, run.snapshot().tail?.status, run.snapshot().tail?.reason, run.snapshot().revision], [COMMITTED_ROWS, 'unavailable', 'lost', 1]);
  });

  it('is refused for an expired run, whose id no longer exists', async () => {
    const run = await started();
    assert.equal(getRun(run.id, 'usr-test'), run);
    await evictIdleRuns(Date.now() + config.runIdleRetentionMs + 1);
    assert.throws(() => getRun(run.id, 'usr-test'), (error: { statusCode?: number }) => error.statusCode === 404);
  });

  it('lets the run a new one replaced expire within a minute unless it is read', async () => {
    ordersPointer = 'previous';
    const old = await started();
    ordersPointer = 'current';
    resetTables();
    const outcome = await refreshed(old);
    assert.equal(outcome.replaced, true);
    await evictIdleRuns(Date.now() + 61_000);
    assert.throws(() => getRun(old.id, 'usr-test'));
    assert.equal(getRun(outcome.run.id, 'usr-test'), outcome.run);
  });
});

async function currentSnapshot(): Promise<string> {
  const text = await readFile(join(FIXTURE_DIR, 'expected.json'), 'utf8');
  return /"current_snapshot_id": (\d+)/.exec(text)?.[1] as string;
}
