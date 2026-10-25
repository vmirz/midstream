/**
 * The live tail's client: every answer the tail app can give — rows, each refusal, a failure on the
 * way — becomes a `TailOutcome`, never a thrown error, and a peek's records come out in the table's
 * own columns, newest first.
 *
 * A stub of the tail app's HTTP API answers; the rows are the fixture's tail files, written by the
 * tail app's own code (`python/tests/console_fixture.py`).
 */

import assert from 'node:assert/strict';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, beforeEach, describe, it } from 'node:test';

type Handler = (request: IncomingMessage, response: ServerResponse) => void | Promise<void>;
let handler: Handler = (_request, response) => {
  response.writeHead(500).end();
};
const requests: IncomingMessage[] = [];
const server = createServer((request, response) => {
  requests.push(request);
  void handler(request, response);
});
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
const port = (server.address() as AddressInfo).port;

const dataDir = await mkdtemp(join(tmpdir(), 'console-tail-client-'));
process.env.CONSOLE_DATA_DIR = dataDir;
process.env.CONSOLE_TAIL_URL = `http://127.0.0.1:${port}`;
process.env.CONSOLE_SERVICE_TOKEN = 'console-service-credential-0123456789abcdef';

const { fetchHead, fetchTail, readPeek } = await import('./tail.ts');
const { config } = await import('../config.ts');
const { setObjectReader } = await import('../iceberg/io.ts');
const { loadTable, resetTables } = await import('../iceberg/table.ts');
const { currentSchema } = await import('../iceberg/metadata.ts');
const { FORK_MAP_PROPERTY, parseForkMap } = await import('../iceberg/schema.ts');
const { FIXTURE_DIR, FIXTURE_LOCATION, fixtureReader } = await import('../iceberg/testing.ts');
const { runQuery } = await import('./duckdb.ts');
const { readParquet } = await import('./logical.ts');

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

const expected = JSON.parse(await readFile(join(FIXTURE_DIR, 'expected.json'), 'utf8')) as { tail: { committed: Record<string, number>; answers: FixtureAnswer[] } };
const FIRST = expected.tail.answers[0] as FixtureAnswer;
const FIRST_BYTES = await readFile(join(FIXTURE_DIR, FIRST.file));
const PERSON = { kind: 'user' as const, token: 'tok-person-0001' };
const REQUEST = { materializationId: 'mat-a1b2c3d4e5f6', table: 'orders', after: expected.tail.committed, maxRows: 500, timeoutMs: 2_000 };

function rowsHeaders(answer: FixtureAnswer): Record<string, string> {
  return {
    'x-tail-rows': String(answer.rows),
    'x-tail-bytes': String(answer.bytes),
    'x-tail-after': JSON.stringify(answer.after),
    'x-tail-through': JSON.stringify(answer.through),
    'x-tail-end-offsets': JSON.stringify(answer.end_offsets),
    'x-tail-status': 'truncated',
    'x-tail-paused': '[1]',
    'x-tail-gap': '[]',
    'x-tail-schema-id': String(answer.schema_id),
    'x-tail-settling': '3',
    'x-tail-lag-ms': '1250',
    'x-tail-member': 'materializer-tail-app-1',
    'x-tail-state': 'paused',
    'x-tail-table-uuid': 'uuid-1',
    'x-tail-generated-at': '2026-10-23T12:00:00.000Z',
    'x-tail-time-min': String(answer.time_min),
    'x-tail-time-max': String(answer.time_max),
  };
}

/** An error as the tail app sends it while Kaspr has one content type per view: JSON, as bytes. */
function refuse(status: number, reason: string, details: Record<string, unknown> = {}): Handler {
  return (_request, response) => {
    response
      .writeHead(status, { 'content-type': 'application/octet-stream' })
      .end(Buffer.from(JSON.stringify({ error: { code: 'error', message: `refused: ${reason}`, details: { reason, ...details } } })));
  };
}

beforeEach(() => {
  requests.length = 0;
});

after(async () => {
  setObjectReader(null);
  resetTables();
  server.closeAllConnections();
  server.close();
  await rm(dataDir, { recursive: true, force: true });
});

describe('fetching a tail', () => {
  it('reads the rows and every header, asking with the person’s session and the console’s credential', async () => {
    handler = (_request, response) => {
      response.writeHead(200, { 'content-type': 'application/octet-stream', ...rowsHeaders(FIRST) }).end(FIRST_BYTES);
    };
    const outcome = await fetchTail(PERSON, REQUEST);
    assert.equal(outcome.kind, 'rows');
    if (outcome.kind !== 'rows') return;
    assert.deepEqual(
      {
        rows: outcome.rows,
        status: outcome.status,
        through: outcome.through,
        endOffsets: outcome.endOffsets,
        paused: outcome.paused,
        schemaId: outcome.schemaId,
        settling: outcome.settling,
        timeMin: outcome.timeMin,
        timeMax: outcome.timeMax,
        lagMs: outcome.lagMs,
        state: outcome.state,
        member: outcome.member,
        tableUuid: outcome.tableUuid,
        generatedAt: outcome.generatedAt,
      },
      {
        rows: FIRST.rows,
        status: 'truncated',
        through: FIRST.through,
        endOffsets: FIRST.end_offsets,
        paused: [1],
        schemaId: FIRST.schema_id,
        settling: 3,
        timeMin: FIRST.time_min,
        timeMax: FIRST.time_max,
        lagMs: 1250,
        state: 'paused',
        member: 'materializer-tail-app-1',
        tableUuid: 'uuid-1',
        generatedAt: '2026-10-23T12:00:00.000Z',
      },
    );
    assert.ok(outcome.bytes.equals(FIRST_BYTES));
    const asked = requests[0] as IncomingMessage;
    const url = new URL(asked.url ?? '/', 'http://stub');
    assert.equal(url.pathname, '/tail/v1/materializations/mat-a1b2c3d4e5f6/tables/orders');
    assert.deepEqual(JSON.parse(url.searchParams.get('after') ?? ''), expected.tail.committed);
    assert.equal(url.searchParams.get('max_rows'), '500');
    assert.equal(url.searchParams.get('wait_ms'), '1500');
    assert.equal(asked.headers.authorization, `Bearer ${PERSON.token}`);
    assert.equal(asked.headers['x-console-token'], config.serviceToken);
  });

  it('turns every refusal into an outcome the status line can tell apart', async () => {
    const cases: Array<[Handler, Record<string, unknown>]> = [
      [refuse(404, 'live_tail_off'), { kind: 'unavailable', reason: 'not_tailed', status: 404 }],
      [refuse(409, 'tail_rebalancing'), { kind: 'unavailable', reason: 'rebalancing', status: 409 }],
      [refuse(409, 'tail_unavailable', { partitions: { '0': 'behind_retention' } }), { kind: 'unavailable', reason: 'unavailable_for_snapshot' }],
      [refuse(503, 'tail_not_ready', { state: 'catching_up' }), { kind: 'unavailable', reason: 'not_ready', state: 'catching_up' }],
      [refuse(503, 'topic_unavailable'), { kind: 'unavailable', reason: 'topic_unavailable' }],
      [refuse(503, 'tail_owner_unreachable'), { kind: 'unavailable', reason: 'unreachable' }],
      [refuse(503, 'control_plane_unreachable'), { kind: 'unavailable', reason: 'unreachable' }],
      [refuse(401, 'session_ended'), { kind: 'unavailable', reason: 'forbidden', status: 401 }],
      [refuse(403, 'console_only'), { kind: 'unavailable', reason: 'forbidden', status: 403 }],
      [refuse(400, 'invalid_request'), { kind: 'unavailable', reason: 'rejected' }],
      [(_request, response) => void response.writeHead(404).end('no such route'), { kind: 'unavailable', reason: 'unreachable', status: 404 }],
      [(_request, response) => void response.writeHead(502).end('<html>bad gateway</html>'), { kind: 'unavailable', reason: 'unreachable', status: 502 }],
    ];
    for (const [answer, want] of cases) {
      handler = answer;
      const outcome = (await fetchTail(PERSON, REQUEST)) as unknown as Record<string, unknown>;
      for (const [key, value] of Object.entries(want)) assert.equal(outcome[key], value, `${JSON.stringify(want)}: ${key}`);
    }
  });

  it('reads a backlog’s size and offsets from the refusal', async () => {
    handler = refuse(409, 'tail_backlogged', { behind: 2_500_000, end_offsets: { '0': 3_000_000 }, committed: { '0': 499_999 } });
    const outcome = await fetchTail(PERSON, REQUEST);
    assert.deepEqual(outcome, {
      kind: 'backlogged',
      behind: 2_500_000,
      endOffsets: { '0': 3_000_000 },
      committed: { '0': 499_999 },
      message: 'refused: tail_backlogged',
    });
  });

  it('says unavailable, never throws, for an answer that is not a tail', async () => {
    const half = FIRST_BYTES.subarray(0, 100);
    const cases: Array<[string, Handler, string]> = [
      [
        'a body shorter than its X-Tail-Bytes',
        (_request, response) => void response.writeHead(200, { ...rowsHeaders(FIRST), 'transfer-encoding': 'chunked' }).end(half),
        'unreadable',
      ],
      [
        'no X-Tail-Through',
        (_request, response) => {
          const { 'x-tail-through': _through, ...headers } = rowsHeaders(FIRST);
          response.writeHead(200, headers).end(FIRST_BYTES);
        },
        'unreadable',
      ],
      ['an X-Tail-Through that does not read', (_request, response) => void response.writeHead(200, { ...rowsHeaders(FIRST), 'x-tail-through': '{"0": "x"}' }).end(FIRST_BYTES), 'unreadable'],
      ['a 200 that is not Parquet', (_request, response) => void response.writeHead(200, { ...rowsHeaders(FIRST), 'x-tail-bytes': '12' }).end('{"rows": []} '), 'unreadable'],
      [
        'through before the caller’s after',
        (_request, response) => void response.writeHead(200, { ...rowsHeaders(FIRST), 'x-tail-through': JSON.stringify({ '0': 3, '1': 11 }) }).end(FIRST_BYTES),
        'unreadable',
      ],
      [
        'a connection reset mid-body',
        (_request, response) => {
          response.writeHead(200, { ...rowsHeaders(FIRST), 'content-length': String(FIRST_BYTES.length) });
          response.write(half);
          setTimeout(() => response.socket?.destroy(), 20);
        },
        'unreachable',
      ],
    ];
    for (const [name, answer, reason] of cases) {
      handler = answer;
      const outcome = await fetchTail(PERSON, REQUEST);
      assert.equal(outcome.kind, 'unavailable', name);
      if (outcome.kind === 'unavailable') assert.equal(outcome.reason, reason, name);
    }
  });

  it('gives up at its timeout, and says so', async () => {
    handler = () => undefined;
    const startedAt = Date.now();
    const outcome = await fetchTail(PERSON, { ...REQUEST, timeoutMs: 300 });
    assert.ok(Date.now() - startedAt < 1_500);
    assert.deepEqual([outcome.kind, outcome.kind === 'unavailable' ? outcome.reason : null], ['unavailable', 'timeout']);
  });

  it('goes without a person’s token when there is none, which the tail app refuses itself', async () => {
    handler = refuse(401, 'sign_in_required');
    const outcome = await fetchTail({ kind: 'user', token: null }, REQUEST);
    assert.equal(requests[0]?.headers.authorization, undefined);
    assert.equal(outcome.kind === 'unavailable' && outcome.reason, 'forbidden');
  });
});

describe('the tail app’s files (E5 for the tail)', () => {
  it('read through the table’s schema by field id hold exactly the rows the tail app built, within their time bounds', async () => {
    setObjectReader(fixtureReader());
    const table = await loadTable(FIXTURE_LOCATION, { fresh: true });
    assert.ok(table);
    const schema = currentSchema(table.metadata);
    assert.ok(schema);
    for (const answer of expected.tail.answers) {
      const path = join(FIXTURE_DIR, answer.file);
      const result = await runQuery(
        `SELECT _kafka_partition AS p, _kafka_offset AS o, order_id, total__v2 AS total, epoch_ms(_kafka_timestamp) AS ms ` +
          `FROM ${readParquet([path], schema)} ORDER BY 1, 2`,
      );
      assert.deepEqual(
        result.rows.map((row) => [row.p, Number(row.o), row.order_id, row.total]),
        answer.served.map((row) => [row._kafka_partition, row._kafka_offset, row.order_id, row.total__v2]),
        answer.file,
      );
      const times = result.rows.map((row) => Number(row.ms));
      assert.deepEqual(
        times.length === 0 ? [null, null] : [Math.min(...times), Math.max(...times)],
        [answer.time_min, answer.time_max],
        `${answer.file}: the bounds its headers carry are its rows' own`,
      );
    }
  });
});

describe('a peek at the topic’s newest records', () => {
  it('reads its headers', async () => {
    handler = (_request, response) => {
      response
        .writeHead(200, {
          'x-tail-peek-rows': String(FIRST.rows),
          'x-tail-peek-offsets': JSON.stringify({ '0': [10, 12], '1': [12, 12] }),
          'x-tail-end-offsets': JSON.stringify(FIRST.end_offsets),
          'x-tail-behind': '2500000',
          'x-tail-state': 'backlogged',
          'x-tail-schema-id': String(FIRST.schema_id),
          'x-tail-settling': '0',
          'x-tail-member': 'materializer-tail-app-0',
          'x-tail-generated-at': '2026-10-23T12:00:00.000Z',
        })
        .end(FIRST_BYTES);
    };
    const outcome = await fetchHead(PERSON, { materializationId: 'mat-a1b2c3d4e5f6', table: 'orders', rows: 200, timeoutMs: 2_000 });
    assert.equal(outcome.kind, 'rows');
    if (outcome.kind !== 'rows') return;
    assert.deepEqual([outcome.rows, outcome.offsets, outcome.behind, outcome.state], [FIRST.rows, { '0': [10, 12], '1': [12, 12] }, 2_500_000, 'backlogged']);
    assert.equal(new URL(requests[0]?.url ?? '/', 'http://stub').pathname, '/tail/v1/materializations/mat-a1b2c3d4e5f6/tables/orders/head');
    assert.equal(new URL(requests[0]?.url ?? '/', 'http://stub').searchParams.get('rows'), '200');
  });

  it('shapes the records as the table’s columns — forks folded, nested fields named — newest first, and leaves no file', async () => {
    setObjectReader(fixtureReader());
    const table = await loadTable(FIXTURE_LOCATION, { fresh: true });
    assert.ok(table);
    const metadata = table.metadata;
    const { rows, columns } = await readPeek(FIRST_BYTES, { schema: currentSchema(metadata), forkMap: parseForkMap(metadata.properties[FORK_MAP_PROPERTY]) }, 3);

    const names = columns.map((column) => column.name);
    assert.ok(names.includes('total') && !names.includes('total__v2'), 'one column for the fork family');
    assert.ok(names.includes('customer.email'), 'nested fields by their paths');
    const newest = [...FIRST.served].sort((a, b) => String(b._kafka_timestamp).localeCompare(String(a._kafka_timestamp))).slice(0, 3);
    assert.deepEqual(
      rows.map((row) => [row.order_id, row.total, row['customer.email']]),
      newest.map((row) => [row.order_id, row.total__v2, (row.customer as { email: string }).email]),
    );
    assert.deepEqual(await readdir(join(config.runDir, 'peeks')), []);
  });
});
