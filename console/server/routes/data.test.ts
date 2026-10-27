/**
 * The Data tab's live tail routes: a run reads its tail with the person's session, a refresh appends
 * to the run or replaces it, and the head peek reads the topic's newest records in the table's
 * columns — each only for someone who may query the table's dataset.
 *
 * A stub of the tail app answers with the fixture's tail files, written by the tail app's own code.
 */

import assert from 'node:assert/strict';
import { createServer, type IncomingMessage } from 'node:http';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, afterEach, before, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

interface Asked {
  path: string;
  query: URLSearchParams;
  headers: IncomingMessage['headers'];
}
const asked: Asked[] = [];
let tailAnswers: Array<{ after: Record<string, number>; file: string; headers: Record<string, string> }> = [];
let headOff = false;

const tail = createServer((request, response) => {
  const url = new URL(request.url ?? '/', 'http://stub');
  asked.push({ path: url.pathname, query: url.searchParams, headers: request.headers });
  void (async () => {
    if (url.pathname.endsWith('/head')) {
      if (headOff) {
        response.writeHead(404).end(JSON.stringify({ error: { code: 'not_found', message: 'not tailed', details: { reason: 'live_tail_off' } } }));
        return;
      }
      const first = tailAnswers[0];
      response
        .writeHead(200, {
          'x-tail-peek-rows': '4',
          'x-tail-peek-offsets': JSON.stringify({ '0': [10, 12], '1': [12, 12] }),
          'x-tail-end-offsets': JSON.stringify({ '0': 12, '1': 12 }),
          'x-tail-behind': '1200000',
          'x-tail-state': 'backlogged',
          'x-tail-schema-id': '1',
          'x-tail-settling': '0',
          'x-tail-member': 'materializer-tail-app-0',
          'x-tail-generated-at': '2026-10-23T12:00:00.000Z',
        })
        .end(await readFile(join(FIXTURE_DIR, first?.file ?? '')));
      return;
    }
    const after = JSON.parse(url.searchParams.get('after') ?? '{}') as Record<string, number>;
    const found = tailAnswers.find((candidate) => JSON.stringify(candidate.after) === JSON.stringify(after));
    if (!found) {
      response.writeHead(409).end(JSON.stringify({ error: { code: 'conflict', message: 'too far back', details: { reason: 'tail_unavailable' } } }));
      return;
    }
    response.writeHead(200, found.headers).end(await readFile(join(FIXTURE_DIR, found.file)));
  })();
});
await new Promise<void>((resolve) => tail.listen(0, '127.0.0.1', resolve));

const dataDir = await mkdtemp(join(tmpdir(), 'console-data-routes-'));
process.env.CONSOLE_DATA_DIR = dataDir;
/** The domain's service token: what its tail app requires of the console. */
const SERVICE_TOKEN = 'console-service-credential-0123456789abcdef';

const { installTestAuth, testPrincipal } = await import('../auth/testing.ts');
const { installTestDomains } = await import('../domains/testing.ts');
const domains = await installTestDomains(['domain_a'], {
  entries: { domain_a: { tailUrl: `http://127.0.0.1:${(tail.address() as AddressInfo).port}`, serviceToken: SERVICE_TOKEN } },
});
const domain = domains.require('domain_a');
const { ConsoleError } = await import('../errors.ts');
const { setObjectReader } = await import('../iceberg/io.ts');
const { setObjectPathResolver } = await import('../data/objects.ts');
const { resetTables } = await import('../iceberg/table.ts');
const { FIXTURE_DIR, FIXTURE_LOCATION, fixturePath, fixtureReader } = await import('../iceberg/testing.ts');
const { resetRuns } = await import('../runs/store.ts');
const { registerDataRoutes, registerRunRoutes } = await import('./data.ts');
const { config } = await import('../config.ts');

import type { CpDetail, CpPrincipal, CpSystemSettings, HeadPeek, RunRefresh, RunSnapshot } from '../../shared/types.ts';

const SALES = 'ds-000000000001';
const MATERIALIZATION = 'mat-a1b2c3d4e5f6';

interface FixtureAnswer {
  file: string;
  after: Record<string, number>;
  through: Record<string, number>;
  end_offsets: Record<string, number>;
  rows: number;
  bytes: number;
  schema_id: number;
  time_min: number | null;
  time_max: number | null;
}
const FIXTURE_TAIL = (JSON.parse(await readFile(join(FIXTURE_DIR, 'expected.json'), 'utf8')) as { tail: { answers: FixtureAnswer[] } }).tail.answers;

function answerHeaders(answer: FixtureAnswer): Record<string, string> {
  return {
    'x-tail-rows': String(answer.rows),
    'x-tail-bytes': String(answer.bytes),
    'x-tail-through': JSON.stringify(answer.through),
    'x-tail-end-offsets': JSON.stringify(answer.end_offsets),
    'x-tail-status': answer.rows === 0 ? 'empty' : 'complete',
    'x-tail-schema-id': String(answer.schema_id),
    'x-tail-settling': '0',
    'x-tail-lag-ms': '0',
    'x-tail-state': 'live',
    'x-tail-member': 'materializer-tail-app-0',
    ...(answer.time_min === null ? {} : { 'x-tail-time-min': String(answer.time_min), 'x-tail-time-max': String(answer.time_max) }),
  };
}

/** Someone who may query sales — and the same person once that is taken away. */
function reader(): CpPrincipal {
  return testPrincipal(['materializations.view', 'materializations.view_details'], { dataset_permissions: { [SALES]: ['data.query', 'dataset.view_schema'] } });
}
function formerReader(): CpPrincipal {
  return testPrincipal(['materializations.view', 'materializations.view_details'], { dataset_permissions: { [SALES]: ['dataset.view_schema'] } });
}

let liveTailOn = true;

function detail(): CpDetail {
  return {
    materialization: {
      materialization_id: MATERIALIZATION,
      name: 'Shop',
      tables: [{ name: 'orders', source_topic: 'orders', dataset: 'sales', dataset_id: SALES }],
      retired_tables: [],
      source_topics: ['orders'],
      destination_uri: 'gs://lake',
      runtime_mode: 'dedicated',
      namespace: 'midstream',
      datasets: ['sales'],
      live_tail: liveTailOn,
    },
    status: {
      desired_phase: 'running',
      observed_phase: 'running',
      phase_reason: 'runtime_ready',
      phase_message: null,
      last_transition_time: '2026-10-22T00:00:00Z',
      last_heartbeat: null,
      last_resource_observation_time: null,
      runtime_health: { level: 'healthy', reason: 'heartbeat_fresh', message: null },
    },
    runtime_binding: null,
    tables: [
      {
        dataset: 'sales',
        dataset_id: SALES,
        name: 'orders',
        source_topic: 'orders',
        identifier: 'sales.orders',
        location: FIXTURE_LOCATION,
        partition_granularity: 'day',
        schema_id: 1,
        current_snapshot_id: null,
        last_commit_at: null,
        valid_through: null,
        totals: {},
        last_error: null,
        state: 'active',
      },
    ],
  } as unknown as CpDetail;
}

const getDetail = domain.live.getDetail.bind(domain.live);
const settings = domain.controlPlane.settings.bind(domain.controlPlane);

before(() => {
  setObjectReader(fixtureReader());
  setObjectPathResolver((uri) => fixturePath(uri));
});

beforeEach(() => {
  asked.length = 0;
  liveTailOn = true;
  headOff = false;
  tailAnswers = FIXTURE_TAIL.map((answer) => ({ after: answer.after, file: answer.file, headers: answerHeaders(answer) }));
  domain.live.getDetail = async () => detail();
  domain.controlPlane.settings = async () => ({ live_tail_enabled: true }) as CpSystemSettings;
  domain.invalidateSystemSettings();
  resetTables();
});

afterEach(async () => {
  domain.live.getDetail = getDetail;
  domain.controlPlane.settings = settings;
  await resetRuns();
});

after(async () => {
  setObjectReader(null);
  setObjectPathResolver(null);
  tail.closeAllConnections();
  tail.close();
  await rm(dataDir, { recursive: true, force: true });
});

async function app(principal: CpPrincipal): Promise<FastifyInstance> {
  const server = Fastify();
  server.setErrorHandler((error: InstanceType<typeof ConsoleError>, _request, reply) =>
    reply.code(error.statusCode ?? 500).send(error instanceof ConsoleError ? error.toBody() : { error: { code: 'internal_error', message: String(error) } }),
  );
  installTestAuth(server, principal);
  await registerRunRoutes(server);
  await server.register(async (scope) => registerDataRoutes(scope), { prefix: '/api/d/:domain' });
  return server;
}

async function finishedRun(server: FastifyInstance, body: Record<string, unknown> = {}): Promise<RunSnapshot> {
  const created = await server.inject({
    method: 'POST',
    url: `/api/d/domain_a/materializations/${MATERIALIZATION}/runs`,
    payload: { kind: 'browse', range: { from: null, to: null }, ...body },
  });
  assert.equal(created.statusCode, 201, created.body);
  let run = created.json() as RunSnapshot;
  for (let attempt = 0; run.state === 'running' && attempt < 200; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 10));
    run = (await server.inject({ url: `/api/runs/${run.id}` })).json() as RunSnapshot;
  }
  return run;
}

describe('runs and their tails', () => {
  it('read the tail with the person’s session, unless the request says no', async () => {
    const server = await app(reader());
    const run = await finishedRun(server);
    assert.equal(run.state, 'succeeded');
    assert.equal(run.rows, 12 + 4);
    assert.equal(run.tail?.status, 'complete');
    assert.equal(asked[0]?.headers.authorization, 'Bearer token-test');
    assert.equal(asked[0]?.headers['x-console-token'], SERVICE_TOKEN);

    asked.length = 0;
    const without = await finishedRun(server, { liveTail: false });
    assert.equal(without.rows, 12);
    assert.equal(without.tail, undefined);
    assert.equal(asked.length, 0);
  });

  it('say the tail is off when a control plane before 4.3 has the materialization’s off', async () => {
    liveTailOn = false;
    const run = await finishedRun(await app(reader()));
    assert.deepEqual([run.rows, run.tail?.status, run.tail?.reason], [12, 'off', 'materialization']);
    assert.equal(asked.length, 0);
  });

  it('say the tail is off when the control plane’s is', async () => {
    domain.controlPlane.settings = async () => ({ live_tail_enabled: false }) as CpSystemSettings;
    domain.invalidateSystemSettings();
    const run = await finishedRun(await app(reader()));
    assert.deepEqual([run.tail?.status, run.tail?.reason], ['off', 'control_plane']);
  });

  it('refuse a liveTail that is not a boolean', async () => {
    const response = await (await app(reader())).inject({
      method: 'POST',
      url: `/api/d/domain_a/materializations/${MATERIALIZATION}/runs`,
      payload: { kind: 'browse', range: { from: null, to: null }, liveTail: 'yes' },
    });
    assert.equal(response.statusCode, 400);
  });
});

describe('refreshing a run', () => {
  it('appends the tail’s newest records to the same run', async () => {
    const server = await app(reader());
    const run = await finishedRun(server);
    const response = await server.inject({ method: 'POST', url: `/api/runs/${run.id}/refresh`, payload: {} });
    assert.equal(response.statusCode, 200, response.body);
    const refreshed = response.json() as RunRefresh;
    assert.equal(refreshed.replaced, false);
    assert.equal(refreshed.run.id, run.id);
    assert.deepEqual([refreshed.run.rows, refreshed.run.revision, refreshed.run.tail?.segments], [18, 1, 2]);
  });

  it('is refused once the person may no longer query the run’s dataset', async () => {
    const run = await finishedRun(await app(reader()));
    const response = await (await app(formerReader())).inject({ method: 'POST', url: `/api/runs/${run.id}/refresh`, payload: {} });
    assert.equal(response.statusCode, 403);
  });

  it('answers 404 for a run that is gone', async () => {
    const response = await (await app(reader())).inject({ method: 'POST', url: '/api/runs/00000000-0000-4000-8000-000000000000/refresh', payload: {} });
    assert.equal(response.statusCode, 404);
    assert.equal((response.json() as { error: { details: { resource: string } } }).error.details.resource, 'run');
  });

  it('refuses a range that is not one', async () => {
    const server = await app(reader());
    const run = await finishedRun(server);
    const response = await server.inject({ method: 'POST', url: `/api/runs/${run.id}/refresh`, payload: { range: { from: 'soon', to: null } } });
    assert.equal(response.statusCode, 400);
  });
});

describe('the head peek', () => {
  it('shows the topic’s newest records in the table’s columns, with the peek’s headers, and starts no run', async () => {
    const response = await (await app(reader())).inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/orders/head` });
    assert.equal(response.statusCode, 200, response.body);
    const peek = response.json() as HeadPeek;
    assert.equal(peek.rows.length, 4);
    assert.ok(peek.columns.some((column) => column.name === 'customer.email'));
    assert.deepEqual(peek.peek, {
      rows: 4,
      offsets: { '0': [10, 12], '1': [12, 12] },
      endOffsets: { '0': 12, '1': 12 },
      behind: 1_200_000,
      state: 'backlogged',
      settling: 0,
      generatedAt: '2026-10-23T12:00:00.000Z',
    });
    assert.equal(asked[0]?.path, `/tail/v1/materializations/${MATERIALIZATION}/tables/orders/head`);
    assert.equal(asked[0]?.query.get('rows'), '200');
    assert.equal(asked[0]?.headers.authorization, 'Bearer token-test');
    const kept = (await readdir(config.runDir).catch(() => [])).filter((name) => name !== 'peeks');
    assert.deepEqual(kept, [], 'no run');
  });

  it('needs data.query on the table’s dataset, and asks the tail app nothing otherwise', async () => {
    const response = await (await app(formerReader())).inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/orders/head` });
    assert.equal(response.statusCode, 403);
    assert.equal(asked.length, 0);
  });

  it('bounds how many records it asks for', async () => {
    const server = await app(reader());
    assert.equal((await server.inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/orders/head?rows=0` })).statusCode, 400);
    assert.equal((await server.inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/orders/head?rows=10001` })).statusCode, 400);
    await server.inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/orders/head?rows=25` });
    assert.equal(asked[0]?.query.get('rows'), '25');
  });

  it('says the tail is off, for the materialization or at the tail app', async () => {
    liveTailOn = false;
    const off = await (await app(reader())).inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/orders/head` });
    assert.equal(off.statusCode, 404);
    assert.equal((off.json() as { error: { code: string } }).error.code, 'live_tail_off');
    liveTailOn = true;
    headOff = true;
    const notTailed = await (await app(reader())).inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/orders/head` });
    assert.equal(notTailed.statusCode, 404);
    assert.deepEqual(
      [(notTailed.json() as { error: { code: string; details: { reason: string } } }).error.code, (notTailed.json() as { error: { details: { reason: string } } }).error.details.reason],
      ['live_tail_off', 'not_tailed'],
    );
  });

  it('refuses a table the materialization does not have', async () => {
    const response = await (await app(reader())).inject({ url: `/api/d/domain_a/materializations/${MATERIALIZATION}/tables/payments/head` });
    assert.equal(response.statusCode, 400);
  });
});
