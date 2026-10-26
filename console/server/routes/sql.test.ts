/**
 * SQL across domains (handbook 20): a statement names tables as `<domain>.<dataset>.<table>`, in any
 * domain the console serves; each is found in its domain's fleet, read at its own snapshot, checked
 * against what the person may do there, and its live tail read from its own domain's tail app.
 *
 * Domain A holds the fixture's orders in `sales`; Domain B its payments in `finance`.
 */

import assert from 'node:assert/strict';
import { createServer, type IncomingMessage } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, afterEach, before, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

/** What each domain's tail app was asked, by domain. */
type Asked = Array<{ path: string; authorization: string | undefined; token: string | undefined }>;
const asked: { domain_a: Asked; domain_b: Asked } = { domain_a: [], domain_b: [] };

function tailApp(domain: string) {
  return createServer((request: IncomingMessage, response) => {
    asked[domain as keyof typeof asked].push({
      path: new URL(request.url ?? '/', 'http://stub').pathname,
      authorization: request.headers.authorization,
      token: request.headers['x-console-token'] as string | undefined,
    });
    // Nothing past the snapshot: the run reads committed rows only.
    response
      .writeHead(200, {
        'x-tail-rows': '0',
        'x-tail-bytes': '0',
        'x-tail-through': '{}',
        'x-tail-end-offsets': '{}',
        'x-tail-status': 'empty',
        'x-tail-schema-id': '1',
        'x-tail-settling': '0',
        'x-tail-lag-ms': '0',
        'x-tail-state': 'live',
        'x-tail-member': `${domain}-tail-app-0`,
      })
      .end();
  });
}
const tails = { domain_a: tailApp('domain_a'), domain_b: tailApp('domain_b') };
for (const server of Object.values(tails)) await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
const port = (domain: keyof typeof tails): number => (tails[domain].address() as AddressInfo).port;

const dataDir = await mkdtemp(join(tmpdir(), 'console-sql-routes-'));
process.env.CONSOLE_DATA_DIR = dataDir;

const { installTestAuth, testPrincipal } = await import('../auth/testing.ts');
const { installTestDomains } = await import('../domains/testing.ts');
const domains = await installTestDomains(['domain_a', 'domain_b'], {
  entries: {
    domain_a: { tailUrl: `http://127.0.0.1:${port('domain_a')}`, serviceToken: 'service-a-0123456789abcdef0123456789' },
    domain_b: { tailUrl: `http://127.0.0.1:${port('domain_b')}`, serviceToken: 'service-b-0123456789abcdef0123456789' },
  },
});
const a = domains.require('domain_a');
const b = domains.require('domain_b');
const { ConsoleError } = await import('../errors.ts');
const { setObjectReader } = await import('../iceberg/io.ts');
const { setObjectPathResolver } = await import('../data/objects.ts');
const { resetTables } = await import('../iceberg/table.ts');
const { FIXTURE_LOCATION, PAYMENTS_LOCATION, fixturePath, fixtureReader } = await import('../iceberg/testing.ts');
const { resetRuns } = await import('../runs/store.ts');
const { registerDataRoutes, registerRunRoutes } = await import('./data.ts');

import type { CpDetail, CpPrincipal, CpSystemSettings, FleetSnapshot, MaterializationSummary, RunSnapshot } from '../../shared/types.ts';

const SALES = 'ds-000000000001';
const FINANCE = 'ds-000000000002';
const SHOP = 'mat-a1b2c3d4e5f6';
const BILLING = 'mat-b1b2c3d4e5f6';

function detail(id: string, dataset: string, datasetId: string, table: string, location: string): CpDetail {
  return {
    materialization: {
      materialization_id: id,
      name: id,
      tables: [{ name: table, source_topic: table, dataset, dataset_id: datasetId }],
      retired_tables: [],
      source_topics: [table],
      destination_uri: 'gs://lake',
      runtime_mode: 'dedicated',
      namespace: 'midstream',
      datasets: [dataset],
      live_tail: true,
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
        dataset,
        dataset_id: datasetId,
        name: table,
        source_topic: table,
        identifier: `${dataset}.${table}`,
        location,
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

function fleet(id: string, tables: string[]): FleetSnapshot {
  return {
    generatedAt: new Date().toISOString(),
    connected: true,
    controlPlaneError: null,
    totals: {} as FleetSnapshot['totals'],
    items: [{ materialization_id: id, name: id, desired_phase: 'running', observed_phase: 'running', tables, source_topics: [] } as unknown as MaterializationSummary],
    series: [],
  };
}

const originals = domains.all().map((domain) => ({
  domain,
  getDetail: domain.live.getDetail.bind(domain.live),
  fleetSnapshot: domain.live.fleetSnapshot.bind(domain.live),
  settings: domain.controlPlane.settings.bind(domain.controlPlane),
}));

before(() => {
  setObjectReader(fixtureReader());
  setObjectPathResolver((uri) => fixturePath(uri));
});

beforeEach(() => {
  asked.domain_a = [];
  asked.domain_b = [];
  a.live.getDetail = async () => detail(SHOP, 'sales', SALES, 'orders', FIXTURE_LOCATION);
  b.live.getDetail = async () => detail(BILLING, 'finance', FINANCE, 'payments', PAYMENTS_LOCATION);
  a.live.fleetSnapshot = () => fleet(SHOP, ['sales.orders']);
  b.live.fleetSnapshot = () => fleet(BILLING, ['finance.payments']);
  for (const domain of [a, b]) {
    domain.controlPlane.settings = async () => ({ live_tail_enabled: true }) as CpSystemSettings;
    domain.invalidateSystemSettings();
  }
  resetTables();
});

afterEach(async () => {
  for (const original of originals) {
    original.domain.live.getDetail = original.getDetail;
    original.domain.live.fleetSnapshot = original.fleetSnapshot;
    original.domain.controlPlane.settings = original.settings;
  }
  await resetRuns();
});

after(async () => {
  setObjectReader(null);
  setObjectPathResolver(null);
  for (const server of Object.values(tails)) {
    server.closeAllConnections();
    server.close();
  }
  await rm(dataDir, { recursive: true, force: true });
});

const QUERY = ['materializations.view', 'materializations.view_details'];
function readerOf(datasetId: string, permissions: string[] = ['data.query', 'data.export']): CpPrincipal {
  return testPrincipal(QUERY, { dataset_permissions: { [datasetId]: permissions } });
}

async function app(perDomain: Record<string, CpPrincipal> = { domain_a: readerOf(SALES), domain_b: readerOf(FINANCE) }): Promise<FastifyInstance> {
  const server = Fastify();
  server.setErrorHandler((error: InstanceType<typeof ConsoleError>, _request, reply) =>
    reply.code(error.statusCode ?? 500).send(error instanceof ConsoleError ? error.toBody() : { error: { code: 'internal_error', message: String(error) } }),
  );
  installTestAuth(server, readerOf(SALES), perDomain);
  await registerRunRoutes(server);
  await server.register(async (scope) => registerDataRoutes(scope), { prefix: '/api/d/:domain' });
  return server;
}

async function statement(server: FastifyInstance, sql: string, extra: Record<string, unknown> = {}) {
  return server.inject({
    method: 'POST',
    url: `/api/d/domain_a/materializations/${SHOP}/runs`,
    payload: { kind: 'sql', range: { from: null, to: null }, sql, ...extra },
  });
}

async function finished(server: FastifyInstance, sql: string, extra: Record<string, unknown> = {}): Promise<RunSnapshot> {
  const created = await statement(server, sql, extra);
  assert.equal(created.statusCode, 201, created.body);
  let run = created.json() as RunSnapshot;
  for (let attempt = 0; run.state === 'running' && attempt < 300; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 10));
    run = (await server.inject({ url: `/api/runs/${run.id}` })).json() as RunSnapshot;
  }
  return run;
}

const JOIN = 'SELECT o.order_id, o.status, p.amount, p.method FROM domain_a.sales.orders o JOIN domain_b.finance.payments p USING (order_id) ORDER BY o.order_id';

describe('a statement across domains', () => {
  it('joins a table of each domain, each at its own snapshot, and says where each is', async () => {
    const server = await app();
    const run = await finished(server, JOIN, { liveTail: false });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    const rows = (await server.inject({ url: `/api/runs/${run.id}/rows?offset=0&limit=20` })).json() as { rows: Record<string, unknown>[] };
    assert.equal(rows.rows.length, 6);
    assert.deepEqual(rows.rows[1], { order_id: 'O-1', status: 'confirmed', amount: 10.25, method: 'card' });
    assert.deepEqual(
      run.tables.map((table) => [table.domain, table.dataset, table.name, table.materializationId]),
      [
        ['domain_a', 'sales', 'orders', SHOP],
        ['domain_b', 'finance', 'payments', BILLING],
      ],
    );
  });

  it('reads native tables as <domain>_iceberg.<dataset>.<table>', async () => {
    const server = await app();
    const run = await finished(server, 'SELECT count(*) AS n FROM domain_b_iceberg.finance.payments', { liveTail: false });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.deepEqual(((await server.inject({ url: `/api/runs/${run.id}/rows` })).json() as { rows: unknown[] }).rows, [{ n: 6 }]);
  });

  it('reads each table’s live tail from its own domain’s tail app, as the person is there', async () => {
    const server = await app();
    const run = await finished(server, JOIN);
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.deepEqual(asked.domain_a.map((entry) => entry.path), [`/tail/v1/materializations/${SHOP}/tables/orders`]);
    assert.deepEqual(asked.domain_b.map((entry) => entry.path), [`/tail/v1/materializations/${BILLING}/tables/payments`]);
    assert.equal(asked.domain_b[0]?.token, 'service-b-0123456789abcdef0123456789', 'Domain B’s tail app gets Domain B’s credential');
  });

  it('names its domain, or is told how to write it', async () => {
    const server = await app();
    const answer = await statement(server, 'SELECT * FROM sales.orders o JOIN iceberg.sales.orders n USING (order_id)');
    assert.equal(answer.statusCode, 400);
    assert.match(answer.json().error.message, /write domain_a\.sales\.orders for sales\.orders and domain_a_iceberg\.sales\.orders for iceberg\.sales\.orders/);
  });

  it('says which tables there are when it names one no domain holds', async () => {
    const server = await app();
    const answer = await statement(server, 'SELECT * FROM domain_b.finance.refunds');
    assert.equal(answer.statusCode, 400);
    assert.match(answer.json().error.message, /There is no table domain_b\.finance\.refunds; domain_b\.finance holds domain_b\.finance\.payments/);
    const nowhere = await statement(server, 'SELECT * FROM domain_b.marketing.leads');
    assert.match(nowhere.json().error.message, /Domain B has no dataset marketing/);
  });

  it('is refused for a table the person may not query in its domain, naming it', async () => {
    const server = await app({ domain_a: readerOf(SALES), domain_b: readerOf(FINANCE, ['dataset.view_schema']) });
    const answer = await statement(server, JOIN);
    assert.equal(answer.statusCode, 403, answer.body);
    assert.match(answer.json().error.message, /domain_b\.finance/);
  });

  it('binds in the editor across domains as it runs', async () => {
    const server = await app();
    const ok = await server.inject({ method: 'POST', url: `/api/d/domain_a/materializations/${SHOP}/data/validate`, payload: { sql: JOIN } });
    assert.equal(ok.statusCode, 200, ok.body);
    const wrong = await server.inject({
      method: 'POST',
      url: `/api/d/domain_a/materializations/${SHOP}/data/validate`,
      payload: { sql: 'SELECT nope FROM domain_b.finance.payments' },
    });
    assert.equal(wrong.statusCode, 400);
    assert.match(wrong.json().error.message, /nope/);
  });

  it('exports only with data.export on every table it read, in each one’s domain', async () => {
    const server = await app({ domain_a: readerOf(SALES), domain_b: readerOf(FINANCE, ['data.query']) });
    const run = await finished(server, JOIN, { liveTail: false });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    const refused = await server.inject({ url: `/api/runs/${run.id}/export?format=csv` });
    assert.equal(refused.statusCode, 403);
  });
});
