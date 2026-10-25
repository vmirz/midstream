/**
 * Datasets in the console's API: the dataset routes, a create that waits for its tables' names to be
 * claimed, and every read of a table checked against the person's access to its dataset - the Data
 * tab's runs, the Schema tab, and the live detail they all share.
 */

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

import { installTestAuth, testPrincipal } from '../auth/testing.ts';
import { controlPlane } from '../controlPlane/client.ts';
import { ConsoleError } from '../errors.ts';
import { setObjectReader } from '../iceberg/io.ts';
import { resetSnapshotCaches } from '../iceberg/snapshot.ts';
import { resetTables } from '../iceberg/table.ts';
import { fixtureReader } from '../iceberg/testing.ts';
import { liveStore, visibleDetail } from '../live/store.ts';
import { registerDataRoutes } from './data.ts';
import { registerDatasetRoutes } from './datasets.ts';
import { registerMaterializationRoutes } from './materializations.ts';
import { registerSchemaRoutes } from './schema.ts';
import type { CpDetail, CpPrincipal, CpSchema, CpTableSpec, DetailSnapshot } from '../../shared/types.ts';

const SALES = 'ds-000000000001';
const FINANCE = 'ds-000000000002';

/** Someone who may see materializations and datasets, and reads and writes sales - nothing of finance. */
function salesWriter(): CpPrincipal {
  return testPrincipal(['materializations.view', 'materializations.view_details', 'materializations.create', 'datasets.view', 'datasets.create'], {
    dataset_permissions: { [SALES]: ['dataset.view_schema', 'data.query', 'data.export', 'catalog.read', 'dataset.write'] },
  });
}

const ORDERS: CpTableSpec = { name: 'orders', source_topic: 'sales.orders', dataset: 'sales', dataset_id: SALES };
const INVOICES: CpTableSpec = { name: 'invoices', source_topic: 'finance.invoices', dataset: 'finance', dataset_id: FINANCE };

function detail(): CpDetail {
  const summary = (spec: CpTableSpec) => ({
    dataset: spec.dataset,
    dataset_id: spec.dataset_id,
    name: spec.name,
    source_topic: spec.source_topic,
    identifier: `${spec.dataset}.${spec.name}`,
    location: `gs://lake/materializer/mat-a1b2c3d4e5f6/${spec.name}`,
    partition_granularity: 'day' as const,
    schema_id: 0,
    current_snapshot_id: null,
    last_commit_at: null,
    valid_through: null,
    totals: {},
    last_error: null,
    state: 'active' as const,
    // As the console's own credential reads them: it sees every dataset's schemas.
    readable: true,
    queryable: false,
  });
  return {
    materialization: {
      materialization_id: 'mat-a1b2c3d4e5f6',
      name: 'Shop',
      tables: [ORDERS, INVOICES],
      retired_tables: [],
      source_topics: [ORDERS.source_topic, INVOICES.source_topic],
      destination_uri: 'gs://lake',
      runtime_mode: 'dedicated',
      namespace: 'kafka-sql',
      datasets: ['finance', 'sales'],
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
    tables: [summary(ORDERS), summary(INVOICES)],
  } as CpDetail;
}

function schema(spec: CpTableSpec): CpSchema {
  return {
    materialization_id: 'mat-a1b2c3d4e5f6',
    table: spec.name,
    dataset: spec.dataset,
    dataset_id: spec.dataset_id,
    source_topic: spec.source_topic,
    schema_id: 0,
    since: null,
    metadata_location: null,
    field_count: 1,
    fields: [],
    changes: [],
  };
}

const original = { ...controlPlane } as Record<string, unknown>;
const getDetail = liveStore.getDetail.bind(liveStore);
const getSchema = liveStore.getSchema.bind(liveStore);

beforeEach(() => {
  liveStore.getDetail = async () => detail();
  liveStore.getSchema = async (_id: string, table: string) => schema(table === 'orders' ? ORDERS : INVOICES);
});

afterEach(() => {
  Object.assign(controlPlane, original);
  liveStore.getDetail = getDetail;
  liveStore.getSchema = getSchema;
});

async function app(principal: CpPrincipal): Promise<FastifyInstance> {
  const server = Fastify();
  server.setErrorHandler((error: ConsoleError, _request, reply) =>
    reply.code(error.statusCode ?? 500).send(error instanceof ConsoleError ? error.toBody() : { error: { code: 'internal_error', message: String(error) } }),
  );
  installTestAuth(server, principal);
  await registerMaterializationRoutes(server);
  await registerDatasetRoutes(server);
  await registerDataRoutes(server);
  await registerSchemaRoutes(server);
  return server;
}

describe('dataset routes', () => {
  it('list datasets as the person, passing the search on', async () => {
    let asked: { token: string | null; q: string } | null = null;
    Object.assign(controlPlane, {
      datasets: async (caller: { token: string | null }, q: string) => {
        asked = { token: caller.token, q };
        return { datasets: [], returned: 0, can_create: true };
      },
    });
    const response = await (await app(salesWriter())).inject({ url: '/api/datasets?q=orders' });
    assert.equal(response.statusCode, 200);
    assert.deepEqual(asked, { token: 'token-test', q: 'orders' });
  });

  it('create a dataset and answer once the directory has', async () => {
    let created: unknown = null;
    Object.assign(controlPlane, {
      createDataset: async (_caller: unknown, input: unknown) => {
        created = input;
        return { request_id: 'idr-1', action: 'create_dataset', entity_type: 'dataset', entity_id: 'ds-1', accepted_at: '', status_url: '', name: 'returns' };
      },
      identityRequest: async () => ({ request_id: 'idr-1', status: 'applied' }),
    });
    const response = await (await app(salesWriter())).inject({ method: 'POST', url: '/api/datasets', payload: { name: 'returns', description: 'Refunds' } });
    assert.equal(response.statusCode, 201);
    assert.deepEqual(created, { name: 'returns', description: 'Refunds' });
    assert.equal(response.json().name, 'returns');
  });

  it('refuse a name SQL cannot read before asking the control plane', async () => {
    Object.assign(controlPlane, {
      createDataset: async () => assert.fail('a name the rules refuse never leaves'),
    });
    for (const name of ['Sales', 'sales-eu', 'main', 'select']) {
      const response = await (await app(salesWriter())).inject({ method: 'POST', url: '/api/datasets', payload: { name } });
      assert.equal(response.statusCode, 400, name);
    }
  });

  it('turn away changes from someone who manages no dataset', async () => {
    const server = await app(salesWriter());
    for (const [method, url, payload] of [
      ['PATCH', '/api/datasets/sales', { description: 'x' }],
      ['DELETE', '/api/datasets/sales', undefined],
      ['GET', '/api/datasets/sales/grants', undefined],
      ['PUT', '/api/datasets/sales/grants/usr-2', { role_ids: [] }],
    ] as const) {
      const response = await server.inject({ method, url, ...(payload ? { payload } : {}) });
      assert.equal(response.statusCode, 403, `${method} ${url}`);
    }
  });

  it('set a grant on a dataset its owner manages', async () => {
    let grant: unknown = null;
    Object.assign(controlPlane, {
      setDatasetGrant: async (_caller: unknown, dataset: string, principalId: string, input: unknown) => {
        grant = { dataset, principalId, input };
        return { request_id: 'idr-2', action: 'set_dataset_grant', entity_type: 'dataset', entity_id: SALES, accepted_at: '', status_url: '' };
      },
      identityRequest: async () => ({ request_id: 'idr-2', status: 'applied' }),
    });
    const owner = testPrincipal(['datasets.view'], { dataset_permissions: { [SALES]: ['dataset.manage', 'dataset.view_schema'] } });
    const response = await (await app(owner)).inject({
      method: 'PUT',
      url: '/api/datasets/sales/grants/all-users',
      payload: { role_ids: [], principal_type: 'group' },
    });
    assert.equal(response.statusCode, 200);
    assert.deepEqual(grant, { dataset: 'sales', principalId: 'all-users', input: { role_ids: [], principal_type: 'group' } });
  });
});

describe('creating a materialization', () => {
  const accepted = {
    materialization_id: 'mat-a1b2c3d4e5f6',
    request_id: 'req-1',
    desired_phase: 'running',
    observed_phase: 'requested',
    phase_reason: 'claiming_tables',
    status_url: '/materializations/mat-a1b2c3d4e5f6',
    accepted_at: '2026-10-22T00:00:00Z',
    tables: [ORDERS],
    claim_request_id: 'idr-claim-1',
    claim_status_url: '/identity/requests/idr-claim-1',
  };
  const body = { tables: [{ source_topic: 'sales.orders', dataset: 'sales' }] };

  it('answers once its tables’ names are claimed', async () => {
    let asked: string | null = null;
    Object.assign(controlPlane, {
      create: async () => accepted,
      identityRequest: async (_caller: unknown, requestId: string) => ((asked = requestId), { request_id: requestId, status: 'applied' }),
    });
    const response = await (await app(salesWriter())).inject({ method: 'POST', url: '/api/materializations', payload: body });
    assert.equal(response.statusCode, 202);
    assert.equal(asked, 'idr-claim-1');
    assert.equal(response.json().claim_status, 'applied');
  });

  it('fails with the refused name, on the row it was about, when another request took it first', async () => {
    Object.assign(controlPlane, {
      create: async () => accepted,
      identityRequest: async () => ({
        request_id: 'idr-claim-1',
        status: 'rejected',
        reason: 'table_name_taken',
        message: 'sales.orders is taken by materialization mat-000000000001.',
        field: 'tables.0.name',
        details: { index: 0, dataset: 'sales', table: 'orders', source_topic: 'sales.orders' },
      }),
    });
    const response = await (await app(salesWriter())).inject({ method: 'POST', url: '/api/materializations', payload: body });
    assert.equal(response.statusCode, 409);
    const error = response.json().error;
    assert.equal(error.details.reason, 'table_name_taken');
    assert.equal(error.details.field, 'tables.0.name');
    assert.equal(error.details.index, 0);
    assert.equal(error.details.materialization_id, 'mat-a1b2c3d4e5f6');
  });
});

describe('reading tables in datasets', () => {
  it('refuses a run over a table the person may not query, naming its dataset', async () => {
    const server = await app(salesWriter());
    const range = { from: null, to: null };
    const browse = await server.inject({
      method: 'POST',
      url: '/api/materializations/mat-a1b2c3d4e5f6/runs',
      payload: { kind: 'browse', table: 'invoices', range },
    });
    assert.equal(browse.statusCode, 403);
    assert.match(browse.json().error.message, /“Query data” on dataset finance/);
    assert.equal(browse.json().error.details.dataset, 'finance');

    const joined = await server.inject({
      method: 'POST',
      url: '/api/materializations/mat-a1b2c3d4e5f6/runs',
      payload: { kind: 'sql', table: 'orders', range, sql: 'SELECT * FROM sales.orders JOIN finance.invoices USING (order_id)' },
    });
    assert.equal(joined.statusCode, 403, 'a statement naming a locked table is refused whatever else it reads');
    assert.equal(joined.json().error.details.dataset, 'finance');

    const validated = await server.inject({
      method: 'POST',
      url: '/api/materializations/mat-a1b2c3d4e5f6/data/validate',
      payload: { sql: 'SELECT count(*) FROM finance.invoices' },
    });
    assert.equal(validated.statusCode, 403);
  });

  it('turns away someone who may query no dataset at all', async () => {
    const viewer = testPrincipal(['materializations.view', 'materializations.view_details']);
    const response = await (await app(viewer)).inject({
      method: 'POST',
      url: '/api/materializations/mat-a1b2c3d4e5f6/runs',
      payload: { kind: 'browse', table: 'orders', range: { from: null, to: null } },
    });
    assert.equal(response.statusCode, 403);
  });

  it('shows a table’s schema only to someone who may see its dataset’s schemas', async () => {
    const server = await app(salesWriter());
    assert.equal((await server.inject({ url: '/api/materializations/mat-a1b2c3d4e5f6/tables/orders/schema' })).statusCode, 200);
    const locked = await server.inject({ url: '/api/materializations/mat-a1b2c3d4e5f6/tables/invoices/schema' });
    assert.equal(locked.statusCode, 403);
    assert.match(locked.json().error.message, /“View schemas” on dataset finance/);
    for (const path of ['schema/versions', 'schema/versions/0', 'schema/diff', 'schema/timeline']) {
      const response = await server.inject({ url: `/api/materializations/mat-a1b2c3d4e5f6/tables/invoices/${path}` });
      assert.equal(response.statusCode, 403, path);
    }
  });

  it('dates the fields of a table whose schemas the person may see, from its manifests', async () => {
    // The orders table of the detail is at the fixture table's location.
    setObjectReader(fixtureReader());
    try {
      const server = await app(salesWriter());
      const response = await server.inject({ url: '/api/materializations/mat-a1b2c3d4e5f6/tables/orders/schema/timeline' });
      assert.equal(response.statusCode, 200);
      const timeline = response.json();
      assert.equal(timeline.available, true);
      assert.equal(timeline.granularity, 'day');
      assert.equal(timeline.dataFrom, '2026-10-17T10:00:00.000Z');
      assert.equal(timeline.dataThrough, '2026-10-18T10:11:00.000Z');
      assert.ok(Object.keys(timeline.fields).length > 5);
    } finally {
      setObjectReader(null);
      resetSnapshotCaches();
      resetTables();
    }
  });

  it('sends each person only the schemas of datasets they may read, and their own access to each table', () => {
    const shared: DetailSnapshot = {
      generatedAt: '2026-10-22T00:00:00Z',
      materializationId: 'mat-a1b2c3d4e5f6',
      detail: detail(),
      schemas: { orders: schema(ORDERS), invoices: schema(INVOICES) },
      error: null,
      schemaErrors: { invoices: 'failed' },
      series: [],
      rowsPerSecond: null,
      bytesPerSecond: null,
      memberRates: {},
      lagHistory: [],
    };

    const seen = visibleDetail(shared, salesWriter());
    assert.deepEqual(Object.keys(seen.schemas), ['orders']);
    assert.deepEqual(seen.schemaErrors, {});
    assert.deepEqual(
      seen.detail?.tables.map((table) => [table.identifier, table.readable, table.queryable]),
      [
        ['sales.orders', true, true],
        ['finance.invoices', false, false],
      ],
    );
    const admin = visibleDetail(shared, testPrincipal(['*']));
    assert.deepEqual(Object.keys(admin.schemas).sort(), ['invoices', 'orders']);
    const guest = visibleDetail(shared, testPrincipal(['materializations.view'], { kind: 'guest' }));
    assert.deepEqual(guest.schemas, {});
    assert.equal(shared.detail?.tables[1]?.readable, true, 'the shared snapshot itself is left as it was');
  });
});
