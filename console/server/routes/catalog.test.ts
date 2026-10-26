/**
 * The federated Iceberg REST catalog and each domain's own (handbook 20): two stub domain catalogs,
 * each answering only the token it knows, behind the console.
 */

import assert from 'node:assert/strict';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { after, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

import { installTestDomains } from '../domains/testing.ts';
import { registerCatalogRoutes } from './catalog.ts';

interface Seen {
  method: string;
  url: string;
  authorization: string | undefined;
  body: string;
}

/** A domain's catalog: its datasets, each with tables; it answers only `token`. */
function domainCatalog(token: string, datasets: Record<string, string[]>, seen: Seen[]): Server {
  return createServer((request: IncomingMessage, response) => {
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    request.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      seen.push({ method: request.method ?? '', url: request.url ?? '', authorization: request.headers.authorization, body });
      const json = (status: number, value: unknown): void => {
        response.writeHead(status, { 'content-type': 'application/json' }).end(JSON.stringify(value));
      };
      if (request.headers.authorization !== `Bearer ${token}`) return json(401, { error: { message: 'no', type: 'NotAuthorizedException', code: 401 } });
      const url = new URL(request.url ?? '/', 'http://stub');
      const path = url.pathname;
      if (path === '/catalog/v1/namespaces') return json(200, { namespaces: Object.keys(datasets).map((name) => [name]), 'next-page-token': null });
      const namespace = /^\/catalog\/v1\/namespaces\/([^/]+)(.*)$/.exec(path);
      if (!namespace) return json(404, { error: { message: 'none', type: 'NoSuchNamespaceException', code: 404 } });
      const dataset = decodeURIComponent(namespace[1] as string);
      const tables = datasets[dataset];
      if (!tables) return json(404, { error: { message: `Namespace does not exist: ${dataset}`, type: 'NoSuchNamespaceException', code: 404 } });
      const rest = namespace[2] as string;
      if (rest === '') return json(200, { namespace: [dataset], properties: {} });
      if (rest === '/tables') return json(200, { identifiers: tables.map((name) => ({ namespace: [dataset], name })), 'next-page-token': null });
      const table = /^\/tables\/([^/]+)(\/metrics)?$/.exec(rest);
      if (!table || !tables.includes(decodeURIComponent(table[1] as string))) return json(404, { error: { message: 'no table', type: 'NoSuchTableException', code: 404 } });
      if (table[2]) return void response.writeHead(204).end();
      return json(200, { 'metadata-location': `gs://lake/${dataset}/${table[1]}/metadata/v3.metadata.json`, metadata: { 'format-version': 2 }, config: {}, echo: body || null, query: url.search });
    });
  });
}

const seen: { a: Seen[]; b: Seen[] } = { a: [], b: [] };
const catalogs = {
  a: domainCatalog('token-for-a', { sales: ['orders'] }, seen.a),
  b: domainCatalog('token-for-b', { finance: ['payments', 'invoices'] }, seen.b),
  // A gateway whose domain is gone: a bare 404 for everything, never a catalog's error.
  d: createServer((_request, response) => void response.writeHead(404).end()),
};
for (const server of Object.values(catalogs)) await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
const url = (server: Server): string => `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

await installTestDomains(['domain_a', 'domain_b', 'domain_c', 'domain_d'], {
  entries: {
    domain_a: { controlPlaneUrl: url(catalogs.a) },
    domain_b: { controlPlaneUrl: url(catalogs.b) },
    // Nothing listens here: Domain C cannot be reached.
    domain_c: { controlPlaneUrl: 'http://127.0.0.1:9' },
    domain_d: { controlPlaneUrl: url(catalogs.d) },
  },
});

async function app(): Promise<FastifyInstance> {
  const server = Fastify();
  await registerCatalogRoutes(server);
  return server;
}

beforeEach(() => {
  seen.a.length = 0;
  seen.b.length = 0;
});

after(() => {
  for (const server of Object.values(catalogs)) {
    server.closeAllConnections();
    server.close();
  }
});

/** A token both stub domains take: what a federated access token is to the domains. */
const BOTH = 'Bearer token-for-a';

describe('the federated catalog', () => {
  it('lists the endpoints a domain does, and a namespace per domain that lists something for the token', async () => {
    const server = await app();
    assert.deepEqual((await server.inject({ url: '/catalog/v1/config' })).json().overrides, {});
    const top = await server.inject({ url: '/catalog/v1/namespaces', headers: { authorization: BOTH } });
    assert.equal(top.statusCode, 200);
    // Domain B refuses this token and Domain C cannot be reached: both are left out.
    assert.deepEqual(top.json().namespaces, [['domain_a']]);
    assert.equal(seen.a[0]?.authorization, BOTH, 'the token goes through unchanged');
  });

  it('lists a domain’s datasets as two-level namespaces, and nothing under a dataset', async () => {
    const server = await app();
    const datasets = await server.inject({ url: '/catalog/v1/namespaces?parent=domain_b', headers: { authorization: 'Bearer token-for-b' } });
    assert.deepEqual(datasets.json().namespaces, [['domain_b', 'finance']]);
    const under = await server.inject({ url: '/catalog/v1/namespaces?parent=domain_b%1Ffinance', headers: { authorization: 'Bearer token-for-b' } });
    assert.deepEqual(under.json().namespaces, []);
  });

  it('answers a domain as a namespace, and hands a dataset’s requests to its domain with the namespace rewritten', async () => {
    const server = await app();
    const domain = await server.inject({ url: '/catalog/v1/namespaces/domain_b', headers: { authorization: 'Bearer token-for-b' } });
    assert.deepEqual(domain.json(), { namespace: ['domain_b'], properties: { name: 'Domain B' } });
    const dataset = await server.inject({ url: '/catalog/v1/namespaces/domain_b%1Ffinance', headers: { authorization: 'Bearer token-for-b' } });
    assert.deepEqual(dataset.json(), { namespace: ['domain_b', 'finance'], properties: {} });
    assert.equal(seen.b.at(-1)?.url, '/catalog/v1/namespaces/finance');
    const tables = await server.inject({ url: '/catalog/v1/namespaces/domain_b%1Ffinance/tables', headers: { authorization: 'Bearer token-for-b' } });
    assert.deepEqual(tables.json().identifiers, [
      { namespace: ['domain_b', 'finance'], name: 'payments' },
      { namespace: ['domain_b', 'finance'], name: 'invoices' },
    ]);
    const head = await server.inject({ method: 'HEAD', url: '/catalog/v1/namespaces/domain_b%1Ffinance', headers: { authorization: 'Bearer token-for-b' } });
    assert.equal(head.statusCode, 200);
    assert.equal(seen.b.at(-1)?.method, 'HEAD');
  });

  it('loads and commits a table as its domain answers, the body handed on byte for byte', async () => {
    const server = await app();
    const loaded = await server.inject({ url: '/catalog/v1/namespaces/domain_a%1Fsales/tables/orders?snapshots=all', headers: { authorization: BOTH } });
    assert.equal(loaded.statusCode, 200);
    assert.equal(loaded.json()['metadata-location'], 'gs://lake/sales/orders/metadata/v3.metadata.json');
    assert.equal(loaded.json().query, '?snapshots=all');
    const commit = '{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"x"}}]}';
    const committed = await server.inject({
      method: 'POST',
      url: '/catalog/v1/namespaces/domain_a%1Fsales/tables/orders',
      headers: { authorization: BOTH, 'content-type': 'application/json' },
      payload: commit,
    });
    assert.equal(committed.statusCode, 200);
    assert.equal(seen.a.at(-1)?.body, commit);
    const metrics = await server.inject({ method: 'POST', url: '/catalog/v1/namespaces/domain_a%1Fsales/tables/orders/metrics', headers: { authorization: BOTH }, payload: '{}' });
    assert.equal(metrics.statusCode, 204);
  });

  it('passes a domain’s refusal through, and says so in the Iceberg shape when it has no such namespace', async () => {
    const server = await app();
    const refused = await server.inject({ url: '/catalog/v1/namespaces/domain_b%1Ffinance/tables', headers: { authorization: BOTH } });
    assert.equal(refused.statusCode, 401);
    assert.equal(refused.json().error.type, 'NotAuthorizedException');
    const nowhere = await server.inject({ url: '/catalog/v1/namespaces/domain_x%1Fsales/tables', headers: { authorization: BOTH } });
    assert.equal(nowhere.statusCode, 404);
    assert.equal(nowhere.json().error.type, 'NoSuchNamespaceException');
    const noTables = await server.inject({ url: '/catalog/v1/namespaces/domain_a/tables', headers: { authorization: BOTH } });
    assert.equal(noTables.statusCode, 404, 'a domain holds datasets, not tables');
  });

  it('says which domain cannot be reached', async () => {
    const server = await app();
    const answer = await server.inject({ url: '/catalog/v1/namespaces/domain_c%1Fsales', headers: { authorization: BOTH } });
    assert.equal(answer.statusCode, 503);
    assert.equal(answer.json().error.type, 'ServiceUnavailableException');
    assert.match(answer.json().error.message, /Domain C/);
  });

  it('says so too when a gateway answers for a domain that is not there, rather than pass its page on', async () => {
    const server = await app();
    for (const path of ['/catalog/v1/namespaces/domain_d%1Fsales', '/catalog/v1/namespaces/domain_d%1Fsales/tables/orders', '/d/domain_d/catalog/v1/namespaces']) {
      const answer = await server.inject({ url: path, headers: { authorization: BOTH } });
      assert.equal(answer.statusCode, 503, path);
      assert.equal(answer.json().error.type, 'ServiceUnavailableException', path);
      assert.match(answer.json().error.message, /Domain D/, path);
    }
    const top = await server.inject({ url: '/catalog/v1/namespaces', headers: { authorization: BOTH } });
    assert.deepEqual(top.json().namespaces, [['domain_a']], 'a domain that is not there lists nothing');
  });

  it('makes and drops nothing', async () => {
    const server = await app();
    for (const [method, path] of [
      ['POST', '/catalog/v1/namespaces'],
      ['DELETE', '/catalog/v1/namespaces/domain_a%1Fsales'],
      ['POST', '/catalog/v1/namespaces/domain_a%1Fsales/tables'],
      ['DELETE', '/catalog/v1/namespaces/domain_a%1Fsales/tables/orders'],
      ['POST', '/catalog/v1/transactions/commit'],
    ] as const) {
      const answer = await server.inject({ method, url: path, headers: { authorization: BOTH }, payload: '{}' });
      assert.equal(answer.statusCode, 501, `${method} ${path}`);
      assert.equal(answer.json().error.type, 'NotImplementedException');
    }
    assert.equal(seen.a.length, 0, 'no domain is asked');
  });
});

describe('each domain’s own catalog', () => {
  it('is handed through as it is, path, query, token and all', async () => {
    const server = await app();
    const answer = await server.inject({ url: '/d/domain_b/catalog/v1/namespaces/finance/tables/payments?x=1', headers: { authorization: 'Bearer token-for-b' } });
    assert.equal(answer.statusCode, 200);
    assert.equal(seen.b.at(-1)?.url, '/catalog/v1/namespaces/finance/tables/payments?x=1');
    assert.equal(seen.b.at(-1)?.authorization, 'Bearer token-for-b');
    assert.deepEqual((await server.inject({ url: '/d/domain_b/catalog/v1/namespaces', headers: { authorization: 'Bearer token-for-b' } })).json().namespaces, [['finance']]);
    assert.equal((await server.inject({ url: '/d/nowhere/catalog/v1/config' })).statusCode, 404);
  });
});
