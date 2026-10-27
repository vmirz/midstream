import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { gunzipSync } from 'node:zlib';

import Fastify from 'fastify';

import { ConsoleError, upstreamError } from '../errors.ts';
import { installTestDomains } from '../domains/testing.ts';
import { registerTopicRoutes } from '../routes/topics.ts';

await installTestDomains(['domain_a']);
import type { CpTopicList, TopicCatalogResponse } from '../../shared/types.ts';
import { acceptsGzip, etagMatches, TopicCatalog, toCatalogResponse } from './catalog.ts';
import { installTestAuth, testPrincipal } from '../auth/testing.ts';

const TTL = 15_000;
const MIN_REFRESH = 5_000;

function upstream(names: string[], extra: Partial<CpTopicList> = {}): CpTopicList {
  return {
    items: names.map((topic) => ({
      topic,
      partitions: 3,
      replication_factor: 2,
      ...(topic.startsWith('__') ? { category: 'kafka' as const } : {}),
    })),
    returned: names.length,
    fetched_at: '2026-10-06T23:00:00Z',
    ...extra,
  };
}

/** A catalog over a scripted control plane and a clock the test moves by hand. */
function harness(responses: Array<CpTopicList | Error> = [upstream(['orders'])]) {
  const clock = { now: 1_000_000 };
  const calls: boolean[] = [];
  let next = 0;
  const catalog = new TopicCatalog(
    async (refresh) => {
      calls.push(refresh);
      const response = responses[Math.min(next++, responses.length - 1)]!;
      await new Promise((resolve) => setImmediate(resolve));
      if (response instanceof Error) throw response;
      return response;
    },
    { ttlMs: TTL, minRefreshMs: MIN_REFRESH, now: () => clock.now },
  );
  return { catalog, clock, calls };
}

const parse = (json: Buffer): TopicCatalogResponse => JSON.parse(json.toString()) as TopicCatalogResponse;

describe('TopicCatalog', () => {
  it('serves two hundred concurrent readers from one control-plane request', async () => {
    const { catalog, calls } = harness();

    const bodies = await Promise.all(Array.from({ length: 200 }, () => catalog.read()));

    assert.equal(calls.length, 1);
    assert.ok(bodies.every((body) => body === bodies[0]));
    assert.deepEqual(parse(bodies[0]!.json).topics, [{ name: 'orders', partitions: 3, replicationFactor: 2 }]);
  });

  it('answers from memory within the TTL', async () => {
    const { catalog, clock, calls } = harness();
    await catalog.read();
    clock.now += TTL - 1;

    await catalog.read();

    assert.equal(calls.length, 1);
  });

  it('answers a stale read at once and replaces the list in the background', async () => {
    const { catalog, clock, calls } = harness([upstream(['orders']), upstream(['orders', 'payments'])]);
    const first = await catalog.read();
    clock.now += TTL;

    const stale = await catalog.read();
    assert.equal(stale, first);
    await catalog.settled();

    assert.equal(calls.length, 2);
    assert.deepEqual(parse((await catalog.read()).json).topics.map((topic) => topic.name), ['orders', 'payments']);
  });

  it('honours an explicit refresh at most once per interval and asks the control plane to reload', async () => {
    const { catalog, clock, calls } = harness();
    await catalog.read();
    for (let index = 0; index < 20; index += 1) {
      clock.now += 200;
      await catalog.read({ refresh: true });
    }
    assert.deepEqual(calls, [false]);

    clock.now += MIN_REFRESH;
    await catalog.read({ refresh: true });

    assert.deepEqual(calls, [false, true]);
  });

  it('keeps serving the last good list, marked stale, when a refresh fails', async () => {
    const { catalog, clock } = harness([upstream(['orders']), new Error('connection refused')]);
    const first = await catalog.read();
    clock.now += TTL;
    await catalog.read();
    await catalog.settled();

    const marked = await catalog.read();
    const response = parse(marked.json);

    assert.notEqual(marked.etag, first.etag);
    assert.equal(response.stale, true);
    assert.equal(response.error, 'connection refused');
    assert.deepEqual(response.topics.map((topic) => topic.name), ['orders']);
  });

  it('fails fast without retrying on every read when nothing has ever loaded', async () => {
    const { catalog, clock, calls } = harness([new Error('connection refused')]);

    await assert.rejects(catalog.read(), (error: ConsoleError) => {
      assert.equal(error.statusCode, 503);
      assert.equal(error.details.reason, 'connection refused');
      return true;
    });
    clock.now += 1_000;
    await assert.rejects(catalog.read());
    assert.equal(calls.length, 1);

    clock.now += MIN_REFRESH;
    await assert.rejects(catalog.read());
    assert.equal(calls.length, 2);
  });

  it('says what to do when the control plane predates the topic list', async () => {
    const { catalog } = harness([upstreamError('control plane returned 404', 404)]);

    await assert.rejects(catalog.read(), (error: ConsoleError) => {
      assert.match(String(error.details.reason), /upgrade it to enable the topic picker/);
      return true;
    });
  });

  it('prepares a gzip encoding of the same body and an ETag that tracks content', async () => {
    const one = await harness([upstream(['orders'])]).catalog.read();
    const same = await harness([upstream(['orders'])]).catalog.read();
    const other = await harness([upstream(['payments'])]).catalog.read();

    assert.deepEqual(gunzipSync(one.gzip), one.json);
    assert.equal(one.etag, same.etag);
    assert.notEqual(one.etag, other.etag);
    assert.match(one.etag, /^W\/"/);
  });
});

describe('toCatalogResponse', () => {
  it('maps the control plane shape and drops entries without a name', () => {
    const response = toCatalogResponse({
      ...upstream(['__consumer_offsets', 'orders']),
      items: [...upstream(['__consumer_offsets', 'orders']).items, { topic: '', partitions: 1, replication_factor: 1 }],
      stale: true,
      error: 'broker unreachable',
    });

    assert.deepEqual(response, {
      topics: [
        { name: '__consumer_offsets', partitions: 3, replicationFactor: 2, category: 'kafka' },
        { name: 'orders', partitions: 3, replicationFactor: 2 },
      ],
      fetchedAt: '2026-10-06T23:00:00Z',
      stale: true,
      error: 'broker unreachable',
    });
  });

  it('rejects a payload without items', () => {
    assert.throws(() => toCatalogResponse({} as CpTopicList));
  });
});

describe('conditional and encoded responses', () => {
  it('matches If-None-Match by weak comparison, including lists and *', () => {
    assert.ok(etagMatches('W/"abc"', 'W/"abc"'));
    assert.ok(etagMatches('"abc"', 'W/"abc"'));
    assert.ok(etagMatches('W/"zzz", W/"abc"', 'W/"abc"'));
    assert.ok(etagMatches('*', 'W/"abc"'));
    assert.ok(!etagMatches('W/"zzz"', 'W/"abc"'));
    assert.ok(!etagMatches(undefined, 'W/"abc"'));
  });

  it('accepts gzip unless it is absent or refused with q=0', () => {
    assert.ok(acceptsGzip('gzip, deflate, br'));
    assert.ok(acceptsGzip('br;q=1.0, gzip;q=0.5'));
    assert.ok(!acceptsGzip('br'));
    assert.ok(!acceptsGzip('gzip;q=0'));
    assert.ok(!acceptsGzip(undefined));
  });
});

describe('GET /api/d/<domain>/topics', () => {
  async function app(responses: Array<CpTopicList | Error>) {
    const server = Fastify();
    installTestAuth(server, testPrincipal(['topics.view']));
    await server.register(async (scope) => registerTopicRoutes(scope, harness(responses).catalog), { prefix: '/api/d/:domain' });
    return server;
  }

  it('serves gzip with an ETag, then a bodiless 304 for the same version', async () => {
    const server = await app([upstream(['orders'])]);

    const first = await server.inject({ url: '/api/d/domain_a/topics', headers: { 'accept-encoding': 'gzip' } });
    assert.equal(first.statusCode, 200);
    assert.equal(first.headers['content-encoding'], 'gzip');
    assert.equal(first.headers['cache-control'], 'private, no-cache');
    assert.equal(first.headers.vary, 'accept-encoding');
    assert.equal(parse(gunzipSync(first.rawPayload)).topics[0]!.name, 'orders');

    const again = await server.inject({
      url: '/api/d/domain_a/topics',
      headers: { 'accept-encoding': 'gzip', 'if-none-match': String(first.headers.etag) },
    });
    assert.equal(again.statusCode, 304);
    assert.equal(again.rawPayload.length, 0);
  });

  it('serves plain JSON to a client that does not accept gzip', async () => {
    const server = await app([upstream(['orders'])]);

    const response = await server.inject({ url: '/api/d/domain_a/topics' });

    assert.equal(response.headers['content-encoding'], undefined);
    assert.equal(response.json<TopicCatalogResponse>().topics[0]!.name, 'orders');
  });

  it('refuses anyone who cannot browse topics', async () => {
    const server = Fastify();
    installTestAuth(server, testPrincipal(['materializations.view']));
    await server.register(async (scope) => registerTopicRoutes(scope, harness([upstream(['orders'])]).catalog), { prefix: '/api/d/:domain' });
    server.setErrorHandler((error: { statusCode?: number }, _request, reply) => reply.code(error.statusCode ?? 500).send({}));

    const response = await server.inject({ url: '/api/d/domain_a/topics' });

    assert.equal(response.statusCode, 403);
  });

  it('answers 503 while the topic list is unavailable', async () => {
    const server = await app([new Error('connection refused')]);

    const response = await server.inject({ url: '/api/d/domain_a/topics' });

    assert.equal(response.statusCode, 503);
  });
});
