import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

import type { ControlPlaneClient } from '../controlPlane/client.ts';
import type { Domain } from '../domains/domain.ts';
import { installTestDomains } from '../domains/testing.ts';
import { ConsoleError } from '../errors.ts';
import { registerAuthRoutes } from '../routes/auth.ts';
import type { CpTokenRevocation } from '../../shared/types.ts';
import { forgetAllSessions } from './principal.ts';
import { RevocationSync, handOn } from './revocations.ts';
import { installTestAuth, testPrincipal } from './testing.ts';

const domains = await installTestDomains(['domain_a', 'domain_b', 'domain_c']);
const [a, b, c] = ['domain_a', 'domain_b', 'domain_c'].map((id) => domains.require(id)) as [Domain, Domain, Domain];
type Methods = Pick<ControlPlaneClient, 'tokenRevocations' | 'applyTokenRevocations' | 'revokeAccessToken' | 'createAccessToken' | 'identityRequest'>;
const originals = new Map<Domain, Methods>(
  domains.all().map((domain) => [
    domain,
    {
      tokenRevocations: domain.controlPlane.tokenRevocations,
      applyTokenRevocations: domain.controlPlane.applyTokenRevocations,
      revokeAccessToken: domain.controlPlane.revokeAccessToken,
      createAccessToken: domain.controlPlane.createAccessToken,
      identityRequest: domain.controlPlane.identityRequest,
    },
  ]),
);

const REVOKED: CpTokenRevocation = { jti: '0123456789ab', exp: 1_900_000_000, signature: 'k1.sig', revoked_at: '2026-10-26T12:00:00Z', source: 'domain_a' };

/** What each domain was handed, by domain id. */
let handed: Record<string, CpTokenRevocation[][]>;
let down: Set<string>;

beforeEach(() => {
  handed = {};
  down = new Set();
  for (const domain of domains.all()) {
    forgetAllSessions(domain);
    domain.controlPlane.applyTokenRevocations = (async (_caller: unknown, revocations: CpTokenRevocation[]) => {
      if (down.has(domain.id)) throw new ConsoleError('control_plane_unreachable', 'down', 503);
      (handed[domain.id] ??= []).push(revocations);
      return { request_id: 'idr-1', accepted: revocations.length };
    }) as never;
  }
});
afterEach(() => {
  for (const [domain, methods] of originals) Object.assign(domain.controlPlane, methods);
});

describe('handing a revocation on', () => {
  it('reaches every other domain, and says which could not be told', async () => {
    down.add('domain_c');
    const results = await handOn([REVOKED], a, domains.served());
    assert.deepEqual(results, { domain_b: 'applied', domain_c: 'failed' });
    assert.deepEqual(handed.domain_b, [[REVOKED]]);
    assert.equal(handed.domain_a, undefined);
  });

  it('never hands a domain back what came from it', async () => {
    await handOn([{ ...REVOKED, source: 'domain_b' }], c, domains.served());
    assert.equal(handed.domain_b?.flat().length ?? 0, 0);
    assert.equal(handed.domain_a?.flat().length, 1);
  });
});

describe('the five-minute pass', () => {
  it('hands on what each domain revoked since it last looked, and keeps trying a domain that was down', async () => {
    const asked: Array<[string, string | null]> = [];
    a.controlPlane.tokenRevocations = (async (_caller: unknown, since: string | null) => {
      asked.push(['domain_a', since]);
      return { revocations: since && since >= REVOKED.revoked_at! ? [] : [REVOKED] };
    }) as never;
    for (const domain of [b, c]) domain.controlPlane.tokenRevocations = (async () => ({ revocations: [] })) as never;
    const sync = new RevocationSync(() => domains.served());

    down.add('domain_c');
    assert.equal(await sync.syncOnce(), 1);
    assert.equal(handed.domain_c, undefined);
    // Domain C was down: the next pass starts from the beginning again.
    down.clear();
    assert.equal(await sync.syncOnce(), 1);
    assert.deepEqual(handed.domain_c, [[REVOKED]]);
    // Everyone has it now: the pass after starts after it.
    assert.equal(await sync.syncOnce(), 0);
    assert.deepEqual(asked.map(([, since]) => since), [null, null, REVOKED.revoked_at]);
  });

  it('carries on past a domain it cannot read', async () => {
    a.controlPlane.tokenRevocations = (async () => {
      throw new ConsoleError('control_plane_unreachable', 'down', 503);
    }) as never;
    b.controlPlane.tokenRevocations = (async () => ({ revocations: [{ ...REVOKED, source: 'domain_b' }] })) as never;
    c.controlPlane.tokenRevocations = (async () => ({ revocations: [] })) as never;
    assert.equal(await new RevocationSync(() => domains.served()).syncOnce(), 1);
    assert.equal(handed.domain_a?.length, 1);
  });
});

describe('the token routes', () => {
  async function app(): Promise<FastifyInstance> {
    const server = Fastify();
    server.setErrorHandler((error: ConsoleError, _request, reply) =>
      reply.code(error.statusCode ?? 500).send(error instanceof ConsoleError ? error.toBody() : { error: { code: 'internal_error' } }),
    );
    installTestAuth(server, testPrincipal(['catalog.read'], { user_id: 'usr-1', email: 'ada@example.com' }));
    await server.register(async (scope) => registerAuthRoutes(scope), { prefix: '/api/d/:domain' });
    return server;
  }

  it('revokes where the token was made and hands the revocation to every other domain', async () => {
    a.controlPlane.revokeAccessToken = (async () => ({ request_id: 'idr-r', revocation: REVOKED })) as never;
    a.controlPlane.identityRequest = (async () => ({ request_id: 'idr-r', status: 'applied' })) as never;
    down.add('domain_c');
    const server = await app();
    const response = await server.inject({ method: 'DELETE', url: '/api/d/domain_a/auth/tokens/0123456789ab' });
    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), { request_id: 'idr-r', status: 'applied', domains: { domain_b: 'applied', domain_c: 'failed' } });
  });

  it('shows a new token only once its domain has recorded it', async () => {
    a.controlPlane.createAccessToken = (async () => ({ request_id: 'idr-c', token: 'tok_secret.text', token_id: '0123456789ab' })) as never;
    a.controlPlane.identityRequest = (async () => ({ request_id: 'idr-c', status: 'rejected', reason: 'limit', message: 'You have 20 tokens already.' })) as never;
    const server = await app();
    const refused = await server.inject({ method: 'POST', url: '/api/d/domain_a/auth/tokens', payload: { name: 'x', permissions: ['catalog.read'], expires_in_days: 30 } });
    assert.equal(refused.statusCode, 409);
    assert.doesNotMatch(refused.body, /tok_secret/);

    a.controlPlane.identityRequest = (async () => ({ request_id: 'idr-c', status: 'applied' })) as never;
    const made = await server.inject({
      method: 'POST',
      url: '/api/d/domain_a/auth/tokens',
      payload: { name: 'x', permissions: ['catalog.read'], expires_in_days: 30, datasets: ['domain_b.finance', 'sales'] },
    });
    assert.equal(made.statusCode, 201);
    assert.equal(made.json().token, 'tok_secret.text');
    assert.equal(made.headers['cache-control'], 'no-store');
    const bad = await server.inject({ method: 'POST', url: '/api/d/domain_a/auth/tokens', payload: { name: 'x', permissions: ['catalog.read'], expires_in_days: 30, datasets: ['Not A Name'] } });
    assert.equal(bad.statusCode, 400);
  });
});
