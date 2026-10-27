import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

import type { ControlPlaneClient } from '../controlPlane/client.ts';
import type { Domain } from '../domains/domain.ts';
import { installTestDomains } from '../domains/testing.ts';
import { ConsoleError } from '../errors.ts';
import { registerAuth } from '../auth/guard.ts';
import { forgetAllSessions } from '../auth/principal.ts';
import { authorizeUrl, challengeFor, forgetFlows, safeReturnPath, startFlow, useProviders, useSsoFetch, type ProviderClient } from '../auth/sso.ts';
import { testPrincipal } from '../auth/testing.ts';
import { registerConsoleAuthRoutes } from './auth.ts';
import { registerSsoRoutes } from './sso.ts';
import type { CpAuthSession, SsoOutcome } from '../../shared/types.ts';

const domains = await installTestDomains(['domain_a', 'domain_b']);
const a = domains.require('domain_a');
const b = domains.require('domain_b');
const originals = new Map<Domain, Pick<ControlPlaneClient, 'ssoLogin' | 'session' | 'logout'>>(
  domains.all().map((domain) => [domain, { ssoLogin: domain.controlPlane.ssoLogin, session: domain.controlPlane.session, logout: domain.controlPlane.logout }]),
);

const DEX: ProviderClient = {
  id: 'oidc',
  label: 'Dex',
  clientId: 'console',
  clientSecret: 'console-secret',
  scopes: 'openid email profile',
  kind: 'oidc',
  endpoints: async () => ({ authorize: 'https://dex.test/auth', token: 'https://dex.test/token' }),
  extraParams: {},
};

const GITHUB: ProviderClient = {
  id: 'github',
  label: 'GitHub',
  clientId: 'gh-client',
  clientSecret: 'gh-secret',
  scopes: 'read:user user:email',
  kind: 'oauth',
  endpoints: async () => ({ authorize: 'https://github.test/authorize', token: 'https://github.test/token' }),
  extraParams: {},
};

function session(token: string): CpAuthSession & { token: string } {
  return {
    authenticated: true,
    principal: testPrincipal(['materializations.view']),
    session: {
      session_id: `ses-${token}`,
      method: 'sso:oidc',
      signed_in_at: new Date().toISOString(),
      issued_at: new Date().toISOString(),
      expires_at: new Date(Date.now() + 3_600_000).toISOString(),
      session_expires_at: new Date(Date.now() + 86_400_000).toISOString(),
      refresh_after: new Date(Date.now() + 600_000).toISOString(),
    },
    providers: [],
    token,
  };
}

/** The provider's token endpoint, and GitHub's revocation, as the flows reach them. */
const providerCalls: Array<{ url: string; body: string }> = [];
function fakeProvider(answer: Record<string, unknown>, status = 200): void {
  useSsoFetch((async (input: string | URL | Request, init?: RequestInit) => {
    providerCalls.push({ url: String(input), body: String(init?.body ?? '') });
    return new Response(JSON.stringify(answer), { status, headers: { 'content-type': 'application/json' } });
  }) as typeof fetch);
}

async function app(): Promise<FastifyInstance> {
  const server = Fastify({ trustProxy: true });
  server.setErrorHandler((error: ConsoleError, _request, reply) =>
    reply.code(error.statusCode ?? 500).send(error instanceof ConsoleError ? error.toBody() : { error: { code: 'internal_error' } }),
  );
  await registerAuth(server);
  await registerConsoleAuthRoutes(server);
  await registerSsoRoutes(server);
  return server;
}

/** Start a flow and come back from the provider with a code, as a browser would. */
async function signIn(server: FastifyInstance, provider = 'oidc', returnTo = '/d/domain_b/datasets') {
  const start = await server.inject({ method: 'GET', url: `/api/auth/sso/${provider}/start?return=${encodeURIComponent(returnTo)}` });
  assert.equal(start.statusCode, 302);
  const location = new URL(String(start.headers.location));
  const flowCookie = String(start.headers['set-cookie']).split(';')[0]!;
  const state = location.searchParams.get('state')!;
  const callback = await server.inject({
    method: 'GET',
    url: `/api/auth/sso/${provider}/callback?code=the-code&state=${encodeURIComponent(state)}`,
    headers: { cookie: flowCookie },
  });
  return { start, location, flowCookie, state, callback };
}

beforeEach(() => {
  forgetFlows();
  providerCalls.length = 0;
  useProviders([DEX, GITHUB]);
  for (const domain of domains.all()) forgetAllSessions(domain);
});
afterEach(() => {
  for (const [domain, methods] of originals) Object.assign(domain.controlPlane, methods);
  useProviders(null);
  useSsoFetch(null);
});

describe('a sign-in through a provider', () => {
  it('sends the browser to the provider with state, nonce and a PKCE challenge, bound to it by a cookie', async () => {
    const server = await app();
    const start = await server.inject({ method: 'GET', url: '/api/auth/sso/oidc/start?return=%2Fdatasets', headers: { host: 'console.test' } });
    assert.equal(start.statusCode, 302);
    const location = new URL(String(start.headers.location));
    assert.equal(location.origin + location.pathname, 'https://dex.test/auth');
    assert.equal(location.searchParams.get('client_id'), 'console');
    assert.equal(location.searchParams.get('redirect_uri'), 'http://console.test/api/auth/sso/oidc/callback');
    assert.equal(location.searchParams.get('code_challenge_method'), 'S256');
    assert.ok(location.searchParams.get('nonce'));
    assert.match(String(start.headers['set-cookie']), /^midstream_session-flow=[^;]+; Path=\/api\/auth\/sso; HttpOnly; SameSite=Lax; Max-Age=600/);
  });

  it('hands the ID token and its nonce to every domain and keeps a session cookie for each that admits the person', async () => {
    fakeProvider({ id_token: 'the-id-token', access_token: 'unused' });
    const asked: Array<{ domain: string; input: unknown }> = [];
    for (const domain of [a, b]) {
      domain.controlPlane.ssoLogin = (async (_caller: unknown, input: unknown) => {
        asked.push({ domain: domain.id, input });
        return domain === a
          ? { status: 200, body: session(`token-${domain.id}`) }
          : { status: 403, body: { error: { code: 'forbidden', message: 'No account here.', details: { reason: 'no_account' } } } };
      }) as never;
    }
    const server = await app();
    const { location, callback } = await signIn(server);

    assert.deepEqual(asked.map((entry) => entry.domain).sort(), ['domain_a', 'domain_b']);
    assert.deepEqual(asked[0]!.input, { provider: 'oidc', credential: { id_token: 'the-id-token' }, nonce: location.searchParams.get('nonce') });
    const tokenRequest = new URLSearchParams(providerCalls[0]!.body);
    assert.equal(tokenRequest.get('code'), 'the-code');
    assert.equal(challengeFor(tokenRequest.get('code_verifier')!), location.searchParams.get('code_challenge'));

    const cookies = ([] as string[]).concat(callback.headers['set-cookie'] as string | string[]);
    assert.ok(cookies.some((value) => value.startsWith('midstream_session_domain_a=token-domain_a;')));
    assert.ok(!cookies.some((value) => value.startsWith('midstream_session_domain_b=')));
    assert.ok(cookies.some((value) => value.startsWith('midstream_session-flow=;') && value.includes('Max-Age=0')));

    const back = new URL(String(callback.headers.location), 'http://console.test');
    assert.equal(back.pathname, '/d/domain_b/datasets');
    const outcome = (await server.inject({ method: 'GET', url: `/api/auth/sso/outcomes/${back.searchParams.get('signin')}` })).json() as SsoOutcome;
    assert.deepEqual(
      outcome.domains.map((entry) => [entry.domain, entry.status, entry.reason]),
      [
        ['domain_a', 'signed_in', null],
        ['domain_b', 'refused', 'no_account'],
      ],
    );
  });

  it('works once: a callback repeated, from another browser or for another provider is refused', async () => {
    fakeProvider({ id_token: 'the-id-token' });
    a.controlPlane.ssoLogin = (async () => ({ status: 200, body: session('t') })) as never;
    b.controlPlane.ssoLogin = a.controlPlane.ssoLogin;
    const server = await app();
    const { state, flowCookie } = await signIn(server);
    for (const attempt of [
      { url: `/api/auth/sso/oidc/callback?code=c&state=${state}`, cookie: flowCookie },
    ]) {
      const again = await server.inject({ method: 'GET', url: attempt.url, headers: { cookie: attempt.cookie } });
      assert.match(String(again.headers.location), /^\/sign-in\?signin=/);
    }
    const second = await server.inject({ method: 'GET', url: '/api/auth/sso/oidc/start' });
    const secondState = new URL(String(second.headers.location)).searchParams.get('state')!;
    const stranger = await server.inject({ method: 'GET', url: `/api/auth/sso/oidc/callback?code=c&state=${secondState}`, headers: { cookie: 'midstream_session-flow=forged' } });
    const outcomeId = new URL(String(stranger.headers.location), 'http://x').searchParams.get('signin')!;
    const outcome = (await server.inject({ method: 'GET', url: `/api/auth/sso/outcomes/${outcomeId}` })).json() as SsoOutcome;
    assert.match(outcome.error ?? '', /another browser/);
    assert.equal(outcome.domains.length, 0);
  });

  it('says why when the provider turns the person away, without asking any domain', async () => {
    let asked = 0;
    a.controlPlane.ssoLogin = (async () => {
      asked += 1;
      return { status: 200, body: session('t') };
    }) as never;
    const server = await app();
    const start = await server.inject({ method: 'GET', url: '/api/auth/sso/oidc/start' });
    const state = new URL(String(start.headers.location)).searchParams.get('state')!;
    const cookie = String(start.headers['set-cookie']).split(';')[0]!;
    const back = await server.inject({ method: 'GET', url: `/api/auth/sso/oidc/callback?error=access_denied&error_description=User%20declined&state=${state}`, headers: { cookie } });
    const id = new URL(String(back.headers.location), 'http://x').searchParams.get('signin')!;
    const outcome = (await server.inject({ method: 'GET', url: `/api/auth/sso/outcomes/${id}` })).json() as SsoOutcome;
    assert.equal(outcome.error, 'Dex did not sign you in: User declined');
    assert.equal(asked, 0);
  });

  it('revokes a GitHub token once every domain has checked it', async () => {
    fakeProvider({ access_token: 'gho_once' });
    for (const domain of [a, b]) domain.controlPlane.ssoLogin = (async () => ({ status: 200, body: session(domain.id) })) as never;
    const server = await app();
    const { callback } = await signIn(server, 'github', '/');
    assert.equal(callback.statusCode, 302);
    await new Promise((resolve) => setTimeout(resolve, 10));
    const revoke = providerCalls.find((call) => call.url.endsWith('/applications/gh-client/token'));
    assert.deepEqual(JSON.parse(revoke!.body), { access_token: 'gho_once' });
  });

  it('lists the providers in the session, and refuses one this console is not registered with', async () => {
    const server = await app();
    const answer = (await server.inject({ method: 'GET', url: '/api/auth/session' })).json() as { providers: unknown };
    assert.deepEqual(answer.providers, [
      { id: 'oidc', label: 'Dex' },
      { id: 'github', label: 'GitHub' },
    ]);
    assert.equal((await server.inject({ method: 'GET', url: '/api/auth/sso/google/start' })).statusCode, 404);
  });
});

describe('where a sign-in returns to', () => {
  it('is only ever a path of this console', () => {
    assert.equal(safeReturnPath('/d/domain_a/materializations?x=1'), '/d/domain_a/materializations?x=1');
    for (const unsafe of ['https://evil.test/', '//evil.test', '/\\evil.test', 'relative', '/sign-in', '/api/auth/logout', 42]) {
      assert.equal(safeReturnPath(unsafe), '/', String(unsafe));
    }
  });

  it('asks the provider for exactly the registered callback', () => {
    const { flow } = startFlow('oidc', '/', 'https://console.test/api/auth/sso/oidc/callback');
    const url = new URL(authorizeUrl(DEX, 'https://dex.test/auth?existing=1', flow));
    assert.equal(url.searchParams.get('existing'), '1');
    assert.equal(url.searchParams.get('redirect_uri'), 'https://console.test/api/auth/sso/oidc/callback');
    assert.equal(url.searchParams.get('response_type'), 'code');
  });
});
