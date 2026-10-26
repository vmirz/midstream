import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

import { applySettingOverrides } from '../config.ts';
import type { ControlPlaneClient } from '../controlPlane/client.ts';
import type { Domain } from '../domains/domain.ts';
import { installTestDomains } from '../domains/testing.ts';
import { ConsoleError } from '../errors.ts';
import { registerAuthRoutes, registerConsoleAuthRoutes } from '../routes/auth.ts';
import type { CpAuthSession, CpPrincipal } from '../../shared/types.ts';
import { parseCookies, secureCookie, sessionCookie } from './cookies.ts';
import { crossSite, permit, registerAuth, requirePermission } from './guard.ts';
import { forgetAllSessions, nobody, resolveAuth } from './principal.ts';
import { AttemptLimiter } from './rateLimit.ts';
import { testAuth, testPrincipal } from './testing.ts';

type Stub = Partial<Record<'session' | 'login' | 'logout' | 'refresh', (...args: never[]) => unknown>>;

const domains = await installTestDomains(['domain_a', 'domain_b']);
const a = domains.require('domain_a');
const b = domains.require('domain_b');
const originals = new Map<Domain, Pick<ControlPlaneClient, 'session' | 'login' | 'logout' | 'refresh'>>(
  domains.all().map((domain) => [
    domain,
    {
      session: domain.controlPlane.session,
      login: domain.controlPlane.login,
      logout: domain.controlPlane.logout,
      refresh: domain.controlPlane.refresh,
    },
  ]),
);

function stub(methods: Stub, domain: Domain = a): void {
  Object.assign(domain.controlPlane, methods);
}

function principal(overrides: Partial<CpPrincipal> = {}): CpPrincipal {
  return testPrincipal(['materializations.view', 'materializations.view_details'], overrides);
}

function signedInSession(overrides: Partial<CpAuthSession> = {}): CpAuthSession {
  return {
    authenticated: true,
    principal: principal(),
    session: {
      session_id: 'ses-1',
      method: 'password',
      signed_in_at: new Date().toISOString(),
      issued_at: new Date().toISOString(),
      expires_at: new Date(Date.now() + 3_600_000).toISOString(),
      session_expires_at: new Date(Date.now() + 86_400_000).toISOString(),
      refresh_after: new Date(Date.now() + 600_000).toISOString(),
    },
    providers: [{ id: 'password', type: 'password', label: 'Email and password' }],
    ...overrides,
  };
}

function guestSession(): CpAuthSession {
  return { authenticated: false, principal: { ...nobody(), permissions: ['materializations.view'] }, session: null, providers: [] };
}

beforeEach(() => {
  for (const domain of domains.all()) forgetAllSessions(domain);
  stub({ session: (async () => guestSession()) as never }, b);
});
afterEach(() => {
  for (const [domain, methods] of originals) Object.assign(domain.controlPlane, methods);
  applySettingOverrides({});
});

describe('the session cookies', () => {
  it('are one per domain, HttpOnly and SameSite, and Secure behind TLS', () => {
    const cookie = sessionCookie('domain_a', 'a b', { maxAgeSeconds: 120.6, secure: true });
    assert.match(cookie, /^midstream_session_domain_a=a%20b; Path=\/; HttpOnly; SameSite=Lax; Max-Age=120; Secure$/);
    assert.doesNotMatch(sessionCookie('domain_a', 'x', { maxAgeSeconds: 0, secure: false }), /Secure/);
    assert.equal(secureCookie('https'), true);
    assert.equal(secureCookie('http'), false);
  });

  it('are read back from a Cookie header', () => {
    const cookies = parseCookies('theme=dark; midstream_session_domain_a=v1.abc.def; broken; =x; other="quoted"');
    assert.equal(cookies.get('midstream_session_domain_a'), 'v1.abc.def');
    assert.equal(cookies.get('other'), 'quoted');
    assert.equal(parseCookies(undefined).size, 0);
  });

});

describe('cross-site changes', () => {
  const request = (headers: Record<string, string>) => ({ headers }) as never;

  it('trusts what the browser says about the site', () => {
    assert.equal(crossSite(request({ 'sec-fetch-site': 'same-origin' })), false);
    assert.equal(crossSite(request({ 'sec-fetch-site': 'none' })), false);
    assert.equal(crossSite(request({ 'sec-fetch-site': 'cross-site', origin: 'https://evil.example' })), true);
    assert.equal(crossSite(request({ 'sec-fetch-site': 'same-site' })), true);
  });

  it('falls back to comparing the origin with the host', () => {
    assert.equal(crossSite(request({ origin: 'https://console.example', host: 'console.example' })), false);
    assert.equal(crossSite(request({ origin: 'https://evil.example', host: 'console.example' })), true);
    assert.equal(crossSite(request({})), false, 'not a browser: nothing to protect');
  });
});

describe('sign-in attempts per address', () => {
  it('are limited over a sliding minute', () => {
    const limiter = new AttemptLimiter(60_000);
    assert.ok(limiter.allow('1.2.3.4', 2, 0));
    assert.ok(limiter.allow('1.2.3.4', 2, 1_000));
    assert.equal(limiter.allow('1.2.3.4', 2, 2_000), false);
    assert.equal(limiter.retryAfter('1.2.3.4', 2_000), 58);
    assert.ok(limiter.allow('5.6.7.8', 2, 2_000), 'other addresses are counted apart');
    assert.ok(limiter.allow('1.2.3.4', 2, 60_001), 'the first attempt has left the window');
  });
});

describe('permissions', () => {
  it('ask a guest to sign in and tell a person they lack access', () => {
    const guest = testAuth({ ...nobody(), permissions: ['materializations.view'] });
    requirePermission(guest, 'materializations.view');
    assert.throws(() => requirePermission(guest, 'data.query'), (error: ConsoleError) => error.statusCode === 401);
    const person = testAuth(testPrincipal(['materializations.view']));
    assert.throws(() => requirePermission(person, 'data.query'), (error: ConsoleError) => error.statusCode === 403);
    requirePermission(testAuth(testPrincipal(['*'])), 'settings.manage');
  });

  it('hold back everything until a chosen password is replaced', () => {
    const auth = testAuth(testPrincipal(['*'], { password_change_required: true }));
    assert.throws(
      () => requirePermission(auth, 'materializations.view'),
      (error: ConsoleError) => error.details.reason === 'password_change_required',
    );
  });

  it('report an unreachable control plane rather than refusing', () => {
    const auth = { ...testAuth(nobody(), 'token'), error: new ConsoleError('control_plane_unreachable', 'down', 503) };
    assert.throws(() => requirePermission(auth, 'materializations.view'), (error: ConsoleError) => error.statusCode === 503);
  });
});

describe('resolving a session', () => {
  it('asks the domain once per check interval', async () => {
    let asked = 0;
    stub({ session: (async () => (asked += 1, signedInSession())) as never });
    applySettingOverrides({ sessionCheckMs: 60_000 });
    const first = await resolveAuth(a, 'token-a');
    const second = await resolveAuth(a, 'token-a');
    assert.equal(asked, 1);
    assert.equal(first.state.principal.user_id, 'usr-test');
    assert.equal(second.token, 'token-a');
  });

  it('keeps each domain’s answer apart: one token, two domains, two checks', async () => {
    let asked = 0;
    applySettingOverrides({ sessionCheckMs: 60_000 });
    stub({ session: (async () => (asked += 1, signedInSession())) as never });
    stub({ session: (async () => (asked += 1, { ...guestSession(), session_error: 'invalid_session' })) as never }, b);
    assert.equal((await resolveAuth(a, 'token-x')).state.authenticated, true);
    assert.equal((await resolveAuth(b, 'token-x')).state.authenticated, false);
    assert.equal(asked, 2);
    forgetAllSessions(b);
    assert.equal((await resolveAuth(a, 'token-x')).state.authenticated, true, 'forgetting one domain’s sessions keeps the others');
    assert.equal(asked, 2);
  });

  it('drops a session the domain no longer accepts', async () => {
    stub({ session: (async () => ({ ...guestSession(), session_error: 'session_ended' })) as never });
    const auth = await resolveAuth(a, 'token-b');
    assert.equal(auth.invalidToken, true);
    assert.equal(auth.token, null);
    assert.equal(auth.state.sessionError, 'session_ended');
  });

  it('rides out a short outage on a session it checked recently', async () => {
    applySettingOverrides({ sessionCheckMs: 0 });
    stub({ session: (async () => signedInSession()) as never });
    await resolveAuth(a, 'token-c');
    stub({
      session: (async () => {
        throw new ConsoleError('control_plane_unreachable', 'down', 503);
      }) as never,
    });
    const during = await resolveAuth(a, 'token-c');
    assert.equal(during.error, null);
    assert.equal(during.state.authenticated, true);
    const unknown = await resolveAuth(a, 'token-never-seen');
    assert.equal(unknown.error?.statusCode, 503);
  });
});

describe('signing in and out', () => {
  async function app(): Promise<FastifyInstance> {
    const server = Fastify({ trustProxy: true });
    server.setErrorHandler((error: ConsoleError, _request, reply) =>
      reply.code(error.statusCode ?? 500).send(error instanceof ConsoleError ? error.toBody() : { error: { code: 'internal_error' } }),
    );
    await registerAuth(server);
    await registerConsoleAuthRoutes(server);
    await server.register(
      async (scope) => {
        await registerAuthRoutes(scope);
        scope.get('/private', { preHandler: permit('materializations.view_details') }, async () => ({ ok: true }));
      },
      { prefix: '/api/d/:domain' },
    );
    return server;
  }

  it('keeps each domain’s token in an HttpOnly cookie of its own and never in the answer', async () => {
    stub({
      session: (async () => guestSession()) as never,
      login: (async () => ({ status: 200, body: { ...signedInSession(), token: 'v1.issued.token' } })) as never,
    });
    const server = await app();
    const response = await server.inject({
      method: 'POST',
      url: '/api/d/domain_a/auth/login',
      headers: { 'sec-fetch-site': 'same-origin' },
      payload: { email: 'test@example.com', password: 'secret password' },
    });
    assert.equal(response.statusCode, 200);
    assert.match(String(response.headers['set-cookie']), /^midstream_session_domain_a=v1.issued.token; Path=\/; HttpOnly; SameSite=Lax; Max-Age=\d+$/);
    assert.equal(response.json().principal.email, 'test@example.com');
    assert.doesNotMatch(response.body, /v1\.issued\.token/);

    stub({ session: (async () => signedInSession()) as never });
    const allowed = await server.inject({ url: '/api/d/domain_a/private', headers: { cookie: 'midstream_session_domain_a=v1.issued.token' } });
    assert.equal(allowed.statusCode, 200);
    const elsewhere = await server.inject({ url: '/api/d/domain_b/private', headers: { cookie: 'midstream_session_domain_a=v1.issued.token' } });
    assert.equal(elsewhere.statusCode, 401, 'a session in one domain is nobody in another');
    stub({ session: (async () => guestSession()) as never });
    const anonymous = await server.inject({ url: '/api/d/domain_a/private' });
    assert.equal(anonymous.statusCode, 401);
  });

  it('says who the browser is in every domain', async () => {
    stub({ session: (async () => signedInSession()) as never });
    const server = await app();
    const response = await server.inject({ url: '/api/auth/session', headers: { cookie: 'midstream_session_domain_a=v1.a.token' } });
    assert.equal(response.statusCode, 200);
    const body = response.json();
    assert.deepEqual(Object.keys(body.domains), ['domain_a', 'domain_b']);
    assert.equal(body.domains.domain_a.state.authenticated, true);
    assert.equal(body.domains.domain_b.state.authenticated, false);
    assert.deepEqual(body.providers, []);
  });

  it('answers a domain it does not serve with 404', async () => {
    const server = await app();
    const response = await server.inject({ url: '/api/d/domain_z/private' });
    assert.equal(response.statusCode, 404);
    assert.equal(response.json().error.details.reason, 'unknown_domain');
  });

  it('passes on why a sign-in failed', async () => {
    stub({
      session: (async () => guestSession()) as never,
      login: (async () => ({
        status: 401,
        body: { error: { code: 'unauthenticated', message: 'Email or password is incorrect.', details: { reason: 'invalid_credentials' } } },
      })) as never,
    });
    const server = await app();
    const response = await server.inject({
      method: 'POST',
      url: '/api/d/domain_a/auth/login',
      payload: { email: 'test@example.com', password: 'wrong' },
    });
    assert.equal(response.statusCode, 401);
    assert.equal(response.json().error.message, 'Email or password is incorrect.');
    assert.equal(response.headers['set-cookie'], undefined);
  });

  it('turns away a flood of attempts from one address, whichever domain they are for', async () => {
    applySettingOverrides({ signInAttemptsPerMinute: 1 });
    for (const domain of [a, b]) {
      stub(
        {
          session: (async () => guestSession()) as never,
          login: (async () => ({ status: 401, body: { error: { code: 'unauthenticated', message: 'no' } } })) as never,
        },
        domain,
      );
    }
    const server = await app();
    const attempt = (domain: string) =>
      server.inject({ method: 'POST', url: `/api/d/${domain}/auth/login`, remoteAddress: '9.9.9.9', payload: { email: 'a@b.co', password: 'x' } });
    assert.equal((await attempt('domain_a')).statusCode, 401);
    const limited = await attempt('domain_b');
    assert.equal(limited.statusCode, 429);
    assert.ok(Number(limited.headers['retry-after']) > 0);
  });

  it('refuses a change from another site before anything else', async () => {
    let asked = false;
    stub({ login: (async () => ((asked = true), { status: 200, body: signedInSession() })) as never });
    const server = await app();
    const response = await server.inject({
      method: 'POST',
      url: '/api/d/domain_a/auth/login',
      headers: { 'sec-fetch-site': 'cross-site', origin: 'https://evil.example' },
      payload: { email: 'a@b.co', password: 'x' },
    });
    assert.equal(response.statusCode, 403);
    assert.equal(asked, false);
  });

  it('clears the cookie on sign-out and when the session has ended', async () => {
    let ended = false;
    stub({
      session: (async () => signedInSession()) as never,
      logout: (async () => ((ended = true), { signed_out: true })) as never,
    });
    const server = await app();
    const response = await server.inject({
      method: 'POST',
      url: '/api/d/domain_a/auth/logout',
      headers: { cookie: 'midstream_session_domain_a=v1.some.token' },
    });
    assert.equal(response.statusCode, 200);
    assert.ok(ended);
    assert.match(String(response.headers['set-cookie']), /^midstream_session_domain_a=; .*Max-Age=0/);

    forgetAllSessions(a);
    stub({ session: (async () => ({ ...guestSession(), session_error: 'session_expired' })) as never });
    const expired = await server.inject({ url: '/api/d/domain_a/auth/session', headers: { cookie: 'midstream_session_domain_a=v1.old.token' } });
    assert.equal(expired.json().sessionError, 'session_expired');
    assert.match(String(expired.headers['set-cookie']), /Max-Age=0/);
  });

  it('signs out of every domain at once', async () => {
    const ended: string[] = [];
    for (const domain of [a, b]) {
      stub(
        {
          session: (async () => signedInSession()) as never,
          logout: (async () => (ended.push(domain.id), { signed_out: true })) as never,
        },
        domain,
      );
    }
    const server = await app();
    const response = await server.inject({
      method: 'POST',
      url: '/api/auth/logout',
      headers: { cookie: 'midstream_session_domain_a=v1.a; midstream_session_domain_b=v1.b' },
    });
    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json().domains, ['domain_a', 'domain_b']);
    assert.deepEqual(ended.sort(), ['domain_a', 'domain_b']);
    const cookies = ([] as string[]).concat(response.headers['set-cookie'] as string[]);
    assert.equal(cookies.filter((cookie) => /Max-Age=0/.test(cookie)).length, 2);
  });

  it('renews a cookie session that is due, in a new cookie', async () => {
    const due = signedInSession();
    due.session!.refresh_after = new Date(Date.now() - 1_000).toISOString();
    stub({
      session: (async (caller: { token: string }) => (caller.token === 'v1.new.token' ? signedInSession() : due)) as never,
      refresh: (async () => ({ ...signedInSession(), token: 'v1.new.token' })) as never,
    });
    const server = await app();
    const response = await server.inject({ url: '/api/d/domain_a/auth/session', headers: { cookie: 'midstream_session_domain_a=v1.old.token' } });
    assert.equal(response.statusCode, 200);
    assert.match(String(response.headers['set-cookie']), /^midstream_session_domain_a=v1.new.token;/);
  });
});

describe('a domain whose control plane says it is another', () => {
  it('is served nothing, and left out of what every domain says', async () => {
    const misconfigured = await installTestDomains(['domain_a', 'domain_b'], { reports: { domain_b: 'domain_c' } });
    try {
      const server = Fastify();
      server.setErrorHandler((error: ConsoleError, _request, reply) => reply.code(error.statusCode ?? 500).send(error.toBody()));
      await registerAuth(server);
      await registerConsoleAuthRoutes(server);
      await server.register(async (scope) => registerAuthRoutes(scope), { prefix: '/api/d/:domain' });
      for (const domain of misconfigured.all()) Object.assign(domain.controlPlane, { session: async () => guestSession() });
      const refused = await server.inject({ url: '/api/d/domain_b/auth/session' });
      assert.equal(refused.statusCode, 503);
      assert.equal(refused.json().error.details.reason, 'domain_misconfigured');
      assert.match(refused.json().error.message, /domain_c/);
      const session = await server.inject({ url: '/api/auth/session' });
      assert.deepEqual(Object.keys(session.json().domains), ['domain_a']);
    } finally {
      await installTestDomains(['domain_a', 'domain_b']);
    }
  });
});
