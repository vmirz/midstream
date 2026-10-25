import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import Fastify, { type FastifyInstance } from 'fastify';

import { applySettingOverrides } from '../config.ts';
import { controlPlane } from '../controlPlane/client.ts';
import { ConsoleError } from '../errors.ts';
import { registerAuthRoutes } from '../routes/auth.ts';
import type { CpAuthSession, CpPrincipal } from '../../shared/types.ts';
import { parseCookies, secureCookie, sessionCookie } from './cookies.ts';
import { crossSite, permit, registerAuth, requirePermission } from './guard.ts';
import { forgetAllSessions, nobody, resolveAuth } from './principal.ts';
import { AttemptLimiter } from './rateLimit.ts';
import { testAuth, testPrincipal } from './testing.ts';

type Stub = Partial<Record<'session' | 'login' | 'logout' | 'refresh', (...args: never[]) => unknown>>;

const originals = {
  session: controlPlane.session,
  login: controlPlane.login,
  logout: controlPlane.logout,
  refresh: controlPlane.refresh,
};

function stub(methods: Stub): void {
  Object.assign(controlPlane, methods);
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

beforeEach(() => forgetAllSessions());
afterEach(() => {
  Object.assign(controlPlane, originals);
  applySettingOverrides({});
});

describe('the session cookie', () => {
  it('is HttpOnly and SameSite, and Secure behind TLS', () => {
    const cookie = sessionCookie('a b', { maxAgeSeconds: 120.6, secure: true });
    assert.match(cookie, /^console_session=a%20b; Path=\/; HttpOnly; SameSite=Lax; Max-Age=120; Secure$/);
    assert.doesNotMatch(sessionCookie('x', { maxAgeSeconds: 0, secure: false }), /Secure/);
    assert.equal(secureCookie('https'), true);
    assert.equal(secureCookie('http'), false);
  });

  it('is read back from a Cookie header', () => {
    const cookies = parseCookies('theme=dark; console_session=v1.abc.def; broken; =x; other="quoted"');
    assert.equal(cookies.get('console_session'), 'v1.abc.def');
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
  it('asks the control plane once per check interval', async () => {
    let asked = 0;
    stub({ session: (async () => (asked += 1, signedInSession())) as never });
    applySettingOverrides({ sessionCheckMs: 60_000 });
    const first = await resolveAuth('token-a');
    const second = await resolveAuth('token-a');
    assert.equal(asked, 1);
    assert.equal(first.state.principal.user_id, 'usr-test');
    assert.equal(second.token, 'token-a');
  });

  it('drops a session the control plane no longer accepts', async () => {
    stub({ session: (async () => ({ ...guestSession(), session_error: 'session_ended' })) as never });
    const auth = await resolveAuth('token-b');
    assert.equal(auth.invalidToken, true);
    assert.equal(auth.token, null);
    assert.equal(auth.state.sessionError, 'session_ended');
  });

  it('rides out a short outage on a session it checked recently', async () => {
    applySettingOverrides({ sessionCheckMs: 0 });
    stub({ session: (async () => signedInSession()) as never });
    await resolveAuth('token-c');
    stub({
      session: (async () => {
        throw new ConsoleError('control_plane_unreachable', 'down', 503);
      }) as never,
    });
    const during = await resolveAuth('token-c');
    assert.equal(during.error, null);
    assert.equal(during.state.authenticated, true);
    const unknown = await resolveAuth('token-never-seen');
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
    await registerAuthRoutes(server);
    server.get('/api/private', { preHandler: permit('materializations.view_details') }, async () => ({ ok: true }));
    return server;
  }

  it('keeps the token in an HttpOnly cookie and never in the answer', async () => {
    stub({
      session: (async () => guestSession()) as never,
      login: (async () => ({ status: 200, body: { ...signedInSession(), token: 'v1.issued.token' } })) as never,
    });
    const server = await app();
    const response = await server.inject({
      method: 'POST',
      url: '/api/auth/login',
      headers: { 'sec-fetch-site': 'same-origin' },
      payload: { email: 'test@example.com', password: 'secret password' },
    });
    assert.equal(response.statusCode, 200);
    assert.match(String(response.headers['set-cookie']), /^console_session=v1.issued.token; Path=\/; HttpOnly; SameSite=Lax; Max-Age=\d+$/);
    assert.equal(response.json().principal.email, 'test@example.com');
    assert.doesNotMatch(response.body, /v1\.issued\.token/);

    stub({ session: (async () => signedInSession()) as never });
    const allowed = await server.inject({ url: '/api/private', headers: { cookie: 'console_session=v1.issued.token' } });
    assert.equal(allowed.statusCode, 200);
    stub({ session: (async () => guestSession()) as never });
    const anonymous = await server.inject({ url: '/api/private' });
    assert.equal(anonymous.statusCode, 401);
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
      url: '/api/auth/login',
      payload: { email: 'test@example.com', password: 'wrong' },
    });
    assert.equal(response.statusCode, 401);
    assert.equal(response.json().error.message, 'Email or password is incorrect.');
    assert.equal(response.headers['set-cookie'], undefined);
  });

  it('turns away a flood of attempts from one address', async () => {
    applySettingOverrides({ signInAttemptsPerMinute: 1 });
    stub({
      session: (async () => guestSession()) as never,
      login: (async () => ({ status: 401, body: { error: { code: 'unauthenticated', message: 'no' } } })) as never,
    });
    const server = await app();
    const attempt = () =>
      server.inject({ method: 'POST', url: '/api/auth/login', remoteAddress: '9.9.9.9', payload: { email: 'a@b.co', password: 'x' } });
    assert.equal((await attempt()).statusCode, 401);
    const limited = await attempt();
    assert.equal(limited.statusCode, 429);
    assert.ok(Number(limited.headers['retry-after']) > 0);
  });

  it('refuses a change from another site before anything else', async () => {
    let asked = false;
    stub({ login: (async () => ((asked = true), { status: 200, body: signedInSession() })) as never });
    const server = await app();
    const response = await server.inject({
      method: 'POST',
      url: '/api/auth/login',
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
      url: '/api/auth/logout',
      headers: { cookie: 'console_session=v1.some.token' },
    });
    assert.equal(response.statusCode, 200);
    assert.ok(ended);
    assert.match(String(response.headers['set-cookie']), /Max-Age=0/);

    forgetAllSessions();
    stub({ session: (async () => ({ ...guestSession(), session_error: 'session_expired' })) as never });
    const expired = await server.inject({ url: '/api/auth/session', headers: { cookie: 'console_session=v1.old.token' } });
    assert.equal(expired.json().sessionError, 'session_expired');
    assert.match(String(expired.headers['set-cookie']), /Max-Age=0/);
  });

  it('renews a cookie session that is due, in a new cookie', async () => {
    const due = signedInSession();
    due.session!.refresh_after = new Date(Date.now() - 1_000).toISOString();
    stub({
      session: (async (caller: { token: string }) => (caller.token === 'v1.new.token' ? signedInSession() : due)) as never,
      refresh: (async () => ({ ...signedInSession(), token: 'v1.new.token' })) as never,
    });
    const server = await app();
    const response = await server.inject({ url: '/api/auth/session', headers: { cookie: 'console_session=v1.old.token' } });
    assert.equal(response.statusCode, 200);
    assert.match(String(response.headers['set-cookie']), /^console_session=v1.new.token;/);
  });
});
