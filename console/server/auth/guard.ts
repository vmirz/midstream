/**
 * Authentication and authorization for the console's own API.
 *
 * Every `/api/*` request is resolved to a caller before its route runs: a session from the cookie
 * (or an `Authorization: Bearer` header, for scripts), else a guest. Routes declare the permission
 * they need with `permit(...)`. A cookie session is renewed as it is used, so it ends only after the
 * idle timeout or the maximum session length. Changes from another site are refused.
 */

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import { controlPlane, type UserCaller } from '../controlPlane/client.ts';
import { config } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import type { AuthState } from '../../shared/types.ts';
import { parseCookies, secureCookie, sessionCookie } from './cookies.ts';
import { can, canAnywhere, forgetSession, guestAuth, rememberSession, resolveAuth, tokenKey, type RequestAuth } from './principal.ts';

declare module 'fastify' {
  interface FastifyRequest {
    /** Who made this request, resolved before any `/api/*` route runs. */
    auth: RequestAuth | null;
  }
}

/** Routes that need no caller at all. */
const UNAUTHENTICATED = ['/api/health'];
const MUTATIONS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

const renewals = new Map<string, Promise<string | null>>();

export async function registerAuth(app: FastifyInstance): Promise<void> {
  app.decorateRequest('auth', null);

  app.addHook('onRequest', async (request, reply) => {
    if (!request.url.startsWith('/api/')) return;
    if (MUTATIONS.has(request.method) && crossSite(request)) {
      throw new ConsoleError('forbidden', 'Changes can only be made from the console itself.', 403, {
        reason: 'cross_site_request',
      });
    }
    if (UNAUTHENTICATED.some((path) => request.url === path || request.url.startsWith(`${path}?`))) return;

    const presented = presentedToken(request);
    let auth = await resolveAuth(presented.token);
    if (presented.source === 'cookie') {
      if (auth.invalidToken) {
        clearSessionCookie(request, reply);
      } else if (auth.token && dueForRenewal(auth.state)) {
        auth = await renew(request, reply, auth);
      }
    }
    request.auth = auth;
  });
}

/** A route's guard: the caller must hold `permission`. */
export function permit(permission: string) {
  return async (request: FastifyRequest): Promise<void> => {
    requirePermission(authOf(request), permission);
  };
}

/**
 * A route's guard for a dataset permission: the caller must hold it on at least one dataset. The
 * route then checks it on the datasets the request is about (`requireOnDatasets`).
 */
export function permitAnywhere(permission: string) {
  return async (request: FastifyRequest): Promise<void> => {
    requirePermission(authOf(request), permission, { anywhere: true });
  };
}

/** A dataset, as a refusal names it. */
export interface DatasetRef {
  id: string;
  name: string;
}

/**
 * Refuse a caller who lacks a dataset permission on any of `datasets`, naming the first dataset
 * they lack it on: 401 for a guest, who could sign in, 403 for anyone else.
 */
export function requireOnDatasets(auth: RequestAuth, permission: string, datasets: Iterable<DatasetRef>, label = permissionLabel(permission)): void {
  const principal = auth.state.principal;
  if (principal.password_change_required) throw passwordChangeRequired();
  for (const dataset of datasets) {
    if (can(principal, permission, dataset.id)) continue;
    if (auth.error) throw auth.error;
    const where = ` on dataset ${dataset.name}`;
    if (principal.kind !== 'user') {
      throw new ConsoleError('unauthenticated', `Sign in to continue: this needs “${label}”${where}.`, 401, {
        reason: 'sign_in_required',
        permission,
        dataset: dataset.name,
      });
    }
    throw new ConsoleError('forbidden', `You do not have permission to do this: it needs “${label}”${where}.`, 403, {
      reason: 'forbidden',
      permission,
      dataset: dataset.name,
    });
  }
}

/** What a refusal calls a dataset permission. */
const PERMISSION_LABELS: Record<string, string> = {
  'dataset.view_schema': 'View schemas',
  'data.query': 'Query data',
  'data.export': 'Export data',
  'catalog.read': 'Connect external tools to the Iceberg catalog',
  'catalog.write': 'Commit to tables from external tools',
  'dataset.write': 'Add and drop tables',
  'dataset.manage': 'Manage datasets',
};

function permissionLabel(permission: string): string {
  return PERMISSION_LABELS[permission] ?? permission;
}

/** A route's guard: the caller must be signed in, whatever they may do. */
export function signedIn(options: { allowPasswordChange?: boolean } = {}) {
  return async (request: FastifyRequest): Promise<void> => {
    const auth = authOf(request);
    if (auth.error && !auth.state.authenticated && auth.token) throw auth.error;
    if (auth.state.principal.kind !== 'user') {
      throw new ConsoleError('unauthenticated', 'Sign in to continue.', 401, { reason: 'sign_in_required' });
    }
    if (auth.state.principal.password_change_required && !options.allowPasswordChange) throw passwordChangeRequired();
  };
}

/** How often an open stream checks that its caller may still see it. */
const STREAM_CHECK_MS = 30_000;

/**
 * Keep checking, while a stream is open, that its caller may still see it; call `onLost` once
 * when they no longer may — signed out, deactivated, or a permission taken away — and `onChecked`
 * with each check that finds they still may, so the stream can follow what else changed, such as
 * the datasets they may read. A check the control plane cannot answer leaves the stream open.
 * Returns a function that stops the checks.
 */
export function watchAccess(
  request: FastifyRequest,
  permission: string,
  onLost: (reason: string) => void,
  onChecked: (auth: RequestAuth) => void = () => undefined,
): () => void {
  const token = presentedToken(request).token;
  const timer = setInterval(() => {
    void (async () => {
      const auth = await resolveAuth(token);
      if (auth.error) return;
      if (can(auth.state.principal, permission)) {
        onChecked(auth);
        return;
      }
      clearInterval(timer);
      onLost(auth.state.authenticated ? 'forbidden' : (auth.state.sessionError ?? 'sign_in_required'));
    })();
  }, STREAM_CHECK_MS);
  timer.unref();
  return () => clearInterval(timer);
}

/**
 * Refuse a caller without `permission`: 401 for a guest, who could sign in, 403 for anyone else.
 * With `anywhere`, a dataset permission held on one dataset is enough.
 */
export function requirePermission(auth: RequestAuth, permission: string, options: { anywhere?: boolean } = {}): void {
  const principal = auth.state.principal;
  if (principal.password_change_required) throw passwordChangeRequired();
  if (options.anywhere ? canAnywhere(principal, permission) : can(principal, permission)) return;
  if (auth.error) throw auth.error;
  if (principal.kind !== 'user') {
    throw new ConsoleError('unauthenticated', 'Sign in to continue.', 401, { reason: 'sign_in_required', permission });
  }
  throw new ConsoleError('forbidden', 'You do not have permission to do this.', 403, { reason: 'forbidden', permission });
}

/** The resolved caller of a request; resolved as a guest if no hook ran for it. */
export function authOf(request: FastifyRequest): RequestAuth {
  if (request.auth) return request.auth;
  throw new ConsoleError('internal_error', 'the request was not authenticated', 500);
}

/** How the console calls the control plane on this request's behalf: with the caller's session. */
export function callerOf(request: FastifyRequest): UserCaller {
  const agent = request.headers['user-agent'];
  return {
    kind: 'user',
    token: request.auth?.token ?? null,
    ip: request.ip,
    userAgent: typeof agent === 'string' ? agent : null,
  };
}

/** Store a new session in the browser. */
export function setSessionCookie(request: FastifyRequest, reply: FastifyReply, token: string, state: AuthState): void {
  const ends = Date.parse(state.session?.session_expires_at ?? '');
  const maxAge = Number.isFinite(ends) ? (ends - Date.now()) / 1000 : 0;
  reply.header('set-cookie', sessionCookie(token, { maxAgeSeconds: maxAge, secure: secureCookie(request.protocol) }));
}

export function clearSessionCookie(request: FastifyRequest, reply: FastifyReply): void {
  reply.header('set-cookie', sessionCookie('', { maxAgeSeconds: 0, secure: secureCookie(request.protocol) }));
}

/** The session token a request carries, and where it came from. */
export function presentedToken(request: FastifyRequest): { token: string | null; source: 'header' | 'cookie' | null } {
  const header = request.headers.authorization;
  if (typeof header === 'string' && header.toLowerCase().startsWith('bearer ')) {
    const token = header.slice(7).trim();
    if (token) return { token, source: 'header' };
  }
  const cookie = parseCookies(request.headers.cookie).get(config.sessionCookieName);
  return cookie ? { token: cookie, source: 'cookie' } : { token: null, source: null };
}

function dueForRenewal(state: AuthState): boolean {
  const due = Date.parse(state.session?.refresh_after ?? '');
  return Number.isFinite(due) && Date.now() >= due;
}

/**
 * Renew a cookie session that is due: a new token, whose idle timeout starts again, in a new cookie.
 * Concurrent requests with the same session share one renewal. If renewal fails the request goes on
 * with the session it has, which stays valid until its own idle timeout.
 */
async function renew(request: FastifyRequest, reply: FastifyReply, auth: RequestAuth): Promise<RequestAuth> {
  const token = auth.token as string;
  const key = tokenKey(token);
  let pending = renewals.get(key);
  if (!pending) {
    pending = (async () => {
      try {
        const answer = await controlPlane.refresh({ kind: 'user', token, ip: request.ip });
        if (!answer.token) return null;
        rememberSession(answer.token, answer);
        forgetSession(token);
        return answer.token;
      } catch (cause) {
        if (cause instanceof ConsoleError && cause.statusCode === 401) {
          forgetSession(token);
          return '';
        }
        logger.debug({ err: cause }, 'session renewal failed; the current session stays in use');
        return null;
      } finally {
        setTimeout(() => renewals.delete(key), 5_000).unref();
      }
    })();
    renewals.set(key, pending);
  }
  const renewed = await pending;
  if (renewed === '') {
    clearSessionCookie(request, reply);
    return guestAuth();
  }
  if (!renewed) return auth;
  const next = await resolveAuth(renewed);
  if (next.token) setSessionCookie(request, reply, renewed, next.state);
  return next;
}

/**
 * Whether a change comes from another site. Browsers say so in `Sec-Fetch-Site`, which no script
 * can forge; without it, the `Origin` header must name this console's own host. A request with
 * neither header does not come from a browser, so it carries no cookie it did not mean to.
 */
export function crossSite(request: FastifyRequest): boolean {
  const site = request.headers['sec-fetch-site'];
  const origin = request.headers.origin;
  if (typeof origin === 'string' && config.corsOrigin && origin === config.corsOrigin) return false;
  if (typeof site === 'string') return site !== 'same-origin' && site !== 'none';
  if (typeof origin !== 'string' || !origin) return false;
  try {
    return new URL(origin).host !== request.headers.host;
  } catch {
    return true;
  }
}

function passwordChangeRequired(): ConsoleError {
  return new ConsoleError('forbidden', 'Choose a new password before you continue.', 403, {
    reason: 'password_change_required',
  });
}
