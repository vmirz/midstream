/**
 * Authentication and authorization for the console's own API, in every domain it serves.
 *
 * Every `/api/*` request is resolved to a caller before its route runs (handbook 20). A request about
 * one domain - every route under `/api/d/<domain>/` - is resolved in that domain: its session cookie
 * (or an `Authorization: Bearer` header, for scripts), else a guest there. The console's own routes -
 * its config, the merged fleet, search, runs - are resolved in every domain at once. Routes declare
 * the permission they need with `permit(...)`, checked in their domain. A cookie session is renewed as
 * it is used, so it ends only after the idle timeout or the maximum session length. Changes from
 * another site are refused.
 */

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import type { UserCaller } from '../controlPlane/client.ts';
import { config } from '../config.ts';
import { domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import type { AuthState } from '../../shared/types.ts';
import { appendCookie, parseCookies, secureCookie, sessionCookie, sessionCookieName } from './cookies.ts';
import { can, canAnywhere, forgetSession, guestAuth, rememberSession, resolveAuth, tokenKey, type RequestAuth } from './principal.ts';

declare module 'fastify' {
  interface FastifyRequest {
    /** The domain a request under `/api/d/<domain>/` is about; null for the console's own routes. */
    domain: Domain | null;
    /** Who made this request, in each domain resolved for it, by domain id. */
    auths: Map<string, RequestAuth> | null;
    /** Resolve the caller in a domain the request was not resolved in (`authInResolving`). */
    resolveDomain: ((domain: Domain) => Promise<RequestAuth>) | null;
  }
}

/** A request about one domain: `/api/d/<id>/…`. */
const DOMAIN_PATH = /^\/api\/d\/([^/?#]+)(?=[/?#]|$)/;

const UNAUTHENTICATED = ['/api/health'];
const MUTATIONS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

const renewals = new Map<string, Promise<string | null>>();

export async function registerAuth(app: FastifyInstance): Promise<void> {
  app.decorateRequest('domain', null);
  app.decorateRequest('auths', null);
  app.decorateRequest('resolveDomain', null);

  app.addHook('onRequest', async (request, reply) => {
    if (!request.url.startsWith('/api/')) return;
    request.resolveDomain = (domain) => resolveIn(request, reply, domain);
    if (MUTATIONS.has(request.method) && crossSite(request)) {
      throw new ConsoleError('forbidden', 'Changes can only be made from the console itself.', 403, {
        reason: 'cross_site_request',
      });
    }
    if (UNAUTHENTICATED.some((path) => request.url === path || request.url.startsWith(`${path}?`))) return;

    const match = DOMAIN_PATH.exec(request.url);
    let targets: Domain[];
    if (match) {
      const domain = domains().require(decodeURIComponent(match[1] as string));
      await domain.ready();
      if (!domain.served) {
        throw new ConsoleError('domain_misconfigured', domain.statusMessage ?? `Domain ${domain.id} is misconfigured.`, 503, {
          reason: 'domain_misconfigured',
          domain: domain.id,
        });
      }
      request.domain = domain;
      targets = [domain];
    } else {
      targets = domains().served();
    }
    const resolved = await Promise.all(targets.map(async (domain) => [domain.id, await resolveIn(request, reply, domain)] as const));
    request.auths = new Map(resolved);
  });
}

/** Who the caller is in one domain: their session there, renewed when due, else a guest. */
async function resolveIn(request: FastifyRequest, reply: FastifyReply, domain: Domain): Promise<RequestAuth> {
  const presented = presentedToken(request, domain);
  let auth = await resolveAuth(domain, presented.token);
  if (presented.source === 'cookie') {
    if (auth.invalidToken) {
      clearSessionCookie(request, reply, domain);
    } else if (auth.token && dueForRenewal(auth.state)) {
      auth = await renew(request, reply, domain, auth);
    }
  }
  return auth;
}

/**
 * A console route's guard for a permission in the settings home: the console's own settings are
 * stored there, so who may see or change them is that domain's to say (handbook 20).
 */
export function permitInHome(permission: string) {
  return async (request: FastifyRequest): Promise<void> => {
    requirePermission(authIn(request, domains().home), permission);
  };
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

/**
 * A console route's guard for a permission held in some domain, on some dataset if it is a dataset
 * permission: the route itself then checks it where its request applies - a run in its domain, on
 * each dataset it reads.
 */
export function permitInSomeDomain(permission: string) {
  return async (request: FastifyRequest): Promise<void> => {
    const auths = [...(request.auths?.values() ?? [])];
    if (auths.some((auth) => !auth.state.principal.password_change_required && canAnywhere(auth.state.principal, permission))) return;
    const unavailable = auths.find((auth) => auth.error);
    if (unavailable?.error && auths.every((auth) => auth.error || auth.state.principal.kind !== 'user')) throw unavailable.error;
    if (auths.every((auth) => auth.state.principal.kind !== 'user')) {
      throw new ConsoleError('unauthenticated', 'Sign in to continue.', 401, { reason: 'sign_in_required', permission });
    }
    throw new ConsoleError('forbidden', 'You do not have permission to do this.', 403, { reason: 'forbidden', permission });
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
  domain: Domain = domainOf(request),
): () => void {
  const token = presentedToken(request, domain).token;
  const timer = setInterval(() => {
    void (async () => {
      const auth = await resolveAuth(domain, token);
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

/** The domain a request under `/api/d/<domain>/` is about. */
export function domainOf(request: FastifyRequest): Domain {
  if (request.domain) return request.domain;
  throw new ConsoleError('internal_error', 'the request names no domain', 500);
}

/** The resolved caller of a request about one domain, in that domain. */
export function authOf(request: FastifyRequest): RequestAuth {
  return authIn(request, domainOf(request));
}

/** The resolved caller of a request in any domain it was resolved in. */
export function authIn(request: FastifyRequest, domain: Domain): RequestAuth {
  const auth = request.auths?.get(domain.id);
  if (auth) return auth;
  throw new ConsoleError('internal_error', `the request was not authenticated in ${domain.id}`, 500);
}

/**
 * The caller in `domain`, resolved now if the request was resolved only in its own: a statement
 * started in one domain may name tables of another, each read as that domain knows the person.
 */
export async function authInResolving(request: FastifyRequest, domain: Domain): Promise<RequestAuth> {
  const known = request.auths?.get(domain.id);
  if (known) return known;
  if (!request.resolveDomain) throw new ConsoleError('internal_error', `the request was not authenticated in ${domain.id}`, 500);
  await domain.ready();
  const auth = await request.resolveDomain(domain);
  request.auths = new Map([...(request.auths ?? new Map<string, RequestAuth>()), [domain.id, auth]]);
  return auth;
}

/** How the console calls a domain's control plane on this request's behalf: with the caller's session there. */
export function callerOf(request: FastifyRequest, domain: Domain = domainOf(request)): UserCaller {
  const agent = request.headers['user-agent'];
  return {
    kind: 'user',
    token: request.auths?.get(domain.id)?.token ?? null,
    ip: request.ip,
    userAgent: typeof agent === 'string' ? agent : null,
  };
}

/** Store a domain's new session in the browser. */
export function setSessionCookie(request: FastifyRequest, reply: FastifyReply, domain: Domain, token: string, state: AuthState): void {
  const ends = Date.parse(state.session?.session_expires_at ?? '');
  const maxAge = Number.isFinite(ends) ? (ends - Date.now()) / 1000 : 0;
  appendCookie(reply, sessionCookie(domain.id, token, { maxAgeSeconds: maxAge, secure: secureCookie(request.protocol) }));
}

export function clearSessionCookie(request: FastifyRequest, reply: FastifyReply, domain: Domain): void {
  appendCookie(reply, sessionCookie(domain.id, '', { maxAgeSeconds: 0, secure: secureCookie(request.protocol) }));
}

/**
 * The token a request carries for one domain, and where it came from: an `Authorization: Bearer`
 * header, for scripts - an access token works in every domain, a session in its own - else the
 * domain's own session cookie.
 */
export function presentedToken(request: FastifyRequest, domain: Domain): { token: string | null; source: 'header' | 'cookie' | null } {
  const header = request.headers.authorization;
  if (typeof header === 'string' && header.toLowerCase().startsWith('bearer ')) {
    const token = header.slice(7).trim();
    if (token) return { token, source: 'header' };
  }
  const cookie = parseCookies(request.headers.cookie).get(sessionCookieName(domain.id));
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
async function renew(request: FastifyRequest, reply: FastifyReply, domain: Domain, auth: RequestAuth): Promise<RequestAuth> {
  const token = auth.token as string;
  const key = `${domain.id}:${tokenKey(token)}`;
  let pending = renewals.get(key);
  if (!pending) {
    pending = (async () => {
      try {
        const answer = await domain.controlPlane.refresh({ kind: 'user', token, ip: request.ip });
        if (!answer.token) return null;
        rememberSession(domain, answer.token, answer);
        forgetSession(domain, token);
        return answer.token;
      } catch (cause) {
        if (cause instanceof ConsoleError && cause.statusCode === 401) {
          forgetSession(domain, token);
          return '';
        }
        logger.debug({ err: cause, domain: domain.id }, 'session renewal failed; the current session stays in use');
        return null;
      } finally {
        setTimeout(() => renewals.delete(key), 5_000).unref();
      }
    })();
    renewals.set(key, pending);
  }
  const renewed = await pending;
  if (renewed === '') {
    clearSessionCookie(request, reply, domain);
    return guestAuth(domain);
  }
  if (!renewed) return auth;
  const next = await resolveAuth(domain, renewed);
  if (next.token) setSessionCookie(request, reply, domain, renewed, next.state);
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
