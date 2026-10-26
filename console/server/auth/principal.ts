/**
 * Who is making a request to the console, as each domain's control plane says.
 *
 * Every domain is the authority on its own sessions and permissions (handbook 20): a person has a
 * session in each domain they signed in to, and is a guest in the others. The console asks the
 * domain and keeps the answer for `CONSOLE_SESSION_CHECK_MS` (5 s by default), keyed by the domain
 * and a hash of the token, so a busy page costs each control plane one check per session per
 * interval. Deactivating someone or ending their sessions therefore reaches the console within that
 * interval, and the control plane itself refuses their very next request to it.
 */

import { createHash } from 'node:crypto';

import { config } from '../config.ts';
import type { Domain } from '../domains/domain.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import { can as holds, canAnywhere as holdsAnywhere } from '../../shared/access.ts';
import type { AuthState, CpAuthSession, CpPrincipal } from '../../shared/types.ts';

/** What the console knows about a request's caller. */
export interface RequestAuth {
  state: AuthState;
  /** The session token, when the request carried a usable one. */
  token: string | null;
  /** The token the request carried could not be used: the console clears its cookie. */
  invalidToken: boolean;
  /** Set when the control plane could not be asked; permission checks then fail with it. */
  error: ConsoleError | null;
}

interface CachedAuth {
  auth: AuthState;
  checkedAt: number;
}

const CACHE_LIMIT = 10_000;
/** How long past its check interval a session is still trusted while the control plane is unreachable. */
const OUTAGE_GRACE_MS = 60_000;

/** Checked sessions, keyed `<domain id>:<token hash>`. */
const sessions = new Map<string, CachedAuth>();
/** What a guest may do, per domain. */
const guests = new Map<string, CachedAuth>();

function sessionKey(domain: Domain, token: string): string {
  return `${domain.id}:${tokenKey(token)}`;
}

export function tokenKey(token: string): string {
  return createHash('sha256').update(token).digest('base64url');
}

/** A principal with no permissions: who a request is while nothing better is known. */
export function nobody(): CpPrincipal {
  return {
    kind: 'guest',
    user_id: null,
    email: null,
    first_name: null,
    last_name: null,
    display_name: 'Guest',
    group_ids: [],
    role_ids: [],
    permissions: [],
    all_permissions: false,
    dataset_permissions: {},
    dataset_limit: null,
    password_change_required: false,
  };
}

export function toAuthState(session: CpAuthSession): AuthState {
  return {
    authenticated: session.authenticated,
    principal: session.principal,
    session: session.session,
    providers: session.providers ?? [],
    passwordPolicy: session.password_policy ?? null,
    sessionError: session.session_error ?? null,
  };
}

/**
 * Whether a principal holds a permission: on `datasetId` when one is named and the permission is
 * held on datasets, across the deployment otherwise (`shared/access.ts`).
 */
export function can(principal: CpPrincipal, permission: string, datasetId?: string | null): boolean {
  return holds({ ...principal, password_change_required: false }, permission, datasetId);
}

/** Whether a principal holds a permission across the deployment, or on at least one dataset. */
export function canAnywhere(principal: CpPrincipal, permission: string): boolean {
  return holdsAnywhere({ ...principal, password_change_required: false }, permission);
}

/** Resolve a request's caller in one domain from its token there, or as a guest when it has none. */
export async function resolveAuth(domain: Domain, token: string | null): Promise<RequestAuth> {
  if (!token) return guestAuth(domain);
  const key = sessionKey(domain, token);
  const cached = sessions.get(key);
  const now = Date.now();
  if (cached && now - cached.checkedAt < config.sessionCheckMs) {
    return { state: cached.auth, token, invalidToken: false, error: null };
  }
  try {
    const answer = toAuthState(await domain.controlPlane.session({ kind: 'user', token }));
    if (!answer.authenticated) {
      sessions.delete(key);
      return { state: answer, token: null, invalidToken: true, error: null };
    }
    remember(key, answer, now);
    return { state: answer, token, invalidToken: false, error: null };
  } catch (cause) {
    const error = asConsoleError(cause);
    if (cached && now - cached.checkedAt < config.sessionCheckMs + OUTAGE_GRACE_MS) {
      return { state: cached.auth, token, invalidToken: false, error: null };
    }
    logger.debug({ err: cause, domain: domain.id }, 'session could not be checked with the control plane');
    return { state: offline(), token, invalidToken: false, error };
  }
}

/** A guest's permissions in one domain, as its Guest group grants them now. */
export async function guestAuth(domain: Domain): Promise<RequestAuth> {
  const now = Date.now();
  const guest = guests.get(domain.id);
  if (guest && now - guest.checkedAt < config.sessionCheckMs) {
    return { state: guest.auth, token: null, invalidToken: false, error: null };
  }
  try {
    const answer = toAuthState(await domain.controlPlane.session({ kind: 'user', token: null }));
    guests.set(domain.id, { auth: answer, checkedAt: now });
    return { state: answer, token: null, invalidToken: false, error: null };
  } catch (cause) {
    if (guest && now - guest.checkedAt < config.sessionCheckMs + OUTAGE_GRACE_MS) {
      return { state: guest.auth, token: null, invalidToken: false, error: null };
    }
    return { state: offline(), token: null, invalidToken: false, error: asConsoleError(cause) };
  }
}

/** Keep a session a domain's control plane just vouched for, as after a sign-in or a renewal. */
export function rememberSession(domain: Domain, token: string, session: CpAuthSession): AuthState {
  const state = toAuthState(session);
  remember(sessionKey(domain, token), state, Date.now());
  return state;
}

/** Forget one session, as when it is signed out. */
export function forgetSession(domain: Domain, token: string): void {
  sessions.delete(sessionKey(domain, token));
}

/**
 * Forget every session of a domain and its guest's permissions, so the next request of each is
 * checked again. Called after a change to users, groups or roles made through this console, which
 * may change what anyone may do in that domain - and in no other.
 */
export function forgetAllSessions(domain: Domain): void {
  const prefix = `${domain.id}:`;
  for (const key of [...sessions.keys()]) {
    if (key.startsWith(prefix)) sessions.delete(key);
  }
  guests.delete(domain.id);
}

function remember(key: string, auth: AuthState, checkedAt: number): void {
  sessions.delete(key);
  sessions.set(key, { auth, checkedAt });
  while (sessions.size > CACHE_LIMIT) {
    const oldest = sessions.keys().next().value;
    if (oldest === undefined) break;
    sessions.delete(oldest);
  }
}

function offline(): AuthState {
  return { authenticated: false, principal: nobody(), session: null, providers: [], passwordPolicy: null, sessionError: null };
}

function asConsoleError(cause: unknown): ConsoleError {
  if (cause instanceof ConsoleError) return cause;
  return new ConsoleError('control_plane_unreachable', 'the control plane could not check this session', 503, {
    reason: cause instanceof Error ? cause.message : String(cause),
  });
}
