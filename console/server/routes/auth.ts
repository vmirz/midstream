import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { config } from '../config.ts';
import { domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import { authIn, authOf, callerOf, clearSessionCookie, domainOf, permitAnywhere, presentedToken, setSessionCookie, signedIn } from '../auth/guard.ts';
import { waitForOutcome, refusal } from '../auth/outcomes.ts';
import { forgetAllSessions, forgetSession, guestAuth, rememberSession } from '../auth/principal.ts';
import { AttemptLimiter } from '../auth/rateLimit.ts';
import { handOn } from '../auth/revocations.ts';
import { ssoProviders } from '../auth/sso.ts';
import { parseBody } from './materializations.ts';
import { TOKEN_PERMISSIONS, type AccessTokenRevoked, type AuthState, type ConsoleSession, type CpAccessTokenList, type CpAuthSession, type CpMySessions, type DomainAuth } from '../../shared/types.ts';

const signInSchema = z.object({
  email: z.string().trim().min(1).max(254),
  password: z.string().min(1).max(256),
});

const signOutSchema = z
  .object({
    scope: z.enum(['current', 'others', 'session']).optional(),
    session_id: z.string().min(1).max(128).optional(),
  })
  .optional();

const passwordSchema = z.object({
  current_password: z.string().min(1).max(256),
  new_password: z.string().min(1).max(256),
});

const createTokenSchema = z.strictObject({
  name: z.string().trim().min(1).max(64),
  permissions: z.array(z.enum(TOKEN_PERMISSIONS)).min(1).max(TOKEN_PERMISSIONS.length),
  expires_in_days: z.number().int().min(1).max(365),
  /**
   * The datasets the token is limited to, as `<domain>.<dataset>` (a name without its domain is the
   * minting domain's); omitted, every dataset its owner may read, in every domain.
   */
  datasets: z
    .array(z.string().trim().min(1).max(97).regex(/^([a-z][a-z0-9_]{0,31}\.)?[a-z][a-z0-9_]{0,62}$/, 'must be <domain>.<dataset>'))
    .max(100)
    .optional(),
});

const signIns = new AttemptLimiter();

/**
 * Who the browser is in every domain, and signing out of all of them (handbook 20).
 *
 * Each domain has its own session, in a cookie of its own; a domain the browser has no session in sees
 * a guest. The session tokens never reach the browser's scripts: these routes answer with who is
 * signed in, never with a token.
 */
export async function registerConsoleAuthRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/auth/session', async (request): Promise<ConsoleSession> => {
    const sessions: Record<string, DomainAuth> = {};
    for (const domain of domains().served()) {
      const auth = authIn(request, domain);
      // A session that cannot be checked right now is neither confirmed nor ended.
      sessions[domain.id] = auth.error && auth.token ? { state: null, error: auth.error.message } : { state: auth.state, error: null };
    }
    return { domains: sessions, providers: ssoProviders() };
  });

  /** Sign out of every domain this browser has a session in. */
  app.post('/api/auth/logout', async (request, reply): Promise<{ signed_out: boolean; domains: string[] }> => {
    const ended: string[] = [];
    for (const domain of domains().served()) {
      const presented = presentedToken(request, domain);
      if (presented.source !== 'cookie' || !presented.token) continue;
      try {
        await domain.controlPlane.logout(callerOf(request, domain), { scope: 'current' });
      } catch (cause) {
        // The cookie is cleared regardless: this browser is signed out even if the record lags.
        logger.warn({ err: cause, domain: domain.id }, 'sign-out could not be recorded with the control plane');
      }
      forgetSession(domain, presented.token);
      clearSessionCookie(request, reply, domain);
      ended.push(domain.id);
    }
    return { signed_out: true, domains: ended };
  });
}

/**
 * Signing in and out of one domain with a password, the session in force there, your own sessions
 * and access tokens there, and changing your password there: under `/api/d/<domain>/`.
 */
export async function registerAuthRoutes(app: FastifyInstance): Promise<void> {
  app.get('/auth/session', async (request): Promise<AuthState> => {
    const auth = authOf(request);
    if (auth.error && auth.token) throw auth.error;
    return auth.state;
  });

  app.post('/auth/login', async (request, reply): Promise<AuthState> => {
    const domain = domainOf(request);
    const body = parseBody(signInSchema, request.body);
    if (!signIns.allow(request.ip, config.signInAttemptsPerMinute)) {
      reply.header('retry-after', String(signIns.retryAfter(request.ip)));
      throw new ConsoleError('too_many_attempts', 'Too many sign-in attempts. Wait a minute and try again.', 429, {
        reason: 'rate_limited',
      });
    }
    const previous = authOf(request).token;
    const answer = await domain.controlPlane.login({ ...callerOf(request), token: null }, body);
    if (answer.status !== 200 || !answer.body?.token) throw signInError(answer.status, answer.body);
    const state = rememberSession(domain, answer.body.token, answer.body);
    setSessionCookie(request, reply, domain, answer.body.token, state);
    if (previous) void endSession(domain, previous, request.ip);
    return state;
  });

  app.post('/auth/logout', async (request, reply): Promise<{ signed_out: boolean; scope: string }> => {
    const domain = domainOf(request);
    const body = parseBody(signOutSchema, request.body ?? undefined) ?? {};
    const scope = body.scope ?? 'current';
    if (scope === 'session' && !body.session_id) {
      throw new ConsoleError('invalid_request', 'session_id: say which session to sign out', 400, { field: 'session_id' });
    }
    const token = authOf(request).token;
    if (token) {
      try {
        await domain.controlPlane.logout(callerOf(request), { scope, ...(body.session_id ? { session_id: body.session_id } : {}) });
      } catch (cause) {
        if (scope !== 'current') throw cause;
        // The cookie is cleared regardless: this browser is signed out even if the record lags.
        logger.warn({ err: cause, domain: domain.id }, 'sign-out could not be recorded with the control plane');
      }
      if (scope === 'current') forgetSession(domain, token);
    }
    if (scope === 'current') clearSessionCookie(request, reply, domain);
    return { signed_out: true, scope };
  });

  app.get(
    '/auth/sessions',
    { preHandler: signedIn({ allowPasswordChange: true }) },
    async (request): Promise<CpMySessions> => domainOf(request).controlPlane.mySessions(callerOf(request)),
  );

  /** Your access tokens, for external engines reading the Iceberg catalog. Never their secrets. */
  app.get('/auth/tokens', { preHandler: signedIn() }, async (request): Promise<CpAccessTokenList> =>
    domainOf(request).controlPlane.accessTokens(callerOf(request)),
  );

  /**
   * Make an access token in this domain, optionally limited to some datasets of any domain (handbook
   * 20). It is good in every domain where its owner has an account. The answer carries its text,
   * shown this once and never stored by the console - and only once the domain has recorded the
   * token: one it refuses is revoked, and its text is never shown.
   */
  app.post('/auth/tokens', { preHandler: permitAnywhere('catalog.read') }, async (request, reply) => {
    const domain = domainOf(request);
    const caller = callerOf(request);
    const input = parseBody(createTokenSchema, request.body);
    const created = await domain.controlPlane.createAccessToken(caller, input);
    const outcome = await waitForOutcome(domain, caller, created.request_id);
    if (outcome.status === 'rejected') throw refusal(outcome);
    return reply.code(outcome.status === 'applied' ? 201 : 202).header('cache-control', 'no-store').send(created);
  });

  /**
   * Revoke a token made in this domain, and hand the revocation to every other domain, which then
   * refuses the token too; a domain that cannot be reached now is told again within five minutes.
   */
  app.delete('/auth/tokens/:tokenId', { preHandler: signedIn() }, async (request, reply): Promise<AccessTokenRevoked> => {
    const domain = domainOf(request);
    const caller = callerOf(request);
    const tokenId = (request.params as { tokenId?: string }).tokenId ?? '';
    const accepted = await domain.controlPlane.revokeAccessToken(caller, tokenId);
    const outcome = await waitForOutcome(domain, caller, accepted.request_id);
    if (outcome.status === 'rejected') throw refusal(outcome);
    const others = accepted.revocation ? await handOn([accepted.revocation], domain, domains().served()) : {};
    reply.code(outcome.status === 'applied' ? 200 : 202);
    return { request_id: accepted.request_id, status: outcome.status === 'applied' ? 'applied' : 'pending', domains: others };
  });

  /**
   * Change your own password in this domain. Every session you have there ends once it applies - this
   * one included - so the console signs you straight back in with the new password and answers with
   * that session.
   */
  app.post('/auth/password', { preHandler: signedIn({ allowPasswordChange: true }) }, async (request, reply) => {
    const domain = domainOf(request);
    const body = parseBody(passwordSchema, request.body);
    const auth = authOf(request);
    const email = auth.state.principal.email ?? '';
    const caller = callerOf(request);
    const accepted = await domain.controlPlane.changePassword(caller, body);
    const outcome = await waitForOutcome(domain, caller, accepted.request_id);
    if (outcome.status === 'rejected' && outcome.reason !== 'session_ended') throw refusal(outcome);
    if (auth.token) forgetSession(domain, auth.token);
    forgetAllSessions(domain);

    const answer = await domain.controlPlane.login({ ...caller, token: null }, { email, password: body.new_password });
    if (answer.status === 200 && answer.body?.token) {
      const state = rememberSession(domain, answer.body.token, answer.body);
      setSessionCookie(request, reply, domain, answer.body.token, state);
      return reply.code(200).send({ status: 'applied', session: state });
    }
    if (outcome.status === 'pending') {
      // Accepted but not applied yet: the old password still works until it is.
      return reply.code(202).send({ status: 'pending', session: auth.state });
    }
    clearSessionCookie(request, reply, domain);
    return reply.code(200).send({ status: 'applied', session: (await guestAuth(domain)).state });
  });
}

/** End a session this browser is replacing, without making the new sign-in wait for it. */
async function endSession(domain: Domain, token: string, ip: string): Promise<void> {
  forgetSession(domain, token);
  try {
    await domain.controlPlane.logout({ kind: 'user', token, ip }, { scope: 'current' });
  } catch {
    // It ends at its own idle timeout in any case.
  }
}

/** The console's error for a refused sign-in, carrying the control plane's message and reason. */
function signInError(status: number, body: CpAuthSession | { error?: { code?: string; message?: string; details?: Record<string, unknown> } } | null): ConsoleError {
  const error = body && 'error' in body ? body.error : undefined;
  const code = error?.code ?? (status === 401 ? 'unauthenticated' : 'sign_in_failed');
  return new ConsoleError(code, error?.message ?? 'Signing in failed.', status >= 400 ? status : 502, error?.details ?? {});
}
