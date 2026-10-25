import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { config } from '../config.ts';
import { controlPlane } from '../controlPlane/client.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import { authOf, callerOf, clearSessionCookie, permitAnywhere, setSessionCookie, signedIn } from '../auth/guard.ts';
import { waitForOutcome, refusal } from '../auth/outcomes.ts';
import { forgetAllSessions, forgetSession, guestAuth, rememberSession } from '../auth/principal.ts';
import { AttemptLimiter } from '../auth/rateLimit.ts';
import { parseBody } from './materializations.ts';
import { TOKEN_PERMISSIONS, type AuthState, type CpAccessTokenList, type CpAuthSession, type CpMySessions } from '../../shared/types.ts';

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
  /** The datasets, by name, the token is limited to; omitted, every dataset its owner may read. */
  datasets: z.array(z.string().trim().min(1).max(63)).max(100).optional(),
});

const signIns = new AttemptLimiter();

/**
 * Signing in and out, the session in force, your own sessions and access tokens, and changing your
 * own password.
 *
 * The session token never reaches the browser's scripts: it travels in an HttpOnly cookie, and these
 * routes answer with who is signed in, never with the token.
 */
export async function registerAuthRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/auth/session', async (request): Promise<AuthState> => {
    const auth = authOf(request);
    // A session that cannot be checked right now is neither confirmed nor ended.
    if (auth.error && auth.token) throw auth.error;
    return auth.state;
  });

  app.post('/api/auth/login', async (request, reply): Promise<AuthState> => {
    const body = parseBody(signInSchema, request.body);
    if (!signIns.allow(request.ip, config.signInAttemptsPerMinute)) {
      reply.header('retry-after', String(signIns.retryAfter(request.ip)));
      throw new ConsoleError('too_many_attempts', 'Too many sign-in attempts. Wait a minute and try again.', 429, {
        reason: 'rate_limited',
      });
    }
    const previous = authOf(request).token;
    const answer = await controlPlane.login({ ...callerOf(request), token: null }, body);
    if (answer.status !== 200 || !answer.body?.token) throw signInError(answer.status, answer.body);
    const state = rememberSession(answer.body.token, answer.body);
    setSessionCookie(request, reply, answer.body.token, state);
    if (previous) void endSession(previous, request.ip);
    return state;
  });

  app.post('/api/auth/logout', async (request, reply): Promise<{ signed_out: boolean; scope: string }> => {
    const body = parseBody(signOutSchema, request.body ?? undefined) ?? {};
    const scope = body.scope ?? 'current';
    if (scope === 'session' && !body.session_id) {
      throw new ConsoleError('invalid_request', 'session_id: say which session to sign out', 400, { field: 'session_id' });
    }
    const token = authOf(request).token;
    if (token) {
      try {
        await controlPlane.logout(callerOf(request), { scope, ...(body.session_id ? { session_id: body.session_id } : {}) });
      } catch (cause) {
        if (scope !== 'current') throw cause;
        // The cookie is cleared regardless: this browser is signed out even if the record lags.
        logger.warn({ err: cause }, 'sign-out could not be recorded with the control plane');
      }
      if (scope === 'current') forgetSession(token);
    }
    if (scope === 'current') clearSessionCookie(request, reply);
    return { signed_out: true, scope };
  });

  app.get(
    '/api/auth/sessions',
    { preHandler: signedIn({ allowPasswordChange: true }) },
    async (request): Promise<CpMySessions> => controlPlane.mySessions(callerOf(request)),
  );

  /** Your access tokens, for external engines reading the Iceberg catalog. Never their secrets. */
  app.get('/api/auth/tokens', { preHandler: signedIn() }, async (request): Promise<CpAccessTokenList> =>
    controlPlane.accessTokens(callerOf(request)),
  );

  /**
   * Make an access token, optionally limited to some datasets. The answer carries its text, shown
   * this once and never stored by the console; the control plane keeps only its hash.
   */
  app.post('/api/auth/tokens', { preHandler: permitAnywhere('catalog.read') }, async (request, reply) => {
    const input = parseBody(createTokenSchema, request.body);
    const created = await controlPlane.createAccessToken(callerOf(request), input);
    return reply.code(202).header('cache-control', 'no-store').send(created);
  });

  app.delete('/api/auth/tokens/:tokenId', { preHandler: signedIn() }, async (request, reply) => {
    const tokenId = (request.params as { tokenId?: string }).tokenId ?? '';
    return reply.code(202).send(await controlPlane.revokeAccessToken(callerOf(request), tokenId));
  });

  /**
   * Change your own password. Every session you have ends once it applies — this one included — so
   * the console signs you straight back in with the new password and answers with that session.
   */
  app.post('/api/auth/password', { preHandler: signedIn({ allowPasswordChange: true }) }, async (request, reply) => {
    const body = parseBody(passwordSchema, request.body);
    const auth = authOf(request);
    const email = auth.state.principal.email ?? '';
    const caller = callerOf(request);
    const accepted = await controlPlane.changePassword(caller, body);
    const outcome = await waitForOutcome(caller, accepted.request_id);
    if (outcome.status === 'rejected' && outcome.reason !== 'session_ended') throw refusal(outcome);
    if (auth.token) forgetSession(auth.token);
    forgetAllSessions();

    const answer = await controlPlane.login({ ...caller, token: null }, { email, password: body.new_password });
    if (answer.status === 200 && answer.body?.token) {
      const state = rememberSession(answer.body.token, answer.body);
      setSessionCookie(request, reply, answer.body.token, state);
      return reply.code(200).send({ status: 'applied', session: state });
    }
    if (outcome.status === 'pending') {
      // Accepted but not applied yet: the old password still works until it is.
      return reply.code(202).send({ status: 'pending', session: auth.state });
    }
    clearSessionCookie(request, reply);
    return reply.code(200).send({ status: 'applied', session: (await guestAuth()).state });
  });
}

/** End a session this browser is replacing, without making the new sign-in wait for it. */
async function endSession(token: string, ip: string): Promise<void> {
  forgetSession(token);
  try {
    await controlPlane.logout({ kind: 'user', token, ip }, { scope: 'current' });
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
