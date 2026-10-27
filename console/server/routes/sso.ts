import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import { config } from '../config.ts';
import { consoleBaseUrl } from '../consoleUrl.ts';
import { domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import { ConsoleError } from '../errors.ts';
import { logger } from '../logger.ts';
import { appendCookie, cookie, flowCookieName, parseCookies, secureCookie } from '../auth/cookies.ts';
import { presentedToken, setSessionCookie } from '../auth/guard.ts';
import { forgetSession, rememberSession } from '../auth/principal.ts';
import { AttemptLimiter } from '../auth/rateLimit.ts';
import {
  FLOW_TTL_MS,
  authorizeUrl,
  exchangeCode,
  keepOutcome,
  outcomeOf,
  providerClient,
  revokeGithubToken,
  startFlow,
  takeFlow,
  type PendingFlow,
  type ProviderClient,
  type ProviderCredential,
} from '../auth/sso.ts';
import type { CpAuthSession, SsoDomainOutcome, SsoOutcome } from '../../shared/types.ts';

const starts = new AttemptLimiter();

/**
 * Signing in through a provider (handbook 20), one sign-in for every domain: `start` sends the
 * browser to the provider, `callback` takes it back and asks every domain to start a session, and
 * `outcomes/:id` says what each domain answered.
 */
export async function registerSsoRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/auth/sso/:provider/start', async (request, reply) => {
    const provider = providerClient((request.params as { provider?: string }).provider ?? '');
    if (!provider) throw new ConsoleError('not_found', 'This console does not sign in with that provider.', 404);
    if (!starts.allow(request.ip, config.signInAttemptsPerMinute)) {
      reply.header('retry-after', String(starts.retryAfter(request.ip)));
      throw new ConsoleError('too_many_attempts', 'Too many sign-in attempts. Wait a minute and try again.', 429, { reason: 'rate_limited' });
    }
    const returnTo = (request.query as { return?: unknown }).return;
    const redirectUri = `${consoleBaseUrl(request)}/api/auth/sso/${provider.id}/callback`;
    let endpoint: string;
    try {
      endpoint = (await provider.endpoints()).authorize;
    } catch (cause) {
      logger.warn({ err: cause, provider: provider.id }, 'a sign-in provider could not be reached');
      return toSignIn(reply, failed(provider, `${provider.label} cannot be reached right now. Try again in a moment.`));
    }
    const { flow, cookie: binding } = startFlow(provider.id, typeof returnTo === 'string' ? returnTo : '/', redirectUri);
    appendCookie(reply, flowCookie(request, binding, FLOW_TTL_MS / 1000));
    return reply.redirect(authorizeUrl(provider, endpoint, flow), 302);
  });

  app.get('/api/auth/sso/:provider/callback', async (request, reply) => {
    const name = (request.params as { provider?: string }).provider ?? '';
    const query = (request.query ?? {}) as { code?: unknown; state?: unknown; error?: unknown; error_description?: unknown };
    const provider = providerClient(name);
    const flow = provider ? takeFlow(provider.id, query.state, parseCookies(request.headers.cookie).get(flowCookieName())) : null;
    appendCookie(reply, flowCookie(request, '', 0));
    if (!provider || !flow) {
      return toSignIn(reply, failed(provider, 'This sign-in expired, or was started in another browser. Sign in again.', name));
    }
    if (typeof query.error === 'string') {
      const said = typeof query.error_description === 'string' ? query.error_description : query.error;
      return toSignIn(reply, failed(provider, `${provider.label} did not sign you in: ${said.slice(0, 300)}`));
    }
    if (typeof query.code !== 'string' || !query.code) {
      return toSignIn(reply, failed(provider, `${provider.label} sent no sign-in back. Sign in again.`));
    }

    let credential: ProviderCredential;
    try {
      credential = await exchangeCode(provider, query.code, flow);
    } catch (cause) {
      logger.warn({ err: cause, provider: provider.id }, 'a sign-in code could not be exchanged');
      return toSignIn(reply, failed(provider, cause instanceof Error ? cause.message : String(cause)));
    }

    const results = await Promise.all(domains().served().map((domain) => signInTo(domain, request, provider, credential, flow)));
    for (const result of results) {
      if (!result.session) continue;
      const { domain, token, body } = result.session;
      const previous = presentedToken(request, domain);
      const state = rememberSession(domain, token, body);
      setSessionCookie(request, reply, domain, token, state);
      if (previous.source === 'cookie' && previous.token && previous.token !== token) void endSession(domain, previous.token, request);
    }
    if ('access_token' in credential) {
      // It was for this sign-in only; every domain has checked it.
      void revokeGithubToken(provider, credential.access_token).catch((cause) => logger.warn({ err: cause }, 'a GitHub sign-in token could not be revoked'));
    }

    const outcome: SsoOutcome = { provider: provider.id, label: provider.label, error: null, domains: results.map((result) => result.outcome) };
    const id = keepOutcome(outcome);
    const signedIn = outcome.domains.some((entry) => entry.status === 'signed_in');
    return reply.redirect(withOutcome(signedIn ? flow.returnTo : '/sign-in', id), 302);
  });

  /** What a sign-in through a provider did, in every domain; kept a few minutes. */
  app.get('/api/auth/sso/outcomes/:id', async (request): Promise<SsoOutcome> => {
    const outcome = outcomeOf((request.params as { id?: string }).id ?? '');
    if (!outcome) throw new ConsoleError('not_found', 'That sign-in is no longer remembered.', 404);
    return outcome;
  });
}

interface DomainSignIn {
  outcome: SsoDomainOutcome;
  session: { domain: Domain; token: string; body: CpAuthSession } | null;
}

/** Ask one domain to start a session with what the provider vouched for; it checks it itself. */
async function signInTo(domain: Domain, request: FastifyRequest, provider: ProviderClient, credential: ProviderCredential, flow: PendingFlow): Promise<DomainSignIn> {
  const base = { domain: domain.id, name: domain.name };
  try {
    const answer = await domain.controlPlane.ssoLogin(
      { kind: 'user', token: null, ip: request.ip, userAgent: request.headers['user-agent'] ?? null },
      { provider: provider.id, credential, nonce: provider.kind === 'oidc' ? flow.nonce : null },
    );
    if (answer.status === 200 && answer.body?.token) {
      return { outcome: { ...base, status: 'signed_in', reason: null, message: null }, session: { domain, token: answer.body.token, body: answer.body } };
    }
    const error = (answer.body as { error?: { message?: string; details?: { reason?: unknown } } } | null)?.error;
    return {
      outcome: {
        ...base,
        status: 'refused',
        reason: typeof error?.details?.reason === 'string' ? error.details.reason : `http_${answer.status}`,
        message: error?.message ?? `It answered ${answer.status}.`,
      },
      session: null,
    };
  } catch (cause) {
    return { outcome: { ...base, status: 'unreachable', reason: 'unreachable', message: cause instanceof Error ? cause.message : String(cause) }, session: null };
  }
}

/** End a session this browser is replacing, without making the sign-in wait for it. */
async function endSession(domain: Domain, token: string, request: FastifyRequest): Promise<void> {
  forgetSession(domain, token);
  try {
    await domain.controlPlane.logout({ kind: 'user', token, ip: request.ip }, { scope: 'current' });
  } catch {
    // It ends at its own idle timeout in any case.
  }
}

function flowCookie(request: FastifyRequest, value: string, maxAgeSeconds: number): string {
  return cookie(flowCookieName(), value, { maxAgeSeconds, secure: secureCookie(request.protocol), path: '/api/auth/sso' });
}

function failed(provider: ProviderClient | null, error: string, fallbackId = 'oidc'): SsoOutcome {
  return {
    provider: provider?.id ?? (['entra', 'github', 'google', 'oidc'].includes(fallbackId) ? (fallbackId as SsoOutcome['provider']) : 'oidc'),
    label: provider?.label ?? 'The provider',
    error,
    domains: [],
  };
}

function toSignIn(reply: FastifyReply, outcome: SsoOutcome): FastifyReply {
  return reply.redirect(withOutcome('/sign-in', keepOutcome(outcome)), 302);
}

function withOutcome(path: string, id: string): string {
  return `${path}${path.includes('?') ? '&' : '?'}signin=${encodeURIComponent(id)}`;
}
