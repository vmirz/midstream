/**
 * Single sign-on (handbook 20): the console runs each provider's flow; every domain checks the result.
 *
 * A sign-in starts at `/api/auth/sso/<provider>/start`: the console remembers a pending flow - its
 * `state`, its `nonce` and its PKCE verifier - binds it to the browser with a short-lived cookie, and
 * sends the browser to the provider. The provider sends it back to `/callback` with a code, which the
 * console exchanges for an ID token (GitHub: an access token). It then hands that credential to every
 * domain at once; each checks it against the provider itself and answers with a session or a refusal.
 * The console keeps the sessions, one cookie each, and forgets the credential. What happened in each
 * domain is kept a few minutes for the page to show.
 */

import { createHash, randomBytes } from 'node:crypto';

import { config } from '../config.ts';
import type { SsoOutcome, SsoProvider, SsoProviderId } from '../../shared/types.ts';

/** How long a flow may take, from the start to the callback. */
export const FLOW_TTL_MS = 10 * 60 * 1000;
/** How long what a sign-in did is kept for the page that shows it. */
export const OUTCOME_TTL_MS = 5 * 60 * 1000;
/** The most flows, and outcomes, kept at once; the oldest go first. */
const MAX_PENDING = 10_000;

/** One provider as this console is registered with it. */
export interface ProviderClient {
  id: SsoProviderId;
  label: string;
  clientId: string;
  clientSecret: string;
  scopes: string;
  /** OpenID Connect providers answer with an ID token; GitHub with an access token only. */
  kind: 'oidc' | 'oauth';
  /** Where the browser is sent, and where the code is exchanged. */
  endpoints: () => Promise<{ authorize: string; token: string }>;
  /** Parameters the provider's authorize request takes beyond the standard ones. */
  extraParams: Record<string, string>;
}

const LABELS: Record<SsoProviderId, string> = { entra: 'Microsoft', github: 'GitHub', google: 'Google', oidc: 'Single sign-on' };

let providersOverride: ProviderClient[] | null = null;

/** Replace the providers read from the deployment, as a test does; null returns to them. */
export function useProviders(replacement: ProviderClient[] | null): void {
  providersOverride = replacement;
}

/** The providers this console is registered with, in the order the sign-in page offers them. */
export function configuredProviders(): ProviderClient[] {
  if (providersOverride) return providersOverride;
  const providers: ProviderClient[] = [];
  if (config.ssoEntraTenantId && config.ssoEntraClientId && config.ssoEntraClientSecret) {
    const tenant = encodeURIComponent(config.ssoEntraTenantId);
    providers.push({
      id: 'entra',
      label: LABELS.entra,
      clientId: config.ssoEntraClientId,
      clientSecret: config.ssoEntraClientSecret,
      scopes: 'openid email profile',
      kind: 'oidc',
      endpoints: async () => ({
        authorize: `https://login.microsoftonline.com/${tenant}/oauth2/v2.0/authorize`,
        token: `https://login.microsoftonline.com/${tenant}/oauth2/v2.0/token`,
      }),
      extraParams: { prompt: 'select_account' },
    });
  }
  if (config.ssoGithubClientId && config.ssoGithubClientSecret) {
    providers.push({
      id: 'github',
      label: LABELS.github,
      clientId: config.ssoGithubClientId,
      clientSecret: config.ssoGithubClientSecret,
      scopes: 'read:user user:email',
      kind: 'oauth',
      endpoints: async () => ({ authorize: 'https://github.com/login/oauth/authorize', token: 'https://github.com/login/oauth/access_token' }),
      extraParams: { allow_signup: 'false' },
    });
  }
  if (config.ssoGoogleClientId && config.ssoGoogleClientSecret) {
    providers.push({
      id: 'google',
      label: LABELS.google,
      clientId: config.ssoGoogleClientId,
      clientSecret: config.ssoGoogleClientSecret,
      scopes: 'openid email profile',
      kind: 'oidc',
      endpoints: async () => ({ authorize: 'https://accounts.google.com/o/oauth2/v2/auth', token: 'https://oauth2.googleapis.com/token' }),
      extraParams: { prompt: 'select_account', ...(config.ssoGoogleHostedDomain ? { hd: config.ssoGoogleHostedDomain } : {}) },
    });
  }
  if (config.ssoOidcIssuer && config.ssoOidcClientId && config.ssoOidcClientSecret) {
    const issuer = config.ssoOidcIssuer;
    providers.push({
      id: 'oidc',
      label: config.ssoOidcLabel || LABELS.oidc,
      clientId: config.ssoOidcClientId,
      clientSecret: config.ssoOidcClientSecret,
      scopes: config.ssoOidcScopes,
      kind: 'oidc',
      endpoints: () => discover(issuer),
      extraParams: {},
    });
  }
  return providers;
}

/** The providers as the sign-in page lists them. */
export function ssoProviders(): SsoProvider[] {
  return configuredProviders().map(({ id, label }) => ({ id, label }));
}

export function providerClient(id: string): ProviderClient | null {
  return configuredProviders().find((provider) => provider.id === id) ?? null;
}

const discovered = new Map<string, { at: number; endpoints: { authorize: string; token: string } }>();

/** An issuer's endpoints, from its discovery document, kept an hour. */
async function discover(issuer: string): Promise<{ authorize: string; token: string }> {
  const cached = discovered.get(issuer);
  if (cached && Date.now() - cached.at < 60 * 60 * 1000) return cached.endpoints;
  const response = await ssoFetch(`${issuer}/.well-known/openid-configuration`, { headers: { accept: 'application/json' } });
  if (!response.ok) throw new Error(`${issuer} answered ${response.status} for its discovery document`);
  const document = (await response.json()) as { authorization_endpoint?: unknown; token_endpoint?: unknown };
  if (typeof document.authorization_endpoint !== 'string' || typeof document.token_endpoint !== 'string') {
    throw new Error(`${issuer}'s discovery document names no authorization or token endpoint`);
  }
  const endpoints = { authorize: document.authorization_endpoint, token: document.token_endpoint };
  discovered.set(issuer, { at: Date.now(), endpoints });
  return endpoints;
}

/** The fetch the flows use; a test replaces it. */
let ssoFetch: typeof fetch = (input, init) => fetch(input, { ...init, signal: init?.signal ?? AbortSignal.timeout(15_000) });

export function useSsoFetch(replacement: typeof fetch | null): void {
  ssoFetch = replacement ?? ((input, init) => fetch(input, { ...init, signal: init?.signal ?? AbortSignal.timeout(15_000) }));
  discovered.clear();
}

/* ------------------------------------------------------------------------------------- flows */

/** A sign-in on its way through a provider. */
export interface PendingFlow {
  provider: SsoProviderId;
  state: string;
  nonce: string;
  verifier: string;
  /** The SHA-256 of the cookie that binds the flow to the browser that started it. */
  binding: string;
  returnTo: string;
  redirectUri: string;
  startedAt: number;
}

const flows = new Map<string, PendingFlow>();
const outcomes = new Map<string, { at: number; outcome: SsoOutcome }>();

function token(bytes = 32): string {
  return randomBytes(bytes).toString('base64url');
}

export function sha256(text: string): string {
  return createHash('sha256').update(text).digest('base64url');
}

/** The PKCE challenge for a verifier (S256). */
export function challengeFor(verifier: string): string {
  return sha256(verifier);
}

/** Where a sign-in may send the browser back to: a path of this console, never another site. */
export function safeReturnPath(value: unknown): string {
  if (typeof value !== 'string' || !value.startsWith('/') || value.startsWith('//') || value.startsWith('/\\')) return '/';
  if (value.startsWith('/sign-in') || value.startsWith('/api/')) return '/';
  return value.length > 2048 ? '/' : value;
}

/** Start a flow: what to remember, and the cookie value that binds it to this browser. */
export function startFlow(provider: SsoProviderId, returnTo: string, redirectUri: string): { flow: PendingFlow; cookie: string } {
  prune();
  const cookie = token();
  const flow: PendingFlow = {
    provider,
    state: token(),
    nonce: token(),
    verifier: token(48),
    binding: sha256(cookie),
    returnTo: safeReturnPath(returnTo),
    redirectUri,
    startedAt: Date.now(),
  };
  flows.set(flow.state, flow);
  while (flows.size > MAX_PENDING) flows.delete(flows.keys().next().value as string);
  return { flow, cookie };
}

/** The provider's authorize URL for a flow. */
export function authorizeUrl(provider: ProviderClient, endpoint: string, flow: PendingFlow): string {
  const url = new URL(endpoint);
  const params: Record<string, string> = {
    client_id: provider.clientId,
    redirect_uri: flow.redirectUri,
    response_type: 'code',
    scope: provider.scopes,
    state: flow.state,
    code_challenge: challengeFor(flow.verifier),
    code_challenge_method: 'S256',
    ...(provider.kind === 'oidc' ? { nonce: flow.nonce } : {}),
    ...provider.extraParams,
  };
  for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
  return url.toString();
}

/**
 * The flow a callback finishes: found by its state, for the same provider, from the browser that
 * started it, not expired. Taken, so a callback works once.
 */
export function takeFlow(provider: string, state: unknown, cookie: string | undefined): PendingFlow | null {
  if (typeof state !== 'string' || !state) return null;
  const flow = flows.get(state);
  if (!flow) return null;
  flows.delete(state);
  if (flow.provider !== provider || !cookie || sha256(cookie) !== flow.binding) return null;
  if (Date.now() - flow.startedAt > FLOW_TTL_MS) return null;
  return flow;
}

/** What the provider gave for a code: an ID token, or GitHub's access token. */
export type ProviderCredential = { id_token: string } | { access_token: string };

/** Exchange a callback's code for the credential the domains check. */
export async function exchangeCode(provider: ProviderClient, code: string, flow: PendingFlow): Promise<ProviderCredential> {
  const { token: endpoint } = await provider.endpoints();
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: flow.redirectUri,
    client_id: provider.clientId,
    client_secret: provider.clientSecret,
    code_verifier: flow.verifier,
  });
  const response = await ssoFetch(endpoint, {
    method: 'POST',
    headers: { accept: 'application/json', 'content-type': 'application/x-www-form-urlencoded' },
    body,
  });
  const answer = (await response.json().catch(() => null)) as Record<string, unknown> | null;
  if (!response.ok || !answer || typeof answer.error === 'string') {
    const reason = typeof answer?.error_description === 'string' ? answer.error_description : typeof answer?.error === 'string' ? answer.error : `HTTP ${response.status}`;
    throw new Error(`${provider.label} did not accept the sign-in: ${reason}`);
  }
  if (provider.kind === 'oidc') {
    if (typeof answer.id_token !== 'string') throw new Error(`${provider.label} answered without an ID token`);
    return { id_token: answer.id_token };
  }
  if (typeof answer.access_token !== 'string') throw new Error(`${provider.label} answered without an access token`);
  return { access_token: answer.access_token };
}

/** Revoke a GitHub access token once every domain has checked it: it was for this sign-in only. */
export async function revokeGithubToken(provider: ProviderClient, accessToken: string): Promise<void> {
  const basic = Buffer.from(`${provider.clientId}:${provider.clientSecret}`).toString('base64');
  await ssoFetch(`https://api.github.com/applications/${encodeURIComponent(provider.clientId)}/token`, {
    method: 'DELETE',
    headers: {
      accept: 'application/vnd.github+json',
      authorization: `Basic ${basic}`,
      'content-type': 'application/json',
      'x-github-api-version': '2022-11-28',
      'user-agent': 'midstream-console',
    },
    body: JSON.stringify({ access_token: accessToken }),
  });
}

/** Keep what a sign-in did, for the page that shows it; returns the id to ask for it by. */
export function keepOutcome(outcome: SsoOutcome): string {
  prune();
  const id = token(18);
  outcomes.set(id, { at: Date.now(), outcome });
  while (outcomes.size > MAX_PENDING) outcomes.delete(outcomes.keys().next().value as string);
  return id;
}

export function outcomeOf(id: string): SsoOutcome | null {
  const kept = outcomes.get(id);
  if (!kept || Date.now() - kept.at > OUTCOME_TTL_MS) return null;
  return kept.outcome;
}

function prune(): void {
  const now = Date.now();
  for (const [state, flow] of flows) {
    if (now - flow.startedAt <= FLOW_TTL_MS) break;
    flows.delete(state);
  }
  for (const [id, kept] of outcomes) {
    if (now - kept.at <= OUTCOME_TTL_MS) break;
    outcomes.delete(id);
  }
}

/** Forget every flow and outcome, as a test does between cases. */
export function forgetFlows(): void {
  flows.clear();
  outcomes.clear();
}
