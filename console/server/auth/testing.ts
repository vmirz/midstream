/**
 * Route tests: a Fastify app whose every request is made by a principal the test chose - the same in
 * every domain unless `perDomain` names another - without a control plane to ask. Not used by the
 * running console.
 */

import type { FastifyInstance } from 'fastify';

import type { AuthState, CpPrincipal } from '../../shared/types.ts';
import { domains } from '../domains/index.ts';
import { nobody, type RequestAuth } from './principal.ts';

/** A signed-in person holding `permissions` (or every permission with `*`). */
export function testPrincipal(permissions: string[], overrides: Partial<CpPrincipal> = {}): CpPrincipal {
  return {
    ...nobody(),
    kind: 'user',
    user_id: 'usr-test',
    email: 'test@example.com',
    first_name: 'Test',
    last_name: 'Person',
    display_name: 'Test Person',
    permissions: permissions.filter((permission) => permission !== '*'),
    all_permissions: permissions.includes('*'),
    ...overrides,
  };
}

export function testAuth(principal: CpPrincipal, token: string | null = principal.kind === 'user' ? 'token-test' : null): RequestAuth {
  const state: AuthState = {
    authenticated: principal.kind === 'user',
    principal,
    session: null,
    providers: [],
    passwordPolicy: null,
    sessionError: null,
  };
  return { state, token, invalidToken: false, error: null };
}

/**
 * Make every request to `app` come from `principal`. Register before the routes under test. As the
 * real guard does, a request about one domain is resolved in that domain alone, and in another only
 * when a route asks (`authInResolving`); the console's own routes in every domain.
 */
export function installTestAuth(app: FastifyInstance, principal: CpPrincipal, perDomain: Record<string, CpPrincipal> = {}): void {
  app.decorateRequest('domain', null);
  app.decorateRequest('auths', null);
  app.decorateRequest('resolveDomain', null);
  app.addHook('onRequest', async (request) => {
    const match = /^\/api\/d\/([^/?#]+)/.exec(request.url);
    const authFor = (domain: { id: string }): RequestAuth => testAuth(perDomain[domain.id] ?? principal);
    if (match) request.domain = domains().require(decodeURIComponent(match[1] as string));
    const targets = request.domain ? [request.domain] : domains().served();
    request.auths = new Map(targets.map((domain) => [domain.id, authFor(domain)]));
    request.resolveDomain = async (domain) => authFor(domain);
  });
}
