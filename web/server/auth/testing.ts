/**
 * Route tests: a Fastify app whose every request is made by a principal the test chose, without
 * a control plane to ask. Not used by the running console.
 */

import type { FastifyInstance } from 'fastify';

import type { AuthState, CpPrincipal } from '../../shared/types.ts';
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

/** Make every request to `app` come from `principal`. Register before the routes under test. */
export function installTestAuth(app: FastifyInstance, principal: CpPrincipal): void {
  app.decorateRequest('auth', null);
  app.addHook('onRequest', async (request) => {
    request.auth = testAuth(principal);
  });
}
