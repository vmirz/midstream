/**
 * The URL people reach this console at: `CONSOLE_URL`, or the URL a request arrived at - which
 * suits development only, since the ingress decides the scheme and host a browser sees.
 */

import type { FastifyRequest } from 'fastify';

import { config } from './config.ts';

/** The console's base URL, without a trailing slash. */
export function consoleBaseUrl(request: Pick<FastifyRequest, 'protocol' | 'headers'>): string {
  if (config.consoleUrl) return config.consoleUrl;
  const host = request.headers['x-forwarded-host'] ?? request.headers.host ?? 'localhost';
  return `${request.protocol}://${Array.isArray(host) ? host[0] : host}`;
}
