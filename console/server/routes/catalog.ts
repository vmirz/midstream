/**
 * The Iceberg REST catalogs the console serves (handbook 20): one over every domain at `/catalog`, and
 * each domain's own at `/d/<domain>/catalog`.
 *
 * The federated catalog's namespaces are `<domain>.<dataset>` - two levels, the protocol's unit
 * separator between them in a path - and each request is handed to the domain the namespace names,
 * with the namespace rewritten to the domain's own single level. The caller's bearer token goes
 * through unchanged, a federated access token or a session: the domain checks it and decides, as it
 * does for an engine that calls it directly; the console adds nothing, stores nothing and caches
 * nothing. Each domain's own catalog is handed through as it is.
 */

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import { domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import { logger } from '../logger.ts';

/** The protocol's separator between a namespace's levels, in a path or a `parent` query. */
const SEPARATOR = '\u001f';
/** How long a domain may take to answer a catalog request: a table's metadata can be large. */
const PROXY_TIMEOUT_MS = 30_000;
/** The largest commit an engine may send through the console. */
const BODY_LIMIT = 16 * 1024 * 1024;
/** What each catalog answers, as a domain lists it (`lakehouse/rest.py::ENDPOINTS`). */
const ENDPOINTS = [
  'GET /v1/{prefix}/namespaces',
  'GET /v1/{prefix}/namespaces/{namespace}',
  'HEAD /v1/{prefix}/namespaces/{namespace}',
  'GET /v1/{prefix}/namespaces/{namespace}/tables',
  'GET /v1/{prefix}/namespaces/{namespace}/tables/{table}',
  'HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}',
  'POST /v1/{prefix}/namespaces/{namespace}/tables/{table}',
  'POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics',
];

/** A catalog error, shaped as the Iceberg REST specification says clients parse them. */
function catalogError(reply: FastifyReply, status: number, type: string, message: string): FastifyReply {
  return reply.code(status).header('content-type', 'application/json').send({ error: { message, type, code: status } });
}

/** A namespace's levels, from its path segment: `domain_a%1Fsales` is `["domain_a", "sales"]`. */
export function namespaceLevels(segment: string): string[] {
  return segment.split(SEPARATOR).filter((level) => level !== '');
}

/** What a domain answered, as it answered it. */
interface Answer {
  status: number;
  contentType: string | null;
  body: Buffer;
}

/** Hand a request to a domain's catalog, with the caller's token and body as they are. */
async function forward(domain: Domain, request: FastifyRequest, path: string): Promise<Answer> {
  const headers: Record<string, string> = { accept: 'application/json' };
  const authorization = request.headers.authorization;
  if (typeof authorization === 'string') headers.authorization = authorization;
  const body = request.body instanceof Buffer && request.body.length > 0 ? request.body : undefined;
  if (body) headers['content-type'] = String(request.headers['content-type'] ?? 'application/json');
  const response = await fetch(`${domain.controlPlane.baseUrl}/catalog${path}`, {
    method: request.method,
    headers,
    body,
    signal: AbortSignal.timeout(PROXY_TIMEOUT_MS),
  });
  const answer = { status: response.status, contentType: response.headers.get('content-type'), body: Buffer.from(await response.arrayBuffer()) };
  // A gateway whose domain is not there answers with a page of its own, never the catalog's JSON
  // error: the domain cannot be reached. A HEAD answer has no body to tell them apart by.
  if (NOT_THERE.has(answer.status) && request.method !== 'HEAD' && !isCatalogError(answer.body)) {
    throw new Error(`${domain.name}'s catalog answered ${answer.status} without the catalog's own error`);
  }
  return answer;
}

/** What a gateway answers when the domain behind it is not there. */
const NOT_THERE = new Set([404, 502, 503, 504]);

/** Whether a body is a catalog's own error: JSON with an `error` object, as the protocol has it. */
function isCatalogError(body: Buffer): boolean {
  try {
    const parsed = JSON.parse(body.toString('utf8')) as { error?: unknown };
    return Boolean(parsed?.error) && typeof parsed.error === 'object';
  } catch {
    return false;
  }
}

function send(reply: FastifyReply, answer: Answer): FastifyReply {
  reply.code(answer.status);
  if (answer.contentType) reply.header('content-type', answer.contentType);
  return reply.send(answer.body);
}

function unreachable(reply: FastifyReply, domain: Domain, cause: unknown): FastifyReply {
  logger.warn({ err: cause, domain: domain.id }, 'a domain’s catalog could not be reached');
  return catalogError(reply, 503, 'ServiceUnavailableException', `${domain.name}'s catalog cannot be reached right now.`);
}

/** The path after `/v1`, with its query, for the domain's own catalog. */
function querySuffix(request: FastifyRequest): string {
  const at = request.url.indexOf('?');
  return at === -1 ? '' : request.url.slice(at);
}

/** Rewrite one domain's single-level namespaces into the federated catalog's two. */
function withDomain(domain: Domain, value: unknown): unknown {
  if (!value || typeof value !== 'object') return value;
  const record = value as Record<string, unknown>;
  const lift = (namespace: unknown): unknown => (Array.isArray(namespace) ? [domain.id, ...namespace] : namespace);
  const out: Record<string, unknown> = { ...record };
  if ('namespace' in record) out.namespace = lift(record.namespace);
  if (Array.isArray(record.namespaces)) out.namespaces = record.namespaces.map(lift);
  if (Array.isArray(record.identifiers)) {
    out.identifiers = record.identifiers.map((identifier) =>
      identifier && typeof identifier === 'object' ? { ...(identifier as Record<string, unknown>), namespace: lift((identifier as Record<string, unknown>).namespace) } : identifier,
    );
  }
  return out;
}

/** The domain a federated namespace names, and the rest of it; or why there is none. */
function route(levels: string[]): { domain: Domain; rest: string[] } | null {
  const domain = domains().served().find((candidate) => candidate.id === levels[0]);
  return domain ? { domain, rest: levels.slice(1) } : null;
}

export async function registerCatalogRoutes(app: FastifyInstance): Promise<void> {
  await app.register(async (scope) => {
    // A commit is handed on byte for byte: its body is never parsed here.
    scope.removeAllContentTypeParsers();
    scope.addContentTypeParser('*', { parseAs: 'buffer', bodyLimit: BODY_LIMIT }, (_request, body, done) => done(null, body));

    /* ------------------------------------------------------------------ federated: /catalog */

    scope.get('/catalog/v1/config', async () => ({ defaults: {}, overrides: {}, endpoints: ENDPOINTS }));

    /** The domains whose catalogs list something for this token; or, under `parent`, a domain's datasets. */
    scope.get('/catalog/v1/namespaces', async (request, reply) => {
      const parent = namespaceLevels(String((request.query as { parent?: string }).parent ?? ''));
      if (parent.length === 0) {
        const listed = await Promise.all(
          domains()
            .served()
            .map(async (domain) => {
              try {
                const answer = await forward(domain, request, '/v1/namespaces');
                if (answer.status !== 200) return null;
                const body = JSON.parse(answer.body.toString('utf8')) as { namespaces?: unknown[] };
                return (body.namespaces ?? []).length > 0 ? [domain.id] : null;
              } catch (cause) {
                logger.warn({ err: cause, domain: domain.id }, 'a domain’s catalog could not be listed');
                return null;
              }
            }),
        );
        return { namespaces: listed.filter((entry): entry is string[] => entry !== null), 'next-page-token': null };
      }
      const target = route(parent);
      if (!target) return catalogError(reply, 404, 'NoSuchNamespaceException', `Namespace does not exist: ${parent.join('.')}`);
      // A dataset has no namespaces under it.
      if (target.rest.length > 0) return { namespaces: [], 'next-page-token': null };
      try {
        const answer = await forward(target.domain, request, '/v1/namespaces');
        if (answer.status !== 200) return send(reply, answer);
        return withDomain(target.domain, JSON.parse(answer.body.toString('utf8')));
      } catch (cause) {
        return unreachable(reply, target.domain, cause);
      }
    });

    const namespaceRoute = async (request: FastifyRequest, reply: FastifyReply, suffix = ''): Promise<unknown> => {
      const levels = namespaceLevels((request.params as { namespace: string }).namespace);
      const target = route(levels);
      if (!target || levels.length > 2) return catalogError(reply, 404, 'NoSuchNamespaceException', `Namespace does not exist: ${levels.join('.')}`);
      if (target.rest.length === 0) {
        // A domain is a namespace of its own, holding its datasets and no tables.
        if (suffix) return catalogError(reply, 404, 'NoSuchNamespaceException', `Namespace does not exist: ${levels.join('.')}: a domain holds datasets, not tables`);
        return request.method === 'HEAD' ? reply.code(204).send() : { namespace: [target.domain.id], properties: { name: target.domain.name } };
      }
      try {
        const answer = await forward(target.domain, request, `/v1/namespaces/${encodeURIComponent(target.rest[0] as string)}${suffix}${querySuffix(request)}`);
        if (answer.status !== 200 || !answer.contentType?.includes('json') || request.method === 'HEAD' || suffix.startsWith('/tables/')) {
          // A table is answered as the domain answered it: its metadata names no namespace.
          return send(reply, answer);
        }
        return withDomain(target.domain, JSON.parse(answer.body.toString('utf8')));
      } catch (cause) {
        return unreachable(reply, target.domain, cause);
      }
    };

    // Fastify answers HEAD for every GET route, through the GET's handler, which hands HEAD on as it came.
    scope.get('/catalog/v1/namespaces/:namespace', (request, reply) => namespaceRoute(request, reply));
    scope.get('/catalog/v1/namespaces/:namespace/tables', (request, reply) => namespaceRoute(request, reply, '/tables'));
    for (const method of ['GET', 'POST'] as const) {
      scope.route({
        method,
        url: '/catalog/v1/namespaces/:namespace/tables/:table',
        handler: (request, reply) =>
          namespaceRoute(request, reply, `/tables/${encodeURIComponent((request.params as { table: string }).table)}`),
      });
    }
    scope.post('/catalog/v1/namespaces/:namespace/tables/:table/metrics', (request, reply) =>
      namespaceRoute(request, reply, `/tables/${encodeURIComponent((request.params as { table: string }).table)}/metrics`),
    );
    // Namespaces and tables are made and dropped by materializations, never through a catalog.
    for (const [method, url] of [
      ['POST', '/catalog/v1/namespaces'],
      ['DELETE', '/catalog/v1/namespaces/:namespace'],
      ['POST', '/catalog/v1/namespaces/:namespace/tables'],
      ['DELETE', '/catalog/v1/namespaces/:namespace/tables/:table'],
      ['POST', '/catalog/v1/transactions/commit'],
    ] as const) {
      scope.route({
        method,
        url,
        handler: (_request, reply) =>
          catalogError(reply, 501, 'NotImplementedException', 'Namespaces and tables are made and dropped by materializations, not through the catalog.'),
      });
    }

    /* ------------------------------------------------------- each domain's own: /d/<domain>/catalog */

    scope.all('/d/:domain/catalog/*', async (request, reply) => {
      const { domain: id } = request.params as { domain: string };
      const domain = domains().served().find((candidate) => candidate.id === id);
      if (!domain) return catalogError(reply, 404, 'NoSuchNamespaceException', `There is no domain ${id}.`);
      // The path as the engine encoded it, query and all: handed on byte for byte.
      const marker = `/d/${encodeURIComponent(id)}/catalog`;
      const raw = request.url.startsWith(marker) ? request.url.slice(marker.length) : request.url.slice(request.url.indexOf('/catalog') + '/catalog'.length);
      try {
        return send(reply, await forward(domain, request, raw));
      } catch (cause) {
        return unreachable(reply, domain, cause);
      }
    });
  });
}
