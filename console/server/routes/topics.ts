import type { FastifyInstance } from 'fastify';

import { domainOf, permit } from '../auth/guard.ts';
import { acceptsGzip, etagMatches, type TopicCatalog } from '../topics/catalog.ts';

/** A domain's topics, under `/api/d/<domain>/`; tests pass a catalog of their own. */
export async function registerTopicRoutes(app: FastifyInstance, catalog?: Pick<TopicCatalog, 'read'>): Promise<void> {
  /**
   * Every topic in the domain's Kafka cluster, from that domain's catalog.
   *
   * `no-cache` plus an ETag makes the browser revalidate on each read, which costs a bodiless 304
   * while the list is unchanged. The body is pre-compressed, so serving it is a buffer write.
   */
  app.get('/topics', { preHandler: permit('topics.view') }, async (request, reply) => {
    const { refresh } = request.query as { refresh?: string };
    const body = await (catalog ?? domainOf(request).topics).read({ refresh: refresh === '1' || refresh === 'true' });

    reply.header('cache-control', 'private, no-cache');
    reply.header('etag', body.etag);
    reply.header('vary', 'accept-encoding');
    if (etagMatches(request.headers['if-none-match'], body.etag)) return reply.code(304).send();

    reply.type('application/json; charset=utf-8');
    if (acceptsGzip(request.headers['accept-encoding'])) {
      reply.header('content-encoding', 'gzip');
      return reply.send(body.gzip);
    }
    return reply.send(body.json);
  });
}
