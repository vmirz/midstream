import type { FastifyInstance } from 'fastify';

import { permit } from '../auth/guard.ts';
import { acceptsGzip, etagMatches, topicCatalog, type TopicCatalog } from '../topics/catalog.ts';

export async function registerTopicRoutes(
  app: FastifyInstance,
  catalog: Pick<TopicCatalog, 'read'> = topicCatalog,
): Promise<void> {
  /**
   * Every topic in the cluster, from the process-wide catalog.
   *
   * `no-cache` plus an ETag makes the browser revalidate on each read, which costs a bodiless 304
   * while the list is unchanged. The body is pre-compressed, so serving it is a buffer write.
   */
  app.get('/api/topics', { preHandler: permit('topics.view') }, async (request, reply) => {
    const { refresh } = request.query as { refresh?: string };
    const body = await catalog.read({ refresh: refresh === '1' || refresh === 'true' });

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
