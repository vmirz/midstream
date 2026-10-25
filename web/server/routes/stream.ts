import type { FastifyInstance, FastifyReply } from 'fastify';

import { authOf, permit, watchAccess } from '../auth/guard.ts';
import { liveStore, visibleDetail } from '../live/store.ts';
import { materializationId } from './materializations.ts';

const HEARTBEAT_MS = 20_000;

/**
 * Open a server-sent-events channel.
 *
 * SSE is used instead of websockets because the stream is strictly server→client, reconnects
 * automatically in every browser, and survives ordinary HTTP infrastructure unchanged.
 */
function openStream(reply: FastifyReply): {
  send: (event: string, payload: unknown) => void;
  close: () => void;
} {
  reply.raw.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-accel-buffering': 'no',
  });
  reply.raw.write(': stream open\n\n');

  const heartbeat = setInterval(() => reply.raw.write(': keep-alive\n\n'), HEARTBEAT_MS);

  return {
    send(event, payload) {
      if (reply.raw.writableEnded) return;
      reply.raw.write(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
    },
    close() {
      clearInterval(heartbeat);
      if (!reply.raw.writableEnded) reply.raw.end();
    },
  };
}

export async function registerStreamRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/stream/fleet', { preHandler: permit('materializations.view') }, (request, reply) => {
    const stream = openStream(reply);
    const unsubscribe = liveStore.subscribeFleet((snapshot) => stream.send('fleet', snapshot));
    const stopWatching = watchAccess(request, 'materializations.view', (reason) => {
      // Tell the page why the stream ended, so it shows a sign-in prompt instead of reconnecting.
      stream.send('auth', { reason });
      unsubscribe();
      stream.close();
    });
    request.raw.on('close', () => {
      unsubscribe();
      stopWatching();
      stream.close();
    });
  });

  /**
   * One materialization, live. The store holds what the console's own credential sees; each event is
   * cut down to what this caller may see (`visibleDetail`), as of their latest access check.
   */
  app.get('/api/stream/materializations/:materializationId', { preHandler: permit('materializations.view_details') }, (request, reply) => {
    const id = materializationId(request);
    let principal = authOf(request).state.principal;
    const stream = openStream(reply);
    const unsubscribe = liveStore.subscribeDetail(id, (snapshot) => stream.send('detail', visibleDetail(snapshot, principal)));
    const stopWatching = watchAccess(
      request,
      'materializations.view_details',
      (reason) => {
        stream.send('auth', { reason });
        unsubscribe();
        stream.close();
      },
      (auth) => {
        principal = auth.state.principal;
      },
    );
    request.raw.on('close', () => {
      unsubscribe();
      stopWatching();
      stream.close();
    });
  });
}
