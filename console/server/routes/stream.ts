import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import { authOf, domainOf, permit, presentedToken, watchAccess } from '../auth/guard.ts';
import { can, resolveAuth, type RequestAuth } from '../auth/principal.ts';
import { config } from '../config.ts';
import { domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import { visibleDetail } from '../live/store.ts';
import { logger } from '../logger.ts';
import { materializationId } from './materializations.ts';
import type { ConsoleFleet, DomainFleet } from '../../shared/types.ts';

const HEARTBEAT_MS = 20_000;
/** How often the merged fleet stream checks, in each domain, that its caller may still see it. */
const ACCESS_CHECK_MS = 30_000;
/** Updates from several domains within this long go out as one frame. */
const MERGE_DELAY_MS = 100;

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

/** One domain's part of the fleet for a caller: its snapshot when they may see its materializations. */
export function domainFleet(domain: Domain, auth: RequestAuth): DomainFleet {
  const base = { domain: domain.id, name: domain.name };
  const principal = auth.state.principal;
  if (can(principal, 'materializations.view')) return { ...base, access: 'ok', snapshot: domain.live.fleetSnapshot() };
  if (auth.error) return { ...base, access: 'unavailable', snapshot: null };
  return { ...base, access: principal.kind === 'user' ? 'forbidden' : 'sign_in_required', snapshot: null };
}

/** Every served domain's part of the fleet, for a caller resolved in each. */
export function consoleFleet(auths: ReadonlyMap<string, RequestAuth>): ConsoleFleet {
  return {
    generatedAt: new Date().toISOString(),
    domains: domains()
      .served()
      .flatMap((domain) => {
        const auth = auths.get(domain.id);
        return auth ? [domainFleet(domain, auth)] : [];
      }),
  };
}

/** The console's own streams: every domain's fleet, merged (handbook 20). */
export async function registerFleetStreamRoutes(app: FastifyInstance): Promise<void> {
  /** Every domain's fleet now, no older than the list cadence. */
  app.get('/api/fleet', async (request): Promise<ConsoleFleet> => {
    await Promise.all(domains().served().map((domain) => domain.live.ensureFresh(config.listPollIntervalMs)));
    return consoleFleet(request.auths ?? new Map());
  });

  /**
   * Every domain's fleet, live: one frame whenever any domain's store refreshes. Each domain's part
   * is what this caller may see there, checked again every 30 seconds in that domain; a domain whose
   * materializations they may not see is named, without its snapshot.
   */
  app.get('/api/stream/fleet', (request, reply) => {
    const auths = new Map(request.auths ?? []);
    const stream = openStream(reply);
    let timer: ReturnType<typeof setTimeout> | null = null;
    const emit = (): void => {
      timer = null;
      stream.send('fleet', consoleFleet(auths));
    };
    const schedule = (): void => {
      timer ??= setTimeout(emit, MERGE_DELAY_MS);
    };
    const unsubscribes = domains()
      .served()
      .map((domain) => domain.live.subscribeFleet(() => schedule()));
    const checks = setInterval(() => void recheck(request, auths).then(schedule), ACCESS_CHECK_MS);
    checks.unref();
    request.raw.on('close', () => {
      for (const unsubscribe of unsubscribes) unsubscribe();
      clearInterval(checks);
      if (timer) clearTimeout(timer);
      stream.close();
    });
  });
}

/** Check the caller again in every domain, keeping what each last said when it cannot answer. */
async function recheck(request: FastifyRequest, auths: Map<string, RequestAuth>): Promise<void> {
  await Promise.all(
    domains()
      .served()
      .map(async (domain) => {
        try {
          const auth = await resolveAuth(domain, presentedToken(request, domain).token);
          if (!auth.error) auths.set(domain.id, auth);
        } catch (cause) {
          logger.debug({ err: cause, domain: domain.id }, 'fleet stream access check failed');
        }
      }),
  );
}

/** A domain's streams, under `/api/d/<domain>/`. */
export async function registerStreamRoutes(app: FastifyInstance): Promise<void> {
  /**
   * One materialization, live. The store holds what the domain's service token sees; each event is
   * cut down to what this caller may see (`visibleDetail`), as of their latest access check.
   */
  app.get('/stream/materializations/:materializationId', { preHandler: permit('materializations.view_details') }, (request, reply) => {
    const domain = domainOf(request);
    const id = materializationId(request);
    let principal = authOf(request).state.principal;
    const stream = openStream(reply);
    const unsubscribe = domain.live.subscribeDetail(id, (snapshot) => stream.send('detail', visibleDetail(snapshot, principal)));
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
