import type { FastifyInstance } from 'fastify';

import { config, dataAccessMode } from '../config.ts';
import { can } from '../auth/principal.ts';
import { domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import type { RequestAuth } from '../auth/principal.ts';
import type { ConsoleConfig, ConsoleDomain } from '../../shared/types.ts';

/**
 * What the console says of one domain, to someone: its status and version, its live tail and
 * autoscaling, and - only for those who may create a materialization there - the defaults a create
 * starts from.
 */
export async function describeDomain(domain: Domain, auth: RequestAuth | undefined): Promise<ConsoleDomain> {
  await domain.ready();
  const principal = auth?.state.principal;
  const status = domain.status;
  const reachable = status === 'ok' || status === 'incompatible';
  const [liveTail, autoscaling, defaults] = await Promise.all([
    domain.liveTail(),
    reachable ? domain.autoscaling() : Promise.resolve(null),
    reachable && principal && can(principal, 'materializations.create') ? domain.materializationDefaults() : Promise.resolve(null),
  ]);
  return {
    id: domain.id,
    name: domain.name,
    settingsHome: domain.settingsHome,
    status,
    statusMessage: domain.statusMessage,
    version: domain.version,
    connected: domain.live.fleetSnapshot().connected,
    liveTail,
    autoscaling,
    materializationDefaults: defaults,
  };
}

/** Whether every domain that says signs access tokens with the same key; null below two. */
export function federationKeysAgree(all: readonly Domain[]): boolean | null {
  const ids = all.map((domain) => domain.federationKeyId).filter((id): id is string => id !== null);
  if (ids.length < 2) return null;
  return new Set(ids).size === 1 && ids.length === all.filter((domain) => domain.status === 'ok').length;
}

/** The console's own system routes: its health and what the web app needs first. */
export async function registerSystemRoutes(app: FastifyInstance): Promise<void> {
  /** Up whatever the domains say; `degraded` while any domain's control plane cannot be read. */
  app.get('/api/health', async () => {
    const all = domains().all();
    const reports = all.map((domain) => {
      const snapshot = domain.live.fleetSnapshot();
      return {
        domain: domain.id,
        status: domain.status,
        connected: snapshot.connected,
        error: snapshot.controlPlaneError ?? domain.statusMessage,
        subscribers: domain.live.subscriberCount,
      };
    });
    const healthy = reports.every((report) => report.connected && report.status === 'ok');
    return {
      status: healthy ? 'ok' : 'degraded',
      controlPlaneConnected: reports.every((report) => report.connected),
      domains: reports,
      generatedAt: new Date().toISOString(),
      version: config.version,
    };
  });

  /**
   * What the web app needs before anything else, signed in or not: every domain, as this person may
   * know it. What only some people need - the defaults a create starts from - goes only to them.
   */
  app.get('/api/config', async (request): Promise<ConsoleConfig> => {
    const all = domains().all();
    const described = await Promise.all(all.map((domain) => describeDomain(domain, request.auths?.get(domain.id))));
    return {
      domains: described,
      settingsHome: domains().home.id,
      federationKeysAgree: federationKeysAgree(all),
      dataAccessMode: dataAccessMode(),
      pageRowLimit: config.pageRowLimit,
      exportRowLimit: config.exportRowLimit,
      resultRowLimit: config.resultRowLimit,
      pollIntervalMs: config.listPollIntervalMs,
      version: config.version,
    };
  });
}
