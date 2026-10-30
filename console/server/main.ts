// First: sizes Node's worker pool, which must happen before anything uses it.
import './workerPool.ts';

import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import cors from '@fastify/cors';
import fastifyStatic from '@fastify/static';
import Fastify, { LogController } from 'fastify';

import { registerAuth } from './auth/guard.ts';
import { config } from './config.ts';
import { ConsoleError } from './errors.ts';
import { consoleSettings, domains } from './domains/index.ts';
import { logger } from './logger.ts';
import { registerAuthRoutes, registerConsoleAuthRoutes } from './routes/auth.ts';
import { registerDataRoutes, registerRunRoutes } from './routes/data.ts';
import { registerDatasetRoutes } from './routes/datasets.ts';
import { registerIdentityRoutes } from './routes/identity.ts';
import { evictIdleRuns } from './runs/store.ts';
import { registerMaterializationRoutes } from './routes/materializations.ts';
import { registerSchemaRoutes } from './routes/schema.ts';
import { registerTableRoutes } from './routes/table.ts';
import { registerSearchRoutes } from './routes/search.ts';
import { registerSsoRoutes } from './routes/sso.ts';
import { registerCatalogRoutes } from './routes/catalog.ts';
import { RevocationSync } from './auth/revocations.ts';
import { registerConsoleSettingsRoutes, registerSettingsRoutes } from './routes/settings.ts';
import { registerFleetStreamRoutes, registerStreamRoutes } from './routes/stream.ts';
import { registerSystemRoutes } from './routes/system.ts';
import { registerTopicRoutes } from './routes/topics.ts';
import { storage, type StorageRegistry } from './storage/registry.ts';

const rootDir = join(dirname(fileURLToPath(import.meta.url)), '..');
const clientDir = join(rootDir, 'dist');

async function main(): Promise<void> {
  const app = Fastify({
    loggerInstance: logger,
    bodyLimit: 1024 * 1024,
    trustProxy: true,
    logController: new LogController({ disableRequestLogging: true }),
    // Server-sent event streams never end on their own, so a graceful close waited on them forever:
    // a restart under `--watch` hung for as long as any browser tab stayed open. Every stream
    // reconnects by itself, so closing them outright costs a client nothing.
    forceCloseConnections: true,
  });

  if (config.corsOrigin) {
    await app.register(cors, { origin: config.corsOrigin, credentials: false });
  }

  app.setErrorHandler((error, request, reply) => {
    if (error instanceof ConsoleError) {
      request.log.debug({ err: error, url: request.url }, 'console error');
      return reply.code(error.statusCode).send(error.toBody());
    }
    request.log.error({ err: error, url: request.url }, 'unhandled console failure');
    return reply.code(500).send({
      error: { code: 'internal_error', message: 'the console failed to handle this request', details: {} },
    });
  });

  // The domains this console serves, and the storage it reads their tables from; a registry that
  // breaks a rule stops the console here.
  const served = domains();
  const profiles = storage();

  // Before every route: who is calling, in each domain, and whether a change comes from this
  // console's own pages.
  await registerAuth(app);

  // The console's own routes: what the web app needs first, the session in every domain, every
  // domain's fleet, search, runs and the console's own settings.
  await registerSystemRoutes(app);
  await registerConsoleAuthRoutes(app);
  await registerSsoRoutes(app);
  await registerCatalogRoutes(app);
  await registerFleetStreamRoutes(app);
  await registerSearchRoutes(app);
  await registerRunRoutes(app);
  await registerConsoleSettingsRoutes(app);

  // Everything about one domain, under /api/d/<domain>/ (handbook 20).
  await app.register(
    async (scope) => {
      await registerAuthRoutes(scope);
      await registerIdentityRoutes(scope);
      await registerMaterializationRoutes(scope);
      await registerDatasetRoutes(scope);
      await registerTopicRoutes(scope);
      await registerSchemaRoutes(scope);
      await registerTableRoutes(scope);
      await registerDataRoutes(scope);
      await registerStreamRoutes(scope);
      await registerSettingsRoutes(scope);
    },
    { prefix: '/api/d/:domain' },
  );

  if (config.serveClient && existsSync(join(clientDir, 'index.html'))) {
    // Wildcard matching is disabled so unknown paths fall through to the SPA handler below
    // instead of being answered by the static plugin.
    await app.register(fastifyStatic, { root: clientDir, index: false, wildcard: false });
    app.setNotFoundHandler((request, reply) => {
      // Only client routes fall back to the SPA shell; a missing asset must stay a 404 so the
      // browser reports a load failure instead of a confusing MIME-type error.
      if (request.url.startsWith('/api/') || /\.[a-z0-9]+(\?|$)/i.test(request.url)) {
        return reply
          .code(404)
          .send({ error: { code: 'not_found', message: `no route for ${request.url}`, details: {} } });
      }
      return reply.sendFile('index.html');
    });
  }

  // Overrides from the Settings page, read from the settings home; the deployment's values apply
  // until they have been read.
  consoleSettings().start();
  served.start();
  // Token revocations reach every domain: at once from the revoke, and every five minutes after.
  const revocations = new RevocationSync(() => served.served());
  revocations.start();

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'shutting down console');
    revocations.stop();
    served.stop();
    consoleSettings().stop();
    await app.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));

  await app.listen({ port: config.port, host: config.host });
  logger.info(
    {
      port: config.port,
      domains: served.all().map((domain) => ({ id: domain.id, controlPlane: domain.controlPlane.baseUrl, tail: domain.tailUrl })),
      settingsHome: served.home.id,
      workerPool: Number(process.env.UV_THREADPOOL_SIZE),
    },
    'midstream console ready',
  );
  logStorage(profiles);

  // Stored statement results are the only thing the console leaves on disk that grows with use.
  const runEviction = setInterval(() => {
    void evictIdleRuns().catch((cause) => logger.warn({ err: cause }, 'run eviction failed'));
  }, 60_000);
  runEviction.unref();
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, 'console failed to start');
  process.exit(1);
});

/**
 * State the storage the console reads at startup: one line per profile — what it serves, how, and
 * whether its credentials were found (never a value) — and what each costs. Cached profiles' budgets
 * are the console's most visible limitation, so they are worth a line at boot rather than being
 * discovered through a refused query.
 */
function logStorage(registry: StorageRegistry): void {
  if (!config.dataAccessEnabled) logger.warn('data access is disabled; the Data tab is unavailable');
  if (registry.size === 0) {
    logger.warn(
      'no storage profiles are configured (CONSOLE_STORAGE_FILE or CONSOLE_STORAGE): the console reads no object storage, ' +
        'and every table answers that no storage is configured for its location',
    );
    return;
  }
  for (const profile of registry.profiles) {
    const described = profile.describe();
    logger.info(
      {
        profile: described.name,
        kind: described.kind,
        scope: described.scope,
        mode: described.mode,
        endpoint: described.endpoint ?? undefined,
        credentials: described.credentialsFound ? 'found' : 'not found',
        readsWith: described.credentials,
        ...(described.mode === 'cached' ? { maxFiles: config.cachedModeMaxFiles, maxBytes: config.cachedModeMaxBytes } : {}),
      },
      described.mode === 'direct'
        ? 'storage profile: DuckDB reads its data files in place'
        : 'storage profile: its data files are downloaded before each scan, within the cached-mode budgets',
    );
    for (const warning of profile.warnings()) logger.warn(warning);
  }
  const direct = registry.profiles.filter((profile) => profile.mode === 'direct').map((profile) => profile.name);
  if (direct.length > 0) {
    logger.warn(
      { profiles: direct },
      'storage profiles read in place hand their credentials to DuckDB: give them read-only access. A statement’s sandbox ' +
        'does not stop COPY … TO a path it may read; only the SQL guard does',
    );
  }
}
