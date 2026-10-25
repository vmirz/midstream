// First: sizes Node's worker pool, which must happen before anything uses it.
import './workerPool.ts';

import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import cors from '@fastify/cors';
import fastifyStatic from '@fastify/static';
import Fastify, { LogController } from 'fastify';

import { registerAuth } from './auth/guard.ts';
import { config, dataAccessMode, hmacCredentialWarning } from './config.ts';
import { ConsoleError } from './errors.ts';
import { liveStore } from './live/store.ts';
import { logger } from './logger.ts';
import { registerAuthRoutes } from './routes/auth.ts';
import { registerDataRoutes } from './routes/data.ts';
import { registerDatasetRoutes } from './routes/datasets.ts';
import { registerIdentityRoutes } from './routes/identity.ts';
import { evictIdleRuns } from './runs/store.ts';
import { registerMaterializationRoutes } from './routes/materializations.ts';
import { registerSchemaRoutes } from './routes/schema.ts';
import { registerTableRoutes } from './routes/table.ts';
import { registerSettingsRoutes } from './routes/settings.ts';
import { registerStreamRoutes } from './routes/stream.ts';
import { registerSystemRoutes } from './routes/system.ts';
import { registerTopicRoutes } from './routes/topics.ts';
import { consoleSettings } from './settings/store.ts';
import { topicCatalog } from './topics/catalog.ts';

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

  // Before every route: who is calling, and whether a change comes from this console's own pages.
  await registerAuth(app);
  await registerAuthRoutes(app);
  await registerIdentityRoutes(app);
  await registerSystemRoutes(app);
  await registerMaterializationRoutes(app);
  await registerDatasetRoutes(app);
  await registerTopicRoutes(app);
  await registerSchemaRoutes(app);
  await registerTableRoutes(app);
  await registerDataRoutes(app);
  await registerStreamRoutes(app);
  await registerSettingsRoutes(app);

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

  // Overrides from the Settings page; the deployment's values apply until they have been read.
  consoleSettings.start();
  liveStore.start();
  // Loaded ahead of the first dialog open; a failure here is retried by the first read.
  topicCatalog.warm();

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'shutting down console');
    liveStore.stop();
    consoleSettings.stop();
    await app.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));

  await app.listen({ port: config.port, host: config.host });
  logger.info(
    { port: config.port, controlPlane: config.controlPlaneUrl, workerPool: Number(process.env.UV_THREADPOOL_SIZE) },
    'materializer console ready',
  );
  logDataAccessMode();
  if (!config.serviceToken) {
    logger.warn(
      'CONSOLE_SERVICE_TOKEN is not set: the live views read the control plane as a guest and show only ' +
        'what guests may see. Set it to the control plane\'s MATERIALIZER_AUTH_SERVICE_TOKEN.',
    );
  }

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
 * State the data-access mode at startup, with what it costs.
 *
 * `cached` is the mode you get by omitting configuration, and its budgets are the console's most
 * visible limitation — so it is worth one explicit line at boot rather than being discovered later
 * through a refused query. `direct` needs only an HMAC key pair and removes the budgets entirely.
 */
function logDataAccessMode(): void {
  if (dataAccessMode() === 'disabled') {
    logger.warn('data access is disabled; the Data tab is unavailable');
    return;
  }
  if (dataAccessMode() === 'direct') {
    const warning = hmacCredentialWarning();
    if (warning) logger.warn(warning);
    logger.info('data access mode: direct — DuckDB reads gs:// in place, with no per-scan budget');
    return;
  }
  logger.info(
    {
      maxFiles: config.cachedModeMaxFiles,
      maxBytes: config.cachedModeMaxBytes,
    },
    'data access mode: cached — objects are mirrored to local disk before each scan, bounded by ' +
      'the budgets above. Set CONSOLE_GCS_HMAC_KEY_ID and CONSOLE_GCS_HMAC_SECRET to read gs:// ' +
      'directly and remove them.',
  );
}
