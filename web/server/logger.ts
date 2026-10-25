import type { FastifyBaseLogger } from 'fastify';
import pino from 'pino';

import { config } from './config.ts';

// Typed as FastifyBaseLogger so Fastify keeps its default logger generic when this instance is
// supplied, which keeps route registrars assignable to a plain FastifyInstance.
export const logger: FastifyBaseLogger = pino({
  level: config.logLevel,
  base: { service: 'materializer-console' },
  redact: {
    paths: ['req.headers.authorization', 'req.headers.cookie'],
    remove: true,
  },
});

export type Logger = FastifyBaseLogger;
