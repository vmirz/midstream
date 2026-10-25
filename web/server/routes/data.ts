import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';

import { authOf, callerOf, permit, permitAnywhere, requireOnDatasets, type DatasetRef } from '../auth/guard.ts';
import { config } from '../config.ts';
import { ConsoleError, badRequest } from '../errors.ts';
import { logger } from '../logger.ts';
import { estimateRange, loadCatalog, summarize } from '../data/catalog.ts';
import { fetchHead, fetchTail, readPeek } from '../data/tail.ts';
import { liveStore } from '../live/store.ts';
import { openExport } from '../runs/export.ts';
import { profileColumn } from '../runs/profile.ts';
import { getRun, pickTable, refreshRun, startRun, tablesNamed, tablesRead, validateStatement, type RunTableSource } from '../runs/store.ts';
import type { Run } from '../runs/run.ts';
import type { TailAccess } from '../runs/tail.ts';
import { materializationId, parseBody } from './materializations.ts';
import { filtersSchema } from './requestSchemas.ts';
import { liveTailOff } from './system.ts';
import {
  RUN_EXPORT_FORMATS,
  RUN_EXPORT_SHAPES,
  readableTables,
  type CpMaterialization,
  type HeadPeek,
  type RunExportFormat,
  type RunRefresh,
} from '../../shared/types.ts';

/** Epoch milliseconds, bounded to the years a Kafka timestamp can plausibly hold. */
const instant = z.number().int().min(0).max(32_503_680_000_000).nullable();
const rangeSchema = z.object({ from: instant, to: instant });
/** A snapshot id: a 64-bit integer, carried as decimal text. */
const snapshotId = z.string().regex(/^-?\d{1,20}$/, 'a snapshot id is a whole number').nullish();
/** One of the materialization's tables, by name; its first when absent. */
const tableName = z.string().min(1).max(128).nullish();

const runSchema = z.discriminatedUnion('kind', [
  z.object({
    kind: z.literal('browse'),
    table: tableName,
    range: rangeSchema,
    rangeLabel: z.string().max(120).nullish(),
    filters: filtersSchema,
    order: z.enum(['newest', 'oldest']).optional(),
    asOf: snapshotId,
    liveTail: z.boolean().optional(),
  }),
  z.object({
    kind: z.literal('sql'),
    table: tableName,
    range: rangeSchema,
    rangeLabel: z.string().max(120).nullish(),
    sql: z.string().min(1).max(20_000),
    asOf: snapshotId,
    liveTail: z.boolean().optional(),
  }),
]);
/** A refresh may bring the run's range resolved again, for the run that replaces it. */
const refreshSchema = z.object({ range: rangeSchema.optional() });
/** The newest records of a topic: at most what the tail app peeks at once (`live_tail_peek_max_rows`). */
const headSchema = z.object({ rows: z.coerce.number().int().min(1).max(10_000).default(200) });

const estimateSchema = z.object({ ranges: z.array(rangeSchema).min(1).max(32), asOf: snapshotId, table: tableName });
const summarySchema = z.object({ asof: snapshotId, table: tableName });
const validateSchema = z.object({ sql: z.string().min(1).max(20_000) });
const rowsSchema = z.object({
  offset: z.coerce.number().int().min(0).max(1_000_000_000).default(0),
  limit: z.coerce.number().int().min(1).max(10_000).default(100),
});
const exportSchema = z.object({
  format: z.enum(RUN_EXPORT_FORMATS).default('csv'),
  // Flat unless asked: it is what the endpoint returned before nesting existed.
  shape: z.enum(RUN_EXPORT_SHAPES).default('flat'),
});
const profileSchema = z.object({ column: z.string().min(1).max(1_024) });

/** Whose runs a request may read: the signed-in person's, or this guest address's. */
function ownerOf(request: FastifyRequest): string {
  return authOf(request).state.principal.user_id ?? `guest:${request.ip}`;
}

function runId(request: FastifyRequest): string {
  const id = (request.params as { runId?: string }).runId ?? '';
  if (!/^[0-9a-f-]{36}$/i.test(id)) throw badRequest(`invalid run id "${id}"`);
  return id;
}

/**
 * Each of a materialization's tables - written or kept - with the dataset SQL names it in,
 * `<dataset>.<table>`, and where it lives once created.
 */
async function tablesOf(id: string): Promise<RunTableSource[]> {
  return (await materializationTables(id)).tables;
}

async function materializationTables(id: string): Promise<{ materialization: CpMaterialization; tables: RunTableSource[] }> {
  const detail = await liveStore.getDetail(id);
  const locations = new Map(detail.tables.map((table) => [table.name, table.location]));
  const tables = readableTables(detail.materialization).map((table) => ({
    name: table.name,
    dataset: table.dataset,
    datasetId: table.dataset_id,
    location: locations.get(table.name) ?? null,
  }));
  return { materialization: detail.materialization, tables };
}

/**
 * How a run of this request reads live tails: with the person's own session, which the tail app
 * checks itself, or not at all when live tail is off here or in the control plane, or the
 * materialization is not running (`off` says which).
 */
async function tailAccess(request: FastifyRequest, materialization: CpMaterialization): Promise<TailAccess> {
  const caller = callerOf(request);
  const id = materialization.materialization_id;
  return {
    off: await liveTailOff(materialization),
    fetch: (table, after, signal) =>
      fetchTail(caller, { materializationId: id, table, after, maxRows: config.liveTailMaxRows, timeoutMs: config.liveTailTimeoutMs, signal }),
  };
}

/** The datasets of some tables, each once, as a refusal names them. */
function datasetsOf(tables: RunTableSource[]): DatasetRef[] {
  return [...new Map(tables.map((table) => [table.datasetId, { id: table.datasetId, name: table.dataset }])).values()];
}

/**
 * Refuse a caller who may not do `permission` with every one of `tables`, naming the first dataset
 * they lack it on: a statement over a table someone may not query is refused, whatever else it reads.
 */
function requireOnTables(request: FastifyRequest, permission: string, tables: RunTableSource[]): void {
  requireOnDatasets(authOf(request), permission, datasetsOf(tables));
}

/** A run, for whoever started it, while they may still `permission` on every dataset it reads. */
function ownRun(request: FastifyRequest, permission = 'data.query'): Run {
  const run = getRun(runId(request), ownerOf(request));
  requireOnDatasets(authOf(request), permission, run.datasets);
  return run;
}

export async function registerDataRoutes(app: FastifyInstance): Promise<void> {
  /**
   * What one of a materialization's tables holds, from its manifests alone: counts and a time span,
   * never a row. The detail page's overview shows it, so it needs only what that page does.
   * `?table=` names the table (the first by default), `?asof=` an older snapshot of it.
   */
  app.get('/api/materializations/:materializationId/data/summary', { preHandler: permit('materializations.view_details') }, async (request) => {
    const id = materializationId(request);
    const query = parseBody(summarySchema, request.query ?? {});
    const table = pickTable(await tablesOf(id), query.table);
    return summarize(await loadCatalog(id, table.location, { snapshotId: query.asof ?? null, table: table.name }));
  });

  /** What each time range would read of a table, before anything is read. Drives the time picker's figures. */
  app.post('/api/materializations/:materializationId/data/estimate', { preHandler: permitAnywhere('data.query') }, async (request) => {
    const id = materializationId(request);
    const body = parseBody(estimateSchema, request.body);
    const table = pickTable(await tablesOf(id), body.table);
    requireOnTables(request, 'data.query', [table]);
    const catalog = await loadCatalog(id, table.location, { snapshotId: body.asOf ?? null, table: table.name });
    return { asOf: new Date(catalog.loadedAt).toISOString(), estimates: body.ranges.map((range) => estimateRange(catalog, range)) };
  });

  /** Bind a statement without running it, so the editor can mark an error while it is typed. */
  app.post('/api/materializations/:materializationId/data/validate', { preHandler: permitAnywhere('data.query') }, async (request) => {
    const id = materializationId(request);
    const body = parseBody(validateSchema, request.body);
    const tables = await tablesOf(id);
    requireOnTables(request, 'data.query', tablesNamed(tables, body.sql));
    await validateStatement({ materializationId: id, tables, sql: body.sql });
    return { ok: true };
  });

  /**
   * Run: the only way the console executes a query. Returns at once; progress streams. The caller
   * must be allowed to query every table it reads, each in its dataset.
   */
  app.post('/api/materializations/:materializationId/runs', { preHandler: permitAnywhere('data.query') }, async (request, reply) => {
    const id = materializationId(request);
    const body = parseBody(runSchema, request.body);
    const { materialization, tables } = await materializationTables(id);
    requireOnTables(request, 'data.query', tablesRead(tables, body));
    const tail = await tailAccess(request, materialization);
    const run = await startRun({ materializationId: id, ownerId: ownerOf(request), tables, request: body, tail });
    return reply.code(201).send(run.snapshot());
  });

  app.get('/api/runs/:runId', { preHandler: permitAnywhere('data.query') }, async (request) => ownRun(request).snapshot());

  /**
   * Bring a run up to date: the live tail's newest records appended to it, or a new run in its place
   * when the table has a newer snapshot or the run is a statement. The Data tab's Live mode calls it
   * every few seconds for a browse run; a statement's Auto re-run gets a new run each time.
   */
  app.post('/api/runs/:runId/refresh', { preHandler: permitAnywhere('data.query') }, async (request): Promise<RunRefresh> => {
    const run = ownRun(request);
    const body = parseBody(refreshSchema, request.body ?? {});
    const { materialization, tables } = await materializationTables(run.materializationId);
    // A run that replaces this one reads what it reads now: a statement may name tables it did not.
    requireOnTables(request, 'data.query', tablesRead(tables, run.request));
    const outcome = await refreshRun({
      run,
      ownerId: ownerOf(request),
      tables,
      range: body.range ?? null,
      tail: await tailAccess(request, materialization),
    });
    return { replaced: outcome.replaced, run: outcome.run.snapshot() };
  });

  /**
   * The newest records on a table's topic, committed or not, in the table's columns: what the Data
   * tab shows while the table's runtime is too far behind for the tail (`tail.status: backlogged`).
   * Never part of a run, its export or its profiles.
   */
  app.get('/api/materializations/:materializationId/tables/:table/head', { preHandler: permitAnywhere('data.query') }, async (request): Promise<HeadPeek> => {
    const id = materializationId(request);
    const query = parseBody(headSchema, request.query ?? {});
    const { materialization, tables } = await materializationTables(id);
    const table = pickTable(tables, (request.params as { table?: string }).table ?? '');
    requireOnTables(request, 'data.query', [table]);
    const off = await liveTailOff(materialization);
    if (off) throw new ConsoleError('live_tail_off', off.message, 404, { reason: off.reason });
    const outcome = await fetchHead(callerOf(request), { materializationId: id, table: table.name, rows: query.rows, timeoutMs: config.liveTailTimeoutMs });
    if (outcome.kind === 'off' || (outcome.kind === 'unavailable' && outcome.reason === 'not_tailed')) {
      throw new ConsoleError('live_tail_off', outcome.message, 404, { reason: outcome.reason });
    }
    if (outcome.kind !== 'rows') {
      const status = outcome.reason === 'forbidden' && outcome.status !== null ? outcome.status : 503;
      throw new ConsoleError('tail_unavailable', outcome.message, status, { reason: outcome.reason });
    }
    const catalog = await loadCatalog(id, table.location, { table: table.name });
    const { rows, columns } = await readPeek(outcome.bytes, catalog, query.rows);
    return {
      rows,
      columns,
      peek: {
        rows: rows.length,
        offsets: outcome.offsets,
        endOffsets: outcome.endOffsets,
        behind: outcome.behind,
        state: outcome.state,
        settling: outcome.settling,
        generatedAt: outcome.generatedAt,
      },
    };
  });

  app.post('/api/runs/:runId/cancel', { preHandler: permitAnywhere('data.query') }, async (request) => {
    // Anyone may stop their own run, whatever they may still read.
    const run = getRun(runId(request), ownerOf(request));
    run.cancel();
    return run.snapshot();
  });

  app.get('/api/runs/:runId/rows', { preHandler: permitAnywhere('data.query') }, async (request) => {
    const run = ownRun(request);
    const query = parseBody(rowsSchema, request.query ?? {});
    return run.readPage(query.offset, Math.min(query.limit, config.pageRowLimit));
  });

  app.post('/api/runs/:runId/profile', { preHandler: permitAnywhere('data.query') }, async (request) => {
    const run = ownRun(request);
    return profileColumn(run, parseBody(profileSchema, request.body).column);
  });

  /** An export needs `data.export` on every dataset the run read. */
  app.get('/api/runs/:runId/export', { preHandler: permitAnywhere('data.export') }, async (request, reply: FastifyReply) => {
    const run = ownRun(request, 'data.export');
    const { format, shape } = parseBody(exportSchema, request.query ?? {});
    const download = await openExport(run, format as RunExportFormat, shape);
    reply.raw.on('close', () => {
      void download.cleanup();
    });
    return reply
      .header('content-type', download.contentType)
      .header('content-disposition', `attachment; filename="${download.filename}"`)
      .header('x-export-rows', String(download.rows))
      .send(download.stream);
  });

  /**
   * A run's progress, as server-sent events.
   *
   * The current snapshot goes out at once, so a client that connects mid-run — after a page
   * refresh, say — is never blank; then every change until the run ends, when the stream closes.
   */
  app.get('/api/stream/runs/:runId', { preHandler: permitAnywhere('data.query') }, async (request, reply: FastifyReply) => {
    const run = ownRun(request);

    reply.raw.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    const send = (payload: unknown): void => {
      try {
        reply.raw.write(`data: ${JSON.stringify(payload)}\n\n`);
      } catch (cause) {
        logger.debug({ err: cause, runId: run.id }, 'run stream write failed');
      }
    };

    send(run.snapshot());
    if (run.isTerminal()) {
      reply.raw.end();
      return reply;
    }

    const heartbeat = setInterval(() => {
      try {
        reply.raw.write(': ping\n\n');
      } catch {
        /* the close handler cleans up */
      }
    }, 15_000);
    const unsubscribe = run.subscribe((snapshot) => {
      run.touch();
      send(snapshot);
      if (snapshot.state !== 'running') {
        stop();
        reply.raw.end();
      }
    });
    const stop = (): void => {
      unsubscribe();
      clearInterval(heartbeat);
    };
    request.raw.on('close', stop);
    return reply;
  });
}
