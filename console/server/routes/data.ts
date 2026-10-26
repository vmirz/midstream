import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';

import { authIn, callerOf, domainOf, permit, permitAnywhere, permitInSomeDomain, requireOnDatasets, type DatasetRef } from '../auth/guard.ts';
import { config } from '../config.ts';
import { ConsoleError, badRequest } from '../errors.ts';
import { logger } from '../logger.ts';
import { estimateRange, loadCatalog, summarize } from '../data/catalog.ts';
import { fetchHead, fetchTail, readPeek } from '../data/tail.ts';
import { domains } from '../domains/index.ts';
import type { Domain } from '../domains/domain.ts';
import { openExport } from '../runs/export.ts';
import { profileColumn } from '../runs/profile.ts';
import { canonicalName, namedTables, nativeCatalog, unqualifiedReferences } from '../runs/names.ts';
import { getRun, pickTable, refreshRun, startRun, tablesRead, validateStatement, type ResolvedTable, type RunTableSource } from '../runs/store.ts';
import { listOf } from '../runs/view.ts';
import type { Run, RunDataset } from '../runs/run.ts';
import type { TailAccess } from '../runs/tail.ts';
import { materializationId, parseBody } from './materializations.ts';
import { filtersSchema } from './requestSchemas.ts';
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

/**
 * Who owns the runs a request starts or reads, in a domain: the person as that domain knows them, or
 * this guest address. A person has an account in each domain, so a run is theirs in its own domain.
 */
function ownerIn(request: FastifyRequest, domain: Domain): string {
  return `${domain.id}:${authIn(request, domain).state.principal.user_id ?? `guest:${request.ip}`}`;
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
async function tablesOf(domain: Domain, id: string): Promise<RunTableSource[]> {
  return (await materializationTables(domain, id)).tables;
}

async function materializationTables(domain: Domain, id: string): Promise<{ materialization: CpMaterialization; tables: RunTableSource[] }> {
  const detail = await domain.live.getDetail(id);
  const locations = new Map(detail.tables.map((table) => [table.name, table.location]));
  const tables = readableTables(detail.materialization).map((table) => ({
    domain: domain.id,
    materializationId: id,
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
async function tailAccess(request: FastifyRequest, domain: Domain, materialization: CpMaterialization): Promise<TailAccess> {
  const caller = callerOf(request, domain);
  const id = materialization.materialization_id;
  return {
    off: await domain.liveTailOff(materialization),
    fetch: (table, after, signal) =>
      fetchTail(domain, caller, { materializationId: id, table, after, maxRows: config.liveTailMaxRows, timeoutMs: config.liveTailTimeoutMs, signal }),
  };
}

/**
 * The tables a statement names, found in their domains (handbook 20): each `<domain>.<dataset>.<table>`
 * through its domain's live fleet to the materialization that writes it, and its location. Refused,
 * naming what to write instead: a table named without its domain, and a name no domain holds. The
 * caller must be allowed to query each table, in its dataset, as their domain knows them.
 */
async function resolveStatement(
  request: FastifyRequest,
  sql: string,
  runDomain: Domain,
  own: RunTableSource[],
): Promise<{ sources: ResolvedTable[]; tableNames: string[] }> {
  const served = domains().served();
  const unqualified = unqualifiedReferences(
    sql,
    runDomain.live.fleetSnapshot().items.flatMap((item) => item.tables.map((name) => splitName(name))),
  );
  if (unqualified.length > 0) {
    const fixes = unqualified.map((name) =>
      name.startsWith('iceberg.') ? `${nativeCatalog(runDomain.id)}.${name.slice('iceberg.'.length)} for ${name}` : `${runDomain.id}.${name} for ${name}`,
    );
    throw badRequest(`Name every table with its domain: write ${listOf(fixes)}.`, { field: 'sql' });
  }
  const sources: ResolvedTable[] = [];
  const seen = new Set<string>();
  for (const reference of namedTables(sql, served.map((domain) => domain.id))) {
    const key = canonicalName(reference.domain, reference.dataset, reference.table);
    if (seen.has(key)) continue;
    seen.add(key);
    const domain = domains().require(reference.domain);
    const qualified = `${reference.dataset}.${reference.table}`;
    const item = domain.live
      .fleetSnapshot()
      .items.find((candidate) => candidate.desired_phase !== 'deleted' && candidate.tables.some((name) => name.toLowerCase() === qualified));
    if (!item) {
      const inDataset = domain.live
        .fleetSnapshot()
        .items.flatMap((candidate) => candidate.tables)
        .filter((name) => name.toLowerCase().startsWith(`${reference.dataset}.`))
        .map((name) => `${domain.id}.${name}`);
      throw badRequest(
        inDataset.length > 0
          ? `There is no table ${key}; ${domain.id}.${reference.dataset} holds ${listOf(inDataset)}.`
          : `There is no table ${key}: ${domain.name} has no dataset ${reference.dataset} with tables.`,
        { field: 'sql' },
      );
    }
    const { materialization, tables } = await materializationTables(domain, item.materialization_id);
    const table = tables.find((candidate) => `${candidate.dataset}.${candidate.name}`.toLowerCase() === qualified);
    if (!table) throw badRequest(`There is no table ${key}.`, { field: 'sql' });
    sources.push({ ...table, tail: await tailAccess(request, domain, materialization) });
  }
  requireOnTables(request, 'data.query', sources);
  const tableNames = [...new Set([...own.map((table) => canonicalName(table.domain, table.dataset, table.name)), ...seen])];
  return { sources, tableNames };
}

/** `sales.orders` as its dataset and table. */
function splitName(name: string): { dataset: string; table: string } {
  const dot = name.indexOf('.');
  return { dataset: name.slice(0, dot), table: name.slice(dot + 1) };
}

/** The datasets of some tables, each once with its domain, as a refusal names them. */
function datasetsOf(tables: RunTableSource[]): RunDataset[] {
  return [...new Map(tables.map((table) => [`${table.domain}:${table.datasetId}`, { domain: table.domain, id: table.datasetId, name: table.dataset }])).values()];
}

/**
 * Refuse a caller who may not do `permission` with every one of `tables`, naming the first dataset
 * they lack it on: a statement over a table someone may not query is refused, whatever else it reads.
 */
function requireOnTables(request: FastifyRequest, permission: string, tables: RunTableSource[]): void {
  requireOnRunDatasets(request, permission, datasetsOf(tables));
}

/** Refuse a caller who may not do `permission` on each dataset, as they are in its domain. */
function requireOnRunDatasets(request: FastifyRequest, permission: string, datasets: readonly RunDataset[]): void {
  const byDomain = new Map<string, DatasetRef[]>();
  for (const dataset of datasets) byDomain.set(dataset.domain, [...(byDomain.get(dataset.domain) ?? []), { id: dataset.id, name: dataset.name }]);
  for (const [domainId, refs] of byDomain) {
    const domain = domains().require(domainId);
    requireOnDatasets(authIn(request, domain), permission, domains().size > 1 ? refs.map((ref) => ({ ...ref, name: `${domain.id}.${ref.name}` })) : refs);
  }
}

/** A run, for whoever started it, while they may still `permission` on every dataset it reads. */
function ownRun(request: FastifyRequest, permission = 'data.query'): Run {
  const run = getRun(runId(request), (domainId) => ownerIn(request, domains().require(domainId)));
  requireOnRunDatasets(request, permission, run.datasets);
  return run;
}

export async function registerDataRoutes(app: FastifyInstance): Promise<void> {
  /**
   * What one of a materialization's tables holds, from its manifests alone: counts and a time span,
   * never a row. The detail page's overview shows it, so it needs only what that page does.
   * `?table=` names the table (the first by default), `?asof=` an older snapshot of it.
   */
  app.get('/materializations/:materializationId/data/summary', { preHandler: permit('materializations.view_details') }, async (request) => {
    const id = materializationId(request);
    const query = parseBody(summarySchema, request.query ?? {});
    const table = pickTable(await tablesOf(domainOf(request), id), query.table);
    return summarize(await loadCatalog(id, table.location, { snapshotId: query.asof ?? null, table: table.name }));
  });

  /** What each time range would read of a table, before anything is read. Drives the time picker's figures. */
  app.post('/materializations/:materializationId/data/estimate', { preHandler: permitAnywhere('data.query') }, async (request) => {
    const id = materializationId(request);
    const body = parseBody(estimateSchema, request.body);
    const table = pickTable(await tablesOf(domainOf(request), id), body.table);
    requireOnTables(request, 'data.query', [table]);
    const catalog = await loadCatalog(id, table.location, { snapshotId: body.asOf ?? null, table: table.name });
    return { asOf: new Date(catalog.loadedAt).toISOString(), estimates: body.ranges.map((range) => estimateRange(catalog, range)) };
  });

  /** Bind a statement without running it, so the editor can mark an error while it is typed. */
  app.post('/materializations/:materializationId/data/validate', { preHandler: permitAnywhere('data.query') }, async (request) => {
    const id = materializationId(request);
    const body = parseBody(validateSchema, request.body);
    const domain = domainOf(request);
    const own = await tablesOf(domain, id);
    const { sources, tableNames } = await resolveStatement(request, body.sql, domain, own);
    await validateStatement({ sources, sql: body.sql, tableNames });
    return { ok: true };
  });

  /**
   * Run: the only way the console executes a query. Returns at once; progress streams. The caller
   * must be allowed to query every table it reads, each in its dataset.
   */
  app.post('/materializations/:materializationId/runs', { preHandler: permitAnywhere('data.query') }, async (request, reply) => {
    const id = materializationId(request);
    const body = parseBody(runSchema, request.body);
    const domain = domainOf(request);
    const { materialization, tables } = await materializationTables(domain, id);
    const named = body.kind === 'sql' ? await resolveStatement(request, body.sql, domain, tables) : null;
    requireOnTables(request, 'data.query', tablesRead(tables, body, named?.sources));
    const tail = await tailAccess(request, domain, materialization);
    const run = await startRun({
      domain: domain.id,
      materializationId: id,
      ownerId: ownerIn(request, domain),
      tables,
      request: body,
      tail,
      ...(named ? { sources: named.sources, tableNames: named.tableNames } : {}),
    });
    return reply.code(201).send(run.snapshot());
  });

  /**
   * The newest records on a table's topic, committed or not, in the table's columns: what the Data
   * tab shows while the table's runtime is too far behind for the tail (`tail.status: backlogged`).
   * Never part of a run, its export or its profiles.
   */
  app.get('/materializations/:materializationId/tables/:table/head', { preHandler: permitAnywhere('data.query') }, async (request): Promise<HeadPeek> => {
    const id = materializationId(request);
    const query = parseBody(headSchema, request.query ?? {});
    const domain = domainOf(request);
    const { materialization, tables } = await materializationTables(domain, id);
    const table = pickTable(tables, (request.params as { table?: string }).table ?? '');
    requireOnTables(request, 'data.query', [table]);
    const off = await domain.liveTailOff(materialization);
    if (off) throw new ConsoleError('live_tail_off', off.message, 404, { reason: off.reason });
    const outcome = await fetchHead(domain, callerOf(request), { materializationId: id, table: table.name, rows: query.rows, timeoutMs: config.liveTailTimeoutMs });
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
}

/**
 * Runs, addressed by id: each belongs to the person who started it, as the domain it was started in
 * knows them, and reading it needs `data.query` again on every dataset it reads, in each one's domain.
 */
export async function registerRunRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/runs/:runId', { preHandler: permitInSomeDomain('data.query') }, async (request) => ownRun(request).snapshot());

  /**
   * Bring a run up to date: the live tail's newest records appended to it, or a new run in its place
   * when the table has a newer snapshot or the run is a statement. The Data tab's Live mode calls it
   * every few seconds for a browse run; a statement's Auto re-run gets a new run each time.
   */
  app.post('/api/runs/:runId/refresh', { preHandler: permitInSomeDomain('data.query') }, async (request): Promise<RunRefresh> => {
    const run = ownRun(request);
    const body = parseBody(refreshSchema, request.body ?? {});
    const domain = domains().require(run.domain);
    const { materialization, tables } = await materializationTables(domain, run.materializationId);
    // A run that replaces this one reads what it names now, in each domain as it is now.
    const named = run.request.kind === 'sql' ? await resolveStatement(request, run.request.sql ?? '', domain, tables) : null;
    requireOnTables(request, 'data.query', tablesRead(tables, run.request, named?.sources));
    const outcome = await refreshRun({
      run,
      ownerId: ownerIn(request, domain),
      tables,
      range: body.range ?? null,
      tail: await tailAccess(request, domain, materialization),
      ...(named ? { sources: named.sources, tableNames: named.tableNames } : {}),
    });
    return { replaced: outcome.replaced, run: outcome.run.snapshot() };
  });

  app.post('/api/runs/:runId/cancel', { preHandler: permitInSomeDomain('data.query') }, async (request) => {
    // Anyone may stop their own run, whatever they may still read.
    const run = getRun(runId(request), (domainId) => ownerIn(request, domains().require(domainId)));
    run.cancel();
    return run.snapshot();
  });

  app.get('/api/runs/:runId/rows', { preHandler: permitInSomeDomain('data.query') }, async (request) => {
    const run = ownRun(request);
    const query = parseBody(rowsSchema, request.query ?? {});
    return run.readPage(query.offset, Math.min(query.limit, config.pageRowLimit));
  });

  app.post('/api/runs/:runId/profile', { preHandler: permitInSomeDomain('data.query') }, async (request) => {
    const run = ownRun(request);
    return profileColumn(run, parseBody(profileSchema, request.body).column);
  });

  /** An export needs `data.export` on every dataset the run read. */
  app.get('/api/runs/:runId/export', { preHandler: permitInSomeDomain('data.export') }, async (request, reply: FastifyReply) => {
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
  app.get('/api/stream/runs/:runId', { preHandler: permitInSomeDomain('data.query') }, async (request, reply: FastifyReply) => {
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
