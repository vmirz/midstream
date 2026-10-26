/**
 * The registry of runs, and the only way one is created.
 *
 * Runs live in this process, which matches the console's single-replica deployment: a run's
 * counts, stored result and listeners are all here. Nothing outside this module decides how a
 * query executes — `startRun` looks at what was asked and picks the run kind.
 */

import { config } from '../config.ts';
import { badRequest, notFound } from '../errors.ts';
import { logger } from '../logger.ts';
import { cancelledError, withSandbox } from '../data/duckdb.ts';
import { loadCatalog, pauseWarming, type Catalog } from '../data/catalog.ts';
import { logicalColumns, resolveFilters } from '../data/logical.ts';
import { assertReadOnlyStatement } from '../data/sql.ts';
import { BrowseRun } from './browse.ts';
import { StatementRun, statementError, type StatementSource } from './statement.ts';
import { assertNativeReferences, listOf, qualifiedName, referencedTables, statementViews } from './view.ts';
import type { Run, RunDataset } from './run.ts';
import type { TailAccess } from './tail.ts';
import type { RunRequest, TimeRange } from '../../shared/types.ts';

const runs = new Map<string, Run>();

/**
 * Admission for scanning, so a burst of runs queues rather than saturating DuckDB.
 *
 * Only the scanning phase is admitted. A run the manifests answer on their own — the first page of
 * any table, a time range over whole files — never waits behind someone else's scan. Background
 * footer warming waits while any run is admitted.
 */
let scanning = 0;
const waiting: Array<{ grant: () => void; signal: AbortSignal; reject: (cause: unknown) => void }> = [];

function admit(signal: AbortSignal): Promise<() => void> {
  if (signal.aborted) return Promise.reject(cancelledError());
  const grant = (): (() => void) => {
    scanning += 1;
    const resumeWarming = pauseWarming();
    return once(() => {
      scanning -= 1;
      resumeWarming();
      pump();
    });
  };
  if (scanning < config.runConcurrency) return Promise.resolve(grant());
  return new Promise((resolve, reject) => {
    const entry = {
      signal,
      reject,
      grant: () => resolve(grant()),
    };
    waiting.push(entry);
    signal.addEventListener(
      'abort',
      () => {
        const index = waiting.indexOf(entry);
        if (index >= 0) waiting.splice(index, 1);
        reject(cancelledError());
      },
      { once: true },
    );
  });
}

function pump(): void {
  while (scanning < config.runConcurrency && waiting.length > 0) {
    const next = waiting.shift();
    if (next && !next.signal.aborted) next.grant();
  }
}

function once(fn: () => void): () => void {
  let called = false;
  return () => {
    if (called) return;
    called = true;
    fn();
  };
}

/** One of a materialization's tables, as the control plane describes it. */
export interface RunTableSource {
  /** The domain it is in (handbook 20). */
  domain: string;
  /** Its name in its dataset: SQL reads it as `<dataset>.<name>`, and `iceberg.<dataset>.<name>`. */
  name: string;
  /** The dataset it is in: its namespace in SQL. */
  dataset: string;
  datasetId: string;
  /** Its location; null before the control plane has created it. */
  location: string | null;
}

export interface StartRunInput {
  /** The domain the run is started in: its materialization's. */
  domain: string;
  materializationId: string;
  /** Who is starting the run, as the domain knows them. Nobody else can read it. */
  ownerId: string;
  /** Every table of the materialization, in its order. */
  tables: RunTableSource[];
  request: RunRequest;
  /**
   * How the run reads its tables' live tails. A request with `liveTail: false`, or `asOf` an older
   * snapshot, reads none whatever this says.
   */
  tail?: TailAccess | null;
}

/**
 * The tables a run would read: the one it browses, or the statement's own and every table the
 * statement names. Whoever starts it must be allowed to query each one, in its dataset.
 */
export function tablesRead(tables: RunTableSource[], request: Pick<RunRequest, 'kind' | 'table' | 'sql'>): RunTableSource[] {
  const selected = pickTable(tables, request.table);
  if (request.kind !== 'sql') return [selected];
  const named = new Set(referencedTables(request.sql ?? '', tables.map((table) => table.name)));
  return tables.filter((table) => table === selected || named.has(table.name));
}

/** The tables a statement names, in the materialization's order. */
export function tablesNamed(tables: RunTableSource[], sql: string): RunTableSource[] {
  const named = new Set(referencedTables(sql, tables.map((table) => table.name)));
  return tables.filter((table) => named.has(table.name));
}

/** The datasets of some tables, each once, in their domains. */
function datasetsOf(tables: RunTableSource[]): RunDataset[] {
  const byId = new Map(tables.map((table) => [`${table.domain}:${table.datasetId}`, { domain: table.domain, id: table.datasetId, name: table.dataset }]));
  return [...byId.values()];
}

/**
 * Create a run, start it, and return it at once; progress arrives through `subscribe`.
 *
 * A browse run reads its table (`request.table`, the materialization's first by default). A
 * statement reads every table it names, each pinned to its current snapshot — the request's table
 * to `asOf` when it names one — and no other.
 */
export async function startRun(input: StartRunInput): Promise<Run> {
  validateRange(input.request);
  const selected = pickTable(input.tables, input.request.table);
  let statement: string | null = null;
  if (input.request.kind === 'sql') {
    statement = assertReadOnlyStatement(input.request.sql ?? '');
    assertNativeReferences(statement, input.tables);
  }
  const datasets = datasetsOf(tablesRead(input.tables, input.request));

  const catalog = await loadCatalog(input.materializationId, selected.location, {
    fresh: true,
    snapshotId: input.request.asOf ?? null,
    table: selected.name,
  });
  assertReadable(catalog);
  const columns = logicalColumns(catalog.schema, catalog.forkMap);
  // A run as of an older snapshot is about the past: what arrived since is not part of it.
  const tail = input.request.asOf || input.request.liveTail === false ? null : (input.tail ?? null);

  if (statement === null) {
    // The run echoes its request back, so it carries the filters as applied: a link that still
    // names `payload.status` gets a run, and a request, about `status`.
    const request = { ...input.request, table: selected.name, filters: resolveFilters(input.request.filters ?? [], columns) };
    return register(
      new BrowseRun({
        domain: input.domain,
        materializationId: input.materializationId,
        ownerId: input.ownerId,
        tableName: selected.name,
        datasets,
        request,
        catalog,
        columns,
        admit,
        tail,
      }),
    );
  }

  const sources: StatementSource[] = await Promise.all(
    tablesNamed(input.tables, statement).map(async (table): Promise<StatementSource> => {
      if (table === selected) return { namespace: table.dataset, name: table.name, catalog, columns };
      const other = await loadCatalog(input.materializationId, table.location, { fresh: true, table: table.name });
      assertReadable(other);
      return { namespace: table.dataset, name: table.name, catalog: other, columns: logicalColumns(other.schema, other.forkMap) };
    }),
  );
  // The run is about the request's table when the statement reads it, else the first it reads.
  const primary =
    sources.find((source) => source.name === selected.name) ?? sources[0] ?? { namespace: selected.dataset, name: selected.name, catalog, columns };
  return register(
    new StatementRun({
      domain: input.domain,
      materializationId: input.materializationId,
      ownerId: input.ownerId,
      tableName: primary.name,
      datasets,
      request: { ...input.request, table: selected.name },
      catalog: primary.catalog,
      columns: primary.columns,
      admit,
      tail,
      statement,
      sources,
      tableNames: input.tables.map((table) => qualifiedName(table.dataset, table.name)),
    }),
  );
}

export interface RefreshRunInput {
  run: Run;
  /** Who is refreshing: the run's owner, as `getRun` checked. A run that replaces it is theirs too. */
  ownerId: string;
  /** Every table of the materialization, in its order, as it is now. */
  tables: RunTableSource[];
  /** The run's range resolved again, for a run that replaces it: a relative range slides. */
  range?: TimeRange | null;
  /** How the refreshed run reads its tail now: the person's current session, the switches as they are. */
  tail: TailAccess | null;
}

/**
 * Bring a run up to date: append the live tail's newest records to it, or start a run in its place.
 *
 * A browse run whose table is still at the snapshot it is pinned to only fetches the records past
 * its tail's `through`, so its rows grow and every row read before keeps its identity. A run is
 * replaced — the same request, its range resolved again — when a commit has landed since it
 * started, when it failed or was cancelled, when it is a statement (whose result is materialized),
 * or when its tail was off on this side when it started and is on now. Nothing else replaces one:
 * a run on the same snapshot would fetch the same tail, so a tail that cannot be had stays the
 * run's status until the table's next commit. A run as of an older snapshot, or still running, is
 * answered as it is. The run a new one replaces expires within a minute unless it is still read.
 */
export async function refreshRun(input: RefreshRunInput): Promise<{ replaced: boolean; run: Run }> {
  const { run } = input;
  const replace = async (): Promise<{ replaced: true; run: Run }> => {
    const request = input.range ? { ...run.request, range: input.range } : run.request;
    const next = await startRun({
      domain: run.domain,
      materializationId: run.materializationId,
      ownerId: input.ownerId,
      tables: input.tables,
      request,
      tail: input.tail,
    });
    run.expireWithin(REPLACED_RUN_GRACE_MS);
    return { replaced: true, run: next };
  };
  if (run.request.asOf) return { replaced: false, run };
  if (run.state === 'running') return { replaced: false, run };
  if (run.request.kind === 'sql' || run.state !== 'succeeded' || !(run instanceof BrowseRun)) return replace();

  const table = pickTable(input.tables, run.tableName);
  const catalog = await loadCatalog(run.materializationId, table.location, { fresh: true, table: table.name });
  if ((catalog.snapshot?.snapshotId ?? null) !== run.snapshotId) return replace();

  const tail = run.tailOf();
  if (!tail) return { replaced: false, run };
  const access = input.tail;
  if (access === null || access.off) {
    if (!tail.stopped) tail.turnOff(access?.off?.reason ?? 'off', access?.off?.message ?? 'Live tail is off.');
    return { replaced: false, run };
  }
  // Switched off when the run started or since, on now: only a new run fetches from its snapshot again.
  if (tail.switchedOff) return replace();
  await run.refreshTail(access);
  return { replaced: false, run };
}

/** How long a run another replaced stays, unless it is read: an export of it may still be going. */
const REPLACED_RUN_GRACE_MS = 60_000;

function register(run: Run): Run {
  runs.set(run.id, run);
  void run.start();
  return run;
}

/** The table a request names, compared without regard to case; the first table when it names none. */
export function pickTable(tables: RunTableSource[], requested: string | null | undefined): RunTableSource {
  if (tables.length === 0) throw badRequest('this materialization has no tables yet', { field: 'table' });
  if (!requested) return tables[0] as RunTableSource;
  const wanted = requested.trim().toLowerCase();
  const table = tables.find((candidate) => candidate.name.toLowerCase() === wanted);
  if (!table) {
    throw badRequest(`there is no table ${requested}; this materialization's tables are ${listOf(tables.map((candidate) => candidate.name))}`, {
      field: 'table',
    });
  }
  return table;
}

/**
 * Refuse a snapshot holding delete files, or data files outside the table's location.
 *
 * The materializer only ever appends and compacts, but an engine with write access to the catalog
 * can commit row-level deletes. The console reads data files directly, so it would show deleted
 * rows; it says so instead. A data file outside the location is one the console never reads
 * (`iceberg/io.ts::isUnderLocation`), so a run without it would be missing rows.
 */
function assertReadable(catalog: Catalog): void {
  // A statement may read several tables: say which one cannot be read.
  const which = catalog.table ? `Table ${catalog.table}: this snapshot` : 'This snapshot';
  const deletes = catalog.snapshot?.deleteFiles ?? 0;
  if (deletes > 0) {
    throw badRequest(
      `${which} holds ${deletes.toLocaleString('en-US')} delete files committed by another engine, which the console ` +
        `cannot apply. Query the table with an engine that reads Iceberg deletes (Table › Connect).`,
      { deleteFiles: deletes },
    );
  }
  const outside = catalog.snapshot?.outsideFiles ?? 0;
  if (outside > 0) {
    throw badRequest(
      `${which} holds ${outside.toLocaleString('en-US')} data files outside the table's location, committed by another ` +
        `engine; the console reads only files under ${catalog.location ?? 'the table'}. Query the table with an engine ` +
        `(Table › Connect).`,
      { outsideFiles: outside },
    );
  }
}

/**
 * Bind a statement without running it, so the editor can mark an error while it is being typed.
 *
 * Binding needs the views' columns, not their rows, and every column's type is in its table's
 * schema, so the views of the tables the statement names are built over no files at all: typed and
 * empty. The sandbox may open nothing, so a statement naming a file is refused at bind time as it
 * would be at run time.
 */
export async function validateStatement(input: { materializationId: string; tables: RunTableSource[]; sql: string }): Promise<void> {
  const statement = assertReadOnlyStatement(input.sql);
  assertNativeReferences(statement, input.tables);
  const views = await Promise.all(
    tablesNamed(input.tables, statement).map(async (table) => {
      const catalog = await loadCatalog(input.materializationId, table.location, { table: table.name });
      return {
        namespace: table.dataset,
        name: table.name,
        sources: [],
        schema: catalog.schema,
        columns: logicalColumns(catalog.schema, catalog.forkMap),
      };
    }),
  );

  await withSandbox({ directories: [], paths: [] }, async (connection) => {
    for (const setup of statementViews(views, { from: null, to: null })) {
      await connection.run(setup);
    }
    try {
      // On its own line, so an error's line number is the statement's line plus one.
      await connection.run(`PREPARE console_bind_check AS\n${statement}`);
    } catch (cause) {
      throw statementError(cause, input.tables.map((table) => qualifiedName(table.dataset, table.name)));
    }
  });
}

function validateRange(request: RunRequest): void {
  const { from, to } = request.range;
  if (from !== null && to !== null && to <= from) {
    throw badRequest('the time range ends before it starts', { field: 'range' });
  }
}

/**
 * A run, for the person who started it. Anyone else is told it does not exist: a run holds query
 * results, and its id is no secret once it is in a shared link. `ownerId` is who is asking - as the
 * run's domain knows them, when it is a function of that domain.
 */
export function getRun(runId: string, ownerId: string | ((domain: string) => string)): Run {
  const run = runs.get(runId);
  if (!run || run.ownerId !== (typeof ownerId === 'function' ? ownerId(run.domain) : ownerId)) {
    throw notFound(`run ${runId} has expired or never existed`, { resource: 'run', id: runId });
  }
  run.touch();
  return run;
}

/** Discard runs nobody has looked at for `runIdleRetentionMs`, with whatever they stored. */
export async function evictIdleRuns(now: number = Date.now()): Promise<number> {
  let evicted = 0;
  for (const [id, run] of runs) {
    if (now - run.lastAccess < config.runIdleRetentionMs) continue;
    runs.delete(id);
    await run.dispose();
    evicted += 1;
  }
  if (evicted > 0) logger.debug({ evicted }, 'idle runs discarded');
  return evicted;
}

/** Test seam. */
export async function resetRuns(): Promise<void> {
  for (const run of runs.values()) await run.dispose();
  runs.clear();
  waiting.length = 0;
  scanning = 0;
}
