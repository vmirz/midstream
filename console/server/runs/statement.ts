/**
 * Statement runs: hand-written SQL over the materialization's tables.
 *
 * Each table is read by its canonical name, dataset and table (`SELECT * FROM sales.orders`), with
 * flat logical columns; `iceberg.sales.orders` is the same rows as the native Iceberg table, structs
 * and lists as they are. A statement may read
 * any of the materialization's tables, joining them as it likes; only the tables it names are
 * planned, pinned and opened. The time range applies to every one — they hold only the rows inside
 * the range, and data files outside it are never opened — so narrowing the range makes any
 * statement cheaper without the statement having to say so, the way a Splunk search is bounded by
 * its time picker.
 *
 * The result is written to local Parquet as it is produced, which is what makes paging it instant
 * and exporting it exact: every page and every export format reads the same stored answer rather
 * than running the statement again. Results beyond `resultRowLimit` are not kept — a `SELECT *`
 * over a wide range would otherwise copy the dataset onto the console's disk — but the total is
 * always counted in full, so a run never ends without its exact row count.
 *
 * A table is read with its records past the snapshot too, from its live tail: the tail's files sit in
 * the run's directory, which the sandbox already allows, and are read by field id like data files,
 * so `sales.orders` and `iceberg.sales.orders` both hold them. A statement gets its tail once, when
 * it runs; a refresh of one is a new run.
 */

import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';

import { config } from '../config.ts';
import { ConsoleError } from '../errors.ts';
import { parseDuckDbError, readAll, statementProgress, withConnection, withSandbox } from '../data/duckdb.ts';
import { assertWithinCachedBudget, objectSources } from '../data/objects.ts';
import { overlapsRange, type Catalog } from '../data/catalog.ts';
import { quoteLiteral } from '../data/sql.ts';
import { Run, tableRead, type RunInit } from './run.ts';
import { ROWS_VIEW, statementViews, type StatementTable } from './view.ts';
import type { LogicalColumn } from '../data/logical.ts';
import type { DuckDBConnection } from '@duckdb/node-api';
import type { RunPage, RunTableRead } from '../../shared/types.ts';

/** One table a statement reads, at the snapshot it is pinned to. */
export interface StatementSource {
  /** The table's dataset: its namespace in SQL. */
  namespace: string;
  name: string;
  catalog: Catalog;
  columns: LogicalColumn[];
}

export interface StatementInit extends RunInit {
  statement: string;
  /** The tables the statement names, in the materialization's order; it may name none. */
  sources: StatementSource[];
  /** Every table of the materialization by its canonical name, so an error can say which there are. */
  tableNames: string[];
}

const PROGRESS_INTERVAL_MS = 300;

export class StatementRun extends Run {
  private readonly statement: string;
  private readonly sources: StatementSource[];
  private readonly tableNames: string[];
  /** In the run's own directory, which its sandbox may write and nothing else, so no run sees another's result. */
  private readonly resultPath: string;

  constructor(init: StatementInit) {
    super(init);
    this.statement = init.statement;
    this.sources = init.sources;
    this.tableNames = init.tableNames;
    this.resultPath = join(this.workDir, 'result.parquet');
  }

  protected tableReads(): RunTableRead[] {
    return this.sources.map((source) => tableRead(source.name, source.catalog, this.tails.get(source.name)));
  }

  protected async execute(): Promise<void> {
    await this.fetchTails(this.sources.map((source) => ({ name: source.name, catalog: source.catalog })));
    this.throwIfCancelled();
    const planned = this.sources.map((source) => ({
      source,
      objects: [...this.tailObjects(source.name), ...source.catalog.objects].filter((object) => overlapsRange(object, this.request.range)),
    }));
    const objects = planned.flatMap((entry) => entry.objects);
    let rowsTotal = 0;
    let bytes = 0;
    for (const object of objects) {
      rowsTotal += object.rows ?? 0;
      bytes += object.bytes;
    }
    this.scope = { objects: objects.length, bytes, rows: rowsTotal };
    this.progress = { rowsTotal, rowsDone: 0, objectsTotal: objects.length, objectsDone: 0, objectsRead: 0, bytesTotal: bytes, fraction: null };
    assertWithinCachedBudget(objects);
    this.changed(true);

    const release = await this.admit(this.signal);
    try {
      await mkdir(this.workDir, { recursive: true });
      const opened = await objectSources(objects);
      await this.executeStatement(
        planned.map(({ source, objects: tableObjects }) => ({
          namespace: source.namespace,
          name: source.name,
          sources: tableObjects.map((object) => opened.get(object.uri) ?? object.uri),
          schema: source.catalog.schema,
          columns: source.columns,
        })),
      );
    } finally {
      release();
    }
  }

  /**
   * Run the statement in a sandbox that can open only this run's data files and write only its
   * result: whatever a statement names, DuckDB refuses any other file or object (`withSandbox`).
   *
   * Every file is read through its table's schema, so the views bind without opening a footer;
   * DuckDB reads the footers as it scans, in parallel, and reports its progress as it goes.
   */
  private async executeStatement(tables: StatementTable[]): Promise<void> {
    this.phase = 'scanning';
    this.progress.fraction = null;
    this.changed(true);

    const access = { directories: [`${this.workDir}/`], paths: tables.flatMap((table) => table.sources) };
    await withSandbox(
      access,
      async (connection) => {
        for (const statement of statementViews(tables, this.request.range)) {
          await connection.run(statement);
        }

        // The statement starts on its own line, so an error on its line N is reported on line
        // N + 1 at the same column — a position the editor can mark exactly.
        const wrapped = `SELECT * FROM (\n${this.statement}\n) AS console_result`;
        const copy =
          `COPY (${wrapped} LIMIT ${config.resultRowLimit + 1}) TO ${quoteLiteral(this.resultPath)} ` +
          `(FORMAT PARQUET, COMPRESSION ZSTD)`;

        const timer = setInterval(() => {
          const fraction = statementProgress(connection);
          if (fraction === null) return;
          this.progress.fraction = fraction;
          this.progress.rowsDone = Math.round(fraction * this.progress.rowsTotal);
          this.changed();
        }, PROGRESS_INTERVAL_MS);
        try {
          await connection.run(copy);
        } catch (cause) {
          throw statementError(cause, this.tableNames);
        } finally {
          clearInterval(timer);
        }

        this.progress.fraction = 1;
        this.progress.rowsDone = this.progress.rowsTotal;
        this.progress.objectsDone = this.progress.objectsTotal;
        this.progress.objectsRead = this.progress.objectsTotal;
        const stored = await countRows(connection, this.resultPath);
        const described = await readAll(connection, `DESCRIBE SELECT * FROM read_parquet(${quoteLiteral(this.resultPath)})`);
        // A field selected as itself keeps its name, and with it the knowledge that it holds JSON text.
        const json = new Set(tables.flatMap((table) => table.columns.filter((column) => column.jsonText).map((column) => column.name)));
        this.columns = described.rows.map((row) => {
          const name = String(row.column_name);
          const type = String(row.column_type);
          return json.has(name) && /^VARCHAR/i.test(type) ? { name, type, json: true } : { name, type };
        });

        if (stored <= config.resultRowLimit) {
          this.rowsRetained = stored;
          this.rows = stored;
        } else {
          // Kept rows stop at the limit; the total does not. Counting reads only what the
          // statement's own projection needs, so it is far cheaper than the copy was.
          this.truncated = true;
          this.rowsRetained = config.resultRowLimit;
          this.phase = 'counting';
          this.progress.fraction = null;
          this.changed(true);
          const total = await readAll(connection, `SELECT count(*)::BIGINT AS n FROM (${wrapped})`);
          this.rows = Number(total.rows[0]?.n ?? stored);
        }
        this.rowsReady = this.rowsRetained;
        this.totalExact = true;
      },
      { signal: this.signal },
    );
  }

  /** One page of the stored result. Empty until the run has succeeded. */
  async readPage(offset: number, limit: number): Promise<RunPage> {
    this.touch();
    const end = Math.min(offset + limit, this.rowsReady);
    let rows: Record<string, unknown>[] = [];
    if (this.state === 'succeeded' && offset < end) {
      const result = await withConnection((connection) =>
        readAll(
          connection,
          `SELECT * EXCLUDE (file_row_number) FROM read_parquet(${quoteLiteral(this.resultPath)}, file_row_number = true) ` +
            `WHERE file_row_number BETWEEN ${offset} AND ${end - 1} ORDER BY file_row_number`,
        ),
      );
      rows = result.rows;
    }
    return {
      runId: this.id,
      offset,
      rows,
      columns: this.columns ?? [],
      complete: this.isTerminal(),
      total: this.rows,
      totalExact: this.totalExact,
    };
  }

  async readRows(offset: number, limit: number): Promise<Record<string, unknown>[]> {
    return (await this.readPage(offset, limit)).rows;
  }

  exportableRows(): number {
    return Math.min(this.rowsRetained ?? 0, config.exportRowLimit);
  }

  async rowsView(): Promise<string[]> {
    return [
      `CREATE OR REPLACE TEMP VIEW ${ROWS_VIEW} AS SELECT * FROM read_parquet(${quoteLiteral(this.resultPath)}) LIMIT ${this.rowsRetained ?? 0}`,
    ];
  }
}

async function countRows(connection: DuckDBConnection, path: string): Promise<number> {
  const result = await readAll(connection, `SELECT count(*)::BIGINT AS n FROM read_parquet(${quoteLiteral(path)})`);
  return Number(result.rows[0]?.n ?? 0);
}

/** A statement failure, with its position moved back onto the statement as the user wrote it. */
export function statementError(cause: unknown, tableNames: readonly string[]): ConsoleError {
  if (cause instanceof ConsoleError) return cause;
  const { message, position } = parseDuckDbError(cause, { tableNames });
  const own = position && position.line > 1 ? { line: position.line - 1, column: position.column } : null;
  return new ConsoleError('query_failed', message, 400, own ? { position: own } : {});
}
