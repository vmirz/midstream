/**
 * Exporting a finished run.
 *
 * An export is the run's result — every row it matched, in the order it was shown — never the page
 * on screen and never a re-execution. It is offered only once a run has succeeded, because only
 * then is there a complete answer to hand over.
 *
 * Text formats stream: rows are read and written a page at a time, so the browser's download
 * starts at once and memory stays flat however large the export. Parquet cannot stream — its
 * footer comes last — so it is written to a temporary file by DuckDB first.
 *
 * JSON and JSON Lines come in two shapes. `flat` writes one key per column, as the grid shows it.
 * `nested` writes each row as the record it came from — `customer.email` and `items[].sku` become
 * `{ "customer": { "email": … }, "items": [ { "sku": … } ] }` — with `json` fields decoded.
 */

import { createReadStream } from 'node:fs';
import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { Readable } from 'node:stream';

import { config } from '../config.ts';
import { badRequest } from '../errors.ts';
import { logger } from '../logger.ts';
import { withConnection } from '../data/duckdb.ts';
import { quoteLiteral } from '../data/sql.ts';
import { rowNester } from '../../shared/fieldPaths.ts';
import { ROWS_VIEW } from './view.ts';
import type { Run } from './run.ts';
import type { RunExportFormat, RunExportShape } from '../../shared/types.ts';

const EXPORT_PAGE_ROWS = 5_000;

const FORMATS: Record<RunExportFormat, { extension: string; contentType: string }> = {
  csv: { extension: 'csv', contentType: 'text/csv; charset=utf-8' },
  json: { extension: 'json', contentType: 'application/json; charset=utf-8' },
  jsonl: { extension: 'jsonl', contentType: 'application/x-ndjson; charset=utf-8' },
  parquet: { extension: 'parquet', contentType: 'application/vnd.apache.parquet' },
};

export interface RunExport {
  stream: Readable;
  contentType: string;
  filename: string;
  rows: number;
  /** Called when the response finishes, to remove any temporary file. */
  cleanup: () => Promise<void>;
}

export async function openExport(run: Run, format: RunExportFormat, shape: RunExportShape = 'flat'): Promise<RunExport> {
  if (run.state !== 'succeeded') {
    throw badRequest('a run can be exported once it has finished', { state: run.state });
  }
  const spec = FORMATS[format];
  if (!spec) throw badRequest(`unsupported export format "${format}"`);

  // The rows stay as they are while the export reads them: a live tail's refresh waits.
  const release = run.holdRows();
  const rows = run.exportableRows();
  const filename = `${run.materializationId}-${run.id.slice(0, 8)}.${spec.extension}`;

  if (format === 'parquet') {
    try {
      await mkdir(config.runDir, { recursive: true });
      const path = join(config.runDir, `${run.id}-export-${Date.now()}.parquet`);
      const setup = await run.rowsView();
      await withConnection(async (connection) => {
        for (const statement of setup) await connection.run(statement);
        await connection.run(
          `COPY (SELECT * FROM ${ROWS_VIEW} LIMIT ${rows}) TO ${quoteLiteral(path)} (FORMAT PARQUET, COMPRESSION ZSTD)`,
        );
      });
      return {
        stream: createReadStream(path),
        contentType: spec.contentType,
        filename,
        rows,
        cleanup: async () => {
          await rm(path, { force: true }).catch((cause) => logger.debug({ err: cause }, 'export file already removed'));
        },
      };
    } finally {
      // The file holds every row it will send.
      release();
    }
  }

  const columns = (run.columns ?? []).map((column) => column.name);
  const layout =
    shape === 'nested' && format !== 'csv' ? rowNester(columns, { jsonColumns: run.jsonTextColumns() }) : null;
  return {
    stream: Readable.from(textExport(run, format, columns, rows, layout, release)),
    contentType: spec.contentType,
    filename,
    rows,
    cleanup: async () => release(),
  };
}

async function* textExport(
  run: Run,
  format: Exclude<RunExportFormat, 'parquet'>,
  columns: string[],
  rows: number,
  layout: ((row: Record<string, unknown>) => Record<string, unknown>) | null,
  release: () => void = () => undefined,
): AsyncGenerator<string> {
  try {
    yield* textRows(run, format, columns, rows, layout);
  } finally {
    release();
  }
}

async function* textRows(
  run: Run,
  format: Exclude<RunExportFormat, 'parquet'>,
  columns: string[],
  rows: number,
  layout: ((row: Record<string, unknown>) => Record<string, unknown>) | null,
): AsyncGenerator<string> {
  if (format === 'csv') yield `${columns.map(csvCell).join(',')}\n`;
  if (format === 'json') yield '[';

  let written = 0;
  while (written < rows) {
    const page = await run.readRows(written, Math.min(EXPORT_PAGE_ROWS, rows - written));
    if (page.length === 0) break;
    const lines = page.map((row, index) => {
      switch (format) {
        case 'csv':
          return `${columns.map((column) => csvCell(row[column])).join(',')}\n`;
        case 'jsonl':
          return `${JSON.stringify(layout ? layout(row) : row)}\n`;
        case 'json':
          return `${written + index === 0 ? '\n' : ',\n'}${JSON.stringify(layout ? layout(row) : row)}`;
      }
    });
    yield lines.join('');
    written += page.length;
  }

  if (format === 'json') yield written === 0 ? ']\n' : '\n]\n';
}

/** RFC 4180: quote anything holding a quote, comma or line break, doubling inner quotes. */
export function csvCell(value: unknown): string {
  if (value === null || value === undefined) return '';
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}
