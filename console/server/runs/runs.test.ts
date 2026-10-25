/**
 * The run engine, against real DuckDB and real Parquet written the way the runtime writes it.
 *
 * Every answer is checked against a brute-force query over all the files, so the tests assert the
 * property that matters — the run returns exactly the rows a full scan would, in the stated order —
 * rather than the mechanics of how it avoided the full scan.
 */

import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, describe, it } from 'node:test';

const dataDir = await mkdtemp(join(tmpdir(), 'console-runs-'));
process.env.CONSOLE_DATA_DIR = dataDir;
process.env.CONSOLE_RESULT_ROW_LIMIT = '50';
process.env.CONSOLE_RUN_SCAN_CONCURRENCY = '3';

const { runQuery } = await import('../data/duckdb.ts');
const { insideRange, overlapsRange, estimateRange, summarize } = await import('../data/catalog.ts');
const { logicalColumns, renderFilter, resolveFilters, selectList, timePredicate } = await import('../data/logical.ts');
const { BrowseRun, orderObjects } = await import('./browse.ts');
const { StatementRun } = await import('./statement.ts');
const { assertNativeReferences, referencedTables } = await import('./view.ts');
const { openExport } = await import('./export.ts');
const { profileColumn } = await import('./profile.ts');

import type { Catalog, DataObject } from '../data/catalog.ts';
import type { IcebergField, IcebergSchema } from '../iceberg/metadata.ts';
import type { ForkEntry } from '../iceberg/schema.ts';
import type { RunRequest, TimeRange } from '../../shared/types.ts';
import type { Run } from './run.ts';

const T0 = Date.parse('2026-01-01T00:00:00.000Z');
const minutes = (count: number): number => T0 + count * 60_000;

const METADATA_FIELDS: IcebergField[] = [
  { id: 1, name: '_kafka_timestamp', required: false, type: 'timestamptz' },
  { id: 2, name: '_kafka_partition', required: true, type: 'int' },
  { id: 3, name: '_kafka_offset', required: true, type: 'long' },
  { id: 4, name: '_kafka_key', required: false, type: 'string' },
  {
    id: 5,
    name: '_kafka_headers',
    required: false,
    type: { type: 'map', 'key-id': 8, key: 'string', 'value-id': 9, value: 'string', 'value-required': false },
  },
  { id: 6, name: '_materialized_at', required: true, type: 'timestamptz' },
  { id: 7, name: '_unmapped', required: false, type: 'string' },
];

/** `total` forked from long to double; `note` was added but no file has been written with it yet. */
const schema: IcebergSchema = {
  schemaId: 2,
  fields: [
    ...METADATA_FIELDS,
    { id: 10, name: 'id', required: false, type: 'string' },
    { id: 11, name: 'total', required: false, type: 'long' },
    { id: 12, name: 'status', required: false, type: 'string' },
    {
      id: 13,
      name: 'items',
      required: false,
      type: {
        type: 'list',
        'element-id': 14,
        element: { type: 'struct', fields: [{ id: 15, name: 'sku', required: false, type: 'string' }] },
        'element-required': false,
      },
    },
    { id: 16, name: 'customer', required: false, type: { type: 'struct', fields: [{ id: 17, name: 'email', required: false, type: 'string' }] } },
    { id: 18, name: 'note', required: false, type: 'string' },
    { id: 19, name: 'total__v2', required: false, type: 'double' },
  ],
};

function fork(fieldId: number, path: string[], forkVersion: number, state: ForkEntry['state'], forkOf: number | null = null): [number, ForkEntry] {
  return [fieldId, { fieldId, path, key: null, forkOf, forkVersion, state }];
}

const forkMap = new Map<number, ForkEntry>([fork(11, ['total'], 1, 'superseded'), fork(19, ['total__v2'], 2, 'current', 11)]);

interface Fixture {
  name: string;
  partition: number;
  offsetStart: number;
  /** Minutes after T0 of the first row; each row is one minute later. Null writes no timestamps. */
  startMinute: number | null;
  /** Which fork carries `total`. */
  totalColumn: 'total' | 'total__v2';
  /** Written before `customer` existed. */
  withCustomer: boolean;
  committedAt: number;
}

const fixtures: Fixture[] = [
  { name: 'a', partition: 0, offsetStart: 0, startMinute: 0, totalColumn: 'total', withCustomer: false, committedAt: minutes(200) },
  { name: 'b', partition: 1, offsetStart: 0, startMinute: 60, totalColumn: 'total__v2', withCustomer: true, committedAt: minutes(210) },
  { name: 'c', partition: 0, offsetStart: 100, startMinute: 100, totalColumn: 'total__v2', withCustomer: true, committedAt: minutes(220) },
  { name: 'legacy', partition: 2, offsetStart: 0, startMinute: null, totalColumn: 'total', withCustomer: true, committedAt: minutes(5) },
];

const FIELD_IDS: Record<string, string> = {
  _kafka_timestamp: '1',
  _kafka_partition: '2',
  _kafka_offset: '3',
  _kafka_key: '4',
  _kafka_headers: '{__duckdb_field_id: 5, key: 8, value: 9}',
  _materialized_at: '6',
  _unmapped: '7',
  id: '10',
  total: '11',
  status: '12',
  items: '{__duckdb_field_id: 13, element: {__duckdb_field_id: 14, sku: 15}}',
  customer: '{__duckdb_field_id: 16, email: 17}',
  total__v2: '19',
};

const ROWS = 100;
const catalogObjects: DataObject[] = [];
let catalog: Catalog;
const columns = logicalColumns(schema, forkMap);
const admit = async (): Promise<() => void> => () => undefined;

before(async () => {
  for (const fixture of fixtures) {
    const path = join(dataDir, `${fixture.name}.parquet`);
    const timestamp =
      fixture.startMinute === null
        ? 'NULL::TIMESTAMPTZ'
        : `TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_minutes(${fixture.startMinute} + i)`;
    const total = fixture.totalColumn === 'total' ? 'i::BIGINT' : '(i + 0.5)::DOUBLE';
    const selected: Array<[string, string]> = [
      ['_kafka_timestamp', timestamp],
      ['_kafka_partition', `${fixture.partition}::INTEGER`],
      ['_kafka_offset', `(${fixture.offsetStart} + i)::BIGINT`],
      ['_kafka_key', `'k-' || i`],
      ['_kafka_headers', fixture.name === 'c' ? `MAP {'trace': 'abc'}` : 'NULL::MAP(VARCHAR, VARCHAR)'],
      ['_materialized_at', `TIMESTAMPTZ '2026-10-07 00:00:00+00'`],
      ['_unmapped', `CASE WHEN i = 3 THEN '{"extra":1}' END`],
      ['id', `${quote(fixture.name)} || '-' || i`],
      [fixture.totalColumn, total],
      ['status', `['new', 'shipped', 'cancelled'][(i % 3) + 1]`],
      ['items', `[{'sku': 'SKU-' || i}, {'sku': 'SKU-X'}]`],
      ...(fixture.withCustomer ? ([['customer', `{'email': ${quote(fixture.name)} || '-' || i || '@x'}`]] as Array<[string, string]>) : []),
    ];
    const ids = selected.map(([name]) => `${name}: ${FIELD_IDS[name]}`).join(', ');
    // Row groups of 25 rows, so reads land on row-group boundaries the way large files do.
    await runQuery(
      `COPY (SELECT ${selected.map(([name, value]) => `${value} AS ${name}`).join(', ')} FROM range(${ROWS}) t(i)) ` +
        `TO ${quote(path)} (FORMAT PARQUET, ROW_GROUP_SIZE 25, FIELD_IDS {${ids}})`,
    );
    catalogObjects.push({
      uri: path,
      name: fixture.name,
      bytes: 1_000,
      rows: ROWS,
      committedAt: fixture.committedAt,
      partition: fixture.partition,
      offsetStart: fixture.offsetStart,
      offsetEnd: fixture.offsetStart + ROWS - 1,
      timeMin: fixture.startMinute === null ? null : minutes(fixture.startMinute),
      timeMax: fixture.startMinute === null ? null : minutes(fixture.startMinute + ROWS - 1),
      timeKnown: true,
      partitionKey: null,
    });
  }
  catalog = {
    materializationId: 'mat-test',
    table: 'orders',
    location: dataDir,
    metadataLocation: join(dataDir, 'metadata', '00001.metadata.json'),
    schema,
    forkMap,
    snapshot: {
      tableUuid: 'uuid',
      snapshotId: '7',
      sequenceNumber: 4,
      committedAt: minutes(220),
      operation: 'append',
      schemaId: 2,
      objects: catalogObjects,
      deleteFiles: 0,
      outsideFiles: 0,
      summary: {},
      offsets: { '0': 199, '1': 99, '2': 99 },
      validThrough: new Date(minutes(159)).toISOString(),
    },
    currentSnapshotId: '7',
    objects: catalogObjects,
    reason: null,
    loadedAt: Date.now(),
  };
});

after(async () => {
  await rm(dataDir, { recursive: true, force: true });
});

function quote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function allFiles(): string {
  return `read_parquet([${catalogObjects.map((object) => quote(object.uri)).join(', ')}], union_by_name = true)`;
}

async function brute(where: string): Promise<number> {
  const result = await runQuery(`SELECT count(*)::BIGINT AS n FROM ${allFiles()} WHERE ${where}`);
  return Number(result.rows[0]?.n);
}

async function finished(run: Run): Promise<Run> {
  await run.start();
  return run;
}

function browse(request: Partial<RunRequest> = {}): InstanceType<typeof BrowseRun> {
  return new BrowseRun({
    materializationId: 'mat-test',
    ownerId: 'usr-test',
    tableName: 'orders',
    datasets: [{ id: 'ds-000000000001', name: 'shop' }],
    request: { kind: 'browse', range: { from: null, to: null }, ...request },
    catalog,
    columns,
    admit,
  });
}

function statement(
  sql: string,
  range: TimeRange = { from: null, to: null },
  tableName = 'orders',
): InstanceType<typeof StatementRun> {
  return new StatementRun({
    materializationId: 'mat-test',
    ownerId: 'usr-test',
    tableName,
    datasets: [{ id: 'ds-000000000001', name: 'shop' }],
    request: { kind: 'sql', range, sql },
    catalog,
    columns,
    admit,
    statement: sql,
    sources: [{ namespace: 'shop', name: tableName, catalog, columns }],
    tableNames: [`shop.${tableName}`],
  });
}

/** Every row of a run, a page at a time, the way a client pages through it. */
async function everyRow(run: Run, pageSize = 37): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for (let offset = 0; offset < run.rows; offset += pageSize) {
    rows.push(...(await run.readPage(offset, pageSize)).rows);
  }
  return rows;
}

describe('the first page of a table', () => {
  it('knows its exact total without reading anything', async () => {
    const run = browse();
    await finished(run);
    assert.equal(run.state, 'succeeded');
    assert.equal(run.rows, 4 * ROWS);
    assert.equal(run.totalExact, true);
    assert.equal(run.progress.objectsDone, 4, 'every file was counted from its manifest entry');
  });

  it('pages newest first: newest object, newest row', async () => {
    const run = browse();
    await finished(run);
    const page = await run.readPage(0, 3);
    assert.deepEqual(
      page.rows.map((row) => row.id),
      ['c-99', 'c-98', 'c-97'],
    );
  });

  it('reads any page directly, including across object boundaries and the last page', async () => {
    const run = browse();
    await finished(run);
    const rows = await everyRow(run);
    assert.equal(rows.length, 4 * ROWS);
    assert.deepEqual(rows.slice(98, 102).map((row) => row.id), ['c-1', 'c-0', 'b-99', 'b-98']);
    // Untimed objects have no place on the time axis and come last.
    assert.equal(rows.at(-1)?.id, 'legacy-0');
    const last = await run.readPage(390, 50);
    assert.equal(last.rows.length, 10);
  });

  it('pages oldest first when asked', async () => {
    const run = browse({ order: 'oldest' });
    await finished(run);
    const page = await run.readPage(0, 2);
    assert.deepEqual(page.rows.map((row) => row.id), ['legacy-0', 'legacy-1']);
  });

  it('folds a forked field into one column across its whole history', async () => {
    const run = browse();
    await finished(run);
    const rows = await everyRow(run);
    const byId = new Map(rows.map((row) => [row.id, row.total]));
    assert.equal(byId.get('a-7'), 7, 'the old fork still reads, widened to the folded type');
    assert.equal(byId.get('b-7'), 7.5);
  });

  it('renders timestamps as UTC ISO-8601 and keeps lists as lists', async () => {
    const run = browse();
    await finished(run);
    const [row] = (await run.readPage(0, 1)).rows;
    assert.equal(row?._kafka_timestamp, new Date(minutes(100 + 99)).toISOString());
    assert.deepEqual(row?.['items[].sku'], ['SKU-99', 'SKU-X']);
  });

  it('reads struct fields as columns, NULL in files written before the struct existed', async () => {
    const run = browse();
    await finished(run);
    const rows = await everyRow(run);
    const byId = new Map(rows.map((row) => [row.id, row]));
    assert.equal(byId.get('c-99')?.['customer.email'], 'c-99@x');
    assert.equal(byId.get('a-5')?.['customer.email'], null);
    assert.equal(byId.get('a-5')?.note, null, 'a field no file has reads NULL');
  });

  it('shows the Kafka metadata columns, headers as entries and unmapped values as JSON text', async () => {
    const run = browse();
    await finished(run);
    const [row] = (await run.readPage(0, 1)).rows;
    assert.deepEqual(row?._kafka_headers, [{ key: 'trace', value: 'abc' }]);
    assert.equal(row?._kafka_key, 'k-99');
    const unmapped = (await everyRow(run)).find((item) => item.id === 'c-3');
    assert.equal(unmapped?._unmapped, '{"extra":1}');
    assert.equal(run.columns?.find((column) => column.name === '_unmapped')?.json, true);
  });
});

describe('a time range', () => {
  const range: TimeRange = { from: minutes(0), to: minutes(105) };

  it('counts an object wholly inside from the manifest and reads only the ones it cuts', async () => {
    const run = browse({ range });
    await finished(run);
    const expected = await brute(
      `_kafka_timestamp >= TIMESTAMPTZ '2026-01-01 00:00:00+00' AND _kafka_timestamp < TIMESTAMPTZ '2026-01-01 01:45:00+00'`,
    );
    assert.equal(run.rows, expected);
    assert.equal(run.rows, 100 + 45 + 5);
    assert.equal(run.scope.objects, 3, 'the untimed object cannot match a bounded range');
  });

  it('returns exactly the rows inside the range, newest first', async () => {
    const run = browse({ range });
    await finished(run);
    const rows = await everyRow(run, 16);
    assert.equal(rows.length, run.rows);
    for (const row of rows) {
      const at = Date.parse(String(row._kafka_timestamp));
      assert.ok(at >= range.from! && at < range.to!, `${String(row._kafka_timestamp)} is inside the range`);
    }
    assert.deepEqual(rows.slice(0, 2).map((row) => row.id), ['c-4', 'c-3']);
  });

  it('matches nothing when no object overlaps it', async () => {
    const run = browse({ range: { from: minutes(10_000), to: null } });
    await finished(run);
    assert.equal(run.rows, 0);
    assert.equal(run.totalExact, true);
    assert.deepEqual((await run.readPage(0, 10)).rows, []);
  });
});

describe('filters', () => {
  it('count exactly what a full scan matches, and every row matches', async () => {
    const run = browse({ filters: [{ column: 'status', operator: 'eq', value: 'shipped' }] });
    await finished(run);
    assert.equal(run.rows, await brute(`status = 'shipped'`));
    const rows = await everyRow(run, 10);
    assert.equal(rows.length, run.rows);
    assert.ok(rows.every((row) => row.status === 'shipped'));
    assert.equal(rows[0]?.id, 'c-97');
  });

  it('match any element of a list column', async () => {
    const run = browse({ filters: [{ column: 'items[].sku', operator: 'eq', value: 'SKU-5' }] });
    await finished(run);
    assert.equal(run.rows, 4);
  });

  it('combine with a time range', async () => {
    const run = browse({
      range: { from: minutes(90), to: minutes(110) },
      filters: [{ column: 'status', operator: 'neq', value: 'new' }],
    });
    await finished(run);
    const expected = await brute(
      `_kafka_timestamp >= TIMESTAMPTZ '2026-01-01 01:30:00+00' AND _kafka_timestamp < TIMESTAMPTZ '2026-01-01 01:50:00+00' AND status <> 'new'`,
    );
    assert.equal(run.rows, expected);
  });

  it('make the first page readable before the total is known', async () => {
    const run = browse({ filters: [{ column: 'status', operator: 'eq', value: 'new' }] });
    const early = await run.readPage(0, 5);
    assert.equal(early.complete, false, 'nothing is counted before the run starts');
    assert.deepEqual(early.rows, []);

    const started = run.start();
    while (run.rowsReady < 5 && !run.isTerminal()) await new Promise((resolve) => setTimeout(resolve, 1));
    const page = await run.readPage(0, 5);
    assert.equal(page.complete, true);
    assert.equal(page.rows.length, 5);
    await started;
    assert.equal(run.totalExact, true);
  });

  it('cache a page only once it is complete', async () => {
    const run = browse();
    await finished(run);
    const first = await run.readPage(0, 10);
    const again = await run.readPage(0, 10);
    assert.equal(first.complete, true);
    assert.deepEqual(again.rows, first.rows);
  });
});

describe('cancelling', () => {
  it('stops the run and says so', async () => {
    const run = browse({ filters: [{ column: 'status', operator: 'eq', value: 'new' }] });
    const started = run.start();
    run.cancel();
    await started;
    assert.equal(run.state, 'cancelled');
  });
});

describe('statements', () => {
  it('are bounded by the time range', async () => {
    const run = statement('SELECT count(*) AS n FROM shop.orders', { from: minutes(0), to: minutes(105) });
    await finished(run);
    assert.equal(run.state, 'succeeded');
    assert.equal((await run.readPage(0, 10)).rows[0]?.n, 150);
  });

  it('page through their stored result with an exact total', async () => {
    const run = statement('SELECT "status", count(*) AS n FROM shop.orders GROUP BY 1 ORDER BY 1');
    await finished(run);
    assert.equal(run.rows, 3);
    assert.deepEqual((await run.readPage(1, 1)).rows, [{ status: 'new', n: 136 }]);
  });

  it('keep at most the result limit, but still count every row', async () => {
    const run = statement('SELECT * FROM shop.orders');
    await finished(run);
    assert.equal(run.truncated, true);
    assert.equal(run.rowsRetained, 50);
    assert.equal(run.rows, 4 * ROWS);
    assert.equal((await run.readPage(40, 50)).rows.length, 10);
  });

  it('report a failure at the place in the statement the engine pointed at', async () => {
    const run = statement('SELECT\n  nope\nFROM shop.orders');
    await finished(run);
    assert.equal(run.state, 'failed');
    assert.match(run.error?.message ?? '', /nope/);
    assert.deepEqual(run.error?.position, { line: 2, column: 3 });
  });

  it('see a column no object has yet as NULL rather than failing to bind', async () => {
    const run = statement(`SELECT count(*) AS n, count("note") AS notes FROM shop.orders`);
    await finished(run);
    assert.deepEqual((await run.readPage(0, 1)).rows[0], { n: 400, notes: 0 });
  });

  it('read the native table as iceberg.<namespace>.<name>, with structs and lists as they are', async () => {
    const run = statement(
      'SELECT count(*) AS n, count(customer.email) AS emails, count(total) AS longs FROM iceberg.shop.orders',
    );
    await finished(run);
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.deepEqual((await run.readPage(0, 1)).rows[0], { n: 400, emails: 300, longs: 200 });

    const unnested = statement(`SELECT unnest(items).sku AS sku FROM iceberg.shop.orders WHERE id = 'c-7' ORDER BY 1`);
    await finished(unnested);
    assert.deepEqual((await unnested.readPage(0, 5)).rows, [{ sku: 'SKU-7' }, { sku: 'SKU-X' }]);
  });

  it('bound the native table by the time range too', async () => {
    const run = statement('SELECT count(*) AS n FROM iceberg.shop.orders', { from: minutes(0), to: minutes(105) });
    await finished(run);
    assert.equal((await run.readPage(0, 1)).rows[0]?.n, 150);
  });

  it('report an error in a native statement where it was written', async () => {
    const run = statement('SELECT\n  nope\nFROM iceberg.shop.orders');
    await finished(run);
    assert.equal(run.state, 'failed');
    assert.deepEqual(run.error?.position, { line: 2, column: 3 });
  });

  it('cannot open any file but the run’s own, however the statement names one (E10)', async () => {
    const outside = join(dataDir, 'outside.parquet');
    await runQuery(`COPY (SELECT 42 AS secret) TO ${quote(outside)} (FORMAT PARQUET)`);
    // Each of these gets past the keyword guard; DuckDB itself refuses them.
    for (const sql of [
      `SELECT * FROM "read_csv"('/etc/hosts')`,
      `SELECT * FROM read_csv_auto('/etc/hosts')`,
      `SELECT * FROM parquet_scan(${quote(outside)})`,
      `SELECT * FROM "read_parquet"(${quote(outside)})`,
      `SELECT * FROM "iceberg_scan"(${quote(dataDir)})`,
    ]) {
      const run = statement(sql);
      await finished(run);
      assert.equal(run.state, 'failed', sql);
      assert.match(run.error?.message ?? '', /Permission Error|disabled by configuration|Cannot access/, sql);
    }
    // The run's own files are still there to read.
    const allowed = statement('SELECT count(*) AS n FROM shop.orders');
    await finished(allowed);
    assert.equal((await allowed.readPage(0, 1)).rows[0]?.n, 400);
  });

  it('cannot change the sandbox from inside a statement', async () => {
    const run = statement(`SELECT current_setting('enable_external_access') AS external, current_setting('lock_configuration') AS locked`);
    await finished(run);
    assert.deepEqual((await run.readPage(0, 1)).rows[0], { external: false, locked: true });
  });

  it('name the column a statement meant when it still spells out the payload root', async () => {
    const run = statement('SELECT "payload.status" FROM shop.orders');
    await finished(run);
    assert.equal(run.state, 'failed');
    assert.match(run.error?.message ?? '', /write "status"/);
  });

  it('read a table by its name, which SQL compares without regard to case', async () => {
    for (const sql of ['SELECT count(*) AS n FROM SHOP.ORDERS', 'SELECT count(*) AS n FROM "Shop"."Orders"']) {
      const run = statement(sql, { from: null, to: null }, 'Orders');
      await finished(run);
      assert.equal(run.state, 'succeeded', sql);
      assert.equal((await run.readPage(0, 1)).rows[0]?.n, 4 * ROWS, sql);
    }
  });

  it('say which tables there are when a statement names one that is not', async () => {
    const run = statement('SELECT count(*) AS n\nFROM materialized');
    await finished(run);
    assert.equal(run.state, 'failed');
    assert.match(run.error?.message ?? '', /This materialization's table is shop\.orders\./);
    assert.doesNotMatch(run.error?.message ?? '', /Did you mean/, 'DuckDB guesses a system table');
    assert.deepEqual(run.error?.position, { line: 2, column: 6 });
  });

  it('keep DuckDB’s guess when it is one of the tables', async () => {
    const run = statement('SELECT count(*) AS n FROM shop.orderz');
    await finished(run);
    assert.equal(run.state, 'failed');
    assert.match(run.error?.message ?? '', /Did you mean "orders"\?/);
  });

  it('point a bare table name at its canonical one', async () => {
    const run = statement('SELECT count(*) AS n FROM orders');
    await finished(run);
    assert.equal(run.state, 'failed');
    assert.match(run.error?.message ?? '', /shop\.orders/);
  });
});

describe('exports', () => {
  it('contain every row of the result, in order', async () => {
    const run = browse({ filters: [{ column: 'status', operator: 'eq', value: 'cancelled' }] });
    await finished(run);
    const download = await openExport(run, 'csv');
    let text = '';
    for await (const chunk of download.stream) text += chunk;
    const lines = text.trim().split('\n');
    assert.equal(lines.length, run.rows + 1);
    assert.match(lines[0] ?? '', /^id,/);
    assert.match(lines[1] ?? '', /^c-98,/);
  });

  it('are refused until the run has finished', async () => {
    const run = browse({ filters: [{ column: 'status', operator: 'eq', value: 'new' }] });
    await assert.rejects(() => openExport(run, 'csv'), /once it has finished/);
  });

  it('write JSON Lines as nested records when asked, and flat by default', async () => {
    const run = browse({ filters: [{ column: 'status', operator: 'eq', value: 'cancelled' }] });
    await finished(run);
    const read = async (shape?: 'nested' | 'flat'): Promise<Record<string, unknown>[]> => {
      let text = '';
      for await (const chunk of (await openExport(run, 'jsonl', shape)).stream) text += chunk;
      return text.trim().split('\n').map((line) => JSON.parse(line) as Record<string, unknown>);
    };

    const nested = await read('nested');
    assert.equal(nested.length, run.rows);
    assert.deepEqual(nested[0]?.items, [{ sku: 'SKU-98' }, { sku: 'SKU-X' }]);
    assert.equal(nested[0]?.id, 'c-98');
    assert.equal(nested[0]?.status, 'cancelled');
    assert.equal(nested[0]?._kafka_offset, 198);
    assert.ok(!('items[].sku' in (nested[0] ?? {})));

    const flat = await read();
    assert.deepEqual(flat[0]?.['items[].sku'], ['SKU-98', 'SKU-X']);
    assert.ok(!('items' in (flat[0] ?? {})));
  });

  it('write a JSON array in either shape', async () => {
    const run = statement('SELECT "id", "items[].sku" FROM shop.orders ORDER BY "id" LIMIT 2');
    await finished(run);
    let text = '';
    for await (const chunk of (await openExport(run, 'json', 'nested')).stream) text += chunk;
    assert.deepEqual(JSON.parse(text), [
      { id: 'a-0', items: [{ sku: 'SKU-0' }, { sku: 'SKU-X' }] },
      { id: 'a-1', items: [{ sku: 'SKU-1' }, { sku: 'SKU-X' }] },
    ]);
  });
});

describe('profiles', () => {
  it('describe the rows of the run', async () => {
    const run = browse({ range: { from: minutes(0), to: minutes(105) } });
    await finished(run);
    const profile = await profileColumn(run, 'status');
    assert.equal(profile.total, 150);
    assert.equal(profile.topValues.length, 3);
  });
});

describe('time pruning', () => {
  const timed = (min: number, max: number): DataObject => ({
    uri: 'x',
    name: 'x',
    bytes: 1,
    rows: 10,
    committedAt: 0,
    partition: 0,
    offsetStart: 0,
    offsetEnd: 9,
    timeMin: min,
    timeMax: max,
    timeKnown: true,
    partitionKey: null,
  });

  it('keeps an object that touches the range and drops one that does not', () => {
    assert.equal(overlapsRange(timed(0, 99), { from: 99, to: null }), true);
    assert.equal(overlapsRange(timed(0, 99), { from: 100, to: null }), false);
    assert.equal(overlapsRange(timed(100, 199), { from: null, to: 100 }), false);
  });

  it('never rules out an object whose span it cannot see', () => {
    assert.equal(overlapsRange({ ...timed(0, 0), timeMin: null, timeMax: null, timeKnown: false }, { from: 5, to: 6 }), true);
  });

  it('treats an object known to hold no timestamps as outside every bounded range', () => {
    const untimed = { ...timed(0, 0), timeMin: null, timeMax: null };
    assert.equal(overlapsRange(untimed, { from: 0, to: null }), false);
    assert.equal(overlapsRange(untimed, { from: null, to: null }), true);
  });

  it('counts an object from the manifest only when it lies wholly inside', () => {
    assert.equal(insideRange(timed(10, 20), { from: 10, to: 21 }), true);
    assert.equal(insideRange(timed(10, 20), { from: 10, to: 20 }), false);
  });

  it('estimates an upper bound unless every object lies inside', () => {
    const estimate = estimateRange(catalog, { from: minutes(0), to: minutes(105) });
    assert.deepEqual([estimate.objects, estimate.rows, estimate.rowsExact], [3, 300, false]);
    assert.equal(estimateRange(catalog, { from: null, to: null }).rowsExact, true);
  });

  it('summarizes what the manifests know, and which snapshot that is', () => {
    const summary = summarize(catalog);
    assert.equal(summary.rows, 400);
    assert.equal(summary.untimedRows, 100);
    assert.equal(summary.timeMin, new Date(minutes(0)).toISOString());
    assert.equal(summary.snapshotId, '7');
    assert.equal(summary.validThrough, new Date(minutes(159)).toISOString());
    assert.equal(summary.source, 'iceberg');
  });

  it('orders untimed objects after timed ones', () => {
    assert.deepEqual(orderObjects(catalogObjects, 'newest').map((object) => object.name), ['c', 'b', 'a', 'legacy']);
  });
});

describe('logical columns', () => {
  it('present a forked field once, typed so every fork fits, newest fork first', () => {
    const total = columns.find((column) => column.name === 'total');
    assert.equal(total?.type, 'DOUBLE');
    assert.deepEqual(total?.forks.map((item) => item.fieldId), [19, 11]);
    assert.equal(total?.expression, 'COALESCE("total__v2", TRY_CAST("total" AS DOUBLE))');
  });

  it('give a list path its list type, read through the list', () => {
    const sku = columns.find((column) => column.name === 'items[].sku');
    assert.equal(sku?.type, 'VARCHAR[]');
    assert.equal(sku?.expression, 'list_transform("items", lambda __e1: (__e1)."sku")');
  });

  it('name struct fields by path', () => {
    assert.equal(columns.find((column) => column.name === 'customer.email')?.expression, '("customer")."email"');
  });

  it('select only the columns a count needs', () => {
    assert.equal(selectList(columns, new Set(['id', 'status'])), '"id" AS "id", "status" AS "status"');
  });

  it('leave an unbounded range unrestricted', () => {
    assert.equal(timePredicate({ from: null, to: null }), null);
    assert.match(timePredicate({ from: 0, to: null }) ?? '', /^"_kafka_timestamp" >= TIMESTAMPTZ/);
  });

  it('refuse ordering comparisons on a list', () => {
    assert.throws(() => renderFilter({ column: 'items[].sku', operator: 'gt', value: 'a' }, columns), /is a list/);
  });

  it('put payload fields first, superseded-only ones after, then the Kafka metadata', () => {
    const names = columns.map((column) => column.name);
    assert.deepEqual(names.slice(0, 6), ['id', 'total', 'status', 'items[].sku', 'customer.email', 'note']);
    assert.deepEqual(names.slice(6), [
      '_kafka_timestamp',
      '_kafka_partition',
      '_kafka_offset',
      '_kafka_key',
      '_kafka_headers',
      '_materialized_at',
      '_unmapped',
    ]);
  });

  it('keep the root where a field would take a metadata column name', () => {
    const clash = logicalColumns(
      { schemaId: 0, fields: [...METADATA_FIELDS, { id: 10, name: '_kafka_offset__payload', required: false, type: 'long' }, { id: 11, name: '_id', required: false, type: 'string' }] },
      new Map([[10, { fieldId: 10, path: ['_kafka_offset__payload'], key: '_kafka_offset', forkOf: null, forkVersion: 1, state: 'current' }]]),
    );
    const names = clash.map((column) => column.name);
    assert.ok(names.includes('payload._kafka_offset'));
    assert.ok(names.includes('_kafka_offset'));
    assert.equal(clash.find((column) => column.name === 'payload._kafka_offset')?.expression, '"_kafka_offset__payload"');
    assert.equal(clash.find((column) => column.name === '_id')?.group, 'payload');
  });

  it('accept the payload-rooted names older links use, and name the column the view uses', () => {
    assert.deepEqual(resolveFilters([{ column: 'payload.status', operator: 'eq', value: 'new' }], columns), [
      { column: 'status', operator: 'eq', value: 'new' },
    ]);
    assert.throws(() => resolveFilters([{ column: 'payload.nope', operator: 'is_null' }], columns), /no column named/);
    assert.match(renderFilter({ column: 'payload.status', operator: 'eq', value: 'new' }, columns), /^"status" = 'new'$/);
  });

  it('fold a collapsed field into JSON text, older values encoded as JSON too', () => {
    const collapsed = logicalColumns(
      {
        schemaId: 3,
        fields: [
          ...METADATA_FIELDS,
          { id: 10, name: 'phone', required: false, type: 'string' },
          { id: 11, name: 'phone__v2', required: false, type: 'long' },
          { id: 12, name: 'phone__json', required: false, type: 'string' },
        ],
      },
      new Map([
        fork(10, ['phone'], 1, 'superseded'),
        fork(11, ['phone__v2'], 2, 'superseded', 10),
        fork(12, ['phone__json'], 3, 'collapsed', 10),
      ]),
    );
    const phone = collapsed.find((column) => column.name === 'phone');
    assert.equal(phone?.jsonText, true);
    assert.equal(phone?.type, 'VARCHAR');
    assert.equal(
      phone?.expression,
      'COALESCE("phone__json", CAST(to_json("phone__v2") AS VARCHAR), CAST(to_json("phone") AS VARCHAR))',
    );
    assert.equal(collapsed.filter((column) => column.name === 'phone').length, 1);
  });

  it('keep a value and a list of the same field apart', () => {
    const shapes = logicalColumns(
      {
        schemaId: 1,
        fields: [
          ...METADATA_FIELDS,
          { id: 10, name: 'tag', required: false, type: 'string' },
          { id: 11, name: 'tag__v2', required: false, type: { type: 'list', 'element-id': 12, element: 'string', 'element-required': false } },
        ],
      },
      new Map([fork(10, ['tag'], 1, 'superseded'), fork(11, ['tag__v2'], 2, 'current', 10)]),
    );
    assert.deepEqual(
      shapes.filter((column) => column.group === 'payload').map((column) => [column.name, column.type]),
      [['tag[]', 'VARCHAR[]'], ['tag', 'VARCHAR']],
    );
  });
});

describe('the tables a statement reads', () => {
  const tables = ['orders', 'payments', 'orders_v2'];

  it('are the ones it names, logical or native, in any case, in the materialization’s order', () => {
    assert.deepEqual(referencedTables('SELECT * FROM shop.payments p JOIN SHOP.ORDERS o ON o.id = p.order_id', tables), ['orders', 'payments']);
    assert.deepEqual(referencedTables('SELECT * FROM iceberg.shop."Payments"', tables), ['payments']);
    assert.deepEqual(referencedTables('SELECT * FROM shop.orders_v2', tables), ['orders_v2'], 'a longer name is another table');
    assert.deepEqual(referencedTables('SELECT 42', tables), []);
  });

  it('are not named in strings or comments', () => {
    assert.deepEqual(referencedTables("SELECT 'payments' AS t FROM shop.orders -- iceberg.shop.orders_v2\n/* payments */", tables), ['orders']);
  });

  it('refuse a native table the materialization does not have, saying which it has', () => {
    const inDatasets = tables.map((name) => ({ dataset: 'shop', name }));
    assert.doesNotThrow(() => assertNativeReferences('SELECT * FROM iceberg.shop.orders JOIN ICEBERG.SHOP."Payments" USING (id)', inDatasets));
    assert.doesNotThrow(() =>
      assertNativeReferences("SELECT 'iceberg.nothing' AS t, x.iceberg FROM shop.orders -- iceberg.nothing", inDatasets),
    );
    const natives = /this materialization's native tables are iceberg\.shop\.orders, iceberg\.shop\.payments and iceberg\.shop\.orders_v2/;
    for (const [sql, named] of [
      ['SELECT * FROM iceberg.shop.customers', 'iceberg.shop.customers'],
      ['SELECT * FROM iceberg.orders', 'iceberg.orders'],
      ['SELECT * FROM iceberg.lake.orders', 'iceberg.lake.orders'],
    ] as const) {
      assert.throws(() => assertNativeReferences(sql, inDatasets), new RegExp(`no table ${named.replace(/\./g, '\\.')}; `), sql);
      assert.throws(() => assertNativeReferences(sql, inDatasets), natives, sql);
    }
  });

  it('may be in several datasets, each read in its own', () => {
    const spread = [
      { dataset: 'sales', name: 'orders' },
      { dataset: 'finance', name: 'payments' },
    ];
    assert.doesNotThrow(() => assertNativeReferences('SELECT * FROM iceberg.sales.orders JOIN iceberg.finance.payments USING (id)', spread));
    assert.throws(
      () => assertNativeReferences('SELECT * FROM iceberg.sales.payments', spread),
      /no table iceberg\.sales\.payments; this materialization's native tables are iceberg\.sales\.orders and iceberg\.finance\.payments/,
      'a table is in its own dataset only',
    );
  });
});
