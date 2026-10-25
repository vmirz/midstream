/**
 * Runs over a real Iceberg table: the fixture the control plane's library wrote, read the way the
 * console reads a table in production — pointer, metadata, manifests, then the data files through
 * the table's schema — rather than over a hand-built catalog.
 */

import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, describe, it } from 'node:test';

const dataDir = await mkdtemp(join(tmpdir(), 'console-table-'));
process.env.CONSOLE_DATA_DIR = dataDir;

const { ObjectMissingError, setObjectReader } = await import('../iceberg/io.ts');
const { setObjectPathResolver } = await import('../data/objects.ts');
const { loadCatalog, summarize } = await import('../data/catalog.ts');
const { logicalColumns } = await import('../data/logical.ts');
const { BrowseRun } = await import('./browse.ts');
const { validateStatement } = await import('./store.ts');
const { StatementRun } = await import('./statement.ts');
const { resetTables } = await import('../iceberg/table.ts');
const { resetSnapshotCaches } = await import('../iceberg/snapshot.ts');
const { readMetadata } = await import('../iceberg/metadata.ts');
const { FIXTURE_DIR, FIXTURE_LOCATION, PAYMENTS_DIR, PAYMENTS_LOCATION, fixturePath, fixtureReader, rewriteAvro } = await import(
  '../iceberg/testing.ts'
);
const { startRun } = await import('./store.ts');

import type { Catalog } from '../data/catalog.ts';
import type { Run } from './run.ts';
import type { RunRequest } from '../../shared/types.ts';

const LOCATION = FIXTURE_LOCATION;
/** The materialization's tables, as the control plane lists them. */
/** The materialization's two tables, in two datasets: SQL reads `shop.orders` and `finance.payments`. */
const TABLES = [
  { name: 'orders', dataset: 'shop', datasetId: 'ds-000000000001', location: LOCATION },
  { name: 'payments', dataset: 'finance', datasetId: 'ds-000000000002', location: PAYMENTS_LOCATION },
];
const admit = async (): Promise<() => void> => () => undefined;

/** A table's current snapshot id, as the fixture recorded it: read as text, since it does not fit a double. */
async function currentSnapshotId(dir: string): Promise<string> {
  const text = await readFile(join(dir, 'expected.json'), 'utf8');
  return /"current_snapshot_id": (\d+)/.exec(text)?.[1] as string;
}
const ORDERS_SNAPSHOT = await currentSnapshotId(FIXTURE_DIR);
const PAYMENTS_SNAPSHOT = await currentSnapshotId(PAYMENTS_DIR);

before(() => {
  setObjectReader(fixtureReader());
  setObjectPathResolver((uri) => fixturePath(uri));
});

after(async () => {
  setObjectReader(null);
  setObjectPathResolver(null);
  resetTables();
  await rm(dataDir, { recursive: true, force: true });
});

function runOver(catalog: Catalog): InstanceType<typeof BrowseRun> {
  return makeBrowse(catalog);
}

function makeBrowse(catalog: Catalog, request: { filters?: Array<{ column: string; operator: 'eq'; value: string }> } = {}): InstanceType<typeof BrowseRun> {
  return new BrowseRun({
    materializationId: 'mat-a1b2c3d4e5f6',
    ownerId: 'usr-test',
    tableName: 'orders',
    datasets: [{ id: 'ds-000000000001', name: 'shop' }],
    request: { kind: 'browse', range: { from: null, to: null }, ...request },
    catalog,
    columns: logicalColumns(catalog.schema, catalog.forkMap),
    admit,
  });
}

function statementOver(catalog: Catalog, sql: string): InstanceType<typeof StatementRun> {
  const columns = logicalColumns(catalog.schema, catalog.forkMap);
  return new StatementRun({
    materializationId: 'mat-a1b2c3d4e5f6',
    ownerId: 'usr-test',
    tableName: 'orders',
    datasets: [{ id: 'ds-000000000001', name: 'shop' }],
    request: { kind: 'sql', range: { from: null, to: null }, sql },
    catalog,
    columns,
    admit,
    statement: sql,
    sources: [{ namespace: 'shop', name: 'orders', catalog, columns }],
    tableNames: TABLES.map((table) => `${table.dataset}.${table.name}`),
  });
}

async function finished<T extends Run>(run: T): Promise<T> {
  await run.start();
  return run;
}

/** Start a run as the route does, over both tables, and wait for it to end. */
async function started(request: RunRequest): Promise<Run> {
  const run = await startRun({ materializationId: 'mat-a1b2c3d4e5f6', ownerId: 'usr-test', tables: TABLES, request });
  if (!run.isTerminal()) {
    await new Promise<void>((resolve) => {
      const stop = run.subscribe((snapshot) => {
        if (snapshot.state === 'running') return;
        stop();
        resolve();
      });
      if (run.isTerminal()) resolve();
    });
  }
  return run;
}

const ALL_TIME = { from: null, to: null };

describe('a run over an Iceberg table', () => {
  it('knows the table’s exact total from its manifests and pages it newest first', async () => {
    const catalog = await loadCatalog('mat-a1b2c3d4e5f6', LOCATION, { fresh: true });
    assert.equal(catalog.objects.length, 3);
    const run = await finished(runOver(catalog));
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.equal(run.rows, 12);
    assert.equal(run.progress.objectsRead, 0, 'nothing was opened to count');
    const rows = (await run.readPage(0, 12)).rows;
    assert.deepEqual(
      rows.slice(0, 3).map((row) => [row._kafka_partition, row._kafka_offset]),
      [
        [1, 11],
        [1, 10],
        [0, 9],
      ],
    );
    assert.equal(run.snapshot().snapshotId, ORDERS_SNAPSHOT);
    assert.equal(run.snapshot().pinned, false);
  });

  it('folds forks across the schema change, and reads a compacted file like any other', async () => {
    const catalog = await loadCatalog('mat-a1b2c3d4e5f6', LOCATION);
    const run = await finished(runOver(catalog));
    const rows = (await run.readPage(0, 12)).rows;
    const at = (partition: number, offset: number): Record<string, unknown> | undefined =>
      rows.find((row) => row._kafka_partition === partition && row._kafka_offset === offset);
    assert.equal(at(0, 2)?.total, 20, 'a long from before the fork, read as the folded DOUBLE');
    assert.equal(at(1, 10)?.total, 100.5);
    assert.equal(at(0, 2)?.['customer.phone'], '+15550100');
    assert.equal(at(1, 10)?.['customer.phone'], null);
    assert.equal(at(1, 10)?.['customer.phone.number'], '5550100');
    assert.equal(at(1, 10)?.coupon, 'SAVE10');
    assert.equal(at(0, 2)?.coupon, null);
    assert.deepEqual(at(0, 2)?.['items[].sku'], ['S2', 'T2']);
  });

  it('filters on a struct field', async () => {
    const catalog = await loadCatalog('mat-a1b2c3d4e5f6', LOCATION);
    const run = await finished(makeBrowse(catalog, { filters: [{ column: 'customer.email', operator: 'eq', value: 'c4@example.com' }] }));
    assert.equal(run.rows, 1);
    assert.equal((await run.readPage(0, 1)).rows[0]?._kafka_offset, 4);
  });

  it('reads the table as of an older snapshot', async () => {
    const metadata = await readMetadata(
      (await loadCatalog('mat-a1b2c3d4e5f6', LOCATION)).metadataLocation as string,
    );
    const third = metadata.snapshots[2]?.snapshotId as string;
    const catalog = await loadCatalog('mat-a1b2c3d4e5f6', LOCATION, { snapshotId: third });
    const run = await finished(runOver(catalog));
    assert.equal(run.rows, 10);
    assert.equal(run.snapshot().snapshotId, third);
    assert.equal(run.snapshot().pinned, true);
    const summary = summarize(catalog);
    assert.equal(summary.snapshotId, third);
    assert.equal(summary.currentSnapshotId, ORDERS_SNAPSHOT);
  });

  it('answers statements over the logical and the native table alike', async () => {
    const catalog = await loadCatalog('mat-a1b2c3d4e5f6', LOCATION);
    const logical = await finished(statementOver(catalog, 'SELECT count(*) AS n, sum(total) AS total FROM shop.orders'));
    assert.deepEqual((await logical.readPage(0, 1)).rows[0], { n: 12, total: 661 });
    const native = await finished(
      statementOver(catalog, 'SELECT customer.phone__v2.number AS number FROM iceberg.shop.orders WHERE _kafka_offset = 10'),
    );
    assert.equal(native.state, 'succeeded', native.error?.message ?? '');
    assert.deepEqual((await native.readPage(0, 1)).rows, [{ number: '5550100' }]);
  });

  it('binds a statement while it is typed without opening anything', async () => {
    const check = (sql: string): Promise<void> =>
      validateStatement({ materializationId: 'mat-a1b2c3d4e5f6', tables: TABLES, sql });
    await check('SELECT "customer.phone.number", count(*) FROM shop.orders GROUP BY 1');
    await check('SELECT customer.phone__v2.number, unnest(items).sku FROM iceberg.shop.orders');
    await check('SELECT o.status, sum(p.amount) FROM shop.orders o JOIN finance.payments p USING (order_id) GROUP BY 1');
    await check('SELECT method FROM iceberg.finance.payments');
    await assert.rejects(() => check('SELECT nope FROM shop.orders'), /nope/);
    await assert.rejects(() => check('SELECT amount FROM shop.orders'), /amount/, 'payments’ column is not orders’');
    await assert.rejects(() => check(`SELECT * FROM read_csv_auto('/etc/hosts')`), /Permission Error|disabled by configuration|Cannot access/);
    await assert.rejects(() => check('SELECT * FROM iceberg.shop.customers'), /native tables are iceberg\.shop\.orders and iceberg\.finance\.payments/);
    await assert.rejects(() => check('SELECT * FROM shop.customers'), /tables are shop\.orders and finance\.payments/);
    await assert.rejects(() => check('SELECT * FROM orders'), /shop\.orders/, 'a bare name is pointed at its canonical one');
  });

  it('cannot list or read another run’s stored result, however the statement names it', async () => {
    const catalog = await loadCatalog('mat-a1b2c3d4e5f6', LOCATION);
    const other = await finished(statementOver(catalog, 'SELECT count(*) AS n FROM shop.orders'));
    assert.equal(other.state, 'succeeded', other.error?.message ?? '');
    const runs = join(dataDir, 'runs');
    const stored = (await readdir(runs, { recursive: true })).filter((name) => name.endsWith('.parquet'));
    assert.ok(stored.length > 0, 'the other run stored its result');
    for (const sql of [
      ...stored.map((name) => `SELECT * FROM "read_parquet"('${join(runs, name)}')`),
      ...stored.map((name) => `SELECT * FROM parquet_scan('${join(runs, name)}')`),
      `SELECT * FROM "glob"('${runs}/**')`,
    ]) {
      const probe = await finished(statementOver(catalog, sql));
      if (probe.state === 'succeeded') {
        const page = await probe.readPage(0, 100);
        const leaked = page.rows.filter((row) => JSON.stringify(row).includes(other.id));
        assert.deepEqual(leaked, [], sql);
        assert.ok(!sql.includes('parquet'), `${sql} read another run's result`);
      } else {
        assert.match(probe.error?.message ?? '', /Permission Error|disabled by configuration|Cannot access/, sql);
      }
    }
  });

  it('reads nothing outside the table’s location, whatever its pointer, metadata or manifests name', async () => {
    const fixture = fixtureReader();
    // As long as the location, so a manifest's encoding survives the rewrite: another materialization's table.
    const sibling = LOCATION.replace('-a1b2c3d4e5f6/', '-a1b2c3d4e5f7/');
    const text = async (uri: string, from: string, to: string): Promise<Buffer> =>
      Buffer.from((await fixture(uri)).toString('utf8').split(from).join(to), 'utf8');
    const reading = async <T>(reader: (uri: string) => Promise<Buffer>, read: () => Promise<T>): Promise<T> => {
      setObjectReader(reader);
      resetTables();
      resetSnapshotCaches();
      try {
        return await read();
      } finally {
        setObjectReader(fixture);
        resetTables();
        resetSnapshotCaches();
      }
    };
    const load = (): Promise<Catalog> => loadCatalog('mat-a1b2c3d4e5f6', LOCATION, { fresh: true });

    const refusals: Array<[string, (uri: string) => Promise<Buffer>, RegExp]> = [
      [
        'a pointer naming metadata elsewhere',
        (uri) => (uri.endsWith('catalog-pointer.json') ? text(uri, `"metadata_location":"${LOCATION}/`, `"metadata_location":"${sibling}/`) : fixture(uri.replace(sibling, LOCATION))),
        /names its metadata outside its location/,
      ],
      [
        'metadata saying it is elsewhere',
        (uri) => (uri.endsWith('.metadata.json') ? text(uri, `"location":"${LOCATION}"`, `"location":"${sibling}"`) : fixture(uri)),
        /says it is at gs:\/\/lake\/materializer\/mat-a1b2c3d4e5f7\/orders/,
      ],
      [
        'a manifest list elsewhere',
        (uri) => (uri.endsWith('.metadata.json') ? text(uri, `"manifest-list":"${LOCATION}/`, `"manifest-list":"${sibling}/`) : fixture(uri)),
        /names a manifest list outside its location/,
      ],
    ];
    for (const [label, reader, message] of refusals) {
      const error = (await reading(reader, () => load().then(() => assert.fail(`${label}: the table read`), (cause: unknown) => cause))) as {
        code?: string;
        statusCode?: number;
        message?: string;
      };
      assert.equal(error.code, 'unavailable', label);
      assert.equal(error.statusCode, 503, label);
      assert.match(error.message ?? '', message, label);
    }

    // Data files elsewhere are never listed, and a run over that snapshot is refused.
    const elsewhere = (uri: string): Promise<Buffer> =>
      uri.endsWith('-m0.avro') || uri.endsWith('-m1.avro')
        ? fixture(uri).then((buffer) => rewriteAvro(buffer, `${LOCATION}/data/`, `${sibling}/data/`))
        : fixture(uri);
    const catalog = await reading(elsewhere, load);
    assert.equal(catalog.objects.length, 0);
    assert.ok((catalog.snapshot?.outsideFiles ?? 0) > 0);
    await reading(elsewhere, () =>
      assert.rejects(
        () =>
          startRun({
            materializationId: 'mat-a1b2c3d4e5f6',
            ownerId: 'usr-test',
            tables: TABLES,
            request: { kind: 'browse', range: { from: null, to: null } },
          }),
        /Table orders: this snapshot holds .* data files outside the table's location/,
      ),
    );
    assert.equal((await load()).objects.length > 0, true);
  });

  it('joins two of the materialization’s tables in one statement, each at its own snapshot', async () => {
    const run = await started({
      kind: 'sql',
      range: ALL_TIME,
      sql: 'SELECT o.order_id, o.status, p.amount, p.method FROM shop.orders o JOIN finance.payments p USING (order_id) ORDER BY o.order_id',
    });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    const rows = (await run.readPage(0, 20)).rows;
    assert.equal(rows.length, 6);
    assert.deepEqual(rows[1], { order_id: 'O-1', status: 'confirmed', amount: 10.25, method: 'card' });
    const snapshot = run.snapshot();
    assert.deepEqual(
      snapshot.tables.map((table) => [table.name, table.snapshotId, table.pinned]),
      [
        ['orders', ORDERS_SNAPSHOT, false],
        ['payments', PAYMENTS_SNAPSHOT, false],
      ],
    );
    assert.equal(snapshot.table, 'orders');
    assert.equal(snapshot.scope.objects, 4, 'three of orders’ files and payments’ one');
  });

  it('reads only the tables a statement names', async () => {
    const run = await started({ kind: 'sql', table: 'orders', range: ALL_TIME, sql: 'SELECT count(*) AS n, sum(amount) AS paid FROM finance.payments' });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.deepEqual((await run.readPage(0, 1)).rows[0], { n: 6, paid: 151.5 });
    assert.deepEqual(run.snapshot().tables.map((table) => table.name), ['payments']);
    assert.equal(run.snapshot().table, 'payments', 'the run is about the table it read');
    assert.equal(run.snapshot().scope.objects, 1);

    const constant = await started({ kind: 'sql', range: ALL_TIME, sql: 'SELECT 42 AS answer' });
    assert.equal(constant.state, 'succeeded', constant.error?.message ?? '');
    assert.deepEqual(constant.snapshot().tables, []);
    assert.equal(constant.snapshot().scope.objects, 0);
  });

  it('browses the table it is asked for, by its name in any case', async () => {
    const run = await started({ kind: 'browse', table: 'PAYMENTS', range: ALL_TIME });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.equal(run.rows, 6);
    assert.equal(run.snapshot().table, 'payments');
    assert.equal(run.snapshot().request.table, 'payments');
    assert.deepEqual(run.snapshot().tables.map((table) => [table.name, table.snapshotId]), [['payments', PAYMENTS_SNAPSHOT]]);
    assert.equal((await run.readPage(0, 1)).rows[0]?.payment_id, 'P-5', 'newest first');
    await assert.rejects(() => started({ kind: 'browse', table: 'customers', range: ALL_TIME }), /no table customers; this materialization's tables are orders and payments/);
  });

  it('reads the request’s table as of an older snapshot and every other table as it is now', async () => {
    const metadata = await readMetadata((await loadCatalog('mat-a1b2c3d4e5f6', LOCATION)).metadataLocation as string);
    const third = metadata.snapshots[2]?.snapshotId as string;
    const run = await started({
      kind: 'sql',
      table: 'orders',
      asOf: third,
      range: ALL_TIME,
      sql: 'SELECT (SELECT count(*) FROM shop.orders) AS orders, (SELECT count(*) FROM finance.payments) AS payments',
    });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.deepEqual((await run.readPage(0, 1)).rows[0], { orders: 10, payments: 6 });
    assert.deepEqual(
      run.snapshot().tables.map((table) => [table.name, table.snapshotId, table.pinned]),
      [
        ['orders', third, true],
        ['payments', PAYMENTS_SNAPSHOT, false],
      ],
    );
    assert.equal(run.snapshot().pinned, true);
  });

  it('bounds every table a statement reads by the run’s time range', async () => {
    // The first four minutes of 2026-10-17: orders O-0 to O-3, and their payments 30 s after each.
    const from = Date.parse('2026-10-17T10:00:00Z');
    const run = await started({
      kind: 'sql',
      range: { from, to: from + 4 * 60_000 },
      sql: 'SELECT (SELECT count(*) FROM shop.orders) AS orders, (SELECT count(*) FROM finance.payments) AS payments, (SELECT count(*) FROM shop.orders JOIN finance.payments USING (order_id)) AS joined',
    });
    assert.equal(run.state, 'succeeded', run.error?.message ?? '');
    assert.deepEqual((await run.readPage(0, 1)).rows[0], { orders: 4, payments: 4, joined: 4 });
  });

  it('says there is no table yet when the pointer is missing', async () => {
    const catalog = await loadCatalog('mat-other', `${LOCATION}-missing`);
    assert.equal(catalog.objects.length, 0);
    assert.equal(summarize(catalog).reason, 'the table has not been created yet');
  });

  it('answers a table it cannot read with a 503 that says why, never storage’s own message', async () => {
    const fixture = fixtureReader();
    const unreadable = async (reader: (uri: string) => Promise<Buffer>): Promise<unknown> => {
      setObjectReader(reader);
      resetTables();
      resetSnapshotCaches();
      try {
        return await loadCatalog('mat-a1b2c3d4e5f6', LOCATION, { fresh: true }).then(
          () => assert.fail('the table read'),
          (error: unknown) => error,
        );
      } finally {
        setObjectReader(fixture);
        resetTables();
        resetSnapshotCaches();
      }
    };
    const refused = Object.assign(new Error('svc@project.iam does not have storage.objects.get access'), { code: 403 });
    const cases: Array<[string, (uri: string) => Promise<Buffer>, RegExp]> = [
      ['refused', async () => { throw refused; }, /could not be read: object storage answered 403$/],
      ['unreachable', async () => { throw new Error('getaddrinfo ENOTFOUND storage.googleapis.com'); }, /could not be read: object storage could not be reached$/],
      [
        'missing a manifest',
        async (uri) => {
          if (uri.endsWith('.avro')) throw new ObjectMissingError(uri);
          return fixture(uri);
        },
        /could not be read: no object at gs:\/\/.*\.avro$/,
      ],
    ];
    for (const [label, reader, message] of cases) {
      const error = (await unreadable(reader)) as { code?: string; statusCode?: number; message?: string };
      assert.equal(error.code, 'unavailable', label);
      assert.equal(error.statusCode, 503, label);
      assert.match(error.message ?? '', message, label);
      assert.doesNotMatch(error.message ?? '', /svc@|ENOTFOUND/, label);
    }
    assert.equal((await loadCatalog('mat-a1b2c3d4e5f6', LOCATION, { fresh: true })).objects.length > 0, true);
  });
});
