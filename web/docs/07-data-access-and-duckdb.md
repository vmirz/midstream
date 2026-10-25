# 07 — Data Access and DuckDB

Sources: `server/data/{catalog,gcs,objects,logical,duckdb,sql,tail}.ts`, `server/iceberg/` (06),
`server/runs/{run,browse,statement,view,export,profile,store,tail}.ts`.

## 07.1 What a table holds

Each source topic of a materialization is an Iceberg table of its own (handbook 11), in the dataset
chosen for it — its Iceberg namespace — and named after the topic unless someone chose another name.
A browse run reads one of them, the one the
Data tab's table picker shows; a statement reads any of them (§07.5). Each table's data files are
Parquet under `<location>/data/ts_day=<YYYY-MM-DD>/` (or `ts_hour=`, `ts_month=`, or no partition directory), one
per seal and table partition, later compacted:

| Group | Columns (field id) |
|-------|---------|
| Kafka metadata | `_kafka_timestamp` (1, `timestamptz`), `_kafka_partition` (2), `_kafka_offset` (3), `_kafka_key` (4), `_kafka_headers` (5, a map), `_materialized_at` (6), `_unmapped` (7, JSON text) |
| Payload | the record's own fields from id 10, nested as the record is: structs, lists, forks (`total__v2`), JSON collapses (`total__json`) |

`_kafka_timestamp` is the record's Kafka timestamp, in microseconds. Every file's manifest entry
carries its exact row count and the bounds of fields 1–3, and every row group carries min/max
statistics for them, which is what lets a time range skip files and row groups.

## 07.2 The catalog: what exists, without reading it

`data/catalog.ts::loadCatalog` turns one snapshot of one table into the run engine's inputs: the
snapshot's live data files as `DataObject`s (06 §6.1), the schema and fork map every snapshot is
read through (the table's current one), and why there is nothing to read when there is not (no
table yet, nothing committed). It reads the **current** snapshot unless a run asks for an older one
(`asOf`, §07.5; the Data tab never does).

| Field | From | Used for |
|---|---|---|
| `uri`, `bytes` | the manifest entry | reading, sizing |
| `rows` | `record_count` | exact totals and row positions with no reads |
| `timeMin`, `timeMax` | `_kafka_timestamp` bounds | pruning by time range |
| `timeKnown` | bounds present, or every value null | telling "no timestamps" from "unknown" |
| `committedAt` | the snapshot that added the file | ordering, "newer data available" |

Freshness is the pointer's (06 §6.2): a run reads a pointer no older than 2 s, the summary and
estimates one up to 5 s old, or up to 5 minutes while a fresh read happens in the background.
Everything behind the pointer is immutable, so caching it is always correct.

After each read of the current snapshot the catalog **warms footers** in the background in direct
mode — newest files first, capped, eight at a time — reading each file's footer into DuckDB's
metadata cache. Warming waits while any run is scanning, so it never competes with a question
someone is waiting on.

### Summary and estimates

`summarize` and `estimateRange` are pure functions over the catalog: no data is read.

- The summary gives the snapshot's rows (exact: every count is in the manifests), the event-time
  span, rows with no timestamp, the newest commit, the snapshot id and its `valid_through`.
- An estimate for a time range sums the files that overlap it. It is an upper bound unless every one
  of them lies wholly inside, which `rowsExact` reports. The time picker shows one per preset.

## 07.3 Time ranges and pruning

A range is half-open, `[from, to)`, in epoch milliseconds; either side may be open. The client
resolves "Last 15 minutes" or "Yesterday" to instants when Run is pressed.

```
overlapsRange(object, range)
  unbounded range                → keep (every file, timed or not)
  span unknown                   → keep: cannot be ruled out
  known to hold no timestamps    → drop: cannot match a bounded range
  timeMax < from or timeMin ≥ to → drop
insideRange(object, range)       → every row matches; its manifest count is the answer
```

Bounds are microseconds in the manifests and milliseconds here, floored, which never prunes a row
that matches: a row at `12:00:05.400999` has `timeMax = 12:00:05.400`, and a range starting at
`.401` cannot contain it.

Within a file that straddles a range, the predicate is applied to the native `_kafka_timestamp`
column ahead of any projection, so DuckDB pushes it into the Parquet reader and skips row groups on
their statistics. The cost of a run therefore scales with the window: an hour reads the handful of
files that hold it, a year reads a year's worth. A day-partitioned table has at most one file per
seal and day, and after compaction a few per day (handbook 11 §11.6).

## 07.4 The logical view

People see `customer.email`, not a struct path, and a forked field once rather than as `total` and
`total__v2`. `data/logical.ts` builds the columns from the table's schema and fork map — the rules
are in 06 §6.3 — and the SQL that produces them over the native read:

```sql
SELECT … AS "total", ("customer")."email" AS "customer.email",
       list_transform("items", lambda __e1: (__e1)."sku") AS "items[].sku", …
FROM read_parquet([...], schema = map {1: {name: '_kafka_timestamp', type: 'TIMESTAMPTZ', default_value: NULL}, …},
                  hive_partitioning = false)
```

Reading by field id (`schema = map {…}`) means a file from before a field existed reads it as NULL,
and DuckDB binds the read without opening a footer (E3). Names are the logical ones the Schema tab
shows: `status`, `customer.email`, `items[].sku`; the metadata columns keep their own. A filter or a
link that still says `payload.status` — the 0.x spelling — resolves to `status` (`findColumn`).

## 07.5 Runs

A run is the console's only unit of query work (`server/runs/`). Pressing Run creates one; the first
page of a table, a time range, filters and a hand-written statement are all runs; paging, profiling
and exporting read from one. Every run is pinned to **one snapshot** — the table's current one when
it started, or the one `request.asOf` names (a snapshot id; the runs API takes one, the Data tab
never sends it) — so paging is stable while
commits land, and a run can read the table as it was at any snapshot still kept. A snapshot with
delete files is refused (`assertReadable`): the console does not apply deletes, so it would show
rows a delete removed. The run's snapshot carries `snapshotId`, `snapshotAt`, `pinned` and
`validThrough`.

### Browse runs — time range and filters

```
plan     objects overlapping the range, ordered newest first
         (latest Kafka timestamp held, then commit time; untimed objects last)
count    each object's matching rows:
           wholly inside, no filters  → manifest count, no read
           otherwise                  → count(*) reading only the timestamp and filtered columns
page     row position → (object, row within object) by prefix sums over the counts
```

A page is a direct read of one object's row range. For a whole object it is
`file_row_number BETWEEN …`, which DuckDB answers from a single row group; for a filtered object it
is the matching rows numbered in file order, with row groups outside the range still skipped. No
page sorts or re-scans anything, so page 1 and page 40,000 cost the same.

| Example | What is read |
|---|---|
| All time, no filters (the opening view) | nothing to count — every count is in the manifests |
| A range, no filters | the timestamp column of the objects the range cuts |
| Any filter | the filtered columns (and timestamp, at the edges) of every object in range |

Objects are counted newest first, `CONSOLE_RUN_SCAN_CONCURRENCY` (48) at a time: each is four round
trips to object storage, and overlapping them is what makes a cold scan fast. Eight workers start
alone on the newest objects, which decide the first page, and the rest join as soon as one object is
counted (or after 500 ms). The leading run of counted objects is pageable at once — `rowsReady` in
the snapshot, pushed immediately the first time it moves — so the first page appears long before the
total is final. Progress is exact: rows of the objects counted so far over rows of every object in
range, both from the manifest.

A page read while counting goes first: counting starts no new object until it is done, because
from outside the cloud the two share one network path. Measured cold, a first page took 1.4 s
beside a full-width count and 0.86 s ahead of it. That holds for the first read of a page and for
the read that completes it; the reads in between, as a partial page fills, wait their turn, so a
sparse filter — whose first page fills only as counting proceeds — cannot stall the count it is
waiting on.

Rows within an object are newest-first by offset (`order: 'oldest'` reverses everything). Across
objects the order is by each object's latest timestamp, so it is newest-first by object, not a
global sort of every row; a global order is a statement's `ORDER BY`.

A complete page is cached on the run, and the next one is read ahead as soon as a page is served,
so the click that asks for it is answered from memory.

### Statement runs — SQL

```
plan     objects overlapping the range
prepare  read every object's footer in parallel, counting (phase `preparing`)
execute  COPY (SELECT * FROM (<statement>) LIMIT resultRowLimit + 1) TO <run>.parquet
count    only when the result was larger than kept: count(*) over the statement
```

A statement reads each of the materialization's tables by its **canonical name**, its dataset and
its name — `sales.orders` and `finance.payments_v2` for a materialization whose `orders` table went in
`sales` and `payments.v2`'s in `finance`, the name an engine on the REST catalog uses too — with the
logical columns, and as **`iceberg.<dataset>.<table>`** with the native ones: structs and lists as they
are, which is what an external engine sees (`SELECT customer.email, unnest(items).sku FROM
iceberg.sales.orders`). A bare `orders` resolves to nothing. A statement may read several of the
tables, across datasets, and join them:

```sql
SELECT o.status, count(*) AS payments, sum(p.amount) AS paid
FROM sales.orders o JOIN finance.payments_v2 p ON p.order_id = o.order_id
GROUP BY 1
```

A kept table (a removed topic's, handbook 11 §11.7) is read the same way, by its name in its dataset.

**Access is per dataset.** The person needs `data.query` on the dataset of every table the statement
names (`runs/store.ts::tablesNamed`), and of the run's own table for a browse run (`tablesRead`); the
route refuses anything else before a sandbox exists, naming the dataset (`requireOnDatasets`, 12
§12.2). The run remembers its datasets, so its rows, profiles, stream and export are refused the
moment the person loses `data.query` (`data.export` for an export) on any of them (`ownRun`).

Every table it names is bounded by the run's time range, so a statement is as cheap as the range is
narrow without saying so in its own `WHERE`; one it does not name is never planned or read
(`runs/view.ts::referencedTables`). `asOf` pins the run's own table — the one the tab shows — and the
others are read at their current snapshot. Before the statement runs, the sandbox creates a schema
for each dataset the statement's tables are in, holding a view for each of its tables, and attaches
an in-memory database, `iceberg`, holding the same schemas with the native views
(`runs/view.ts::statementViews`); all of it goes with the statement. `iceberg.<anything else>` is
refused: `there is no table iceberg.sales.x; this materialization's native tables are
iceberg.sales.orders and iceberg.finance.payments_v2` (`assertNativeReferences`). A name that is none
of the tables — a bare `orders` included — fails with DuckDB's own message, ending `This
materialization's tables are sales.orders and finance.payments_v2.` — DuckDB's nearest-name guess is
kept when it is one of them and dropped when it is a system table (`data/duckdb.ts::suggestTableName`).
Because a dataset is a DuckDB schema, it cannot be named after one of DuckDB's own catalogs or schemas
(`main`, `memory`, `temp`, `temporary`, `system`, `information_schema`, `pg_catalog`;
`NAMESPACE_KEYWORDS`); a table, inside its dataset, still can.

`tableNamesForTopics` in `shared/tableNames.ts` derives the names the way the control plane does, so
the create dialog shows them before anything is created, and the person can type others; the tables
themselves are the control plane's, read from the detail. Profiles and exports read a run's rows
through a view of the console's own, `__run_rows`, and the native scan is `__table`: a name always
starts with a letter, so neither a dataset nor a table can be named onto either. The result is stored as local Parquet, which makes paging it a
local `file_row_number` read and exporting it exact. Results beyond `CONSOLE_RESULT_ROW_LIMIT`
(1,000,000) are not kept, but the total is always counted, so every run ends with its exact row
count. `DESCRIBE`, `SUMMARIZE`, `PIVOT` and the rest are wrapped the same way as `SELECT`.

The statement starts on its own line inside the wrapper, so an engine error on line *N* is reported
on line *N + 1* at the same column; the console subtracts one and the editor marks the exact token.

### Admission and lifetime

At most `CONSOLE_RUN_CONCURRENCY` (4) runs scan at once; more wait. Only the scanning phase is
admitted — a run the manifest answers on its own never waits behind someone else's scan. A run nobody
has read for `CONSOLE_RUN_RETENTION_MS` (30 min) is discarded with its stored result; a live event
stream keeps it alive.

Cancelling aborts every DuckDB connection the run has open, so it stops at once rather than when its
current query finishes. Starting a run from the tab cancels the one before it if it is still working.

## 07.6 DuckDB

**Browsing** — pages, counts, profiles, exports — uses one in-memory instance per process:

| Setting | Why |
|---|---|
| `parquet_metadata_cache = true` | footers are read once per file for the life of the process |
| `enable_http_metadata_cache = true` | so are the HEAD responses every remote open starts with |
| `enable_external_file_cache = true` | byte ranges already read are served from memory, within `memory_limit` |
| `TimeZone = 'UTC'` | a value never depends on where the console runs |
| `threads = CONSOLE_DUCKDB_THREADS` (32) | remote reads block a thread per range request, so threads bound one query's requests in flight |

All of this is safe only because data files never change once committed.

**Statements** run in a **sandbox of their own** (`data/duckdb.ts::withSandbox`): a fresh in-memory
instance with the same settings, `allowed_directories` set to the run's own directory
(`<CONSOLE_RUN_DIR>/<run id>/`, which holds its result and nothing else) and
`allowed_paths` to the snapshot's data files, then `enable_external_access = false` and
`lock_configuration = true`. DuckDB enforces both instance-wide and will not undo them, so the
sandbox lives exactly as long as the statement: every file outside its access is refused by DuckDB
itself, however the statement names it — `"read_csv"('/etc/hosts')`, `read_csv_auto`,
`parquet_scan('gs://…')`, `iceberg_scan` all fail with a permission error (E10). A sandbox costs
~25–35 ms to set up and starts with cold caches; statement validation runs in one with no access at
all, over empty views.

Every query also holds one of Node's libuv worker threads for as long as it runs, waiting on GCS
included; `server/workerPool.ts` sizes the pool at startup (`RUN_CONCURRENCY × RUN_SCAN_CONCURRENCY
+ 32`).

Measured against the lab bucket from a laptop: binding a read of 200 files by field id takes 1 ms
where `union_by_name` took 1,400 ms (E3). On the E13 table — 102.6 M rows in 2,840 files, 17.2 GB
(handbook 13 §13.8) — a filter over its busiest day (564,837 rows in 5 files) counted in 0.63 s and
a statement grouping that day in 0.39 s; over all time a filter counted in 14.4 s, its first page
at 64 ms, and a `count(*)` statement took 21.1 s in its cold sandbox. Time-range estimates for five
presets answered in 1 ms.

Each unit of work runs on its own connection, so an interrupt is local to it. `direct` mode reads
`gs://` in place with an HMAC secret; `cached` mode mirrors files to local disk first and refuses
scans beyond `CONSOLE_CACHED_MODE_MAX_FILES`/`_BYTES` (a page reads one file at a time and is never
refused).

### Values

`toJsonValue` converts results to plain JSON: timestamps with a zone become ISO-8601 in UTC
(`2026-10-05T12:00:00.500Z`) so the browser can render them in the reader's zone; integers beyond
2^53 and decimals stay strings; lists and structs keep their shape.

## 07.7 Exports and profiles

An export is the finished run's result — every row, in the order shown — never a re-execution and
never the page on screen. CSV, JSON and JSON Lines stream a page at a time, so the download starts at
once and memory stays flat; Parquet is written by DuckDB to a temporary file first, since its footer
comes last. Exports stop at `CONSOLE_EXPORT_ROW_LIMIT` (1,000,000) and say so.

A page of an export holds 5,000 rows, which under a sparse filter can come from dozens of objects.
A page reads at most four objects at once: reading all of them together decompressed every column
of every object at the same time, and ran DuckDB out of memory exporting 33,805 rows from 171
objects of 81 columns.

JSON and JSON Lines come in two layouts (`shape`). **Flat** writes one key per column, as the grid
shows it: `{"customer.email": …, "items[].sku": ["A", "B"]}`. **Nested** rebuilds each row into the
record it came from (`shared/fieldPaths.ts::rowNester`):

| Column | Nested |
|---|---|
| `customer.email` | `{"customer": {"email": …}}` |
| `items[].sku`, `items[].qty` | `{"items": [{"sku": "A", "qty": 1}, {"sku": "B", "qty": 2}]}` — list columns under one array line up element by element |
| a `json` field | the value its JSON text encodes |
| Kafka metadata, an aggregate's alias | stays a top-level key |

A field whose shape changed over its history can have a column for each shape. Values are placed
before nulls,
overlapping columns merge, and equal values agree whatever their encoding (`3.01` and `"3.01"`), so
a record is never repeated; should two different values ever claim one place, the later keeps its
flat name rather than being lost. Measured on one day of the load-test topic (108,463 rows, 81
columns), every one of 8,785,503 values in the flat export was found at its place in the nested
one, and the nested file was 35% smaller. CSV and Parquet are always flat.

A profile summarises one column over a run's rows — its range and filters, or a statement's
result — under the short `CONSOLE_QUERY_STATS_TIMEOUT_MS` budget, reporting `partial` rather than
holding the panel open.

## 07.8 SQL guard

Applies to user-authored SQL only (`sql.ts`). One statement; must start with `SELECT`, `WITH`,
`DESCRIBE`, `SUMMARIZE`, `PIVOT`, `UNPIVOT`, `FROM`, `TABLE` or `VALUES`; no file-access functions
(`read_parquet`, `read_csv`, `read_avro`, `iceberg_scan`, `iceberg_metadata`, `iceberg_snapshots`,
`iceberg_table_properties`, …) or DDL/DML tokens outside strings and comments. It is defence in
depth and a clear error message: the real control is the statement's sandbox (§07.6), which refuses
every file but the run's own whatever the guard misses. A
materialization cannot be named after any word the guard refuses, so a valid table is always
queryable. The guard can be bypassed, though — see [12 §12.4](./12-security-model.md). See
[12 — Security model](./12-security-model.md).

## 07.9 The live tail

Every table is read with its records **past the snapshot** too, with the live tail on:
the newest ones, which its runtime has not committed yet, from the tail app (handbook 19).

### A run's tail

At the start of a run that reads such a table — browse or statement, unless `asOf` pins an older
snapshot or the request says `liveTail: false` — the console asks the tail app for the records past
the snapshot's `materializer.offsets` (`iceberg/snapshot.ts::committedOffsets`), with the person's
session and the console's credential (`data/tail.ts::fetchTail`). The answer is one Parquet file in
the table's own physical format; the run writes it into its own directory
(`<CONSOLE_RUN_DIR>/<run id>/tail-<table>-<n>.parquet`) and adds it as one more `DataObject`
(`runs/tail.ts::TableTail`):

| Field | From |
|---|---|
| `uri`, `bytes` | the file in the run's directory |
| `rows` | `X-Tail-Rows`, checked by DuckDB reading the file |
| `timeMin`, `timeMax` | `X-Tail-Time-Min`/`-Max`; `timeKnown: true` (absent means no row has a timestamp) |
| `tail: true` | so it sorts first among objects of the same time, and the status line can say it apart |

From there the engine needs nothing new. A browse run orders it first, counts it (from its row count
when it lies wholly inside the range, by reading it otherwise), prunes it by time and filters it like
any object; a statement's `<dataset>.<table>` and `iceberg.<dataset>.<table>` hold it, read by field
id like a data file; exports and profiles include it. The statement's sandbox already allows the
run's directory.

Snapshot and tail are disjoint and follow on from each other, so a run holds every record up to the
tail's `through` exactly once (handbook 19 §19.2). A table with nothing committed yet is read from the
tail alone.

**When the tail cannot be had**, the run is not failed: it reads committed rows and its `tail` says
why (`RunTail`, 08 §8.17a) — `unavailable` with a reason (`not_ready`, `rebalancing`,
`unavailable_for_snapshot`, `not_tailed`, `topic_unavailable`, `unreachable`, `timeout`,
`forbidden`, `unreadable`), `backlogged` with how far behind the runtime is, or `off` (live tail off
in this console or the control plane, or for the materialization on a control plane before 4.3; the
materialization stopped, or no tail app configured). `not_tailed` is the tail app's `404 live_tail_off`: it does not follow the table
yet — just after a resume, or before the table's first tick — so the next refresh asks again. A tail that does
not answer within `CONSOLE_LIVE_TAIL_TIMEOUT_MS` (3 s) is `timeout`; a file DuckDB cannot read, or
reads a different number of rows from, is dropped (`unreadable`).

### Refresh: appending what arrives

`POST /api/runs/:runId/refresh` brings a run up to date (`runs/store.ts::refreshRun`):

- a finished **browse run** whose table is still at the snapshot it is pinned to fetches only the
  records past its tail's `through`, writes them as the next file, counts it, and puts it in run
  order; every row's position is worked out again and the run's `revision` bumps, so a page read
  before is read again (the client keys pages by revision). The snapshot stays pinned; only its tail
  grows. A refresh while another is in flight waits for that one;
- a run is **replaced** — the same request, its range resolved again by the client — when the table
  has a newer snapshot (a commit landed), when it failed or was cancelled, when it is a statement
  (whose result is materialized: a statement's Auto re-run gets a new run every time), or when a
  switch turned its tail off — this console's, the control plane's, the materialization's, or a stop
  (`RUN_TAIL_SWITCHES`) — and it is on now. A tail off because no tail app is configured never is. The replaced run expires within a minute unless it
  is still read;
- nothing else replaces a run: a new run on the same snapshot would fetch the same tail, so a tail
  that cannot be had stays the run's status until the next commit;
- while an export reads a run, a refresh fetches nothing (`Run.holdRows`): rows prepended between the
  export's pages would repeat or skip.

A tail file gone from the run's directory (by hand) is dropped with every file after it: the run
pages its committed rows alone and its tail says `lost`.

### Live mode

The Data tab's browse view refreshes every `CONSOLE_LIVE_TAIL_REFRESH_MS` (1.5 s) while Live is on,
the tab is visible, the run has succeeded, the page is the first (newest first) and no row is open
in the drawer (`src/features/data/live.ts::liveHold`). A relative range ("Last 15 minutes") has an
open end, so the rows the tail appends are inside it, and is resolved again for the run that replaces
the old one after a commit, so the window slides. A range whose end has passed polls nothing. A
statement is never refreshed in place; its SQL panel's **Auto re-run** starts a new run five seconds
after the last one succeeded, never while one runs, and not while the editor holds another statement.

### The head peek

`GET /api/materializations/:id/tables/:table/head?rows=200` reads the newest records on the topic
from the tail app — committed or not — while the table is backlogged, writes them to a file of their
own, reads it through the table's logical view on the shared DuckDB instance, newest first, removes
the file, and answers rows and columns as a page does, with the peek's figures. It is never part of
a run, an export or a profile.

Measured on the lab (handbook 13 §13.8e): a record produced was on screen in the Data tab's Live
mode at a p50 of 0.93 s and a p95 of 1.6 s, and in a statement run at a p50 of 0.70 s and a p95 of
1.1 s.

