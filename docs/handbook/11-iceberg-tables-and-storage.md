# 11 — Iceberg Tables and Storage

Every materialization writes one Apache Iceberg v2 table per source topic in its destination bucket,
each in a **dataset** — its Iceberg namespace — chosen when the topic is picked: a materialization of
`sales.orders` and `finance.invoices` may write `sales.orders` and `finance.invoices`, read in SQL by
those names. This document covers what is in the bucket, how a table
is defined, how a commit works without a catalog server (the GCS-pointer catalog), what a snapshot
records, how files are compacted and removed, how tables are created and dropped, the REST catalog
external engines use, and how to read a table by hand.

Source: `lakehouse/` — `tables.py` (definition), `pointer.py`, `catalog.py`, `io.py`, `gcs.py`,
`files.py`, `commits.py`, `offsets.py`, `maintenance.py`, `lifecycle.py`, `rest.py`;
`web/catalog.py` (the REST facade); `services/workflow.py` (`create_table`, `drop_table`).

> 1.0 replaced the 0.x Parquet lake — `materialization_id=…/schema_version=…/ingest_date=…/` paths,
> `_runtime_state/` commit markers and the control plane's `_manifest/` — with Iceberg tables. There
> is no migration: a 0.x materialization is deleted and created again (spec §16, 12 §12.13).
>
> 2.0 lets one materialization read several topics, each into a table of its own, and made the
> materialization the tables' namespace. A 1.0 materialization (one table directly under the
> materialization's directory) is likewise deleted and created again.
>
> 3.0 puts tables in datasets: a table's namespace is its dataset, any number of materializations'
> tables share a dataset, and the storage is laid out by materialization id. A 2.0 materialization is
> deleted and created again (16 §16.6).

## 11.1 Layout

A materialization's tables live under `<destination_uri>/<storage prefix>/<materialization id>`
(`materialization_location`): the deployment's storage prefix (`CONTROL_PLANE_STORAGE_PREFIX`,
the release name by default, which keeps deployments sharing a bucket apart) and the materialization's id,
which names nothing else, so a table dropped and created again elsewhere never meets a half-deleted
location. Each table is the directory named after it below (`table_location`). The dataset exists only
in the catalog, not in the bucket:

```
gs://my-lake/midstream/mat-5c8ed527453d/
├── finance_invoices/           finance.finance_invoices · topic finance.invoices
└── refunds/                    sales.refunds            · topic sales.refunds
```

**Table names** are chosen for each topic when it is picked, and default to one derived from the
topic (`table_names.py`): lower case, every run of characters other than letters and digits becomes
`_`, leading and trailing `_` go, a name that would start with a digit takes `t_`, one that would be a
reserved SQL word takes `_topic`, at most 63 characters. A name is unique in its dataset across the
deployment (the directory claims it, 18 §18.16) and in its materialization, so the directory names
below a materialization never collide. A table's dataset and name are fixed once it exists. One table:

```
gs://my-lake/midstream/mat-5c8ed527453d/finance_invoices/
├── data/
│   ├── ts_day=2026-10-17/
│   │   ├── p03-o0000000100-0000000199-<member>-<seal>-0.parquet     runtime-written, one Kafka partition
│   │   ├── p03-o0000000200-0000000299-<member>-<seal>-0-r2.parquet  … masked under protection revision 2
│   │   └── compact-<job>-<n>-<uuid>.parquet                         a compaction's output
│   └── ts_day=__null__/                                             rows without a Kafka timestamp
└── metadata/
    ├── catalog-pointer.json                                         the catalog (§11.2)
    ├── version-hint.text                                            best effort, for path-based readers
    ├── 00042-<uuid>.metadata.json                                   one per commit
    ├── snap-<snapshot id>-<n>-<uuid>.avro                           manifest lists
    └── <uuid>-m<n>.avro                                             manifests
```

Hour granularity uses `ts_hour=2026-10-17-10/`, month `ts_month=2026-10/`; `none` writes everything
under `data/`.

## 11.2 The pointer: a catalog with no server

`metadata/catalog-pointer.json` is the whole catalog for a table (`pointer.py`):

```json
{"format": 1, "identifier": ["finance", "finance_invoices"], "location": "gs://…/mat-5c8ed527453d/finance_invoices",
 "materialization_id": "mat-4f0c21a9be37", "table_uuid": "…",
 "metadata_location": "gs://…/metadata/00042-….metadata.json",
 "previous_metadata_location": "gs://…/metadata/00041-….metadata.json",
 "sequence": 42, "updated_at": "2026-10-19T03:43:22.501Z", "updated_by": "midstream-mat-…-app-0"}
```

**Commit protocol** (`catalog.py`, spec §5.2): read the pointer and its object generation; validate
the commit's requirements against the metadata it names; write the new metadata file create-only
(`if_generation_match=0`); replace the pointer on the condition that its generation has not moved
(GCS compare-and-swap). A moved pointer raises `CommitFailedException` (HTTP 412 underneath) and the
metadata file just written is deleted. A pointer can therefore never move backwards, and two writers
can never both win.

`commit_once` commits a transaction exactly once — without PyIceberg's automatic retry, which
re-runs the snapshot producer blindly and could add a file twice. Callers retry by reloading and
deciding again (10 §10.6).

GCS throttles more than about one update per second to one object (experiment E11), so commits to a
table are spaced by at least 1 s. After each commit `version-hint.text` names the new metadata file
(best effort), so path-based readers such as DuckDB's `iceberg_scan(<location>)` find the latest
version without a catalog (E12).

Who may move a pointer: the runtime's committer (appends, schema), the maintenance app (compaction,
expiry), the control plane (`create_table`, `drop_table`), and an external engine through the REST
facade with `catalog.write` (§11.8). All of them go through the same compare-and-swap. Every table has a pointer of its own, so
the tables of one materialization commit independently: one table's failing commits never hold
another's back (10 §10.6).

`find_latest_metadata` can rebuild a lost pointer from the newest `*.metadata.json` whose
`table-uuid` matches (12 §12.13).

## 11.3 The table definition

`lakehouse/tables.py`:

| Id | Column | Type | Notes |
|---|---|---|---|
| 1 | `_kafka_timestamp` | `timestamptz` | the record's Kafka timestamp; null when Kafka gave none; the partition source |
| 2 | `_kafka_partition` | `int` (required) | |
| 3 | `_kafka_offset` | `long` (required) | |
| 4 | `_kafka_key` | `string` | UTF-8, or `b64:` + base64 |
| 5 | `_kafka_headers` | `map<string, string>` (key 8, value 9) | values UTF-8, or `b64:` + base64 |
| 6 | `_materialized_at` | `timestamptz` (required) | when the runtime read the record |
| 7 | `_unmapped` | `string` | JSON object of values no column could take (09 §9.8) |
| 10+ | payload | inferred | 09 |

- **Partition spec** (spec id 0): `day(_kafka_timestamp)` named `ts_day` (field id 1000), or
  `hour(…)` as `ts_hour`, or `month(…)` as `ts_month`, or unpartitioned, as the materialization's
  `partition_granularity` says (default from the `default_partition_granularity` setting, `day`).
  Fixed at creation (`lakehouse/tables.py::partition_spec`). A month's partition value is the months
  since January 1970, as Iceberg's transform computes it, written `2026-10`
  (`lakehouse/files.py::partition_text`). Month suits a topic writing a few megabytes a day, where
  day partitions would each hold one small file; hour, one writing many gigabytes an hour. There is
  no week: Iceberg's time transforms are year, month, day and hour, and a week cannot be built from
  them, nor read as one by engines that prune on the transform.
- **Sort order** (order id 1): `_kafka_timestamp ASC NULLS FIRST` — advisory, honoured by compaction.
- **Properties**:

| Property | Value |
|---|---|
| `write.parquet.compression-codec` | `zstd` |
| `write.target-file-size-bytes` | 268435456 |
| `write.metadata.delete-after-commit.enabled`, `write.metadata.previous-versions-max` | `true`, `20` — old metadata files are deleted after commits |
| `commit.manifest-merge.enabled`, `commit.manifest.min-count-to-merge`, `commit.manifest.target-size-bytes` | `true`, `50`, 8 MiB — merge-append keeps the manifest count bounded (E2) |
| `commit.retry.num-retries` | `6` (for external engines; the materializer commits once) |
| `history.expire.max-snapshot-age-ms`, `history.expire.min-snapshots-to-keep` | from the runtime retention settings at creation |
| `materializer.materialization-id`, `.source-topic`, `.created-by`, `.created-at`, `.package-version`, `.partition-granularity` | provenance; `.source-topic` is this table's topic |
| `materializer.fork-map` | the fork map (09 §9.5), updated by every schema commit |
| `materializer.protection` | what the table masks (22 §22.6): the policy and its revision, the protection key's id, every field that is or was protected with its kind, mask and schema ids, and the last 100 policies. Written at creation, by every schema commit that adds fields, and by a property-only commit when a policy is recorded; absent for a table of a materialization from before data protection until an edit gives it one |

The metadata is built directly as `TableMetadataV2` (`new_table_metadata`): PyIceberg's own
`new_table_metadata` re-assigns field ids, which would move the metadata columns off their fixed ids.

## 11.4 Snapshots and their summaries

Every runtime commit is one **merge-append** snapshot (`commit_append`). Besides Iceberg's own
summary fields (`added-data-files`, `added-records`, `total-records`, `total-data-files`,
`total-files-size`, …), each carries the materializer's (`offsets.py`):

| Summary property | Value |
|---|---|
| `materializer.offsets` | `{"0": 9, "1": 11}`: per source partition, the inclusive last offset such that every record at or below it is in the table or needs no row — **the runtime's checkpoint** |
| `materializer.event-times` | per partition, the latest committed record's Kafka timestamp (ms) |
| `materializer.valid-through` | the minimum of those: every record before it is in this snapshot, on every partition that has committed — "is my record there yet?" |
| `materializer.commit-id`, `materializer.committed-at`, `materializer.runtime` | provenance |
| `materializer.files`, `materializer.records`, `materializer.operation` | what this commit added: `append`, `compaction` or `reprotect` |
| `materializer.reprotect-job`, `materializer.reprotected-files`, `materializer.protection-revision` | a reprotect's: its job id, the files it replaced, and the revision it masked them under (22 §22.14) |

Maintenance snapshots (a compaction's or a reprotect's `replace`) carry the offsets forward unchanged;
a snapshot without them (an external writer's) is skipped by walking back to its parent
(`committed_offsets`).

A schema change is a metadata commit without a snapshot (`commit_schema`), and so is a recorded
protection policy (`commit_protection`: the property alone, asserting the current schema id).

Metadata grows about 0.9 KB per snapshot with 5 partitions' offsets, 1.3 KB with 32 (E11/E2); with
snapshot expiry at 72 hours and a commit every 30 s that bounds `metadata.json` near 11 MB, read whole
by every commit and every load — the reason for `snapshot_keep_all_minutes` (one snapshot an hour is
kept beyond it).

## 11.5 Data files

The runtime writes Iceberg-conformant Parquet itself (`writer/seal.py`, its rows from
`lakehouse/rows.py::build_row`): field ids in the schema,
one file per partition value per seal, zstd, statistics on. Committing a file is a matter of
describing it (`files.build_data_file`): size, record count, partition value and per-column
statistics read from the Parquet footer alone (one or two range requests,
`FOOTER_PREFETCH_BYTES` 256 KiB). PyIceberg's `add_files` is not used — it is meant for files
without field ids and would set a name mapping on the table.

Bounds are kept for every primitive column, nested ones in structs included (`statistics_plan`);
readers rely on ids 1–3: the console decodes them from manifests to know each file's timestamp
span, Kafka partition and offsets without opening it (console 07).

Names: `p<partition>-o<first offset>-<last offset>-<member>-<seal id>-<n>.parquet` for runtime files
(the seal id makes an upload idempotent: 10 §10.4) and `compact-<job id>-<n>-<random>.parquet` for
compaction outputs (the random part keeps a re-run from colliding with a failed run's orphan). A file
whose rows were masked under a protection revision other than 0 ends `-r<revision>.parquet`
(`files.protection_revision_of`), so which policy masked a file is known from the manifests alone.
A reprotect's output, `reprotect-<job id>-<n>-<random>-r3p1.parquet`, names its whole lineage, newest
first: masked under revision 3, from rows written under 1 (`files.protection_label_of`). Compaction
never mixes lineages in one output (22 §22.5).

## 11.6 Compaction, expiry and orphans

The maintenance app runs these for every table, written or kept, whatever the runtime is doing
(stopped included); scheduling and fault tolerance are in 10 §10.10, how the control plane ticks it in
07 §7.13. The storage view:

- **Compaction** rewrites small files into files of up to ~256 MiB in one `replace` snapshot
  (`maintenance.commit_compaction`). What live traffic still writes waits (10 §10.10): today or this
  hour until no commit has added to it for an hour, and this month's files each until they are an
  hour old, so a month-partitioned table is not left with a month of small files. A job takes the
  oldest partition with enough small files and,
  while they fit in `compaction_max_input_bytes` (1 GiB) and 2,000 files, the whole partitions after
  it — a backfill's past days several at a time — writing each partition into files of its own.
  PyIceberg 0.12 cannot summarize a `replace`, so the library computes the summary as an overwrite
  and stamps `operation: replace` (`_ReplaceFiles`). Before committing it checks every input is still
  in the current snapshot (`CompactionInputGone` otherwise — another compaction or an expiry got
  there first) and, partition by partition, that the outputs hold exactly the inputs' rows, inside
  the inputs' time bounds, and that no output lands in a partition no input is in
  (`CompactionMismatch`). Rows stay readable throughout: the inputs
  are live until the replace lands, and older snapshots keep referencing them after.
- **Snapshot expiry** removes snapshot entries from the metadata (`expire_snapshots`, one commit). It
  **deletes no files** (E7): the files only the expired snapshots referenced become orphans. Its
  report carries the new metadata file and snapshot count, which the table record takes at the same
  sequence number (05 §5.12).
- **Orphan cleanup** (`find_orphans`) lists `data/` and `metadata/` and deletes what no retained
  snapshot references (`referenced_files`: the current metadata file and those in its metadata log,
  and every retained snapshot's manifest list, manifests and data files — a manifest's `deleted`
  entries are not references) and is older than `orphan_min_age_hours` (24). The pointer, a dropped
  pointer and the version hint are never candidates, and an object whose age cannot be read is never
  deleted. Files
  a compaction replaced stay referenced by the snapshots before it until those expire, so time travel
  to them keeps working.
- **Reprotect** (22 §22.14) replaces each file holding in plain text what the table masks now by its
  rewrite, one for one, in one `replace` snapshot (`maintenance.commit_reprotect`) with the same checks
  as compaction's. Its producer (`_RewriteFiles`) keeps **no `deleted` manifest entry** of the files it
  removes: a deleted entry keeps the file's statistics, whose column bounds hold the very plain text the
  job masks, and the current snapshot would carry them on; the removal is still validated and the
  summary still counts it. With `purge`, once nothing is left to rewrite, `purge_before` expires every
  snapshot older than the current one (refs excepted, `min_snapshots_to_keep` ignored), deletes the
  files only those snapshots referenced - data files, manifests and manifest lists, whatever their
  age, since all were committed once - and then the table's other orphans past `orphan_min_age_hours`.

On the load-test topic, whose 2 years of event time spread every seal across up to ~50 days,
runtime files average ~3,000 rows before compaction; compaction brings a busy day back to a file or
two (spec §14: files per day ≤ rows per day ÷ 1 M).

## 11.7 Creating and dropping tables

**`create_table`** (07 §7.3; `lifecycle.create_materializer_table`, on a worker thread) creates one
table a pass, in the order of the topics, and queues itself again until every table exists: it
builds metadata version 0 with the table's schema 0 from discovery, writes it, and creates the
pointer with `if_generation_match=0`. It is idempotent: a pointer with the same `materialization_id`
means done; a foreign pointer fails the step (`table_location_in_use`). It persists the control
plane's `materialization-table:<id>:<table>` record (05 §5.12).

**`drop_table`** (replaces 0.x's destination cleanup) drops the tables one after another, each in
bounded passes with the same retry code; one operation record counts the passes and names the tables
already dropped (`tables_dropped`), so a redelivered or retried pass picks up where the last left off.
Only a table's own directory is ever deleted: the guard requires a location of the form
`<…>/<materialization id>/<table>`, the directory named exactly the materialization's id and the
table's a lowercase table name (`storage/objects.py::table_object_prefixes`).
Deleting a materialization drops every table; an update that removes a topic, or drops a kept
table, drops only the tables in state `dropping`, once no runtime member writes them (07 §7.9a).

**A table's lifecycle** (`MaterializationTable.state`): `active` while its runtime writes it;
`retired` once its topic is removed and the table kept — read-only, still in its dataset, the catalog
and the console, still maintained; `dropping` while it is being dropped; and, for a table dropped with
cleanup off, `dropped`: its files stay at its location (`catalog-pointer.dropped.json` keeps it
readable by path), and its record stays so this materialization never uses the name, and with it the
location, again — a new table there would take the old files for orphans. With cleanup on, a dropped
table's record goes, and a topic added later may take the name: its table is a new one at the same
location, with a new UUID. Either way the name is released in its dataset as soon as the drop is
asked for — the table left the catalog then — and another materialization may claim it.

| `destination_cleanup_enabled` | What a drop does |
|---|---|
| on (default) | delete `data/`, then `metadata/` except the pointer, then the pointer (`drop_table_objects_pass`) |
| off | rename the pointer to `catalog-pointer.dropped.json` (`retire_table_pointer`): the table leaves the catalog but stays readable by path |

## 11.8 The Iceberg REST catalog facade

The control plane serves the Iceberg REST catalog protocol (`lakehouse/rest.py`, `web/catalog.py`)
so external engines can attach the lake as a catalog. Engines configure the **catalog URI
`<control plane>/catalog`** (without `/v1`: clients append `/v1/…` themselves; E4) and a bearer
token — an access token (`tok_…`, 18 §18.15) or a session's.

| Method and path | Permission | Answer |
|---|---|---|
| `GET /catalog/v1/config` | `catalog.read` on a dataset | `{"defaults": {}, "overrides": {}, "endpoints": [...]}` — no `prefix`, no vended credentials |
| `GET /catalog/v1/namespaces`, `GET …/namespaces/{ns}` (and `HEAD`) | `catalog.read` on the dataset | one namespace per **dataset** the caller may read through the catalog; a locked one is neither listed nor found (`404`). Its `properties` carry the dataset's `description` |
| `GET /catalog/v1/namespaces/{ns}/tables` | `catalog.read` on it | the dataset's tables — whichever materialization writes each — created so far, written or kept; one being dropped, or dropped, is gone |
| `GET /catalog/v1/namespaces/{ns}/tables/{table}` (and `HEAD`) | `catalog.read` on it | `LoadTableResult`: metadata location and metadata |
| `POST /catalog/v1/namespaces/{ns}/tables/{table}` | `catalog.write` on it | commit through the pointer's CAS; `409 CommitFailedException` when another commit landed first; `400` for a commit that would move the table's files (below) |
| `POST …/tables/{table}/metrics` | `catalog.read` on it | `204`, dropped |
| `POST …/namespaces/{ns}/tables`, `DELETE …/tables/{table}`, `POST /catalog/v1/transactions/commit` | `catalog.read` | `501`: tables are created and dropped with materializations; datasets are made with `POST /datasets` and no namespace can be created through the catalog |

**Resolving a name** takes three keyed reads, never a scan: `dataset-name:<name>` gives the dataset
id, `dataset-table:<dataset_id>:<table>` (the claim) the materialization, and
`materialization-table:<materialization_id>:<table>` the table's record and location (`web/catalog.py`).
Engines list namespaces in order to load them, which is why a locked dataset is hidden here, while the
API and the console list it (18 §18.16).

A commit may change what a table holds, never where its files are: a `set-location` update, or
setting `write.data.path`, `write.metadata.path` or `write.py-location-provider.impl`, is refused
with `400 BadRequestException` before anything is written (`rest.py::_refuse_moving_files`). The
location is the materialization's — the runtime writes under it, the drop removes it, and the
console reads nothing outside it (console 12 §12.6).

Errors use the Iceberg shape clients parse: `{"error": {"message", "type", "code"}}`
(`catalog_errors` maps refused authorizations too). With `catalog_api_enabled` off every endpoint
answers 503. A loaded table is answered again from memory for `catalog_metadata_cache_seconds` (5),
so an engine planning many queries does not cost a GCS read each (a first load took ~150 ms in E4,
cached ones under 1 ms). The cache is keyed by the dataset, the name and the table's UUID: a name
freed and claimed again by another materialization's table is never answered with the old one's
metadata.

The facade vends **no storage credentials**: an engine reads data files with its own GCS access
(an HMAC key for DuckDB, ADC for PyIceberg). Verified with DuckDB 1.5.6 and PyIceberg 0.12 (E4, and
on a test cluster through the HTTPRoute with an access token); Spark and Trino follow the same protocol but
have not been run against it.

**Across domains.** The shared console serves the same protocol over every domain at
`<console>/catalog`, whose namespaces are `<domain>.<dataset>`, handing each request to the domain the
namespace names with the caller's token unchanged; and each domain's own catalog at
`<console>/d/<domain>/catalog` (20 §20.10). An access token made in any domain is good in every one
where its owner has an account (18 §18.15).

## 11.9 Reading a table by hand

DuckDB, by path (no catalog; the version hint names the latest metadata):

```sql
INSTALL iceberg; LOAD iceberg;
CREATE SECRET gcs (TYPE GCS, KEY_ID '<HMAC key id>', SECRET '<HMAC secret>');
SELECT count(*) FROM iceberg_scan('gs://my-lake/midstream/mat-bbc480ce42f0/orders');
SELECT * FROM iceberg_snapshots('gs://…/mat-bbc480ce42f0/orders');
```

DuckDB through the REST facade — each dataset is a namespace, so tables of any datasets and
materializations the token may read can be joined:

```sql
CREATE SECRET materializer (TYPE ICEBERG, TOKEN 'tok_…');
ATTACH 'warehouse' AS lake (TYPE ICEBERG, SECRET materializer, ENDPOINT 'https://<control plane>/catalog');
SELECT customer_id, unnest(items).sku FROM lake.sales.orders LIMIT 10;
SELECT o.status, sum(i.amount) FROM lake.sales.orders o JOIN lake.finance.invoices i USING (order_id) GROUP BY 1;
```

On a test cluster (3.0, DuckDB 2.0): `lake.sales.orders` and `lake.finance.finance_invoices`, written by two
materializations, joined to the expected 3,005 rows through a reader's token; a token limited to
`sales` saw only `sales`.

**Masked columns** read like any other: a hashed one holds 32 hex characters, equal for equal values
across every table of the deployment — and of every domain sharing the protection key — so it still
joins and counts (`count(distinct email)`); a partial one holds `****9848`-style fragments; a withheld
one is not there. What a table masks is in its `materializer.protection` property, which the catalog
returns with the table's metadata (22 §22.6).

PyIceberg:

```python
from pyiceberg.catalog.rest import RestCatalog
catalog = RestCatalog("lake", uri="https://<control plane>/catalog", token="tok_…")
rows = catalog.load_table("sales.orders").scan(row_filter="_kafka_offset >= 100").to_arrow()
```

`iceberg_scan` prunes data files by their manifest bounds: a one-day predicate read 1 of 2 files and
a predicate past the data none (E3). PyIceberg 0.12 reads a null list of structs as an empty list —
a record without `items` comes back with `items = []` — where DuckDB reads it as written
(14 §14.5). The console's Table tab shows these snippets with the
deployment's catalog URI filled in (console 11).

## 11.10 Failure modes

| Symptom | Cause | Recovery |
|---|---|---|
| A runtime reports `waiting_for_table` | the pointer is missing or unreadable | check `metadata/catalog-pointer.json` and the runtime's GCS credentials; a lost pointer can be rebuilt (12 §12.13) |
| `commit_failed`, repeatedly | the pointer CAS keeps losing (another writer), or GCS refuses writes | the committer's logs; an external engine committing in a tight loop must back off |
| Data files under `data/` that no snapshot lists | uploads no commit took (a covered or overlapping seal, a crashed member), a compaction that died before its commit | orphan cleanup removes them after `orphan_min_age_hours` |
| `metadata.json` grows | many snapshots retained | lower `snapshot_retention_hours` or `snapshot_keep_all_minutes` (17) |
| Delete files appear | an external engine with `catalog.write` committed row-level deletes | the console refuses to read such a snapshot (console 07); engines that apply deletes read it correctly |
