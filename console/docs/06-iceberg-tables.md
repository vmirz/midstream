# 06 — Reading Iceberg Tables and Schemas

Sources: `server/iceberg/{json,io,pointer,metadata,manifests,snapshot,table,schema}.ts`,
`server/storage/{registry,gcs,s3,azure,profile,uris}.ts`, `server/data/{catalog,logical}.ts`,
`server/schema/diff.ts`, `server/routes/{schema,table}.ts`.

The console reads each of a materialization's Iceberg tables itself — one per source topic: its
pointer, metadata and manifests from object storage, through the storage profile that covers its
location (GCS, S3 or Azure, 07 §07.10) — and its data files through DuckDB (07). It needs the
control plane only to learn the tables and where each is (`GET /materializations/{id}` →
`materialization.tables` and `tables[].location`) and for the schema projections. Experiment E5 checked the reader against PyIceberg: identical file sets, row counts,
sizes, bounds, offsets and projections.

## 6.1 From pointer to files

```
<location>/metadata/catalog-pointer.json   → metadata_location           (pointer.ts)
<metadata_location>                         → schemas, snapshots, refs     (metadata.ts)
snapshot.manifest-list (Avro)               → the snapshot's manifests     (manifests.ts)
each manifest (Avro)                        → data files, with counts and column bounds
```

| Step | Module | Notes |
|---|---|---|
| Read an object | `io.ts::readObject` | `gs://`, `s3://` or `abfss://` through the storage profile whose scope is the longest prefix of the URI (`storage/registry.ts::profileFor`) — GCS's JSON API, S3's `GetObject`, Azure's blob download; a local path or `file://` from disk (the test fixtures). An object no profile covers is a `503 storage_not_configured`, never read. `setObjectReader` is the test seam, and each profile takes reader seams of its own |
| The pointer | `pointer.ts::readPointer` | `null` when there is none: not created yet, or dropped |
| The metadata file | `metadata.ts::readMetadata` | Parsed with `json.ts::parseJson`, which keeps every `snapshot-id` exact (below). A metadata file never changes, so each is parsed once (`table.ts::metadataAt`, 256 kept) |
| The manifest list and manifests | `manifests.ts` | Decoded with `avsc`: 64-bit `long`s as `BigInt`, null/deflate/zstandard codecs; status, sequence number and snapshot id inherited per the spec when a manifest entry leaves them out |
| A snapshot's files | `snapshot.ts::snapshotCatalog` | Every live data file as a `DataObject` (below); cached per snapshot (64), and the manifests behind it per path (5,000), so the next snapshot reads only the manifests it added |

**Reading object storage.** Every backend reads alike (`storage/profile.ts::withRetries`): a read is
retried on `408`, `429` and `5xx` answers, failed connections and a checksum that does not hold,
three attempts in all, each given 30 s for a metadata object and 10 minutes for a data file; a `404`
is final, and the table reader takes it as a table not created yet. The SDKs' own retries are off,
so attempts never multiply.

| Profile | Reads with | Checked against |
|---------|------------|-----------------|
| `gcs` | a plain GET of GCS's JSON API (`?alt=media`, `Cache-Control: no-store`) with the client library's endpoint and credentials — the profile's key file, else ADC — and, as the library does, without credentials for an emulator (`endpoint`) | the CRC32C GCS stored, for every object not stored compressed |
| `s3` | `@aws-sdk/client-s3` `GetObject` with `ChecksumMode: ENABLED`, the profile's keys or the SDK's default chain | the object's checksum when it carries one, verified by the SDK as the body is read |
| `azure` | `@azure/storage-blob` download: a connection string, the account key, a service principal or `DefaultAzureCredential`; an emulator's `endpoint`, the account appended (`<endpoint>/<account>`), as the account URL | the blob's MD5 when it carries one |

Not GCS's library `download()`: its stream plumbing gave every download more listeners than Node
allows by default, and printed `MaxListenersExceededWarning` twice for every object — every few
seconds while a Data tab follows a table. A cached profile's data files go the same way, into a file
beside their place in the cache that is renamed into it once its checksum holds
(`storage/profile.ts::writeWhole`), so an interrupted download never leaves part of a file that the
next scan would read as all of it.

**64-bit snapshot ids.** Iceberg snapshot ids use all 64 bits, and `JSON.parse` turns them into
doubles that differ from the real id. `parseJson` uses `JSON.parse`'s source-text access to keep
any integer beyond 2^53 — and every key ending in `snapshot-id` or `snapshot_id` — as its exact
decimal text. Snapshot ids travel through the console and its API as strings. The control-plane
client parses the control plane's answers the same way.

### A `DataObject`

| Field | From | Used for |
|---|---|---|
| `uri`, `name`, `bytes` | the manifest entry | reading, sizing |
| `rows` | `record_count` | exact totals and row positions with no reads |
| `committedAt` | the snapshot that added it | ordering, "newer data available" |
| `partition`, `offsetStart`, `offsetEnd` | bounds of `_kafka_partition` (field 2) and `_kafka_offset` (3) | the Kafka partition and offsets of a file that holds one partition; null for a compacted file spanning several |
| `timeMin`, `timeMax` | bounds of `_kafka_timestamp` (1), microseconds floored to milliseconds | pruning by time range |
| `timeKnown` | bounds present, or every value null | telling "no timestamps" from "unknown" |
| `partitionKey` | the entry's partition | `2026-10-17` (day), `2026-10-17-10` (hour), `2026-10` (month), `__null__`, or null when unpartitioned — the runtime's own names |

A snapshot's catalog also carries its sequence number, commit time, operation, schema id, summary,
the **committed offsets** (`materializer.offsets`, walking back past a snapshot without them — an
external commit), `valid_through`, and how many **delete files** it has. The materializer writes
none; an engine with `catalog.write` might, and the console refuses to read such a snapshot rather
than show rows a delete removed.

## 6.2 Freshness

Reading the pointer is the only way to learn there is newer data, so freshness is about the pointer
(`table.ts::loadTable`):

| Asked by | Pointer read no older than |
|---|---|
| a run (`fresh`) | `CONSOLE_SNAPSHOT_CACHE_MS` (2 s): a run includes what was committed moments before Run was pressed |
| the Data tab's summary and estimates | `CONSOLE_TABLE_METADATA_FRESH_MS` (5 s); up to `CONSOLE_CATALOG_MAX_STALE_MS` (5 min) is answered at once and refreshed in the background |

Records newer than the pointer's snapshot are not in the table yet; with the live tail on, a run
reads them from the tail app beside the snapshot, so a Live view is seconds behind the topic rather
than a commit behind it (07 §07.9). Live re-reads the pointer at each refresh, through the 2 s cache,
and starts a new run when it names a new snapshot; the summary, which the Data tab reads every
30 s, follows it.

Everything behind the pointer is immutable — metadata files, manifests and data files are never
rewritten — so nothing else needs a freshness rule, and DuckDB may cache any of it for the life of
the process (07). Measured from a laptop against the test cluster's E13 table (handbook 13 §13.8), mid-load
at 3,563 files: pointer, metadata and every manifest on a cold BFF in 1,758 ms; warm, 41 ms; after a
new commit, 198 ms, reading only the snapshot's new manifests. On the finished table — 102.6 M rows
in 2,840 files — the Data tab showed its exact count 1.9 s after a cold console opened it and its
first rows at 3.2 s; warm, 274 ms and 459 ms.

## 6.3 Schemas: the projection and the table

Two readings of a schema exist, and the console uses both:

- **The projection** — `GET /materializations/{id}/tables/{table}/schema` from the control plane
  (handbook 05 §5.5), one entry per field with its logical name, physical path, type, state and the
  schema it appeared in. Every table has its own, evolving on its own. The Schema tab shows it; the
  live store keeps the current one per table and notices a new `schema_id` (05).
- **The table's own schema and fork map** — from the metadata file, for reading data: which field
  ids exist, their types, and which fields form one logical column (`iceberg/schema.ts`). That is
  what the Data tab and every run use: the schema current when the table was read, even for a run
  pinned to an older snapshot. Nothing is ever dropped, so it holds every field the older snapshot
  has, and a field added since reads as NULL there.

`iceberg/schema.ts` is a TypeScript port of the control plane's `lakehouse/schema/index.py` and
`render.py`: it walks the schema with the fork map (`materializer.fork-map`) and names every field
as the projection does — record keys, fork families (`total`, `total__v2`), JSON collapses
(`total__json`), list elements as `items[].sku`. A test compares its projection of the fixture
table with the control plane's, field for field (`iceberg.test.ts`).

The same metadata says what the table masks: `materializer.protection` (control-plane handbook 22
§22.6), which `parseProtection` reads into a map from field id to its kind and mask — fields protected now
only, not those written plain again. Each logical column a run returns carries
`protection: {kind, mask}` when the field behind it is protected, so the Data tab marks it from the
table itself, whatever the materialization's policy says now. A property this console cannot read is
treated as nothing protected (`server/data/protection.test.ts`, against `testdata/protection.json`,
which the control plane's own code writes).

### The logical columns

`data/logical.ts::logicalColumns` turns the schema into the columns people see:

| Rule | Why |
|---|---|
| A fork family is one column: `COALESCE` of its members, newest first | rows written before a fork hold the value in the older field |
| When the newest member is a JSON collapse, older plain members are cast to JSON text (`to_json`) | a collapsed family reads as text from then on |
| Forks that disagree are cast to one type: numbers to `DOUBLE`, anything else to text, keeping list depth | casting a number to text breaks every range filter and sort, so text is the last resort |
| A value and a list of the same key stay apart, the list as `name[]` | they cannot be one column |
| Struct fields are reached with `("customer")."email"`; list elements with `list_transform(x, lambda __e1: (__e1)."sku")` | DuckDB's nested access, quoted so any key works |
| Metadata columns keep their names: `_kafka_timestamp`, `_kafka_partition`, `_kafka_offset`, `_kafka_key`, `_kafka_headers`, `_materialized_at`, `_unmapped` | `shared/fieldPaths.ts::METADATA_COLUMNS` |

Data files are read by **field id**, not by name: `readParquet` passes DuckDB a `schema` map of
every field id to its name and type, with `default_value: NULL`. A file written before a field
existed reads that field as NULL, a renamed or forked field is matched by its id, and — the point
of it — DuckDB binds the read without opening a footer: 1 ms for 200 files where
`union_by_name` took 1,400 ms (E3).

### When the data had each field

`iceberg/presence.ts::tablePresence` answers the Schema tab's dates (`GET …/schema/timeline`, 08
§8.12a): when each field first and last held a value, by the records' Kafka timestamps. It reads
only what the snapshot catalog reads — the snapshot's manifests, through `snapshotManifests`, with
the same location checks — and never a data file. Every live data entry counts each column's values
and nulls (a list's elements included) and bounds the file's `_kafka_timestamp`, so a field holds a
value in the file when `value_counts − null_value_counts > 0`. Across those files the first record
with a value lies between the smallest lower bound and the smallest upper bound, the last between
the largest lower and the largest upper. Structs, lists and maps have no statistics and are filled
in from their fields, walking the current schema, which holds every field the table ever had.

The answer is as precise as the files: a UTC day in a day-partitioned table, an hour in an hour's,
up to a month in a month's. Each manifest's aggregate is kept in a `WeakMap` keyed by its cached
entries, so it is computed once per manifest and goes when the manifest leaves the cache. On a test cluster
(2026-10-23, from a laptop) a table the console had not read yet answered in 260–390 ms, and in
1–2 ms once its manifests were cached.

Versions are numbered in the order the runtime met each shape, not the order the topic's records
had them: a backfill reads partitions at their own pace, and schema 0 already holds what discovery
sampled from the newest records beside the oldest. So the client dates a version by its data
(`src/features/schema/timeline.ts::versionDataSpans`, the earliest first value among the fields it
introduced) and labels schema 0 *Initial · sampled* rather than giving it a date.

## 6.4 Schema diffs

`GET /api/materializations/:id/tables/:table/schema/diff?from=&to=` compares two of a table's schemas
(`server/schema/diff.ts`). `to` defaults to the current schema and `from` to the one before it;
schema 0 diffs against nothing. Both come from the control plane's archive of projections, so every
replica and every console sees the same history.

Fields are matched by **field id**, which never changes for the life of a field: a rename would keep
it, a fork is a new field with an id of its own, nothing is ever dropped. So:

| Kind | Meaning |
|---|---|
| `added` | a field only `to` has |
| `forked` | a new member of an existing family (`fork_version` ≥ 2, or `fork_of` set) |
| `collapsed` | the family's JSON field, or a member that became collapsed |
| `type_widened` | an Iceberg type promotion (`int` → `long`, `float` → `double`) — never made by the materializer, which forks a field instead; kept for tables an engine changed |
| `required_changed` | nullability changed |
| `superseded` | a member a newer fork replaced |
| `removed` | a field only `from` has — never produced by the materializer, kept for tables an engine changed |
| `unchanged` | the rest |

The metadata columns are left out. Entries sort by kind, then name. The other schema routes pass
the control plane's answers through: `…/tables/:table/schema`, `…/schema/versions`,
`…/schema/versions/:schemaId`. A `:table` the materialization does not have is a `404` naming its
tables.

## 6.5 The Table tab's view

`GET /api/materializations/:id/tables/:table` (`routes/table.ts`, `materializations.view_details`)
joins the control plane's record of that table (handbook 04 §4.6.4) with what the console reads from
the table itself:

| Part | From |
|---|---|
| `record` | the control plane: state (written, kept, being dropped), location, totals, offsets, maintenance history and the job running now, last error |
| `metadata` | the table: the newest 200 snapshots with their operation and summary, partitions with files, bytes, records and small files (`compactionDue` once a partition has `compaction_min_input_files` of them that the maintenance app's planner would take now: what is still being written waits for the hot partition delay, `server/iceberg/compaction.ts`), totals, the partition spec and sort order as text, properties — without the fork map, `materializer.protection` and `schema.*` for someone who may not see the dataset's schemas |
| `metadataError` | why the table could not be read, when it could not |
| `connection` | the catalog URI for the Connect panel, whose snippets name the table `lake.<dataset>.<table>`; the panel shows only for someone with `catalog.read` on the table's dataset |
| `storage` | the console's storage profile for the table's location (`routes/table.ts::tableStorage`): its name, kind, scope, endpoint, region, path style, mode and Azure account — never a credential, nor which ones the console holds; `null` when no profile covers the location. The Connect panel's snippets follow the location's scheme with it: DuckDB's `s3` secret with the endpoint, path style and TLS the profile says, an `azure` secret, or a `gcs` HMAC secret; PyIceberg's `s3.*` or `adls.*` properties; Spark's `S3FileIO`, `ADLSFileIO` or `GCSFileIO`; Trino's native S3, Azure or GCS file system — every credential a placeholder (`src/features/detail/connect.ts`) |

Small-file thresholds come from the runtime settings (`compaction_small_file_bytes`,
`compaction_min_input_files`, `compaction_hot_partition_delay_minutes`), the catalog URI from the control plane's `catalog_base_url`, both
read with the service credential and cached for a minute. `POST …/maintenance` (`materializations.maintain`)
forwards a maintenance request, for the table the tab shows (`{ job, table }`; without `table` the
maintenance app runs the job on every table, kept ones included).
