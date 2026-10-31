# 11 — Iceberg Tables and Storage

Every materialization writes one Apache Iceberg v2 table per source topic in its destination,
each in a **dataset** — its Iceberg namespace — chosen when the topic is picked: a materialization of
`sales.orders` and `finance.invoices` may write `sales.orders` and `finance.invoices`, read in SQL by
those names. A destination is a bucket in Google Cloud Storage or Amazon S3 (or an S3-compatible
server), or a container in Azure Blob Storage, with an optional prefix (§11.1). This document covers
what is in the bucket, how Midstream reaches each backend (the object store layer), how a table is
defined, how a commit works without a catalog server (the pointer catalog, on each backend's
compare-and-swap), what a snapshot records, how files are compacted and removed, how tables are
created and dropped, the REST catalog external engines use, and how to read a table by hand.

Source: `lakehouse/` — `tables.py` (definition), `store/` (the object store layer: `base.py`,
`registry.py`, `uris.py`, `gcs.py`, `s3.py`, `azure.py`, `file.py`, `memory.py`, `check.py`),
`pointer.py`, `catalog.py`, `io.py`, `files.py`, `commits.py`, `offsets.py`, `maintenance.py`,
`lifecycle.py`, `rest.py`; `storage/objects.py` (the drop); `web/catalog.py` (the REST facade);
`services/workflow.py` (`create_table`, `drop_table`).

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
gs://example-lake/midstream/mat-5c8ed527453d/
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
gs://example-lake/midstream/mat-5c8ed527453d/finance_invoices/
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

**Destinations.** The destination (`default_destination_uri`, 17 §17.3) names its backend by its
scheme, in the one form every engine reads natively, and every location below it begins with it: a
table's location is written into its metadata, its manifests and its data files' paths for good. A
materialization keeps the destination it was created with.

| Backend | Destination | A table's location |
|---|---|---|
| Google Cloud Storage | `gs://<bucket>[/<prefix>]` | `gs://example-lake/midstream/mat-5c8ed527453d/orders` |
| Amazon S3, or an S3-compatible server | `s3://<bucket>[/<prefix>]` | `s3://example-lake/prod/midstream/mat-5c8ed527453d/orders` |
| Azure Blob Storage | `abfss://<container>@<account>.dfs.core.windows.net[/<prefix>]` | `abfss://lake@exampleaccount.dfs.core.windows.net/midstream/mat-5c8ed527453d/orders` |
| the local disk, for tests and a laptop | `file:///<absolute path>` | `file:///tmp/lake/midstream/mat-5c8ed527453d/orders` |

One rule checks a destination wherever one is set (`lakehouse/store/uris.py::destination_problem`):
the Settings page's `destination_uri` kind, the create view (`409 destination_not_configured`,
04 §4.5.2) and, in the chart's own patterns, `controlPlane.settings.defaultDestinationUri`. A GCS
bucket is lowercase letters, digits, dots, hyphens and underscores; an S3 bucket 3 to 63 lowercase
letters, digits, dots and hyphens; an Azure container 3 to 63 lowercase letters, digits and single
hyphens, in an account of 3 to 24 lowercase letters and digits; each starts and ends with a letter or
a digit. A destination is at most 1,024 characters, its trailing slashes trimmed. Other spellings of
the same backends — `gcs://`, `s3a://`, `s3n://`, `abfs://`, `az://`, `wasb://`, `wasbs://`,
`adl://`, a web address — are refused with the form to use, so no table has two names in different
engines. `file://` is refused unless `CONTROL_PLANE_ALLOW_FILE_DESTINATIONS` is true, which the chart
never sets: every app and every runtime would need the same disk.

Changing the destination changes where new materializations go, nothing else. Every object is
reached through the store of its own URI's scheme (§11.1a), so a deployment whose destination moved
from a `gs://` bucket to an `s3://` one goes on writing, maintaining, tailing and serving its older
tables in GCS and its newer ones in S3, given credentials for both. A table never moves between
backends: its metadata names its files absolutely, so moving one is a new materialization.

An Azure location names the account's DFS endpoint, through which Spark, Trino, PyIceberg and DuckDB
read `abfss://`. Midstream's store and the console reach the same account through its Blob endpoint,
which every account has, with or without a hierarchical namespace; an account external engines read
should have one (§11.8).

## 11.1a The object store layer

Everything Midstream does to a bucket goes through `lakehouse/store/`: one interface, `ObjectStore`
(`base.py`), and a store per URI scheme — `GcsStore` (`gs://`, google-cloud-storage), `S3Store`
(`s3://`, boto3), `AzureBlobStore` (`abfss://`, azure-identity and azure-storage-blob 12.31 or later:
earlier releases name a chunked upload's blocks by their offsets, so racing create-only uploads
could commit mixed content), `FileStore` (`file:///`, tests) and `MemoryStore` (the test suite's,
which stands in for any scheme). `store_for(uri)` (`registry.py`) returns the process's one store for
the URI's scheme, built from the environment the first time it is asked for; a backend's library is
imported only then. Every store is blocking and thread-safe: callers on an event loop run it on a
worker thread. Nothing above the layer imports a storage library or branches on the backend — the
pointer and the catalog, PyIceberg's FileIO (`ObjectStoreFileIO`, `io.py`), footer reads, the
runtime's uploads and its committer, the maintenance app and its child processes, the tail app's
fallback seed, the control plane's create and drop and the REST facade each find a table's store by
its location.

| Operation | What it does |
|---|---|
| `get(uri, start=, end=)` | the bytes, whole or an inclusive byte range, and the version they were read at, from one request |
| `head(uri)` | the size, version and last-modified time; None when there is no such object |
| `put(uri, data, if_version=)` | writes the whole object: unconditionally (`None`), create-only (`ABSENT`), or only while it is still at a version — the compare-and-swap, of writers holding one version exactly one wins; returns the version written |
| `put_file`, `get_to_file` | the same, streaming a local file |
| `delete(uri, if_version=)` | True when the object is gone, False when the store knows it already was |
| `delete_many(uris)` | up to `delete_batch_size` deletes a request, each object's outcome reported, nothing retried |
| `list(prefix)` | every object under a prefix with its size, version and time, pagination hidden |

A **version** is an opaque string, compared for equality only and only with versions the same store
gave for the same object, without the quotes some answers put around an ETag (`normalize_version`):

| | GCS | S3 | Azure | file |
|---|---|---|---|---|
| A version | the object's generation | its ETag: for a single-part upload, the MD5 of its bytes | its ETag: random, new on every write | `<inode>-<size>-<mtime in ns>`, the mtime set by the store, at least 1 µs on from the last write in the directory |
| Create-only | `if_generation_match=0` | `If-None-Match: *` | `If-None-Match: *` | `os.link` of a temporary file, which fails if the target exists |
| Compare-and-swap | `if_generation_match=<generation>` | `If-Match: "<etag>"` | `If-Match: "<etag>"` | `os.replace`, under a process lock and an `flock` on the directory's `.midstream-store.lock` |
| The condition did not hold | 412; a 404 under a generation | 412 `PreconditionFailed`; 409 `ConditionalRequestConflict`, another conditional write in flight; a 404 under `If-Match` | 412 `ConditionNotMet`; 409 `BlobAlreadyExists`; 404 `BlobNotFound` under `If-Match` | — |
| A read and its version | one download, the generation from its answer | one `GetObject`, its ETag | one GET up to 8 MiB; past that 8 MiB chunks, each after the first under `If-Match`, the whole read again (three reads at most) when the blob changes | `fstat` of the open file |
| A large upload | the library's resumable upload | one streamed `PutObject`, up to 5 GB: the transfer manager cannot carry a condition | 8 MiB blocks (four in flight for a file), the condition on the block-list commit; a conditional one checks the ETag first and stages nothing when the condition already fails | a copy |
| One delete request | a JSON API batch of 100; one object against an emulator (`STORAGE_EMULATOR_HOST`), which cannot answer batches | `DeleteObjects` of 1,000 keys, quiet | a blob batch of 256; one object where an account refuses batches | no request: 100 at a time, each file deleted in turn |
| Deleting a missing object | False | True: S3 does not say | False | False |
| A conditional delete | yes | not offered: refused before any request | yes | yes |
| The time the orphan guard reads | creation time | `LastModified` | `Last-Modified` | mtime |
| Retries inside an operation | the library's | botocore's standard mode, six attempts in all | three retries, the first after about a second | — |

**Errors** are mapped once, in each store:

| The backend's answer | Raises |
|---|---|
| a condition that did not hold | `PreconditionFailedError`: for the pointer, another commit won or the write's own retry was refused (§11.2); for a create-only upload, the object is there |
| no such object | `ObjectNotFoundError`; `head` answers None, `delete` False |
| a bucket or container that does not exist, an endpoint or region naming the wrong server, refused credentials, an API version an emulator does not know: S3's `NoSuchBucket`; Azure's `ContainerNotFound`, `ResourceNotFound`, `AuthorizationFailure`, `AuthorizationPermissionMismatch`, `AuthenticationFailed`, `InvalidAuthenticationInfo`, `NoAuthenticationInformation`, `InvalidHeaderValue`; GCS's "The specified bucket does not exist." | `StoreConfigurationError`: never retryable, and never taken for a missing object, which would make a table look missing |
| anything else, a 403 from S3 or GCS included | `StoreError` with the HTTP status and the backend's code; no status when the request got no answer |

A HEAD's answer carries no code, so S3's `head` of an object in a missing bucket answers None. A
request that got no answer, a 408, a 429 or a 5xx is worth another try (`is_retryable_status`); any
other answer would come back the same.

**Credentials and endpoints** come from the environment, read when a process first meets a scheme.
The credentials are each library's own variables, which workload identities inject, so a pod with a
cloud identity needs no Secret; the endpoints are Midstream's `STORAGE_*` settings (`StoreSettings`),
which the Settings page shows read-only (17 §17.3). The chart renders both from `storage.gcs`,
`storage.s3` and `storage.azure` into the control plane, the maintenance and tail apps and the
runtime spec alike, Secrets by reference, never a value (02 §2.9a; each variable in 03 §3.3a):

| Scheme | Credentials, in order | Endpoint |
|---|---|---|
| `gs://` | `GOOGLE_APPLICATION_CREDENTIALS`, a mounted service-account key; else application default credentials (GKE workload identity) | Google's; `STORAGE_EMULATOR_HOST` names an emulator |
| `s3://` | `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with `AWS_SESSION_TOKEN` for temporary keys; else boto3's chain: IRSA, EKS Pod Identity, an instance role | `STORAGE_S3_ENDPOINT`, an S3-compatible server's `http(s)://host[:port]` (unset: AWS); `STORAGE_S3_REGION` (`us-east-1`); `STORAGE_S3_PATH_STYLE` (false; most S3-compatible servers want true) |
| `abfss://` | `AZURE_STORAGE_CONNECTION_STRING`, which must name the URI's account; else `AZURE_STORAGE_ACCOUNT_KEY`; else `DefaultAzureCredential`: AKS workload identity, a managed identity, or a service principal from `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_SECRET` | a connection string's own endpoints; else `STORAGE_AZURE_ENDPOINT/<account>` for an emulator; else `https://<account>.blob.core.windows.net` |
| `file://` | none | the path |

One store serves every bucket of its scheme, with the process's one identity and, for S3, its one
endpoint: `s3://` destinations on two different servers cannot share a deployment. A credential that
cannot be found fails the store's first operation with its library's own error; no store puts a key
or a connection string in a log or a message. The control plane, the runtimes and the maintenance app write,
and every app shares the chart's credentials: give that identity read, write, delete and list on the
destination (on S3 `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket`; a
conditional `PutObject` needs `s3:GetObject` as well). The console reads through storage profiles of
its own, with read-only credentials (console 07 §07.10).

**The conformance suite.** `python -m midstream.lakehouse.store.check <uri>`, run from a laptop or
inside any Midstream pod with the same environment, checks that a store keeps the contract the tables
rely on, under `<uri>/<run id>/`, and deletes everything it wrote:

| Check | Asserts |
|---|---|
| `create_only`, `racing_creators` | a second create-only write of a key is refused; of writers released together to create one key, exactly one wins, and the key holds its bytes |
| `read_with_version`, `compare_and_swap`, `missing_under_condition` | the version a read returns is accepted back, a stale one refused; a compare-and-swap on a missing key refused, whatever the server answered |
| `missing_object` | `get`, a ranged `get` and `get_to_file` of a missing key raise `ObjectNotFoundError`, and `get_to_file` leaves nothing behind |
| `concurrent_writers` | 16 threads, 25 rounds each, read a counter and write it back incremented under the version read: it ends at exactly the number of writes that succeeded, and no thread ever reads it go back |
| `pointer_never_repeats` | a `TablePointer` advanced 1,000 times never serializes to the same bytes twice (§11.2) |
| `range_read`, `head_and_list` | inclusive ranges; sizes, versions and times within 15 minutes of the local clock; a listing of 1,050 objects, more than a GCS or S3 page, each once |
| `file_transfer`, `large_object` | streamed uploads and downloads under the same conditions; with `--large-mb`, a large file up and down, timed, and create-only uploads of such files racing for one key |
| `delete`, `delete_many`, `conditional_delete` | deletes of present and missing objects, each reported; a stale conditional delete refused (skipped where none is offered) |
| `cleanup` | nothing is left under the run's prefix |

`--writers`, `--rounds`, `--race-rounds` (at most 6 keys unless given), `--list-objects`,
`--large-mb` and `--skip` adjust it; it exits 0 only when every check passes. Run it against a store
before pointing a deployment at it: the pointer protocol is only as safe as the server's conditional
writes, and a server that ignores `If-Match` loses commits. The unit tests run it over the memory and
file stores (`test_store_contract.py`); CI's Object stores job runs it, and each backend's own cases
(`test_store_live.py`), against SeaweedFS 4.51 and moto 5.2.5 (`s3://`), Azurite 3.37.0 (`abfss://`)
and fake-gcs-server 1.56.1 (`gs://`), and `STORAGE_LIVE_TESTS` runs the same against any base URI
by hand. S3 is verified against SeaweedFS and moto, not yet against Amazon S3 itself — moto
models AWS's answers, not its atomicity — and Azure against Azurite, not yet against a storage
account. fake-gcs-server ignores preconditions on DELETE and cannot answer batch requests, so against
it `test_store_live.py` skips the conditional delete (by hand, `--skip conditional_delete`) and the
store deletes one object per request; `test_store_gcs.py` checks both against GCS's own answers.

## 11.2 The pointer: a catalog with no server

`metadata/catalog-pointer.json` is the whole catalog for a table (`pointer.py`):

```json
{"format": 1, "identifier": ["finance", "finance_invoices"], "location": "gs://…/mat-5c8ed527453d/finance_invoices",
 "materialization_id": "mat-5c8ed527453d", "table_uuid": "…",
 "metadata_location": "gs://…/metadata/00042-….metadata.json",
 "previous_metadata_location": "gs://…/metadata/00041-….metadata.json",
 "sequence": 42, "updated_at": "2026-10-19T03:43:22.501Z", "updated_by": "midstream-mat-…-app-0"}
```

**Commit protocol** (`catalog.py::PointerCatalog`, spec §5.2): read the pointer and its version in
one request; validate the commit's requirements against the metadata it names; write the new metadata
file create-only; replace the pointer on the condition that its version has not moved — the store's
compare-and-swap (§11.1a): a generation precondition on GCS, `If-Match` on the ETag read on S3 and
Azure. A moved pointer raises `CommitFailedException` and the metadata file just written is deleted.
A pointer can therefore never move backwards, and two writers can never both win. Each table goes
through the store of its location's scheme, so one catalog serves tables on every backend.

**No two pointers alike.** An S3 ETag is the MD5 of the object's bytes, so a pointer whose bytes came
back to an earlier version's would let a stale `If-Match` through. They never do:
`TablePointer.sequence` rises by one with every commit (`advanced`) and `updated_at` moves with it,
and nothing but `pointer.py` writes a pointer. The conformance suite checks it against the real
serializer (`pointer_never_repeats`, §11.1a). A GCS generation and an Azure ETag are new on every
write in any case.

**Lost answers.** A failed pointer write is not always a lost race. Every storage library retries a
conditional write whose answer it never got — google-cloud-storage an upload under a generation
precondition, boto3 and azure-core after a connection error — and the retry of a write that landed is
refused as though another writer had won; a write that got no answer at all may have landed too. So
the catalog reads the pointer back after any failed pointer write (`_landed`) and goes by what it
finds:

| The pointer read back | The commit | The staged metadata file |
|---|---|---|
| names the staged file, moved on from the metadata this commit started from | landed: answered as committed, and the version hint written | kept |
| has moved on from the staged file, or its metadata's log names the staged file | landed: answered as committed; the newer commit's version hint is left alone | kept |
| has moved on from the metadata this commit started from to another commit's, or its metadata's log reaches back to that start without the staged file | lost: `CommitFailedException` | deleted |
| is gone: the table was dropped | lost | deleted |
| is still at the version this commit read: the write, or a conflicting one (S3's 409 `ConditionalRequestConflict`), may still be in flight | unknown: `CommitStateUnknownException` | kept |
| cannot be read, or names metadata that cannot be read; has moved on further than its metadata's log reaches back; or names the staged file after other metadata than this commit's start | unknown | kept |

The log is the current metadata's `metadata-log`, which reaches back
`write.metadata.previous-versions-max` (20) commits. An unknown outcome deletes nothing that a table
may reference: `commit_once` keeps the manifests it wrote, the runtime's committer keeps its files
pending and decides again against the table it reloads (10 §10.6), the maintenance app keeps a job's
outputs (10 §10.10), and the REST facade answers `503 CommitStateUnknownException` (§11.8). Orphan
cleanup removes what no commit took (§11.6).

The staged metadata file's own create-only write gets the same care: its name carries a fresh uuid,
so when the store refuses it and the object at that name has the size just sent, it is this write,
landed, and the commit goes on; no object there, or one of another size, fails the commit with
`CommitFailedException`. Data files are uploaded the same way (10 §10.4, §10.10).

`commit_once` commits a transaction exactly once — without PyIceberg's automatic retry, which
re-runs the snapshot producer blindly and could add a file twice. Callers retry by reloading and
deciding again (10 §10.6).

GCS throttles more than about one update per second to one object (experiment E11), so commits to a
table are spaced by at least 1 s on every backend. After each commit `version-hint.text` names the
new metadata file (best effort, an unconditional write), so path-based readers such as DuckDB's
`iceberg_scan(<location>)` find the latest version without a catalog (E12).

Who may move a pointer: the runtime's committer (appends, schema), the maintenance app (compaction,
expiry), the control plane (`create_table`, `drop_table`), and an external engine through the REST
facade with `catalog.write` (§11.8). All of them go through the same compare-and-swap. Every table has a pointer of its own, so
the tables of one materialization commit independently: one table's failing commits never hold
another's back (10 §10.6).

`find_latest_metadata` can rebuild a lost pointer from the newest `*.metadata.json` whose
`table-uuid` matches (12 §12.11).

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
statistics read from the Parquet footer alone (`files.read_parquet_footer`: the object's size from the
store's `head`, then one or two range reads, `FOOTER_PREFETCH_BYTES` 256 KiB). PyIceberg's `add_files`
is not used — it is meant for files without field ids and would set a name mapping on the table.

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
  are live until the replace lands, and older snapshots keep referencing them after. The child
  process downloads each input and uploads each output create-only through the table's store, under
  a name with a fresh id: an output whose upload the store refuses counts as written when the object
  at its name has the local file's size — only this job's own write, retried after its answer was
  lost, can be there — and fails the job otherwise. A commit that provably did not land deletes the
  outputs; one whose outcome is unknown (§11.2) keeps them, and orphan cleanup removes them if the
  table never took them.
- **Snapshot expiry** removes snapshot entries from the metadata (`expire_snapshots`, one commit). It
  **deletes no files** (E7): the files only the expired snapshots referenced become orphans. Its
  report carries the new metadata file and snapshot count, which the table record takes at the same
  sequence number (05 §5.12).
- **Orphan cleanup** (`find_orphans`) lists `data/` and `metadata/` through the table's store and
  deletes what no retained snapshot references (`referenced_files`: the current metadata file and
  those in its metadata log, and every retained snapshot's manifest list, manifests and data files —
  a manifest's `deleted` entries are not references) and is older than `orphan_min_age_hours` (24),
  by the time the store reports for it (§11.1a). The pointer, a dropped pointer and the version hint
  are never candidates, and an object whose age cannot be read is never deleted. Files
  a compaction replaced stay referenced by the snapshots before it until those expire, so time travel
  to them keeps working.
- **Reprotect** (22 §22.14) replaces each file holding in plain text what the table masks now by its
  rewrite, one for one, in one `replace` snapshot (`maintenance.commit_reprotect`) with the same checks
  as compaction's. Its producer (`_RewriteFiles`) keeps **no `deleted` manifest entry** of the files it
  removes: a deleted entry keeps the file's statistics, whose column bounds hold the very plain text the
  job masks, and the current snapshot would carry them on; the removal is still validated and the
  summary still counts it. Its child process reads and writes through the table's store as
  compaction's does, and its outputs are kept or deleted by the same rules. With `purge`, once
  nothing is left to rewrite, `purge_before` expires every snapshot older than the current one (refs
  excepted, `min_snapshots_to_keep` ignored), deletes the files only those snapshots referenced -
  data files, manifests and manifest lists, whatever their age, since all were committed once - and
  then the table's other orphans past `orphan_min_age_hours`.

On the load-test topic, whose 2 years of event time spread every seal across up to ~50 days,
runtime files average ~3,000 rows before compaction; compaction brings a busy day back to a file or
two (spec §14: files per day ≤ rows per day ÷ 1 M).

## 11.7 Creating and dropping tables

**`create_table`** (07 §7.3; `lifecycle.create_materializer_table`, on a worker thread) creates one
table a pass, in the order of the topics, and queues itself again until every table exists: it
builds metadata version 0 with the table's schema 0 from discovery, writes it create-only as
`metadata/00000-<table uuid>.metadata.json`, and creates the pointer create-only, both through the
store of the location's scheme (§11.1a). It is idempotent: a pointer with the same
`materialization_id` means done; a foreign pointer fails the step (`table_location_in_use`). A refused
write of the metadata file is this attempt's own, landed before its retry: its name carries the new
table's uuid. When the store refuses the pointer, the catalog reads it back: this materialization's
is the table, made by this attempt or an earlier one; another's fails the step; and none at all — a
write that may still land — raises `CommitStateUnknownException`, which fails the step for a resume
to finish (07 §7.3). It persists the control plane's `materialization-table:<id>:<table>` record
(05 §5.12).

**`drop_table`** (replaces 0.x's destination cleanup) drops the tables one after another, each in
bounded passes with the same retry code; one operation record counts the passes and names the tables
already dropped (`tables_dropped`), so a redelivered or retried pass picks up where the last left off.
A pass (`storage/objects.py::drop_table_objects_pass`) lists the table's objects through its store,
lazily, about a page in memory, and hands them to `delete_many` a `delete_batch_size` at a time, one
request each: a GCS batch of 100, an S3 `DeleteObjects` of 1,000, an Azure blob batch of 256
(§11.1a). It stops after `CLEANUP_PASS_MAX_REQUESTS` (100) requests' worth of objects — never fewer
than `CLEANUP_PASS_MAX_OBJECTS` (1,000): 10,000 objects on GCS, 100,000 on S3, 25,600 on Azure — or
after about `CLEANUP_PASS_MAX_SECONDS` (10 s), checked between requests. A delete that may succeed
later (no answer, 408, 429, a 5xx) is tried again after 1, 2 and 4 s while the pass has time, and ends
the pass if it still fails; one the store refuses for its object (403 for an object under a hold,
say) is left for the next pass; and a request the store turns away as a whole ends the pass, since
every request like it would fare the same. A pass whose every delete failed raises
`DestinationDeleteError`, which the workflow retries with its backoff (07 §7.10). Only a table's own
directory is ever deleted: the guard requires a location of the form
`<…>/<materialization id>/<table>`, the directory named exactly the materialization's id and the
table's a lowercase table name, judged on the key below the bucket or container on every backend
(`storage/objects.py::table_object_prefixes`); a location of no known scheme is refused before any
request. Deleting a materialization drops every table; an update that removes a topic, or drops a kept
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
| `POST /catalog/v1/namespaces/{ns}/tables/{table}` | `catalog.write` on it | commit through the pointer's CAS; `409 CommitFailedException` when another commit landed first; `503 CommitStateUnknownException` when the pointer write failed and nobody can tell whether it landed (§11.2); `400` for a commit that would move the table's files (below) |
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
so an engine planning many queries does not cost a storage read each (on GCS a first load took
~150 ms in E4, cached ones under 1 ms). The cache is keyed by the dataset, the name and the table's
UUID: a name freed and claimed again by another materialization's table is never answered with the
old one's metadata.

The facade vends **no storage credentials** and no FileIO properties: an engine reads metadata and
data files with its own read access to the table's backend, through its own FileIO or file system for
the location's scheme. DuckDB takes a secret — `TYPE GCS` with an HMAC key; `TYPE S3` with an access
key and, for an S3-compatible server, `ENDPOINT` as `host:port`, `USE_SSL` and `URL_STYLE 'path'`;
`TYPE AZURE` with a connection string — PyIceberg `s3.*` or `adls.*` properties, or Google's
application default credentials; Spark `S3FileIO`, `ADLSFileIO` or `GCSFileIO`; Trino its native S3,
Azure or GCS file system. The console's Connect panel prints them for each table's backend, filled
from its own storage profile for the location — endpoint, region, path style, account — and never a
credential (console 11). Engines read an Azure table's `abfss://` paths through the account's DFS
endpoint, so an account they read should have a hierarchical namespace. Verified on GCS with DuckDB
1.5.6 and PyIceberg 0.12 (E4, and on a test cluster through the HTTPRoute with an access token);
Spark and Trino follow the same protocol but have not been run against it.

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
SELECT count(*) FROM iceberg_scan('gs://example-lake/midstream/mat-bbc480ce42f0/orders');
SELECT * FROM iceberg_snapshots('gs://…/mat-bbc480ce42f0/orders');
```

On S3, or an S3-compatible server, and on Azure, the secret is the backend's:

```sql
CREATE SECRET s3 (TYPE S3, KEY_ID '<access key id>', SECRET '<secret access key>', REGION 'us-east-1');
-- an S3-compatible server adds ENDPOINT 'host:port' (no scheme), USE_SSL false for http, URL_STYLE 'path'
SELECT count(*) FROM iceberg_scan('s3://example-lake/midstream/mat-bbc480ce42f0/orders');

INSTALL azure; LOAD azure;
CREATE SECRET azure (TYPE AZURE, CONNECTION_STRING '<connection string>');
SELECT count(*) FROM iceberg_scan('abfss://lake@exampleaccount.dfs.core.windows.net/midstream/mat-bbc480ce42f0/orders');
```

DuckDB through the REST facade — each dataset is a namespace, so tables of any datasets and
materializations the token may read can be joined; the data files are read with the backend's secret,
as above:

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

On GCS PyIceberg reads the data files with Google's application default credentials. On S3 the
catalog takes the FileIO's properties beside the token — `s3.region`, `s3.access-key-id`,
`s3.secret-access-key`, and `s3.endpoint` for an S3-compatible server, which PyIceberg then
addresses path-style — and on Azure `adls.account-name` and `adls.account-key`.

`iceberg_scan` prunes data files by their manifest bounds: a one-day predicate read 1 of 2 files and
a predicate past the data none (E3). PyIceberg 0.12 reads a null list of structs as an empty list —
a record without `items` comes back with `items = []` — where DuckDB reads it as written
(14 §14.5). The console's Table tab shows these snippets with the deployment's catalog URI and the
table's backend filled in (console 11).

## 11.10 Failure modes

| Symptom | Cause | Recovery |
|---|---|---|
| `create_table` fails with `invalid_destination` | the materialization's destination cannot hold a table location of a known scheme | delete it and create it again under a destination of one of the forms of §11.1 |
| `create_table_failed` naming a refusal (401, 403, S3's `AccessDenied`) or a missing bucket or container (`StoreConfigurationError`) | the control plane has no credentials, or the wrong ones, for the destination's backend; the bucket or container does not exist, or the endpoint or region names another server | fix `storage.*` (02 §2.9a) or the identity's grants, or create the bucket or container, then `POST …/resume`; a materialization keeps its destination, so one created under the wrong one is deleted and created again |
| A runtime reports `waiting_for_table` | the pointer is missing or unreadable; the runtime's pods lack credentials the control plane has | check `metadata/catalog-pointer.json` and the storage credentials of the runtime spec, which the chart renders from the same `storage.*` (§11.1a); a lost pointer can be rebuilt (12 §12.11) |
| `commit_failed`, repeatedly | the pointer CAS keeps losing (another writer), or the store refuses writes; a server whose `If-Match` cannot be trusted | the committer's logs; an external engine committing in a tight loop must back off; run the conformance suite against the store (§11.1a) |
| `commit_failed`: *Commits are failing: the pointer write of … failed …* | a pointer write failed and the pointer read back could not tell whether it landed (`CommitStateUnknownException`, §11.2) | nothing: the committer reloads and decides again on its next attempt, and the files stay pending meanwhile |
| A compaction or reprotect `failed`: *The commit's outcome is unknown, so the job's outputs are kept* | the same, for the maintenance app's commit | nothing: the job runs again; orphan cleanup removes the outputs if the table never took them |
| Data files under `data/` that no snapshot lists | uploads no commit took (a covered or overlapping seal, a crashed member), a compaction that died before its commit, the outputs of a job whose commit's outcome was unknown | orphan cleanup removes them after `orphan_min_age_hours` |
| The console says *no storage is configured for* a location | no console storage profile's scope covers it | add a profile for the destination prefix (console 07 §07.10) |
| `metadata.json` grows | many snapshots retained | lower `snapshot_retention_hours` or `snapshot_keep_all_minutes` (17) |
| Delete files appear | an external engine with `catalog.write` committed row-level deletes | the console refuses to read such a snapshot (console 07); engines that apply deletes read it correctly |
