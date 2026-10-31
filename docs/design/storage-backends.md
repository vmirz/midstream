# Storage backends: the lake in GCS, Amazon S3, Azure Blob Storage or a local store

**Status:** agreed (2026-10-30), built. Written against `main` at 432a582 (1.0.0-rc.4 with data
protection) and revised the same day with the owner's decisions (§15) and the results of experiments
against SeaweedFS, Azurite, moto and fake-gcs-server (§3.5). §17 lists where the build departs from
this design.

**Audience:** the owner, for review; then the implementer, who should be able to build from this
without coming back to its author.

---

## 0. Summary

Midstream writes every table to Google Cloud Storage and nowhere else: the pointer catalog's
compare-and-swap is a GCS generation precondition, the PyIceberg FileIO wraps the GCS client, the
runtime uploads with the GCS client, the console reads `gs://` through GCS's JSON API and scans it with
a GCS HMAC key, and the chart mounts one Google service-account key into every pod (handbook 11 §11.2,
14 §14.1; console 06, 07). This design adds **Amazon S3** (AWS, and any S3-compatible server that
honours conditional writes), **Azure Blob Storage**, and a **local file store** for tests, behind one
pluggable interface, and keeps GCS as a first-class backend.

**Feasible, and not a rewrite.** Everything that touches the bucket does so through eight
operations — read an object (whole or a byte range), read its size and age, write it create-only,
replace it on the condition that it has not moved, delete one, delete many, list a prefix — in about a
dozen Python call sites and three console modules, with one in-memory fake behind the whole test
suite. Nothing in the committer's exactly-once accounting, the schema machinery, the workflows, the
live tail, data protection or the REST catalog knows what a bucket is. Every target provides the one
thing the pointer protocol cannot do without, a true compare-and-swap: GCS by object generation,
Azure by ETag, and **Amazon S3 by ETag since November 2024** (`If-Match` on `PutObject`; `If-None-Match:
*` since August 2024).

**The design.** An `ObjectStore` protocol in `lakehouse/store/` with five implementations — GCS, S3,
Azure Blob, a filesystem store and an in-memory store — chosen per object URI by its scheme, so one
deployment may hold tables on different destinations and nothing above the store branches on the
backend. The pointer, the FileIO, footer reads, uploads, the orphan scan and the drop move onto it
with their behaviour unchanged. A **conformance suite** (`python -m midstream.lakehouse.store.check
<uri>`) proves a store's conditional-write semantics — create-only, compare-and-swap, sixteen
concurrent writers with exactly one winner per round — before a deployment is pointed at it; CI runs it
against emulators of all three clouds. The console gets a **storage registry** beside its registry of
domains: one profile per destination prefix with its own credentials and DuckDB secret, matched to
every object by longest prefix exactly as DuckDB matches scoped secrets, so the console keeps every
feature on every backend and one statement may join tables on different backends. The chart gains a
`storage:` block that hands each app, each runtime and the console Secret references, never values.

**Clean slate.** Every installation is new (decision D10): no migration from a GCS-only release, no
compatibility aliases — the chart's `gcs.*` and `console.hmacSecret`, the console's
`CONSOLE_GCS_HMAC_*` variables and the setting kind `gcs_uri` are replaced, not kept beside the new
names.

**Local and test storage.** SeaweedFS — Apache-2.0, an official Helm chart, and a one-process `weed
mini` mode its project recommends for single nodes — is the S3 server for development and test
clusters; its S3 gateway honours `If-Match` and `If-None-Match` on `PutObject`. MinIO's community edition
was archived in February 2026 and ships no binaries or images any more; Garage cannot compare-and-swap
by design. Azurite, Microsoft's emulator, stands in for an Azure storage account while the backend is
built; fake-gcs-server stands in for GCS in CI. A test cluster runs both of its domains on an
in-cluster SeaweedFS (decision D2).

**Cost.** Roughly 4–5 thousand lines across Python, the console, the chart, tests and documentation.
Three Python dependencies (boto3, azure-storage-blob, azure-identity) and three console packages.

---

## 1. Requirements, as acceptance criteria

| # | Requirement | Verified by |
|---|---|---|
| R1 | A deployment's destination may be `gs://bucket[/prefix]`, `s3://bucket[/prefix]` or `abfss://container@account.dfs.core.windows.net[/prefix]`; `file:///path` serves tests only and the chart refuses it | `test_config.py`, `test_settings.py`, the chart tests |
| R2 | Every guarantee of handbooks 10 and 11 holds on every backend: the pointer's compare-and-swap, create-only data files, `commit_once`, exactly once, the orphan age guard, the drop guard | the existing suites over the memory store; the conformance suite over every live store |
| R3 | One code path above the store: no backend-specific branch in the committer, the writer, the maintenance app, the tail, the workflows or the catalog | review: the SDKs are imported only under `lakehouse/store/` |
| R4 | The console serves every feature on every backend — Table, Schema, Data, runs, statements, exports, profiles, the live tail, the Connect panel — and a console serving several domains reads domains on different backends, in one statement too | the console's tests; a test cluster |
| R5 | Credentials reach pods as Secret references or a cloud identity; none appears in a rendered runtime spec, a topic, a log, a table property or an API answer | the chart tests |
| R6 | A backend's conditional-write semantics are verified by a runnable suite, never assumed | `lakehouse/store/check.py`, in CI against emulators and by hand against any endpoint |
| R7 | Every installation is new: no migration and no compatibility aliases (D10) | — |
| R8 | A test cluster runs both domains on an in-cluster SeaweedFS, deployed from the cluster's infrastructure repository; an Azure emulator validates the Azure backend end to end; GCS is validated on the same cluster before the switch | handbook 13 |
| R9 | No significant performance regression on GCS: the same storage requests per operation as before, and a full backfill within noise of the base commit's | a request-parity harness; a backfill on a test cluster (§10) |

---

## 2. Where storage is touched today

The whole dependency, by module. "Operations" are the primitive calls each makes on the
google-cloud-storage client.

| Module | Lines | Operations |
|---|---|---|
| `lakehouse/gcs.py` | 116 | the shared client and its test seam; `parse_gcs_uri`, `is_precondition_failed`, `is_not_found`, `delete_object`, `upload_bytes_create_only`, `download_bytes` (whole or a range), `object_size` |
| `lakehouse/pointer.py` | 228 | read the pointer and its generation in one request; replace it with `if_generation_match`; delete it; write the version hint; list `metadata/` to rebuild a lost pointer |
| `lakehouse/io.py` | 180 | PyIceberg's `FileIO`: read whole, write create-only or overwrite, delete, exists, size |
| `lakehouse/catalog.py` | 406 | `GcsPointerCatalog`: only through the two modules above plus `upload_bytes_create_only` for metadata files |
| `lakehouse/files.py::read_parquet_footer` | 35 | size, then one or two range reads |
| `lakehouse/maintenance.py::find_orphans` | 40 | list `data/` and `metadata/` with each object's creation time |
| `storage/objects.py` | ~560 | list a prefix; delete in GCS batch requests of 100 (one by one for a client that cannot batch); copy and delete the pointer to retire it |
| `runtime_app/writer/upload.py` | 60 | upload a file create-only; on a precondition failure, compare the existing object's size |
| `maintenance_app/compact_worker.py` | 3 call sites | download an input to a file; upload an output create-only |
| `maintenance_app/worker.py`, `runtime_app/committer.py`, `runtime_app/pipeline.py`, `runtime_app/table_state.py`, `tail_app/worker.py`, `web/catalog.py`, `lakehouse/commits.py`, `lakehouse/lifecycle.py`, `lakehouse/rest.py` | plumbing | pass the shared client; `delete_object` for a covered or replaced file; the tail's fallback seed reads the table through the catalog and labels its source `"gcs"` |
| `config.py`, `settings/catalog.py`, `web/materializations.py`, `services/workflow.py` | ~60 | the destination must match `^gs://…`: `is_gcs_destination_uri`, the setting kind `gcs_uri`, `deployment_destination` ("is not a gs:// URI"), `InvalidTableLocationError`, the drop guard's `parse_gcs_uri` |
| `python/pyproject.toml` | 1 | `google-cloud-storage>=2.18.0` |
| `python/tests/gcs_fakes.py` and the support modules | ~230 | an in-memory GCS with generations, raising the library's own `PreconditionFailed` and `NotFound` |

The console:

| Module | What it assumes |
|---|---|
| `server/data/gcs.ts` (~290 lines) | a GET of GCS's JSON API with the client library's credentials, retries and CRC32C; the emulator; the cached-mode mirror and its eviction |
| `server/iceberg/io.ts` | `gs://` → `downloadObject`, anything else → the local disk |
| `server/data/objects.ts` | `direct` scans `gs://` in place; `cached` mirrors `gs://`; the budget message names the HMAC variables |
| `server/data/duckdb.ts` | one `TYPE GCS` secret from `CONSOLE_GCS_HMAC_KEY_ID`/`_SECRET`, on the shared instance and in every sandbox; a GCS-specific correction of DuckDB's region hint |
| `server/config.ts`, `server/main.ts`, `server/routes/settings.ts`, `server/settings/catalog.ts` | the HMAC variables decide `direct` or `cached` for the whole console; the boot log and the Settings page say so |
| `server/iceberg/snapshot.ts::objectName` | strips `gs://bucket/` |
| `src/features/detail/TableTab.tsx` | the Connect panel's DuckDB, PyIceberg, Spark and Trino snippets name GCS credentials, `GCSFileIO` and `fs.native-gcs.enabled` |
| `src/features/settings/values.ts`, `SettingRow.tsx`; `src/features/datasets/DatasetAccess.tsx` | the setting kind `gcs_uri` and its `gs://` pattern; a sentence about bucket credentials |
| `package.json` | `@google-cloud/storage` 8; `@duckdb/node-api` 1.5.5 |

The chart: `gcs.credentialsSecret`/`credentialsKey` and the helpers `midstream.gcsVolumes`,
`gcsVolumeMounts`, `gcsEnv`, included by the control plane, the maintenance and tail apps, the runtime
spec every materialization is rendered from, and the console's Deployment; `console.hmacSecret`;
`controlPlane.settings.defaultDestinationUri`; the values files under `ci/`.

---

## 3. Assessment

### 3.1 What a backend must provide

| Operation | Used by | Needed semantics |
|---|---|---|
| `get(uri)` → bytes **and a version token, from the same request** | the pointer, metadata, manifests | reading the pointer and learning which version was read must be one request, or two writers can both believe they read the latest |
| `get(uri, range)` | Parquet footers | an inclusive byte range |
| `head(uri)` → size, version, last-modified | upload idempotency, orphan age | |
| `put(uri, data, if_version=ABSENT)` | metadata files, data files, the pointer's creation | create-only: fails if the object exists |
| `put(uri, data, if_version=v)` | the pointer | **compare-and-swap**: succeeds only if the object is still at version `v`, and of two concurrent writers exactly one wins |
| `put(uri, data)` | the version hint, the retired pointer copy | unconditional |
| `delete(uri)`, `delete_many(uris)` | drops, orphans, covered files | a missing object is not an error |
| `list(prefix)` → name, size, version, last-modified | orphans, lost-pointer repair, drops | pagination hidden; times for the age guard |

Consistency, retries, checksums and batch sizes are each backend's own business, inside its store.

### 3.2 The candidates against it

| Store | Create-only | Compare-and-swap | Version token | Notes |
|---|---|---|---|---|
| Google Cloud Storage | `if_generation_match=0` | `if_generation_match=N` | object generation | what runs today |
| Amazon S3 | `If-None-Match: *` | `If-Match: <etag>` on `PutObject` | ETag | 412 on a lost race; 409 `ConditionalRequestConflict` while another conditional write is in flight (re-read and retry); `If-Match` needs `s3:GetObject` as well as `s3:PutObject`; strongly consistent since 2020 |
| Azure Blob Storage | `If-None-Match: *` (409 `BlobAlreadyExists`) | `If-Match: <etag>` on `Put Blob` | ETag (changes on every write) | the oldest of the three; works on accounts with or without a hierarchical namespace |
| SeaweedFS | yes | yes, evaluated under the object's write lock | ETag | Apache-2.0; active; official Helm chart; `weed mini` runs master, volume, filer and S3 in one process |
| MinIO | yes | yes | ETag | community edition archived in February 2026: source only, no images |
| Garage | no | no | — | its documentation calls conditional writes "structurally impossible" without consensus: excluded |
| Ceph RGW | yes | yes | ETag | correct and heavy: an operator, raw devices, several daemons |
| Azurite | yes | yes | ETag | Microsoft's emulator; Blob only, no DFS endpoint |
| fake-gcs-server | yes | yes (`ifGenerationMatch`) | generation | GCS's JSON API |
| moto (server mode) | yes | yes | ETag | the AWS semantics model, for CI; not a store |
| a local filesystem | by an atomic link | under a lock | inode, size and time | one machine only: tests |

### 3.3 Three subtleties of ETag-based compare-and-swap

1. **ABA.** On S3 an ETag is a content hash (the MD5 of a single-part PUT). If the pointer could
   return to an earlier byte-identical content, a stale `If-Match` would succeed. It cannot:
   `TablePointer.sequence` increases by one on every advance and `updated_at` moves, so no two pointer
   versions are equal (`pointer.py::advanced`). The conformance suite asserts it against the real
   serializer, and no pointer is ever written outside `pointer.py`.
2. **409, not only 412.** S3 answers a conditional write that collides with another in-flight
   operation with `409 ConditionalRequestConflict`. For the pointer it is the same news as a 412 —
   someone else is committing — so both map to `PreconditionFailedError` and the committer does what it
   does today: reload, decide again (`commit_once`, 10 §10.6). For a create-only upload it means "check
   whether the object is there with the right size", the path `upload.py` already has for a 412.
3. **A missing object under `If-Match`.** S3 answers 404 where GCS and Azure answer 412. For
   `write_pointer` on a table whose pointer vanished — dropped under a committer — either means "you
   lost", so the S3 store maps a 404 on a conditional write to `PreconditionFailedError` as well, and the
   caller's reload finds no table.
4. **A conflict can be a win.** Every SDK retries a conditional write whose answer was lost
   (google-cloud-storage retries uploads that carry a generation precondition, boto3 and azure-core retry
   on connection errors), and the retry of a write that succeeded is answered with a precondition failure.
   Azurite reproduced it: an applied create-only PUT came back as 409, an applied compare-and-swap as 412.
   Today, on GCS, `commit_table` then deletes the staged metadata file — which the pointer may already
   name, leaving the table unreadable. The catalog therefore re-reads the pointer on a conflict: if it
   names the staged metadata, the commit won and completes; only otherwise is the staged file deleted.
   Uploads already handle the same case (`upload.py`: an existing object of the right size is the upload).

### 3.4 Verdict

Feasible within the current architecture, with no change to any invariant (01 §1.6) and no change to a
table's layout, properties or snapshot summaries. The risks, in order:

- **Conditional-write fidelity on S3-compatible servers.** The protocol is only as safe as the
  server's `If-Match`. The conformance suite's concurrent-writer check runs against every endpoint
  before a deployment uses it, and the handbook names each verified store and version.
- **The console's DuckDB layer against several credentials at once.** DuckDB scopes secrets by path
  prefix; the registry mirrors that model one to one, and the sandbox tests cover a statement spanning
  two profiles.
- **The Azure emulator has no DFS endpoint.** The console reads Azure data through the Blob endpoint
  in any case (§5.8), which works on every account and on Azurite.
- **Test churn.** Nineteen test modules reach into the GCS fake. The memory store keeps the fake's
  inspection surface, so most of them change only their imports and parameter names.
- **Install weight.** boto3 and the Azure SDKs are plain dependencies (D6), installed by every pod at
  start; §10 measures what they add.

### 3.5 What the experiments showed

Measured on 2026-10-30 against SeaweedFS 4.51, Azurite 3.37.0 with azure-storage-blob 12.31.1, moto 5.2.5
with boto3 1.43, fake-gcs-server 1.56.1 with google-cloud-storage 3.17.1, and DuckDB 1.5.5 through
`@duckdb/node-api` 1.5.5-r.4, on a laptop and in the console's own image.

| Finding | Consequence |
|---|---|
| Every store held compare-and-swap under 16 concurrent writers: SeaweedFS, Azurite (nine runs), fake-gcs-server and moto (each three or more runs of 16 × 40), no lost and no phantom update; create-only races had exactly one winner per round. moto's check is not atomic by construction (only the interpreter's lock narrows the window) | moto models AWS's answers, not its atomicity: a store counts as verified only on a real server |
| moto, as AWS: a stale `If-Match` → 412 `PreconditionFailed`; `If-Match` on a missing key → 404 `NoSuchKey`; `If-None-Match` with anything but `*` is accepted *and writes*; `If-Match: *` → 412; never a 409 | the S3 store sends only `IfNoneMatch='*'` and real ETags; the 409 path is unit-tested |
| S3 ETags are the content's MD5: same → other → same gives the first ETag again, and a stale `If-Match` naming it succeeds | the pointer's contents never repeat (§3.3); the conformance suite checks it |
| `head_object` on a missing key answers 404 with code `404`, `get_object` with `NoSuchKey` | not-found is the HTTP status, not the code |
| boto3's transfer manager (`upload_file`) refuses `IfNoneMatch`/`IfMatch` | `put_file` is one `PutObject` streaming the file |
| Azure: create-only loses with 409 `BlobAlreadyExists`, compare-and-swap with 412 `ConditionNotMet` (also on a missing blob); `overwrite=False` plus a condition makes the SDK rewrite a 412 into `BlobAlreadyExists`; azure-storage-blob up to 12.30.0 names chunked blocks by offset, and concurrent create-only chunked uploads then committed mixed content | `upload_blob(overwrite=True)` with an explicit match condition; errors classified by status and `x-ms-error-code`; azure-storage-blob ≥ 12.31 |
| Azure ETags are random, change on identical re-uploads, and are unquoted in listings but quoted elsewhere; a multi-chunk download sends later chunks with `If-Match` and fails with 412 if the blob changed | versions are compared for equality only, quotes stripped; `get` retries a read the blob changed under |
| fake-gcs-server: create-only, compare-and-swap and a missing object under a condition give 412; the download records its generation in the same response; DELETE ignores preconditions; batch deletes from the Python client delete nothing | the live GCS check skips conditional delete and the batch request on the emulator; unit tests cover both |
| DuckDB scopes secrets by a plain, case-sensitive, longest-prefix match; with two equal scopes it silently picks the alphabetically first secret; a path no secret covers is sent anonymously to the public cloud (`https://<bucket>.s3.amazonaws.com`, the real Azure host) | the registry refuses equal scopes and requires the trailing `/`; no path reaches DuckDB without a profile |
| DuckDB's S3 secret needs `ENDPOINT` as `host:port` without the scheme, `USE_SSL false` for plain HTTP, `URL_STYLE 'path'`; its Azure secret takes keys only inside a connection string, and its `ENDPOINT` is a DNS suffix, always HTTPS | the console builds connection strings itself; an emulator's address goes in `BlobEndpoint` |
| DuckDB reads `az://<account>.blob.core.windows.net/<container>/<path>` from a real account or, under a connection string with `BlobEndpoint`, from Azurite, checking the account; `abfss://` needs a `DfsEndpoint` on Azurite and the DFS endpoint on a real account | the console hands DuckDB the fully qualified `az://` form for every Azure profile |
| Warm reads cost no requests on S3 and GCS, but 2–12 HEAD requests per file per query on Azure (DuckDB's HTTP metadata cache does not cover its Azure extension) | a known cost of the Azure backend, documented |
| `duckdb_secrets()` and `which_secret()` pass the console's SQL guard and list every secret's scope, endpoint, region and key id (values redacted) — today the GCS HMAC key id | fixed: the guard refuses them; a sandbox holds only the secrets its run needs |
| A locked DuckDB sandbox still allows `COPY … TO` an allowed object path; the guard's refusal of `COPY` is what stops it | console profiles use read-only credentials |
| Azurite takes the first label of a dotted hostname as the account, and rejects the SDKs' newest API version | `--disableProductStyleUrl` and `--skipApiVersionCheck` wherever Azurite runs |
| A SIGKILLed Azurite loses up to 5 s of acknowledged writes, a compare-and-swap included | Azurite is a functional stand-in, never stopped by force under a test |
| SeaweedFS's `weed mini -bucket` creates the bucket at start; identities scoped per bucket (`Read:`, `List:`, `Write:`) work as they read; data and conditions survive the pod returning with another address when its components talk over loopback (`-ip=127.0.0.1`) | the test cluster's manifest (§14) |

---

## 4. Alternatives considered

### 4.1 PyIceberg's own FileIO (`PyArrowFileIO`, `FsspecFileIO`) — rejected

PyIceberg reads and writes `s3://`, `gs://` and `abfss://` already, but its FileIO writes
unconditionally (no create-only, no compare-and-swap), takes credentials through its own property
names rather than the pods' Secrets, and cannot be put in front of an in-memory store, which is how
every test exercises the catalog. The pointer, the uploads and the orphan scan would still need a
client of their own: two credential paths and two retry policies per backend. The FileIO becomes one
over the store, a single implementation for every backend.

### 4.2 fsspec (`s3fs`, `gcsfs`, `adlfs`) as the abstraction — rejected

Conditional writes are not part of fsspec's interface: `gcsfs` exposes a generation match, `s3fs` no
`If-Match` on `pipe`/`put`, `adlfs` partly. The one operation the design cannot compromise on would be
the one the abstraction hides. The SDKs expose it plainly.

### 4.3 A catalog service instead of the pointer — rejected

With a catalog server (Polaris, Nessie, Lakekeeper) or the control plane's own `state` table as the
catalog, the store would need no compare-and-swap. It would put a server in the commit path of every
runtime (today a runtime commits with nothing but bucket access), add a component to deploy and change
invariant 5. Every target store compare-and-swaps; the pointer stays.

### 4.4 A lock table for S3 — unnecessary

Before November 2024 the only safe way to commit to S3 without a catalog was a DynamoDB-style lock
(Delta Lake's `S3DynamoDBLogStore`). `If-Match` on `PutObject` made it obsolete.

### 4.5 The local option

- **MinIO** — archived community edition; an unmaintained binary from a fork. No.
- **Garage** — light and well made, and its own documentation rules out compare-and-swap. No.
- **A `file://` destination on a shared volume** — every pod would need the same RWX volume and a lock
  protocol over NFS, which no one should trust with a catalog. The file store serves tests, and the
  chart refuses `file://` as a destination.
- **Ceph RGW through Rook** — correct and far too heavy for a test cluster.
- **LocalStack, moto** — development tools; moto earns a place in CI as the model of AWS's answers.
- **SeaweedFS** — chosen (D1): one pod running `weed mini` on a persistent volume covers a test
  cluster; its Helm chart covers a production cluster.
- **Azurite** — the Azure stand-in while the backend is built (D3), run for the build and its
  validation only.

### 4.6 The console's credentials: per location

A domain can hold materializations on different destinations — each keeps the destination it was
created with, and the default can change — so the only credential model that never misreads a table is
by the table's **location**: a profile per destination prefix, longest prefix wins, which is also how
DuckDB chooses among scoped secrets (D7). A domain's registry entry needs no storage fields.

### 4.7 One URI form per backend, as engines expect it

A table's location is written into its metadata, manifests and data-file paths for good. The forms are
the ones every engine reads natively: `gs://bucket/key`, `s3://bucket/key`, and for Azure the Hadoop
form `abfss://<container>@<account>.dfs.core.windows.net/<path>` (Spark, Trino, PyIceberg and DuckDB's
Azure extension read exactly this form). One form per backend is accepted (D4): `az://` or `wasbs://`
destinations would give the same table two names in different engines.

---

## 5. Architecture

### 5.1 The `ObjectStore` protocol (`lakehouse/store/`)

```
lakehouse/store/
  base.py       ObjectStore (Protocol), ObjectInfo, DeleteOutcome, ABSENT, the errors
  uris.py       parse a URI per scheme into (container, key[, account]); join; destination validation
  registry.py   store_for(uri): one store per scheme per process, built from the environment on first
                use; set_store()/reset_stores(), the test seam
  gcs.py        GcsStore        google-cloud-storage: generations, batch deletes of 100
  s3.py         S3Store         boto3: ETags, If-None-Match/If-Match, DeleteObjects of 1,000
  azure.py      AzureBlobStore  azure-storage-blob: ETags, If-None-Match/If-Match, blob batches
  file.py       FileStore       file://: atomic links for create-only, a lock for compare-and-swap
  memory.py     MemoryStore     the tests' store, with the old fake's inspection surface
  check.py      the conformance suite; also `python -m midstream.lakehouse.store.check <uri>`
```

The protocol (§6.1 has the signatures): `get` returns the bytes and the version read, from one
request; `head`; `put` with `if_version` — `None` unconditional, `ABSENT` create-only, a version string
compare-and-swap; `put_file` and `get_to_file` stream a local file; `delete` (False when the store knows
the object was already gone); `delete_many` with a per-object outcome; `list` with pagination hidden. A
version is an opaque string: GCS's generation, S3's or Azure's ETag, the file store's stamp.

Errors every backend maps to: `PreconditionFailedError` (a condition did not hold), `ObjectNotFoundError`,
`StoreError` for anything else, carrying the HTTP status when there is one, so `storage/objects.py` keeps
its retry classes (408, 429, 5xx and no answer retried; anything else refused).

Blocking throughout, as today: callers on an event loop run it on their worker threads (the table
lifecycle thread, the pipeline's upload threads, the maintenance app's pool). Every store is
thread-safe, and one instance per scheme serves the process, as the shared GCS client does now.

This is the shape the Rust `object_store` crate settled on for the same backends (`PutMode::Create` /
`PutMode::Update(version)`): the smallest interface that carries the pointer protocol. It is not a
filesystem.

### 5.2 Choosing a store, and its credentials

`store_for(uri)` looks at the scheme and returns the process's store for it:

| Scheme | Store | Credentials, in order | Endpoint |
|---|---|---|---|
| `gs://` | `GcsStore` | `GOOGLE_APPLICATION_CREDENTIALS` (a mounted key), else application default credentials (workload identity) | the library's; `STORAGE_EMULATOR_HOST` for an emulator |
| `s3://` | `S3Store` | `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (`AWS_SESSION_TOKEN`), else boto3's default chain (IRSA, EKS pod identity, an instance role) | `STORAGE_S3_ENDPOINT` for an S3-compatible server; `STORAGE_S3_REGION` (default `us-east-1`); `STORAGE_S3_PATH_STYLE` |
| `abfss://` | `AzureBlobStore` | `AZURE_STORAGE_CONNECTION_STRING`, else `AZURE_STORAGE_ACCOUNT_KEY` for the account the URI names, else `DefaultAzureCredential` (workload identity, managed identity, a service principal from `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_CLIENT_SECRET`) | `STORAGE_AZURE_ENDPOINT`, an emulator's blob endpoint to which the account is appended; else `https://<account>.blob.core.windows.net` |
| `file://` | `FileStore` | none | the path |

The credential variables are the SDKs' own names on purpose: IRSA, EKS pod identity and AKS workload
identity inject exactly those, so a pod with a cloud identity needs no Secret, as a GKE pod with
workload identity needs none today. Midstream's own knobs are the `STORAGE_*` variables. A scheme
whose credentials are absent still builds a store, since the SDK's chain may find an identity; its first
operation fails with the SDK's error, which the workflow reports as it reports a GCS refusal today.

One store per scheme serves every bucket of that scheme, with the process's one identity and, for S3,
its one endpoint: a deployment whose destinations span two S3 endpoints is not supported.

### 5.3 Locations

A table's location stays `<destination>/<storage prefix>/<materialization id>/<table>`
(`lakehouse/tables.py`); only the destination's scheme widens:

| Destination | A table's location |
|---|---|
| `gs://my-lake` | `gs://my-lake/midstream/mat-5c8ed527453d/orders` |
| `s3://my-lake/prod` | `s3://my-lake/prod/midstream/mat-5c8ed527453d/orders` |
| `abfss://lake@acme.dfs.core.windows.net` | `abfss://lake@acme.dfs.core.windows.net/midstream/mat-5c8ed527453d/orders` |
| `file:///tmp/lake` (tests) | `file:///tmp/lake/midstream/mat-5c8ed527453d/orders` |

`uris.parse(uri)` returns the container (bucket, or Azure container), the key and, for Azure, the
account. The drop's guard (`storage/objects.py::table_object_prefixes`: the parent directory named
exactly the materialization's id, the last segment a table name) works on the parsed key, so the drop can
delete outside a table on no backend.

Azure's location names the DFS endpoint because that is what Spark, Trino and PyIceberg read through the
REST catalog; Midstream's own store, and the console, use the Blob endpoint of the same account, where
`If-Match` and batch deletes live and which every account has. A hierarchical namespace is recommended
for accounts external engines read (their `abfss://` reads go to the DFS endpoint), not required by
Midstream.

### 5.4 Behaviour per backend

| | GCS | S3 | Azure | file | memory |
|---|---|---|---|---|---|
| create-only put | `if_generation_match=0` | `IfNoneMatch='*'` | `overwrite=False` | atomic link of a temporary file | under a lock |
| compare-and-swap | `if_generation_match=N` | `IfMatch=etag` | `etag` + `MatchConditions.IfNotModified` | under the store's lock | under a lock |
| read with version | `download_as_bytes` + `blob.generation` | `GetObject` body + `ETag` | `download_blob` + `properties.etag` | `fstat` of the open file | — |
| range read | `start`/`end` | `Range: bytes=a-b` | `offset`/`length` | seek | slice |
| delete a missing object | `False` | `True` (S3 does not say) | `False` | `False` | `False` |
| batch delete | the JSON API's batch of 100 | `DeleteObjects` of 1,000, quiet | the blob batch of 256 (one by one where unsupported) | one by one | one by one |
| object time for the age guard | `time_created` | `LastModified` | `last_modified` | mtime | the store's clock |
| large files | the library's resumable upload | one streamed `PutObject` (≤ 5 GB; a seal's file is far below) — not the transfer manager, which cannot carry the condition | `upload_blob` chunks past 64 MiB and applies the condition at the block-list commit | copy | — |
| known pitfall | ~1 update/s per object (E11): commits stay 1 s apart on every backend | see §3.5 for the checksum settings S3-compatible servers need | an account key or connection string must never reach a log; `DefaultAzureCredential` is slow on first use and is built once | tests only | tests only |

`delete` returning `True` for an already-missing S3 object changes one counter in the maintenance app's
orphan log line; nothing calls the catalog's own `drop_table`, whose "no such table" depends on it.

### 5.5 What changes above the store

| File | Change |
|---|---|
| `lakehouse/gcs.py` | removed; its helpers move into `store/gcs.py` and `store/uris.py` |
| `lakehouse/pointer.py` | `read_pointer(store, location) -> (pointer, version)`; `write_pointer(store, …, expected_version)` (create-only when `None`); `delete_pointer`; `find_latest_metadata` over `store.list`. Generations become version strings |
| `lakehouse/io.py` | `ObjectStoreFileIO`: one FileIO over the store for every backend |
| `lakehouse/catalog.py` | `PointerCatalog` (was `GcsPointerCatalog`); `client=` becomes `store=`, resolved per location when not given |
| `lakehouse/files.py::read_parquet_footer`, `lakehouse/maintenance.py::find_orphans`, `lakehouse/commits.py`, `lakehouse/lifecycle.py`, `lakehouse/rest.py` | take `store=` |
| `storage/objects.py` | the drop lists through `store.list` and deletes through `store.delete_many`, `delete_batch_size` objects to a request; the GCS batch request moves into `GcsStore.delete_many`. The retry, budget and deadline logic stays here, generic: a pass lists and deletes at most `max(CLEANUP_PASS_MAX_OBJECTS, CLEANUP_PASS_MAX_REQUESTS × delete_batch_size)` objects — 1,000 through a store that deletes one object per request, 10,000 in GCS's batches of 100, 25,600 in Azure's of 256, 100,000 in S3's `DeleteObjects` of 1,000 — and stops after about `CLEANUP_PASS_MAX_SECONDS` (10 s), checked between requests. A delete with no answer, 408, 429 or a 5xx is sent again after 1, 2 and 4 s while the pass has time, and ends the pass if it still fails; any other refusal leaves its object to the next pass, and a request the store turned away as a whole ends the pass (§17). A pass that deleted nothing raises `DestinationDeleteError`, which the workflow retries after its backoff |
| `runtime_app/writer/upload.py`, `runtime_app/pipeline.py`, `runtime_app/committer.py`, `runtime_app/table_state.py` | `store` in place of `client` |
| `maintenance_app/compact_worker.py`, `maintenance_app/worker.py` | `store.get_to_file`, `store.put_file(if_version=ABSENT)`, `store.delete` |
| `tail_app/worker.py` | the fallback seed's source label `"gcs"` becomes `"table"` |
| `config.py` | `is_destination_uri` accepting the three deployable schemes, and `file://` only when `CONTROL_PLANE_ALLOW_FILE_DESTINATIONS=true` (tests) |
| `settings/catalog.py`, `web/materializations.py`, `services/workflow.py` | the setting kind `destination_uri`; refusals name the three forms |
| `pyproject.toml` | `boto3`, `azure-storage-blob`, `azure-identity` beside `google-cloud-storage` |
| tests | `gcs_fakes.py` stays only as the GCS client double behind `GcsStore`'s own tests; everything else runs over `MemoryStore` |

### 5.6 The conformance suite (`lakehouse/store/check.py`)

| Check | Asserts |
|---|---|
| create-only | a `put(ABSENT)` on a new key returns a version; a second raises `PreconditionFailedError`; the first content is intact |
| read with version | `get` returns the bytes and a version a following `put(if_version=…)` accepts |
| compare-and-swap | `put(if_version=v1)` returns `v2 ≠ v1`; `put(if_version=v1)` again raises; `get` holds the second content |
| missing under a condition | `put(if_version=v)` on a missing key raises `PreconditionFailedError` whatever the server said |
| missing object | `get`, a ranged `get` and `get_to_file` of a missing key raise `ObjectNotFoundError` — not a bare `StoreError`, not empty bytes — and `get_to_file` leaves nothing behind |
| racing creators | writers behind a barrier create the same fresh key: exactly one wins, every other gets `PreconditionFailedError`, and the key holds the winner's bytes |
| file transfer | `put_file` create-only, then a refused second create; `put_file` compare-and-swap, then a refused stale one; each `get_to_file` replaces its target whole |
| **concurrent writers** | 16 threads each loop: read a counter object, write it incremented with `if_version`; at the end the object holds exactly the number of successful writes, and no thread ever reads a smaller value than it read before |
| content never repeats | a `TablePointer` advanced 1,000 times never serializes to the same bytes twice |
| range read | inclusive, matching the slice |
| head and list | size, version and a `last_modified` near the write; a listing with paging returns every object once |
| delete | a missing object does not raise; `delete_many` reports each object |
| cleanup | nothing is left under the check's prefix |

It runs three ways: by the unit tests over the memory and file stores; by a CI job against SeaweedFS,
Azurite, fake-gcs-server and moto started in the job; and by an operator, `python -m
midstream.lakehouse.store.check s3://my-lake/_check`, from a laptop or inside any Midstream pod, before
pointing a deployment at a new store.

### 5.7 The chart

```yaml
kaspr:
  podLabels: {}                 # on every Kaspr pod, the runtimes included: AKS workload identity's label
  serviceAccountAnnotations: {} # on each app's and runtime's ServiceAccount, <app>-app: IRSA's role ARN,
                                # AKS workload identity's client id
storage:
  gcs:
    credentialsSecret: ""   # a Secret with service-account.json; empty: application default credentials
  s3:
    credentialsSecret: ""   # a Secret with access-key-id and secret-access-key (session-token optional);
                            # empty: the pod's identity (IRSA, EKS pod identity)
    region: ""              # us-east-1 when empty
    endpoint: ""            # an S3-compatible server, e.g. http://seaweedfs-s3.seaweedfs.svc:8333
    pathStyle: false        # true for most S3-compatible servers
  azure:
    credentialsSecret: ""   # a Secret with connection-string, or account-key, or client-id, tenant-id
                            # and client-secret; empty: workload identity
    endpoint: ""            # an emulator's blob endpoint (the account is appended); empty: Azure
console:
  storage: []                   # the console's storage profiles (§5.8, §6.4)
  podLabels: {}                 # the console's own identity, for a profile without a Secret
  serviceAccountAnnotations: {} # on <release>-console, the ServiceAccount the chart creates for it
```

- `midstream.storageEnv` renders, for each backend with a Secret named, one `env` entry per key with
  `valueFrom.secretKeyRef` — the pattern `midstream.protectionKeyEnv` uses; S3's `session-token` and
  every Azure key optional — plus the plain `STORAGE_*` entries; `midstream.storageVolumes`/
  `storageVolumeMounts` mount the GCS key at `/etc/midstream/gcs`, which
  `GOOGLE_APPLICATION_CREDENTIALS` names. Included in the control plane, the maintenance and tail apps
  and the runtime spec; the console reads through its own profiles instead (below).
- A rendered runtime spec names a Secret, never a value.
- A backend without a Secret uses the pods' own cloud identity, which `kaspr.podLabels` and
  `kaspr.serviceAccountAnnotations` give every Kaspr pod, the runtimes included: labels on each pod (a
  KasprApp's `template.pod`, `midstream.kasprPodMetadata`) and annotations on the ServiceAccount
  kaspr-operator makes for each app and each runtime, `<app>-app` (`template.serviceAccount`,
  `midstream.kasprServiceAccount`). Every materialization brings a ServiceAccount of its own, so the
  identity must trust them all: IRSA by a `StringLike` condition on
  `system:serviceaccount:<namespace>:*-app`, which leaves the console's `<release>-console` out; AKS
  workload identity by a federated credential for each, at most 20 per identity. EKS Pod Identity needs
  an association for each ServiceAccount and GKE workload identity a role granted to the namespace,
  the console's pods included unless the console runs in a namespace of its own; neither needs a
  value. A label the chart sets itself is refused.
- `controlPlane.settings.defaultDestinationUri` accepts the three forms; the chart fails a `file://`
  destination. Every endpoint — `storage.s3.endpoint`, `storage.azure.endpoint`, a console profile's —
  must be `http(s)://host[:port]` naming the server alone (`midstream.endpoint`).
- The console's profiles are `console.storage[]` (§5.8), rendered to `storage.json`
  (`CONSOLE_STORAGE_FILE`) with every rule of §6.4 checked, two `s3` profiles of one bucket on
  different endpoints included (§17). Their credentials come as `CONSOLE_STORAGE_<NAME>_*` variables
  from each profile's Secret, every key optional, and a gcs profile's key file is mounted at
  `/etc/midstream-storage/<name>/service-account.json`, which its `keyFile` names. A profile without a
  Secret reads as the console's own cloud identity: `console.podLabels` and
  `console.serviceAccountAnnotations`, on the ServiceAccount the chart creates for the console,
  `<release>-console`. A change of profiles restarts the console (a checksum annotation), which reads
  them once. `NOTES.txt` warns when the console has no profile, or none for the release's own
  destination.

### 5.8 The console

A **storage registry** (`server/storage/`), the twin of the domains registry:

```json
[
  {"name": "lake", "scope": "s3://example-lake/", "kind": "s3",
   "endpoint": "http://seaweedfs-s3.seaweedfs.svc:8333", "region": "us-east-1", "pathStyle": true},
  {"name": "archive", "scope": "gs://example-archive/", "kind": "gcs",
   "keyFile": "/etc/midstream-storage/archive/service-account.json"},
  {"name": "azure-lake", "scope": "abfss://lake@exampleaccount.dfs.core.windows.net/", "kind": "azure"}
]
```

`CONSOLE_STORAGE_FILE` (the chart) or `CONSOLE_STORAGE` (development) carries this non-secret part;
each profile's credentials come from `CONSOLE_STORAGE_<NAME>_…` variables — `GCS_HMAC_KEY_ID` and
`GCS_HMAC_SECRET`; `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` and `SESSION_TOKEN`; `AZURE_CONNECTION_STRING`
or `AZURE_ACCOUNT_KEY` or `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_SECRET` — never from the
file. Without a registry the console reads no object storage: a table answers "no storage is configured
for <location>", which the Table and Data tabs show as they show an unreadable table today.

Every object the console touches resolves to the profile with the longest matching scope. A profile
provides:

| | gcs | s3 | azure |
|---|---|---|---|
| reading metadata objects | the JSON-API GET of today, with the profile's key file or ADC, CRC32C-checked | `@aws-sdk/client-s3` `GetObject` | `@azure/storage-blob` `download` |
| cached mode's whole-file download | as today | the same, renamed into place once complete | the same |
| its DuckDB secret | `TYPE gcs` from the HMAC pair, `SCOPE 'gs://bucket/'` | `TYPE s3` with keys (or `PROVIDER credential_chain`), `REGION`, and for an S3-compatible server `ENDPOINT` as `host:port`, `USE_SSL` from the endpoint's scheme and `URL_STYLE 'path'`; `SCOPE 's3://bucket/…/'` | `TYPE azure` with a connection string (the console builds one from an account key, adding `BlobEndpoint` for an emulator), a service principal or `credential_chain`; `SCOPE` the profile's scope in the `az://` form |
| what DuckDB is given for a data file | the URI | the URI | `az://<account>.blob.core.windows.net/<container>/<path>`, for real accounts and emulators alike |
| direct reads | with the HMAC pair; otherwise cached | always (keys or a chain) | always |
| the local cache path | `<cache>/<profile>/<container>/<key>`, with the segment rules of console 12 §12.6 | same | same |

A profile may set `"access": "cached"` to make the console download objects first even where DuckDB
could read them in place.

`data/duckdb.ts` registers every direct profile's secret on the shared instance, and in a sandbox the
secrets of the profiles its run reads; DuckDB picks the secret whose scope matches the path, so one
statement reads a GCS table and an S3 table with no change to `runs/view.ts`. The Azure extension is
loaded only where an Azure profile is involved (it costs about 13 ms per sandbox and 9 MB of download per
pod). The sandbox's `allowed_paths` holds the exact sources DuckDB opens, whatever their scheme, spelled
as in the secrets' scopes. The location checks of console 12 §12.6 stay on the canonical URIs; only the
hand-over to DuckDB rewrites an Azure path. No path reaches DuckDB without a profile covering it: DuckDB
would send an uncovered `s3://` path anonymously to AWS. The SQL guard refuses `duckdb_secrets()` and
`which_secret()`, which list every secret's scope, endpoint and key id.

The rest is renaming: `iceberg/io.ts` dispatches through the registry instead of
`uri.startsWith('gs://')`; `snapshot.ts::objectName` uses the registry's parser; the misleading-region
correction applies to each kind's errors with the right advice; the Connect panel's snippets follow the
table's location scheme, filled from the console's own profile for it (endpoint, region, path style;
never a credential); the Settings page and the boot log list each profile, its mode and whether its
credentials were found; `values.ts` validates the `destination_uri` kind.

### 5.9 Console feature parity across backends

| Feature | gcs | s3 | azure | How |
|---|---|---|---|---|
| Table tab: pointer, metadata, snapshots, partitions, compaction due | yes | yes | yes | metadata through the profile |
| Schema tab, timeline, diffs | yes | yes | yes | manifests through the profile; projections from the control plane |
| Data tab, browse runs, paging | yes | yes | yes | DuckDB direct (scoped secret) or cached |
| Statements, across domains and backends | yes | yes | yes | every profile's secret in the sandbox |
| Exports, profiles, the head peek | yes | yes | yes | read from the run, storage-blind |
| Live tail | yes | yes | yes | Kafka and the tail app; storage plays no part (19) |
| Freshness and caching | identical | identical | identical | everything behind the pointer is immutable on every store |
| Integrity of metadata reads | CRC32C always | the object's checksum when it carries one | the blob's MD5 when it carries one | |
| Connect panel | GCS secret | S3 secret with the endpoint | Azure secret | by location scheme |

### 5.10 Settings, API, permissions

- `default_destination_uri` keeps its key and scope; its kind is `destination_uri` (D8); the Settings
  page shows the three forms.
- No API changes: the control plane's answers are the same shapes, with locations of any scheme. The
  REST catalog vends no storage credentials and no FileIO properties; engines bring their own access.
- No new permission. The deployment settings list gains the `STORAGE_*` variables, read-only, beside
  `storage_prefix`.

---

## 6. Contracts

### 6.1 `ObjectStore`

```python
# lakehouse/store/base.py (shape)

class Absent: ...
ABSENT = Absent()                         # if_version=ABSENT: the object must not exist

@dataclass(frozen=True, slots=True)
class ObjectInfo:
    uri: str
    size: int
    version: str
    last_modified: datetime | None        # tz-aware; None when unknown (never deleted as an orphan)

@dataclass(frozen=True, slots=True)
class DeleteFailure:
    uri: str
    status: int | None                    # the HTTP status, None when there was no answer
    message: str

@dataclass(frozen=True, slots=True)
class DeleteOutcome:
    deleted: tuple[str, ...]              # gone now, or already missing
    failed: tuple[DeleteFailure, ...]

class StoreError(RuntimeError): status: int | None; code: str | None
class ObjectNotFoundError(StoreError): ...
class PreconditionFailedError(StoreError): ...
class StoreConfigurationError(StoreError): ...   # never retryable, never "not found"

class ObjectStore(Protocol):
    scheme: str
    delete_batch_size: int                # what one request deletes: 1 when the store sends one per object
    supports_conditional_delete: bool     # False: delete(if_version=...) raises NotImplementedError first
    def get(self, uri: str, *, start: int | None = None, end: int | None = None) -> tuple[bytes, str]: ...
    def head(self, uri: str) -> ObjectInfo | None: ...
    def put(self, uri: str, data: bytes, *, if_version: str | Absent | None = None,
            content_type: str = "application/octet-stream") -> str: ...
    def put_file(self, uri: str, path: str, *, if_version: str | Absent | None = None,
                 content_type: str = "application/octet-stream") -> str: ...
    def get_to_file(self, uri: str, path: str) -> ObjectInfo: ...
    def delete(self, uri: str, *, if_version: str | None = None) -> bool: ...
    def delete_many(self, uris: Sequence[str]) -> DeleteOutcome: ...
    def list(self, prefix: str) -> Iterator[ObjectInfo]: ...

def store_for(uri: str) -> ObjectStore: ...                          # registry.py
def set_store(scheme: str, store: ObjectStore | None) -> None: ...  # the test seam
```

### 6.2 Error mapping

| Backend answer | Error |
|---|---|
| GCS 412 | `PreconditionFailedError` |
| S3 412 `PreconditionFailed`; 409 `ConditionalRequestConflict`; 404 `NoSuchKey` under a condition | `PreconditionFailedError` |
| Azure (412, `ConditionNotMet`); (409, `BlobAlreadyExists`); (404, `BlobNotFound`) under a condition | `PreconditionFailedError` |
| GCS 404; S3 404 (any code: a HEAD has none); Azure (404, `BlobNotFound`) | `ObjectNotFoundError`; `head` returns `None`, `delete` `False` |
| Azure (404, `ContainerNotFound`), (404, `ResourceNotFound`), (403, `AuthorizationFailure`/`AuthorizationPermissionMismatch`/`AuthenticationFailed`), (401, `InvalidAuthenticationInfo`/`NoAuthenticationInformation`), (400, `InvalidHeaderValue`); S3 404 `NoSuchBucket` (never on a HEAD, whose answer has no code: `head` returns `None`); GCS 404 "The specified bucket does not exist." | `StoreConfigurationError` naming the problem, never retryable — never "not found", which would make a table look missing |
| 408, 429, 5xx after the SDK's own retries; no answer | `StoreError` with that status (`None` for no answer): retryable |
| anything else, a 403 from S3 or GCS included | `StoreError` with its status and code |

### 6.3 Environment

Apps and runtimes (the chart renders them):

| Variable | Backend | Meaning |
|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | gcs | the mounted key; absent: application default credentials |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | s3 | a key pair; absent: boto3's chain |
| `STORAGE_S3_ENDPOINT`, `STORAGE_S3_REGION`, `STORAGE_S3_PATH_STYLE` | s3 | an S3-compatible endpoint; the region; path-style addressing |
| `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_CLIENT_SECRET` | azure | a connection string, the account's key, a service principal; absent: `DefaultAzureCredential` |
| `STORAGE_AZURE_ENDPOINT` | azure | an emulator's blob endpoint; absent: `https://<account>.blob.core.windows.net` |
| `CONTROL_PLANE_ALLOW_FILE_DESTINATIONS` | file | `true` lets a destination be `file://`; tests only, never set by the chart |

The console: `CONSOLE_STORAGE_FILE` or `CONSOLE_STORAGE`, and per profile `<NAME>` (upper case, `-` as
`_`) the variables of §5.8.

### 6.4 Chart values

§5.7, and `console.storage[]`: `{name, scope, kind, endpoint, region, pathStyle, access,
credentialsSecret}`, the Secret's keys mapped to the profile's variables as
`console.domains[].serviceTokenSecret` is today: `hmac-key-id`, `hmac-secret` and `service-account.json`
for gcs; `access-key-id`, `secret-access-key` and `session-token` for s3; `connection-string`,
`account-key`, `client-id`, `tenant-id` and `client-secret` for azure. Validation in `_helpers.tpl`: a
scope ends with `/`, names are unique and usable in a variable name, a kind matches its scope's scheme.

### 6.5 Destination validation

| Scheme | Pattern |
|---|---|
| `gs://` | `^gs://[a-z0-9][a-z0-9._-]*[a-z0-9](/\S*)?$` |
| `s3://` | `^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](/\S*)?$` |
| `abfss://` | `^abfss://[a-z0-9](?:[a-z0-9]\|-(?=[a-z0-9])){2,62}@[a-z0-9]{3,24}\.dfs\.core\.windows\.net(/\S*)?$` |
| `file://` | `^file:///\S+$`, only with `CONTROL_PLANE_ALLOW_FILE_DESTINATIONS` |

Trailing slashes trimmed as today; at most 1,024 characters.

---

## 7. Behaviour on change

### 7.1 Changing the destination

A materialization keeps the destination it was created with (14 §14.1); changing
`default_destination_uri` affects new ones. Because a store is chosen per object URI, a deployment whose
destination moved from `gs://old` to `s3://new` keeps writing, maintaining, tailing and serving the old
tables on GCS and the new ones on S3, given credentials for both. Moving a table between backends is not
offered: its metadata names its files absolutely, so a move is a new materialization.

### 7.2 Rotating credentials

A Secret's change reaches a pod at its next start, as the protection key does (22 §22.9). No store
caches a credential beyond what its SDK refreshes itself.

### 7.3 An endpoint that moves

`STORAGE_S3_ENDPOINT` and `STORAGE_AZURE_ENDPOINT` are not part of a location, so a server that moves
needs a values change and a rollout, nothing in the bucket. That is why the Azure location keeps its
canonical form even on an emulator: the same objects copied to a real account are the same table.

### 7.4 Across domains

Each domain writes its own lake with its own credentials (20 §20.11); the console reads each through
the profile its locations match.

---

## 8. Security model

- **Credentials.** Pods receive Secret references or a cloud identity; a rendered runtime spec names a
  Secret and never a value. Nothing in a pointer, a table property, a snapshot summary, a heartbeat or an
  event names a credential. The console's registry file holds no secret, and no credential is logged:
  warnings name a variable, a length and a prefix at most.
- **Least privilege.** The runtimes, the maintenance app and the control plane need read, write, delete
  and list on the destination (S3: `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject` on its objects,
  `arn:aws:s3:::<bucket>/<prefix>*`, and `s3:ListBucket` on the bucket itself, `arn:aws:s3:::<bucket>`,
  without which S3 answers a missing object 403 rather than 404; a compare-and-swap, `PutObject` with
  `If-Match`, needs `s3:GetObject` besides `s3:PutObject`); the tail app read and list; the console read
  and list only — and for the console it matters: a locked DuckDB sandbox still allows `COPY … TO` a
  path it may read, so only the SQL guard stands between a statement and an overwrite when the
  console's identity can write. The installation guide gives one policy per cloud: a read-only
  SeaweedFS identity, an Azure "Storage Blob Data Reader" role or a read-and-list SAS, S3 `GetObject`
  and `ListBucket`, GCS `objectViewer`.
- **What a table may make the console read** is unchanged: every path a table names is checked against
  its location (console 12 §12.6), the cache path drops `.`/`..` segments and null bytes and is keyed by
  profile and container, and a statement's sandbox allows exactly the run's sources. A location no
  profile serves is refused, never guessed.
- **The gap that stays.** Storage enforces no dataset: whoever holds a profile's credentials reads every
  table under its scope (14 §14.8).
- **Emulators** use well-known or generated keys inside a cluster's network; Azurite's default account key
  is public and must never guard anything reachable from outside.

---

## 9. Failure modes

| Symptom | Cause | Behaviour |
|---|---|---|
| `create_table` fails with `invalid_destination` | a destination whose scheme is not one of three, or `file://` in a deployment | refused before any I/O, as a non-`gs://` destination is today |
| `create_table` fails naming a 401/403 | credentials missing or wrong for the destination's scheme | the step retries with backoff; the status names the scheme, never a value |
| a runtime stays in `waiting_for_table` | the runtime's pods lack the credentials the control plane has | the chart gives every app and the runtime spec the same Secret references |
| commits fail with `commit_failed` repeatedly | a server that mis-implements `If-Match` | run the conformance suite before using a store |
| a 409 `ConditionalRequestConflict` on AWS | two conditional writes in flight | `PreconditionFailedError`: reload and decide again |
| orphan cleanup deletes nothing | `last_modified` missing from a listing | an object whose age cannot be read is never deleted (11 §11.6) |
| a drop pass refuses every delete | a bucket policy, a lock, a retention rule | `DestinationDeleteError`, retried by the workflow, as today |
| a table "unreadable: no storage is configured for …" | a location outside every profile's scope | add a profile; the message names the location |

---

## 10. Performance

- **The hot path is unchanged in shape.** The committer reads a footer with one `head` and one or two
  range reads per file, writes manifests and one metadata file, and replaces the pointer; the writer
  uploads each file once. On GCS the store calls the same library methods with the same arguments as
  today, so the requests are the same; the store adds a function call and a URI parse per request.
- **Request parity.** A harness drives a scripted workload — table creation, appends with uploads and
  footer reads, a schema commit, compaction, expiry, an orphan scan, a drop — against the GCS client
  double on the base commit and on this branch, and compares the count of every request kind. Any
  difference is a regression to explain or fix.
- **A full backfill on a test cluster**, as handbook 20 §20.15 measured the domains release: the
  load-test topic's 111 million offsets, one replica autoscaling to 16 members, create to last commit,
  ingest at 16 members, and the console's request timings, for the base commit on GCS and this branch on
  GCS, then this branch on S3.
- **Install weight.** boto3 and the Azure SDKs are installed by every pod at start (D6); their download
  size and the install time are measured and reported.
- boto3's and Azure's clients are thread-safe and built once per process; `DefaultAzureCredential` once.

---

## 11. Test plan

### 11.1 Unit (every CI run)

- The existing suites over the memory store: proof that the move onto the store changed no behaviour.
- `test_store_contract.py`: the conformance suite over `MemoryStore` and `FileStore`.
- `test_store_uris.py`: parsing and validation of the four schemes; the drop guard on each.
- `test_store_gcs.py`, `test_store_s3.py`, `test_store_azure.py`: each backend over a stubbed SDK client
  (the existing GCS fake; botocore's `Stubber`; a stubbed Azure client): the exact parameters sent and the
  error mapping of §6.2.
- `test_config.py`, `test_settings.py`, `test_web.py`: destinations accepted and refused, the kind's name,
  the refusal text.
- Chart tests: the env of every app, the runtime spec and the console for each backend; `file://` refused;
  no Secret value inlined.
- Console: registry and profile matching, credential variables, DuckDB secret statements, the Azure
  rewrite, cache paths, a table outside every scope, a statement over two profiles, snippets by scheme.

### 11.2 Live conformance (CI and by hand)

A CI job starts SeaweedFS, Azurite, fake-gcs-server and moto and runs `test_store_live.py`: the
conformance suite against each, then the stores' backend-specific behaviour (batch deletes, large files).
The console's live tests read objects and run DuckDB against the same emulators. The same module runs on
a laptop with `STORAGE_LIVE_TESTS` naming the endpoints.

### 11.3 Against the real services

GCS through a test cluster's domains before they move to S3. Azure and AWS once someone runs the
conformance command against an account; until then the handbook says S3 is verified against SeaweedFS
and moto and Azure against Azurite.

### 11.4 On a test cluster

| | What | Passes when |
|---|---|---|
| S0 | the base commit and this branch on GCS: the backfill of §10, a materialization's life (create, commits, compaction, expiry, orphans, delete) | identical behaviour; performance within noise |
| S1 | both domains reinstalled on an in-cluster SeaweedFS; the backfill | exactly once by the E13 recipe (per-partition offset counts and sums against the topic; every live file present) |
| S2 | the committer deleted mid-commit, the runtime rescaled with files pending | no duplicate, no gap; the pointer's sequence strictly increasing across the metadata log |
| S3 | deleting a materialization | every object gone, in bounded passes |
| S4 | the console on both domains: Table, Schema, Data, live tail, exports, the Connect snippets run as printed | as on GCS |
| S5 | a temporary Azurite: the conformance suite from a pod; a materialization in one domain; a console statement joining it with an S3 table of the other | passes |

---

## 12. Rollout

- **Version** 1.1.0 (D9). No migration and no aliases (D10): every installation of this release is
  new.
- **Documentation**: this design in `docs/design/`; handbooks 01, 02, 03, 11, 12, 13, 14, 15, 16, 20;
  console handbooks 01, 03, 06, 07, 12, 13, 16; `docs/install.md` with one path per backend; `AGENTS.md`.
- Release notes in handbook 16 for 1.1.0, written with the release.

---

## 13. Implementation phases, mapped to files

| Phase | Scope | Proof |
|---|---|---|
| 1. The store | `lakehouse/store/`, its unit tests, the conformance suite | the contract suite over memory and file; the live suite against the emulators |
| 2. Onto the store | every call site of §5.5, the test support modules, the tests | the whole suite green over the memory store; request parity |
| 3. The chart | `storage:`, `console.storage[]`, helpers, templates, `ci/`, chart tests | `helm lint`/`template` on every values file; the chart tests |
| 4. The console | `server/storage/`, DuckDB per profile, the Settings page, the Connect snippets, tests | `npm run typecheck`, `npm test`, `npm run build`; a console against a test cluster |
| 5. Documentation and validation | the handbooks, the installation guide, the CI job, the test-cluster runs of §11.4 | handbook 13 |

**Coordination.** The data-protection follow-up that rewrites files already written (22 §15.3) changes the
maintenance app's I/O in the same files as phase 2. It merges first; this branch rebases onto it and
moves its storage calls onto the store.

---

## 14. Test clusters and laptops

- **S3: SeaweedFS.** One StatefulSet running `weed mini` with its data directory on a persistent volume,
  a Service for the S3 port, and an identities file from a Secret: a read-write identity for Midstream's
  apps and a read-only one for the console. A bucket per cluster; each domain writes under its own
  storage prefix. `docs/install.md` gives the manifests.
- **Azure: Azurite.** Started for the build and its validation and removed afterwards (D3), as
  `azurite-blob -l /data --skipApiVersionCheck --disableProductStyleUrl --disableTelemetry` with its own
  account (`AZURITE_ACCOUNTS`), a volume and graceful termination; a real storage account comes later.
- **GCS: fake-gcs-server** in CI; real GCS for a test cluster's GCS runs.

---

## 15. Decisions

| # | Decision | Owner's answer |
|---|---|---|
| D1 | The test cluster's S3 server: SeaweedFS as one `weed mini` pod on a persistent volume, or its full Helm chart | `weed mini` |
| D2 | Topology: both domains on the in-cluster SeaweedFS | yes: two domains, both on SeaweedFS |
| D3 | Azure: Azurite for the build and its validation, not part of the cluster's infrastructure; a real storage account later | yes |
| D4 | One Azure location form: `abfss://container@account.dfs.core.windows.net/…` | yes |
| D5 | `file://`: a shipped store for tests, refused by the chart | yes |
| D6 | boto3 and the Azure SDKs as plain dependencies rather than extras | plain |
| D7 | The console's credentials: a registry of profiles by destination prefix | yes |
| D8 | The setting kind `destination_uri`, and only that | yes |
| D9 | Version 1.1.0 | yes |
| D10 | Compatibility aliases for `gcs.*` and `CONSOLE_GCS_HMAC_*` | no: every installation is new |
| D11 | S3 on AWS documented as verified against SeaweedFS and moto until someone runs the suite on AWS | yes |

---

## 16. Sources

- Handbook 11 §11.2 (the pointer protocol), 14 §14.1 and §14.11, 20 §20.11; console 06, 07, 12 §12.6;
  `lakehouse/gcs.py`, `pointer.py`, `io.py`, `catalog.py`, `storage/objects.py`,
  `runtime_app/writer/upload.py`, `maintenance_app/compact_worker.py`,
  `console/server/data/{gcs,duckdb,objects}.ts`, `console/server/iceberg/io.ts`,
  `charts/midstream/templates/_helpers.tpl`, `python/tests/gcs_fakes.py`.
- Amazon S3's conditional writes (the S3 user guide; the boto3 `put_object` reference: `IfMatch` "412
  Precondition Failed", "409 ConditionalRequestConflict … fetch the object's ETag and retry";
  `IfNoneMatch` expects `*`).
- MinIO's repository, archived 2026-02-13; SeaweedFS releases and its S3 API and `weed mini` pages;
  Garage's known issues; the Azurite README; DuckDB's Azure extension and S3 API pages; moto's changelog.

---

## 17. Departures from the design

What the build changed, each checked against the code it names:

1. **Any failed pointer write is checked, refused or unanswered.** §3.3 item 4 re-reads the pointer on a
   conflict; the catalog does so for every pointer write that fails, whether refused or left without an
   answer (`catalog.py::_landed`). The pointer read back, and the current metadata's log as far back as
   it reaches, tell which it was: the write landed, and the commit stands; it provably did not (another
   commit moved on from where this one started, or the table was dropped), and the staged metadata file
   is deleted and `CommitFailedException` raised; or nobody can tell (the pointer has not moved, as
   under a write still in flight or S3's 409; it cannot be read; or it has moved on past the log's
   reach), and `CommitStateUnknownException` is raised with nothing deleted. A table's creation refused while it
   still has no pointer is unknown too. The staged metadata file's own create-only write counts as
   landed when the object at its name, which carries a fresh uuid, has the size sent.
2. **The maintenance app deletes a job's outputs only when its commit provably did not land**: a lost
   race, inputs gone, a mismatch, an error before the pointer write. An unknown outcome fails the job and
   keeps its outputs, which orphan cleanup removes if the table never took them; a commit that landed
   stands even when the table cannot be read back after it, and so does a snapshot expiry.
3. **A compaction or reprotect output whose create-only write was refused counts as written** when the
   object at its name, which carries a fresh id, has the local file's size, as the writer counts a seal's
   file (`compact_worker.py::upload_output`); any other object there fails the job.
4. **`delete_many` marks the failures of a request the store turned away as a whole**
   (`DeleteFailure.whole_request`): a GCS batch request, an S3 `DeleteObjects`, an Azure blob batch. The
   drop pass ends on them, since every request like it would fare the same (§5.5).
5. **`StoreConfigurationError`, never retryable and never read as a missing object**, is what S3's
   `NoSuchBucket`, Azure's configuration answers (`AzureConfigurationError`: a missing container, a
   refused credential or network, an unknown API version) and GCS's "The specified bucket does not
   exist." raise (§6.2); the GCS store judges a 404 itself, since the library answers `None` for any.
6. **A GCS store's `delete_batch_size` is per store**: 100, or 1 when it deletes one object per request
   (`GcsStore(batch_deletes=False)`, or a client that cannot batch), so the drop pass budgets 1,000 single
   deletes. The registry builds such a store when `STORAGE_EMULATOR_HOST` is set: fake-gcs-server cannot
   answer the library's batch requests.
7. **The conformance suite's racing creators race at most six keys** (`check.RACE_ROUNDS`, or
   `--race-rounds`), whatever the compare-and-swap rounds: SeaweedFS settles create-only writes racing for
   one key at about 0.6 s a writer, and six keys already race every writer six times.
8. **The chart refuses two `s3` console profiles of one bucket on different endpoints**, comparing
   endpoints as the console does — scheme and host in lower case, a default port dropped, none meaning
   AWS: an `s3://` path names no endpoint, so neither DuckDB nor the console could tell their objects
   apart. The console refuses them at start too.
9. **Reprotect, merged while this was built, moved onto the store with everything else** (§13): its
   child downloads each input and uploads each output create-only through the table's store, as
   compaction's does, and its purge lists the orphans and deletes through the app's store.
10. **The console's live tests run by hand, not in CI** (§11.2): `CONSOLE_STORAGE_LIVE=1 node --test
    server/storage/live.test.ts` reads through a direct and a cached profile of each backend from
    SeaweedFS, Azurite and fake-gcs-server, at their local addresses unless `CONSOLE_STORAGE_LIVE_*`
    names others. The Object stores job runs `test_store_live.py` alone.
11. **An Azure profile with an emulator `endpoint` but neither a connection string nor the account key
    is cached** (§5.8 had every Azure profile read in place): DuckDB sends a read authenticated by a
    token to the account's real host, never to an endpoint, so the console downloads its data files
    through the SDK instead, and says so at start (`storage/azure.ts`).
12. **The SQL guard also refuses `query()` and `json_execute_serialized_sql()`** (§5.8 named only
    `duckdb_secrets()` and `which_secret()`): both run SQL handed to them as a string, which no keyword
    check sees. All four are refused as calls, however spelled, so a table or column of the same name
    stays queryable (`data/sql.ts::FORBIDDEN_FUNCTIONS`).
13. **Azure transfers go in 8 MiB pieces** (§5.4 said 64 MiB): an upload larger than 8 MiB goes in
    8 MiB blocks, four in flight for a file, with the condition on the block-list commit, and a
    conditional one first reads the blob's ETag and stages no block when the condition already
    fails; a download larger than 8 MiB goes in 8 MiB chunks, each after the first under `If-Match`,
    and starts again, three reads at most, when the blob changes (`azure.py::TRANSFER_CHUNK_BYTES`).
14. **S3-compatible servers need no checksum settings** (§5.4 pointed at some): the S3 store keeps
    botocore's defaults, CRC32 request checksums and responses validated when supported, which
    SeaweedFS 4.51 and moto accept as they are (`s3.py`).
15. **The conformance suite has sixteen checks** (§5.6): besides the table's, `conditional_delete`,
    reported skipped for a store that offers no conditional delete, as S3's does not
    (`supports_conditional_delete`), and `large_object`, a file of `--large-mb` streamed up and down
    and create-only uploads of such files racing for one key, skipped without a size.
