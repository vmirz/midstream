# 01 — System Overview

## 1.1 What the system does

Midstream continuously materializes JSON records from Kafka topics into **Apache
Iceberg tables — one per source topic — in Google Cloud Storage, Amazon S3 or an S3-compatible
server, or Azure Blob Storage**, exactly once, with a self-service HTTP API, schemas that evolve by
themselves, and an Iceberg REST catalog that any engine can attach.
A materialization reads one or more topics with one runtime, each into a table in a **dataset**: a
named group of tables and their Iceberg namespace, so SQL names a table `sales.orders`. A dataset
holds tables from many materializations, a materialization may write into several datasets, and
access is granted per dataset (18 §18.16). A materialization's name is only a label. Topics can be
added and removed after it is created (07 §7.9a).

A user submits:

```json
{ "name": "Shop", "tables": [ { "source_topic": "sales.orders", "dataset": "sales", "name": "orders" },
                              { "source_topic": "finance.invoices", "dataset": "finance", "name": "invoices" } ] }
```

and the system:

1. Assigns a `materialization_id`, and claims each table's name in its dataset — `sales.orders`,
   `finance.invoices` — so no other table can take it (18 §18.16).
2. Samples up to 200 records of each topic, all at once, and infers each table's schema 0 (09 §9.6).
3. Creates the Iceberg tables under the deployment's destination, in
   `<destination>/<release>/<materialization id>/` — `gs://example-lake/…`, `s3://example-lake/…` or
   `abfss://lake@exampleaccount.dfs.core.windows.net/…` — one directory each, partitioned by the day
   of `_kafka_timestamp`, each with a catalog pointer (11).
4. Renders and applies **one dedicated Kaspr runtime application** for both into Kubernetes.
5. That runtime's members consume both topics, mask the values of fields that look sensitive —
   emails, phone numbers and national identifiers hashed, secrets withheld, unless the
   materialization chose otherwise (22) — stage each record as JSONL in its table's batch,
   seal batches into Parquet files with Iceberg field ids — in a child process, off the consumer's
   event loop — and upload them. **One member, the committer, commits them to the tables**: per
   table, one append per commit interval, whose snapshot summary carries the Kafka offsets it covers.
   A record's offset commits in Kafka only once its row is in its table, so every record lands exactly
   once (10 §10.6, §10.7). More topics never mean more seals, uploads or commits at once (10).
6. When records carry fields its table's schema does not have, the committer evolves that schema —
   a new column, a fork for a changed type, JSON for a family past its limits — without a deploy
   and without any writer waiting (09).
7. A separate **maintenance app**, one for every materialization, compacts small files, expires old
   snapshots and removes orphans, so only it is sized for compaction's memory (10 §10.10, 11 §11.6).
8. A third app, the **tail app**, reads the topics of the tables someone is watching in the console
   and serves their records past the committed snapshot, built by the seal's own row builder: the
   console shows them beside the snapshot, each record exactly once, within seconds of being
   produced (19).
9. Status flows back continuously: `GET /materializations/{id}` and its table records reflect
   reality, and the Midstream Console reads the tables themselves — joining them in SQL if asked.
   An engine on the REST catalog reads `lake.sales.orders`, and joins it with any table, in any
   dataset, its token may read.

## 1.2 The two planes

### Control plane

A single Kaspr application (`<release>-control-plane`) deployed once per environment. It owns
desired state and observes the runtimes; it never touches data files.

| Concern | Implementation |
|---------|----------------|
| HTTP API | 59 `KasprWebView` CRDs (`charts/midstream/templates/webviews/`), each authorizing its caller first |
| Desired state | `materialization:` records in the global `state` table |
| Observed state | `materialization-status:` records |
| Table records | `materialization-table:<id>:<table>` records: location, latest snapshot, totals, maintenance history |
| Schema projection | `materialization-schema:<id>:<table>` records, a read-only copy of each table's schemas |
| Runtime bookkeeping | `runtime-binding:` records |
| Idempotency | `materialization-op:<id>:<op_id>` records |
| Reconciliation | 8 agents + 7 tasks |
| Table lifecycle | creating a table and its pointer; dropping it on delete or when its topic is removed (`lakehouse/`) |
| Table maintenance | ticks every table each minute for the maintenance app (07 §7.13) |
| Live tail | names every tailed table each minute for the tail app, and cancels the others (19 §19.4) |
| Iceberg REST catalog | 10 WebViews under `/catalog`, for DuckDB, PyIceberg and other engines (11 §11.8) |
| Kubernetes writes | server-side apply against `kaspr.io/v1alpha1` resources |
| Kafka cleanup | topic and consumer-group deletion on materialization delete |
| Settings | overrides chosen in the console: the `settings` table, the `settings-intake` agent, three WebViews (17) |
| Identity and access | users, groups, roles, sessions, access tokens, datasets, their grants and the claims of table names, and the audit log: the `directory` and `identity-audit` tables, the `identity-intake` agent, two tasks, 38 WebViews (18) |

### Data plane (runtime)

One `KasprApp` per materialization, plus a table, three agents and three tasks — **eight Kubernetes
resources**, rendered from Jinja2 templates in
`python/src/midstream/runtime_app/templates/`.

| Resource | Template | Role |
|----------|----------|------|
| `<rt>` (KasprApp) | `kaspr-app.yaml.j2` | runtime container, env, storage, version pin |
| `<rt>-state` (KasprTable) | `state-table.yaml.j2` | global, one partition: each table's schema and committed offsets as the committer publishes them |
| `<rt>-writer` (KasprAgent) | `writer-agent.yaml.j2` | consumes every source topic, stages each record in its table's batch, rolls batches, detects drift; `noack`, acknowledging records once they are in their table |
| `<rt>-committer` (KasprAgent) | `committer-agent.yaml.j2` | consumes the one-partition `<rt>-commits` topic: commits files and schema changes |
| `<rt>-settings` (KasprAgent) | `settings-agent.yaml.j2` | consumes the control plane's runtime control topic: live settings |
| `<rt>-emit-status` (KasprTask) | `status-task.yaml.j2` | heartbeat every 30 s onto `materialization-status` |
| `<rt>-pipeline` (KasprTask) | `pipeline-task.yaml.j2` | every 5 s: advance the batch pipeline — seals in child processes, uploads on pool threads — announce files, release acknowledgements |
| `<rt>-commit-timer` (KasprTask) | `commit-timer-task.yaml.j2` | leader only, every 5 s: a `commit_tick`, the committer's clock; a commit is due `commit_interval_seconds` after the last |

Both planes run the **same Python package** (`midstream-platform`). The runtime side lives
under `runtime_app/`, the maintenance app's under `maintenance_app/`, the Iceberg library they all
use under `lakehouse/` — and under `lakehouse/store/` the object store layer they reach the lake
through — the control-plane side everywhere else. One wheel, one install, one version pin.

### The maintenance app

One more Kaspr application, `<release>-maintenance` (`charts/midstream/templates/maintenance/`), deployed
once per environment beside the control plane: compaction, snapshot expiry, orphan cleanup and
reprotect (masking rows written before a protection change, 22 §22.14) for every materialization's
tables, running or stopped. It keeps no state: the control plane ticks
each table every minute on `<release>-maintenance-requests`, keyed by table, so each member
maintains the tables of its partitions, and reports on `materialization-table-events`. Compaction
is the memory-hungry job, so this app alone is sized for it, and the runtimes are not (10 §10.10,
02 §2.10a).

### The tail app

A third Kaspr application, `<release>-tail` (`charts/midstream/templates/tail/`), deployed once per
environment: for each table someone is watching, the records past its committed offsets, read from
Kafka and built into rows by `lakehouse/rows.py::build_row` — the seal's own code — and served to
the console as Parquet under `/tail/v1`. It keeps no state: the control plane names every tailed
table each minute on `<release>-tail-requests`, keyed by table, so each member tails the tables
of its partitions and forwards a request for another's. It never writes a table (19).

### The object store layer

Every app and runtime reaches the lake through `lakehouse/store/` (11 §11.1a): an `ObjectStore`
protocol of the few operations Midstream asks of a bucket — read an object with the version read;
read its size, version and age; write it unconditionally, create-only, or as a compare-and-swap on
the version read; delete one or many; list a prefix — and one store per URI scheme, chosen by
`store_for(uri)`:

| Scheme | Store | Backend, through |
|---|---|---|
| `gs://` | `GcsStore` | Google Cloud Storage, google-cloud-storage |
| `s3://` | `S3Store` | Amazon S3 or an S3-compatible server that honours conditional writes, boto3 |
| `abfss://` | `AzureBlobStore` | Azure Blob Storage, azure-storage-blob, through the account's Blob endpoint |
| `file://` | `FileStore` | the local disk: tests and a laptop only |

The pointer catalog, PyIceberg's FileIO, the writer's uploads, the committer, the maintenance app,
the tail app, table creation and drops and the REST catalog all go through it, and none of them
knows which backend serves a table. A table's location names its backend for good, and the
deployment's destination decides it for new materializations, so one deployment may hold tables on
several backends. Each process keeps one store per scheme, with one identity — the SDK's own
credential variables, or the pod's cloud identity — and Midstream's `STORAGE_*` variables: an S3
endpoint, region and path style, an Azure emulator's endpoint (02 §2.9a, 03 §3.3a).

The conformance suite, `python -m midstream.lakehouse.store.check <uri>`, checks a store's
conditional writes — create-only, compare-and-swap, sixteen writers racing with exactly one winner
per round — and is run against a store before a deployment is pointed at it (02 §2.11). CI runs it
against SeaweedFS and moto for `s3://`, Azurite for `abfss://` and fake-gcs-server for `gs://`
(02 §2.13). S3 is verified against SeaweedFS and moto, not against AWS itself, and Azure Blob Storage
against Azurite, not yet against a real storage account.

### The console

The Midstream Console (`/console`) is a Node BFF and a React client. It talks to the control plane
over HTTP only, and reads tables itself, through storage profiles of its own — one per destination
prefix, each with credentials of its own, which should be read-only — the longest matching scope
serving each object. It reads their pointers, metadata and manifests with each backend's SDK, and
their data files through DuckDB, in place with a secret scoped to the profile or from a downloaded
copy, so one statement may join tables on different backends (console 03 §3.6a, 06, 07); a location
no profile covers is never read. It is meant to be extractable into its own repository.

## 1.3 Why "dedicated runtime" and not one shared consumer

Each materialization gets its own Kaspr app so that:

- Kafka consumer groups, offsets and rebalances are isolated per materialization.
- One bad source topic (malformed records, huge messages) cannot stall others.
- Each table has exactly one committer — the runtime member owning `<rt>-commits` — so commits
  never race each other.
- Scaling, stopping and deleting are per-materialization Kubernetes operations.

The trade-off is Kubernetes resource pressure: N materializations means N StatefulSets. The model's
`runtime_mode` allows `"shared"`, but `build_dedicated_runtime_template_input()` refuses it — shared
mode is not implemented (14 §14.1).

## 1.4 Control loops

The system is a set of cooperating loops. None of them block an HTTP request.

### Loop A — Request intake

```
WebView (202 Accepted, does NOT persist)
   └─▶ materialization-requests topic
          └─▶ request-intake agent
                 ├─ persists Materialization + MaterializationStatus
                 ├─▶ <release>-identity-requests (claim or release of table names)
                 └─▶ materialization-work topic (first step; for a create, once the names are claimed)
```

The WebView returns 202 immediately after validating input. **Nothing is persisted by the
WebView** (`tests/test_web.py` asserts it). Persistence happens only in `request-intake`, which
makes the request topic the single ordered source of truth for lifecycle mutations.

### Loop B — Workflow execution

```
materialization-work
   └─▶ workflow-apply agent  (routed outputs)
          ├─ "work"      → back onto materialization-work
          ├─ "scheduler" → scheduler topic with x-scheduler-* headers (delayed delivery)
          └─ "topic"     → another topic (runtime settings for a runtime being applied)
```

| Consumed by | Steps (`WORK_STEPS`, `models/events.py`) |
|-------------|-------------|
| `workflow-apply` | `tables_claimed`, `claim_rejected`, `start_discovery`, `discover_tables`, `create_table`, `apply_runtime`, `await_runtime_drained`, `delete_runtime`, `cleanup_runtime_kafka`, `drop_table`, `purge_materialization_records` |
| `resource-observation` | `observe_runtime`, `sweep_observations` |
| `maintenance` | `purge_deleted_materializations`, `refresh_consumer_lag`, `record_consumer_lag`, `schedule_table_maintenance` |

### Loop C — Observation

```
observe_runtime work
   └─▶ resource-observation agent
          ├─ reads the KasprApp via the Kubernetes API
          ├─ derives observed_phase / phase_reason
          ├─ persists the updated MaterializationStatus
          └─▶ schedules the next observation with backoff (15 s, 45 s, 120 s, 300 s)
```

Plus a leader-only cron sweep every 2 minutes (`*/2 * * * *`) and a startup sweep that re-observes
everything that looks stale, and re-applies a runtime whose rendered spec changed (08).

### Loop D — Runtime heartbeat

```
<rt>-emit-status task (every 30 s, on every member)
   └─▶ materialization-status topic
          └─▶ runtime-status agent
                 └─ merges progress, health and phase into MaterializationStatus;
                    the committer member's heartbeat carries the table block
```

### Loop E — Data into the table

```
writer agent (every member): record → JSONL line in its table partition's open batch
   └─ batch rolls → pipeline: seal (child process) → upload → announce
          └─▶ <rt>-commits  data_file_ready
                 └─▶ committer (one member): per table, decide each seal, append, CAS its pointer
                        ├─▶ materialization-table-events  table_committed
                        └─ members see the committed offsets cover their batch → acknowledge
```

Exactly once: a file the table already covers is refused, a gap waits, and the offsets commit in
the same snapshot as the rows (10 §10.6).

### Loop F — Schema evolution

```
writer sees values the schema cannot take (drift detection, no waiting)
   └─▶ <rt>-commits  schema_change_request (shapes only)
          └─▶ committer: debounce window → evolve → commit the new schema
                 ├─▶ <rt>-state  schema:<table> (every member's next seal conforms to it)
                 └─▶ materialization-table-events  schema_changed (naming the table)
                        └─▶ table-events agent → the table's schema projection and record
```

A seal that saw the change waits up to `seal_schema_wait_seconds` for it; values nothing can hold
go to `_unmapped` (09).

### Loop G — Maintenance

```
schedule-table-maintenance (every minute, leader) or POST /materializations/{id}/maintenance
   └─▶ maintenance agent: reads the table records, writes nothing
          └─▶ <release>-maintenance-requests  maintenance_tick / maintenance_request /
              maintenance_cancel, keyed maintenance:<id>:<table>
                 └─▶ maintenance app: per member, one long job at a time — compaction or
                     reprotect (child process, then one replace commit), orphan cleanup;
                     snapshot expiry
                        └─▶ materialization-table-events  maintenance_started / maintenance_completed
```

The app's commits go through the same pointer CAS as the committer's appends; a replace that loses
the race re-reads the table and tries again (10 §10.10). The same step names every tailed table for
the tail app:

```
   maintenance agent ─▶ <release>-tail-requests  tail_tick / tail_cancel, keyed tail:<id>:<table>
                           └─▶ tail app: the owning member reads the table's topic while someone asks
console run ─▶ GET /tail/v1/materializations/{id}/tables/{table}?after=<the snapshot's offsets>
                           └─▶ the records past them, as Parquet, read beside the snapshot (19)
```

### Loop H — Settings

```
console Settings page ─▶ PATCH /settings/{scope} ─▶ <release>-settings-requests
   └─▶ settings-intake agent ─▶ settings table
          ├─ every control-plane handler resolves its configuration from it, per event
          └─ runtime scope ─▶ materialization-schema-authority-updates (materialization_id "*")
                                └─▶ every runtime's settings agent ─▶ applied within seconds
```

An override wins over the environment variable until it is reset. Nothing restarts (17).

### Loop I — Identity

```
every WebView ─▶ authorize(request, directory table, permission[, dataset])   401 / 403, or go on
POST /auth/login ─▶ password checked, signed session token issued at once
   └─▶ <release>-identity-requests (key "identity": one partition, strictly ordered)
POST /users, /groups, /roles, /datasets, /auth/tokens … ─▶ same topic
request-intake ─▶ claim_tables / release_tables ─▶ same topic
   └─▶ identity-intake agent ─▶ directory table (+ identity-audit)
          ├─ directory revision + 1 ─▶ every replica's next request sees the change
          └─▶ materialization-work: tables_claimed / claim_rejected, keyed by the materialization
```

People, groups, roles, datasets, grants, tokens and table-name claims change only through the
agent, one at a time, which keeps emails and `<dataset>.<table>` names unique, keeps an
administrator, and stops anyone granting access they do not hold (18).

## 1.5 Request lifecycle at a glance

```
POST /materializations
      │  202 { materialization_id, request_id, status_url, ... }
      ▼
materialization-requests ──▶ request-intake
      │                          persists: desired_phase=running, observed_phase=requested, claiming_tables
      │                          sends claim_tables ──▶ identity-intake ──▶ step=tables_claimed
      ▼
step=start_discovery    samples up to 200 records per topic, builds each table's schema 0; observed_phase=discovered
      ▼
step=create_table       per pass, creates one Iceberg table and its pointer and persists its record
      ▼
step=apply_runtime      renders 8 resources, server-side applies them
      │                 persists RuntimeBinding; observed_phase=deploying
      └──▶ scheduler: observe_runtime (attempt 1, +15 s)
      ▼
observe_runtime → deploying (waiting_for_runtime_ready, then waiting_for_table)
      │  once ready and the heartbeats are healthy → running
      ▼
members stage, seal, upload; the committer commits; table events keep the table records current
```

## 1.6 Key invariants

1. **The WebView never writes to the table.** Only `request-intake` and the reconciliation agents
   persist control-plane state; the `settings` and `identity` tables likewise have one writer each,
   their intake agent.
2. **All persisted records are wrapped in a `RecordEnvelope`** with `entity_type`, `entity_id`,
   `version`, `updated_at`, `payload`. Readers reject envelopes whose `entity_type` does not match.
3. **The runtime spec hash is invariant to schema changes, settings overrides and protection
   policies.** Schemas live in the table, and settings and policies ride the runtime control topic,
   so none of them redeploys anything (`tests/test_runtime_templates.py`, `test_runtime_settings.py`,
   `test_workflow.py`).
4. **Every runtime installs exactly the package the control plane runs.** `installed_package_source()`
   reads how pip installed it: from git, the runtime pins that commit at that repository; from a
   package index, that version. A local install is refused, since no pod could repeat it. Runtimes
   never float on a branch.
5. **Every commit goes through the pointer's compare-and-swap.** The control plane creates
   `metadata/catalog-pointer.json` and retires it on a drop; between the two only the runtime's
   committer, the maintenance app — and the REST facade, for an engine with `catalog.write` —
   replace it, always on the condition that it is still at the version read: a GCS generation
   precondition, an S3 or Azure `If-Match` on its ETag. So no commit is ever lost to another, and a
   write whose answer was lost is judged by the pointer read back: its staged metadata is deleted
   only when the commit provably did not land (11 §11.2).
6. **Exactly once into the table.** A partition's committed Kafka offset never passes a record
   whose row is not in the table, and the committer refuses a file the table already covers
   (10 §10.7). The E13 load test checked it on a test cluster with the committer deleted mid-commit and
   mid-compaction and the runtime rescaled with files pending; its first run, which also `SIGKILL`ed
   a member and the committer, found the defect fixed in 223ae74 (13 §13.8).
7. **Every WebView authorizes before it acts.** Its first line is
   `authorize(request, directory_table, "<permission>")` — on the dataset a request is about, for a
   dataset permission; only `GET /system/version` is public.
   `tests/test_agent_manifests.py::AuthorizationWiringTests` fails for a view that skips it.
8. **A snapshot and its live tail hold each record once, as the table will.** The tail serves only
   records past the offsets a snapshot carries, and builds their rows with the seal's own
   `build_row`, so the console shows each record exactly once and in the row the commit will hold
   (`tests/test_tail_exactly_once.py`, `test_tail_parity.py`; 19 §19.2).
9. **No protected value is staged plain.** The writer and the tail mask before staging; a member that
   must hash and has no key writes nothing rather than write plain (`test_runtime_protection.py`;
   22 §22.5).

## 1.7 What this system is *not*

- Not a general ETL engine: no transformations, filters, joins or projections.
- Not a CDC pipeline: a table is its topic's log — tombstones are rows and nothing is deleted or
  upserted (14 §14.1).
- Not for every object store: the pointer's compare-and-swap needs a store that keeps conditional
  writes — Google Cloud Storage, Amazon S3 or an S3-compatible server that honours them, Azure Blob
  Storage — which the conformance suite checks (11 §11.1a); `file://` serves tests and a laptop only.
- Not a mover between stores: a table stays where it was created, since its metadata names every
  file by its full URI; moving one to another bucket or backend is a new materialization.
- Not a schema registry client: schemas are inferred from message content, and values must be JSON.

## 1.8 Technology stack

| Layer | Technology | Version constraint |
|-------|-----------|--------------------|
| Language | Python | `>=3.12` (`python/pyproject.toml`) |
| Packaging | setuptools | `>=69` |
| Stream framework | Kaspr (`kaspr.io/v1alpha1` CRDs), on twm-faust | operator-provided |
| Table format | Apache Iceberg v2, through PyIceberg | `pyiceberg[pyarrow]>=0.12,<0.13` (pinned minor: the committer uses internal update APIs) |
| Columnar format | pyarrow | `>=18.1.0` |
| Object storage | google-cloud-storage; boto3; azure-storage-blob and azure-identity | `>=2.18.0`; `>=1.36` (conditional `PutObject`); `>=12.31.0,<13` (from 12.31 a chunked upload's block ids are unique) and `>=1.19` |
| Templating | Jinja2 | `>=3.1.0` |
| Config parsing | PyYAML | `>=6.0.0` |
| Kubernetes client | kubernetes-asyncio | `>=33.1.0` |
| Kafka admin/consumer | aiokafka | **not declared**; provided by the Kaspr runtime image |
| Console | Node 24+, React, DuckDB (`@duckdb/node-api` 1.5.5), avsc; `@google-cloud/storage`, `@aws-sdk/client-s3`, `@azure/storage-blob` and `@azure/identity` for its storage profiles | `console/package.json` |
| Deployment | Helm (`charts/midstream`) | `helm` 3.12+ |
| Tests | stdlib `unittest`; `node --test` in `/console` | — |

There is **no linter or formatter configured** in this repository. Do not introduce one.

## 1.9 Repository layout

```
/AGENTS.md                          Agent instructions (short, always-on)
/docs/source-of-truth.md                Condensed end-to-end narrative
/docs/handbook/                     This handbook
/charts/midstream/                  The Helm chart: values.yaml; templates/ (apps, agents, tasks, tables, webviews, console); ci/ values
/docs/install.md                    Installing it
/python/
  pyproject.toml                    Package metadata and dependencies
  src/midstream/
    config.py                       Control-plane config + defaults
    protocol_logging.py             Structured protocol event logging
    models/                         Domain, event, status, table, binding, envelope models
    storage/                        Table keys, envelopes, persistence helpers
    services/                       Workflow orchestration (request intake, workflow, observation, table events)
    handlers/                       Thin Kaspr-facing adapters
    web/                            WebView validation, mutations, responses; the REST catalog
    lakehouse/                      Iceberg: pointer catalog, table creation, commits, schema policy, maintenance, REST
      store/                        The object store layer: a store per scheme (GCS, S3, Azure Blob, file, memory), the conformance suite
    identity/                       Permissions, principals, authorization, session and access tokens, passwords
    settings/                       Settings catalog and effective-configuration resolution
    observation/                    Status derivation, backoff, staleness
    schema/                         Discovery
    runtime/                        Template rendering, naming, bindings
    runtime_app/                    Runtime-side code + Jinja2 templates
    maintenance_app/                The maintenance app: ticks, job slots, compaction child
    tail_app/                       The tail app: ticks, the consumer, the buffer, answers, routing
    kubernetes/                     Server-side apply client
    kafka/                          Topic/consumer-group admin, metadata
    scheduler/                      Routed-output builders
  tests/                            71 unittest modules
/console/                           Midstream Console: BFF + React client (self-contained)
  docs/                             Console handbook (17 documents)
```

## 1.10 Where to go next

- Deploying or reading manifests → [02 — Deployment and manifests](./02-deployment-and-manifests.md)
- Where the lake lives, and each backend's credentials → [02 §2.9a](./02-deployment-and-manifests.md), [03 §3.3a](./03-configuration-reference.md)
- Tuning behavior → [03 — Configuration reference](./03-configuration-reference.md)
- Calling the API or attaching an engine → [04 — HTTP API reference](./04-http-api-reference.md)
- Signing in, users, roles, access tokens → [18 — Identity and access](./18-identity-and-access.md)
- Understanding a specific transition → [07 — Control-plane workflows](./07-control-plane-workflows.md)
- Debugging why something is `degraded` → [08 — Observation and status](./08-observation-and-status.md)
- Debugging columns, forks, `_unmapped` → [09 — Schema evolution and forks](./09-schema-evolution-and-forks.md)
- How records reach the table → [10 — Runtime internals](./10-runtime-internals.md)
- How the newest records reach the console before they are committed → [19 — The live tail](./19-live-tail.md)
- Which values are masked, and how → [22 — Data protection](./22-data-protection.md)
- The table, its files and maintenance → [11 — Iceberg tables and storage](./11-iceberg-tables-and-storage.md)
