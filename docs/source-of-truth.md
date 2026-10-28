# Midstream: Source of Truth

This document explains the Midstream repository as it exists in code at 1.0.0-rc.1: one
long-form narrative for engineering, design discussion and LLM-assisted changes. It covers every
part in a few paragraphs and points to the handbook for depth.

Where this document and the code disagree, the implementation is authoritative, in this order:

1. `charts/midstream/`
2. `python/src/midstream/`
3. `python/tests/`

> **Looking for more depth?** `docs/handbook/` is a 21-document reference manual: full field
> tables, decision tables, worked examples, an operations runbook, a test map and a known-gaps
> register. Start at [`docs/handbook/README.md`](./handbook/README.md) and load only the
> document you need; this file cites it as `handbook <n>`. The console has its own, `console/docs/`.

## 1. What this project is

Midstream continuously materializes JSON records from Kafka topics into **Apache
Iceberg tables, one per source topic**, in Google Cloud Storage, exactly once. A materialization
reads one or more topics with one dedicated runtime, and its topics can be changed later. It has a
self-service HTTP API, a schema that evolves by itself, table maintenance by an app of its own, an Iceberg REST catalog
any engine can attach, accounts and permissions, settings that change without a redeploy, and a web
console that reads the tables directly.

A user sends `{"name": "Shop", "tables": [{"source_topic": "orders", "dataset": "sales"},
{"source_topic": "payments.v2", "dataset": "finance"}]}`; the control plane claims each table's name
in its **dataset** — `sales.orders` and `finance.payments_v2`, named after the topics unless named,
unique across the deployment — samples each topic and infers its schema 0, creates the tables under
`<destination>/<storage prefix>/<materialization id>/<table>`, and deploys one dedicated Kaspr
runtime. The destination, the runtime's namespace and name, and each member's storage are the
deployment's, never the request's. A dataset is a named group of tables and their Iceberg namespace: SQL reads
`<dataset>.<table>`, a dataset holds tables of many materializations, and access is granted per
dataset. The materialization's name is only a label. The runtime's members consume every topic, stage records, seal them into Parquet files with
Iceberg field ids, and upload them; one member, the **committer**, commits them to each table and
acknowledges the Kafka offsets only once their rows are in it.

It is not an ETL engine (no transformations), not CDC (the table is the topic's log: tombstones are
rows, nothing is upserted), not multi-cloud (GCS only), and not a schema-registry client (values
must be JSON; schemas are inferred).

## 2. High-level architecture

```
HTTP ─▶ KasprWebView ─▶ materialization-requests ─▶ request-intake ─▶ materialization-work
                                                                           │
                                                           workflow-apply: discovery │ create_table │
                                                                apply_runtime │ await_runtime_drained │ drop_table
                                                                           │
           runtime KasprApp (one per materialization, every topic) ◀───────┘
             every member: writer ─▶ batch ─▶ pipeline (seal, upload, announce)
                                                         │
                                               <rt>-commits (one partition)
                                                         │
             one member: committer ◀─────────────────────┘
                commit · schema evolution
                         │
             Iceberg tables in GCS ◀── engines via the REST catalog, and the console
                         │            ◀── maintenance app: compaction · expiry · orphans, every table
                         │                 ▲ ticked each minute on <release>-maintenance-requests
                         │            the console ◀── tail app: each watched table's records past its
                         │                 snapshot, from Kafka (ticked on <release>-tail-requests)
                         │
             materialization-table-events ─▶ table-events agent ─▶ control-plane state
heartbeats ─▶ materialization-status ─▶ runtime-status agent ─┘   ▲
                                         resource-observation agent + sweeps
```

- **Control plane** — one long-lived KasprApp, `<release>-control-plane`. Owns the HTTP API and
  the REST catalog, all desired and observed state, creating and dropping tables, Kubernetes
  reconciliation, settings and identity. It never writes data files.
- **Data plane** — one dedicated KasprApp per materialization, rendered from Jinja2 templates and
  server-side applied. Every member writes files; the one owning `<rt>-commits` commits them.
- **Maintenance app** — one KasprApp, `<release>-maintenance`, beside the control plane: it
  compacts, expires and cleans up every materialization's tables, stopped ones included, so only it
  is sized for compaction's memory. It keeps no state; the control plane ticks it.
- **Tail app** — one KasprApp, `<release>-tail`: for each table someone watches in the console,
  the records past its committed snapshot, read from Kafka and built into rows by the seal's own
  code, served to the console alone. The console shows them beside the snapshot, each record once,
  within seconds of being produced (the **live tail**, handbook 19). It keeps no state and never
  writes a table; the control plane ticks it.
- **Console** — `/console`, a Node BFF and a React client. It talks to the control plane over HTTP only
  and reads the tables itself; it is meant to be extractable into its own repository.

Every app runs the same Python package (`<release>-control-plane`): the runtime side under
`runtime_app/`, the maintenance app under `maintenance_app/`, the tail app under `tail_app/`, the
Iceberg library they share under `lakehouse/`, the control plane everywhere else.
One wheel, pinned to an exact git commit (handbook 01, 16).

## 3. Repository layout

| Path | Contents |
|---|---|
| `python/src/midstream/` | The package. `models/` (immutable models with `to_dict`/`from_dict`), `storage/` (table keys, envelopes), `services/` (workflow, request intake, observation, table events), `handlers/` (thin Kaspr adapters), `web/` (WebView logic, the REST catalog views), `lakehouse/` (pointer catalog, table lifecycle, commits, schema policy, maintenance, REST protocol), `runtime/` (template rendering, naming, bindings), `runtime_app/` (writer, pipeline, committer, templates), `maintenance_app/` (ticks, job slots, the compaction child), `tail_app/` (the live tail: ticks, one consumer per member, buffers, answers, routing), `observation/`, `schema/` (discovery), `kubernetes/`, `kafka/`, `scheduler/`, `settings/`, `identity/` |
| `python/tests/` | The `unittest` suite: the executable specification |
| `charts/midstream/templates/` | The control plane's CRDs: app, agents, tasks, tables, webviews, RBAC; `maintenance/`, the maintenance app's; `tail/` and `webviews/tail-*.yaml`, the tail app's |
| `console/` | The Midstream Console and its handbook (`console/docs/`) |
| `docs/handbook/` | The reference manual |

## 4. Stack

Python ≥ 3.12; PyIceberg 0.12 with its minor pinned (the committer uses documented-but-internal
update APIs); pyarrow ≥ 18; google-cloud-storage ≥ 2.18; Jinja2; PyYAML; kubernetes-asyncio ≥ 33;
aiokafka from the Kaspr image. Kaspr (`kaspr.io/v1alpha1`) runs on twm-faust. Helm renders the
chart. Tests are stdlib `unittest`; the console uses `node --test`, `tsc` and Vite. There is no
linter or formatter by design (handbook 01 §1.8).

## 5. Deployed resources

**Control plane** (`charts/midstream/templates/`): one KasprApp; four tables — `state` (everything about
materializations), `settings`, `directory` (people, groups, roles, datasets, grants, table-name
claims; `identity` before 3.0), `identity-audit` — each written by exactly one agent;
8 agents (`request-intake`, `workflow-apply`, `resource-observation`, `runtime-status`,
`table-events`, `maintenance`, `settings-intake`, `identity-intake`); 8 tasks (observation sweeps,
purges, consumer-lag refresh, table-maintenance ticks, identity bootstrap and maintenance, the
runtime settings' lineage at startup); 67
WebViews, ten of them the REST catalog under `/catalog` and eight the datasets'. Every name derives
from the Helm release (handbook 02 §2.9).

**The maintenance app** (`charts/midstream/templates/maintenance/`): a KasprApp, the agent `maintenance-worker`
on `<release>-maintenance-requests` (six partitions, keyed by table) and the task
`maintenance-step` (every 5 s); 1 GiB requested, 3 GiB limit, one long job at a time per member
(handbook 02 §2.10a).

**The tail app** (`charts/midstream/templates/tail/`): a KasprApp of two members (1 CPU and 2 GiB requested, 2
and 4 GiB at most), the agent `tail-worker` on `<release>-tail-requests` (64 partitions, keyed by
table: a table belongs to the member assigned its partition), the task `tail-step` (every 5 s), and
four WebViews under `/tail/v1` that answer the console alone (handbook 02 §2.10b, 19).

**Each runtime**: eight resources — the KasprApp; `<rt>-state` (a one-partition global table: each
table's schema and committed offsets as the committer publishes them, stamped with the table's
uuid); agents `writer` (every source topic, `noack`), `committer` (`<rt>-commits`) and `settings`
(live settings); tasks `emit-status` (heartbeat every 30 s), `pipeline` (every 5 s) and
`commit-timer` (leader only). The spec hash ignores schemas and settings, so neither redeploys
anything (handbook 02, 10 §10.1).

**Vertical pod autoscaling**: every KasprApp, the runtimes' through the runtime spec, asks
kaspr-operator (0.19.5+) for a VerticalPodAutoscaler. It sets the kaspr container's CPU request
from use and resizes pods in place, keeping each request under its CPU limit; the limits and
memory stay as the chart sets them. Runtime members are limited to two cores, and a quiet
runtime's members reserve about 100m each (handbook 02 §2.10c).

## 6. Configuration and settings

Every component reads environment variables through one config object
(`cfg = get_config() if config is None else config`). On top, the **settings catalog**
(`settings/catalog.py`) lists what an administrator may override from the console, with bounds.
Overrides live in the `settings` table, written only by `settings-intake`; control-plane handlers
resolve their effective configuration per event, and runtime overrides travel on
`materialization-schema-authority-updates` to every runtime's settings agent, which applies them
within seconds; the maintenance settings reach the maintenance app with its ticks, within a minute.
Runtime settings carry a lineage, begun at the control plane's first start, and runtimes keep the
newest by lineage, then revision, so a control plane installed afresh over a delivery topic that
kept an earlier one's deliveries is still followed. An override wins over the environment until
reset (handbook 03, 17).

The settings that shape a table: `commit_interval_seconds` (30), `max_files_per_commit` (500), the
batch roll policy (250,000 rows / 256 MiB, 600 s ceiling), `seal_schema_wait_seconds`, the schema
limits (forks per field, struct fields, nesting depth, total fields), compaction thresholds
(`compaction_small_file_bytes`, `compaction_min_input_files`, `compaction_max_input_bytes` 1 GiB,
`compaction_hot_partition_delay_minutes`), snapshot retention (72 h, every snapshot of the last
hour, the newest 20 always), and `orphan_min_age_hours`.

## 7. The HTTP API

Every endpoint is a KasprWebView; there is no standalone server. Each one's first line is
`principal = authorize(request, directory_table, "<permission>")` — on the dataset a request is
about, for a dataset permission (only `GET /system/version` is public) — and
`AuthorizationWiringTests` fails a view that skips it. Mutations validate, publish an
event and answer **202**; agents persist. Groups (handbook 04):

- `/materializations` — create (`tables`, each a topic with its dataset and name), list (`?q=`),
  detail, stop, resume, update (the label, `replicas`, `autoscaling`, and the topics: `tables` with
  `removed_tables` keeping or dropping each removed topic's table), delete;
  per table `/tables/{table}/schema`, `/tables/{table}/schema/versions[/{id}]`, `/tables/{table}`
  (the table record); `/maintenance` (run a job now, on one table or all, kept ones included).
- `/catalog/v1/…` — the Iceberg REST catalog: config, a namespace per dataset the caller may read,
  its tables, load, commit (`catalog.write` on the dataset). Commits that would move a table's files
  are refused.
- `/datasets` — list (locked ones too), create, detail with tables, description, delete when empty,
  grants (handbook 04 §4.15).
- `/topics`, `/system/*`, `/settings/*`.
- `/auth/*` (sign-in answered at once with a signed session token; `/auth/tokens` for access
  tokens), `/users`, `/groups`, `/roles`, `/permissions`, `/audit`, `/identity/requests/{id}`.

## 8. Persisted state

All control-plane records are `RecordEnvelope`s (`entity_type`, `entity_id`, `version`,
`updated_at`, `payload`) in the `state` table (handbook 05 §5.2):

| Key | Holds |
|---|---|
| `materialization:<id>` | desired state: name, the tables (each a name and its topic) and the kept ones, destination, replicas and whether they are autoscaled, partition granularity |
| `materialization-status:<id>` | observed state: phase and reason, runtime health and progress (per table), resource observation, consumer lag (per topic) |
| `materialization-table:<id>:<table>` | each table's record: state (`active`, `retired`, `dropping`, `dropped`), location, UUID, latest snapshot, schema id, totals, committed offsets, maintenance history and running job, last error |
| `materialization-schema:<id>:<table>` / `materialization-schema-version:<id>:<table>:<n>` | each table's schema projection, current and earlier |
| `runtime-binding:<id>` | what was applied: app name, template version, spec hash, topics; a drain in progress |
| `runtime-autoscale:<id>` | autoscaling: the members it gave the runtime past its own count and why, and the last 16 lag measurements |
| `materialization-op:<id>:<op>` | idempotency |

The key `materialization:<id>` is also the Kafka key of every event about a materialization, so they
are processed in order on one partition — and, since a write to the global `state` table lands in
the shard of the event being handled, every record about it lives in that one shard. Scheduled
`__system__` steps (sweeps, the lag refresh, retention) only read and hand their work on, keyed by
the materialization (handbook 06 §6.4).

## 9. Topics

| Topic | Carries |
|---|---|
| `materialization-requests` | lifecycle requests from the WebViews |
| `materialization-work` | workflow steps, routed by the agents; delayed work goes through the scheduler topic |
| `materialization-status` | runtime heartbeats |
| `materialization-table-events` | `table_committed`, `schema_changed`, `table_error` from committers; `maintenance_started`, `maintenance_completed` and `table_error` from the maintenance app |
| `materialization-schema-authority-updates` | runtime settings (its 0.x name kept) |
| `<release>-maintenance-requests` | the maintenance app's ticks, requests and cancels, keyed `maintenance:<id>:<table>` |
| `<release>-tail-requests` | the tail app's ticks and cancels, each minute for every table, keyed `tail:<id>:<table>` (64 partitions) |
| `<rt>-commits` | inside a runtime: `data_file_ready`, `schema_change_request`, ticks, wakeups |
| `<release>-settings-requests`, `<release>-identity-requests` | settings and identity changes, each to its one agent |

Topics added by a deploy keep the old consumer-group assignment until one forced rebalance
(handbook 06, 14 §14.3).

Every app and runtime connects as one Kafka user, `kaspr.authentication`. On a cluster that
enforces ACLs it needs Create, Read and Write on topics prefixed `<release>-` (every app declares
its topics at every start), Read on groups and Write on transactional ids prefixed so (Kaspr runs
every app exactly once, transactionally), and Read on each source topic; Delete on `<release>-mat-`
topics and groups only to delete materializations; nothing on the cluster (handbook 21).

## 10. Lifecycle

**Create** (handbook 07): the WebView validates — each topic's dataset and table name (lowercase,
readable unquoted), the topics (at most `max_materialization_topics`); a request naming a
destination, namespace, runtime name or storage is refused, and with no `gs://` destination
configured no create is accepted (`409 destination_not_configured`, handbook 04 §4.5.2) —
previews the claim of the tables' names against the directory, asks Kafka for every topic — one it
does not list, missing or hidden from the Kafka user, is refused, `400 source_topic_missing`, and a
Kafka that does not answer is a `503` — and answers 202. `request-intake`
persists desired state and sends `claim_tables` to the directory; `identity-intake` claims every name
or none and answers `tables_claimed` — and the workflow emits `start_discovery` — or
`claim_rejected`, which marks the materialization deleted with the reason (handbook 07 §7.2a).

1. `start_discovery` samples up to 200 records of each topic and infers each table's schema 0 — for
   an empty topic, the metadata columns alone, the runtime adding fields as data arrives. A topic
   gone from the metadata since the request fails the create as `source_topic_missing`.
2. `create_table` writes each table's metadata v0 and catalog pointer, one table at a time: field ids 1–9 for the metadata columns
   (`_kafka_timestamp`, partition, offset, key, headers, `_materialized_at`, `_unmapped`), payload
   from 10; partitioned by the UTC day (or hour, or month, or none) of `_kafka_timestamp`; a location that cannot hold
   a table fails the step as `invalid_destination`.
3. `apply_runtime` renders the eight resources — one runtime for every table — and server-side
   applies them; observation follows.

Observation (resource observation with backoff 15/45/120/300 s, sweeps every 2 minutes, runtime
heartbeats merged in) moves the phase from `deploying` to `running` once the KasprApp is ready, the
members report and every table exists (handbook 08). Each observation also reads the runtime's pods:
members crash-looping, killed for memory or restarting often in the last 30 minutes make it
`degraded` (`runtime_members_restarting`), which a heartbeat cannot clear until they age out.

**Autoscaling** (handbook 07 §7.12a): after each minute's lag measurement, in the materialization's
shard, a running, healthy runtime more than 100,000 records behind that would take longer than 30
minutes to catch up at its pace gets the members that pace says it needs — at most 16, and no more
than its partitions keep busy — and its own `replicas` again after three measurements under the
floor. The scaled count is the control plane's record, not the materialization's `replicas`; every
render reads it, so the sweep keeps it. It is on unless the materialization or the deployment turns
it off; Kubernetes' horizontal autoscaler plays no part.

**Stop / resume / update**: the runtime is scaled to zero, back up, or to a new replica count; the
tables are untouched, and the maintenance app goes on maintaining them. **Changing the topics**
(handbook 07 §7.9a): an added topic is sampled and its table created; a removed topic's table is
kept read-only (`retired`, still maintained, written again if its topic comes back) or dropped. Kaspr's
static members keep their old assignment, so the runtime is *drained* — held at zero until its
consumer group is empty, about two minutes — the added topics' offsets are set, and it starts
again: about five minutes in all. A table re-created under an old name is a new incarnation, and
the runtime ignores what its predecessor left. **Delete**: the runtime is deleted, its Kafka topics and group cleaned up,
every table dropped (pointer retired; its objects removed while `destination_cleanup_enabled`,
Settings › Delete object data, is on), and the records purged (`purge_materialization_records`, or
the retention sweep after `deleted_retention_hours`).

## 11. Inside a runtime

**Writer** (handbook 10 §10.2): every member stages each record as a JSONL line in its partition's
open batch — never waiting on anything. It detects values the schema cannot take and asks the
committer for an evolution (shapes only, no values). Batches roll at 250,000 rows, 256 MiB, or after
120 s once reasonably full and 600 s in any case. A partition of each topic is its own batch stream;
the member's seal and upload slots, and its upload backlog, are shared by every table, so output stays
bounded however many topics it reads, and backpressure pauses every topic.

**Pipeline** (§10.4): a child process seals a batch — spooling its lines to a file per partition
value, then, one partition at a time, conforming every value to the schema and writing one zstd
Parquet file with field ids and statistics, a row group at a time, so a seal holds one row group
however far its batch spreads — then pool threads upload each file create-only, and the outbox announces it
on `<rt>-commits` with the seal's `after_offset`, `offset_start` and `offset_end`. Uncommitted files
are re-announced every 60 s and to every new committer. Backlog pauses consumption.

**Committer** (§10.6): one member, the owner of the one-partition commits topic, committing to
every table of the runtime, one commit at a time, the longest-waiting first. Every
`commit_interval_seconds` it decides each complete seal against the offsets committed *in its
table* — **accepted** (continues exactly where the table is), **covered** (already in), **overlap**
(partly in: the member re-seals the tail), **deferred** (a seal before it is missing), **error**
(file absent) — and commits the accepted files as one merge-append snapshot whose summary carries
the new offsets and `valid-through`, through the pointer's compare-and-swap, once, retrying by
reloading and deciding again. Covered files are never deleted (they may be the table's own); only an
overlapping seal's are. A member's announcements carry its session generation (its rebalance count)
and its process's epoch. The committer ignores only an older session of the same process, so a
restarted member is heard, though its count has started again.

**Acknowledgement** (§10.7): the writer runs `noack`; a record is acknowledged only once the table's
committed offsets cover its batch, so a partition's committed Kafka offset never passes a record
whose row is not in the table. A restart, a rebalance or a crash reads again from below, and the
committer refuses what the table holds: **exactly once**. Each batch holds back only its first
record; batches Faust would not honour that way (the first batch of a partition, gaps) are
acknowledged in strict order, working around twm-faust defects (handbook 14 §14.3).

**Schema evolution** (handbook 09): hands-free. A new key adds a nullable field; a type Iceberg cannot
widen forks a sibling `<name>__v<n>` with its own field id; a family past `max_forks_per_field`, a
wide struct or deep nesting collapses to `<name>__json`; past `max_schema_fields` values go to
`_unmapped`. Nothing is dropped or renamed. The fork map (`materializer.fork-map` property) names
every field's family and original key. The committer debounces requests, evolves, commits the
schema, and publishes it; a seal that saw the change waits for it.

**Maintenance** (§10.10, handbook 11 §11.6): not the runtime's — the maintenance app's. Each minute
the control plane ticks every written and kept table with its settings and when each job last ran;
the member owning the table's partition runs one long job at a time by default — compaction in a
child process then one `replace` commit, with the past partitions of a backfill taken several at a
time up to 1 GiB and the partition being written left until it settles; orphan cleanup with an age
guard — and snapshot expiry. Its commits and the committer's both go through the pointer's CAS and
retry. Jobs report `maintenance_started` and `maintenance_completed`; a drop waits for a running job.

**Heartbeat**: every member every 30 s — progress per table, health, settings revision and lineage; the
committer's adds a block per table (snapshot, totals, files pending).

## 12. The Iceberg tables

(handbook 11.) Iceberg v2, one table per source topic at
`<destination>/<storage prefix>/<materialization id>/<table>`, in the Iceberg namespace of its
dataset (`lake.<dataset>.<table>` to an engine):
`data/ts_day=…/` Parquet files and `metadata/`. The **catalog is a pointer**:
`metadata/catalog-pointer.json` names the current metadata file and is replaced only with a GCS
generation precondition, by the committer, the maintenance app, the control plane (create, drop) and the REST facade —
so no commit is ever lost and a pointer never moves backwards. `version-hint.text` lets path-based
readers find the latest metadata. Snapshot summaries carry `materializer.offsets` (the exactly-once
checkpoint) and `materializer.valid-through` ("is my record there yet?").

External engines attach `<control plane>/catalog` with an access token (`tok_…`, `catalog.read`, or
`catalog.write` to commit): DuckDB, PyIceberg, and engines on the same protocol. The facade vends no
storage credentials. `catalog.write` is for maintenance tools; a commit cannot move the table's
files, and Midstream assumes it is the only writer of data (handbook 14 §14.2).

## 13. Identity and settings, briefly

People sign in with email and password; the control plane issues signed session tokens. Users,
groups, roles, sessions, access tokens, datasets, their grants and the claims of table names live in
the `directory` table, written only by `identity-intake`, one request at a time on one partition.
Permissions are per action (for example `materializations.create`, `data.query`, `catalog.read`),
granted through roles held by groups; those about a dataset's tables (`dataset.view_schema`,
`data.query`, `data.export`, `catalog.read`, `catalog.write`, `dataset.write`, `dataset.manage`) are
also granted on single datasets, as Dataset Reader, Writer or Owner (handbook 18). The console signs
people in with an HttpOnly cookie, checks the same permissions on its own routes — per dataset — and
cuts its shared live views down to what each person may see.

## 14. The console

`console/` — a Fastify BFF and a React client (console 01). Its live store polls the control plane and fans
out snapshots over SSE. For data it reads each table itself: pointer, metadata and Avro manifests
from GCS (exact 64-bit ids, cached because they never change), so totals and time pruning cost no
data reads; data files through DuckDB, by field id, folding fork families into logical columns.
Every query is a **run** pinned to a snapshot (the current one; the runs API can ask for an older
one, the Data tab never does); a statement can read and
join any of the materialization's tables the person may query, each named `<dataset>.<table>`. Statements run in a DuckDB sandbox that can open only the
run's files and write only its own directory, and nothing is read outside each table's recorded
location (console 06, 07, 12). The Schema tab dates each change by when the data had it: the manifests'
per-file value counts and `_kafka_timestamp` bounds give each field's first and last value, in UTC,
as precise as the table's partitions (console 06 §6.3).

With the live tail on, a run also asks the tail app for the records past its snapshot's offsets and
reads them as one more Parquet file of the run: snapshot and tail are disjoint and follow on from
each other, so every record shows exactly once. The Data tab's **Live** mode asks again every 1.5 s
and starts a new run when a commit lands; a statement's *Auto re-run* runs it again 5 s after each
run (console 07 §07.9, handbook 19).

## 15. Commands

```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -s python/tests -p 'test_*.py' -v
python -m compileall python/src
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
cd console && npm install && npm run typecheck && npm test && npm run build
python python/tests/console_fixture.py      # regenerate the console's fixture table
```

Deploying and verifying on a cluster is in handbook 12; installing from scratch is `docs/install.md`.

## 16. Tests as the specification

The Python suite asserts payload shapes, workflow steps, observation, the lakehouse library against
an in-memory GCS fake (`gcs_fakes.py`, `lakehouse_support.py`) — commits, exactly-once decisions,
schema policy, compaction planning — runtime templates and pinning, settings, identity, and that
every WebView authorizes. The console suite runs its Iceberg reader against a table the control
plane's library wrote and its run engine against real Parquet. Behaviour that only a cluster shows
is in the experiments (E1–E14) and the E13 load test (handbook 13).

## 17. Sharp edges

From handbook 14, the ones most likely to matter:

- **Faust commit defects** force ordered acknowledgement for some batches until the twm-faust fix
  ships; a deploy adding topics needs one forced consumer-group rebalance.
- **The table is only as fresh as the commit interval**, plus a batch per partition: a busy
  backfill writes many small files per seal, which compaction works through afterwards. The
  console's live tail shows the newest records within seconds; engines on the REST catalog still
  read committed snapshots only.
- **A tail app member's restart is a 30–60 s outage for its tables** (they have no standby), and
  scaling the tail app restarts every member (handbook 14 §14.16).
- **Commit time grows with files per commit** — about 2 s plus 16 ms per file on the lab — so a
  backfill's commits take seconds.
- **Observation lags**: a degraded phase lingers until the next resource observation, up to 600 s;
  members that kept restarting keep it `degraded` for 30 minutes after the last restart.
- **Autoscaling reads the lag only**: a runtime held back by something other than members (a hot
  partition, a slow committer) gets members to no gain, and a scaled runtime keeps its count until
  it is caught up.
- **A change of topics stops the runtime for about five minutes**: the drain waits out the broker's
  session timeout.
- **Not multi-cloud, no shared runtime mode, no per-table access grants, no SSO.**
- **`catalog.write` is trusted**: an external commit's deletes or rewrites are not interpreted by
  the runtime.

## 18. Bottom line

A materialization is one or more topics, a dedicated runtime, and an Iceberg table for each topic. The control plane decides
what should exist and observes what does; the runtime's members write files and one committer
commits them exactly once, with the Kafka offsets inside the table's own snapshots; one maintenance
app keeps every table compacted and trimmed; one tail app serves the console each watched table's
newest records before they are committed, each exactly once beside the snapshot; the schema evolves
by forks, never by rewrites; and anything that speaks Iceberg REST — or the console — reads the
result.
