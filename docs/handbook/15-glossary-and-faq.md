# 15 — Glossary and FAQ

## 15.1 Glossary

**Access token** — A credential a person makes for a tool, `tok_<id>_<secret>`, carrying
`catalog.read` (and optionally `catalog.write`), never more than its owner holds. Shown once; only
its hash is stored ([18 §18.15](./18-identity-and-access.md)).

**Ack ledger** — `runtime_app/acks.py`. Holds back a source record's Kafka acknowledgement until the
table's committed offsets cover its batch, so a committed Kafka offset never passes a row that is not
in the table ([10 §10.7](./10-runtime-internals.md)).

**Agent** — A Kaspr `KasprAgent`: a long-running stream processor bound to Kafka topics. The control
plane runs 8 (`request-intake`, `workflow-apply`, `resource-observation`, `runtime-status`,
`table-events`, `maintenance`, `settings-intake`, `identity-intake`); each runtime runs 3 (`writer`,
`committer`, `settings`); the maintenance app and the tail app one each (`maintenance-worker`,
`tail-worker`).

**Announcement** — A member's `data_file_ready` message on `<rt>-commits`: a sealed, uploaded file
and its seal's offsets. Repeated every `announce_retry_seconds` until a commit covers it
([06 §6.8](./06-event-and-topic-contracts.md)).

**Audit log** — Every sign-in, failed sign-in, lockout and change to users, groups, roles and access
tokens, in the `identity-audit` table, newest first through `GET /audit`. Kept for
`audit_log_retention_days` (90).

**Autoscaling** — The control plane running a runtime with more members than its materialization's
`replicas` while it is far behind its topics, and with its own count again once caught up. Decided
after each lag measurement, from the lag and its pace; on for every materialization unless it or the
deployment turns it off ([07 §7.12a](./07-control-plane-workflows.md)). Kubernetes' horizontal
autoscaler plays no part.

**Backlogged (live tail)** — A table whose runtime is more than twice `live_tail_max_rows_per_table`
records behind its topic (a backfill, a burst): the tail app reads nothing for it but its end offsets
until the runtime is within 75 % of the bound, and the console offers the **head peek** meanwhile
([19 §19.6](./19-live-tail.md)).

**Batch** — A unit of buffered output on one member and partition: a JSONL file while open, one
Parquet file per table partition once sealed, uploaded, announced, and released once a commit covers
it. Identified as `batch-<member>-p<partition>-o<start_offset>`.

**Batch pipeline** — `runtime_app/pipeline.py`. Seals rolled batches in child processes, uploads
them from a thread pool, announces them, and releases acknowledgements, off the writer's event loop
([10 §10.4](./10-runtime-internals.md)).

**Binding** — `RuntimeBinding`, stored at `runtime-binding:<id>`: which KasprApp a materialization is
bound to, its `last_applied_spec_hash` and `last_apply_operation_id`, and the managed agent names.
**A materialization without a binding is invisible to the sweep.**

**Bootstrap administrator** — The account named by the auth Secret's `bootstrap-admin-email`, made
(or restored) an administrator at startup whenever no active administrator exists. The way back in
when every administrator is lost.

**Catalog pointer** — `<table>/metadata/catalog-pointer.json`: names the table's current metadata
file. Every commit replaces it with a GCS generation precondition — the compare-and-swap that makes
the bucket a catalog ([11 §11.2](./11-iceberg-tables-and-storage.md)).

**Collapse** — What a field family does past its limits (`max_forks_per_field`, `max_struct_fields`,
`max_nesting_depth`): its values go to `<name>__json` as JSON text from then on
([09 §9.5](./09-schema-evolution-and-forks.md)).

**Committed offsets** — `materializer.offsets` in a snapshot's summary: per Kafka partition, the
last offset whose record the table holds. The runtime's checkpoint, stored with the rows
([11 §11.4](./11-iceberg-tables-and-storage.md)).

**Committer** — The runtime member that owns `<rt>-commits`'s single partition: it commits files and
schema changes to the runtime's tables. Kaspr moves it when its member goes away
([10 §10.6](./10-runtime-internals.md)).

**Compaction** — Rewriting a partition's small files into larger ones: a child process of the
maintenance app writes the outputs, and the app commits one `replace` snapshot, rows and offsets
unchanged ([11 §11.6](./11-iceberg-tables-and-storage.md)).

**Complete through** — `materializer.valid-through`: the instant up to which every partition's
records are in the table. The console's Data tab shows it.

**Control plane** — The single shared `KasprApp` that owns orchestration: intake, discovery, table
creation, apply, observation, deletion, settings, identity, the REST catalog. One per environment.

**Covered / overlap / deferred** — The committer's decisions on a seal against the table's committed
offsets: already in (covered: refused, its files kept for orphan cleanup), partly in (overlap: its
member seals the rest again), or after a gap (deferred: waits for the seal before it)
([10 §10.6](./10-runtime-internals.md)).

**Dataset** — A named group of tables, and their Iceberg namespace: SQL reads `<dataset>.<table>`
(`lake.<dataset>.<table>` on the REST catalog). It holds tables of any number of materializations,
and access to its tables is granted on it. Its name is fixed, lowercase and unique; it is keyed by
an id, `ds-<12 hex>` ([18 §18.16](./18-identity-and-access.md)).

**Dataset grant** — The dataset roles a person or a group holds on one dataset: what they may do
with its tables. Its owners make them, on the dataset's Access tab
([18 §18.16](./18-identity-and-access.md)).

**Dataset role** — A role holding only dataset permissions, so it can be granted on one dataset:
Dataset Reader (schemas, rows, exports, the catalog), Dataset Writer (and adding and dropping
tables), Dataset Owner (and managing the dataset and its access) ([18 §18.3](./18-identity-and-access.md)).

**Dedupe key** — A string on scheduled work used to collapse duplicate scheduler deliveries, e.g.
`observe:<id>:<attempt>`, `drop_table:<id>`.

**Deployment value** — What a setting is set to by the deployment's environment variable, as opposed
to an override or the built-in default. A reset returns a setting to it ([17](./17-settings.md)).

**Desired phase** — What the operator asked for (`running`, `stopped`, `deleting`, `deleted`;
`paused` on records from before 0.4.0). Contrast with observed phase.

**Directory** — The people, groups, roles, access tokens, datasets, their grants and the claims of
table names in the `directory` table (`identity` before 3.0). Its `revision` rises with every change,
and what each person may do is cached against it.

**Discovery** — Sampling up to 200 records of each source topic, all at once, and evolving each
table's schema 0 from its topic's by the same policy the committer applies later
([09 §9.6](./09-schema-evolution-and-forks.md)).

**Drain** — What a change of topics does to its runtime: held at zero replicas until its consumer
group is empty, so the broker forgets its static members, then the added topics' offsets are set
and it starts again — about five minutes, `changing_topics` meanwhile
([07 §7.9a](./07-control-plane-workflows.md)).

**Drift detection** — The writer's per-record walk against the cached schema index, finding values
the schema cannot take; it sends shapes, never values, as a `schema_change_request` and never waits
([09 §9.7](./09-schema-evolution-and-forks.md)).

**Field id** — Iceberg's identity for a column, assigned once and never reused: 1–9 are the Kafka
metadata columns, payload fields start at 10. Readers match columns by id, not name.

**Fork** — A sibling field created when a field's values change type incompatibly: `total` (double)
then a string gives `total__v2`. Both are kept; the fork map ties them to one logical name
([09 §9.5](./09-schema-evolution-and-forks.md)).

**Fork map** — The table property `materializer.fork-map`: logical paths → their physical fields,
with each member's state (`current`, `superseded`, `collapsed`).

**Forwarding (live tail)** — A tail app member asked for a table another member owns passes the
request to the owner, once (`X-Tail-Forwarded-By`), and returns its answer unchanged ([19 §19.5](./19-live-tail.md)).

**Group** — A set of people and the roles they hold through it. Built in: Guest (everyone), All
Users (everyone signed in), Viewer, Editor, Admin; administrators add their own.

**Guest** — A caller without a session. Holds what the Guest group grants: by default the
materialization list and status.

**Head peek** — The newest records on a table's topic, committed or not, built as rows: what the
Data tab shows while the table is backlogged. Never part of a run ([19 §19.9](./19-live-tail.md)).

**Hot partition** — The partition live traffic still writes — today, or this hour — which compaction
leaves alone until no commit has added to it for `compaction_hot_partition_delay_minutes` (60). In
this month's partition each file waits that long instead.

**Iceberg REST catalog** — The control plane's `/catalog/v1/…` endpoints, so engines attach the lake
as a catalog with an access token ([11 §11.8](./11-iceberg-tables-and-storage.md)).

**Incarnation** — One table of a name: a table dropped and created again under the same name is a
new incarnation, with another `table_uuid`. The runtime stamps what it keeps of a table with its
uuid and ignores another incarnation's ([10 §10.8](./10-runtime-internals.md)).

**Kept table** — A table whose topic was removed and that was kept: state `retired`, read-only,
still in its dataset and the catalog, its name still claimed, still maintained. Adding its topic back writes it again
from where it stopped ([11 §11.7](./11-iceberg-tables-and-storage.md)).

**Label** — A materialization's name: free text, not unique, of no meaning in SQL or storage, and
its id when none was given; it can be changed at any time ([04 §4.5.1](./04-http-api-reference.md)).

**Lineage (of runtime settings)** — The history a runtime-settings revision belongs to:
`<UTC time>-<random>`, begun when the control plane first starts on settings without one. Runtimes
keep the newest settings by lineage, then revision, so a control plane installed afresh is
followed whatever revisions an earlier one reached ([17 §17.7](./17-settings.md)).

**Live settings** — The 33 runtime settings — batches, commits, schema caps, maintenance, protocol
logging — that change while runtimes run, delivered on the runtime control topic and applied within
seconds, without a restart; the maintenance ones reach the maintenance app with its ticks
([17 §17.7](./17-settings.md)).

**Live mode** — The Data tab's **Live** toggle: while the tab is seen, on its first page and with no
row open, it asks for the live tail's newest records every 1.5 s and appends them to the run, and a
new run replaces it when a commit lands (web 11).

**Live tail** — Every materialization's tables' records past their committed snapshot, served by
the **tail app** from Kafka within seconds, which a console run reads beside the snapshot so that
each record shows exactly once; the deployment's `live_tail_enabled` turns it off for all of them
([19](./19-live-tail.md)).

**Maintenance app** — The `<release>-maintenance` KasprApp, one per environment: compaction,
snapshot expiry and orphan cleanup for every materialization's tables, stopped ones and kept ones
included, sized for compaction so the runtimes need not be
([10 §10.10](./10-runtime-internals.md)).

**Maintenance tick** — The control plane's message to the maintenance app, each minute, for each
maintained table: where it is, the maintenance settings, when each job last ran
([07 §7.13](./07-control-plane-workflows.md)).

**Locked (dataset)** — A dataset the person cannot read: they see its name, description, owners and
tables' names, never its schemas or rows; the REST catalog does not list it for them
([18 §18.16](./18-identity-and-access.md)).

**Materialization** — One or more source topics, each into an Iceberg table of its own in a dataset,
read by one runtime; the top-level entity, `mat-<12 hex>`. Its name is a label. Its topics can be
changed by an edit ([04 §4.5](./04-http-api-reference.md), §4.8.1).

**Table name** — What SQL reads a topic's rows by in its dataset, `<dataset>.<table>`: chosen when
the topic is picked, and derived from the topic unless renamed — `orders-v2` → `orders_v2`. Unique
in its dataset and in its materialization; fixed once the table exists
([11 §11.1](./11-iceberg-tables-and-storage.md)).

**Table-name claim** — The directory's record that `<dataset>.<table>` belongs to one
materialization's table, written before the table is created and removed when the table leaves the
catalog; what keeps a name unique across the deployment ([18 §18.16](./18-identity-and-access.md)).

**Member id** — Which runtime replica a record, batch or file belongs to: the pod name
(`RUNTIME_MEMBER_ID`). It appears in batch ids and data file names, and the heartbeat's
`table.member_id` names the committer.

**Metadata columns** — `_kafka_timestamp`, `_kafka_partition`, `_kafka_offset`, `_kafka_key`,
`_kafka_headers`, `_materialized_at`, `_unmapped` (field ids 1–7, the headers map's key and value 8–9).

**Observation** — A control-plane read of a runtime's real state from two sources, the KasprApp's
status and the members' heartbeats, merged into one observed phase ([08](./08-observation-and-status.md)).

**Observed phase** — What is true now (`requested`, `discovering`, `discovered`, `deploying`,
`running`, `degraded`, `stopped`, `paused`, `deleting`, `deleted`, `failed`). Derived, never set
directly by an operator.

**Operation record** — An entry at `materialization-op:<id>:<operation_id>` recording that a workflow
step ran, with status `applied` / `skipped` / `failed` / `in_progress`. Also the idempotency guard.

**Ordered batch** — A batch whose every record is acknowledged in offset order once in the table,
rolled at 5,000 rows: a new materialization's first batch per partition, and one after a compaction
gap. A twm-faust workaround ([14 §14.3](./14-known-gaps-and-roadmap.md)).

**Orphan cleanup** — The maintenance job that deletes files under the table's location which no
retained snapshot references, older than `orphan_min_age_hours` (24).

**Owner (live tail)** — The tail app member a table belongs to: the one assigned the partition of
`<release>-tail-requests` its key `tail:<materialization>:<table>` hashes to ([19 §19.5](./19-live-tail.md)).

**Override** — A setting value chosen on the Settings page, stored in the `settings` table; it wins
over the deployment value until it is reset.

**Parked (live tail)** — A tailed table nobody has asked for in `live_tail_idle_seconds`: it holds
nothing and reads nothing until a request activates it ([19 §19.6](./19-live-tail.md)).

**Partition (table)** — A day (or hour, or month) of `_kafka_timestamp`, in UTC: `ts_day=2026-10-18`,
`ts_month=2026-10`. Untimed rows go to `__null__`. Not to be confused with a Kafka partition.

**Permission** — One thing the application lets someone do, such as `materializations.create`; the
catalog is `identity/permissions.py`. A permission may imply others, within its scope: the deployment,
or — for what concerns a dataset's tables, such as `data.query` — one dataset at a time.

**Phase reason** — A machine-readable sub-state within a phase, e.g. `waiting_for_table`,
`commits_stalled`, `table_missing`. A `phase_reason` that is literally a phase name means no real
status exists yet.

**Principal** — Who made a request: a signed-in `user`, a `guest`, the console's `service`
credential, or an access `token`. Every WebView resolves it first, with `authorize(...)`.

**Projection (schema)** — The control plane's read-only copy of one of the table's schemas, one entry
per field with its logical name, at `materialization-schema:<id>` ([05 §5.5](./05-data-model-reference.md)).

**Restarting members** — `runtime_members_restarting`: a running materialization whose runtime has
a member crash-looping, killed for memory in the last 30 minutes, or restarted in them after three
restarts. Degraded, and shown as **Unhealthy**, until those restarts age out ([08 §8.3, row
3d](./08-observation-and-status.md)).

**Revision (settings)** — A settings scope's count of applied changes, carried by every change as
`base_revision` so a change made against stale settings is refused.

**Role** — A reusable set of permissions that groups hold, or that is granted on a dataset. Eight
are built in (Administrator, User Manager, Operator, Viewer, Status Viewer, and the dataset roles
Dataset Reader, Writer and Owner).

**Rollout** — How far the current runtime-settings revision has reached the running runtimes:
`current`, `pending`, `stopped`, `not_reporting`, `unsupported`.

**Routed output** — The envelope shape control-plane functions return instead of performing I/O:
`{"route": "work" | "scheduler" | "topic", "key", "value", "headers"}`. What makes the control plane
unit-testable without a broker.

**Runtime** — A per-materialization `KasprApp` with its state table, three agents and three tasks,
that reads every source topic into its table. Named `<prefix>-<materialization_id>`. Its seals,
uploads, backlog and commits are bounded for all its tables together (10).

**Runtime control topic** — `materialization-schema-authority-updates`, kept from 0.x: runtime
settings, consumed by every runtime's settings agent ([06 §6.7](./06-event-and-topic-contracts.md)).

**Scope (settings)** — `control-plane`, `runtime` or `console`: one document each in the `settings`
table, with its own revision and history.

**Seal** — Turning a rolled batch's JSONL into Parquet files with field ids, one per table partition
its rows touch, in a child process. A seal is committed whole or not at all. It spools the lines to
a file per partition first, then writes one partition's file at a time, so it holds one row group
however far its batch spreads ([10 §10.4](./10-runtime-internals.md)).

**Service token** — The console's read-only credential for its shared live views
(`AUTH_SERVICE_TOKEN` = `CONSOLE_SERVICE_TOKEN`).

**Session** — One sign-in: a signed token valid at once, expiring after the idle timeout (8 h) unless
renewed and after the maximum length (7 days) in any case.

**Session (runtime)** — A member's reading of a table partition between two rebalances. It is named
by its generation, the app's rebalance count, and its process's epoch, a random id made at the
process's start. A restarted member keeps its name but has a new epoch, and its generations start
again ([10 §10.5](./10-runtime-internals.md)).

**Settling (live tail)** — A tail row whose new fields wait in `_unmapped` for the schema change the
runtime will commit; once it is in, the tail builds the row again with those fields in columns, as
the seal does ([19 §19.8](./19-live-tail.md)).

**Snapshot** — One version of the table: every commit, compaction and expiry makes one. Its summary
carries the committed offsets; the console's runs API can read an older one (`asOf`), though the
Data tab always reads the latest.

**Spec hash** — `sha256` over the rendered runtime resources; stored on the binding. The sweep
re-applies whenever the freshly rendered hash differs.

**Static membership** — Kafka's KIP-345 group membership, which Kaspr uses for every app. A restarted
pod rejoins without a rebalance, which is why a release that adds topics needs one forced rebalance,
and why a change of a runtime's topics drains it.

**Stopped** — A materialization whose runtime is scaled to zero replicas. Its KasprApp, topics,
consumer group, volumes and tables stay, and the maintenance app goes on maintaining the tables;
resume scales it back to its `replicas`.

**Sweep** — The `*/2 * * * *` cron task (`onLeader: true`) that reconciles drift and re-observes
anything stale or stuck.

**Table record** — `MaterializationTable` at `materialization-table:<id>:<table>`: state, location,
newest snapshot, totals, offsets, maintenance history and running job, last error
([05 §5.12](./05-data-model-reference.md)).

**Table state** — `active` (written), `retired` (kept read-only), `dropping`, or `dropped` (its
files kept, with `destination_cleanup_enabled` off; its name stays taken)
([11 §11.7](./11-iceberg-tables-and-storage.md)).

**Tail app** — `<release>-tail`, a KasprApp of its own (`tail_app/`): reads the topics of the
tables people watch and serves their records past a snapshot's offsets as Parquet, to the console
alone. It keeps no state and never writes a table ([19](./19-live-tail.md)).

**Tail tick / tail cancel** — The control plane's message, each minute, for every table in the
catalog: a tick for a table that is tailed (its target, the live-tail settings, hints), a cancel with
the reason for one that is not ([19 §19.4](./19-live-tail.md)).

**`through` (live tail)** — Per Kafka partition, the last offset a tail answer covers. A run asks next
past it, so the snapshot and its tail files follow on from each other without a gap or a repeat
([19 §19.2](./19-live-tail.md)).

**`_unmapped`** — The column that keeps, as JSON, values the schema could not take: past its field
cap, a payload that is not an object, or values of a collapsed family's array element
([09 §9.8](./09-schema-evolution-and-forks.md)).

**Work event** — The command message on `materialization-work` that drives one workflow step.

**Work id** — A work message's identity, also its operation id, used for idempotency.

## 15.2 FAQ — architecture

**Why two planes instead of one app doing everything?**
Blast radius and resource isolation. A malformed topic or a memory-hungry payload takes down only
its own runtime. The control plane stays up and can observe, report and repair.

**Why does every materialization get its own KasprApp?**
Isolation, and one committer for its tables without coordination. Shared mode is not implemented
([14 §14.1](./14-known-gaps-and-roadmap.md)); the cost is eight Kubernetes resources and at least one
pod per materialization — which is why a materialization can read several topics: topics that
belong together share one runtime, its members and its committer.

**Should related topics be one materialization or several?**
One, when they are read together: a materialization of `orders` and `payments` costs one runtime
instead of two, and the console's Data tab joins its tables in one statement. Where its tables go is
another matter: each topic's table goes in the dataset that fits it, whichever materialization
writes it, and an engine on the REST catalog joins tables of any datasets it may read. Its runtime never does more at once than one of a single topic
— seals, uploads, the backlog and commits are bounded for all its tables together
([10](./10-runtime-internals.md)) — so more topics only mean more work, spread over its members. Keep
topics apart when they should scale, stop or be deleted independently: those are a
materialization's, not a table's. Topics can be added and removed later, each change stopping the
runtime for about five minutes. A topic can be read by any number of materializations, each into a
table of its own.

**Who compacts the tables — every runtime member, or one?**
Neither: the maintenance app, a KasprApp of its own that compacts, expires and cleans up every
materialization's tables. Before 2.0 the committer member of each runtime did, so every runtime pod
had to be sized for compaction's memory; now only the maintenance app is
([10 §10.10](./10-runtime-internals.md), [12 §12.10](./12-operations-runbook.md)).

**Why a catalog pointer in the bucket instead of a catalog server?**
Nothing to run, back up or keep available: a GCS object with a generation precondition is a
compare-and-swap, which is all a single-writer Iceberg catalog needs. The control plane serves the
REST protocol over it for engines that want a catalog (decision D1).

**Why does only one member commit?**
Commits to one table must be ordered, and Iceberg's optimistic concurrency would make sixteen
writers fight over the pointer. The committer is whoever owns `<rt>-commits`'s one partition — Kaspr
already elects that — and every member hands it files (decision D7).

**Why do users, groups and roles live in the control plane?**
Every WebView authorizes its caller against them, and Kaspr binds only an app's own tables to its
operations. Identity is a self-contained module with its own tables, topic, agent and views — the seam
to cut along if it ever becomes a service ([18 §18.13](./18-identity-and-access.md)).

**Every administrator is gone. How do I get back in?**
Put `bootstrap-admin-email` and `bootstrap-admin-password` in the auth Secret and restart the control
plane ([18 §18.8](./18-identity-and-access.md)).

**Why is there no local server?**
The control plane only exists as a KasprApp; its HTTP surface is WebViews. Validate with unit tests
and `helm template`; test end to end against a cluster.

**What guarantees exactly once into the table?**
The committed offsets live in the same snapshot as the rows, and the commit is a compare-and-swap:
the committer refuses a seal the table already covers, never jumps a gap, and members acknowledge
records only once the table covers them ([10 §10.6, §10.7](./10-runtime-internals.md)). The E13 load
test checks it ([13 §13.8](./13-testing-and-development.md)).

## 15.3 FAQ — schema

**Why did discovery not find all my fields?**
It samples up to 200 records; fields none of them had are added when they first arrive, a commit or
two later ([09 §9.7](./09-schema-evolution-and-forks.md)).

**Why is there a `__v2` column?**
A field's values changed type. Old files keep the old column, new values go to the fork; the
console shows the family as one column, and other engines coalesce (09 §9.10).

**Why did my field become `__json`?**
Its family passed `max_forks_per_field` (3), or its object passed `max_struct_fields`, or it nests
deeper than `max_nesting_depth`. From then on its values are kept as JSON text.

**Why are values in `_unmapped`?**
The schema is at `max_schema_fields`, the payload is not a JSON object, or an element of a collapsed
array could not hold the value ([09 §9.8](./09-schema-evolution-and-forks.md)).

**Can a field be removed?**
No. Iceberg could, but nothing removes one: a field that stops arriving stays, null in new rows.

**Does a schema change restart the runtime?**
No. The committer commits it to the table, and members read it from the state table before their
next seal.

## 15.4 FAQ — runtime and data

**Are there duplicate rows?**
No: delivery into the table is exactly once. Re-read records are refused by the committer as
covered. A `SELECT _kafka_partition, _kafka_offset, count(*) … HAVING count(*) > 1` returns nothing.

**Can a stop, crash or rebalance lose records?**
No: an offset commits only once its row is in the table, so whatever was staged is read again, and
the committer refuses what the table already holds. The first E13 run found the one way it could —
the committer deleted files it had itself committed when they were announced again — fixed before
release ([10 §10.6](./10-runtime-internals.md), [13 §13.8](./13-testing-and-development.md)).

**Why does consumer lag not reach zero on a caught-up materialization?**
It counts records not yet committed to the table: up to a batch per partition plus a commit interval.

**How long until a record is in the table?**
The batch's soft age (120 s) once it holds enough rows, then the seal and upload, then the next commit
(30 s). On a quiet topic, the 600 s ceiling. **Complete through** says up to when everything is in.

**Then how does the console show a record a second after it was produced?**
From the live tail: a run reads the table's snapshot and, beside it, the records past the snapshot's
offsets, which the tail app reads from Kafka. Each record is in one or the other, never both, and
its row is built by the seal's own code, so it is the row the commit will hold. On the lab the Data
tab's Live mode showed new records at a p95 of 1.6 s ([19](./19-live-tail.md)).

**Does an engine on the REST catalog see the live tail?**
No: an engine reads committed snapshots only. The tail is the console's, through the endpoint only it
may call; the console's count less an engine's is the tail's rows at that moment.

**Can I add a topic to a materialization, or remove one?**
Yes, with **Edit** or `PATCH /materializations/{id}` and `source_topics`. An added topic gets a new
table from its earliest records — or, if it is a kept table's topic, writes that table again from
where it stopped. A removed topic's table is kept read-only or dropped, as you choose each time.
The runtime stops until its consumer group empties, about five minutes
([07 §7.9a](./07-control-plane-workflows.md)).

**Why are my rows in old days' partitions?**
Partitions follow `_kafka_timestamp`, not the time a row was written; `_materialized_at` is that.

**Why are there so many small files?**
Each seal writes a file per partition its rows touch, and a backfill of old data touches many.
Compaction joins them: whole small partitions a gigabyte at a time, past days first
([11 §11.6](./11-iceberg-tables-and-storage.md)).

**Why did my runtime pause a partition?**
The upload backlog reached `max_pending_upload_batches` or `max_pending_upload_bytes`: seals or
uploads fall behind. It resumes once the backlog is down to three quarters.

**Why does my runtime have more members than its replicas?**
Autoscaling: it was far behind its topics, and would have taken longer than the catch-up target
(30 minutes) to catch up at its pace. It goes back to its own count once it has caught up. The
detail's `autoscale.reason` — the Overview's *Scaled up to N members* — says why; turn autoscaling
off for the materialization to stop it ([07 §7.12a](./07-control-plane-workflows.md)).

**Why is my runtime Unhealthy when its members report healthy?**
Members keep restarting — killed for memory, most often — and a member that is up reports healthy
between the kills. The message says how many and why; it clears 30 minutes after the last restart
([08 §8.3, row 3d](./08-observation-and-status.md), [12 §12.3](./12-operations-runbook.md)).

**What happens to a tombstone?**
A row with a null payload, unless `skip_tombstones` was set at creation. Nothing is deleted
([14 §14.1](./14-known-gaps-and-roadmap.md)).

**Can I read the table without the console?**
Yes: `iceberg_scan('<location>')` in DuckDB, or attach the REST catalog with an access token
([11 §11.9](./11-iceberg-tables-and-storage.md)).

## 15.5 FAQ — operations

**A materialization is stuck. What is the single most useful command?**

```bash
curl -s localhost:6065/materializations/$MAT -H "authorization: Bearer $TOKEN" | python3 -m json.tool
```

`observed_phase` and `phase_reason` name what fired; look them up in [12 §12.3](./12-operations-runbook.md).

**Nothing self-heals a `failed` status?**
Correct. `POST …/resume` is the recovery; `table_missing` and `invalid_destination` need a delete
and a new create.

**Why does it say `commits_stalled` while commits flow?**
The reason lingers after disruptions until the next resource observation, up to ten minutes
([08 §8.6](./08-observation-and-status.md)). The table record's `last_commit_at` is the live answer.
If `last_commit_at` has stopped too, commits really did stop: see `degraded` / `commits_stalled` in
[12 §12.3](./12-operations-runbook.md).

**I edited a runtime resource with `kubectl edit` and it reverted.**
The sweep saw a spec-hash mismatch and re-applied within two minutes.

**Does delete remove my data?**
Yes, by default, and it cannot be undone: it drops the table. With `destination_cleanup_enabled` off
it only retires the pointer, and the table stays readable by path ([07 §7.10](./07-control-plane-workflows.md)).

**The materialization reads `deleted` but is still listed.**
Its table is still being dropped, a pass at a time; follow `control_plane_drop_table_progress`.

**Why does upgrading the control plane restart every runtime?**
Runtime specs pin the package to a git commit; a new commit changes every spec hash, and the sweep
re-applies every runtime. Intentional: runtimes and control plane must not skew.

**I changed an environment variable and nothing happened.**
An override for the same setting wins until it is reset. The Settings page shows each setting's
source.

**Do settings changes restart anything?**
No. Settings never reach the rendered runtime spec.

**How do I see what the system is doing?**
Turn on protocol logging (Settings → Diagnostics, Runtimes) and grep for the events in
[03 §3.7](./03-configuration-reference.md).

**Is there a metrics endpoint?**
No ([14 §14.7](./14-known-gaps-and-roadmap.md)).

## 15.6 FAQ — development

**What do I run before calling a change done?**

```bash
python -m unittest discover -s python/tests -p 'test_*.py'
python -m compileall -q python/src
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
cd console && npm run typecheck && npm test && npm run build
```

**Which linter should I use?**
None is configured. Do not add one as a side effect of an unrelated change.

**Where do I put a new test?**
In the module that already owns the behavior ([13 §13.4](./13-testing-and-development.md)). Tests of
the table go through `lakehouse_support.LakeFixture`, a fake bucket holding a real Iceberg table;
runtime tests through `runtime_support.RuntimeHarness`.

**How do I build config in a test?**
`ControlPlaneConfig(...)` or `RuntimeAppConfig(...)` directly; `get_config()` is cached.

**Why can I not render a runtime spec locally?**
Package pinning raises `RuntimeError` on editable installs, which no pod could repeat ([13 §13.5](./13-testing-and-development.md)).

**I renamed a topic and things broke.**
Topic names live in `config.py` and the chart's templates, derived from the release; see
[13 §13.6](./13-testing-and-development.md).

## 15.7 Where to look for what

| Question | Document |
|----------|----------|
| What is this thing? | [01](./01-system-overview.md) |
| What gets deployed? | [02](./02-deployment-and-manifests.md) |
| What does this env var do? | [03](./03-configuration-reference.md) |
| What does this endpoint return? | [04](./04-http-api-reference.md) |
| What fields does this model have? | [05](./05-data-model-reference.md) |
| What is on this topic? | [06](./06-event-and-topic-contracts.md) |
| Why did this step skip? | [07](./07-control-plane-workflows.md) |
| Why is it in this phase? | [08](./08-observation-and-status.md) |
| Why did my column change? | [09](./09-schema-evolution-and-forks.md) |
| How does a record reach the table? | [10](./10-runtime-internals.md) |
| What is in the table, and how is it kept? | [11](./11-iceberg-tables-and-storage.md) |
| It is broken, now what? | [12](./12-operations-runbook.md) |
| How do I change it safely? | [13](./13-testing-and-development.md) |
| Why is it like this? | [14](./14-known-gaps-and-roadmap.md) |
| Which version is running? | [16](./16-versioning.md) |
| Can I change this setting without a redeploy? | [17](./17-settings.md) |
| Who may do what? | [18](./18-identity-and-access.md) |
| What does this word mean? | this document |

**Why are datasets not enforced in the bucket?**
Grants are enforced by the control plane, the REST catalog and the console (decision). The data files
are read by engines themselves — DuckDB with an HMAC key, PyIceberg with Google credentials — and such
credentials usually reach the whole bucket, whatever anyone's roles. Restricting a dataset restricts
people who read through the catalog or the console without bucket credentials; table-scoped
credentials vended by the catalog would close the gap ([14 §14.8](./14-known-gaps-and-roadmap.md)).

**Why can two materializations not both have `sales.orders`?**
`<dataset>.<table>` is what every engine reads; two tables of one name would be one table to each of
them. The directory claims a name before its table is created, so of two requests racing for one name
exactly one wins, and the other is refused with `table_name_taken` before anything is created
([18 §18.16](./18-identity-and-access.md)).
