# Few-second freshness: the live tail

**Status:** implemented in 4.1.0 on `feature/live-tail` and validated on the lab, 2026-10-24. What
was built is described in handbook 19, which supersedes this plan where they differ (its §19.18 lists
where and why); the lab results are in handbook 13 §13.8e and web 14 §14.4. The plan is kept as the
record of the reasoning.

Written 2026-10-23 against `develop` at 5b45cda (control
plane and maintenance app 4.0.0, console 4.0.0) and revised the same day after the owner's review:
live tail is **on by default** for new materializations; the tail runs in **a KasprApp of its own**,
`materializer-tail`, with request routing between its members designed in from the start so that it
scales to hundreds of materialized topics (§3.5, §5.1, §11). Every claim about current behaviour
below was checked against the code named beside it; where the handbook and the code disagree, the
code is cited.

**Audience:** the implementer. This document is meant to be built from without coming back to its
author. The owner's decisions are recorded in §17 with what little remains open.

---

## 0. Summary

A record produced to a materialized topic is queryable today only once its batch has rolled
(up to 120 s, 600 s on a quiet topic), been sealed, uploaded, announced and committed (a commit
every 30 s, 2 s + 18 ms per file), and the console has re-read the table's pointer (≤ 2 s).
Minutes, by construction.

The design **serves the newest records from Kafka itself**, as a bounded, exactly-once *tail*
stitched onto the committed snapshot:

- Every snapshot's summary says, per Kafka partition, the last offset it holds (`materializer.offsets`).
  The tail of a snapshot *S* is, for each partition *p*, the records Kafka holds with offset
  `> O_S(p)`. The snapshot and its tail are disjoint by offset and together cover every record, so
  the union is exactly once with no new protocol.
- A new KasprApp, **`materializer-tail`**, follows each tailed topic from the table's committed
  offsets, converts each record **with the seal's own row-building code** (the same `Conformer`, the
  same Arrow schema with field ids, the same Parquet writer) and keeps a bounded, contiguous,
  in-memory buffer of conformed rows per table. It answers one HTTP question: *the rows past these
  offsets, as a Parquet file in the table's exact physical format.* One app for the whole deployment,
  like the maintenance app — not one per materialization: each table belongs to one of its members,
  the member Kaspr gives its ticks to; any member forwards a request to the owner; the app scales by
  adding members.
- The console treats the answer as one more data object of the run: newest first in the Data tab,
  one more file behind `sales.orders` in a statement. A run stays pinned to one snapshot *and* the
  tail fetched for it, so paging is stable; a **Live** mode fetches what is new every 1.5 s.
- `live_tail` is a per-materialization boolean, **on by default** for new materializations
  (`live_tail_default`), switchable per materialization and off for the whole deployment with
  `live_tail_enabled`. The tail sits behind the console's existing per-dataset `data.query` check and
  is checked again by the tail app against the person's own session; the endpoint is the console's
  alone. The table format, the committer and the exactly-once protocol do not change.
- While a materialization works through a large backlog — a new materialization on a topic with
  hours of history — the tail does not apply and does not read: the table is `backlogged` until the
  runtime is within the tail's bound of the topic's head, the console says so and shows how far
  behind, and the tail goes live by itself as the runtime catches up (§5.1 "Backlogs and bursts").
  Meanwhile a **head peek** shows the newest records on the topic as they will look in the table,
  apart from the table and never in a run.
- A table is tailed only while someone is watching it: the first request activates it (a second or
  two to read its window, which the request waits for), ten minutes without one parks it again. So
  the app's reads, memory and CPU follow the open Data tabs, not the 500 opted-in topics. It persists
  nothing: every member's state is rebuilt from the ticks, the control plane, the table events and
  Kafka.

Produce → visible lands at about 1.1 s typical and 2.7 s at p95 in the Data tab (§9), against a
5 s target. Faster commits, the runtime serving its uncommitted rows, a separate hot table, a
Node/TypeScript tail inside the console, and hosting the tail inside the control plane were each
examined (§3).

---

## 1. Requirements, as acceptance criteria

| # | Requirement | How this plan meets it | Verified by |
|---|---|---|---|
| R1 | A record produced to a steady topic shows in the console's Data tab and in SQL runs within 5 s at p95 | The tail is read from Kafka, not from files; budget in §9 sums to 2.7 s p95 | §13.4 lab probe, 300 samples |
| R2 | No row duplicated or missing while records move into the table, across commits, compaction and runtime restarts | Snapshot and tail are disjoint by offset and contiguous per partition; the tail is pinned to the run's snapshot; compaction and restarts never move offsets backwards | §7; §13.4 disruption matrix |
| R3 | A freshly read row looks exactly as it will once committed, forks, JSON collapses and `_unmapped` included | One row-building function shared by the seal and the tail; the tail conforms with the table's current schema, as the seal does; one documented deviation, `_materialized_at` (§8) | `test_tail_parity.py`; lab diff of tail rows against committed rows |
| R4 | Opt-in per materialization, behind the console's existing data access and per-dataset permissions, bounded reads | `live_tail` on the materialization (default on, switchable); `data.query` on the table's dataset checked by the console route and again by the tail app with the person's session; per-table row/byte bounds, a per-member budget and a per-request cap | `test_web.py`, `test_tail_app.py`, `web/server/runs/tail.test.ts` |
| R5 | No change to the table format or the exactly-once protocol unless argued and flagged | None proposed. The one refactor (§5.3) moves code, not behaviour; the seal's output is byte-identical | `test_runtime_seal.py`, fixture regeneration unchanged |

Out of scope, stated as limits (§11): external engines on the REST catalog and `iceberg_scan` see
committed snapshots only; **As of** runs, kept (retired) tables and stopped materializations have
no tail.

---

## 2. Why the committed path cannot reach seconds

Measured and coded facts the design rests on:

| Fact | Where |
|---|---|
| An open batch rolls at 120 s once it holds 10,000 rows or 8 MiB, at 600 s regardless; the pipeline task runs every 5 s | `runtime_app/config.py` (`max_open_file_age_seconds`, `min_rows_per_file`, `max_open_file_age_ceiling_seconds`, `upload_interval`), `writer/batches.py::should_roll_batch` |
| A seal runs in a child process, an upload after it; files are announced on `<rt>-commits` and committed `commit_interval_seconds` (30 s) after the last commit | `runtime_app/pipeline.py`, `committer.py::_append_due` |
| A commit costs 2,003 ms + 18.5 ms per file; p50 3.3 s, p95 10 s on the load test | handbook 13 §13.8 |
| Commits to one table are spaced ≥ 1 s because GCS throttles a single object past about one update a second; every commit rewrites the pointer and the metadata file | `committer.py::COMMIT_SPACING_SECONDS`, handbook 11 §11.2 (E11) |
| The console learns of new data only by re-reading the pointer: no older than 2 s for a run, 5 s for the summary | `web/server/config.ts` (`snapshotCacheMs`, `tableMetadataFreshMs`) |
| A seal writes one file per partition value it touches; compaction leaves the partition being written alone until it has been quiet for 60 min (month partitions: each file waits that long) | `lakehouse/maintenance.py::plan_compaction`, handbook 11 §11.6 |

Shrinking the batch age and the commit interval to their minima (10 s and 5 s) still leaves
roll + seal + upload + announce + commit + pointer re-read at 10–20 s typical, and each commit of a
32-partition topic adds 32 files to the hot partition: 10 s commits are 276,000 files a day that
compaction may not touch for an hour. Tuning cannot reach 5 s and would damage the table.

---

## 3. Alternatives considered

### 3.1 Faster commits — rejected

Reduce every interval, cache the manifest list between commits, read footers ahead of the commit.
The floor is structural: the GCS pointer allows about one commit a second, a commit costs ≥ 2 s,
and files per commit scale with Kafka partitions. Even an ideal 3 s commit cadence puts
produce → visible at 6–8 s after the pointer re-read, and multiplies small files in the partition
compaction deliberately avoids (`compaction_hot_partition_delay_minutes`). Metadata growth
(`metadata.json` is read whole by every commit and load, 11 §11.4) would also worsen. The
committer improvements in 14 §14.4 are worth doing on their own, but they do not reach the target.

### 3.2 The runtime serving rows it has not committed yet — rejected

Two variants:

- **Members serve their staged rows.** Each member holds raw JSONL per table partition in the
  container's filesystem (10 §10.3). Serving them needs an HTTP endpoint per runtime (a Service per
  materialization), a fan-out across all members per query, conforming raw rows on the member's
  one-core hot path (14 §14.4: the writer deliberately does no per-record conversion), and
  reasoning about abandoned sessions and rebalances (10 §10.5) in the reader. A member restart drops
  its staging directory, so a row visible a moment ago vanishes until the partition's new owner
  re-reads it — a "missing" row from the reader's side.
- **Readers open uploaded-but-uncommitted files.** Sealed Parquet in `data/` is already in the
  table's format, and the committer's `plan_append` rules are pure and could be applied by a reader
  to pick a contiguous accepted chain. But a file exists only after roll + seal + upload — the same
  ≥ 120 s (or ≥ 10 s with aggressive settings, and the same file explosion as §3.1). Freshness
  through files is small files.

The Kafka tail returns the same records — the uncommitted window *is* the records past the
committed offsets — from a source that is durable, ordered, independently readable, and already
knows its own position.

### 3.3 A separate hot table (or a hot branch of the same table) — rejected

A second Iceberg table committed every few seconds with tiny files and aggressive expiry, unioned
with the cold table by offset. The hot table has its own pointer with the same one-per-second limit
and ≥ 2 s commit cost, so it cannot get under ~4 s either; it doubles the write pipeline, needs its
own compaction/expiry, shows external engines a second table they must union themselves, and still
pays the console's pointer re-read. A branch of the same table shares the same pointer and is
strictly worse.

### 3.4 The console reading Kafka itself (Node, TypeScript conform) — rejected

The BFF already holds the run engine, DuckDB and the snapshot's offsets, and has a TypeScript port of
the schema walk (`web/server/iceberg/schema.ts`). Rejected on fidelity and boundary grounds: R3
needs the runtime's conversion, and a second implementation of `lakehouse/schema/conform.py` diverges
in exactly the places that matter — number formatting in JSON-collapsed fields (`json.dumps` vs
`JSON.stringify`: `1.0`, `1e16`, non-ASCII), `b64:` encoding, candidate ordering across forks. It
would also give the console Kafka credentials and a Kafka client, against the rule that it talks to
the materializer over HTTP only (AGENTS.md). The conversion runs in Python.

### 3.5 Hosting: a KasprApp of its own, and how it scales

Three hostings were weighed: a KasprApp of its own; the control plane's own process; and a child
process the control plane supervises. The owner chose the **KasprApp of its own**. What decides it:

**Isolation.** Conforming records is Python CPU, ~60–120 µs a record, and buffers are tens to
hundreds of MiB per table (§11.1). The control plane runs every agent and every WebView on one event
loop under one GIL (`handlers/maintenance.py` docstring; 12 §12.10) in 512 MiB: a busy table's
ingest there slows heartbeat handling, workflow steps, sign-in and the REST catalog in proportion,
and a buffer overrun or a pyarrow crash is the API's outage. The maintenance app was split out in
2.0 for the same reason (02 §2.10a). A child process inside the control plane's pod gets the CPU
isolation but shares the pod's memory limit, couples the tail's restarts to the control plane's
deploys, and still leaves the control plane the one place every byte of tail data passes through.

**Scale.** A table's tail is sequential work — one ordered consumer, one contiguous buffer — so it is
one process's wherever it runs, and no hosting makes one table's tail faster than one core of Python.
What scales is *tables across members*. Doing that inside the control plane would mean running the
control plane with several replicas, which is a project of its own (`replicas: 1` in every overlay,
`onLeader` steps and in-memory cursors written for one member, global tables recovered per member,
rebalances under static membership never run with several) and would still need what the tail app
needs anyway: a way for a request about table *T* to reach the member that tails *T*. A KasprApp of
its own gets that from Kaspr for free — the ticks about *T* all carry one key, so Kaspr hands them to
one member — and adds the one piece Kaspr does not provide, forwarding a request to that member (§5.1
"Ownership and routing"). Its members are sized and scaled for the tail alone: members ≤ the tick
topic's 64 partitions, each planned at ~5,000 records/s and 2.5 GiB of buffers (§11.1).
Replication — every member able to tail any watched table, no ownership and no forwarding — was
weighed once activation on demand (§5.1) made the watched set small: simpler, and free of the Kaspr
confirmations, but capped at one member's ingest for everything watched and reading each watched
topic once per member. The owner chose routing (§17 D12), so capacity grows with members from the
first release.

**Costs accepted.** A new app, topic, Service, HTTPRoute rule and overlay patches; the tail app
checks sessions through the control plane's `GET /auth/session` rather than a bound `directory`
table, and `AuthorizationWiringTests` learns that exception; the console gets a second upstream URL.

### 3.6 What was kept from the alternatives

Nothing in §3 is wasted: the tail app reuses the committer's offset model, the seal's row builder,
the maintenance app's tick/forget shape, the console's run engine and sandbox, and the console's own
session-check pattern for authorization.

---

## 4. Recommended architecture

```
 producer ──▶ Kafka topic ──┬──▶ runtime members ─▶ seal ─▶ upload ─▶ committer ─▶ Iceberg table (GCS)
                            │        (unchanged)                                      │ pointer + snapshots
                            │                                                         │ materializer.offsets
                            └──▶ materializer-tail (new KasprApp, N members)          │
                                   member = owner of the tables whose ticks Kaspr gives it
                                   • seed: schema from the control plane's schema API + offsets from the tick
                                   • follow: materialization-table-events  ◀── committer / maintenance app
                                       table_committed → trim, schema_changed → re-conform
                                   • ingest: one aiokafka consumer per member, assigned every owned
                                             partition, conform with the seal's row builder, buffer
                                   • serve: GET /tail/v1/…?after={p:o} → Parquet (field ids, zstd) + X-Tail-*
                                             a request for another member's table is forwarded to it
                                   • auth: the person's session → control plane GET /auth/session (cached)
 control plane (changed):                                                              ▲
   • Materialization.live_tail (default on) · POST/PATCH · detail · list              │
   • schedule_table_maintenance also emits tail_tick / tail_cancel ──────────────────┘ (membership, settings, hints)
   • live_tail_* settings (resolved with overrides, carried by the ticks)
 console (changed):
   • a run = pinned snapshot ⊕ its tail file(s) in the run's directory; browse: newest first; SQL: one more file in the views
   • Live mode: POST /api/runs/:id/refresh every 1.5 s → fetch only the rows past what the run has
   • data.query per dataset as today; the tail request carries the person's token
```

Principles:

1. **The table stays the truth; the tail is derived.** The tail is defined by the snapshot's
   offsets and Kafka's contents, never by anything the runtime holds in memory or on disk.
2. **One row builder.** The seal and the tail call the same function on the same staged line.
3. **Contiguity over recency.** A tail is always a prefix of the records past the committed offset.
   When it cannot hold everything, it is *truncated at the newest end* and says so; it never has a
   hole in the middle.
4. **Pinned, like everything else in a run.** A run keeps the tail it fetched; refresh is explicit
   (Live mode makes it continuous) and only ever appends.
5. **Degrade to today.** Whenever the tail is unavailable, the run is what it is today — the
   committed snapshot — with a visible notice, never an error.
6. **One owner per table, any door.** Kaspr decides which member tails a table; every member
   accepts every request and forwards what is not its own.
7. **Tail what is watched.** A table costs nothing until someone asks for it, and nothing again
   ten minutes after the last one did.

---

## 5. Components and data flow

### 5.1 The tail app: `materializer-tail`

A KasprApp of its own, `manifests/base/tail/`, package `python/src/materializer_control_plane/tail_app/`,
built on the maintenance app's shape (`maintenance_app/`, 02 §2.10a): one agent on a request topic
keyed by table, one periodic task, no KasprTable, the same image, Kafka user, GCP credentials and
scheduling as the control plane — plus two WebViews, since Kaspr apps serve HTTP the same way the
control plane does and the operator creates `<app>-app-api` on `webPort` for any app
(`kaspr-operator/kaspr/resources/kasprapp.py`, `kasprapp_resources.py`).

#### One app, many members

There is **one tail app per deployment** — `develop-materializer-tail` on the lab — serving every
materialization's tables in that environment, exactly as the one maintenance app maintains them.
It is not one app per materialization and it is not part of the runtimes: runtimes are one app per
materialization because they *write* and scale by their topic's partitions; the tail only reads and
holds bounded buffers, so one shared app is the right unit. The app's pods are its **members**
(`replicas`, 1–64). Kaspr forms one consumer group over the app's control topic and gives each
member a share of its 64 partitions; a table's ticks all carry one key, so they land in one
partition and one member owns the table (next subsection). With hundreds of tables the key hash
spreads them evenly, each member owning about 1/N. **Scaling is changing `replicas`:** a new member
takes some partitions and the tables in them (each re-seeds in seconds); a removed member's tables
move the same way; nothing is done per materialization, and a new materialization's tables are owned
by whichever member their keys hash to from their first tick. A member that restarts keeps its
partitions (static membership), so its tables are unavailable for the restart, not moved. The
console calls the app's Service and never learns which member answered.

#### What it consumes and produces

| Reads | What | How |
|---|---|---|
| `materializer-tail-requests` (new, 64 partitions, key `tail:<materialization_id>:<table>`) | one `tail_tick` per tailed table per minute — target, settings, hints — and `tail_cancel` (§6.1) | the Kaspr agent `tail-worker`: Kaspr spreads the partitions over the members, which is what assigns tables to members |
| `materialization-table-events` (exists) | `table_committed` (new committed offsets → trim), `schema_changed` (new schema → re-conform) | one plain aiokafka consumer per member, no group, from the latest offset at start |
| the tailed **source topics** | the records | one plain aiokafka consumer per member, manually assigned every partition of every **active** table the member owns — a table someone is watching ("Activation", below); a parked table's topic is not read — seeked to `committed + 1` |
| the control plane's `GET /materializations/{id}/tables/{table}/schema?raw=true` | the table's Iceberg schema, fork map, rejected paths, last column id, at seed | with the service token (`SERVICE_PERMISSIONS` holds `dataset.view_schema`); GCS (`load_table`) only as a fallback |
| the control plane's `GET /auth/session` | who a caller is, for authorization | with the caller's bearer, cached 5 s by token hash |

| Writes | What |
|---|---|
| HTTP answers | Parquet bytes and `X-Tail-*` headers to the console; health and membership views |
| logs | protocol events (§5.1 "Observability") |
| **nothing on Kafka, nothing in GCS** | it never commits, never writes a table, never moves an offset |

#### Persistence: none

The tail app keeps nothing durable. It has no KasprTable and writes no changelog; the persistent
volume Kaspr gives every app holds nothing of its (as the maintenance app's holds nothing, 02
§2.10a). Everything a member holds is in memory and comes from somewhere it can read again: which
tables exist and their settings from the ticks (within a minute), a table's schema from the control
plane (one call at activation), its committed offsets from the ticks' hints and the `table_committed`
events, the records from Kafka, the member map from the consumer group. A restarted member re-seeds
the tables that are asked for and answers `tail_not_ready` for a few seconds meanwhile; nothing is
lost that cannot be read again. The only horizon is Kafka's own retention: a record older than it
cannot be tailed, and is long committed by then.

#### Membership: the ticks

The control plane's per-minute `schedule_table_maintenance` step (already iterating every
materialization and table, `services/table_maintenance.py`) also emits, routed `tail`, on
`materializer-tail-requests`:

- a **`tail_tick`** for every table in state `active` of a materialization with `live_tail: true`
  and `desired_phase == "running"`, carrying the target (`materialization_id`, `dataset`, `dataset_id`,
  `table`, `location`, `source_topic`, `partition_granularity`, `table_uuid`, `skip_tombstones`), the
  live-tail settings as resolved this minute (§6.5), and **hints** from the table record: `offsets`,
  `schema_id`, `metadata_location`;
- a **`tail_cancel`** for a table `dropping`, or of a materialization `deleting`/`deleted`.

A table no tick has named for `MATERIALIZER_TAIL_FORGET_AFTER_SECONDS` (300) is forgotten: its
partitions unassigned, its buffer dropped. That is how opt-out and a stopped materialization end a
tail; the console stops asking the moment the detail says `live_tail: false`.

Volume: one message per tailed table per minute — 300 tables are five a second.

#### Ownership and routing

**The member that receives a table's ticks owns the table.** Every tick about *T* carries the same
key, Kaspr's agent gives one partition to one member, so one member tails *T*, whatever the member
count. A request for *T* may arrive at any member through the Service; that member either serves it
or forwards it to the owner:

1. **Owner partition.** `partition(T) = DefaultPartitioner()(key, partitions, partitions)` over the
   UTF-8 bytes of `tail:<id>:<table>` — the same function the control plane's producer applies when
   it sends the tick: Kaspr's default key serializer is `raw` (`kaspr/types/settings.py`
   `KEY_SERIALIZER`) and Faust's producer uses aiokafka's murmur2 `DefaultPartitioner`
   (`faust/transport/drivers/aiokafka.py:310`). **Self-check:** when a tick arrives, the agent
   compares the computed partition with the event's actual partition (`context["event"].message.partition`,
   as the writer reads it) and logs `tail_routing_mismatch` as an error if they differ, so a
   partitioner disagreement is caught in the first minute on the lab rather than as misrouted requests.
2. **Partition → member URL.** Each member keeps a map refreshed every 15 s, and at once (1 s floor)
   when a request names a table it does not own: `describe_consumer_groups([<this app's group id>])`
   through the admin client (`kafka/admin.py` gains `describe_consumer_group`); for each member,
   decode the protocol assignment for the tick topic's partitions and the Faust `ClientMetadata` in
   its member metadata, whose `url` is the member's `canonical_url` (`faust/assignor/client_assignment.py`,
   `partition_assignor.py::_metadata`): `http://<pod>.<headless service>.<namespace>.svc.cluster.local:<port>`,
   because the operator sets `web_host` to exactly that FQDN (`kasprapp.py` ~1931, `prepare_fqdn`).
   Fallback when the metadata cannot be read: the URL built from the member's static-membership
   instance id (the pod name) and this member's own `web_host` suffix. Faust's `Router.key_store`
   is not usable here: it maps only table-changelog partitions (`_tps_url` from
   `changelog_distribution`).
3. **Forwarding.** The receiving member authorizes the caller (below), then forwards the request —
   path, query, `Authorization` and the `x-forwarded-*` headers, plus `X-Tail-Forwarded-By: <member>`
   — to the owner over HTTP and streams the answer back unchanged. One hop only: an owner that does
   not tail the table answers `409 tail_rebalancing`, and so does a member whose map is stale after
   one refresh; the console retries on its next refresh. `X-Tail-Member` names the member that
   answered.
4. **Rebalances.** Adding or removing members moves tick partitions, so tables move: the new owner
   seeds (a schema fetch and a seek; a few hundred milliseconds) and starts from the committed
   offsets with an empty buffer, so for a few seconds the table answers `not_ready`, then its tail is
   complete again. The old owner stops a table when its ticks stop or, sooner, when its map shows it
   no longer owns the partition. Static membership (Kaspr sets the group instance id to the pod
   name) means a restarted member keeps its partitions: a pod restart moves nothing.

#### Activation: tail what is watched

Being opted in says a table *may* be tailed; being watched is what makes tailing it worth anything.
Of 500 opted-in tables, the ones with a Data tab open or a statement running at any moment are tens
at most; reading the other hundreds continuously would cost Kafka, memory and CPU for nothing. So a
member tails a table only while someone asks for it:

- A table arrives **`parked`**: known from its ticks, nothing assigned, nothing buffered, nothing
  read. A parked table costs its owner one tick a minute.
- The first request for it — a tail or a head peek — **activates** it: the owner fetches its schema,
  applies the backlog rule (below), assigns its partitions and reads the window from `committed + 1`
  (`catching_up`), and is `live` once it reaches the head: a second or two for a quiet topic, whose
  window is a few thousand rows. The request waits up to its `wait_ms` (the console sends 2,500) for
  that, so the first look after a quiet spell usually includes the tail; past the wait it answers
  what it has, and the console's next refresh completes it.
- Every request renews the table's `last_asked_at`; a table nobody has asked about for
  `live_tail_idle_seconds` (600; `0` never parks) is **parked** again: partitions unassigned, buffer
  dropped. Live mode asks every 1.5 s, so a table stays active exactly as long as a tab shows it,
  and ten minutes beyond.

The backlog rule applies to active tables; a parked table is neither `backlogged` nor `live`, it is
simply not looked at. Membership and settings keep arriving for parked tables (the ticks), and a
parked table's `schema_changed` events are ignored: activation fetches the schema afresh. With
this, the app's Kafka reads, memory and CPU follow the tables being watched, not the fleet (§11.1).

#### Seeding a table

When a table is activated (and on a re-seed, below):

1. Fetch the schema from the control plane: `GET /materializations/{id}/tables/{table}/schema?raw=true`
   with the service token → `iceberg_schema`, `fork_map`, `rejected_paths`, `last_column_id`,
   `schema_id` (`MaterializationSchema`, `web/responses.py::build_materialization_schema`). Take the
   committed offsets, the table UUID and the granularity from the tick. Nothing is read from GCS: a
   restart with 300 tables is 300 small HTTP calls (eight at a time), not 300 metadata files.
2. **Fallback:** when the fetch fails or the tick carries no `schema_id`, load the table through its
   pointer as `runtime_app/table_state.py::load_table` does and take schema, fork map, offsets and
   UUID from it.
3. Read the partitions and their end offsets from the cluster's metadata (`fetch_all_metadata()`
   and `end_offsets()`, as discovery does — never by subscribing: a broker that auto-creates topics
   must not create one, `schema/discovery.py::_start`) and evaluate the lag ("Backlogs and bursts",
   below). A table more than its bound behind is **`backlogged`**: nothing is assigned, and the table
   answers `409 tail_backlogged` until the runtime catches up.
4. Otherwise assign the table's partitions to the member's **one source consumer** (below), seeking
   each to `O(p) + 1` (`O(p) = -1` for a partition never committed → its beginning), and mark the
   table **ready** once its first fetch returns. Until then it answers `503 tail_not_ready`.

The control plane's record of a table's offsets can lag the table by one event, never lead it, so
seeding from it starts the tail at or before the table's true position — rows the console then
filters out by offset. Re-seed when: the tick's `table_uuid` differs (the table was dropped and
created again under its name, 10 §10.8); its `schema_id` is newer than the schema held and no
`schema_changed` event arrived; or a seek fell outside the partition's range. Never on a tick whose
hints merely repeat what is known.

#### One consumer per member

A member runs **one** `aiokafka.AIOKafkaConsumer` for every table it owns, not one per table — with
hundreds of tables that is one set of broker connections and one fetch loop instead of hundreds:
`group_id=None`, `enable_auto_commit=False`, `isolation_level` **equal to the runtime's**
(`read_uncommitted`: Faust's default unless the app is transactional, `faust/transport/drivers/aiokafka.py:350`),
`fetch_max_wait_ms` = `live_tail_fetch_wait_ms` (200), `fetch_min_bytes=1`, `max_partition_fetch_bytes=1 MiB`,
`fetch_max_bytes=32 MiB`, `max_poll_records=2000`, `auto_offset_reset="earliest"`. Adding or
removing a table calls `assign()` with the new union of partitions and seeks the new ones; the
fetcher restarts, a hiccup of some hundred milliseconds for the member's other tables, which phase 2
measures (a few consumer shards per member is the fallback if it matters). Records are dispatched by
`(topic, partition)` to their table's buffer.

#### Following the table: `materialization-table-events`

One plain consumer per member (no group, from the latest offset at start; the seed covers anything
earlier) on `MATERIALIZER_TAIL_TABLE_EVENTS_TOPIC`, applying for owned tables that are active — a
parked table fetches its schema and offsets afresh when activated (06 §6.6):

- **`table_committed`** → `payload.offsets` are the table's new committed offsets: advance each
  partition's `committed` (never backwards), which starts the retention clock on buffered rows at or
  below it. Nothing else: a commit never changes rows.
- **`schema_changed`** → `payload.schema`, `payload.fork_map`, `payload.schema_id`,
  `payload.rejected_paths`: install the new `Conformer`; every cached Arrow batch conformed under
  the older schema is stale and is rebuilt from its raw lines when next served, or eagerly in the
  background, oldest segment first.

Both are idempotent and may arrive late or twice; an event whose `schema_id` or offsets are not
newer changes nothing. Fleet volume is one event per commit or schema change per table — tens a
second at most.

#### Ingest and the buffer

For each record the fetch loop delivers:

1. Build the **staged line** exactly as the writer does: `staged_line(RuntimeSourceRecordMetadata, value)`
   (`runtime_app/writer/batches.py`), with the metadata from the same helpers the writer uses on a
   Faust event — `format_timestamp(record.timestamp)` (aiokafka gives milliseconds; the helper
   handles ms vs s and Kafka's "no timestamp"), `decode_kafka_bytes(record.key)`,
   `record_headers(record)` (aiokafka's `(key, bytes)` pairs are a sequence the helper takes),
   `observed_at = current_timestamp_ms()` (`runtime_app/writer/context.py`). The value is decoded as
   Kaspr's `json` serializer decodes it for the writer; a value that fails to decode yields no row
   and is counted `undecodable` (see the verification item in §15, phase 2).
2. A tombstone (`value is None`) with `skip_tombstones` yields no row and advances the position, as
   the writer does (`handlers/writer.py` lines 132–143); without it, a row with null payload.
3. Append the line to the partition's **open segment**; conform it with the current `Conformer`
   through the shared row builder (§5.3) and append the row to the segment's pending rows. A segment
   closes at `SEGMENT_ROWS` (2,000) or `SEGMENT_BYTES` (4 MiB) of staged JSON; on close its rows
   become one Arrow `RecordBatch` in the table's Arrow schema (`lakehouse/parquet.py::arrow_schema`),
   keyed by the schema id they were conformed under; the row dicts are dropped and the raw lines are
   kept **zstd-compressed** (`pyarrow.compress`, 5–10× on JSON) so the segment can be re-conformed
   after a schema change at a fraction of its raw size.
4. Between fetch batches the loop yields, so the serve path and the other consumers get the loop.

Per partition the buffer records `committed` (from the seed and table events), `next_offset` (the
next offset to fetch), `end_offset` (Kafka's high watermark from the last fetch, refreshed when idle
on each tick), `paused`, and whether the seek fell out of range (`gap`).

**Trim.** A segment whose last offset is `≤ committed` is *retained* for `live_tail_retention_seconds`
(120) after the commit that covered it, then dropped. Retention is what lets a run pinned to a
snapshot the pointer cache made up to 5 s old, that then pages for a while, still fetch its tail; a
run older than that is told the tail is unavailable for its snapshot and the console re-pins (§5.4).

**Bounds.** A table's buffer is bounded by `live_tail_max_rows_per_table` (250,000 uncommitted rows)
and `live_tail_max_bytes_per_table` (256 MiB resident: Arrow batches, compressed raw lines and the
open segments together). At a bound ingest **pauses** for that table (its partitions are paused on
the consumer) rather than evicting, so what is buffered stays a contiguous prefix; it resumes at
75 % of both limits. The member also holds `MATERIALIZER_TAIL_MEMORY_BUDGET_BYTES` (2.5 GiB resident
across every table); as it is approached, retained segments are dropped first, then the table with
the largest buffer pauses. Paused partitions are reported in every response (`X-Tail-Status:
truncated`, `X-Tail-Paused`, `X-Tail-End-Offsets`), in the health view, and logged once per
transition (`tail_fetch_paused`, `tail_fetch_resumed`).

A table pauses only when its uncommitted window outgrows its bound, which is exactly when honesty
about what is shown matters more than freshness. When the runtime is far behind, the tail does not
try at all:

#### Backlogs and bursts: when the tail applies

The tail is for a table whose runtime is keeping up: its uncommitted window is then a few minutes
of records, fits the bound, and the newest record is seconds away. Two situations break that, and
the tail handles each explicitly rather than by reading everything behind the runtime.

**A backlog.** A new materialization of a topic with hours of history, or a runtime that has fallen
far behind, has committed offsets millions of records short of the topic's head. Reading from
`committed + 1` would make the tail re-read the whole backlog at the runtime's pace — doubling a
multi-hour backfill's Kafka reads and conforming every record a second time — to show the *oldest*
uncommitted records, which nobody wants, while the newest stay out of reach behind the contiguity
rule. So the tail **does not apply** during a backlog. A table whose lag,
`Σ_p (end(p) − 1 − committed(p))`, is more than `live_tail_max_rows_per_table` is **`backlogged`**:
its partitions are not assigned, nothing is read but the end offsets (one metadata call per tick),
and requests answer `409 tail_backlogged` with `behind`, the end offsets and the committed offsets,
which the console shows as *Live tail starts once the runtime catches up · N records behind* over
the committed rows, which keep growing as they do today — `behind` doubles as the backfill's
progress. The lag is re-evaluated on every tick and every `table_committed`; once it falls below
75 % of the bound the table becomes **`catching_up`**: assigned and seeked to `committed + 1`, it
reads the remaining window (at most the bound) in seconds, serving the prefix it has with
`truncated` meanwhile, and is **`live`** once its fetches reach the head. A new materialization's
first tick finds `committed = −1` everywhere, so it starts `backlogged` and goes live on its own
some seconds before the runtime finishes the backfill, with no Kafka traffic from the tail in the
hours before.

**A burst.** A topic that is usually quiet produces far more than its runtime commits for a while.
The window outgrows the bound: the table **pauses** (its partitions paused on the consumer), holds
the contiguous prefix it has, answers `truncated` with `behind`, and resumes at 75 % of the bound
as commits land — a few commit intervals for a burst a few times the bound. A burst that keeps
growing past **twice** the bound is a backlog: the buffer is dropped and the table is `backlogged`
until the runtime is within 75 % of the bound again (enter at 2 ×, leave at 0.75 ×, so a table at
the boundary does not flap). A member's other tables are protected by a per-table ingest ceiling,
`live_tail_max_records_per_second_per_table` (5,000): a table bursting past it is read at the
ceiling — its partitions paused for the rest of each second — falls behind at the ceiling's pace
and reports it, while the rest of the member's tables keep their freshness. A single table
sustained above the ceiling is beyond one member's Python ingest and stays `backlogged` or
`catching_up`: a stated limit (§11.1), not the owner's fleet.

The states, as the health view and `X-Tail-State` report them: `parked`, `seeding`, `backlogged`,
`catching_up`, `live`, `paused`, `unavailable` (the topic cannot be read), `stopped`. The `tail-step` task re-evaluates them every 5 s from
the committed offsets it knows and the end offsets it last read.

#### The head peek

While a table is `backlogged` — and, less often, `paused` far behind — the Data tab shows the
committed rows and the lag, and nothing of what is arriving now. The **head peek** fills that: *the
newest N records on the topic, as they will look in the table*, on demand, apart from the table.

- **What it is not.** The peek is not stitched into any run: its rows are in no count, statement,
  export or profile, and the panel that shows them says they are not in the table yet. They will be,
  once the runtime gets there, and until then the same records can appear in the peek and later in
  the tail or the table — expected, and R2 is about runs. R3 holds: the rows come from the same row
  builder, under the table's current schema, with the same `_materialized_at` caveat (§8).
- **How the owner answers** `GET …/tables/{table}/head?rows=N` (§6.2), routed to the table's owner
  like any tail request: when the table is `live`, from its buffer — the newest N rows across its
  partitions, no Kafka read; otherwise from a **dedicated peek consumer** the member keeps for the
  purpose (manual assignment, the runtime's isolation level), which assigns the table's partitions,
  seeks each to `end(p) − ⌈N / P⌉`, fetches once, unassigns, and hands the records through the
  ingest path's decode and `build_row`; the rows are sorted newest first by Kafka timestamp, then
  offset, and cut to N. Tombstones and undecodable values are treated as ingest treats them. One peek
  fetch at a time per member (`MATERIALIZER_TAIL_PEEK_CONCURRENCY`, 2); each answer is cached for one
  second per table, so several tabs share one read.
- **Bounds.** `N ≤ live_tail_peek_max_rows` (1,000; the console asks for 200); one seek and one fetch
  per partition, at most `max_partition_fetch_bytes` each. The answer carries `X-Tail-Peek-Offsets`
  (per partition, the first and last offset included), `X-Tail-End-Offsets`, `X-Tail-Behind`,
  `X-Tail-State`, `X-Tail-Schema-Id` and `X-Tail-Settling`.
- **In the console** (§5.4): a *Peek at the newest records* button in the status line whenever the
  run's `tail.status` is `backlogged`, or `truncated` with `behind` beyond the per-request cap; it
  opens a panel over the grid — *Newest 200 records on the topic · not in the table yet · the runtime
  is M records behind* — with the table's columns, refreshed every `liveTailRefreshMs` while open and
  visible, closing by itself when the table goes `live`.

#### Serving

`GET /tail/v1/materializations/{materialization_id}/tables/{table}?after=…&max_rows=…&max_bytes=…`
(contract in §6.2), on `MATERIALIZER_TAIL_SERVE_CONCURRENCY` (4) threads per member:

1. Authorize (below). Resolve the table; forward if another member owns it; `404` if not tailed;
   `503 tail_not_ready` while seeding.
2. For each partition `p` with `after[p] = a`: if the buffer's oldest retained offset is `> a + 1`,
   the tail is unavailable for this snapshot on `p` → the whole answer is `409 tail_unavailable`
   naming the partitions (a mixed answer would be a hole). Otherwise select the segments with rows
   `> a`, in offset order, up to the per-request caps.
3. Rebuild any selected segment whose Arrow batch is not of the current schema id from its raw
   lines (the same row builder); slice the boundary segment with `pyarrow.compute.greater(offset, a)`;
   concatenate; write **one Parquet file** to memory with `lakehouse/parquet.py::write_table`'s
   settings — zstd, statistics on, the table's Arrow schema with field ids — the writer the seal uses.
   Rows of several partitions share the file, as a compacted file's do.
4. Answer the bytes with the `X-Tail-*` headers. `through[p]` is the last offset served for `p`
   (`max(a, last buffered)`); `status` is `complete` when no partition is paused and every
   partition's `through` reaches its `next_offset − 1`, `truncated` otherwise.

Settling: rows whose `_unmapped` holds a path the schema has neither a field for nor rejected are
counted in `X-Tail-Settling`; they are what a schema change in flight will move into columns (§8).

Because the console asks each refresh only for rows past what its run already has (§5.4), a
steady-state answer is a second and a half of records; the whole window is sent once per run.

#### Authorization

The console sends the **person's** session token (`callerOf(request)`, as it does for every change
it forwards). The tail member resolves it the way the console itself does (`web/server/auth/principal.ts`):
`GET <control plane>/auth/session` with the bearer, cached by a hash of the token for
`MATERIALIZER_TAIL_SESSION_CHECK_SECONDS` (5), kept 60 s more if the control plane is unreachable.
The answer's `principal` (`Principal.describe()`: `permissions`, `all_permissions`,
`dataset_permissions`, `dataset_limit`, `password_change_required`) is rebuilt into a `Principal` by
a new helper `identity/principals.py::principal_from_description(payload)`, so the check is the
control plane's own `Principal.can("data.query", dataset_id)`. A guest is whatever the Guest group
grants; the service token resolves to the service principal, which holds no `data.query`
(`SERVICE_PERMISSIONS`), so it is refused — only people and their access tokens read tails. A
`password_change_required` principal is refused, as everywhere. A forwarding member checks before
forwarding and the owner checks again (one cached lookup).

These WebViews do not call `authorize(request, directory_table, …)`: the tail app has no `directory`
table. `AuthorizationWiringTests` learns that views labelled `kaspr.io/app: materializer-tail`
authorize through `tail_app.auth.authorize_tail` (§15, phase 2).

#### Observability

Protocol events (behind `protocol_logging_enabled`, carried by the ticks): `tail_table_seeded`
(`source`: `control_plane` or `gcs`, `offsets`, `schema_id`), `tail_table_forgotten`,
`tail_fetch_paused`, `tail_fetch_resumed`, `tail_schema_applied`, `tail_served` (`rows`, `bytes`,
`ms`, `forwarded`), `tail_forwarded`, `tail_routing_mismatch`, `tail_rebalanced` (tables gained and
lost). Views: `GET /tail/v1/health` (this member's tables, partitions, buffered rows and bytes,
lag, pauses, errors, the budget), `GET /tail/v1/members` (the partition → member map and, with
`?health=true`, every member's health fanned out).

#### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `MATERIALIZER_TAIL_APP_NAME` | `materializer-tail` | names the member in logs and views |
| `MATERIALIZER_TAIL_CONTROL_PLANE_URL` | — (required) | in-cluster `http://materializer-control-plane-app-api:6065` (`develop-` prefixed in dev): session checks and schema fetches |
| `MATERIALIZER_TAIL_SERVICE_TOKEN` | — (from the `materializer-auth` Secret, key `service-token`) | the read-only service credential, for schema fetches only |
| `MATERIALIZER_TAIL_TABLE_EVENTS_TOPIC` | `materialization-table-events` | the topic it follows (prefixed per overlay) |
| `MATERIALIZER_TAIL_REQUESTS_TOPIC` | `materializer-tail-requests` | the tick topic, for owner computation (prefixed per overlay) |
| `MATERIALIZER_TAIL_FORGET_AFTER_SECONDS` | `300` | a table no tick names for this long is dropped |
| `MATERIALIZER_TAIL_MEMORY_BUDGET_BYTES` | `2684354560` (2.5 GiB) | resident buffer bytes across every table before retained rows are dropped and the largest table pauses |
| `MATERIALIZER_TAIL_SESSION_CHECK_SECONDS` | `5` | how long a checked session is trusted |
| `MATERIALIZER_TAIL_SERVE_CONCURRENCY` | `4` | Parquet builds at once per member |
| `MATERIALIZER_TAIL_PEEK_CONCURRENCY` | `2` | head-peek fetches at once per member |
| `MATERIALIZER_TAIL_MEMBER_MAP_REFRESH_SECONDS` | `15` | how often the partition → member map is refreshed |
| `MATERIALIZER_RUNTIME_MEMBER_ID` | the pod name | as the maintenance app |
| `K_KAFKA_*`, `K_WEB_HOST`, `GOOGLE_APPLICATION_CREDENTIALS` | from the KasprApp spec and the operator | the Kafka client (as `kafka/metadata.py::from_environment` reads it), this member's own URL, GCS for the fallback seed |

Resources per member: requests 1 CPU / 2 GiB, limits 2 CPU / 4 GiB (§11.1); `replicas` by §11.1's
rule, 2 to start. What the app does per table — bounds, retention, fetch wait — comes with every tick
from the control-plane settings, so a change applies within a minute without a restart, as
maintenance settings do (17 §17.7).

### 5.2 Control plane changes

- **Model.** `Materialization.live_tail: bool = False` (`models/materialization.py`; `from_dict`
  reads a record written before the field as off). `MaterializationRequestSpec.live_tail: bool | None = None`
  (None: take the deployment's default) and `MaterializationRequestEvent.live_tail: bool | None = None`
  (`models/events.py`) — an update carries the new value only when it changes it, the pattern `label`
  uses.
- **API.** `POST /materializations` takes `live_tail` (boolean, optional); absent, the create
  resolves it from `live_tail_default` **now**, like `partition_granularity` from its default
  (`web/materializations.py::_normalize_request_spec`), so a later settings change does not affect
  the request. `PATCH` takes it as a mutable setting (`MUTABLE_SETTINGS` += `live_tail`): changing
  it bumps neither `spec_version` nor the runtime (the runtime knows nothing of it), exactly like
  `name`. `GET /materializations/{id}` carries `materialization.live_tail`
  (`web/responses.py::build_materialization_detail`); `GET /materializations` carries it per item.
  Request intake applies it on create (`_persist_create_request_state`) and on update
  (`_apply_settings_update`, from `request_event.live_tail`). A kept table is never tailed, so the
  flag means "the written tables".
- **Ticks.** `services/live_tail.py::build_live_tail_messages(table, config, issued_at)` returns the
  `tail_tick`s and `tail_cancel`s of §5.1; `handlers/maintenance.py` yields them routed `tail` from
  the existing `schedule_table_maintenance` step (it still writes nothing — `ShardRoutingTests` stays
  green); the `maintenance` agent gains an output topic `materializer-tail-requests` (declared,
  **64 partitions**, predicate `route == "tail"`), patched to `develop-materializer-tail-requests` in
  the dev overlay. No new work step: no `WORK_STEPS`, filter or task change (16 §16.3). The control
  plane only *sends* to the new topic, so the deploy needs no forced consumer-group rebalance.
- **Settings** (§6.5): control-plane scope, resolved with overrides by `resolve_config`, carried by
  the ticks. `live_tail_enabled` is the deployment-wide switch: off, no ticks are sent and the
  console's `GET /api/config` says so; `live_tail_default` decides what a create takes when it does
  not say.

Nothing changes in the runtime, the committer, the maintenance app, the REST catalog, the table
layout, the snapshot summaries, or the control plane's own resources.

### 5.3 The shared row builder (seal ↔ tail parity)

Today `runtime_app/writer/seal.py::run_seal` (lines 142–209) turns a staged line into a row inline:
`conformer.conform(staged["v"])`, the metadata columns from `staged["m"]` (`t`, `p`, `o`, `k`, `h`,
`a`), `_unmapped` via `unmapped_text`, then `partition_text` for the file it goes to. Extract the
row part, unchanged, into a new module:

```python
# python/src/materializer_control_plane/lakehouse/rows.py
@dataclass(frozen=True, slots=True)
class BuiltRow:
    row: dict[str, Any]          # every column of the table's current schema, metadata first
    unmapped_values: int         # len(unmapped) or 0
    moment: datetime | None      # the parsed Kafka timestamp, for partitioning and bounds

def build_row(staged: dict[str, Any], *, conformer: Conformer, default_partition: int, now: Callable[[], datetime]) -> BuiltRow:
    """One table row from one staged line, exactly as the seal writes it."""
```

`run_seal` calls `build_row` and keeps everything else (grouping by partition value, row groups,
file naming). The tail calls the same function with the same `Conformer` class and writes with the
same `arrow_schema` and Parquet settings. The seal's output must be byte-identical before and after:
`test_runtime_seal.py` and a regenerated console fixture (`python/tests/console_fixture.py`) prove it.
This is the whole of R3's machinery; §8 covers what the shared function cannot make identical.

### 5.4 Console changes

The console depends on the tail through one HTTP endpoint (§6.2) and stays extractable.

- **Configuration.** Fixed: `CONSOLE_TAIL_URL`, the tail app's base URL; empty, the feature is off in
  this console. In every deployment the console runs in-cluster and the URL is the Service,
  `http://materializer-tail-app-api:6065` (`develop-` prefixed in dev). During development and
  testing the console runs on a laptop: the lab's HTTPRoute routes `/tail/*` on the control plane's
  host to the tail Service, and `CONSOLE_TAIL_URL` is that host — the endpoint is still the console's
  alone there (§6.2). Tunable (section `live-tail`, §6.5):
  `liveTailEnabled`, `liveTailRefreshMs` (1,500), `liveTailMaxRows` (100,000: the most tail rows one
  fetch takes, sent as `max_rows`), `liveTailTimeoutMs` (3,000: past it a run proceeds without its
  tail). `GET /api/config` reports `liveTail: { configured, enabled, refreshMs }`; `enabled` is this
  console's switch and the control plane's `live_tail_enabled` together (the console already reads
  `GET /system/settings` for the create dialog's defaults; it reads `live_tail_default` from the
  same answer to pre-check the toggle).
- **A tail client** (`web/server/data/tail.ts`): `fetchTail(caller, {materializationId, table, after, maxRows, timeoutMs})`
  → `{ bytes, through, endOffsets, status, rows, timeMin, timeMax, schemaId, settling, lagMs, member, generatedAt }`
  from the body and headers; non-2xx and timeouts become a `TailOutcome` of `unavailable` with a
  reason (`not_ready`, `rebalancing`, `unavailable_for_snapshot`, `off`, `unreachable`, `forbidden`),
  never a thrown error. The bearer is the person's token (`callerOf(request).token`), and
  `X-Materializer-Console` carries the console's service credential (`config.serviceToken`), which
  the tail app requires: the endpoint is the console's alone (§6.2).
- **Runs.** Every run gets a work directory (`<CONSOLE_RUN_DIR>/<run id>/`, today only statements
  have one; `runs/run.ts` takes it over and `releaseStorage` removes it). At the start of
  `execute()`, when the request's `liveTail` is on, the table's `live_tail` is on, the console setting
  is on and the run is not **As of** an older snapshot, the run fetches the tail of each table it
  reads past that table's pinned `catalog.snapshot.offsets` (`iceberg/snapshot.ts::committedOffsets`
  already parses `materializer.offsets`), writes `tail-<table>-0.parquet` into its directory and
  adds a `DataObject` for it: `uri` the local path (`data/objects.ts::objectSources` already passes a
  non-`gs://` URI through), `rows` from `X-Tail-Rows`, `bytes`, `timeMin/timeMax` from the headers,
  `timeKnown: true`, `partition: null`, `offsetStart/offsetEnd: null`, `partitionKey: null`,
  `committedAt: null`, `tail: true`. From there the engine needs nothing new:
  - a **browse run** orders objects by `timeMax` (`runs/browse.ts::orderObjects`), so the tail file
    is first and the first page is served from local disk; `overlapsRange`/`insideRange` prune it by
    time like any file; counts under filters read it like any file;
  - a **statement** lists the tail file among its table's sources, so `sales.orders` and
    `iceberg.sales.orders` include it (`runs/view.ts::statementViews` reads by field id, so the
    file binds like a data file), and the sandbox already allows the run's directory
    (`data/duckdb.ts::withSandbox`, `allowed_directories`).
  The run snapshot gains `tail` per table read (§6.4) and a `revision` counter.
- **Refresh, incremental.** `POST /api/runs/:runId/refresh`: re-read the pointer (`fresh`); if the
  table's current snapshot is the run's and the run is a browse run, fetch **only the rows past the
  run's current `tail.through`** (`after = through`), write them as `tail-<table>-<n+1>.parquet`, add
  it as a further object (newest, so first), count it (local), extend `through`, bump `revision`,
  emit on the run's stream, answer `{ replaced: false, run }`; otherwise start a new run with the
  same request and answer `{ replaced: true, run }`. Live never refreshes a statement; a statement's
  *Auto re-run* (below) calls the same endpoint and always gets a new run (`replaced: true`), its
  result being materialized. The page query key includes `revision` (`useRun.ts::pageQueryKey`), so the grid
  refetches after a refresh. Appending keeps I2 (§7): each fetch continues at the previous `through`.
- **Live mode (the browse view).** On a tail-enabled materialization the Data tab's structured view
  shows a **Live** toggle, on by default, remembered in the view state (`viewState.ts`, `live=1` in
  the address bar). While on, the tab is visible (`document.visibilityState`) and the run has
  succeeded, the client calls `refresh` every `liveTailRefreshMs`; the status line reads *Live ·
  through now · tail N rows*, or the tail's notice: *Live tail unavailable — showing committed rows*
  (`unavailable`), *The newest N records are not shown yet; the runtime is M records behind*
  (`truncated`, from `endOffsets − through`), *n rows are waiting for a schema change*
  (`settling > 0`), *Live tail is k s behind* (`lagMs`). **Newer data available** keeps its meaning
  for the committed part. Under Live a **relative range** ("Last 15 minutes") keeps its end open —
  `range.to = null`, so the rows the tail appends are never pruned by the run's own window — and
  re-resolves its start when a new run replaces the old one after a commit, so the window slides; an
  absolute range whose end is past has nothing new to show, and the status line says so instead of
  polling. **Statements** are not refreshed by Live: a statement gets its tail once, when it runs,
  and the SQL panel offers an *Auto re-run* toggle, off by default, that starts a new run 5 s after
  the previous one finished — never while one is running — through the same `refresh` endpoint.
- **Head peek.** When a run's `tail.status` is `backlogged` (or `truncated` with `behind` beyond the
  per-request cap), the status line offers *Peek at the newest records*. The BFF route
  `GET /api/materializations/:id/tables/:table/head?rows=200` (`permitAnywhere('data.query')`,
  `requireOnTables`) fetches the owner's Parquet with the person's token, writes it to a temporary
  file, reads it through the table's logical view (`readParquet` + `selectList(columns)`, newest
  first) on the shared DuckDB instance, and answers rows and columns in the shape of a `RunPage` plus
  the peek's headers. `HeadPeek.tsx` renders them with the grid's cells in a panel over the grid,
  labelled *not in the table yet*, polling every `liveTailRefreshMs` while open and visible and
  closing when the table goes `live`. Nothing of it reaches runs, exports or profiles.
- **Permissions.** Unchanged: `POST …/runs` and `refresh` require `data.query` on every table read
  (`routes/data.ts::requireOnTables`, `ownRun`), and the tail call carries the person's token for the
  tail app's own check. `visibleDetail` needs no change: `live_tail` is not sensitive.
- **Create and edit.** A *Live tail* toggle in the create dialog's advanced fields
  (`CreateMaterializationDialog.tsx`, beside `skip_tombstones`), pre-checked from `live_tail_default`;
  the same toggle in the edit dialog (`EditMaterializationDialog.tsx`); the Overview's settings panel
  shows *Live tail: On/Off* (`OverviewTab.tsx`); `createMaterializationSchema`/`updateMaterializationSchema`
  accept `live_tail` (`routes/materializations.ts`); `CreateMaterializationInput`/`MaterializationUpdate`
  carry it.

### 5.5 Sequence: produce → visible (Data tab, Live mode)

```
t0        producer → broker ack
t0+0.2 s  the owner's fetch returns the record (fetch_max_wait 200 ms); staged line built; conformed; appended
t0+0.2–1.7 s  the console's live timer fires (period 1.5 s)
          BFF → POST refresh → pointer read (cached ≤ 2 s) → snapshot unchanged
          BFF → tail Service GET /tail/v1/…?after={p:through} → (forwarded to the owner if needed) → Parquet of the new rows
          BFF writes tail-<table>-<n>.parquet, counts it, bumps revision, emits
t+0.1 s   browser receives the run snapshot on the stream, refetches page 1 (local file), renders
```

The committed path keeps running underneath. When a commit lands, the next refresh sees a new
snapshot, starts a new run pinned to it, and asks the tail past the new offsets — the records that
were in the tail a moment ago are now in a data file, each exactly once (§7).

---

## 6. Contracts

### 6.1 `materializer-tail-requests` (new topic)

**64 partitions**; key `tail:<materialization_id>:<table>` (`models/live_tail.py::live_tail_routing_key`);
value `LiveTailMessage.to_dict()`:

```json
{
  "kind": "tail_tick",
  "target": { "materialization_id": "mat-…", "dataset": "sales", "dataset_id": "ds-…", "table": "orders",
              "location": "gs://…/mat-…/orders", "source_topic": "orders", "partition_granularity": "day",
              "table_uuid": "…", "skip_tombstones": false },
  "issued_at": "2026-10-23T12:00:00Z",
  "settings": { "live_tail_max_rows_per_table": 250000, "live_tail_max_bytes_per_table": 268435456,
                "live_tail_retention_seconds": 120, "live_tail_fetch_wait_ms": 200,
                "live_tail_max_records_per_second_per_table": 5000, "live_tail_peek_max_rows": 1000,
                "live_tail_idle_seconds": 600, "protocol_logging_enabled": true },
  "hints": { "offsets": { "0": 1041, "1": 998 }, "schema_id": 3, "metadata_location": "gs://…/00042-….metadata.json" }
}
```

`tail_cancel` carries `kind`, `target`, `issued_at`, `reason` (`table_dropping`,
`materialization_deleting`). A malformed message is logged and dropped. Producer: the control plane's
`maintenance` agent; consumer: the tail app's `tail-worker` agent. Declared by both (`declare: true`,
`partitions: 64`), whichever starts first, as `materializer-maintenance-requests` is. The partition
count bounds the app's members; changing it later moves every table once.

### 6.2 The tail HTTP API (tail app)

**The console's alone.** These endpoints exist for the Materializer Console and no other caller:
every request must carry the console's service credential in `X-Materializer-Console` besides the
person's bearer, or it is `403 forbidden`, reason `console_only`. They are not part of the public
API — handbook 04 lists them only to say so, handbook 19 documents them — and may change between
releases without a deprecation.

**`GET /tail/v1/materializations/{materialization_id}/tables/{table}`**

| Query | Meaning |
|---|---|
| `after` | required: JSON object of partition → inclusive last offset the caller already has (`{"0":1041,"1":-1}`); `-1` for a partition the snapshot never committed. Partitions absent from it are taken as `-1` |
| `max_rows` | optional, default and cap `live_tail_max_rows_per_table`; the console sends `liveTailMaxRows` |
| `max_bytes` | optional cap on the body, default `live_tail_max_bytes_per_table` |
| `format` | `parquet` (default) or `json` (rows as JSON objects, at most 1,000, for tests and debugging; the same row builder) |
| `wait_ms` | optional, default 0, at most 5,000: how long the owner may hold the answer while the table is activating or catching up, so the first look at a parked table usually includes its tail; the console sends `liveTailTimeoutMs − 500` |

Headers: `Authorization: Bearer <session or access token>`; `X-Materializer-Console: <the console's
service credential>` (required, above); `x-forwarded-for`/`x-forwarded-user-agent` as the console
forwards them; `X-Tail-Forwarded-By` between members only.

**`200`**, `content-type: application/octet-stream`, body one Parquet file (zstd, statistics, the
table's Arrow schema with field ids; possibly zero rows). Headers:

| Header | Value |
|---|---|
| `X-Tail-Rows`, `X-Tail-Bytes` | rows in the body; bytes of the body |
| `X-Tail-After` | the request's `after`, normalized (JSON) |
| `X-Tail-Through` | per partition, the last offset included (JSON); `through[p] ≥ after[p]` always |
| `X-Tail-End-Offsets` | per partition, Kafka's last existing offset (high watermark − 1) as last seen (JSON) |
| `X-Tail-Status` | `complete` (every partition served through its newest fetched record and none paused), `truncated` (a per-request cap or a buffer bound stopped a partition short), `empty` (nothing past `after`) |
| `X-Tail-Paused` | JSON list of paused partitions (bound reached), possibly empty |
| `X-Tail-Gap` | JSON list of partitions whose `after + 1` was below Kafka's beginning (records retention removed; served from the beginning) |
| `X-Tail-Schema-Id` | the schema the rows were conformed with |
| `X-Tail-Settling` | rows holding `_unmapped` paths neither in the schema nor rejected (§8) |
| `X-Tail-Time-Min`, `X-Tail-Time-Max` | epoch ms of `_kafka_timestamp` over the body; absent when every row is untimed |
| `X-Tail-Lag-Ms` | 0 when the last fetch of every partition reached the high watermark; else ms since it last did |
| `X-Tail-Member` | the member that answered (its pod name); `X-Tail-Forwarded: 1` when another member forwarded |
| `X-Tail-State` | the table's state at the owner: `catching_up`, `live` or `paused` (a `backlogged` or `seeding` table answers an error instead) |
| `X-Tail-Table-Uuid`, `X-Tail-Generated-At` | provenance |

Errors are the control plane's usual error body, `{"error": {"code", "message", "details"}}`.
Kaspr fixes one content type per WebView today (`kaspr/types/models/webview/response.py`,
`build_error` uses the view's), which would put JSON in an `application/octet-stream` body. **A
Kaspr change is in scope for this alone** (§17 D22): an error content type per view, so the tail's
errors are `application/json`. Until that change is deployed, the view's `bodySelector.onError`
JSON-encodes the error as bytes and the console parses a non-2xx body as JSON whatever its content
type — and keeps doing so after, so either Kaspr works. **Process:** when the implementer makes the
Kaspr change, they pause and ask the owner to deploy it to the kaspr-operator and the lab, so every
later test runs on a Kaspr that has it.

| Status | `code` / `details.reason` | When |
|---|---|---|
| `401` / `403` | as `identity/authorization.py` raises them | no usable session; `data.query` not held on the table's dataset; password change required |
| `403 forbidden` | `console_only` | the request lacks the console's credential in `X-Materializer-Console`, or carries a wrong one |
| `404 not_found` | `live_tail_off` | the table is not tailed by this app: never ticked, forgotten, cancelled, or the materialization is not running |
| `409 conflict` | `tail_unavailable`, `details.partitions: {"0": "behind_retention"}` | `after[p]` is older than the retained range: the caller's snapshot is too old — re-pin |
| `409 conflict` | `tail_rebalancing` | the table is moving between members: the member the map named does not own it yet, or the map is stale after a refresh — retry on the next refresh |
| `409 conflict` | `tail_backlogged`, `details.behind`, `details.end_offsets`, `details.committed` | the runtime is more than the table's bound behind the topic's head (a backfill, or a burst that outgrew twice the bound): the tail does not apply and reads nothing until the runtime is within 75 % of the bound — the console shows how far behind |
| `503 unavailable` | `tail_not_ready` (`details.state`: `seeding` or `catching_up`; `retry-after: 1`) once `wait_ms` has run out, `control_plane_unreachable` (the session could not be checked), `tail_owner_unreachable` (forwarding failed) | |

**`GET /tail/v1/materializations/{materialization_id}/tables/{table}/head`** — the head peek (§5.1).
Query `rows` (default 200, at most `live_tail_peek_max_rows`) and `format` as above. Authorization
and routing as for the tail. `200` with a Parquet body in the same physical format and
`X-Tail-Peek-Rows`, `X-Tail-Peek-Offsets` (per partition, the first and last offset included, JSON),
`X-Tail-End-Offsets`, `X-Tail-Behind`, `X-Tail-State`, `X-Tail-Schema-Id`, `X-Tail-Settling`,
`X-Tail-Member`, `X-Tail-Generated-At`. Errors as for the tail, except that a `backlogged` table
answers — that is the point — and `503 tail_not_ready` applies only while its schema is being fetched.

**`GET /tail/v1/health`** and **`GET /tail/v1/members`** (`materializations.view_details`): §5.1
"Observability".

### 6.3 Control-plane API changes

| Endpoint | Change |
|---|---|
| `POST /materializations` | body `live_tail` (boolean, optional; absent → `live_tail_default`); echoed in the acceptance |
| `PATCH /materializations/{id}` | body `live_tail` (boolean) is a mutable setting; alone it is a valid change; applies at once; no `spec_version` change, no runtime re-apply; needs `materializations.edit` |
| `GET /materializations/{id}` | `materialization.live_tail` |
| `GET /materializations` | each item: `live_tail` |
| `GET /system/settings` | `live_tail_enabled` and `live_tail_default` among the effective control-plane settings (as every catalog setting already is) |

No new permission (R4 asks for the existing ones): enabling is editing a materialization; reading
is `data.query` on the dataset; health is `materializations.view_details`.

### 6.4 Console API changes

| Endpoint | Change |
|---|---|
| `POST /api/materializations/:id/runs` | `RunRequest.liveTail?: boolean` (default true when the materialization has `live_tail`); ignored with `asOf` |
| `POST /api/runs/:runId/refresh` (new) | `permitAnywhere('data.query')`, `ownRun`; answers `{ replaced: boolean, run: RunSnapshot }` (§5.4). Live calls it for browse runs; a statement's *Auto re-run* calls it and always gets `replaced: true` |
| `RunSnapshot` | `revision: number`; `tables[].tail?: RunTail` and `tail?: RunTail` for the run's table |
| `RunTail` | `{ status: 'complete' \| 'truncated' \| 'empty' \| 'backlogged' \| 'unavailable' \| 'off', state?: 'catching_up' \| 'live' \| 'paused' \| 'backlogged', reason?: string, rows: number, segments: number, through: Record<string, number>, endOffsets: Record<string, number>, behind: number, settling: number, lagMs: number \| null, member: string \| null, generatedAt: string \| null }` — `behind` is `Σ (endOffsets − through)` (for `backlogged`, `endOffsets − committed`), `rows` the sum over segments |
| `GET /api/materializations/:id/tables/:table/head?rows=` (new) | `permitAnywhere('data.query')` + `requireOnTables`; answers `{ rows, columns, peek: { rows, offsets, endOffsets, behind, state, settling, generatedAt } }` — the newest records on the topic shaped as the table's logical columns; never part of a run |
| `GET /api/config` | `liveTail: { configured: boolean, enabled: boolean, refreshMs: number }` |
| `POST /api/materializations`, `PATCH …/:id` | `live_tail` passed through (zod `z.boolean().optional()`) |
| `GET /api/materializations/:id` | `materialization.live_tail` passes through the detail |

### 6.5 Settings

Control plane (`settings/catalog.py`, `CONTROL_PLANE_SETTINGS`, new section `live-tail`; config
fields and env vars `MATERIALIZER_CONTROL_PLANE_<KEY>` per the adding-settings skill):

| Key | Kind | Default | Bounds | Description (as shown) |
|---|---|---|---|---|
| `live_tail_enabled` | boolean | true | — | Serve the newest records of materializations that ask for it from Kafka before they are committed. Off, nothing is tailed and the console shows committed rows only. |
| `live_tail_default` | boolean | true | — | Whether a new materialization has live tail on when its request does not say. Each materialization can be changed afterwards. |
| `live_tail_max_rows_per_table` | integer, rows | 250,000 | 1,000–10,000,000 | Most uncommitted records the tail keeps for one table, and how far behind the topic's head a runtime may be for the tail to apply: further behind — a new materialization's backfill — the table is backlogged and the tail reads nothing until the runtime catches up; past it during a burst the tail pauses, and queries say the newest records are not shown yet. |
| `live_tail_max_bytes_per_table` | integer, bytes | 268,435,456 | 8 MiB–4 GiB | Most memory the tail spends on one table. Roughly the table's uncommitted records' size; the tail app's members are sized for the sum (handbook 19). |
| `live_tail_retention_seconds` | integer, seconds, advanced | 120 | 10–3,600 | How long committed records stay in the tail, so a query that read the table a moment before a commit still finds them. |
| `live_tail_fetch_wait_ms` | integer, ms, advanced | 200 | 50–2,000 | Longest the tail waits for new records on each read of Kafka. Lower is fresher and costs more broker requests. |
| `live_tail_max_records_per_second_per_table` | integer, records, advanced | 5,000 | 100–100,000 | Fastest the tail reads one table, so one topic's burst cannot slow the others on its member. A table producing faster falls behind at this pace and says so. |
| `live_tail_peek_max_rows` | integer, rows, advanced | 1,000 | 10–10,000 | Most records one peek at a topic's newest records reads. The console asks for 200. |
| `live_tail_idle_seconds` | integer, seconds | 600 | 0–86,400 | How long a table stays tailed after the last query asked for it. Past it the tail lets go of the table until the next query, which then waits a second or two for it. 0 keeps every table on. |

`live_tail_default` has `effect="new materializations"`. All but the first two travel with the
ticks; none is read while rendering a runtime spec. Cross-field rule: none. Tests: `test_settings.py`
(catalog ↔ config ↔ env), `test_runtime_settings.py`'s spec-hash override dict (the keys do not reach
the runtime spec).

Console (`web/server/config.ts` `tunableDeployment`, `web/server/settings/catalog.ts`, section `live-tail`):

| Key | Env | Default | Bounds |
|---|---|---|---|
| `liveTailEnabled` | `CONSOLE_LIVE_TAIL_ENABLED` | true | — |
| `liveTailRefreshMs` | `CONSOLE_LIVE_TAIL_REFRESH_MS` | 1,500 | 500–60,000 |
| `liveTailMaxRows` | `CONSOLE_LIVE_TAIL_MAX_ROWS` | 100,000 | 1,000–10,000,000 |
| `liveTailTimeoutMs` | `CONSOLE_LIVE_TAIL_TIMEOUT_MS` | 3,000 | 500–30,000 |

Fixed: `CONSOLE_TAIL_URL` (shown read-only on the Deployment section, `routes/settings.ts::consoleDeployment`).
New section in `web/src/features/settings/sections.ts`: `{ id: 'live-tail', label: 'Live tail', group: 'platform', permission: 'settings.view', kind: 'settings' }`
— the implementer confirms the page renders settings of several scopes under one section id, as
`materializations` (control plane) and `live` (console) are rendered today, and otherwise splits it.
The tail app's own variables (§5.1) are deployment-fixed and documented in handbook 19, not shown on
the page: the app has no settings table and the control plane does not read them.

---

## 7. Why the result is exactly once

Definitions. For a table, a snapshot *S* with summary offsets `O_S(p)` (`materializer.offsets`;
`−1` for an absent partition: `CommittedOffsets.get`). A record is at `(p, o)`; it *needs a row*
unless it is a tombstone of a `skip_tombstones` materialization or undecodable.

**I1 — the snapshot invariant.** A record that needs a row is in *S* ⇔ `o ≤ O_S(p)`. Forward
direction: `plan_append` accepts a seal only when `after_offset ≤ c < offset_start` for the current
cursor `c` and then sets `c = offset_end`, so the committed files of a partition cover exactly
`(previous, O_S(p)]`, and every record in a seal's range is in its files or needs no row
(`lakehouse/commits.py` docstring and lines 373–408). Backward direction: no accepted seal has
`offset_end > O_S(p)`, by the same cursor arithmetic. Compaction preserves it: `commit_compaction`
checks per partition that the outputs hold exactly the inputs' rows and carries the offsets forward
unchanged (`lakehouse/maintenance.py`, 10 §10.10). Expiry removes snapshots, not rows of the current
one. An external writer with `catalog.write` is outside this argument, as it is outside the
console's today (it refuses delete files; 14 §14.2).

**I2 — the tail invariant.** A tail answered for `after = A` returns, for each `p`, exactly the
records Kafka holds with `A(p) < o ≤ through(p)`, each once, or refuses (`409`) for the whole
request. By construction: the owner's buffer is filled by one consumer from `O(p) + 1` upward in
offset order with no gaps but Kafka's own (a segment's rows are the fetch's records in order); trim
removes only whole segments at the old end; pause stops at the new end; the serve path selects
segments by offset and filters the boundary one with `> A(p)`; a partition whose retained range no
longer reaches `A(p) + 1` fails the request rather than answering a hole. A forwarded request is the
owner's answer, unchanged; a member that is not the owner never answers rows.

**The union.** A run pinned to *S* fetches tails `T_0, T_1, …` with `A_0 = O_S` and `A_{k+1} =
through_k`, so the tails are disjoint and contiguous and `T = ∪ T_k` is exactly the records with
`O_S(p) < o ≤ through(p)`. A record with `o ≤ O_S(p)` is in *S* and not in *T*; one in
`(O_S(p), through(p)]` is in *T* and not in *S*; one beyond `through(p)` is in neither and the run
says so (`behind`). Each record that needs a row appears at most once, and none in the covered
range is missing.

**Across commits.** A later snapshot *S′* has `O_S′ ≥ O_S` (`CommittedOffsets.advanced` only
raises; the pointer never moves backwards, 11 §11.2). A run keeps *S* and the tails it fetched (the
files live in the run's directory) for as long as it pages; a refresh extends *T* for the *same* *S*
while *S* is current, and starts a new run on *S′* otherwise. The two are never mixed: the
invariant holds per run.

**Across compaction.** Offsets are unchanged, so *T* is unchanged; the console's catalog of *S* is
immutable (`snapshotCatalog` is cached per snapshot id).

**Across runtime restarts, rebalances and a stale committer.** The table's committed offsets are
the only checkpoint and only grow; a restarted member re-reads from them and the committer refuses
covered and overlapping seals (10 §10.6). None of it moves `O_S`, so none of it changes any tail.
The tail app does not read the runtime at all.

**Across tail-app restarts and rebalances.** A member that (re)gains a table seeds from the
control plane's record of the table's offsets — at or before the table's true position, never past
it — and from Kafka: it either serves a correct prefix past `A` or `503 tail_not_ready`; a run that
cannot get its tail is the committed snapshot with a notice — today's behaviour, never a duplicate.
An incremental fetch after a move asks for rows `> through`; the new owner either has them (it has
caught up past `through`) or answers `empty` with `through` unchanged — never a hole.

**Retention, Kafka compaction and transactions.** "The records Kafka holds" is the standard on both
sides: offsets Kafka compaction removed need no row for the runtime (Faust counts them as
acknowledged gaps, 10 §10.7) and simply do not arrive for the tail; `through` is the last offset
*served*, not an arithmetic bound. A partition whose `after + 1` is below Kafka's beginning is
served from the beginning and flagged `X-Tail-Gap` (those records are gone for the runtime too).
The tail consumer uses the runtime's isolation level, so an aborted transaction's records are seen by
both or by neither.

**Schema changes** do not touch offsets; they change how a record's *value* is laid out (§8), not
whether it appears.

---

## 8. Fidelity: forks, collapses, `_unmapped`, and the schema race

The seal conforms every staged row against **the schema current when the seal runs**, waiting up to
`seal_schema_wait_seconds` (30 s) for an evolution its rows asked for (`pipeline._waiting_for_schema`,
09 §9.7). The tail conforms against **the schema current when the row is served**, re-conforming
from its raw line after every `schema_changed`. Both therefore use "the latest schema at the moment
of conversion", and in the steady state — no change in flight — the two rows are identical by
construction: same input line, same `Conformer`, same function (§5.3), same Arrow schema and writer.

While a change is in flight (a new key or type first seen → writer's request → the committer's
2 s window → one metadata commit → `schema_changed`; 6.3 s end to end on the load test, 13 §13.8),
the tail serves the affected rows conformed against the old schema — the new value in `_unmapped`,
or a forked value in the family's older member where it fits — and counts them in `X-Tail-Settling`.
Within a few seconds it serves them under the new schema. The committed row usually gets the new
schema too, because seals wait for it; a seal that gave up waiting writes `_unmapped`, in which case
the *committed* row carries the older form and the tail's later form is the one that disagrees. No
reader can know the seal's timing, so no design can be exact in that window; this one converges to
the most likely committed form, is exact everywhere else, and tells the console which rows are
still settling (decided: rows are shown at once; §17 D6).

**Collapses** are the one case where a schema change rewrites rows that had no unmapped value (after
`<name>__json` exists every value of the key goes there); re-conforming every segment from raw lines
covers it.

**`_materialized_at`** is "when the runtime read the record" (11 §11.3). The tail cannot know that
instant; it writes its own read time, typically within a few hundred milliseconds of the runtime's.
This is the one column whose committed value will differ, documented as such (§17 D1).

**Everything else matches by sharing code:** `_kafka_key` and `_kafka_headers` with `b64:` for
non-UTF-8 (`decode_kafka_bytes`), `_kafka_timestamp` null when Kafka gave none, non-object values
under `$` in `_unmapped`, tombstones as null-payload rows or no rows, field ids and types from the
same `arrow_schema(schema)`, zstd Parquet from the same writer.

---

## 9. Latency budget

Produce → visible in the Data tab, Live mode, steady topic, console in the cluster or on a laptop
against the lab (the lab figures in 13 §13.8 and web 06 §6.2 are from a laptop):

| Step | Typical | p95 | Source |
|---|---|---|---|
| Producer → broker ack, record fetchable | 20 ms | 50 ms | Kafka |
| Owner's fetch returns it (`fetch_max_wait_ms` 200, `fetch_min_bytes` 1) | 100 ms | 250 ms | aiokafka; mean half the wait |
| Staged line + conform + append (a fetch batch) | 20 ms | 60 ms | ~60–120 µs a record for ~1.4 KB records; measured in phase 2 |
| Wait for the console's live timer (period 1,500 ms) | 750 ms | 1,430 ms | uniform |
| `refresh`: pointer read (cached ≤ 2 s) | 5 ms | 200 ms | web 06 §6.2 (41 ms warm) |
| BFF → tail Service → (forward to the owner) → session check (cached 5 s) → select segments → Parquet of a second's records → transfer | 70 ms | 300 ms | pyarrow write ~20–50 ms; a forward hop ~2 ms; in-cluster transfer ~1 ms/MB |
| BFF: write file, count the new object (local DuckDB), emit | 40 ms | 120 ms | local read |
| Browser: stream frame, page 1 refetch (local file), render | 120 ms | 300 ms | web 07 §07.5 first page 64 ms warm |
| **Total** | **≈ 1.1 s** | **≈ 2.7 s** | 2.3 s of headroom to 5 s |

The p95 column adds the steps' p95s, which is pessimistic (they are independent). A 1 s refresh
period would take ~0.5 s off at twice the request rate; left at 1.5 s so headroom covers GC pauses
and a cold tail segment rebuild after a schema change (a 2,000-row segment ≈ 150 ms).

**SQL runs.** A statement run started at *T* fetches its tail during planning, so it holds every
record the owner had buffered by then: the record must have been fetched by the owner (≤ 0.3 s after
produce at p95). A probe that re-runs `SELECT count(*) … WHERE probe_id = …` once a second sees the
record within the poll period plus the statement's own duration (~0.4–1 s over a bounded range):
p95 ≈ 2 s. Without Live mode, a browse run pressed by hand is the same.

**What the budget does not cover.** The first query after a table is opted in, a member (re)started,
a table moved, a backlog ended or a parked table activated (seeding: a schema fetch and a seek, then
catching up from the committed offset — a second or two for a quiet topic, which the request waits
for up to its `wait_ms`, and `tail_not_ready` past that); a new key's first records (the schema race,
§8, ~6 s to their final form); a tail-app or console restart. R1 is about a steady topic, and each
of these is reported rather than hidden.

**The head peek** (§5.1) during a backlog is an on-demand fetch of ~100–300 ms plus the panel's
refresh period: a record produced at the head is in the panel within one to two seconds — outside
the table, which is still hours behind.

---

## 10. Failure modes

| Situation | What happens | What the person sees | Recovery |
|---|---|---|---|
| Tail app down, unreachable, or `CONSOLE_TAIL_URL` wrong | `fetchTail` → `unavailable` within `liveTailTimeoutMs`; the run proceeds committed-only | *Live tail unavailable — showing committed rows* | every refresh retries |
| A member's pod restarts | static membership keeps its partitions; its tables re-seed on return (~2–5 s + catching up) | the notice for those tables | automatic |
| A member is lost for longer than the session timeout, or members are added or removed | Kaspr rebalances the tick topic; moved tables re-seed on their new owner; maps refresh within 15 s | `tail_rebalancing` / `not_ready` notices for the moved tables, seconds | automatic |
| Member map stale (a request reaches a non-owner) | one immediate map refresh, then forward; still wrong → `409 tail_rebalancing` | the notice until the next refresh | ≤ 15 s |
| Partitioner disagreement (ticks land where the owner computation does not expect) | `tail_routing_mismatch` errors on every tick; forwards fail with `409` | the notice | fix the key bytes or partitioner — caught in the first minute on the lab (§13.4 step 0) |
| Table just opted in / just seeded / just moved | `503 tail_not_ready` | same notice | ready in ~2–5 s |
| Kafka unreachable from a member | ingest stalls; `X-Tail-Lag-Ms` grows; buffered rows still served | *Live tail is k s behind* | aiokafka reconnects |
| Source topic deleted while tailed | fetch errors name the topic; the table is `unavailable` with the reason; nothing else on the member is affected | *Live tail unavailable — the topic is gone* | the runtime is failing too; the table recovers if the topic returns, or leaves with its materialization |
| Control plane unreachable from the tail app | schema fetches fall back to GCS; session checks use the 60 s grace, then `503 control_plane_unreachable` | the notice (and the console itself is degraded) | — |
| A burst outgrows the bound (up to twice it) | the table's ingest pauses at the bound, holding its contiguous prefix; `truncated`, `X-Tail-Paused`, `X-Tail-End-Offsets` | *The newest N records are not shown yet; the runtime is M behind* | resumes at 75 % of the bound as commits land — a few commit intervals |
| A backlog: a new materialization on a topic with history, a runtime hours behind, or a burst past twice the bound | the table is `backlogged`: nothing assigned or read but the end offsets; a held buffer is dropped; `409 tail_backlogged` with `behind` | *Live tail starts once the runtime catches up · N records behind*, over the committed rows, which keep growing | automatic: `catching_up` once within 75 % of the bound, `live` seconds later — some seconds before the backfill ends |
| One table sustained above `live_tail_max_records_per_second_per_table` | read at the ceiling, its partitions paused for the rest of each second; it falls behind at that pace, the member's other tables do not | *Live tail is k s behind*, then backlogged if it keeps growing | raise the ceiling for that deployment if its members have the CPU; a single table beyond one member's ingest is a stated limit (§11.1) |
| Member memory budget reached | retained rows dropped first, then the largest buffer pauses | same, for that table; health says which | add members or raise the budget and the limit |
| Nobody has asked about a table for `live_tail_idle_seconds` | parked: partitions unassigned, buffer dropped; ticks still followed | nothing — no tab shows it | the next request activates it |
| First request for a parked table | activated: schema fetched, partitions assigned, the window read; the request waits up to `wait_ms` | usually the tail in the first answer; else *Live tail warming up* for a second or two | automatic |
| Pod at its memory limit | the kernel kills the member; Kaspr restarts it with the same partitions | the notice for its tables until re-seeded | lower the budget or raise the limit; the rule in §11.1 |
| Run's snapshot older than retention (long paging; a run left open) | `409 tail_unavailable` | Live mode re-pins (new run); otherwise the notice | — |
| **As of** an older snapshot | no tail requested | *Older snapshot* as today | — |
| Materialization stopped | ticks stop; forgotten after 300 s; `404 live_tail_off` | Live toggle hidden; committed rows | ticks resume within a minute of resume |
| Schema change in flight | rows served under the old schema; `settling` counted | *n rows are waiting for a schema change* | re-conformed after `schema_changed` (~3–6 s) |
| `schema_changed` / `table_committed` missed (member restarted, consumer hiccup) | the next tick's hints disagree → re-fetch the schema / trim | at most 60 s of stale schema or extra memory | automatic |
| Partition count grew | discovered on the next tick's end-offset refresh; new partitions assigned from `−1 + 1` | nothing | ≤ 60 s |
| Table dropped / materialization deleted | `tail_cancel` → forgotten at once; `404` after | the console stops asking (detail) | — |
| Opt-out / deployment switch off | ticks stop; forgotten after 300 s | the console stops asking at once | — |
| Console restart | runs and tail files gone (as today: runs live in the process) | the tab runs the view again | — |
| Tail Parquet unreadable / mismatched schema id (a bug) | DuckDB error on the tail object → the run marks the tail `unavailable`, logs, and proceeds without it | the notice | — |
| A record Kaspr cannot decode | no row, counted `undecodable`, position advances (mirrors the writer) | nothing | — |
| Offsets below Kafka's beginning | served from the beginning, `X-Tail-Gap` | *Records before offset x are no longer in Kafka* | — |

In no case does the console show a row twice, invent one, or fail a run because the tail failed.

---

## 11. Cost and limits

### 11.1 Capacity model

**The uncommitted window of a table is the runtime's doing, not the tail's.** A batch rolls at
10,000 rows or 120 s (a quiet topic's at the 600 s ceiling), then a commit every 30 s plus the
commit itself; so at default runtime settings a table's window holds about

```
rows_window ≈ min(rate × 630 s, max(10,000, rate × 150 s))
```

— 6,300 rows for a 10 records/s topic (ten minutes of them), 15,000 at 100/s, 150,000 at 1,000/s.
At the lab's 1.4 KB records that is 9 MB, 21 MB and 210 MB raw. A table is "busy" for the tail when
that window outgrows `live_tail_max_bytes_per_table`; the 256 MiB default fits a 1,000/s topic.

**Per table, resident memory** ≈ 1–1.3 × the raw window (Arrow batches ≈ 0.6–0.8 ×, zstd raw lines
≈ 0.15 ×, open segments and overhead) plus ~1 MB fixed (the consumer is the member's, not the
table's): about 2 MB at 1/s, 11 MB at 10/s, 16 MB at 50/s, 25 MB at 100/s, 245 MB at 1,000/s. A
`backlogged` table costs nothing but its schema. Phase 2 measures the factor.

**Per member** (requests 1 CPU / 2 GiB, limits 2 CPU / 4 GiB, budget 2.5 GiB):

- CPU: ingest ≈ 60–120 µs a record of Python in one process — one core at most — so plan
  **5,000 records/s per member** aggregate, serving (pyarrow, which releases the GIL) on threads
  beside it.
- Memory: the budget, 2.5 GiB of buffers, leaving 1.5 GiB for the process, the consumer's fetch
  buffers (up to `fetch_max_bytes` in flight) and Parquet builds.
- Kafka: one consumer per member for every owned partition, one table-events consumer, one admin
  client; broker connections O(brokers), not O(tables).
- GCS: none in steady state; the fallback seed only.

**Members** = `max(Σ rate ÷ 5,000, Σ table memory ÷ 2.5 GiB)`, rounded up, plus one for headroom;
at most 64 (the tick topic's partitions). Tables spread over members by key hash, so with hundreds
of tables the load is even to within the heaviest table.

Worked examples, 1.4 KB records, default runtime settings:

| Fleet | Aggregate rate | Members by CPU | Table memory | Members |
|---|---|---|---|---|
| 300 topics × 10 rec/s | 3,000/s | 1 | ~3.3 GiB | **2** |
| 300 × 100 rec/s | 30,000/s | 6 | ~7.5 GiB | **6–7** |
| 300 × 1,000 rec/s | 300,000/s | 60 | ~73 GiB | **60–64**; the runtime fleet itself is ~215 members at that rate (1,400 rec/s each, 13 §13.8) |

**The owner's fleet** (2026-10-23): 10–100 materializations per control plane, at most 500 topics
across them, most at 1–50 records/s with occasional bursts. Opted in, all 500 tables cost their
ticks and nothing else until watched (§5.1 "Activation"). A watched table of that kind holds
630–10,000 rows of window, 2–16 MB, and a few hundred records/s at most; how many are watched at
once is how many Data tabs and statements are open — tens, not hundreds — so steady-state buffers
are tens to a few hundred MB and ingest a few thousand records/s. **Two members** (one is enough;
two for availability; requests 1 CPU / 2 GiB, limits 2 CPU / 4 GiB) carry it; were every table kept
always on (`live_tail_idle_seconds` 0) the formula gives three. Add a member when the members view
shows one member's ingest above 4,000 records/s or its budget binding. A burst on one topic is
bounded by that table's 256 MiB and the ceiling, and a backfill costs the tail nothing (§5.1).

**What does not grow with topics:** GCS (nothing per query, nothing at seed), the control plane
(500 ticks a minute, one session check per token per 5 s), the console (per open tab, incremental
fetches) — and, with activation on demand, the tail's own Kafka reads, memory and CPU, which follow
the tables being watched; only the ticks and the member map grow with the fleet.

**Levers.** (1) The runtime's `max_open_file_age_ceiling_seconds` (600): quiet topics hold ten
minutes of records because of it; at 120 s their windows and memory shrink five-fold, at the cost of
a file per partition every two minutes on quiet topics for compaction to fold. (2) Record size: the
window scales with it. (3) The per-table bound: lower it to cap what one topic may take, at the cost
of `truncated` on that topic. (4) Later, per-record cost: a member can run its ingest across
subprocess shards to use its second core — the same cores, fewer pods.

### 11.2 Resources elsewhere

- **The control plane:** unchanged in size; the ticks and session checks are negligible.
- **Kafka:** a watched table's topic is read twice while it is watched — by the runtime and by its
  tail owner — and a parked table's not at all. Reading is per broker, not per partition: a member's
  one consumer with thousands of partitions sends one fetch per broker every 200 ms, as MirrorMaker
  does, so even the all-watched case is routine for the cluster, if wasteful.
- **Console:** the first fetch of a run is the whole window (≤ `liveTailMaxRows`, zstd Parquet:
  100,000 rows of ~1.4 KB ≈ 5–8 MB compressed) once per new run — after each commit of a table on
  screen; every refresh after that is the records since (a second and a half of them). One local
  DuckDB count per fetched segment; disk per run for its tail files, removed with the run.

### 11.3 External engines see committed snapshots only

Spark, Trino, DuckDB and PyIceberg on the REST catalog (11 §11.8) and `iceberg_scan` by path read
the table; the tail exists only in the console's runs. The handbook and the Table tab's Connect
panel say so. Offering the tail to engines is possible later and is not in this plan (§17 D13).

### 11.4 What the tail does not cover

- **As of** runs; kept (retired) tables; materializations with `live_tail` off or not running
  (§17 D3); the first seconds after opt-in, a member restart or a table move; a table whose runtime
  is more than the bound behind the topic's head — a new materialization's backfill above all —
  which is `backlogged` until the runtime is nearly caught up (§5.1 "Backlogs and bursts").
- Exports and profiles of a run read the run's rows, tail included (§17 D2).
- Statements get their tail once, at start; Live does not refresh them — *Auto re-run*, when asked,
  starts a new run 5 s after the previous one finished.
- The head peek is a window on the topic, never part of a run: its rows are in no count, statement,
  export or profile, and may overlap what the tail or the table hold moments later.

### 11.5 Bounds, stated

Per table `live_tail_max_rows_per_table` / `_bytes_per_table`; per request `max_rows` (console:
`liveTailMaxRows`) and `max_bytes`; per member `MATERIALIZER_TAIL_MEMORY_BUDGET_BYTES`; retention
`live_tail_retention_seconds`; serve concurrency 4 per member; the session cache bounded (10,000
entries, as the console's); members ≤ 64. A query reads at most its caps' worth of tail rows plus
the snapshot files its range selects, as today.

### 11.6 Version skew

An older console ignores `live_tail` and `tail` fields; an older control plane refuses `live_tail`
in a body as an unknown setting (400) — the console sends it only when the control plane's detail
carries the field. Runtimes are untouched. Additive everywhere: a **minor** release by 16 §16.3
(§17 D11).

---

## 12. Security

- The tail endpoint needs a person's session or access token holding `data.query` on the table's
  dataset, checked by the control plane's own rule rebuilt from `GET /auth/session` (§5.1). The
  console checks the same on its route before calling. Guests and the service token hold no
  `data.query` by default.
- Dataset limits on access tokens apply (`Grants.dataset_limit` is part of the description the tail
  app rebuilds).
- A member serves rows only of tables the control plane ticked it for, by `materialization_id` and
  `table`; it never reads a location it was not told. Forwarding carries the caller's own bearer;
  the owner checks it again.
- The body is data the caller may read; the console stores it under the run's own directory, which
  only that run's sandbox may open and which goes with the run (`runs/store.ts::getRun` keeps runs
  private to their owner).
- The service token the tail app holds is the existing read-only one, used only to fetch schemas.
  No new secret.
- Session checks are cached 5 s; revocation reaches the tail app within that, as it does the console.
- Health and membership views carry no row data and need `materializations.view_details`.
- The head peek needs the same `data.query` on the table's dataset, is bounded by
  `live_tail_peek_max_rows`, is shown apart from the table and is stored nowhere.
- The endpoint is the console's alone: every request must carry the console's service credential
  in `X-Materializer-Console` besides the person's bearer, so a script holding a valid token cannot
  use it (`403 console_only`); it is not a public API and is documented as console-internal
  (handbook 19), free to change between releases. In every deployment the console runs in-cluster
  and the Service is cluster-internal; the lab's HTTPRoute exposes `/tail/*` for development and
  testing from a laptop, where the credential requirement still holds.

---

## 13. Test plan

Three rules, then the tests. (1) **Every failure mode in §10 has a named test** — the matrix in
§13.5 is the checklist, and a §10 row without a test is a gap in this plan, not in the code.
(2) **Exactly once is proved by a model, not by examples**: `test_tail_exactly_once.py` drives the
buffer and a simulated runtime, console and Kaspr through random commits, compactions, bursts,
parks, moves and schema changes and asserts the invariants of §7 after every step. (3) **What a
unit test cannot reach is reached on the lab**: real Kafka, brokers restarting, members killed, the
control plane and the console restarting, an hour of soak — §13.4 steps 14–19. Fakes, not mocks, as
the suite does today (13 §13.3); no test is skipped, and every phase ends with the whole repo's
suites green (§13.7).

### 13.1 Python unit tests (new and extended)

| Module | Covers |
|---|---|
| `test_row_builder.py` (new) | `build_row` on every value kind the policy knows: scalars into exact and accepting members, forks, a collapsed family, list elements, struct keys, `_unmapped` for rejected and unknown paths, `$` for a non-object, tombstones, `b64:` keys and headers, missing timestamp; and that `run_seal`'s rows equal `build_row`'s for the same lines |
| `test_runtime_seal.py` (extended) | byte-identical output before and after the refactor on the existing cases (keep a golden file written before the change) |
| `test_tail_parity.py` (new) | property test: random records (generated with the shapes `test_lakehouse_schema_policy.py` uses), sealed with `run_seal` into Parquet and served by the tail's `serve.py` from the same staged lines → identical Arrow tables after dropping `_materialized_at`; under schema evolution mid-stream (evolve between two segments) the tail's re-conformed rows equal a seal under the new schema; a collapse (`<name>__json`) re-conforms rows that had no unmapped value, matching the seal; a fork arriving before its `schema_changed` is served under the old schema and counted settling, then under the new |
| `test_tail_buffer.py` (new) | pure buffer rules: contiguity, segment close and raw-line compression round trip, trim after commit + retention, bounds pause/resume at 75 %, selection past `after` including the boundary filter, `through`, `unavailable` for an `after` behind the retained range, `gap`, the budget dropping retained segments before pausing the largest table |
| `test_tail_routing.py` (new) | owner partition from the key bytes with aiokafka's `DefaultPartitioner`; the map from a fake group description (assignments + Faust `ClientMetadata` URLs, and the FQDN fallback); forwarding with headers preserved and one hop only; `tail_rebalancing` on a stale map after one refresh; the tick self-check logging `tail_routing_mismatch`; a forwarded request is never forwarded again (`X-Tail-Forwarded-By` ends the chain); an unreachable owner → `503 tail_owner_unreachable`; after a move, the old owner forwards rather than serving its stale buffer once its map says so |
| `test_tail_app.py` (new; `tail_support.py` with a `FakeAIOKafkaConsumer` over in-memory partitions supporting `assign`/`seek`/`pause`/`getmany`, a fake control plane answering the schema and session endpoints, `gcs_fakes.FakeGcs` and `lakehouse_support.LakeFixture` for the fallback) | seed from the control plane's schema answer and the tick's hints, and from GCS when the fetch fails; one consumer per member with the union of partitions re-assigned as tables come and go; ingest; serve Parquet readable by pyarrow with the table's field ids; `table_committed` trims after retention; `schema_changed` re-conforms; a tick with a newer `schema_id` and no event re-fetches; another `table_uuid` re-seeds; `tail_cancel`; forgotten after `forget_after_seconds`; partitions added; a restart re-seeds and serves the same prefix; the bound pauses only that table's partitions and reports; a table more than the bound behind at its first tick is `backlogged` with nothing assigned and answers `tail_backlogged` with `behind`, becomes `catching_up` when a `table_committed` brings it within 75 % of the bound and `live` once caught up; a burst past twice the bound drops the buffer and goes `backlogged`, with no flapping at the boundary; a table above the ingest ceiling is read at the ceiling while the member's other tables keep up; `skip_tombstones` honoured; undecodable values counted; the serve view's auth (guest refused, `data.query` on another dataset refused, limited token refused, service token refused, person allowed, forwarded request re-checked); errors in the agreed shape; a table is parked until asked for and activated by a request, which waits up to its `wait_ms` for the window; it parks again after `live_tail_idle_seconds` without a request and nothing of it stays; `0` keeps it active; the head peek answers the newest N rows from the buffer when `live` and from the dedicated peek consumer when `backlogged`, equal to the row builder's rows, newest first, bounded by `live_tail_peek_max_rows`, forwarded to the owner, cached one second, refused without `data.query`; `format=json` equals the Parquet's rows |
| `test_live_tail_scheduler.py` (new) | `build_live_tail_messages`: ticks only for `active` tables of `live_tail` materializations that are `running`; cancels for dropping/deleting; settings and hints carried; nothing when `live_tail_enabled` is off |
| `test_maintenance.py::ShardRoutingTests` (extended) | the scheduled step still writes nothing with tail outputs present |
| `test_web.py` (extended) | create with `live_tail` and without (resolves `live_tail_default`); refused when not boolean; `PATCH` with `live_tail` alone accepted, no `spec_version` bump, no runtime re-apply; detail and list carry it |
| `test_request_intake.py` (extended) | create persists it; an update applies `request_event.live_tail` and leaves `spec_version` alone |
| `test_models.py` (extended) | round trips for `Materialization`, `MaterializationRequestSpec`, `MaterializationRequestEvent`, `LiveTailMessage` |
| `test_settings.py`, `test_runtime_settings.py` (extended) | the six new keys; spec-hash override dict covers them |
| `test_kafka_admin.py` (extended) | `describe_consumer_group` decoding assignments and member metadata, with its deadline |
| `test_agent_manifests.py` (extended) | the `maintenance` agent's `tail` output topic (64 partitions) and its dev-overlay patch; the tail app owns its agent, task and WebViews (as `test_the_maintenance_app_owns_its_agent_and_task`); tail WebViews authorize through `authorize_tail` (`AuthorizationWiringTests` taught the exception by app label); every overlay prefixes the tail topics and the tail app's env |
| `test_tail_exactly_once.py` (new) | model-based: random records on 1–8 partitions; a simulated runtime committing random prefixes (offsets only ever growing); compaction (offsets unchanged); bursts; the buffer's trims, pauses, parks and re-seeds (buffer dropped); member moves (a fresh buffer seeded from a lagging record of the offsets); schema changes (re-conform); and a simulated console that pins snapshots at random moments and fetches tails incrementally (`after = through`), with and without `wait`. After every step: the union of a pinned snapshot's rows and its tail segments is exactly the records with offset ≤ `through`, each once; `through` never regresses within a run; every answer is a contiguous prefix or a `409`, never a hole; a `backlogged` or `parked` table reads nothing. Ten thousand random schedules per run of the suite, seeded so a failure replays |
| `test_tail_failures.py` (new; fault injection through the fakes) | **Kafka:** a fetch raising mid-ingest keeps the buffer, retries with backoff, grows `lag_ms`, keeps serving what is buffered, and resumes from `next_offset` with no gap or duplicate; a partition whose leader is unavailable blocks only itself; the source topic deleted → the table is `unavailable` with its reason, answers `503`, and recovers when the topic returns. **Control plane:** the schema fetch failing → GCS fallback; both failing → retries 1, 2, 5, 10, 30 s and `not_ready` meanwhile; session checks failing → cached sessions served through the 60 s grace, then `503 control_plane_unreachable`, and recovery afterwards. **Messages:** malformed ticks, cancels and table events logged and dropped; duplicate and late ticks and events idempotent; a tick whose hints carry offsets below those known never moves `committed` backwards; a `table_committed` ahead of ingest (committed > `next_offset`) trims nothing unread, and the rows fetched afterwards still serve an older snapshot within retention. **Serving:** `serve_concurrency` requests at once queue the rest, never fail them; a `wait_ms` request on a table that never catches up answers at the deadline with what it has; a forwarded request the owner answers `409 tail_rebalancing` is passed through unchanged; the peek's dedicated consumer failing leaves the tail's own consumer untouched |
| `test_tail_security.py` (new) | a forged `X-Tail-Forwarded-By` from outside the app gains nothing (the bearer is checked regardless); no token → `401`; an expired or ended session → `401` within `session_check_seconds`; a revoked access token likewise; a token limited to another dataset → `403`; the service token alone → `403` on rows and peek, `200` on health; a person's bearer without `X-Materializer-Console` → `403 console_only`, with a wrong value → `403 console_only`, and the forwarded request carries it to the owner; path parameters validated as the control plane's identifiers are (no traversal; an unknown table is `404` and leaks nothing else); the peek's `rows` above the cap clamped, below 1 refused; no row content in any log line |

### 13.2 Console tests (`node --test`)

| File | Covers |
|---|---|
| `web/server/data/tail.test.ts` (new) | header parsing, every status mapped to a `TailOutcome`, timeout → `unavailable`, the person's token forwarded, non-2xx JSON-in-bytes bodies parsed; `fetchHead` and the peek's rows shaped through the logical view (forks folded, nested paths named) newest first |
| `web/server/routes/data.test.ts` (new) | the head route: `data.query` on the table's dataset required, `rows` bounded, the answer's `peek` block from the headers, no run created or touched |
| `web/server/runs/tail.test.ts` (new; tail Parquet files written by `console_fixture.py`, served by a stub tail server) | browse run: the tail object first, exact counts include it, time pruning on its bounds, paging across the tail/file boundary newest-first and oldest-first; filters count it; statement: `sales.orders` and `iceberg.sales.orders` include the tail rows, a join across two tables each with a tail, the sandbox opens the tail files and nothing else; `asOf` → no tail; `refresh` with the same snapshot fetches only past `through`, appends a segment and bumps `revision`; with a new snapshot starts a new run; `unavailable` → the run completes without a tail and says so; `rebalancing` → same, retried next refresh; a tail file DuckDB cannot read → `unavailable`, not a failed run; a Live browse run of a relative range keeps `range.to` null so the rows its tail appends are not pruned, and the run that replaces it after a commit re-resolves `range.from`; `refresh` of a statement run always answers `replaced: true` |
| `web/server/routes/materializations.test.ts` (extended) | `live_tail` through create and update schemas |
| `web/server/settings/catalog.test.ts` (extended) | the four console settings |
| `web/server/routes/system.test.ts` (extended) | `/api/config` `liveTail` block |
| `web/src/features/data/hooks.test.ts` (extended) | `pageQueryKey` includes `revision`; the live timer pauses when hidden and while a run is running; it stops on an auth notice and on `tail.status: 'off'`; it never runs for a statement; the statement's *Auto re-run* fires 5 s after the previous run finished and never while one runs; a relative range under Live is sent with an open end and its start re-resolved on each new run; an absolute range with a past end polls nothing |
| `web/server/data/tail.test.ts` (failure cases) | a connection reset mid-body, a body shorter than `X-Tail-Bytes`, a missing or unparsable `X-Tail-Through`, a `200` whose body is not Parquet → each `unavailable` with a reason, never thrown; `409 tail_backlogged` → `backlogged` with `behind`; `409 tail_rebalancing` and `503 tail_not_ready` → `unavailable` with reasons the status line tells apart |
| `web/server/runs/tail.test.ts` (failure cases) | a refresh while another refresh of the same run is in flight is serialized, not doubled; a refresh of an expired run → `404` and the client starts a new run; of a cancelled or failed run → a new run; a tail that becomes `off` mid-live (the materialization's `live_tail` turned off) → `tail.status` `off`, no further fetches; the person losing `data.query` on the dataset between refreshes → `ownRun` refuses and the stream closes with the auth reason; the first fetch of a run timing out → the run completes committed-only within the timeout and says `unavailable`; a tail segment file missing from disk → that tail `unavailable`, the committed rows still page |
| Fixture | `python/tests/console_fixture.py` also writes `web/testdata/iceberg/orders/tail-after-{offsets}.parquet` and a second file for an incremental fetch, with the expected rows (records past the fixture's committed offsets, built with `build_row`), so `iceberg.test.ts` can compare the TypeScript side's reading with Python's, as E5 does for the table |

### 13.3 Browser (Playwright, per the lab recipes)

Live toggle on a tail-enabled materialization: rows appear without pressing Run; the status line's
states (*Live*, unavailable, truncated, settling, behind) each driven by stubbing the tail response
in a dev BFF; the toggle persists in the address bar; Live pauses when the tab is hidden; a
materialization with `live_tail` off shows no toggle; the create dialog's toggle is pre-checked and
round-trips; the edit dialog toggles it; during a stubbed backlog the *Peek at the newest records*
button appears, the panel opens with the table's columns, refreshes while open, and closes when the
stub reports the table `live`; the failure states driven by the stub — *warming up*, *rebalancing*,
*unavailable*, *off*, and access lost mid-live (the sign-in prompt, the Live timer stopped) — each
render and recover; the Live toggle is absent in SQL, whose *Auto re-run* re-runs 5 s after each
completion and stops when turned off; "Last 15 minutes" under Live keeps showing records produced
after Run was pressed; light and dark themes.

### 13.4 Lab validation (shesuelab)

Deploy per the lab procedure (overlay → kustomize → chart → helm; the tail app added to the overlay
as the maintenance app was, with its `kaspr.io/app` label patches by selector, its topic names, env
and package ref; an HTTPRoute rule for `/tail/*` to `develop-materializer-tail-app-api`). Start with
**2 members**. Then:

0. **Routing self-check.** Within the first minute of ticks, no `tail_routing_mismatch` in either
   member's logs; `GET /tail/v1/members` shows every tick partition owned, split across the two
   members; a request for a table through the Service answers with `X-Tail-Member` naming its owner
   whichever member the Service picked (repeat ten times).
1. **Steady freshness (R1).** Create a 3-partition topic `fresh.orders` (Strimzi KafkaTopic), a
   materialization (live tail defaults on) after seeding a few records first (the seed-before-bulk
   rule). Run a probe (`lab/freshness_probe.py` in the session scratchpad; aiokafka through the
   external listener): produce one record a second for 5 minutes with `probe_id` and `produced_at`;
   in parallel poll (a) the console's Data tab in Live mode through Playwright, reading the first
   row's `probe_id`, and (b) `POST /api/materializations/{id}/runs` with
   `SELECT max(probe_id) FROM fresh.fresh_orders` once a second. Record first-visible − `produced_at`
   per record; report p50/p95/max for both; **pass: p95 ≤ 5 s** over ≥ 300 records. Also record
   `X-Tail-Lag-Ms` from the health view.
2. **Exactly once (R2), steady.** Every poll also runs
   `SELECT count(*), count(DISTINCT probe_id), min(probe_id), max(probe_id) FROM fresh.fresh_orders`
   and, per partition, `count(*) = max(_kafka_offset) − min(_kafka_offset) + 1`; pass: count equals
   distinct, and the set of visible ids is a prefix of the produced ids per partition, at every poll,
   across the default 30 s commits — including the incremental refreshes between commits.
3. **Across compaction.** Lower `compaction_min_input_files` to 2 and request compaction while
   producing; repeat check 2; reset the setting after.
4. **Across runtime restarts.** Delete the committer pod and a writer member pod (graceful; a true
   SIGKILL needs the node-debug pod — ask first) while producing; repeat check 2; confirm the tail's
   `through` never regresses.
5. **Tail app restarts and rebalances.** Delete the owner's pod while producing: static membership
   brings it back with its tables; the console shows the notice, then recovers; check 2 holds. Then
   scale the app 2 → 3 → 1 members: tables move, `tail_rebalanced` is logged, the members view
   agrees, every table is served again within seconds, check 2 holds throughout; no duplicate across
   any move.
6. **Schema change (R3).** Produce records with a new key and a type fork (`total` string); watch
   `settling` go up then to 0; after the next commit, fetch the tail Parquet for the same offsets
   (`format=parquet`, before the commit) and the committed data file (after), and diff by
   `(_kafka_partition, _kafka_offset)`: every column equal except `_materialized_at`.
7. **The bound.** Set `live_tail_max_rows_per_table` to 1,000, produce 5,000 quickly: the response is
   `truncated`, `behind` ≈ 4,000, rows contiguous from the committed offset; the banner shows it; the
   member's other tables keep serving; after the commit catches up the tail resumes. Reset.
8. **Permissions (R4).** A token limited to another dataset, and a user without `data.query`, get
   no tail (console: no request made; direct call: 403, also when the request is forwarded); a
   `catalog.read` token through DuckDB on the REST catalog counts only committed rows, and
   `console count − catalog count` equals the tail's rows at that moment.
9. **Switches.** `PATCH live_tail: false`: the toggle disappears at once, the table leaves the owner
   within 5 minutes (health); `live_tail_enabled` off: every Live toggle disappears, no ticks; stop
   the materialization: the same for its tables; resume: back within a minute; delete: gone at once.
10. **Scale and resources.** Create 50 small topics with a script (3 partitions, ~5 rec/s each) and
    50 materializations of them (seed first); confirm every table is tailed across the members, the
    one-consumer-per-member re-assign hiccup on each new table stays under a second for the member's
    other tables (`X-Tail-Lag-Ms`), and record per-member RSS and CPU (`/proc`) against §11.1's
    model at 5 rec/s and, on one topic, at 1,000 rec/s; raise one topic's partitions 3 → 6 and see
    the new partitions tailed within a minute. Delete the topics after.
11. **Backlog.** Create a materialization of `materializer-loadtest-orders` (111 M records) with
    live tail on: the health view shows its tables `backlogged` from the first tick, their
    `next_offset` never moves and the member's ingest counters stay flat (a plain consumer is
    invisible to `kafka-consumer-groups.sh`, so the member's own counters are the evidence), the
    console shows *Live tail starts once the runtime catches up* with `behind` falling as the runtime
    commits; near the end of the backfill the table goes `catching_up` then `live` without
    intervention, and check 2 holds from then on. While it is backlogged, open the peek: produce a
    probe record at the head and see it in the panel within seconds, shaped as the table's columns,
    and confirm it is in no run's count and no statement. Delete the materialization after (13 §13.8).
12. **Bursts.** On `fresh.orders` produce 50,000 records in ten seconds: the table pauses
    (`truncated`, `behind` shown), the member's other tables keep `X-Tail-Lag-Ms` low, and the tail
    resumes within a few commit intervals; then 1,000,000 records in a minute: the table goes
    `backlogged` (buffer dropped), the console says so, and it comes back `live` once the runtime has
    drained the burst; check 2 holds before, during and after.
13. **Parking and activation.** Leave a table unwatched for eleven minutes: the health view shows it
    `parked` and the member's assignment no longer lists its partitions; open its Data tab: the
    first run's tail arrives within the wait (record the time), the table is `live` within seconds,
    and check 1's budget holds from the second refresh on. Repeat with `live_tail_idle_seconds` 0:
    the table never parks.
14. **Broker restart.** While the probe of step 1 runs, roll one Kafka broker (a Strimzi manual
    rolling update of that pod — ask first): the tail's `lag_ms` rises for the affected partitions
    and returns to zero without a member restart; check 2 holds; no `tail_routing_mismatch`.
15. **Control plane restart.** `rollout restart` the control plane while the probe runs: tails keep
    serving on cached sessions through it; a table activated during it seeds from GCS (the fallback
    is logged); no `tail_rebalancing`; check 2 holds; ticks resume within a minute.
16. **Member killed mid-burst.** During step 12's first burst, delete the owner's pod: on return it
    re-seeds the table from the committed offsets and reports `catching_up`, then `paused` or
    `live`; the console shows the notices in that order; check 2 holds; no duplicate across the kill.
17. **Console restart.** Restart the BFF with two Live tabs open: the tabs re-run their views and
    Live resumes; the tail app logs nothing unusual (abandoned run directories go with the process).
18. **Soak.** Leave the step-1 probe and two Live tabs running for an hour with the 50 tables of
    step 10 watched in rotation (a script opening and closing their Data tabs): per-member RSS flat
    within 10 % after the first ten minutes (no leak from segments, caches or the member map),
    `tail_served` p95 ≤ 300 ms, check 2 at every poll.
19. **Security probes** (from the laptop, through the HTTPRoute): the tail endpoint with no token,
    an expired session, a token limited to another dataset, the service token, a forged
    `X-Tail-Forwarded-By`, a `rows=1000000` peek and a `..`-bearing table name — each refused as §12
    says, and no log line naming a row.

Record the results in 13 §13.8e and web 14 §14.4, as earlier releases did.

### 13.5 Failure-mode matrix

Every row of §10, and where it is tested. The implementer keeps this table true as the code lands.

| §10 situation | Python | Console | Lab step |
|---|---|---|---|
| Tail app down, unreachable, URL wrong | — | `runs/tail.test.ts` (first fetch times out → committed-only, `unavailable`) | 5, 17 |
| A member's pod restarts | `test_tail_app` (restart re-seeds, same prefix) | — | 5 |
| Member lost / added / removed (rebalance) | `test_tail_routing` (moves, maps), `test_tail_exactly_once` (moves) | `data/tail.test.ts` (`rebalancing` reason) | 5, 16 |
| Member map stale | `test_tail_routing` | — | 0, 5 |
| Partitioner disagreement | `test_tail_routing` (`tail_routing_mismatch`) | — | 0 |
| Just opted in / seeded / moved | `test_tail_app` (`not_ready`), `test_tail_failures` (`wait_ms` deadline) | `data/tail.test.ts` | 13 |
| Kafka unreachable from a member | `test_tail_failures` | — | 14 |
| Control plane unreachable from the tail app | `test_tail_failures` | — | 15 |
| Source topic deleted while tailed | `test_tail_failures` | — | not run on the lab (destructive) |
| A burst outgrows the bound | `test_tail_buffer`, `test_tail_app`, `test_tail_exactly_once` | `data/tail.test.ts` (`truncated`) | 7, 12 |
| A backlog | `test_tail_app`, `test_tail_exactly_once` | `data/tail.test.ts` (`backlogged`) | 11, 12 |
| One table above the ceiling | `test_tail_app` | — | 12 |
| Member memory budget reached | `test_tail_buffer` | — | 10 |
| Pod at its memory limit | — | — | 10 (observed), 16 |
| Nobody asked for `idle_seconds` (parked) | `test_tail_app`, `test_tail_exactly_once` | — | 13 |
| First request for a parked table | `test_tail_app` (`wait_ms`) | `data/tail.test.ts` | 13 |
| Snapshot older than retention | `test_tail_buffer` (`unavailable`), `test_tail_exactly_once` | `runs/tail.test.ts` (re-pin) | 2 (a run left paging) |
| As of an older snapshot | — | `runs/tail.test.ts` | 6 |
| Materialization stopped | `test_live_tail_scheduler`, `test_web` | — | 9 |
| Schema change in flight | `test_tail_parity`, `test_tail_app` | — | 6 |
| Missed `schema_changed` / `table_committed` | `test_tail_app` (hints reconcile) | — | 15 |
| Partition count grew | `test_tail_app` | — | 10 |
| Table dropped / deleted / opt-out / switch off | `test_live_tail_scheduler`, `test_tail_app` (cancel, forget) | `runs/tail.test.ts` (`off` mid-live) | 9 |
| Console restart | — | — | 17 |
| Tail Parquet unreadable or truncated | — | `data/tail.test.ts`, `runs/tail.test.ts` | — |
| A record Kaspr cannot decode | `test_tail_app` | — | — |
| Offsets below Kafka's beginning | `test_tail_buffer` (`gap`) | `data/tail.test.ts` (`X-Tail-Gap`) | — |
| Forwarding failures (owner unreachable, loops, stale owner) | `test_tail_routing`, `test_tail_failures` | — | 5 |
| Authorization and forged headers | `test_tail_security` | `routes/data.test.ts` | 8, 19 |
| Leaks under load | — | — | 18 |

### 13.6 Measurements with pass thresholds (phase 2 exit)

On the lab's record shapes (`materializer-loadtest-orders`, ~1.4 KB staged), measured and recorded
in handbook 19 before phase 3 starts; a miss re-opens §9 and §11.1:

| Measurement | Threshold |
|---|---|
| Ingest cost per record (`build_row` + Arrow, one core) | ≤ 120 µs (≥ 8,000 records/s per core) |
| Resident bytes per raw byte buffered (Arrow + compressed raw) | ≤ 1.3 |
| Serve: 10,000 rows → Parquet | ≤ 200 ms p95 |
| Serve: 100,000 rows → Parquet | ≤ 1.5 s p95 |
| Activation of a quiet table (schema fetch, assign, window read) | ≤ 2 s |
| Re-assign hiccup for a member's other tables | < 1 s of added `lag_ms` |
| Member map refresh (`describe_consumer_groups` + decode) | ≤ 50 ms |
| Head peek, 200 rows, 3 partitions | ≤ 300 ms p95 |
| Produce → visible, Data tab Live, step 1 | p95 ≤ 5 s (R1); the §9 table re-derived from the measured rows |

### 13.7 Definition of done, per phase

- **Phase 0:** golden equality of the seal's output; the regenerated fixture differs only in ids;
  the full Python suite green.
- **Phase 1:** the §13.1 rows for the control plane green; `kubectl kustomize` of every overlay;
  `ShardRoutingTests`; the full Python suite green.
- **Phase 2:** every Python row of §13.5 green, `test_tail_exactly_once.py` included; the §13.6
  thresholds met on the lab; lab steps 0–19 run and recorded; the Kaspr confirmations (§15 phase 2,
  items a–c) answered in handbook 19.
- **Phase 3:** the console rows of §13.5 green; `npm run typecheck && npm test && npm run build`;
  the Playwright pass of §13.3, failure states included; the fixture parity test.
- **Phase 4:** 13 §13.8e and web 14 §14.4 written from the lab results; handbook 19 and the touched
  documents updated; versions bumped; nothing in this plan's §17 left undecided.

---

## 14. Rollout order

1. **Phase 0** (row builder) ships first as a patch of the current release: no behaviour change,
   proven by golden-file equality. Deploy; nothing to validate on the lab beyond a commit.
2. **Phase 2** (the tail app) deployed with no table ticked: it starts, follows table events, serves
   health and members. Check its Kafka user can read the source topics and
   `materialization-table-events`, and that it reaches the control plane's schema and session views.
3. **Phase 1** (control plane) with `live_tail_default` on: new materializations are ticked from now
   on; existing ones stay off until edited (`PATCH live_tail: true`). The control plane only *sends*
   to the new topic, so no forced consumer-group rebalance is needed.
4. **Phase 3** (console) with `CONSOLE_TAIL_URL` set (or the ingress rule); the feature is invisible
   until then even though tables are tailed.
5. Run §13.4 on the lab; fix; then document for users.
6. **Phase 4** docs and the release (§15).

Rolling back any phase is independent: the console works without the tail app (it degrades to
committed rows), the tail app without ticks (it tails nothing), the control plane field is ignored by
an older console.

---

## 15. Implementation phases, mapped to files

Each phase ends with the repo's checks: `python -m unittest discover -s python/tests -p 'test_*.py'`,
`python -m compileall python/src`, `kubectl kustomize manifests/base` and `manifests/dev`, and in `web/`
`npm run typecheck && npm test && npm run build`.

### Phase 0 — the shared row builder

| Change | Files |
|---|---|
| New `build_row` / `BuiltRow` | `python/src/materializer_control_plane/lakehouse/rows.py` (new) |
| `run_seal` uses it; nothing else changes | `python/src/materializer_control_plane/runtime_app/writer/seal.py` |
| Tests | `python/tests/test_row_builder.py` (new), `python/tests/test_runtime_seal.py` (golden equality), regenerate `web/testdata/iceberg` with `python/tests/console_fixture.py` and confirm `git diff` shows only ids/UUIDs (expected per web 14 §14.2) |

### Phase 1 — the control plane

| Change | Files |
|---|---|
| `live_tail` on the model, spec and request event | `models/materialization.py`, `models/events.py` |
| Create resolves `live_tail_default`; update carries `live_tail` like `label`; `MUTABLE_SETTINGS` | `web/materializations.py` |
| Persist on create; apply on update | `services/request_intake.py` |
| Detail and list payloads | `web/responses.py`, `storage/materializations.py` (`list_materializations` item shape) |
| Settings: config fields + env parsing; catalog entries | `config.py`, `settings/catalog.py` |
| Tick messages and the scheduler | `models/live_tail.py` (new), `models/__init__.py`, `services/live_tail.py` (new), `handlers/maintenance.py` (yield `tail` outputs from `schedule_table_maintenance`) |
| Manifests: the `maintenance` agent's new output (64 partitions); overlay patch | `manifests/base/agents/maintenance.yaml`, `manifests/dev/kustomization.yaml` (and any other name-prefixing overlay) |
| Tests | `test_web.py`, `test_request_intake.py`, `test_models.py`, `test_settings.py`, `test_runtime_settings.py`, `test_maintenance.py`, `test_live_tail_scheduler.py` (new), `test_agent_manifests.py` |
| Docs | handbook 04 (§4.5, §4.6, §4.8.1), 05 (§5.4, §5.9), 06 (§6.1 inventory, new §6.18), 07 (§7.13), 17 (§17.3) |

### Phase 2 — the tail app

| Change | Files |
|---|---|
| Package | `python/src/materializer_control_plane/tail_app/__init__.py`, `config.py`, `buffer.py` (pure), `consumer.py` (the member's one source consumer + fake seam), `events.py` (the table-events follower), `table_tail.py` (one table: activation and parking, seed, ingest, serve), `routing.py` (owner partition, member map, forwarding), `serve.py` (Parquet), `peek.py` (the head peek: its dedicated consumer, the buffer path, the cache), `auth.py` (session check, `authorize_tail`), `control_plane.py` (schema fetch, session fetch, with the service token), `worker.py` (tables by key, ticks, forget-after, budget), `handlers.py` (thin: `handle_tail_message`, `advance_tail`, `tail_rows_view`, `tail_health_view`, `tail_members_view`, `tail_head_view`) — all new |
| `principal_from_description` | `identity/principals.py` |
| `describe_consumer_group` | `kafka/admin.py` (decode assignments and Faust `ClientMetadata` from member metadata) |
| Manifests | `manifests/base/tail/app.yaml` (replicas 2, resources, env incl. `MATERIALIZER_TAIL_SERVICE_TOKEN` from the `materializer-auth` Secret), `tail-worker.yaml` (agent on `materializer-tail-requests`, `partitions: 64`, `declare: true`), `tail-step.yaml` (task every 5 s: forget stale tables, trim, refresh the member map, refresh end offsets and re-evaluate the `backlogged`/`catching_up`/`paused` states), `manifests/base/webviews/tail-rows.yaml`, `tail-health.yaml`, `tail-members.yaml`, `tail-head.yaml` (labelled `kaspr.io/app: materializer-tail`); `manifests/base/kustomization.yaml`; `manifests/dev/kustomization.yaml` (labels by selector, topic names, `MATERIALIZER_TAIL_*` env, the Secret name, package ref) |
| Tests | `python/tests/tail_support.py`, `test_tail_buffer.py`, `test_tail_routing.py`, `test_tail_app.py`, `test_tail_parity.py` (new), `test_kafka_admin.py`, `test_agent_manifests.py`, `test_package_layout.py` (the new subpackage imports) |
| Docs | handbook 02 (§2.10b the tail app), 03 (§3.4 constants), 04 (§4.1: the `/tail/v1` endpoints are the console's alone, documented in 19, not part of the public API), 10 (a section pointing to the new doc), new `docs/handbook/19-live-tail.md` (the model, the contracts, ownership and routing, exactly once, capacity and sizing, operating it), 12 (runbook entries: health and members views, pauses, re-seeds, moves, sizing in §12.10), 14 (limits), README table; `AGENTS.md` project map (`tail_app/`) |
| **Verification items, first thing** | (a) Kaspr's key serializer for the control plane's `topicSend` keys is `raw` and the producer's partitioner is aiokafka's default: produce one tick and compare its partition with the owner computation (the self-check does this continuously). (b) `describe_consumer_groups` in the installed aiokafka returns member metadata and assignments in a form the decoder handles, and the Faust `ClientMetadata` decompresses with `PartitionAssignor._decompress`; if not, use the FQDN fallback (the pod name from the static member's instance id and this member's own `K_WEB_HOST` suffix). (c) How Kaspr deserializes a `json` agent input and what it does with a value that is not UTF-8 JSON: nothing in `runtime_app/` produces `RuntimeMalformedRecord` today (only `state.py` consumes it), so the writer never sees such a value — mirror that outcome (no row; position advances). (d) Measure the row builder's cost per record and the resident-bytes-per-raw factor on the lab's record shapes, and the re-assign hiccup, to replace §9's and §11.1's estimates |
| **Kaspr change, in scope and gated** | an error content type per WebView in `kaspr/types/models/webview/response.py` (`build_error` taking an error content type, the view's by default), so the tail's errors are `application/json`. When made: **pause and ask the owner to deploy it to the kaspr-operator and the lab** before any further testing, so everything runs on a Kaspr that has it; until then the JSON-in-octet-stream fallback stands and the console parses both. No other Kaspr change is in scope: if confirmation (b) fails, the FQDN fallback is the answer |

### Phase 3 — the console

| Change | Files |
|---|---|
| Config and settings | `web/server/config.ts` (`tailUrl` fixed; four tunables), `web/server/settings/catalog.ts`, `web/server/routes/settings.ts` (deployment entry), `web/src/features/settings/sections.ts` |
| Tail client | `web/server/data/tail.ts` (new: `fetchTail`, `fetchHead`; the peek's rows shaped through the logical view with DuckDB) |
| Runs: work dir for every run, tail objects, incremental refresh, `tail` + `revision` in snapshots | `web/server/runs/run.ts`, `browse.ts`, `statement.ts`, `store.ts` (`startRun` tail options; `refreshRun`) |
| Routes | `web/server/routes/data.ts` (`liveTail` in `runSchema`; `POST /api/runs/:runId/refresh`; `GET /api/materializations/:id/tables/:table/head`), `web/server/routes/system.ts` (`/api/config`), `web/server/routes/materializations.ts` (`live_tail` in both schemas) |
| Types and client | `web/shared/types.ts` (`CpMaterialization.live_tail`, `RunRequest.liveTail`, `RunTail`, `RunSnapshot.tail/revision`, `ConsoleConfig.liveTail`, `CpSystemSettings.live_tail_default`, `HeadPeek`), `web/server/controlPlane/client.ts` (`live_tail` on create/update), `web/src/lib/api.ts` (`refreshRun`, `live_tail`) |
| UI | `web/src/features/data/useRun.ts` (refresh, revision in keys, the live timer), `DataTab.tsx` (the Live toggle for the browse view, visibility, the statement's *Auto re-run*), `SqlPanel.tsx` (the toggle's place), `timeRange.ts` (open-ended relative ranges under Live), `StatusBar.tsx` (tail states, the peek button), `HeadPeek.tsx` (new: the panel), `viewState.ts` (`live`), `web/src/features/materializations/CreateMaterializationDialog.tsx` (toggle pre-checked from the default), `web/src/features/detail/EditMaterializationDialog.tsx`, `OverviewTab.tsx` |
| Tests | `web/server/data/tail.test.ts`, `web/server/runs/tail.test.ts` (new), `routes/materializations.test.ts`, `settings/catalog.test.ts`, `routes/system.test.ts`, `src/features/data/hooks.test.ts`; fixture additions in `python/tests/console_fixture.py` and `web/server/iceberg/iceberg.test.ts` |
| Docs | web 01, 03 (§3.10), 06, 07 (a section on tail objects and incremental refresh), 08 (§8.4, §8.16, new §8.17a), 11 (§11.3 create dialog, §11.4 edit, §11.8 Live), 12 (§12.2 the tail call), 14, 16, 17; handbook 18 §18.9 (the tail call carries the person's session) |

### Phase 4 — lab validation, docs, release

§13.4; 13 §13.8e and web 14 §14.4 entries; `pyproject.toml` and `web/package.json` bumped (§17 D11);
`docs/handbook/README.md` reading paths ("Debugging a tail that is behind, moving or unavailable").

---

## 16. Verified against the source

Facts this plan relies on, and where each was checked:

- `materializer.offsets` is the inclusive last offset per partition such that every record at or
  below it is in the table or needs no row; `committed_offsets` walks back past snapshots without it;
  `advanced` only raises — `lakehouse/offsets.py`.
- `plan_append` accepts only a seal continuing from the cursor and moves the cursor to its
  `offset_end`; covered and overlapping seals are refused — `lakehouse/commits.py` 351–414.
- Compaction's `replace` carries offsets forward and checks outputs equal inputs per partition —
  `lakehouse/maintenance.py` (handbook 10 §10.10, 11 §11.6).
- The seal's row construction and its inputs — `runtime_app/writer/seal.py` 142–209; the staged line —
  `writer/batches.py::staged_line`; key, header and timestamp helpers — `writer/context.py`; the
  `Conformer` — `lakehouse/schema/conform.py`; the Arrow schema and Parquet writer — `lakehouse/parquet.py`.
- The writer's handling of tombstones and the replay guard — `runtime_app/handlers/writer.py` 124–143.
- Batch roll and commit defaults — `runtime_app/config.py`; commit spacing and attempts —
  `runtime_app/committer.py` 91–96, `_append_due`.
- Commit cost 2,003 ms + 18.5 ms per file, schema evolution 6.3 s, ~1,400 records/s per runtime
  member — handbook 13 §13.8.
- The control plane's schema view answers the Iceberg schema, fork map, rejected paths and last
  column id with `?raw=true` and requires `dataset.view_schema`, which the service principal holds —
  `web/materializations.py::get_materialization_schema_view`, `models/materialization.py::MaterializationSchema`,
  `identity/permissions.py::SERVICE_PERMISSIONS`; the table record's `offsets` come from
  `table_committed` events — `services/table_events.py::_apply_table_committed`.
- The console reads `materializer.offsets` per snapshot (`iceberg/snapshot.ts::committedOffsets`),
  orders browse objects by `timeMax` (`runs/browse.ts::orderObjects`), passes local paths through
  (`data/objects.ts::objectSources`), binds files by field id (`data/logical.ts::readParquet`), pins
  runs to one snapshot (`runs/store.ts::startRun`), sandboxes statements to the run's directory
  (`runs/statement.ts`, `data/duckdb.ts::withSandbox`), checks `data.query` per dataset
  (`routes/data.ts::requireOnTables`, `ownRun`), forwards the person's token on their requests
  (`controlPlane/client.ts::headersFor`, `auth/guard.ts::callerOf`), and checks sessions with the
  control plane cached by token hash (`auth/principal.ts`).
- Pointer freshness 2 s / 5 s / 5 min — `web/server/config.ts`.
- The maintenance tick pattern: `services/table_maintenance.py`, `handlers/maintenance.py`,
  `manifests/base/agents/maintenance.yaml`, `manifests/base/tasks/schedule-table-maintenance.yaml`;
  the maintenance app's shape — `maintenance_app/`, `manifests/base/maintenance/`, the dev overlay's
  label and topic patches.
- `authorize`, `Principal.can`, `Principal.describe`, `Grants.dataset_limit` — `identity/authorization.py`,
  `identity/principals.py`.
- The update path for a setting the runtime knows nothing of (`label`) —
  `web/materializations.py::update_materialization`, `services/request_intake.py::_apply_settings_update`;
  a default resolved at create — `_normalize_request_spec` (`partition_granularity`).
- Kafka clients built from `K_KAFKA_*` — `kafka/metadata.py::from_environment`,
  `schema/discovery.py` (partitions from cluster metadata, never by subscribing; small fetches); the
  admin client — `kafka/admin.py`.
- Kaspr: default key serializer `raw` — `kaspr/types/settings.py` (`KEY_SERIALIZER`); `web_host` and
  `canonical_url` settings — `kaspr/types/settings.py`; WebViews with one content type per view,
  `application/octet-stream` through `web.bytes` with `headersSelector`/`statusCodeSelector` —
  `kaspr/types/models/webview/response.py`; a view reads query parameters as
  `manifests/base/webviews/topics-list.yaml` does.
- The operator: a headless Service per app, pods addressed as `<app>-<i>.<headless>.<ns>.svc.cluster.local`
  (`kasprapp.py::prepare_fqdn`), `web_host` set to that FQDN on the container (~1931), `<app>-app-api`
  on `webPort` (`kasprapp_resources.py`) — `kaspr-operator/kaspr/resources/`.
- Faust: the producer partitions with `producer_partitioner or DefaultPartitioner()` (murmur2) —
  `faust/transport/drivers/aiokafka.py:310`; consumer isolation `read_uncommitted` unless
  transactional — 350–353; the assignor's member metadata carries each member's `url`
  (`canonical_url`) — `faust/assignor/partition_assignor.py::_metadata`,
  `client_assignment.py::ClientMetadata`; `Router.key_store` covers only changelog topics
  (`_tps_url` from `changelog_distribution`) — `partition_assignor.py` 95–101, 395.
- `RuntimeMalformedRecord` has no producer in `runtime_app/` (grep): undecodable values never reach
  the writer (phase 2 verification item c).

---

## 17. Decisions, and what remains open

Decided by the owner on 2026-10-23:

| # | Decision |
|---|---|
| D1 | `_materialized_at` in tail rows is the tail's own read time, documented as the one column that differs from the committed row |
| D2 | Exports and profiles of a run include its tail rows; the `tail` block says how many; exports carry `x-export-includes-tail` |
| D3 | A stopped materialization is not tailed (`desired_phase != running` → no ticks → `404 live_tail_off`) |
| D4 | **Live tail is on by default for new materializations** (`live_tail_default`, a control-plane setting defaulting to true), switchable per materialization (`PATCH live_tail`) and for the deployment (`live_tail_enabled`). Existing materializations keep what they had (off) until edited — no migration flips them |
| D5 | Live mode is on by default on a tail-enabled materialization, with a visible toggle kept in the address bar; it pauses while the tab is hidden |
| D6 | Rows during a schema change are served at once under the current schema and counted as settling; nothing is held |
| D7 | Bounds and sizing as §6.5 and §11.1: 250,000 rows / 256 MiB per table, 120 s retention, a 5,000 records/s ceiling per table, 2.5 GiB per member, members at requests 1 CPU / 2 GiB, limits 2 CPU / 4 GiB; **two members** for the owner's fleet of ≤ 500 topics at 1–50 records/s with tables tailed while watched (three if every table is kept always on) |
| D8 | **The tail is a KasprApp of its own, `materializer-tail`** (§3.5); the child-process and in-control-plane hostings are rejected |
| D9 | The tail app reads `materialization-table-events` itself |
| D10 | Authorization is the control plane's rule, rebuilt from `GET /auth/session` with the person's token, checked by the receiving member and again by the owner |
| D11 | Version 4.1.0 for the control plane and the console (additive, 16 §16.3) |
| D12 | **Scaling is built in:** ownership by tick partition, request forwarding between members, 64 tick partitions; members by §11.1's rule. Chosen over replication (every member tailing any watched table) so capacity grows with members from the first release (§3.5) |
| D13 | Exposing the tail beyond the console (REST catalog engines) is out of scope |
| D14 | **The tail does not apply during a backlog.** A table whose runtime is more than the bound behind the topic's head is `backlogged`, reads nothing, and goes live by itself once the runtime is within 75 % of the bound (a burst enters the state at twice the bound). A new materialization's backfill therefore costs the tail nothing, and the console shows the lag over the committed rows |
| D15 | A per-table ingest ceiling (`live_tail_max_records_per_second_per_table`, 5,000) keeps one topic's burst from slowing the member's other tables; a single table sustained above one member's ingest is a stated limit |
| D16 | **The head peek:** while a table is backlogged (or paused far behind) the console offers the newest N records on the topic as they will look in the table, answered by the owner on demand and shown apart from the table — never in a run, a count, a statement or an export |
| D17 | **Tables are tailed only while watched:** the first request activates a table and waits up to 2.5 s for its window; `live_tail_idle_seconds` (600) without a request parks it; `0` keeps every opted-in table on |
| D18 | **Nothing is persisted:** a member's state is in memory and rebuilt from the ticks, the control plane, the table events and Kafka; a restart costs seconds of `tail_not_ready` for the tables being watched |
| D19 | **Live applies to the browse view.** A statement gets its tail once per run; the SQL panel's *Auto re-run* (off by default) starts a new run 5 s after the previous one finished |
| D20 | **Relative ranges slide under Live:** the end stays open so appended tail rows are never pruned, the start is re-resolved on each new run; an absolute range with a past end polls nothing |
| D21 | **The tail endpoint is the console's alone:** every request carries the console's service credential in `X-Materializer-Console` besides the person's bearer (`403 console_only` otherwise); not a public API, documented as console-internal. The console runs in-cluster in every deployment (`CONSOLE_TAIL_URL` is the Service); the lab's HTTPRoute `/tail/*` serves development and testing from a laptop |
| D22 | **Kaspr changes:** in scope for one thing only, an error content type per WebView; when made, the implementer pauses and asks the owner to deploy it to the kaspr-operator and the lab before further testing. No other Kaspr change: the routing confirmations fall back to the FQDN scheme if needed |
| D23 | `live_tail_idle_seconds` stays deployment-wide; no per-materialization always-on mode in this release |
| D24 | The fleet is up to 100 materializations and 500 topics **per control plane**; §11.1 is sized for that |

Still open — none blocks the build:

- **The quiet-topic lever.** Quiet topics hold ten minutes of records because of the runtime's
  600 s batch ceiling; lowering it shrinks the tail's memory five-fold on them at the cost of more
  small files. Not needed for the owner's fleet, whose tables are tailed only while watched; worth
  revisiting if many tables are kept always on or the records grow.
- **Kaspr-side confirmations** (phase 2, items a–b): the key bytes and partitioner, and the group
  description's member metadata. Both are believed present; the FQDN fallback covers (b) if not.
