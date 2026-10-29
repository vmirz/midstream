# 10 — Runtime Internals

Each materialization runs in a dedicated runtime: a KasprApp of `replicas` members — more while
autoscaling has it catching up (07 §7.12a) — that read the materialization's source topics, stage records, seal them into Parquet, upload the files, and commit
them to the materialization's Iceberg tables — one per topic. Exactly one member — the **committer** —
writes to the tables; every member writes files. This document follows a record from its topic into
a committed snapshot of its table, and covers acknowledgement, sessions, backpressure, the
committer and the heartbeat. Table maintenance - compaction, snapshot expiry, orphan cleanup - is not
the runtime's: the maintenance app, a KasprApp of its own, does it for every materialization (§10.10).

**More topics, the same work at once.** A runtime reading ten topics seals and uploads no more at a
time than one reading one: the seal and upload workers, the upload backlog and its backpressure are
the member's, shared by every table (§10.4); and the committer commits one table at a time (§10.6).
Only the work scales with the topics, never the concurrency.

Source: `runtime_app/handlers/writer.py`, `runtime_app/writer/` (batches, seal, seal worker,
upload, context), `runtime_app/pipeline.py`, `runtime_app/outbox.py`, `runtime_app/committer.py`,
`runtime_app/acks.py`, `runtime_app/state.py`, `runtime_app/table_state.py`, `runtime_app/flow.py`,
`runtime_app/handlers/status.py`, `runtime_app/handlers/runtime_settings.py`,
`runtime_app/live_settings.py`, `runtime_app/templates/*.yaml.j2`, `lakehouse/commits.py`; the
maintenance app: `maintenance_app/`, `services/table_maintenance.py`, `lakehouse/maintenance.py`.

## 10.1 Anatomy

`runtime/templates.py` renders these resources from `runtime_app/templates/` (`<rt>` is the runtime
app name, e.g. `midstream-mat-a0690b63eb78`):

| Resource | Template | Runs on | Does |
|---|---|---|---|
| KasprApp `<rt>` | `kaspr-app.yaml.j2` | — | the members; spec from `CONTROL_PLANE_RUNTIME_APP_SPEC` plus `replicas` |
| KasprTable `<rt>-state` | `state-table.yaml.j2` | every member (global) | each table's schema, committed offsets, file decisions, totals; live settings (§10.8) |
| KasprAgent `<rt>-writer` | `writer-agent.yaml.j2` | owners of source partitions | reads every source topic (`input.topic.name: a,b,c`) and stages each record in its topic's table (§10.2); `isolatedPartitions: true` (one actor per topic partition), `noack: true` |
| KasprAgent `<rt>-committer` | `committer-agent.yaml.j2` | the owner of `<rt>-commits` (1 partition) | commits files and schema changes (§10.6) |
| KasprAgent `<rt>-settings` | `settings-agent.yaml.j2` | one member | receives live settings (§10.12) |
| KasprTask `<rt>-pipeline` | `pipeline-task.yaml.j2` | every member, every `upload_interval` (5 s) | seals, uploads, announces, releases acknowledgements (§10.4) |
| KasprTask `<rt>-emit-status` | `status-task.yaml.j2` | every member, every `heartbeat_interval` (30 s) | the heartbeat (§10.11) |
| KasprTask `<rt>-commit-timer` | `commit-timer-task.yaml.j2` | the leader, every `commit_timer_interval` (5 s) | `commit_tick` on `<rt>-commits` |

A runtime before 2.0 also had a `<rt>-maintenance-timer` task; every apply deletes it if it is there
(`runtime/naming.py::retired_runtime_components`), as does the runtime's delete.

Topics a runtime uses: the source topics; `<rt>-commits` (declared, 1 partition: file
announcements, schema change requests, ticks); `materialization-status` (heartbeats);
`materialization-table-events` (what the committer did, for the control plane);
`materialization-schema-authority-updates` (live settings from the control plane — the name predates
1.0); the state table's changelog.

The template input carries `source_topics`, `tables` (`[{name, source_topic, dataset, location,
table_uuid}]`, in the topics' order — the tables the runtime writes, never the kept ones; each table's
`dataset` is its Iceberg namespace, so one runtime writes tables of several datasets),
`partition_granularity` and the package revision; the agents' init blocks build the table contexts
once from it (`table_state.table_contexts`). Nothing in the runtime depends on a dataset beyond the
table's identifier: it writes and commits by location. Neither a schema nor any setting
is in it, so the spec hash — and with it the runtime — changes only when the deployment, the replica
count (the autoscaled one while it is scaled), the topics or the package does (07 §7.7). The runtime is rendered only once every table
exists, and with no members at all while its topics change (07 §7.9a).

Faust gives partition `i` of every source topic with the same partition count to the same member
(co-partitioning), so the members a runtime keeps busy are the *distinct* partition counts summed:
topics of 32, 32 and 8 partitions keep at most 40 members busy, not 72 (`scale.assignable_members`;
the create dialog's and the topic analysis's `combined` recommendation account for it).

## 10.2 The writer agent: stage, never wait

`stage_source_record` (`handlers/writer.py`), per record, on the event loop:

0. **Table**: the record's topic names its table (`contexts_by_topic`, cached). A record of a topic
   the runtime does not read is logged and acknowledged without a row. Everything below — sessions,
   batches, acknowledgements, schema requests — is per table and partition: partition 0 of two
   topics is two partitions here.
1. **Session**: note the rebalance generation the record arrived in, and the table's committed
   offset for its partition — read from the table when the partition's session starts (§10.5).
2. **Replay guard**: a record at or below the table's committed offset is already in the table; it
   is acknowledged without being written.
3. **Drift**: the record is checked against the current schema's index (`SchemaView`); whatever the
   schema cannot take yet becomes a `schema_change_request` for the committer, through the outbox
   (09 §9.7). **The record is not held.**
4. **Stage**: the record is appended as it arrived — one JSON line in its partition's open batch —
   and the batch rolls when the roll policy says so (§10.3).
5. **Acknowledge** through the ledger (§10.7): only once a commit covers its batch.

A staged line is `{"m": {"p", "o", "t", "k", "h", "a"}, "v": <value>}`: partition, offset, Kafka
timestamp (ms), key and headers (UTF-8, or `b64:` + base64 when not UTF-8), the time the record was
read, then the value untouched. Nothing is normalized per record: conforming values to the schema is
the seal's work, in a child process, against the schema current at seal time.

Tombstones (null values) are rows with a null payload unless the materialization has
`skip_tombstones`, in which case they are acknowledged without a row. A value that is not an object is
kept whole in `_unmapped` under `$` (09 §9.8).

## 10.3 Batches and the roll policy

Each table partition has one open batch at a time under `staging_root_path`
(`/tmp/materializer/staging/<materialization>/<table>/p<partition>/`, in the container's own
filesystem on the node's disk, not on the member's persistent volume, so it goes with the container
(§10.5); the batch id is `batch-<member>-<table>-p<partition>-o<offset>`). `should_roll_batch`
(`writer/batches.py`) closes it when:

| Condition | Default |
|---|---|
| `row_count >= max_rows_per_file` | 250,000 |
| `estimated_bytes >= target_file_size_bytes` | 256 MiB |
| age `>= max_open_file_age_ceiling_seconds` | 600 s — a quiet topic's row is visible within ten minutes |
| age `>= max_open_file_age_seconds` **and** (`row_count >= min_rows_per_file` or `estimated_bytes >= min_file_size_bytes`) | 120 s, 10,000 rows, 8 MiB |

A schema change deliberately does not roll a batch. A batch whose acknowledgements are *ordered*
(§10.7) rolls early, at 5,000 rows.

## 10.4 The batch pipeline: seal, upload, announce

`BatchPipeline` (`pipeline.py`) does everything slow off the event loop. The pipeline task (every
5 s) and the writer (on every roll) call `schedule`, which starts whatever is due and returns at once.
It is the member's, shared by every table: `seal_worker_count` seals and `upload_worker_count`
uploads run at a time whatever the number of tables, taken oldest roll first across them (then by
table and offset), and one upload per table partition at a time keeps announcements in offset order.

**Seal** (`writer/seal.py`, in a child process `writer/seal_worker.py`, `seal_worker_count` 1 at a
time, killed after `SEAL_TIMEOUT_SECONDS` 1,800), in two passes over the batch's JSONL. First, each
line goes to a spool file for its partition value — the UTC hour, day or month of its Kafka
timestamp, lines without one to `__null__` — in its order; for a re-seal, only the lines after an
offset. Only the timestamp is read here. Then, one partition value at a time, each spooled line is
turned into a row — every value conformed (09 §9.5, `lakehouse/schema/conform.py`) beside the Kafka
metadata columns, by `lakehouse/rows.py::build_row`, which the live tail shares so that the rows it
serves are the rows the seal writes (19) — and written to that value's zstd Parquet file, field ids in
its schema and statistics on, a row group at a time: about `target_row_group_size_bytes` (32 MiB) of
staged JSON each. With one file open at a time, the child's memory is bounded by one row group
whatever the batch's size or spread: measured at about 300 MiB for 50,000 rows of 1.7 KB in one
month (three row groups) and less when they are spread — 154 MiB over ten months, 267 MiB for
250,000 rows over two years. Written as the rows arrived, with every partition's file open at once
and each flushing on its own, a batch spread over many months held every row until the end: the
same 50,000 rows over ten months took 523 MiB, the 250,000 over two years 2.1 GiB, which is what
killed 1 GiB runtime pods during the load test's backfill (12 §12.10). The spools are deleted as
each file closes, or when a seal fails. A batch holding values the schema cannot take yet waits up
to `seal_schema_wait_seconds` for an evolution first (09 §9.7).

**Upload** (`writer/upload.py`, `upload_worker_count` 4 threads): each file goes to its final object,
create-only (`if_generation_match=0`). File names carry the seal's id
(`L/data/ts_day=2026-10-18/p03-o0000000100-0000000199-<member>-<seal>-<n>.parquet`), so an object
already there is this very file from an earlier attempt, and a matching size makes the upload done.
Failures back off from `upload_initial_backoff_seconds` (5 s) to 300 s and never give up.

**Announce**: each uploaded file becomes a `data_file_ready` message in the outbox
(`outbox.py`), keyed so a newer message for the same file replaces an older one. The pipeline task's
`topicSend` drains the outbox to `<rt>-commits` with `ack: true`: a message leaves the outbox only
once the broker has it. A seal's files are announced together, with the seal's `after_offset`,
`offset_start`, `offset_end`, partition, member, session generation and session epoch (§10.5). Files
no commit has covered are announced again every `announce_retry_seconds` (60 s) — which is what lets
a new committer rebuild its pending set from nothing.

**Release**: once its table's committed offsets (state table key `committed-offsets:<table>`) cover
a batch, its held acknowledgements are released and its staged and sealed files deleted.

**Decisions**: the committer's decisions (state table key `file-decisions`, §10.6) are applied here:
`overlap` re-seals the batch's tail past the committed offset (`reseal_tail`) and announces that
instead; `covered` and `error` drop the batch's announcement.

**Backpressure** (`flow.py`): one backlog for the member, every table's pending batches counted
together. While it is at `max_pending_upload_batches` (32) or `max_pending_upload_bytes` (2 GiB), the
member's partitions of **every** source topic are paused — the topics it has had no record of yet
included, since the writer registers every table with its first record (`register_tables`) — and an
open batch that finds the backlog full keeps filling rather than rolling. They resume once the backlog
is down to `BACKLOG_RESUME_FRACTION` (0.75) of both limits. Each reason holds the pause under its own
name, so one ending never lets another's records through (this is the only one). Only source topics
are paused: the commits topic and the state table's changelog keep flowing, or the backlog could
never drain.

## 10.5 Sessions

Every rebalance ends with Faust seeking each assigned partition back to its committed offset, so a
member reads again whatever no commit covered. A table partition's state therefore belongs to a
**session**, named by the app's rebalance count:

- the first record of a partition after a rebalance starts a new session (`ensure_partition_session`):
  the old session's batches are abandoned and their files deleted, its held acknowledgements
  dropped, and the table's committed offsets are read — from GCS once per rebalance generation, never
  older than the state table's (`_offsets_for_session`; a member waits up to
  `SESSION_START_WAIT_SECONDS` 120 for in-flight work on the partition first);
- the first batch of a session continues the partition from the table's committed offset: its
  `after_offset` is that offset, which is what lets the committer refuse what is already in the table
  and never skip what is not (§10.6);
- nothing is acknowledged while the app rebalances, and anything of an earlier generation is dropped
  rather than acknowledged.

The rebalance count belongs to the process (`app.rebalancing_count`). A member that restarts keeps
its name, the pod's, but its count starts again at 0 and is 1 after its first rebalance. So each
announcement also carries the member's **session epoch**: a random id the process makes once at its
start (`PROCESS_EPOCH`, `writer/context.py`). A generation is compared only with others of the same
epoch (§10.6). Before the epoch, a committer still running after another member restarted compared
the restarted member's generation with the one it remembered from the member's last process. It took
every new announcement for one of an older session and ignored it, without a word, for as long as it
ran. The member's partitions never committed again, and the member reported `commits_stalled` (seen
on develop, 2026-10-22).

With static membership (Kaspr sets the group instance id to the pod name) a member that restarts
within the session timeout (120 s) does not cause a rebalance; it still reads again what it had
staged, since its staging directory went with the container.

## 10.6 The committer

`RuntimeCommitter` (`committer.py`) runs in the process of the member that owns `<rt>-commits`, a
topic of one partition, so exactly one member runs it at a time and Kaspr moves it when that member
goes. It holds a `Committer` per table — its own pending files, schema window, commit clock and
failure backoff — and routes each event by the table it names. Everything that changes a table goes
through it, one event at a time:

| Event (`kind`) | From | Effect |
|---|---|---|
| `data_file_ready` | every member's pipeline | the file joins its table's pending set (`PendingFiles`, grouped into seals), unless it is stale (below) |
| `schema_change_request` | every member's writer | observations join its table's schema window (09 §9.7) |
| `commit_tick` | the leader, every 5 s | the clock: commits are due `commit_interval_seconds` (30 s) after the last, sooner at `max_files_per_commit` (500) or `max_bytes_per_commit` (8 GiB) |
| `wakeup` | the committer itself | ends a schema window without waiting for a tick |

It is not the table's only writer: the maintenance app commits compactions and snapshot expiries to
the same tables (§10.10). Every append attempt therefore reloads the table first, and a commit that
lost the pointer's compare-and-swap to one of the app's reloads and decides again.

Nothing is persisted here: pending files live in memory. A new committer starts empty, seeds the
state table from each table itself (schema, committed offsets), and rebuilds the pending sets from the
members' re-announcements within `announce_retry_seconds`.

**Stale announcements.** For each member and partition, `PendingFiles` keeps the newest session it
has heard, as epoch and generation (§10.5):

- **Same epoch, older generation:** the announcement is **stale**. That process has ended the session
  and abandoned its batches, so the announcement is ignored.
- **Same epoch, newer generation:** it replaces the member's older seals of the partition.
- **Another epoch:** the announcement comes from the member's next process, whatever its generation,
  and replaces the last process's seals of the partition. A StatefulSet never runs two pods of one
  name, the next process starts only after the last has stopped, and the commits topic has one
  partition, so the committer hears the new process after everything the last one announced. No
  clock is compared.

An announcement with no epoch is judged by generation alone, as before. Seals that are replaced leave
their files to orphan cleanup. Each stale announcement is counted (`stale_announcements` in the
table block, §10.11) and logged as a warning, `Ignoring a file announcement … from an older
session`, at most once a minute per member and partition (`STALE_LOG_SECONDS`).

**One table at a time.** On every event the tables whose commit is due commit one after another,
the one that has waited longest first, on one worker thread — so a runtime of many tables never has
more than one commit in flight, and a table whose commits fail (its own backoff, its own
`commit_failed` error) holds no other back. A wakeup is scheduled for the earliest open schema window
of any table.

### The append cycle

On a commit, on a worker thread (the event loop keeps consuming and writing):

1. load the table through its pointer;
2. decide every complete seal against the table's committed offsets (`plan_append`,
   `lakehouse/commits.py`):

   | Seal against committed offset `c` of its partition | Decision | Effect |
   |---|---|---|
   | `offset_end <= c` | **covered** | already in the table: these very files, announced again, or the same records under other names; the files are **kept** — orphan cleanup removes them once no snapshot references them |
   | `offset_start <= c < offset_end` | **overlap** | part is in already; the member re-seals the rest and announces that; the files are deleted |
   | `after_offset <= c < offset_start` | **accepted** | continues exactly where the table is |
   | `after_offset > c` | deferred | a seal between them is missing (typically lost with a restarted committer); it waits for the re-announcement — accepting it would let `c` jump past records never committed |
   | a file not in the bucket | **error** | the member seals again from its staged rows |

3. read each accepted file's Parquet footer (two small range requests) and build its `DataFile`
   with full statistics (`lakehouse/files.py`); commit them as **one merge-append snapshot** whose
   summary carries the new committed offsets, event times, valid-through, commit id and runtime
   (`commit_append`; 11 §11.4);
4. commit **once** through the pointer's compare-and-swap (`commit_once`: no PyIceberg retry, which
   would re-run the snapshot producer blindly); a lost race reloads the table and decides again, up to
   `COMMIT_ATTEMPTS` (5), spaced by `COMMIT_SPACING_SECONDS` (1 s) — GCS throttles more than one update
   per second on one object (E11);
5. publish: `committed-offsets:<table>`, `file-decisions:<table>` (only what members must act on) and
   `table:<table>` in the state table; a `table_committed` event, naming the table, to
   `materialization-table-events`.

A failed cycle backs off from 5 s to 300 s (`FAILURE_BACKOFF_SECONDS`), leaves the files pending and
their records unacknowledged, and raises the `commit_failed` table error.

Only a **complete** seal — every file announced — is decided. A member that loses its partition
after announcing only some of a seal's files never completes it, while the partition's new owner
reads and commits the same records; so before planning and after each commit, an incomplete seal
whose last offset the table already covers is forgotten (`_forget_covered_incomplete`), its files
left for orphan cleanup. Before 7f001ae such a seal stayed pending until the committer restarted:
on the E13 load test the heartbeat showed 15 files pending for the half hour between the last record
going in and the committer's next restart.

Log events: `runtime_commit_started` (`seals`, `files`, `pending_files`), `runtime_commit_completed`
(`snapshot_id`, `sequence_number`, `files`, `records`, `duration_ms`, `attempts`, `pending_files`),
`runtime_commit_failed`, and one `runtime_file_decision` per decided file.

### Exactly once

A record is committed **at most once** because covered and overlapping files are refused against
offsets stored *in the table* (they can never disagree with the data), and **at least once** because
a gap is never jumped, acknowledgements are released only by those offsets (§10.7), and nothing the
committer refuses is deleted unless it provably is not the table's.

The last part is what the first E13 run got wrong (13 §13.8). A member announces an uploaded file
again until it sees the commit that took it — every `announce_retry_seconds`, and to every new
committer — so a file can arrive after its own commit. The committer decided it covered, as it is, and
deleted it, though the table referenced it: 2,091 files and 6,780,524 rows went that way, most right
after the run's committer restarts. Covered files are now kept: only an overlapping seal's are deleted
at once (its seal was never accepted, and committed offsets only grow, so it cannot be the table's),
and everything else unreferenced waits for orphan cleanup and its age guard
(`test_a_committed_seal_announced_again_keeps_its_file`). A stale
committer — one still running after its partition moved — is harmless: its commit either loses the
pointer race, or wins with decisions made against the table it just loaded.

## 10.7 Acknowledgement: committing only offsets that are in the table

The writer agent runs with Kaspr's `noack`, so Faust acknowledges nothing by itself; `acks.py`
acknowledges each record only when it is safe:

> **A partition's committed Kafka offset never passes a record whose row is not in the table.**
> A member that stops, crashes or loses a partition resumes — itself or another member — from an
> offset below which everything is committed, reads the rest again, and the committer refuses what
> the table already holds. Delivery into the table is exactly once.

- A record that needed no row — skipped, a tombstone with `skip_tombstones`, one the table already
  holds — is acknowledged in its turn.
- A record whose row went into a batch is acknowledged once that batch is **durable**: its table's
  committed offsets cover it (`acks.batch_durable`, from the pipeline's release step). Ledgers are
  kept per table partition.

Faust commits, per partition, the end of the first run of consecutive acknowledged offsets
(`Consumer._new_offset`), counting the gaps it saw while fetching — offsets compaction removed — as
acknowledged. One unacknowledged record holds the committed offset at or below itself, so the ledger
holds back only **one record per batch, its first: the batch's guard**; every other record is
acknowledged at once.

Faust honours a guard only when the partition already has a committed offset of at least 1, and the
guard is not inside a fetch gap (twm-faust 1.17.22; `guard_is_honoured` checks both). A batch whose
guard would not be honoured is **ordered**: every record from its first on is held and acknowledged
strictly in offset order once durable, and the batch rolls at `ORDERED_BATCH_MAX_ROWS` (5,000). The
first batch of each partition of a new materialization is ordered. The Faust defects behind this, and
their fix (twm-faust branch `fix-commit-gaps`, not yet released), are in 14 §14.3.

`K_STREAM_WAIT_EMPTY=false` (set by the generated runtime spec): Faust's default makes a rebalance
and a shutdown wait until every delivered record is acknowledged, which a guard waiting for a commit
would stall past the group's timeouts and the pod's grace period. Not waiting is safe because an
unacknowledged record is read again.

What each event costs:

| Event | Committed Kafka offset | Read again | Lost / duplicated |
|---|---|---|---|
| A commit covers a batch | advances to the next batch's guard | — | — |
| Graceful stop (`SIGTERM`, pod delete, `stop`, rollout) | final commit of what was acknowledged | staged and in-pipeline batches | nothing |
| Abrupt kill (`SIGKILL`, OOM, node lost) | Faust's last periodic commit | as above, plus what was acknowledged since | nothing (the replay guard and the committer refuse what the table holds) |
| Rebalance | unchanged | the session's uncommitted batches | nothing |
| The committer killed mid-commit | unchanged | — | nothing: the commit either landed (its files then decide `covered`) or did not (the files are re-announced) |
| Seal child killed | unchanged | — (the seal retries) | nothing |
| Upload failing | unchanged; consumption pauses at the backlog limit | — | nothing |

Visible effects: consumer lag counts records not yet *committed to the table*, so a caught-up busy
topic shows up to a batch per partition plus a commit interval; commits come in steps.

## 10.8 The state table (`<rt>-state`)

A global table: every member reads its replica locally, within one changelog hop of the writer. Only
agents write tables (a Kaspr rule), so the committer writes the table keys and the settings agent
the settings key:

| Key | Value | Written by |
|---|---|---|
| `schema:<table>` | the table's current schema, fork map, metadata location, rejected paths | committer |
| `committed-offsets:<table>` | committed offsets, event times and valid-through as of the table's latest snapshot | committer |
| `file-decisions:<table>` | decisions members act on (overlap, covered, error), bounded by `DECISIONS_LIMIT` 5,000 | committer |
| `table:<table>` | the table's totals for the heartbeat | committer |
| `runtime-settings` | live settings and their revision (§10.12) | settings agent |

Each member's replica is a RocksDB store on its persistent volume (`/var/lib/data/tables`), the only
thing on it: `storage_size`, `64Mi` by default since 4.0 (`1Gi` before), is that store's room. Its
keys are bounded per table whatever the topic's volume — a schema, offsets per partition, at most
5,000 decisions — but they are rewritten on every commit, and RocksDB keeps every write in its
write-ahead log until the memtable holding it is flushed. Kaspr flushes at a 64 MiB write buffer by
default: at the few kilobytes a commit writes per table on a test cluster, a busy runtime's log would reach
that in days, and one of twenty tables in hours — more than a 64Mi volume holds. So every runtime
gets `K_STORE_ROCKSDB_WRITE_BUFFER_SIZE` = 4 MiB and `K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER` = 2
(`runtime/templates.py::_default_runtime_spec`, Kaspr's RocksDB settings, which apply to every
table of the app — the state table is its only one): the log never holds more than two buffers,
8 MiB, and the store little more than its keys. They are set for the app because a KasprTable's own
`options` do not reach Kaspr (14 §14.2). On a test cluster a runtime's store took 76–368 KiB (2026-10-23).

A fresh runtime's state table is empty until its committer has handled a first event, so a member
that finds no schema of a table there loads it from the table itself, once, off the event loop
(`ensure_view`; retried at 1, 2, 5, 10, 30 s). A member reports `waiting_for_table`, naming the
tables it is still loading, until it knows every table's schema.

**Incarnations.** The keys are table names, and a name can outlive its table: a topic removed with
its table dropped and added back gets a new table under the old name (07 §7.9a), while the state
table still holds the dropped one's committed offsets and schema. Taken for the new table's, those
offsets started each session past records the new table did not hold, and released them unwritten.
So the committer stamps `schema:` and `committed-offsets:` with the UUID of the table they came from,
the template gives each table context the UUID the control plane recorded, and the readers
(`read_committed_offsets`, `read_schema_record`, `current_view`) ignore an entry of another table — or
one an earlier version wrote without a UUID — and read the table itself instead; the committer
overwrites it with the new table's at its next event (`same_incarnation`, `table_state.py`).

## 10.9 Process state

`runtime_app/state.py` keeps per-partition sessions, open and in-pipeline batches and cumulative
counters in memory, and is lost on restart on purpose: the only checkpoint is the table. What a
member staged but no commit covered is read again by whoever owns the partition next.

## 10.10 Maintenance: the maintenance app

Compaction, snapshot expiry and orphan cleanup run in the **maintenance app**, a KasprApp of its own
(`<release>-maintenance`, `charts/midstream/templates/maintenance/`, `maintenance_app/`) that maintains every
materialization's tables - written and kept ones, stopped and paused materializations included.
Compaction's memory (about 1.3 GiB for a 30-day backlog's child process, E13) is then this app's to
size, not every runtime member's. It keeps no records of its own:

- The control plane ticks each table every minute, and cancels jobs on tables on their way out
  (07 §7.13). A tick carries the table's location, the runtimes' maintenance settings with the
  Settings page's overrides, when each job last finished (from the record's history) and the job the
  record says runs.
- `maintenance-worker` (agent, on `<release>-maintenance-requests`, 6 partitions, keyed by table)
  hands each message to the member's `MaintenanceWorker` and answers at once. Each table one member
  maintains has a `TableMaintenance`: its target, settings, schedule and failures.
- `maintenance-step` (task, every member, every 5 s) finishes what is done on every table - reports
  results, commits a finished compaction, stops one past its time - and then, while a job slot is
  free, starts the next job: a table someone asked for first, then the tables in rotation from where
  the last step stopped. A member runs `MAINTENANCE_MAX_CONCURRENT_JOBS` (1) long jobs
  - a compaction child or an orphan cleanup - at once across its tables. Quick work (a snapshot
  expiry, a compaction plan that finds nothing) takes no slot, so a step goes on to the next table
  until the slots are full, every table has been visited, or `STEP_BUDGET_SECONDS` (10) of quick
  work has gone by.

Per table, in priority order (`_next_job`): what someone asked for; compaction every
`compaction_interval_minutes` (15); snapshot expiry every `snapshot_expiry_interval_minutes` (60);
orphan cleanup every `orphan_cleanup_interval_hours` (24); and, while the last compaction plan left
work behind (the backlog), another compaction at the next step. When a job last ran comes from the
ticks as well as from what the member ran itself, so a restarted member does not run again what its
predecessor just did - but a job that ran and reported nothing (a compaction plan finding nothing, an
expiry with nothing to expire) runs once more after a restart.

- **Compaction** plans partitions oldest first (`plan_compaction`, `lakehouse/maintenance.py`). A
  partition qualifies with at least `compaction_min_input_files` (8) files smaller than
  `compaction_small_file_bytes` (64 MiB). Only the partition whose time window reaches into the last
  `compaction_hot_partition_delay_minutes` (60; a request ignores it) waits until nothing has been
  added to it for that long — a past day a backfill still adds to is compacted anyway; where no
  time ends a partition soon (an unpartitioned table, `__null__`, and the current month: a window
  longer than a day, `LONG_PARTITION_MS`) each file waits for that age instead, so a month's files
  settle one by one rather than after the month ends. The first qualifying partition is taken up to `compaction_max_input_bytes` (1 GiB) and
  2,000 files, and the whole partitions after it join while they fit, so a backfill's past days go
  several at a time. A child process (`maintenance_app/compact_worker.py`) reads the inputs oldest
  first, a batch of 10,000 rows at a time, each projected onto the table's schema by field id
  (`lakehouse/parquet.py::project_batch`, which keeps a null list a null list where PyIceberg's own
  projection empties it). It accumulates up to a memory budget (128 MiB of Arrow data), sorts that
  by `_kafka_timestamp, _kafka_partition, _kafka_offset`, and appends it to the partition's open
  output a row group at a time; an output closes at `compaction_target_file_size_bytes` (256 MiB)
  or when its partition ends. So the child holds about the budget plus a row group, and an output
  is a run of sorted row groups. The member then commits one `replace` snapshot after checking
  every input is still live and, per partition, that the outputs hold exactly the inputs' rows
  inside their time bounds; the snapshot carries the committed offsets forward unchanged, so the
  runtime's exactly-once accounting never notices it.
- **Two writers, one pointer.** The runtime's committer appends to the table while the app compacts
  and expires it. Both commit through the pointer's compare-and-swap (11 §11.2), and whichever lost
  reloads and tries again: the committer every commit attempt (§10.6), the app up to
  `REPLACE_ATTEMPTS` (8) times, a second apart. An input an append or an expiry removed meanwhile
  makes the replace `abandoned`, and its outputs are deleted. Measured on a test cluster: a compaction of 9
  files (120,125 rows) took 6.6 s while the runtime went on committing.
- **Fault tolerance** (spec §7.9.1): the child only *adds* files, so a job that dies before the
  replace commit leaves orphans for orphan cleanup, and one that dies after it is done. A job is
  killed at `compaction_timeout_minutes` (30); when the table is cancelled - being dropped, or its
  materialization deleted - reported `cancelled`; and when no tick has named its table for
  `MAINTENANCE_FORGET_AFTER_SECONDS` (300; its partition moved to another member). When
  a job of several partitions fails, each of them is compacted alone from then on, so one bad
  partition cannot hold the others back. A partition whose own compaction fails backs off from 5
  minutes to 6 hours, and after `compaction_max_attempts` (5) failures in a row is skipped with a
  `compaction_failed` table error until someone asks for compaction again.
- **Snapshot expiry** removes snapshots older than `snapshot_retention_hours` (72), keeping at least
  `min_snapshots_to_keep` (20) and, beyond `snapshot_keep_all_minutes` (60), one an hour. It is one
  metadata commit and deletes no files (E7).
- **Orphan cleanup** lists the table's `data/` and `metadata/` and deletes what no retained snapshot
  references and is older than `orphan_min_age_hours` (24) — never a file of unknown age — at most
  `ORPHAN_PASS_LIMIT` (1,000) per pass. The app cannot see the files a runtime uploaded and has not
  committed yet, which the runtime's own maintenance used to spare; the age guard protects them,
  and a file that waited for its commit longer than that - a day's stall, by default - is resealed
  by its member when the committer finds it missing (§10.6).

**Reporting.** A compaction or an orphan cleanup reports `maintenance_started` (`job`, `started_at`,
`partition`, `attempt`), which sets the table record's `maintenance_job` - what the console shows as
running and what a drop waits for (07 §7.10). Every job reports a `maintenance_completed` table event
naming its table (`job`, `outcome` ∈ `succeeded`, `skipped`, `failed`, `abandoned`, `cancelled`,
`counts`, `duration_ms`, `partition`, `snapshot_id`, `started_at`), which clears it — a requested job
always reports, `skipped` with the reason when there was nothing to do. The events carry
`runtime_app_ref` = `MAINTENANCE_APP_NAME` and the member's name. A tick naming a job no
member runs any more - this member's predecessor's, or one that has run longer than any job can -
is answered with `maintenance_completed` `abandoned`, so the record lets go of it. Log events:
`maintenance_compaction_started`, `maintenance_compaction_completed`, `maintenance_compaction_failed`,
`maintenance_snapshot_expiry_completed`, `maintenance_orphan_cleanup_completed`.

## 10.11 The heartbeat

Every member's `emit-status` task reports on `materialization-status` every 30 s
(`build_runtime_heartbeat`): its phase and health, cumulative counters (rows, bytes, files, skipped
records, `unmapped_values`) in all and per table (`table_counters`), the latest offsets it handled per
table and partition (`latest_offsets`), each table's schema id (`schema_versions`), skip reasons,
`files_pending_commit`, the settings revision and lineage it runs, its acknowledgement and pipeline backlog, and
a performance window (`performance.py`, the last ~30 s).

The committer member adds a `tables` block, one entry per table (`committer_table_blocks`):
`member_id` (itself), the table and its topic, metadata location, current snapshot id and time,
sequence number, totals, schema id, committed offsets, valid-through, snapshot count, last commit time
and duration, pending files, and the stale announcements it has ignored since it started (§10.6).
(Before 2.0 it also carried small files and the running maintenance job; the table record's
`maintenance_job` replaces them.)

A member's phase: `waiting_for_table` while it lacks a table's schema; `processing`;
`waiting_for_source_records`; or degraded — `runtime_reported_degraded` on an error, and
`commits_stalled` when a file it uploaded has gone uncommitted for 10 commit intervals
(`COMMITS_STALLED_INTERVALS`; 5 minutes at defaults). How the control plane merges members is in 08.

## 10.12 Live settings and requests

The settings agent consumes the control plane's broadcast topic: `runtime_settings` messages are
written to the state table's `runtime-settings` key and installed in the receiving member at once;
the others pick the change up from the table on their next record or heartbeat (at most
`REFRESH_INTERVAL_SECONDS` apart). A delivery replaces what the runtime holds only when it is newer
by lineage, then revision (`live_settings.py::settings_position`), so the settings an earlier
installation left on the topic never outrank the current control plane's (17 §17.7). Every
runtime-scope setting (17 §17.3) is read at its point of
use through `get_runtime_app_config()`, so changes need no restart. The maintenance settings reach
the maintenance app with its ticks instead (§10.10). A `maintenance_request` an older control plane
still sends here is dropped.

## 10.13 Package pinning

Every runtime installs exactly the package the control plane runs, however the control plane itself
was installed. `runtime/templates.py::installed_package_source()` reads the running distribution's
metadata (PEP 610 `direct_url.json`):

| The control plane was installed | `PackageSource` | The runtime's requirement |
|---|---|---|
| from a git repository (`package.source: git`) | `kind: git`, `reference`: the commit | `git+<that repository>@<commit>#egg=midstream-platform&subdirectory=python` |
| from a package index (`package.source: index`) | `kind: index`, `reference`: the version | `midstream-platform==<version>` |
| from a local path, an editable checkout say | refused with `RuntimeError` | none: no pod could repeat it |

`_pin_runtime_package()` then rewrites the deployment's runtime spec: every `pythonPackages.packages`
entry that names this distribution, a git URL at a branch, `midstream-platform==<version>` or just `midstream`,
becomes that requirement, and one is inserted if none did. The rest of the block, an index URL, its
credentials and the cache, stays as the chart rendered it. The `reference` is the runtime's
`control-plane/package-revision` annotation and the Settings page's *Package revision*.

Runtimes therefore never float: a control plane installed from a branch pins its runtimes to the
commit it is actually running, and one installed from an index to its exact version. A control-plane
upgrade changes the requirement, the spec hash with it, and the sweep re-applies every runtime
(07 §7.8); until each is re-applied it keeps running the older package.

## 10.14 Protocol log events

With `PROTOCOL_LOGGING_ENABLED` (or the live setting `protocol_logging_enabled`),
members log one structured line per protocol event (`protocol_event {json}`):

| Area | Events |
|---|---|
| writer | `runtime_partition_session_started`, `runtime_schema_change_requested` |
| pipeline | `runtime_batch_sealed`, `runtime_batch_seal_failed`, `runtime_seal_waiting_for_schema`, `runtime_unmapped_values`, `runtime_batch_upload_failed`, `runtime_file_announced`, `runtime_batch_committed` (its acknowledgements released), `runtime_source_paused_for_upload_backlog`, `runtime_source_resumed_after_upload_backlog` |
| committer | `runtime_commit_started`, `runtime_commit_completed`, `runtime_commit_failed`, `runtime_file_decision`, `runtime_schema_committed`, `runtime_schema_commit_failed` |
| maintenance app | `maintenance_compaction_started`, `maintenance_compaction_completed`, `maintenance_compaction_failed`, `maintenance_snapshot_expiry_completed`, `maintenance_orphan_cleanup_completed` |
| other | `runtime_settings_applied`, `runtime_processing_performance_window` |

## 10.15 Failure modes

| Symptom | Likely cause | Where to look |
|---|---|---|
| Status `waiting_for_table` for minutes | the pointer is unreadable (permissions, a dropped table) | member logs: `load_table`; 11 §11.2 |
| `commits_stalled` | the committer is not committing: crash-looping, its member stuck, or every commit failing | the committer member's logs (`runtime_commit_failed`); the table record's `last_error` |
| `commits_stalled`, the committer's log quiet and its table block's `pending_files` 0 while a member holds files | the committer ignores the member's announcements as stale (§10.6). A runtime that predates session epochs did this after a member restarted | the committer's `… from an older session` warnings, `stale_announcements`; 12 §12.3 |
| `commits_stalled` right after disruptions, while commits flow | members held files uncommitted for 5 minutes during repeated committer restarts; clears on the next resource observation (08 §8.6) | — |
| Consumption paused | the upload backlog is full: uploads failing, or seals slow | `Pausing N source partition(s) …` warnings |
| `schema_evolution_failed` / `schema_field_limit` | 09 §9.12 | |
| `compaction_failed` | a partition failed compaction 5 times | the maintenance app's `maintenance_compaction_failed` lines; ask for compaction again to retry |
| A table never compacts | the maintenance app is down, or not ticked | its pod and `maintenance-step` logs; the control plane's `schedule_table_maintenance` work (07 §7.13) |

## 10.16 Verified on a test cluster

See 13 §13.8 for the E13 load test: 16 members, 102,582,940 records from offset 0, twice. The first
run — committer pods deleted mid-commit and mid-compaction, a rolling restart, a 16 → 12 → 16 scale
with files pending, and `SIGKILL`s of a member and of the committer mid-commit — found the deletion
of covered files (§10.6). The second, on the fix, repeated the deletions and the rescale; an abrupt
kill on the fixed code is still to be repeated.
