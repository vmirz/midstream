# 14 — Known Gaps, Sharp Edges, and Roadmap

The state of 1.0.0. Every item is derived from the code or from a measurement on the lab cluster,
not speculation, and names the file a change would start from. The 0.x entries that 1.0 resolved
or made obsolete — the schema authority's waits, commit markers, the object manifest, duplicate
rows — are listed once in §14.13, for readers who knew them.

## 14.1 Not in 1.0, by design

### Shared runtime mode

`Materialization.runtime_mode` is a field, but `build_dedicated_runtime_template_input` refuses
anything other than `"dedicated"` (`runtime/templates.py`):

```python
if materialization.runtime_mode != "dedicated":
    raise ValueError("only dedicated runtime mode is supported in MVP")
```

Every materialization gets its own KasprApp, `<rt>-state` table, three agents (writer, committer,
settings) and three tasks (pipeline, status, commit timer) — eight resources and at least one pod.
At hundreds of materializations this is the dominant cost. Maintenance is already shared: one
maintenance app serves every materialization (10 §10.10).

*To implement:* per-materialization partition state in `runtime_app/state.py`, a writer that routes
by materialization, one committer per table on the runtime's `<rt>-commits` topic (one partition
per table), and a binding model that maps N materializations to one app.

### Google Cloud Storage only

The lake is GCS: `lakehouse/gcs.py`, the PyIceberg FileIO in `lakehouse/io.py`, and the pointer's
compare-and-swap, which is a GCS generation precondition (`if_generation_match`, 11 §11.2). The
console reads `gs://` the same way (console 06). *To implement* another store: a FileIO, object
listing for orphan cleanup, and a conditional write for the pointer — S3's `If-Match` /
`If-None-Match`, Azure's ETags.

### Iceberg format version 2

Tables are created with `format-version: 2` (`FORMAT_VERSION`, `lakehouse/tables.py`; decision
D2) because PyIceberg 0.12 cannot write v3 manifests. Once it can, upgrading is a metadata change,
and `_unmapped` could become a `variant` column instead of JSON text.

### No weekly partitions

A table is partitioned by the UTC hour, day or month of its Kafka timestamp, or not at all (11
§11.3). Iceberg's time transforms are year, month, day and hour; a week is none of them. It could
only be an `identity` partition on a week column the runtime adds to every row, which engines would
not prune a `_kafka_timestamp` predicate by — the reason to partition by time at all. Month is the
coarse option: a topic writing a few megabytes a day gets a file or two a month once compacted.

### The table is the topic's log: tombstones are rows, nothing is deleted

A record with a null value becomes a row with a null payload, or is dropped with `skip_tombstones`
(10 §10.2). Nothing applies deletes or keeps one row per key, so a compacted topic's table holds
every version of every key that the runtime read. A reader that wants the latest per key asks for
it:

```sql
SELECT * FROM iceberg_scan('gs://…/orders-a0690b63eb78')
QUALIFY row_number() OVER (PARTITION BY _kafka_key ORDER BY _kafka_timestamp DESC, _kafka_offset DESC) = 1;
```

*To implement:* a maintained latest-per-key table, or equality deletes. Either needs the console
to read delete files, which it refuses today (console 07).

### A payload that is not a JSON object

Values are decoded as JSON (`valueSerializer: json`, `writer-agent.yaml.j2`). A bare string,
number or array takes no field: the value goes to `_unmapped` as `{"$": value}`
(`ROOT_VALUE_PATH`, `lakehouse/schema/conform.py`), and drift detection ignores it
(`runtime_app/schema/drift.py`). Nothing is lost and nothing is typed. Avro and Protobuf topics
are not read at all.

### The REST catalog is read-mostly

The facade answers `501` to table creation, drop and multi-table transactions — tables are made
and dropped with materializations, datasets with `POST /datasets` — and vends no storage
credentials, so every engine needs its own GCS access (11 §11.8). DuckDB and PyIceberg are verified against it;
Spark and Trino speak the same protocol but have not been run against it.

### No warehouse index

The spec's optional `<destination>/<namespace>/_catalog/tables.json` (spec §5.5) was not built.
Finding a table needs the control plane — its API (`GET /datasets/{dataset}/tables/{table}`) or the
REST facade — or a known location. The bucket knows nothing of datasets: tables are stored by
materialization (11 §11.1).

### An edit changes only the replicas and the topics

`PATCH /materializations/{id}` changes the label (`name`), `replicas` and the topics (`tables`,
`removed_tables`), nothing else. Labels, annotations, storage size and the rest are refused rather
than applied; a storage size change in particular would need the StatefulSet's volume claim
template replaced, which Kubernetes does not allow in place. The partition granularity is the
tables' partition spec and the destination their location, both fixed when the tables are created.
A table's dataset and name are fixed too: a topic keeps the table it was given, and to move it to
another dataset its table must be dropped first. A topic added back after its table was dropped
with its files kept needs a new name, since the old one still names files in the bucket.

A create chooses less still since 4.0: its destination, namespace, runtime name and storage per
member are the deployment's (04 §4.5.2). One deployment writes one place, under names it generates,
with volumes sized for what they hold, the state table alone; a deployment that needs another
destination is another deployment.

### Changing the topics stops the runtime for about five minutes

Kaspr's static membership hands a rejoining member its old assignment, so a runtime re-applied
with another set of topics would never read the added ones (§14.3). The control plane therefore
drains it: the runtime is held at zero replicas until its consumer group is empty — the broker
drops a static member only after the session timeout (120 s) — then the added topics' offsets are
set and the runtime starts again (07 §7.9a). On the lab the whole change took 314 s, the group
emptying at 129 s. Nothing is lost: records wait in their topics, and lag grows meanwhile, for every
topic of the materialization, not only the changed ones. *To improve:* remove the static members
through the admin API (`LeaveGroup` by `group.instance.id`, KIP-345) once the pods are gone, which
would end the wait at once.

### One table's commits are serialized behind the others'

The committer commits its tables one after another, on one worker thread (10 §10.6), so a
materialization of many busy topics commits each a little later than its own commit interval says:
twenty tables whose commits take two seconds each are each committed about every forty seconds at
worst. That is the price of never having more than one commit in flight per runtime; split very
busy topics into materializations of their own if each must commit on its own cadence.

### No migration from 0.x, nor from 1.0

1.0 is a fresh start (decision D8, 12 §12.13): 0.x Parquet lakes are not converted. A 0.x
destination's `ingest_date=…/`, `_manifest/` and `_runtime_state/` objects are left for an operator
to delete. 2.0 (several topics per materialization) is a fresh start too: a 1.0 materialization is
deleted with the 1.0 control plane — which drops its table — and created again with 2.0, whose
records and table layout 1.0's do not match.

### The `repair_state` step

`repair_state` is one of the `WORK_STEPS` `models/events.py` accepts, but the step dispatcher in
`services/workflow.py` has no handler for it: dispatching it would fall through to the
unknown-step path.

### One Kafka user for the whole deployment

Every app and every runtime authenticate as `kaspr.authentication` (02 §2.3), so a runtime holds
the grants only the control plane needs: deleting any runtime's topics and group, and reading every
materialized topic, not just its own (21 §21.1). *To narrow it*: chart values for a runtime user
of its own. That user would need Create, Read and Write on `<release>-mat-` topics, Read on
`<release>-mat-` groups and Write on its transactional ids; Write on the status and table-events
topics, Create and Read on the runtime control topic; Read on the source topics; and no Delete.

## 14.2 Correctness hazards

### A topic the Kafka user cannot see passes for an empty one

Discovery reads partitions from a metadata request for every topic (09 §9.6), and a broker leaves
out the topics the user may not describe. So a topic the deployment's Kafka user cannot see
samples like an empty one: the materialization is created, its runtime is assigned none of the
topic's partitions, and the table stays empty while the materialization reads `running`; only the
lag's `topic does not exist` says otherwise (21 §21.5). A topic that does not exist yet reads the
same. *To fix*: refuse the create, and an update that adds the topic, at the API when the topic is
missing from the cluster's metadata, naming it; the metadata client the topic list uses reads every
topic and names none, so asking creates nothing. Failing `start_discovery` too is a safe backstop.
Failing `discover_tables` is not: an update's tables are written into the materialization at
`tables_claimed`, before it runs, and `apply_runtime` then refuses the table that has no schema
(`table_missing`).

### Exactly once covers what the topic still holds

Delivery into the table is exactly once for every record the runtime reads (10 §10.7). A record
that Kafka's retention or compaction removes before the runtime reads it is never seen: a
materialization created long after a topic's retention, or one stopped for longer, starts from the
earliest offset the topic still has. Nothing reports the gap; compare the table's first offsets
with what the topic should hold when it matters.

### The materializer assumes it is the table's only writer of data

`catalog.write` lets an external engine commit through the REST facade (11 §11.8), and every
commit is checked against the pointer, so nothing is overwritten. But the runtime does not
interpret what such a commit does: row-level deletes make the console refuse the snapshot (console 07),
a rewrite that drops rows makes the table hold fewer than the topic gave, a column added outside
the fork map is a field the runtime does not write, and the committed offsets are read from the
newest snapshot that carries them, walking past external ones (`committed_offsets`,
`lakehouse/offsets.py`). Grant `catalog.write` to maintenance tools that rewrite files, not to
writers of data. What such a commit cannot do is move the table: `set-location` and the properties
that put files elsewhere are refused (11 §11.8), and the console reads nothing outside the table's
location even if a file there is named (console 12 §12.6).

### The scheduler topic's name is derived

The Kaspr scheduler reads delayed work from `<app id>-schedule-requests`; a template that spells
the topic by hand can write to a name nothing reads. The chart derives the name from the control
plane's app id for every installation, `<release>-control-plane-app-schedule-requests`, and
`ValuesWiringTests` renders every values file.

### Runtime topic deletion is prefix-based

Kafka cleanup deletes every topic whose name starts with the runtime's name
(`_runtime_owned_topics`, `services/workflow.py`). Runtime names end in a random 12-hex
materialization id, so a collision is effectively impossible, but any naming scheme that lets one
runtime's name be a prefix of another's (`kaspr-orders`, `kaspr-orders-2`) would delete the other's
topics. Since 4.0 no request can choose one; a materialization created earlier with a chosen
`runtime_name` keeps that name, and with it the hazard, until it is deleted.

### A KasprTable's `options` never reach Kaspr

kaspr-operator's KasprTable CRD (0.19.4) declares `options` and `extraTopicConfigs` as `type: object`
with no properties, and without `x-kubernetes-preserve-unknown-fields`, so the API server prunes
whatever they hold: a table rendered with `options: {write_buffer_size: "4194304"}` is stored with
`options: {}` (seen on the lab, 2026-10-23), although Kaspr would apply them. The runtime sets its
RocksDB write buffer through the app's `K_STORE_ROCKSDB_*` environment instead (10 §10.8). *To
fix*: mark both fields `x-kubernetes-preserve-unknown-fields: true` (or `additionalProperties: {type:
string}`) in the operator's CRD.

### Runtimes declare their topics

A runtime spec without `topicAllowDeclare: true` leaves a runtime unable to create its own topics.
The chart renders one runtime spec for every installation, from `controlPlane.runtime.config`, with
it set.

### The sweep cannot rescue a materialization that has no runtime yet

`build_sweep_observation_outputs` (`services/observation.py`) skips every materialization without
a `runtime-binding:<id>`, and a binding exists only once `apply_runtime` succeeds. A
materialization that failed at discovery or at `create_table` is invisible to the sweep and never
heals by itself; `POST …/resume` is the recovery. Discovery itself cannot hang: it fails at its
deadline (07 §7.2).

### An error that escapes an agent's map rebuilds its clients

When an exception escapes a Kaspr map — a `ValueError` in `workflow-apply`, a KasprApp read that
fails other than by timing out in `resource-observation` — Kaspr restarts the agent and runs its
init block again, building new Kubernetes and Kafka clients. Each holds a background loop on a
thread of its own that nothing stops, so every such crash leaves a thread and its connections
behind. Timeouts do not escape: they are recorded as failed steps, or skipped observations.
*To fix:* build the clients once per process, or stop the old ones when the scope is rebuilt.

### The lag refresh asks Kafka one consumer group at a time

The control plane's `maintenance` agent handles one event at a time, and the consumer-lag refresh
asks Kafka once per distinct consumer group, one after another, each lookup allowed 30 s. Meanwhile
every other event on `materialization-work` — which the agent receives before its filter drops
them — queues behind it, and once its 4,096-event buffer is full Faust stops delivering to every
agent of the app. *To improve:* look the groups up concurrently.

### Table drops are batched, but their speed on GCS is unmeasured

Dropping a table deletes its objects 100 per GCS batch request, in bounded passes on a worker
thread (`drop_table_objects_pass`, `storage/objects.py`; 07 §7.10). How fast that is depends on how
GCS runs a batch's calls, which it does not document: against a fake, ~1,700 objects a second when
they run together, ~70 when they run one after another at 10 ms each. Until a large drop on GCS
measures it (`pass_deleted_count`, `duration_ms`), plan on the slower figure. *To improve:* send
two to four batches at once, minding the bucket's write rate, which the runtimes' uploads share.

## 14.3 Kafka and Faust

### twm-faust's commit bookkeeping, which manual acknowledgement exposes

How Faust picks the offset to commit (`Consumer._new_offset`, twm-faust 1.17.22): per partition it
keeps the set of acknowledged offsets, adds the offsets it recorded as *gaps* while fetching —
offsets with no record, removed by compaction — and commits the end of the first unbroken run that
starts at the committed offset. An unacknowledged offset breaks the run, so nothing past it
commits; the runtime's batch guards rely on exactly that (10 §10.7). Three defects break it, each
shown on the smallest partition that triggers it.

**A. A gap swallows the record after it.** Offsets 3 and 4 were compacted away:

```text
records:        0  1  2        5  6
gap on 5:                3  4  5        should be 3 4: _add_gap adds [from, to + 1)
acknowledged:   0  1  2           6     5 is still in an open batch
committed:      7                       should be 5 - a crash now loses record 5
```

The list-based gaps before the interval-tree port (twm-faust `f46f0a0e`) used `range(from, to)`,
which excluded the fetched record. With automatic acknowledgement and one record in flight it never
shows: 5 is acknowledged before 6 is. A held acknowledgement it silently voids. The ack ledger
detects a batch whose first record lies inside a gap (`guard_is_honoured` reads `_gap`) and makes
it an *ordered* batch: every record held and acknowledged in offset order once in the table. That
is safe because Faust merges a gap into the run only once an offset above it is acknowledged, and
an ordered batch acknowledges nothing above its first record until it is in the table. The cost:
ordered batches roll at 5,000 rows, so a compacted topic writes more, smaller files at its gaps.

**B. No gap after offset 0.** Offset 1 was compacted away:

```text
records:        0     2  3  4  ...
gap on 2:       none: the loop tests `if gap > 1 and r_offset`, and a read offset of 0 is false
acknowledged:   0     2  3  4  ...
committed:      1, then 1 forever: offset 1 is neither acknowledged nor a gap
```

Nothing is lost, but the partition never commits past 1, its acknowledged-offset set is never
trimmed (it grows by every record, and each commit computes `max()` over it), every restart reads
the partition again from offset 1 — which the committer then refuses as already in the table — and
the console shows its lag as nearly the whole partition. Seen on 2026-10-18 on partitions 2 and 14
of `materializer-loadtest-orders`.

**C. Without a committed offset, any run commits.** A new consumer group:

```text
records:        0  1  2  3
acknowledged:      1  2  3     0 is still in an open batch
committed:      4              should be nothing yet - a crash now loses record 0
```

`_new_offset` checks the run against the committed offset only when there is a truthy one, so in a
new group it commits the first run of acknowledged offsets wherever that starts. The ledger orders
the first batch of each partition of a new materialization for that reason (a guard needs a
committed offset of at least 1).

*Fixed in twm-faust* on branch `fix-commit-gaps`, not yet pushed or released: `_add_gap` excludes
the fetched record, the loop tests `r_offset is not None`, and a partition without a committed
offset is checked against the first offset read from it. Its tests run Faust's fetch loop over each
partition above. Until a release includes it, the ledger's workarounds stay.

### A release that adds topics is not assigned them

Kaspr runs every app with static group membership. A restarted pod rejoins within the session
timeout and gets the group's stored assignment back, so topics a release adds to an app are never
assigned until something forces a rebalance — 0.5.0's settings topics, 0.6.0's identity topics,
1.0's `materialization-table-events`. The 1.0 rollout scales the control plane to zero anyway
(12 §12.13); for any other release the procedure is in 17 §17.12. A runtime whose topics an edit
changes meets the same thing, and the control plane drains it rather than rebalance it (§14.1).
So does a runtime whose Kafka user could not read one of its topics when it started: granted Read
later, it is not assigned the topic until it stops for longer than the session timeout (21 §21.7).

*To fix (in Kaspr):* after a static rejoin, a member whose assignment lacks topics it subscribes to
should rejoin. The broker rebalances a stable group when its leader rejoins under a known member
id, so a rejoin request from the leader is enough.

### Deleting a control-plane changelog topic breaks its table store

Each Kaspr table keeps a RocksDB store on the control plane's volume
(`/var/lib/data/tables/<table>-<n>.db`). Delete the table's changelog topic and the store still
holds the old offsets: the app fails at startup with Faust's `ConsistencyError` until the store's
directories are removed too. The 1.0 rollout, which deletes the 0.x topics, removes them
(12 §12.13).

### A write lands in the shard of the event being handled

The `state` table is global: every member reads every shard. A write or a delete, though, goes to
the changelog shard of the event being handled, not to one chosen by the record's key. So a record
written while handling an event keyed by something else gets a second copy in another shard. No
delete of its materialization reaches that copy, and a keyed read can return either one. 2.0 keeps
every write about a materialization on events keyed by it: the scheduled steps on `__system__`
write nothing and hand their work on (06 §6.4), and `ShardRoutingTests` holds them to it. Scans
keep the newest copy of a record (`resolve_envelope`, `_scan_materializations`). *Still open:* a
delete removes only the handled shard's copy, so a copy stranded before 2.0 lasts until something
writes or deletes it from its own shard. The status copies 1.0's lag refresh left are of this kind,
and nothing reads them. Deleting a key from every shard needs Kaspr's per-partition delete, which
`KasprGlobalTable` does not offer yet.

### The scheduler's first dispatch after a start can be lost

Kaspr 0.11.31's scheduler partitions a request by the producer's partition count of
`<app id>-schedule-actions`; on the first request after the control plane starts, that metadata can
still be unfetched, and the `distribute` actor crashes with `NotReady("Producer metadata for topic
'…-schedule-actions' not available yet")`. Faust's supervisor restarts it at once, and every later
request dispatches, but the request in flight is gone: on the lab (2026-10-26) it was a new
materialization's first delayed observation, and the observation sweep (07 §7.7) brought the
materialization to `running` in its stead. A delayed step that only the scheduler would run again
is late by one sweep at worst. The fix belongs in Kaspr (`kaspr/scheduler/manager.py`,
`_request_partition`): wait for the topic's metadata, or retry on `NotReady`.

## 14.4 Resources and throughput

### Runtime pods are sized for the whole deployment

Every runtime pod takes `resources` from `CONTROL_PLANE_RUNTIME_APP_SPEC`; a
materialization's `resource_profile` is carried but unused, so a busy topic's runtime is limited
like a quiet one's. Since 4.2 that holds for the limits only, CPU included. The chart's values
limit each member to 2 CPUs, room for a seal child beside a main process that uses a full core on a
busy topic (below). A VerticalPodAutoscaler then sets the members' CPU request from what each
runtime's members use (02 §2.10c). Memory is still the spec's. On the load test, without limits,
members peaked at 1.97 GiB during seals, past the shipped 1 GiB (12 §12.10): a backfill's seal held a
row group for every partition value its batch touched, so a batch spread over months held every row
until its end (1.36 GiB on the load test, 2.1 GiB measured for a 250,000-row batch over two years).
Since 4.4 the seal writes one partition value's file at a time from spools on disk, so it holds one
row group whatever the batch (10 §10.4): about 300 MiB at the default row group, and the shipped 1 GiB
holds a backfill — at most 773 MiB of anonymous memory a member on the lab's, with no kill (12
§12.10), a quarter of the limit to spare. Before 2.0 the committer member
also compacted, and peaked at 2.98 GiB; compaction is the maintenance app's now, sized on its own.

*To do:* use `resource_profile` so a busy materialization's limits can be set apart from quiet ones'.

### Autoscaling reads the lag only

Autoscaling (07 §7.12a) adds members from the lag and its pace, and cannot tell whether members are
what the runtime lacks. A runtime held back by something else — one hot partition, a committer
whose commits take longer than its interval (§14.4 *Commit time grows*), a slow destination — gets
members up to what its partitions keep busy, to no gain, until it has caught up. It does not check
the cluster either: members that cannot be scheduled stay `Pending`, the runtime reads `deploying`
meanwhile and gets no more, and its running members go on reading every partition.

Once scaled up a runtime keeps its count until it is under the lag floor, however fast it then
drains: there is no step down on the way. The first decision after a runtime starts measures its
pace partly from before it read anything, so a runtime created on a large backlog may first be
doubled and only sized from its pace at the next step. Every change of members rebalances its
consumer group, which pauses its reading for the rebalance (tens of seconds with static members).

*To do:* scale on the members' CPU as well as the lag, and step down as the lag falls.

### A member's throughput is bounded by one core of per-record Python

Each member's main process runs at one full core on a busy topic (`kubectl top`: 0.97–1.0 CPU per
pod outside seals): Faust's deserialization, drift detection's walk of each record, and staging it
as a JSONL line. 1.0 removed the per-record normalization of 0.x — nothing is normalized per record
any more (10 §10.2) — and a member moves about 1,400 records a second on the load-test topic's
~1.4 KB records, against 812 in 0.7.0 (13 §13.8). Past that, more throughput means more members:
Kafka partitions bound the count — and for a materialization of several topics, the distinct
partition counts summed, since topics of the same count share members (10 §10.1).

### Commit time grows with the files in a commit

A commit reads every announced file's footer (in parallel), writes a manifest, merges manifests
once there are 50 (`commit.manifest.min-count-to-merge`), and rewrites the metadata file. On the
load test a commit took about 2 s plus 18 ms per file — 2.4 s for 17 files, 9.5 s for 414
(13 §13.8). While a commit runs, more seals arrive, so a busy backfill commits a few hundred files
every 40–50 s. The spec's target, 3 s at 100 files, is roughly met; a burst of many hundreds of
files is not. *To improve:* cache the manifest list and the last merged manifests between commits,
and read footers before the commit's turn rather than inside it.

### Records reach the table in steps

A record is in the table once its batch is sealed, uploaded, announced and committed: at most the
batch's soft age (`max_open_file_age_seconds`), then a commit interval, then the commit itself. On
a quiet topic, the ceiling. The console shows this as **complete through** (console 07); consumer lag
counts the records not yet committed to the table, so it never reads zero on a busy topic. The
console's live tail shows a materialization's newest records within seconds (19); an engine on the
REST catalog still reads committed snapshots only.

### Control-plane state growth

| What grows | Where | Rate | Bound |
|-----------|-------|------|-------|
| Control-plane state entries | Kaspr `state` table | a set per materialization | delete purges them (`purge_records_on_delete`); the retention sweep collects what a delete left |
| Operation records | `materialization-op:<id>:<op>` | one per workflow operation | none |
| Sweep spec rendering | control plane, every 2 min | a full render per materialization | O(materializations) CPU per cycle |
| Snapshots in `metadata.json` | each table | one per commit, ~1 KB each with 16 partitions' offsets | snapshot expiry (`snapshot_retention_hours`, `snapshot_keep_all_minutes`) |

### The control plane uses half a core while idle: Kaspr's scheduler collects garbage four times a second per partition

On the lab (2026-10-24), with no materializations, the control plane used 0.48–0.51 CPU, all of it in
its main thread, while the maintenance and tail apps, which do not run Kaspr's scheduler, used 2–10
millicores. The cause is in Kaspr 0.11.30's scheduler: `Dispatcher._dispatch`
(`kaspr/scheduler/dispatcher.py:257`) calls `gc.collect()` on every pass, every 0.25 s for each
timetable partition, and `Janitor._clean` (`janitor.py:278`) every 3 s per partition. A full
collection walks every tracked object, so it costs what the heap is: 17 ms with Kaspr alone (97k
objects), 33 ms with the control plane's modules and definitions loaded (171k), about 42 ms in the
lab's long-running process. With the default three timetable partitions that is eleven collections
a second: 3 × 1/(0.25 s + 42 ms) + 3/3 s.

How it was shown: sampling the main thread's `schedstat` at 1 ms found its CPU in synchronous chunks
of 37–42 ms, mostly three back to back every 305 ms — the three dispatchers in phase — and nothing
else above 1 ms/s. The same image run locally (podman) with the lab's 87 definitions and the package,
its collector timed with `gc.callbacks`: 11.1–11.4 full collections a second at 33 ms, 364–377 of the
thread's 376 ms/s; py-spy put 99% of the thread's samples on those two lines. The same run with the
two calls removed: no full collections, 5 ms/s. Kaspr's other `gc.collect()` calls run only at
shutdown and rebalance.

*To do:* fix it in Kaspr: drop the per-pass collections (the automatic collector still runs on its
thresholds), or collect once a minute at most. Until then every Kaspr app with the scheduler on pays
about this much per timetable partition, and the control plane's VerticalPodAutoscaler requests it
(671m on the lab, 02 §2.10c); after the fix the request should settle near the 100m floor.

## 14.5 Data quality and privacy

### Malformed records are kept in runtime memory

`RuntimeMalformedRecord.raw_sample` (`runtime_app/models/processing_state.py`) holds the decoded
record that a member skipped, in that member's memory, where a log line can reach it. It is not in
the heartbeat or the API. 1.0's schema change requests carry shapes only, never a value, so the 0.x
proposals topic and its whole records are gone (09 §9.7). *To improve:* keep only the shape here
too.

### Inference is conservative

Strings that look like timestamps, dates or decimals stay strings, and integers never become
`int`: a wrong guess costs a fork forever, so inference only claims what JSON says (09 §9.2,
§9.13). Readers cast.

### A field that changed type is several columns

`total` that was a number and became a string is `total` and `total__v2` in the table (09 §9.5).
The console folds the forks into one logical column (console 06); hand-written SQL against the table,
or another engine, reads each fork as its own column and must coalesce them, guided by the
`materializer.fork-map` property.

### Keys and headers that are not UTF-8

A Kafka key or header value that is not UTF-8 is stored as `b64:` followed by its base64
(`runtime_app/writer/context.py`). A reader comparing keys must expect the prefix.

### PyIceberg reads a null list of structs as an empty list

PyIceberg 0.12 projects every data file onto the table's schema (`_to_requested_schema`) and
rebuilds a list or map of structs from its offsets alone, which carry no nulls: through PyIceberg a
record without `items` reads as `items = []`, the same as one whose `items` was empty. The files
hold the null, and DuckDB — so the console — reads it as written. Compaction used that projection
until 6fed494, so files compacted before it hold `[]` where a record had no list (the lab's E13
table, 13 §13.8); it now projects with `lakehouse/parquet.py::project_batch`, which keeps the nulls
(`test_compaction_keeps_a_missing_list_null_and_an_empty_one_empty`). *To do:* report it upstream,
and drop the workaround once PyIceberg keeps the nulls itself.

## 14.6 Iceberg tables and maintenance

### A large backfill leaves small files until compaction catches up

A backfill's seals each write a file for every day their rows touch, so its days fill with small
files. Compaction takes whole small partitions together, up to `compaction_max_input_bytes`
(1 GiB) and 2,000 files a job, and starts the next job at the next minute's tick while there is
work. Loading two years of the load-test topic — 730 days — it kept pace: the table never held more
than 5,997 files, and fifteen minutes after the last record was committed no day qualified for
compaction any more (13 §13.8). Until a partition is compacted, queries read its small files: the
console's Data tab is slower over such a table, not wrong.

### The small-file count is as of the last compaction plan

The `small_files` a compaction's report gives is the count the plan saw
(`TableMaintenance.small_files`), not a live figure: between plans files are added and compacted
away. The console's Table tab counts from the snapshot it reads (console 11).

### Schema dates are as precise as the files that hold them

The console dates a schema change from the table's manifests (console 06): a field first held a value
somewhere within the span of the earliest file holding one, by Kafka timestamp. A day-partitioned
table's file holds one UTC day, so its dates are exact to the day, and an hour's to the hour. A
month's are as wide as its files, up to the month, and every change of one month reads the same. An
unpartitioned table's files span whatever a seal or a compaction put together, and compaction merges
them across all of the table's time, so once compacted its changes can all read as the table's whole
span. A value no longer in the current snapshot dates nothing. Exact dates would need the data read
— a `min(_kafka_timestamp)` of each field's non-null rows, a scan per field — which the console
does not do. The API's schema `since` is the commit time, not the data's (04 §4.6.1).

### The maintenance app runs one long job at a time per member

A member runs `MAINTENANCE_MAX_CONCURRENT_JOBS` compactions or orphan cleanups at once
(1 by default), visiting its tables in turn, and a compaction may take up to
`compaction_timeout_minutes`. Many tables with a backlog each, on one member, wait for each
other: each gets a job in turn, not a job an interval. Snapshot expiry takes no slot. More replicas
share the tables, up to the six partitions of `<release>-maintenance-requests`; more slots need
about 1.3 GiB of memory more each (12 §12.10).

### A maintenance app restart forgets compaction failures

The app keeps a partition's failures and backoff in memory (`TableMaintenance`). The ticks bring
back when each job last finished — from the table record's history — and a restarted member
reports a job it was running as abandoned, but a partition that had failed is tried again at once,
and one skipped after `compaction_max_attempts` failures is tried again from the start.
*To improve:* carry the failures in the record's history, as the last runs are.

### Orphan cleanup does not know what a runtime has not committed

Before 2.0 the committer ran orphan cleanup and spared the files waiting for its commits. The
maintenance app cannot see them, so it deletes any file no retained snapshot references and older
than `orphan_min_age_hours` (24 h by default). Only a runtime whose commits stalled for that long
holds such a file; its committer then finds the file missing, and its member seals the rows again
(10 §10.6). Keep `orphan_min_age_hours` well above any stall that should heal by itself.

### Orphan cleanup deletes one file at a time

A pass deletes at most 1,000 files (`ORPHAN_PASS_LIMIT`), one request each, oldest names first, and
a pass that hit the limit makes the next due at the next step. On the E13 table that was a pass
every three minutes, about 5.5 files a second: its ~24,000 unreferenced files — compaction inputs
whose snapshots had expired — took over an hour (13 §13.8). A busy table that replaces many small
files a day spends that much of a maintenance app slot deleting them. *To improve:* batch the
deletes, 100 to a request, as the table drop does (07 §7.10).

### `commits_stalled` lingers after disruptions

A runtime whose members held files uncommitted for five minutes — during repeated committer
restarts, for example — reports `commits_stalled`, and the materialization keeps that reason until
the next resource observation, up to ten minutes after commits resume (08 §8.6). The Table tab's
**Last commit** is the live answer.

Do not confuse this with commits that stop for good. A runtime that predates session epochs ignored a
restarted member's announcements, without a word, until its committer restarted (10 §10.5, 12 §12.3).
That stall never clears by itself, and **Last commit** stops moving.

### An older snapshot can expire while someone reads it

A console run pinned to a chosen snapshot (the runs API's `asOf`; the Data tab always reads the
latest) can lose it to snapshot expiry meanwhile, and the next page answers that the snapshot is not in the table and may have expired (console 07). Expiry keeps
every snapshot of the last `snapshot_keep_all_minutes`, then one an hour, up to
`snapshot_retention_hours` (11 §11.6).

### One object takes every commit

Every commit replaces `metadata/catalog-pointer.json`, and GCS throttles a single object updated
more than about once a second (E11). The runtime commits seconds apart; an external engine
committing through the REST facade in a tight loop slows the runtime's commits, which then retry
(11 §11.10).

### No manifest rewrite job

The spec's fourth maintenance job, a manifest rewrite past a threshold (spec §7.9), does not exist:
merge-append keeps manifests in check (`commit.manifest-merge.enabled`, 50 to merge, 8 MiB target),
and every compaction rewrites the manifests it touches.

## 14.7 Observability gaps

- **No metrics.** Everything is structured log lines behind `protocol_logging_enabled`; there is
  no Prometheus endpoint. Operators read health from the heartbeat (the `table` block: last
  commit, pending files, small files, the running maintenance job) and the console.
- **No tracing.** `work_id` is carried through control-plane events as a correlation id, but
  nothing emits spans.
- **No alerting hooks.** A `failed` status, a `table_error` (`commit_failed`,
  `schema_evolution_failed`, `compaction_failed`) and a paused partition are visible in the
  console and the logs, and nothing pages anyone.
- **Protocol logging is all-or-nothing.** One boolean turns on every category; there is no
  per-category filter.
- **Restart health has fixed thresholds.** A runtime is `runtime_members_restarting` for 30 minutes
  after a member's last out-of-memory kill, or its last restart after three (08 §8.3, row 3d); the
  window and the count are constants, not settings, and only the `kaspr` container counts. A member
  restarted more often than the sweep observes — every two minutes, or sooner while a phase is
  chased — is seen by its restart count, not each exit.

*Cheap wins:* export the heartbeat's counters — rows written, commit duration, pending files,
small files — as metrics, and alert on `table_error` events.

## 14.8 Identity and access

### Identity writes are serialized on one partition

Every identity request — directory changes, sign-ins, renewals, sign-outs, token creation and
revocation — is keyed `identity`, so one agent instance applies them all in order. That is what
keeps emails unique and an administrator present without locks, and it is cheap: passwords and
token secrets are hashed in the WebViews, and each request is a few table writes. It does cap
identity throughput at one consumer (18 §18.13).

### Access is per dataset, and not enforced by the storage

Grants are made per dataset, not per table: whoever reads one table of a dataset reads all of them,
and the facade filters no rows or columns. Access tokens carry `catalog.read` and optionally
`catalog.write`, on each dataset intersected with what their owner holds there, and may be limited to
some datasets (`_token_principal`, `identity/authorization.py`; 18 §18.15).

The grants decide what the control plane, the REST catalog and the console show and serve, **not
what the bucket serves**: DuckDB reads data files with an HMAC key, PyIceberg with Google
credentials, and either usually reaches the whole bucket. Restricting a dataset therefore restricts
only people without such credentials (decision; 18 §18.16). Table-scoped storage credentials vended
by the REST catalog (`config` → `storage-credentials`, or GCS downscoped tokens per table location)
would close the gap; the catalog's resolution of a name to one table's location is already in place
for it.

Names are not secret either: everyone who may see datasets sees every dataset's name, description,
owners and table names, locked ones included.

### A dataset grant cannot be scoped to a materialization

A Dataset Writer may delete any materialization that has a table in their dataset — together with
its tables in other datasets they may also write — and a materialization's stop, resume and scaling
stay deployment permissions (decision). A team that shares a dataset shares its tables' lifecycles.

### No password recovery, no email, no single sign-on yet

There is no "forgot password": an administrator resets it and passes it on. Nothing sends email.
Single sign-on is designed in — `User.external_identities`, `Group.external_groups`, the `amr`
claim, `GET /auth/session` → `providers` — but no provider is implemented (18 §18.13).

### Reported addresses can be chosen by a direct client

The console trusts `X-Forwarded-For` from its ingress and forwards the browser's address to the
control plane, which records it on sessions and audit entries as reported. A client that reaches
the console or the control plane without passing through the ingress can claim any address, and so
evade the console's per-address sign-in limit; the per-email lockout still applies.

### No deployment-locked settings

Anyone with `settings.manage` can override any setting the catalog marks overridable. A deployment
cannot forbid an override; a lock would be a per-setting flag checked in
`services/settings.py::validate_change`.

## 14.9 Manifest hygiene

| Issue | File | Impact |
|-------|------|--------|
| RBAC lacks `list`/`watch` | `charts/midstream/templates/control-plane/rbac.yaml` | only `get/create/patch/delete` on four kinds |
| `workflow-apply` declares its dynamic topic, `resource-observation` does not | `charts/midstream/templates/agents/*.yaml` | the latter relies on the topic existing |

None of these break anything today; all are cheap to fix.

## 14.10 Things that look like bugs but are not

Before "fixing" any of these, read the linked section.

| Observation | Why it is intentional |
|-------------|----------------------|
| Intake persists **before** validating | the request is durable even if validation rejects it; the status carries the rejection (07) |
| `phase_reason` equal to a phase name | a sentinel from `_fallback_status` meaning "no real status exists yet" (05) |
| `heartbeat_is_stale(None)` returns `True` | a never-seen heartbeat must not read as fresh (08) |
| Neither source alone can produce `running` | it takes both a Ready resource and a fresh healthy heartbeat (08) |
| `spec_hash` changes when the control-plane commit changes | package pinning is intentional; the sweep then re-applies every runtime (10 §10.13) |
| `spec_hash` does **not** change when a setting is overridden | settings never reach the rendered runtime spec, so a change restarts nothing (17 §17.6) |
| Changing an env var has no effect | an override for the same setting wins until it is reset (17 §17.2) |
| Consumer lag never reaches zero while records arrive | offsets commit only once their rows are in the table (10 §10.7) |
| A restart or rebalance reads many records again | what was staged but not committed to the table is read again, and the committer refuses what the table already holds (10 §10.7) |
| A new materialization's first files hold 5,000 rows | each partition's first batch is ordered: Faust would commit past a held record without a committed offset (§14.3, 10 §10.7) |
| A batch writes one file per partition it touches | rows are written to the day (or hour) of their Kafka timestamp; a topic whose timestamps are not in offset order writes many small files, and compaction joins them (11 §11.5, §11.6) |
| Rows land in old days' partitions | partitions follow `_kafka_timestamp`, not the time a row was written; `_materialized_at` is that (11 §11.3) |
| `_unmapped` holds values of known fields | the schema was at its field cap, or a value did not fit a collapsed family (09 §9.8) |
| A snapshot whose operation is `replace` changes no rows | compaction: files rewritten, rows and offsets carried forward (11 §11.6) |
| Files under `data/` that no snapshot lists | uploads no commit took, or a compaction that died; orphan cleanup removes them after a day (11 §11.6) |
| Seconds of `pending_files` while commits flow | files wait for the next commit interval, and for the files before them in their partition (10 §10.6) |
| The console calls a query's rows exact without reading a file | row counts come from the manifests (console 06) |
| Deleted materializations vanish from the list | delete ends by purging the records; turn off `purge_records_on_delete` to keep them until the retention sweep (07 §7.10) |
| A stopped materialization's tables get new snapshots | the maintenance app goes on compacting and expiring them (10 §10.10); nothing is appended |
| A materialization shows `deploying` for minutes after an edit of its topics | the runtime drains until its consumer group is empty (§14.1, 07 §7.9a) |
| A kept table's row count no longer changes | it is `retired`: read-only, still in the namespace and the catalog, still maintained (11 §11.7) |

## 14.11 Extension points

| To add… | Start in |
|---------|----------|
| A new HTTP endpoint | `web/materializations.py` + a WebView in `charts/midstream/templates/webviews/`, authorizing with `authorize(...)` first (18 §18.14) |
| A new permission | `identity/permissions.py` and the built-in roles, then the views and console routes (the `adding-permissions` skill) |
| A new setting | `settings/catalog.py`, and `runtime_app/live_settings.py` for a runtime one (the `adding-settings` skill) |
| A new request action | `services/request_intake.py` `REQUEST_ACTION_TO_STEP` + `EXPECTED_DESIRED_PHASE` |
| A new workflow step | the dispatcher in `services/workflow.py`, and `WORK_STEPS` in `models/events.py` |
| A new phase or phase reason | `LIFECYCLE_PHASES` in `models/materialization.py` + `observation/mapping.py` (rules are ordered; first match wins) |
| A new table event | `services/table_events.py`, and the runtime's `committer.py` or the maintenance app's `maintenance_app/worker.py` (`_event`) |
| An inference or evolution rule | `lakehouse/schema/infer.py`, `policy.py` (09 §9.13) |
| A metadata column | `lakehouse/tables.py` (ids 1–9 are taken), `lakehouse/rows.py::build_row` (the seal's and the live tail's rows), the console's `METADATA_COLUMNS` |
| A maintenance job | `lakehouse/maintenance.py` (planning, committing) + `maintenance_app/worker.py` (`TableMaintenance._next_job`) + `MAINTENANCE_JOBS` (`models/table.py`) + `services/table_maintenance.py` for a setting it needs |
| A REST catalog endpoint | `lakehouse/rest.py`, `web/catalog.py`, a WebView |
| A storage backend | `lakehouse/gcs.py`, `lakehouse/io.py`, `lakehouse/pointer.py` (§14.1) |
| A runtime agent or task | a `.j2` in `runtime_app/templates/` + `runtime/templates.py` |
| A runtime env var | `runtime_app/config.py` + the relevant `.j2` |
| A persisted control-plane entity | `storage/keys.py` + `storage/envelopes.py` + a model + `test_storage.py` |

## 14.12 Suggested roadmap, roughly by value

**Correctness and safety**

1. Done: the chart derives the scheduler topic from the app id, and the chart tests render every
   values file (§14.2).
2. Release twm-faust's commit-bookkeeping fixes (`fix-commit-gaps`) and move Kaspr onto the
   release, so ordered batches are needed only for a new materialization's first batch (§14.3).
3. Make Kaspr rebalance when a static member's assignment lacks topics it subscribes to (§14.3).
4. End a change of topics' drain by removing the runtime's static members instead of waiting out
   the session timeout (§14.1).

**Operability**

5. Export the heartbeat's and the tail app's counters as metrics; alert on `failed` statuses and
   `table_error` events (§14.7, §14.16).
6. Raise the shipped runtime resources and size runtimes per materialization through
   `resource_profile` (§14.4).
7. Let the sweep see materializations without a binding, so a failed discovery or table creation
   retries (§14.2).
8. Single sign-on (OIDC) on the seams 18 §18.13 describes; per-table grants for access tokens
   (§14.8).
9. Shorten a tail app member's restart for its tables — a standby for each table, or a seed that
   starts before the member's package install finishes (§14.16).

**Scale**

10. Faster commits for large bursts: cache manifests between commits, read footers ahead (§14.4).
11. Cache or incrementalize the sweep's spec rendering (hash the inputs, not the output).
12. Shared runtime mode for low-volume topics (§14.1).

**Capability**

13. A latest-per-key table for compacted topics, and delete files the console can read (§14.1).
14. Spark and Trino verified against the REST facade, and vended storage credentials so an engine
    needs no GCS key of its own (§14.1).
15. Iceberg v3 once PyIceberg writes it: `_unmapped` as a `variant` (§14.1).
16. Other object stores behind the FileIO and the pointer's conditional write (§14.1).
17. Avro and Protobuf topics.

## 14.13 What 1.0 resolved or retired

| 0.x entry | In 1.0 |
|-----------|--------|
| Duplicate rows after any restart (at-least-once) | **Exactly once.** The table's snapshot summaries carry the committed offsets; the committer refuses what the table holds (10 §10.6, §10.7) |
| Stopping a runtime lost its staged rows | Resolved in 0.7.0; in 1.0 offsets commit only once rows are in the table |
| The seed-schema wait could deadlock on a backlog of small records | **Gone.** No writer waits for a schema: records are staged at once and the committer evolves the table (10 §10.2) |
| A decision wait on a backlogged member blocked it for its budget; decisions reached one member; the decision-signal routing bug | **Gone** with the schema authority (09) |
| Same-window conflicts collapsed to `json` | Replaced by deterministic rules: observations committed together are applied in order (09 §9.3) |
| A materialized array kept its column after gaining element fields | **Gone.** The table schema is nested; there is no `materialized` flag (09) |
| Discovery sampled one message | It samples up to 200 records (09 §9.6) |
| Ingest partitioning used processing time | Partitions follow `_kafka_timestamp` (11 §11.3) |
| `raw_sample` on the proposals topic | **Gone.** Schema change requests carry shapes only (§14.5) |
| Commit markers and the object manifest; their compaction and its fleet cursor; a manifest part outliving a deleted destination | **Gone.** The Iceberg snapshot is the only index (11) |
| Silent destination fallback (`gs://unconfigured-destination`) | **Gone.** A runtime without a readable table reports `waiting_for_table` (10 §10.15) |
| Flat upload backoff, then a permanent skip | Resolved in 0.7.0: uploads retry with a doubling backoff, without limit |
| Backpressure did not stop consumption | Resolved in 0.7.0: source partitions pause at the backlog limits |
| `last_error` was sticky | A member clears a data or infrastructure error once it writes again (`runtime_app/state.py`) |
| Two runtime state systems | The table-backed module is gone; `runtime_app/state.py` is the only one |
| A runtime spec without resource limits | The chart renders one runtime spec for every installation, limits and `topicAllowDeclare` included (§14.2) |
| No `onError` status selector on WebViews: errors might answer `200` | Without one, Kaspr answers with the raised `WebError`'s `status_code` and body (`build_error`, Kaspr's `webview/response.py`) — a `400` for an unknown maintenance job on the lab |
| `DEFAULT_MANAGED_AGENT_NAMES = ["writer", "schema-proposer"]` | Removed: the agent names come from the rendered template |
| The authority-update fan-out (O(materializations²)) | **Gone** with the authority topic |
| Compaction held back every partition written in the last hour, and started one partition an interval | Fixed before release: only the current partition waits, small partitions compact together, and a backlog proceeds a job a tick (§14.6) |
| Fork-aware logical view for hand-written SQL (console roadmap) | The console reads forks as one logical column (console 06) |

## 14.14 What 2.0 resolved

| 1.0 entry | In 2.0 |
|-----------|--------|
| One topic per materialization | A materialization reads up to `max_materialization_topics` topics, one table each, in its own namespace (04 §4.5) |
| A materialization's topics are fixed | An edit adds and removes them; a removed topic's table is kept read-only or dropped (07 §7.9a) |
| Every runtime sized for compaction, which ran on its committer member | The maintenance app runs it for every materialization, sized on its own (10 §10.10) |
| Maintenance starts over after every committer start | The ticks carry each job's last run from the table record; a restarted member reports a job it was running abandoned |
| No maintenance while a materialization is stopped | The maintenance app maintains stopped materializations' tables, and kept ones |
| The `maintenance` agent wrote and deleted other materializations' records from the `__system__` event | It hands each purge and measurement to an event keyed by its materialization (§14.3) |

## 14.15 Datasets: open items

- **Explore** — queries across materializations and datasets in the console — is a separate scope.
  This release ships what it needs: the `/datasets` endpoints to browse and resolve
  `<dataset>.<table>`, and per-dataset permissions in `GET /auth/session`. The console's run engine
  still reads one materialization's tables; its statements name tables by `<dataset>.<table>`.
- **Renaming a dataset** is not offered. Datasets are keyed by id with the name as an index, so it
  stays possible, but every engine's SQL names the old name.
- **Labels or tags** on datasets, and **descriptions on tables**, for search: not built; datasets
  have a description.
- **Bulk creates** are serialized through the directory agent with every other directory change, a
  few writes each — ample at console scale.

## 14.16 The live tail: open items

Handbook 19 has the model; these are its limits as built in 4.1.

- **A member restart is a short outage for its tables.** A table lives on one member, with no
  standby. A restarted member takes its partitions back (static membership) and seeds its tables
  again from the committed offsets: on the lab their newest records were unavailable for 30–60 s, the
  console saying so (unavailable, moving between members, warming up) and showing committed rows.
- **Scaling the tail app restarts every member.** The operator rolls the StatefulSet's pods on any
  change to the KasprApp's spec, a replica count included, so a scale costs each member's tables the
  outage above, one member after the other.
- **The bound can be passed by one fetch.** A table pauses once it holds `live_tail_max_rows_per_table`
  rows, after the fetch that took it there: on the lab, 1,221 rows against a bound of 1,000.
- **Each member holds transactional producers it does not use.** Kaspr runs every app with Faust's
  exactly-once processing: one producer per tick partition a member is assigned, 32 each with two
  members (19 §19.3). Kaspr has no switch for it.
- **The deployment-wide switches reach open consoles within two minutes.** `live_tail_enabled`
  changed elsewhere than the console's Settings page shows in the console within a minute (its cache
  of the control plane's settings), and each open Data tab reads the console's config every minute.
  A materialization's own switch, and a stop, show at once.
- **Live re-reads the table's pointer.** Each refresh checks for a new snapshot, at most every 2 s per
  table (`snapshotCacheMs`): one small GCS read.
- **Settling rows.** A record whose new fields await a schema change has them in `_unmapped` until
  the runtime commits the change — seconds to minutes; the console counts them.
- **No metrics.** The tail app reports through its health view and log events only; nothing is
  exported to a metrics system (§14.7).

## 14.17 What 4.1 resolved

| Entry | In 4.1 |
|-------|--------|
| Settings revisions restarted if the `settings` table was lost, and runtimes ignored every delivery until the new revision passed theirs. A control plane installed afresh over a Kafka cluster that kept its delivery topic did the same to the runtimes created after it, and its rollout called them `current` (on the lab, a new runtime held revision 14 against the control plane's 3) | Runtime settings have a **lineage**, begun when the control plane first starts on settings without one (`settings-bootstrap`). Every delivery carries it; a runtime keeps the newest by lineage, then revision, and the rollout counts a member `current` only on the current lineage and revision (17 §17.7) |
| On a broker that creates topics on demand (Kafka's default), deleted topics came back, empty, with one partition: the control plane described topics by name — for the lag refresh every minute, topic analysis, and edits, so describing a misspelt topic made it — and its and the tail app's offset lookups left aiokafka naming every topic they had looked up in each metadata refresh. On the lab, 52 topics deleted after their materializations came back at once | No request names a topic: topics are described from a read of every topic, and the clients that look up offsets track none (`kafka/metadata.py::track_no_topics`; 19 §19.7) |
| The console's GCS reads printed `MaxListenersExceededWarning` twice per object — every few seconds while a Data tab follows a table, whose pointer it re-reads | The console reads objects with a plain GET through GCS's JSON API, with the client library's endpoint, credentials, retries and CRC32C check, not through its download stream (console 06) |

## 14.18 What 4.2 resolved

| Entry | In 4.2 |
|-------|--------|
| The shipped runtime spec limited each member to one core, so a seal shared it with the worker; and every member of every runtime requested the spec's CPU, busy or quiet (§14.4) | Members are limited to two cores, and a VerticalPodAutoscaler sets each runtime's CPU request from what its members use, in place; so are the control plane's, the maintenance app's and the tail app's (02 §2.10c). Memory and `resource_profile` stay open (§14.4) |
