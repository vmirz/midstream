# 19 — The live tail

How a table's records reach the console within seconds of being produced: the tail app, which
reads each tailed table's topic and serves the records its runtime has not committed yet, and how
the console joins them to the committed snapshot so that every record is shown exactly once.

Source: `tail_app/` (`worker.py`, `table_tail.py`, `buffer.py`, `ingest.py`, `serve.py`, `views.py`,
`routing.py`, `consumer.py`, `auth.py`, `peek.py`, `config.py`, `http.py`, `control_plane.py`),
`services/live_tail.py`, `models/live_tail.py`, `lakehouse/rows.py`; chart templates:
`charts/midstream/templates/tail/`, `charts/midstream/templates/webviews/tail-*.yaml`, the maintenance agent's third output
(`charts/midstream/templates/agents/maintenance.yaml`); console: `console/server/data/tail.ts`, `console/server/runs/tail.ts`,
`console/src/features/data/live.ts`. Design: `docs/design/few-second-freshness.md`.

## 19.1 The model in one paragraph

A runtime commits a table every few minutes, so a committed snapshot is minutes behind its topic.
The **tail app** (`<release>-tail`, a KasprApp of its own) reads the topic of every table someone
is watching and keeps the records past the table's committed offsets, built into rows by the same
code the seal uses (`lakehouse/rows.py::build_row`). A console run pins the table's current snapshot,
then asks the tail app for the records **past that snapshot's offsets** and reads them as one more
Parquet file. The snapshot holds every record up to its offsets and the tail every record after
them, so together they hold each record exactly once. The Data tab's **Live** mode asks again every
1.5 s for the records past what it already has, and starts a new run when a commit lands. Every
materialization has a live tail; the deployment's `live_tail_enabled` turns it off for all of them.

## 19.2 Exactly once: the snapshot and its tail

A snapshot's summary carries `materializer.offsets`: per Kafka partition, the last offset the
snapshot holds (handbook 11). For a run pinned to that snapshot:

| | Holds, per partition `p` |
|---|---|
| the snapshot | every record with `offset ≤ offsets[p]` |
| the tail, asked with `after = offsets` | every record with `after[p] < offset ≤ through[p]` |
| each later fetch, asked with `after = through` of the one before | the next records, `through_prev[p] < offset ≤ through_new[p]` |

The tail answers `through[p]` for every partition it holds, and `through[p] ≥ after[p]` always. The
pieces are disjoint and follow on from each other, so a run holds every record up to `through`
exactly once. A record that needs no row — one Kaspr's `json` serializer cannot decode, a tombstone
of a materialization that skips them, an offset with no record (compaction, a transaction marker) —
is in neither, and `through` still moves past it.

When a commit lands, Live starts a new run pinned to the new snapshot, which asks the tail past the
new offsets: the records that were in the tail a moment ago are now in a data file, each exactly
once. A partition missing from `after` is read from its beginning: a table with nothing committed
yet is read from the tail alone.

The tail keeps committed records for `live_tail_retention_seconds` (120) after the commit that
covered them, so a run pinned a moment before a commit still finds its tail. An `after` older than
what the tail holds is refused (`409 tail_unavailable`, `behind_retention`) rather than answered with
a hole. Records Kafka's retention removed before the tail read them are the one exception: the
runtime cannot have them either, so the tail answers from Kafka's beginning and names the partition
in `X-Tail-Gap`.

`python/tests/test_tail_exactly_once.py` drives the real app — worker, buffer, fetch plan, answers —
through random schedules of produce, commit, ticks, restarts, bursts, schema changes, parking and
incremental fetches, and checks after every answer that the snapshot and its tails hold every record
exactly once; 150 schedules run with the suite, and 10,000 passed once. On the lab it held at every
poll across commits, a compaction, runtime restarts and rebalances of the tail app (§19.17).

## 19.3 The tail app

| Resource | Base name | What it is |
|---|---|---|
| KasprApp | `<release>-tail` | 2 replicas, 1 CPU / 2 GiB requested, 2 CPU / 4 GiB limit, the CPU request then set from use (02 §2.10c), web port 6065 |
| KasprAgent | `tail-worker` | Reads `<release>-tail-requests` (64 partitions): ticks and cancels |
| KasprTask | `tail-step` | Every 5 s: the member's housekeeping (map refresh, trims, parking, forgetting) |
| KasprWebView | `tail-rows`, `tail-head`, `tail-health`, `tail-members` | The HTTP API (§19.9) |

The app keeps no state of its own: no KasprTable, no changelog. Everything a member holds is in
memory and comes back from Kafka and the control plane after a restart. Its labels are
`kaspr.io/app: <release>-tail`; its topics, control-plane URL and Secret derive from the release
(§19.12).

Kaspr runs every app with Faust's exactly-once processing (`kaspr/app.py`), so each member holds one
transactional producer per tick partition it is assigned — 32 each with two members — although the
tail app produces nothing. It is a fixed, small cost (§19.15); Kaspr offers no way to choose another
guarantee.

## 19.4 Ticks and cancels

The control plane names every tailed table once a minute, from the same scheduled step that ticks
the maintenance app (`handlers/maintenance.py::_table_maintenance`, `services/live_tail.py`):

- a **`tail_tick`** for every table its runtime writes (`state: active`) of a materialization that
  is running, carrying the target (materialization, dataset, table, location,
  topic, partitioning, table UUID, `skip_tombstones`), the nine live-tail settings as resolved that
  minute, and hints from the table's record: committed offsets, schema id, metadata location;
- a **`tail_cancel`** for every other table still in the catalog, with a reason:
  `table_dropping`, `table_retired`, `materialization_deleting`, `materialization_stopped`. The app
  still takes `live_tail_off`, from a control plane before 4.3, for a materialization switched off.

With `live_tail_enabled` off nothing is sent. Every message is keyed `tail:<materialization>:<table>`
(`live_tail_routing_key`), so all of a table's messages land on one of the 64 partitions of
`<release>-tail-requests`, and the member assigned that partition owns the table. A tick's hints
lag the table by up to a minute and are never trusted over what the member has seen itself; they
seed a table and catch a missed event. Topic contract: handbook 06 §6.18.

## 19.5 Ownership and routing

The **owner** of a table is the member Faust assigned the tick partition its key hashes to: Kafka's
default partitioner, murmur2 of the UTF-8 key, `& 0x7fffffff`, modulo 64 (`routing.py::owner_partition`,
verified against the partitions live ticks arrived on). A member reads its own assignment from the
Faust app on every request; a request for a table it does not own is **forwarded once** to the
owner, named by the **member map** — the app's consumer group as `DescribeGroups` reports it, each
member's assignment and the URL Faust publishes in its metadata (`K_WEB_HOST`, `K_WEB_PORT`).

- The map is read every `TAIL_MEMBER_MAP_REFRESH_SECONDS` (15) and again when a
  request names a partition it does not know.
- A forwarded request carries `X-Tail-Forwarded-By`; a member receiving one for a table it does not
  own answers `409 tail_rebalancing` instead of forwarding again. One hop, never a loop.
- The forwarder checks the caller can query some dataset (`can_anywhere("data.query")`) before it
  forwards; the owner checks the table's own dataset (§19.10).
- A tick that arrives on a partition its key does not hash to is counted (`routing_mismatches` in the
  health view) and logged `tail_routing_mismatch`: it would mean the partitioner differs.
- A member that owns a table no tick has named yet — a materialization created a few seconds ago —
  asks the control plane for it (`GET /materializations/{id}`) and tails it if it is running (and,
  from a control plane before 4.3, does not say its live tail is off).

A rebalance moves tick partitions between members, and with them their tables: the member that
gained one seeds its tables on the next request or tick; the one that lost it drops them
(`tail_rebalanced` is logged with what moved). Static membership brings a restarted member back to
its own partitions.

## 19.6 A table's life

| State | Means | A request is answered with |
|---|---|---|
| `parked` | Known from ticks, but nobody asked for it lately: nothing is read | `503 tail_not_ready` after the wait, while it starts |
| `seeding` | Fetching its schema and placing its partitions | `503 tail_not_ready` (`details.state`) |
| `catching_up` | Reading from the committed offsets towards the head | rows, `X-Tail-State: catching_up` |
| `live` | At the head; each fetch returns within `live_tail_fetch_wait_ms` | rows |
| `paused` | At its bound, its rate ceiling or the member's budget: some partitions wait | rows, `X-Tail-Status: truncated` |
| `backlogged` | The runtime is too far behind the topic for the tail to apply | `409 tail_backlogged` with `behind` |
| `unavailable` | Its topic cannot be read (deleted, no leader for long) | `503 topic_unavailable`; retried every 30 s |

**Activation.** A table is read only while someone asks for it. A request activates a parked table
and waits up to `wait_ms` (at most 5 s; the console sends its timeout minus 500 ms) for it to become
answerable. A table nobody asked for in `live_tail_idle_seconds` (600; 0 keeps every table on)
parks: its buffer is dropped and its partitions leave the consumer. A table no tick has named for
`TAIL_FORGET_AFTER_SECONDS` (300) is forgotten.

**Seeding.** The schema comes from the control plane (`GET …/tables/{table}/schema?raw=true`: the
Iceberg schema, fork map, last column id and rejected paths) with the service credential, and from
the table's GCS metadata when the control plane cannot answer (logged, `seed_source: gcs`). Each
partition starts at the table's committed offset plus one — the newest the member knows from the
tick, an event or the table — so the tail holds nothing the table holds already.

**Backlog.** A table seeded with more than `live_tail_max_rows_per_table` (250,000) records between
its committed offsets and the head — a new materialization's backfill — is `backlogged` from the
start: it reads nothing and holds nothing, and the console says how far behind the runtime is. An
active table enters it when the runtime falls more than twice the bound behind (a burst). It leaves
when the runtime is within 75 % of the bound, re-seeding from the committed offsets. While a table is
backlogged its end offsets are still refreshed, so `behind` stays current.

## 19.7 Reading Kafka

**One consumer per member.** Every active table's partitions are assigned to one aiokafka
consumer — groupless, nothing committed, `read_committed` as the runtime reads (Kaspr's apps are
transactional, so Faust reads committed records only) — so a member tailing hundreds of tables keeps
one set of broker connections and one fetch loop (`consumer.py`, `worker.py::fetch_once`).

**Shared topics.** Two materializations may read one topic and one member may own both tables. A
topic partition is then read once, from the lowest next offset of the tables that want it, and each
table takes only a fetch that started at or below its own next offset. A paused table does not want
its partitions; one nobody wants is paused on the consumer. aiokafka forgets every position when the
assignment changes, so assigning a new table re-seeks the member's other partitions: a hiccup of a
few hundred milliseconds for its other tables.

**Pace.** The broker holds each of the member's fetches until 64 KiB are new
(`SOURCE_FETCH_MIN_BYTES`) or `live_tail_fetch_wait_ms` (200) has passed, so a member following many
quiet tables wakes a few times a second, not once a record: after every fetch it goes over all of
its active tables. A busy table fills the fetch at once. `live_tail_max_records_per_second_per_table`
(5,000) caps how fast one table is read, so one topic's burst cannot slow a member's other tables; a
table producing faster falls behind at that pace and says so (`X-Tail-Lag-Ms`).

**Lag.** `lag_ms` is 0 while every partition's last fetch reached the head, and otherwise the time
since it last did — so a partition without a leader holds back only itself and reports how long.

**No topic named in vain.** A metadata request that names a topic makes a broker that creates
topics on demand (Kafka's default) create it if it is missing. A member's other consumers — the
one that looks up offsets for seeding and backlogs, the peek's — never leave a topic tracked after
a lookup (`kafka/metadata.py::track_no_topics`), and a topic's partitions are read from a request
for every topic (`ConsumerAdapter.partitions_for`). Otherwise aiokafka keeps every topic it once
looked up and names them all in each refresh, every five minutes: on the lab, the topics of
deleted materializations came back, empty, with one partition each. The source consumer names
the topics it reads, as every consumer of them does, until the topic check lets a deleted one go.

## 19.8 The buffer and its rows

**Rows.** A record becomes the staged line the writer would stage (`{"m": {...}, "v": value}`, the
value decoded as Kaspr's `json` serializer decodes it: `json.loads` of its UTF-8, the timestamp
converted from milliseconds to seconds as Faust does), then a row by `build_row` with the table's
current schema and fork map — the seal's own code (handbook 10 §10.4). A record that cannot be
decoded gets no row and its offset is passed; a tombstone is skipped when the materialization skips
them. A row whose values went to `_unmapped` and that the writer's drift detection would turn into a
schema change is **settling** (`X-Tail-Settling`): once the change is committed, the seal puts those
values in columns, and so does the tail, which rebuilds held segments under the new schema from their
staged lines.

**Segments.** Each partition's rows accumulate in an open segment that closes at 2,000 rows, 4 MiB
of staged lines or 5 s, into an Arrow batch plus its staged lines compressed with zstd (level 1).
A partition covers `[covered_from, next_offset)`: every record in that range is either held or needs
no row.

**Bounds.** A table holds at most `live_tail_max_rows_per_table` uncommitted rows and
`live_tail_max_bytes_per_table` (256 MiB) resident bytes; at either it pauses, and resumes below
75 %. The member's resident bytes across its tables stay under `TAIL_MEMORY_BUDGET_BYTES`
(2.5 GiB of the 4 GiB limit): past it, retained (committed) segments go first, then the largest
table pauses. Committed segments are trimmed `live_tail_retention_seconds` after their commit.

## 19.9 The HTTP API

The tail app's WebViews, under `/tail/v1`. Every one requires the console's credential in
`X-Console-Token` (the console's service token) and a person's bearer token: the endpoint is
the console's alone, and the person is authorized on each request (§19.10).

**`GET /tail/v1/materializations/{id}/tables/{table}`** — the records past `after`.

| Query | |
|---|---|
| `after` | Required. JSON object, partition → the last offset the caller has (−1 for none); a partition left out is read from its beginning |
| `max_rows`, `max_bytes` | Caps for this answer; never above the table's bounds |
| `wait_ms` | How long a table that is warming up may be waited for (≤ 5,000) |
| `format` | `parquet` (default) or `json` (tests and debugging; at most 1,000 rows) |

`200`: one Parquet file in the table's own physical format — its Arrow schema with field ids, zstd —
holding every row past `after` up to `through`, newest last; rows of several partitions share it, as a
compacted file's do. Headers:

| Header | |
|---|---|
| `X-Tail-Rows`, `X-Tail-Bytes` | The rows and the body's length |
| `X-Tail-After`, `X-Tail-Through` | The request's `after`, normalized; per partition the last offset the answer covers (JSON) |
| `X-Tail-End-Offsets` | Per partition, the topic's last offset as last seen |
| `X-Tail-Status` | `complete`, `truncated` (a cap or a paused partition stopped it short), `empty` |
| `X-Tail-Paused`, `X-Tail-Gap` | Paused partitions; partitions answered from Kafka's beginning (§19.2) |
| `X-Tail-Schema-Id`, `X-Tail-Settling` | The schema the rows were built with; rows waiting for a schema change |
| `X-Tail-Time-Min`, `X-Tail-Time-Max` | Epoch ms of the rows' Kafka timestamps, floored exactly; absent when no row has one |
| `X-Tail-Lag-Ms`, `X-Tail-State` | §19.7, §19.6 |
| `X-Tail-Member`, `X-Tail-Forwarded` | The owner that answered; `1` when another member forwarded the request |
| `X-Tail-Table-Uuid`, `X-Tail-Generated-At` | Provenance |

**`GET …/tables/{table}/head?rows=200`** — the newest `rows` records on the topic (at most
`live_tail_peek_max_rows`, 1,000), committed or not, built as rows: what the console shows while a
table is backlogged. Read with a consumer of its own, cached for a second. Headers
`X-Tail-Peek-Rows`, `X-Tail-Peek-Offsets` (per partition the first and last offset shown),
`X-Tail-End-Offsets`, `X-Tail-Behind`, `X-Tail-State`, `X-Tail-Schema-Id`, `X-Tail-Settling`,
`X-Tail-Member`, `X-Tail-Generated-At`.

**`GET /tail/v1/health`** — this member: its partitions, tables (state, partitions, committed, end
and next offsets, rows, bytes, `behind`, `lag_ms`, last request and tick, counters), resident bytes
against its budget, counters (`ticks`, `cancels`, `malformed`, `routing_mismatches`, `forwarded`,
`served`) and its loops. **`GET /tail/v1/members`** — the member map: the group's state, each
member's partitions and URL, the partition → member table; `?health=true` adds every member's
health, asked of each. Both need `materializations.view_details`, which the console's service
credential holds.

**Errors** are the control plane's error body, `{"error": {"code", "message", "details"}}`, sent as
JSON (`errorContentType`; Kaspr 0.11.30 and kaspr-operator 0.19.7 or later):

| Status | `details.reason` | When |
|---|---|---|
| `400` | the field | A bad `after`, `max_rows`, `format`; a name that is not one |
| `401` | `sign_in_required`, `session_ended`, `token_revoked`, … | No usable session, as the control plane says |
| `403` | `console_only` | `X-Console-Token` missing or wrong |
| `403` | the permission and dataset | No `data.query` on the table's dataset; `password_change_required` |
| `404` | `live_tail_off` | The table is not tailed: not running, dropping, not ticked yet, unknown |
| `409` | `tail_unavailable` (`details.partitions`) | `after` is older than the tail holds: a newer snapshot has a tail |
| `409` | `tail_rebalancing` | The table is moving between members |
| `409` | `tail_backlogged` (`behind`, `end_offsets`, `committed`) | §19.6 |
| `503` | `tail_not_ready` (`details.state`, `Retry-After: 1`) | Warming up past `wait_ms` |
| `503` | `topic_unavailable`, `tail_owner_unreachable`, `control_plane_unreachable`, `peek_failed` | |

## 19.10 Who may read a tail

1. **The console alone.** `X-Console-Token` must equal the service token the app was given
   (`TAIL_SERVICE_TOKEN`, from the same Secret as the control plane's); `403
   console_only` otherwise, on all four views.
2. **The person, checked with the control plane.** The bearer token is resolved with
   `GET /auth/session` and the principal rebuilt from its description
   (`identity/principals.py::principal_from_description`), which decides exactly as the control
   plane would — on every dataset, a token's dataset limit included. A check is trusted for 5 s
   (`TAIL_SESSION_CHECK_SECONDS`), and for 60 s while the control plane cannot be
   reached; past that, `503 control_plane_unreachable`. At most 10,000 sessions are cached.
3. **On the table's dataset.** Rows and the peek need `data.query` on the table's dataset; health and
   members need `materializations.view_details`. The service credential reads health and members,
   never rows.

The console calls the tail app with the person's own session, the same token it calls the control
plane with (handbook 18 §18.9); no row reaches a log line, and neither does a token.

## 19.11 The console's side

In brief; web 07 §07.9 has the detail.

- `CONSOLE_TAIL_URL` names the tail app (in a cluster its Service; from a laptop the HTTPRoute's
  `/tail/` rule on the control plane's host). Unset, the console shows committed rows only.
- A run of a table fetches the tail past its snapshot's
  offsets and keeps it as a Parquet file in the run's own directory: an object like any data file,
  first in newest-first order, counted, filtered, pruned by its time bounds, and read by statements
  as part of `<dataset>.<table>` and `iceberg.<dataset>.<table>`.
- `POST /api/runs/:runId/refresh` appends only the records past the tail's `through` to a finished
  browse run (its `revision` bumps), or starts a new run in its place when the table has a newer
  snapshot, the run failed, or it is a statement.
- Live refreshes every `liveTailRefreshMs` (1.5 s) while the tab is seen, on the first page, with no
  row open; a table with nothing committed yet is queryable through its tail alone.
- A tail that cannot be had is the run's status, never its failure: the run shows committed rows and
  says why. The tail app's `404 live_tail_off` — a table not followed there yet, just after a resume
  or before its first tick — is `unavailable` (`not_tailed`) and asked again on the next refresh.
- **Switches.** Live follows every materialization while it runs, in a console and a control plane
  that have the live tail on. The toggle goes as soon as the materialization is stopped (the
  console's live view of it), and for a deployment-wide switch within two minutes: the console
  reads the control plane's settings at most a minute old —
  at once after a change saved on its own Settings page — and each Data tab reads the console's
  config again every minute. A run whose tail a switch turned off, or whose materialization was
  stopped, is replaced by a new run once it is on again, so Live picks the tail back up by itself.

## 19.12 Configuration

**The tail app** — fixed by the deployment, read once at start (`tail_app/config.py`):

| Variable | Default | |
|---|---|---|
| `TAIL_APP_NAME` | `<release>-tail` | The KasprApp's name (`develop-` prefixed in dev) |
| `TAIL_CONTROL_PLANE_URL` | — | The control plane's Service, for sessions, schemas and tables |
| `TAIL_SERVICE_TOKEN` | — | From the auth Secret's `service-token`; at least 32 characters |
| `TAIL_TABLE_EVENTS_TOPIC` | `materialization-table-events` | Commits and schema changes |
| `TAIL_REQUESTS_TOPIC` | `<release>-tail-requests` | Ticks and cancels (also the `tail-worker` agent's input) |
| `TAIL_MEMORY_BUDGET_BYTES` | 2,684,354,560 | §19.8; keep it well under the pod's limit |
| `TAIL_FORGET_AFTER_SECONDS` | 300 | §19.6 |
| `TAIL_SESSION_CHECK_SECONDS` | 5 | §19.10 |
| `TAIL_SERVE_CONCURRENCY`, `…_PEEK_CONCURRENCY` | 4, 2 | Answers and peeks built at once |
| `TAIL_MEMBER_MAP_REFRESH_SECONDS` | 15 | §19.5 |
| `RUNTIME_MEMBER_ID` | the pod's name | The member's name |

Kafka is reached with Kaspr's own `K_KAFKA_*` variables. **The control plane's** nine settings
(`live_tail_*`, Settings › Live tail) are in handbook 17 §17.3; every tick carries them, so a change
reaches the tail app within a minute. **The console's** four (`liveTailEnabled`,
`liveTailRefreshMs`, `liveTailMaxRows`, `liveTailTimeoutMs`) are in web 03 §3.10.

## 19.13 Observability

- **Health and members** (§19.9), through the console's service credential or from inside the
  cluster: which member owns what, each table's state and figures, and the counters.
- **Log events** (with protocol logging on): `tail_served` (table, rows, bytes, ms, status, member,
  forwarded), `tail_forwarded`, `tail_table_seeded`, `tail_fetch_paused`, `tail_rebalanced`; warnings
  for a seed that cannot read a schema, a topic gone, a malformed message, and `tail_routing_mismatch`.
- **The console** says, in plain words, how far behind the newest records the Data tab is — in amber
  on its status line — and otherwise a few muted words there (starting,
  reconnecting, paused, new fields, seconds behind); its More actions menu enables the head peek while
  a table is backlogged (web 11 §11.8).

## 19.14 Failure modes

| Situation | What happens |
|---|---|
| The tail app is down or unreachable, `CONSOLE_TAIL_URL` wrong | Runs show committed rows and say the live tail is unavailable; Live keeps asking |
| A member restarts | Static membership brings it back to its partitions; its tables re-seed from the committed offsets on the next request or tick |
| Members are added or removed | Tables move with their tick partitions; a request meanwhile is `409 tail_rebalancing`, retried on the next refresh |
| A table was just created, activated or moved | `503 tail_not_ready` until it is answerable; the console's wait covers a quiet table's activation |
| A partition loses its leader | Only that partition stops; `lag_ms` says for how long |
| The control plane is unreachable, or restarting | Sessions are trusted for the grace; seeding falls back to GCS; ticks resume after. A restarted control plane answers `503 starting` until it has read its tables back (18 §18.7), which the tail and the console ride out the same way |
| The source topic is deleted | `unavailable`, retried every 30 s; nothing else on the member is affected |
| A burst beyond the bound, or a backlog | `truncated` (paused), or `backlogged` past twice the bound; the console says which and how far |
| A schema change in flight | Rows settle: `_unmapped` until the change is committed, then rebuilt |
| A snapshot older than the retention | `409 tail_unavailable`: a new run, on a newer snapshot, has its tail |
| The materialization is stopped or deleted | Cancelled at the next tick (a table no longer tailed answers `404 live_tail_off`); the console hides Live at once and asks for no tail. Resumed: the toggle is back at once and the tail within a minute |
| `live_tail_enabled` is turned off | No ticks are sent; each member forgets its tables within the forget window; consoles stop asking within two minutes (§19.11) and pick the tail back up the same way once it is on |
| A Parquet answer cannot be read | The console drops it and says `unavailable`; the run is not failed |

## 19.15 Capacity and cost

Measured with the tail's own code on 100,000 records shaped as the load-test topic's (1.1 KB each,
28 columns), one core, inside a tail app pod on the lab (the laptop's figures in brackets):

| Measurement | Result | Plan threshold |
|---|---|---|
| Ingest: decode, stage, `build_row`, Arrow | 101 µs per record, 9,900 records a second a core (114 µs) | ≤ 120 µs |
| Resident bytes per raw byte buffered | 1.06 (1.06) | ≤ 1.3 |
| Serve 10,000 rows to Parquet | 28 ms p95 (60 ms) | ≤ 200 ms |
| Serve 100,000 rows to Parquet | 193 ms p95 (436 ms) | ≤ 1.5 s |
| Activation of a quiet table: request to `live` | 0.84 s p50, 0.99 s p95, 1.12 s at most over 50 tables | ≤ 2 s |
| Re-assign hiccup for a member's other tables | none seen in `lag_ms`, sampled every 0.3 s through 48 activations | < 1 s |
| Member map refresh (the members view: `DescribeGroups`, decode, HTTP), in the pod | 0.7 ms p50, 1.3 ms p95 | ≤ 50 ms |
| Head peek of 200 rows over 3 partitions, in the pod | 6.5 ms p50, 59 ms p95; a million rows asked is 1,000 shown | ≤ 300 ms p95 |

**Fifty tables on two members** (lab step 10: 50 topics of three partitions at 5 records a second,
in three materializations): 32 tables on one member, 18 on the other. The first member, at 160
records a second, used 0.044 cores and 194 MiB (5.7 MB of it buffers), the second 0.032 cores and
175 MiB: about 1.4 millicores a quiet table. With one of the tables at 1,000 records a second more,
the first member used 0.068 cores (about 60 µs a record of 150 bytes) and 225 MiB for 153,000 rows
held. A member idle with nothing active uses about 0.01 cores and 150–200 MiB: Python, Faust and its
producers.

A member's memory is roughly its tables' uncommitted records' raw size, plus Python and the Arrow
pools; a table's uncommitted window is its runtime's commit interval times its rate. Sizing: handbook
12 and the design's §11.1, which these figures bear out.

## 19.16 Operating it

**A tail that is behind.** Open the table's Data tab; the status line names the state. The health
view shows the table's `state`, `behind` (records between the committed offsets and the head) and
`lag_ms`. `backlogged`: the runtime is catching up — the tail starts once it is within 75 % of the
bound. `paused`: the table is at its bound or rate ceiling; raise `live_tail_max_rows_per_table` or
`live_tail_max_records_per_second_per_table` if it is routine.

**A tail that is moving.** `GET /tail/v1/members` shows the group's state and who owns which
partition. `tail_rebalancing` answers last a refresh or two; a member stuck out of the group shows
in the map.

**A tail that is unavailable.** The table's `last_error` in the health view says why: the topic, the
control plane, the schema. A table that cannot seed is retried; one whose topic is gone every 30 s.

**From a laptop.** The lab's HTTPRoute sends `/tail/` on the control plane's host to the tail app's
Service, so a console run locally can set `CONSOLE_TAIL_URL` to that host.

## 19.17 Verification

Unit and model tests: handbook 13. Lab validation of 4.1 (steps of the design's §13.4): 13 §13.8e,
with the console's browser checks in web 14 §14.4.

## 19.18 Decisions that differ from the design plan

| Plan | As built | Why |
|---|---|---|
| The tail reads `read_uncommitted` | `read_committed` | Kaspr apps run Faust's exactly-once processing, which reads committed records only; the tail must see what the runtime sees |
| Ticks for tailed tables only | Also a cancel for every other table in the catalog | A table stops being tailed at once, not after the forget window |
| A member waits for a tick before tailing a table | A member that owns an unticked table asks the control plane | A new materialization is tailed within seconds, not a minute |
| Ownership from the member map | From the Faust app's own assignment, read live; the map is the fallback and names the owner to forward to | A member's assignment is known to it at once; the map lags a rebalance |
| "Settling" left to be defined | A row with `_unmapped` values the writer's `DriftDetector` would raise a schema change for | The same rule the writer applies |
| — | Paused and backlogged tables keep refreshing their end offsets | `behind` stays true while nothing is read |
| — | An unavailable table is retried every 30 s, and answers `topic_unavailable` meanwhile | A deleted topic must not cost a seed per request |
| The forwarder checks the dataset | The forwarder checks `data.query` on some dataset; the owner checks the table's | The forwarder does not know the table's dataset; the owner does |
| Errors as JSON in an octet-stream body | `errorContentType: application/json` | Kaspr 0.11.30 gave a WebView's errors a content type of their own (design D22) |
| The Data tab waits for the first commit | A table is queryable as soon as it exists, through its live tail | Otherwise a new materialization would show nothing for its first ten minutes |
| A switch per materialization, `live_tail`, with `live_tail_default` for new ones (4.1) | None: every materialization has a live tail, and only the deployment's `live_tail_enabled` turns it off. The API still answers `live_tail: true` and takes `true` from a 4.1 console; a control plane before 4.3 still has its `false` and its `live_tail_off` cancel honoured | Every materialization gets the benefit; the owner's decision after 4.1 |
| A refused tail is `unavailable` with reason `off` | `unavailable` with reason `not_tailed`, asked again on every refresh | A resumed table, or one not ticked yet, comes back by itself |
| — | A stopped materialization is off for the console: no Live toggle, no tail requests (`stopped`) | The control plane tails a running materialization only |
| — | A run whose tail a switch turned off is replaced once the switch is on again | Otherwise an open tab would follow nothing until someone pressed Run |
| The source consumer's `fetch_min_bytes` 1 | 64 KiB, with `fetch_max_wait_ms` the fetch wait | With 1, a trickle across fifty tables woke the fetch loop for every record, and each pass went over every active table: 0.72 cores for 33 quiet tables on the lab, 0.044 after (with a segment's size read once). It adds up to the fetch wait, 200 ms, to a quiet table's records, which the budget's p95 already allowed |
