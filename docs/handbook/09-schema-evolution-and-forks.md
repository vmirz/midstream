# 09 — Schema Evolution and Forks

Every table of a materialization has an Iceberg schema that follows its topic **hands-free**:
nobody approves a field, and no record ever waits for one. A materialization of several topics has a
schema per table — one topic's new field never touches another's table. This document covers how a value becomes a
field, what happens when a key's values change type (forks, then JSON), the caps that keep a
pathological topic from growing a pathological schema, and how the schema travels to the runtime's
members, the control plane, the API and the console.

Source: `lakehouse/schema/infer.py`, `lakehouse/schema/policy.py` (the rules),
`lakehouse/schema/forkmap.py`, `lakehouse/schema/index.py`, `lakehouse/schema/conform.py`,
`lakehouse/schema/render.py`, `runtime_app/schema/drift.py`, `runtime_app/committer.py`
(the schema window), `schema/discovery.py`, `services/table_events.py`,
`services/schema_projection.py`.

> 1.0 replaced the 0.x *schema authority* — proposals, a control-plane debounce, promotion, writers
> pausing until a decision — with the policy below, applied by each runtime's committer to its own
> table. There is no `schema-proposals` agent, no proposal or decision event, and no
> `schema_version`-in-the-path layout any more.

## 9.1 Principles

1. **The table's schema is the truth.** It lives in the table's metadata file; the control plane
   keeps a projection of it (§9.9) and the runtime's state table a copy (§9.7), both derived.
2. **The policy always has a safe move**, so it never blocks a writer and never asks anyone (§9.3).
3. **Nothing is ever dropped or renamed.** A field that stops being written stays: readers of old
   snapshots need it, and so do the console's logical columns, which fold a key's forks back into
   one column (§9.10).
4. **No value is ever lost.** A value no field can take is kept, as JSON, in the row's `_unmapped`
   column (§9.8).
5. **One writer.** Only the runtime's committer commits schema changes, one event at a time
   (spec §7.6), so the schema can never race itself.

## 9.2 Observations: what a value says about its type

`infer(value)` (`lakehouse/schema/infer.py`) turns a JSON value into an *observation*, plain JSON so
it can travel on a topic and be compared as text:

| Value | Observation |
|---|---|
| `null` | `"unknown"` — carries no type |
| `true` / `false` | `"boolean"` |
| an integer within int64 | `"long"`; beyond int64, `"double"` |
| a float | `"double"` |
| a string | `"string"` |
| an object | `{"type": "struct", "fields": {key: observation}}` |
| an array | `{"type": "list", "element": <union of its items>}` |
| anything else | `"json"` |

The elements of one array must share a type, so they are *unioned*: numbers widen to `double`,
structs merge key by key, lists merge their elements, and anything else that disagrees (text and a
number, an object and a scalar) can only be held as `"json"`. An observation made only of nulls and
empties (`is_unknown`) creates no field: a key first seen as `null` or `[]` waits until a value with
a type arrives.

**No timestamps, decimals or UUIDs are inferred from strings**: a wrong guess would cost a fork.

## 9.3 The evolution rules

`evolve(schema, fork_map, observations, limits=…)` (`lakehouse/schema/policy.py`) applies each
observation to the current schema. Its module docstring is the reference table:

| Existing | Observed | Action |
|---|---|---|
| nothing | typed | add a nullable field |
| nothing | null, or only nulls and empties | defer: no field yet |
| `string` | any scalar | nothing: the value is written as text |
| `double` | `long` | nothing: the value is widened |
| `long` | `double`, or scalar ↔ other scalar | **fork**: a sibling field `<name>__vN` of the new type |
| scalar | object or array, or object ↔ array | **fork** |
| `struct` | struct with new keys | add the keys inside the struct |
| `list<e>` | `list<e'>` | recurse into the element; a conflict there forks the list field |
| any | a type one of its forks already has | nothing: the value goes to that fork |
| family with `max_forks_per_field` forks | yet another type | **collapse**: values go to `<name>__json` as JSON text |
| struct with `max_struct_fields` fields | a new key | collapse the struct to JSON text |
| deeper than `max_nesting_depth` | | collapse at the depth limit |
| `max_schema_fields` fields in all | a new field | **reject**: the values go to `_unmapped` |

A type that comes back reuses the fork that already has it, without any schema change, so a field
whose values alternate between two types costs one fork and no further commits. (The spec's
"reactivated" change kind does not exist: there is nothing to reactivate.)

The result, `SchemaEvolution`, carries the new `Schema` (with a new `schema_id` when anything
changed), the new fork map, `last_column_id`, and `changes`: one `SchemaChange` per thing done,
`kind` ∈ `added`, `forked`, `collapsed`, `rejected`, with the logical `path`, the new `field_id`,
its `physical_path` and `type`, and for rejections a `reason` (`schema_field_limit`).

### Same-batch rules

Observations committed together are applied in order; a `long` and a `double` for the same new key
in one window become one `double` field rather than a field and a fork (`_collect_double_paths`).

### Field ids

Ids 1–7 are the Kafka metadata columns and 8/9 the header map's key and value; payload fields start
at 10 (`FIRST_PAYLOAD_FIELD_ID`, `lakehouse/tables.py`). New ids continue from the table's
`last-column-id`; a new struct's fields get ids before their children's types (Iceberg's order).
Ids are never reused.

## 9.4 Names

Record keys become field names as they are, except (`policy._sanitize`, `_unique`):

| Key | Field name | Why |
|---|---|---|
| empty | `_` | Iceberg names cannot be empty |
| contains `.` | `.` → `_` (`ref.code` → `ref_code`) | Parquet joins nested column paths with dots, and PyIceberg's statistics are keyed by those paths |
| equal, ignoring case, to a sibling's name | `<name>__k2` (`__k3`, …) | names are unique case-insensitively |
| a top-level key equal to a metadata column's name | `<name>__payload` | metadata names are taken |

The fork map records the original key of every renamed field, so the projection (§9.9), the
seal's conform step and the console all still say `ref.code`.

## 9.5 Forks, collapses and the fork map

A **family** is every field holding one record key's values in one struct, oldest first
(`lakehouse/schema/index.py`):

```
total          long    fork_version 1   state superseded
total__v2      double  fork_version 2   state current      fork_of → total
```

A **collapse** adds `<name>__json` (`string`, state `collapsed`): from then on the key's values are
written to it as JSON text, whatever their type, and the family's other members are superseded. A
struct collapses as a whole when it would pass `max_struct_fields`; a list's *element* can collapse
on its own (`items: list<string>` holding JSON text — the projection types it `list<json>`).

The **fork map** (`lakehouse/schema/forkmap.py`) records exactly those fields and nothing else — a
plain field named after its key has no entry:

```json
{"fields": {
  "11": {"path": ["total"], "fork_of": null, "fork_version": 1, "state": "superseded"},
  "20": {"path": ["total__v2"], "fork_of": 11, "fork_version": 2, "state": "current"},
  "27": {"path": ["ref_code"], "key": "ref.code", "fork_of": null, "fork_version": 1, "state": "current"}
}}
```

It is stored as the table property `materializer.fork-map`, beside the schema in the runtime's state
table, and in the control plane's projection. The seal's conform step, the writer's drift detector,
the evolution policy, the API projection and the console's logical columns all read it through the
same `build_index` walk (the console has a TypeScript port, `console/server/iceberg/schema.ts`, checked
against the Python projection by the E5 fixture).

## 9.6 Discovery: schema 0

`start_discovery` (`services/workflow.py`, `schema/discovery.py`) samples every source topic before
the tables exist, so a topic with several historical shapes starts with its forks in place and the
first minutes of running bring no churn (spec §8.8):

- per topic, up to `discovery_sample_records` (200) records, all topics within one
  `discovery_sample_seconds` (5 s) budget, **half from the earliest offsets of every partition and
  half from just before the latest**;
- every topic is read at once, by one consumer, so discovery takes no longer with more topics; its
  fetches are kept small (`FETCH_MAX_BYTES` 8 MiB in all, `MAX_PARTITION_FETCH_BYTES` 64 KiB a
  partition) so neither does its memory, and a topic stops being read once it has its sample;
- partitions are read from the cluster's metadata (`consumer._client.fetch_all_metadata()`), never
  by subscribing, so a broker that auto-creates topics never creates a missing one;
- the samples go through `evolve(build_schema(), ForkMap(), …, schema_id=0)` — the very policy the
  committer applies later;
- connecting is bounded by 60 s (`CONNECT_TIMEOUT_SECONDS`): a cluster that never answers fails the
  step rather than passing for an empty topic.

Each table's schema 0 is evolved from its own topic's samples. An empty topic yields schema 0 with
the metadata columns only, which is fine: the runtime adds every field as data arrives (§9.7); the
status message names the topics that had no records yet. The step logs one
`control_plane_schema_discovery_completed` per table with `table`, `source_topic`,
`sampled_records`, `schema_fields` and `forks`, and persists each projection of schema 0 before
`create_table` creates the tables with them (07 §7.3).

On the lab, sampling `materializer-loadtest-orders` (32 partitions) took 375 ms and produced 139
payload fields with the dataset's two forks (`customer.phone` string → struct, `items[].category`
list → struct).

## 9.7 Evolution at run time

```
writer (every member)                 committer (one member)                 every member
  │ drift: values the schema           │ schema window: observations          │ state table key
  │ cannot take yet                    │ for schema_evolution_debounce_s      │ `schema:<table>`:
  ├─ schema_change_request ───────────►│ (2 s), then evolve + commit_schema   │ new SchemaView,
  │  (via the outbox → <rt>-commits)   │ (one metadata commit, no snapshot)   │ new drift detector
  │ the record is staged as it is      ├─ schema_changed ─► table-events ─────┤
  │ (never held)                       │                                      │ seals conform to it
```

Everything below happens per table: a record is checked against its own table's schema, a request
names its table, the committer keeps a window per table, and the state table holds each table's
schema under `schema:<table>`.

1. **Drift** (`runtime_app/schema/drift.py`): for every record, one walk against an index built once
   per schema id finds the values no member of their family accepts. Each becomes an observation
   `(logical path, type)` in a `schema_change_request` for the committer, naming the table; the same
   observation is requested again at most every 10 s per table (`REQUEST_DEDUPE_SECONDS`). Paths the schema has already
   rejected (§9.8) are not requested again. **The record is staged as it is** — 1.0 writers never
   wait.
2. **The window**: the committer collects observations for `schema_evolution_debounce_seconds` (2 s)
   from the first one — a fixed window, so a stream of novel fields cannot starve it — then evolves
   and commits once (`commit_schema`: one metadata commit through the pointer's compare-and-swap; a
   lost race reloads and evolves again, up to five attempts). It logs `runtime_schema_committed`.
3. **Publication**: the new schema, fork map and metadata location go to the runtime's state table
   (`schema:<table>`), which every member reads locally within one changelog hop, and to the control
   plane as a `schema_changed` table event naming the table (06).
4. **Seals**: a rolled batch whose rows hold values the schema cannot take yet waits up to
   `seal_schema_wait_seconds` (30 s) from its roll for the new schema (`pipeline._waiting_for_schema`),
   then seals anyway. The conform step places every value in the field that takes it; whatever still
   has no field goes to `_unmapped` (§9.8).

End to end, a new key is a column in a few seconds: on the lab, a shape change produced in the smoke
topic became schema 2 (two forks, two additions) within one commit cycle.

### The cap on schema size

`max_schema_fields` counts every field id, nested ones and list elements included
(`SchemaIndex.field_count`). At the cap, new fields are **rejected**: the change is recorded with
`kind: rejected, reason: schema_field_limit`, the path stops being requested, and its values go to
`_unmapped`. The console shows "Schema is at its field limit" (console 06).

## 9.8 `_unmapped`

`_unmapped` (field id 7, `string`) holds, as one JSON object per row, every value no column could
take — keyed by its logical path (`customer.notes`), or `$` for a record whose value is not an object
(`conform.ROOT_VALUE_PATH`). It is null on almost every row. Values land there when:

- the schema rejected their field (field limit), or
- a seal gave up waiting for an evolution (§9.7, step 4), or
- a value fits no member and no fork can be made (a type a collapsed list element cannot hold).

A key the schema does not know whose value is null or empty (`{}`, `[]`) is **not** unmapped: it
carries nothing a field could hold, and counting it would flag every record with an empty array.

Members count what they write there (`unmapped_values` on the heartbeat); the control plane keeps
`cumulative_unmapped_values` and `unmapped_last_seen_at` (08), and the schema endpoint returns them as
`unmapped: {total_values, last_seen_at}`.

## 9.9 The control plane's projection

The runtime publishes every schema it commits as a `schema_changed` table event
(`services/table_events.py`, 06). The control plane keeps:

| Key | What |
|---|---|
| `materialization-schema:<id>:<table>` | the table's current schema's projection (`MaterializationSchema`) |
| `materialization-schema-version:<id>:<table>:<schema_id>` | every schema's projection, bounded by `schema_history_limit` per table (oldest dropped) |
| the table record's `schema_id` | the table's current schema id (05 §5.12) |

A projection (`lakehouse/schema/render.py`, `render_schema`) has one entry per field — metadata
columns first, then payload in schema order with each family's members together — with the Iceberg
type as text (`long`, `timestamptz`, `struct`, `list<string>`, `list<json>`), the physical `path`
(`[]` for a list element), the logical `name` a person reads (`customer.email`, `items[].sku`, one
name for every fork), `kind` (`value`, `container`, `json`, `metadata`), `state` (`current`,
`superseded`, `collapsed`), `fork_version`, `fork_of` and `since_schema_id`. Shapes are in 04 §4.6.

Events can arrive late or twice; a schema older than the stored one changes nothing.

**When a schema happened.** A projection's `since` is when the runtime committed that schema, and
schema ids count the shapes in the order the runtime met them, which is not the order the topic's
records had them: schema 0 already holds what discovery sampled from the topic's newest records
beside its oldest (§9.6), and a backfill reads partitions at their own pace, so a shape from last
year can arrive after one from last week. When the data had each field is in the table: every data
file's manifest entry counts each column's values and nulls and bounds its `_kafka_timestamp`, so the
first and last record holding a value of a field are known to within a file — a day in a
day-partitioned table. The console's Schema tab reads them (console 06 §6.3) and dates each version by
its data, schema 0 being *Initial · sampled*; the control plane keeps `since`.

## 9.10 How readers see forks

Readers of the native table see the forks as they are: `total` (long) and `total__v2` (double), the
`__json` field, `ref_code`. The console presents a **logical** table instead (console 07):

| Native | Logical column | Expression |
|---|---|---|
| `status` | `status` | `"status"` |
| `customer: struct<email>` | `customer.email` | `("customer")."email"` |
| `items: list<struct<sku>>` | `items[].sku` (`VARCHAR[]`) | `list_transform("items", lambda __e1: (__e1)."sku")` |
| `total` (long) + `total__v2` (double) | `total` (`DOUBLE`) | `COALESCE("total__v2", TRY_CAST("total" AS DOUBLE))` |
| `phone`, `phone__v2`, `phone__json` | `phone` (JSON text) | `COALESCE("phone__json", CAST(to_json("phone__v2") AS VARCHAR), …)` |

Statements can read either: `FROM orders` (logical) or `FROM iceberg.orders` (native).

## 9.11 Protocol log events

| Event | Where | Fields |
|---|---|---|
| `control_plane_schema_discovery_completed` | control plane | `sampled_records`, `schema_fields`, `forks`, `duration_ms` |
| `runtime_schema_committed` | committer | `schema_id`, `changes`, `requests` (schema change requests in the window), `duration_ms` |
| `runtime_schema_commit_failed` | committer | `error` |

A failed evolution also raises the table error `schema_evolution_failed`, and rejections raise
`schema_field_limit` ("The schema is at its field limit (N): k new field(s) go to _unmapped
instead."); both degrade the materialization's status until the next commit (08 §8.5).

## 9.12 Failure modes

| Symptom | Cause | What happens |
|---|---|---|
| Discovery samples nothing on a topic with data | the cluster's metadata does not list the topic (a typo the console's picker would not allow), or the connection failed | schema 0 holds metadata only; the runtime adds every field from the first records — nothing is lost, the first minutes just commit more schema changes |
| A schema commit keeps losing races | another writer commits to the table (an external engine with `catalog.write`) | five attempts with 1 s spacing, then `runtime_schema_commit_failed` and the `schema_evolution_failed` table error; seals stop waiting after `seal_schema_wait_seconds` and the values go to `_unmapped` until a later window succeeds |
| `_unmapped` fills up | the schema is at `max_schema_fields`, or a family is collapsed to JSON and an array element cannot hold a value | raise `max_schema_fields` (runtime setting, 17); collapsed families keep their values as JSON text |
| A key appears as `key__k2` | two keys of one object differ only by case | both are kept; the fork map remembers the original keys |

## 9.13 Extension points

- New inference (timestamps from strings, decimals): `infer.py` and `policy._scalar_type`; a wrong
  guess costs a fork, so any heuristic must be conservative.
- New caps: `SchemaLimits` (`policy.py`) and `limits_of`, plus a runtime setting (17 §17.3).
- Type promotions Iceberg allows (`int → long`, `float → double`) never arise, because inference
  produces only `long` and `double`. The rules table's widening changes no schema: a `long` value is
  written into an existing `double` field. A `long` field that sees a `double` forks, since Iceberg
  cannot promote `long` to `double`.
