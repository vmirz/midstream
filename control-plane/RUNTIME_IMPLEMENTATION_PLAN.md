# Runtime App Implementation Plan

## Purpose

This document defines a concrete implementation plan for the runtime materializer app.

It also serves as the current-state reference for the runtime implementation in this repository. Earlier sections still explain the design intent, while the "current implementation status" section below captures what is actually implemented today.

The runtime app is the data-plane component created and managed by the control plane for a single materialization in the MVP. Its job is to consume the Kafka topics wired into it by the control plane, materialize records to parquet, report health and processing position, and cooperate with the control plane on authoritative schema updates.

This plan follows the same operating pattern as the control plane:

- core Python logic lives in the external package, not inline in Kaspr YAML
- Kaspr CRDs remain thin harness resources
- runtime Kaspr resources are rendered from Jinja templates with typed input
- event handlers stay bounded and replay-safe

## Current Implementation Status

The current runtime implementation behaves as follows:

- each materialization gets its own dedicated runtime `KasprApp`
- the writer agent runs with `isolatedPartitions: true`
- schema proposals are emitted through agent `output.topics`
- the shared `materialization-schema-authority-updates` topic carries both promoted authoritative schema updates and `schema_decision` signal envelopes
- the per-runtime `<runtime-app>-signals` topic carries targeted runtime signals such as `seed_schema` and `refresh_schema`
- blocked source partitions pause while waiting for schema progress and resume when the wait resolves
- if a blocked record still drifts after a later promotion, the writer requeues a fresh proposal instead of waiting forever on the original proposal id
- open batches are staged locally as JSONL and sealed into parquet when the batch rolls
- seal-time parquet conversion first infers one explicit PyArrow schema across the full staged JSONL so structurally flattened list columns do not fail on cross-block type drift
- authoritative schema state lives in the runtime schema-state table, while handled/committed offsets and pending upload state live in member-local in-memory partition state
- restart recovery rebuilds processing state from immutable GCS commit markers plus local batch manifests
- the upload task can seal aged open batches and process multiple uploads per tick up to `upload_worker_count`
- runtime status snapshots include phase reason/message, latest handled offsets, latest committed offsets, pending sealed-upload backlog, and bounded performance windows
- `pending_upload_batches` reflects sealed backlog only; it does not count an active open batch that has not sealed yet

## Scope

In scope:

- runtime responsibilities and boundaries
- runtime Kaspr resource set
- runtime package/module layout
- topic contracts between runtime and control plane
- runtime state and checkpoint model
- runtime message-processing design for scalable Kafka-to-Parquet materialization
- GCS parquet object-management model
- Jinja template rendering strategy for runtime resources
- phased MVP implementation order

Out of scope:

- advanced cross-file compaction, clustering, or rewrite optimization
- multi-cloud storage abstractions beyond the initial GCS backend
- shared-runtime multiplexing
- advanced query-serving behavior beyond producing queryable parquet outputs

## Constraints And Decisions

The following are explicit runtime decisions for MVP:

- Each runtime app is a dedicated `KasprApp` for one materialization.
- The runtime app is rendered entirely from Jinja templates owned by the control plane package.
- Runtime business logic lives under a dedicated runtime module in the external Python package: `materializer_control_plane.runtime_app`.
- Inline Python in runtime CRDs must stay thin and only import reusable package code.
- The control plane remains the lifecycle owner and authoritative schema owner.
- The runtime app must consume authoritative schema updates from the control plane.
- The runtime app must also accept per-runtime control signals from the control plane for bootstrap and future targeted runtime actions.
- The runtime app may emit additive schema proposals, but it must not self-promote schema versions.
- The runtime app must emit per-instance status feedback through Kafka events.
- The runtime app must persist enough local state to resume safely after restart.
- Runtime pause and resume behavior should be driven by the applied Kaspr spec and lower-level Kaspr/Faust behavior, not by agent-managed lifecycle checks.
- Periodic runtime status emission should use a per-instance `KasprTask`.
- GCS is the initial parquet destination backend, and the runtime should rely on Application Default Credentials through the preconfigured `GOOGLE_APPLICATION_CREDENTIALS` environment.
- The runtime should prefer asynchronous or background GCS upload work over per-event blocking remote writes, while still preserving durable checkpoint intent.
- Parquet files should be immutable once committed to GCS.
- The MVP runtime should materialize topics as append-only event datasets, not as last-write-wins tables or compacted changelog views.
- A single parquet file should never mix authoritative schema versions; schema version changes should roll the active file before new rows are appended.
- Runtime-local state must distinguish between the highest offset durably handled in local state and the highest offset durably committed to GCS-backed parquet output.
- Runtime status should primarily report the highest durably handled offset, while separate counters or backlog metrics should show when GCS materialization lags behind local staged progress.

## Message-Processing Design Goals

The runtime processing engine should optimize for five things at once.

- scale from low-volume topics to very large topics without changing the core write model
- keep memory bounded per active partition rather than buffering large record sets in RAM
- produce parquet datasets in GCS that are practical for engines such as DuckDB to scan efficiently
- preserve robust recovery behavior when Kafka events replay, uploads fail, or members restart unexpectedly
- keep the writer hot path small enough that normal processing remains fast and predictable

## Runtime Responsibilities

The runtime app must do six things.

### 1. Consume The Assigned Source Topic

- consume whatever source topics the control plane wires into the runtime resource definition
- begin normal processing as part of standard runtime startup and keep processing while messages are delivered to the agent
- keep per-event handling bounded and replay-tolerant

### 2. Materialize Records To Parquet

- normalize incoming records according to the current authoritative schema
- stage flattened physical rows locally, seal them into parquet, and upload immutable parquet objects under the configured destination URI
- roll files or batches according to runtime policy
- avoid corrupting output on replay or restart

### 3. Maintain Processing State

- track latest handled and latest GCS-committed offsets per partition
- store enough batch/manifest state plus commit markers for safe resumption
- store current authoritative schema version in local runtime state

### 4. Emit Status Events

- publish periodic per-instance runtime status events to the control plane
- include health, heartbeat, processed offsets, and consumed-message timestamps when available
- emit degraded status with `last_error` when processing encounters recoverable failures

### 5. Participate In Schema Evolution

- consume authoritative schema update events from the control plane
- use the latest schema version for normalization and writing
- emit additive schema proposals with evidence when new compatible fields are observed

### 6. Reflect Lifecycle Intent

- start processing as part of normal runtime startup without adding agent-level lifecycle checks for `running`
- rely on lower-level Kaspr or Faust pause behavior so agents naturally stop receiving new messages when the control plane applies a paused spec
- keep deletion simple: the runtime should not own special delete workflow logic beyond keeping its durable state in Kaspr-managed resources that can be removed safely when the control plane deletes the runtime app

## Runtime Feedback To The Control Plane

The runtime provides feedback through two channels.

### Kafka Event Feedback

The runtime publishes:

- `materialization-status` events
- `materialization-schema-proposals` events

These are the primary signals for:

- heartbeat
- health
- processed offsets
- consumed-message timestamps when available
- schema observations

These events should be treated as per-instance processing signals, not as the source of truth for app-level lifecycle or readiness.

Runtime heartbeat payloads should also carry a concise protocol reason and optional message so operators can distinguish:

- waiting for authoritative bootstrap schema
- ready but idle with no source traffic yet
- actively processing
- degraded because of data or infrastructure issues

Recommended runtime status reasons include:

- `waiting_for_seed_schema`
- `waiting_for_source_records`
- `processing`
- `runtime_data_validation_degraded`
- `runtime_infrastructure_degraded`

### KasprApp Resource Feedback

The control plane also observes the runtime `KasprApp` resource directly to infer:

- readiness
- paused state
- rollout convergence
- resource absence after delete

The runtime does not need to emit terminal lifecycle states like `paused` or `deleted` itself.

### Recommended Aggregation Boundary

The control plane should aggregate runtime feedback in two layers.

#### App-Level Lifecycle Summary

Derived primarily from the runtime `KasprApp` CRD.

Use it for:

- ready versus not ready
- rollout convergence
- paused state
- member availability
- resource absence after delete

#### Processing-Signal Summary

Derived from per-instance runtime status events.

Use it for:

- recent member heartbeats
- per-partition processed offsets
- most recent consumed event timestamps
- member-local degraded conditions and error details
- schema version currently in use by runtime members

The control plane should combine these two views into one coarse materialization status, without trying to reconstruct app readiness purely from member heartbeats.

The runtime should not overuse error logs for normal warm-up states. Expected protocol waits should be visible primarily through heartbeat `phase_reason` / `phase_message` fields and optional structured `protocol_event` logs. Throughput troubleshooting should prefer bounded heartbeat-window summaries over per-record logs so operators can separate schema-wait time, write/seal time, and upload time without flooding the console. Those summaries should also expose active in-flight schema waits (ready-vs-decision counts plus oldest wait age/type) so blocked throughput is visible even before waits resolve.

## Kaspr Resource Model

The runtime resource set should stay small and explicit.

### Required Resources

#### `KasprApp`

One dedicated runtime app per materialization.

Responsibilities:

- package installation
- bootstrap server config
- runtime storage config
- app-level serializer defaults
- labels and annotations that tie the app back to the materialization

#### `KasprAgent`: Writer

Consumes the assigned source topic.

Responsibilities:

- decode and normalize records
- consult authoritative schema state
- update processing state
- write parquet output
- emit optional schema proposals

This is the primary runtime agent.

#### `KasprAgent`: Schema Update Consumer

Consumes `materialization-schema-authority-updates`.

Responsibilities:

- filter updates for the assigned `materialization_id`
- persist latest authoritative schema version and shape into a runtime table
- notify waiting writers after schema state is available

#### `KasprAgent`: Runtime Signals Consumer

Consumes a per-runtime `<runtime-app>-signals` topic.

Responsibilities:

- accept control-plane bootstrap or repair signals scoped to one runtime
- seed runtime-local authoritative schema state when a new runtime is ready before it has consumed a shared schema-authority update
- notify waiting writers after schema state is available

#### `KasprTable`: Processing State

Purpose:

- store offsets, file state, and resumability data
- use a **global table** in the MVP so every runtime pod can read the current checkpoint and sealed-batch metadata during rebalance or upload recovery

Suggested keys:

- `writer-offset:<partition>`
- `writer-batch:<partition>`
- `writer-summary:<member_id>`

#### `KasprTable`: Authoritative Schema State

Purpose:

- store the latest control-plane-approved schema version and shape used by the writer
- use a **global table** so every runtime pod sees the authoritative schema without depending on single-partition local ownership

### Required Supporting Resource

#### `KasprTask`: Periodic Status Emitter

Use a per-instance `KasprTask` to publish periodic runtime status events.

Responsibilities:

- read runtime-local state needed for status snapshots
- publish instance-local heartbeat-style `materialization-status` events on a fixed cadence
- keep status emission separate from the writer hot path

This task should run on every runtime member, not only on the leader.

#### `KasprTask`: Local Upload Worker

Use a per-instance `KasprTask` to scan the member's sealed local batch artifacts, perform at most one bounded GCS upload step, and then write a GCS commit marker that becomes the durable commit point for that partition batch.

Responsibilities:

- inspect the member's local sealed parquet manifests
- upload one eligible batch without blocking the writer hot path
- write an immutable GCS commit marker only after the parquet object exists
- keep local-file I/O on the owning member because staged files live on member-local storage

This task should also run on every runtime member, not only on the leader.

### Optional MVP Resources

#### Optional `KasprTask`: Housekeeping

Useful later for lightweight maintenance such as stale-batch cleanup.

### Resources Not Expected Initially

- no `KasprWebView`
- no `KasprJoin`
- no per-materialization scheduler resources inside the runtime app

## Topic Contracts

### Runtime Inputs

#### Source Topic

The assigned Kafka topic from the materialization request.

Responsibilities:

- provide the raw records to materialize
- deliver source values to runtime handlers as already-decoded Python-native dictionary-like objects

#### `materialization-schema-authority-updates`

Produced by the control plane.

Responsibilities:

- deliver the latest authoritative schema version and shape
- deliver `schema_decision` signal envelopes for stale or rejected proposals on the same draining control stream

#### `<runtime-app>-signals`

Produced by the control plane.

Responsibilities:

- deliver targeted runtime signals such as `seed_schema` and `refresh_schema`
- bootstrap a newly deployed runtime with authoritative schema when the resource is ready before the runtime has persisted local schema state

### Runtime Outputs

#### `materialization-status`

Produced by the runtime.

Required behavior:

- emit healthy `running` events while processing normally
- emit `degraded` events when the runtime encounters recoverable issues
- include `reported_at`, `schema_version`, the writer agent name, and a stable member identifier

Recommended payload:

- member identifier
- latest processed offsets by partition
- most recent consumed event timestamp when available
- `last_error` when degraded

Recommended contract note:

- `phase` in runtime status events should remain narrow and represent only processing-side signals such as `running` or `degraded`
- app-level lifecycle such as `paused`, readiness, or deletion should continue to come from `KasprApp` observation

#### `materialization-schema-proposals`

Produced by the runtime.

Required behavior:

- emit only when live data suggests additive schema evolution or incompatible changes that require control-plane review
- include sample location and raw sample evidence
- treat later-missing previously-seen fields as nullable/empty rather than as fresh schema drift
- allow the control plane to debounce/coalesce additive proposals for the same materialization before promoting a single authority update
- if concurrent additive proposals disagree on the same field path before promotion, coalesce them to a permissive `json` field rather than deadlocking writers behind repeated pending-conflict rejections

## Package Layout And Ownership

Runtime business logic should expand the existing `materializer_control_plane.runtime_app` package.

Suggested shape:

```text
python/src/materializer_control_plane/
  runtime/
    bindings.py
    naming.py
    templates.py
  runtime_app/
    __init__.py
    config.py
    models/
    storage/
    writer/
    schema/
    status/
    handlers/
    templates/
```

### `runtime/`

This remains control-plane-facing.

Responsibilities:

- deterministic resource naming
- ownership metadata
- typed Jinja input assembly
- rendered resource hashing and apply ordering

### `runtime_app/`

This is runtime-facing business logic.

Responsibilities:

- writer pipeline implementation
- parquet write logic
- runtime-local state access
- schema adoption and proposal detection
- status event construction
- thin handlers imported by runtime Kaspr resources

### Recommended Runtime Submodules

#### `runtime_app/config.py`

- writer thresholds and batch limits
- GCS upload concurrency and retry limits
- local staging-disk budgets and backpressure thresholds
- parquet Kafka-metadata inclusion mode
- heartbeat cadence
- schema proposal limits
- destination URI options

#### `runtime_app/models/`

- internal checkpoint models
- schema state models
- write batch state models

#### `runtime_app/storage/`

- runtime table key helpers
- checkpoint persistence
- schema state persistence

#### `runtime_app/writer/`

- record normalization
- parquet batch assembly
- local staging-file lifecycle
- GCS upload coordination and commit logic
- resumable processing logic

#### `runtime_app/schema/`

- authoritative schema application
- additive field detection
- proposal building

#### `runtime_app/status/`

- heartbeat building
- member identity helpers
- processing-position summaries
- degraded-status event creation

#### `runtime_app/handlers/`

- thin Kaspr-facing adapters for writer and schema-update agents

### Concrete Runtime Internal Contracts

The next implementation step should define these contracts explicitly before adding the real parquet writer.

#### `RuntimeAppConfig`

`RuntimeAppConfig` should evolve from naming-only defaults into a typed runtime contract that includes:

- harness resource names
  - `schema_state_table_name`
  - `writer_agent_name`
  - `schema_update_agent_name`
  - `signals_agent_name`
  - `upload_task_name`
  - `status_task_name`
  - `heartbeat_interval`
- writer behavior
  - `staging_root_path`
  - `target_file_size_bytes`
  - `target_row_group_size_bytes`
  - `max_rows_per_file`
  - `max_open_file_age_seconds`
- upload behavior
  - `upload_worker_count`
  - `max_pending_upload_batches`
  - `max_pending_upload_bytes`
  - `upload_retry_limit`
  - `upload_initial_backoff_seconds`
- processing policy
  - `kafka_metadata_mode`
    - `columns`
    - `none`
  - `malformed_record_mode`
    - `continue_with_error`
    - `strict`

The runtime should allow these to vary by profile later, but the code contract should be typed and explicit immediately.

#### Core Runtime Models

The runtime should use small typed models rather than loose dictionaries for hot-path decisions and persisted state.

##### `SourceRecordMetadata`

Represents the broker-level metadata extracted from Kaspr `context["event"]`.

Required fields:

- `materialization_id`
- `source_topic`
- `partition`
- `offset`
- `timestamp`
- `member_id`
- `observed_at`

This model is the source of truth for parquet metadata columns, deterministic file naming, and replay checks.

##### `PartitionCheckpoint`

Represents the durable per-partition processing position.

Required fields:

- `materialization_id`
- `partition`
- `owner_member_id`
- `active_batch_id`
- `handled_offset`
- `committed_offset`
- `schema_version`
- `latest_event_time`
- `updated_at`
- `last_error`

Required semantics:

- `handled_offset` means the highest source offset whose outcome has been durably captured in runtime-local state
  - either appended to a local staged parquet batch
  - or intentionally skipped under configured malformed-record policy with durable evidence/counters
- `committed_offset` means the highest source offset durably reflected in successfully uploaded GCS parquet objects
- `committed_offset` must never exceed `handled_offset`
- replay suppression should use `handled_offset`
- status events should use `handled_offset` for `latest_offsets`
- status payloads should include backlog signals so operators can see when `committed_offset` lags behind `handled_offset`

##### `BatchState`

Represents one local parquet file from creation through GCS commit.

Required fields:

- `batch_id`
- `materialization_id`
- `owner_member_id`
- `partition`
- `schema_version`
- `local_path`
- `gcs_uri`
- `state`
  - `open`
  - `ready`
  - `uploading`
  - `committed`
  - `failed`
- `start_offset`
- `end_offset`
- `row_count`
- `skipped_record_count`
- `estimated_bytes`
- `event_time_min`
- `event_time_max`
- `created_at`
- `rolled_at`
- `committed_at`
- `updated_at`

Required invariants:

- at most one `open` batch per partition per member at a time
- multiple `ready`, `uploading`, or `failed` batches may coexist for a partition while backlog drains
- batches for a single partition must be committed to GCS in offset order
- `batch_id` should be deterministic from partition, schema version, owner member, and starting offset

##### `RuntimeProcessingSummary`

Represents bounded, status-friendly aggregate runtime state.

Required fields:

- `materialization_id`
- `member_id`
- `latest_handled_offsets`
- `latest_committed_offsets`
- `rows_written`
- `bytes_written`
- `files_written`
- `records_skipped`
- `pending_upload_batches`
- `pending_upload_bytes`
- `last_error`
- `last_file_path`
- `last_malformed_record`
- `updated_at`

This summary should favor bounded counters and the latest diagnostic snapshot rather than an unbounded event history.

#### GCS Commit-Marker Contract

The durable processing truth should live in immutable GCS commit markers written under the destination prefix.

Suggested path:

- `gs://<bucket>/<prefix>/_runtime_state/materialization_id=<mid>/source_partition=<partition>/offset_end=<offset>.json`

Each marker should record at least:

- `materialization_id`
- `source_partition`
- `offset_start`
- `offset_end`
- `schema_version`
- `parquet_uri`
- `row_count`
- `bytes_written`
- `writer_member_id`
- `committed_at`

#### Writer Module Interface Contract

The runtime writer package should be split into focused modules with small contracts.

Suggested shape:

- `runtime_app/writer/context.py`
  - extract `SourceRecordMetadata` from Kaspr handler context
- `runtime_app/writer/normalize.py`
  - map decoded source values into the canonical parquet row projection
  - return either a normalized row or malformed-record evidence
- `runtime_app/writer/batches.py`
  - load or create the active `BatchState`
  - append rows to the local staged parquet file
  - decide when a batch should roll
  - seal `open` batches into `ready`
- `runtime_app/writer/naming.py`
  - build deterministic local file paths and final GCS object URIs
- `runtime_app/writer/manifests.py`
  - persist sealed-batch manifest sidecars beside local parquet files
- `runtime_app/writer/upload.py`
  - scan owned sealed-batch manifests
  - perform idempotent GCS commits
  - write immutable GCS commit markers after successful uploads
- `runtime_app/writer/pipeline.py`
  - orchestrate the per-record hot path used by the Kaspr writer handler

#### Per-Record Hot Path Contract

The writer hot path should remain synchronous only for local state and local staged-file updates.

For each source record:

1. extract `SourceRecordMetadata`
2. load the current authoritative schema, waiting asynchronously for runtime-local schema state if bootstrap has not landed yet
3. load or initialize the partition's in-memory runtime state from the latest GCS commit marker plus any surviving local sealed manifests
4. if `offset <= handled_offset`, treat the record as replay and return without side effects
5. normalize the decoded payload
6. if normalization yields malformed-record evidence:
   - update in-memory summary state according to `malformed_record_mode`
   - surface degraded diagnostics
   - skip parquet append for that record
7. otherwise, append the normalized row to the `open` batch's local parquet file
8. update the partition's in-memory active batch, handled offset, and summary state
9. if a roll condition is met, seal the batch to `ready` and persist a sealed-batch manifest sidecar for the upload task
10. emit optional schema proposal output when live data reveals additive or incompatible schema drift

This contract keeps the Kaspr agent handler bounded while still ensuring replay-safe local durability.

#### Upload Worker Contract

Upload work should stay off the writer hot path and remain serialized within a partition.

Required behavior:

- a per-instance upload task should only process batches owned by the current member's local staging directory
- within a partition, `ready` batches must be committed in offset order
- before upload starts, the upload task should mark the in-memory batch `uploading`
- on successful create-if-absent upload, the upload task should write the immutable GCS commit marker and then:
  - advance the partition's committed offset in memory
  - update committed counters in `RuntimeProcessingSummary`
  - leave local cleanup to the owning member once the commit marker exists
- on retryable failure, the upload task should mark the batch `failed` with error details in memory
- local staged files and manifests should be deleted only after the owning member observes committed batch state

#### Member Ownership And Recovery Contract

Because staged files live on member-local storage, each batch record must carry `owner_member_id`.

Required behavior:

- a member should only attempt to upload or repair batches whose `owner_member_id` matches its own identity
- on restart, a member should scan its owned sealed-batch manifests together with the latest GCS commit markers so pending batches can resume idempotently
- if a partition moves to a different member during rebalance, the old member may finish uploading its already-sealed local batches while the new member begins writing future batches
- if a local staged file required for an `open`, `ready`, or `uploading` batch is missing, the owning member should mark the partition degraded explicitly

## Jinja Template Strategy

All runtime Kaspr resources should be rendered from Jinja templates owned by the external package.

### Template Ownership

Templates should live under:

```text
python/src/materializer_control_plane/runtime_app/templates/
```

The control plane renderer should own converting a typed materialization input into a rendered resource set.

Pure authoritative schema-version promotion should not, by itself, change the rendered runtime spec hash. Runtime schema updates should flow through the shared schema-authority topic rather than forcing full runtime reapply/restart churn.

When a ready `KasprApp` also has a recent runtime heartbeat, resource observation should preserve the more specific runtime-derived processing state/reason (for example `waiting_for_source_records` or data-validation degradation) instead of downgrading it to a generic `runtime_silent` interpretation.

### Rendered Resource Set

The runtime template family should output:

- one `KasprApp`
- one writer `KasprAgent`
- one schema-update `KasprAgent`
- one runtime-signals `KasprAgent`
- one periodic-status `KasprTask`
- one per-member upload `KasprTask`
- one schema-state `KasprTable`
- optional future housekeeping components

### Template Rules

- all names must be deterministic from materialization input
- the full rendered resource set must be hashable for idempotent apply
- labels and annotations must include materialization ownership metadata
- inline Python must remain thin and import runtime package entrypoints
- template version must be persisted in `runtime-binding`

## Processing Model

### High-Level Writer Semantics

The writer should treat each Kafka partition independently.

- each assigned source partition owns its own active write state
- each active write state is bound to one authoritative schema version at a time
- rows should first be written to a local staged JSONL batch on runtime storage
- completed staged files should then be uploaded to GCS through a bounded background upload path
- local handled position should advance in partition-local in-memory state, while GCS-committed position should advance only after the corresponding parquet file and its immutable GCS commit marker have both been written successfully

### Writer Path

For each consumed record, the writer agent should:

1. load current authoritative schema state
2. decode and normalize the input record
3. detect additive or incompatible schema observations
4. update the current write batch or file state
5. persist latest processed offsets and local write state
6. emit optional schema proposal outputs

The precise batching implementation can evolve, but the contract is that replay or restart must not corrupt the destination or lose checkpoint intent.

### Detailed Message-Processing Engine Design

#### Data Shape Written To Parquet

Each parquet row should contain:

- flattened physical columns for the current materialized authoritative fields derived from the schema version currently in force
- runtime-managed metadata according to a configurable metadata mode

Recommended metadata modes:

- `columns` (recommended default)
  - emit metadata as top-level reserved parquet columns
  - required columns:
    - `_materialization_id`
    - `_source_topic`
    - `_kafka_partition`
    - `_kafka_offset`
    - `_kafka_timestamp`
    - `_schema_version`
    - `_materialized_at`
- `none`
  - write only the normalized payload projection with no extra Kafka metadata columns

The runtime should reserve the `_` prefix for its own metadata columns so payload fields cannot collide with runtime-managed names.

Recommended rules:

- authoritative schema paths such as `payload.customer.id` should map to flattened physical parquet columns such as `payload__customer__id`
- missing fields should be written as null
- additive fields observed in live traffic should not be written speculatively before the control plane promotes them
- files written under schema version `N` should remain immutable after schema version `N+1` is adopted

If a source value cannot be represented safely under the current authoritative schema, the runtime should degrade explicitly rather than silently coercing the value to a weaker type. Shape/type evolution that can be represented through forward-only schema forks should flow through the schema-proposal path instead of being treated as an immediate breaking write failure.

#### Partition-Scoped Writer Lifecycle

The runtime should maintain one active batch per assigned source partition under normal conditions.

The active batch should be rolled when any of the following occurs:

- target file size is reached
- target row count is reached
- max open age is reached
- authoritative schema version changes
- partition revocation or app shutdown begins
- local error recovery requires sealing the current file

If a writer is blocked waiting for seed schema or schema-decision progress when shutdown begins, it should check `app.should_stop`, abort that in-flight handler promptly, and rely on source replay after restart instead of holding the member open during `wait_empty`.

To keep active-writer count bounded, the initial object layout should use processing-time buckets rather than event-time partitioning:

- `schema_version=<n>/ingest_date=<yyyy-mm-dd>/ingest_hour=<hh>/kafka_partition=<p>/...`

This avoids opening many simultaneous files for late or out-of-order source events while still giving query engines a useful directory structure.

Event-time bucketed object layouts can improve pruning for some SQL workloads, but they also increase open-file fanout, small-file risk, and writer-state complexity when source events arrive late or out of order. The MVP should optimize first for bounded ingestion complexity and reliable throughput.

#### Local Staging And Memory Model

The writer hot path should append rows to a local staged JSONL file on the runtime's durable storage and convert that file to parquet only when the batch seals.

Recommended behavior:

- an `open` batch may use an append-friendly local staging artifact and then seal into its final local parquet file when the batch rolls
- when an `open` batch seals, the runtime should persist a JSON manifest sidecar beside the parquet file so upload recovery does not depend on mutating tables from a task
- the seal step should infer one explicit PyArrow schema across the whole staged JSONL before opening it for parquet conversion so structurally flattened array/list columns tolerate later non-null typed values
- build parquet row groups incrementally instead of retaining large in-memory row lists
- keep only the current row-group buffer plus lightweight batch metadata in memory
- target row-group sizes around tens of MiB, with larger file-size targets driven by runtime profile
- start with file targets around `128-256 MiB` for default workloads, while allowing smaller or larger targets through configuration

This keeps memory low while still producing files large enough for efficient GCS and DuckDB reads.

Including Kafka metadata columns should remain cheap because the values already exist in Kaspr handler context. Kaspr exposes broker-level metadata such as partition, offset, and timestamp on the current event, so the runtime should derive these fields directly from the event context without extra Kafka lookups.

#### GCS Object Commit Model

The runtime should upload sealed local parquet files directly to their deterministic final GCS object path.

Recommended final object shape:

- `gs://<bucket>/<prefix>/materialization_id=<id>/schema_version=<n>/ingest_date=<yyyy-mm-dd>/ingest_hour=<hh>/kafka_partition=<p>/offset_start=<s>-offset_end=<e>.parquet`

Recommended commit rules:

- final object names should be deterministic from materialization id, schema version, partition, and offset range
- uploads should use a create-if-absent precondition so retries do not overwrite an already committed object
- object metadata should include materialization id, schema version, partition, offset start, offset end, row count, and event-time min/max when available
- if an upload retry finds an existing object with matching metadata, the runtime should treat that as an idempotent success
- local staged files should be deleted only after runtime state is updated to reflect successful GCS commit

Because GCS does not provide a native atomic rename primitive comparable to a filesystem move, the runtime should avoid temp-object rename flows in the MVP unless a later benchmark proves they are necessary.

#### Async Upload, Backpressure, And Recovery

The runtime should keep GCS I/O off the writer hot path as much as possible.

Recommended model:

- the writer seals a local parquet file and persists a local manifest sidecar
- a per-instance upload task scans local manifests on a fixed cadence, seals aged open batches, and advances up to `upload_worker_count` eligible uploads per tick
- the upload task performs the member-local GCS I/O and writes an immutable GCS commit marker that becomes the durable commit point
- if upload backlog or local staging-disk usage exceeds configured thresholds, the runtime should degrade and apply backpressure rather than allowing unbounded local growth

Backpressure is preferred over silent data loss:

- status should continue to report the highest GCS-committed offset
- in-flight rows may exist locally ahead of reported committed offsets
- once backlog falls and upload succeeds, committed offsets can advance

#### Schema Update Interaction

Authoritative schema updates must integrate with file management cleanly.

Required behavior:

- when a newer schema version is adopted, the current active batch for the old schema version should be sealed before writing any new rows
- the next file for that partition should start under the new schema-version prefix
- additive schema proposals may be emitted while the runtime continues writing only the currently authoritative projection
- breaking schema observations should not auto-promote the schema and should surface through degraded status plus a proposal event with evidence

#### Malformed And Non-Representable Record Policy

The runtime should support configurable strictness when a decoded source value cannot be normalized safely under the current authoritative schema.

Recommended policy modes:

- `continue_with_error` (recommended default)
  - do not write the offending record to parquet
  - record structured evidence including partition, offset, schema version, reason, and a bounded raw sample
  - surface the condition through degraded status, counters, and structured logs so the issue is visible without stopping the entire materialization
  - continue processing subsequent records
- `strict`
  - record the same evidence
  - stop forward progress for the affected materialization until operator intervention, rather than silently skipping data

The MVP should start with `continue_with_error` as the default because it preserves ingestion continuity while still surfacing correctness problems clearly. A later strict mode can share the same evidence model while changing the runtime response.

#### Restart And Repair Path

On startup, the runtime should inspect persisted write-state records before resuming normal processing.

Recommended recovery behavior:

- if a batch record is `ready`, `uploading`, or `failed` and the sealed local manifest still exists, the upload task should resume it idempotently
- if a local uploaded marker exists but the batch record is not yet `committed`, the upload task should re-emit a `committed` result event until state catches up
- if a batch record is `open` and the local staged file exists, reopening or deterministic sealing remains hardening work for a later phase
- if a batch record is not committed but its local staged file is missing, mark the partition degraded and stop advancing committed offsets for that partition until repaired

The runtime should prefer explicit degraded states over inventing success-shaped defaults when local staged data cannot be proven durable.

### Schema Update Path

For each authoritative schema update event, the runtime should:

1. filter by `materialization_id`
2. compare with current local schema version
3. ignore stale or duplicate updates
4. persist the latest authoritative schema state

For each runtime bootstrap signal, the runtime should:

1. validate the signal type and materialization id
2. persist the carried authoritative schema payload into the same runtime-local schema-state table
3. set an in-process readiness event so any writer currently waiting for schema initialization can resume

### Status Emission Model

Status emission should follow these principles:

- use a per-instance `KasprTask` for periodic heartbeat-style status emission
- publish `running` while the runtime is healthy and able to process records
- publish `degraded` when the runtime can continue but write quality or downstream systems are impaired
- prefer durable handled offsets and observed event timestamps over any notion of end-to-end progress percentage
- include a member identifier so the control plane can persist and compare per-instance snapshots
- build the per-instance snapshot from the runtime member's in-memory partition state while reporting the latest GCS-committed offsets known to that member
- do not try to re-aggregate app readiness from heartbeats when the Kaspr operator already maintains that at the `KasprApp` layer
- keep events bounded and additive; do not attempt control-plane orchestration from the runtime
- include upload backlog and local-staging pressure indirectly through degraded reasons or additional counters once available
- when possible, distinguish local handled progress from GCS-committed materialization progress in internal summaries and optional status counters
- include a bounded per-heartbeat performance window that summarizes source-record count/rate plus average and max timings for schema wait, write, seal, and upload stages
- ensure the performance window is recorded from the actual runtime writer pipeline stages used in production, not only from convenience wrappers used by tests
- emit a more specific runtime status reason such as `waiting_for_schema_decisions` when schema waits are actively blocking throughput despite a healthy app
- include structured breaking-change diagnostics with field path, expected type, observed type, and sample partition/offset so live schema conflicts can be triaged from logs without re-reading raw topic traffic first

## State Model

The runtime should keep local state intentionally small.

### Required Persisted State

- current authoritative schema version
- authoritative schema shape
- immutable GCS commit markers per committed parquet object
- local batch manifest sidecars for sealed-but-not-yet-committed batches

### Implemented Member-Local Processing State

The current runtime does not use a runtime processing table as the durable source of truth for writer progress.

Instead it uses:

- member-local in-memory partition state for:
  - latest handled offset
  - latest committed offset
  - active open batch
  - pending sealed/uploading/failed batches
  - aggregate counters and last error
- the runtime schema-state table for authoritative schema only
- immutable GCS commit markers plus local manifests to rebuild writer state after restart

Recommended batch-state fields per active or recently sealed file:

- `batch_id`
- `materialization_id`
- `owner_member_id`
- `partition`
- `schema_version`
- `local_path`
- `gcs_uri`
- `state`
  - `open`
  - `ready`
  - `uploading`
  - `committed`
  - `failed`
- `start_offset`
- `end_offset`
- `row_count`
- `skipped_record_count`
- `estimated_bytes`
- `event_time_min`
- `event_time_max`
- `created_at`
- `rolled_at`
- `committed_at`
- `updated_at`

The control plane should be expected to persist per-instance runtime status snapshots separately from the coarse materialization status summary.

### Idempotency Expectations

- deterministic write identifiers where feasible
- no in-memory-only checkpoint that would make restart unsafe
- stale schema updates must be ignored cleanly
- duplicate source event replay must not produce unbounded duplicate side effects
- GCS object creation must be idempotent by object path and precondition
- handled offsets must never advance past the highest offset durably reflected in local runtime state
- committed offsets must never advance past the highest offset included in a successfully committed parquet object

## Failure Handling

The runtime should assume:

- Kafka events may replay
- parquet writes may fail after partial output creation
- schema updates may arrive late or duplicate
- control-plane observation may lag runtime status emission
- source-event processing is at-least-once from the runtime's perspective, so a message may be processed again if the agent finishes work but the source offset commit fails before shutdown
- runtime-local RocksDB state is durable once written, even if the corresponding table changelog publication later fails

Required practices:

- persist checkpoints before advertising new processing position in status
- include `last_error` in degraded events
- treat authoritative schema updates as monotonic by version
- keep runtime handlers small enough that restart and replay remain safe
- distinguish clearly between locally staged progress and GCS-committed progress
- retain enough batch metadata to retry uploads without rewriting the file from scratch when possible
- treat missing local staged files for uncommitted batches as explicit degraded conditions
- do not overengineer recovery around table-changelog publication failures when local runtime state has already been written durably

## Initial MVP

### MVP Goals

- consume control-plane-assigned source topics
- persist authoritative schema state locally
- write parquet outputs to GCS through a reusable writer path
- manage parquet files through local staging plus asynchronous background upload
- emit status events with health, offsets, and timestamps
- emit schema proposal events when live data diverges additively from current schema
- support pause and resume through applied runtime spec changes

### MVP Non-Goals

- shared runtime hosting multiple materializations
- runtime-hosted query APIs
- advanced cross-file compaction, clustering, or rewrite optimization
- keyed latest-state or upsert materialization modes over Kafka message keys
- exact-once guarantees across Kafka and object storage
- autonomous runtime schema promotion

## Phase Plan

Each phase should be small enough to review and validate independently.

### Phase 1: Runtime Package Skeleton And Template Family

- expand `runtime_app/` package structure
- define runtime config and internal models
- define Jinja templates for `KasprApp`, agents, and tables
- extend the renderer to output the full runtime resource set, not just the shell app

Validation:

- unit tests for typed template input and rendered resource structure
- `kubectl kustomize manifests/base`

### Phase 2: Schema State Ingestion

- implement schema-state table helpers
- implement schema-authority update consumer agent handler
- persist and read authoritative schema version locally

Validation:

- unit tests for update filtering, version monotonicity, and table persistence

### Phase 3: Writer State And Checkpoint Model

- implement GCS commit-marker helpers
- define checkpoint and batch-state models
- implement durable committed-offset semantics distinct from any in-flight local staged progress
- implement writer entrypoint that keeps partition-local in-memory state and recovers committed progress lazily from GCS

Validation:

- unit tests for checkpoint persistence and replay-safe progression

### Phase 4: Normalization And Local Parquet Staging

- implement record normalization against authoritative schema
- implement nested payload mapping to parquet-compatible structures
- implement staged local parquet file assembly
- connect staged-file outputs to local write-state persistence

Validation:

- focused unit tests for normalization and write-path behavior

### Phase 5: Async GCS Upload And Commit Protocol

- implement deterministic final GCS object naming
- implement per-member upload-task orchestration plus GCS commit-marker writes
- implement idempotent create-if-absent upload semantics
- connect successful upload completion to committed-offset advancement
- implement startup recovery for committed and pending sealed batches through GCS commit markers plus local manifests

Validation:

- focused tests for deterministic object naming and state transitions
- integration-style tests for idempotent upload retry behavior

### Phase 6: Runtime Status Emission

- build runtime status event helpers
- add a periodic-status `KasprTask`
- emit healthy and degraded per-instance status events from task-driven snapshots of runtime-local state
- include stable member identity in the event payload
- include offsets, consumed-event timestamps, and upload-degradation reasons when available

Control-plane implication:

- aggregate per-instance status snapshots into materialization-level processing health without replacing CRD-based lifecycle observation

Validation:

- unit tests for event payloads and degraded error reporting

### Phase 7: Schema Proposal Emission

- detect additive fields and incompatible changes
- emit schema proposal events with evidence
- dedupe repeated proposals where practical

Validation:

- unit tests for additive detection and proposal payload shape

### Phase 8: Pause, Resume, And Hardening

- ensure paused runtime spec results in agents naturally no longer receiving new source events
- verify resumed spec returns to healthy `running`
- harden restart and replay behavior

Validation:

- narrow runtime behavior tests where possible
- rendered template diff checks for running versus paused spec

## Recommended First Step

Start with Phase 1 by extending the runtime template family and `runtime_app` package skeleton together.

That gives the project a stable harness boundary first:

- control plane keeps rendering deterministic runtime resources from Jinja
- runtime logic has a clear package home before writer behavior is implemented
- later phases can add behavior without reworking the control-plane apply model

## Resolved Assumptions Before Implementation

The runtime message-processing design is now concrete enough to start implementation with the following working assumptions.

- **Topic semantics:** the MVP runtime is append-only first. A keyed latest-state or upsert-style materialization mode is valuable, but it changes storage semantics enough that it should be treated as a later phase rather than folded into the first writer implementation.
- **Payload format:** source values are already decoded Python-native dictionary-like objects by the time a runtime handler processes them.
- **Source processing semantics:** runtime processing is effectively at-least-once. Once agent work completes, the source message becomes eligible for commit, but commit failure plus shutdown can still cause the same message to be processed again after restart.
- **Runtime-local durability:** once a value is written to runtime-local RocksDB state, the runtime may treat it as durably persisted even if the corresponding table changelog message later fails to publish. This failure mode is known and should not be overengineered in the MVP.
- **Initial object layout choice:** the MVP should keep processing-time bucketed object prefixes because they bound active writer count and work well with late or out-of-order source traffic. More query-optimized event-time layouts or compaction strategies can be added later if real workloads justify them.