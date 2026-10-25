# Control Plane Implementation Plan

## Purpose

This document defines a concrete implementation plan for the control plane that manages Kafka topic materialization workflows.

It also serves as the current-state reference for the implementation in this repository. The main sections still describe the intended architecture, while the "current implementation status" section below records what is actually implemented today.

The control plane is responsible for:

- accepting external materialization requests
- persisting desired materialization state
- running schema discovery as a control-plane-managed workflow step
- creating, updating, pausing, resuming, and deleting Kaspr runtime resources via Kubernetes CRDs
- acting as the authoritative schema coordinator
- consuming status and schema proposal events from runtime apps
- publishing authoritative schema and control events back to runtime apps

When the authoritative schema is promoted, the control plane must also persist the promoted `schema_version` back onto the materialization record so later runtime renders, sweep reconciliation, and delete/reapply flows all operate on consistent state.

This plan intentionally focuses on the control plane first. Runtime app internals are treated only as interfaces the control plane depends on.

## Current Implementation Status

The current control-plane implementation includes all of the following:

- WebView endpoints for create/list/detail/pause/resume/delete
- accepted mutation responses that persist desired state and enqueue request/work events without waiting inline for runtime convergence
- bounded workflow steps for discovery, runtime apply, runtime delete, and runtime observation
- deterministic dedicated runtime rendering plus persisted runtime bindings with spec hashes
- idempotent skip behavior for duplicate `apply_runtime` work and no-op sweep reapply work
- schema-only reconcile behavior that preserves a recent healthy running state instead of bouncing back to `runtime_apply_in_progress`
- one-shot observation retries with sweep-based observation and startup sweep emission
- immediate direct observation follow-up after `delete_runtime`
- seed-schema signaling when observation finds a ready runtime that still has not emitted a healthy heartbeat
- buffered/coalesced additive schema proposals with delayed flush
- current-field-aware schema subsumption so inactive historical forks do not suppress new current additions
- permissive `json` coalescing for buffered additive conflicts on the same field path
- schema promotions persisted back onto both authoritative schema and `materialization.schema_version`
- runtime-facing schema transport that uses:
  - `materialization-schema-authority-updates` for promoted schema updates keyed by `materialization_id`
  - the same topic for `schema_decision` signal envelopes keyed uniquely per decision
  - per-runtime signals topics for direct `seed_schema` and `refresh_schema` signals

## Scope

In scope:

- control-plane responsibilities and boundaries
- control-plane resource model
- persisted control-plane state model
- Kafka topics used by the control plane
- control-plane workflows and phase transitions
- failure handling and idempotency strategy
- CRD rendering and deployment approach
- initial MVP sequencing

Out of scope:

- detailed runtime app implementation
- detailed UI implementation
- detailed parquet writer internals
- cloud storage layout details beyond what the control plane must track

## Constraints And Decisions

The following are explicit decisions and constraints already agreed:

- The control plane will be implemented as a KasprApp.
- The control plane will expose external APIs through one or more KasprWebViews.
- The control plane will create and manage Kaspr runtime resources programmatically through Kubernetes CRDs.
- The control plane must remain event-driven and should not block a synchronous event-processing path waiting for external state changes.
- Runtime placement will be specified explicitly in persisted materialization state rather than inferred by policy initially.
- Assume all runtimes are dedicated at first, but preserve a spec shape that supports shared placement later.
- The control plane will persist state in one global KasprTable initially.
- The global KasprTable will use JSON key and JSON value serialization.
- The control plane will use Kafka topics to communicate with runtime apps.
- Runtime apps will use Kafka topics to communicate back to the control plane.
- The control plane is the authoritative schema owner.
- Runtime apps will use optimistic additive schema evolution and emit schema proposals back to the control plane.
- Schema proposals should include raw samples for troubleshooting.
- The control plane will render CRD resources from predefined templates.
- The control plane will run schema discovery itself as an isolated workflow step.
- Dedicated runtime naming should be configurable per request, with default `<runtime-name-prefix>-<materialization_id>`.
- Target namespace should be configurable per request, with default `kafka-sql`.
- WebViews assume no authentication or authorization for the MVP.
- promoted authoritative schema updates should be keyed by materialization id, while runtime-targeted decision signals must use unique per-signal keys so concurrent waits do not overwrite one another.

## High-Level Architecture

The system is split into two planes.

### Control Plane

Implemented as a dedicated KasprApp.

Responsibilities:

- accept requests
- persist desired state
- advance materialization workflows through short, idempotent event handlers
- run discovery
- publish authoritative schema
- manage runtime CRDs
- consume runtime status and schema proposals
- update authoritative state based on events

### Data Plane

Implemented as one or more runtime KasprApps.

Responsibilities:

- consume assigned Kafka topics continuously
- materialize records to parquet
- emit status/progress events
- emit schema proposals
- consume authoritative schema updates from control plane

The control plane owns lifecycle intent. The runtime owns continuous processing and emits processing-side observations, but the control plane may also need to inspect Kaspr resource status directly in order to derive lifecycle observations the runtime cannot emit.

## Event Processing Model

Kaspr agents must finish processing each consumed event so offsets can be committed cleanly. That makes long-running waits in an event handler a correctness risk, especially during shutdown.

Design rules:

- an agent must not hold an event open while waiting for a runtime status change, Kubernetes operator convergence, or a human action
- each handler should perform a bounded unit of work: validate, persist state, emit follow-up event, apply a spec, or update observed state
- progress across steps should be represented by persisted records plus new Kafka events, not by in-memory loops
- all handlers must be safe under at-least-once delivery and replay after restart

Shutdown behavior:

- if an event is not committed before shutdown, it should be replayed on restart
- correctness should rely on idempotent writes, deterministic names, dedupe keys, and persisted operation records
- the system should tolerate replay without duplicating runtime resources or corrupting observed state
- runtime-side waits that are still holding a source event when shutdown begins should abort promptly so the event can be replayed rather than keeping the app stuck in `wait_empty`

Manual event acknowledgements may be useful in the framework long term, but they should not be the primary consistency mechanism for this design. The primary mechanism should be small handlers plus replay-safe state transitions.

## Control Plane Resource Model

The control plane should model materialization as a first-class domain resource.

### Materialization Spec

Recommended fields:

- `materialization_id`
- `name`
- `source_topic`
- `destination_uri`
- `runtime_mode`
  - `dedicated`
  - `shared`
- `runtime_group`
  - optional grouping key for shared mode later
- `runtime_name`
  - optional explicit runtime resource name override
- `runtime_app_ref`
  - optional explicit app identity if pre-assigned
- `namespace`
- `desired_phase`
- `schema_version`
- `created_at`
- `updated_at`
- `created_by`
- `labels`
- `annotations`

### Materialization State Machine

Suggested phases:

- `requested`
- `discovering`
- `discovered`
- `deploying`
- `running`
- `paused`
- `deleting`
- `deleted`
- `failed`
- `degraded`

Notes:

- `discovered` means authoritative schema v1 exists.
- `deploying` means desired runtime specs were applied and the control plane is awaiting either a healthy runtime heartbeat or enough resource-observation evidence to classify the runtime differently.
- `degraded` means runtime exists but reported recoverable issues.
- `failed` means the workflow cannot proceed without intervention.
- `deploying` should also cover intermediate protocol wait states such as `waiting_for_runtime_agents`, `waiting_for_runtime_ready`, and `waiting_for_seed_schema`; those should not be collapsed into `degraded` until the observation retry budget is exhausted.

### Desired Vs Observed State

Control-plane state should distinguish between desired intent and observed runtime condition.

- desired state is written from external requests or control-plane workflow decisions
- observed state is derived from a hybrid of runtime status events, control-plane apply results, and external inspection of Kaspr resource status
- no synchronous handler should block waiting for observed state to catch up to desired state

Examples:

- desired state: `running`, observed state: `deploying`
- desired state: `paused`, observed state: `running`
- desired state: `deleted`, observed state: `deleting`

### Observation Strategy

Observed state should use a hybrid push and pull model.

Push signals from runtime are best for:

- `running`
- progress and heartbeats
- runtime health degradation
- schema proposal evidence

Pull signals from Kaspr resource inspection are best for:

- `paused`
- `deleted`
- resource not found
- deployment readiness or failure before the runtime is able to emit status
- confirming whether the desired runtime spec has been accepted and reflected by the resource

The control plane should treat KasprApp CRD status as the authoritative source for resource lifecycle observations that require the runtime process to already be stopped, absent, or not yet healthy.

### Runtime Wait-State Detail

The coarse materialization phase should be paired with a more specific `phase_reason` and optional `phase_message` so operators can distinguish expected warm-up from actual failure.

Recommended protocol reasons include:

- `waiting_for_runtime_agents`
- `waiting_for_runtime_ready`
- `waiting_for_seed_schema`
- `waiting_for_source_records`
- `runtime_ready`
- `runtime_data_validation_degraded`
- `runtime_infrastructure_degraded`

These reasons should be surfaced through the detail API and preserved when runtime heartbeats provide more specific context than resource inspection alone.

### Observation Follow-Up And Logging

Observation work should leave a traceable protocol trail without flooding logs.

- resource observations should log their attempt number, result, and follow-up scheduling decision
- seed-schema emission should log both successful sends and explicit skip reasons
- an early mixed-agent snapshot should normally remain `deploying` and schedule another observation instead of being treated as a terminal degradation
- runtime heartbeats should be allowed to move a materialization from stale `degraded` startup observations back into specific warm-up states when appropriate
- delete workflows should prefer an immediate direct observation after runtime delete apply, so resource absence can finalize `deleted` even if scheduled follow-up delivery is temporarily unreliable

### Concrete Observation Inputs

The observation agent should read the following KasprApp fields when present:

- `metadata.generation`
- `status.observedGeneration`
- `status.conditions[]`
- `status.rolloutInProgress`
- `status.availableMembers`
- `status.rebalanceRequired`
- `status.rebalancingMembers`
- `status.members[]`
- `status.linkedResources.agents[]`

The most important per-agent signal is:

- `status.linkedResources.agents[].state`

The control plane should filter `linkedResources.agents[]` down to the materialization-owned agent names recorded in `runtime-binding` and derive agent-state summaries only from that owned subset.

## Concrete Observed State Schema

The control plane should persist a concrete `materialization-status` payload rather than a loose status blob.

Suggested payload shape:

```json
{
  "materialization_id": "mat-123",
  "desired_phase": "running",
  "observed_phase": "deploying",
  "phase_reason": "waiting_for_runtime_ready",
  "phase_message": "KasprApp exists and spec is applied, but runtime has not emitted a healthy running status yet.",
  "last_transition_time": "2026-07-10T12:00:00Z",
  "last_heartbeat": "2026-07-10T12:00:00Z",
  "last_resource_observation_time": "2026-07-10T12:00:05Z",
  "observation_source": "hybrid",
  "runtime_health": {
    "level": "unknown",
    "reason": null,
    "message": null
  },
  "runtime_progress": {
    "rows_written": 1200,
    "bytes_written": 1048576,
    "files_written": 3,
    "latest_offsets": {
      "0": 1024,
      "1": 991
    }
  },
  "resource_observation": {
    "namespace": "kafka-sql",
    "kaspr_app_name": "kaspr-materializer-orders",
    "resource_exists": true,
    "metadata_generation": 4,
    "observed_generation": 4,
    "ready_condition": {
      "status": "True",
      "reason": "Healthy",
      "message": "App is ready",
      "last_transition_time": "2026-07-10T11:59:58Z"
    },
    "progressing_condition": {
      "status": "False",
      "reason": "ReconcileComplete",
      "message": "All resources are in desired state",
      "last_transition_time": "2026-07-10T11:59:58Z"
    },
    "rollout_in_progress": false,
    "available_members": "16/16",
    "rebalance_required": false,
    "rebalancing_members": "0/16",
    "managed_agent_states": {
      "writer": "running",
      "schema-updates": "running",
      "signals": "running"
    }
  },
  "last_error": null,
  "status_version": 1
}
```

### `materialization-status` Field Definitions

- `materialization_id`: stable materialization identifier
- `desired_phase`: latest requested lifecycle state
- `observed_phase`: latest derived lifecycle state
- `phase_reason`: short machine-friendly reason code for the current observed phase
- `phase_message`: operator-readable explanation of the observed phase
- `last_transition_time`: when `observed_phase` last changed
- `last_heartbeat`: latest runtime-emitted heartbeat timestamp, if any
- `last_resource_observation_time`: latest control-plane resource inspection timestamp, if any
- `observation_source`: `runtime_event`, `resource_observation`, or `hybrid`
- `runtime_health`: summarized runtime health level and explanation
- `runtime_progress`: latest runtime progress counters when available
- `resource_observation`: normalized snapshot of the latest KasprApp inspection
- `last_error`: latest relevant error string if one exists
- `status_version`: version number for this persisted status payload shape

### Runtime Health Levels

Allowed values for `runtime_health.level`:

- `healthy`
- `degraded`
- `unknown`
- `unreachable`

`unreachable` should mean the control plane expected runtime signals or resource availability but could not confirm them within the observation window.

### Managed Agent State Summary Schema

`resource_observation.managed_agent_states` should be a map keyed by agent name.

Allowed values should be treated as open-ended strings because Kaspr may evolve them, but the control plane should explicitly recognize at least:

- `running`
- `paused`
- `unknown`

The observation agent should additionally compute these summaries from the owned agents:

- `managed_agents_total`
- `managed_agents_running`
- `managed_agents_paused`
- `managed_agents_unknown`

These derived counts can either be stored alongside `managed_agent_states` or recomputed at read time. Storing them is reasonable if WebView filtering or list views will use them heavily.

## Control Plane State Storage

Use one global KasprTable initially, but store typed records with explicit keys.

### Table Key Strategy

Use namespaced keys.

Examples:

- `materialization:<id>`
- `materialization-status:<id>`
- `materialization-schema:<id>`
- `materialization-op:<id>:<operation_id>`
- `runtime-binding:<id>`

### Record Envelope

Every record should use a common envelope shape:

```json
{
  "entity_type": "materialization",
  "entity_id": "mat-123",
  "version": 3,
  "updated_at": "2026-07-08T00:00:00Z",
  "payload": {}
}
```

This keeps a single-table model workable without losing structure.

### Required Record Types

#### `materialization`

Desired state and durable user intent.

Fields:

- materialization id
- name
- source topic
- destination uri
- runtime mode
- runtime name override
- resolved runtime name
- target namespace
- desired phase
- created by
- spec version

#### `materialization-status`

Observed workflow state.

Fields:

- observed phase
- desired phase snapshot
- phase reason
- phase message
- last transition time
- last error
- last heartbeat
- last resource observation time
- discovery status
- runtime apply status
- runtime health summary
- progress summary
- observation source summary

#### `materialization-schema`

Authoritative schema state.

Fields:

- current schema version
- schema columns or shape
- discovered at
- latest promoted additive proposal id
- latest rejected proposal id

#### `materialization-op`

Tracks short-lived control-plane operations for idempotency and restart safety.

Examples:

- discovery requested
- runtime apply requested
- pause requested
- delete requested

Suggested fields:

- operation id
- materialization id
- operation type
- status
- dedupe key
- created at
- updated at
- last result summary

#### `runtime-binding`

Tracks where the materialization is deployed.

Fields:

- runtime mode
- kaspr app name
- kaspr agent names
- namespace
- template version
- last applied spec hash
- last apply operation id

## Kafka Topic Contracts

The control plane should use explicit Kafka topics for workflow coordination.

### Control-Plane Owned Topics

#### `materialization-requests`

Purpose:

- durable intake of create, update, delete, pause, and resume requests after WebView acceptance

Produced by:

- control-plane WebView handlers

Consumed by:

- control-plane request intake agent

#### `materialization-work`

Purpose:

- internal control-plane work queue for next-step execution and retries

Produced by:

- control plane

Consumed by:

- control plane workflow agents

This topic is useful when WebView handlers should only validate, persist, and enqueue work.

The control plane scheduler can also deliver delayed or recurring observation work into this topic.

#### `materialization-status`

Purpose:

- runtime-to-control-plane lifecycle and progress reporting

Produced by:

- runtime apps

Consumed by:

- control plane

#### `materialization-schema-proposals`

Purpose:

- runtime additive schema proposals

Produced by:

- runtime apps

Consumed by:

- control plane

#### `materialization-schema-authority-updates`

Purpose:

- authoritative schema updates emitted by control plane
- shared delivery channel for ongoing authoritative schema promotions
- shared delivery channel for runtime `schema_decision` signal envelopes that must keep draining while writers are waiting

Produced by:

- control plane

Consumed by:

- runtime apps

### Event Schemas

#### Materialization Request Event

Fields:

- `request_id`
- `materialization_id`
- `action`
  - `create`
  - `pause`
  - `resume`
  - `delete`
  - `update`
- `submitted_at`
- `submitted_by`
- `spec`

#### Materialization Work Event

Fields:

- `work_id`
- `materialization_id`
- `step`
  - `start_discovery`
  - `apply_runtime`
  - `delete_runtime`
  - `observe_runtime`
  - `sweep_observations`
  - `repair_state`
- `requested_at`
- `caused_by`
- `dedupe_key`

#### Materialization Status Event

Fields:

- `event_id`
- `materialization_id`
- `runtime_app_ref`
- `member_id`
- `agent_name`
- `phase`
- `health`
- `rows_written`
- `bytes_written`
- `files_written`
- `latest_offsets`
- `schema_version`
- `event_time`
- `phase_reason`
- `phase_message`
- `health_reason`
- `reported_at`
- `last_error`

Minimum useful contract:

- event id
- materialization id
- runtime app ref
- member id
- agent name
- phase
- health
- schema version
- reported at
- last error when applicable

Progress fields such as rows, bytes, files, and current processed offsets should be included when available, but should remain optional.

An idle but successfully started runtime should still be allowed to emit `health: healthy` even before it has processed its first source record. Lack of processed offsets or counters should not by itself imply `unknown` or `degraded` health.

The runtime should not be required to report metrics that depend on knowing topic end state, such as lag, highwater offsets, or max offsets.

Recommended semantics:

- `phase` should be limited to processing-side states the runtime can truthfully emit while alive, such as `running` or `degraded`
- runtime events should not be required to emit `paused` or `deleted`
- `member_id` should identify the specific runtime member or pod instance that emitted the event so the control plane can persist per-instance heartbeats separately from app-level lifecycle observation
- `agent_name` should identify which runtime agent emitted the event so the control plane can correlate it with `linkedResources.agents[].name`

### Concrete Runtime Status Event Schema

Suggested payload shape:

```json
{
  "event_id": "evt-01jzstatus",
  "materialization_id": "mat-123",
  "runtime_app_ref": "kaspr-materializer-orders",
  "member_id": "member-0",
  "agent_name": "writer",
  "phase": "running",
  "health": "healthy",
  "schema_version": 2,
  "rows_written": 1200,
  "bytes_written": 1048576,
  "files_written": 3,
  "latest_offsets": {
    "0": 1024,
    "1": 991
  },
  "event_time": "2026-07-10T12:00:00Z",
  "reported_at": "2026-07-10T12:00:00Z",
  "last_error": null
}
```

The control plane should treat these events as additive per-instance observations that refresh heartbeat, health, and processing position. They should not be the sole source of lifecycle truth.

#### Schema Proposal Event

Fields:

- `proposal_id`
- `materialization_id`
- `runtime_app_ref`
- `base_schema_version`
- `proposed_additions`
- `breaking_changes`
- `sample_partition`
- `sample_offset`
- `raw_sample`
- `proposed_at`

#### Schema Authority Update Event

Fields:

- `materialization_id`
- `schema_version`
- `schema`
- `promoted_from_proposal_id`
- `published_at`

The same topic also carries runtime signal envelopes of the form:

- `signal_id`
- `materialization_id`
- `signal_type`
- `sent_at`
- `payload`

Currently this is used for `schema_decision` deliveries, while direct per-runtime signals such as `seed_schema` and `refresh_schema` go to the dedicated runtime signals topic.

## Kaspr Resources For Control Plane

Initial control-plane resource set:

### `KasprApp`

One control-plane app.

Responsibilities:

- bootstrap servers
- storage
- python packages for:
  - kubernetes client
  - any schema helpers
  - any cloud or shared libs if required later

### `KasprTable`

One global control-plane state table.

Purpose:

- authoritative state persistence for control-plane entities

### `KasprWebView`

One or more webviews.

Suggested endpoints:

- `GET /materializations`
- `POST /materializations`
- `GET /materializations/{id}`
- `POST /materializations/{id}/pause`
- `POST /materializations/{id}/resume`
- `DELETE /materializations/{id}`

List endpoint constraints:

- list materializations by scanning the `materialization:` key prefix in the global KasprTable
- support only bounded `limit` values of `10`, `30`, `100`, or `all`
- avoid rich filtering or search in the MVP because the underlying table access pattern is prefix scan, not indexed query
- return compact list entries rather than full detailed materialization payloads

The WebView should not perform full orchestration inline. It should:

- validate request
- write or update table state
- publish a request event
- return accepted response

MVP assumption:

- no authentication or authorization

### `KasprAgent` Roles

Suggested agent decomposition:

#### Request Intake Agent

Consumes:

- `materialization-requests`

Responsibilities:

- load materialization state
- determine the next workflow step
- persist desired state changes
- emit `materialization-work` events

#### Workflow Apply Agent

Consumes:

- `materialization-work`

Responsibilities:

- execute one idempotent workflow step at a time
- run discovery when requested
- render runtime CRDs
- apply or delete runtime CRDs
- persist operation records and binding state
- schedule follow-up observation work when needed
- emit follow-up work only when another bounded step is required

#### Runtime Status Agent

Consumes:

- `materialization-status`

Responsibilities:

- update observed state
- mark degraded, failed, or running from runtime-emitted signals
- update progress fields
- emit follow-up work only when a state transition is needed

#### Resource Observation Agent

Consumes:

- `materialization-work`

Responsibilities:

- inspect KasprApp resource status for a specific materialization or a sweep batch
- infer `deploying`, `paused`, `deleting`, `deleted`, or resource-absence conditions
- update observed state when runtime cannot emit the relevant lifecycle signal
- reschedule bounded follow-up observation work when convergence has not happened yet

This agent should not run a long-lived polling loop. It should process an observation work item, inspect resource state once, persist results, optionally schedule another observation, and finish.

### KasprApp-To-Observed-Phase Mapping

The observation agent should derive `observed_phase` using these rules in order.

1. If the KasprApp resource is not found:
  - if desired phase is `deleting`, observed phase becomes `deleted`
  - otherwise observed phase becomes `failed` or `degraded` depending on whether absence is expected for the workflow step
2. If `status.observedGeneration` is missing or lower than `metadata.generation`, observed phase remains `deploying`.
3. If `status.rolloutInProgress` is `true`, observed phase remains `deploying`.
4. If the Ready condition is `True` and the owned `linkedResources.agents[]` subset exists and every owned agent state is `paused`, observed phase becomes `paused`.
5. If the Ready condition is `True`, observed phase becomes `running`.
6. If the Ready condition is not `True`, or owned agent states are mixed or unknown after readiness should have been reached, observed phase becomes `degraded`.
7. Escalate from `degraded` to `failed` only when a higher-level workflow timeout or explicit fatal apply result says the control plane should stop retrying automatically.

Important implications:

- `status.conditions` takes precedence for application-level readiness because the Kaspr operator already evaluates member health internally
- `linkedResources.agents[].state` is the primary signal for confirming `paused` when the app itself is still ready
- `deleted` should be inferred from resource absence during delete workflows, not from runtime-emitted events
- `failed` should be a control-plane policy decision, not a direct translation of a single CRD snapshot
- member-level flags such as `recovering` or `rebalancing` should not be reinterpreted independently by the control plane when `Ready` is already present; those are part of the operator's readiness decision

### Observation Reason Codes

Recommended `phase_reason` values:

- `requested`
- `discovering`
- `schema_discovered`
- `runtime_apply_in_progress`
- `waiting_for_runtime_ready`
- `runtime_ready`
- `all_managed_agents_paused`
- `resource_missing_after_delete`
- `resource_generation_not_observed`
- `resource_rollout_in_progress`
- `runtime_heartbeat_stale`
- `resource_ready_but_runtime_silent`
- `resource_not_ready`
- `mixed_agent_states`
- `apply_failed`
- `observation_timeout`

#### Schema Proposal Agent

Consumes:

- `materialization-schema-proposals`

Responsibilities:

- validate additive proposals
- reject breaking proposals
- promote additive proposals to authoritative schema versions
- emit `materialization-schema-authority-updates`

## Scheduler And Delayed Work

The control-plane KasprApp should enable the built-in scheduler.

Recommended use:

- use one-shot scheduled messages for delayed follow-up observation after `apply_runtime`, `delete_runtime`, pause, and resume operations
- use a shared recurring cron for periodic sweeps of materializations whose observed state may become stale
- run one startup-only sweep task when the control-plane runtime boots so observation coverage begins immediately after restart
- deliver scheduled messages into `materialization-work`

Recommended pattern:

- one-shot delayed observe events should use stable scheduler request ids such as `observe:<materialization_id>` so they can be replaced or canceled
- periodic sweep cron should use `missed_fire_policy=skip` because these are monitoring checks, not must-replay business events
- follow-up observe events should use bounded backoff instead of tight polling
- the startup-only sweep should be a separate `KasprTask` without `cron` or `timer`

Best-fit design for MVP:

- do not create a dedicated cron per materialization by default
- create a single shared sweep cron for the control plane
- add one shared startup sweep trigger for the control plane
- combine that sweep with per-materialization one-shot delayed observations triggered by mutations

This keeps the design event-driven without introducing long-running handlers or a large number of cron definitions.

## CRD Rendering Strategy

The control plane should render Kaspr runtime resources from predefined templates.

### Recommendation

Use a small template renderer with typed input models.

Inputs should include:

- materialization id
- runtime mode
- source topic
- destination uri
- schema version
- runtime app name
- namespace
- resource profile
- template version

Defaults:

- default runtime app naming convention: `<runtime-name-prefix>-<materialization_id>`
- default namespace: `kafka-sql`

Both defaults should be overridable in the materialization request.

Outputs should include:

- `KasprApp`
- one or more `KasprAgent` resources
- optional runtime `KasprTable` resources if required by runtime design

### Template Rules

- templates must be versioned
- rendered resource names must be deterministic
- output must be comparable for apply dedupe and optional repair detection
- control plane should store last applied template version and spec hash in `runtime-binding`
- any control-plane-managed runtime package reference included in rendered resources must be immutable from the control plane's perspective

For the current Python package installation model, that means the rendered dedicated runtime spec must pin `materializer-control-plane` to the exact control-plane package revision or another immutable artifact reference. Floating refs such as branch names are not sufficient, because they allow pod-level code drift without changing the rendered runtime spec.

### Idempotent Apply Model

For each apply step:

1. compute desired runtime resource set
2. fetch or infer existing resource state
3. compare desired spec hash with last applied hash
4. apply only if drift or state transition requires it

Important limitation:

- this comparison only detects drift that is encoded in the rendered runtime resources
- it does not detect mutable external content hidden behind an unchanged spec, such as a runtime app that installs Python code from a floating Git branch
- therefore any externally resolved runtime package or image reference that should participate in reconcile must be rendered as an immutable revisioned value

The control plane does not need to implement a heavyweight reconciliation loop in order to deploy Kaspr resources. The Kaspr Operator already reconciles the applied CRDs.

Control-plane responsibility is narrower:

- compute the desired CRD spec from persisted intent
- apply that spec idempotently
- persist what was applied
- react asynchronously to later runtime status, scheduled observation work, direct resource inspection results, and new requests

An optional lightweight repair pass may still be useful later for partial-failure recovery, but it should be framed as replayable work generation rather than a long-lived synchronous reconciliation loop.

## Workflow Design

### Create Materialization

1. WebView validates request.
2. WebView writes `materialization` and `materialization-status` with desired phase `running` and observed phase `requested`.
3. WebView emits `materialization-requests` create event.
4. Request intake agent emits `materialization-work` with step `start_discovery`.
5. Workflow apply agent updates observed state to `discovering` and runs discovery.
6. Control plane writes `materialization-schema` version 1.
7. Control plane updates observed state to `discovered` and emits `materialization-work` with step `apply_runtime`.
8. Workflow apply agent renders runtime CRDs.
9. Control plane applies runtime CRDs.
10. Control plane writes `runtime-binding`, marks observed state `deploying`, and schedules delayed `observe_runtime` work.
11. Event processing finishes.
12. Runtime starts independently.
13. If resource observation shows the runtime app is ready but the runtime is still waiting for first healthy status, the control plane emits a `seed_schema` signal to `<runtime-app>-signals`.
14. The runtime signals agent persists schema v1 into the runtime-local schema-state table.
15. The writer can then resume normal processing and the status task emits healthy `materialization-status` events.
16. Runtime status agent updates observed state to `running` when a healthy runtime event is received. That healthy event may legitimately contain no progress counters yet if the runtime is idle.
17. Ongoing authoritative schema promotions continue to flow on `materialization-schema-authority-updates`.

### Pause Materialization

1. Pause request updates desired state to `paused` and emits a request event.
2. Request intake agent emits `materialization-work` for runtime apply.
3. Workflow apply agent computes the paused runtime spec and applies it.
4. Control plane updates observed state to `deploying` or `pausing` if a transient phase is useful.
5. Event processing finishes.
6. Control plane schedules delayed `observe_runtime` work.
7. Resource observation agent inspects KasprApp status and marks observed state `paused` when the resource reflects the paused condition.

### Resume Materialization

1. Resume request updates desired state to `running` and emits a request event.
2. Request intake agent emits `materialization-work` for runtime apply.
3. Workflow apply agent computes the resumed runtime spec and applies it.
4. Control plane updates observed state to `deploying` or `resuming` if a transient phase is useful.
5. Event processing finishes.
6. Runtime later emits `running` status.
7. Runtime status agent updates observed state.
8. If runtime status is absent or delayed, resource observation work can inspect KasprApp status to distinguish between still-deploying and failed-to-become-ready.

### Delete Materialization

1. Delete request updates desired state to `deleting` and emits a request event.
2. Request intake agent emits `materialization-work` with step `delete_runtime`.
3. Workflow apply agent deletes runtime CRDs and persists the delete operation.
4. Workflow apply agent schedules delayed `observe_runtime` work.
5. Event processing finishes without waiting.
6. Resource observation agent marks observed state `deleted` once the KasprApp resource is absent or its status indicates terminal removal.

### Runtime Schema Proposal

1. Runtime detects additive field.
2. Runtime continues optimistically.
3. Runtime emits `materialization-schema-proposals` event.
4. Control plane validates the proposal against current authoritative schema.
5. If additive-only, control plane increments schema version.
6. Control plane updates `materialization-schema`.
7. Control plane emits `materialization-schema-authority-updates`.
8. Runtimes consume and adopt the official schema version.

This same topic should also be used for initial schema v1 delivery and runtime repair after `apply_runtime`. Runtime schema adoption should not depend solely on later proposal-driven updates.

### Runtime Breaking Change Proposal

1. Runtime detects incompatible field type.
2. Runtime does not auto-promote.
3. Runtime emits a proposal or error event with evidence and raw sample.
4. Control plane records rejection or degraded status.
5. Operator or follow-up policy decides remediation.

## Discovery Design

Discovery will be executed by the control plane as an isolated workflow step.

### Why

- discovery is finite, not continuous
- discovery directly establishes authoritative schema v1
- it avoids an extra synchronization loop with a separate discovery app initially
- it simplifies first implementation

### Important Rule

Discovery should not run as a synchronous WebView request path.

It should run as a durable, internal control-plane workflow step triggered by stored request state and Kafka events.

That preserves:

- retryability
- idempotency
- failure visibility
- non-blocking API behavior
- bounded event handler execution

### Discovery Bootstrap Strategy

Initial schema discovery should sample the source Kafka topic directly using the control plane's Kafka credentials.

Recommended behavior:

- consume a bounded sample, starting from the earliest available record
- derive schema v1 from the first successfully decoded payload using the same field-path vocabulary the runtime uses for proposal detection
- record top-level and nested fields as `payload.*` paths, including arrays and nested objects
- if the topic is currently empty, fall back to a generic bootstrap schema rather than blocking materialization creation forever

For the current MVP, the empty-topic fallback may remain a generic JSON payload schema such as a single `value` field. That fallback should be treated as a bootstrap convenience for new idle topics, not as equivalent evidence to an observed payload-derived schema.

Discovery itself only establishes the authoritative schema in control-plane state. Initial delivery of that authoritative schema to a fresh runtime should happen after `apply_runtime` through a per-runtime bootstrap signal once resource observation confirms the runtime app exists and is ready to consume it. Later schema promotions continue to use the shared `materialization-schema-authority-updates` topic.

## Resource Observation Design

The control plane should explicitly observe Kaspr resources as part of lifecycle management.

Recommended model:

- mutation handlers schedule one-shot delayed `observe_runtime` work after apply or delete actions
- delete currently emits an immediate direct `observe_runtime` follow-up so resource absence is checked right away after delete apply
- a shared scheduler cron emits `sweep_observations` work periodically for stale, transitional, or suspicious materializations
- a startup-only `KasprTask` emits one `sweep_observations` work event whenever the control-plane runtime starts
- the observation agent queries the Kubernetes API for the target KasprApp and interprets resource status fields
- observation results update `materialization-status` and may schedule another delayed observation if convergence is still pending
- when observation sees `waiting_for_runtime_ready` for a runtime whose resource is ready, the control plane may emit a direct `seed_schema` runtime signal instead of waiting for some later unrelated event to initialize runtime schema state

The sweep-based reconcile path should also re-render the desired runtime resources and compare their spec hash with the persisted `runtime-binding.last_applied_spec_hash`. If those differ, the control plane should emit `apply_runtime` repair work ahead of ordinary observation work.

That repair path only works when the desired runtime differences are visible in the rendered resources. In practice, this means mutable runtime package refs such as `@develop` must not be used for control-plane-managed dedicated runtimes, because periodic sweep cannot infer code drift that is not represented in the rendered spec.

This is preferable to a continuously running polling loop because it preserves the bounded-event-processing model while still allowing the control plane to learn states the runtime cannot self-report.

## Failure Modes And Idempotency

The control plane must assume any step can fail after partial success.

### Core Principle

Every step must be resumable from persisted state.

### Required Practices

- persist desired state before acting
- emit internal work events after persistence
- write operation records for multi-step actions
- prefer emitting follow-up work over waiting inline for external convergence
- use delayed observation work for lifecycle confirmation when runtime status is insufficient
- use deterministic resource naming
- store last applied template version and spec hash
- treat repeated events as safe retries
- make sweep-triggered `apply_runtime` work idempotent so duplicate reconcile deliveries do not reapply the same spec or re-emit follow-up churn
- when a schema-only reconcile happens while a recent healthy runtime heartbeat already exists, preserve the effective running state instead of regressing back to `waiting_for_runtime_ready` or re-emitting seed schema

### Example Failure Cases

#### Request Accepted, Discovery Never Starts

Mitigation:

- request persisted as `requested`
- work topic replay or a retry producer picks it up later

#### Discovery Succeeds, Runtime CRDs Fail To Apply

Mitigation:

- authoritative schema already stored
- observed status remains `discovered` or `deploying`
- a later work event retries apply idempotently

#### Runtime Never Emits Status After Deploy

Mitigation:

- delayed observation work inspects KasprApp status
- control plane distinguishes between still-deploying, failed readiness, paused, or missing resource
- startup sweep covers stale state immediately after control-plane restarts
- periodic sweep cron catches materializations whose observation work was missed or whose state became stale

#### Runtime Pod Code Drifts Without A Rendered Spec Change

Example:

- the runtime spec installs the control-plane package from a floating branch ref
- the running runtime pod keeps an older resolved commit
- the control plane later moves to a newer commit, but the rendered runtime spec remains textually unchanged

Mitigation:

- render managed runtime package refs as immutable revision-pinned values
- include that pinned value in the rendered runtime spec hash persisted in `runtime-binding`
- let periodic sweep emit `apply_runtime` when the pinned revision changes

#### Runtime CRDs Created, Binding Not Persisted

Mitigation:

- deterministic names let later work rediscover intended resources
- control plane can rebuild missing binding state

#### Runtime Emits Additive Schema Proposal Multiple Times

Mitigation:

- proposal ids or proposal content hashes
- control plane deduplicates before version promotion

#### Pause Request Arrives During Deployment

Mitigation:

- desired state becomes paused
- a later work item applies the paused spec once deployment-related work is replayed or completed

#### App Shuts Down With Uncommitted Event In Flight

Mitigation:

- event is replayed on restart
- operation records and dedupe keys prevent duplicate side effects
- observed state is rebuilt from persisted records plus later status events and resource observations

## Initial MVP

### MVP Goals

- create materialization through WebView
- persist materialization state in global control-plane table
- perform discovery in control plane
- persist schema v1
- render and apply dedicated runtime CRDs from template
- observe runtime status events
- observe KasprApp resource status for lifecycle states runtime cannot emit
- process additive schema proposals
- promote authoritative schema versions

### MVP Assumptions

- all runtime apps are dedicated
- one global control-plane table
- one namespace or a small controlled set of namespaces
- one runtime template family
- no shared runtime placement logic yet

### MVP Non-Goals

- automated placement policy
- dynamic shared runtime multiplexing
- cross-cluster deployment
- fine-grained quota management
- advanced schema governance
- manual event acknowledgement support in the Kaspr framework

## Phase Plan

### Phase 1: Control Plane Skeleton

- define table record schema
- define Kafka event contracts
- implement WebView endpoints
- implement bounded prefix-scan list endpoint for materializations
- implement request persistence
- implement internal work topic flow

### Phase 2: Discovery Workflow

- implement discovery handler in control plane
- persist authoritative schema v1
- transition workflow state correctly

### Phase 3: Runtime CRD Apply Flow

- implement template rendering
- implement deterministic naming
- implement create, update, and delete apply logic
- persist runtime binding records

This phase is about short idempotent apply operations, not a long-lived control-plane reconcile loop.

### Phase 4: Runtime Event Handling

- consume status events
- implement delayed and recurring observation scheduling
- inspect KasprApp resource status
- consume schema proposals
- update observed state
- publish authoritative schema updates

### Phase 5: Reliability Hardening

- add retry policies
- add operation records
- add dedupe keys for events
- add repair paths for partial failures

## Resolved Defaults And Decisions

- KasprTable serialization: JSON key and JSON value
- default dedicated runtime name: `<runtime-name-prefix>-<materialization_id>`
- default target namespace: `kafka-sql`
- WebView authentication and authorization: none for MVP
- schema proposals should include raw samples
- authoritative schema updates should be keyed by materialization id only

## Runtime Status Contract

Runtime status contract is now defined in this document.

Summary:

- minimum required fields: `event_id`, `materialization_id`, `runtime_app_ref`, `agent_name`, `phase`, `health`, `schema_version`, and `reported_at`
- optional fields: progress counters, current processed offsets, `event_time`, `phase_reason`, `phase_message`, `health_reason`, and `last_error`
- runtime events should describe live processing state, not terminal lifecycle states such as `deleted`

## Implementation Readiness

Before starting control-plane implementation, the remaining key items are:

- `0` key open questions
- `0` blocking contract-definition tasks
- `0` optional cleanup tasks

### Resolved Observation Precedence

- `status.conditions` is authoritative for application-level readiness.
- If `Ready=True`, the application is considered running from the operator's perspective because the Kaspr operator already evaluated member-level health.
- Member-level flags such as `recovering=true` are not re-evaluated independently by the control plane when `Ready` is already present.
- A ready application can still have owned agents in `paused` state, and in that case the control plane should report observed phase `paused`.
- Resource absence during a delete workflow remains the signal for `deleted`.

### Resolved Decision

- Do not explicitly set retention or compaction policies for control-plane Kafka topics.
- Rely on platform defaults.
- Assume KasprTables use their default compact retention behavior.

### Remaining Tasks

All major control-plane contract artifacts needed to begin implementation now exist under `control-plane/contracts/` and `control-plane/PACKAGE_LAYOUT.md`.

Cleanup tasks are complete.

## Recommended Next Step

Begin control-plane implementation with the current contract set, using the current MVP observation rules and refining the exact KasprApp status precedence only if real runtime manifests reveal gaps.