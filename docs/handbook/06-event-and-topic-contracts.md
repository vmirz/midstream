# 06 — Event and Topic Contracts

Every message between components is JSON on Kafka. This document is the complete wire contract
of 1.0. §6.16 lists what 1.0 removed.

## 6.1 Topic inventory

| Topic (base) | Producers | Consumers | Key |
|--------------|-----------|-----------|-----|
| `materialization-requests` | the mutation WebViews | `request-intake` agent | `materialization:<id>` |
| `materialization-work` | `request-intake`, `workflow-apply`, `resource-observation`, `identity-intake` (a claim's answer), the sweep and maintenance tasks, the scheduler | `workflow-apply`, `resource-observation`, `maintenance` | `materialization:<id>` |
| `materialization-status` | each runtime's `emit-status` task | `runtime-status` agent | `<id>` |
| `materialization-table-events` | each runtime's committer agent; the maintenance app's `maintenance-worker` agent and `maintenance-step` task | `table-events` agent; every member of the tail app, with a plain consumer of no group (19) | `materialization:<id>` (§6.6) |
| `materialization-schema-authority-updates` | `settings-intake`, `workflow-apply` | every runtime's `settings` agent | `runtime-settings`, `protection-policy:<id>` (§6.7) |
| `<runtime>-commits` | the runtime's pipeline and commit-timer tasks, its committer (wakeups) | the runtime's committer agent | the constant `commits` (§6.8) |
| `<release>-maintenance-requests` | the control plane's `maintenance` agent, the `materializations-maintenance` WebView | the maintenance app's `maintenance-worker` agent | `maintenance:<id>:<table>` (§6.17) |
| `<release>-tail-requests` | the control plane's `maintenance` agent | the tail app's `tail-worker` agent | `tail:<id>:<table>` (§6.18) |
| `<release>-control-plane-app-schedule-requests` | `workflow-apply`, `resource-observation` | the Kaspr scheduler | `materialization:<id>` |
| `<release>-settings-requests` | the `settings-update` WebView; the `settings-bootstrap` task | `settings-intake` agent | `settings:<scope>` (§6.14) |
| `<release>-identity-requests` | the auth, users, groups, roles, dataset and token WebViews; `request-intake` (claims and releases of table names); `identity-bootstrap` and `identity-maintenance` tasks | `identity-intake` agent | `identity`, always (§6.15) |

Every topic name starts with the Helm release name, the scheduler topic as
`<release>-control-plane-app-schedule-requests` (§6.9). Runtime topics carry the runtime
app's name, which already starts with it.

> **Key convention split.** Control-plane topics (`requests`, `work`, `table-events`) key on the
> **prefixed** `materialization:<id>` (`materialization_routing_key`). The heartbeat topic keys on
> the **bare** `<id>`. Intentional, and easy to get wrong when writing a new producer.

Every topic a control-plane agent uses declares `valueSerializer: json` and `declare: true` (the
app has `topicAllowDeclare: true`). The table-events topic is new in 1.0: a deploy that adds it to
a running control plane needs one consumer-group rebalance (14 §14.3); the 1.0 rollout starts the
control plane from zero members anyway (12 §12.13).

`materialization-schema-authority-updates` kept its 0.x name so no deployment had to rename it; it now
carries only runtime settings (§6.7).

## 6.2 Routed-output envelope

Agents cannot decide at runtime which of several static output topics to publish to. Kaspr solves
this with **predicates on outputs**; this codebase solves the general case with a routed-output
envelope produced by `midstream/scheduler/__init__.py`.

A handler yields dicts of this shape, and the agent manifest has one output per route with a
matching `predicate`.

### Route `work`

```python
# code-derived
build_direct_work_output(work_event) == {
    "route":   "work",
    "key":     materialization_routing_key(work_event.materialization_id),
    "value":   work_event.to_dict(),
    "headers": {},
}
```

Matched by the manifest output whose predicate is `value.get("route") == "work"` and whose `name`
is `materialization-work`.

### Route `scheduler`

```python
# code-derived
build_scheduled_work_output(work_event, request_id=..., deliver_to=..., deliver_at=...) == {
    "route":   "scheduler",
    "key":     materialization_routing_key(work_event.materialization_id),
    "value":   work_event.to_dict(),
    "headers": {
        "x-scheduler-action":     b"REPLACE",
        "x-scheduler-request-id": request_id.encode("utf-8"),
        "x-scheduler-deliver-to": deliver_to.encode("utf-8"),
        "x-scheduler-deliver-at": deliver_at.encode("utf-8"),
    },
}
```

`build_scheduled_topic_output(topic=..., key=..., value=..., request_id=..., deliver_at=...)` is the
same for an arbitrary topic; it sets `x-scheduler-deliver-to` to `topic`.

### Route `topic`

```python
# code-derived
build_direct_topic_output(topic=..., key=..., value=...) == {
    "route":   "topic",
    "key":     key,
    "value":   value,
    "topic":   topic,       # ← only this route carries a "topic" field
    "headers": {},
}
```

Matched by the manifest output that has a `nameSelector` returning `value.get("topic")`. Runtime
settings deliveries take this route.

### Runtime routes `events` and `commits`

The runtime's committer yields `{"route": "events", "key": "materialization:<id>", "value": TableEvent}`
for `materialization-table-events` (`Committer._event`), as the maintenance app does
(`TableMaintenance._event`). The templates match it with a predicate, as the control plane does.
(Before 2.0 the settings agent also yielded `{"route": "commits", …}`, forwarding maintenance
requests to `<runtime>-commits`.)

### Route `maintenance`

The control plane's `maintenance` agent yields `{"route": "maintenance", "key": "maintenance:<id>:<table>",
"value": MaintenanceMessage}` (`services/table_maintenance.py::maintenance_output`), sent on
`<release>-maintenance-requests` by its second output (§6.17).

### Route `tail`

The same agent yields `{"route": "tail", "key": "tail:<id>:<table>", "value": LiveTailMessage}`
(`services/live_tail.py::live_tail_output`), sent on `<release>-tail-requests` by its third
output (§6.18).

### Scheduler header semantics

| Header | Type | Meaning |
|--------|------|---------|
| `x-scheduler-action` | bytes | Always `b"REPLACE"` in this codebase — replaces any pending schedule with the same request id |
| `x-scheduler-request-id` | bytes | Stable identity for the schedule. Re-emitting with the same id cancels and replaces the earlier one |
| `x-scheduler-deliver-to` | bytes | Destination topic once the delay elapses |
| `x-scheduler-deliver-at` | bytes | UTC ISO-8601 delivery time |

**`REPLACE` is what makes retries idempotent.** Two observation follow-ups with
`request_id = "observe:mat-abc"` collapse into one pending delivery rather than doubling.

| Producer | `x-scheduler-request-id` |
|----------|--------------------------|
| observation follow-up | `observe:<materialization_id>` |
| other scheduled work | `<step>:<materialization_id>` |
| missing-state retry | `missing-state:<materialization_id>:<step>` |
| Kafka cleanup and table drop retries | see `services/workflow.py` |

## 6.3 `materialization-requests`

**Producer:** the mutation WebViews (create, stop, resume, update, delete), via `topicSend` with
`passThrough: true`.
**Consumer:** `request-intake` agent.
**Key:** `materialization:<materialization_id>`
**Value:** `MaterializationRequestEvent.to_dict()`

```json
{
  "request_id": "req-9f8e7d6c5b4a",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "action": "create",
  "submitted_at": "2026-10-18T10:30:00Z",
  "submitted_by": "jane@example.com",
  "spec": {
    "name": "Shop",
    "tables": [
      { "name": "orders", "source_topic": "sales.orders", "dataset": "sales", "dataset_id": "ds-5c1e…" },
      { "name": "invoices", "source_topic": "finance.invoices", "dataset": "finance", "dataset_id": "ds-9a0b…" }
    ],
    "destination_uri": "gs://my-lake",
    "runtime_mode": "dedicated",
    "runtime_name": "midstream-mat-a1b2c3d4e5f6",
    "namespace": "midstream",
    "resource_profile": null,
    "storage_size": "64Mi",
    "replicas": 16,
    "skip_tombstones": false,
    "autoscaling": true,
    "partition_granularity": "day",
    "labels": {},
    "annotations": {}
  },
  "claim_request_id": "idr-3e8f0a1b2c4d"
}
```

`tables` are the materialization's tables, one per source topic, each in its dataset and named as
the create view decided; a table's dataset and name are fixed for its life, as is
`partition_granularity` (`hour`, `day`, `month`, `none`; null takes `default_partition_granularity`).
`destination_uri`, `namespace`, `runtime_name` (the runtime's generated, length-bounded name) and
`storage_size` are the deployment's, filled in by the API: a request chooses none of them since 4.0
(04 §4.5.2).
`claim_request_id` is the id of the `claim_tables` request intake sends (§6.15); `label`, on an
update, a new label. An event from before 4.3 may carry `live_tail`, on an update or in a create's
spec; it is read and dropped. `action` ∈ `create`, `stop`, `resume`, `update`, `delete`,
plus `pause`, which no endpoint produces since 0.4.0 but which still decodes so events already on
the topic are not poison. `update` is produced by `PATCH /materializations/{id}` and carries the
new `replicas`, and `autoscaling` when it switches it (null otherwise; on a create, null is on). An
event from before 4.4 has no `autoscaling`: a create from it gets autoscaling on.

### Consumer behavior

`handlers/request_intake.handle_request_event_for_output` → `services/request_intake.process_request_event`:

1. **Persist** (`_persist_request_state`):
   - `create`: insert `Materialization` (desired `running`) if absent, then
     `MaterializationStatus` (observed `requested`) if absent. Both are idempotent inserts.
   - other actions: `replace(materialization, desired_phase=..., updated_at=...)` and
     `replace(status, desired_phase=...)`. If no status exists, a fallback is synthesized first.
2. **Validate** (`build_work_event_from_request`):
   - the materialization must exist → `ValueError: materialization <id> is missing`
   - `materialization.desired_phase` must equal `EXPECTED_DESIRED_PHASE[action]`
   - for `create` only: the status must exist and `observed_phase == "requested"`
3. **Emit** a `MaterializationWorkEvent`.

### Action → step and expected phase

| `action` | work `step` | expected `desired_phase` |
|----------|-------------|--------------------------|
| `create` | `start_discovery` | `running` |
| `stop` | `apply_runtime` | `stopped` |
| `pause` (legacy) | `apply_runtime` | `paused` |
| `resume` | `apply_runtime` | `running` |
| `delete` | `delete_runtime` | `deleting` |
| `update` | `apply_runtime` | unchanged — any phase but `deleting`/`deleted`; intake applies `spec.replicas` and `spec.autoscaling` and keeps the phase |

Because intake persists *before* validating, the expected-phase check can only fail if something
else concurrently changed desired state — which is why keying every event on `materialization:<id>`
(single-partition ordering per materialization) matters.

## 6.4 `materialization-work`

**Producers:** `request-intake`, `workflow-apply`, `resource-observation`, `maintenance`, the sweep
and maintenance tasks, and the Kaspr scheduler (delayed deliveries).
**Consumers**, each filtering its own steps:

| Agent | Steps |
|---|---|
| `workflow-apply` | `tables_claimed`, `claim_rejected`, `start_discovery`, `discover_tables`, `create_table`, `apply_runtime`, `await_runtime_drained`, `delete_runtime`, `cleanup_runtime_kafka`, `drop_table`, `purge_materialization_records` |
| `resource-observation` | `observe_runtime`, `sweep_observations` |
| `maintenance` | `purge_deleted_materializations`, `refresh_consumer_lag`, `record_consumer_lag`, `schedule_table_maintenance` |

**Key:** `materialization:<materialization_id>`
**Value:** `MaterializationWorkEvent.to_dict()`

The key is also what places a materialization's records in the `state` table. The table is global
(every member reads every shard), but a write or a delete goes to the changelog shard of the event
being handled. So everything written about a materialization is written while handling an event
keyed by it, and lands in one shard. The scheduled triggers are keyed `materialization:__system__`
and write nothing. What they find for a materialization goes back onto this topic, keyed by that
materialization: the observation sweep's `observe_runtime` and reconcile `apply_runtime`, the
retention sweep's `purge_materialization_records`, and the lag refresh's `record_consumer_lag`,
which carries the measured `consumer_lag` — and, handling it, autoscaling writes its record and may
send an `apply_runtime` of its own (07 §7.12a). `identity-intake` answers a claim of table names the same
way: `tables_claimed` or `claim_rejected`, keyed by the materialization and carrying the `claim`
(05 §5.9), so the workflow applies it in the materialization's shard; the directory agent itself never
writes the `state` table. Before 2.0 the lag refresh saved statuses, and the retention sweep deleted
records, from the `__system__` event. That left a second status copy in
that shard for every tracked materialization, and tombstones in a shard that held none of the
records being purged.

The step sets are disjoint, so every message is handled once — but `repair_state` is a declared
`WORK_STEP` that **no agent filters for**; emitting it would silently drop (14 §14.1).

### Full example

```json
{
  "work_id": "work-req-9f8e7d6c5b4a",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "step": "start_discovery",
  "requested_at": "2026-10-18T10:30:00Z",
  "caused_by": { "kind": "request", "id": "req-9f8e7d6c5b4a" },
  "dedupe_key": "start_discovery:mat-a1b2c3d4e5f6:req-9f8e7d6c5b4a",
  "target_runtime": {
    "namespace": "midstream",
    "kaspr_app_name": "midstream-mat-a1b2c3d4e5f6"
  }
}
```

Observation follow-up:

```json
{
  "work_id": "work-observe-mat-a1b2c3d4e5f6",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "step": "observe_runtime",
  "requested_at": "2026-10-18T10:30:20Z",
  "caused_by": { "kind": "scheduler", "id": "observe:mat-a1b2c3d4e5f6" },
  "dedupe_key": "observe:mat-a1b2c3d4e5f6:2",
  "target_runtime": { "namespace": "midstream", "kaspr_app_name": "midstream-mat-a1b2c3d4e5f6" },
  "observation_policy": {
    "attempt": 2,
    "max_attempts": 4,
    "scheduled_for": "2026-10-18T10:31:05Z",
    "mode": "one_shot"
  }
}
```

Sweep trigger (synthetic):

```json
{
  "work_id": "work-sweep-sweep-observations-task-2026-10-18T10:30:00Z",
  "materialization_id": "__system__",
  "step": "sweep_observations",
  "requested_at": "2026-10-18T10:30:00Z",
  "caused_by": { "kind": "scheduler", "id": "sweep-observations-task" },
  "dedupe_key": "sweep:sweep-observations-task:2026-10-18T10:30:00Z"
}
```

A measured lag, handed to the materialization's own shard (`consumer_lag` as in 04 §4.6):

```json
{
  "work_id": "work-record-consumer-lag-mat-a1b2c3d4e5f6-2026-10-18T10:31:00Z",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "step": "record_consumer_lag",
  "requested_at": "2026-10-18T10:31:00Z",
  "caused_by": { "kind": "scheduler", "id": "refresh-consumer-lag" },
  "dedupe_key": "record_consumer_lag:mat-a1b2c3d4e5f6:2026-10-18T10:31:00Z",
  "consumer_lag": {
    "total_lag": 1204,
    "topics": { "orders": { "total_lag": 1204, "partition_lag": { "0": 600, "1": 604 } } },
    "measured_at": "2026-10-18T10:31:00Z"
  }
}
```

### `work_id` conventions

| Origin | `work_id` |
|--------|-----------|
| request intake | `work-<request_id>` |
| discovery → table creation | `work-create-table-<materialization_id>` |
| table creation → apply | `work-apply-<materialization_id>` |
| apply → observe follow-up | `work-observe-<materialization_id>` |
| observation follow-up (retry) | **reuses the incoming `work_id`** |
| sweep observe | `work-observe-<materialization_id>` |
| sweep reconcile apply | `work-apply-reconcile-<materialization_id>-<spec_hash[:12]>` |
| deleted → Kafka cleanup | `work-cleanup-kafka-<materialization_id>` |
| Kafka cleanup → table drop | `<work_id>-drop-table` |
| table drop → next pass, or retry | **reuses the incoming `work_id`** |
| table drop → purge | `<work_id>-purge-records` |
| retention sweep → purge | `work-retention-purge-<materialization_id>-<requested_at>` |
| lag refresh → record | `work-record-consumer-lag-<materialization_id>-<measured_at>` |
| autoscaling → apply | `work-apply-autoscale-<materialization_id>-<measured_at>` |
| sweep trigger | `work-sweep-<task_id>-<requested_at>` |
| missing-state retry | **reuses the incoming `work_id`** |

`work_id` doubles as the **operation id** of operation records, which is why retries reuse it:
repeated attempts collapse into one record. Table-drop passes reuse it for the same reason — the
record accumulates the deleted count across passes.

### `dedupe_key` conventions

| Origin | `dedupe_key` |
|--------|--------------|
| request intake | `<step>:<materialization_id>:<request_id>` |
| discovery → table creation | `create_table:<materialization_id>:<work_id>` |
| table creation → apply | `apply_runtime:<materialization_id>:<work_id>` |
| observation follow-up | `observe:<materialization_id>:<attempt>` |
| sweep observe | `observe:<materialization_id>:sweep` |
| sweep reconcile apply | `apply_runtime:<materialization_id>:reconcile:<full_spec_hash>` |
| Kafka cleanup | `cleanup_runtime_kafka:<materialization_id>` |
| table drop, every pass and retry | `drop_table:<materialization_id>` |
| purge after delete, or by the retention sweep | `purge_materialization_records:<materialization_id>` |
| lag refresh → record | `record_consumer_lag:<materialization_id>:<measured_at>` |
| autoscaling → apply | `apply_runtime:<materialization_id>:autoscale:<replicas>:<measured_at>` |
| sweep trigger | `sweep:<task_id>:<requested_at>` |
| missing-state retry | `<step>:<materialization_id>:<work_id>:missing-state:<attempt>` |

`dedupe_key` is recorded in operation records for traceability. The duplicate guard for
`apply_runtime` is the **spec hash**, not the dedupe key.

## 6.5 `materialization-status`

**Producer:** each runtime member's `emit-status` task, every `heartbeat_interval` (30 s).
**Consumer:** `runtime-status` agent.
**Key:** bare `<materialization_id>`
**Value:** `RuntimeStatusEvent.to_dict()` (`runtime_app/handlers/status.py::build_runtime_heartbeat`)

```json
{
  "event_id": "evt-status-midstream-mat-a1b2c3d4e5f6-app-0-2026-10-18T10:35:00Z",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "runtime_app_ref": "midstream-mat-a1b2c3d4e5f6",
  "member_id": "midstream-mat-a1b2c3d4e5f6-app-0",
  "agent_name": "midstream-mat-a1b2c3d4e5f6-writer",
  "reported_at": "2026-10-18T10:35:00Z",
  "phase": "running",
  "health": "healthy",
  "phase_reason": "processing",
  "phase_message": "Runtime is actively processing source records.",
  "runtime_version": "2.0.0",
  "settings_revision": 4,
  "settings_lineage": "20261024T101502Z-3f9a1c2e",
  "schema_versions": { "orders": 2, "payments": 0 },
  "latest_offsets": { "orders": { "0": 51234, "1": 50880 }, "payments": { "0": 9120 } },
  "latest_committed_offsets": { "orders": { "0": 49998, "1": 49870 }, "payments": { "0": 9000 } },
  "event_time": "2026-10-18T10:34:58Z",
  "rows_written": 152340,
  "bytes_written": 48210944,
  "files_written": 31,
  "records_skipped": 0,
  "pending_upload_batches": 1,
  "pending_upload_bytes": 2097152,
  "files_pending_commit": 2,
  "unmapped_values": 0,
  "table_counters": { "orders": { "rows_written": 120000, "unmapped_values": 0, "protected_values": 120000, "withheld_values": 0 }, "payments": { "rows_written": 32340 } },
  "protection_revisions": { "orders": 2, "payments": 2 },
  "protection_key_id": "3f9a1c2e",
  "performance": { "…": "a window of rates, plus acknowledgements and the pipeline in flight" },
  "tables": {
    "orders": {
    "member_id": "midstream-mat-a1b2c3d4e5f6-app-0",
    "table": "orders",
    "source_topic": "orders",
    "metadata_location": "gs://…/shop-a1b2c3d4e5f6/orders/metadata/00042-….metadata.json",
    "current_snapshot_id": 8410567672455764292,
    "snapshot_at": "2026-10-18T10:34:41.512Z",
    "sequence_number": 41,
    "total_records": 152001, "total_files": 29, "total_bytes": 41875202,
    "schema_id": 2,
    "committed_offsets": { "0": 49998, "1": 49870 },
    "valid_through": "2026-10-18T10:34:12Z",
    "snapshots": 41,
    "last_commit_at": "2026-10-18T10:34:41Z",
    "last_commit_duration_ms": 2367,
    "pending_files": 2
    },
    "payments": { "member_id": "midstream-mat-a1b2c3d4e5f6-app-0", "table": "payments", "…": "" }
  }
}
```

`schema_versions` holds each table's schema id as the member last loaded it; `latest_offsets` the
highest offset it handled per table and partition; `table_counters` its own counters per table, the
values it masked and withheld among them; `protection_revisions` the protection revision it masks
each table with, and `protection_key_id` the id of the protection key it loaded (22 §22.7).
**Only the committer member sends `tables`** (`committer_table_blocks`), one block per table; their
`member_id` names it, and the console marks that member as the committer. A runtime before 2.0 also
reported `small_files`, `last_compaction_at` and the running maintenance job here; maintenance is the
maintenance app's now, and reports itself (§6.6).

Degraded example:

```json
{
  "phase": "degraded",
  "health": "degraded",
  "health_reason": "commits_stalled",
  "phase_reason": "commits_stalled",
  "phase_message": "Files this member uploaded 312 s ago are not committed yet: the committer is not committing. Its member's logs say why."
}
```

A member that must hash and has no protection key, or another than the deployment's, reports
`health_reason` `protection_key_missing` or `protection_key_mismatch`, degraded, and writes nothing
for that table until it has the right key (22 §22.5).

### Runtime `phase_reason` values

| Value | Meaning |
|-------|---------|
| `waiting_for_table` | the member has not loaded its table's pointer yet (unreadable, or just starting) |
| `waiting_for_source_records` | the table is loaded and nothing has been read |
| `processing` | records are being written |
| `commits_stalled` | files this member uploaded have waited `COMMITS_STALLED_INTERVALS` commit intervals (10) for a commit |
| `runtime_data_validation_degraded`, `runtime_infrastructure_degraded` | the member's `last_error` and its kind |
| `runtime_reported_degraded` | an error without a kind |

### Merge rules (`services/observation.py::apply_runtime_status_event`)

A heartbeat for a materialization being deleted is ignored. One for a stopped materialization only
records progress: the stop is confirmed by observing the KasprApp at zero members. Otherwise the
event's member is merged into `runtime_progress.member_progress`, every recently reporting member
is summarized (`summarize_member_states`), and the first matching row applies:

| Condition (over the summarized members) | `observed_phase` | `phase_reason` |
|-----------|------------------|----------------|
| health `degraded` | `degraded` | `_runtime_degraded_phase_reason(health_reason)` — e.g. `commits_stalled` |
| reported reason `waiting_for_table` | `deploying` | `waiting_for_table` |
| `ready_condition.status == "True"` and health `healthy` | `running` | the reported reason, or `runtime_ready` |
| `ready_condition.status == "True"` | `deploying` | the reported reason, or `waiting_for_runtime_ready` |
| otherwise | unchanged | unchanged |

Always: `last_heartbeat`, `runtime_version`, `runtime_health`, `runtime_progress` (absolute
counters per member, replaced, never accumulated), `last_error`, `observation_source`, and
`last_transition_time` only when `observed_phase` changed. The `table` block is kept per member
and surfaces as `progress.table` in the API (04, 05).

> **Consequence:** if the KasprApp's `ready_condition` has never been observed as `True`, a
> perfectly healthy heartbeat will not move `observed_phase` to `running`. Runtime health alone is
> never sufficient (08).

## 6.6 `materialization-table-events`

**Producers:** each runtime's committer agent — the one member that commits (10 §10.6) — and the
maintenance app, for maintenance (10 §10.10).
**Consumer:** the `table-events` agent (`handlers/table_events.py` → `services/table_events.py`),
which updates the record of the table the event names (`materialization-table:<id>:<table>`,
05 §5.12) and, for schema changes, that table's projections the API serves.
**Key:** `materialization:<materialization_id>`
**Value:** `TableEvent.to_dict()` (`models/table.py`)

```json
{
  "event_id": "evt-table-table_committed-4f1c2d3e5a6b7c8d",
  "kind": "table_committed",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "table": "orders",
  "occurred_at": "2026-10-18T10:34:41Z",
  "runtime_app_ref": "midstream-mat-a1b2c3d4e5f6",
  "member_id": "midstream-mat-a1b2c3d4e5f6-app-0",
  "payload": { "…": "by kind, below" }
}
```

| `kind` | `payload` |
|---|---|
| `table_committed` | `snapshot_id`, `sequence_number`, `committed_at`, `files_added`, `records_added`, `bytes_added`, `totals` (`records`, `files`, `bytes`), `offsets` (partition → the last offset in the table, inclusive), `valid_through`, `duration_ms`, `metadata_location`, `schema_id`, `snapshots` |
| `schema_changed` | `schema_id`, `schema` (Iceberg JSON), `fork_map`, `fields` and `field_count` (the rendered projection, 09 §9.9), `changes` (`added`, `forked`, `collapsed`, `rejected`, each with its path, field id and type), `metadata_location`, `last_column_id`, `rejected_paths`, `duration_ms`, and `protection`, the table's `materializer.protection` property, when it records one. A recorded policy is announced the same way with the same `schema_id` and `changes: []`; its projection replaces the one of the same schema by its protection revision (22 §22.6) |
| `maintenance_started` | `job` (`compaction`, `orphan_cleanup`), `started_at`, `partition` (compaction), `attempt` — sets the record's `maintenance_job` |
| `maintenance_completed` | `job` (`compaction`, `expire_snapshots`, `orphan_cleanup`), `outcome` (`succeeded`, `skipped`, `abandoned`, `failed`, `cancelled`), `started_at` (a long job's, clearing the record's `maintenance_job`), `partition` (compaction), `snapshot_id`, `duration_ms`, `message`, `counts` (compaction: `partitions`, `inputs`, `outputs`, `records`, `input_bytes`, `output_bytes`; expiry: `expired`, `remaining`; orphans: `listed`, `deleted`…), and after a compaction's commit `totals`, `metadata_location`, `sequence_number`, `snapshots` |
| `table_error` | `reason` (`table_unavailable`, `commit_failed`, `schema_evolution_failed`, `schema_field_limit`, `compaction_failed`, `protection_commit_failed`), `message` |

Every event names its `table` (2.0); one without — from a 1.0 runtime — is logged and dropped, as is
one for a table the materialization has no record of, or one being dropped or dropped, and every
event about a materialization being deleted — but for the `maintenance_completed` that ends the
running job of a table being dropped, or of a materialization being deleted, which a drop waits for
(07 §7.10). Before 4.4.1 the latter was dropped too.
A compaction's report changes the record's totals and snapshot only when its `sequence_number` is
newer than the record's, as a commit's does: the two producers report apart. A committer repeats a `table_error` of one
reason at most every `ERROR_REPEAT_SECONDS`, per table. The agent applies a commit only if its
sequence number is newer than the table record's (`_newer`), so a redelivered or reordered event
changes nothing; a `table_error` degrades the materialization with that reason and the message
`Table <table>: <message>`, and the table's next successful commit clears it — unless another table
still has an open error, which then degrades it in its stead (`degrade_for_table_error`,
`_clear_table_degradation`).

## 6.7 `materialization-schema-authority-updates`: runtime control

The broadcast topic every runtime's `settings` agent consumes
(`runtime_app/handlers/runtime_settings.py::handle_control_message`); a runtime drops messages for
another materialization before they reach the package.

### Runtime settings

**Producers:** `settings-intake`, after a change to the `runtime` scope, and when it begins the
scope's lineage at startup (§6.14); `workflow-apply`, when it applies a runtime, addressed to that
runtime alone (`runtime_settings_output_for_apply`).
**Key:** `runtime-settings`

```json
{
  "kind": "runtime_settings",
  "materialization_id": "*",
  "target_materialization_id": null,
  "revision": 7,
  "lineage": "20261024T101502Z-3f9a1c2e",
  "values": { "commit_interval_seconds": 15, "compaction_min_input_files": 8 },
  "published_at": "2026-10-18T10:40:00Z"
}
```

A delivery carries **every** override, not the change, so whichever one a runtime sees last leaves
it right. A runtime ignores one that is not newer than what it holds, by `lineage`, then
`revision`: a later lineage wins whatever revision an earlier one reached, and a delivery without a
lineage (`null`, from a control plane before 4.1) sorts before every lineage. The settings agent
stores it in the runtime's state table and installs it (`live_settings.py`, 17 §17.7).

### Protection policy

**Producer:** `workflow-apply`, at every apply of a runtime — its creation, an edit, a resume, the
sweep's re-apply, an upgrade — addressed to that runtime alone
(`services/protection.py::protection_policy_output_for_apply`); nothing for a materialization from
before data protection that no edit gave a policy (revision 0).
**Key:** `protection-policy:<materialization_id>`

```json
{
  "kind": "protection_policy",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "revision": 4,
  "policy": { "kinds": { "email": "partial", "phone": "hash", "secret": "redact" } },
  "changed_by": "jane@example.com",
  "published_at": "2026-11-20T15:12:39Z"
}
```

`reason` is added when the edit recorded the same policy again with the current rules
(`apply_rules`). The policy is not part of the runtime's spec, so an edit is such an apply with an
unchanged spec: nothing is applied to Kubernetes, nothing restarts. The settings agent stores the
newest revision in the state table (`protection-policy`), ignoring one that is not newer or that is
addressed to another runtime, and the committer records it in each table (22 §22.7). A runtime from
before data protection drops the kind unread.

A `maintenance_request` here is a 1.x control plane's; a 2.0 runtime drops it (maintenance requests go
to the maintenance app, §6.17).

## 6.8 `<runtime>-commits`

Each runtime's control topic: **one partition**, retention one hour, declared by the runtime
(`committer-agent.yaml.j2`). The member that owns the partition runs the committer; Kaspr moves it
when that member goes away (10 §10.6). Every message is keyed `commits`.

| `kind` | Producer | Meaning |
|---|---|---|
| `data_file_ready` | a member's pipeline task, through its outbox | a sealed file is uploaded and may be committed to the `table` it names |
| `schema_change_request` | a member's writer, through its outbox | values the named `table`'s schema cannot take |
| `commit_tick` | `commit-timer` task (`onLeader`), every 5 s | the committer's clock: commit what is pending once `commit_interval_seconds` have passed since the last commit |
| `wakeup` | the committer itself | a schema window's debounce ended |

```json
{"kind": "data_file_ready", "materialization_id": "mat-a1b2c3d4e5f6", "table": "orders",
 "member_id": "midstream-mat-a1b2c3d4e5f6-app-2", "session_generation": 12, "session_epoch": "9b0e…",
 "batch_id": "batch-…", "seal_id": "4f1c…", "file_count": 2,
 "partition": 3, "after_offset": 99, "offset_start": 100, "offset_end": 199,
 "file_path": "gs://…/shop-a1b2c3d4e5f6/orders/data/ts_day=2026-10-18/p03-o0000000100-0000000199-midstream-mat-a1b2c3d4e5f6-app-2-….parquet",
 "file_size": 10485760, "record_count": 61000,
 "partition_value": {"ts_day": "2026-10-18"},
 "event_time_min": "2026-10-18T09:58:01Z", "event_time_max": "2026-10-18T10:02:12Z",
 "schema_id": 4, "announced_at": "2026-10-18T10:02:40Z", "attempt": 1}

{"kind": "schema_change_request", "materialization_id": "mat-a1b2c3d4e5f6", "table": "orders",
 "runtime_app_ref": "midstream-mat-a1b2c3d4e5f6", "member_id": "midstream-mat-a1b2c3d4e5f6-app-2",
 "base_schema_id": 4,
 "observations": [{"path": ["customer", "phone"], "observed": {"type": "struct", "fields": {"number": "string"}}},
                  {"path": ["items", "[]", "tax"], "observed": {"type": "double"}}],
 "sample": {"partition": 3, "offset": 120}, "requested_at": 1760781742123}

{"kind": "commit_tick", "materialization_id": "mat-a1b2c3d4e5f6", "sent_at": "2026-10-18T10:02:45Z"}
```

A seal that splits into several files (one per table partition its rows touch) announces each with
the seal's `seal_id`, `file_count` and `after_offset`: the committer takes a seal only whole, and only
when `after_offset` meets the offsets the table already has (10 §10.6).

`member_id` is the pod's name, which a restarted member keeps. `session_generation` is the member's
session, the app's rebalance count, which starts again when the member restarts. `session_epoch` names
the member's process: a random id it makes once at its start. The committer compares generations only
within one epoch, so an announcement of another epoch counts as the member's next process, whatever
its generation (10 §10.5, §10.6). A runtime that predates the epoch sends none, and its announcements
are judged by generation alone.

`observed` carries **shapes only, never a value**, so no record content travels on this topic. A
member re-announces what is not yet committed after a committer change, so the topic's one-hour
retention loses nothing that matters.

## 6.9 Scheduler topic

`<release>-control-plane-app-schedule-requests`: the control plane's Kaspr app id,
`<release>-control-plane-app`, plus `-schedule-requests`, as the scheduler names it.

Consumed by the Kaspr scheduler, enabled by `spec.config.schedulerEnabled: true` and
`schedulerCronEnabled: true` on the control-plane app. The scheduler holds the message until
`x-scheduler-deliver-at`, then republishes the `value` to `x-scheduler-deliver-to` with the original
key.

> Before the chart, the base manifests wrote to a topic without the `-app-` segment while the dev
> overlay wrote to the right one (14 §14.2). The chart derives the name from the app id. If delayed
> work silently disappears, compare the agents' second output with the scheduler's topic first.

## 6.10 Complete message flow for a create

```
1. POST /materializations
   → materialization-requests  key=materialization:mat-X  value=MaterializationRequestEvent(action=create)

2. request-intake persists Materialization + MaterializationStatus
   → materialization-work  value=WorkEvent(step=start_discovery)

3. workflow-apply: samples up to 200 records of the topic, builds schema 0 (09 §9.6),
   sets observed_phase=discovered
   → materialization-work  value=WorkEvent(step=create_table)

4. workflow-apply: creates the Iceberg table and its pointer (11 §11.7), persists the table record
   → materialization-work  value=WorkEvent(step=apply_runtime)

5. workflow-apply: renders + applies the runtime's resources, persists RuntimeBinding,
   sets observed_phase=deploying
   → materialization-schema-authority-updates  key=runtime-settings  (only if runtime settings were ever changed)
   → scheduler  headers={deliver-to: materialization-work, deliver-at: +15s}
                value=WorkEvent(step=observe_runtime, policy.attempt=1)

6. runtime members start, load the table (waiting_for_table until then), read the topic
   → <runtime>-commits  data_file_ready, schema_change_request, commit_tick…
   committer → materialization-table-events  table_committed, schema_changed

7. every member's emit-status task (30 s)
   → materialization-status  key=mat-X  value=RuntimeStatusEvent(phase=running, health=healthy)

8. runtime-status merges → observed_phase=running once ready_condition is True
```

## 6.11 Complete message flow for a record

```
1. writer agent (any member): the record is staged as a JSONL line in its partition's open batch;
   drift detection finds `customer.phone`, which schema 4 does not have
   → outbox → <runtime>-commits  schema_change_request(base_schema_id=4, observations=[customer.phone])

2. committer: opens a schema window, waits schema_evolution_debounce_seconds for more,
   evolves schema 4 → 5 (09 §9.3), commits it through the pointer
   → <rt>-state  schema record (every member reads it before its next seal)
   → materialization-table-events  schema_changed(schema_id=5, changes=[added customer.phone])

3. the batch rolls; its seal waits for schema 5 if it saw the change (seal_schema_wait_seconds),
   conforms its rows to it, writes Parquet with field ids, uploads it
   → outbox → <runtime>-commits  data_file_ready(after_offset, offset_start, offset_end, …)

4. committer, at the next commit_tick: decides each seal (covered / accepted / deferred…),
   commits the accepted files in one append whose summary carries the new offsets
   → materialization-table-events  table_committed(snapshot_id, offsets, valid_through)

5. members see the table's committed offsets cover their batch and acknowledge its records;
   Faust commits the Kafka offsets (10 §10.7)
```

## 6.12 Ordering and delivery guarantees

- **Per-materialization ordering** holds on `materialization-requests`, `materialization-work` and
  `materialization-table-events`: every message keys on `materialization:<id>`.
- **`materialization-work` has three consumer agents** in the same app, each filtering a disjoint
  step set, so nothing is processed twice.
- **`<runtime>-commits` has one partition and one committer**: announcements, ticks and requests
  are handled strictly in order by the only process that commits to the table.
- **Control-plane delivery is at-least-once**, and reprocessing is safe: persistence is idempotent
  insert/replace, `apply_runtime` is guarded by the spec hash, table events by their sequence
  number, settings changes by `request_id`.
- **Delivery into the table is exactly once.** Announcements may be delivered twice or after a
  committer change; the committer refuses a file the table already covers and commits offsets in
  the same snapshot as the rows (10 §10.6).
- **`x-scheduler-action: REPLACE`** collapses duplicate delayed work by request id.
- **Settings changes apply in order per scope** (`settings:<scope>` keys); a redelivered request
  changes nothing.
- **Runtime settings deliveries are complete snapshots** with a revision, so a runtime can miss
  some, see them twice or out of order, and still end on the newest it saw.
- **The runtime control topic is broadcast**: every runtime's settings agent reads every message
  and keeps its own. Messages are rare — a settings change, a deploy, a maintenance request.

## 6.13 Adding a new event type — checklist

1. Add the model to `models/events.py` (or `models/table.py` for a table event) with
   `to_dict`/`from_dict` and `__post_init__` validation; extend the relevant kinds tuple
   (`WORK_STEPS`, `TABLE_EVENT_KINDS`).
2. Export it from `models/__init__.py`.
3. Add a builder in `scheduler/__init__.py` if it needs routing, or in a service if it is a plain
   payload.
4. Add the producing call site in a `services/` module, or the runtime's `committer.py`.
5. Add the consuming handler in `handlers/` (control plane) or `runtime_app/handlers/`.
6. Wire the topic in `charts/midstream/templates/agents/<agent>.yaml` (input topic, or output with predicate),
   or in a runtime template.
7. Derive its name from the release in the template, as every other topic's is.
8. If the topic name should be configurable, add it to `ControlPlaneConfig` **and** to the
   control-plane app template's env.
9. Add a round-trip test in `python/tests/test_models.py` and a behavior test in the nearest
   `test_*.py`.
10. Run `python -m unittest discover -s python/tests -p 'test_*.py' -v` and
    `helm template midstream charts/midstream -f charts/midstream/ci/test-values.yaml`.

## 6.14 `<release>-settings-requests`

**Producers:** the `settings-update` WebView (`PATCH /settings/{scope}`), after validating the
change; the `settings-bootstrap` task, once at every start.
**Consumer:** the `settings-intake` agent, the only writer of the `settings` table.
**Key:** `settings:<scope>` (`storage/settings.py::settings_routing_key`).
**Value:** `SettingsChangeRequest.to_dict()`, or the task's `SettingsBootstrapRequest.to_dict()` (05 §5.13)

```json
{
  "request_id": "set-1ce02c047587",
  "scope": "control-plane",
  "submitted_at": "2026-10-12T23:18:51Z",
  "submitted_by": "jane@example.com",
  "values": { "max_runtime_replicas": 32 },
  "reset": ["default_destination_uri"],
  "base_revision": 13,
  "fallbacks": {}
}
```

The agent applies it, or records why it refused it as the scope's `last_rejection`
(`settings_changed`, `invalid_setting`). For the `runtime` scope it also yields the runtime settings
delivery of §6.7.

The task's request, keyed `settings:runtime` so it applies in order with that scope's changes:

```json
{ "kind": "bootstrap", "scope": "runtime", "requested_at": "2026-10-24T10:15:02Z" }
```

When the `runtime` scope has no lineage, the agent begins one, saves it, and yields the scope's
delivery to every runtime (§6.7); otherwise it does nothing.

## 6.15 `<release>-identity-requests`

**Producers:** the views that change people, groups, roles, datasets, their grants and access tokens
(`users-*`, `groups-*`, `roles-*`, `datasets-*` mutations, `auth-password`, `auth-tokens-create`,
`auth-tokens-revoke`), the auth views that record sessions (`auth-login`, `auth-refresh`,
`auth-logout`), `request-intake` (claims and releases of table names), and the `identity-bootstrap`
and `identity-maintenance` tasks.
**Consumer:** the `identity-intake` agent, the only writer of the `directory` and `identity-audit`
tables. It also reads the `state` table, for the claim sweep, and answers claims on
`materialization-work` (§6.4).
**Key:** the constant `identity` (`storage/identity.py::identity_routing_key`), for every message.
**Value:** `IdentityRequest.to_dict()` (05 §5.14)

```json
{
  "request_id": "idr-7e8f9a0b1c2d",
  "action": "sign_in_succeeded",
  "submitted_at": "2026-10-14T09:00:00Z",
  "actor_id": "usr-3f9c2a1b7d4e",
  "actor_email": "jane@example.com",
  "payload": {
    "session_id": "ses-8c1e0f2a9b3d4c5e",
    "user_id": "usr-3f9c2a1b7d4e",
    "method": "password",
    "created_at": "2026-10-14T09:00:00Z",
    "expires_at": "2026-10-21T09:00:00Z"
  },
  "base_revision": null,
  "context": { "ip": "10.1.2.3", "user_agent": "Mozilla/5.0 …" }
}
```

One key means one partition: every identity change, sign-in event and housekeeping request is
applied strictly in order by one agent instance. That is what lets the agent enforce rules that
span records — one account per email, an active administrator always remains, nobody grants
access they do not hold — without locks. Passwords and token secrets are hashed in the WebView,
never in the agent, and never travel on the topic in plain text (18 §18.15).

| Action group | Sent by | Outcome stored? |
|---|---|---|
| Directory changes (`create_user` … `delete_role`, `change_password`) | the directory views, `auth-password` | yes, `request:<id>`, for `GET /identity/requests/{id}` |
| `create_access_token`, `revoke_access_token` | `auth-tokens-create`, `auth-tokens-revoke` | yes |
| `create_dataset`, `update_dataset`, `delete_dataset`, `set_dataset_grant` | the `datasets-*` views | yes |
| `claim_tables` | `request-intake`, for a create or an update adding tables | yes, and answered on `materialization-work` (`tables_claimed` / `claim_rejected`) |
| `release_tables` | `request-intake`: a delete, or a change of topics that drops tables and adds none | no |
| `sign_in_succeeded`, `sign_in_failed`, `session_refreshed`, `sign_out` | `auth-login`, `auth-refresh`, `auth-logout` | no |
| `bootstrap`, `maintenance` | `identity-bootstrap` (startup), `identity-maintenance` (cron `41 * * * *`) | no |

A redelivered directory change returns its stored outcome and changes nothing. Treat the topic and
the tables' changelogs as sensitive. See 18.

## 6.16 Removed in 1.0

| 0.x topic or message | What replaced it |
|---|---|
| `materialization-schema-proposals` | `schema_change_request` on `<runtime>-commits` (§6.8): shapes only, decided by the runtime's committer |
| `<runtime>-signals` (`seed_schema`, `schema_decision`, `refresh_schema`) | nothing: no writer waits for a schema; the table is the schema (09) |
| Schema promotions and rejections on `materialization-schema-authority-updates` | `schema_changed` table events (§6.6); the topic carries only runtime control (§6.7) |
| The control plane's `schema-proposals` agent, the runtime's `schema-updates` and `signals` agents | the runtime's `committer` and `settings` agents |
| The `compact_object_manifests` and `cleanup_destination_objects` steps | `drop_table` (§6.4); the object manifest is gone (11) |

## 6.17 `<release>-maintenance-requests`

What the control plane tells the maintenance app (07 §7.13, 10 §10.10). Six partitions, declared by
the app's `maintenance-worker` agent and by the control plane's `maintenance` agent, whichever starts
first. Every message about a table has the same key, `maintenance:<materialization id>:<table>`
(`maintenance_routing_key`), so one member of the app handles each table.

**Producers:** the control plane's `maintenance` agent, every minute (`schedule_table_maintenance`):
a `maintenance_tick` per table `active` or `retired` of every materialization not being deleted, a
`maintenance_cancel` per table being dropped or of a materialization being deleted; and the
`materializations-maintenance` WebView, a `maintenance_request` per table a request names.
**Consumer:** `maintenance-worker`.
**Value:** `MaintenanceMessage.to_dict()` (`models/maintenance.py`, 05 §5.9)

```json
{
  "kind": "maintenance_tick",
  "target": { "materialization_id": "mat-a1b2c3d4e5f6", "namespace": "sales", "table": "orders",
              "location": "gs://my-lake/midstream/mat-a1b2c3d4e5f6/orders",
              "source_topic": "orders", "partition_granularity": "day", "state": "active" },
  "issued_at": "2026-10-20T04:11:00Z",
  "settings": { "compaction_enabled": true, "compaction_interval_minutes": 15, "…": "every MAINTENANCE_SETTING_FIELDS" },
  "last_runs": { "compaction": "2026-10-20T02:38:23.302Z", "orphan_cleanup": "2026-10-20T01:25:17.377Z" },
  "running": null
}
```

A request adds `job`, `request_id` and `requested_by`; a cancel adds `reason` (`table_dropping`,
`materialization_deleting`) and may carry `running`. A malformed message is logged and dropped.

## 6.18 `<release>-tail-requests`

What the control plane tells the tail app, the KasprApp that serves the newest records of the
materializations' tables from Kafka before they are committed (19). **64 partitions**, declared by
the app's `tail-worker` agent and by the control plane's `maintenance` agent, whichever starts first.
Every message about a table has the same key, `tail:<materialization id>:<table>`
(`models/live_tail.py::live_tail_routing_key`), so it lands in one partition, and the one member of
the app Kaspr gives that partition to **owns** the table: it tails it, and every other member
forwards requests about it there (19). The partition count bounds the app's members; changing it
later moves every table once.

**Producer:** the control plane's `maintenance` agent, every minute (`schedule_table_maintenance`,
the step that ticks the maintenance app, §6.17). While `live_tail_enabled` is on it names **every
table** still in the catalog: a `tail_tick` for each table its runtime writes (`active`) of a
materialization that is running, and a `tail_cancel` for every other - a table being dropped
(`table_dropping`), one kept read-only after its topic was removed (`table_retired`: records past its
committed offsets will never be committed), or of a materialization being deleted
(`materialization_deleting`) or stopped (`materialization_stopped`). A control plane before 4.3 also
cancelled a materialization with live tail turned off (`live_tail_off`), which the app still takes.
A dropped table is named no more. With `live_tail_enabled` off nothing is sent,
and the app forgets every table once no tick has named it for its forget window.
**Consumer:** `tail-worker`.
**Value:** `LiveTailMessage.to_dict()` (`models/live_tail.py`, 05 §5.9)

```json
{
  "kind": "tail_tick",
  "target": { "materialization_id": "mat-a1b2c3d4e5f6", "dataset": "sales", "dataset_id": "ds-5c1e…",
              "table": "orders", "location": "gs://my-lake/midstream/mat-a1b2c3d4e5f6/orders",
              "source_topic": "orders", "partition_granularity": "day",
              "table_uuid": "6b1c…", "skip_tombstones": false, "protection_key_id": "3f9a1c2e" },
  "issued_at": "2026-10-23T12:00:00Z",
  "settings": { "live_tail_max_rows_per_table": 250000, "live_tail_max_bytes_per_table": 268435456,
                "live_tail_retention_seconds": 120, "live_tail_fetch_wait_ms": 200,
                "live_tail_max_records_per_second_per_table": 5000, "live_tail_peek_max_rows": 1000,
                "live_tail_idle_seconds": 600, "protocol_logging_enabled": true },
  "hints": { "offsets": { "0": 1041, "1": 998 }, "schema_id": 3,
             "metadata_location": "gs://…/metadata/00042-….metadata.json", "protection_revision": 2 }
}
```

The settings are the control plane's live-tail settings as resolved that minute, overrides included
(17 §17.3), so a change reaches the app within a minute without a restart. The hints are the table
record's (§6.6): its committed offsets can lag the table by one event, never lead it; a
`protection_revision` newer than the tail holds is a recorded policy it missed, so it fetches the
schema again. `protection_key_id` is the deployment's protection key's id, absent without one: a tail
with another key, or none, refuses a table that hashes rather than serve rows the writers would not
write (22 §22.5). A cancel
carries `kind`, `target`, `issued_at` and `reason`. A malformed message is logged and dropped; one
naming a table the receiving member does not know changes nothing.
