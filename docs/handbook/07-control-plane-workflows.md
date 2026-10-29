# 07 — Control-Plane Workflows

Everything the control plane does is a **bounded workflow step**: consume one work event,
perform a finite amount of work, persist state, emit at most a small number of follow-ups,
return. There are no long-running orchestrators and no in-memory workflow state.

Source: `python/src/midstream/services/workflow.py`,
`services/request_intake.py`, `services/observation.py`, `handlers/workflow_apply.py`,
`handlers/resource_observation.py`, `services/settings.py`.

Each handler resolves the **effective** configuration once per event, the environment with the
Settings page's overrides applied (`resolve_config(settings_table)`), and passes it to the step.
A settings change therefore applies from the next event on. See [17 — Settings](./17-settings.md).

## 7.1 The step dispatcher

```python
# services/workflow.py::process_work_event  (structure, code-derived)
if   step == "start_discovery":       _process_start_discovery(...)
elif step == "discover_tables":       _process_discover_tables(...)        # topics an update added (§7.9a)
elif step == "await_runtime_drained": _process_await_runtime_drained(...)  # requires kafka_admin_client (§7.9a)
elif step == "create_table":          _process_create_table(...)           # GCS: metadata v0 + pointer
elif step == "apply_runtime":         _process_apply_runtime(...)          # requires kaspr_app_client
elif step == "delete_runtime":        _process_delete_runtime(...)         # requires kaspr_app_client
elif step == "cleanup_runtime_kafka": _process_cleanup_runtime_kafka(...)  # requires kafka_admin_client
elif step == "drop_table":            _process_drop_table(...)             # GCS: the table's objects
elif step == "purge_materialization_records":
                                      _process_purge_materialization_records(...)
else:                                 follow_up = None
```

A step returns nothing, one work event, or several (`FollowUp`): an apply that ends a change of topics
queues the runtime's observation and the drop of the tables being dropped (§7.9a). The handler
routes each (`follow_ups()`): a follow-up with an `observation_policy` through the scheduler,
replacing the pending one of its materialization and step (`scheduled_request_id`), any other
straight onto `materialization-work`.

`observe_runtime` and `sweep_observations` are **not** handled here — they are handled by
`handlers/resource_observation.py` on the `resource-observation` agent. `repair_state` is a
declared step with no handler anywhere.

Every call is wrapped in protocol logging: `control_plane_work_completed` on success (with
`duration_ms`, `follow_up_step`, `follow_up_work_id`) or `control_plane_work_failed` on error
(with `error_type`, `error`).

### Which dispatcher the agent runs

The `workflow-apply` agent's map is a coroutine. It awaits
`handlers/workflow_apply.handle_work_event_for_routed_output_async`, which calls
`process_work_event_async`: the dispatcher above, except that no step waits on the network on the
event loop.

- `start_discovery`, `discover_tables`, `apply_runtime`, `await_runtime_drained`, `delete_runtime`
  and `cleanup_runtime_kafka` await their adapters' `*_async` calls (`discover_async`, `apply_async`,
  `consumer_group_state_async`, `delete_async`, `list_topics_async`, …).
  Each adapter runs its asyncio client on a loop of its own, on its own thread
  (`utils/background_loop.py`), so awaiting leaves the agent's loop free.
- `create_table` and `drop_table` run their GCS calls on the one-thread table-lifecycle pool
  (`_TABLE_EXECUTOR`) or a worker thread (§7.3, §7.10).
- `purge_materialization_records` touches only the table and never gives up the loop.

That event loop also serves the HTTP API and runs every other agent and the Kafka consumer's
heartbeats. Whatever a step does synchronously holds all of them until it returns, which is why
each step is bounded and why nothing that waits on Kubernetes, Kafka or a bucket waits there.

**Other work runs while a step waits.** Each converted step is split in three
(`services/workflow.py`):

1. A *plan*, on the loop: read the records, check duplicates and preconditions, decide what to do.
2. The adapter calls: blocking in `process_work_event`, awaited in `process_work_event_async`. This
   is the only part the two dispatchers do not share.
3. A *record*, on the loop: re-read the materialization and its status (`_state_after_wait`) and
   write only the fields the step owns onto them. Runtime status heartbeats, resource observations,
   consumer-lag refreshes and user requests written during the wait survive. If a purge removed the
   records meanwhile, nothing is written (`control_plane_<step>_skipped`,
   `reason=materialization_records_purged`).

The synchronous `process_work_event` and `handle_work_event*` functions run the same plan and record
code and remain for tests and tools; they block their caller for as long as the adapters take, up
to each call's deadline.

**Every adapter wait has a deadline**, a constructor parameter with a module default. When it
passes, the caller stops waiting, the call is cancelled on its adapter's loop, and
`AdapterTimeoutError` is raised (a `TimeoutError`, never a `ValueError`):

| Adapter | Deadline | Default |
|---|---|---|
| `KubernetesKasprAppClient` (apply, delete, observe) | `call_timeout_seconds`, per call | 90 s (`kubernetes/kaspr_apps.py::CALL_TIMEOUT_SECONDS`) |
| `AIOKafkaAdminClient` (list/delete topics and groups) | `call_timeout_seconds`, per call, connecting included | 120 s (`kafka/admin.py::ADMIN_CALL_TIMEOUT_SECONDS`) |
| `KafkaTopicDiscoveryService.discover` | `connect_timeout_seconds` to connect, `poll_timeout_ms` to poll, 5 s to stop, under `deadline_seconds` | 60 s + 5 s + 5 s, and 75 s overall (`schema/discovery.py`) |

The deadlines bound hangs; they do not police slowness. kubernetes_asyncio sets no timeout of its
own, and aiokafka bounds single requests (40 s) but not a call made of several, so these are the
only bounds those calls have — and each sits above the server's own limit, so that a slow but
healthy server answers, or fails with its own error, first:

- Kubernetes: the API server gives up on a request after 60 s (`--request-timeout`); 90 s.
- Kafka admin: a call can make three requests in a row at 40 s each — the bootstrap that starts the
  client, finding a group's coordinator, the request itself; 120 s.
- Discovery: aiokafka retries a bootstrap or a metadata refresh past its 40 s per request; 60 s to
  connect, and the overall deadline follows.

Since the agents await these calls, a longer deadline costs the agent time, never the loop; a
shorter one would fail healthy calls, and a failed step stays `failed` until someone resumes it.
A Kafka admin call that times out also drops the client it waited on; the next call connects afresh.

### Error policy — this is subtle and important

```python
# services/workflow.py::process_work_event, except block (code-derived)
if isinstance(exc, ValueError):
    raise                                       # ← contract violation: propagate, poison the message
if not _persist_workflow_failure(table, work_event, exc):
    raise                                       # ← could not record failure: propagate
return None                                     # ← failure recorded, swallow
```

| Exception class | Behavior |
|-----------------|----------|
| `ValueError` (incl. `MissingWorkflowStateError`) | Re-raised. Caller decides. |
| Anything else, state exists | Status → `failed`, operation record → `failed`, exception swallowed, `None` returned |
| Anything else, state missing | Re-raised |

A deadline that passes (`AdapterTimeoutError`) is "anything else": the step is recorded as
`<step>_failed`, and `last_error` names the call and the deadline, e.g.
`Kubernetes apply of KasprApp midstream/midstream-mat-1 timed out after 90s`. No step retries a timeout:
`cleanup_runtime_kafka` retries only a consumer group that still has members, and a timeout is not
that (§7.5). A table drop retries every failed pass, timeouts included (§7.10).

`_persist_workflow_failure` writes:

- `observed_phase = "failed"`
- `phase_reason = "<step>_failed"` (e.g. `apply_runtime_failed`)
- `phase_message = "Workflow step <step> failed: <error>"`
- `runtime_health = RuntimeHealth(level="degraded", reason="<step>_failed", message=<error>)`
- `last_error = <error>`
- operation record with `status: "failed"` and `error`

Three failures carry a reason of their own instead of `<step>_failed`: `start_discovery` finding a
source topic the cluster's metadata does not list (`SourceTopicMissingError`,
`source_topic_missing`, §7.2), `create_table` finding another materialization's table at a table's
location (`TableLocationInUseError`, `table_location_in_use`), and `apply_runtime` for a
materialization with no table record at all, one of whose tables has no discovered schema either
(`TableMissingError`, `table_missing`): delete it and create it again. A table with a discovered
schema but no record — a create that did not finish — is created by the apply instead, and a table
with neither beside tables that exist — a change of topics whose discovery failed — is discovered
first (§7.3).

**There is no automatic retry of a `failed` status.** The sweep will keep observing
(desired/observed mismatch staleness), but nothing re-drives the failed step. Recovery is a
new API request (`POST .../resume`) or a manual work event.

## 7.2 Step: `start_discovery`

Trigger: `create` request intake.
Preconditions: materialization exists, status exists.

### Early drop

```python
# _should_drop_start_discovery (code-derived)
materialization.desired_phase in {"deleting", "deleted"}
or status.observed_phase in {"deleting", "deleted"}
```

If true, writes an operation record with `status: "skipped"`,
`reason: "materialization_no_longer_requires_discovery"` and returns `None` — no follow-up.
This handles "user deleted while discovery was queued".

### Validation

```python
# _validate_start_discovery (code-derived)
materialization.desired_phase == "running"   else ValueError
status.observed_phase       == "requested"   else ValueError
```

### Execution

1. Status → `observed_phase=discovering`, `phase_reason=discovering`,
   `phase_message="Sampling the source topic"` (`topics` for several).
2. `discovery_service.discover(materialization)` → `DiscoveryResult(records_by_topic, discovered_at,
   samples)`; the agent awaits `discover_async`. The implementation (`schema/discovery.py`) reads,
   with one consumer for every topic at once, up to `discovery_sample_records` (200) records of each
   topic spread across its partitions, for at most `discovery_sample_seconds` (5) in all (09 §9.6).
3. Each table's schema 0 is evolved from its topic's samples by the same policy the runtime's
   committer applies later (`lakehouse/schema/policy.py::evolve`), so a topic with several historical
   shapes starts with its forks in place. One protocol event `control_plane_schema_discovery_completed`
   per table, with `table`, `source_topic`, `sampled_records`, `schema_fields`, `forks`,
   `duration_ms`.
4. Re-read the materialization and its status. If the materialization is now `deleting` or
   `deleted` — a delete arrived while discovery waited — record the operation `skipped` as the early
   drop does, log `control_plane_start_discovery_skipped`, and stop: the queued `delete_runtime`
   takes over. Any other change, such as a stop, is kept.
   A source topic the cluster's metadata does not list — it does not exist, or the Kafka user may
   not describe it (`DiscoverySample.missing`) — fails the step: `SourceTopicMissingError`, `failed` /
   `source_topic_missing`, naming the topic. Its runtime would be assigned none of the topic's
   partitions, so the table would stay empty while the materialization read `running`. The API
   refuses such a topic before the create is accepted (04 §4.5); this catches one gone since.
5. Persist each table's projection of schema 0 (`MaterializationSchema`, 05 §5.5).
6. `replace(materialization, updated_at=discovered_at)`, on the re-read record.
7. Status → `observed_phase=discovered`, `phase_reason=schema_discovered`,
   `phase_message="Schema 0 discovered from N sampled record(s)"` — for several topics `Schema 0
   discovered for K tables from N sampled record(s)`, naming the topics that hold no records yet —
   with `desired_phase` from the re-read materialization.

### Follow-up

```python
MaterializationWorkEvent(
    work_id=f"work-create-table-{materialization_id}",
    step="create_table",
    requested_at=discovery_result.discovered_at,   # ← discovery time, not now
    caused_by=work_event.caused_by,                # ← original request cause is preserved
    dedupe_key=f"create_table:{materialization_id}:{work_event.work_id}",
    target_runtime=work_event.target_runtime,
)
```

No `observation_policy`, so it is published directly (`build_direct_work_output`), not scheduled.

### Failure modes

| Symptom | Cause |
|---------|-------|
| `failed` / `source_topic_missing`, "Kafka has no topic … or the deployment's Kafka user may not describe it" | The topic was deleted, or the Kafka user lost Describe on it, between the request and discovery |
| `failed` / `start_discovery_failed`, `last_error` "connecting to Kafka to sample topic … timed out after 60s" | The consumer could not connect or authenticate within `connect_timeout_seconds` |
| `failed` / `start_discovery_failed`, "schema discovery from topic … timed out after 75s" | The whole read overran its deadline, e.g. a consumer that blocked its loop |
| `observed_phase=failed`, `phase_reason=start_discovery_failed` | Discovery raised another non-`ValueError` |
| Message poisoned in a retry loop | Discovery raised `ValueError` and no state existed |

An empty source topic is not a failure: discovery returns no records, and schema 0 holds the
metadata columns only; the runtime adds every field as data arrives (09 §9.7). Only that poll's timeout means "empty"; a connection that never
completes raises, where it once could pass for an empty topic, and so does a topic missing from the
metadata, which an empty topic is not: it has partitions. Stopping the consumer is bounded
cleanup (5 s) and never replaces what was read or the error being raised.

## 7.2a Steps: `tables_claimed` and `claim_rejected`

A create, and an update that adds tables, wait for the directory to claim the new tables' names
(18 §18.16) before anything is created. The API previews the claim and refuses a conflict it can see;
request intake saves the materialization — a create in `requested` / `claiming_tables`, an update's
change of topics as its `pending_topic_change` — and sends a `claim_tables` request on the identity
topic, keyed `identity` (`services/table_claims.py::claim_request`, with the `claim_request_id` the
API generated). `identity-intake` applies it in order with every other directory change and answers on
the work topic, keyed by the materialization (`claim_outcome_work`, `work_id`
`work-claim-<claim_request_id>`), so the answer is applied in the materialization's own shard of the
`state` table:

| Step | For a create | For an update |
|---|---|---|
| `tables_claimed` | `start_discovery` (`work-<request_id>`), if the materialization is still `requested` / `claiming_tables`; a redelivery after that does nothing | apply the pending change exactly as planned (`apply_topic_change`), clear it, and go on as an update does: `discover_tables` for the new tables (§7.9a) |
| `claim_rejected` | mark the materialization `deleted` — desired and observed — with `phase_reason` the refusal's reason (`table_name_taken`, `dataset_not_found`, `forbidden`, `materialization_deleted`, `invalid`) and `phase_message` `Not created: <message>`. Nothing was created, so there is nothing to drop; the retention sweep purges it | drop the pending change, leaving the materialization as it was, with `The change of topics was not applied: <message>` in its status |

A redelivered claim returns the directory's earlier outcome, so it answers the same way again; the
workflow's own checks make a second answer change nothing. The two steps run on `workflow-apply`
(`WORKFLOW_APPLY_STEPS`) like the others.

**Ordering.** Request intake sends the claim before anything else it emits for the materialization,
and a delete's release follows the claim on the same Kafka key, so the directory always sees them in
that order. A claim that arrives after a release of every name — a delete accepted while the create's
claim was in flight — is refused `materialization_deleted`, so it cannot take a name nothing would
release.

## 7.3 Steps: `create_table` and `apply_runtime`

### `create_table`

Trigger: discovery follow-up, or an `apply_runtime` that finds a table missing. Code:
`_plan_create_table`, `_create_table`, `_record_create_table`; the agent runs the GCS part on
`_TABLE_EXECUTOR`, a pool of one thread of its own. **One table a pass**: the step creates the first
table, in the topics' order, that has no record, and queues itself again — same `work_id` and
`dedupe_key` — until every table exists, so the step stays bounded however many topics there are.

1. **Plan.** Drop the step (log `skipped`, `materialization_deleting`) if the materialization is
   being deleted. Resolve each table's identity: namespace the table's dataset, name the table's,
   location `<destination_uri>/<storage_prefix>/<materialization_id>/<table>` (11 §11.1). An unusable destination raises `InvalidTableLocationError` (`invalid_destination`)
   before any I/O. A table whose record has that location was created by an earlier pass, or the pass
   was redelivered: skip it. With every table there, log `table_already_created` and go on to the
   apply. The table's schema projection must exist
   (`ValueError("create_table requires the discovered schema of table <t>")`).
2. **Create** (`lakehouse/lifecycle.py::create_materializer_table`): metadata version 0 — schema 0
   with explicit field ids, the partition spec, the sort order, the table properties
   (`materializer.*`, the fork map, snapshot retention, the write and commit properties) — then the
   pointer, with `if_generation_match=0` (11 §11.7). A pointer of the same materialization means
   done; another materialization's pointer at the location fails the step `table_location_in_use`.
3. **Record**, on the re-read records: the table record (`materialization-table:<id>:<table>`,
   05 §5.12) and the projection with its metadata location; status `phase_reason=table_created`,
   `phase_message="Iceberg table sales.orders created (1 of 3)"`; protocol event
   `control_plane_create_table_completed` with `table`, `source_topic`, `location`,
   `metadata_location`, `position` and `tables`.

Follow-up: the next `create_table` pass while a table is left; after the last, `apply_runtime`,
`work_id=work-apply-<id>` (or `work-apply-<id>-<resumed work>` for a create that a resume finished),
published directly.

### `apply_runtime`

Triggers: the last table's creation; a `stop`/`resume`/`update` request; sweep drift
reconciliation. Preconditions: materialization, status and **every table record** exist — the
runtime writes all of them, so it is never rendered with some.

`kaspr_app_client is None` → `ValueError("kaspr_app_client is required for apply_runtime")`.
A table without a record: when its schema was discovered (a create that did not finish), the step
answers a `create_table` work event instead, which creates what is missing and queues the apply
again (log `control_plane_apply_runtime_creating_tables`) — so `POST …/resume` finishes an
interrupted create. A table with no discovered schema either, beside tables that exist, is what a
change of topics leaves when its `discover_tables` failed: the step answers `discover_tables`
(`work-discover-tables-<id>-<work id>`, log `control_plane_apply_runtime_discovering_tables`),
which samples it, creates it and applies — so `POST …/resume` finishes that change too. With no
table record at all — a create whose discovery failed, or a record from before 1.0 — →
`TableMissingError`, `failed` / `table_missing`: delete the materialization and create it again
(12 §12.13).

No schema is part of the runtime's spec: the runtime reads each from its table. The template input
carries the source topics, every table's name, topic and location, the namespace and the partition
granularity, so a schema change never re-renders a runtime.

### Render before deciding

```python
runtime_name       = resolve_runtime_app_name(materialization, config=config)
template_input     = build_dedicated_runtime_template_input(materialization, table_records=..., config=...)
rendered_resources = render_dedicated_runtime_resources(template_input)
rendered_hash      = spec_hash(rendered_resources)
existing_binding   = get_runtime_binding(table, materialization_id)
```

Rendering happens **before** the duplicate check because the hash is the duplicate signal.

### Duplicate / no-op guard

```python
# _duplicate_apply_skip_reason (code-derived, in order)
op = get_operation_record(table, materialization_id, operation_id=work_event.work_id)
if op and op["payload"]["status"] in {"applied", "skipped"}:            return "duplicate_apply_work"
if existing_binding is None:                                            return None
if existing_binding.last_apply_operation_id == work_event.work_id:      return "duplicate_apply_work"
if (caused_by.kind == "scheduler"
    and caused_by.id == "reconcile-runtime-drift"
    and existing_binding.last_applied_spec_hash == rendered_hash):      return "duplicate_reconcile_noop"
return None
```

On skip: log `control_plane_apply_runtime_skipped` with the reason, return `None`.

> Note the asymmetry: the spec-hash equality check applies **only** to
> `reconcile-runtime-drift` work. A user-triggered `stop`/`resume`/`update` always re-applies
> even if the rendered spec is byte-identical. That is deliberate — a user action should
> always touch the cluster.

### Apply with rollback

```python
applied_resources = []
try:
    for resource in iter_rendered_resources(rendered_resources):   # app → tables → agents → tasks
        kaspr_app_client.apply(resource)
        applied_resources.append(resource)
except Exception:
    _cleanup_partially_applied_resources(client, namespace=..., applied_resources=applied_resources)
    raise
```

`_cleanup_partially_applied_resources` deletes in **reverse order** and swallows every delete
exception (`except Exception: continue`). Best-effort rollback — orphans are possible, and the
sweep's drift reconciliation is what eventually fixes them. The agent awaits each apply and each
rollback delete (`_process_apply_runtime_async`).

**Only a first apply is rolled back** — one with no runtime binding before it (`_RuntimeApply.first_apply`).
A failed re-apply (stop, resume, update, drift reconciliation) leaves what is deployed in place: its
resources are the runtime that is serving, server-side apply is idempotent, and the next apply or the
sweep's reconciliation converges. Rolling it back would delete the live KasprApp and take the
materialization down until someone resumed it. Either way the step is recorded `apply_runtime_failed`.

Each apply and each rollback delete has its own deadline (90 s). An apply that times out is handled
like any failed apply; the resource whose apply timed out is not in a first apply's rollback list,
although the API server may have applied it.

### Persist

1. `RuntimeBinding` via `build_runtime_binding(... managed_agent_names=template_input["managed_agent_names"], template_version=cfg.default_runtime_template_version, spec_hash=rendered_hash, operation_id=work_event.work_id ...)`
   — what was rendered and applied, from the records as they were when the step started.
2. Operation record: `{operation_type: "apply_runtime", status: "applied", spec_hash, runtime_app_name}`.
3. Status — re-read after the applies, keeping whatever other agents wrote meanwhile (heartbeats,
   observations, consumer lag, a user's stop); `desired_phase` comes from the re-read
   materialization. Two branches, decided on that re-read status:

```python
# _should_preserve_running_status_during_reapply (code-derived)
caused_by.kind == "scheduler"
and caused_by.id == "reconcile-runtime-drift"
and status.observed_phase == "running"
and status.runtime_health.level == "healthy"
and not heartbeat_is_stale(status.last_heartbeat, now=requested_at)
```

- **True** → keep `observed_phase=running`, only refresh `desired_phase` and the
  `resource_observation` namespace/app name. A healthy running materialization does not flap to
  `deploying` because of a background drift fix.
- **False** → `observed_phase=deploying`, `phase_reason=runtime_apply_in_progress`,
  `phase_message="Runtime spec applied; awaiting observation."`, `last_transition_time` bumped.

### Follow-up: up to two outputs

`handlers/workflow_apply.handle_work_event_for_routed_output` yields, in order:

1. **Runtime settings delivery** — once runtime settings were ever changed or have a lineage,
   which they have from the control plane's first start:
   `services/settings.py::runtime_settings_output_for_apply` sends the current runtime settings
   to `materialization-schema-authority-updates`, targeted at this materialization, so a runtime that
   was just created, resumed or re-applied runs the current values, and none an earlier
   installation left on the topic (06 §6.7, 17 §17.7).
2. **Observation follow-up** — `_build_observe_runtime_follow_up(scheduled=True)` produces:

```python
MaterializationWorkEvent(
    work_id=f"work-observe-{materialization_id}",
    step="observe_runtime",
    dedupe_key=f"observe:{materialization_id}:{work_event.work_id}:1",
    target_runtime=TargetRuntime(namespace, runtime_name),
    observation_policy=build_one_shot_observation_policy(attempt=1, requested_at=...),
      # → attempt=1, max_attempts=4, mode="one_shot", scheduled_for=requested_at + 15s
)
```

Because `observation_policy is not None`, the handler routes it through
`build_scheduled_work_output(request_id=f"observe:{materialization_id}", deliver_to=cfg.materialization_work_topic, deliver_at=policy.scheduled_for)`.

## 7.4 Step: `delete_runtime`

Trigger: `DELETE /materializations/{id}` → `delete` request.
`kaspr_app_client is None` → `ValueError`.

### Deletion order (fixed, dependency-aware)

```python
# _process_delete_runtime (code-derived)
[
  ("KasprTask",  component_names["pipeline_task_name"]),
  ("KasprTask",  component_names["commit_timer_task_name"]),
  *retired_runtime_components(runtime_name),   # `<rt>-maintenance-timer`, which runtimes before 2.0 had
  ("KasprTask",  component_names["status_task_name"]),
  ("KasprAgent", component_names["settings_agent_name"]),
  ("KasprAgent", component_names["writer_agent_name"]),
  ("KasprAgent", component_names["committer_agent_name"]),   # last, so a final commit can land
  ("KasprTable", component_names["state_table_name"]),
  ("KasprApp",   runtime_name),
]
```

Workloads first, then the table, then the app. There is **no try/except** around the loop, so
the first failing delete aborts the rest and the exception propagates into
`process_work_event`'s handler (non-`ValueError` → status `failed`). The agent awaits each delete,
each under its 90 s deadline; one that times out fails the step as `delete_runtime_failed`.

`KasprAppClient.delete` is idempotent-tolerant for 404s (see `kubernetes/`), so re-running the
step is safe.

### Persist

- Operation record `{operation_type: "delete_runtime", status: "applied", runtime_app_name}`.
- Status, re-read after the deletes → `desired_phase=deleting`, `observed_phase=deleting`,
  `phase_reason=runtime_apply_in_progress`,
  `phase_message="Runtime delete applied; awaiting resource absence."`; everything else as other
  agents left it.

> `phase_reason=runtime_apply_in_progress` during a *delete* is intentional reuse; the
> distinguishing signal is `observed_phase=deleting`.

### Follow-up

`_build_observe_runtime_follow_up(..., scheduled=False)` — **no observation policy**, so it is
published immediately via `build_direct_work_output`. Deletes are confirmed without a 15s delay.

## 7.5 Step: `cleanup_runtime_kafka`

Trigger: `resource-observation` emits it after observing `observed_phase == "deleted"`, via
`build_deleted_runtime_cleanup_output`.
`kafka_admin_client is None` → `ValueError`.

### Duplicate guard

```python
# _duplicate_cleanup_skip_reason (code-derived)
op status in {"applied","skipped"}                 → "duplicate_cleanup_work"
materialization.desired_phase == "deleted"         → "materialization_already_deleted"
```

### Readiness gate

```python
# _cleanup_runtime_kafka_ready (code-derived)
status.observed_phase == "deleted"
or (status.desired_phase == "deleting"
    and status.observed_phase == "deleting"
    and status.phase_reason == "waiting_for_consumer_group_empty")
```

Otherwise raises
`ValueError("cleanup_runtime_kafka requires deleted runtime observation before Kafka cleanup, got observed_phase=… phase_reason=…")`.
The second clause exists purely so retries can re-enter.

### Execution

```python
runtime_name  = work_event.target_runtime.kaspr_app_name or resolve_runtime_app_name(materialization)
group_name    = runtime_consumer_group_name(runtime_name)          # f"{runtime_name}-app"
matched       = sorted(t for t in admin.list_topics() if t.startswith(runtime_name))
has_group     = group_name in admin.list_consumer_groups()
if matched:   admin.delete_topics(matched)
if has_group: admin.delete_consumer_group(group_name)
```

The agent awaits each of the four calls, each under its 120 s deadline (connecting included).
A call that times out fails the step as `cleanup_runtime_kafka_failed`; it is not taken for a busy
consumer group, and the client it waited on is dropped so that a later attempt connects afresh.

> **Prefix deletion is a blunt instrument.** Any topic in the cluster starting with the runtime
> app name is deleted — the runtime's `-commits` topic, its state table's changelog, and anything
> else so named. Since 4.0 every runtime name is derived, `<prefix>-<materialization_id>`, so the
> prefix ends in the materialization's random id. Before it a create request could choose
> `runtime_name`, and a chosen name that prefixed unrelated topics would take them out; a
> materialization created so keeps its name in `resolved_runtime_name` (14 §14.2).

### `ConsumerGroupNotEmptyError` → retry loop

Raised by `kafka/admin.py` when the broker returns error code **68** (`NON_EMPTY_GROUP`).
This is normal: the KasprApp pods take time to fully leave the group after deletion.

```python
retry = _build_cleanup_runtime_kafka_retry(work_event)   # attempt+1, cap 4
if retry is None:
    raise RuntimeError("consumer group <g> remained non-empty after Kafka cleanup retry budget was exhausted")
_persist_cleanup_runtime_kafka_waiting(...)
return retry
```

`_persist_cleanup_runtime_kafka_waiting` writes:

- Status: `desired_phase=deleting`, `observed_phase=deleting`,
  `phase_reason=waiting_for_consumer_group_empty`,
  `phase_message="Runtime resources are deleted; waiting for consumer group <g> to become empty before retrying Kafka cleanup."`,
  `runtime_health=RuntimeHealth(level="degraded", reason="waiting_for_consumer_group_empty", message=<broker error>)`,
  `last_error=<broker error>`.
- Operation record `{operation_type: "cleanup_runtime_kafka", status: "waiting", deleted_topics, deleted_consumer_group: false, retry_attempt, retry_scheduled_for, error}`.

The retry event reuses `work_id` (so the operation record keeps updating in place), gets
`caused_by=EventCause(kind="scheduler", id=f"retry-cleanup-runtime-kafka:{materialization_id}")`,
`dedupe_key=f"cleanup_runtime_kafka:{id}:{work_id}:retry:{n}"`, and an observation policy whose
`scheduled_for` uses the standard backoff `(15, 45, 120, 300)`.

Retry budget = `MAX_CLEANUP_KAFKA_RETRY_ATTEMPTS = len(DEFAULT_OBSERVATION_BACKOFF_SECONDS) = 4`.
Total wait ≈ 15 + 45 + 120 + 300 = **480 seconds** before the terminal `RuntimeError`.

Note the retry event's `requested_at` is set to `retry_policy.scheduled_for` (the future time),
not now — the follow-up carries its own intended execution time.

### Success

The materialization and its status are re-read once Kafka has answered, so an observation of the
absent runtime made meanwhile keeps its `resource_observation` and `last_resource_observation_time`;
the waiting state below is written the same way.

1. `delete_runtime_binding(table, materialization_id)`.
2. `replace(materialization, desired_phase="deleted", updated_at=requested_at)`.
3. Status: `desired_phase=deleted`, `observed_phase=deleted`,
   `phase_reason=runtime_and_kafka_resources_deleted`,
   `phase_message="Runtime app, runtime-owned Kafka topics, and consumer group are deleted."`,
   `runtime_health=RuntimeHealth(level="unknown")`, `last_error=None`.
4. Operation record `{status: "applied", deleted_topics: [...], deleted_consumer_group: bool}`.
5. Returns the `drop_table` follow-up (§7.10): with `destination_cleanup_enabled` it deletes the
   table's objects; with it off it only retires the pointer, so the table leaves the catalog and
   stays readable by path.

> The record reads `deleted` from here on, and leaves `GET /materializations` when the chain's final
> `purge_materialization_records` step runs, or when the retention sweep collects it.

## 7.6 Missing-state retry (`handlers/workflow_apply.py`)

`_require_materialization` / `_require_status` raise `MissingWorkflowStateError`, a subclass of
`ValueError`. This exists because Kaspr table writes are asynchronous: a work event can arrive
at a partition whose changelog has not yet caught up.

```python
except MissingWorkflowStateError:
    retry_output = _build_missing_state_retry_output(work_event, config=cfg)
    if retry_output is not None:
        yield retry_output
    return                       # ← never re-raise; do not poison the topic
```

Retry construction:

- `attempt = (work_event.observation_policy.attempt + 1) if policy and policy.attempt else 1`
- if `attempt > MAX_MISSING_STATE_RETRIES` (= 4) → return `None`, give up silently
- `requested_at = _utc_now_timestamp()` (**now**, not the original request time)
- reuses `work_id` and `target_runtime`
- `caused_by = EventCause(kind="scheduler", id=f"retry-missing-state:{materialization_id}")`
- `dedupe_key = f"{step}:{id}:{work_id}:missing-state:{attempt}"`
- scheduler `request_id = f"missing-state:{id}:{step}"`

### The stale-retry suppressor

```python
except ValueError as exc:
    if _is_stale_missing_state_retry(work_event, exc):
        return              # ← swallow
    raise
```

```python
# _is_stale_missing_state_retry (code-derived)
work_event.step == "start_discovery"
and work_event.caused_by.kind == "scheduler"
and work_event.caused_by.id.startswith("retry-missing-state:")
and str(error).startswith("start_discovery requires observed_phase requested, got ")
```

Scenario: state was missing, a retry was scheduled, then the state landed *and* discovery
already ran via a different path. The retry arrives, sees `observed_phase != "requested"`, and
would normally poison the topic. This narrowly-scoped suppressor drops it.

## 7.7 Sweep-driven reconciliation

Runs every 2 minutes (`sweep-observations` cron, `onLeader: true`) and once at startup
(`sweep-observations-startup`). Both produce a synthetic
`WorkEvent(materialization_id="__system__", step="sweep_observations")` which the
`resource-observation` agent turns into `build_sweep_observation_outputs`.

```python
# services/observation.py::build_sweep_observation_outputs (structure, code-derived)
for key, _ in table.prefix_scan(materialization_prefix()):
    materialization_id = key.removeprefix(materialization_prefix())
    materialization = get_materialization(...)         ; if None: continue
    status  = get_materialization_status(...)
    binding = get_runtime_binding(...)
    if status is None or binding is None: continue     # ← never deployed, or already cleaned up

    reapply = _build_runtime_reapply_output_if_needed(...)
    if reapply is not None:
        reapply_outputs.append(reapply); continue      # ← drift wins; skip observation this cycle

    if not _should_sweep(status, now=observed_at): continue
    observation_outputs.append(build_direct_work_output(observe_runtime event))

return [*reapply_outputs, *observation_outputs]         # ← reapplies are emitted first
```

The sweep runs on the `__system__` event and writes nothing: each output is keyed by its
materialization, so the observation or reapply is handled in that materialization's own shard of
the `state` table (06 §6.4). The retention sweep and the consumer-lag refresh follow the same rule
(§7.10, 02 §2.5 `maintenance`). `tests/test_maintenance.py` (`ShardRoutingTests`) holds all three to
it.

`binding is None` means a materialization that was never applied — including one that failed at
discovery or at `create_table` — is **invisible to the sweep**; `POST …/resume` is its recovery
(14 §14.2).

### Drift detection: `_build_runtime_reapply_output_if_needed`

```python
if materialization.desired_phase in {"deleting","deleted"}: return None
if binding.draining:                                        return None   # held at zero on purpose (§7.9a)
table_records = list_materialization_tables(...)
if not set(materialization.table_names) <= {record.name for record in table_records}:
                                                            return None   # a table is still being created
desired_hash = spec_hash(render_dedicated_runtime_resources(
                   build_dedicated_runtime_template_input(
                       scaled_materialization(materialization, autoscale),   # at the count autoscaling gave it (§7.12a)
                       table_records=table_records)))
if desired_hash == binding.last_applied_spec_hash:          return None
→ emit apply_runtime
```

The rendered spec holds no schema and no setting (10 §10.1), so what makes the hashes differ is the
deployment's runtime spec, the replica count, or the package pin — a control-plane upgrade re-applies
every runtime this way. The replica count is the one the apply renders: the autoscaled count while
autoscaling has the runtime scaled up (§7.12a), so a scale-up is not drift. **Any manual `kubectl edit` of a runtime resource is reverted** within ~2
minutes, because the desired hash no longer matches `last_applied_spec_hash`… *unless* the edit does
not change what the control plane renders. In practice, hand-editing runtime resources is futile.

The emitted event:

```python
work_id    = f"work-apply-reconcile-{materialization_id}-{desired_hash[:12]}"
caused_by  = EventCause(kind="scheduler", id="reconcile-runtime-drift")
dedupe_key = f"apply_runtime:{materialization_id}:reconcile:{desired_hash}"
```

The `caused_by.id` is exactly the sentinel checked by both `_duplicate_apply_skip_reason` and
`_should_preserve_running_status_during_reapply`. Changing that string breaks two behaviors at
once.

### Staleness triggers: `_should_sweep`

```python
observed_phase in {"deploying","deleting"}                                  → True   # always chase transitions
resource_observation_is_stale(last_resource_observation_time, now)          → True   # > 600s
desired_observed_mismatch_is_stale(desired, observed, last_transition, now) → True   # > 600s
heartbeat_is_stale(last_heartbeat, now) and observed in {running,degraded,paused} → True  # > 120s
```

Sweep-emitted observations carry `ObservationPolicy(attempt=1, max_attempts=1, mode="sweep")`.
`max_attempts=1` means `build_observation_follow_up_output` will never chain a follow-up from a
sweep observation (`policy.attempt >= policy.max_attempts`), so sweeps do not amplify into
retry storms. The next sweep is the retry.

### Observing off the event loop

The `resource-observation` agent's map is a coroutine, like `workflow-apply`'s, returning
`handle_observation_work_for_output_async`'s generator. It awaits the KasprApp read
(`observe_async`) before touching the table, then updates the status on the loop from the record as
it is then, so a heartbeat written during the read is kept. A sweep reads only the table and never
gives up the loop.

A read that passes its 90 s deadline is **skipped**, not failed. Nothing is written and nothing is
scheduled; the handler logs a warning and `control_plane_runtime_observation_timed_out`, and the
sweep observes every runtime in transition or gone stale again within two minutes. Letting the error
escape would crash the agent: Kaspr then restarts it and re-runs its init block, building another
Kubernetes client with another thread. Other read errors still escape, as before.

## 7.8 End-to-end: create

| # | Where | What happens | State after |
|---|-------|--------------|-------------|
| 1 | WebView `create` | validate, preview the claim against the directory, build `MaterializationRequestEvent`, `topicSend` | none persisted |
| 2 | `request-intake` | insert `Materialization` + `MaterializationStatus`; send `claim_tables` to the directory | `requested` / `claiming_tables` |
| 2a | `identity-intake` | claim every new table's name, or none; answer `tables_claimed` or `claim_rejected` (§7.2a) | the claims, in the `directory` table |
| 3 | `workflow-apply` `start_discovery` | sample up to 200 records of each topic, evolve each table's schema 0 | `discovering` → `discovered` / `schema_discovered` |
| 4 | `workflow-apply` `create_table` ×tables | per pass, one table's metadata v0, its pointer and its record | `discovered` / `table_created` |
| 5 | `workflow-apply` `apply_runtime` | render eight resources, apply, bind | `deploying` / `runtime_apply_in_progress` |
| 6 | scheduler +15 s | deliver `observe_runtime` attempt 1 | — |
| 7 | `resource-observation` | read the KasprApp, derive the phase | `deploying` / `waiting_for_runtime_ready` |
| 8 | runtime members | load the tables from their pointers, start reading every topic | heartbeats: `waiting_for_table`, then `processing` |
| 9 | `runtime-status` | merge (needs `ready_condition == "True"`) | `running` / `processing` |
| 10 | the committer | first commits; `table_committed` → `table-events` | each table record has a snapshot |

Typical wall-clock from POST to `running`: 60–180 s, dominated by pod scheduling and package
installation. On a test cluster the load-test materialization's discovery took 375 ms and its table's
creation about a second.

## 7.9 End-to-end: stop, resume and update

```
POST /materializations/{id}/stop
  → Materialization.desired_phase = "stopped", status.desired_phase = "stopped"
  → work step apply_runtime
  → the KasprApp is rendered with replicas: 0; Materialization.replicas is untouched
  → 8 resources re-applied; the operator deletes the HPA and scales the StatefulSet to zero
  → status observed_phase = "deploying" (reapply-preservation does NOT apply: caused_by is a request)
  → observe_runtime +15s: deploying / resource_generation_not_observed until the operator catches up,
      then deploying / waiting_for_runtime_scale_down while members are still up
  → available_members "0/0"
  → observed_phase = "stopped", phase_reason = "runtime_scaled_to_zero", runtime_health unknown
```

`resume` is the mirror image: `desired_phase="running"` renders `replicas` again, the operator
scales the StatefulSet up from zero, and the phase converges to `running` once the members
heartbeat. `update` (`PATCH`) re-applies with the new `replicas` and leaves the desired phase as it
was — a stopped materialization stays stopped and resumes at the new count. An update that changes
the topics is §7.9a.

Measured on a test cluster (`materializer-loadtest-orders`, 16 replicas): stop converged in about
a minute, resume in about four (pods 1 → 5 → 10 → 16 as the HPA doubles them).

Stopping leaves the KasprApp, its state table, its Kafka topics and consumer group, its persistent
volumes and the Iceberg tables in place. The maintenance app goes on compacting the tables and
expiring their snapshots while the runtime is stopped (§7.13), and `POST …/maintenance` works then too. It does **not** flush the runtime first: rows staged on the
pods but not yet in the table are left behind, and since their offsets were never committed
(10 §10.7) the resumed runtime reads them again; the committer refuses whatever the table already
holds.

A runtime that is shutting down keeps heartbeating until its last member exits.
`apply_runtime_status_event` records those heartbeats and their counters for a materialization whose
desired phase is `stopped`, but never lets them change the observed phase: only the resource
observation can confirm or deny the stop.

Before 0.4.0 this section described `pause`. It never paused anything: no template rendered a
paused state, so the re-apply was a no-op and the phase stayed `running` while the desired phase
read `paused`. `paused` remains a lifecycle phase only so that such records still load.

## 7.9a End-to-end: changing the topics

`PATCH /materializations/{id}` with `tables` sets the topics the runtime reads from now on, in order —
a new one with the dataset and name of its table; `removed_tables` says, for each table whose topic it
removes, `keep` (read-only) or `drop` (with its data), and can also `drop` a table kept earlier
(04 §4.8.1). The view plans the change at
once (`services/topic_changes.py::plan_topic_change`) to refuse what cannot be applied — a removed
topic's table left without a decision, a table still read named in `removed_tables`, dropping a kept
table while adding its topic back — and answers with what the change does to each table
(`topic_change`). Request intake plans it again against the records as they are then, so of two
racing updates the later is refused in the status (`The change of topics was not applied: …`) rather
than applied to a materialization it was not planned for.

What the plan does:

| Topic | Table afterwards | Record state |
|---|---|---|
| Already read | the same table, same name | `active` |
| Removed, `keep` | in `Materialization.retired_tables`: in its dataset and the catalog, readable, maintained, no longer written; its name stays claimed | `retired` |
| Removed, `drop` | gone once the runtime has stopped reading it | `dropping`, then deleted |
| A kept table's topic, added back | written again, from where it stopped | `active` |
| New | a new table in the dataset chosen, under the name chosen or derived from the topic — apart from every name the materialization holds (written tables', kept ones', those being dropped, and those dropped with their files kept), and free in its dataset | created `active` |

A table dropped with `destination_cleanup_enabled` off keeps a record in state `dropped`: its files
stay where they were (`catalog-pointer.dropped.json`), so its name — and with it its location — is
never used again by this materialization, or a new table there would take the old files for orphans.
Its name in its dataset is released all the same: it left the catalog. With cleanup on, the record
goes and the name is free in the materialization too.

**Kaspr's static members keep their assignment** (17 §17.12): a group that keeps its members keeps the
partitions it had, so a runtime applied with another set of topics would never be assigned the new
ones. The runtime is therefore *drained*: held at zero members until its consumer group has emptied,
then started again with the topics it reads now.

```
PATCH /materializations/{id}  {"tables": [...], "removed_tables": {...}}
  → request intake: plan again; with new tables, keep the plan as pending_topic_change and claim
                    their names (releasing dropped tables' in the same request) — §7.9b; else
                    apply it now and release dropped tables' names:
                    update tables/retired_tables, set each record's state,
                    spec_version + 1 (one per request, replicas included)
  → work step: discover_tables when topics were added (after tables_claimed); drop_table when only
               kept tables are dropped; apply_runtime otherwise
discover_tables   sample only the added topics, save each new table's schema 0   (status message only);
                  a topic gone since the API asked Kafka for it starts empty, with a warning logged.
                  Failed (Kafka unreachable, say): failed / discover_tables_failed, and the change is
                  applied but its tables missing — POST …/resume discovers and creates them
create_table      create each new table, a pass each; then apply_runtime
apply_runtime     the binding's topics differ from the materialization's: drain
                  → render with replicas: 0, binding.drain_started_at + topics_before_drain
                  → deploying / changing_topics; await_runtime_drained scheduled 15 s on
await_runtime_drained   every 15 s, up to 60 times (DRAIN_CHECK_SECONDS, DRAIN_MAX_ATTEMPTS)
    group not Empty/Dead → "waiting for consumer group <runtime>-app to empty (Stable)"; again
    group empty          → set where it resumes each added topic (below), clear the drain,
                           apply_runtime (work-apply-<id>-drained-<started_at>)
    60 checks in vain    → failed / consumer_group_not_empty (resume starts the drain again)
apply_runtime     not draining: render with the replicas, observe; and queue the drop of every
                  table being dropped (a live drop, below)
```

**Where the group resumes.** Once the group is empty, the step commits offsets for each topic the
runtime did not read before the change (`topics_before_drain`), as Kafka's admin tools do — an
`OffsetCommit` with generation −1 and no member, which Kafka refuses for a group with members: a kept
table resumes after its last committed offset (`offset + 1`, per partition), a new table — and any
partition its table never committed — from the partition's first offset. The group may still hold
offsets for the topic from a table dropped earlier, which a new table must not resume from. Records a
table already holds are skipped by the runtime whatever the group says (10 §10.6).

**While the drain runs** the status stays `deploying` / `changing_topics`: heartbeats of members
shutting down update counters only, a resource observation of a KasprApp at zero keeps the phase
(08 §8.3, §8.5), and the sweep re-applies nothing (§7.7). Stop, resume and further updates are accepted:
an apply during the drain renders at zero again and waits on the same drain.

**Drops.** A *live drop* (`work_id` `work-drop-tables-<id>-<stamp>`, `is_live_drop`) goes through the
tables in state `dropping` only, and leaves the materialization's other records alone:

- it starts from the non-draining apply that ends the drain — by then no member reads the topic —
  or straight from intake when the change only drops kept tables, which no runtime writes. A kept
  table the *same* drain kept a moment ago may still be written by members that have not left the
  group: dropping it then goes through `apply_runtime`, waits on that drain, and is dropped by the
  apply that ends it;
- each table is dropped in passes as on delete (§7.10); once it is gone, its schemas go, and its
  record goes too — or, with cleanup off, stays `dropped`;
- if the maintenance app runs a job on the table, the drop waits for it (§7.10);
- a drop that keeps failing does not fail the materialization: after the retries, the table keeps a
  `drop_failed` error (shown in the console and the status) and stays `dropping`, and the next apply
  of the runtime — a resume, say — drops it again;
- a materialization deleted meanwhile stops the live drop: its delete drops every table.

**A table re-created under an old name.** Removing a topic with its table dropped and adding it back
makes a new table, with a new UUID, at the same location. The runtime's state table may still hold
what the dropped table left under that name — committed offsets above all, which would make the new
table skip its records as already in it. Each runtime knows the UUID of each table it writes (from
its template) and ignores state of another incarnation (10 §10.8).

Measured on a test cluster (2 runtime members, `shop`): adding a topic — sampling 0.2 s, held at zero from
3 s, group empty at 129 s (the 120 s session timeout after the members stopped), running at 314 s;
the new table then held all 3,005 records. Re-adding a kept table's topic resumed it exactly where it
stopped (80,005 + the 1,000 records produced meanwhile), and a dropped table's files were gone 1.5 s
after the drain ended. Every change of topics costs the runtime its group's session timeout plus a
pod start, about four to five minutes.

## 7.9b A change of topics waits for its names

An update that adds tables does not apply at once: their names must be claimed in their datasets
first (§7.2a), or two materializations could create tables of one `<dataset>.<table>`.

1. **Request intake** plans the change against the materialization and its table records as they
   are, and keeps the plan as `Materialization.pending_topic_change`, with the update's
   `request_id` and `claim_request_id`. It sends one `claim_tables` request: the added tables'
   names, and a release of every table the change drops, applied together or not at all. A label
   or replica change in the same request applies at once.
2. While a change waits, another change of topics is refused: the API answers `409
   topic_change_pending`, and request intake drops one that slips through, saying so in the status.
3. **`tables_claimed`** applies the pending plan as it stands — not planned again — and goes on as
   §7.9a: `discover_tables`, `create_table`, the drain.
4. **`claim_rejected`** clears the pending change; nothing else changed, and the dropped tables' names
   stay claimed, since the release went with the claim.

A change that adds no table — removing topics, dropping kept tables — needs no claim: request intake
applies it at once and releases the dropped tables' names (`release_tables`), as a delete releases
every name.

Measured on a test cluster: the claim, its answer and the next step together took a few milliseconds; the
console's Edit dialog closed about 0.1 s after Save.

## 7.10 End-to-end: delete

```
DELETE /materializations/{id}
  → desired_phase = "deleting"
  → request intake: release_tables (all) to the directory — every name is free again at once
  → delete_runtime: 9 deletes in dependency order       → deleting / runtime_apply_in_progress
  → observe_runtime (immediate, no policy)
  → observation: resource absent                        → deleted / resource_missing_after_delete
  → build_deleted_runtime_cleanup_output → cleanup_runtime_kafka (immediate)
  → delete topics prefixed with runtime_name; delete group "<runtime_name>-app"
      ├─ ConsumerGroupNotEmptyError → deleting / waiting_for_consumer_group_empty, retry ≤ 4
      └─ success → binding deleted, desired=observed=deleted,
                   phase_reason = runtime_and_kafka_resources_deleted
  → drop_table, table after table, each in bounded passes queueing the next:
      destination_cleanup_enabled on:  <location>/data/, then <location>/metadata/ but the pointer,
                                       then metadata/catalog-pointer.json
      destination_cleanup_enabled off: the pointer is renamed catalog-pointer.dropped.json — the table
                                       leaves the catalog and stays readable by path
  → purge_materialization_records: every table's schema projections and archive, every table
      record, operation records, runtime binding, status, materialization row
```

**Delete removes everything the materialization owns.** The runtime, its Kafka topics and consumer
group, its tables, and finally every control-plane record — in that order, each step queued by the
one before it. Its tables' names in their datasets are released when the delete is taken in, since
its tables leave the catalog then; a new materialization can claim them while the old files are still
being deleted, at a location of its own. Its *source* topics are not its own and are left alone. A materialization deleted
before any table was created has no table record: the drop is skipped
(`control_plane_drop_table_skipped`, `no_table`) and the chain goes on to the purge. Tables are
dropped in the order of the topics, then any other record the materialization has.

### Dropping a table: bounded passes, off the event loop

A table can hold hundreds of thousands of objects. The drop is a chain of passes
(`services/workflow.py::_process_drop_table`, `storage/objects.py::drop_table_objects_pass`):

1. On the loop: read the materialization, and go straight to the purge if this `work_id`'s
   operation record is already `applied`. A location that is not a `gs://` URI raises `ValueError`
   here, before any request. If the table's record says the maintenance app runs a job on it
   (`maintenance_job`), the pass waits instead: it records `maintenance_wait` and checks again 15 s
   later, for up to 5 minutes (`MAINTENANCE_WAIT_SECONDS`), then forgets the job on the record
   (`control_plane_drop_table_maintenance_wait_expired`) and drops anyway. The control plane
   cancels jobs on a table being dropped (§7.13); a compaction left running would go on writing
   files into the directory being deleted. The app answers a cancel with `maintenance_completed`
   (`cancelled`, or `abandoned` for a job a restarted member no longer runs), which clears the
   record's job even while the materialization is being deleted (06 §6.6), so a drop usually waits a
   minute or two. Only an app that is down keeps it waiting the full five minutes. Before 4.4.1 a
   deleted materialization's completions were dropped, so a delete that met a running job waited
   the full time — and, the job staying recorded, again before every later pass, since each pass
   records the drop's progress afresh.
2. On the `destination-cleanup` thread: one pass empties `data/`, then `metadata/` but for the
   pointer, then deletes the pointer — so the table stays loadable until its last pass. It deletes
   what it lists in GCS batch requests of 100 calls (`DELETE_BATCH_SIZE`, the JSON API's limit),
   at most `CLEANUP_PASS_MAX_BATCHED_OBJECTS` (10,000 — ten listing pages, read 100 at a time, so
   memory holds one page) for about `CLEANUP_PASS_MAX_SECONDS` (10 s, checked between batch
   requests); the time budget is what usually ends a pass. A client that cannot batch — only the
   test doubles — deletes one object per request and is held to `CLEANUP_PASS_MAX_OBJECTS` (1,000).
   The result is complete only once the pointer is gone.
3. On the loop: re-read the materialization — if a purge ran meanwhile, write nothing and stop —
   then update the operation record that every pass of the drop shares:

| Pass | Record `status` | Follow-up |
|---|---|---|
| Objects may remain, or another table is left | `in_progress`, with the running `deleted_count`, `passes` and `tables_dropped` (the tables done) | the next pass, at once: same `work_id`, no `observation_policy`, `requested_at` = now; it takes the first table not in `tables_dropped` |
| Nothing left of the last table | `applied`, with the totals | `purge_materialization_records` |
| Raised | `in_progress`, with `error`, `retry_attempt`, `retry_scheduled_for` | the same pass through the scheduler, after the observation backoff (15, 45, 120, 300 s) |

A pass that succeeds starts the retry count again. The fifth failure in a row raises
`RuntimeError("dropping the table still failing after 4 retries: …")`, which records the step as
`failed` (§7.1). Objects already deleted stay deleted.

**Failed deletes.** google-cloud-storage retries no call in a batch and reports each call's status
only in the batch's response (`raise_exception=False`), so the pass reads them itself:

| A call's answer | Meaning | The pass |
|---|---|---|
| 2xx or 404 | Gone (404: already) | Counts it in `deleted_count` |
| 408, 429, 5xx — or the batch request gets no answer | GCS is throttling or failing | Retries those calls after 1, 2 and 4 s (`BATCH_RETRY_DELAYS_SECONDS`), never past its deadline; if they still fail, stops there |
| Anything else, such as 403 for an object under a hold or retention policy | GCS refuses that object | Leaves it and goes on |

A pass that left objects is not complete, so the next one lists them again, and logs
`destination cleanup pass left objects behind: could not delete N objects from gs://<bucket>
(<status>: <count>); <first object>: <GCS message>` as a warning. A pass that deleted nothing —
every delete it tried failed, or its first listing did — raises instead (`DestinationDeleteError`
with that message), which is the "Raised" row above. So a cleanup deletes all it can and fails only
on what GCS keeps refusing. An error after a pass has deleted something ends the pass with its
count rather than losing it.

Deleted objects drop out of the listing, so a pass needs no cursor: a redelivered or duplicated
pass lists what remains and deletes that. Passes travel through `materialization-work` like any
other step, so other work runs between them and waits at most one pass.

**How long it takes.** One listing request per 1,000 objects and one batch request per 100: about
11 requests per 1,000 objects, where one `DELETE` per object took 1,001. The time depends on how
long GCS takes to answer a batch of 100 deletes, which it does not document. Estimates, from the
real client against a fake GCS JSON API:

| A batch of 100 is answered in | Objects per second | A million objects |
|---|---|---|
| ~60 ms: 30 ms a request, the calls run together (measured on the fake) | ~1,700 | ~10 minutes |
| ~1 s: the calls run one after another, 10 ms each (measured on the fake) | ~70 | ~4 hours |
| Before batching: one request per object, 20–50 ms each | 20–50 | 6–14 hours |

GCS does not document whether a batch's calls run together or one after another, so plan on the
slower row until a real drop shows otherwise: each `control_plane_drop_table_progress` carries
`pass_deleted_count` and `duration_ms`, whose ratio is the rate. The materialization reads `deleted`
throughout and leaves `GET /materializations` when the purge runs. Follow it in the protocol log:
`control_plane_drop_table_progress` after each pass (`deleted_count` so far, `passes`),
`..._retry_scheduled` after a failed one, `..._completed` at the end.

**Retention.** The passes read the table's location from the materialization's records, so the
retention sweep leaves those records alone while the drop is making progress — its `in_progress`
record was updated within `deleted_retention_hours`. A drop that has stopped is collected a retention
window after its last pass.

The final purge is governed by `purge_records_on_delete` (default **on**). Turning it off restores
the older behaviour where records linger until the retention sweep collects them
(`deleted_retention_hours`, default 168), which is worth doing only if you rely on a tombstone
window for audit. The retention sweep still runs either way and remains the safety net for anything
a delete chain failed to finish.

The retention sweep (`purge-deleted-materializations`, hourly) runs on the `__system__` event and
purges nothing there: a delete goes to the shard of the event being handled. It hands each
deleted materialization past the window a `purge_materialization_records`, keyed by that
materialization, which this agent runs in the materialization's own shard. A materialization record
this version cannot read — one an earlier, incompatible release left behind — is a candidate too
once it says `deleted` (12 §12.9).

`purge_materialization_records` deliberately does not require the materialization to still exist:
the step is idempotent so a retried or duplicated event cannot fail merely because the first
attempt succeeded.

## 7.11 Idempotency summary

| Mechanism | Where | Guards against |
|-----------|-------|----------------|
| Insert-if-absent | `_persist_create_request_state` | duplicate create requests |
| Operation record status check | `_duplicate_apply_skip_reason`, `_duplicate_cleanup_skip_reason` | redelivery of the same `work_id` |
| `last_apply_operation_id` | runtime binding | same |
| `last_applied_spec_hash` | runtime binding | reconcile churn |
| `x-scheduler-action: REPLACE` | all scheduled work | duplicate delayed deliveries |
| `desired_phase == "deleted"` check | cleanup | double cleanup |
| A table record at the same location | `create_table` | a redelivered create |
| `if_generation_match=0` on the pointer | `create_table` | two creates of one table; another materialization's table at the location |
| Shared `in_progress` record, re-listing what remains | table drop passes | redelivered, duplicated or interrupted passes |
| Recent `in_progress` drop record | `find_purge_candidates` | the retention sweep purging a drop's records mid-way |
| Status re-read, newer `measured_at` wins | `record_consumer_lag` | a lag saved over a newer one, after a purge, or over a heartbeat written since the measurement |
| Reverse-order rollback | `_cleanup_partially_applied_resources` | half-applied first runtimes |
| Planning again at intake | `_apply_topic_change` | two updates of the topics racing; a redelivered update |
| `x-scheduler-action: REPLACE` per materialization | `await_runtime_drained` | two drain checks at once |
| Record states (`dropping`), not operation records | live drops | redelivered or duplicated live drops |
| Each table's UUID | the runtime's state table (10 §10.8) | a re-created table taking a dropped one's offsets |
| Table events' sequence number | `table-events` agent | redelivered or reordered commit events |
| The table's committed offsets | the runtime's committer | source reprocessing: delivery into the table is exactly once (10 §10.6) |
| `recent_request_ids` | settings document | redelivered settings changes |
| `base_revision` | settings change requests | a change overwriting one its sender never saw |
| delivery `lineage`, then `revision` | runtime settings | an older delivery replacing a newer one; an earlier installation's outranking this one's |

## 7.12 Settings changes

Not a work step: settings changes have their own topic and agent. `PATCH /settings/{scope}`
validates and publishes a `SettingsChangeRequest` to `<release>-settings-requests`. The
`settings-intake` agent then runs `services/settings.py::apply_settings_change`:

```
already in recent_request_ids        → nothing (redelivery)
base_revision no longer current      → refuse: last_rejection = settings_changed
values no longer valid together      → refuse: last_rejection = invalid_setting
nothing would change                 → record the request id, no new revision
otherwise                            → revision + 1, save the document, record the change,
                                       prune history to 100; runtime scope: deliver to runtimes
```

At every start the `settings-bootstrap` task also sends a `bootstrap` request, keyed
`settings:runtime`; the agent runs `bootstrap_runtime_settings`, which begins the runtime settings'
lineage when they have none and delivers them to every runtime.

Full detail, including how the console waits for the result: [17 — Settings](./17-settings.md)
§17.5.

## 7.12a Runtime autoscaling

A runtime far behind its topics runs more members than its materialization's own `replicas` until it
has caught up (`services/autoscaling.py`). Kubernetes' horizontal autoscaler plays no part; the
vertical one keeps sizing each member's CPU (02 §2.10c).

**When it decides.** After every consumer-lag measurement: once a minute the lag refresh hands each
materialization its lag as `record_consumer_lag`, keyed by the materialization (02 §2.5), and the
`maintenance` agent, once the lag is saved, runs `autoscale_after_lag` in the same event — so the
autoscaling record, `runtime-autoscale:<id>` (05 §5.6a), is written in the materialization's own
shard. It keeps the last 16 measurements, the count it gave the runtime and why, when that last
changed, and the time to catch up at the pace measured. A materialization autoscaling may not touch —
not running, switched off, or off for the deployment — and with no record gets none.

**The pace** is the lag's fall per second from the first measurement after the last change to the
latest, within ten minutes; the time to catch up is the lag divided by it, and unknown while it does
not fall.

```
measurement no newer than the last one                    → nothing (a redelivery)
autoscaling off (deployment or materialization), or the
  materialization not running                             → back to its own count if scaled; else nothing
scaled up, and the last 3 measurements ≤ the lag floor    → back to its own count
lag ≤ floor, at the cap, not observed running + healthy,
  within the cooldown of the last change, fewer than 3
  measurements since it, or catching up within the target → nothing
otherwise → min(cap, max(current + 1, wanted)) members,
  wanted = current × time to catch up ÷ target            (the drain shared evenly between members)
         = current × 2 while the lag does not fall
  cap    = runtime_autoscale_max_replicas, or the members its topics' partitions keep busy
           (`assignable_members`) if fewer, never under its own count
```

The floor is `runtime_autoscale_min_lag_records` (100,000), the target
`runtime_autoscale_target_minutes` (30), the cooldown `runtime_autoscale_cooldown_seconds` (300) and
the maximum `runtime_autoscale_max_replicas` (16); `runtime_autoscaling_enabled` turns it off for
every materialization (17 §17.3). Only a runtime observed running and healthy gets more members:
members added to one whose members keep restarting (08 §8.3, row 3d) would only restart too.

**A change** returns an `apply_runtime` work event (`caused_by` scheduler `runtime-autoscaling`,
`work_id` `work-apply-autoscale-<id>-<measured_at>`) and logs `control_plane_runtime_autoscaled`.
The apply renders the materialization through `scaled_materialization` — its own `replicas`
replaced by the scaled count while that is more — and so does the sweep's drift check (§7.7). The
materialization's `replicas` stay what the person set, and its `spec_version` does not move.

**Requests.** A create sets `autoscaling` (on unless the request turns it off); an update may switch
it. Turned off while the runtime is scaled up, intake forgets the record and re-applies the runtime
at its own count at once. A stop or a delete forgets it too, so a resumed runtime starts at its own
count and autoscaling decides afresh. A replica count raised to the scaled one or past it ends the
scaling; one below it applies once the runtime has caught up. The retention sweep purges the record
with the rest of the materialization's.

## 7.13 Table maintenance ticks

Compaction, snapshot expiry and orphan cleanup are the maintenance app's, a KasprApp of its own that
maintains every materialization's tables (10 §10.10). The control plane tells it what to do; the app
keeps nothing of its own.

- Every minute `schedule-table-maintenance` (`onLeader`) triggers `schedule_table_maintenance` on
  the `maintenance` agent, keyed `materialization:__system__`. It only reads
  (`services/table_maintenance.py::build_table_maintenance_messages`) and sends, on
  `<release>-maintenance-requests`, keyed `maintenance:<id>:<table>`:
  - a `maintenance_tick` for each table `active` or `retired`, of every materialization not being
    deleted — stopped and paused ones included. It carries the table (`MaintenanceTarget`), the
    runtimes' maintenance settings with the Settings page's overrides (`maintenance_settings`), when
    each job last finished (`last_runs`, from the record's history) and the job the record says runs;
  - a `maintenance_cancel` for each table `dropping`, or of a materialization being deleted.
- `POST …/maintenance` (04 §4.6.5) builds a `maintenance_request` for each table it names, written or
  kept, whatever the phase; the WebView sends each, keyed by its table.
- The app reports on `materialization-table-events`: `maintenance_started` (a compaction or an orphan
  cleanup), which sets the record's `maintenance_job`, and `maintenance_completed`, which clears it
  and joins the record's history. A drop waits for `maintenance_job` to clear (§7.10).

### The tail app's ticks

The same step names every table to the tail app, which serves the tables' newest records from Kafka
before they are committed (19). It keeps nothing of its own either: what it knows of each table is
the last tick. `services/live_tail.py::build_live_tail_messages` reads the same records and sends, on
`<release>-tail-requests`, keyed `tail:<id>:<table>` (06 §6.18):

- a `tail_tick` for each table `active` of a materialization that is running — every materialization
  has a live tail: the table (`LiveTailTarget`, with the materialization's `skip_tombstones`), the live-tail settings
  resolved with the Settings page's overrides (`live_tail_settings`), and hints from the record — its
  committed offsets, schema id and metadata location;
- a `tail_cancel` for every other table in the catalog, with the reason (`table_dropping`,
  `table_retired`, `materialization_deleting`, `materialization_stopped`); a dropped table is named no
  more.

With `live_tail_enabled` off it sends nothing. It writes nothing, as the maintenance ticks do
(`ShardRoutingTests`). A stop or a resume changes what the next minute's step sends: the tables are
cancelled while the materialization is stopped and ticked again once it runs.

## 7.14 Extending the workflow — checklist

1. Add the step name to `WORK_STEPS` in `models/events.py`.
2. Add a `_process_<step>` function in `services/workflow.py` and a branch in
   `process_work_event`.
3. Decide the error contract: `ValueError` for contract violations (propagate),
   other exceptions for transient failures (recorded as `failed`).
4. Write an operation record with a stable `operation_id` (normally `work_event.work_id`).
5. Add a duplicate-skip guard if the step is not naturally idempotent.
6. Keep the step bounded. Work that grows with the size of something — objects, topics, records —
   does a bounded amount per event and queues the rest. Anything that waits on the network is
   awaited in `process_work_event_async` (an adapter's `*_async` call under its deadline, or a
   worker thread, as the table drop does — §7.10), and what the step writes afterwards it
   writes onto records it re-reads (§7.1).
7. Add the step to the correct agent's `filter` in `charts/midstream/templates/agents/*.yaml` — and to
   `WORKFLOW_APPLY_STEPS` in `services/workflow.py` for `workflow-apply` — and confirm the agents'
   step sets stay disjoint.
8. Add tests to `python/tests/test_workflow.py`.
9. Run `python -m unittest python.tests.test_workflow -v` and `helm template midstream charts/midstream -f charts/midstream/ci/test-values.yaml`.
