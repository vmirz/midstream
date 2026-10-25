# 08 — Observation and Status Derivation

The control plane never trusts a single source. It merges **two independent signals** — what
Kubernetes says about the KasprApp resource, and what the runtime says about itself — into one
`MaterializationStatus`. This document is the complete derivation.

Source: `observation/mapping.py` (260 lines), `observation/policy.py`, `observation/reader.py`,
`services/observation.py`, `handlers/resource_observation.py`.

## 8.1 Why two signals

| Signal | Knows | Cannot know |
|--------|-------|-------------|
| KasprApp resource observation | Pods scheduled, rollout complete, agents paused/running, generation observed | Whether records are actually flowing, whether the writer is wedged |
| Runtime status event | Rows written, offsets, schema version, internal errors | Whether the resource was deleted, whether a rollout is stuck, whether the pod even exists |

Each is authoritative in a different regime, so the merge rules encode **which one wins when**.

`observation_source` on the status records which regime produced the current values:

| Value | Set when |
|-------|----------|
| `resource_observation` | Resource is absent (the only case where K8s alone decides) |
| `runtime_event` | A runtime status arrived and no resource observation has ever happened |
| `hybrid` | Everything else — the normal steady state |

## 8.2 The resource observation input

`handlers/resource_observation.py` reads the live KasprApp through `KasprAppClient` and builds
a `KasprAppObservationInput`:

| Field | Type | Source |
|-------|------|--------|
| `namespace` | `str` | work event target |
| `kaspr_app_name` | `str` | work event target |
| `resource_exists` | `bool` | 404 vs found |
| `managed_agent_states` | `dict[str, str]` | per-agent `running` / `paused` / `unknown` |
| `metadata_generation` | `int \| None` | `.metadata.generation` |
| `observed_generation` | `int \| None` | `.status.observedGeneration` |
| `ready_condition` | `ResourceCondition \| None` | `.status.conditions[type=Ready]` |
| `progressing_condition` | `ResourceCondition \| None` | `.status.conditions[type=Progressing]` |
| `rollout_in_progress` | `bool \| None` | operator-reported |
| `available_members` | `str \| None` | e.g. `"1/1"` — **a string, not a number** |
| `rebalance_required` | `bool \| None` | operator-reported |
| `rebalancing_members` | `str \| None` | string |
| `members` | `tuple[RuntimeMemberState, ...] \| None` | the runtime's pods (`kaspr.io/app=<runtime>`): each member's restarts, last exit and back-off (§8.3, row 3d) |

`members` is read with the core API's `list_namespaced_pod`, which the control plane's role allows
(`get`, `list` on `pods`, 02 §2.7). Pods that cannot be read leave it `None` — the observation goes on
without them, and the client logs the failure once.

`ResourceCondition.status` is the Kubernetes tri-state **string** `"True"` / `"False"` /
`"Unknown"`. Comparing it to a Python `bool` silently fails; always compare to `"True"`.

### Normalization

`normalize_resource_observation(observation, managed_agent_names=...)`:

```python
filtered = {n: s for n, s in observation.managed_agent_states.items()
            if managed_agent_names is None or not managed_agent_names or n in managed_agent_names}
total   = len(filtered)
running = count(s == "running")
paused  = count(s == "paused")
unknown = total - running - paused        # ← everything not running/paused
```

`managed_agent_names` comes from the persisted `RuntimeBinding` (`services/observation.py`
passes `list(binding.managed_agent_names)`), so agents that are not control-plane-managed are
excluded from the counts. If the binding is missing, **no filtering happens** — every agent on
the app counts.

`unknown` is a residual: any state string that is neither `running` nor `paused` (including
`""`, `"starting"`, `"crashloop"`) increments it.

## 8.3 The derivation decision tree

`derive_status_from_resource_observation(current_status, resource_observation, observed_at, observation_policy)`.

### Precomputed predicates

```python
ready_is_true             = ro.ready_condition is not None and ro.ready_condition.status == "True"
has_recent_runtime        = not heartbeat_is_stale(current_status.last_heartbeat, now=observed_at)
has_recent_healthy_runtime = current_status.runtime_health.level == "healthy" and has_recent_runtime
all_paused                = (ro.managed_agents_total is not None
                             and ro.managed_agents_total > 0
                             and ro.managed_agents_total == ro.managed_agents_paused)
```

`heartbeat_is_stale(None, ...)` returns **`True`** (see `_is_stale`: a `None` timestamp is
always stale). A materialization that has never heartbeated is treated as silent.

### The full table, in evaluation order

| # | Condition | `observed_phase` | `phase_reason` | `runtime_health` override | `observation_source` |
|---|-----------|------------------|----------------|---------------------------|----------------------|
| 1 | `not resource_exists` and `desired == "deleting"` | `deleted` | `resource_missing_after_delete` | — | `resource_observation` |
| 2 | `not resource_exists` (otherwise) | `degraded` | `resource_not_ready` | `unreachable` | `resource_observation` |
| 3 | `metadata_generation` set and (`observed_generation` is `None` or `<` it) | `deploying` | `resource_generation_not_observed` | — | `hybrid` |
| 3a | `desired == "stopped"` and `available_members` reads zero (`0/0`) | `stopped` | `runtime_scaled_to_zero` | `unknown` | `hybrid` |
| 3b | `desired == "stopped"`, members still up, **timeout reached** | `degraded` | `runtime_scale_down_stalled` | `degraded` | `hybrid` |
| 3c | `desired == "stopped"`, members still up (or the count is unknown) | `deploying` | `waiting_for_runtime_scale_down` | — | `hybrid` |
| 3d | `desired == "running"` and members keep restarting (`restart_instability`) | `degraded` | `runtime_members_restarting` | `degraded` (`members_restarting`) | `hybrid` |
| 4 | `rollout_in_progress is True` | `deploying` | `resource_rollout_in_progress` | — | `hybrid` |
| 5 | `ready_is_true` and `all_paused` | `paused` | `all_managed_agents_paused` | — | `hybrid` |
| 6 | `ready_is_true` and `has_recent_runtime` and current phase ∈ {`running`,`degraded`,`paused`} | **unchanged** | current or `runtime_ready` | current (preserved) | `hybrid` |
| 7 | `ready_is_true` and `has_recent_healthy_runtime` | `running` | `runtime_ready` | `healthy` | `hybrid` |
| 8 | `ready_is_true` and not healthy-recent, **timeout reached** | `degraded` | `resource_ready_but_runtime_silent` | `unreachable` | `hybrid` |
| 9 | `ready_is_true` and not healthy-recent, timeout not reached | `deploying` | `waiting_for_runtime_ready` | — | `hybrid` |
| 10 | mixed/unknown agent states, **timeout reached** | `degraded` | `mixed_agent_states` | `degraded` | `hybrid` |
| 11 | mixed/unknown agent states, timeout not reached | `deploying` | `waiting_for_runtime_agents` | — | `hybrid` |
| 12 | fallback, **timeout reached** | `degraded` | `resource_not_ready` | `degraded` | `hybrid` |
| 13 | fallback | `deploying` | `waiting_for_runtime_ready` | — | `hybrid` |

Rows 10–13 are only reachable when `ready_is_true` is **false** (rows 5–9 all require it), so
they describe an app whose Ready condition is `False`/`Unknown`/absent.

### Rows 3a–3c: a stop is confirmed by the member count

A KasprApp scaled to zero reports itself `Ready` (the operator's test is "available members equal
desired members", and 0 = 0), so readiness cannot tell a stopped runtime from a running one, and a
heartbeat sent just before the last member exited would let rule 6 keep it `running` for two more
minutes. Rows 3a–3c come before every readiness rule and read `available_members`, the operator's
`"available/desired"` string (`available_member_count` parses it; a bare count is accepted, anything
else is unknown and never taken for zero).

### Row 3d: members that keep restarting

A member that is up sends healthy heartbeats however often it is killed, and the KasprApp is ready
whenever every member happens to be up: a runtime whose members ran out of memory every few minutes
read `running` and healthy, and `deploying` whenever one was down. The pods show it
(`observation/restarts.py`). A member is unstable while

- it waits in `CrashLoopBackOff`;
- its last exit was `OOMKilled`, within the last 30 minutes (`RESTART_WINDOW_SECONDS`);
- its last exit was within those 30 minutes and its container has restarted 3 times or more
  (`FREQUENT_RESTARTS`) since its pod began.

One restart for another reason — a node drain, a deploy — is not instability. Any unstable member of
a materialization meant to run makes it `degraded`, with a message in plain words naming how many and
the commonest cause: *8 of 10 members restarted in the last 30 minutes: out of memory.*, *1 of 2
members is crash-looping: exited with code 1.* The runtime health carries the same message, which
the console shows as the **Unhealthy** badge's tooltip. A heartbeat cannot clear it either (§8.5):
`apply_runtime_status_event` checks the members the last observation read against the heartbeat's
own time, so the state ends 30 minutes after the last restart, when the restarts age out, whether or
not the pods are read again. A pod replaced — a deploy, a scale-down — starts again at zero restarts.

### Rule 6 is the one people miss

```python
if ready_is_true and has_recent_runtime and current_status.observed_phase in {"running","degraded","paused"}:
    return _replace_status(..., observed_phase=current_status.observed_phase,
                                phase_reason=current_status.phase_reason or "runtime_ready",
                                runtime_health=current_status.runtime_health)
```

Once a materialization is in a *settled* phase and the runtime is heartbeating, **the resource
observation stops overwriting the phase**. It refreshes `last_resource_observation_time` and
the `resource_observation` block, but the phase, reason, message, and health stay exactly as
the runtime last set them.

Consequences worth internalizing:

- A `degraded` materialization caused by data-validation errors will **not** be flipped back to
  `running` by a resource observation. Only a healthy runtime status event clears it.
- A `paused` materialization stays `paused` even if the operator briefly reports agents as
  running mid-rollout.
- If the runtime goes silent for > 120 s, `has_recent_runtime` becomes false, rule 6 stops
  applying, and rules 7–9 take over.

### `_has_mixed_or_unknown_states`

```python
states = set(ro.managed_agent_states.values())
return "unknown" in states or ("running" in states and "paused" in states)
```

The literal string `"unknown"` — not the derived `managed_agents_unknown` counter. An agent
state of `"crashloop"` bumps the *counter* but does **not** trip this predicate.

### `_replace_status` — what always happens

```python
transition_time = current_status.last_transition_time
if current_status.observed_phase != observed_phase:
    transition_time = observed_at
replace(current_status,
        observed_phase=..., phase_reason=..., phase_message=...,
        last_transition_time=transition_time,
        last_resource_observation_time=observed_at,     # ← always
        resource_observation=resource_observation,      # ← always
        observation_source=...,
        runtime_health=current if override is None else override)
```

`last_transition_time` only moves on a real phase change — it measures how long the current
phase has held, which is exactly what `desired_observed_mismatch_is_stale` needs.

## 8.4 Observation policy and backoff

```python
# observation/policy.py (code-derived)
DEFAULT_OBSERVATION_BACKOFF_SECONDS               = (15, 45, 120, 300)
DEFAULT_HEARTBEAT_STALE_SECONDS                   = 120
DEFAULT_RESOURCE_OBSERVATION_STALE_SECONDS        = 600
DEFAULT_DESIRED_OBSERVED_MISMATCH_STALE_SECONDS   = 600
```

`observation_backoff_seconds(attempt)`:

| attempt | delay |
|---------|-------|
| < 1 | `ValueError("attempt must be >= 1")` |
| 1 | 15 s |
| 2 | 45 s |
| 3 | 120 s |
| 4 | 300 s |
| ≥ 5 | 300 s (clamped to the last element) |

`build_one_shot_observation_policy(attempt, requested_at)` →
`ObservationPolicy(attempt, max_attempts=4, scheduled_for=requested_at + backoff, mode="one_shot")`.

Cumulative deadline for a deployment: **15 + 45 + 120 + 300 = 480 s** from apply before the
final attempt lands and `_observation_timeout_reached` flips the phase to `degraded`.

### `_observation_timeout_reached`

```python
if policy is None:                                  return False
if policy.mode != "one_shot":                       return False   # ← "sweep" never times out
if policy.attempt is None or policy.max_attempts is None: return False
return policy.attempt >= policy.max_attempts
```

Three ways to never reach a timeout:

1. No policy at all — e.g. the delete-flow observation (`scheduled=False`).
2. `mode == "sweep"` — sweeps observe indefinitely without ever declaring failure.
3. Missing attempt counters.

This is deliberate: only a *deployment* has a deadline. Background sweeps are for keeping the
picture fresh, not for adjudicating failure.

### Staleness helpers

```python
def _is_stale(timestamp, *, now, stale_after_seconds):
    if timestamp is None: return True
    return (_parse(now) - _parse(timestamp)).total_seconds() > stale_after_seconds
```

| Helper | Threshold | Used by |
|--------|-----------|---------|
| `heartbeat_is_stale` | 120 s | rules 6/7 predicates, `_should_sweep`, `_should_preserve_running_status_during_reapply` |
| `resource_observation_is_stale` | 600 s | `_should_sweep` |
| `desired_observed_mismatch_is_stale` | 600 s (only when desired ≠ observed) | `_should_sweep` |

Since the runtime heartbeats every **30 s** and staleness is **120 s**, up to three consecutive
heartbeats can be lost before the runtime is considered silent.

`_parse_timestamp` accepts any ISO-8601 with a `Z` or offset; `_format_timestamp` always emits
`...Z` at second precision.

## 8.5 Runtime status merge

Covered in [06 §6.5](./06-event-and-topic-contracts.md#65-materialization-status). The
essential asymmetry:

| | Resource observation | Runtime status |
|---|---|---|
| Can set `running` | only with a recent **healthy** heartbeat (rule 7) | only with `ready_condition == "True"`, and no member restarting (row 3d) |
| Can set `degraded` | yes (rules 2, 3d, 8, 10, 12) | yes (unconditionally, on `phase == degraded` or `health == degraded`; and while the members the last observation read keep restarting, row 3d) |
| Can set `paused` | yes (rule 5) | no |
| Can set `stopped` | yes (rule 3a) | no — while the desired phase is `stopped`, a heartbeat updates `last_heartbeat` and `runtime_progress` only |
| While a change of topics drains the runtime | keeps the phase (`deploying` / `changing_topics`) and records the KasprApp | updates `last_heartbeat` and `runtime_progress` only |
| Can set `deleted` | yes (rule 1) | no |
| Can set `deploying` | yes (rules 3, 4, 9, 11, 13) | yes (`waiting_for_table`, or ready-but-not-healthy) |

**A drain is neither source's to end.** While the runtime binding is `draining` (07 §7.9a) the
KasprApp is ready with no members on purpose, and members shutting down still report: neither may
set the phase, which `await_runtime_drained` and the apply after it own
(`apply_resource_observation`, `apply_runtime_status_event`).

**A status for a materialization the control plane does not hold is dropped.** A runtime can
outlive its record — purged while it still reported, or left running by an incompatible earlier
release (12 §12.14). `apply_runtime_status_event` then returns nothing and logs the materialization
once (`Ignoring runtime status from …`); before 3.0 it raised, and the `runtime-status` agent read
the same event again on every restart, applying no runtime's status at all.

**Neither source alone can produce `running`.** Both must agree. This is the single most
important invariant in the status system, and the most common source of "why is it stuck at
deploying?" — see [12 — Operations runbook](./12-operations-runbook.md).

The heartbeat merge summarizes every member that reported within `MEMBER_STATE_FRESHNESS_SECONDS`
(120 s, `observation/progress.py`), so one degraded member degrades the materialization, and a
member that stopped reporting drops out after two minutes. The runtime's own degraded reasons:
`commits_stalled` (a member's uploaded files waited ten commit intervals for a commit — the
committer is not committing), `runtime_data_validation_degraded`, `runtime_infrastructure_degraded`
(06 §6.5).

### Table errors

A third source writes the status: the `table-events` agent (06 §6.6). A `table_error` from the
committer — `commit_failed`, `schema_evolution_failed`, `schema_field_limit`, `table_unavailable` —
or from the maintenance app — `compaction_failed` — is stored on the table record and, for a
materialization meant to run, degrades it with the error's reason as its `phase_reason`
(`degrade_for_table_error`). Only a written table's error holds the status, or a `drop_failed`
of a table whose drop keeps failing (`MaterializationTable.error_holds`): a kept table is written by
nothing, so its last error is history, not health. The next
successful commit clears the record's error and, if the status still shows that reason, sets it
back to `running` / `runtime_ready` (`_clear_table_degradation`); the next heartbeat then says how
the runtime is. Every table error clears that way, `compaction_failed` included, so on a busy table a
degradation lasts about a commit interval; a quiet table's lasts until its next commit. The failure
itself stays in the table record's `maintenance` history, and the console's Table tab shows the
partition that compaction gave up on (web 11).

## 8.6 Observation follow-up, and why a phase can outlast its cause

`build_observation_follow_up_output(table, updated_status, work_event, observed_at, deliver_to)`:

```python
if updated_status.observed_phase != "deploying":            return None   # ← settled: stop chasing
if policy is None or policy.mode != "one_shot" or policy.attempt is None or policy.max_attempts is None:
                                                            return None
if policy.attempt >= policy.max_attempts:                   return None   # ← budget exhausted
→ scheduled observe_runtime with attempt+1
```

Three observations:

1. **Only `deploying` chains.** `running`, `paused`, `degraded`, `deleted`, `failed` all stop the
   one-shot chain immediately. Continued visibility comes from the sweep.
2. **Sweep observations never chain** (`mode == "sweep"` fails the mode check, and
   `max_attempts=1` would fail the budget check anyway).
3. The next event **reuses `work_id`** and sets `dedupe_key = f"observe:{id}:{attempt+1}"`,
   `caused_by = EventCause(kind="scheduler", id=f"observe:{id}")`, scheduler
   `request_id = f"observe:{id}"` — so `REPLACE` collapses duplicates.

Logged as `control_plane_runtime_observation_follow_up_scheduled` with
`current_phase_reason`, `next_attempt`, `scheduled_for`.

### A degraded phase can outlast its cause by up to ten minutes

A healthy heartbeat moves a materialization to `running` only while the **stored** resource
observation reads Ready (§8.5). A disruption that makes the KasprApp briefly not Ready — a committer
pod deleted, a rolling restart — may be observed exactly then, and the observation is stored. The
members recover, their heartbeats turn healthy, and the phase stays where the disruption left it,
`degraded` / `commits_stalled` or `mixed_agent_states`: no heartbeat can move it without a Ready
observation, and the sweep re-observes a `degraded` materialization only once its
desired/observed mismatch is `DEFAULT_DESIRED_OBSERVED_MISMATCH_STALE_SECONDS` (600 s) old, or its
resource observation is that old (`_should_sweep`, 8.4).

Seen on the E13 load test (13 §13.8): members healthy from 03:26:42, the materialization
`degraded` / `commits_stalled` until 03:34:43. While it lasts, the table record's **Last commit**
and the members' own states (the console's Overview) are the live answer. *To improve:* have the
sweep re-observe a degraded materialization whose members all report healthy (14 §14.6).

## 8.7 Deleted → Kafka cleanup handoff

```python
# build_deleted_runtime_cleanup_output (code-derived)
if updated_status.observed_phase != "deleted": return None
if target_runtime is None or namespace is None or kaspr_app_name is None: return None
→ build_direct_work_output(WorkEvent(
      work_id=f"work-cleanup-kafka-{id}",
      step="cleanup_runtime_kafka",
      caused_by=EventCause(kind="resource_observation", id=f"runtime-deleted:{id}"),
      dedupe_key=f"cleanup_runtime_kafka:{id}",
      target_runtime=TargetRuntime(namespace, kaspr_app_name)))
```

Immediate, unscheduled. `caused_by.kind == "resource_observation"` is the only place that cause
kind is used.

## 8.8 Interaction matrix — what changes what

| Event | `observed_phase` | `phase_reason` | `runtime_health` | `runtime_progress` | `last_heartbeat` | `last_resource_observation_time` |
|-------|:---:|:---:|:---:|:---:|:---:|:---:|
| Resource observation | maybe (rule 6 preserves) | maybe | maybe | no | no | **always** |
| Runtime status | maybe | maybe | **always** | **always** | **always** | no |
| `apply_runtime` | usually → `deploying` | yes | no | no | no | no |
| `delete_runtime` | → `deleting` | yes | no | no | no | no |
| `cleanup_runtime_kafka` | → `deleted` | yes | → `unknown` | no | no | no |
| Workflow failure | → `failed` | `<step>_failed`, `table_location_in_use`, `table_missing` | → `degraded` | no | no | no |
| Table error (`table-events`) | → `degraded` | the error's reason | → `degraded` | no | no | no |
| A commit after a table error | → `running` | `runtime_ready` | → `healthy` | no | no | no |

`runtime_progress` is **replaced wholesale**, never accumulated — the runtime sends absolute
counters over a rolling window. See [10 — Runtime internals](./10-runtime-internals.md) for
`reset_window=True` semantics.

## 8.9 Reading a status like an operator

```jsonc
{
  "observed_phase": "deploying",
  "phase_reason":   "waiting_for_runtime_ready",
  "last_heartbeat": null,                         // runtime has never reported
  "last_resource_observation_time": "…:31:00Z",   // K8s side is being polled
  "resource_observation": {
    "resource_exists": true,
    "ready_condition": { "type": "Ready", "status": "True" },
    "managed_agents_total": 3, "managed_agents_running": 3
  }
}
```

Reading: the KasprApp is fully up and all three agents are running, but the runtime process has
never emitted a heartbeat: the `emit-status` task is not running, or the members cannot reach
Kafka. Check the runtime pods' logs. A runtime that heartbeats but cannot load its table reports
`waiting_for_table` instead — check `metadata/catalog-pointer.json` and the pods' GCS credentials
(11 §11.10).

```jsonc
{
  "observed_phase": "running",
  "phase_reason":   "waiting_for_source_records",
  "runtime_health": { "level": "healthy" },
  "runtime_progress": { "rows_written": 0, "latest_offsets": { "orders": {} } }
}
```

Reading: everything is healthy; the source topics simply have no new records. Not an error.

```jsonc
{
  "observed_phase": "degraded",
  "phase_reason":   "resource_ready_but_runtime_silent",
  "runtime_health": { "level": "unreachable" }
}
```

Reading: the observation retry budget (480 s) was exhausted with a Ready app and no heartbeat.
The runtime container is up but its tasks never reported. Check the members' logs.

### Diagnostic shortcut

If `phase_reason` is literally equal to a phase name (`running`, `paused`, `deleting`,
`deleted`, `requested`), the status is a **synthesized fallback** from `_build_fallback_status`
or `_get_or_build_status` — no real observation has occurred yet.

## 8.10 Tuning guidance

All observation constants live in `observation/policy.py` and are **not** environment-tunable.
Changing them is a code change plus a package-revision-pinned redeploy of every runtime.

| Constant | Raise it when | Lower it when |
|----------|---------------|---------------|
| `DEFAULT_OBSERVATION_BACKOFF_SECONDS` | Image pulls are slow; runtimes routinely exceed 480 s to first heartbeat | You need faster failure signal in a fast cluster |
| `DEFAULT_HEARTBEAT_STALE_SECONDS` (120) | Heartbeat interval is raised above 40 s | Never below ~3× the heartbeat interval |
| `DEFAULT_RESOURCE_OBSERVATION_STALE_SECONDS` (600) | Sweep cron interval is lengthened | You want tighter K8s-side freshness |
| `DEFAULT_DESIRED_OBSERVED_MISMATCH_STALE_SECONDS` (600) | Pauses/resumes legitimately take longer | You want faster detection of stuck transitions |

If you raise `MATERIALIZER_RUNTIME_HEARTBEAT_INTERVAL` (default `"30s"`), you **must** raise
`DEFAULT_HEARTBEAT_STALE_SECONDS` to at least 3× the new value, or every healthy runtime will
be intermittently classified as silent and flipped between `running` and `degraded`.
