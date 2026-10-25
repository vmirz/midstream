# Observation Policy

## Purpose

This document defines the control-plane policy for delayed runtime observation, stale-heartbeat detection, and retry backoff.

## Principles

- Observation work must stay bounded and replay-safe.
- Runtime status events are preferred for live processing state.
- KasprApp resource inspection is authoritative for lifecycle states the runtime cannot emit.
- The control plane should avoid tight polling loops.

## One-Shot Observation After Mutations

Schedule `observe_runtime` after these operations:

- apply runtime for create
- apply runtime for pause
- apply runtime for resume
- delete runtime

Current implementation note:

- apply flows schedule delayed one-shot observation using the retry backoff below
- delete emits an immediate direct `observe_runtime` follow-up first, then later observation behavior is driven by the normal observation result and sweep policy

Recommended delays:

1. attempt 1: 15 seconds
2. attempt 2: 45 seconds
3. attempt 3: 2 minutes
4. attempt 4: 5 minutes

Maximum one-shot attempts per operation:

- `4`

After the final attempt:

- if resource state is converging but not terminal, mark `observed_phase=degraded`
- set `phase_reason=observation_timeout`
- rely on the recurring sweep to continue background observation

## Recurring Sweep

Use one shared control-plane cron that emits `sweep_observations`.
Also run one startup-only `KasprTask` that emits the same work event when the control-plane runtime boots.

Recommended cadence:

- every 5 minutes

Recommended scheduler policy:

- `missed_fire_policy=skip`

The sweep should only enqueue targeted `observe_runtime` work for materializations that match at least one of these conditions:

- observed phase is `deploying`, `deleting`, or `degraded`
- last heartbeat is stale
- last resource observation is stale
- desired phase and observed phase disagree for longer than policy allows
- rendered runtime spec hash differs from the persisted binding, in which case sweep should emit `apply_runtime` repair work before ordinary observation work

## Staleness Thresholds

Use these initial thresholds:

- runtime heartbeat stale after 2 minutes
- resource observation stale after 10 minutes
- desired/observed mismatch stale after 10 minutes

These values should be configurable later, but fixed defaults are acceptable for MVP.

## Running Detection

To classify a materialization as `running`, require both:

- KasprApp ready condition is `True`
- at least one recent healthy runtime status event within the heartbeat staleness window

If the resource is ready but runtime remains silent:

- keep `observed_phase=deploying` during one-shot observation attempts
- transition to `observed_phase=degraded` with `phase_reason=resource_ready_but_runtime_silent` after timeout
- emit a direct `seed_schema` runtime signal while the resource is ready and the runtime is still in the `waiting_for_runtime_ready` warm-up path

## Pause Detection

To classify a materialization as `paused`, require:

- all owned `linkedResources.agents[].state` values are `paused`

Do not require runtime heartbeat for `paused`.

## Delete Detection

To classify a materialization as `deleted`, require one of:

- KasprApp resource is absent after a delete workflow
- explicit resource status confirms terminal removal, if Kaspr later exposes such a signal

## Failure Escalation

Observation should not directly mark a materialization `failed` unless:

- apply result was explicitly fatal
- repeated observation failures exceeded the policy and product semantics require operator intervention

Default behavior:

- prefer `degraded` over `failed`

## Work Event Conventions

Recommended `dedupe_key` pattern:

- `observe:<materialization_id>:<operation_id>:<attempt>`

Recommended scheduler request id pattern:

- `observe:<materialization_id>` for replaceable one-shot observation
- `sweep:materializations` for the shared recurring cron