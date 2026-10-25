# Workflow Transition Table

## Purpose

This document defines the control-plane transition rules for `create`, `pause`, `resume`, and `delete`.

## Create

| Trigger | Precondition | Action | Desired Phase | Observed Phase | Follow-up |
| --- | --- | --- | --- | --- | --- |
| `create` request accepted | materialization absent | persist materialization and status records | `running` | `requested` | emit `start_discovery` work |
| `start_discovery` work | status is `requested` | run discovery and write schema v1 | `running` | `discovering` then `discovered` | emit `apply_runtime` work |
| `apply_runtime` work | schema exists | render and apply runtime CRD, write binding | `running` | `deploying` | schedule `observe_runtime` |
| runtime status event | runtime healthy and ready | refresh heartbeat/progress | `running` | `running` | no-op |
| observation timeout | resource ready but runtime silent | mark degraded | `running` | `degraded` | recurring sweep |

## Pause

| Trigger | Precondition | Action | Desired Phase | Observed Phase | Follow-up |
| --- | --- | --- | --- | --- | --- |
| `pause` request accepted | materialization exists | persist desired phase | `paused` | unchanged | emit `apply_runtime` work |
| `apply_runtime` work | desired is `paused` | apply paused runtime spec | `paused` | `deploying` or `pausing` | schedule `observe_runtime` |
| resource observation | all owned agent states are `paused` | persist observed pause | `paused` | `paused` | no-op |
| observation timeout | paused state not confirmed | mark degraded | `paused` | `degraded` | recurring sweep |

## Resume

| Trigger | Precondition | Action | Desired Phase | Observed Phase | Follow-up |
| --- | --- | --- | --- | --- | --- |
| `resume` request accepted | materialization exists | persist desired phase | `running` | unchanged | emit `apply_runtime` work |
| `apply_runtime` work | desired is `running` | apply resumed runtime spec | `running` | `deploying` or `resuming` | schedule `observe_runtime` |
| runtime status event | runtime healthy and ready | refresh heartbeat/progress | `running` | `running` | no-op |
| resource observation | resource not ready or mixed agent states persist | mark degraded | `running` | `degraded` | recurring sweep |

## Delete

| Trigger | Precondition | Action | Desired Phase | Observed Phase | Follow-up |
| --- | --- | --- | --- | --- | --- |
| `delete` request accepted | materialization exists | persist desired phase | `deleting` | unchanged | emit `delete_runtime` work |
| `delete_runtime` work | desired is `deleting` | delete runtime CRD and persist operation | `deleting` | `deleting` | schedule `observe_runtime` |
| resource observation | KasprApp resource absent | persist terminal state | `deleting` | `deleted` | optional archival cleanup |
| observation timeout | resource still present unexpectedly | mark degraded | `deleting` | `degraded` | recurring sweep |

## General Rules

- Handlers must be bounded and replay-safe.
- Desired phase changes come from external requests or control-plane workflow decisions.
- Observed phase changes come from runtime events or resource observation.
- `failed` should be reserved for explicit fatal apply errors or policy-based escalation after repeated degraded outcomes.
- `degraded` is the default non-terminal error state.