# Control Plane Package Layout

## Purpose

This document defines a package layout that aligns with the Kaspr harness model described in the agent and python package documentation, while also conforming to the repository and kustomize structure used by the Kaspr template projects.

The key rule is:

- Kaspr is the runtime harness
- control-plane business logic lives in a normal importable Python package
- Kaspr CRDs stay thin and mostly wire events, tables, and webviews to package entrypoints

## Alignment With Kaspr Harness

The earlier layout captured useful domain boundaries, but it implied a more traditional application package with its own bootstrap and registration layer.

That is not the best fit for Kaspr.

Based on the Kaspr docs, the preferred model is:

- install the control-plane package at the `KasprApp` level via `spec.pythonPackages`
- keep complex logic in importable modules with ordinary Python tests
- keep agent and webview inline Python minimal, using it as a thin harness that imports and calls package code
- perform shared resource initialization in Kaspr `init` blocks, not in per-event handlers

So the package should be designed primarily as a reusable library, not as a standalone long-running app bootstrap.

## Recommended Repository Shape

```text
control-plane/
  PACKAGE_LAYOUT.md
  IMPLEMENTATION_PLAN.md
  contracts/
  manifests/
    base/
      apps/
      agents/
      tables/
      webviews/
      tasks/
      kustomization.yaml
      kustomconfig.yaml
    dev/
      kustomization.yaml
    staging/
      kustomization.yaml
    prod/
      kustomization.yaml
  python/
    pyproject.toml
    src/
      materializer_control_plane/
        __init__.py
        config.py
        models/
        storage/
        services/
        handlers/
        observation/
        kubernetes/
        schema/
        runtime/
        scheduler/
        web/
        utils/
    tests/
```

## Why This Shape Fits Better

- `python/src/materializer_control_plane/` holds the importable package that Kaspr agents and webviews import
- `manifests/` holds the harness-facing CRD definitions and kustomize overlays in the same style as the Kaspr template repos
- package code remains testable outside the Kaspr runtime
- inline Python in CRDs can stay minimal, usually just imports, one-time `init`, and thin handler wrappers

## Manifests Layer

The `manifests/` directory should contain the harness-facing resources, not the business logic.

Suggested shape:

```text
manifests/
  base/
    apps/
      control-plane.yaml
    agents/
      request-intake.yaml
      workflow-apply.yaml
      runtime-status.yaml
      resource-observation.yaml
      schema-proposal.yaml
    tables/
      state.yaml
    webviews/
      materializations.yaml
    tasks/
      # optional task resources later if needed
    kustomization.yaml
    kustomconfig.yaml
  dev/
    kustomization.yaml
  staging/
    kustomization.yaml
  prod/
    kustomization.yaml
```

This matches the structure used by `kaspr-app-template` and concrete repos such as `kaspr-price-scheduler`:

- top-level `manifests/`
- `base/` for shared resources
- per-environment overlay directories with their own `kustomization.yaml`
- resource-type subdirectories such as `apps/`, `agents/`, and `tables/`

Responsibilities of the harness layer:

- define topics, tables, webviews, and routing
- define base manifests and environment-specific overlays via kustomize
- define tiny `init` blocks that construct shared service objects once
- define tiny `map` or `filter` handlers that call package functions
- attach Kaspr tables into handler parameters where needed

The harness layer should not own domain logic.

### `manifests/base/`

Owns the shared control-plane resource definitions.

Expected contents:

- one `KasprApp`
- multiple `KasprAgent` definitions
- the global `KasprTable`
- one or more `KasprWebView` definitions
- optional `KasprTask` resources later if the design needs them
- `kustomization.yaml`
- `kustomconfig.yaml`

### Environment Overlays

The `dev/`, `staging/`, and `prod/` directories should each contain a `kustomization.yaml` that references `./../base` and applies environment-specific patches.

Typical overlay responsibilities:

- name prefixes such as `develop-` in dev
- namespace changes
- replica count changes
- environment-specific resources, secrets references, or package settings

## Python Package Layout

The Python package should be library-first.

Suggested package root:

```text
python/src/materializer_control_plane/
  __init__.py
  config.py
  models/
  storage/
  services/
  handlers/
  observation/
  kubernetes/
  schema/
  runtime/
  scheduler/
  web/
  utils/
```

## Module Responsibilities

### `config.py`

- environment and settings loading
- default values for namespace, scheduler timing, naming conventions, and package behavior

### `models/`

- typed internal representations of materialization records
- schema wrappers for persisted table values
- response model serializers

Suggested files:

- `models/materialization.py`
- `models/status.py`
- `models/binding.py`
- `models/events.py`

### `storage/`

- KasprTable access helpers
- prefix-scan list helpers
- record envelope encode and decode
- persistence functions for materializations, status, bindings, and operations

Suggested files:

- `storage/materializations.py`
- `storage/status.py`
- `storage/bindings.py`
- `storage/operations.py`

### `services/`

- orchestration-friendly service classes that implement core control-plane behavior
- no Kaspr CRD wiring here

Suggested files:

- `services/materializations.py`
- `services/workflow.py`
- `services/observation.py`
- `services/schema_authority.py`

### `handlers/`

- thin adapter functions designed to be called from Kaspr agent `map` or `filter` handlers
- bridge between Kaspr `context`, injected tables, and the service layer

Suggested files:

- `handlers/request_intake.py`
- `handlers/workflow_apply.py`
- `handlers/runtime_status.py`
- `handlers/resource_observation.py`
- `handlers/schema_proposals.py`

These should stay thin. Their job is to unpack Kaspr inputs and delegate to `services/`.

### `observation/`

- KasprApp status interpretation
- observed-phase derivation
- stale heartbeat logic

Suggested files:

- `observation/reader.py`
- `observation/mapping.py`
- `observation/policy.py`

### `kubernetes/`

- Kubernetes API client wrapper
- KasprApp CRUD helpers
- status fetch helpers

Suggested files:

- `kubernetes/kaspr_apps.py`
- `kubernetes/client.py`

### `schema/`

- schema proposal validation
- additive change promotion
- schema authority update building

Suggested files:

- `schema/proposals.py`
- `schema/authority.py`

### `runtime/`

- runtime binding logic
- CRD template input building
- runtime naming and ownership helpers

Suggested files:

- `runtime/bindings.py`
- `runtime/templates.py`
- `runtime/naming.py`

### `scheduler/`

- delayed observe scheduling helpers
- recurring sweep setup helpers
- scheduler request-id generation

Suggested files:

- `scheduler/observe.py`
- `scheduler/sweeps.py`

### `web/`

- WebView-specific validation and response shaping
- thin endpoint call adapters into services

Suggested files:

- `web/materializations.py`
- `web/responses.py`
- `web/errors.py`

### `utils/`

- time helpers
- id generation
- hashing helpers

## Harness Patterns

The intended Kaspr usage pattern should look like this:

1. `manifests/base/apps/control-plane.yaml` installs the external package through `spec.pythonPackages`.
2. Agent `init` blocks import service factories or handler modules and construct shared objects once.
3. Agent operation handlers call thin functions from `handlers/`.
4. Those handlers delegate into `services/`, `storage/`, `observation/`, and `kubernetes/`.
5. WebViews follow the same pattern: thin harness, importable package logic.

Example shape of a harness-side pattern:

```python
from materializer_control_plane.handlers.request_intake import handle_request

def process(value, materializations_table, status_table):
    return handle_request(value, materializations_table, status_table, context=context)
```

That is the desired level of inline Kaspr Python: thin and declarative.

## Ownership Boundaries

- `manifests/` owns harness resources, kustomize structure, and minimal inline Python only.
- `handlers/` owns Kaspr-facing adapter logic only.
- `services/` owns control-plane behavior.
- `storage/` owns table key construction and record persistence.
- `observation/` owns lifecycle-state derivation from runtime events and KasprApp status.
- `kubernetes/` owns direct Kubernetes and KasprApp API access.
- `runtime/` owns deterministic app naming, agent ownership metadata, and CRD template input assembly.
- `web/` owns request validation and response shaping for WebViews.

## What To Avoid

- do not put large business logic blobs inline in CRD YAML
- do not make Kaspr handlers construct expensive clients per event
- do not make the package depend on an internal bootstrap flow that only exists inside a monolithic `app.py`
- do not let harness code become the main place where workflow decisions are made

## Recommended Scaffolding Order

1. `python/pyproject.toml` and package skeleton under `python/src/materializer_control_plane/`
2. `models/`, `storage/`, and `config.py`
3. `services/` and `observation/`
4. `handlers/` and `web/`
5. `kubernetes/`, `runtime/`, and `scheduler/`
6. initial `manifests/base/` resources and environment overlays that import the package

## Testing Boundaries

- unit test `observation/mapping.py` heavily because it encodes lifecycle precedence
- unit test `storage/` prefix scan and key mapping behavior
- unit test `services/` without Kaspr runtime dependencies
- unit test `handlers/` as thin adapter layers with fake `context` and fake tables
- unit test `web/` request validation and bounded list endpoint behavior
- integration test `kubernetes/` adapters separately from workflow state logic
- keep most tests outside the Kaspr CRD lifecycle so the package remains independently verifiable
