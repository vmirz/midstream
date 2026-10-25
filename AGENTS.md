## Overview
- Kaspr control plane and dedicated runtime package for materializing Kafka topics into Apache Iceberg tables on GCS, on Kubernetes. A materialization reads one or more topics with one runtime, each topic into an Iceberg table of its own in a **dataset** — a named group of tables and their Iceberg namespace, so SQL reads `<dataset>.<table>`; the materialization's name is a label. One runtime member, the committer, commits to each table exactly once. Access is granted per dataset. A separate KasprApp, the maintenance app, compacts and expires every materialization's tables. Another, the tail app, serves the console each watched table's records past its committed snapshot straight from Kafka (the **live tail**), so the newest records show within seconds, each exactly once.
- Tech stack: Python 3.12+, setuptools 69+, Jinja2 3, PyYAML 6, kubernetes-asyncio 33, pyarrow 18, PyIceberg 0.12 (minor pinned), google-cloud-storage 2, Kaspr CRDs with Kustomize overlays.

## Essential Commands
```bash
# Install local dev deps from repo root
python -m pip install -r requirements-dev.txt

# Build a wheel for the Python package
python -m pip wheel --no-deps ./python

# Full unit test suite
python -m unittest discover -s python/tests -p 'test_*.py' -v

# Target a single test module while iterating (tests import their sibling support modules, so go
# through discovery: `python -m unittest python.tests.<module>` fails for many of them)
python -m unittest discover -s python/tests -p 'test_web.py' -v
python -m unittest discover -s python/tests -p 'test_runtime_templates.py' -v

# The live tail's exactly-once model runs 150 random schedules with the suite; run more after touching tail_app/
TAIL_EXACTLY_ONCE_SCHEDULES=2000 python -m unittest discover -s python/tests -p 'test_tail_exactly_once.py' -v

# Repo has no configured linter/formatter; use a syntax smoke check instead
python -m compileall python/src

# Render manifests to validate Kustomize structure
kubectl kustomize manifests/base
kubectl kustomize manifests/dev

# Access the running dev API locally (there is no standalone local web server in this repo)
kubectl port-forward -n midstream svc/develop-materializer-control-plane-app-api 6065:6065

# Materializer Console (separate npm workspace under /console)
cd console && npm install && npm run dev   # BFF on :8787, client on :5173
cd console && npm run typecheck            # both TypeScript projects
cd console && npm test                     # node --test: reader, run engine, routes
cd console && npm run build                # production client bundle

# Regenerate the console's fixture table (console/testdata/iceberg) after changing what the control plane writes
python python/tests/console_fixture.py

# Read a materialization's table by hand (handbook 11 §11.9): DuckDB through the REST catalog,
# with an access token from the console's Account page
#   CREATE SECRET m (TYPE ICEBERG, TOKEN 'tok_…');
#   ATTACH 'warehouse' AS lake (TYPE ICEBERG, SECRET m, ENDPOINT 'https://<control plane>/catalog');
#   SELECT count(*) FROM lake.<materialization>.<table>;
```

## Project Map / Architecture
- `/python/src/midstream/` — importable control-plane package; business logic lives here.
  - `models/` — immutable domain/event/status models with `to_dict()` / `from_dict()`.
  - `storage/` — Kaspr table keys, envelopes, persistence helpers, bounded listings.
  - `services/` — workflow orchestration and control-plane behavior.
  - `handlers/` — thin Kaspr-facing adapters that deserialize inputs and delegate to services.
  - `web/` — WebView request validation, mutations, and response shaping.
  - `runtime/` — dedicated runtime manifest/template rendering and apply helpers.
  - `runtime_app/` — runtime-side writer, batches and seals, upload, the committer, and template assets.
  - `maintenance_app/` — the maintenance KasprApp (`manifests/base/maintenance/`): compaction (in a child process), snapshot expiry and orphan cleanup of every materialization's tables, stopped ones included, ticked each minute by the control plane (`services/table_maintenance.py`).
  - `tail_app/` — the tail KasprApp (`manifests/base/tail/`): each table's records past its committed offsets, read from Kafka, built into rows by the seal's own `lakehouse/rows.py::build_row`, and served to the console as Parquet; ticked each minute by the control plane (`services/live_tail.py`). It never writes a table.
  - `lakehouse/` — the Iceberg side: the GCS-pointer catalog, table creation and drop, schema inference/evolution/fork map (`lakehouse/schema/`), the row builder the seal and the tail share (`lakehouse/rows.py`), exactly-once commit planning, compaction/expiry/orphan planning, and the REST catalog protocol.
  - `observation/`, `schema/`, `kubernetes/`, `kafka/`, `scheduler/` — status interpretation, schema discovery from a topic sample, cluster/Kafka integration, scheduling helpers.
  - `settings/` — the settings catalog (what is overridable, bounds, sections) and effective-config resolution.
  - `identity/` — permission catalog (each permission's scope: deployment or dataset) and built-in roles/groups, principals and their per-dataset grants, `authorize()`, session tokens, password hashing. Datasets, dataset grants and table-name claims live in the directory too (`services/identity.py`, `storage/identity.py`, `web/datasets.py`).
- `/python/tests/` — `unittest` suite; use these tests as the source of truth for behavior.
- `/manifests/base/` — shared Kaspr CRDs: app, agents, tasks, table, roles, webviews.
- `/manifests/{dev,staging,prod}/` — Kustomize overlays; `dev/` adds `develop-` prefixes and topic/table renames.
- `/console/` — Materializer Console: a self-contained Node BFF plus React client. It depends on the control plane only through its public HTTP API and is intended to be extractable into its own repository. See `console/README.md`, and `console/docs/README.md` for the full 17-document handbook.
- `/KAFKA_MATERIALIZER_SOURCE_OF_TRUTH.md` — code-derived, end-to-end reference document for deep architecture and implementation context.
- `/docs/handbook/` — 19-document deep reference manual. Load the specific document you need rather than the whole set:
  - `01` system overview · `02` deployment/manifests · `03` configuration · `04` HTTP API · `05` data models
  - `06` event & topic contracts · `07` control-plane workflows · `08` observation & status derivation
  - `09` schema evolution & forks · `10` runtime internals · `11` Iceberg tables & storage
  - `12` operations runbook · `13` testing & development · `14` known gaps & roadmap · `15` glossary & FAQ
  - `16` versioning · `17` settings (console overrides, the `settings` table, live runtime settings)
  - `18` identity & access (users, groups, roles, permissions and their scopes, datasets, grants and table-name claims, sessions, how every request is authorized)
  - `19` the live tail (the tail app, exactly once with the snapshot, ownership and forwarding, a table's states, the console's side)
  - Start at `docs/handbook/README.md` for the selection table.

## Code Style & Patterns
- Prefer immutable typed models and config objects:
```python
@dataclass(frozen=True, slots=True)
class Materialization:
    materialization_id: str
    source_topic: str
```

- Use the existing optional-config pattern instead of reading env directly in call sites:
```python
cfg = get_config() if config is None else config
```

- Keep Kaspr handlers thin; push logic into `services/` or `runtime_app/` modules:
```python
request_event = MaterializationRequestEvent.from_dict(value)
return process_request_event(table, request_event, config=cfg, work_id=work_id)
```

- Follow current formatting: `from __future__ import annotations`, standard-library imports first, explicit return types, and concise docstrings on public functions.
- Preserve the `to_dict()` / `from_dict()` round-trip pattern for persisted models and emitted events.

## Guardrails & Boundaries
- Never commit secrets, kube credentials, GCP credentials, SSH keys, or `.env`-style files.
- Never move core business logic into inline manifest Python; CRD YAML should stay as a thin harness over package code.
- Never invent a new linter/test runner/build system; this repo currently uses `pip`, `unittest`, and Kustomize validation for the Python/manifest side, and npm + `tsc` + Vite inside `/console`.
- Never change topic names, table names, `develop-` overlay prefixes, or Kaspr resource names without updating both manifests and affected tests.
- Never write a materialization's Iceberg tables except through its committer, the maintenance app's compaction and expiry (`maintenance_app/`), or the REST catalog with `catalog.write`: every commit goes through the catalog pointer's compare-and-swap, the committer's exactly-once accounting relies on the snapshot summaries it writes, and a maintenance commit carries those offsets forward unchanged.
- Never replace runtime package pinning with floating or implicit refs; runtime template/package revision behavior is intentional and covered by tests.
- Never edit compiled/generated vendor content; this repo’s source of truth is `python/src/`, `python/tests/`, and `manifests/`.
- Always run the relevant `python -m unittest ...` command for touched Python code and `kubectl kustomize ...` for touched manifests before finishing.
- If a change affects WebView or workflow behavior, update or add the nearest existing `python/tests/test_*.py` case instead of relying on manual reasoning only.
- There is no standalone local control-plane server in the repo; prefer unit tests and manifest rendering unless you are intentionally working against the running cluster via port-forward.
- Never import from `python/` inside `/console`, or from `/console` inside `python/`. The console must stay extractable; it talks to the control plane over HTTP only.
- Every WebView must authorize its caller before doing anything: `principal = authorize(request, directory_table, "<permission>")` — with `dataset=` for a dataset permission on a dataset in the path — with the `directory` table bound (and patched in every overlay). Every console `/api` route declares `permit('<permission>')`, `permitAnywhere('<dataset permission>')` plus a per-dataset check, or `signedIn()`. `AuthorizationWiringTests` enforces the control-plane half.
- A table's name in its dataset is claimed by the directory before the table exists (`claim_tables`, sent by request intake) and released when it leaves the catalog; never create a table without a claim, and never write claims except through `identity-intake`. The console's live views read with the service credential and must be cut down per person (`visibleDetail`) before they are sent.
- Never write or delete a materialization's records in the `state` table while handling an event keyed by anything but that materialization (`materialization_routing_key`): a write lands in the handled event's shard, so the copy would strand there. Scheduled steps on `__system__` events only read and hand their work on, keyed by the materialization (handbook 06 §6.4); `ShardRoutingTests` checks them.
- Never write the directory tables from a WebView; publish an `IdentityRequest` keyed `identity_routing_key()` and let `identity-intake` apply it. Never put a plain-text password on a topic, in a log, or in a test assertion's message.
- Never build a table row anywhere but `lakehouse/rows.py::build_row`: the seal and the live tail share it, so the rows the console shows before a commit are the rows the commit holds (`test_tail_parity.py`).
- Adding a setting or a permission has a checklist: `.claude/skills/adding-settings/SKILL.md` and `.claude/skills/adding-permissions/SKILL.md`.
