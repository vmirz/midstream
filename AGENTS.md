## Overview
- Kaspr control plane and dedicated runtime package for materializing Kafka topics into Apache Iceberg tables, on Kubernetes. The **lake** — every materialization's tables — lives in Google Cloud Storage (`gs://`), Amazon S3 or an S3-compatible server that honours conditional writes (`s3://`), or Azure Blob Storage (`abfss://<container>@<account>.dfs.core.windows.net`), each reached through the store of its URI scheme (`lakehouse/store/`); `file://` serves tests and a laptop only, and the chart refuses it. A materialization reads one or more topics with one runtime, each topic into an Iceberg table of its own in a **dataset** — a named group of tables and their Iceberg namespace, so SQL reads `<dataset>.<table>`; the materialization's name is a label. One runtime member, the committer, commits to each table exactly once. Access is granted per dataset. A separate KasprApp, the maintenance app, compacts and expires every materialization's tables, and masks rows written before a protection change (**reprotect**). Another, the tail app, serves the console each watched table's records past its committed snapshot straight from Kafka (the **live tail**), so the newest records show within seconds, each exactly once. **Data protection** masks sensitive values — recognised by field name as kinds such as `email` or `secret` — before anything stages them: hashed, partial or withheld, by each materialization's policy (handbook 22).
- One deployment of all that is a **domain** (`domain.id`, e.g. `domain_a`): its own accounts, datasets, tables, lake and settings. Any number of domains share one console — one URL, `/d/<domain>` per domain, pages across every domain, search, SQL across domains (`<domain>.<dataset>.<table>`, always with the domain), sign-in through Entra ID, GitHub, Google or OpenID Connect into every domain at once, federated access tokens every domain honours, and one Iceberg REST catalog over every domain at the console's `/catalog` (handbook 20).
- Tech stack: Python 3.12+, setuptools 69+, Jinja2 3, PyYAML 6, kubernetes-asyncio 33, pyarrow 18, PyIceberg 0.12 (minor pinned), google-cloud-storage 2.18+, boto3 1.36+, azure-storage-blob 12.31+ (major pinned), azure-identity 1.19+, Kaspr CRDs in a Helm chart. The console's storage SDKs: @google-cloud/storage 8, @aws-sdk/client-s3 3.1160.0, @azure/storage-blob 12.34.0, @azure/identity 4.14.1 (the last three pinned exactly).

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

# The conformance suite against a live store, before a deployment is pointed at it (the same environment and
# credentials as a pod: AWS_*/STORAGE_S3_*, AZURE_*/STORAGE_AZURE_ENDPOINT, GOOGLE_APPLICATION_CREDENTIALS)
python -m midstream.lakehouse.store.check s3://example-lake/_check

# The live store tests: the suite and each backend's own cases, skipped unless STORAGE_LIVE_TESTS names base URIs
# (one S3 endpoint per run); CI's Object stores job runs them against SeaweedFS, moto, Azurite and fake-gcs-server
STORAGE_EMULATOR_HOST=http://localhost:4443 STORAGE_LIVE_TESTS=gs://live-check \
  python -m unittest discover -s python/tests -p 'test_store_live.py' -v

# Repo has no configured linter/formatter; use a syntax smoke check instead
python -m compileall python/src

# One version in pyproject.toml, package.json, its lock file and Chart.yaml (CI fails when they differ)
python tools/version.py check
python tools/version.py set 1.2.0-rc.1   # before tagging v1.2.0-rc.1, which releases everything

# Lint and render the Helm chart (the chart tests render it with every file under charts/midstream/ci/)
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
helm template midstream charts/midstream -n midstream -f charts/midstream/ci/test-values.yaml

# Access the running dev API locally (there is no standalone local web server in this repo)
kubectl port-forward -n midstream svc/midstream-control-plane-app-api 6065:6065

# Midstream Console (separate npm workspace under /console)
cd console && npm install && npm run dev   # BFF on :8787, client on :5173
cd console && npm run typecheck            # both TypeScript projects
cd console && npm test                     # node --test: reader, run engine, routes
cd console && CONSOLE_STORAGE_LIVE=1 node --test server/storage/live.test.ts   # storage profiles vs local SeaweedFS, Azurite, fake-gcs-server
cd console && npm run build                # production client bundle

# Regenerate the console's fixture table (console/testdata/iceberg) after changing what the control plane writes
python python/tests/console_fixture.py

# Read a materialization's table by hand (handbook 11 §11.9): DuckDB through a domain's REST catalog
# behind the console, with an access token from the console's Account page
#   CREATE SECRET m (TYPE ICEBERG, TOKEN 'tok_…');
#   -- and a secret for the data files' backend, TYPE GCS, S3 or AZURE (handbook 11 §11.9; the Table tab's Connect panel)
#   ATTACH 'warehouse' AS lake (TYPE ICEBERG, SECRET m, ENDPOINT 'https://<console>/d/<domain>/catalog');
#   SELECT count(*) FROM lake.<dataset>.<table>;
# PyIceberg and Spark can use the federated catalog instead: uri https://<console>/catalog,
# tables lake.<domain>.<dataset>.<table>
```

## Project Map / Architecture
- `/python/src/midstream/` — importable control-plane package; business logic lives here.
  - `models/` — immutable domain/event/status models with `to_dict()` / `from_dict()`.
  - `storage/` — Kaspr table keys, envelopes, persistence helpers, bounded listings, and the drop pass that deletes a materialization's tables through the store (`objects.py`).
  - `services/` — workflow orchestration and control-plane behavior.
  - `handlers/` — thin Kaspr-facing adapters that deserialize inputs and delegate to services.
  - `web/` — WebView request validation, mutations, and response shaping.
  - `runtime/` — dedicated runtime manifest/template rendering and apply helpers.
  - `runtime_app/` — runtime-side writer, batches and seals, upload, the committer, and template assets.
  - `maintenance_app/` — the maintenance KasprApp (`charts/midstream/templates/maintenance/`): compaction and reprotect (each in a child process), snapshot expiry and orphan cleanup of every materialization's tables, stopped ones included, ticked each minute by the control plane (`services/table_maintenance.py`), each tick carrying a reprotect the table's record asks for.
  - `tail_app/` — the tail KasprApp (`charts/midstream/templates/tail/`): each table's records past its committed offsets, read from Kafka, built into rows by the seal's own `lakehouse/rows.py::build_row`, and served to the console as Parquet; ticked each minute by the control plane (`services/live_tail.py`). It never writes a table.
  - `lakehouse/` — the Iceberg side: the pointer catalog (`catalog.py::PointerCatalog`, `pointer.py`: one pointer object per table, replaced by the store's compare-and-swap; a failed pointer write is judged by the pointer read back: landed, provably not, or unknown), PyIceberg's FileIO over the store (`io.py`), table creation and drop, schema inference/evolution/fork map (`lakehouse/schema/`), the row builder the seal and the tail share (`lakehouse/rows.py`), exactly-once commit planning, compaction/expiry/orphan planning, reprotect planning and rewriting (`lakehouse/reprotect.py`), and the REST catalog protocol.
  - `lakehouse/store/` — the object store layer: the `ObjectStore` protocol (`base.py`: read with the version read, head, put unconditional, create-only or compare-and-swap, stream a file up or down, delete one or many, list), one store per URI scheme from `registry.py::store_for` (`gcs.py`, `s3.py`, `azure.py`; `file.py` and `memory.py` for tests), the URI forms and destination rules (`uris.py`), and the conformance suite (`check.py`).
  - `observation/`, `schema/`, `kubernetes/`, `kafka/`, `scheduler/` — status interpretation, schema discovery from a topic sample, cluster/Kafka integration, scheduling helpers.
  - `settings/` — the settings catalog (what is overridable, bounds, sections) and effective-config resolution.
  - `protection/` — data protection: the kinds and their name rules (`catalog.py`, `rules.py`), the masks (`mask.py`), policies and a table's `materializer.protection` record (`policy.py`), the plan the writer and the tail mask with and how the committer records fields (`plan.py`), and the protection key (`secrets.py`). Delivery and rollout state: `services/protection.py`, `runtime_app/protection_state.py`.
  - `identity/` — permission catalog (each permission's scope: deployment or dataset) and built-in roles/groups, principals and their per-dataset grants, `authorize()`, session tokens, password hashing, federated access tokens and signed revocations (`access_tokens.py`), and sign-in providers (`providers/`: each domain verifies an Entra ID, Google or OpenID Connect ID token, or a GitHub token, itself). Datasets, dataset grants and table-name claims live in the directory too (`services/identity.py`, `storage/identity.py`, `web/datasets.py`).
- `/python/tests/` — `unittest` suite; use these tests as the source of truth for behavior.
- `/charts/midstream/` — the Helm chart: one domain's control plane, maintenance and tail apps (`domain.*`, `controlPlane.enabled`) with its lake's credentials (`storage.gcs`, `storage.s3`, `storage.azure`), and the shared console with its registry of every domain (`console.domains[]`) and its storage profiles (`console.storage[]`), every resource, topic and table named after the release; a release can hold a domain, the console, or both. `templates/` holds the Kaspr CRs (agents, tasks, tables, webviews), the console's Deployment and the routes (with the redirect from old per-domain hostnames), `ci/` the values files the tests render.
- `/docs/install.md` — the installation guide: an empty namespace to a first materialization, with each storage backend's steps.
- `/console/` — Midstream Console: a self-contained Node BFF plus React client, one for every domain. It depends on each control plane only through its public HTTP API and is intended to be extractable into its own repository. `server/domains/` is the registry and each domain's client, live store and status; `server/auth/sso.ts` the providers' flows; `server/routes/catalog.ts` the catalogs; `server/storage/` the storage profiles (one per destination prefix, the longest scope serving an object, each profile's DuckDB secret). See `console/README.md`, and `console/docs/README.md` for the full 17-document handbook.
- `/docs/source-of-truth.md` — code-derived, end-to-end reference document for deep architecture and implementation context.
- `/docs/handbook/` — 22-document deep reference manual. Load the specific document you need rather than the whole set:
  - `01` system overview · `02` deployment and the chart · `03` configuration · `04` HTTP API · `05` data models
  - `06` event & topic contracts · `07` control-plane workflows · `08` observation & status derivation
  - `09` schema evolution & forks · `10` runtime internals · `11` Iceberg tables & storage
  - `12` operations runbook · `13` testing & development · `14` known gaps & roadmap · `15` glossary & FAQ
  - `16` versioning · `17` settings (console overrides, the `settings` table, live runtime settings)
  - `18` identity & access (users, groups, roles, permissions and their scopes, datasets, grants and table-name claims, sessions, how every request is authorized)
  - `19` the live tail (the tail app, exactly once with the snapshot, ownership and forwarding, a table's states, the console's side)
  - `20` domains and the shared console (topologies, the registry, routes and redirects, sessions and single sign-on, federated access tokens, SQL across domains, the federated catalog, lake access)
  - `21` Kafka access and ACLs (the one Kafka user every app shares, the names its grants cover, required vs feature-only ACLs, what breaks without each, `kafka-acls.sh` and Strimzi examples)
  - `22` data protection (kinds and rules, masks, the default and per-materialization policies, masking before staging, a table's `materializer.protection`, live policy edits, the protection key and adding it to an installation made without one, permissions)
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
- Never commit secrets, kube credentials, GCP, AWS or Azure credentials, SSH keys, or `.env`-style files.
- Never move core business logic into inline manifest Python; CRD YAML should stay as a thin harness over package code.
- Never invent a new linter/test runner/build system; this repo currently uses `pip`, `unittest`, and `helm lint`/`helm template` for the Python and chart side, and npm + `tsc` + Vite inside `/console`. CI (`.github/workflows/ci.yml`) runs exactly these; a `v*` tag releases through `release.yml`; every branch push publishes its console image through `dev-images.yml` (handbook 16 §16.3).
- Never change topic names, table names or Kaspr resource names without updating both the chart templates and the affected tests; every deployed name derives from the Helm release name.
- Never write a materialization's Iceberg tables except through its committer, the maintenance app's compaction, expiry and reprotect (`maintenance_app/`), or the REST catalog with `catalog.write`: every commit goes through the catalog pointer's compare-and-swap, the committer's exactly-once accounting relies on the snapshot summaries it writes, and a maintenance commit carries those offsets forward unchanged. A commit that removes files holding plain text keeps no `deleted` manifest entry of them (`_RewriteFiles`): their column bounds hold that text.
- Never import a storage SDK (google-cloud-storage, boto3/botocore, azure-storage-blob, azure-identity) in `python/src/` outside `lakehouse/store/`, nor `@google-cloud/storage`, `@aws-sdk/*` or `@azure/*` outside `console/server/storage/`: everything else reaches a bucket through `store_for(uri)` (the console: the profile covering the URI), so nothing above the store branches on the backend. A new storage operation goes into `ObjectStore`, every store and the conformance suite.
- Never replace runtime package pinning with floating or implicit refs; runtime template/package revision behavior is intentional and covered by tests.
- Never edit compiled/generated vendor content; this repo’s source of truth is `python/src/`, `python/tests/`, and `charts/midstream/`.
- Always run the relevant `python -m unittest ...` command for touched Python code and `helm lint`/`helm template` plus the chart tests (`test_agent_manifests.py`) for touched templates before finishing.
- If a change affects WebView or workflow behavior, update or add the nearest existing `python/tests/test_*.py` case instead of relying on manual reasoning only.
- There is no standalone local control-plane server in the repo; prefer unit tests and chart rendering unless you are intentionally working against the running cluster via port-forward.
- Never import from `python/` inside `/console`, or from `/console` inside `python/`. The console must stay extractable; it talks to the control plane over HTTP only.
- Every WebView must authorize its caller before doing anything: `principal = authorize(request, directory_table, "<permission>")` — with `dataset=` for a dataset permission on a dataset in the path — with the `directory` table bound (named after the release in its template). Every console `/api` route declares `permit('<permission>')`, `permitAnywhere('<dataset permission>')` plus a per-dataset check, or `signedIn()` — checked in the route's domain, every domain route living under `/api/d/<domain>/` — or, for the console's own routes, `permitInHome('<permission>')` (the console's settings) or `permitInSomeDomain('<permission>')` plus a check in each domain a request touches. `AuthorizationWiringTests` enforces the control-plane half.
- Never let one domain's credential reach another: a session, a domain's service token and its tail are only ever sent to their own domain; only federated access tokens cross, and every domain checks them itself. The console never decides who someone is: a provider's credential goes to every domain, and each verifies it (`identity/providers/`).
- A table's name in its dataset is claimed by the directory before the table exists (`claim_tables`, sent by request intake) and released when it leaves the catalog; never create a table without a claim, and never write claims except through `identity-intake`. The console's live views read with the service credential and must be cut down per person (`visibleDetail`) before they are sent.
- Never write or delete a materialization's records in the `state` table while handling an event keyed by anything but that materialization (`materialization_routing_key`): a write lands in the handled event's shard, so the copy would strand there. Scheduled steps on `__system__` events only read and hand their work on, keyed by the materialization (handbook 06 §6.4); `ShardRoutingTests` checks them.
- Never write the directory tables from a WebView; publish an `IdentityRequest` keyed `identity_routing_key()` and let `identity-intake` apply it. Never put a plain-text password on a topic, in a log, or in a test assertion's message.
- Never build a table row anywhere but `lakehouse/rows.py::build_row`: the seal and the live tail share it, so the rows the console shows before a commit are the rows the commit holds (`test_tail_parity.py`).
- Never stage, upload, log or serve a value before the protection plan has masked it (`ProtectionPlan.protect`, writer step 2a and `tail_app/ingest.py::protect`), and never write a protected value plain when the key is missing: wait and report degraded instead. The protection key lives only in the auth Secret; only its id is ever shown. A policy rides the runtime control topic, never the rendered runtime spec.
- Adding a setting or a permission has a checklist: `.claude/skills/adding-settings/SKILL.md` and `.claude/skills/adding-permissions/SKILL.md`.
