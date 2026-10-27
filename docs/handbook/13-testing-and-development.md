# 13 — Testing and Development

Everything needed to build, test, and extend the codebase locally, and what the lab showed.

## 13.1 Environment setup

```bash
# Python 3.12+ required (pyproject: requires-python = ">=3.12")
python3 --version

# install the package in editable mode + dev deps
python3 -m pip install -r requirements-dev.txt
```

`requirements-dev.txt` is a single line: `-e ./python`. There are no separate test-only
dependencies — the suite is stdlib `unittest`.

> On macOS with a framework Python install, `python` may not be on `PATH` while `python3` is.
> All commands below work with either; substitute whichever your shell resolves.

### Runtime dependencies actually required by tests

| Package | Declared in `pyproject.toml`? | Needed by |
|---------|-------------------------------|-----------|
| `pyiceberg[pyarrow]>=0.12,<0.13` | yes | `lakehouse/` and everything built on it: the catalog, commits, schema policy, maintenance, the REST protocol |
| `pyarrow>=18.1.0` | yes | seals, compaction, footers (`test_runtime_seal`, `test_runtime_flow`, `test_lakehouse_*`) |
| `google-cloud-storage>=2.18.0` | yes | its exceptions are what `gcs_fakes.py` raises; `test_batched_destination_deletes` runs the real client against a fake JSON API and is skipped without it |
| `jinja2>=3.1.0`, `pyyaml>=6.0.0` | yes | `runtime/templates.py`, `test_runtime_templates`, `test_agent_manifests` |
| `kubernetes-asyncio>=33.1.0` | yes | `kubernetes/client.py` |
| `aiokafka` | **no** | `schema/discovery.py`, `kafka/admin.py` — imported lazily; `test_kafka_admin` asserts a clean failure when absent |
| `faust` / Kaspr runtime | **no** | supplied by the Kaspr image at runtime; code has fallbacks. With twm-faust installed (plus `intervaltree` and `sortedcontainers`), `AckLedgerAgainstFaustTests` also runs the ack ledger's properties against Faust's own commit bookkeeping; without it they are skipped |

The suite passes without `aiokafka` and without `faust`. Nothing needs network, a bucket or a
cluster: GCS is an in-memory fake with object generations (§13.3).

## 13.2 Commands

```bash
# full suite (1,248 tests, ~90 s)
python3 -m unittest discover -s python/tests -p 'test_*.py' -v

# quiet
python3 -m unittest discover -s python/tests -p 'test_*.py'

# one module — through discovery, which puts python/tests on sys.path for the support modules
# (python -m unittest python.tests.<module> fails for every module that imports one)
python3 -m unittest discover -s python/tests -p 'test_lakehouse_commits.py' -v

# one class or test: -k matches names
python3 -m unittest discover -s python/tests -p 'test_runtime_committer.py' -k announced -v

# stop on first failure
python3 -m unittest discover -s python/tests -p 'test_*.py' -f

# syntax smoke check (there is no linter configured)
python3 -m compileall -q python/src

# build a wheel
python3 -m pip wheel --no-deps ./python

# render the chart
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
helm template midstream charts/midstream -n midstream -f charts/midstream/ci/test-values.yaml

# the console (console/docs/14)
cd console && npm run typecheck && npm test && npm run build
```

**There is no linter, formatter, or type-checker configured.** No `ruff`, `black`, `flake8`,
`mypy`, `pytest`, `tox`, or `pre-commit` config exists. Do not add one as a side effect of an
unrelated change.

**There is no local server.** The control plane only runs as a KasprApp inside Kubernetes.
Validate locally with unit tests and `helm template`; validate end-to-end against a cluster
(§13.8, and [12](./12-operations-runbook.md)).

## 13.3 Test suite map

1200 tests across 52 modules. Treat these as the executable specification — when prose and tests
disagree, the tests win.

### The Iceberg side

| Module | Tests | Covers |
|--------|-------|--------|
| `test_lakehouse_catalog.py` | 17 | The GCS-pointer catalog: the pointer's compare-and-swap, table creation with fixed field ids, commits once and only once, drops |
| `test_lakehouse_commits.py` | 29 | The committer's decisions (`plan_append`): a seal continuing the table accepted, skipped records no gap, covered and overlapping seals refused, a gap never jumped; pending files across sessions and processes — a restarted member heard whatever its generation, an older session of one process stale; appends carrying offsets, totals and valid-through; one file per day for a seal spanning days, and per month in a month table, its partition as Iceberg computes it; file naming |
| `test_lakehouse_schema_policy.py` | 28 | The schema policy row by row (09 §9.4): additions, deferrals, forks, collapses, the caps and `_unmapped`, key naming |
| `test_lakehouse_conform.py` | 16 | Conforming records onto a schema, drift detection and the API projection agree on what fits |
| `test_lakehouse_maintenance.py` | 32 | Compaction planning — the partition being written waits, this month's files settle one by one, a backfill's past partitions do not and a past month is compacted into files of its own, several small partitions in one job each into its own files, a partition only whole, a failed one alone — the worker — inputs read and written a budget at a time, outputs closed at the target size and sorted a row group at a time, a missing list kept null — the commit's per-partition checks; snapshot expiry; orphans |
| `test_catalog.py` | 18 | The REST catalog: config, a namespace per dataset the caller may read — locked ones hidden — its tables, whichever materialization writes them, kept ones included; names resolved through their claims; loads from each table's own location and a cache keyed by the table's UUID; commits through the pointer with `catalog.write` on the dataset, `409` on a stale commit, commits that would move files refused, `501`s, errors in the shape engines parse |

### The runtime

| Module | Tests | Covers |
|--------|-------|--------|
| `test_runtime_flow.py` | 14 | The runtime end to end in one process (`runtime_support.RuntimeHarness`): records become rows and are acknowledged once committed; a batch's first record held until the commit; a replayed record the table holds acknowledged without a row; a new field evolving the schema before its batch seals; announcements naming their process; a restarted member committing to a committer that ran on (`restart_member`) |
| `test_runtime_multi_topic.py` | 12 | One runtime reading several topics: each topic's records rows of its own table exactly once, a partition of each topic its own, replays recognised per table, a topic it does not read dropped, a new field evolving only its table's schema, a table whose commits fail holding no other back, the heartbeat naming every table; seals and uploads bounded by the member's workers whatever the tables, the backlog counted for all tables together and pausing every topic — one not yet read included — and resuming them |
| `test_runtime_committer.py` | 18 | The committer's event handling: a seal after a gap waits; a seal the table holds is refused and its file **kept**; a committed seal announced again keeps its file; an overlapping seal's files deleted; a restarted member's seals committed though its generation starts again; a stale announcement logged and counted; reporting; a table re-created under its old name reading none of its predecessor's offsets or schema (`RecreatedTableTests`) |
| `test_runtime_writer.py` | 11 | The writer: every record staged as it arrived, never waiting, acknowledged only when safe |
| `test_runtime_acks.py` | 21 | The ack ledger: property tests over simulated partitions — gaps, skipped records, random rolls and durability, rebalances — against `FaustCommitModel` and, where installed, twm-faust itself: the committed offset never passes a row that is not in the table |
| `test_runtime_seal.py` | 12 | The seal: staged JSONL conformed and written as Iceberg Parquet with field ids, a month table's rows split by month; a batch over two years written a partition at a time from spools flushed in pieces, none left behind, a failed seal's included; byte for byte what it wrote before the shared row builder (`seal_reference.py`), and the golden rows (`seal_cases.py`, `golden/seal_rows.json`) |
| `test_row_builder.py` | 14 | The shared row builder (`lakehouse/rows.py`): every value kind into its column — forks, a collapsed family, structs and lists, `_unmapped`, `$` for a value that is not an object, tombstones, `b64:` keys and headers, a missing timestamp — and the seal writing exactly the rows it builds |
| `test_runtime_status.py` | 6 | What each member reports on its heartbeat, the committer's table block |
| `test_runtime_templates.py` | 20 | The eight rendered resources, the state store's write buffer bounded well inside the default volume, naming and truncation, `spec_hash` stability, package pinning, the writer's `noack`, `K_STREAM_WAIT_EMPTY`, the timers; one runtime reading every topic into its own table, and none rendered before every table exists; members autoscaled as the deployment says |
| `test_runtime_settings.py` | 23 | Live runtime settings: overrides over the environment, deliveries stored and installed, the newest kept by lineage then revision — an earlier installation's never outranking the current one's — the settings agent, the heartbeat's lineage, and that no editable setting changes the rendered spec |
| `test_runtime_config.py` | 4 | `get_runtime_app_config()` defaults, commit and maintenance settings, combinations that cannot work refused |

### The maintenance app

| Module | Tests | Covers |
|--------|-------|--------|
| `test_maintenance_app.py` | 30 | The app over real tables on a fake GCS (`MaintenanceHarness`): a compaction rewriting small files with every row kept, the partition being written left alone, a backfill's past days taken, small days in one job, a backlog a job a step, expiry not held up, the target file size, a failed multi-day job retried a day at a time, backoff and giving up, the deadline; the runtime committing meanwhile; expiry and orphans; the schedule from the ticks, abandoned and stale jobs, cancels, forgetting a table, settings that do not fit; one slot shared across tables, more slots; the handlers |
| `test_table_maintenance.py` | 5 | What the control plane ticks: every written and kept table, stopped materializations' included, with the overridden settings and the last runs; cancels for tables being dropped or deleted; the scheduled step writing nothing |

### The live tail

| Module | Tests | Covers |
|--------|-------|--------|
| `test_tail_exactly_once.py` | 1 | R2 by a model: random schedules (150 with the suite; `TAIL_EXACTLY_ONCE_SCHEDULES` for more) drive the real worker, buffer, fetch plan and answers against a simulated Kafka, runtimes, control plane and console — offsets with no record, undecodable values, two tables of one topic, commits, lagging hints, bursts past small bounds, ceilings, retention and idle windows, restarts, schema changes — and check after every answer that a pinned snapshot and its tails hold every record exactly once |
| `test_tail_parity.py` | 8 | R3: each record taken the runtime's way (Faust event, the writer's staging, `run_seal`) and the tail's (aiokafka record, `ingest.py`, the buffer, `serve.py`) gives the same rows under the same schema, before and after it evolves; a change in flight counted as settling; time bounds floored exactly |
| `test_tail_app.py` | 35 | The members end to end over fakes (`TailHarness`): ticks and cancels, activation and seeding, ingest, answers and their headers, commits trimming, schema changes rebuilding held rows, bounds pausing and resuming, backlogs, parking and forgetting, the head peek; the source consumer waiting for a batch of records while the others answer at once; offset lookups leaving no topic tracked, an assignment its own; a 4.1 control plane's cancel and switch still honoured |
| `test_tail_buffer.py` | 15 | A table's records in memory: everything past `after` once across closed and open segments, records needing no row covered, an `after` older than what is held refused for the whole table, a partition not named read from its beginning, retention and trims, commits only rising, caps cutting each partition to a prefix, segments closing at their size or age with their staged lines compressed and weighed once, a gap Kafka made |
| `test_tail_routing.py` | 14 | Ownership as the control plane's producer partitions the keys, one owner per table and hundreds spread; the member map from `DescribeGroups` metadata (a member's pod name when it does not read), truncated data refused; one forwarding hop, a stale map read again once, an unreachable owner, its refusals passed through, a lost partition let go; a tick on another partition logged |
| `test_tail_security.py` | 17 | The console's credential on every view; the principal rebuilt from a session deciding as the control plane would on every dataset; no session, an ended one, a revoked token, a password to change; `data.query` on the table's dataset for rows and the peek, a token limited to another dataset; the service credential reading health, never rows; sessions trusted through an outage for the grace only; a forged forwarding header, paths, bad queries; no row in a log line |
| `test_tail_failures.py` | 13 | A failed fetch resumed with no gap or duplicate, a partition without a leader holding back only itself, a deleted topic and its return, a seed with no schema anywhere retried, malformed messages, duplicate and late ticks, a commit ahead of ingest, other tables' events, more requests than serve slots, a table that never catches up answering at its deadline, a failing peek |
| `test_tail_http.py` | 4 | The app's HTTP client against a real aiohttp server: a body read to its end across chunks, bounded, or a failure — never part of one |
| `test_live_tail_scheduler.py` | 8 | What the control plane ticks: active tables of every running materialization, a record from before 4.3 switched off included, cancels for every other table with their reasons, the settings and hints carried, nothing with `live_tail_enabled` off |

### The control plane

| Module | Tests | Covers |
|--------|-------|--------|
| `test_datasets.py` | 40 | Datasets, grants and table-name claims in the directory agent: a dataset made with its owner and All Users grants, renamed never, deleted only empty; grants only by those who hold the roles, never Guest, gone with their person, group or role; claims all or none, with each refusal reason and the row it names, redeliveries, releases and the marker that refuses a late claim; the hourly sweep; authorization per dataset, and tokens limited to datasets; the dataset views |
| `test_topic_changes.py` | 17 | Planning a change of topics: an added topic's table in its dataset and its name, tables kept or dropped as asked, a decision required for each removed topic, a kept table written again or dropped, names never reused, redeliveries |
| `test_workflow_topic_changes.py` | 18 | The change end to end through the workflow, its new tables' names claimed first (`ClaimedTopicChangeTests`): discover, create, drain until the group is empty, offsets set for added topics, apply; a group that never empties; heartbeats and sweeps leaving the drain alone; live drops after the drain, with cleanup off, failing, waiting for a running maintenance job and for a maintenance app that never answers; a delete meanwhile |
| `test_workflow.py` | 33 | A claim's answer — discovery once the names are claimed, a refused create marked deleted with its reason (`ClaimAnswerTests`) — discovery, the tables, the runtime, and deletion back to nothing: step dispatch, each topic's schema 0, `create_table` a table a pass, a destination that cannot hold a table, apply, a resumed create finishing its tables, delete ordering, Kafka cleanup, every table dropped under one operation and the drop resuming with its table; a job the maintenance app forgot, waited out once rather than before every pass |
| `test_request_intake.py` | 32 | Persisting requests, action → step, scale, tombstone handling, granularity, a live tail switch in a request from before 4.3 dropped; the claim a create sends, the pending change and claim an update adding tables sends, releases on a delete and a drop; a change of topics planned again at intake, and a raced one refused in the status |
| `test_table_events.py` | 22 | The table-events agent keeping each table's record and schema projection in step with the runtime and the maintenance app — the running job set and cleared, reports for tables being dropped or materializations being deleted ignored but for the end of a running job, which their drop waits for; a status degraded while any table has an error |
| `test_observation.py` | 56 | Backoff schedule, staleness, the phase-derivation rules, stops, runtime status events — one for a materialization the control plane does not hold dropped, each member's settings lineage kept — the async handlers; members that keep restarting: crash loops, memory kills and frequent restarts within the window, the message, a heartbeat unable to clear it until the restarts age out |
| `test_schema_discovery.py` | 15 | Sampling both ends of every partition within its budget and deadlines; every topic at once by one small-fetching consumer |
| `test_table_names.py` | 15 | Dataset and table names — lowercase only, reserved words, DuckDB's own namespaces — labels, table names derived from topics (the vectors the console's port shares), suffixes in order, source-topic validation |
| `test_web.py` | 108 | Autoscaling on create and update and in the detail and system settings; create validation (tables in datasets, names, labels, granularity; no materialization without a live tail; the destination, namespace, runtime name and storage refused, naming the deployment's, and no create without a destination), the claim previewed — `404`, `403`, `409 table_name_taken` naming the row — list search and shaping, detail with each table's access for the caller, schema views refused without `dataset.view_schema` on the table's dataset, table views, maintenance, mutations and their refusals, a change of topics needing write access to every dataset it touches, topics |
| `test_autoscaling.py` | 24 | Runtime autoscaling: the decision (lag floor, pace, target, cap by partitions and maximum, health, cooldown, measurements since the last change, back once calm or switched off, a redelivered measurement), every render at the scaled count — the apply's and the sweep's, so a scaled runtime is not drift — the handler after a recorded lag, and requests: a create's switch, an update turning it off releasing the members at once, a stop forgetting it |
| `test_maintenance.py` | 89 | Replica rules and the combined recommendation, list filters, cumulative progress, retention, the table drop in bounded passes, topic analysis, consumer lag per topic, the async maintenance handler, and shard routing: the scheduled steps write nothing on the `__system__` event and leave each record in its materialization's own shard (`ShardRoutingTests`) |
| `test_batched_destination_deletes.py` | 14 | The drop's deletes through the real google-cloud-storage client over a fake GCS JSON API: batching, 404 counted as gone, retries, refusals left behind |
| `test_scheduler_routing.py` | 10 | Scheduler headers, `REPLACE` semantics, routed outputs |
| `test_storage.py` | 33 | Key layout, envelopes, persistence, schema-projection history per table, bounded listings, records an earlier release left that this one cannot read |
| `test_models.py` | 33 | `to_dict`/`from_dict` round trips and validation for every persisted model, a pending change of topics, a claim's answer, the settings lineage and the live tail's messages included, and the live tail switch of a record from before 4.3 read and dropped |
| `test_settings.py` | 54 | The settings catalog against config and env vars, precedence, applying changes, views — the runtime view saying how members are autoscaled — the runtime settings' lineage begun at startup (the task's own code run) and carried by every delivery, the runtime rollout by lineage and revision, a runtime upgraded from before lineages included |
| `test_identity.py` | 85 | Permissions, their scopes and built-in roles, passwords, session and access tokens, authorization, bootstrap, every directory rule, sign-in and lockout, the views; `503 starting` for every caller, the session view and sign-in while a restarted control plane reads its tables back (`StartingTests`) |
| `test_sso.py` | 37 | Sign-in through a provider (18 §18.17), against a fake of each provider's endpoints: Entra ID by tenant, client and key, Google's verified emails and hosted domains, a generic issuer's discovery, GitHub's token check with the App's secret; nonces used once, old and unsigned tokens refused, an unknown key read again at most once a minute, a provider that cannot be reached; then each domain's side — linking by email, `open` making one account for two sign-ins at once, `invited` refusing without counting towards a lockout, allowed email domains first, a deactivated account, a provider session needing no password change; password sign-in off for everyone but administrators |
| `test_federated_tokens.py` | 16 | Federated access tokens and revocations (20 §20.8): the format, any change breaking the signature, the previous key still checking; a token made in one domain read in another as its owner there, never with more than they hold there, dataset limits per domain, the owner found by provider account before email, refused where the owner has no account; a revocation signed, forged ones refused by the view and the agent, listed for the console to hand on, kept until its token has expired |
| `test_chart_domains.py` | 24 | The chart's domains (20 §20.2): a console alone, a domain alone, the control plane told which domain it is, the federation key from the auth Secret, the registry with no token in it and the console rolled when it changes, the routes and the redirect (Gateway API and ingress-nginx), and every values combination the chart refuses |
| `test_versioning.py` | 8 | Version reporting across the control plane and its runtimes |
| `test_agent_manifests.py` | 31 | Chart wiring, rendered with every values file under `charts/midstream/ci/`: every app's CPU request set from its use; the `directory` table bound in every view and agent that authorizes (`DirectoryWiringTests`), settings bound where configuration is read, async agent maps, every WebView authorizing (`AuthorizationWiringTests`), identity requests keyed one way, every overlay defining what its views and agents use, the maintenance app owning its agent and task, ticks keyed by table; the tail app's views answering their errors as JSON, the schema views passing `raw` on |
| `test_topic_catalog.py` | 24 | The topic list: classification, the shared cache, the metadata client's deadlines, analysis off the loop; no metadata request naming a topic, nor a topic left tracked by an offset lookup |
| `test_kafka_admin.py`, `test_kubernetes_client.py` | 19, 11 | Adapter deadlines and loops; a group's state and its offsets committed from outside it; a runtime's pods read for their restarts, and an observation without them when they cannot be; graceful behaviour without `aiokafka` |
| `test_config.py`, `test_package_layout.py` | 10, 2 | Config contract defaults and env parsing; every subpackage imports |

### Test doubles

The suite uses hand-written fakes, not mocks. Reusing them is the fastest way to write a new test.

| Fake | Defined in | Emulates |
|------|-----------|----------|
| `FakeGcs`, `FakeClient`, `FakeBucket`, `FakeBlob` | `gcs_fakes.py` | GCS in memory with object generations: `if_generation_match` honoured as GCS does (0: must not exist), the library's own `PreconditionFailed` and `NotFound`, range downloads, prefix listings, failures injected per operation |
| `LakeFixture` | `lakehouse_support.py` | A bucket holding one materializer table, with helpers to write seals as data files and commit them |
| `RuntimeHarness` | `runtime_support.py` | One runtime member, its committer and its tables — one per topic in `topics`, each seeded from `seeds_by_topic` — in one process: the writer's own staging, the batch pipeline, the committer, the outbox's messages passed on as the commits topic would; Faust modelled by `FakeConsumer` |
| `build_table_record` | `table_records.py` | A table record as `create_table` stores it |
| `FakeTable` | most modules, `runtime_support.py` | Kaspr table: `get`/`__setitem__`/`__delitem__`/`prefix_scan` over a dict |
| `FakeKasprAppClient`, `FailingKasprAppClient` | `test_workflow`, `test_observation`, `test_scheduler_routing` | Kubernetes apply/delete/read; records calls, or always raises |
| `FakeKafkaAdminClient` | `test_workflow` | Topic and consumer-group deletion, `ConsumerGroupNotEmptyError` |
| `FakeGcsJsonApi` | `test_batched_destination_deletes` | The GCS JSON API behind `requests.Session.request`, for the real client |
| `LoopHandshake`, `AwaitingMetadataClient` | `test_topic_catalog`, `test_maintenance` | Prove the event loop ran while a call was in flight; Kafka lookups that suspend and let another agent write meanwhile |
| `TailHarness`, `FakeKafka`, `FakeConsumer`, `FakeAdmin`, `FakeControlPlane`, `Clock` | `tail_support.py` | Tail app members in one process: topics with offsets that hold no record, a consumer that fetches and seeks as aiokafka does, the group as `DescribeGroups` reports it, the control plane's sessions, schemas and tables, and a clock the test moves |

`console_fixture.py` is not a test: it writes the console's fixture tables — `orders` and
`payments`, the two tables of one materialization `shop` (`console/testdata/iceberg/{orders,payments}`) —
with the library, `python python/tests/console_fixture.py` (console 14 §14.2).

## 13.4 Adding a test

Place it in the module that already owns the behavior — do not create new files unless you are
adding a genuinely new subsystem.

| Change | Test module to extend |
|--------|----------------------|
| New/changed HTTP endpoint or validation | `test_web.py` |
| New workflow step or step behavior | `test_workflow.py` |
| Observation/phase derivation rule | `test_observation.py` |
| Schema evolution: a rule, a cap, naming | `test_lakehouse_schema_policy.py`, and `test_lakehouse_conform.py` for what fits |
| What the committer decides or commits | `test_lakehouse_commits.py`; its event handling `test_runtime_committer.py` |
| Records to rows, acknowledgement | `test_runtime_flow.py`, `test_runtime_acks.py` |
| Compaction, expiry, orphans | `test_lakehouse_maintenance.py`; through the maintenance app `test_maintenance_app.py`; what it is ticked `test_table_maintenance.py` |
| A change of topics: the plan, the drain, the drops | `test_topic_changes.py`, `test_workflow_topic_changes.py` |
| The pointer catalog | `test_lakehouse_catalog.py`; the REST catalog `test_catalog.py` |
| A setting: catalog entry, precedence, applying a change | `test_settings.py` (runtime: also `test_runtime_settings.py`) |
| A permission, a directory rule, sign-in or sessions, an identity view | `test_identity.py` |
| A new WebView | `test_agent_manifests.py` checks it authorizes; add its behavior to the module that owns it |
| New persisted model or field | `test_models.py` + `test_storage.py` |
| Manifest/template rendering | `test_runtime_templates.py` |
| Scheduler headers or routed outputs | `test_scheduler_routing.py` |
| New config field | `test_config.py` or `test_runtime_config.py` |

Skeleton following the house style:

```python
from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import LakeFixture  # noqa: E402

from midstream.lakehouse.maintenance import live_data_files  # noqa: E402


class MyBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lake = LakeFixture({"order_id": "O-1", "total": 5})
        self.addCleanup(self.lake.close)

    def test_something_specific(self) -> None:
        self.lake.commit(self.lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1]))
        self.assertEqual(2, sum(item.record_count for item in live_data_files(self.lake.load())))


if __name__ == "__main__":
    unittest.main()
```

Conventions to follow:

- `from __future__ import annotations` at the top of every module.
- Build config by constructing the dataclass directly — `ControlPlaneConfig()` for defaults,
  `ControlPlaneConfig(max_runtime_replicas=8)` to flip one field. `get_config()` reads
  `os.environ` and is `lru_cache`d, so calling it in a test leaks state; if you must, call
  `get_config.cache_clear()` in `setUp` and `tearDown` the way `test_config.py` does.
- Assert on the **routed-output envelopes** returned by services, not on side effects.
- Use `subTest` for table-driven cases.
- Freeze time by passing explicit `now` / `observed_at` arguments; the code takes them as
  parameters so tests do not need to patch `datetime`.
- A test that a fix is right should fail on the code before it: the covered-files fix
  (`test_a_committed_seal_announced_again_keeps_its_file`) was checked that way.

## 13.5 Local iteration loops

```bash
# control-plane logic
python3 -m unittest discover -s python/tests -p 'test_workflow.py' -v
# the write and commit path
for m in test_lakehouse_commits test_runtime_committer test_runtime_flow test_runtime_acks; do
  python3 -m unittest discover -s python/tests -p "$m.py"; done
# runtime templates and the chart
python3 -m unittest discover -s python/tests -p 'test_runtime_templates.py' -v
python3 -m unittest discover -s python/tests -p 'test_agent_manifests.py' -v
helm template midstream charts/midstream -n midstream -f charts/midstream/ci/test-values.yaml > /tmp/chart.yaml
```

### Inspecting a rendered runtime spec by hand

```python
from midstream.config import ControlPlaneConfig
from midstream.runtime.templates import (
    build_dedicated_runtime_template_input,
    iter_rendered_resources,
    render_dedicated_runtime_resources,
    spec_hash,
)

template_input = build_dedicated_runtime_template_input(
    materialization,                       # one table per source topic
    table_records=table_records,           # every table's record, as create_table stores them
    config=ControlPlaneConfig(),
)
rendered = render_dedicated_runtime_resources(template_input)
for resource in iter_rendered_resources(rendered):
    print(resource["kind"], resource["metadata"]["name"])
print(spec_hash(rendered))
```

`iter_rendered_resources` flattens the render in apply order (app → table → agents → tasks).
Rendering resolves the package pin, which raises `RuntimeError` for a local install (an editable
checkout, say; 10 §10.13): patch `installed_package_source` as the tests do.

## 13.6 Changes that require a matched update elsewhere

| If you change… | You must also update… |
|----------------|----------------------|
| A topic name | `config.py` defaults, the chart's templates (derived from the release), `test_config.py` |
| A resource name pattern | `runtime/naming.py`, `test_runtime_templates.py`, RBAC if the kind changes |
| A model field | `to_dict`/`from_dict`, `test_models.py`, any code reading the envelope |
| A workflow step name | `WORK_STEPS`, the step dispatcher, scheduler request ids, `test_workflow.py` |
| A phase or phase reason | `models/materialization.py`, `observation/mapping.py`, the rules in [08](./08-observation-and-status.md), `test_observation.py` |
| A runtime env var | `runtime_app/config.py`, the relevant `.j2` template, `test_runtime_config.py` |
| An agent or task of the runtime | `runtime/templates.py`, the `.j2` files, observation, `test_runtime_templates.py` |
| A control-plane agent | `charts/midstream/templates/agents/*.yaml`, `handlers/`, RBAC if needed |
| What the table holds: field ids, partitioning, a summary property | `lakehouse/`, the console's reader (`console/server/iceberg/`), the fixture table (`console_fixture.py`), [11](./11-iceberg-tables-and-storage.md) |
| A derived name's form | `charts/midstream/templates/_helpers.tpl` and the templates that use it |
| An overridable setting | its config field and env var, its entry in `settings/catalog.py` (runtime: also the live fields), `test_settings.py` (`.claude/skills/adding-settings/SKILL.md`) |
| An operation that reads configuration | bind the `settings` table in its template (`SettingsWiringTests`) |
| The topics or tables of a Kaspr app | plan one forced consumer-group rebalance per environment at deploy ([14 §14.3](./14-known-gaps-and-roadmap.md)) |
| A WebView | its first line authorizes the caller, it binds the `directory` table by its derived name (`AuthorizationWiringTests`, `ValuesWiringTests`) |
| A permission | `identity/permissions.py`, the views and console routes that check it, [18 §18.3](./18-identity-and-access.md) (`.claude/skills/adding-permissions/SKILL.md`) |

## 13.7 Debugging techniques

**Reproduce a control-plane decision without a cluster.** Every service function is pure with
respect to its inputs — build a `FakeTable`, seed it with envelopes (`storage.envelopes`), and call
the function.

**Reproduce a commit decision.** Build the table with `LakeFixture`, announce the seals in question
to `PendingFiles`, and call `plan_append` against the loaded table: every decision is a function of
the seal's offsets and the table's committed ones (10 §10.6).

**Read a lab table directly.** PyIceberg or DuckDB (11 §11.9); `iceberg_snapshots` and the snapshot
summaries' `materializer.offsets` show what each commit covered.

**Check a spec hash drift.** Render twice with the same inputs and compare `spec_hash`.

**Verify exactly once on a cluster**, for any change to acknowledgement, batching, the pipeline or
the committer — the E13 method (§13.8):

1. A topic whose contents are known exactly. Compute, per partition, the count and the sum,
   sum of squares, minimum and maximum of the offsets, and the tombstone count.
2. Materialize it from offset 0 with enough members, and disrupt it while it loads: delete the
   committer pod as a commit starts and as a compaction starts, rescale with files pending, and
   restart members.
3. Once every partition's committed offset is its end offset and nothing is pending, compute the
   same aggregates per `_kafka_partition` from the table. Equal counts and sums mean every offset
   is there exactly once; a duplicate or a gap changes the sum of squares.
4. List the current snapshot's live data files and check each exists in the bucket. A dangling
   reference is data the table claims and cannot serve — what run 1 found.

## 13.8 Verified on the lab: the E13 load test

Experiment E13 loaded the lab's `materializer-loadtest-orders`
topic — 32 partitions, 102,582,940 records after compaction, 1.4 KB JSON orders over two years of
Kafka timestamps in four schema shapes, with tombstones — from offset 0 into a fresh table, with 16
members and the default settings, and disrupted it throughout. It ran twice.

### Run 1: the data loss it found

`mat-a0690b63eb78`, about 03:04–04:55 UTC on 2026-10-19, 16 → 12 → 16 members. All 102,582,940 rows
were committed, but checking the table found **2,091 data files it referenced missing from the
bucket — 6,780,524 rows**. The committer had deleted files it decided `covered`: a member announces
an uploaded file again until it sees the commit that took it, so a file can reach the committer after
its own commit, and the committer deleted those files although the table held them. 72 of the 91
logged covered decisions came right after a committer restart (03:51), 19 in normal running
(03:59). Fixed in 223ae74: covered files are kept, and orphan cleanup removes what no snapshot
references (10 §10.6). The run is stopped and its table kept as evidence.

Disruptions in run 1, all survived without a commit failure: the committer pod deleted four times —
at arbitrary moments, between commits, as a commit started — a control-plane restart that rolled the
runtime, a 16 → 12 → 16 rescale with files pending, the committer deleted as a compaction started,
and **`SIGKILL`s**, from a privileged node-debug pod, of a member and of the committer as a commit
started. The committer's partition never moved to another member: static membership returned it to
the restarted pod each time.

What run 1 measured, on the code before e4e827f:

| | |
|---|---|
| Throughput | ~15,000 rows/s early, ~22,500 rows/s mid-run; 102.6 M rows in about 1 h 51 min |
| Commits (80, after 03:50) | p50 3,715 ms, p90 7,208 ms, p95 10,705 ms, max 22,889 ms; files per commit p50 136, p90 322, max 414; about 2,175 ms + 15.9 ms per file |
| Compaction | one partition per run, so a two-year backfill outran it: 14,816 files over 618 days at 55 M rows. This is what e4e827f changed (several past partitions a job, a job a tick while there is a backlog) |
| Snapshot expiry | 04:53:57: 60 expired, 99 kept |
| Members (no limits, 1,376 samples) | CPU mean 0.83, p90 1.5, max 2.0 cores; memory mean 413 MiB, p90 723 MiB, max 1.77 GiB |

### Run 2: on the fix

`mat-d89c130365f6` (`orders_e13b`), created 05:05:31 UTC on 223ae74, the same topic and settings.

| Time (UTC) | Disruption | What followed |
|---|---|---|
| 05:15:39 | committer pod deleted one second into a 14-file commit | new container at 05:16:00, first commit 05:16:21, 455 files pending again by 05:16:51: the members' re-announcements rebuilt its state within a minute |
| 05:35:04 | `replicas` 16 → 12 with 455 files pending | rollout; commits continued |
| 05:46:00 | `replicas` 12 → 16 with 229 files pending | `deploying` until 05:48:49; commits continued |
| 05:50:24 | committer pod deleted as a 59-partition compaction (623 files, 688 MB) started | the new committer started a 67-partition compaction at 05:52:05 — the interrupted one's days and newer data — and committed it at 05:55:15, 852 files → 89. The interrupted job had uploaded one output (15 MB) |
| 06:34:05 | committer pod deleted 60 s into a 33-partition compaction (908 files, 1.06 GB) | 33 outputs (459 MB) left in the bucket, referenced by nothing; compaction resumed under the new committer |

No `SIGKILL` this time: creating the privileged node-debug pod it needs was refused by the session's
permission policy, so an abrupt kill of a member or the committer on the fixed code is still to be
repeated.

All 102,582,940 records were in the table at 07:01:45, 1 h 56 min after the create, and checking
it found them **exactly once**: per partition, the count, the sum and the sum of squares of the
offsets, and the lowest and highest offset equal the topic's, on all 32 partitions — and **no data
file the table references was missing** (0 of 2,840). Every covered decision after a committer
restart kept its file.

| | Run 2 |
|---|---|
| Throughput | 22,518 rows/s over its best ten minutes, ~1,400 per member; 16,483 rows/s committed on average; partitions finished between 06:04 and 07:01 |
| Commits (96, 05:51–07:01) | p50 3.3 s, p90 8.7 s, p95 10.0 s, max 15.0 s; files per commit p50 71, p90 334, max 494; 2,003 ms + 18.5 ms per file; p95 6.4 s for commits of 50–150 files; no failure or retry |
| Schema evolution | 6.3 s and 5.8 s from a writer's request to the committed schema |
| Members (15, no limits) | CPU mean 0.74, p90 1.49, max 2.0 cores; memory mean 397 MiB, p90 627 MiB, max 1.97 GiB |
| Committer member | memory mean 1.25 GiB, p90 2.15 GiB, max 2.98 GiB: its worker ~0.6 GiB, a seal up to 1.36 GiB, a compaction up to 1.83 GiB |
| Console, from a laptop | Data tab warm: exact count 274 ms, first rows 459 ms; cold console 1.9 s and 3.2 s; a filter over the busiest day (564,837 rows) 0.63 s, over all time 14.4 s; `count(*)` over all time 21.1 s |

It also found four defects, each fixed and tested:

- **A null list came out empty** (6fed494): PyIceberg's projection, which the worker used, rebuilds a
  list of structs without its nulls (14 §14.5). Found because the tombstone count — rows whose
  payload is all null — came out low: compacted tombstones held `[]`.
- **Outputs stopped at the memory budget** (16f5535), 16–20 MB, below the small-file size, so a busy
  day could qualify again and again; `compaction_target_file_size_bytes` never reached the worker.
  The table ended with a median of 4 files a day; rerun, the fixed worker left 98 days with one
  file each.
- **One large input was decoded whole** (e7f19a2): the compaction child reached 1.83 GiB; 1.27 GiB
  at most after the fix.
- **Incomplete seals stayed pending** (7f001ae): 15 files after the load ended, until the committer
  restarted (10 §10.6).

The interrupted compactions' outputs stayed in the bucket until orphan cleanup took them: with
`orphan_min_age_hours` lowered to 1 it deleted R2-C's at 08:08, 1,000 files a pass, and after
10,000 deletions every live file was still there.

Multi-partition compaction kept a backfill's file count bounded: the first job, 05:17:20–05:21:21,
took 1,596 files (1.05 GB) across 47 days into 89, and the table never held more than 5,997 files,
where run 1 had reached 14,816 at 55 M rows.

## 13.8a Verified on the lab: several topics, one runtime (2.0)

On 2026-10-20 the lab ran control plane 2.0.0 (51ca6ab) after the three 1.0 materializations were
deleted. `shop` read `mtm-orders` (4 partitions), `mtm-payments` (2) and `mtm.shipments.v1` (3) with
2 replicas; `audit` read `mtm-orders` too, with its own runtime and consumer group. Five records per
topic were produced first, the rest once both ran: 120,005, 80,005 and 60,005 records, the second
half of the payments with a new `currency` field.

| Check | Observed |
|---|---|
| Create | Tables `mtm_orders`, `mtm_payments`, `mtm_shipments_v1` (and `audit`'s `mtm_orders`); discovery per topic, `create_table` one table at a time, one `apply_runtime` for all three, `running` in under three minutes |
| Exactly once | Through the REST catalog with DuckDB (`lake.shop.mtm_orders`, …, `lake.audit.mtm_orders`), every partition of every table holds each offset once: count = distinct offsets = the topic's end offset, min 0, max end − 1, the sum of offsets `n(n−1)/2` |
| Schemas | `mtm_payments` evolved to schema 1 (`currency`); `mtm_orders` and `mtm_shipments_v1` stayed at schema 0. An unknown table answers `404` naming the three |
| Bounded output | From each member's own log (`seal_ms` / `upload_ms` intervals): `shop`'s two members sealed 26 and 23 batches of all three tables, never more than one seal (`seal_worker_count`) or one upload at a time — the same as `audit`'s member with one table |
| Lag per topic | While each partition's last batch waited for the open-file ceiling: 5,005, 5,005 and 10,005, exactly the rows not yet committed; 0 for every topic once they were; both groups on `mtm-orders` caught up independently |
| Shard routing | In the `state` changelog every `shop` record — 97 status writes, lag records among them — is in partition 2, its home; none in the `__system__` partition (06 §6.4) |
| Catalog | Namespaces `audit` and `shop`; `shop` lists its three tables |
| Maintenance | `expire_snapshots` on `mtm_payments` alone ran on the committer a second after the request; a table it does not have is a `400` (before the maintenance app, §13.8b) |
| Console | 21 checks in each theme (console 14 §14.4) |
| Delete | `audit` left the list 2.5 minutes after `DELETE`: runtime, Kafka topics and group, and every object of its table gone, `shop`'s untouched; all ten of its keys in the `state` changelog tombstoned, none left live in any partition |

The tail of each partition, below `min_rows_per_file`, sealed at `max_open_file_age_ceiling_seconds`
(600 s) as designed: about ten minutes after the last record, every table held all of its topic.

## 13.8b Verified on the lab: changing topics, and the maintenance app (2.0)

On 2026-10-20, with the control plane and runtimes at 0d27b75 and the maintenance app at 59550ba,
on `shop` (2 replicas): every change through the console's Edit dialog and the API, checked in the
API, the console (Playwright) and SQL.

| Check | Observed |
|---|---|
| Maintenance app | A requested compaction took a day's 9 files to 1 in 6.6 s; the Table tab showed the job running, then its result. **Run now** with no table named requested the job on every table. Orphan cleanup ran on a stopped materialization's tables. A changed maintenance setting reached the app with the next tick |
| Adding a topic | `mtm-refunds` sampled at 0.2 s, the runtime held at zero from 3 s, its group empty at 129 s, `running` at 314 s; the new `mtm_refunds` then held all 3,005 records |
| Keeping a table | `mtm_refunds` kept read-only: still listed, `SELECT count(*) FROM shop.mtm_refunds` read 3,005, and **Run now** of snapshot expiry ran on it |
| Resuming a kept table | `mtm_payments` removed with keep, 1,000 records produced, its topic added back: 80,005 + 1,000 = 81,005 rows, each offset once |
| Dropping | `mtm_shipments_v1` dropped with its topic removed: its files gone 1.5 s after the drain ended; a kept table dropped without a change of topics, with no restart |
| Exactly once at the end | per partition, count = distinct offsets = the log end offset: `mtm_orders` 120,125, `mtm_payments` 81,005, `mtm_refunds` 3,005 |

Two defects were found and fixed on the way:

- **A kept table dropped during its own drain** was dropped at once, while members that had not yet
  left the group could still write it. The drop now waits for the drain and runs from the apply
  that ends it (696c354, `test_a_table_kept_by_a_change_still_draining_is_dropped_once_the_drain_is_over`).
- **A table re-created under an old name skipped its records.** `mtm_refunds`, dropped and added
  back, received none of its 3,005 records: the runtime's state table still held the dropped table's
  committed offsets under that name, so every record looked written already and was released
  unwritten. State is now stamped
  with the table's uuid and another incarnation's ignored (0d27b75, `RecreatedTableTests`, which fail
  without the fix); on the lab the table was dropped and added again, and held all 3,005.

## 13.8c Verified on the lab: datasets and per-dataset access (3.0)

On 2026-10-22, with the control plane and runtimes at 4cdb65b, on a clean slate (the 2.0 `shop` and
its tables removed, the `directory` table new): datasets `sales` and `finance`, made by an Editor who
owned them; `finance` with All Users removed and one person granted Dataset Reader, `sales` with one
Editor granted Dataset Writer. Materializations "Ledger" (`finance.invoices` → `finance.finance_invoices`
and `sales.refunds` → `sales.refunds`, two replicas) and "Orders" (`sales.orders` → `sales.orders`),
3,005 orders, 3,005 invoices and 405 refunds. Checked through the API, DuckDB on the REST catalog,
and the console (Playwright, as four people).

| Check | Observed |
|---|---|
| Names | `Sales`, `main`, `select` and `sales-eu` refused as dataset names; `Orders2` refused as a table name with `orders2` suggested; a taken `sales.refunds` refused at once (`409 table_name_taken`, `field: tables.0.name`) |
| The race | Two creates claiming `sales.returns` at the same moment: one claim applied; the other refused `table_name_taken` naming the table and the holder, its materialization `deleted` / `table_name_taken`, `Not created: sales.returns is taken…` |
| Delete and create again | The winner deleted: the name free and gone from the catalog at once; a new materialization claimed it, and `lake.sales.returns` resolved to the new table (new UUID, its own location) |
| Permission matrix | 24 API checks: schemas refused (`403`, naming `finance`) to those without a role on `finance`; `readable`/`queryable` per table per caller; a dataset table's location shown to readers only; a Writer refused a table in `finance`, refused removing or deleting Ledger's `finance` table, allowed to relabel |
| REST catalog | A Reader's token listed `finance` and `sales`; one limited to `sales` listed `sales` only, and `finance.finance_invoices` answered `404`; an Outsider's token did not list `finance`; `catalog.write` refused to a Writer's token |
| Joining datasets | DuckDB 2.0 through the catalog: `lake.sales.orders JOIN lake.finance.finance_invoices`, two materializations, 3,005 rows, the expected sum; in the console, `sales.refunds JOIN finance.finance_invoices` in one statement |
| Exactly once | Through the catalog, per partition of each of Ledger's tables (two datasets) and Orders': count = distinct offsets = the topic's end offset, min 0, max end − 1, sum `n(n−1)/2`. Later `sales.order_refunds` and the re-created `sales.returns`, both of `sales.refunds`: 405 rows each, 140/138/127 per partition, every offset once, the same 405 in both |
| Console | 36 browser checks: a locked dataset listed and marked, its page saying so without an Access tab; a locked table marked on the detail page, and neither selectable in the Schema and Data tabs nor readable through the console's API (`403`, naming the dataset); the Access tab with grants, candidates and the storage note; the create dialog starting a row in the only writable dataset, saying a taken name and an upper-case one as typed, `finance` locked in the picker; the edit dialog adding `sales.order_refunds`, closed once its name was claimed (~0.1 s) |
| Change of topics | Orders gained `sales.order_refunds`: claim, answer and discovery within milliseconds, then the usual drain |

One defect was found and fixed on the way: a status event from a runtime the control plane holds no
record of — the 2.0 `shop`, still running — crashed the `runtime-status` agent, which then read the
same event on every restart and applied no runtime's status. Such an event is now dropped and logged
once (4cdb65b, `test_a_status_from_a_materialization_the_control_plane_does_not_hold_is_dropped`).

## 13.8d Verified on the lab: month partitions, the deployment's place and scale, dates from the data (4.0)

On 2026-10-23, with the control plane and maintenance app at 396027a, and the three 3.x
materializations (Ledger, Orders, *Returns, third*) re-applied by the upgrade. A topic made for it,
`history.orders`: three partitions of 2,000 records, produced through the cluster's external
listener with Kafka timestamps spread evenly from 2025-01-01 to 2026-10-19, in four shapes —
`device`, `items[].tax` and `promo_code` from 2025-04-15 (1,270 records), `customer.phone` an
object, `total` a double and `coupon` from 2025-09-01 (1,479), `customer.email` and `promo_code`
gone and `customer.email_hash` and `fraud` from 2026-02-10 (2,301), after 950 in the first.
Materializations *History by day* (`history.orders_by_day`, `day`) and *History by month*
(`history.orders_by_month`, `month`) read it, with the discovery sample holding the first and last
shapes in schema 0, and `promo_code` adding schema 1.

| Check | Observed |
|---|---|
| Create | `destination_uri`, `namespace`, `runtime_name` and `storage_size` each refused `400`, naming the deployment's value; with the destination override set to none, `409 destination_not_configured`, and the setting reset; `POST /topics/analyze` recommending replicas alone |
| Runtimes | Both in `midstream` as `<release>-mat-<id>`, each member's volume `128Mi` (the default then), 72 KiB used once committed; the 3.x runtimes kept `1Gi`, and the control plane and maintenance app their own `1Gi` |
| The 64Mi default | Every runtime re-applied with the write buffer (RocksDB's options file: `write_buffer_size=4194304`, `max_write_buffer_number=2`). A third materialization of `history.orders`: a `64Mi` volume (55 MiB usable), its store 67 KiB and its write-ahead log 10.7 KB after the backfill, every offset once through the catalog; then deleted. The first attempt, a KasprTable `options`, was stored empty: kaspr-operator's CRD prunes it (14 §14.2) |
| Month partitions | 22 partitions `2025-01` … `2026-10`, three files each (one per Kafka partition); PyIceberg reads the spec as `month` and each partition value as months since 1970, and every file's timestamp bounds lie in its partition |
| Exactly once | DuckDB 2.0 through the REST catalog, with a token limited to `history`: per Kafka partition of both tables, offsets 0–1,999 once each (count, distinct, sum, sum of squares); rows per day (657 days) and per month (22) as the producer's timestamps give; each shape's fields as counted by era |
| Dates from the data | The day table's first values on 2025-04-15, 2025-09-01 and 2026-02-10 and last values on 2025-08-31 and 2026-02-09 — the eras to the day, read from the manifests alone; the month table's as those months |
| Compaction | With `compaction_min_input_files` at 2 for the test: the day table's 657 partitions in one job, 1,971 files to 657 in 4 minutes; the month table's 21 past months, 63 files to 21, while the open month's three files waited until they were an hour old and went into the next pass, 68 minutes after their commit; both exactly once after, every output in its partition |

No defect in the control plane or the maintenance app; the console's three are in console 14 §14.5
(24–26).

## 13.8e Verified on the lab: the live tail (4.1)

On 2026-10-23 and 24, with the control plane, runtimes, maintenance app and tail app at
`feature/live-tail` (each deploy noted below), Kaspr 0.11.30 and kaspr-operator 0.19.7, the console
server on a laptop through the lab's HTTPRoute (`/tail/` to the tail app), Playwright (Chromium) in
light and dark. The lab materializer was installed afresh for it (12 §12.1). Topics made for it:
`fresh.orders` (3 partitions; probe records numbered and timed by a producer through the external
listener), `scale.t01`–`scale.t50` (3 partitions, 5 records a second each), `backlog.orders`.

| Step | Observed |
|---|---|
| 0. Routing | Ten requests through the Service, all answered by the owner, six of them forwarded with the whole body (5,803 bytes) — after the fix for a forwarder that passed on only the first chunk (737 of 5,797 bytes; `tail_app/http.py`, 3a75976). The group stable at 32 and 32 partitions; no `tail_routing_mismatch`. The first request activated the table in 773 ms; answers after it 22–35 ms |
| 1–2. Freshness (R1) | Produce → visible, 300 probes: the Data tab in Live p50 0.93 s, p95 1.63 s, at most 1.95 s; a statement run as each record arrived p50 0.70 s, p95 1.11 s. Live followed both commits in the window |
| 2. Exactly once (R2) | The poller (`SELECT … GROUP BY _kafka_partition`, a statement over the snapshot and its tail, once a second) saw every probe once at each of 343 polls across 2 commits, and at every poll of every later step |
| 3–5. Disruptions | Exactly once at 768 polls across a compaction (a replace commit), a runtime pod restart, the owner's restart, and the tail app scaled 2 → 3 → 1 → 2. Each member restart left its tables' newest records out for 30–60 s; the console said *unavailable*, *moving between members*, *warming up*, then *Live* |
| 6. Schema change (R3) | Records with a new key and a type fork: `settling` 30 → 0 within 3 s of schema 1; the tail's 30 rows against the committed rows, every column but `_materialized_at`: 0 cells differ |
| 7. The bound | At 1,000 rows: a burst of 1,800 paused the table at 1,221 (one fetch past the bound, 14 §14.16) and it resumed; 3,000 more backlogged it, the peek was offered, and it was live again once the runtime caught up. Exactly once at 418 polls across 4 commits |
| 8. Who may read (R4) | A user who may query only another dataset: `403` naming `data.query` on `fresh` through either member and for the peek, and no Data tab; her token limited to the other dataset `403`. DuckDB on the REST catalog: the console's count less the catalog's = 120, the tail's rows at that moment |
| 9. Switches | `live_tail` off: the Live toggle gone in 2 s, the table dropped by the tail app at the next tick (27 s); on: toggle back in 1 s, table live in 4 s. `live_tail_enabled` off (set through the API, not the console): no ticks, toggles gone in 32 s; on: ticks within 25 s, toggles back in 32 s. Stop: toggle gone in 2 s, table dropped in 24 s; resume: toggle back in 1 s, table live in 4 s. The first run found an open tab neither losing the toggle nor picking the tail back up after the switch returned (44b64cb) |
| 10. Fifty tables | 50 topics in three materializations (20, 20, 10 topics: three runtimes rather than fifty, for the lab's sake), every table tailed: 32 on one member, 18 on the other. Activation p50 0.84 s, p95 0.99 s; no added `lag_ms` on the member's other tables at 48 activations. 0.044 and 0.032 cores, 194 and 175 MiB — after the fix this step found: before it, 0.72 and 0.44 cores (d5372d5, 19 §19.18). With one topic at 1,000 records a second more: 0.068 cores, 225 MiB. A topic grown from 3 to 6 partitions: tailed 8 s after, its new records read 2 s after they were produced; the runtime committed them too |
| 11. A backlog | A topic of 600,000 records, `backlog.orders`, materialized with live tail on — not the load-test topic, whose exact counts other checks rely on, and a backfill of minutes rather than hours. On the Data tab's first request the table was `backlogged`, 600,000 behind, and read and held nothing (no next offsets, `ingested` 0) while the console said *The runtime is 600,000 records behind; the live tail starts once it catches up*, counting down (438,842, 367,186 …) as the runtime committed, with **Peek** open. A probe produced at the head was in no statement's count (0, the tail backlogged). Within 75 % of the bound the table caught up by itself and was live 2 minutes after the first request, every one of the 600,001 records once at every poll. The run found the peek closing itself at the first commit after it opened (console 14 §14.5, 30); on the fix, a peek held open through four runs showed each of 88 probes produced meanwhile, p50 0.9 s after it was produced |
| 12. Bursts | At the default bound, 50,000 records in 10 s: the table read at its ceiling of 5,000 a second, partitions paused, *the newest records are not shown yet*, then live holding all 50,000; the burst's records carried a field the table did not have yet, so the console also counted them *waiting for a schema change* until the runtime committed it. Then 1,000,000 records in 38 s: backlogged 20 s later, its buffer dropped, *The runtime is 517,755 records behind; the live tail starts once it catches up* with **Peek**; once the runtime was within 75 % of the bound, catching up, *Live tail is 241 s behind*, and live with 143,122 rows, 4 minutes after the burst. Exactly once at all 666 polls — each partition's rows contiguous from offset 0, none twice — up to 1,064,356 rows across 18 snapshots. No other table of that member was receiving records, so the isolation of one table's burst from the others rests on `test_tail_app` (a table above the ceiling read at it, the others keeping up) |
| 13. Parking | Unwatched, the table parked at 600–611 s and its member's consumer let its 3 partitions go; the Data tab's first run brought it back, *Live · through now* with no *warming up*, 3–4 s after the page opened. With `live_tail_idle_seconds` 0 it was still live 803 s after its last request |
| 14. Broker restart | With the owner's approval: broker 1, which leads `fresh.orders`' partition 0 (every topic has one replica on the lab), rolled by Strimzi while the probe ran, away 28 s. The tail's `lag_ms` rose to 13 s and returned to 0 by itself, no member restarted, no `tail_routing_mismatch`; exactly once at all 283 polls across 17 commits; the console showed the stalled probes as soon as the producer, which waits for each acknowledgement, could send them |
| 15. Control plane restart | Its pod deleted while the probe, a Live tab and the poller ran, away for about 35 s: the tail app kept serving on the sessions it had checked; a parked table asked for meanwhile seeded from GCS (`seed_source: gcs`) and was live in 2 s; ticks went on each minute; exactly once at every poll. The first run found every console user signed out: the restarted control plane answered from a directory it had not read back yet ("invalid session"), and the console cleared its cookie. Fixed in f382b0a (`503 starting` until the tables are read back, 18 §18.7); on the fix, the Live tab followed through the restart and resumed by itself |
| 16. A member killed mid-burst | The owner deleted 20 s into the 50,000: back 19 s later, it seeded the table again from the committed offsets and read the window again — *catching up*, paused at the ceiling, then *live* 47 s after the kill; the console said *unavailable*, *moving between members*, *warming up*, *the newest 13,380 records are not shown yet*, then *Live*, in that order. No record twice across the kill (step 12's polls) |
| 17. Console restart | Two Live tabs open, the console stopped for 4 s: each tab ran its view again and followed the tail 6 s after the stop; all 230 probes of the window once in both, the three produced while it was down within 7.5 s; nothing at warning level in the restarted console or the tail app |
| 18. An hour's soak | The probe at one record a second with two Live tabs: 3,540 probes each, p50 1.13 s, p95 1.9 s, p99 2.7 s (5 of 7,080 over 5 s, at most 6.7 s); exactly once at all 2,827 polls across 102 commits; the 50 tables of step 10 watched in rotation — 180 visits, parked tables among them, every one *Live* p50 1.2 s and p95 2.0 s after the page opened. After the first ten minutes each member's RSS stayed within 5.7 % and 2.7 % of its value then (212–225 and 184–189 MiB), at 0.042 and 0.032 cores; `tail_served` 9,779 answers, p50 2 ms, p95 48 ms, p99 525 ms. No browser errors. It also found *Newer data available* on screen 57 % of the time Live followed a table (console 14 §14.5, 29) |
| 19. Security probes | Through the HTTPRoute: no console header or a wrong one `403 console_only`; no bearer `401 sign_in_required`; an ended session `401 session_ended`; no `data.query` on the dataset, or a token limited to another, `403` naming the permission and dataset, through either member; a forged `X-Tail-Forwarded-By` `403` at the owner and `409 tail_rebalancing` at the other member; the service token `403` for rows and the peek; a peek of a million rows answered with 1,000 (`live_tail_peek_max_rows`), the console's own `400`; a bad `after` `400`; `..`, `%2E%2E`, `../..`, `;` and a NUL in a name refused, by the proxy or `400`. No row's content in the tail app's 2,177 log lines of six hours |
| 13.6 measurements | In a tail pod: ingest 101 µs a record, 1.06 bytes per raw byte, serve 10,000 rows 28 ms and 100,000 rows 193 ms p95; the members view (`DescribeGroups` and decode) p95 1.3 ms; a peek of 200 rows p95 59 ms (19 §19.15) |
| 20. The settings lineage | Deployed over the lab's settings, at revision 17 with no lineage (3926620, helm revision 3): at start `settings-bootstrap` sent its request, and the agent logged `Runtime settings begin lineage 20261024T110007Z-1b0bf109 at revision 17` and delivered revision 17 with that lineage to every runtime — the topic's newest message, after the reinstalled control plane's own deliveries without one. `GET /settings/runtime` reported the lineage, the revision and who last changed it as before. A second restart began no lineage and sent nothing. Then, at 633be69, a runtime following it (materialization `lineage_check` on a 3-partition topic, live tail off): created, it read the delivery topic's 137 messages — an earlier installation's revisions up to 14 and this one's up to 17, none with a lineage, then the lineage at 17 — and applied 3–14, 15–17 and the lineage's 17 (`runtime_settings_applied` with `settings_lineage`), its heartbeat reporting the lineage at 17 and the rollout `current`. A delivery for it alone with no lineage, revision 99 and `max_rows_per_file` 123 — what an earlier installation leaves — was consumed (its group at the topic's end) and ignored. A change through the API (revision 18) was applied the second it was published, after that revision 99; a restarted member reported the lineage at 18 from its state table, with no delivery; the reset (19) reached it too. A later lineage sent to it alone was taken, and the rollout said `pending` with no revision, the console's panel *Applying* (console 14 §14.4). Everything made for it deleted |
| 21. Deleted topics stay deleted | The cleanup after step 19 found the 52 deleted test topics back, empty, with one partition each, all made within 60 ms at 06:03:01: the broker creates topics on demand (`auto.create.topics.enable=true`), and a client still named them in its metadata refresh. With aiokafka 0.14 against the lab's broker: a consumer that looked up a new topic's offsets tracked it, and after the topic's deletion its refresh made it again with one partition; with `track_no_topics` after the lookup, the refresh made nothing. aiokafka's describe of a missing topic by name made it; the control plane's describe answered "topic does not exist" and made nothing |

## 13.8f Verified on the lab: vertical pod autoscaling (4.2)

On 2026-10-24, with the control plane, maintenance and tail apps and the runtimes at
`feature/vertical-pod-autoscaling` (63bb576), kaspr-operator 0.19.7, VPA 1.8.0 (its updater run with
`--in-place-skip-disruption-budget=true`) on k3s 1.34, and the console on a laptop with Playwright
(Chromium) in light and dark. The lab's runtime spec, which had no resources, was given the shipped
ones with the VPA. Made for it: the topic `vpa-check.orders` (3 partitions) and the materialization
*VPA check* (two members, its table `test.vpa_check_orders`).

| Step | Observed |
|---|---|
| A. The release without VPAs | Helm revision 4, the three apps' `verticalPodAutoscaler` left out: their pods rolled once, for the new package |
| B1. VPAs on | Helm revision 5: within 10 s a VPA each for the control plane, maintenance and tail apps, targeting its KasprApp, and `VerticalPodAutoscalerReady=True` (`Configured`). The StatefulSets' generations and revisions, the pods' UIDs, containers and restart counts unchanged. A minute later the recommendations: control plane 671m (lower bound 586m), maintenance and tail 100m, the floor |
| B1. Resized in place | Two minutes after, the updater resized all four pods in place: control plane 500m → 671m, maintenance 500m → 100m, both tail members 1 → 100m (`InPlaceResizedByVPA`, `ResizeCompleted`). Same containers, no restarts; `cpu.weight` 74 and 17 in their cgroups, `cpu.max` (the limits) unchanged; the API kept answering |
| B2. The runtime spec | Helm revision 6, the runtime spec given requests of 0.5 CPU / 1 GiB, limits of 2 CPU / 1 GiB, and the VPA: only the control plane restarted, its environment having changed |
| C. A runtime | *VPA check*'s runtime got its VPA, the floor recommended within a minute. Its second member, created after that, was admitted at 100m (`vpaUpdates`); the first was resized in place 500m → 100m as the materialization came up `running` |
| C. A burst | 300,000 records produced in 3 s: a member at 1.34 cores at its peak (the worker and a seal, under the 2-core limit), all committed within 4 minutes in 51 files. The recommendation followed, 100m → 511m, and fell back as the runtime idled. No restarts |
| C. Exactly once | Through the console: 300,012 rows, as many distinct order ids (1 to 300,012) and as many distinct partition and offset pairs; the committed offsets one below the end offsets (99,972, 99,999, 100,038) |
| D1. A change to the VPA alone | Helm revision 7, the runtime spec's floor 100m → 400m: the control plane restarted and re-applied the runtime 40 s later (generation 2), the VPA's floor followed, and 10 s later both members were resized in place 100m → 400m. The runtime's StatefulSet and pods unchanged |
| D2. And back | Helm revision 8, the floor back to 100m: re-applied (generation 3); the requests stayed at 400m, inside the new range, where the updater leaves them for 12 hours; no restarts |
| Settings page | Deployment › Runtimes: *Resources per replica* `requests: cpu 0.5, memory 1Gi; limits: cpu 2, memory 1Gi`, then *Vertical autoscaling* with its bounds and update mode, in both themes |
| Admission | The control plane's, maintenance app's and tail app's new pods started from their manifests' requests: the lab's VPA admission controller had looked their pods up while the KasprApp CRD had no scale subresource (2026-10-21 and 22) and keeps every lookup (02 §2.10c). A server dry run of such a pod came back unpatched, while the new runtime's pods were patched. The updater resized each in place within a minute of its start |

Not verified: the fallback to evicting a pod whose node cannot fit its resize; an updater without
`--in-place-skip-disruption-budget`, which leaves single-replica apps to their next pod; the 12-hour
rule for a request inside the range.

## 13.8g Verified on the lab: a live tail for every materialization (4.3)

On 2026-10-24, with the control plane, maintenance app and tail app at d1592b4 (helm revision 9), the
console on a laptop through the lab's HTTPRoute, Playwright (Chromium) in light and dark.

| Check | Observed |
|---|---|
| The API | `GET /system/settings`: `live_tail_enabled: true` and, for 4.1 consoles, `live_tail_default: true`; the settings catalog without `live_tail_default`. A create with `live_tail: false` refused, `400`, `field: "live_tail"`, naming `live_tail_enabled`; without it accepted, answering `live_tail: true`. A `PATCH` with `live_tail: false` refused the same way; `live_tail: true` with a new name accepted, the name changed and `spec_version` still 1. The detail and the list say `live_tail: true` |
| Tailed unasked | A materialization created without a word of live tail (`everytail.orders`, 3 partitions): its Data tab *Live · through now*, every record from the tail before the first commit (*0 files and the live tail*); a record produced while the tab watched showed in 6.1 s and 2.9 s, the console producer's start in a broker pod included |
| The console | No *Live tail* option in the create dialog's advanced settings, no switch in the edit dialog — *Change its name, the topics it reads and its scale* — and no *Live tail* row on the Overview; no page errors, light and dark |

The deployment-wide switch was left alone on the shared lab: its behaviour is unchanged, and the unit
suite covers it.

## 13.8h Verified on the lab: restart health, the bounded seal, autoscaling (4.4)

On 2026-10-25, the control plane, maintenance and tail apps at `feature/runtime-autoscaling` (helm
revision 2 of the release in namespace `default`), the runtimes at each commit as the control plane
installed it, and the console on a laptop through the lab's HTTPRoute, Playwright (Chromium) in light
and dark. The user's materialization of `materializer-loadtest-orders` (ten members, its table
`test.materializer_loadtest_orders`) was 5.5 million records behind after a restart, its members
killed for memory every few minutes while the status said `running` and healthy. Made for the test:
the dataset `autoscale_test` and *Autoscale test* (`mat-f4c07620bc08`), one replica, reading the
whole load-test topic (111,031,820 records, 32 partitions, two years by month).

| Step | Observed |
|---|---|
| Restart health (1fcfd46) | The next observation after the deploy: `degraded` / `runtime_members_restarting`, *8 of 10 members restarted in the last 30 minutes: out of memory.*; the console's header **Degraded** and **Unhealthy** with that tooltip, the fleet's attention list *Runtime members restarting*; the lab's other materialization still healthy. Once the seal fix had replaced the pods, the restart counts began again at zero and the status went back to `running` |
| The bounded seal (1a54cd1) | The same backfill on the fix: no member killed again, ten ready throughout, `kubectl top` at most 472 MiB a member (1 GiB limit), 5.07 million records drained in 12 minutes and the lag 0 at 15:39 |
| Scaled up on a backlog | *Autoscale test* `running` at 15:52; at the 15:53 measurement, its third, *110,869,158 records behind, about 22 h 43 min to catch up with 1 member* → 16 members, the cap (its 32 partitions keep 32 busy). The StatefulSet asked for 5, 10, then 16; all ready at 15:57, the group `Stable` with 16 members, `running` at 15:58. The lag then fell by up to 3 million records a minute |
| The Overview and dialogs | *Scaled up to 16 members 13 minutes ago: 110,869,158 records behind, about 22 h 43 min to catch up with 1 member. Back to 1 once caught up.*; Specification *Replicas 1 · 16 while catching up*, *Autoscaling On · up to 16*. The edit dialog's switch on, its hint *Running 16 members while it catches up; back to 1 once caught up.*, unchecked *…Turned off, it goes back to 1 at once.* and **Save** enabled; the create dialog's switch on under *Partitioning, replicas, autoscaling, labels, tombstones*. No page errors, light and dark |
| Off for the materialization | Saved in the edit dialog at 16:06:16 (toast *autoscaling off*): the StatefulSet at 1 within 10 s; the detail `autoscaling: false`, the record forgotten, `spec_version` still 1 |
| On again | Saved at 16:07:45: a new record from 16:08. With one member after the rebalance nothing was committed for three minutes, so at 16:10 *98,495,063 records behind and not catching up with 1 member* → 2 (doubled). After the cooldown, at 16:15, *97,805,796 records behind, about 9 h 28 min to catch up with 2 members* → 16; all ready at 16:18 |
| Memory, 16 members | Each member's anonymous memory sampled every second from 16:37 to 16:47, mid-backfill: peaks of 550–773 MiB, the most on the member whose worker was largest (its peak RSS 516 MiB); page cache from the spools and Parquet files took the rest up to the 1 GiB limit, where the kernel reclaimed it (`memory.events` `max` 45 to 1,922 times by 16:36), and `oom_kill` stayed 0 on every member |
| Caught up, and back | At 16 members the lag fell by 1 to 1.6 million records a minute: under the floor at 17:31 (87,565), 0 at 17:32, and at 17:33, the third measurement under it, the runtime went back to its one member (`changed_at` 17:33, `replicas` null); `running` and healthy within a minute, and the Overview without its *Scaled up* line, *Replicas 1*. Its table: 102,033,103 rows — the 102,582,940 of the user's table of the same topic, which keeps tombstones as rows, less exactly the topic's 549,837 tombstones. Nothing lost or written twice through five changes of members, 1 → 16 → 1 → 2 → 16 → 1 |

The deployment-wide switch was left alone on the shared lab; the unit suite covers it.

## 13.8i Verified on the lab: the first installation from the published artifacts (1.0.0-rc.1)

On 2026-10-26 the lab was installed from scratch by following `docs/install.md`: the chart
`oci://ghcr.io/vmirz/charts/midstream` 1.0.0-rc.1 with `package.source: index`, so every app and
runtime installed `midstream-platform==1.0.0-rc.1` from PyPI, and the console image
`ghcr.io/vmirz/midstream-console:1.0.0-rc.1`, as the Helm release `midstream` in a new namespace
`midstream` (kaspr-operator 0.19.8, Kaspr 0.11.31, two brokers with single-replica topics, a
credential-free listener). The lab's two brokers needed Kaspr's `topicReplicationFactor` at 1, which
rc.1 could set only through each app's and the runtime spec's `extraEnv` (`K_TOPIC_REPLICATION_FACTOR`);
`kaspr.config` is the value for it since rc.2.

| Step | Observed |
|---|---|
| Secrets and install | The three Secrets of §2, `helm install` from the OCI chart; the control plane, maintenance and tail apps `Ready` and the console available two minutes after the install (each pod's package install about 40 s); the 71 WebViews attached over about six minutes, one every 5 s. |
| The topics | 23 topics under the release name, every one with replication factor 1; the state changelog with 3 partitions. |
| §5 | `GET /system/version` → `1.0.0rc1`; `POST /auth/login` as the bootstrap administrator → `password_change_required: true`. |
| §6 in the console | Through the chart's HTTPRoute and a port-forward (Playwright, Chromium): sign-in, *Choose a new password*, the dataset `sales`, the materialization *Shop* on the lab's `orders` topic (console 14 §14.4). |
| An empty source topic | `orders` held no records any more (its beginning offsets equal its end offsets): *Shop* reached `running` / `waiting_for_source_records` with a table of the seven Kafka columns and no rows, which is what an empty topic yields. |
| §6 by the API | `POST /materializations` with `{"name":"Smoke","tables":[{"source_topic":"midstream-smoke-orders","dataset":"sales"}]}` on a scratch topic of six JSON orders: `schema_discovered` within a second, the runtime `midstream-mat-ab6b417c59ab` (one member, its VerticalPodAutoscaler provided) `running` after two minutes, schema 0 with nine inferred fields (`items[].sku` and `items[].qty` among them). |
| The live tail | The Data tab showed the six records, newest first, about two minutes after the create and before any commit; the tail app forwarded the console's requests from the member that answered to the table's owner (`tail_forwarded`, `tail_served`). |
| The first commit | Ten minutes after the records were read, at the seal's age ceiling (`max_open_file_age_ceiling_seconds`, 600 s): one batch sealed (6 rows, 7,543 bytes of Parquet), announced, and committed by the committer in 1.2 s as snapshot 1 with offsets `p0: 5`; the table under `<destination>/midstream/mat-ab6b417c59ab/midstream_smoke_orders/` held the data file, two metadata files, the manifest list, the manifest, `catalog-pointer.json` and `version-hint.text`. The console's Overview then read *6 rows committed*, *complete through* the records' time, *7.5 KB, 1 data file, 1 snapshot*; the maintenance app's orphan cleanup had already run on the table (3 listed, 0 deleted). |

One finding is Kaspr's, not Midstream's: the control plane's first scheduled request, 28 minutes
after its start, crashed the Kaspr scheduler's `distribute` actor with `NotReady("Producer metadata
for topic 'midstream-control-plane-app-schedule-actions' not available yet")`. Its supervisor
restarted it at once, but that request (*Shop*'s first delayed observation) was never dispatched:
`schedule-requests` held four requests and `schedule-actions` three actions. The sweeps brought the
materialization to `running` instead, and every later request dispatched (14 §14.3).

## 13.8j Verified on the lab: domains and the shared console

On 2026-10-26 two domains ran on the lab from this chart — Domain A with the shared console, Domain B
reached only through its API route — after a clean uninstall of 1.0.0-rc.2, whose materializations,
topics, consumer groups and bucket prefix were deleted first. What was checked, every fault injected,
the five defects found and fixed, and the performance against rc.2 on the 111-million-record load-test
topic are in handbook 20 §20.15; the browser record is console 14 §14.4. In short: both backfills
committed exactly 102,033,103 rows, in 89.4 and 88.9 minutes; the console's requests answered within a
millisecond or two of rc.2's; nothing on the data path changed between the builds.

## 13.9 Pre-finish checklist

Before considering any change complete:

```bash
python3 -m unittest discover -s python/tests -p 'test_*.py'   # must be OK
python3 -m compileall -q python/src                            # must be silent
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml   # if the chart was touched
git status --short                                             # no stray files, no secrets
```

If `console/` changed: `cd console && npm test && npm run typecheck && npm run build`, and check the
change in a browser in both themes ([console 14](../../console/docs/14-testing-and-verification.md)).

A values file added under `charts/midstream/ci/` is rendered by `ValuesWiringTests` and
`VerticalPodAutoscalingTests` too. Keep an environment's real values out of the repository.

The backpressure tests log `Pausing N source partition(s) …` warnings and some runtime tests log
protocol events; those lines are expected output, not failures.
