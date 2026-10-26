# 16 — Versioning

## 16.1 What is versioned

Three things carry a version, and they are deployed independently — so at any moment they can
legitimately differ:

| Component | Declared in | Reported by |
|-----------|-------------|-------------|
| Control plane | `python/pyproject.toml` | `GET /system/version` |
| Runtime (per materialization) | `python/pyproject.toml` — same distribution | its status events → `status.runtime_version` |
| Web console | `console/package.json` | `GET /api/config` → `version` |
| Helm chart | `charts/midstream/Chart.yaml` (`version` and `appVersion`) | `helm list` |

The control plane and every runtime app are the **same Python distribution**, so one number
describes the code both execute. They still report separately because a runtime keeps serving the
image it was created with until its spec is re-applied: after a control-plane upgrade, runtimes
legitimately lag until they are rolled.

`midstream.version.PACKAGE_VERSION` reads the installed distribution metadata, so
`pyproject.toml` is the single source of truth. Outside an installed context it reports
`0.0.0+unknown` rather than a plausible-looking number.

The console reads `console/package.json` the same way. `CONSOLE_VERSION` still overrides it, which is
useful when a build system wants to stamp something more specific.

> **Not the same as `template_version`.** The runtime binding's `template_version` (`v1`) versions
> the *shape of the generated KasprApp spec*, not the code. Changing the runtime's Python does not
> bump it, and bumping it does not imply a code change.

## 16.2 Where a user sees it

- **Console and control plane** — the version sits in the sidebar footer; clicking it opens
  **About this deployment**, which lists both with a note on why a runtime may differ.
- **A materialization's runtime** — its Overview tab, in the Reconciliation panel beside
  **Tables**. Blank until that runtime has reported status at least once.
- **A table** — its `materializer.package-version` property names the release that created it
  (11 §11.3), and every snapshot's `materializer.runtime` the runtime that committed it.

## 16.3 Releasing

Semantic versioning, per component:

- **patch** — fixes with no contract change.
- **minor** — additive: a new field, endpoint, or step that older peers can ignore.
- **major** — anything an older peer cannot tolerate: a removed or renamed field, a changed event
  shape, a step whose meaning changed.

The four files declare one version, SemVer where SemVer is spoken and PEP 440 in `pyproject.toml`
(`1.0.0-rc.1` is `1.0.0rc1`); `tools/version.py check` fails CI when they disagree. To cut a release:
`tools/version.py set 1.2.0`, commit, tag `v1.2.0` and push the tag. The release workflow
(`.github/workflows/release.yml`) checks the tag against the declared version, runs the tests, and
publishes the Python package to PyPI, the console image and the chart to GHCR, and a GitHub release;
a pre-release tag such as `v1.2.0-rc.1` publishes a pre-release and no `latest` image.

### What "major" means here

Runtimes outlive control-plane deploys, so the pair must interoperate across versions. Two rules
keep that safe:

1. **New fields on events are optional and ignorable.** `runtime_version` was added this way: a
   runtime that predates it simply reports nothing, and the control plane keeps the last value it
   knew rather than blanking it. `settings_revision` (0.5.0) likewise: a runtime without it shows
   as `unsupported` in the settings rollout. `session_epoch` on file announcements (3.1.0) too: a
   3.0 committer ignores it, and a 3.1 committer judges an announcement without one by its
   generation alone ([10 §10.6](./10-runtime-internals.md)). `lineage` on runtime-settings deliveries
   (4.1) too: a 4.0 runtime ignores it and compares revisions, and a 4.1 runtime sorts a delivery
   without one before every lineage ([17 §17.7](./17-settings.md)). New message kinds on a shared topic are addressed so
   older consumers drop them unread: runtime settings use `materialization_id: "*"`
   ([17 §17.7](./17-settings.md)).
2. **A new work step is a minor change only if nothing depends on it running.** Adding one takes
   three edits, and each omission fails differently:

   | Edit | If you forget it |
   |------|------------------|
   | `WORK_STEPS` in `models/events.py` | `MaterializationWorkEvent` raises `ValueError` on construction. A cron task builds its trigger *inside the app*, so this **crashes the control plane into CrashLoopBackOff** |
   | The consuming agent's input filter (`charts/midstream/templates/agents/`) | The event is consumed and dropped in silence — see [07 §7.1](./07-control-plane-workflows.md#71-the-step-dispatcher) |
   | The topic's derived name in the task's template (`<release>-…`) | The task publishes to a topic no agent consumes; the schedule fires into nothing |

   `tests/test_agent_manifests.py` guards all three. Run it after adding a step rather than
   discovering the gap in a pod.

3. **A release that adds a topic or table to an app needs one forced consumer-group rebalance
   when it is deployed**, whatever its version number. Kaspr's static membership keeps the old
   assignment across the deploy's restart, so the new topics are never assigned until then.
   0.5.0 added the settings topics, 0.6.0 the identity topics, 1.0.0
   `materialization-table-events`. The procedure is in [17 §17.12](./17-settings.md).
4. **From 1.0, a breaking change bumps the major version.** Below 1.0 it bumped the minor: control
   plane 0.6.0 (with console 0.14.0) made every API call authorized, so scripts that called the API
   anonymously had to sign in (18). 1.0.0 is the first major release (§16.4).
5. **A release can depend on the Kaspr it runs on.** The writer agent is rendered with `noack`
   (since 0.7.0), which needs Kaspr 0.11.26 and kaspr-operator 0.19.1 or later, and every runtime
   gets `K_STREAM_WAIT_EMPTY=false` (10 §10.7). Against an older operator, whose CRD drops the field,
   records would be acknowledged before they are in the table, and exactly once would not hold.
   Consumer lag measures records not yet committed to the table. The tail app's views (4.1) set
   `errorContentType`, which needs Kaspr 0.11.30 and kaspr-operator 0.19.7; older ones send the same
   JSON refusals as bytes, which the console reads too, so it is not required.
6. **The table format is part of the contract.** Tables are Iceberg format version 2, with field ids
   1–9 for the metadata columns and the snapshot-summary properties of 11 §11.4. Changing either is
   a major change: every table already written, and every reader of it, depends on them.

## 16.4 What 1.0.0 changed

Control plane and console **1.0.0** (decision D10) replace the Parquet lake and the schema authority
with an Iceberg table per materialization: a breaking release for every peer.

| Change | Who must care |
|---|---|
| Output is an Iceberg v2 table at `<destination>/<namespace>/<name>-<hex12>`, read through `iceberg_scan`, the REST catalog or the console | anyone who read the 0.x Parquet prefixes or ran `reader/main.py` |
| Delivery into the table is exactly once; no `(_kafka_partition, _kafka_offset)` dedupe needed | readers of the output |
| The schema is the table's: nested, forked by type, `_unmapped`; the schema endpoints return a projection (04 §4.6.1) | API clients of the schema endpoints |
| New topic `materialization-table-events`; `materialization-schema-proposals` and the runtime signal topics are gone | deployers: one forced rebalance (§16.3, rule 3), and the rollout of 12 §12.13 |
| New endpoints: the table, maintenance, access tokens, the REST catalog; new permissions `materializations.maintain`, `catalog.read`, `catalog.write` | API clients, role editors |
| Console 1.0 requires control plane 1.0: its `/api/config` reports `controlPlaneCompatible`, and the console shows a banner against an older one (web 16) | deployers |

There is no migration (decision D8): 0.x materializations are deleted and created again, the
identity and settings stores are kept (12 §12.13). A 0.x materialization record left behind fails
`apply_runtime` as `table_missing`.

## 16.5 What 2.0.0 changed

Control plane and console **2.0.0** let a materialization read several topics with one runtime,
each into an Iceberg table of its own, make the materialization its tables' Iceberg namespace, let
an edit add and remove topics, and move table maintenance out of the runtimes into a KasprApp of
its own. Event shapes, records and API fields changed, so by rule 4 it is a major release for every
peer.

| Change | Who must care |
|---|---|
| A create request names `source_topics` (a list); `source_topic` is refused. Each topic becomes a table named after it (`orders-v2` → `orders_v2`); `max_materialization_topics` (20) bounds them (04 §4.5) | API clients that create materializations |
| `PATCH /materializations/{id}` takes `source_topics` and `removed_tables` (`{"<table>": "keep" \| "drop"}`): an edit adds and removes topics. A removed topic's table is kept read-only (`retired`) or dropped with its data (`dropped`, its name never reused); a change of topics stops the runtime until its consumer group empties, about four minutes (04 §4.8.1, 07 §7.9a) | API clients, the console's Edit dialog |
| Table maintenance is the **maintenance app**'s, a KasprApp of its own (`charts/midstream/templates/maintenance/`), for every materialization, stopped ones included. A new control-plane task, `schedule-table-maintenance`, ticks each table every minute on a new topic, `<release>-maintenance-requests`, and `POST …/maintenance` sends there. Runtimes lose `<rt>-maintenance-timer` and run no maintenance; their settings agent no longer takes maintenance requests (02 §2.10a, 10 §10.10) | deployers: deploy the app, and in a renaming overlay patch its topics and keep its labels apart (02 §2.9); runtime pods can shrink |
| Table records gain `state` (`active`, `retired`, `dropping`, `dropped`) and `maintenance_job`; table events add `maintenance_started`; maintenance runs report the app as `runtime_app_ref`; the detail adds `retired_tables` (05 §5.12, §5.17, 06 §6.6) | API clients, anything reading the topics |
| The maintenance log events are the app's: `runtime_compaction_*`, `runtime_snapshot_expiry_completed` and `runtime_orphan_cleanup_completed` are now `maintenance_compaction_*`, `maintenance_snapshot_expiry_completed` and `maintenance_orphan_cleanup_completed` (03 §3.7) | log searches, alerts |
| Tables live at `<destination>/<deployment namespace>/<materialization>-<hex12>/<table>`; the REST catalog has a namespace per materialization: engines read `lake.<materialization>.<table>` instead of `lake.<deployment namespace>.<materialization>` (11 §11.8) | readers of the tables, external engines |
| Schema and table endpoints are per table: `…/tables/{table}/schema`, `…/tables/{table}/schema/versions[/{id}]`, `…/tables/{table}`; maintenance takes an optional `table` (04 §4.6) | API clients |
| The detail's `table` is `tables` (a list); `materialization.source_topic`/`schema_version` are `tables`/`source_topics`; `latest_offsets` is per table; `consumer_lag` is per topic (05 §5.17) | API clients, dashboards |
| Status and table events carry per-table fields (`schema_versions`, `tables`, `table_counters`, `table`) (06) | anything reading the topics |
| Console 2.0 requires control plane 2.0 (`controlPlaneCompatible`): it reads several tables per materialization and joins them in SQL (web 07) | deployers |
| The `maintenance` agent sends work: the retention sweep hands each purge to `workflow-apply`, and the lag refresh hands each measurement to a new `record_consumer_lag` step, both keyed by the materialization. 1.0 saved statuses and deleted records from the `__system__` event, which left copies in the wrong shard of the `state` table (06 §6.4). Its log event `control_plane_retention_purge_completed` is now `control_plane_retention_purge_requested` | deployers (the agent has an output topic: patch it in a renaming overlay, 02 §2.9), log searches |

There is no migration: 1.0 materializations are deleted with the 1.0 control plane — which drops
their tables — and created again on 2.0. A 1.0 runtime's table events, which name no table, are
dropped by 2.0's `table-events` agent. The control-plane app only sends to its one new topic,
`<release>-maintenance-requests`, and consumes nothing new, so the deploy needs no forced
rebalance (rule 3); the maintenance app is new, so its group starts from nothing.

2.0 cannot read a 1.0 materialization or table record, and a deletion on 1.0 can leave one behind:
a copy in another shard of the `state` table, which 1.0's shard routing could strand. 2.0 skips such
a record wherever it scans, logging it once (12 §12.9), and its retention sweep purges it once it
says `deleted`. The status copies 1.0's lag refresh left in the `__system__` shard belong to no
materialization any more; nothing reads them.

## 16.6 What 3.0.0 changed

Control plane and console **3.0.0** put tables in **datasets** and grant access per dataset. A
dataset is a named group of tables and their Iceberg namespace, so SQL reads `<dataset>.<table>`; a
materialization's name becomes a label. Request bodies, records, events and permissions changed, so
by rule 4 it is a major release for every peer.

| Change | Who must care |
|---|---|
| A create request names `tables: [{source_topic, dataset, name?}]`; `source_topics` is refused. Each table goes in the dataset given, named after its topic unless named. `name` is an optional label (free text, 1–128 characters, not unique) that defaults to the id and can be changed (04 §4.5) | API clients that create materializations |
| `PATCH /materializations/{id}` takes `name` (the label) and `tables` in the same shape; a topic change that adds tables is applied once the directory has claimed their names, one at a time (`409 topic_change_pending`) (04 §4.8.1, 07 §7.9b) | API clients, the console's Edit dialog |
| `<dataset>.<table>` is unique across the deployment. The directory agent claims each new table's name before the table is created and releases it when the table leaves the catalog; a create whose claim is refused is marked deleted with the reason (`table_name_taken`, `dataset_not_found`, `forbidden`) (07 §7.2a, 18 §18.16) | API clients: watch `claim_request_id` |
| New endpoints: `/datasets`, `/datasets/{dataset}`, its grants and its tables (04 §4.15) | API clients |
| Permissions have a scope. `dataset.view_schema`, `data.query`, `data.export`, `catalog.read`, `catalog.write`, `dataset.write` and `dataset.manage` hold on a dataset; `materializations.view_schema` and the Data Reader role are gone; new roles Dataset Reader, Writer and Owner, and `datasets.view`, `datasets.create` (18 §18.3) | role editors, anyone relying on Data Reader |
| The REST catalog's namespaces are datasets, and lists only those the caller may read: engines read `lake.<dataset>.<table>` (11 §11.8). Access tokens can be limited to datasets | external engines, token holders |
| Tables live at `<destination>/<storage prefix>/<materialization id>/<table>`; `CONTROL_PLANE_ICEBERG_NAMESPACE` is `CONTROL_PLANE_STORAGE_PREFIX` (03 §3.2.1, 11 §11.1) | deployers: rename the variable in every overlay |
| The `identity` KasprTable is `directory`: it holds datasets, grants and claims too. Every WebView and `identity-intake` bind it as `directory_table` (02 §2.4, §2.8) | deployers: patch the binding in a renaming overlay; the new table starts empty |
| `request-intake` also sends to the identity requests topic (claims and releases), and `identity-intake` sends `tables_claimed` / `claim_rejected` work events and reads the `state` table (06 §6.4, §6.15; 02 §2.5) | deployers: patch both outputs in a renaming overlay |
| Table records, the detail and the list carry `dataset` and `dataset_id`; `namespace` is gone from the table summary; a table says whether the caller may read it (`readable`, `queryable`) (05 §5.12, §5.17) | API clients, dashboards |
| Console 3.0 requires control plane 3.0 (`controlPlaneCompatible`, web 08 §8.4) | deployers |

There is no migration (one release on a clean slate). Before upgrading, delete every 2.0
materialization with the 2.0 control plane: 3.0 cannot read a 2.0 materialization or table record.
It skips such a record wherever it scans, logging it once, and drops status events from its runtime,
also logged once (08 §8.5); a 2.0 runtime left running is no longer managed, and is removed by hand
(12 §12.14). The `directory` table starts empty, so the bootstrap administrator is created again from
the auth Secret and must choose a new password; the 2.0 `identity` changelog topic is no longer read.
The control-plane app consumes the new `directory` changelog, so the deploy needs one rebalance
(rule 3), which kaspr-operator 0.19.4 or later makes on its own.

## 16.7 What 3.1.0 changed

Control plane and runtime **3.1.0** fix a stall. A committer that outlived a member's restart could
ignore that member's file announcements for good: nothing of the member's partitions committed
again, and the member reported `commits_stalled` (10 §10.5). Announcements now carry a
`session_epoch`, and the committer's table block reports `stale_announcements` (10 §10.6, §10.11).

The change is additive, so this is a minor release. There is no migration, and console 3.0 works with
it. Upgrading re-applies every runtime. The rolling restart that follows also restarts each runtime's
committer, which clears a runtime that is stuck this way.

## 16.8 What 4.0.0 changed

Control plane and console **4.0.0** take a materialization's place and scale from the deployment,
partition tables by month as well, and date schema changes by when the data had them. A create
request no longer accepts fields it did, so by rule 4 it is a major release.

| Change | Who must care |
|---|---|
| A create request chooses none of `destination_uri`, `namespace`, `runtime_name` and `storage_size`: every materialization takes the deployment's destination, namespace and storage per member, and a runtime name generated from its id. A request naming one is refused with `400`, the message naming the deployment's value; with no destination configured, or one that is not `gs://`, every create is `409 destination_not_configured` (04 §4.5.2) | API clients that create materializations |
| The materialization record and its projection lose `runtime_name`, the name a request chose; `resolved_runtime_name` is the runtime's name (05 §5.4). An update naming `runtime_name` is refused | API clients, dashboards |
| `POST /topics/analyze` recommends replicas only: `storage_size` is gone from each `recommendation` and from `combined` (04 §4.9.1a) | API clients |
| A new runtime member's volume is `64Mi` (`default_runtime_storage_size`), down from `1Gi`: it holds only the state table, whose RocksDB write buffer is 4 MiB so its write-ahead log stays under 8 MiB; batches are staged on the node's disk (10 §10.3, §10.8; 12 §12.10). The control plane's and the maintenance app's own volumes stay 1Gi | deployers who set `…_DEFAULT_RUNTIME_STORAGE_SIZE` themselves |
| `partition_granularity` may be `month`: Iceberg's `month(_kafka_timestamp)`, `ts_month=2026-10`. Compaction lets this month's files settle one by one rather than waiting for the month to end; there is no week, which Iceberg has no transform for (11 §11.3, §11.6) | API clients; engines read it as any Iceberg month partition |
| Console 4.0 requires control plane 4.0. Its create dialog asks for none of the four and says where the tables will go; its Schema tab dates each change by the records' Kafka timestamps, read from the table's manifests (`GET …/schema/timeline`, web 08 and 11), while the API's schema `since` stays the commit time (04 §4.6.1) | deployers |

There is no migration. A 3.x materialization keeps what it was created with: its destination,
namespace, volume and runtime name, which `resolved_runtime_name` already held. Its record's
`runtime_name` is no longer read. Upgrading re-applies every runtime. On the lab the three 3.x
runtimes came back on 4.0 with their `1Gi` volumes (2026-10-23).

## 16.9 What 4.1.0 changed

Control plane, runtime and console **4.1.0** add the live tail: the console shows a table's newest
records within seconds of their being produced, before the runtime commits them, each exactly once
beside the committed snapshot (19). Everything is additive, so this is a minor release.

| Change | Who must care |
|---|---|
| A new KasprApp, the tail app `<release>-tail` (`charts/midstream/templates/tail/`, 02 §2.10b), and a topic, `<release>-tail-requests` (64 partitions), on which the control plane's `maintenance` agent names every tailed table each minute. The control plane only sends to it: no forced rebalance (§16.3 rule 3 does not apply) | deployers |
| A materialization has `live_tail`: on create, `PATCH` and every answer that carries the materialization (04 §4.5, §4.8.1). New materializations take `live_tail_default` (on); existing ones read off until an edit turns it on. The runtime knows nothing of it, so changing it re-applies nothing | API clients |
| Nine control-plane settings, `live_tail_*` (17 §17.3), among them the deployment-wide `live_tail_enabled`; `GET /system/settings` reports `live_tail_enabled` and `live_tail_default` | administrators |
| `?raw=true` on the schema views adds the table's `last_column_id` and `rejected_paths` (04 §4.6.1), and passes `raw` on from the request, which the views had dropped | API clients |
| The seal builds its rows with `lakehouse/rows.py::build_row`, shared with the tail app: byte for byte what it wrote before (13 §13.3) | none |
| A restarted control plane answers every request `503 unavailable` (`starting`) until it has read its tables back, where it used to answer from a half-read directory — sessions read as ended, so the console signed its people out (18 §18.7) | API clients: retry a `503` |
| Runtime settings have a **lineage** (17 §17.7), so runtimes follow a control plane installed afresh, or one whose `settings` table was lost, whatever revisions an earlier one reached. A new KasprTask, `settings-bootstrap` (at startup, on the leader), sends a `bootstrap` request on `<release>-settings-requests`, which the settings agent already consumes — no forced rebalance; the first start begins the lineage and delivers the runtime settings to every runtime under it. Deliveries carry `lineage`, heartbeats `settings_lineage`, and `GET /settings/runtime` the scope's `lineage`; a runtime re-applied after a lineage begins is sent the settings even with none changed. A 4.0 runtime compares revisions alone, as before, and the rollout judges it by its revision | deployers: the task, and its `topicSend` patch in a renaming overlay |
| The control plane and the tail app no longer create topics by asking about them: on a broker that creates topics on demand, describing a missing topic made it, and a deleted topic came back at the next metadata refresh of a client that had looked up its offsets (19 §19.7). A topic the control plane may not describe now reads as missing in topic analysis | none |
| The console reads GCS objects with a plain GET of the JSON API, not the client library's download stream, which printed `MaxListenersExceededWarning` twice per object; cached mode writes each data file whole or not at all (web 06) | none |
| Console 4.1: `CONSOLE_TAIL_URL`, the Data tab's Live mode, a statement's Auto re-run, the head peek, the Live tail switch in the create and edit dialogs, and four settings (web 03). Against a 4.0 control plane it shows committed rows only, as before | deployers |

There is no migration. Upgrading re-applies every runtime (its package pin changes); the tail app
starts with an empty group and serves a table once someone asks for it.

## 16.10 What 4.2.0 changed

Control plane and runtime **4.2.0** give every Kaspr app of the deployment a VerticalPodAutoscaler,
and the runtimes a second core. Everything is additive, so this is a minor release; console 4.1
works with it and shows the new read-only setting as it shows any.

| Change | Who must care |
|---|---|
| Every KasprApp — the control plane, the maintenance and tail apps, and every runtime through the runtime spec — has `verticalPodAutoscaler` (02 §2.10c): its CPU request follows its use, resized in place, within bounds under its CPU limit; the limits and memory stay as set. It needs kaspr-operator 0.19.5 or later and the Vertical Pod Autoscaler add-on (Kubernetes 1.33+ and VPA 1.4+ to resize in place); without them the apps run with their manifest resources | deployers: an overlay that patches an app's `resources` keeps `maxAllowed.cpu` under `limits.cpu`, and one that replaces the control plane's environment carries the runtime spec's block too, as the dev overlay does |
| The runtime spec limits each member to two cores, up from one, so a seal runs beside the worker (12 §12.10) | deployers |
| The Settings page's **Deployment** section shows the runtimes' **Vertical autoscaling** beside their resources (`GET /settings/runtime` → `deployment`, 17 §17.3) | none |

There is no migration. Upgrading restarts the control plane, maintenance and tail apps once, for
the new package; their VPAs restart nothing. Every runtime is re-applied and rolls once, since its
package pin and CPU limit change.

## 16.11 What 4.3.0 changed

Control plane, runtime and console **4.3.0** give every materialization a live tail: there is no
switch per materialization any more, and only the deployment's `live_tail_enabled` turns the live tail
off, for all of them (19). It stays compatible with 4.1 and 4.2 peers, so this is a minor release.

| Change | Who must care |
|---|---|
| Every running materialization's tables are tailed: those created before 4.1, and those a control plane before 4.3 had switched off, included. A stored `live_tail` is read and dropped | none |
| A create or `PATCH` that says `live_tail: false` is refused (`400`, `field: "live_tail"`); `true`, which a 4.1 console sends back, changes nothing (04 §4.5, §4.8.1). Answers still carry `live_tail: true`, and `GET /system/settings` `live_tail_default: true`, so a 4.1 console offers Live | API clients |
| The setting `live_tail_default` is gone, with `CONTROL_PLANE_LIVE_TAIL_DEFAULT`; a stored override of it is never applied | administrators |
| The tail app still honours a control plane before 4.3: its `live_tail_off` cancel, and `live_tail: false` on a materialization it resolves | none |
| Console 4.3: no *Live tail* checkbox in the create dialog, no switch in the edit dialog, no *Live tail* row on the Overview tab. Against a control plane before 4.3 it still honours a materialization switched off there | none |

There is no migration. Upgrading re-applies every runtime (its package pin changes).

## 16.12 What 4.4.0 changed

Control plane, runtime and console **4.4.0** scale a runtime that is far behind, show one whose
members keep restarting as unhealthy, and bound a seal's memory. Everything is additive, so this is
a minor release: a 4.3 console works with it, and a 4.4 console hides what a control plane before
4.4 does not offer.

| Change | Who must care |
|---|---|
| **Autoscaling** (07 §7.12a): after each lag measurement, a running, healthy runtime more than `runtime_autoscale_min_lag_records` (100,000) behind that would take longer than `runtime_autoscale_target_minutes` (30) to catch up gets the members its pace says it needs — at most `runtime_autoscale_max_replicas` (16), no more than its partitions keep busy, changes at least `runtime_autoscale_cooldown_seconds` (300) apart — and its own `replicas` again after three measurements under the floor. `runtime_autoscaling_enabled` turns it off for every materialization (Settings › Materializations) | deployers: on by default, for existing materializations too — check the cluster has room (12 §12.1) |
| A materialization has `autoscaling`, on unless a create or `PATCH` turns it off (04 §4.5, §4.8.1); records and events before 4.4 read as on. The detail has `autoscale` — the scaled count, why, and the time to catch up (04 §4.6) — and `GET /system/settings` `autoscaling_enabled`, `autoscale_max_replicas` and `autoscale_target_minutes` | API clients |
| A new record, `runtime-autoscale:<id>` (05 §5.6a), written in the materialization's shard on `record_consumer_lag`, which may now send an `apply_runtime` (`caused_by` `runtime-autoscaling`) | none |
| **Restart health** (08 §8.3, row 3d): each observation reads the runtime's pods. A member crash-looping, killed for memory in the last 30 minutes, or restarted in them after three restarts makes a running materialization `degraded` / `runtime_members_restarting`, with a plain message, and a heartbeat cannot clear it until the restarts age out. The control plane's role gains `get` and `list` on pods (02 §2.7) | deployers: apply the role; without it the restarts go unobserved, logged once |
| **The seal holds one row group** whatever its batch's size or spread (10 §10.4): its lines are spooled to a file per partition value first, then each file written in turn. Its output is byte for byte what it was; a batch over two years of months now takes 267 MiB where it took 2.1 GiB | none: the shipped 1 GiB per member now holds a backfill (12 §12.10) |
| Console 4.4: an **Autoscale** switch in the create dialog (Advanced) and the edit dialog; the Overview's *Scaled up to N members*, with why, in the Reconciliation panel, and *Autoscaling* in the Specification; a degraded runtime's health reads **Unhealthy**, its message as the badge's tooltip. `GET /api/config` has `autoscaling`. The Data tab of the release before it: one plain status row (rows, then *N records behind*), Peek, Share and Export under *More actions*, the latest snapshot always, and committed row counts in the lists (web 11) | none |

There is no migration. Upgrading re-applies every runtime (its package pin changes).

## 16.13 What 4.4.1 changed

Control plane and runtime **4.4.1** fix how a table's drop waits for a maintenance job. It is a fix
with no contract change, so a patch release; the console stays 4.4.0.

| Change | Who must care |
|---|---|
| The `maintenance_completed` that ends a running job — `cancelled` when the drop's cancel stopped it, `abandoned` when a restarted member no longer runs it — now clears the table record's job while its materialization is being deleted, as it did for a table dropped by a change of topics (06 §6.6). Before, every event about a materialization being deleted was ignored, so a delete that met a running compaction or orphan cleanup waited the full five minutes for it | none |
| A job the drop has waited five minutes for is forgotten on the record (07 §7.10). Before, each pass recorded the drop's progress afresh, so a job still recorded was waited for in full before every pass: on the lab (2026-10-25) a 17 GB table's drop paused five minutes between passes, for a compaction the maintenance app had stopped when the delete began | none |

There is no migration. A drop already waiting goes on at its next check under 4.4.1.
