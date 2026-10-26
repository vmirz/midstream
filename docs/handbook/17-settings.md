# 17 — Settings

How an administrator changes the system's behaviour from the console's Settings page, where those
changes are stored, and how each component picks them up without a restart.

Source: `settings/catalog.py`, `settings/effective.py`, `services/settings.py`,
`storage/settings.py`, `models/settings.py`, `web/settings.py`, `handlers/settings.py`,
`runtime_app/live_settings.py`, `runtime_app/handlers/runtime_settings.py`; console:
`console/server/settings/`, `console/server/routes/settings.ts`, `console/src/features/settings/`.

## 17.1 The model in one paragraph

Every component is still configured by its environment, as before. A subset of those settings —
the ones that are safe to change while the system runs — may also be **overridden** from the
console. An override is stored with the control plane, in its own `settings` KasprTable, and wins
over the environment until it is **reset**. Settings that must match the deployment itself (topic
names, namespaces, credentials, pod sizing) are never overridable; the console shows them
read-only. No component reads an override at startup only: each reads its effective configuration
where the value is used, so a change applies to the next event, request, poll or batch.

## 17.2 Precedence

For every overridable setting:

1. an **override** chosen on the Settings page, else
2. the **deployment value** — the environment variable, when the deployment sets it, else
3. the **built-in default** in code.

The Settings page shows which of the three applies (`source`: `override`, `deployment`,
`default`), who made an override and when, and the value a reset would return to.

Consequences worth knowing:

| Situation | Result |
|---|---|
| A deployment changes an env var that also has an override | The override still wins. The page shows both, so the drift is visible; reset the override to follow the deployment again |
| A bad override breaks something | Reset it from the page, or `PATCH /settings/{scope}` with `{"reset": ["<key>"]}`. Redeploying with a different env value does **not** beat an override |
| An override is stored and a later release tightens its validation | The stored value is ignored (logged) and the deployment value applies until someone sets a valid one |
| The control plane is unreachable | The console keeps the last overrides it read; a console started during the outage runs on its deployment values. Nothing can be saved, because the store is the control plane |

There is no way for a deployment to forbid an override (no "locked" settings). Anyone holding
`settings.manage` can change any overridable setting; see §17.11.

## 17.3 What is overridable

Three **scopes** hold overrides, one `SettingsDocument` each:

| Scope | Configures | Described and validated by |
|---|---|---|
| `control-plane` | The control plane's agents and API | The control plane: `CONTROL_PLANE_SETTINGS` in `settings/catalog.py` |
| `runtime` | Every runtime, delivered while they run (§17.7) | The control plane: `RUNTIME_SETTINGS`, which must equal `LIVE_SETTING_FIELDS` in `runtime_app/live_settings.py` |
| `console` | The web console | The console: `console/server/settings/catalog.ts`. The control plane stores these values without knowing them, so the console can add a setting without a control-plane release |

### Control plane (`control-plane`)

`…` is `CONTROL_PLANE_`. Each key is also the `ControlPlaneConfig` field it overrides;
the label is what the Settings page calls it, in the section named in bold. A setting marked
advanced sits behind the section's **Advanced** toggle.

**Materializations**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `default_destination_uri` | `…DEFAULT_DESTINATION_URI` | none | a `gs://` URI | Destination — where every new materialization's tables are created; none, and no materialization can be created |
| `default_runtime_replicas` | `…DEFAULT_RUNTIME_REPLICAS` | 1 | 1–1,024 | Default replicas |
| `max_runtime_replicas` | `…MAX_RUNTIME_REPLICAS` | 64 | 1–1,024 | Maximum replicas |
| `default_runtime_storage_size` | `…DEFAULT_RUNTIME_STORAGE_SIZE` | `64Mi` | a Kubernetes quantity | Storage per replica — every new runtime member's volume, which holds only its state table |
| `default_partition_granularity` | `…DEFAULT_PARTITION_GRANULARITY` | `day` | `hour`, `day`, `month`, `none` | Default partitioning |
| `runtime_autoscaling_enabled` | `…RUNTIME_AUTOSCALING_ENABLED` | on | — | Autoscale runtimes — more members for a runtime far behind its topics, its own count once caught up; off, scaled runtimes go back within a minute |
| `runtime_autoscale_max_replicas` | `…RUNTIME_AUTOSCALE_MAX_REPLICAS` | 16 | 1–1,024 | Autoscaling maximum replicas — never more than the topics' partitions keep busy |
| `runtime_autoscale_target_minutes` | `…RUNTIME_AUTOSCALE_TARGET_MINUTES` | 30 | 1–1,440 | Catch-up target |
| `runtime_autoscale_min_lag_records` | `…RUNTIME_AUTOSCALE_MIN_LAG_RECORDS` | 100,000 | 1–1,000,000,000 | Lag worth scaling for (advanced) |
| `runtime_autoscale_cooldown_seconds` | `…RUNTIME_AUTOSCALE_COOLDOWN_SECONDS` | 300 | 60–3,600 | Time between scaling steps (advanced) |
| `max_materialization_topics` | `…MAX_MATERIALIZATION_TOPICS` | 20 | 1–100 | Topics per materialization — the most topics one materialization (one runtime, one table each) may read; applies to new materializations |

**Deletion**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `destination_cleanup_enabled` | `…DESTINATION_CLEANUP_ENABLED` | on | — | Delete object data |
| `purge_records_on_delete` | `…PURGE_RECORDS_ON_DELETE` | on | — | Purge records on delete |
| `deleted_retention_hours` | `…DELETED_RETENTION_HOURS` | 168 | 1–8,760 | Deleted record retention |
| `deleted_purge_batch_limit` | `…DELETED_PURGE_BATCH_LIMIT` | 50 | 1–10,000 | Records purged per sweep (advanced) |

**Schema**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `discovery_sample_records` | `…DISCOVERY_SAMPLE_RECORDS` | 200 | 1–10,000 | Discovery sample |
| `discovery_sample_seconds` | `…DISCOVERY_SAMPLE_SECONDS` | 5 | 1–60 | Discovery time limit |
| `schema_history_limit` | `…SCHEMA_HISTORY_LIMIT` | 50 | 1–10,000 | Schema versions kept |

**Catalog**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `catalog_api_enabled` | `…CATALOG_API_ENABLED` | on | — | Iceberg REST catalog |
| `catalog_metadata_cache_seconds` | `…CATALOG_METADATA_CACHE_SECONDS` | 5 | 1–300 | Table metadata cache (advanced) |

**Jobs**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `consumer_lag_refresh_batch_limit` | `…CONSUMER_LAG_REFRESH_BATCH_LIMIT` | 100 | 1–10,000 | Lag measurements per pass (advanced) |

**Topics**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `topic_catalog_ttl_seconds` | `…TOPIC_CATALOG_TTL_SECONDS` | 15 | 1–3,600 | Topic list cache |
| `topic_catalog_min_refresh_seconds` | `…TOPIC_CATALOG_MIN_REFRESH_SECONDS` | 5 | 1–3,600 | Minimum topic list refresh |
| `topic_analysis_max_topics` | `…TOPIC_ANALYSIS_MAX_TOPICS` | 20 | 1–1,000 | Topics per analysis |

**Diagnostics**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `protocol_logging_enabled` | `PROTOCOL_LOGGING_ENABLED` | off | — | Protocol logging |

**Security**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `session_idle_timeout_minutes` | `…SESSION_IDLE_TIMEOUT_MINUTES` | 480 | 5–10,080 | Idle timeout |
| `session_max_lifetime_hours` | `…SESSION_MAX_LIFETIME_HOURS` | 168 | 1–2,160 | Maximum session length |
| `session_refresh_minutes` | `…SESSION_REFRESH_MINUTES` | 10 | 1–60 | Session renewal interval (advanced) |
| `password_min_length` | `…PASSWORD_MIN_LENGTH` | 12 | 8–128 | Minimum password length |
| `sign_in_max_failed_attempts` | `…SIGN_IN_MAX_FAILED_ATTEMPTS` | 10 | 3–100 | Failed sign-ins before lockout |
| `sign_in_lockout_minutes` | `…SIGN_IN_LOCKOUT_MINUTES` | 15 | 1–1,440 | Lockout duration |
| `audit_log_retention_days` | `…AUDIT_LOG_RETENTION_DAYS` | 90 | 1–3,650 | Audit log retention |

**Live tail** — the newest records of a materialization's tables, served from Kafka before they are
committed ([19 — The live tail](./19-live-tail.md)). The first two are the control plane's own; the
rest travel with the tail app's ticks, so a change reaches it within a minute (§17.7).

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `live_tail_enabled` | `…LIVE_TAIL_ENABLED` | on | — | Live tail — every materialization's tables; off, nothing is tailed and the console shows committed rows only |
| `live_tail_max_rows_per_table` | `…LIVE_TAIL_MAX_ROWS_PER_TABLE` | 250,000 | 1,000–10,000,000 | Records per table — also the backlog bound |
| `live_tail_max_bytes_per_table` | `…LIVE_TAIL_MAX_BYTES_PER_TABLE` | 256 MiB | 8 MiB–4 GiB | Memory per table |
| `live_tail_retention_seconds` | `…LIVE_TAIL_RETENTION_SECONDS` | 120 | 10–3,600 | Committed records kept (advanced) |
| `live_tail_fetch_wait_ms` | `…LIVE_TAIL_FETCH_WAIT_MS` | 200 | 50–2,000 | Kafka read wait (advanced) |
| `live_tail_max_records_per_second_per_table` | `…LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE` | 5,000 | 100–100,000 | Read rate per table (advanced) |
| `live_tail_peek_max_rows` | `…LIVE_TAIL_PEEK_MAX_ROWS` | 1,000 | 10–10,000 | Records per peek (advanced) |
| `live_tail_idle_seconds` | `…LIVE_TAIL_IDLE_SECONDS` | 600 | 0–86,400 | Tail after the last query — 0 keeps every table on |

The Security section's seven are described in [18 — Identity and access](./18-identity-and-access.md)
§18.11. `default_partition_granularity` is the first `enum` setting: its definition lists `options`,
and the page shows a select.

Cross-field rules: the default replica count may not exceed the maximum; the idle timeout may not
be longer than the maximum session length; the renewal interval must be shorter than the idle
timeout. When an override and a
deployment value disagree on that (each valid alone), `resolve_config` clamps the default to the
maximum rather than let a create default to more replicas than it may have. The session rules are
checked against what a change would produce, like every cross-field rule.

Each takes effect at its point of use: a default for the next create, the deletion settings for
deletes that have not reached that step, the discovery settings for the next discovery, the catalog
settings for the next catalog request, the retention settings for the next hourly sweep.

### Runtimes (`runtime`)

`…` is `RUNTIME_`. Every one is a `RuntimeAppConfig` field (03 §3.3.1).

**Runtimes**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `max_open_file_age_seconds` | `…MAX_OPEN_FILE_AGE_SECONDS` | 120 | 10–86,400 | Seal after |
| `max_open_file_age_ceiling_seconds` | `…MAX_OPEN_FILE_AGE_CEILING_SECONDS` | 600 | 10–86,400 | Always seal after |
| `min_rows_per_file` | `…MIN_ROWS_PER_FILE` | 10,000 | 1–100,000,000 | Minimum rows per file |
| `min_file_size_bytes` | `…MIN_FILE_SIZE_BYTES` | 8 MiB | 1 B–16 GiB | Minimum file size |
| `max_rows_per_file` | `…MAX_ROWS_PER_FILE` | 250,000 | 1–100,000,000 | Maximum rows per file |

**Commits**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `commit_interval_seconds` | `…COMMIT_INTERVAL_SECONDS` | 30 | 5–3,600 | Commit interval |
| `max_files_per_commit` | `…MAX_FILES_PER_COMMIT` | 500 | 1–10,000 | Files per commit |
| `max_bytes_per_commit` | `…MAX_BYTES_PER_COMMIT` | 8 GiB | 1 MiB–1024 GiB | Bytes per commit (advanced) |
| `announce_retry_seconds` | `…ANNOUNCE_RETRY_SECONDS` | 60 | 10–3,600 | Announcement retry (advanced) |

**Schema**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `schema_evolution_debounce_seconds` | `…SCHEMA_EVOLUTION_DEBOUNCE_SECONDS` | 2 | 1–60 | Schema change batching |
| `seal_schema_wait_seconds` | `…SEAL_SCHEMA_WAIT_SECONDS` | 30 | 0–600 | Seal wait for schema (advanced) |
| `max_forks_per_field` | `…MAX_FORKS_PER_FIELD` | 3 | 1–10 | Type forks per field |
| `max_struct_fields` | `…MAX_STRUCT_FIELDS` | 300 | 10–10,000 | Fields per object |
| `max_nesting_depth` | `…MAX_NESTING_DEPTH` | 10 | 2–32 | Nesting depth |
| `max_schema_fields` | `…MAX_SCHEMA_FIELDS` | 2,000 | 50–100,000 | Fields per table |

**Maintenance** — runtime-scope settings that steer the maintenance app (§17.7)

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `compaction_enabled` | `…COMPACTION_ENABLED` | on | — | Compaction |
| `compaction_interval_minutes` | `…COMPACTION_INTERVAL_MINUTES` | 15 | 1–1,440 | Compaction interval |
| `compaction_min_input_files` | `…COMPACTION_MIN_INPUT_FILES` | 8 | 2–10,000 | Files worth compacting |
| `compaction_small_file_bytes` | `…COMPACTION_SMALL_FILE_BYTES` | 64 MiB | 1 MiB–2 GiB | Small file size |
| `compaction_target_file_size_bytes` | `…COMPACTION_TARGET_FILE_SIZE_BYTES` | 256 MiB | 8 MiB–2 GiB | Target file size |
| `compaction_max_input_bytes` | `…COMPACTION_MAX_INPUT_BYTES` | 1 GiB | 8 MiB–1024 GiB | Data per compaction (advanced) |
| `compaction_hot_partition_delay_minutes` | `…COMPACTION_HOT_PARTITION_DELAY_MINUTES` | 60 | 0–1,440 | Hot partition delay (advanced) |
| `compaction_timeout_minutes` | `…COMPACTION_TIMEOUT_MINUTES` | 30 | 5–240 | Compaction time limit (advanced) |
| `compaction_max_attempts` | `…COMPACTION_MAX_ATTEMPTS` | 5 | 1–20 | Compaction attempts (advanced) |
| `snapshot_retention_hours` | `…SNAPSHOT_RETENTION_HOURS` | 72 | 1–8,760 | Snapshot retention |
| `min_snapshots_to_keep` | `…MIN_SNAPSHOTS_TO_KEEP` | 20 | 1–10,000 | Snapshots always kept |
| `snapshot_keep_all_minutes` | `…SNAPSHOT_KEEP_ALL_MINUTES` | 60 | 1–10,080 | Keep every snapshot for (advanced) |
| `snapshot_expiry_interval_minutes` | `…SNAPSHOT_EXPIRY_INTERVAL_MINUTES` | 60 | 5–1,440 | Snapshot expiry interval (advanced) |
| `orphan_cleanup_enabled` | `…ORPHAN_CLEANUP_ENABLED` | on | — | Orphan file cleanup |
| `orphan_min_age_hours` | `…ORPHAN_MIN_AGE_HOURS` | 24 | 1–720 | Orphan minimum age (advanced) |
| `orphan_cleanup_interval_hours` | `…ORPHAN_CLEANUP_INTERVAL_HOURS` | 24 | 1–168 | Orphan cleanup interval (advanced) |

**Runtimes**

| Key | Env var | Default | Bounds | Label |
|---|---|---|---|---|
| `protocol_logging_enabled` | `PROTOCOL_LOGGING_ENABLED` | off | — | Protocol logging |
| `protocol_log_slow_wait_seconds` | `PROTOCOL_LOG_SLOW_WAIT_SECONDS` | 30 | 1–3,600 | Slow wait log interval (advanced) |

Cross-field rules: the soft age may not exceed the ceiling, the minimum rows may not exceed the
maximum, the small-file size must stay below the target size, and the seal's schema wait may not
exceed the ceiling.

A runtime's deployment value is read from the environment of the runtime spec
(`CONTROL_PLANE_RUNTIME_APP_SPEC` → `template.kasprContainer.env`), which is what
every runtime pod is started with (`settings/effective.py::runtime_environment`). Each runtime
applies these values at their point of use (`get_runtime_app_config()`): the roll thresholds at
the next roll check, the commit settings at the next commit, the schema caps at the next schema
window, logging at the next line. The compaction, expiry and orphan settings are the maintenance
app's: it applies them at its next tick, within a minute (§17.7). The table's own
`history.expire.*` properties, which other engines' expiry procedures read, are written when the
table is created and not updated after: the maintenance app's expiry follows the live setting.

### The console (`console`)

See the console handbook, [03 — Configuration](../../console/docs/03-configuration-reference.md).
Twenty-nine of its settings are overridable — refresh cadence, chart and rate windows, query and
export limits, the Data explorer switch, log level, request timeout, how often it re-checks a
session and how many sign-in attempts one address may make — and everything that is sized with
the pod or wires it to its environment is not. The attribution setting (`defaultSubmittedBy`) was
retired when accounts arrived; a stored override for it is ignored.

### Never overridable

Reported read-only on the Settings page's **Deployment** section (`DEPLOYMENT_SETTINGS`):

| Setting | Why it stays with the deployment |
|---|---|
| Topic names (seven), `K_APP_NAME`, the state, settings and directory table bindings | Must match the chart's agent and WebView wiring |
| `CONTROL_PLANE_STORAGE_PREFIX` | Part of every table's location |
| `CONTROL_PLANE_CATALOG_BASE_URL` | The address engines reach the control plane at; the console's connection snippets show it |
| `CONTROL_PLANE_DEFAULT_NAMESPACE` | A namespace needs the runtimes' role, service account and secrets |
| `CONTROL_PLANE_RUNTIME_NAME_PREFIX` | Names every runtime and decides which topics a delete removes |
| `CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION`, `…_RUNTIME_APP_SPEC` | Shape of the rendered runtime; changing it re-applies — and restarts — every runtime. Shown as **Resources per replica**, **Storage class** and **Vertical autoscaling**; a change to the vertical autoscaling alone restarts none (02 §2.10c) |
| Credentials, `GOOGLE_APPLICATION_CREDENTIALS`, the session signing keys, the console's service token, the bootstrap administrator | Never through a UI. The page says only whether each identity secret is configured |
| The runtime's task intervals (heartbeat, pipeline, commit timer) | Rendered into the runtime's tasks; built in |
| The maintenance app's job slots and forget window (03 §3.4) | Read from its environment at start; they size its pod |

`CONTROL_PLANE_DEFAULT_LIST_LIMIT` is parsed but used by nothing, so it is not shown.

## 17.4 Storage

A separate **`settings`** KasprTable (`charts/midstream/templates/tables/settings.yaml`; `<release>-settings`
in dev), global, 3 partitions, JSON. It is kept apart from the `state` table on purpose: it holds
configuration rather than reconciliation state, it has exactly one writer (the `settings-intake`
agent), and its changelog is the settings' own history in Kafka.

| Key | Entity type | Holds |
|---|---|---|
| `settings:<scope>` | `settings` | `SettingsDocument`: the scope's overrides with who set each and when, `revision`, the last 20 applied request ids, and the last refusal |
| `settings-change:<scope>:<revision, 10 digits>` | `settings-change` | `SettingsChangeRecord`: what one applied request changed — each setting's value and source before and after |

```json
{
  "scope": "control-plane",
  "revision": 3,
  "values": {
    "max_runtime_replicas": { "value": 32, "updated_at": "2026-10-09T12:50:01Z", "updated_by": "jane@example.com" }
  },
  "updated_at": "2026-10-09T12:50:01Z",
  "updated_by": "jane@example.com",
  "recent_request_ids": ["set-0c1d…", "set-9a8b…"],
  "last_rejection": null
}
```

`revision` counts applied changes and only increases; 0 means the scope was never changed. A
change that sets every value to what it already is applies without a new revision. The `runtime`
scope's document also has a `lineage`, which names its history of revisions (§17.7); it exists
from the control plane's first start, before anything is changed.

History is bounded: the newest **100** changes per scope are kept, older ones are deleted as each
new one is recorded (`storage/settings.py::save_settings_change`). Reads take the highest revision
when a key is found in more than one table shard, the same defence `resolve_envelope` gives the
`state` table.

## 17.5 Changing a setting

```
PATCH /settings/{scope}   (settings-update WebView)
  validate: known keys, values in bounds, cross-field rules against what the change would produce,
            base_revision equal to the stored revision (else 409 settings_changed)
  → <release>-settings-requests   key settings:<scope>        (no table write)
  ← 202 { scope, request_id, base_revision, accepted_at, status_url }

settings-intake agent → services/settings.py::apply_settings_change
  request already applied (in recent_request_ids)  → nothing
  base_revision no longer current                  → refused: last_rejection = settings_changed
  values no longer valid together                  → refused: last_rejection = invalid_setting
  otherwise → revision + 1, overrides saved, change recorded
              runtime scope: also deliver to every runtime (§17.7)

settings-bootstrap task (at startup, leader) → <release>-settings-requests   key settings:runtime
settings-intake agent → services/settings.py::bootstrap_runtime_settings
  runtime scope has a lineage  → nothing
  otherwise → lineage begun and saved, scope delivered to every runtime (§17.7)
```

Like every mutation in this system, the API validates and publishes; only an agent writes the
table. Requests are keyed by scope, so one scope's changes apply in order on one partition, and the
changelog partition of every write to a scope's keys is that same partition. The startup request
`{"kind": "bootstrap", "scope": "runtime", "requested_at": …}` carries the runtime scope's key for
the same reason.

**Optimistic concurrency.** A client sends the revision it last read as `base_revision`. The API
refuses a stale one at once with `409 conflict`; the agent checks again, because two requests can
pass the API inside the same moment. A refusal found by the agent cannot become an HTTP error —
the 202 is already sent — so it is stored as `last_rejection`, and the console, which waits for
its request to be applied (§17.8), reports it.

**Redelivery.** The agent remembers the last 20 request ids it applied; a request it sees again
changes nothing.

## 17.6 How the control plane applies its settings

Every handler that reads configuration resolves it once per event and passes it down:

```python
# code-derived (handlers/request_intake.py and the other handlers)
cfg = resolve_config(settings_table) if config is None else config
```

`resolve_config` (`settings/effective.py`) reads `settings:control-plane` and returns the
environment's `ControlPlaneConfig` with valid overrides applied. The merge is cached against the
document's revision, so an unchanged scope costs one table read per event. Called with no
settings table — tests, and any caller that predates settings — it returns the environment's
config unchanged.

Every operation that calls such a handler binds the `settings` table as `settings_table`: the
agents that read configuration (`request-intake`, `workflow-apply`, `resource-observation`,
`table-events`, `maintenance`, `settings-intake`, `identity-intake`) and the WebViews that do —
`materializations-create`, `materializations-update`, `materializations-maintenance`,
`system-settings`, `topics-list`, `topics-analyze`, the catalog views and the auth views.
`tests/test_agent_manifests.py::SettingsWiringTests` parses each operation's Python and fails when
one calls a configuration-reading entry point without binding and passing the table — the
mistake would otherwise show up only as one component ignoring a change.

Two rules keep this safe:

- **Settings never reach the rendered runtime spec.** No overridable field is read while rendering
  a runtime, so the spec hash — and therefore the sweep's drift check — is the same with or
  without overrides. Were it otherwise, every change would restart every runtime.
  `tests/test_runtime_settings.py` renders a runtime with every control-plane setting overridden and
  asserts the hash is unchanged.
- **Limits are checked when a request is accepted.** A create or edit carries the values its
  defaults resolved to, and request intake no longer re-checks `max_runtime_replicas`, so lowering
  the maximum never fails a request the API already accepted.

`GET /system/settings` — the defaults the console's create dialog shows — reports the effective
values.

## 17.7 How runtimes get their settings

Runtimes are separate Kaspr apps and cannot read the control plane's table, so runtime settings
are **delivered**. They travel on the runtime control topic, `materialization-schema-authority-updates`
(named before 1.0, 06 §6.7), which every runtime's settings agent consumes:

```json
{
  "kind": "runtime_settings",
  "materialization_id": "*",
  "target_materialization_id": null,
  "revision": 4,
  "lineage": "20261024T101502Z-3f9a1c2e",
  "values": { "max_open_file_age_seconds": 60 },
  "published_at": "2026-10-09T13:02:11Z"
}
```

| When | Sent by | `target_materialization_id` |
|---|---|---|
| A runtime-scope change is applied | `settings-intake` | none: every runtime |
| A runtime is applied (create, resume, edit, drift re-apply, upgrade) | `workflow-apply` | that runtime, so one that missed a change or was just created catches up. Sent once the settings have a lineage, with no change made too |
| The control plane starts on runtime settings without a lineage, and begins one | `settings-intake`, asked by the `settings-bootstrap` task | none: every runtime |

The message carries **every** override, not the change, so whichever delivery a runtime sees
last leaves it on the right values; a reset is simply an override set without that key.

**Lineage.** A runtime cannot tell a control plane's revision 3 from an earlier one's revision 3,
and a new runtime reads the delivery topic from its start, which can hold an earlier installation's
deliveries: a control plane installed afresh over a Kafka cluster that kept the topic, or one whose
`settings` table was lost, starts its revisions again. The settings therefore have a **lineage**,
which names one history of revisions. The `settings-bootstrap` task (at startup, on the leader)
asks the settings agent to begin one when the `runtime` scope has none — on a new installation, on
the first start after an upgrade from before 4.1, or after the table was lost — and the agent
delivers the scope to every runtime under it, revision 0 with no values on a new installation. It
never changes after (a change that finds none begins one too). A lineage starts with the UTC time
it began, in a fixed width, then a random part (`20261024T101502Z-3f9a1c2e`), so a later one sorts
after an earlier one. A runtime keeps the newest settings **by lineage, then by revision**:
whatever revision an earlier lineage reached, the first delivery of a later one replaces it, and
deliveries without a lineage, from a control plane that predates them, sort before every lineage.

On the runtime side (`runtime_app/handlers/runtime_settings.py`):

1. The settings agent lets through its own materialization id and `*`; a delivery targeted at
   another runtime is ignored.
2. A delivery not newer than what the runtime holds — by lineage, then revision
   (`live_settings.py::settings_position`) — is ignored. Otherwise its values are sanitised to the
   fields this runtime knows (`LIVE_SETTING_FIELDS`), stored with its revision and lineage in the
   runtime's `<rt>-state` table under `runtime-settings`, and installed in the process.
3. The state table is global, so the other members of the runtime see the write. Each
   member re-reads it at most every 5 seconds, from the writer on its next record and from the
   status task on its next heartbeat (`runtime_app/live_settings.py::refresh_runtime_settings`).
   A restarted member reads it back the same way.
4. `get_runtime_app_config()` returns the environment with the installed overrides applied. If
   they make an unworkable combination against this runtime's environment — a ceiling below the
   soft age — the runtime logs it once and keeps its environment's values; a bad delivery can
   never stop a runtime from processing records. A malformed delivery is ignored, never raised,
   because the same agent carries every later delivery.

**The tail app** gets its settings the same way as the maintenance app, below: every tick carries
the control plane's live-tail settings resolved that minute (06 §6.18), and the app uses each table's
latest tick. Its own variables (`TAIL_*`, 19) size its pod and are fixed.

**The maintenance app** is a Kaspr app of its own too, but it is not sent deliveries. Every tick
the control plane sends it (07 §7.13) carries the maintenance settings resolved for that minute —
the runtime scope's overrides on top of the runtime spec's environment
(`services/table_maintenance.py::maintenance_settings`) — and the app uses the values of the
tick it last received for that table. A change applies within a minute, to stopped
materializations' tables too, and the app has no revision to report: the Settings page's
rollout covers runtimes only. A runtime ignores the maintenance fields it is delivered.

**Rollout.** Each member reports the revision and lineage it runs as `settings_revision` and
`settings_lineage` on its heartbeat (`""` while its settings have no lineage); the control plane
keeps them per member (`MemberProgress`). `GET /settings/runtime` reports the scope's `lineage` and
summarises how far the current revision has reached:

| State | Meaning |
|---|---|
| `current` | Every member heard from in the last two minutes runs the current revision of the current lineage |
| `pending` | A member still runs an older revision, or settings of another lineage — an earlier installation's. Its `revision` is then `null`: a number from another history says nothing |
| `stopped` | The materialization is stopped; it takes the current settings when it resumes |
| `not_reporting` | No member has reported in the last two minutes |
| `unsupported` | Its runtime predates live settings; it takes them at its next deploy |

A runtime from before 4.1 reports no lineage and keeps comparing revisions alone; the rollout
judges it by its revision, as before. Upgrading re-applies it within minutes, and the re-applied
runtime usually holds its revision without a lineage: its old member received the start's delivery
and the re-apply's, and ignored both, being on that revision already. The document keeps the
revision its lineage began at (`lineage_revision`), and a member on that revision without a
lineage, with nothing changed since, runs exactly the settings the lineage began from: it counts as
`current`. The next change reaches it under the lineage.

**Version skew.** 1.0 runtimes and control planes run together only at 1.0: the fresh-start
rollout (12 §12.13) leaves no older runtime. Upgrading the control plane changes every runtime's package pin, so the sweep
re-applies every runtime within about two minutes, and each re-apply delivers the current settings.

## 17.8 The console's settings

The console stores its overrides in the `console` scope and reads them back:

- at start, every 30 seconds after, and right after a save (`console/server/settings/store.ts`);
- each stored value is validated against the console's own catalog, and one this console does not
  accept — written by an older or newer console — is ignored and logged;
- valid overrides become the live values behind the console's `config` object, so every replica
  converges on the same values within 30 seconds.

A console change request carries `fallbacks` — what each changed setting reads as when it is not
overridden — because the control plane cannot know the console's defaults, and the change history
should read on its own.

**Saving.** `PATCH /api/settings` takes one change per scope, validates console values itself,
forwards control-plane then runtime then console changes, and stops at the first refusal. It then
waits — up to 10 seconds — until each request id appears in its scope's `recent_request_ids`
(applied) or as its `last_rejection` (refused), and answers with the settings as they now are:
`200` when applied, `202` when the control plane accepted but has not applied them yet, `409`
when refused.

## 17.9 API

| Method | Path | Answer |
|---|---|---|
| `GET` | `/settings/{scope}` | The scope: `revision`, `updated_at/by`, `recent_request_ids`, `last_rejection`; for `control-plane` and `runtime` every setting with its definition, `value`, `source`, `default_value`, `deployment_value`, `deployment_set`, `overridden`, `updated_at/by`, plus the read-only `deployment` entries; for `runtime` also `rollout`; for `console` the stored `values` |
| `PATCH` | `/settings/{scope}` | Body `{ "values": {…}, "reset": […], "base_revision": n }`; `202` on acceptance, `400 invalid_request` with `details.field`, `409 conflict` with `details.reason = settings_changed`, `404` for an unknown scope |
| `GET` | `/settings/{scope}/history?limit=` | The scope's recorded changes, newest first (at most 100) |

```bash
# Read, change, and reset a setting, signed in as someone with settings.manage
# (AUTH="authorization: Bearer <token from POST /auth/login>", 18 §18.10)
curl -s localhost:6065/settings/control-plane -H "$AUTH" | python3 -m json.tool
curl -s -X PATCH localhost:6065/settings/control-plane -H "$AUTH" \
     -H 'content-type: application/json' -d '{"values":{"max_runtime_replicas":32},"base_revision":0}'
curl -s -X PATCH localhost:6065/settings/control-plane -H "$AUTH" \
     -H 'content-type: application/json' -d '{"reset":["max_runtime_replicas"]}'
curl -s localhost:6065/settings/control-plane/history -H "$AUTH" | python3 -m json.tool
```

Full shapes: [04 — HTTP API reference](./04-http-api-reference.md).

## 17.10 Operating it

| Symptom | Cause | Action |
|---|---|---|
| Every save fails with `500`, and the control-plane log shows `KeyError: '…<release>-settings-requests'` | The consumer group still runs on an assignment made before 0.5.0 added the settings topics; restarts do not change it (§17.12) | Force one rebalance, as §17.12 describes. Once per environment |
| A save is refused: "These settings changed after this page loaded" | Someone else saved the same scope first | Reload the page; edits are kept |
| A runtime stays `pending` | A member has not refreshed: it is busy in a long schema wait, or not consuming | It catches up on its next record or heartbeat; with protocol logging on, `runtime_settings_applied` is logged when a member stores it |
| A runtime stays `pending` with no revision shown | It runs settings of another lineage — an earlier installation's — and its settings agent has not read this one's delivery yet | It takes them when the agent reads it. If it does not within minutes, the agent is not consuming: see the runtime's log, and re-apply it (stop and resume) |
| A runtime is `unsupported` | It runs a package older than 0.5.0 | It takes the settings at its next apply; resume it or wait for the sweep |
| A runtime logs `Ignoring runtime settings revision N` | The overrides do not fit that runtime's environment | Fix the override; the runtime keeps its own values meanwhile |
| An env var change "does nothing" | An override for the same setting wins | Reset the override |

Inspecting the store directly:

```bash
kafka-console-consumer --bootstrap-server $BROKERS --from-beginning --property print.key=true \
  --topic midstream-control-plane-app-develop-settings-changelog
```

## 17.11 Who can change settings

Reading settings needs `settings.view`; changing or resetting any of them needs
`settings.manage`, which only the built-in Administrator role holds by default
([18 — Identity and access](./18-identity-and-access.md) §18.3). The Settings page shows its
settings sections read-only to people who may view but not change them, and hides them from
people who may do neither. Every override and history entry records `updated_by` as the email of
the signed-in person who made it.

`settings.manage` covers every setting, the Security section and **Delete object data** included;
there is no per-section permission. The console's own sections are stored with the control plane
the same way, so the same permission governs them.

Storage is keyed by scope, so a per-user scope (`user:<id>`) could hold personal preferences
later; the console's Account section is where they would appear.

## 17.12 Design notes and known edges

**Why not a separate KasprApp for settings.** Kaspr binds tables to an operation only from its own
app, and tables are written only by agents processing events, so a separate settings app could
never be read by the control plane directly. The control plane would need its own synchronised copy
of every setting, fed by a topic — a second store in front of the one it actually reads, plus a
second deployable that could be down or on another version. Settings therefore live inside the
control plane, as a self-contained module with its own table, topic, agent and views, which is the
seam to cut along if application data ever grows into a service of its own.

**A release that adds topics needs one rebalance.** Kaspr runs every app with static group
membership: `group.instance.id` is the pod name (`K_CONSUMER_GROUP_INSTANCE_ID`). When a static
member rejoins within the broker session timeout (`K_BROKER_SESSION_TIMEOUT`, 120 s by default),
the broker hands back the group's stored assignment and does not rebalance. That is the point of
static membership. It also means topics a release adds to the control plane are never assigned:
0.5.0 adds the settings request topic and the `settings` table's changelog, and the pod's
subscription lists them, but the stored assignment, computed before they existed, does not.
Restarting the pod changes nothing, because it rejoins the same way. Two things break:

- Faust's transactional producer looks up a topic's partition group in that assignment, so the
  `settings-update` WebView's send fails with `KeyError` and every save answers `500`.
- The `settings-intake` agent is assigned no partitions, so it consumes nothing.

To confirm it, list the member's assignment. The two settings topics are missing from it:

```bash
kafka-consumer-groups.sh --bootstrap-server $BROKERS --describe --members --verbose \
  --group midstream-control-plane-app
```

To fix it, once per environment, stop the control plane for longer than the session timeout so
that the broker drops the member. Its next join then runs a full assignment:

```bash
kubectl patch kasprapp midstream-control-plane -n midstream --type merge -p '{"spec":{"replicas":0}}'
# wait until the pod is gone, then longer than K_BROKER_SESSION_TIMEOUT (120 s by default)
kubectl patch kasprapp midstream-control-plane -n midstream --type merge -p '{"spec":{"replicas":1}}'
```

Runtimes keep running meanwhile. The console shows settings read-only until the control plane
answers. Removing the static member through the Kafka admin API (`removeMembersFromConsumerGroup`,
KIP-345) has the same effect without the wait.

This applies to any release that adds a topic or table to a Kaspr app. Runtime settings travel on
a topic every runtime already consumes (§17.7), so runtimes are not affected. The lasting fix
belongs in Kaspr: after a static rejoin, a member whose assignment lacks topics it subscribes to
should request a rebalance.

**Why maintenance settings ride on the ticks.** The maintenance app cannot read the `settings`
table either. A delivery of its own would need the app to keep the values, a revision to compare,
and a catch-up when it starts. The ticks already reach it every minute, so they carry the
settings: the app keeps nothing between them, and a restarted member has every value within a
minute.

**Revisions only increase, within a lineage.** Losing the `settings` table and its changelog
restarts revisions, and the overrides are gone with it. The next start begins a later lineage, and
runtimes follow it: they drop the lost overrides and take whatever is set from then on. Restoring an
older copy of the changelog instead keeps the old lineage at an older revision, which runtimes
ignore until it passes theirs — so let a lost table start afresh rather than restore an old copy.

## 17.13 Adding a setting

Control plane or runtime:

1. Read the value where it is used, from the resolved config (`cfg.<field>` in a service that is
   passed `config`), never captured at import.
2. Add a `SettingDefinition` to `CONTROL_PLANE_SETTINGS` (or `RUNTIME_SETTINGS` plus
   `LIVE_SETTING_FIELDS`/`LIVE_SETTING_ENV_VARS`) with its section, label, description, bounds and
   `effect`. The console needs no change: it renders whatever the catalog describes.
3. If a new handler reads configuration, bind the `settings` table to its operation and pass it
   (`SettingsWiringTests` enforces this).
4. Tests: `test_settings.py` checks every catalog key is a config field read from its env var and
   that every primary setting has a description.

The console: see the console handbook,
[15 — Extending the console](../../console/docs/15-extending-the-console.md).
