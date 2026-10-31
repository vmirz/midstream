# 03 — Configuration Reference

There are two independent configuration objects. Both are frozen dataclasses. Neither reads
configuration files — only environment variables and code defaults.

| Object | Module | Loaded by | Cached |
|--------|--------|-----------|--------|
| `ControlPlaneConfig` | `midstream/config.py` | `get_config()` | Yes — `@lru_cache(maxsize=1)` |
| `RuntimeAppConfig` | `midstream/runtime_app/config.py` | `get_runtime_app_config()` | **No** — rebuilt on every call |

Since 0.5.0 the environment is no longer the last word for every field. Settings that are safe to
change while the system runs can be **overridden** from the console's Settings page. An override
is stored in the control plane's `settings` table and wins over the environment variable until it
is reset. The fields that can be overridden are listed in §3.2.5 and §3.3.1. Everything about how
overrides are stored, validated and delivered is in [17 — Settings](./17-settings.md).

## 3.1 The optional-config pattern

Every function that needs configuration accepts an optional override and resolves it once:

```python
# code-derived
def some_service(table, event, *, config: ControlPlaneConfig | None = None):
    cfg = get_config() if config is None else config
    ...
```

This is the required pattern. Do **not** call `os.getenv` at a call site. Tests rely on being
able to inject a `ControlPlaneConfig(...)` instance directly.

Because `get_config()` is `lru_cache`d, changing an environment variable after first call has
no effect in-process. Tests that need different values construct the dataclass directly rather
than mutating `os.environ`.

Handlers, which sit at the edge where Kaspr calls in, resolve the **effective** configuration
instead: the environment's, with the stored overrides applied. They resolve it once per event and
pass it down, so services keep the pattern above:

```python
# code-derived (handlers/request_intake.py and the other handlers)
cfg = resolve_config(settings_table) if config is None else config
```

`resolve_config` (`settings/effective.py`) caches the merge against the settings document's
revision. With no settings table it returns `get_config()` unchanged.

## 3.2 Control-plane configuration

### 3.2.1 Full field table

| Field | Env var | Default | Validation |
|-------|---------|---------|------------|
| `control_plane_table_name` | `CONTROL_PLANE_TABLE_NAME` | `state` | none |
| `default_namespace` | `CONTROL_PLANE_DEFAULT_NAMESPACE` | the pod's own namespace, read from its service-account mount | the Kubernetes namespace every runtime runs in (a create request cannot choose another); with neither the variable nor the mount, the configuration refuses to load |
| `default_runtime_name_prefix` | `CONTROL_PLANE_RUNTIME_NAME_PREFIX` | `kaspr` | none; a runtime is named `<prefix>-<materialization_id>`, never by its request |
| `default_runtime_template_version` | `CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION` | `v1` | none |
| `runtime_app_spec` | `CONTROL_PLANE_RUNTIME_APP_SPEC` | `{}` | must parse to a YAML mapping |
| `default_list_limit` | `CONTROL_PLANE_DEFAULT_LIST_LIMIT` | `"30"` | one of `10`, `30`, `100`, `all` |
| `default_destination_uri` | `CONTROL_PLANE_DEFAULT_DESTINATION_URI` | `None` | trimmed, trailing slashes dropped; blank is `None`. Where every new materialization's tables are created; a create request cannot name another, and a materialization keeps the one it was created with. `gs://<bucket>[/<prefix>]`, `s3://<bucket>[/<prefix>]` or `abfss://<container>@<account>.dfs.core.windows.net[/<prefix>]`, at most 1,024 characters (`lakehouse/store/uris.py::destination_problem`, 17 §17.3); `file:///<path>` only with `allow_file_destinations`. `None`, or any other value, refuses every create (`409 destination_not_configured`, saying why). Reported by `GET /system/settings` |
| `allow_file_destinations` | `CONTROL_PLANE_ALLOW_FILE_DESTINATIONS` | `False` | boolean parse; on, a destination may also be `file:///<absolute path>` on the local disk: for tests and a laptop, where every app and runtime shares that disk. Never set by the chart, which refuses a `file://` destination |
| `default_runtime_replicas` | `CONTROL_PLANE_DEFAULT_RUNTIME_REPLICAS` | `1` | integer, `>= 1`; replicas for a create request that names none |
| `max_runtime_replicas` | `CONTROL_PLANE_MAX_RUNTIME_REPLICAS` | `64` | integer, `>= 1`; the most a create or update may ask for |
| `runtime_autoscaling_enabled` | `CONTROL_PLANE_RUNTIME_AUTOSCALING_ENABLED` | `True` | boolean parse; on, a runtime far behind its topics gets more members until it has caught up, unless its materialization turned autoscaling off; off, none does, and scaled runtimes go back to their own count at the next lag measurement (07 §7.12a) |
| `runtime_autoscale_max_replicas` | `CONTROL_PLANE_RUNTIME_AUTOSCALE_MAX_REPLICAS` | `16` | integer, `>= 1`; the most members autoscaling gives one runtime — fewer when its topics' partitions keep fewer busy, never below its own count. Independent of `max_runtime_replicas` |
| `runtime_autoscale_target_minutes` | `CONTROL_PLANE_RUNTIME_AUTOSCALE_TARGET_MINUTES` | `30` | integer, `>= 1`; the catch-up time autoscaling scales for: a runtime that would take longer at its pace gets members enough to make it |
| `runtime_autoscale_min_lag_records` | `CONTROL_PLANE_RUNTIME_AUTOSCALE_MIN_LAG_RECORDS` | `100000` | integer, `>= 1`; the lag floor: no runtime this close is scaled up, and a scaled one goes back after three measurements at or under it |
| `runtime_autoscale_cooldown_seconds` | `CONTROL_PLANE_RUNTIME_AUTOSCALE_COOLDOWN_SECONDS` | `300` | integer, `>= 60`; the least time between two changes of a runtime's members |
| `default_runtime_storage_size` | `CONTROL_PLANE_DEFAULT_RUNTIME_STORAGE_SIZE` | `64Mi` | a Kubernetes quantity; every new runtime member's persistent volume, which holds only its state table (10 §10.8). A create request cannot choose another. Not the control plane's own volume, which its KasprApp sets (1Gi) |
| `default_partition_granularity` | `CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY` | `day` | `hour`, `day`, `month` or `none`; a new materialization's tables' partitioning when its create request names none |
| `default_protection` | `CONTROL_PLANE_DEFAULT_PROTECTION` | `email=hash,phone=hash,national_id=hash,secret=redact` | kinds of sensitive data and their masks, `kind=mask` separated by commas, a kind alone taking its default mask, empty for none; normalised to the catalog's order, and an unknown kind or a mask its kind does not allow stops the control plane at start, naming the variable. What a create request that names no `protection` gets (22 §22.4) |
| `max_materialization_topics` | `CONTROL_PLANE_MAX_MATERIALIZATION_TOPICS` | `20` | integer, `>= 1`; the most source topics one create request may name — each a table, all read by one runtime. Reported by `GET /system/settings` as `max_topics` |
| `destination_cleanup_enabled` | `CONTROL_PLANE_DESTINATION_CLEANUP_ENABLED` | `true` | delete drops the table: every object under its location — data, manifests, metadata, the pointer. Disable to keep the table; the delete chain then ends after the Kafka cleanup |
| `purge_records_on_delete` | `CONTROL_PLANE_PURGE_RECORDS_ON_DELETE` | `true` | delete removes every control-plane record as its final step; disable to leave them for the retention sweep |
| `deleted_retention_hours` | `CONTROL_PLANE_DELETED_RETENTION_HOURS` | `168` | how long the retention sweep leaves a deleted materialization's records before collecting them |
| `deleted_purge_batch_limit` | `CONTROL_PLANE_DELETED_PURGE_BATCH_LIMIT` | `50` | integer, `>= 1`; deleted materializations the retention sweep collects per run |
| `consumer_lag_refresh_batch_limit` | `CONTROL_PLANE_CONSUMER_LAG_REFRESH_BATCH_LIMIT` | `100` | integer, `>= 1`; materializations whose consumer lag one pass measures |
| `topic_analysis_max_topics` | `CONTROL_PLANE_TOPIC_ANALYSIS_MAX_TOPICS` | `20` | integer, `>= 1`; most topics one `POST /topics/analyze` may inspect |
| `materialization_requests_topic` | `CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC` | `materialization-requests` | none |
| `materialization_work_topic` | `CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC` | `materialization-work` | none |
| `materialization_status_topic` | `CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC` | `materialization-status` | none |
| `schema_authority_updates_topic` | `CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC` | `materialization-schema-authority-updates` | none; the runtime control topic: runtime settings (06 §6.7) |
| `table_events_topic` | `CONTROL_PLANE_TABLE_EVENTS_TOPIC` | `materialization-table-events` | none; what runtimes' committers and the maintenance app publish (06 §6.6) |
| `storage_prefix` | `CONTROL_PLANE_STORAGE_PREFIX` | `materializer` | trimmed, slashes stripped; the directory under the destination that every materialization's tables are created in: `<destination>/<storage_prefix>/<materialization id>/<table>` (11 §11.1). Not a namespace — a table's namespace is its dataset. Fixed for the deployment |
| `catalog_base_url` | `CONTROL_PLANE_CATALOG_BASE_URL` | `None` | trimmed, trailing slash dropped; the address engines reach the REST catalog at, shown in the console's connection snippets |
| `catalog_api_enabled` | `CONTROL_PLANE_CATALOG_API_ENABLED` | `true` | boolean parse; off, every `/catalog` endpoint answers `503` |
| `catalog_metadata_cache_seconds` | `CONTROL_PLANE_CATALOG_METADATA_CACHE_SECONDS` | `5` | integer, `>= 1`; how long a loaded table is answered again from memory |
| `discovery_sample_records` | `CONTROL_PLANE_DISCOVERY_SAMPLE_RECORDS` | `200` | integer, `>= 1`; records discovery reads to build schema 0 |
| `discovery_sample_seconds` | `CONTROL_PLANE_DISCOVERY_SAMPLE_SECONDS` | `5` | integer, `>= 1`; how long discovery waits for them |
| `settings_requests_topic` | `CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC` | `<release>-settings-requests` | none; the topic the settings WebView publishes changes to |
| `identity_requests_topic` | `CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC` | `<release>-identity-requests` | none; the topic the identity views and tasks publish to |
| `session_idle_timeout_minutes` | `CONTROL_PLANE_SESSION_IDLE_TIMEOUT_MINUTES` | `480` | integer, `>= 1`; how long a session lasts unused |
| `session_max_lifetime_hours` | `CONTROL_PLANE_SESSION_MAX_LIFETIME_HOURS` | `168` | integer, `>= 1`; how long a session lasts however active |
| `session_refresh_minutes` | `CONTROL_PLANE_SESSION_REFRESH_MINUTES` | `10` | integer, `>= 1`; how often a client renews an active session |
| `password_min_length` | `CONTROL_PLANE_PASSWORD_MIN_LENGTH` | `12` | integer, `>= 8`; the shortest password anyone may choose or be given |
| `sign_in_max_failed_attempts` | `CONTROL_PLANE_SIGN_IN_MAX_FAILED_ATTEMPTS` | `10` | integer, `>= 1`; wrong passwords in a row before an email is locked out |
| `sign_in_lockout_minutes` | `CONTROL_PLANE_SIGN_IN_LOCKOUT_MINUTES` | `15` | integer, `>= 1` |
| `audit_log_retention_days` | `CONTROL_PLANE_AUDIT_LOG_RETENTION_DAYS` | `90` | integer, `>= 1` |
| `live_tail_enabled` | `CONTROL_PLANE_LIVE_TAIL_ENABLED` | `True` | boolean parse; on, every materialization's tables are tailed; off, none (19) |
| `live_tail_max_rows_per_table` | `CONTROL_PLANE_LIVE_TAIL_MAX_ROWS_PER_TABLE` | `250000` | integer, `>= 1000`; one table's uncommitted records the tail holds, and the backlog bound |
| `live_tail_max_bytes_per_table` | `CONTROL_PLANE_LIVE_TAIL_MAX_BYTES_PER_TABLE` | `268435456` | integer, `>= 8 MiB`; one table's tail memory |
| `live_tail_retention_seconds` | `CONTROL_PLANE_LIVE_TAIL_RETENTION_SECONDS` | `120` | integer, `>= 10`; how long committed records stay in a tail |
| `live_tail_fetch_wait_ms` | `CONTROL_PLANE_LIVE_TAIL_FETCH_WAIT_MS` | `200` | integer, `>= 50`; the tail's Kafka fetch wait |
| `live_tail_max_records_per_second_per_table` | `CONTROL_PLANE_LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE` | `5000` | integer, `>= 100`; one table's ingest ceiling |
| `live_tail_peek_max_rows` | `CONTROL_PLANE_LIVE_TAIL_PEEK_MAX_ROWS` | `1000` | integer, `>= 10`; the most records one head peek reads |
| `live_tail_idle_seconds` | `CONTROL_PLANE_LIVE_TAIL_IDLE_SECONDS` | `600` | integer, `>= 0`; how long a table stays tailed after its last query; `0` never parks |
| `protocol_logging_enabled` | `PROTOCOL_LOGGING_ENABLED` | `False` | boolean parse |
| `protocol_log_slow_wait_seconds` | `PROTOCOL_LOG_SLOW_WAIT_SECONDS` | `30` | integer, `>= 1` |
| `schema_history_limit` | `CONTROL_PLANE_SCHEMA_HISTORY_LIMIT` | `50` | integer, `>= 1`; schema projections kept per materialization (05 §5.5) |
| `topic_catalog_ttl_seconds` | `CONTROL_PLANE_TOPIC_CATALOG_TTL_SECONDS` | `15` | integer, `>= 1`; how long `GET /topics` answers from memory |
| `topic_catalog_min_refresh_seconds` | `CONTROL_PLANE_TOPIC_CATALOG_MIN_REFRESH_SECONDS` | `5` | integer, `>= 1`; floor between topic-list loads, forced and failed ones included |
| `kaspr_app_name` | `K_APP_NAME` (set by Kaspr) | `None` | none; topics prefixed with it are this control plane's own |

1.0 retired `schema_proposals_topic`, `schema_proposal_debounce_seconds` and the three
`object_manifest_*` fields with what they configured; their variables are ignored.

The identity secrets — `AUTH_SIGNING_KEY`, `…_PREVIOUS_SIGNING_KEY`,
`…_SERVICE_TOKEN`, `…_BOOTSTRAP_ADMIN_EMAIL`, `…_BOOTSTRAP_ADMIN_PASSWORD`,
`…_BOOTSTRAP_ADMIN_FIRST_NAME`, `…_BOOTSTRAP_ADMIN_LAST_NAME` — are deliberately **not**
`ControlPlaneConfig` fields, so no settings view can ever report one. They are read once by
`identity/secrets.py::get_identity_secrets`; keys and the service token shorter than 32
characters are ignored and logged. See [18 — Identity and access](./18-identity-and-access.md)
§18.11. The protection key, `PROTECTION_KEY`, is not a field either: `protection/secrets.py::get_protection_key`
reads it once, on the control plane, the tail and maintenance apps and every runtime alike, ignores
and logs one shorter than 32 characters, and only its id is ever shown (22 §22.8).

### 3.2.2 Value parsing rules

**Booleans** (`_parse_bool_env`):

| Accepted true | Accepted false |
|---------------|----------------|
| `1`, `true`, `yes`, `on` | `0`, `false`, `no`, `off` |

Comparison is case-insensitive and whitespace-stripped. Anything else raises:

```
ValueError: boolean environment values must be one of: 1, 0, true, false, yes, no, on, off
```

**Integers** (`_parse_int_env`): must parse as `int` and be `>= minimum`, otherwise
`ValueError: <NAME> must be an integer` or `ValueError: <NAME> must be >= <minimum>`.

**List limit** (`normalize_list_limit`): `None` or `""` → `"30"`. Otherwise must be one of
`"10"`, `"30"`, `"100"`, `"all"`, else
`ValueError: limit must be one of: 10, 30, 100, all`.

**Runtime app spec** (`_load_runtime_app_spec`): `None` or `""` → `{}`. Otherwise parsed with
`yaml.safe_load`. `None` result → `{}`. Non-mapping result raises
`ValueError: CONTROL_PLANE_RUNTIME_APP_SPEC must parse to a mapping`.

### 3.2.3 Derived helper

```python
# code-derived
def runtime_name_for_materialization(self, materialization_id: str) -> str:
    return f"{self.default_runtime_name_prefix}-{materialization_id}"
```

So `mat-a1b2c3d4e5f6` becomes `midstream-mat-a1b2c3d4e5f6` for a release named `midstream`.

### 3.2.4 Which variables the chart actually sets

Set in `charts/midstream/templates/control-plane/app.yaml`:

- `CONTROL_PLANE_DEFAULT_NAMESPACE`
- `CONTROL_PLANE_DEFAULT_LIST_LIMIT`
- `CONTROL_PLANE_RUNTIME_NAME_PREFIX`
- `CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION`
- `CONTROL_PLANE_RUNTIME_APP_SPEC`
- the requests, work, status, runtime control and table-events topic variables,
  `CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC` and
  `CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC`
- `CONTROL_PLANE_STORAGE_PREFIX` (the release name, unless `controlPlane.settings.storagePrefix` says otherwise)
- `CONTROL_PLANE_DELETED_RETENTION_HOURS`, `…_DEFAULT_RUNTIME_STORAGE_SIZE`,
  `…_DEFAULT_RUNTIME_REPLICAS`, `…_DEFAULT_DESTINATION_URI`
- `AUTH_SIGNING_KEY`, `…_PREVIOUS_SIGNING_KEY`, `…_SERVICE_TOKEN`,
  `…_BOOTSTRAP_ADMIN_EMAIL` and `…_BOOTSTRAP_ADMIN_PASSWORD`, from the `<release>-auth` Secret
- `PROTOCOL_LOGGING_ENABLED` = `"true"`
- the lake's storage variables, from `storage.*`: each backend's credentials as Secret references and
  the `STORAGE_*` settings (§3.3a; consumed by the object stores, not by `ControlPlaneConfig`)

**Not set anywhere** (code defaults apply):

- `CONTROL_PLANE_TABLE_NAME`
- `PROTOCOL_LOG_SLOW_WAIT_SECONDS`
- `CONTROL_PLANE_SCHEMA_HISTORY_LIMIT`
- `CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY`, `…_DISCOVERY_SAMPLE_RECORDS`,
  `…_DISCOVERY_SAMPLE_SECONDS`
- `CONTROL_PLANE_CATALOG_BASE_URL` — set it per environment to the address engines
  use, or the console's snippets show a placeholder — `…_CATALOG_API_ENABLED`,
  `…_CATALOG_METADATA_CACHE_SECONDS`
- the seven session, password, lockout and audit settings (Settings page overrides apply instead)
- `CONTROL_PLANE_ALLOW_FILE_DESTINATIONS`: `file://` destinations serve tests only, and the chart
  refuses one

Note that `control_plane_table_name` is a config field, but the table is bound by the Kaspr
agent/webview `tables:` block (`name: <release>-state`), not by this variable.
Changing the env var alone will **not** rebind the table. The `settings`, `identity` and
`identity-audit` tables have no variable at all; they are bound the same way (`name: <release>-settings`,
and so on).

### 3.2.5 Which fields the Settings page can override

48 fields, listed with their bounds in [17 — Settings](./17-settings.md) §17.3:

| Section | Fields |
|---|---|
| Materializations | `default_destination_uri`, `default_runtime_replicas`, `max_runtime_replicas`, `default_runtime_storage_size`, `default_partition_granularity`, `runtime_autoscaling_enabled`, `runtime_autoscale_max_replicas`, `runtime_autoscale_target_minutes`, `runtime_autoscale_min_lag_records`, `runtime_autoscale_cooldown_seconds`, `max_materialization_topics` |
| Data protection | `default_protection` — which also needs `settings.manage_protection` (22 §22.10) |
| Deletion | `destination_cleanup_enabled`, `purge_records_on_delete`, `deleted_retention_hours`, `deleted_purge_batch_limit` |
| Schema | `discovery_sample_records`, `discovery_sample_seconds`, `schema_history_limit` |
| Catalog | `catalog_api_enabled`, `catalog_metadata_cache_seconds` |
| Jobs | `consumer_lag_refresh_batch_limit` |
| Topics | `topic_catalog_ttl_seconds`, `topic_catalog_min_refresh_seconds`, `topic_analysis_max_topics` |
| Diagnostics | `protocol_logging_enabled` |
| Security | `session_idle_timeout_minutes`, `session_max_lifetime_hours`, `session_refresh_minutes`, `password_min_length`, `sign_in_max_failed_attempts`, `sign_in_lockout_minutes`, `audit_log_retention_days` |
| Sign-in | `sso_entra_enabled`, `sso_github_enabled`, `sso_google_enabled`, `sso_oidc_enabled`, `sso_first_sign_in`, `sso_allowed_email_domains`, `password_sign_in_enabled` |
| Live tail | `live_tail_enabled`, `live_tail_max_rows_per_table`, `live_tail_max_bytes_per_table`, `live_tail_retention_seconds`, `live_tail_fetch_wait_ms`, `live_tail_max_records_per_second_per_table`, `live_tail_peek_max_rows`, `live_tail_idle_seconds` |

The rest stay with the deployment — topic and table names, the namespace and runtime naming, the
storage prefix and the catalog's base URL, how the object stores reach `s3://` and `abfss://`
destinations and whether `file://` ones are allowed, the runtime template and spec, and credentials —
and the Settings page lists them read-only, saying of an identity secret only whether it is
configured and nothing of a storage credential (17 §17.3). `protocol_log_slow_wait_seconds`
is not offered for the control plane because nothing there reads it; runtimes use it, and the
Runtimes section offers it.

## 3.3 Runtime configuration

`RuntimeAppConfig` (`runtime_app/config.py`) is a frozen dataclass. **33 of its fields are live
settings** (§3.3.1): read from the environment, and changeable while runtimes run from the Settings
page's Runtimes, Commits, Schema and Maintenance sections. The control plane delivers a change to
every runtime, and `get_runtime_app_config()` returns the environment's values with the delivered
overrides applied (17 §17.7). The rest (§3.3.2) are constants of the release.

### 3.3.1 Live fields

Every one is read from its environment variable when no override applies
(`LIVE_SETTING_ENV_VARS`, `runtime_app/live_settings.py`): `RUNTIME_<FIELD>` in upper
case, except the two protocol-logging fields, which read `PROTOCOL_LOGGING_ENABLED`
and `PROTOCOL_LOG_SLOW_WAIT_SECONDS`.

**Batches** (10 §10.3)

| Field | Default | Meaning |
|-------|---------|---------|
| `max_rows_per_file` | `250_000` | a batch rolls at this many rows |
| `max_open_file_age_seconds` | `120` | the soft age: past it a batch rolls once it holds `min_rows_per_file` rows or `min_file_size_bytes` |
| `min_rows_per_file` | `10_000` | |
| `min_file_size_bytes` | `8 MiB` | of staged JSONL |
| `max_open_file_age_ceiling_seconds` | `600` | the hard age: a batch rolls regardless; `>= max_open_file_age_seconds` |

**Commits** (10 §10.6)

| Field | Default | Meaning |
|-------|---------|---------|
| `commit_interval_seconds` | `30` | the committer commits pending files at most this often (sooner at the limits below) |
| `max_files_per_commit` | `500` | |
| `max_bytes_per_commit` | `8 GiB` | |
| `announce_retry_seconds` | `60` | a member announces an uploaded file again after this long while no commit covers it |

**Schema** (09)

| Field | Default | Meaning |
|-------|---------|---------|
| `schema_evolution_debounce_seconds` | `2` | the committer gathers schema change requests this long, then commits them together |
| `seal_schema_wait_seconds` | `30` | a batch holding values its schema could not take waits this long for the evolution; `0` allowed; `<= max_open_file_age_ceiling_seconds` |
| `max_forks_per_field` | `3` | a family with this many forks collapses to JSON |
| `max_struct_fields` | `300` | a struct with this many fields collapses to JSON |
| `max_nesting_depth` | `10` | deeper values are kept as JSON |
| `max_schema_fields` | `2_000` | past this, new fields are refused and their values go to `_unmapped` |

**Maintenance** (11 §11.6) — runtime settings that steer the maintenance app, not the runtimes: the
control plane resolves them, overrides included, and sends them with every tick (07 §7.13,
`services/table_maintenance.py::MAINTENANCE_SETTING_FIELDS`). A change applies at the app's next
tick, within a minute; the runtimes receive these fields too, and ignore them.

| Field | Default | Meaning |
|-------|---------|---------|
| `compaction_enabled` | `true` | |
| `compaction_interval_minutes` | `15` | how often the maintenance app looks for partitions worth compacting in each table; while it finds them, it compacts again at the next step |
| `compaction_min_input_files` | `8` | small files a partition needs before it is compacted |
| `compaction_small_file_bytes` | `64 MiB` | files smaller than this are compacted; `< compaction_target_file_size_bytes` |
| `compaction_target_file_size_bytes` | `256 MiB` | |
| `compaction_max_input_bytes` | `1 GiB` | data one job reads; small partitions are compacted together up to it |
| `compaction_hot_partition_delay_minutes` | `60` | the partition still being written - today, this hour - waits this long after its last commit; in this month's partition each file waits this long instead; `0` allowed |
| `compaction_timeout_minutes` | `30` | a job running longer is killed and counts as failed |
| `compaction_max_attempts` | `5` | failures in a row before a partition is skipped with `compaction_failed` |
| `snapshot_retention_hours` | `72` | snapshots older than this expire |
| `min_snapshots_to_keep` | `20` | |
| `snapshot_keep_all_minutes` | `60` | younger snapshots are all kept; older ones are thinned to one an hour |
| `snapshot_expiry_interval_minutes` | `60` | |
| `orphan_cleanup_enabled` | `true` | |
| `orphan_min_age_hours` | `24` | an unreferenced file younger than this is never deleted |
| `orphan_cleanup_interval_hours` | `24` | |

**Diagnostics**

| Field | Default | Meaning |
|-------|---------|---------|
| `protocol_logging_enabled` | `false` | the generated runtime spec sets it `true` |
| `protocol_log_slow_wait_seconds` | `30` | |

Every integer must be `>= 1`, except the two that allow `0`.

### 3.3.2 Fields of the release

| Field | Default | Meaning |
|-------|---------|---------|
| `heartbeat_interval` | `"30s"` | `emit-status` task interval |
| `upload_interval` | `"5s"` | `pipeline` task interval |
| `commit_timer_interval` | `"5s"` | `commit-timer` task interval; commits still wait for `commit_interval_seconds` |
| `staging_root_path` | `/tmp/materializer/staging` | where batches are staged; the maintenance app's compactions work under its `maintenance/` |
| `target_file_size_bytes` | `256 MiB` | a seal's target file size |
| `target_row_group_size_bytes` | `32 MiB` | rows per Parquet row group: as many as fit in this much staged JSONL |
| `upload_worker_count` | `4` | uploads a member runs at once; at most one per partition |
| `seal_worker_count` | `1` | seals a member runs at once, each in a child process |
| `max_pending_upload_batches` | `32` | backpressure: at this many pending batches, stop rolling and pause the source partitions |
| `max_pending_upload_bytes` | `2 GiB` | backpressure by size |
| `upload_initial_backoff_seconds` | `5` | first wait before retrying a failed upload or seal; doubles up to 300 s |
| `malformed_record_mode` | `continue_with_error` | `strict` re-raises instead of skipping a record |

Resource names are not configuration: `runtime_component_names()` (`runtime/naming.py`) derives
every child resource and the commits topic from the app name (02 §2.10).

### 3.3.3 Runtime-only environment variables not in `RuntimeAppConfig`

| Variable | Set by | Consumed by |
|----------|--------|-------------|
| `RUNTIME_MEMBER_ID` | Downward API (`metadata.name`) in the runtime app spec | `runtime_app/writer/context.py::current_member_id()` |
| The lake's storage variables (§3.3a) | the chart's runtime spec, from `storage.*` | the object stores that the writer's uploads, the committer and its FileIO (`ObjectStoreFileIO`) go through |
| `K_STREAM_WAIT_EMPTY` | the generated runtime spec, `false` | Kaspr's `stream_wait_empty`: Faust does not wait, on a rebalance or at shutdown, for delivered records to be acknowledged. The writer holds acknowledgements until rows are in the table (10 §10.7), so waiting would stall both; a record left unacknowledged is read again |
| `K_STORE_ROCKSDB_WRITE_BUFFER_SIZE`, `K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER` | the generated runtime spec, `4194304` and `2` | Kaspr's RocksDB write buffer for the state table, the app's one store: flushing every 4 MiB keeps its write-ahead log under 8 MiB on a 64Mi volume (10 §10.8). Kaspr's own default is 64 MiB |
| `PROTECTION_KEY` | the chart's runtime spec, from the auth Secret's `protection-key` (optional) | `protection/secrets.py`: the key the writer hashes with (22 §22.8) |
| `PROTECTION_KEY_ID` | the generated runtime spec, only when the control plane holds a key | the id of the deployment's key: a member whose key has another id, or none, writes nothing for a table that hashes and reports `protection_key_mismatch` or `protection_key_missing` |

The table's identity — location, namespace, name, partition granularity — reaches the runtime as
agent arguments in its templates, not as variables. The generated spec's env list is merged with
the deployment's `CONTROL_PLANE_RUNTIME_APP_SPEC` by variable name, the deployment's
entries winning.

`current_member_id()` stamps batch ids and data file names, and names the committer in the
heartbeat. A member deletes the staging files it finds under its own id when it starts; nothing
staged by an earlier process is resumed — it is read again from Kafka (10 §10.9).

## 3.3a Object storage

Every process that reads or writes the lake — the control plane, the maintenance and tail apps,
every runtime, and `python -m midstream.lakehouse.store.check` — builds the store of a URI scheme from
its environment the first time it meets that scheme (`lakehouse/store/registry.py`), and keeps it for
its lifetime: a changed variable takes a restart. None of these variables is a `ControlPlaneConfig`
field. The credential variables are each SDK's own, the names workload identities inject; Midstream's
own are the `STORAGE_*` variables (`StoreSettings`), which the S3 and Azure stores read. A store whose
credentials are absent is built all the same, since its SDK's default chain may find the pod's
identity; its first request then fails with the SDK's own error. The chart sets them from `storage.*`
(02 §2.9a); what each store does with them is 11 §11.1a.

| Variable | Scheme | Unset | Meaning | From the chart |
|---|---|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | `gs://` | application default credentials: GKE workload identity | the path of a Google service-account key | `/etc/midstream/gcs/service-account.json`, the key `service-account.json` of `storage.gcs.credentialsSecret` |
| `STORAGE_EMULATOR_HOST` | `gs://` | GCS itself | google-cloud-storage's own: an emulator's address, such as fake-gcs-server's. Set, the GCS store deletes one object per request, since the emulator cannot answer batch requests | never set |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `s3://` | the rest of boto3's default chain: IRSA, EKS Pod Identity, an instance role | a key pair | `access-key-id` and `secret-access-key` of `storage.s3.credentialsSecret`, both required |
| `AWS_SESSION_TOKEN` | `s3://` | none | the token of temporary keys | `session-token`, optional |
| `STORAGE_S3_ENDPOINT` | `s3://` | AWS | an S3-compatible server: an `http://` or `https://` URL, trailing slashes dropped. One endpoint serves every `s3://` URI of the process, so a deployment's `s3://` destinations share one server | `storage.s3.endpoint` |
| `STORAGE_S3_REGION` | `s3://` | `us-east-1` | the region requests are signed for; the store hands it to boto3 itself, so `AWS_REGION` does not change it | `storage.s3.region` |
| `STORAGE_S3_PATH_STYLE` | `s3://` | `false` | `1`/`0`, `true`/`false`, `yes`/`no`, `on`/`off`: the bucket in the path (`http://host/bucket/key`) rather than the host name, as most S3-compatible servers want | `"true"` when `storage.s3.pathStyle` is |
| `AZURE_STORAGE_CONNECTION_STRING` | `abfss://` | the account key | a connection string, used first. It serves its own account only — a URI of another account is refused (`AccountMismatch`) — and its endpoints are used as written | `connection-string` of `storage.azure.credentialsSecret`, optional |
| `AZURE_STORAGE_ACCOUNT_KEY` | `abfss://` | `DefaultAzureCredential` | without a connection string, the shared key of the account each URI names | `account-key`, optional |
| `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET` | `abfss://` | the rest of the chain | read by `DefaultAzureCredential` (azure-identity), which signs when neither of the above is set: a service principal from these three, AKS workload identity (whose webhook injects variables of its own), a managed identity. Built once per process | `client-id`, `tenant-id`, `client-secret`, each optional |
| `STORAGE_AZURE_ENDPOINT` | `abfss://` | `https://<account>.blob.core.windows.net` | an emulator's Blob endpoint, an `http://` or `https://` URL to which the account is appended: `http://azurite:10000` reaches `http://azurite:10000/<account>`. A connection string's own endpoints win over it | `storage.azure.endpoint` |

A `STORAGE_*` value the stores cannot use — an endpoint that is no `http(s)://` URL, a path style
that is no boolean — raises `ValueError` naming the variable when the S3 or Azure store is built, and
the Settings page shows it as `invalid: …` (17 §17.3). `file://` URIs need nothing: the file store
reads and writes the local disk, and a destination may be one only with `allow_file_destinations`
(§3.2.1).

The chart gives the console none of these. Its storage profiles come from `CONSOLE_STORAGE_FILE`,
or `CONSOLE_STORAGE` inline for development, each profile's credentials from
`CONSOLE_STORAGE_<NAME>_*`, and a profile without them reads as the console's own identity, through
its SDK's default chain (console 03 §3.6a).

## 3.4 Constants that behave like configuration but are not configurable

These are module-level constants with no environment override. Changing them requires a code
change and a package release.

### Observation policy — `observation/policy.py`

| Constant | Value | Meaning |
|----------|-------|---------|
| `DEFAULT_OBSERVATION_BACKOFF_SECONDS` | `(15, 45, 120, 300)` | Delay before observation attempt 1..4; attempts past 4 reuse `300` |
| `DEFAULT_HEARTBEAT_STALE_SECONDS` | `120` | A heartbeat older than this is stale |
| `DEFAULT_RESOURCE_OBSERVATION_STALE_SECONDS` | `600` | A resource observation older than this is stale |
| `DEFAULT_DESIRED_OBSERVED_MISMATCH_STALE_SECONDS` | `600` | A desired≠observed mismatch older than this is stale |

`max_attempts` on a generated `ObservationPolicy` is
`len(DEFAULT_OBSERVATION_BACKOFF_SECONDS)` = **4**.

### Runtime — `runtime_app/`

| Constant | Value | Meaning |
|----------|-------|---------|
| `ORDERED_BATCH_MAX_ROWS` (`acks.py`) | `5_000` | an ordered batch rolls at this many rows (10 §10.7) |
| `COMMIT_ATTEMPTS`, `COMMIT_SPACING_SECONDS` (`committer.py`) | `5`, `1.0` | tries at a commit that keeps losing the pointer's CAS, a second apart |
| `FAILURE_BACKOFF_SECONDS` (`committer.py`) | `(5, 300)` | backoff of a failing commit cycle |
| `ERROR_REPEAT_SECONDS` (`committer.py`) | `300` | a `table_error` of one reason is published at most this often |
| `STALE_LOG_SECONDS` (`committer.py`) | `60` | a stale announcement is logged at most this often per member and partition (10 §10.6) |
| `COMMITS_STALLED_INTERVALS` (`handlers/status.py`) | `10` | commit intervals a member's uploaded files may wait before it reports `commits_stalled` |
| `SEAL_TIMEOUT_SECONDS` (`pipeline.py`) | `1800` | a seal child running longer is killed |
| `BACKLOG_RESUME_FRACTION` (`pipeline.py`) | `0.75` | paused partitions resume once the backlog is down to this fraction of its limits |
| `COMPACTION_BACKOFF_SECONDS` (`maintenance_app/worker.py`) | `(300, 21600)` | a failing partition's backoff |
| `MAX_PLAN_INPUT_FILES` (`lakehouse/maintenance.py`) | `2_000` | input files one compaction takes at most |
| `ORPHAN_PASS_LIMIT` (`maintenance_app/worker.py`) | `1000` | files one orphan-cleanup pass deletes |
| `REPLACE_ATTEMPTS`, `REPLACE_RETRY_SECONDS` (`maintenance_app/worker.py`) | `8`, `1.0` | tries at a replace or an expiry that keeps losing the pointer's CAS to the runtime's appends |
| `STEP_BUDGET_SECONDS` (`maintenance_app/worker.py`) | `10` | quick maintenance work one step does across tables |
| `REPROTECT_WAIT_SECONDS` (`maintenance_app/worker.py`) | `30` | how often a reprotect waiting for the table to record its protection revision, or for the batches open under the older one, looks again |
| `REPROTECT_SETTLE_MARGIN_SECONDS` (`maintenance_app/worker.py`) | `120` | beyond the batch ceiling after a revision was recorded, what a reprotect allows the older revision's last batches to seal and commit before it is done |
| `STATES_LIMIT` (`protection/policy.py`) | `100` | per-revision states a protected field keeps in the table's record; files of older revisions become of unknown lineage (22 §22.14) |
| `PROTECTION_KEY_WAIT_SECONDS` (`handlers/writer.py`) | `15` | how often a member that must hash and cannot checks its key again (22 §22.5) |

### Data protection — `protection/`

| Constant | Value | Meaning |
|----------|-------|---------|
| `KINDS`, `DEFAULT_PROTECTION` (`catalog.py`) | 11 kinds; email, phone, national_id hashed, secret withheld | the kinds the rules recognise and the default as it ships (22 §22.2) |
| `HASH_HEX_LENGTH`, `PARTIAL_TAIL`, `HIDDEN` (`mask.py`) | `32`, `4`, `****` | a hashed value's hex characters (128 bits); the most significant characters a partial keeps; what hides the rest |
| `MIN_PROTECTION_KEY_LENGTH` (`secrets.py`) | `32` | a shorter key is ignored and logged |
| `HISTORY_LIMIT` (`policy.py`) | `100` | policies a table's `materializer.protection` keeps in its history |

### Object stores — `lakehouse/store/`, `storage/objects.py`

| Constant | Value | Meaning |
|----------|-------|---------|
| `BATCH_SIZE` (`gcs.py`), `DELETE_BATCH_SIZE` (`s3.py`), `BATCH_SIZE` (`azure.py`) | `100`, `1000`, `256` | deletes one request carries: a JSON API batch, a `DeleteObjects`, a blob batch. A GCS store that deletes one object per request (`STORAGE_EMULATOR_HOST` set) counts `1` |
| `MAX_RETRIES` (`s3.py`) | `5` | retries of a failed S3 request in botocore's standard mode, six attempts in all |
| `RETRY_TOTAL`, `RETRY_INITIAL_BACKOFF_SECONDS` (`azure.py`) | `3`, `1` | retries of a failed Azure request, the first after about a second, then growing |
| `MAX_POOL_CONNECTIONS` (`s3.py`), `POOL_SIZE` (`azure.py`) | `32` | pooled connections per host, where botocore and requests keep 10 by default |
| `CONNECT_TIMEOUT_SECONDS`, `READ_TIMEOUT_SECONDS` (`s3.py`, `azure.py`) | `10`, `120` | |
| `TRANSFER_CHUNK_BYTES`, `TRANSFER_CONCURRENCY` (`azure.py`) | `8 MiB`, `4` | an Azure upload larger than a chunk goes in blocks, the block-list commit carrying the condition; a larger download in chunks, each after the first under `If-Match` |
| `READ_ATTEMPTS` (`azure.py`) | `3` | whole reads of a blob that keeps changing under a chunked download |
| `CLEANUP_PASS_MAX_OBJECTS`, `CLEANUP_PASS_MAX_REQUESTS`, `CLEANUP_PASS_MAX_SECONDS` (`storage/objects.py`) | `1000`, `100`, `10.0` | one drop pass lists and deletes at most `max(CLEANUP_PASS_MAX_OBJECTS, CLEANUP_PASS_MAX_REQUESTS × delete_batch_size)` objects — 1,000 through a store that deletes one object per request, 10,000 in GCS's batches of 100, 25,600 in Azure's of 256, 100,000 in S3's `DeleteObjects` of 1,000 — and stops after about 10 s, checked between requests |
| `BATCH_RETRY_DELAYS_SECONDS` (`storage/objects.py`) | `(1.0, 2.0, 4.0)` | waits before a drop pass deletes again what failed in a way that may succeed later: `delete_many` retries nothing |

GCS requests are retried by google-cloud-storage with its own defaults, except inside a batch.

### The maintenance app (`maintenance_app/config.py`)

| Variable | Default | Meaning |
|---|---|---|
| `MAINTENANCE_APP_NAME` | `<release>-maintenance` | names the app in its commits and table events |
| `MAINTENANCE_MAX_CONCURRENT_JOBS` | `1` | long jobs one member runs at once, across its tables |
| `MAINTENANCE_FORGET_AFTER_SECONDS` | `300` (≥ 60) | a table no tick names for this long is forgotten, its job stopped |
| `PROTECTION_KEY` | none | the auth Secret's `protection-key`: a reprotect hashes with it, and refuses to run without it, or with another than the one the table's hashes were made with, when the table hashes (22 §22.14) |

Read from the app's environment (02 §2.10a); what the jobs do comes with the ticks.

### The tail app (`tail_app/config.py`)

| Variable | Default | Meaning |
|---|---|---|
| `TAIL_APP_NAME` | `<release>-tail` | the app, as its consumer group and the member map name it |
| `TAIL_CONTROL_PLANE_URL` | none | the control plane's Service: sessions, schemas, tables |
| `TAIL_SERVICE_TOKEN` | none | the console's credential, which every request must carry, and the app's own for schemas; at least 32 characters, never a config field |
| `TAIL_TABLE_EVENTS_TOPIC` | `materialization-table-events` | commits and schema changes |
| `TAIL_REQUESTS_TOPIC` | `<release>-tail-requests` | ticks and cancels |
| `TAIL_MEMORY_BUDGET_BYTES` | `2684354560` | resident bytes of a member's tables together |
| `TAIL_FORGET_AFTER_SECONDS` | `300` | a table no tick names for this long is forgotten |
| `TAIL_SESSION_CHECK_SECONDS` | `5` | how long a session the control plane answered for is trusted (60 s while it cannot be reached) |
| `TAIL_SERVE_CONCURRENCY`, `…_PEEK_CONCURRENCY` | `4`, `2` | answers and peeks built at once |
| `TAIL_MEMBER_MAP_REFRESH_SECONDS` | `15` | how often the member map is read again |
| `PROTECTION_KEY` | none | the auth Secret's `protection-key`: the tail masks what it buffers as the writers do, and answers `503 protection_key_missing` for a table that hashes when it has no key, or another than the deployment's (22 §22.5) |
| `RUNTIME_MEMBER_ID` | the pod's name | the member's name |

Read once at start (02 §2.10b); the nine `live_tail_*` settings come with every tick (§3.2.1, 17).
Constants: segments close at `SEGMENT_ROWS` 2,000 rows, `SEGMENT_BYTES` 4 MiB or
`SEGMENT_MAX_AGE_SECONDS` 5 (`buffer.py`); a table resumes from a pause or a backlog below
`RESUME_FRACTION` 0.75 of the bound and enters a backlog past `BACKLOG_ENTRY_BOUNDS` 2 bounds, and an
unreadable topic is tried again every `UNAVAILABLE_RETRY_SECONDS` 30 (`table_tail.py`); the source
consumer's fetches wait for `SOURCE_FETCH_MIN_BYTES` 64 KiB or the fetch wait, `read_committed`
(`consumer.py`); a member asks the control plane for an unticked table at most every
`RESOLVE_RETRY_SECONDS` 10 and looks its active tables' topics up every `TOPIC_CHECK_SECONDS` 30
(`worker.py`); a peek is kept `CACHE_SECONDS` 1 (`peek.py`).
| `FORMAT_VERSION`, `FIRST_PAYLOAD_FIELD_ID` (`lakehouse/tables.py`) | `2`, `10` | new tables' format version; field ids below 10 are the metadata columns |

### Naming bounds — `runtime/naming.py`

| Constant | Value |
|----------|-------|
| `MAX_KUBERNETES_NAME_LENGTH` | `63` |
| `MAX_RUNTIME_APP_NAME_LENGTH` | `59` |
| `MAX_RUNTIME_TABLE_NAME_LENGTH` | `57` |
| `TABLE_COMPONENT_SUFFIXES` | `{"state"}` |

The app name is capped at 59 (not 63) so operator-derived StatefulSet names such as
`<app>-app` stay within Kubernetes limits. Table names are capped at 57 to leave room for
Kaspr changelog topic suffixes.

### Workflow — `services/workflow.py`

`MAX_CLEANUP_KAFKA_RETRY_ATTEMPTS` bounds the retry budget for
`cleanup_runtime_kafka` when a consumer group is still non-empty.

## 3.5 Precedence and effective values in each environment

| Setting | Code default | The chart |
|---------|-------------|-----------|
| Namespace | none: the pod's own, or the variable | the release namespace (downward API) |
| Runtime name prefix | `kaspr` | the release name |
| Template version | `v1` | `v1` |
| List limit | `30` | `30` |
| Requests topic | `materialization-requests` | `<release>-materialization-requests` |
| Work topic | `materialization-work` | `<release>-materialization-work` |
| Status topic | `materialization-status` | `<release>-materialization-status` |
| Runtime control topic | `materialization-schema-authority-updates` | `<release>-materialization-schema-authority-updates` |
| Table events topic | `materialization-table-events` | `<release>-materialization-table-events` |
| Storage prefix | `materializer` | the release name |
| Destination | none: no create | `controlPlane.settings.defaultDestinationUri`, checked (02 §2.9a); unset |
| File destinations | off | off, never set |
| S3 endpoint, region, path style | AWS, `us-east-1`, off | `storage.s3.endpoint`, `region`, `pathStyle`; unset |
| Azure Blob endpoint | each account's own | `storage.azure.endpoint`; unset |
| Catalog base URL | — | (unset) |
| Protocol logging | `False` | `true` |
| Slow-wait threshold | `30s` | (unset → 30) |
| Table name | `state` | the KasprTable `<release>-state`, its `spec.name` `state` |
| Settings table | `settings` | the KasprTable `<release>-settings`, its `spec.name` `settings` |
| Settings request topic | `materializer-settings-requests` | `<release>-settings-requests` |
| Runtime CPU | — | 0.5 requested, 2 at most, the request then set from use ([02 §2.10c](./02-deployment-and-manifests.md)) |
| Runtime memory | — | 1Gi requested and at most |
| Runtime `topicAllowDeclare` | — | `true` |

For every field the Settings page can override (§3.2.5, §3.3.1), an override sits above all three
columns: **override → deployment value (the env var) → code default**. The page shows which one is
in force for each setting. A deployment that changes an env var does not beat an override; reset
the override to follow the deployment again.

## 3.6 How to change a setting safely

**Anything the Settings page offers** — change it there. It applies without a restart, is recorded
with who changed it, and can be reset. See [17 — Settings](./17-settings.md).

**Topic names** — change in three places or the pipeline breaks:
1. `charts/midstream/templates/control-plane/app.yaml` env var
2. the corresponding agent `spec.input.topic.name` / `spec.output.topics[].name`
3. the chart's templates, which derive it from the release

Then re-run `helm template midstream charts/midstream -f charts/midstream/ci/test-values.yaml` and the unit tests.

**Runtime behavior** (batch sizing, commit cadence, schema caps, maintenance) — change it on the
Settings page's Runtimes, Commits, Schema and Maintenance sections; running runtimes apply it within
seconds, and the maintenance app at its next tick, without a restart.
To change the deployment value instead, set the env var in the runtime spec
(`CONTROL_PLANE_RUNTIME_APP_SPEC` → `template.kasprContainer.env`). That changes
every runtime's spec hash, so the sweep re-applies, and restarts, every runtime. A compile-time
field (§3.3.2) still needs a package release, and runtimes pick it up only when their package pin
advances.

**Where new tables go** — the destination, on the Settings page or as
`controlPlane.settings.defaultDestinationUri`. It decides new materializations only: each keeps the
destination it was created with, so the apps and runtimes need credentials for every backend a
destination of theirs has named (02 §2.9a). A table never moves between backends — its metadata
names its files by their full URIs — so a move is a new materialization.

**Storage credentials and endpoints** — change the Secret, or `storage.*`, and roll out. A Secret's
new content reaches a pod when it next starts; a change of `storage.*` changes every runtime's spec,
so the sweep re-applies, and restarts, every runtime. `STORAGE_S3_ENDPOINT` and
`STORAGE_AZURE_ENDPOINT` are no part of any location, so a server that moves needs a values change
and a rollout, nothing in the bucket.

**Observation timing** — edit `observation/policy.py`. Affects the control plane only;
no runtime redeploy needed.

**The storage prefix** (`CONTROL_PLANE_STORAGE_PREFIX`) is part of every table's location: change it
only for a new deployment. Existing tables keep the location they were created with.

## 3.7 Protocol logging

When enabled, `protocol_logging.log_protocol_event()` emits a single INFO line per event:

```
protocol_event {"event":"control_plane_apply_runtime_completed","materialization_id":"mat-...","occurred_at":"2026-01-15T10:30:00Z",...}
```

The JSON is `sort_keys=True, separators=(",", ":")`. `None`-valued fields are dropped.
Every payload always contains `event` and `occurred_at`.

Complete list of event names (grep `event="` under `python/src`):

**Control plane**

```
control_plane_request_intake_completed
control_plane_request_intake_failed
control_plane_work_completed
control_plane_work_failed
control_plane_schema_discovery_completed
control_plane_create_table_completed
control_plane_apply_runtime_skipped
control_plane_apply_runtime_completed
control_plane_delete_runtime_completed
control_plane_cleanup_runtime_kafka_skipped
control_plane_cleanup_runtime_kafka_retry_scheduled
control_plane_cleanup_runtime_kafka_completed
control_plane_drop_table_progress
control_plane_drop_table_retry_scheduled
control_plane_drop_table_skipped
control_plane_drop_table_completed
control_plane_purge_materialization_records_completed
control_plane_runtime_observation_completed
control_plane_runtime_observation_follow_up_scheduled
control_plane_runtime_observation_timed_out
control_plane_consumer_lag_refresh_completed
control_plane_retention_purge_requested
control_plane_settings_applied
control_plane_settings_rejected
table_event_applied
identity_request_applied
```

**Runtime**

```
runtime_partition_session_started
runtime_processing_performance_window
runtime_schema_change_requested
runtime_seal_waiting_for_schema
runtime_batch_sealed
runtime_batch_seal_failed
runtime_batch_upload_failed
runtime_file_announced
runtime_source_paused_for_upload_backlog
runtime_source_resumed_after_upload_backlog
runtime_unmapped_values
runtime_batch_committed
runtime_settings_applied
```

**Runtime — the committer**

```
runtime_commit_started
runtime_commit_completed
runtime_commit_failed
runtime_file_decision
runtime_schema_committed
runtime_schema_commit_failed
```

**The maintenance app**

```
maintenance_compaction_started
maintenance_compaction_completed
maintenance_compaction_failed
maintenance_snapshot_expiry_completed
maintenance_orphan_cleanup_completed
```

**The control plane — changes of topics** (07 §7.9a)

```
control_plane_apply_runtime_draining
control_plane_runtime_drained
control_plane_await_runtime_drained_failed
control_plane_drop_table_waiting_for_maintenance
control_plane_drop_table_maintenance_wait_expired
control_plane_drop_table_failed
```

What each carries, and how to read them while debugging: 10 §10.14, 09 §9.11, 12.

## 3.8 Timestamp format

Every timestamp produced by this codebase uses the same helper shape:

```python
# code-derived
datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
```

Result: `2026-01-15T10:30:00Z` — UTC, second precision, `Z` suffix, no microseconds.

Parsing tolerates both `Z` and `+00:00` and normalizes naive timestamps to UTC
(`protocol_logging._parse_timestamp`, `observation/policy._parse_timestamp`,
`runtime_app/writer/seal._parse_timestamp`). Iceberg values are microseconds
(`timestamptz`); snapshot summaries carry millisecond ISO-8601 text.
