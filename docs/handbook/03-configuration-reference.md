# 03 — Configuration Reference

There are two independent configuration objects. Both are frozen dataclasses. Neither reads
configuration files — only environment variables and code defaults.

| Object | Module | Loaded by | Cached |
|--------|--------|-----------|--------|
| `ControlPlaneConfig` | `materializer_control_plane/config.py` | `get_config()` | Yes — `@lru_cache(maxsize=1)` |
| `RuntimeAppConfig` | `materializer_control_plane/runtime_app/config.py` | `get_runtime_app_config()` | **No** — rebuilt on every call |

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
| `control_plane_table_name` | `MATERIALIZER_CONTROL_PLANE_TABLE_NAME` | `state` | none |
| `default_namespace` | `MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE` | the pod's own namespace, read from its service-account mount | the Kubernetes namespace every runtime runs in (a create request cannot choose another); with neither the variable nor the mount, the configuration refuses to load |
| `default_runtime_name_prefix` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX` | `kaspr` | none; a runtime is named `<prefix>-<materialization_id>`, never by its request |
| `default_runtime_template_version` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION` | `v1` | none |
| `runtime_app_spec` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC` | `{}` | must parse to a YAML mapping |
| `default_list_limit` | `MATERIALIZER_CONTROL_PLANE_DEFAULT_LIST_LIMIT` | `"30"` | one of `10`, `30`, `100`, `all` |
| `default_destination_uri` | `MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI` | `None` | trimmed, trailing slashes dropped; blank is `None`. Where every new materialization's tables are created; a create request cannot name another. `None`, or a URI that is not `gs://`, refuses every create (`409 destination_not_configured`). Reported by `GET /system/settings` |
| `default_runtime_replicas` | `MATERIALIZER_CONTROL_PLANE_DEFAULT_RUNTIME_REPLICAS` | `1` | integer, `>= 1`; replicas for a create request that names none |
| `max_runtime_replicas` | `MATERIALIZER_CONTROL_PLANE_MAX_RUNTIME_REPLICAS` | `64` | integer, `>= 1`; the most a create or update may ask for |
| `runtime_autoscaling_enabled` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALING_ENABLED` | `True` | boolean parse; on, a runtime far behind its topics gets more members until it has caught up, unless its materialization turned autoscaling off; off, none does, and scaled runtimes go back to their own count at the next lag measurement (07 §7.12a) |
| `runtime_autoscale_max_replicas` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_MAX_REPLICAS` | `16` | integer, `>= 1`; the most members autoscaling gives one runtime — fewer when its topics' partitions keep fewer busy, never below its own count. Independent of `max_runtime_replicas` |
| `runtime_autoscale_target_minutes` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_TARGET_MINUTES` | `30` | integer, `>= 1`; the catch-up time autoscaling scales for: a runtime that would take longer at its pace gets members enough to make it |
| `runtime_autoscale_min_lag_records` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_MIN_LAG_RECORDS` | `100000` | integer, `>= 1`; the lag floor: no runtime this close is scaled up, and a scaled one goes back after three measurements at or under it |
| `runtime_autoscale_cooldown_seconds` | `MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_COOLDOWN_SECONDS` | `300` | integer, `>= 60`; the least time between two changes of a runtime's members |
| `default_runtime_storage_size` | `MATERIALIZER_CONTROL_PLANE_DEFAULT_RUNTIME_STORAGE_SIZE` | `64Mi` | a Kubernetes quantity; every new runtime member's persistent volume, which holds only its state table (10 §10.8). A create request cannot choose another. Not the control plane's own volume, which its KasprApp sets (1Gi) |
| `default_partition_granularity` | `MATERIALIZER_CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY` | `day` | `hour`, `day`, `month` or `none`; a new materialization's tables' partitioning when its create request names none |
| `max_materialization_topics` | `MATERIALIZER_CONTROL_PLANE_MAX_MATERIALIZATION_TOPICS` | `20` | integer, `>= 1`; the most source topics one create request may name — each a table, all read by one runtime. Reported by `GET /system/settings` as `max_topics` |
| `destination_cleanup_enabled` | `MATERIALIZER_CONTROL_PLANE_DESTINATION_CLEANUP_ENABLED` | `true` | delete drops the table: every object under its location — data, manifests, metadata, the pointer. Disable to keep the table; the delete chain then ends after the Kafka cleanup |
| `purge_records_on_delete` | `MATERIALIZER_CONTROL_PLANE_PURGE_RECORDS_ON_DELETE` | `true` | delete removes every control-plane record as its final step; disable to leave them for the retention sweep |
| `deleted_retention_hours` | `MATERIALIZER_CONTROL_PLANE_DELETED_RETENTION_HOURS` | `168` | how long the retention sweep leaves a deleted materialization's records before collecting them |
| `deleted_purge_batch_limit` | `MATERIALIZER_CONTROL_PLANE_DELETED_PURGE_BATCH_LIMIT` | `50` | integer, `>= 1`; deleted materializations the retention sweep collects per run |
| `consumer_lag_refresh_batch_limit` | `MATERIALIZER_CONTROL_PLANE_CONSUMER_LAG_REFRESH_BATCH_LIMIT` | `100` | integer, `>= 1`; materializations whose consumer lag one pass measures |
| `topic_analysis_max_topics` | `MATERIALIZER_CONTROL_PLANE_TOPIC_ANALYSIS_MAX_TOPICS` | `20` | integer, `>= 1`; most topics one `POST /topics/analyze` may inspect |
| `materialization_requests_topic` | `MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC` | `materialization-requests` | none |
| `materialization_work_topic` | `MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC` | `materialization-work` | none |
| `materialization_status_topic` | `MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC` | `materialization-status` | none |
| `schema_authority_updates_topic` | `MATERIALIZER_CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC` | `materialization-schema-authority-updates` | none; the runtime control topic: runtime settings (06 §6.7) |
| `table_events_topic` | `MATERIALIZER_CONTROL_PLANE_TABLE_EVENTS_TOPIC` | `materialization-table-events` | none; what runtimes' committers and the maintenance app publish (06 §6.6) |
| `storage_prefix` | `MATERIALIZER_CONTROL_PLANE_STORAGE_PREFIX` | `materializer` | trimmed, slashes stripped; the directory under the destination that every materialization's tables are created in: `<destination>/<storage_prefix>/<materialization id>/<table>` (11 §11.1). Not a namespace — a table's namespace is its dataset. Fixed for the deployment; `iceberg_namespace` / `…_ICEBERG_NAMESPACE` before 3.0 |
| `catalog_base_url` | `MATERIALIZER_CONTROL_PLANE_CATALOG_BASE_URL` | `None` | trimmed, trailing slash dropped; the address engines reach the REST catalog at, shown in the console's connection snippets |
| `catalog_api_enabled` | `MATERIALIZER_CONTROL_PLANE_CATALOG_API_ENABLED` | `true` | boolean parse; off, every `/catalog` endpoint answers `503` |
| `catalog_metadata_cache_seconds` | `MATERIALIZER_CONTROL_PLANE_CATALOG_METADATA_CACHE_SECONDS` | `5` | integer, `>= 1`; how long a loaded table is answered again from memory |
| `discovery_sample_records` | `MATERIALIZER_CONTROL_PLANE_DISCOVERY_SAMPLE_RECORDS` | `200` | integer, `>= 1`; records discovery reads to build schema 0 |
| `discovery_sample_seconds` | `MATERIALIZER_CONTROL_PLANE_DISCOVERY_SAMPLE_SECONDS` | `5` | integer, `>= 1`; how long discovery waits for them |
| `settings_requests_topic` | `MATERIALIZER_CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC` | `materializer-settings-requests` | none; the topic the settings WebView publishes changes to |
| `identity_requests_topic` | `MATERIALIZER_CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC` | `materializer-identity-requests` | none; the topic the identity views and tasks publish to |
| `session_idle_timeout_minutes` | `MATERIALIZER_CONTROL_PLANE_SESSION_IDLE_TIMEOUT_MINUTES` | `480` | integer, `>= 1`; how long a session lasts unused |
| `session_max_lifetime_hours` | `MATERIALIZER_CONTROL_PLANE_SESSION_MAX_LIFETIME_HOURS` | `168` | integer, `>= 1`; how long a session lasts however active |
| `session_refresh_minutes` | `MATERIALIZER_CONTROL_PLANE_SESSION_REFRESH_MINUTES` | `10` | integer, `>= 1`; how often a client renews an active session |
| `password_min_length` | `MATERIALIZER_CONTROL_PLANE_PASSWORD_MIN_LENGTH` | `12` | integer, `>= 8`; the shortest password anyone may choose or be given |
| `sign_in_max_failed_attempts` | `MATERIALIZER_CONTROL_PLANE_SIGN_IN_MAX_FAILED_ATTEMPTS` | `10` | integer, `>= 1`; wrong passwords in a row before an email is locked out |
| `sign_in_lockout_minutes` | `MATERIALIZER_CONTROL_PLANE_SIGN_IN_LOCKOUT_MINUTES` | `15` | integer, `>= 1` |
| `audit_log_retention_days` | `MATERIALIZER_CONTROL_PLANE_AUDIT_LOG_RETENTION_DAYS` | `90` | integer, `>= 1` |
| `live_tail_enabled` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_ENABLED` | `True` | boolean parse; on, every materialization's tables are tailed; off, none (19) |
| `live_tail_max_rows_per_table` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_ROWS_PER_TABLE` | `250000` | integer, `>= 1000`; one table's uncommitted records the tail holds, and the backlog bound |
| `live_tail_max_bytes_per_table` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_BYTES_PER_TABLE` | `268435456` | integer, `>= 8 MiB`; one table's tail memory |
| `live_tail_retention_seconds` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_RETENTION_SECONDS` | `120` | integer, `>= 10`; how long committed records stay in a tail |
| `live_tail_fetch_wait_ms` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_FETCH_WAIT_MS` | `200` | integer, `>= 50`; the tail's Kafka fetch wait |
| `live_tail_max_records_per_second_per_table` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE` | `5000` | integer, `>= 100`; one table's ingest ceiling |
| `live_tail_peek_max_rows` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_PEEK_MAX_ROWS` | `1000` | integer, `>= 10`; the most records one head peek reads |
| `live_tail_idle_seconds` | `MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_IDLE_SECONDS` | `600` | integer, `>= 0`; how long a table stays tailed after its last query; `0` never parks |
| `protocol_logging_enabled` | `MATERIALIZER_PROTOCOL_LOGGING_ENABLED` | `False` | boolean parse |
| `protocol_log_slow_wait_seconds` | `MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS` | `30` | integer, `>= 1` |
| `schema_history_limit` | `MATERIALIZER_CONTROL_PLANE_SCHEMA_HISTORY_LIMIT` | `50` | integer, `>= 1`; schema projections kept per materialization (05 §5.5) |
| `topic_catalog_ttl_seconds` | `MATERIALIZER_CONTROL_PLANE_TOPIC_CATALOG_TTL_SECONDS` | `15` | integer, `>= 1`; how long `GET /topics` answers from memory |
| `topic_catalog_min_refresh_seconds` | `MATERIALIZER_CONTROL_PLANE_TOPIC_CATALOG_MIN_REFRESH_SECONDS` | `5` | integer, `>= 1`; floor between topic-list loads, forced and failed ones included |
| `kaspr_app_name` | `K_APP_NAME` (set by Kaspr) | `None` | none; topics prefixed with it are this control plane's own |

1.0 retired `schema_proposals_topic`, `schema_proposal_debounce_seconds` and the three
`object_manifest_*` fields with what they configured; their variables are ignored.

The identity secrets — `MATERIALIZER_AUTH_SIGNING_KEY`, `…_PREVIOUS_SIGNING_KEY`,
`…_SERVICE_TOKEN`, `…_BOOTSTRAP_ADMIN_EMAIL`, `…_BOOTSTRAP_ADMIN_PASSWORD`,
`…_BOOTSTRAP_ADMIN_FIRST_NAME`, `…_BOOTSTRAP_ADMIN_LAST_NAME` — are deliberately **not**
`ControlPlaneConfig` fields, so no settings view can ever report one. They are read once by
`identity/secrets.py::get_identity_secrets`; keys and the service token shorter than 32
characters are ignored and logged. See [18 — Identity and access](./18-identity-and-access.md)
§18.11.

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
`ValueError: MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC must parse to a mapping`.

### 3.2.3 Derived helper

```python
# code-derived
def runtime_name_for_materialization(self, materialization_id: str) -> str:
    return f"{self.default_runtime_name_prefix}-{materialization_id}"
```

So `mat-a1b2c3d4e5f6` becomes `kaspr-mat-a1b2c3d4e5f6` in base and
`develop-mat-a1b2c3d4e5f6` in dev.

### 3.2.4 Which variables the manifests actually set

Set in `manifests/base/apps/control-plane.yaml`:

- `MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE`
- `MATERIALIZER_CONTROL_PLANE_DEFAULT_LIST_LIMIT`
- `MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX`
- `MATERIALIZER_CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION`
- `MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC`
- the requests, work, status, runtime control and table-events topic variables,
  `MATERIALIZER_CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC` and
  `MATERIALIZER_CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC`
- `MATERIALIZER_CONTROL_PLANE_STORAGE_PREFIX` (`materializer`; dev `develop`)
- `MATERIALIZER_CONTROL_PLANE_DELETED_RETENTION_HOURS`, `…_DEFAULT_RUNTIME_STORAGE_SIZE`,
  `…_DEFAULT_RUNTIME_REPLICAS`, `…_DEFAULT_DESTINATION_URI`
- `MATERIALIZER_AUTH_SIGNING_KEY`, `…_PREVIOUS_SIGNING_KEY`, `…_SERVICE_TOKEN`,
  `…_BOOTSTRAP_ADMIN_EMAIL` and `…_BOOTSTRAP_ADMIN_PASSWORD`, from the `materializer-auth` Secret
- `MATERIALIZER_PROTOCOL_LOGGING_ENABLED` = `"true"`
- `GOOGLE_APPLICATION_CREDENTIALS` (consumed by the GCS client, not by `ControlPlaneConfig`)

**Not set anywhere** (code defaults apply):

- `MATERIALIZER_CONTROL_PLANE_TABLE_NAME`
- `MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS`
- `MATERIALIZER_CONTROL_PLANE_SCHEMA_HISTORY_LIMIT`
- `MATERIALIZER_CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY`, `…_DISCOVERY_SAMPLE_RECORDS`,
  `…_DISCOVERY_SAMPLE_SECONDS`
- `MATERIALIZER_CONTROL_PLANE_CATALOG_BASE_URL` — set it per environment to the address engines
  use, or the console's snippets show a placeholder — `…_CATALOG_API_ENABLED`,
  `…_CATALOG_METADATA_CACHE_SECONDS`
- the seven session, password, lockout and audit settings (Settings page overrides apply instead)

Note that `control_plane_table_name` is a config field, but the table is bound by the Kaspr
agent/webview `tables:` block (`name: state` / `name: develop-state`), not by this variable.
Changing the env var alone will **not** rebind the table. The `settings`, `identity` and
`identity-audit` tables have no variable at all; they are bound the same way (`name: settings` /
`name: develop-settings`, and so on).

### 3.2.5 Which fields the Settings page can override

36 fields, listed with their bounds in [17 — Settings](./17-settings.md) §17.3:

| Section | Fields |
|---|---|
| Materializations | `default_destination_uri`, `default_runtime_replicas`, `max_runtime_replicas`, `default_runtime_storage_size`, `default_partition_granularity`, `runtime_autoscaling_enabled`, `runtime_autoscale_max_replicas`, `runtime_autoscale_target_minutes`, `runtime_autoscale_min_lag_records`, `runtime_autoscale_cooldown_seconds`, `max_materialization_topics` |
| Deletion | `destination_cleanup_enabled`, `purge_records_on_delete`, `deleted_retention_hours`, `deleted_purge_batch_limit` |
| Schema | `discovery_sample_records`, `discovery_sample_seconds`, `schema_history_limit` |
| Catalog | `catalog_api_enabled`, `catalog_metadata_cache_seconds` |
| Jobs | `consumer_lag_refresh_batch_limit` |
| Topics | `topic_catalog_ttl_seconds`, `topic_catalog_min_refresh_seconds`, `topic_analysis_max_topics` |
| Diagnostics | `protocol_logging_enabled` |
| Security | `session_idle_timeout_minutes`, `session_max_lifetime_hours`, `session_refresh_minutes`, `password_min_length`, `sign_in_max_failed_attempts`, `sign_in_lockout_minutes`, `audit_log_retention_days` |
| Live tail | `live_tail_enabled`, `live_tail_max_rows_per_table`, `live_tail_max_bytes_per_table`, `live_tail_retention_seconds`, `live_tail_fetch_wait_ms`, `live_tail_max_records_per_second_per_table`, `live_tail_peek_max_rows`, `live_tail_idle_seconds` |

The rest stay with the deployment — topic and table names, the namespace and runtime naming, the
Iceberg namespace and the catalog's base URL, the runtime template and spec, and credentials, the
identity secrets included — and the Settings page lists them read-only. `protocol_log_slow_wait_seconds`
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
(`LIVE_SETTING_ENV_VARS`, `runtime_app/live_settings.py`): `MATERIALIZER_RUNTIME_<FIELD>` in upper
case, except the two protocol-logging fields, which read `MATERIALIZER_PROTOCOL_LOGGING_ENABLED`
and `MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS`.

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
| `MATERIALIZER_RUNTIME_MEMBER_ID` | Downward API (`metadata.name`) in the runtime app spec | `runtime_app/writer/context.py::current_member_id()` |
| `GOOGLE_APPLICATION_CREDENTIALS` | runtime app spec | the GCS client and PyIceberg's FileIO |
| `K_STREAM_WAIT_EMPTY` | the generated runtime spec, `false` | Kaspr's `stream_wait_empty`: Faust does not wait, on a rebalance or at shutdown, for delivered records to be acknowledged. The writer holds acknowledgements until rows are in the table (10 §10.7), so waiting would stall both; a record left unacknowledged is read again |
| `K_STORE_ROCKSDB_WRITE_BUFFER_SIZE`, `K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER` | the generated runtime spec, `4194304` and `2` | Kaspr's RocksDB write buffer for the state table, the app's one store: flushing every 4 MiB keeps its write-ahead log under 8 MiB on a 64Mi volume (10 §10.8). Kaspr's own default is 64 MiB |

The table's identity — location, namespace, name, partition granularity — reaches the runtime as
agent arguments in its templates, not as variables. The generated spec's env list is merged with
the deployment's `MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC` by variable name, the deployment's
entries winning.

`current_member_id()` stamps batch ids and data file names, and names the committer in the
heartbeat. A member deletes the staging files it finds under its own id when it starts; nothing
staged by an earlier process is resumed — it is read again from Kafka (10 §10.9).

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

### The maintenance app (`maintenance_app/config.py`)

| Variable | Default | Meaning |
|---|---|---|
| `MATERIALIZER_MAINTENANCE_APP_NAME` | `materializer-maintenance` | names the app in its commits and table events |
| `MATERIALIZER_MAINTENANCE_MAX_CONCURRENT_JOBS` | `1` | long jobs one member runs at once, across its tables |
| `MATERIALIZER_MAINTENANCE_FORGET_AFTER_SECONDS` | `300` (≥ 60) | a table no tick names for this long is forgotten, its job stopped |

Read from the app's environment (02 §2.10a); what the jobs do comes with the ticks.

### The tail app (`tail_app/config.py`)

| Variable | Default | Meaning |
|---|---|---|
| `MATERIALIZER_TAIL_APP_NAME` | `materializer-tail` | the app, as its consumer group and the member map name it |
| `MATERIALIZER_TAIL_CONTROL_PLANE_URL` | none | the control plane's Service: sessions, schemas, tables |
| `MATERIALIZER_TAIL_SERVICE_TOKEN` | none | the console's credential, which every request must carry, and the app's own for schemas; at least 32 characters, never a config field |
| `MATERIALIZER_TAIL_TABLE_EVENTS_TOPIC` | `materialization-table-events` | commits and schema changes |
| `MATERIALIZER_TAIL_REQUESTS_TOPIC` | `materializer-tail-requests` | ticks and cancels |
| `MATERIALIZER_TAIL_MEMORY_BUDGET_BYTES` | `2684354560` | resident bytes of a member's tables together |
| `MATERIALIZER_TAIL_FORGET_AFTER_SECONDS` | `300` | a table no tick names for this long is forgotten |
| `MATERIALIZER_TAIL_SESSION_CHECK_SECONDS` | `5` | how long a session the control plane answered for is trusted (60 s while it cannot be reached) |
| `MATERIALIZER_TAIL_SERVE_CONCURRENCY`, `…_PEEK_CONCURRENCY` | `4`, `2` | answers and peeks built at once |
| `MATERIALIZER_TAIL_MEMBER_MAP_REFRESH_SECONDS` | `15` | how often the member map is read again |
| `MATERIALIZER_RUNTIME_MEMBER_ID` | the pod's name | the member's name |

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

| Setting | Code default | Base manifest | Dev overlay |
|---------|-------------|---------------|-------------|
| Namespace | `midstream` | `midstream` | `midstream` |
| Runtime name prefix | `kaspr` | `kaspr` | `develop` |
| Template version | `v1` | `v1` | `v1` |
| List limit | `30` | `30` | `30` |
| Requests topic | `materialization-requests` | same | `develop-materialization-requests` |
| Work topic | `materialization-work` | same | `develop-materialization-work` |
| Status topic | `materialization-status` | same | `develop-materialization-status` |
| Runtime control topic | `materialization-schema-authority-updates` | same | `develop-materialization-schema-authority-updates` |
| Table events topic | `materialization-table-events` | same | `develop-materialization-table-events` |
| Iceberg namespace | `materializer` | `materializer` | `develop` |
| Catalog base URL | — | (unset) | (unset) |
| Protocol logging | `False` | `true` | `true` |
| Slow-wait threshold | `30s` | (unset → 30) | (unset → 30) |
| Table name | `state` | `state` | `develop-state` |
| Settings table | `settings` | `settings` | `develop-settings` |
| Settings request topic | `materializer-settings-requests` | same | `develop-materializer-settings-requests` |
| Runtime CPU | — | 0.5 request, 1 limit - too little on a busy topic ([12 §12.10](./12-operations-runbook.md)) | same |
| Runtime memory | — | 1Gi request and limit - likewise | same |
| Runtime `topicAllowDeclare` | — | `true` | (unset) |

For every field the Settings page can override (§3.2.5, §3.3.1), an override sits above all three
columns: **override → deployment value (the env var) → code default**. The page shows which one is
in force for each setting. A deployment that changes an env var does not beat an override; reset
the override to follow the deployment again.

## 3.6 How to change a setting safely

**Anything the Settings page offers** — change it there. It applies without a restart, is recorded
with who changed it, and can be reset. See [17 — Settings](./17-settings.md).

**Topic names** — change in three places or the pipeline breaks:
1. `manifests/base/apps/control-plane.yaml` env var
2. the corresponding agent `spec.input.topic.name` / `spec.output.topics[].name`
3. `manifests/dev/kustomization.yaml` patches

Then re-run `kubectl kustomize manifests/dev` and the unit tests.

**Runtime behavior** (batch sizing, commit cadence, schema caps, maintenance) — change it on the
Settings page's Runtimes, Commits, Schema and Maintenance sections; running runtimes apply it within
seconds, and the maintenance app at its next tick, without a restart.
To change the deployment value instead, set the env var in the runtime spec
(`MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC` → `template.kasprContainer.env`). That changes
every runtime's spec hash, so the sweep re-applies, and restarts, every runtime. A compile-time
field (§3.3.2) still needs a package release, and runtimes pick it up only when their package pin
advances.

**Observation timing** — edit `observation/policy.py`. Affects the control plane only;
no runtime redeploy needed.

**The Iceberg namespace** is part of every table's location and identifier: change it only for a
new deployment. Existing tables keep the namespace they were created in.

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
