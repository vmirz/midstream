# 05 — Data Model Reference

Every model in this system is a `@dataclass(frozen=True, slots=True)` with:

- validation in `__post_init__` (raising `ValueError` with a precise message),
- a `to_dict()` that produces the wire/persistence form,
- a `from_dict()` classmethod that reconstructs it.

The `to_dict()` / `from_dict()` round-trip is a hard requirement for anything persisted or
published. `python/tests/test_models.py` exercises it.

Two conventions recur:

- **Required-string validation**: `if not self.x: raise ValueError("x is required")`.
- **Optional-field omission**: `to_dict()` omits keys whose value is `None`, except for a
  handful of models (`Materialization`, `RuntimeBinding`, `MaterializationSchema`,
  `MaterializationTable`, `TableEvent`, `RecordEnvelope`, `RuntimeHealth`, `ResourceCondition`)
  which always emit their full key set.

1.0 removed `AuthoritativeSchema`, `SchemaProposalEvent` (with `FieldProposal` and
`BreakingChange`), `RuntimeSignalEvent` and the runtime's commit-marker and manifest models; §5.16
lists what replaced each.

---

## 5.1 Enumerations

All defined as module-level tuples and validated via `_validate_choice`.

| Constant | Module | Values |
|----------|--------|--------|
| `LIFECYCLE_PHASES` | `models/materialization.py` | `requested`, `discovering`, `discovered`, `deploying`, `running`, `paused` (legacy: nothing requests it since 0.4.0), `stopped`, `deleting`, `deleted`, `failed`, `degraded` |
| `RUNTIME_MODES` | `models/materialization.py` | `dedicated`, `shared` |
| `REQUEST_ACTIONS` | `models/events.py` | `create`, `pause` (legacy, decoded but never produced), `stop`, `resume`, `delete`, `update` |
| `WORK_STEPS` | `models/events.py` | `tables_claimed`, `claim_rejected` (the directory's answer to a claim, 07 §7.2a), `start_discovery`, `discover_tables`, `create_table`, `apply_runtime`, `await_runtime_drained`, `delete_runtime`, `cleanup_runtime_kafka`, `drop_table`, `purge_materialization_records`, `observe_runtime`, `sweep_observations`, `purge_deleted_materializations`, `refresh_consumer_lag`, `record_consumer_lag`, `schedule_table_maintenance`, `repair_state` |
| `TABLE_ACTIONS` | `models/events.py` | `drop`, `keep`: what becomes of a removed topic's table (07 §7.9a) |
| `CLAIM_PURPOSES`, `CLAIM_STATUSES` | `models/events.py` | `create`, `update`; `applied`, `rejected`: a claim's answer (§5.9) |
| `PARTITION_GRANULARITIES` | `models/materialization.py` (also `config.py`) | `hour`, `day`, `month`, `none`: Iceberg's time transforms, which have no week (11 §11.3) |
| `CAUSE_KINDS` | `models/events.py` | `request`, `runtime_status`, `resource_observation`, `scheduler`, `repair` |
| `OBSERVATION_MODES` | `models/events.py` | `one_shot`, `sweep` |
| `RUNTIME_EVENT_PHASES` | `models/events.py` | `running`, `degraded` |
| `RUNTIME_EVENT_HEALTH` | `models/events.py` | `healthy`, `degraded`, `unknown` |
| `TABLE_EVENT_KINDS` | `models/table.py` | `schema_changed`, `table_committed`, `maintenance_started`, `maintenance_completed`, `table_error` |
| `MAINTENANCE_JOBS` | `models/table.py` | `compaction`, `expire_snapshots`, `orphan_cleanup` |
| `TABLE_STATES` | `models/table.py` | `active`, `retired`, `dropping`, `dropped` (11 §11.7) |
| `MAINTENANCE_MESSAGE_KINDS` | `models/maintenance.py` | `maintenance_tick`, `maintenance_request`, `maintenance_cancel` |
| `MAINTAINED_TABLE_STATES` | `models/maintenance.py` | `active`, `retired` |
| `OBSERVATION_SOURCES` | `models/status.py` | `runtime_event`, `resource_observation`, `hybrid` |
| `RUNTIME_HEALTH_LEVELS` | `models/status.py` | `healthy`, `degraded`, `unknown`, `unreachable` |
| `CONDITION_STATUSES` | `models/status.py` | `True`, `False`, `Unknown` (strings) |
| `BATCH_STATES` | `runtime_app/models/processing_state.py` | `open`, `rolled`, `sealing`, `sealed`, `uploading`, `uploaded`, `failed` |
| File decisions | `lakehouse/commits.py` | `accepted`, `covered`, `overlap`, `deferred`, `error` (10 §10.6) |
| `USER_STATUSES` | `models/identity.py` | `active`, `deactivated` |
| `GROUP_MEMBERSHIPS` | `models/identity.py` | `assigned`, `authenticated` (All Users), `guests` (Guest) |
| `IDENTITY_ACTIONS` | `models/identity.py` | `DIRECTORY_ACTIONS` (`create_user`, `update_user`, `set_user_password`, `change_password`, `sign_out_user`, `unlock_user`, `delete_user`, `create_group`, `update_group`, `delete_group`, `create_role`, `update_role`, `delete_role`, `create_access_token`, `revoke_access_token`, `create_dataset`, `update_dataset`, `delete_dataset`, `set_dataset_grant`, `claim_tables`) + `SESSION_ACTIONS` (`sign_in_succeeded`, `sign_in_failed`, `session_refreshed`, `sign_out`) + `SYSTEM_ACTIONS` (`bootstrap`, `maintenance`, `release_tables`) |
| `GRANT_PRINCIPAL_TYPES` | `models/identity.py` | `user`, `group`: who a dataset grant is for |
| `REQUEST_STATUSES` | `models/identity.py` | `applied`, `rejected` |
| `AUDIT_OUTCOMES` | `models/identity.py` | `success`, `failure` |
| `ERROR_KINDS` | `runtime_app/models/processing_state.py` | `data_validation`, `infrastructure` |

Note the asymmetry: `MaterializationStatus` phases use the full 11-value `LIFECYCLE_PHASES`,
but a `RuntimeStatusEvent` can only report `running` or `degraded`. The runtime never claims
to be `deploying`, `stopped`, or `deleted` — those are control-plane conclusions.

---

## 5.2 Control-plane table key layout

Every record about materializations lives in one global `KasprTable` named `state`
(`develop-state` in dev), 3 partitions, JSON key and value serializers. Settings live in a second
table (§5.2.1), and people, groups, roles and sessions in two more (§5.2.2).

| Prefix | Builder | Entity type | Holds |
|--------|---------|-------------|-------|
| `materialization:` | `materialization_key(id)` | `materialization` | Desired state |
| `materialization-status:` | `materialization_status_key(id)` | `materialization-status` | Observed state |
| `materialization-schema:<id>:<table>` | `materialization_schema_key(id, table)` | `materialization-schema` | One table's current schema projection (§5.5) |
| `materialization-schema-version:<id>:<table>:<schema_id, 10 digits>` | `materialization_schema_version_key(id, table, n)` | `materialization-schema-version` | A table's earlier projections, the newest `schema_history_limit` kept per table |
| `materialization-table:<id>:<table>` | `materialization_table_key(id, table)` | `materialization-table` | One table's record (§5.12) |
| `materialization-op:<id>:<op_id>` | `materialization_operation_key(id, op_id)` | `materialization-op` | Idempotency |
| `runtime-binding:` | `runtime_binding_key(id)` | `runtime-binding` | Applied runtime record |
| `runtime-autoscale:` | `runtime_autoscale_key(id)` | `runtime-autoscale` | How autoscaling scaled the runtime, and the lag it decided from (§5.6a) |

A materialization's per-table records share a prefix (`materialization_schema_prefix(id)`,
`materialization_schema_version_prefix(id)`, `materialization_table_prefix(id)`), so its tables are
listed and purged with one bounded prefix scan each; the trailing `:` keeps `mat-1` from matching
`mat-12`, and a table name never contains one.

`materialization_routing_key(id)` is an alias for `materialization_key(id)` — the same
`materialization:<id>` string is used as the **Kafka message key** for every event about that
materialization. This guarantees all events for one materialization land on the same partition
and are processed in order.

### 5.2.1 The `settings` table

A second global table, `settings` (`develop-settings` in dev), same shape. Only the
`settings-intake` agent writes it. Builders are in `storage/settings.py`.

| Key | Builder | Entity type | Holds |
|-----|---------|-------------|-------|
| `settings:<scope>` | `settings_key(scope)` | `settings` | `SettingsDocument` (§5.13) |
| `settings-change:<scope>:<revision, 10 digits>` | `settings_change_key(scope, revision)` | `settings-change` | `SettingsChangeRecord` (§5.13); the newest 100 per scope |

`settings_routing_key(scope)` is `settings:<scope>`, the Kafka key of every change request for
that scope, so one scope's changes apply in order on one partition.

### 5.2.2 The `directory` and `identity-audit` tables

Two more global tables, same shape (`develop-directory`, `develop-identity-audit` in dev; the
`directory` table was `identity` before 3.0). Only the `identity-intake` agent writes them.
Builders are in `storage/identity.py`; models in §5.14.

| Key | Builder | Entity type | Holds |
|-----|---------|-------------|-------|
| `user:<user_id>` | `user_key(id)` | `user` | `User` |
| `user-email:<email>` | `user_email_key(email)` | `user-email` | `{email, user_id}`, the unique email index (lower-cased) |
| `group:<group_id>` | `group_key(id)` | `group` | `Group`; a built-in group reads its code default until stored |
| `role:<role_id>` | `role_key(id)` | `role` | `Role`, custom roles only |
| `session:<user_id>:<session_id>` | `session_key(user_id, session_id)` | `session` | `Session` |
| `token:<token_id>` | `token_key(id)` | `access-token` | `AccessToken` (§5.14); only the secret's hash |
| `sign-in:<email>` | `sign_in_guard_key(email)` | `sign-in-guard` | `SignInGuard` |
| `activity:<user_id>` | `activity_key(id)` | `user-activity` | `UserActivity` |
| `request:<request_id>` | `request_outcome_key(id)` | `identity-request` | `IdentityRequestOutcome`, kept a day |
| `dataset:<dataset_id>` | `dataset_key(id)` | `dataset` | `Dataset` |
| `dataset-name:<name>` | `dataset_name_key(name)` | `dataset-name` | `{name, dataset_id}`, the unique name index (lower-case) |
| `dataset-grant:<dataset_id>:<user\|group>:<id>` | `dataset_grant_key(…)` | `dataset-grant` | `DatasetGrant`; typed, since a user and a group may share an id |
| `dataset-table:<dataset_id>:<table>` | `table_claim_key(…)` | `table-claim` | `TableClaim`: a table name taken in a dataset |
| `claim-release:<materialization_id>` | `claim_release_key(id)` | `claim-release` | `ClaimRelease`, kept 7 days |
| `directory` | — | `directory` | `DirectoryState` |
| `audit:<occurred_at>:<entry_id>` (identity-audit table) | `audit_key(entry)` | `audit-entry` | `AuditEntry`; the timestamp leads, so one day is one prefix |

`identity_routing_key()` is the constant `identity`, the Kafka key of every identity request:
they all apply in order, on one partition, which is what keeps cross-record rules (one account
per email, an administrator always remains) true without locks.

### Prefix scan

Only `materialization_prefix()` (`"materialization:"`) is scanned. Note that this prefix is a
**string prefix of** `materialization-status:` and friends… except it is not, because the
separator differs (`:` vs `-`). `materialization:` will not match `materialization-status:`
since the 16th character is `:` in one and `-` in the other. This is why the key scheme uses
a colon for the primary entity and hyphens for the others.

---

## 5.3 `RecordEnvelope`

`models/envelopes.py`. Every persisted value is wrapped in this.

| Field | Type | Required | Validation |
|-------|------|----------|-----------|
| `entity_type` | `str` | yes | non-empty |
| `entity_id` | `str` | yes | non-empty |
| `version` | `int` | yes | `>= 1` |
| `updated_at` | `str` | yes | non-empty |
| `payload` | `dict` | yes | — |

```json
{
  "entity_type": "materialization",
  "entity_id": "mat-a1b2c3d4e5f6",
  "version": 1,
  "updated_at": "2026-01-15T10:30:00Z",
  "payload": { "...": "the model's to_dict()" }
}
```

`storage/envelopes.py` provides `encode_payload_envelope(...)` and
`decode_envelope(raw, expected_entity_type=...)`. **`decode_envelope` returns `None` when the
stored `entity_type` does not match `expected_entity_type`**, so a mis-typed record is treated
as absent rather than raising. This is what makes the list prefix-scan tolerant of foreign
records.

`version` is always written as `1` by the current persistence helpers. It exists for future
schema migration of the envelope itself, not for optimistic concurrency.

---

## 5.4 `Materialization` — desired state

`models/materialization.py`. Key `materialization:<id>`, entity type `materialization`.

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `materialization_id` | `str` | — | required, non-empty |
| `tables` | `tuple[TableSpec, ...]` | — | at least one; unique names (without regard to case) and unique topics, kept tables included (`validate_table_specs`). The tables the runtime writes: one per source topic, in the order of the topics. An update can add and remove topics (07 §7.9a) |
| `destination_uri` | `str` | — | required, non-empty; the deployment's destination when it was created (04 §4.5.2) |
| `desired_phase` | `str` | — | ∈ `LIFECYCLE_PHASES` |
| `created_at` | `str` | — | required, non-empty |
| `updated_at` | `str` | — | required, non-empty |
| `runtime_mode` | `str` | `"dedicated"` | ∈ `RUNTIME_MODES` |
| `name` | `str \| None` | `None` | A **label**: free text, 1–128 characters, not unique; set to the id when none was given, and changed by `PATCH`. `label` reads it, or the id |
| `resolved_runtime_name` | `str \| None` | `None` | its runtime's name, generated from its id at creation (`<prefix>-<id>`, length-bounded). 4.0 removed `runtime_name`, the name a request could choose |
| `runtime_group` | `str \| None` | `None` | reserved for shared mode; unused |
| `runtime_app_ref` | `str \| None` | `None` | reserved; unused by control-plane code |
| `namespace` | `str` | required | must not be `""` — the Kubernetes namespace of the runtime: the deployment's `default_namespace` when it was created |
| `created_by` | `str \| None` | `None` | the creator's email (`principal.attribution`); `x-submitted-by` before 0.6.0 |
| `created_by_id` | `str \| None` | `None` | the creator's user id; `None` for materializations created before accounts |
| `resource_profile` | `str \| None` | `None` | carried, not consumed |
| `storage_size` | `str` | `"64Mi"` | Kubernetes quantity; persistent volume per replica, which holds only the state table: the deployment's `default_runtime_storage_size` when it was created (`1Gi` before 4.0, which earlier records keep) |
| `replicas` | `int` | `1` | `>= 1`; the runtime's member count. Rendered as `0` while `desired_phase == "stopped"`, and left alone by a stop, so resume restores it. Changed only by `PATCH`. Autoscaling renders more while the runtime is far behind (§5.6a) and never changes it |
| `autoscaling` | `bool` | `True` | must be a `bool`; whether the control plane may scale the runtime past `replicas` while it is far behind (07 §7.12a). Set at creation, changed by `PATCH`; absent in records before 4.4, read as `True` |
| `skip_tombstones` | `bool` | `False` | must be a `bool`; rendered into the writer agent only when `True` |
| `partition_granularity` | `str` | `"day"` | ∈ `PARTITION_GRANULARITIES` (`hour`, `day`, `month`, `none`); its tables' partition spec, fixed at creation |
| `labels` | `dict[str,str]` | `{}` | — |
| `annotations` | `dict[str,str]` | `{}` | — |
| `spec_version` | `int` | `1` | `>= 1`; incremented once per update that changes replicas, topics or both |
| `retired_tables` | `tuple[TableSpec, ...]` | `()` | tables kept read-only after their topics were removed: in their datasets and the catalog, maintained, not written. Adding a kept table's topic back writes it again |
| `pending_topic_change` | `PendingTopicChange \| None` | `None` | an edit that adds tables, waiting for the directory to claim their names: `request_id`, `claim_request_id`, `submitted_at`, and `plan`, the `TopicChangePlan` it applies as is once claimed (07 §7.9b). At most one |

`TableSpec` is `{name, source_topic, dataset, dataset_id}`: the table's name, the topic it holds, and
the dataset it is in — SQL reads it as `qualified_name`, `<dataset>.<name>`. Neither the dataset nor
the name changes for as long as the table exists. The model's `source_topics` and `table_names` list
the written tables in order, `dataset_ids` every dataset its tables are in, and `table(name)` finds a
written one without regard to case; `all_tables` and `any_table(name)` include the kept ones. Each
table's current schema id is on its table record (§5.12), not here.

There is no live-tail field: every materialization has a live tail (19). A record a control plane
before 4.3 stored may still carry `live_tail`; it is read and dropped.

`TopicChangePlan` (`tables`, `retired_tables`, `added`, `resumed`, `removed`, `retired`, `dropped`,
each a tuple of `TableSpec`) is what a change of topics does to every table; `services/
topic_changes.py::plan_topic_change` makes it, and `apply_topic_change` applies it.

`to_dict()` emits **all** keys including `None` values, and `source_topics` besides `tables`, for
readers that only want the topics.

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "name": "Shop",
  "tables": [
    { "name": "orders", "source_topic": "orders", "dataset": "sales", "dataset_id": "ds-5c1e…" },
    { "name": "payments", "source_topic": "payments", "dataset": "finance", "dataset_id": "ds-9a0b…" }
  ],
  "source_topics": ["orders", "payments"],
  "destination_uri": "gs://kafka-materializations",
  "runtime_mode": "dedicated",
  "runtime_group": null,
  "resolved_runtime_name": "develop-mat-a1b2c3d4e5f6",
  "runtime_app_ref": null,
  "namespace": "midstream",
  "desired_phase": "running",
  "created_at": "2026-10-18T10:30:00Z",
  "updated_at": "2026-10-18T10:41:12Z",
  "created_by": "jane@example.com",
  "created_by_id": "usr-3f9c2a1b7d4e",
  "resource_profile": null,
  "storage_size": "64Mi",
  "replicas": 16,
  "skip_tombstones": false,
  "autoscaling": true,
  "partition_granularity": "day",
  "labels": {},
  "annotations": {},
  "spec_version": 1,
  "retired_tables": [],
  "pending_topic_change": null
}
```

### Name resolution precedence

```python
# code-derived (runtime/naming.py)
resolve_runtime_app_name(m) == normalize_runtime_app_name(
    m.resolved_runtime_name
    or f"{cfg.default_runtime_name_prefix}-{m.materialization_id}"
)
```

`normalize_runtime_app_name` caps at 59 characters, appending `-<sha1(name)[:8]>` on overflow.

---

## 5.5 `MaterializationSchema` — the schema projection

`models/materialization.py`. Key `materialization-schema:<id>:<table>` (current) and
`materialization-schema-version:<id>:<table>:<schema_id>` (archive), `storage/schemas.py`.

Each table's Iceberg metadata is its schema's truth (09). This record is the control plane's
read-only projection of one of a table's schemas, what `GET …/tables/{table}/schema` answers from
without opening the table. It is written at discovery (schema 0) and from each `schema_changed`
table event; an older schema never replaces a newer one, and `resolve_materialization_schema`
restores the newest archived one if a changelog replay rolled the current one back.

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `materialization_id` | `str` | — | required |
| `table` | `str` | — | required: the table this is a schema of |
| `schema_id` | `int` | — | `>= 0`; the Iceberg schema id |
| `since` | `str` | — | when this schema became current |
| `fields` | `list[dict]` | `[]` | the projection: one entry per field, metadata first (09 §9.9) |
| `field_count` | `int` | `0` | Iceberg fields, nested ones included |
| `fork_map` | `dict` | `{}` | the table's `materializer.fork-map`: logical paths → physical fields (09 §9.5) |
| `iceberg_schema` | `dict` | `{}` | the schema as Iceberg serializes it (`?raw=true`) |
| `metadata_location` | `str \| None` | `None` | the metadata file that holds it |
| `changes` | `list[dict]` | `[]` | what changed from the previous schema: `added`, `forked`, `collapsed`, `rejected` |
| `last_column_id` | `int \| None` | `None` | the table's `last-column-id` |
| `rejected_paths` | `list[list[str]]` | `[]` | logical paths refused at the field limit; writers stop asking for them |

Two entries of a projection — `total`, a number that later arrived as a string and forked:

```json
[
  { "field_id": 14, "path": ["total"], "name": "total", "type": "double", "required": false,
    "fork_version": 1, "state": "superseded", "kind": "value", "since_schema_id": 0 },
  { "field_id": 31, "path": ["total__v2"], "name": "total", "type": "string", "required": false,
    "fork_version": 2, "state": "current", "kind": "value", "since_schema_id": 3, "fork_of": 14 }
]
```

`path` is physical (`[]` for a list element), `name` logical (`items[].sku`), `kind` ∈ `value`,
`container`, `json`, `metadata`, and `state` ∈ `current`, `superseded`, `collapsed` (09 §9.5).

Envelope version 3 holds one table's projection; version 2 held a single-table materialization's
and version 1 the retired authority's schema. Neither is read.

---

## 5.6 `RuntimeBinding`

`models/binding.py`. Key `runtime-binding:<id>`, entity type `runtime-binding`.

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `materialization_id` | `str` | — | must not be `""` |
| `runtime_mode` | `str` | — | ∈ `RUNTIME_MODES` |
| `kaspr_app_name` | `str` | — | must not be `""` |
| `namespace` | `str` | — | must not be `""` |
| `managed_agent_names` | `list[str]` | — | non-empty **and unique** |
| `template_version` | `str` | — | must not be `""` |
| `last_applied_spec_hash` | `str` | — | must not be `""` |
| `last_apply_operation_id` | `str` | — | must not be `""` |
| `resource_profile` | `str \| None` | `None` | — |
| `source_topics` | `list[str]` | `[]` | the topics the applied runtime reads, in table order |
| `destination_uri` | `str \| None` | `None` | — |
| `created_at` | `str \| None` | `None` | — |
| `updated_at` | `str \| None` | `None` | — |
| `drain_started_at` | `str \| None` | `None` | set while the runtime is held at zero until its consumer group empties, after a change of topics (`draining`); cleared by `await_runtime_drained` (07 §7.9a) |
| `topics_before_drain` | `list[str]` | `[]` | the topics the runtime read before that change, so the ones it adds are known |

`managed_agent_names` is the three rendered agent names:

```json
[
  "develop-mat-a1b2c3d4e5f6-writer",
  "develop-mat-a1b2c3d4e5f6-committer",
  "develop-mat-a1b2c3d4e5f6-settings"
]
```

`last_applied_spec_hash` is `sha256` over
`json.dumps(rendered_resources, sort_keys=True, separators=(",",":"))` — the *entire* rendered
resource dict, not just the app spec. It is the primary duplicate-apply guard.

---

## 5.6a `RuntimeAutoscale` — how the runtime was scaled

`models/autoscale.py`. Key `runtime-autoscale:<id>`, entity type `runtime-autoscale`, written only by
`services/autoscaling.py` in the materialization's own shard (07 §7.12a).

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `materialization_id` | `str` | — | required |
| `replicas` | `int \| None` | `None` | `>= 1`; the members the runtime runs while scaled up, more than the materialization's `replicas`. `None` at its own count |
| `reason` | `str \| None` | `None` | why it was scaled up, in words for a person; `None` at its own count |
| `changed_at` | `str \| None` | `None` | the measurement at which the count last changed, up or back |
| `eta_seconds` | `int \| None` | `None` | `>= 0`; the time to catch up at the pace last measured, `None` while the lag does not fall or too few measurements show it |
| `points` | `tuple[LagPoint, ...]` | `()` | the last `LAG_POINTS_KEPT` (16) measurements, oldest first: each `{at, lag}`, `lag >= 0` |
| `updated_at` | `str \| None` | `None` | the last measurement read |

`scaled` is `replicas is not None`. A record exists for every running materialization autoscaling
may scale, scaled or not — its points are the pace to come; a stop, a delete or switching
autoscaling off deletes it, and the retention sweep purges it with the rest.

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "replicas": 12,
  "reason": "5,470,010 records behind, about 2 h 3 min to catch up with 4 members",
  "changed_at": "2026-10-18T10:20:00Z",
  "eta_seconds": 1310,
  "points": [{ "at": "2026-10-18T10:34:00Z", "lag": 4351880 }, { "at": "2026-10-18T10:35:00Z", "lag": 4180115 }],
  "updated_at": "2026-10-18T10:35:00Z"
}
```

---

## 5.7 `MaterializationStatus` — observed state

`models/status.py`. Key `materialization-status:<id>`, entity type `materialization-status`.

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `materialization_id` | `str` | — | required |
| `desired_phase` | `str` | — | ∈ `LIFECYCLE_PHASES` |
| `observed_phase` | `str` | — | ∈ `LIFECYCLE_PHASES` |
| `phase_reason` | `str` | — | required, non-empty |
| `last_transition_time` | `str` | — | required, non-empty |
| `observation_source` | `str` | — | ∈ `OBSERVATION_SOURCES` |
| `runtime_health` | `RuntimeHealth` | — | nested |
| `resource_observation` | `ResourceObservation` | — | nested |
| `status_version` | `int` | `1` | **must be exactly 1** |
| `phase_message` | `str \| None` | `None` | omitted when `None` |
| `last_heartbeat` | `str \| None` | `None` | omitted when `None` |
| `last_resource_observation_time` | `str \| None` | `None` | omitted when `None` |
| `runtime_progress` | `RuntimeProgress` | empty | omitted when it serializes to `{}` |
| `last_error` | `str \| None` | `None` | omitted when `None` |

`status_version != 1` raises `ValueError("status_version must be 1")` — a deliberate
forward-compatibility tripwire.

`last_transition_time` only advances when `observed_phase` actually changes
(`observation/mapping.py::_replace_status`).

### `RuntimeHealth`

| Field | Type | Default |
|-------|------|---------|
| `level` | `str` ∈ `RUNTIME_HEALTH_LEVELS` | — |
| `reason` | `str \| None` | `None` |
| `message` | `str \| None` | `None` |

Always emits all three keys.

### `RuntimeProgress`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `rows_written` | `int \| None` | `None` | `>= 0` |
| `bytes_written` | `int \| None` | `None` | `>= 0` |
| `files_written` | `int \| None` | `None` | `>= 0` |
| `records_skipped` | `int \| None` | `None` | `>= 0` |
| `latest_offsets` | `dict[str, dict[str,int]]` | `{}` | per table, per partition: the highest offset any member handled; partition keys numeric strings, values `>= 0` (`validate_table_offsets`) |
| `cumulative_rows_written`, `cumulative_bytes_written`, `cumulative_files_written`, `cumulative_records_skipped`, `cumulative_unmapped_values` | `int \| None` | `None` | monotonic totals across member restarts, every table together |
| `unmapped_last_seen_at` | `str \| None` | `None` | when a member last reported values going to `_unmapped` |
| `cumulative_unmapped_by_table`, `unmapped_last_seen_by_table` | `dict[str,int]`, `dict[str,str]` | `{}` | the same per table, from the members' `table_counters` — what each table's schema view reports |
| `member_progress` | `dict[str, MemberProgress]` | `{}` | keyed by runtime member id |
| `skip_reasons` | `dict[str, SkipReasonTotal]` | `{}` | skipped records by category |
| `tables` | `dict[str, dict]` | `{}` | each table's heartbeat block as the committer member last sent it (06 §6.5), by table name; `table(name)` reads one |

`to_dict()` omits every `None` field and omits `latest_offsets` when empty, so an untouched
`RuntimeProgress` serializes to `{}` and is therefore dropped from the status payload entirely.

### `MemberProgress`

The last reading from one runtime member. Runtime counters reset when a pod restarts; keeping each
member's previous reading is what keeps the totals monotonic.

| Field | Type | Default |
|-------|------|---------|
| `rows_written`, `bytes_written`, `files_written`, `records_skipped` | `int` | `0` |
| `reported_at` | `str \| None` | `None` |
| `health`, `health_reason`, `phase_reason`, `phase_message`, `last_error` | `str \| None` | `None` |
| `unmapped_values` | `int` | `0`; values this member's seals wrote to `_unmapped` |
| `files_pending_commit` | `int \| None` | `None`; files this member uploaded that no commit covers yet |
| `settings_revision` | `int \| None` | `None`; the live runtime-settings revision the member runs, `None` from runtimes older than 0.5.0 |
| `settings_lineage` | `str \| None` | `None`; the lineage of those settings, `""` when they have none, `None` from runtimes older than 4.1 |
| `skip_counts` | `dict[str,int]` | `{}` |
| `table_unmapped` | `dict[str,int]` | `{}`; this member's `_unmapped` counter per table, so per-table totals stay monotonic too |

### `ResourceCondition`

| Field | Type | Validation |
|-------|------|-----------|
| `status` | `str` | ∈ `("True", "False", "Unknown")` — **strings, not booleans** |
| `reason` | `str \| None` | — |
| `message` | `str \| None` | — |
| `last_transition_time` | `str \| None` | — |

`from_dict(None)` returns `None`.

### `ResourceObservation`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `namespace` | `str` | — | required |
| `kaspr_app_name` | `str` | — | required |
| `resource_exists` | `bool` | — | — |
| `managed_agent_states` | `dict[str,str]` | — | — |
| `metadata_generation` | `int \| None` | `None` | `>= 0` |
| `observed_generation` | `int \| None` | `None` | `>= 0` |
| `ready_condition` | `ResourceCondition \| None` | `None` | — |
| `progressing_condition` | `ResourceCondition \| None` | `None` | — |
| `rollout_in_progress` | `bool \| None` | `None` | — |
| `available_members` | `str \| None` | `None` | note: **string**, e.g. `"3/3"` |
| `rebalance_required` | `bool \| None` | `None` | — |
| `rebalancing_members` | `str \| None` | `None` | string |
| `managed_agents_total` | `int \| None` | `None` | `>= 0` |
| `managed_agents_running` | `int \| None` | `None` | `>= 0` |
| `managed_agents_paused` | `int \| None` | `None` | `>= 0` |
| `managed_agents_unknown` | `int \| None` | `None` | `>= 0` |
| `members` | `tuple[RuntimeMemberState, ...] \| None` | `None` | the runtime's pods as last read (08 §8.3, row 3d); `None` when they could not be read |

`to_dict()` always emits the first four keys; all others are omitted when `None`.

### `RuntimeMemberState`

One runtime member's pod, from `kaspr.io/app=<runtime>`: `name`, `restarts` (the `kaspr` container's
`restartCount`, default `0`), and from its last termination `last_exit_reason` (`OOMKilled`,
`Error`, …), `last_exit_code` and `last_exited_at`, and `waiting_reason` (`CrashLoopBackOff`, …),
each `None` when absent and omitted from `to_dict()`. `observation/restarts.py` reads them as
restart instability (08 §8.3, row 3d).

`managed_agent_states` maps agent name → state string, sourced from
`KasprApp.status.linkedResources.agents`.

### Full status example

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "desired_phase": "running",
  "observed_phase": "running",
  "phase_reason": "processing",
  "phase_message": "Runtime is processing source records.",
  "last_transition_time": "2026-01-15T10:32:15Z",
  "last_heartbeat": "2026-01-15T10:35:00Z",
  "last_resource_observation_time": "2026-01-15T10:34:00Z",
  "observation_source": "hybrid",
  "runtime_health": { "level": "healthy", "reason": null, "message": null },
  "runtime_progress": {
    "rows_written": 152340,
    "bytes_written": 48210944,
    "files_written": 3,
    "records_skipped": 2,
    "latest_offsets": { "orders": { "0": 51234, "1": 50880 }, "payments": { "0": 9120 } }
  },
  "resource_observation": {
    "namespace": "midstream",
    "kaspr_app_name": "develop-mat-a1b2c3d4e5f6",
    "resource_exists": true,
    "managed_agent_states": {
      "develop-mat-a1b2c3d4e5f6-writer": "running",
      "develop-mat-a1b2c3d4e5f6-committer": "running",
      "develop-mat-a1b2c3d4e5f6-settings": "running"
    },
    "metadata_generation": 3,
    "observed_generation": 3,
    "ready_condition": {
      "status": "True", "reason": "Ready", "message": null,
      "last_transition_time": "2026-01-15T10:32:00Z"
    },
    "rollout_in_progress": false,
    "available_members": "1/1",
    "managed_agents_total": 3,
    "managed_agents_running": 3,
    "managed_agents_paused": 0,
    "managed_agents_unknown": 0
  },
  "status_version": 1
}
```

---

## 5.8 Operation records

Key `materialization-op:<materialization_id>:<operation_id>`, entity type
`materialization-op`. Entity id is `<materialization_id>:<operation_id>`.

The payload is a **free-form dict** — there is no dataclass. Shapes observed in code:

### Duplicate/skip guard

```json
{
  "operation_id": "work-apply-mat-a1b2c3d4e5f6",
  "operation_type": "apply_runtime",
  "status": "applied",
  "dedupe_key": "apply_runtime:mat-a1b2c3d4e5f6:req-9f8e7d6c5b4a",
  "runtime_app_name": "develop-mat-a1b2c3d4e5f6",
  "spec_hash": "3f5a...c91"
}
```

### Skipped discovery

```json
{
  "operation_id": "work-discovery-mat-a1b2c3d4e5f6",
  "operation_type": "start_discovery",
  "status": "skipped",
  "dedupe_key": "start_discovery:mat-...:req-...",
  "reason": "materialization_no_longer_requires_discovery",
  "desired_phase": "deleting",
  "observed_phase": "deleting"
}
```

`get_operation_record()` returns the **entire envelope** as a dict (`envelope.to_dict()`), not
just the payload — unlike every other getter, which returns a model. Callers must reach into
`record["payload"]`.

---

## 5.9 Event models

Published on Kafka topics. Full topic-level details in
[06 — Event and topic contracts](./06-event-and-topic-contracts.md).

### `MaterializationRequestSpec`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `tables` | `tuple[TableSpec, ...]` | — | at least one, unique names and topics (`validate_table_specs`): the tables the create view named, each in its dataset |
| `destination_uri` | `str` | — | required; the deployment's destination, as the API resolved it |
| `name` | `str \| None` | `None` | the label, `None` for none |
| `runtime_mode` | `str` | `"dedicated"` | ∈ `RUNTIME_MODES` |
| `runtime_name` | `str \| None` | `None` | the runtime's generated, length-bounded name |
| `namespace` | `str \| None` | `None` | the deployment's namespace |
| `resource_profile` | `str \| None` | `None` | — |
| `storage_size` | `str \| None` | `None` | the deployment's storage per replica, a Kubernetes quantity |
| `replicas` | `int \| None` | `None` | `>= 1` |
| `skip_tombstones` | `bool` | `False` | must be a `bool`; absent in older events, read as `False` |
| `autoscaling` | `bool \| None` | `None` | on a create, `None` is on; on an update, `None` leaves it as it is |
| `partition_granularity` | `str \| None` | `None` | ∈ `PARTITION_GRANULARITIES`; `None` takes `default_partition_granularity` |
| `labels` | `dict[str,str]` | `{}` | — |
| `annotations` | `dict[str,str]` | `{}` | — |

Emits all keys. Since 4.0 a request chooses none of `destination_uri`, `runtime_name`, `namespace` and
`storage_size`: the API fills them in from the deployment (04 §4.5.2), so the event says what the
materialization gets. An event from before 4.3 may carry `live_tail`, in its spec or on an update;
it is read and dropped.

### `MaterializationRequestEvent`

| Field | Type | Validation |
|-------|------|-----------|
| `request_id` | `str` | required |
| `materialization_id` | `str` | required |
| `action` | `str` | ∈ `REQUEST_ACTIONS` |
| `submitted_at` | `str` | required |
| `submitted_by` | `str` | required; the sender's email |
| `spec` | `MaterializationRequestSpec` | nested |
| `submitted_by_id` | `str \| None` | the sender's user id; omitted from `to_dict()` when `None` |
| `topic_change` | `TopicChangeRequest \| None` | an update's change of topics; omitted when `None` |
| `claim_request_id` | `str \| None` | the id of the `claim_tables` request intake sends for a create, or an update that adds tables: the API generated it, so the client can wait on it; omitted when `None` |
| `label` | `str \| None` | an update's new label; omitted when `None` |

`TopicChangeRequest`: `source_topics` (the topics to read from now on, in order; at least one),
`table_actions` (`{table: "drop" | "keep"}`, each ∈ `TABLE_ACTIONS`) and `new_tables`: the table each
new topic gets, in the dataset the request chose, under the name it chose or derived
(`new_table(topic)`).

### `EventCause`

| Field | Type | Validation |
|-------|------|-----------|
| `kind` | `str` | ∈ `CAUSE_KINDS` |
| `id` | `str` | required |

### `TargetRuntime`

| Field | Type | Default |
|-------|------|---------|
| `namespace` | `str \| None` | `None` |
| `kaspr_app_name` | `str \| None` | `None` |

`to_dict()` omits `None` fields, so an empty `TargetRuntime` serializes to `{}`.
`from_dict(None)` returns `None`.

### `ObservationPolicy`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `attempt` | `int \| None` | `None` | `>= 1` |
| `max_attempts` | `int \| None` | `None` | `>= 1` |
| `scheduled_for` | `str \| None` | `None` | — |
| `mode` | `str \| None` | `None` | ∈ `OBSERVATION_MODES` |

### `MaterializationWorkEvent`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `work_id` | `str` | — | required |
| `materialization_id` | `str` | — | required |
| `step` | `str` | — | ∈ `WORK_STEPS` |
| `requested_at` | `str` | — | required |
| `caused_by` | `EventCause` | — | nested |
| `dedupe_key` | `str` | — | required |
| `target_runtime` | `TargetRuntime \| None` | `None` | omitted when `None` |
| `observation_policy` | `ObservationPolicy \| None` | `None` | omitted when `None` |
| `consumer_lag` | `ConsumerLag \| None` | `None` | omitted when `None`; required for `record_consumer_lag`, the lag the refresh measured |
| `claim` | `TableClaimResult \| None` | `None` | required for `tables_claimed` and `claim_rejected`: the directory's answer — `request_id`, `purpose` (∈ `CLAIM_PURPOSES`), `status` (∈ `CLAIM_STATUSES`), and for a refusal `reason`, `message` and `details` (the table and dataset it was about) |

### `RuntimeStatusEvent`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `event_id` | `str` | — | required |
| `materialization_id` | `str` | — | required |
| `runtime_app_ref` | `str` | — | required |
| `member_id` | `str` | — | required |
| `agent_name` | `str` | — | required |
| `phase` | `str` | — | ∈ `("running","degraded")` |
| `health` | `str` | — | ∈ `("healthy","degraded","unknown")` |
| `reported_at` | `str` | — | required |
| `schema_versions` | `dict[str,int]` | `{}` | each table's schema id as the member last loaded it, `>= 0` |
| `runtime_version` | `str \| None` | `None` | the package version the member runs |
| `rows_written` | `int \| None` | `None` | `>= 0` |
| `bytes_written` | `int \| None` | `None` | `>= 0` |
| `files_written` | `int \| None` | `None` | `>= 0` |
| `records_skipped` | `int \| None` | `None` | `>= 0` |
| `latest_offsets` | `dict[str, dict[str,int]]` | `{}` | per table, per partition: the highest offset this member handled; numeric-string partition keys, `>= 0` values |
| `event_time` | `str \| None` | `None` | — |
| `phase_reason` | `str \| None` | `None` | — |
| `phase_message` | `str \| None` | `None` | — |
| `last_error` | `str \| None` | `None` | — |
| `health_reason` | `str \| None` | `None` | — |
| `settings_revision` | `int \| None` | `None` | `>= 0`; the live runtime-settings revision this member runs. Runtimes older than 0.5.0 send none |
| `settings_lineage` | `str \| None` | `None` | the lineage of those settings (17 §17.7); `""` when they have none. Runtimes older than 4.1 send none |
| `skip_reasons` | `list[dict]` | `[]` | `RuntimeSkipReason`s |
| `unmapped_values` | `int \| None` | `None` | values the member's seals wrote to `_unmapped` |
| `files_pending_commit` | `int \| None` | `None` | files uploaded and not yet committed |
| `tables` | `dict[str, dict] \| None` | `None` | the committer member's block for each table, by name (06 §6.5) |
| `table_counters` | `dict[str, dict[str,int]]` | `{}` | this member's own counters per table: rows, bytes and files written, values unmapped |

The heartbeat carries more than the model keeps — `latest_committed_offsets`, the upload backlog,
`performance` — for logs and the console's member view; `from_dict` ignores what it does not hold.

### `TableEvent`

`models/table.py`. On `materialization-table-events` (06 §6.6).

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `event_id` | `str` | — | required; `evt-table-<kind>-<16 hex>` |
| `kind` | `str` | — | ∈ `TABLE_EVENT_KINDS` |
| `materialization_id` | `str` | — | required |
| `table` | `str` | — | required: the table it is about. An event without one (from a 1.0 runtime) is dropped with a warning |
| `occurred_at` | `str` | — | required |
| `runtime_app_ref` | `str \| None` | `None` | the runtime's app name, or the maintenance app's (`MAINTENANCE_APP_NAME`) |
| `member_id` | `str \| None` | `None` | the committer member, or the maintenance app's member |
| `payload` | `dict` | `{}` | by kind (06 §6.6) |

### Maintenance messages (`models/maintenance.py`)

On `materializer-maintenance-requests`, keyed `maintenance:<materialization id>:<table>`
(`maintenance_routing_key`), for the maintenance app (07 §7.13, 10 §10.10).

| Field | Type | Notes |
|---|---|---|
| `kind` | `str` | ∈ `MAINTENANCE_MESSAGE_KINDS` |
| `target` | `MaintenanceTarget` | `materialization_id`, `namespace`, `table`, `location`, `source_topic`, `partition_granularity`, `state` |
| `issued_at` | `str` | required |
| `settings` | `dict` | tick, request: the runtimes' maintenance settings (`MAINTENANCE_SETTING_FIELDS`) as they are now |
| `last_runs` | `dict[str, str]` | tick: when each job last finished on the table |
| `running` | `RunningMaintenance \| None` | tick, cancel: the job the table record says runs |
| `job`, `request_id`, `requested_by` | `str \| None` | request: the job asked for, and by whom; `job` ∈ `MAINTENANCE_JOBS` |
| `reason` | `str \| None` | cancel: `table_dropping`, `materialization_deleting` |

### Live-tail messages (`models/live_tail.py`)

On `materializer-tail-requests`, keyed `tail:<materialization id>:<table>` (`live_tail_routing_key`),
for the tail app (06 §6.18, 19).

| Field | Type | Notes |
|---|---|---|
| `kind` | `str` | ∈ `LIVE_TAIL_MESSAGE_KINDS`: `tail_tick`, `tail_cancel` |
| `target` | `LiveTailTarget` | `materialization_id`, `dataset`, `dataset_id`, `table`, `location`, `source_topic`, `partition_granularity`, `table_uuid`, `skip_tombstones` |
| `issued_at` | `str` | required |
| `settings` | `dict` | tick: the live-tail settings as resolved that minute (`LIVE_TAIL_SETTING_FIELDS`, `services/live_tail.py`) |
| `hints` | `LiveTailHints \| None` | tick: the table record's `offsets` (partition as text → last offset in the table), `schema_id`, `metadata_location` - never ahead of the table, at most an event behind |
| `reason` | `str \| None` | cancel, required: ∈ `LIVE_TAIL_CANCEL_REASONS` - `table_dropping`, `table_retired`, `materialization_deleting`, `materialization_stopped`, and `live_tail_off`, which only a control plane before 4.3 sends |

## 5.10 Runtime-local models

`runtime_app/models/processing_state.py`. These are **not** stored in the control-plane table.

### `RuntimeMalformedRecord`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `partition` | `int` | — | `>= 0` |
| `offset` | `int` | — | `>= 0` |
| `schema_version` | `int \| None` | — | the schema id the member held; `None` (and omitted) before it loaded the table |
| `reason` | `str` | — | required |
| `raw_sample` | `Any` | `None` | omitted when `None` |
| `recorded_at` | `str \| None` | `None` | omitted when `None` |
| `category` | `str \| None` | `None` | the skip reason it is counted under; omitted when `None` |

### `RuntimeWriteResult`

The unit of progress accounting. Accumulated per record and merged.

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `rows_increment` | `int` | `0` | `>= 0` |
| `bytes_increment` | `int` | `0` | `>= 0` |
| `files_increment` | `int` | `0` | `>= 0` |
| `records_skipped_increment` | `int` | `0` | `>= 0` |
| `last_error` | `str \| None` | `None` | — |
| `last_error_kind` | `str \| None` | `None` | ∈ `("data_validation","infrastructure")` |
| `last_file_path` | `str \| None` | `None` | — |
| `last_malformed_record` | `RuntimeMalformedRecord \| None` | `None` | — |

`bytes_increment` is the **sealed Parquet object's size**, added once when the batch pipeline
finishes sealing a batch (`record_sealed_batch`), never at append time. `bytes_written` in status
therefore tracks the bytes sent to object storage, and moves in steps, one sealed object at a time.
(Until 0.3.1 the idle-flush seal added nothing, so a quiet topic, whose batches the upload task
sealed, reported no bytes at all.)

### `RuntimeSourceRecordMetadata`

| Field | Type | Default | Validation |
|-------|------|---------|-----------|
| `materialization_id` | `str` | — | required |
| `source_topic` | `str` | — | required |
| `partition` | `int` | — | `>= 0` |
| `offset` | `int` | — | `>= 0` |
| `member_id` | `str` | — | required |
| `observed_at` | `str` | — | required |
| `timestamp` | `str \| None` | `None` | the Kafka record timestamp, millisecond ISO-8601 UTC |
| `key` | `str \| None` | `None` | the record key as text: UTF-8, or `b64:` + base64 |
| `headers` | `tuple[tuple[str, str \| None], ...]` | `()` | `[name, value]` pairs, values decoded like the key |

`observed_at` is *processing* time and drives batch age; `timestamp` is *event* time and decides
the table partition a row goes to, and each file's `_kafka_timestamp` bounds.

### `RuntimeBatchState`

One table partition's batch on one member, from its first row until a commit covers it.

| Field | Meaning |
|-------|---------|
| `batch_id` | `batch-<member>-<table>-p<partition>-o<start_offset>` |
| `materialization_id`, `table`, `owner_member_id`, `partition` | identity: the table is the batch's topic's |
| `local_path` | the staged JSONL, kept until a commit covers the batch: a refused seal is sealed again from it |
| `state` | ∈ `BATCH_STATES` (10 §10.4) |
| `start_offset`, `end_offset` | inclusive offset range |
| `after_offset` | where the batch continues its partition from: every record in `(after_offset, end_offset]` is in it or needed no row |
| `session_generation` | the member's session when it was opened (10 §10.5) |
| `row_count`, `estimated_bytes` | staged rows and JSONL bytes: the roll policy's and the backlog's measures |
| `event_time_min`, `event_time_max` | from the Kafka timestamps |
| `seal_id`, `files`, `schema_id`, `unmapped_values` | the seal: its id, its `SealedFile`s (one per table partition), the schema it conformed to |
| `observations` | drift observations among the staged rows; the seal waits for the schema to take them, up to `seal_schema_wait_seconds` |
| `announced_at`, `announce_attempts`, `reseals` | the files' announcement, and how often the batch was sealed again |
| `upload_attempts`, `seal_attempts`, `last_error` | retries |
| `created_at`, `rolled_at`, `updated_at` | lifecycle |

### `SealedFile`

One Parquet file of a seal: `local_path`, `uri`, `partition_text` (`2026-10-18`, `__null__`, or
`None`), `partition_value`, `record_count`, `file_size`, `event_time_min`, `event_time_max`,
`uploaded`.

### Commit planning (`lakehouse/commits.py`)

The committer's models, in its process only:

| Model | Holds |
|---|---|
| `AnnouncedFile` | one `data_file_ready` announcement (06 §6.8) |
| `Seal` | the files of one seal, complete once all `file_count` have arrived |
| `PendingFiles` | every announcement not yet decided, by seal, and the newest session (epoch, generation) heard from each member and partition, which decides what is stale (10 §10.6) |
| `FileDecision` | a decision on one file: `accepted`, `covered`, `overlap`, `deferred`, `error`, with `covered_through` |
| `AppendPlan`, `AppendResult` | the seals one append takes and what it committed: snapshot id, files, records, bytes, offsets |

---

## 5.11 The runtime's state table (`<rt>-state`)

One global Kaspr table per runtime, one partition, written only by the committer agent (Kaspr
writes a table only from within a stream event) and read by every member. Its keys are in 10 §10.8,
one set per table: `table:<table>` (totals and the metadata location), `schema:<table>`
(`SchemaRecord`: the schema, fork map and rejected paths every member conforms to, and the
`table_uuid` it came from), `committed-offsets:<table>` (with its `table_uuid`),
`file-decisions:<table>`; and `runtime-settings` (§5.13). Members ignore a `schema:` or
`committed-offsets:` entry of another table under the same name (10 §10.8, *Incarnations*). Everything else a member holds — open batches, the ack ledger, its
pipeline — is process memory, rebuilt from the topic and the table after a restart (10 §10.9).

---

## 5.12 `MaterializationTable` — the table record

`models/table.py`. Key `materialization-table:<id>:<table>`, entity type `materialization-table`:
one record per table.

Created by the `create_table` step, kept current by the `table-events` agent from what the
runtime's committer publishes (06 §6.6). The table in the bucket is the truth; this record is what
the API and the console's Overview answer from. `list_materialization_tables(id)` reads a
materialization's records with one prefix scan.

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `materialization_id` | `str` | — | required |
| `dataset`, `dataset_id` | `str` | — | the dataset it is in: its Iceberg namespace (`namespace` reads `dataset`) |
| `name` | `str` | — | the table's name; its identifier is `(dataset, name)`, `qualified_name` `<dataset>.<name>` |
| `source_topic` | `str` | — | the topic it holds |
| `location` | `str` | — | `<destination>/<storage prefix>/<materialization id>/<table>` (11 §11.1) |
| `table_uuid` | `str` | — | the Iceberg table's uuid |
| `partition_granularity` | `str` | `"day"` | `hour`, `day`, `month`, `none` |
| `created_at`, `updated_at` | `str` | — | required |
| `metadata_location` | `str \| None` | `None` | the newest metadata file reported |
| `current_snapshot_id`, `sequence_number` | `int \| None` | `None` | of the newest commit reported |
| `last_commit_at`, `last_commit_duration_ms` | | `None` | |
| `totals` | `dict[str,int]` | `{}` | `records`, `files`, `bytes` |
| `offsets` | `dict[str,int]` | `{}` | committed Kafka offsets by partition: the last offset of each partition the table holds, inclusive |
| `valid_through` | `str \| None` | `None` | every partition's records up to this instant are in the table |
| `snapshots` | `int \| None` | `None` | snapshots the metadata holds |
| `schema_id` | `int \| None` | `None` | the newest schema id reported |
| `maintenance` | `tuple[MaintenanceRun, ...]` | `()` | the newest 50 finished jobs, newest first |
| `last_error` | `TableError \| None` | `None` | the open table error, cleared by the next commit. The status names the table: `Table orders: …`; it stays degraded while any written table, or one whose drop is failing, has an open error (`error_holds`) |
| `state` | `str` | `"active"` | ∈ `TABLE_STATES` (11 §11.7): `active` written; `retired` kept read-only; `dropping`; `dropped` with its files kept |
| `maintenance_job` | `RunningMaintenance \| None` | `None` | the compaction or orphan cleanup the maintenance app runs on it: set by `maintenance_started`, cleared by the `maintenance_completed` with its `started_at` |

`MaintenanceRun`: `job`, `completed_at`, `outcome` (`succeeded`, `skipped`, `failed`, `abandoned`,
`cancelled`), `duration_ms`, `snapshot_id`, `counts`, `message`, `partition` (a compaction's
partitions, as `2026-10-18` or `2024-10-05 … 2024-11-21 (47 partitions)`), `event_id` (so a
redelivered event is recorded once), `started_at` (a long job's).
`RunningMaintenance`: `job`, `started_at`, `partition`, `attempt`, `member_id`.
`TableError`: `reason`, `message`, `occurred_at`.

Events for a table being dropped, or dropped, are ignored, but for the `maintenance_completed` that
ends its running job. A compaction's report changes the record's totals and snapshot only if its
sequence number is newer than the record's: the maintenance app and the runtime report apart, and the
runtime's next commit may arrive first.

---

## 5.13 Settings models

`models/settings.py`. Every model round-trips through `to_dict()` / `from_dict()`. See
[17 — Settings](./17-settings.md) for how they are used.

Constants: `SETTINGS_SCOPES = ("control-plane", "runtime", "console")`,
`SETTING_SOURCES = ("default", "deployment", "override")`, `RECENT_REQUEST_LIMIT = 20`.

### `SettingsDocument` — one scope's overrides

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `scope` | `str` | — | one of `SETTINGS_SCOPES` |
| `revision` | `int` | `0` | applied changes; only increases |
| `values` | `dict[str, SettingOverride]` | `{}` | the overrides in force, by key |
| `updated_at`, `updated_by` | `str \| None` | `None` | the last applied change |
| `recent_request_ids` | `list[str]` | `[]` | the last 20 applied requests, oldest first, for redelivery |
| `last_rejection` | `SettingsRejection \| None` | `None` | the last refused request |
| `lineage` | `str \| None` | `None` | `runtime` scope only: the history its revisions belong to, `<UTC time, %Y%m%dT%H%M%SZ>-<8 hex>`; begun once, never changed (17 §17.7). `None` until the control plane's next start on a document stored before 4.1 |
| `lineage_revision` | `int \| None` | `None` | the revision the scope had when its lineage began, `<= revision`; deliveries up to it went out without a lineage. Set with `lineage` |

`SettingOverride`: `value`, `updated_at`, `updated_by`.
`SettingsRejection`: `request_id`, `reason` (`settings_changed` or `invalid_setting`),
`message`, `rejected_at`.

### `SettingsChangeRequest` — on `materializer-settings-requests`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `request_id` | `str` | — | `set-<12 hex>` |
| `scope` | `str` | — | |
| `submitted_at`, `submitted_by` | `str` | — | `submitted_by` is the sender's email |
| `values` | `dict[str, Any]` | `{}` | settings to override |
| `reset` | `list[str]` | `[]` | settings to return to their fallback; disjoint from `values` |
| `base_revision` | `int \| None` | `None` | the revision the client read |
| `fallbacks` | `dict[str, SettingFallback]` | `{}` | `console` only: `{value, source}` each changed setting reads as without an override |

A request must set or reset at least one setting.

### `SettingsBootstrapRequest` — on `materializer-settings-requests`

The `settings-bootstrap` task's startup request: `{"kind": "bootstrap", "scope": "runtime",
"requested_at": …}`, keyed `settings:runtime`. `SettingsBootstrapRequest.matches(value)` tells it
from a change request. The agent begins the runtime scope's lineage if it has none.

### `SettingsChangeRecord` — one applied change, for the history

| Field | Type | Notes |
|-------|------|-------|
| `scope`, `revision`, `request_id`, `updated_at`, `updated_by` | | the applied request |
| `changes` | `list[SettingChange]` | one per setting whose value or source changed |

`SettingChange`: `key`, `before`, `after`, `before_source`, `after_source` (each a `SETTING_SOURCES`
value).

### The runtime's copy

A runtime keeps the settings delivered to it in its own `<rt>-state` table, under
`runtime-settings` (`runtime_app/live_settings.py::RUNTIME_SETTINGS_KEY`):

```json
{ "revision": 4, "lineage": "20261024T101502Z-3f9a1c2e", "values": { "max_open_file_age_seconds": 60 },
  "published_at": "2026-10-09T13:02:11Z", "applied_at": "2026-10-09T13:02:12Z" }
```

`lineage` is `null` for settings delivered without one, and absent from a copy stored before 4.1.

---

## 5.14 Identity models

`models/identity.py`. Every model round-trips through `to_dict()` / `from_dict()` and emits its
full key set. See [18 — Identity and access](./18-identity-and-access.md) for how they are used.

### `User`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `user_id` | `str` | — | `usr-<12 hex>`; never changes |
| `email` | `str` | — | unique without regard to case; how they sign in |
| `first_name`, `last_name` | `str` | — | at most 100 characters each |
| `status` | `str` | `"active"` | ∈ `USER_STATUSES` |
| `group_ids` | `list[str]` | `[]` | groups they were added to; Guest and All Users are implied |
| `password` | `PasswordCredential \| None` | `None` | `{hash, set_at, set_by, must_change}`; `must_change` is set whenever someone else chose it |
| `external_identities` | `list[ExternalIdentity]` | `[]` | `{provider, subject}` per single-sign-on account; reserved |
| `session_epoch` | `int` | `0` | raised to end every session at once |
| `revision` | `int` | `1` | raised by every change; `base_revision` is checked against it |
| `created_at`, `created_by`, `updated_at`, `updated_by` | `str` | — | `*_by` is an email, or `system` |
| `deactivated_at`, `deactivated_by` | `str \| None` | `None` | |

### `Group`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `group_id` | `str` | — | `grp-<12 hex>`, or a built-in id: `guest`, `all-users`, `viewer`, `editor`, `admin` |
| `name` | `str` | — | unique without regard to case; at most 80 characters |
| `description` | `str` | `""` | at most 500 characters |
| `role_ids` | `list[str]` | `[]` | |
| `membership` | `str` | `"assigned"` | ∈ `GROUP_MEMBERSHIPS` |
| `built_in` | `bool` | `False` | |
| `external_groups` | `list[str]` | `[]` | identity-provider groups whose members belong here; reserved |
| `revision`, `created_*`, `updated_*` | | | as for `User` |

Membership is stored on each `User` (`group_ids`), not on the group.

### `Role`

`role_id` (`rol-<12 hex>` or a built-in id), `name`, `description`, `permissions` (keys from the
catalog; `*` only in the built-in Administrator role), `built_in`, `revision`, `created_*`,
`updated_*`. Built-in roles are never stored.

### `Session`

| Field | Type | Notes |
|-------|------|-------|
| `session_id` | `str` | `ses-<16 hex>`; the token's `sid` |
| `user_id` | `str` | |
| `method` | `str` | `password`; `sso:<provider>` in future |
| `created_at`, `expires_at`, `last_seen_at` | `str` | `expires_at` is the absolute end; `last_seen_at` moves with each renewal |
| `ip`, `user_agent` | `str \| None` | as reported |
| `ended_at`, `ended_reason`, `ended_by` | `str \| None` | set when it was ended rather than expired |

### `AccessToken`

A credential someone made for a tool, such as an engine reading the REST catalog (18 §18.15).

| Field | Type | Notes |
|-------|------|-------|
| `token_id` | `str` | the token's public part: `tok_<token_id>_<secret>` is the whole token |
| `user_id` | `str` | its owner; the token acts as them, with at most `permissions` |
| `name` | `str` | what the owner called it, up to 64 characters |
| `permissions` | `tuple[str, ...]` | ⊆ `TOKEN_PERMISSIONS` (`catalog.read`, `catalog.write`) |
| `datasets` | `tuple[str, ...]` | the datasets it is limited to, by id; empty, every dataset its owner may use it on |
| `secret_hash` | `str` | the secret's hash; the secret itself is shown once and never stored |
| `created_at`, `expires_at` | `str` | at most 365 days apart |
| `revoked_at`, `revoked_by` | `str \| None` | set when revoked |

### `SignInGuard`, `UserActivity`, `DirectoryState`

- `SignInGuard`: `email`, `failed_attempts`, `first_failed_at`, `last_failed_at`, `locked_until`.
- `UserActivity`: `user_id`, `last_sign_in_at`, `last_active_at`, `sign_in_count`.
- `DirectoryState`: `revision` (raised by every change to people, groups, roles, datasets or grants;
  grants are cached against it), `updated_at`, `seeded_at`.

### `Dataset`, `DatasetGrant`, `TableClaim`, `ClaimRelease`

- `Dataset`: `dataset_id` (`ds-<12 hex>`), `name` (lowercase, unique, fixed), `description`,
  `revision`, `created_at`, `created_by`, `updated_at`, `updated_by`.
- `DatasetGrant`: `dataset_id`, `principal_type` (∈ `GRANT_PRINCIPAL_TYPES`), `principal_id`,
  `role_ids` (dataset roles), `updated_at`, `updated_by`.
- `TableClaim`: `dataset_id`, `table`, `materialization_id`, `source_topic`, `claimed_at`,
  `claimed_by`, `request_id` — `<dataset>.<table>` taken by one materialization's table (18 §18.16).
- `ClaimRelease`: `materialization_id`, `released_at` — every name of a materialization being deleted
  released; a later claim of it is refused.

### `IdentityRequest` — on `materializer-identity-requests`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `request_id` | `str` | — | `idr-<12 hex>` |
| `action` | `str` | — | ∈ `IDENTITY_ACTIONS` |
| `submitted_at` | `str` | — | |
| `actor_id` | `str` | — | a user id, `anonymous` for a failed sign-in with an unknown email, or `system` |
| `actor_email` | `str \| None` | `None` | |
| `payload` | `dict` | `{}` | per action ([04 §4.13](./04-http-api-reference.md)); a password only ever as `password_hash` |
| `base_revision` | `int \| None` | `None` | `>= 1` when set |
| `context` | `RequestContext` | `{ip: null, user_agent: null}` | where it came from |

`tracked` is true for `DIRECTORY_ACTIONS`: only those leave an outcome.

### `IdentityRequestOutcome`

`request_id`, `action`, `status` (∈ `REQUEST_STATUSES`), `completed_at`, `actor_id`,
`entity_type`, `entity_id`, and for a refusal `reason` (`invalid`, `not_found`,
`already_exists`, `changed`, `forbidden`, `self_action`, `last_administrator`, `built_in`,
`locked_role`, `dataset_not_empty`, `dataset_not_found`, `table_name_taken`,
`materialization_deleted`), `message`, `field` and `details` — for a claim, the `index`,
`dataset`, `table` and `source_topic` it was about.

### `AuditEntry`

`entry_id` (`aud-<14 hex µs since the epoch><4 hex>`, so it orders entries of the same second), `occurred_at`, `action` (e.g. `user.created`; the full list is in
[04 §4.13.4](./04-http-api-reference.md)), `outcome` (∈ `AUDIT_OUTCOMES`), `actor_id`,
`actor_email`, `target_type` (`user`, `group`, `role`, `email`, `directory`), `target_id`,
`target_label`, `details` (what changed, e.g. `{"name": {"from": …, "to": …}}`), `ip`,
`user_agent`. Names and emails are copied in, so an entry reads correctly after its subject is
renamed or deleted.

---

## 5.15 Model → storage → API mapping

| Model | Key prefix | Written by | Read by |
|-------|-----------|------------|---------|
| `Materialization` | `materialization:` | `request_intake`, `workflow` | list view, detail view, all services |
| `MaterializationStatus` | `materialization-status:` | `request_intake`, `workflow`, `observation`, `runtime_status` | list view, detail view |
| `MaterializationSchema` | `materialization-schema:<id>:<table>`, `materialization-schema-version:<id>:<table>:` | `workflow` (discovery), `table_events` | schema views, the REST catalog's `fork-map` readers |
| `MaterializationTable` | `materialization-table:<id>:<table>` | `workflow` (`create_table`), `table_events` | detail and table views, the REST catalog, `workflow` (apply, drop) |
| `RuntimeBinding` | `runtime-binding:` | `workflow` (apply) | detail view, `observation`, `workflow` (delete) |
| `RuntimeAutoscale` | `runtime-autoscale:` | `autoscaling` (on `record_consumer_lag`), `request_intake` (forgets it) | detail view, `workflow` (apply), `observation` (drift) |
| operation records | `materialization-op:` | `workflow` | `workflow` |
| `SettingsDocument` | `settings:` (settings table) | `settings` (the `settings-intake` agent) | `resolve_config` in every handler, settings views |
| `SettingsChangeRecord` | `settings-change:` (settings table) | `settings` | `GET /settings/{scope}/history` |
| `User`, `Group`, `Role`, `Session`, `AccessToken`, `SignInGuard`, `UserActivity`, `DirectoryState`, `IdentityRequestOutcome`, `Dataset`, `DatasetGrant`, `TableClaim`, `ClaimRelease` | §5.2.2 (directory table) | `identity` (the `identity-intake` agent) | `authorize` in every WebView, the auth, directory and dataset views, the REST catalog (claims), the claim sweep |
| `AuditEntry` | `audit:` (identity-audit table) | `identity` | `GET /audit` |

Everything else (`MaterializationRequestEvent`, `MaterializationWorkEvent`,
`RuntimeStatusEvent`, `TableEvent`, `SettingsChangeRequest`, `IdentityRequest`) is **transient** —
Kafka messages only, never persisted in a table.

---

## 5.16 Removed in 1.0

| 0.x model | In 1.0 |
|-----------|--------|
| `AuthoritativeSchema` (`materialization-schema:`) | the table's Iceberg schema; the control plane keeps a projection of it (§5.5) |
| `SchemaProposalEvent`, `FieldProposal`, `BreakingChange` | `schema_change_request` on `<rt>-commits`, shapes only (06 §6.8) |
| `RuntimeSignalEvent` | none: nothing signals a runtime |
| The `schema-proposal-buffer` operation record | the committer's schema window (09 §9.7) |
| `RuntimePartitionCheckpoint`, `RuntimeBatchManifest`, `RuntimeUploadedMarker`, `RuntimeCommitMarker` | the table's snapshot summaries carry the committed offsets (11 §11.4); a batch's state is in member memory until a commit covers it (§5.10) |
| The table-backed `runtime_app/storage/processing_state.py` | removed: `runtime_app/state.py` is the only runtime state |

## 5.17 Changed in 2.0

A materialization reads one or more topics, one table each. No record is migrated: a 1.0
materialization is deleted and created again.

| 1.0 | 2.0 |
|-----|-----|
| `Materialization.source_topic`, `MaterializationRequestSpec.source_topic` | `tables: tuple[TableSpec, ...]` (and `source_topics` in `to_dict`) |
| `Materialization.schema_version`, `RuntimeBinding.schema_version` | each table record's `schema_id`; the binding's `source_topics` |
| `materialization-schema:<id>`, `materialization-schema-version:<id>:<n>`, `materialization-table:<id>` | the same with `:<table>` (§5.2) |
| `RuntimeStatusEvent.schema_version`, flat `latest_offsets`, `table` | `schema_versions`, `latest_offsets` per table, `tables`, `table_counters` |
| `RuntimeProgress.table`, flat `latest_offsets` | `tables`, `latest_offsets` per table, `cumulative_unmapped_by_table` |
| `ConsumerLag.partition_lag`, `end_offsets`, `partitions` | `topics: dict[str, TopicLag]`, each with its own |
| `TableEvent` without a table | `table`, required |
| A materialization's topics fixed at creation | `Materialization.retired_tables` (kept tables), `MaterializationTable.state` (`TABLE_STATES`), `RuntimeBinding.drain_started_at` and its added topics: an edit changes the topics (§5.4, §5.6, §5.12) |
| Maintenance run by the committer, requested on `<rt>-commits` | `MaintenanceMessage` on `materializer-maintenance-requests` for the maintenance app (§5.9); `MaterializationTable.maintenance_job`; the `maintenance_started` table event |
| The runtime state table's entries keyed by table name only | each stamped with its table's `table_uuid`, so a table re-created under the same name never reads its predecessor's schema or offsets (§5.11) |

## 5.18 Changed in 3.0

Tables are in datasets. No record is migrated: a 2.0 materialization is deleted and created again,
and 3.0 skips any 2.0 materialization or table record it finds (`read_payload`).

| 2.0 | 3.0 |
|-----|-----|
| `TableSpec {name, source_topic}` | `{name, source_topic, dataset, dataset_id}` |
| `Materialization.name`: the tables' namespace, unique, fixed | a label: free text, not unique, changeable, defaulting to the id |
| `MaterializationTable.namespace` (the materialization's name) | `dataset`, `dataset_id`; `namespace` is a property reading `dataset` |
| A table at `<destination>/<namespace>/<name>-<hex12>/<table>` | `<destination>/<storage prefix>/<materialization id>/<table>` |
| `TopicChangeRequest {source_topics, table_actions}` | adds `new_tables`; `Materialization.pending_topic_change` |
| `MaterializationRequestEvent` | adds `claim_request_id`, `label` |
| `MaterializationWorkEvent` | adds `claim` (`TableClaimResult`) for the new steps `tables_claimed`, `claim_rejected` |
| The `identity` table | `directory`, adding `Dataset`, `DatasetGrant`, `TableClaim`, `ClaimRelease` (§5.2.2) |
| `AccessToken` | adds `datasets` |
| `IdentityRequestOutcome` | adds `details` |
