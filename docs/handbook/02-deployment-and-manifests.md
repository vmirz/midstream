# 02 — Deployment and Manifests

Everything in this system is deployed as Kubernetes custom resources managed by the Kaspr
operator (`kaspr.io/v1alpha1`) plus two RBAC objects. There are no Deployments, Services, or
Ingresses authored in this repository — the Kaspr operator derives those from `KasprApp`.

## 2.1 Kustomize structure

```
manifests/
  kustomization.yaml            resources: [./dev, ./staging, ./prod]   ← aggregate, rarely used directly
  base/
    kustomization.yaml          100 resources + configurations: kustomconfig.yaml
    kustomconfig.yaml           (empty file — placeholder)
    apps/control-plane.yaml
    agents/{identity-intake,maintenance,request-intake,resource-observation,runtime-status,
            settings-intake,table-events,workflow-apply}.yaml
    tasks/{identity-bootstrap,identity-maintenance,purge-deleted-materializations,
           refresh-consumer-lag,settings-bootstrap,sweep-observations,
           sweep-observations-startup}.yaml
    tables/{state,settings,directory,identity-audit}.yaml
    roles/{control-plane-kasprapp-role,control-plane-kasprapp-rolebinding}.yaml
    webviews/materializations-{list,create,detail,schema,schema-versions,schema-version,
                              table,maintenance,stop,resume,update,delete}.yaml
    webviews/catalog-{config,namespaces,namespace,tables,table,table-commit,table-metrics,
                      tables-create,table-drop,transactions}.yaml
    webviews/settings-{read,update,history}.yaml
    webviews/{system-version,system-settings,topics-analyze,topics-list}.yaml
    webviews/auth-{login,logout,refresh,session,sessions,password,tokens-list,tokens-create,
                   tokens-revoke}.yaml
    webviews/users-{list,detail,create,update,delete,password,sign-out,unlock}.yaml
    webviews/{groups,roles}-{list,detail,create,update,delete}.yaml
    webviews/{permissions-list,audit-list,identity-request}.yaml
    webviews/datasets-{list,create,detail,update,delete,grants,grant-update,table}.yaml
  dev/kustomization.yaml        namePrefix: develop- + ~80 JSON-6902 patches   ← the only substantive overlay
  staging/kustomization.yaml    resources: [../base]                            ← pass-through
  prod/kustomization.yaml       resources: [../base]                            ← pass-through
```

Render and validate:

```bash
kubectl kustomize manifests/base
kubectl kustomize manifests/dev
kubectl kustomize manifests/staging
kubectl kustomize manifests/prod
```

Apply:

```bash
kubectl apply -k manifests/dev
```

> `manifests/base/kustomconfig.yaml` is an **empty file** referenced under `configurations:`.
> Kustomize tolerates it. Do not delete it without also removing the reference.

## 2.2 Resource inventory (base)

| Kind | Name | Purpose |
|------|------|---------|
| KasprApp | `materializer-control-plane` | The control-plane application |
| KasprTable | `state` | Global single table for all control-plane records |
| KasprTable | `settings` | Global table of settings overrides and their history ([17](./17-settings.md)) |
| KasprTable | `directory` | Global table of users, groups, custom roles, datasets, their grants, the claims of table names, sessions and sign-in state ([18](./18-identity-and-access.md)); `identity` before 3.0 |
| KasprTable | `identity-audit` | Global table of the audit log of sign-ins and directory changes |
| KasprAgent | `request-intake` | `materialization-requests` → persist → `materialization-work` |
| KasprAgent | `workflow-apply` | Executes discovery, table creation, apply, delete, Kafka cleanup and table drop |
| KasprAgent | `resource-observation` | Executes `observe_runtime` and `sweep_observations` |
| KasprAgent | `runtime-status` | Applies runtime heartbeats to persisted status |
| KasprAgent | `table-events` | Applies what runtimes report about their tables to the table records and schema projections |
| KasprAgent | `maintenance` | The retention sweep, the consumer-lag refresh and the maintenance and tail apps' table ticks, asked for by the tasks below; records each measured lag |
| KasprAgent | `settings-intake` | `materializer-settings-requests` → apply to the `settings` table; delivers runtime settings |
| KasprAgent | `identity-intake` | `materializer-identity-requests` → apply to the `directory` and `identity-audit` tables; answer claims of table names on `materialization-work` |
| KasprTask | `sweep-observations` | Cron `*/2 * * * *`, `onLeader: true` |
| KasprTask | `sweep-observations-startup` | Runs once at startup, `onLeader: true` |
| KasprTask | `refresh-consumer-lag` | Cron `*/1 * * * *` |
| KasprTask | `purge-deleted-materializations` | Cron `17 * * * *` |
| KasprTask | `schedule-table-maintenance` | Cron `*/1 * * * *`, `onLeader: true`: ticks every maintained table for the maintenance app (07 §7.13) |
| KasprTask | `identity-bootstrap` | Runs once at startup, `onLeader: true`: built-in groups and the first administrator |
| KasprTask | `settings-bootstrap` | Runs once at startup, `onLeader: true`: begins the runtime settings' lineage if they have none (17 §17.7) |
| KasprTask | `identity-maintenance` | Cron `41 * * * *`, `onLeader: true`: expired sessions, stale counts, audit retention |
| KasprWebView | `materializations-list` | `GET /materializations` |
| KasprWebView | `materializations-create` | `POST /materializations` |
| KasprWebView | `materializations-detail` | `GET /materializations/{materialization_id}` |
| KasprWebView | `materializations-schema` | `GET /materializations/{materialization_id}/tables/{table_name}/schema` |
| KasprWebView | `materializations-stop` | `POST /materializations/{materialization_id}/stop` |
| KasprWebView | `materializations-resume` | `POST /materializations/{materialization_id}/resume` |
| KasprWebView | `materializations-update` | `PATCH /materializations/{materialization_id}` |
| KasprWebView | `materializations-schema-versions` | `GET /materializations/{materialization_id}/tables/{table_name}/schema/versions` |
| KasprWebView | `materializations-schema-version` | `GET /materializations/{materialization_id}/tables/{table_name}/schema/versions/{schema_version}` |
| KasprWebView | `materializations-table` | `GET /materializations/{materialization_id}/tables/{table_name}` |
| KasprWebView | `materializations-maintenance` | `POST /materializations/{materialization_id}/maintenance` |
| KasprWebView | `materializations-delete` | `DELETE /materializations/{materialization_id}` |
| KasprWebView | `system-settings` | `GET /system/settings` |
| KasprWebView | `system-version` | `GET /system/version` |
| KasprWebView | `topics-list` | `GET /topics` |
| KasprWebView | `topics-analyze` | `POST /topics/analyze` |
| KasprWebView | `settings-read` | `GET /settings/{scope}` |
| KasprWebView | `settings-update` | `PATCH /settings/{scope}` |
| KasprWebView | `settings-history` | `GET /settings/{scope}/history` |
| KasprWebView | `auth-login`, `auth-logout`, `auth-refresh`, `auth-session`, `auth-sessions`, `auth-password` | `/auth/*`: sign in and out, renew, who am I, my sessions, change my password |
| KasprWebView | `auth-tokens-list`, `auth-tokens-create`, `auth-tokens-revoke` | `/auth/tokens[/{token_id}]`: my access tokens |
| KasprWebView | `catalog-*` (10) | `/catalog/v1/…`: the Iceberg REST catalog (11 §11.8) |
| KasprWebView | `users-list`, `users-detail`, `users-create`, `users-update`, `users-delete`, `users-password`, `users-sign-out`, `users-unlock` | `/users` and `/users/{user_id}[/password\|/sign-out\|/unlock]` |
| KasprWebView | `groups-{list,detail,create,update,delete}`, `roles-{list,detail,create,update,delete}` | `/groups[/{group_id}]`, `/roles[/{role_id}]` |
| KasprWebView | `permissions-list`, `audit-list`, `identity-request` | `GET /permissions`, `GET /audit`, `GET /identity/requests/{request_id}` |
| KasprWebView | `datasets-list`, `datasets-create`, `datasets-detail`, `datasets-update`, `datasets-delete`, `datasets-grants`, `datasets-grant-update`, `datasets-table` | `/datasets`, `/datasets/{dataset}`, its grants and its tables (04 §4.15) |
| KasprApp | `materializer-maintenance` | **The maintenance app** (`maintenance/app.yaml`): compaction, expiry and orphan cleanup of every materialization's tables (10 §10.10) |
| KasprAgent | `maintenance-worker` | the maintenance app's: `materializer-maintenance-requests` → its worker; table events → `materialization-table-events` |
| KasprTask | `maintenance-step` | the maintenance app's: every 5 s on every member, finishes jobs and starts those due |
| KasprApp | `materializer-tail` | **The tail app** (`tail/app.yaml`): the newest records of the tables people watch, served to the console before they are committed (19) |
| KasprAgent | `tail-worker` | the tail app's: `materializer-tail-requests` (64 partitions) → the ticks and cancels of the tables the member owns |
| KasprTask | `tail-step` | the tail app's: every 5 s on every member — its loops, seeding, parking, trims and bounds |
| KasprWebView | `tail-rows`, `tail-head`, `tail-health`, `tail-members` | the tail app's `/tail/v1/…` (19 §19.9), for the console alone |
| Role | `materializer-control-plane-kasprapp-manager` | get/create/patch/delete on Kaspr CRDs; get/list on pods |
| RoleBinding | `materializer-control-plane-kasprapp-manager` | Binds Role to SA `materializer-control-plane-app` |

## 2.3 The control-plane KasprApp

`manifests/base/apps/control-plane.yaml`

### Core spec

```yaml
spec:
  replicas: 1
  bootstrapServers: crs-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
  authentication:
    type: scram-sha-512
    username: materializer-control-plane-user
    passwordSecret: { secretName: materializer-control-plane-user, passwordKey: password }
  storage: { type: persistent-claim, class: default, size: 1Gi, deleteClaim: True }
  config:
    schedulerEnabled: true
    schedulerCronEnabled: true
    valueSerializer: json
    topicAllowDeclare: true
    webBasePath: /
    webPort: 6065
  resources:
    requests: { cpu: 0.5, memory: 512Mi }
    limits:   { cpu: 1,   memory: 512Mi }
  verticalPodAutoscaler:
    enabled: true
    updateMode: InPlaceOrRecreate
    controlledResources: [cpu]
    controlledValues: RequestsOnly
    minAllowed: { cpu: 100m }
    maxAllowed: { cpu: 900m }
```

Notes:

- `replicas: 1`. The control plane is intentionally single-replica; the two sweep tasks are
  additionally guarded with `onLeader: true`.
- `verticalPodAutoscaler`: the CPU request follows the pod's use, resized in place, between 100m
  and 900m; the limits stay the ceilings (§2.10c).
- `schedulerEnabled` + `schedulerCronEnabled` are **required**. Delayed work (observation
  backoff, schema-buffer flush, kafka-cleanup retries) uses the Kaspr scheduler topic.
- `topicAllowDeclare: true` lets agents declare their input topics (`declare: true`) instead of
  requiring pre-created topics.
- `webPort: 6065` and `webBasePath: /` define the HTTP surface.

### Package installation

```yaml
pythonPackages:
  packages:
    - git+ssh://git@github.com/TotalWineLabs/kaspr-kafka-materializer.git@main#egg=materializer-control-plane&subdirectory=python
  installPolicy: { retries: 3, timeout: 600, onFailure: block }
```

The control plane installs **itself** from git over SSH at pod start. `onFailure: block`
means a failed install prevents the app from starting rather than running a stale version.

The dev overlay replaces `@main` with `@develop`.

### Secrets consumed

| Secret | Key(s) | Mounted at | Used for |
|--------|--------|------------|----------|
| `github-deploy-key` | `id_ed25519`, `known_hosts` | `/var/run/secrets/github-ssh` (init container, `defaultMode: 256`) | Cloning the package over SSH |
| `kafka-materializer-gcp-credentials` | `google_application_credentials.json` | `/app/gcp` (kaspr container, read-only) | GCS access via `GOOGLE_APPLICATION_CREDENTIALS` |
| `materializer-control-plane-user` | `password` | referenced by `authentication.passwordSecret` | Kafka SCRAM-SHA-512 auth |
| `materializer-auth` (`develop-materializer-auth` in dev) | `signing-key`, `previous-signing-key`, `service-token`, `bootstrap-admin-email`, `bootstrap-admin-password` | `MATERIALIZER_AUTH_*` env vars, each `optional: true` | Session signing, the console's service token, the first administrator ([18](./18-identity-and-access.md) §18.11) |

**Never** commit, echo, or reproduce the contents of these secrets. They are referenced by
name only.

Every `materializer-auth` key is optional so that a deployment without the Secret still starts.
Without `signing-key`, though, nobody can sign in and every request is a guest's; create the Secret
before the first deploy ([18](./18-identity-and-access.md) §18.12).

### Init container SSH configuration

```yaml
pythonPackagesInitContainer:
  env:
    - name: GIT_SSH_COMMAND
      value: >-
        ssh -i /var/run/secrets/github-ssh/id_ed25519
        -o IdentitiesOnly=yes
        -o UserKnownHostsFile="/tmp/github_known_hosts /var/run/secrets/github-ssh/known_hosts"
        -o StrictHostKeyChecking=accept-new
```

### Scheduling constraints

Both the control plane and every generated runtime carry:

```yaml
nodeSelector: { app: sipapi }
tolerations:
  - { effect: NoSchedule, key: app, operator: Equal, value: twmapp }
```

### The embedded runtime app spec

`MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC` is a **YAML document embedded in an environment
variable**. It is parsed by `config._load_runtime_app_spec()` and used as the base `spec:` for
every generated runtime `KasprApp`. Template rendering merges/overrides on top of it.

Base value (abridged; see `control-plane.yaml:115-204`):

```yaml
replicas: 16                     # replaced by each materialization's own replica count
bootstrapServers: crs-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
authentication: { type: scram-sha-512, username: materializer-control-plane-user, ... }
storage: { type: persistent-claim, class: default, size: 64Mi, deleteClaim: true }   # replaced by the materialization's storage_size
resources:                       # every runtime pod, whatever its topic - see below
  requests: { cpu: 0.5, memory: 1Gi }
  limits:   { cpu: 2,   memory: 1Gi }
verticalPodAutoscaler:           # the members' CPU request from their use (§2.10c)
  enabled: true
  updateMode: InPlaceOrRecreate
  controlledResources: [cpu]
  controlledValues: RequestsOnly
  minAllowed: { cpu: 100m }
  maxAllowed: { cpu: 1500m }
config: { topicAllowDeclare: true, brokerRebalanceTimeout: 300 }
pythonPackages:
  packages: [ git+ssh://...@main#egg=materializer-control-plane&subdirectory=python ]
template:
  pod: { nodeSelector, tolerations, volumes: [github-ssh, gcp-credentials] }
  pythonPackagesInitContainer: { env: [GIT_SSH_COMMAND], volumeMounts: [github-ssh] }
  kasprContainer:
    env:
      - name: MATERIALIZER_RUNTIME_MEMBER_ID
        valueFrom: { fieldRef: { fieldPath: metadata.name } }
      - name: GOOGLE_APPLICATION_CREDENTIALS
        value: /app/gcp/google_application_credentials.json
      - name: MATERIALIZER_PROTOCOL_LOGGING_ENABLED
        value: "true"
    volumeMounts: [ { name: gcp-credentials, mountPath: /app/gcp, readOnly: true } ]
```

Two important differences from the control plane itself:

1. **Runtimes get 1Gi memory and 2 CPUs at most** (the control plane gets 512Mi and one), and
   every runtime gets the same limits: a materialization's `resource_profile` is carried but not
   used. A member's main process uses a full core on a busy topic and a seal child needs one of its
   own, hence two. The CPU the members *request* is their runtime's own, though: a
   VerticalPodAutoscaler sets it from what they use (§2.10c), so a quiet materialization's members
   reserve little. The
   memory is too small for a backfill's seals. Sizing, with measurements:
   [12 §12.10](./12-operations-runbook.md), 14 §14.4.
2. **`MATERIALIZER_RUNTIME_MEMBER_ID` comes from `metadata.name`** (the pod name). This is the
   `member_id` that owns batches, appears in data file names, and names the committer.

The control plane adds `K_STREAM_WAIT_EMPTY=false`, `K_STORE_ROCKSDB_WRITE_BUFFER_SIZE=4194304` and
`K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER=2` to every runtime, merged by name with this env list
([03 §3.3.4](./03-configuration-reference.md)); a variable the list sets itself wins.

> **Kaspr versions.** The writer agent sets `noack: true`, which needs Kaspr 0.11.26 and
> kaspr-operator 0.19.1 or later. An older operator's CRD drops the field silently, and records
> would be acknowledged before they are in the table — exactly once would not hold (10 §10.7).

> **Important:** the `pythonPackages.packages` entry inside this embedded spec is **overwritten
> at render time** with an exact commit pin derived from the installed distribution. The `@main`
> / `@develop` ref here is only a fallback shape; see
> [10 — Runtime internals](./10-runtime-internals.md) § package pinning.

### Control-plane environment variables

| Variable | Base value |
|----------|------------|
| `MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE` | `kafka-sql` |
| `MATERIALIZER_CONTROL_PLANE_DEFAULT_LIST_LIMIT` | `"30"` |
| `MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI` | `""` — set per environment; empty means every create request names its own destination |
| `MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX` | `kaspr` |
| `MATERIALIZER_CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION` | `v1` |
| `GOOGLE_APPLICATION_CREDENTIALS` | `/app/gcp/google_application_credentials.json` |
| `MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC` | the embedded YAML above |
| `MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC` | `materialization-requests` |
| `MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC` | `materialization-work` |
| `MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC` | `materialization-status` |
| `MATERIALIZER_CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC` | `materialization-schema-authority-updates` (runtime control, 06 §6.7) |
| `MATERIALIZER_CONTROL_PLANE_TABLE_EVENTS_TOPIC` | `materialization-table-events` |
| `MATERIALIZER_CONTROL_PLANE_STORAGE_PREFIX` | `materializer` (dev: `develop`): the directory under the destination every materialization's tables are created in, which keeps deployments sharing a bucket apart (`ICEBERG_NAMESPACE` before 3.0; a table's Iceberg namespace is its dataset) |
| `MATERIALIZER_CONTROL_PLANE_DELETED_RETENTION_HOURS`, `…_DEFAULT_RUNTIME_STORAGE_SIZE`, `…_DEFAULT_RUNTIME_REPLICAS` | retention of deleted records; a new runtime's volume per member (`64Mi`: it holds only the state table, 10 §10.8) and default replicas. The control plane's own volume is its KasprApp's `storage` (1Gi), the maintenance app's likewise |
| `MATERIALIZER_CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC` | `materializer-settings-requests` |
| `MATERIALIZER_CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC` | `materializer-identity-requests` |
| `MATERIALIZER_AUTH_SIGNING_KEY`, `…_PREVIOUS_SIGNING_KEY`, `…_SERVICE_TOKEN`, `…_BOOTSTRAP_ADMIN_EMAIL`, `…_BOOTSTRAP_ADMIN_PASSWORD` | from the `materializer-auth` Secret |
| `MATERIALIZER_PROTOCOL_LOGGING_ENABLED` | `"true"` |

Notably **absent** from manifests (code defaults apply):
`MATERIALIZER_CONTROL_PLANE_TABLE_NAME` (default `state`),
`MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS` (default `30`), and the catalog settings
(`catalog_api_enabled`, `catalog_base_url`, 03).

See [03 — Configuration reference](./03-configuration-reference.md) for the complete list. Many of
these can also be overridden from the console's Settings page, and an override wins over the value
set here until it is reset; see [17 — Settings](./17-settings.md).

## 2.4 The global tables

`manifests/base/tables/state.yaml`

```yaml
kind: KasprTable
metadata: { name: state }
spec:
  name: state
  global: true
  keySerializer: json
  valueSerializer: json
  partitions: 3
```

`global: true` means every control-plane replica sees the full table. This is what makes
prefix scans (used by `GET /materializations` and by the sweep) work from any instance.

All control-plane records live in this one table, namespaced by key prefix. See
[05 — Data model reference](./05-data-model-reference.md) § key layout.

`manifests/base/tables/settings.yaml` declares a second table, **`settings`**, with the same shape
(global, JSON, 3 partitions). It holds the settings overrides chosen in the console and the last 100
changes of each scope. Only the `settings-intake` agent writes it. Every operation that reads
configuration binds it as `settings_table`. See [17 — Settings](./17-settings.md) §17.4.

`tables/directory.yaml` and `tables/identity-audit.yaml` declare **`directory`** and
**`identity-audit`**, again global, JSON, 3 partitions. They hold users, groups, custom roles,
datasets, their grants, the claims of table names, sessions, sign-in state and the audit log; only
the `identity-intake` agent writes them. Every WebView binds `directory` as `directory_table` to
authorize its caller. See [18 — Identity and access](./18-identity-and-access.md) §18.4.

## 2.5 Agent wiring

Each agent is a `KasprAgent` with an input topic, optional routed output topics, and inline
Python that delegates to the installed package.

### `request-intake`

```
input:  materialization-requests (declare: true)
output: materialization-work               route "work",     key value["key"]
        materializer-identity-requests     route "identity", key identity_routing_key()
tables: state → control_plane_table, settings → settings_table
entrypoint: handlers.request_intake.handle_request_event_for_output
```

The handler yields routed values, `{"route", "key", "value"}`: the work a request needs, and for a
create, an update that adds or drops tables, or a delete, the claim or release of table names it
sends to the directory (18 §18.16), the claim first.

### `workflow-apply`

```
input:  materialization-work (declare: true)
filter: step in {start_discovery, create_table, apply_runtime, delete_runtime,
                 cleanup_runtime_kafka, drop_table, purge_materialization_records}
outputs (route-discriminated):
  1. materialization-work                              when route == "work"
  2. materializer-control-plane-schedule-requests       when route == "scheduler"
  3. <dynamic via nameSelector>, declare: true, partitions: 1   when route == "topic"
init:   KafkaTopicDiscoveryService.from_environment()
        AIOKafkaAdminClient.from_environment()
        KubernetesKasprAppClient()
entrypoint: async execute_work → await handlers.workflow_apply.handle_work_event_for_routed_output_async
            (returns a generator of routed outputs)
```

The third output uses `nameSelector` to send to an arbitrary topic named in the payload: the
runtime settings delivery that brings a runtime being applied onto the current settings
(`materialization-schema-authority-updates`, 06 §6.7). Its `declare: true, partitions: 1` dates from
the per-runtime signal topics 1.0 removed.

`execute_work` is a coroutine so that no step waits on the network on the event loop that also
serves the API: discovery and the Kubernetes and Kafka admin calls are awaited on the clients' own
threads, each under a deadline above the server's own limits (90 s per Kubernetes call, 120 s per
Kafka admin call; 60 s to connect for discovery, 75 s for all of it), and table creation and drops
run on a worker thread ([07 §7.1, §7.3, §7.10](./07-control-plane-workflows.md)). The deadlines are constructor parameters with
module defaults; the init block builds the clients with the defaults.
It must hand back what the handler returns, a plain generator. `execute-work` is the second
operation in the pipeline, and there Kaspr iterates a generator, sends a list as a single value,
and raises `TypeError` on an async generator. `tests/test_agent_manifests.py` holds the map to
that shape.

### `resource-observation`

```
input:  materialization-work (declare: true)
filter: step in {observe_runtime, sweep_observations}
outputs: same 3-way route discrimination as workflow-apply, except the dynamic topic
         output has NO declare/partitions settings
init:   KubernetesKasprAppClient()
entrypoint: async execute_observation → await handlers.resource_observation.handle_observation_work_for_output_async
            (returns a generator of routed outputs)
```

`execute_observation` is a coroutine for the same reason as `execute_work`: the KasprApp read is
awaited on the Kubernetes client's thread, under its 90 s deadline, and a read that times out is
skipped for the next sweep ([07 §7.7](./07-control-plane-workflows.md)). It is the second operation
in its pipeline too, so it returns the handler's plain generator; `tests/test_agent_manifests.py`
holds it to that shape.

### `runtime-status`

```
input:  materialization-status (declare: true)
tables: state → control_plane_table
entrypoint: handlers.runtime_status.handle_runtime_status
```

No outputs. Purely a state mutator; returns the input value unchanged so the pipeline
completes.

### `table-events`

```
input:  materialization-table-events (declare: true)
tables: state → control_plane_table, settings → settings_table
entrypoint: handlers.table_events.handle_table_event
```

No outputs. Applies `table_committed`, `schema_changed`, `maintenance_completed` and `table_error`
events to the table record and the schema projection (06 §6.6), and degrades or clears the
materialization for a table error.

### `maintenance`

```
input:  materialization-work (declare: true)
filter: step in {purge_deleted_materializations, refresh_consumer_lag, record_consumer_lag,
                 schedule_table_maintenance}
tables: state → control_plane_table, settings → settings_table
output: materialization-work               when route == "work"         (keyed by the materialization)
        materializer-maintenance-requests  when route == "maintenance"  (keyed by the table;
                                           declare: true, partitions: 6)
init:   get_metadata_client()   (the process-wide Kafka metadata client, shared with the topic views)
entrypoint: async execute_maintenance → await handlers.maintenance.handle_maintenance_work_async
            (returns a generator of routed outputs)
```

The two scheduled steps arrive keyed `materialization:__system__` and only read the table. A write
goes to the shard of the event being handled (06 §6.4), so what they find for each materialization
goes back onto the work topic, keyed by that materialization. The retention sweep hands each
expired materialization a `purge_materialization_records`, which `workflow-apply` runs. The lag
refresh hands each one its measured lag as `record_consumer_lag`, which this agent saves onto the
status. Both are handled in the materialization's own shard.

`execute_maintenance` is a coroutine so that the consumer-lag refresh waits for Kafka on the
metadata client's loop — not on the event loop that serves the API. Table maintenance is not here:
`schedule_table_maintenance` only reads the table and sends each maintained table's tick, with the
runtimes' maintenance settings resolved from the `settings` table, to the maintenance app (§2.10a). The retention sweep only reads the table, a
bounded batch, and runs on the loop. Like `execute_work`, the map is the second operation of its
pipeline, so it must return the handler's generator, never yield one: Kaspr cannot iterate an async
generator there. `tests/test_agent_manifests.py` (`MaintenanceEventLoopTests`) holds it to that
shape, and `OverlayTopicWiringTests` checks that every overlay sends its output to the topic it
consumes.

### `settings-intake`

```
input:  materializer-settings-requests (declare: true)
tables: settings → settings_table
output: <dynamic via nameSelector>   when route == "topic"   (runtime-settings deliveries)
entrypoint: handlers.settings.handle_settings_request_for_output  (a generator)
```

The only writer of the `settings` table. A runtime-scope change also yields a delivery to
`materialization-schema-authority-updates` for every runtime ([17](./17-settings.md) §17.7).

### `identity-intake`

```
input:  materializer-identity-requests (declare: true)
output: materialization-work               route "work", key value["key"] (the materialization)
tables: directory → directory_table, identity-audit → audit_table, settings → settings_table,
        state → control_plane_table (read only: the claim sweep)
entrypoint: handlers.identity.handle_identity_request
```

The only writer of the directory tables. Every request it consumes is keyed `identity`, so they
apply one at a time, in order ([18](./18-identity-and-access.md) §18.5). Its one output is a claim's
answer, `tables_claimed` or `claim_rejected`, keyed by the materialization (07 §7.2a); it never
writes the `state` table.

### The `settings` table binding

Every operation whose handler reads configuration also binds the `settings` table as
`settings_table` and passes it through. Seven agents bind it: `request-intake`,
`workflow-apply`, `resource-observation`, `table-events`, `maintenance`, `settings-intake` and
`identity-intake`. So do the
WebViews in §2.8. `test_agent_manifests.SettingsWiringTests` fails when one of them calls a
configuration-reading entry point without it.

## 2.6 Scheduled tasks

The sweep tasks are `onLeader: true`, so only the elected leader emits them. The
`refresh-consumer-lag`, `purge-deleted-materializations` and `schedule-table-maintenance` tasks ask
the `maintenance` agent for their work; `identity-bootstrap` (at startup) and
`identity-maintenance` (cron `41 * * * *`), both `onLeader: true`, send a `bootstrap` or
`maintenance` request to `materializer-identity-requests` keyed `identity`
([18](./18-identity-and-access.md) §18.8). `settings-bootstrap` (at startup, `onLeader: true`)
sends a `bootstrap` request to `materializer-settings-requests` keyed `settings:runtime`
([17](./17-settings.md) §17.5).

### `sweep-observations`

```yaml
onLeader: true
schedule: { cron: "*/2 * * * *" }
pipeline: build-sweep-trigger → send-sweep-trigger (topicSend materialization-work)
```

`build_sweep_observation_trigger(requested_at=<now>)` produces:

```json
{
  "work_id": "work-sweep-sweep-observations-task-2026-01-15T10:30:00Z",
  "materialization_id": "__system__",
  "step": "sweep_observations",
  "requested_at": "2026-01-15T10:30:00Z",
  "caused_by": { "kind": "scheduler", "id": "sweep-observations-task" },
  "dedupe_key": "sweep:sweep-observations-task:2026-01-15T10:30:00Z"
}
```

The synthetic `materialization_id` is the literal string `__system__`.

### `sweep-observations-startup`

Identical, but no `schedule:` block (fires once at app start) and passes
`task_id="sweep-observations-startup-task"`.

> **Doc/manifest mismatch:** `control-plane/contracts/observation-policy.md` recommends a
> 5-minute sweep. The manifest uses 2 minutes. The manifest wins.

## 2.7 RBAC

```yaml
Role materializer-control-plane-kasprapp-manager (ns kafka-sql)
  apiGroups: [kaspr.io]
  resources: [kasprapps, kasprtables, kaspragents, kasprtasks]
  verbs:     [get, create, patch, delete]
  ---
  apiGroups: [""]
  resources: [pods]
  verbs:     [get, list]

RoleBinding materializer-control-plane-kasprapp-manager
  subjects: [ ServiceAccount materializer-control-plane-app ]
```

Observations:

- **`list` and `watch` are not granted on Kaspr resources.** The control plane reads a single named
  `KasprApp` via `get`; it never lists. This is consistent with `kubernetes/kaspr_apps.py`, which
  only calls `get_namespaced_custom_object` for `kasprapps`.
- **Pods are listed, by label.** Each observation lists the runtime's pods (`kaspr.io/app=<runtime>`)
  for their restart counts and last exits, which show members that keep restarting (08 §8.3, row
  3d). Since 4.4; without the rule the observation goes on without them, reported once.
- **`kasprwebviews` is not granted.** Runtimes never create WebViews.
- The dev overlay patches the RoleBinding subject to `develop-materializer-control-plane-app`
  because `namePrefix` does not rewrite RBAC subject names automatically.

## 2.8 Web views

Every WebView attaches to the control plane through
`metadata.labels["kaspr.io/app"] = materializer-control-plane`. Views that read control-plane
records bind the `state` table as `control_plane_table`. Views that read configuration bind the
`settings` table as `settings_table`: `materializations-create`, `materializations-update`,
`system-settings`, `topics-list`, `topics-analyze`, and the three settings views.

Common shape for mutations (`create`, `stop`, `resume`, `update`, `delete`):

```
pipeline: prepare-<action>  →  send-request-event  →  return-response
  prepare-<action>: map → calls web.materializations.<fn>, returns
                    {"response": ..., "request_event": ...}
  send-request-event: topicSend materialization-requests, passThrough: true,
                      keySelector = materialization_routing_key(request_event.materialization_id)
                      valueSelector = value["request_event"]
  return-response: map → value["response"]
response: contentType application/json, statusCodeSelector onSuccess → 202
```

Read views (`list`, `detail`) have a single `map` operation and no `statusCodeSelector` body
(the `detail` view has an empty `statusCodeSelector:` key — harmless).

`settings-update` has the same three-step shape: `prepare-settings-change` calls
`web.settings.update_settings`, `send-settings-request` sends to `materializer-settings-requests`
keyed by `settings_routing_key(scope)`, and the view answers `202`.

Every WebView binds the `directory` table as `directory_table`, and the first line of its Python is
`principal = authorize(request, directory_table, "<permission>")` — with `dataset=` for a dataset
permission on a dataset in the path. `submitted_by` and `created_by`
are the signed-in person's email (`principal.attribution`), and `submitted_by_id` and
`created_by_id` their user id; the `x-submitted-by` header is no longer read.
`test_agent_manifests.AuthorizationWiringTests` fails when a view does not authorize, names an
unknown permission, or sends an identity request with any key but `identity_routing_key()`.

Directory changes (`users-create`, `groups-update`, …) have the same three-step shape as the
materialization mutations, sending to `materializer-identity-requests` and answering `202`. The
auth views answer at once with a status their first operation chooses, and send an event only
when there is one (`topicSend` with a `predicate`).

`materializations-create` and `materializations-update` use `async def` + `await request.json()`
to read a body; the other mutations are synchronous since they take only the id from the path.

`topics-list` and `topics-analyze` ask Kafka, so they are `async def` and await the shared metadata
client (`list_topics_view`, `analyze_topics_view_async`), whose requests run on its own loop. A
view's first operation that returns a coroutine is awaited by Kaspr, and the dict it returns is the
response body. A synchronous call to Kafka there would hold every other request until Kafka
answered; `TopicsAnalyzeEventLoopTests` fails if `topics-analyze` goes back to one.

Every overlay that prefixes names must patch each mutation WebView's table and request topic.
`test_agent_manifests.OverlayTopicWiringTests` renders every overlay and fails when a WebView reads
a table the overlay does not define or sends to a topic no agent in it consumes.

The list view probes three request shapes to find `limit`:
`request.query_params` → `request.query` → `request.rel_url.query`.

Full API details: [04 — HTTP API reference](./04-http-api-reference.md).

## 2.9 The dev overlay in detail

`manifests/dev/kustomization.yaml` is the only overlay that changes behavior. It applies
`namePrefix: develop-` plus targeted JSON-6902 patches.

### What `namePrefix` does automatically

Renames `metadata.name` on every resource:
`materializer-control-plane` → `develop-materializer-control-plane`, `state` → `develop-state`,
`request-intake` → `develop-request-intake`, etc.

### What must be patched manually

`namePrefix` does **not** rewrite values inside `spec:`, so each of these is patched explicitly:

| Patch target | Change |
|--------------|--------|
| KasprApp `materializer-control-plane` | package ref `@main` → `@develop`; entire `kasprContainer.env` replaced |
| RoleBinding | subject `materializer-control-plane-app` → `develop-materializer-control-plane-app` |
| every KasprAgent / KasprWebView / KasprTask / KasprTable labelled `kaspr.io/app=materializer-control-plane` (`labelSelector`) | label `kaspr.io/app: develop-materializer-control-plane` |
| the maintenance app's agent and task (`labelSelector: kaspr.io/app=materializer-maintenance`) | label `kaspr.io/app: develop-materializer-maintenance`. Without the selectors, the blanket patch would hand them to the control plane (`test_the_maintenance_app_owns_its_agent_and_task`) |
| KasprApp `materializer-maintenance` | package ref → `@develop`; `MATERIALIZER_MAINTENANCE_APP_NAME` → `develop-materializer-maintenance` |
| `maintenance-worker` | input topic → `develop-materializer-maintenance-requests`, output topic 0 → `develop-materialization-table-events` |
| `maintenance-step` | `topicSend` → `develop-materialization-table-events` |
| `request-intake` | input topic, output topic 0 (`develop-materialization-work`), output topic 1 (`develop-materializer-identity-requests`), table names |
| `resource-observation` | input topic, output topic 0, output topic 1 (scheduler), table name |
| `maintenance` | input topic, output topic 0, output topic 1 (`develop-materializer-maintenance-requests`), table names |
| `runtime-status` | input topic, table name |
| `table-events` | input topic, table names |
| `workflow-apply` | input topic, output topic 0, output topic 1 (scheduler), table name |
| `sweep-observations`, `sweep-observations-startup`, `refresh-consumer-lag`, `purge-deleted-materializations`, `schedule-table-maintenance` | `topicSend` name → `develop-materialization-work` |
| every WebView that binds the state table | table name → `develop-state`; mutation views (`create`, `stop`, `resume`, `update`, `delete`) also `topicSend` → `develop-materialization-requests`, and `materializations-maintenance` → `develop-materializer-maintenance-requests` |
| every agent and WebView that binds the settings table | table name → `develop-settings` |
| `settings-intake` | input topic → `develop-materializer-settings-requests` |
| `settings-update`, `settings-bootstrap` | `topicSend` → `develop-materializer-settings-requests` |
| every WebView, and `identity-intake` | directory table → `develop-directory`; `identity-intake` also `identity-audit` → `develop-identity-audit`, `state` → `develop-state`, `settings` → `develop-settings`, input topic → `develop-materializer-identity-requests` and output topic 0 → `develop-materialization-work` |
| every view that sends an identity request, `identity-bootstrap`, `identity-maintenance` | `topicSend` → `develop-materializer-identity-requests` |
| `audit-list` | table → `develop-identity-audit` |
| KasprApp `materializer-control-plane` env | `MATERIALIZER_AUTH_*` read the `develop-materializer-auth` Secret |

### Dev-specific behavioral changes

| Setting | Base | Dev |
|---------|------|-----|
| Package git ref | `@main` | `@develop` |
| `MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX` | `kaspr` | `develop` |
| Table names | `state`, `settings`, `directory`, `identity-audit` | `develop-state`, `develop-settings`, `develop-directory`, `develop-identity-audit` |
| Materialization topics | `materialization-*` | `develop-materialization-*` |
| Iceberg namespace | `materializer` | `develop` |
| Settings request topic | `materializer-settings-requests` | `develop-materializer-settings-requests` |
| Identity request topic | `materializer-identity-requests` | `develop-materializer-identity-requests` |
| Auth Secret | `materializer-auth` | `develop-materializer-auth` |
| Scheduler topic in patches | `materializer-control-plane-schedule-requests` | `develop-materializer-control-plane-app-schedule-requests` |
| Runtime app spec | `resources` (0.5-1 CPU, 1Gi) and `config: {topicAllowDeclare: true, brokerRebalanceTimeout: 300}` | the same `resources`; `config` **omits** `topicAllowDeclare` |

Two sharp edges worth knowing:

1. **Scheduler topic name drift.** The base manifests target
   `materializer-control-plane-schedule-requests` (no `-app-`), while the dev patches target
   `develop-materializer-control-plane-app-schedule-requests` (with `-app-`). If delayed work
   silently disappears in one environment, check whether the topic the Kaspr scheduler actually
   consumes matches the topic the agents write to.
2. **Dev runtimes may not declare topics.** The dev `MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC`
   sets `config: { brokerRebalanceTimeout: 300 }` without `topicAllowDeclare: true`, so dev
   runtimes may not be allowed to declare their topics.

## 2.10 Runtime resources (created at runtime, not in `manifests/`)

The eight per-materialization resources are **not** in `manifests/`. They are rendered from Jinja2
templates and applied by the control plane via server-side apply (10 §10.1).

| Resource name | Kind | Bounded length |
|---------------|------|----------------|
| `<runtime-app>` | KasprApp | ≤ 59 chars |
| `<runtime-app>-state` | KasprTable | ≤ 57 chars |
| `<runtime-app>-writer` | KasprAgent | ≤ 63 chars |
| `<runtime-app>-committer` | KasprAgent | ≤ 63 chars |
| `<runtime-app>-settings` | KasprAgent | ≤ 63 chars |
| `<runtime-app>-emit-status` | KasprTask | ≤ 63 chars |
| `<runtime-app>-pipeline` | KasprTask | ≤ 63 chars |
| `<runtime-app>-commit-timer` | KasprTask | ≤ 63 chars |

Before 2.0 a runtime also had `<runtime-app>-maintenance-timer`; every apply, and the runtime's
delete, removes it if it is there.

Apply order is fixed: **KasprApp → tables → agents → tasks**. Delete order is the reverse, the
committer agent last among the agents so a final commit can land.

Names longer than the limit are truncated and suffixed with `-<sha1(name)[:8]>` — see
`runtime/naming.py`. The runtime consumer group is `runtime_component_name(app, "app")`, i.e.
`<runtime-app>-app`.

Kafka topics created for a runtime:

| Topic | Created by | Notes |
|-------|-----------|-------|
| `<runtime-app>-commits` | the committer agent (`declare: true`, `partitions: 1`, retention 1 h) | the runtime's control topic (06 §6.8) |
| `<runtime-app>-*` (the state table's changelog, Faust's own) | Kaspr | prefix-matched during delete cleanup |

On delete, `kafka/admin.py` deletes **every topic whose name starts with `<runtime-app>`** plus
the consumer group `<runtime-app>-app`, then `drop_table` deletes the table's objects. See
[07 — Control-plane workflows](./07-control-plane-workflows.md) § delete.

## 2.10a The maintenance app

`manifests/base/maintenance/`: a KasprApp of its own, `materializer-maintenance`, with one agent and
one task (10 §10.10):

| Resource | Does |
|---|---|
| KasprApp `materializer-maintenance` | 1 replica; requests 0.5 CPU / 1 GiB, limits 2 CPU / 3 GiB (a compaction child holds up to ~1.3 GiB), the CPU request then set from use between 100m and 1500m (§2.10c); the same package, Kafka user, GCP credentials and scheduling as the control plane |
| KasprAgent `maintenance-worker` | consumes `materializer-maintenance-requests` (declared, 6 partitions, keyed by table) and hands each message to the member's worker; outputs table events (route `events`) to `materialization-table-events` |
| KasprTask `maintenance-step` | every 5 s on every member (not `onLeader`): finishes jobs, starts those due, sends their table events |

| Environment variable | Default | Meaning |
|---|---|---|
| `MATERIALIZER_MAINTENANCE_APP_NAME` | `materializer-maintenance` | names the app in its commits' snapshot summaries (`materializer.runtime`) and its table events |
| `MATERIALIZER_MAINTENANCE_MAX_CONCURRENT_JOBS` | `1` | long jobs - compactions, orphan cleanups - one member runs at once, across its tables |
| `MATERIALIZER_MAINTENANCE_FORGET_AFTER_SECONDS` | `300` | a table no tick has named for this long is forgotten and its job stopped |
| `MATERIALIZER_RUNTIME_MEMBER_ID` | the pod name | names the member in its events |

What the jobs do - intervals, sizes, retention - comes with every tick from the runtime settings
(17 §17.3): no setting needs the app restarted. It holds no state of its own: no table, no volume
it needs - its persistent volume is Kaspr's. More replicas (up to the topic's 6 partitions) share the
tables between them; each table is one member's.

## 2.10b The tail app

`manifests/base/tail/` and the four `webviews/tail-*.yaml`: a KasprApp of its own,
`materializer-tail`, labelled `kaspr.io/app: materializer-tail` (19):

| Resource | Does |
|---|---|
| KasprApp `materializer-tail` | 2 replicas; requests 1 CPU / 2 GiB, limits 2 CPU / 4 GiB, the CPU request then set from use between 100m and 1500m (§2.10c); web port 6065; the same package, Kafka user, GCP credentials and scheduling as the control plane |
| KasprAgent `tail-worker` | consumes `materializer-tail-requests` (declared, **64 partitions**, keyed by table): the member assigned a table's partition owns it |
| KasprTask `tail-step` | every 5 s on every member (not `onLeader`) |
| KasprWebView `tail-rows` | `GET /tail/v1/materializations/{id}/tables/{table}`: `contentType: application/octet-stream` (Parquet) with `errorContentType: application/json` |
| KasprWebView `tail-head` | `GET …/head`: the same content types |
| KasprWebView `tail-health`, `tail-members` | `GET /tail/v1/health`, `GET /tail/v1/members` |

| Environment variable | Base value | Meaning |
|---|---|---|
| `MATERIALIZER_RUNTIME_MEMBER_ID` | the pod name | names the member in answers, health and logs |
| `MATERIALIZER_TAIL_APP_NAME` | `materializer-tail` | the app, as its consumer group and member map name it |
| `MATERIALIZER_TAIL_CONTROL_PLANE_URL` | `http://materializer-control-plane-app-api:6065` | sessions, schemas and tables |
| `MATERIALIZER_TAIL_SERVICE_TOKEN` | Secret `materializer-auth`, key `service-token` (optional) | the console's credential and the app's own; without one, every request is `403 console_only` |
| `MATERIALIZER_TAIL_TABLE_EVENTS_TOPIC` | `materialization-table-events` | commits and schema changes, read by every member |
| `MATERIALIZER_TAIL_REQUESTS_TOPIC` | `materializer-tail-requests` | the ticks' topic, as the member map reads the assignment |
| `MATERIALIZER_TAIL_MEMORY_BUDGET_BYTES` | `2684354560` | resident bytes a member's tables may hold together (19 §19.8) |

The control plane's `maintenance` agent declares and writes the requests topic (its third output,
route `tail`), so ticks can be sent before the tail app first starts. The partition count bounds the
useful members and is part of the routing: changing it moves every table. The dev overlay prefixes
everything `develop-` and patches the app's package ref, env 1–5 (name, control-plane URL, Secret,
topics), the `kaspr.io/app` labels of its agent, task and views, and the worker's input topic.

`errorContentType` needs Kaspr 0.11.30 and kaspr-operator 0.19.7 or later; with older ones the
refusals carry the same JSON body as bytes, which the console reads too.

## 2.10c Vertical pod autoscaling

Every Kaspr app of the deployment asks kaspr-operator for a VerticalPodAutoscaler
(`spec.verticalPodAutoscaler`; kaspr.dev, *Vertical Pod Autoscaling*): the control plane, the
maintenance app, the tail app and, through the runtime spec, every runtime. Each VPA,
`<app>-app-vpa`, is owned by its KasprApp and goes with it. It sets the kaspr container's **CPU
request** from what the app's pods use and resizes running pods **in place**, without a restart.
The VPA aggregates the use of all of an app's pods, so every member of an app, or of one runtime,
gets the same request:

| App | Request it starts with | CPU limit | The VPA keeps the request within |
|---|---|---|---|
| Control plane | 0.5 | 1 | 100m – 900m |
| Each runtime member | 0.5 | 2 | 100m – 1500m |
| Maintenance app | 0.5 | 2 | 100m – 1500m |
| Each tail app member | 1 | 2 | 100m – 1500m |

Why it is set up this way:

- **CPU only.** Memory stays as the manifests set it. kaspr-operator restarts the container on a
  memory resize, because Kaspr sizes its RocksDB block cache from the memory limit when it starts.
  And the apps' memory comes in bursts: a backfill's seal, a compaction child, a watched table's
  buffers. A request lowered over a quiet week would leave the next burst unreserved.
- **Requests only** (`RequestsOnly`), so the limits stay the ceilings. A pod bursts above its
  request while its node has CPU to spare, and the VPA keeps what it reserves close to what it
  uses. A quiet materialization's members reserve about 100m each rather than the spec's 0.5, and a
  busy one's up to 1.5 cores, from the same spec.
- **Each ceiling stays under its CPU limit.** The VPA would cap a request at the limit. The control
  plane's and the runtimes' memory requests equal their memory limits, so a CPU request at the CPU
  limit would make the pod Guaranteed. A resize may not change a pod's QoS class, so the pod could
  never be resized in place again. `VerticalPodAutoscalingTests` holds every overlay to this.
- **The 100m floor** keeps an idle app's event loop responsive on a busy node: its Kafka
  heartbeats, its web server. The top of the range follows what each app can use. A worker's event
  loop takes at most a core (one GIL, 12 §12.10), and a runtime's seal child or the maintenance
  app's compaction child takes a second.
- **`InPlaceOrRecreate`.** A running pod is resized in place. Only a resize that its node cannot fit,
  or that stalls, falls back to evicting the pod, so that it is created where it fits. A runtime
  member evicted that way restarts as after any eviction, from its committed offsets, with nothing
  lost (10 §10.7).

**Single-replica apps** (the control plane, the maintenance app, a one-member runtime): under the
VPA updater's default `--min-replicas=2`, the updater leaves them alone. They take the
recommendation when their pod is next created, at a deploy or a restart. An updater run with
`--in-place-skip-disruption-budget=true` (beta in VPA 1.8) resizes them in place, since a CPU-only
resize restarts nothing, and still never evicts them; the lab's runs that way. The manifests leave
`minReplicas` unset: at `1`, the updater could evict an app's only pod.

**Requirements.** kaspr-operator 0.19.5 or later, for the field and the KasprApp's scale
subresource (a VPA must target the pods' top-most owner). The Vertical Pod Autoscaler add-on. And
for in-place resizes, Kubernetes 1.33+ and VPA 1.4+. Without the add-on, each app reports
`VerticalPodAutoscalerReady=False` (`NotInstalled`) and runs with its manifest resources. An older
operator keeps the field and ignores it. The control plane reads only a runtime's `Ready` and
`Progressing` conditions (08), so a missing VPA never marks a runtime unhealthy.

**Upgrading the operator under a running VPA.** Restart the VPA's admission controller once if it
ran before the KasprApp CRD had its scale subresource, that is with kaspr-operator before 0.19.5.
VPA 1.8's admission controller looks up each pod's owners when the pod is created, and keeps every
lookup, failures included, for as long as it runs. Only the recommender refreshes those lookups.
An app whose pods it saw before the upgrade stays resolved to its StatefulSet, which no VPA targets,
so its new pods start from the manifest's request rather than the recommendation. The updater still
resizes them in place within a minute or two when they are outside the recommended range. Runtimes
created after the upgrade are new names and unaffected. The updater keeps its lookups the same
way, so restart it too if anything targeted these apps' pods before.

**Checking it:**

```bash
kubectl -n kafka-sql get vpa                       # one per app and one per runtime
kubectl -n kafka-sql describe vpa develop-materializer-control-plane-app-vpa   # its recommendation
kubectl -n kafka-sql get kasprapp develop-materializer-control-plane \
  -o jsonpath='{.status.conditions[?(@.type=="VerticalPodAutoscalerReady")]}'
kubectl -n kafka-sql get events --field-selector reason=InPlaceResizedByVPA
kubectl -n kafka-sql get pod develop-materializer-control-plane-app-0 \
  -o jsonpath='{.spec.containers[?(@.name=="kaspr")].resources}'
```

**Changing it.** Patch a KasprApp's `/spec/verticalPodAutoscaler` in the overlay, `enabled: false`
to turn it off, or the runtime spec's for every runtime. Keep `maxAllowed.cpu` under
`limits.cpu` whenever either changes. A changed runtime spec re-applies every runtime, but a change
to the VPA alone restarts none: with the CPU only, the pod template does not change. Adding
`memory` to `controlledResources` restarts each app's pods once, since kaspr-operator then gives
the container a `resizePolicy`. The Settings page shows the runtimes' value read-only, at
**Deployment › Runtimes › Vertical autoscaling** (17 §17.3).

## 2.11 Deployment checklist

```bash
# 1. Validate manifests render
kubectl kustomize manifests/dev > /dev/null && echo "dev renders OK"

# 2. Confirm prerequisite secrets exist (names only — never print contents)
kubectl -n kafka-sql get secret github-deploy-key kafka-materializer-gcp-credentials \
        materializer-control-plane-user develop-materializer-auth -o name

# 3. Apply
kubectl apply -k manifests/dev

# 4. Watch the app come up
kubectl -n kafka-sql get kasprapp develop-materializer-control-plane -w

# 5. Verify all child resources are linked
kubectl -n kafka-sql get kasprapp develop-materializer-control-plane \
        -o jsonpath='{.status.linkedResources}' | python -m json.tool

# 6. Port-forward and smoke test
kubectl -n kafka-sql port-forward svc/develop-materializer-control-plane-app-api 6065:6065 &
curl -s localhost:6065/system/version | python -m json.tool          # public
curl -s localhost:6065/auth/session | python -m json.tool            # who am I: a guest here
curl -s localhost:6065/materializations | python -m json.tool        # guests may list by default
```

Anything more needs a session; see [18](./18-identity-and-access.md) §18.10 for signing in with
`curl`.

**Upgrading to 1.0** is a fresh start: the 0.x runtimes, tables and materialization-plane topics
are removed and the control plane comes up from zero members, keeping the identity and settings
tables — the procedure is 12 §12.13.

**Upgrading to 4.1** adds the tail app and its topic. The control plane only sends to
`materializer-tail-requests`, so it needs no forced rebalance; the tail app starts with its own
group. Point the console at it with `CONSOLE_TAIL_URL` (web 03).

**Upgrading to 4.2** gives every app a VerticalPodAutoscaler and raises the runtimes' CPU limit to
two cores (§2.10c). Use kaspr-operator 0.19.5 or later to get the VPAs: an older one ignores the
field, and the apps run as before. The deploy restarts the control plane, maintenance and tail apps
once, for the new package, but not for their VPAs. The runtimes roll once when the control plane
re-applies them with the new limit. No topics change.

**Upgrading to a release that adds topics** (0.5.0 the settings topics, 0.6.0 the identity topics,
1.0 `materialization-table-events`): the control plane uses static group membership, so a rolling
restart does not assign the new topics. Force one rebalance per environment after the deploy, as
[17 — Settings](./17-settings.md) §17.12 describes. Until then the new agent consumes nothing.

## 2.12 CI

`Jenkinsfile` at the repository root is a four-line shared-library call:

```groovy
buildDeployManifests {
    namespace="kafka-sql"
    destination="crs"
    branchDeploy=false
}
```

All build/deploy logic lives in the `buildDeployManifests` shared library, not in this
repository. This repository has no linter or formatter; the meaningful local gates are:

```bash
python -m unittest discover -s python/tests -p 'test_*.py' -v
python -m compileall python/src
kubectl kustomize manifests/base
kubectl kustomize manifests/dev
```
