# 02 — Deployment and Manifests

Everything in this system is deployed as Kubernetes custom resources managed by the Kaspr
operator (`kaspr.io/v1alpha1`), two RBAC objects and the console. The Kaspr operator derives each
app's StatefulSet, Services and ServiceAccount from its `KasprApp`; besides those custom resources and
the RBAC, the chart authors only the console's Deployment, Service, ServiceAccount, ConfigMap and
optional volume claim, and optional HTTPRoutes or Ingresses (§2.1).

## 2.1 The chart

Everything is deployed by one Helm chart, `charts/midstream`: three `KasprApp`s with their agents,
tasks, tables and views, the console's Deployment, Service and ServiceAccount, the control plane's
Role and RoleBinding, and optional HTTPRoutes or Ingresses. A release is one **domain** (§2.12, 20): its
control plane and apps, with the console or without it — or the shared console alone, serving any
number of domains. Every resource, topic and table is named after
the release (`fullnameOverride` replaces the release name), so one namespace can hold several
installations and nothing in the chart carries a fixed name.

```
charts/midstream/
  Chart.yaml                    version = appVersion = the platform version
  values.yaml                   every value, documented in place
  README.md                     requirements and the values table
  ci/                           values files the tests render: test (a domain on GCS with the console serving another),
                                minimal, domain-only (no console), console-only (no domain), s3 (a domain on an
                                S3-compatible server), azure (a domain on Azure with workload identity)
  templates/
    _helpers.tpl                derived names, labels, the package reference, the Kaspr volumes, the lake's storage
                                (Secret references, the pods' identity), the destination and console profile checks,
                                the runtime spec
    control-plane/app.yaml      the control plane; control-plane/rbac.yaml its Role and RoleBinding
    maintenance/                app.yaml, maintenance-worker.yaml (agent), maintenance-step.yaml (task)
    tail/                       app.yaml, tail-worker.yaml (agent), tail-step.yaml (task); off with tail.enabled=false
    agents/   tasks/   tables/  the control plane's 8 agents, 8 tasks and 4 tables
    webviews/                   74 views: the HTTP API, the REST catalog, the tail app's four
    console/deployment.yaml     the console: Deployment, Service, ServiceAccount, the ConfigMap of its domains and storage
                                profiles, optional PersistentVolumeClaim; off with console.enabled=false
    route.yaml                  HTTPRoutes (route.enabled) or Ingresses (ingress.enabled): the domain's API, the console,
                                and redirects from old hostnames (§2.12)
    NOTES.txt                   what helm prints after an install
```

Derived names, with a release called `midstream`:

| Kind | Names |
|------|-------|
| Kaspr apps | `midstream-control-plane`, `midstream-maintenance`, `midstream-tail`; their Services `<app>-app-api`, consumer groups `<app>-app` |
| Agents, tasks, views | `midstream-request-intake`, `midstream-sweep-observations`, `midstream-materializations-create`, … |
| Tables | `midstream-state`, `midstream-settings`, `midstream-directory`, `midstream-identity-audit`; `spec.name` stays bare |
| Topics | `midstream-materialization-requests`, `-work`, `-status`, `-table-events`, `-schema-authority-updates`; `midstream-settings-requests`, `-identity-requests`, `-maintenance-requests`, `-tail-requests`; the scheduler's `midstream-control-plane-app-schedule-requests` |
| Runtimes | `midstream-mat-<id>` (`controlPlane.settings.runtimeNamePrefix`), their tables under `<destination>/midstream/` (`storagePrefix`) |
| Secrets | `midstream-auth` unless `auth.existingSecret`; the Kafka Secret, the lake's (`storage.*.credentialsSecret`) and the console profiles' (`console.storage[].credentialsSecret`) are named in values |
| ServiceAccounts | `midstream-console`, the chart's; kaspr-operator gives each app and each runtime its own, `<app>-app`: `midstream-control-plane-app`, `midstream-maintenance-app`, `midstream-tail-app`, `midstream-mat-<id>-app` (§2.9a) |

Lint, render and apply:

```bash
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
helm template midstream charts/midstream -n midstream -f my-values.yaml | less
helm upgrade --install midstream charts/midstream -n midstream --create-namespace -f my-values.yaml
```

`docs/install.md` walks through a first installation; `python/tests/test_agent_manifests.py` renders
the chart with every file under `ci/` and checks the wiring (13 §13.3).

## 2.2 Resource inventory (base)

| Kind | Name | Purpose |
|------|------|---------|
| KasprApp | `<release>-control-plane` | The control-plane application |
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
| KasprAgent | `settings-intake` | `<release>-settings-requests` → apply to the `settings` table; delivers runtime settings |
| KasprAgent | `identity-intake` | `<release>-identity-requests` → apply to the `directory` and `identity-audit` tables; answer claims of table names on `materialization-work` |
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
| KasprWebView | `materializations-protection` | `GET /materializations/{materialization_id}/protection` (the protection review, 22 §22.11) |
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
| KasprApp | `<release>-maintenance` | **The maintenance app** (`maintenance/app.yaml`): compaction, expiry, orphan cleanup and reprotect of every materialization's tables (10 §10.10) |
| KasprAgent | `maintenance-worker` | the maintenance app's: `<release>-maintenance-requests` → its worker; table events → `materialization-table-events` |
| KasprTask | `maintenance-step` | the maintenance app's: every 5 s on every member, finishes jobs and starts those due |
| KasprApp | `<release>-tail` | **The tail app** (`tail/app.yaml`): the newest records of the tables people watch, served to the console before they are committed (19) |
| KasprAgent | `tail-worker` | the tail app's: `<release>-tail-requests` (64 partitions) → the ticks and cancels of the tables the member owns |
| KasprTask | `tail-step` | the tail app's: every 5 s on every member — its loops, seeding, parking, trims and bounds |
| KasprWebView | `tail-rows`, `tail-head`, `tail-health`, `tail-members` | the tail app's `/tail/v1/…` (19 §19.9), for the console alone |
| Role | `<release>-control-plane-kasprapp-manager` | get/create/patch/delete on Kaspr CRDs; get/list on pods |
| RoleBinding | `<release>-control-plane-kasprapp-manager` | Binds Role to SA `<release>-control-plane-app` |

## 2.3 The control-plane KasprApp

`charts/midstream/templates/control-plane/app.yaml`

### Core spec

```yaml
spec:
  replicas: 1
  bootstrapServers: <kaspr.bootstrapServers>
  authentication: <kaspr.authentication>            # scram-sha-512, scram-sha-256 or plain
  tls: <kaspr.tls>                                  # when set; {} turns TLS to the brokers on
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
- `kaspr.config` adds an installation's shared Kaspr settings to every app and runtime,
  `topicReplicationFactor` on a small cluster for one; the keys above stay the chart's.
- `kaspr.tls`, when set, is the `tls` of every app and, through the runtime spec, every runtime,
  as the CRD spells it. `{}` turns TLS to the brokers on: kaspr-operator, which reads no field of
  it, has the app connect with `SASL_SSL`, or with `SSL` when `kaspr.authentication` is
  `{type: plain}` alone.

### Package installation

```yaml
pythonPackages:
  packages:
    - midstream-platform==<appVersion>
  installPolicy: { retries: 3, timeout: 600, onFailure: block }
```

Every app installs the package when its pod starts, from `package.source`: `index`, the default,
installs `midstream-platform==<package.index.version>`, the chart's appVersion unless set, from PyPI
or `package.index.url`, with `package.index.credentialsSecret` for a private index; `git` installs
this repository at `package.git.ref` over https (no deploy key: the repository is public), for
development. `onFailure: block` means a failed install
prevents the app from starting rather than running a stale version. The runtimes get the same block
through the runtime spec, with the control plane pinning the exact revision (10 §10.1).

### Secrets consumed

| Value | Default | Key(s) | Used for |
|-------|---------|--------|----------|
| `kaspr.authentication.passwordSecret` | none, required | `password` | the Kafka user's password, for every app and runtime |
| `storage.gcs.credentialsSecret` | none: application default credentials | `service-account.json`, mounted at `/etc/midstream/gcs` and named by `GOOGLE_APPLICATION_CREDENTIALS` | the lake on GCS, for the control plane, the maintenance and tail apps and every runtime (§2.9a) |
| `storage.s3.credentialsSecret` | none: the pods' own identity | `access-key-id`, `secret-access-key`; `session-token`, optional | the lake on S3 or an S3-compatible server, likewise, as `AWS_*` |
| `storage.azure.credentialsSecret` | none: the pods' own identity | `connection-string`, `account-key`, `client-id`, `tenant-id`, `client-secret`, each optional | the lake on Azure Blob Storage, likewise, as `AZURE_*` |
| `auth.existingSecret` | `<release>-auth` | `signing-key`, `previous-signing-key`, `service-token`, `bootstrap-admin-email`, `bootstrap-admin-password`, `federation-key`, `previous-federation-key`, `protection-key`, each `optional: true` | session signing, the console's and the tail app's service token, the first administrator ([18](./18-identity-and-access.md) §18.11); access tokens every domain honours ([20](./20-domains-and-the-shared-console.md)); the key data protection hashes with, given to the control plane, the tail and maintenance apps and every runtime ([22](./22-data-protection.md) §22.8) |
| `console.storage[].credentialsSecret` | none: the console's own identity | by the profile's kind, each optional (§2.9a) | the console's reads of one destination prefix, as `CONSOLE_STORAGE_<NAME>_*` and, for `gcs`, a mounted key file |
| `package.index.credentialsSecret` | none | `username`, `password` | a private package index |

**Never** commit, echo, or reproduce the contents of these secrets. The chart references them by
name only; `docs/install.md` creates them.

Every `auth` key is optional so that a deployment without the Secret still starts. Without
`signing-key`, though, nobody can sign in and every request is a guest's; create the Secret before
the first install ([18](./18-identity-and-access.md) §18.12). Without `protection-key` nothing can be
hashed: a create or an edit whose policy hashes is refused, and the shipped default hashes — create
it with the Secret (`docs/install.md` §2); added later, it reaches the apps when they restart
([22](./22-data-protection.md) §22.9).

### Scheduling constraints

`kaspr.nodeSelector` and `kaspr.tolerations` are rendered into the control plane, the maintenance
and tail apps and, through the runtime spec, every runtime; the console has `console.nodeSelector`
and `console.tolerations` of its own. Both are empty by default.

### Cloud identity

`kaspr.podLabels` label the control plane's pods, the maintenance and tail apps' and, through the
runtime spec, every runtime's; `kaspr.serviceAccountAnnotations` annotate their ServiceAccounts. They
carry the cloud identity a storage backend without a Secret is reached with — IRSA's role, AKS
workload identity's client id — and the console has `console.podLabels` and
`console.serviceAccountAnnotations` of its own (§2.9a). Both are empty by default.

### The embedded runtime app spec

`CONTROL_PLANE_RUNTIME_APP_SPEC` is a **YAML document embedded in an environment
variable**. It is parsed by `config._load_runtime_app_spec()` and used as the base `spec:` for
every generated runtime `KasprApp`. Template rendering merges/overrides on top of it.

Base value (abridged; `midstream.runtimeSpec` in `templates/_helpers.tpl`):

```yaml
# no replicas: the control plane sets each materialization's own
bootstrapServers: <kaspr.bootstrapServers>
authentication: <kaspr.authentication>
tls: <kaspr.tls>                 # when set
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
config: { valueSerializer: json, topicAllowDeclare: true, brokerRebalanceTimeout: 300 }   # plus kaspr.config
pythonPackages:                  # the same block as the apps'; the control plane pins the revision
  packages: [ midstream-platform==<appVersion> ]
template:
  serviceAccount: { metadata: { annotations } }   # kaspr.serviceAccountAnnotations, when set (§2.9a)
  pod: { metadata: { labels }, nodeSelector, tolerations, affinity, volumes: [gcs-credentials] }
                                 # labels: kaspr.podLabels; the volume: storage.gcs.credentialsSecret
  kasprContainer:
    env:
      - name: RUNTIME_MEMBER_ID
        valueFrom: { fieldRef: { fieldPath: metadata.name } }
      - <the lake's storage variables>   # from storage.*: Secret references and STORAGE_* (§2.9a)
      - name: PROTECTION_KEY       # the auth Secret's protection-key, optional (22 §22.8)
        valueFrom: { secretKeyRef: { name: <release>-auth, key: protection-key, optional: true } }
      - name: PROTOCOL_LOGGING_ENABLED
        value: "true"
    volumeMounts: [ { name: gcs-credentials, mountPath: /etc/midstream/gcs, readOnly: true } ]
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
2. **`RUNTIME_MEMBER_ID` comes from `metadata.name`** (the pod name). This is the
   `member_id` that owns batches, appears in data file names, and names the committer.

The control plane adds `K_STREAM_WAIT_EMPTY=false`, `K_STORE_ROCKSDB_WRITE_BUFFER_SIZE=4194304` and
`K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER=2` to every runtime, merged by name with this env list
([03 §3.3.3](./03-configuration-reference.md)); a variable the list sets itself wins. When it holds
a protection key it also adds `PROTECTION_KEY_ID`, the key's id, so a member can tell that the key it
loaded is the deployment's, and adding or changing the key changes every runtime's spec: the sweep
re-applies each runtime once (22 §22.8).

> **Kaspr versions.** The writer agent sets `noack: true`, which needs Kaspr 0.11.26 and
> kaspr-operator 0.19.1 or later. An older operator's CRD drops the field silently, and records
> would be acknowledged before they are in the table — exactly once would not hold (10 §10.7).

> **Important:** the `pythonPackages.packages` entry inside this embedded spec is **overwritten
> at render time** with the exact source the control plane itself was installed from: the git
> commit at the same repository, or `midstream-platform==<version>` from the same index. The ref here is
> only a fallback shape; see [10 — Runtime internals](./10-runtime-internals.md) §10.13.

### Control-plane environment variables

| Variable | Base value |
|----------|------------|
| `CONTROL_PLANE_DEFAULT_NAMESPACE` | the release's namespace, from the pod's `metadata.namespace` |
| `CONTROL_PLANE_DEFAULT_LIST_LIMIT` | `"30"` |
| `CONTROL_PLANE_DEFAULT_DESTINATION_URI` | `controlPlane.settings.defaultDestinationUri`, checked by the chart (§2.9a); empty, no materialization can be created until the Settings page sets one |
| `CONTROL_PLANE_RUNTIME_NAME_PREFIX` | the release name, unless `controlPlane.settings.runtimeNamePrefix` names another |
| `CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION` | `v1` |
| The lake's storage variables | from `storage.*` (§2.9a): each backend's credentials as Secret references, and the `STORAGE_*` settings; the same as every runtime's |
| `CONTROL_PLANE_RUNTIME_APP_SPEC` | the embedded YAML above |
| `CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC` | `<release>-materialization-requests` |
| `CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC` | `<release>-materialization-work` |
| `CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC` | `<release>-materialization-status` |
| `CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC` | `<release>-materialization-schema-authority-updates` (runtime control, 06 §6.7) |
| `CONTROL_PLANE_TABLE_EVENTS_TOPIC` | `<release>-materialization-table-events` |
| `CONTROL_PLANE_STORAGE_PREFIX` | the release name, unless `controlPlane.settings.storagePrefix` names another, trimmed of whitespace and slashes as the control plane trims it (`materializer` when the variable is unset): the directory under the destination every materialization's tables are created in, which keeps deployments sharing a bucket apart (not a namespace: a table's Iceberg namespace is its dataset) |
| `CONTROL_PLANE_DELETED_RETENTION_HOURS`, `…_DEFAULT_RUNTIME_STORAGE_SIZE`, `…_DEFAULT_RUNTIME_REPLICAS` | retention of deleted records; a new runtime's volume per member (`64Mi`: it holds only the state table, 10 §10.8) and default replicas. The control plane's own volume is its KasprApp's `storage` (1Gi), the maintenance app's likewise |
| `CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC` | `<release>-settings-requests` |
| `CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC` | `<release>-identity-requests` |
| `AUTH_SIGNING_KEY`, `…_PREVIOUS_SIGNING_KEY`, `…_SERVICE_TOKEN`, `…_BOOTSTRAP_ADMIN_EMAIL`, `…_BOOTSTRAP_ADMIN_PASSWORD`, `…_FEDERATION_KEY`, `…_PREVIOUS_FEDERATION_KEY` | from the `<release>-auth` Secret |
| `PROTECTION_KEY` | the `<release>-auth` Secret's `protection-key`, optional; also on the tail and maintenance apps (22 §22.8) |
| `PROTOCOL_LOGGING_ENABLED` | `"true"` |

Notably **absent** from the chart (code defaults apply):
`CONTROL_PLANE_TABLE_NAME` (default `state`),
`PROTOCOL_LOG_SLOW_WAIT_SECONDS` (default `30`), the catalog settings
(`catalog_api_enabled`, `catalog_base_url`, 03), and `CONTROL_PLANE_ALLOW_FILE_DESTINATIONS`
(off: a `file://` destination serves tests only, and the chart refuses one).

See [03 — Configuration reference](./03-configuration-reference.md) for the complete list. Many of
these can also be overridden from the console's Settings page, and an override wins over the value
set here until it is reset; see [17 — Settings](./17-settings.md).

## 2.4 The global tables

`charts/midstream/templates/tables/state.yaml`

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

`charts/midstream/templates/tables/settings.yaml` declares a second table, **`settings`**, with the same shape
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
        <release>-identity-requests     route "identity", key identity_routing_key()
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
  2. <release>-control-plane-app-schedule-requests       when route == "scheduler"
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
        <release>-maintenance-requests  when route == "maintenance"  (keyed by the table;
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
shape, and `ValuesWiringTests` checks that, with every values file, it sends its output to the topic it
consumes.

### `settings-intake`

```
input:  <release>-settings-requests (declare: true)
tables: settings → settings_table
output: <dynamic via nameSelector>   when route == "topic"   (runtime-settings deliveries)
entrypoint: handlers.settings.handle_settings_request_for_output  (a generator)
```

The only writer of the `settings` table. A runtime-scope change also yields a delivery to
`materialization-schema-authority-updates` for every runtime ([17](./17-settings.md) §17.7).

### `identity-intake`

```
input:  <release>-identity-requests (declare: true)
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
`maintenance` request to `<release>-identity-requests` keyed `identity`
([18](./18-identity-and-access.md) §18.8). `settings-bootstrap` (at startup, `onLeader: true`)
sends a `bootstrap` request to `<release>-settings-requests` keyed `settings:runtime`
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

## 2.7 RBAC

```yaml
Role <release>-control-plane-kasprapp-manager (ns midstream)
  apiGroups: [kaspr.io]
  resources: [kasprapps, kasprtables, kaspragents, kasprtasks]
  verbs:     [get, create, patch, delete]
  ---
  apiGroups: [""]
  resources: [pods]
  verbs:     [get, list]

RoleBinding <release>-control-plane-kasprapp-manager
  subjects: [ ServiceAccount <release>-control-plane-app ]
```

Observations:

- **`list` and `watch` are not granted on Kaspr resources.** The control plane reads a single named
  `KasprApp` via `get`; it never lists. This is consistent with `kubernetes/kaspr_apps.py`, which
  only calls `get_namespaced_custom_object` for `kasprapps`.
- **Pods are listed, by label.** Each observation lists the runtime's pods (`kaspr.io/app=<runtime>`)
  for their restart counts and last exits, which show members that keep restarting (08 §8.3, row
  3d). Since 4.4; without the rule the observation goes on without them, reported once.
- **`kasprwebviews` is not granted.** Runtimes never create WebViews.
- The RoleBinding's subject is `<release>-control-plane-app`, the service account kaspr-operator gives the control plane's pods
  because `namePrefix` does not rewrite RBAC subject names automatically.

**Kafka.** The Role is the deployment's only Kubernetes permission; in Kafka it has one user,
every app's and runtime's `kaspr.authentication`. On a cluster that enforces ACLs, that user must
create, read and write the release's topics, read its consumer groups, write its transactional ids
(every app runs exactly once) and read the source topics; to delete materializations, it must also
delete their runtimes' topics and groups. It needs nothing on the cluster resource. The grants,
the names they cover and what breaks without each are in [21](./21-kafka-access.md).

## 2.8 Web views

Every WebView attaches to the control plane through
`metadata.labels["kaspr.io/app"] = <release>-control-plane`. Views that read control-plane
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
`web.settings.update_settings`, `send-settings-request` sends to `<release>-settings-requests`
keyed by `settings_routing_key(scope)`, and the view answers `202`.

Every WebView binds the `directory` table as `directory_table`, and the first line of its Python is
`principal = authorize(request, directory_table, "<permission>")` — with `dataset=` for a dataset
permission on a dataset in the path. `submitted_by` and `created_by`
are the signed-in person's email (`principal.attribution`), and `submitted_by_id` and
`created_by_id` their user id; the `x-submitted-by` header is no longer read.
`test_agent_manifests.AuthorizationWiringTests` fails when a view does not authorize, names an
unknown permission, or sends an identity request with any key but `identity_routing_key()`.

Directory changes (`users-create`, `groups-update`, …) have the same three-step shape as the
materialization mutations, sending to `<release>-identity-requests` and answering `202`. The
auth views answer at once with a status their first operation chooses, and send an event only
when there is one (`topicSend` with a `predicate`).

`materializations-create` and `materializations-update` use `async def` + `await request.json()`
to read a body; the other mutations are synchronous since they take only the id from the path.

`topics-list` and `topics-analyze` ask Kafka, so they are `async def` and await the shared metadata
client (`list_topics_view`, `analyze_topics_view_async`), whose requests run on its own loop. A
view's first operation that returns a coroutine is awaited by Kaspr, and the dict it returns is the
response body. A synchronous call to Kafka there would hold every other request until Kafka
answered; `TopicsAnalyzeEventLoopTests` fails if `topics-analyze` goes back to one.

Every table and request topic a WebView names derives from the release.
`test_agent_manifests.ValuesWiringTests` renders the chart with every values file and fails when a
WebView reads a table the chart does not define or sends to a topic no agent in it consumes.

The list view probes three request shapes to find `limit`:
`request.query_params` → `request.query` → `request.rel_url.query`.

Full API details: [04 — HTTP API reference](./04-http-api-reference.md).

## 2.9 Values and derived names

One `values.yaml` holds every default, and an installation changes what it needs in a values file of
its own (`docs/install.md` §3). Every name comes from one derived name:
`include "midstream.fullname"`, the release name unless `fullnameOverride` says otherwise, prefixes
every resource, topic and table, and every binding, `topicSend` and label is written in terms of it.
A template cannot name a topic the agents do not consume unless it spells the suffix wrong, which
`ValuesWiringTests` catches with each values file under `ci/`.

| Values | What they decide |
|--------|------------------|
| `package.*` | where the Python package comes from: this repository at a ref, or a release from an index |
| `kaspr.*` | the Kafka address and authentication, the Kaspr version, storage class, scheduling and cloud identity (`podLabels`, `serviceAccountAnnotations`) of every Kaspr pod, runtimes included |
| `storage.*` | the lake's object storage: each backend's Secret and settings (§2.9a) |
| `auth.*` | the auth Secret, and the sign-in the domain accepts (`sso`, `passwordSignIn`) |
| `controlPlane.settings.*` | the deployment settings of 03 §3.2 (the destination, which the chart checks, §2.9a; prefixes; limits); `controlPlane.runtime.*` the runtime spec of §2.3 |
| `maintenance.*`, `tail.*` | the two apps' replicas, resources, autoscaling and their request topics' partitions; `tail.enabled` |
| `console.*` | the console's image, variables, cache volume, placement, storage profiles and cloud identity (§2.9a); `console.enabled` |
| `route.*`, `ingress.*`, `rbac.create`, `tables.partitions` | how it is reached, the Role, the global tables' partitions |

The CPU of every runtime member, the memory of the seals and the compaction child, and the live
settings are not values: the first two are the runtime spec's `resources` (§2.3, 12 §12.10), the
last the Settings page's (17).

Two things follow from it: the scheduler topic is derived from the app id
(14 §14.2), and every installation's runtimes may declare their topics.

## 2.9a Object storage

A table's location names its object storage for good: `gs://` Google Cloud Storage, `s3://` Amazon S3
or an S3-compatible server that honours conditional writes, `abfss://` Azure Blob Storage (11 §11.1).
The control plane, the maintenance and tail apps and every runtime reach it through the object store
layer, one store per scheme with the process's one identity (11 §11.1a, 03 §3.3a); the console reads
it through storage profiles of its own. The chart gives both Secret references, never values: no
rendered manifest, runtime spec or ConfigMap holds a credential, and a credential typed into the
values goes nowhere (`python/tests/test_chart_storage.py`). `docs/install.md` §2 creates each
backend's bucket, identities and Secrets.

### The apps and the runtimes: `storage.*`

Fill in the block of every backend this domain's destinations use, and leave the others empty. A
materialization keeps the destination it was created with, so a domain whose destination moves to
another backend keeps its older tables where they are, and needs both blocks.

| Value | Secret keys | Renders, in the control plane, the maintenance and tail apps and the runtime spec |
|---|---|---|
| `storage.gcs.credentialsSecret` | `service-account.json`, a Google service-account key | the volume `gcs-credentials`, mounted read-only at `/etc/midstream/gcs`, and `GOOGLE_APPLICATION_CREDENTIALS=/etc/midstream/gcs/service-account.json` |
| `storage.s3.credentialsSecret` | `access-key-id` and `secret-access-key`, required: a pod does not start without them; `session-token` for temporary keys, optional | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, each from its key |
| `storage.s3.region` | — | `STORAGE_S3_REGION`; empty renders nothing, and the stores sign for `us-east-1` |
| `storage.s3.endpoint` | — | `STORAGE_S3_ENDPOINT`, an S3-compatible server; empty: AWS. One endpoint serves every `s3://` destination of the domain |
| `storage.s3.pathStyle` | — | `STORAGE_S3_PATH_STYLE: "true"` when `true`: path-style addressing, which most S3-compatible servers need |
| `storage.azure.credentialsSecret` | `connection-string`, or `account-key`, or `client-id`, `tenant-id` and `client-secret` (a service principal) | `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_SECRET`, each from its key with `optional: true`, so the apps use what the Secret holds |
| `storage.azure.endpoint` | — | `STORAGE_AZURE_ENDPOINT`, an emulator's Blob endpoint to which the account is appended: `http://azurite:10000` reaches the account at `http://azurite:10000/<account>`. Empty: `https://<account>.blob.core.windows.net` |

The credential variables are each SDK's own, the names workload identities inject. A backend without
a Secret renders none, and its SDK's default chain finds the pods' own identity (below). An endpoint
is an `http://` or `https://` URL naming the server alone, `http(s)://host[:port]`, trailing slashes
dropped: a bare `host:port`, a path or a user fails the install, naming the value. What each store
does with these variables is 03 §3.3a and 11 §11.1a. The console takes none of them: it reads through
its profiles alone.

The variables are part of the runtime spec, so a change of `storage.*` changes every runtime's spec,
and the sweep re-applies, and restarts, each one (07 §7.7). A Secret's new content reaches a pod when
it next starts.

### The pods' own identity: `kaspr.podLabels`, `kaspr.serviceAccountAnnotations`

A backend without a Secret is reached with the pods' cloud identity. `kaspr.podLabels` label every
Kaspr pod (each KasprApp's `template.pod.metadata.labels`, and the runtime spec's), and
`kaspr.serviceAccountAnnotations` annotate the ServiceAccount kaspr-operator creates for each app and
each runtime (`template.serviceAccount.metadata.annotations`), named `<app>-app`:
`<release>-control-plane-app`, `<release>-maintenance-app`, `<release>-tail-app`, and
`<runtimeNamePrefix>-<materialization id>-app`, a new one with every materialization. The identity
must trust them all:

| Identity | Values | Trust |
|---|---|---|
| IRSA | `serviceAccountAnnotations: {eks.amazonaws.com/role-arn: <the role's ARN>}` | the role trusts `system:serviceaccount:<namespace>:*-app`, a `StringLike` condition that leaves the console's `<release>-console` out |
| EKS Pod Identity | none | an association for each ServiceAccount, every new runtime's included |
| AKS workload identity | `podLabels: {azure.workload.identity/use: "true"}`, `serviceAccountAnnotations: {azure.workload.identity/client-id: <the identity's client id>}` | a federated credential for each ServiceAccount, every new runtime's included; at most 20 per identity |
| GKE workload identity | none | the bucket's role granted to the namespace, the console's pods included unless the console runs in a namespace of its own |

Both values must be maps, and every value renders as a string. A label the chart sets itself —
`app.kubernetes.io/name`, `…/instance`, `…/version`, `…/managed-by`, `…/component`, `helm.sh/chart` —
is refused. `ci/azure-values.yaml` renders AKS workload identity.

### The destination

`controlPlane.settings.defaultDestinationUri` decides where new materializations' tables go
(`CONTROL_PLANE_DEFAULT_DESTINATION_URI`). The chart checks it by the control plane's own rules
(03 §3.2.1, 17 §17.3), trimmed of surrounding whitespace and with trailing slashes dropped:
`gs://bucket[/prefix]`, `s3://bucket[/prefix]` or `abfss://container@account.dfs.core.windows.net[/prefix]`,
each bucket or container named as its backend allows, at most 1,024 characters. Anything else fails
the install, naming the rule, and so does `file://`, which serves tests and a laptop only. Empty, no
materialization can be created until the Settings page sets one. `controlPlane.settings.storagePrefix`
is trimmed as the control plane trims it, of whitespace and then of slashes; what is left, or else
the release name, is `CONTROL_PLANE_STORAGE_PREFIX`, and a destination's tables live under
`<destination>/<storage prefix>/`.

### The console's storage profiles: `console.storage[]`

The console reads every domain's tables itself, through **storage profiles**: one per destination
prefix, each with credentials of its own. An object is read with the profile whose `scope` is the
longest prefix of its URI, as DuckDB picks among scoped secrets, so one console reads domains on
different backends, in one statement too (console 03 §3.6a, console 07 §07.10). A location no profile
covers is never read: its table answers "no storage is configured for <location>". With no profile,
the console lists every domain's materializations and reads none of their tables.

| Field | Takes |
|---|---|
| `name` | a lower-case letter, then up to 39 lower-case letters, digits or hyphens; unique. The profile's credential variables are `CONSOLE_STORAGE_<NAME>_*`, the name in upper case with `-` as `_` |
| `scope` | the destination prefix it serves, ending with `/` and naming a bucket, or a container and its account — `gs://example-lake/`, `s3://example-lake/prod/`, `abfss://lake@exampleaccount.dfs.core.windows.net/`; unique |
| `kind` | `gcs`, `s3` or `azure`, matching the scope's scheme |
| `endpoint` | `s3`: an S3-compatible server; `azure`: an emulator's Blob endpoint, to which the account is appended; `gcs`: an emulator, read without credentials; `http(s)://host[:port]`, as above. Empty: the cloud's. The `s3` profiles of one bucket name one endpoint, compared as the console compares them: an `s3://` path names none, so neither DuckDB nor the console could tell their objects apart |
| `region` | `s3`: the bucket's region; empty: `us-east-1` |
| `pathStyle` | `s3`: `true` or `false`, path-style addressing |
| `access` | `direct`, the default: DuckDB scans the objects in place; `cached`: the console downloads each file first. A `gcs` profile scans in place only with an HMAC key, and an `azure` profile with an `endpoint` only with a connection string or the account key |
| `credentialsSecret` | a Secret in the release's namespace, every key optional |

The chart refuses any other field — a credential typed into a profile among them — and any profile
that breaks a rule, naming the profile and the rule. It renders the profiles, without credentials, as
`storage.json` in the ConfigMap `<release>-console-domains`, mounted at `/etc/midstream` beside
`domains.json` and named by `CONSOLE_STORAGE_FILE`; a change of profiles restarts the console
(`checksum/storage`). Each profile's Secret reaches the console by kind, every key `optional: true`:

| `kind` | Secret keys | Reach the console as |
|---|---|---|
| `gcs` | `hmac-key-id`, `hmac-secret` | `CONSOLE_STORAGE_<NAME>_GCS_HMAC_KEY_ID`, `…_GCS_HMAC_SECRET`, for DuckDB's scans |
| `gcs` | `service-account.json` | a file mounted at `/etc/midstream-storage/<name>`, named by the profile's `keyFile`, for metadata reads |
| `s3` | `access-key-id`, `secret-access-key`, `session-token` | `…_ACCESS_KEY_ID`, `…_SECRET_ACCESS_KEY`, `…_SESSION_TOKEN` |
| `azure` | `connection-string`, `account-key`, `client-id`, `tenant-id`, `client-secret` | `…_AZURE_CONNECTION_STRING`, `…_AZURE_ACCOUNT_KEY`, `…_AZURE_CLIENT_ID`, `…_AZURE_TENANT_ID`, `…_AZURE_CLIENT_SECRET` |

A profile without a `credentialsSecret` reads as the console's own identity: application default
credentials, the SDK's credential chain, workload identity. The console runs as its own
ServiceAccount, `<release>-console`, which the chart creates; `console.serviceAccountAnnotations`
annotate it and `console.podLabels` label the console's pods, by the same rules as `kaspr.*` — IRSA's
`eks.amazonaws.com/role-arn` of a read-only role, or AKS workload identity's label and client id. The
console never writes a table, so give every profile read-only credentials: a statement's sandbox does
not stop a write to a path it may read, only the SQL guard does (console 12 §12.4).

### What helm prints

`NOTES.txt` warns when `console.storage` names no profile, since the console could then read no
table, and, in a release with a domain whose console serves it, when no profile's scope is a prefix
of `<destination>/<storage prefix>/`, where the domain's tables live. Without a destination it says
that none is configured yet.

## 2.10 Runtime resources (created at runtime, not in the chart)

The eight per-materialization resources are **not** in the chart. They are rendered from Jinja2
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

`charts/midstream/templates/maintenance/`: a KasprApp of its own, `<release>-maintenance`, with one agent and
one task (10 §10.10):

| Resource | Does |
|---|---|
| KasprApp `<release>-maintenance` | 1 replica; requests 0.5 CPU / 1 GiB, limits 2 CPU / 3 GiB (a compaction child holds up to ~1.3 GiB), the CPU request then set from use between 100m and 1500m (§2.10c); the same package, Kafka user, storage credentials, cloud identity and scheduling as the control plane |
| KasprAgent `maintenance-worker` | consumes `<release>-maintenance-requests` (declared, 6 partitions, keyed by table) and hands each message to the member's worker; outputs table events (route `events`) to `materialization-table-events` |
| KasprTask `maintenance-step` | every 5 s on every member (not `onLeader`): finishes jobs, starts those due, sends their table events |

| Environment variable | Default | Meaning |
|---|---|---|
| `MAINTENANCE_APP_NAME` | `<release>-maintenance` | names the app in its commits' snapshot summaries (`materializer.runtime`) and its table events |
| `MAINTENANCE_MAX_CONCURRENT_JOBS` | `1` | long jobs - compactions, reprotects, orphan cleanups - one member runs at once, across its tables |
| `MAINTENANCE_FORGET_AFTER_SECONDS` | `300` | a table no tick has named for this long is forgotten and its job stopped |
| `RUNTIME_MEMBER_ID` | the pod name | names the member in its events |

What the jobs do - intervals, sizes, retention - comes with every tick from the runtime settings
(17 §17.3): no setting needs the app restarted. It holds no state of its own: no table, no volume
it needs - its persistent volume is Kaspr's. More replicas (up to the topic's 6 partitions) share the
tables between them; each table is one member's.

## 2.10b The tail app

`charts/midstream/templates/tail/` and the four `webviews/tail-*.yaml`: a KasprApp of its own,
`<release>-tail`, labelled `kaspr.io/app: materializer-tail` (19):

| Resource | Does |
|---|---|
| KasprApp `<release>-tail` | 2 replicas; requests 1 CPU / 2 GiB, limits 2 CPU / 4 GiB, the CPU request then set from use between 100m and 1500m (§2.10c); web port 6065; the same package, Kafka user, storage credentials, cloud identity and scheduling as the control plane |
| KasprAgent `tail-worker` | consumes `<release>-tail-requests` (declared, **64 partitions**, keyed by table): the member assigned a table's partition owns it |
| KasprTask `tail-step` | every 5 s on every member (not `onLeader`) |
| KasprWebView `tail-rows` | `GET /tail/v1/materializations/{id}/tables/{table}`: `contentType: application/octet-stream` (Parquet) with `errorContentType: application/json` |
| KasprWebView `tail-head` | `GET …/head`: the same content types |
| KasprWebView `tail-health`, `tail-members` | `GET /tail/v1/health`, `GET /tail/v1/members` |

| Environment variable | Base value | Meaning |
|---|---|---|
| `RUNTIME_MEMBER_ID` | the pod name | names the member in answers, health and logs |
| `TAIL_APP_NAME` | `<release>-tail` | the app, as its consumer group and member map name it |
| `TAIL_CONTROL_PLANE_URL` | `http://<release>-control-plane-app-api:6065` | sessions, schemas and tables |
| `TAIL_SERVICE_TOKEN` | Secret `<release>-auth`, key `service-token` (optional) | the console's credential and the app's own; without one, every request is `403 console_only` |
| `TAIL_TABLE_EVENTS_TOPIC` | `materialization-table-events` | commits and schema changes, read by every member |
| `TAIL_REQUESTS_TOPIC` | `<release>-tail-requests` | the ticks' topic, as the member map reads the assignment |
| `TAIL_MEMORY_BUDGET_BYTES` | `2684354560` | resident bytes a member's tables may hold together (19 §19.8) |

The control plane's `maintenance` agent declares and writes the requests topic (its third output,
route `tail`), so ticks can be sent before the tail app first starts. The partition count bounds the
useful members and is part of the routing: changing it moves every table. Its name, topics,
control-plane URL and Secret derive from the release; `tail.enabled: false` leaves the app, its
agent, task and views out, and the console shows committed rows only.

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

- **CPU only.** Memory stays as the values set it. kaspr-operator restarts the container on a
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
  never be resized in place again. `VerticalPodAutoscalingTests` holds every values file to this.
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
resize restarts nothing, and still never evicts them; the test cluster's updater runs that way. The chart leaves
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
kubectl -n midstream get vpa                       # one per app and one per runtime
kubectl -n midstream describe vpa midstream-control-plane-app-vpa   # its recommendation
kubectl -n midstream get kasprapp midstream-control-plane \
  -o jsonpath='{.status.conditions[?(@.type=="VerticalPodAutoscalerReady")]}'
kubectl -n midstream get events --field-selector reason=InPlaceResizedByVPA
kubectl -n midstream get pod midstream-control-plane-app-0 \
  -o jsonpath='{.spec.containers[?(@.name=="kaspr")].resources}'
```

**Changing it.** Set the app's `verticalPodAutoscaler` value, `enabled: false`
to turn it off, or the runtime spec's for every runtime. Keep `maxAllowed.cpu` under
`limits.cpu` whenever either changes. A changed runtime spec re-applies every runtime, but a change
to the VPA alone restarts none: with the CPU only, the pod template does not change. Adding
`memory` to `controlledResources` restarts each app's pods once, since kaspr-operator then gives
the container a `resizePolicy`. The Settings page shows the runtimes' value read-only, at
**Deployment › Runtimes › Vertical autoscaling** (17 §17.3).

## 2.11 Deployment checklist

```bash
# 0. Before the first install on a store: check that it keeps the conditional writes the pointer
#    relies on, with the storage variables and credentials the pods will have (03 §3.3a)
python -m midstream.lakehouse.store.check s3://example-lake/_check

# 1. Lint and render
helm lint charts/midstream -f my-values.yaml
helm template midstream charts/midstream -n midstream -f my-values.yaml > /tmp/midstream.yaml

# 2. Confirm the Secrets the values name exist (names only — never print contents): the Kafka and
#    auth Secrets, storage.*.credentialsSecret and console.storage[].credentialsSecret (here the
#    names ci/s3-values.yaml gives them)
kubectl -n midstream get secret midstream-kafka midstream-auth midstream-s3 midstream-s3-reader -o name

# 3. Install or upgrade
helm upgrade --install midstream charts/midstream -n midstream --create-namespace -f my-values.yaml

# 4. Watch the apps come up
kubectl -n midstream get kasprapp -w

# 5. Verify all child resources are linked
kubectl -n midstream get kasprapp midstream-control-plane \
        -o jsonpath='{.status.linkedResources}' | python -m json.tool

# 6. Port-forward and smoke test
kubectl -n midstream port-forward svc/midstream-control-plane-app-api 6065:6065 &
curl -s localhost:6065/system/version | python -m json.tool          # public
curl -s localhost:6065/auth/session | python -m json.tool            # who am I: a guest here
curl -s localhost:6065/materializations | python -m json.tool        # guests may list by default
```

Anything more needs a session; see [18](./18-identity-and-access.md) §18.10 for signing in with
`curl`.

**There is no upgrade from an earlier release.** Every installation of this release is new, with
`storage.*` and `console.storage` written for it, and nothing converts the values, tables or records
an earlier installation left (14 §14.1, `docs/install.md` §8). `helm upgrade` applies
changed values to an installation of this release.

**A later release that adds topics**, applied with `helm upgrade`: the control plane uses static
group membership, so a rolling restart does not assign the new topics. Force one rebalance per
environment after the deploy, as [17 — Settings](./17-settings.md) §17.12 describes. Until then the
new agent consumes nothing.

## 2.12 Routes, domains and the shared console

A release is a **domain** — `domain.id` and `domain.name` — and the switches choose what it deploys
(handbook 20 for the whole design):

| Values | Deploys |
|---|---|
| `controlPlane.enabled: true` (default), `console.enabled: true` (default) | The domain and the console; `console.domains` empty serves this domain alone |
| `console.enabled: false` | The domain alone: its control plane, maintenance and tail apps |
| `controlPlane.enabled: false` | The console alone (`maintenance` and `tail` follow the control plane), serving `console.domains` |

`console.domains` is the console's registry (20 §20.2): each domain's id and name; `local: true` for
this release's own domain, reached by its Services; for another, `controlPlaneUrl`, `tailUrl` and
`serviceTokenSecret: {name, key}`; and exactly one `settingsHome`. The chart renders it as
`domains.json` in the ConfigMap `<release>-console-domains`, beside the storage profiles'
`storage.json` (§2.9a), and each remote domain's token as `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`, and
refuses a registry that breaks a rule. A domain's entry names no storage: the console reads every
domain's tables through the profile their locations match, whatever the backend. `console.url` is the
URL people use; `console.sso.*` the console's OAuth clients and `auth.sso.*` what the domain accepts
(18 §18.17); every domain's auth Secret holds the same `federation-key` (18 §18.11).

**Routes.** With `route.enabled` the chart renders Gateway API HTTPRoutes:

| HTTPRoute | Hostnames | Rules |
|---|---|---|
| `<release>-api` | `route.hostnames`: the domain's API | `/tail/` to the tail app's Service; everything else to the control plane's, `/catalog` included |
| `<release>-console` | `route.consoleHostnames` | the console's Service: its pages, `/api`, `/catalog` (the federated catalog) and `/d/<domain>/catalog` |
| `<release>-redirect` | `route.redirectFrom`: old person-facing hostnames | `RequestRedirect`, `ReplacePrefixMatch` `/` → `/d/<domain id>/`, to `console.url`'s host, 301 |

`ingress.host`, `ingress.consoleHost` and `ingress.redirectFrom` render the same as Ingresses, the
redirect as the nginx `permanent-redirect` annotation; any other controller's redirect is applied by
hand (20 §20.13). `python/tests/test_chart_domains.py` renders each shape.

## 2.13 CI

`.github/workflows/ci.yml` runs on every pull request and every push to `main`. This repository has
no linter or formatter; the jobs are the gates:

| Job | Runs |
|---|---|
| One version everywhere | `python tools/version.py check` |
| Python 3.12, 3.13 | the unit suite, `python -m compileall -q python/src`, the sdist and the wheel |
| Console | `npm run typecheck`, `npm test`, `npm run build`, and the console's image, not pushed |
| Helm chart | `helm lint` and `helm template` with every values file under `ci/` |
| Object stores | the conformance suite and each store's own cases (`python/tests/test_store_live.py`) against SeaweedFS 4.51 and moto 5.2.5 for `s3://`, Azurite 3.37.0 for `abfss://` and fake-gcs-server 1.56.1 for `gs://`, each started in the job. A check a store cannot run is skipped and named: conditional deletes on S3, whose store offers none; on fake-gcs-server the conditional deletes it ignores and the batch requests it cannot answer |

Locally:

```bash
python -m unittest discover -s python/tests -p 'test_*.py' -v
python -m compileall python/src
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
helm template midstream charts/midstream -n midstream -f charts/midstream/ci/test-values.yaml
```

The live suite runs against any store by hand, with `STORAGE_LIVE_TESTS` naming base URIs and the
store's own variables set (the module's docstring has examples).
