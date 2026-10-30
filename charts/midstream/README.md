# midstream

The Helm chart for Midstream: one **domain** — the control plane, the maintenance app and the tail
app, as Kaspr applications — and the shared console that serves every domain, as a Deployment, in one
namespace. A release holds a domain, the console, or both (`controlPlane.enabled`, `console.enabled`);
any number of domains, each its own release, share one console
([handbook 20](../../docs/handbook/20-domains-and-the-shared-console.md)). Every resource, topic and
table is named after the release, so one namespace can hold several installations.

```bash
helm install midstream charts/midstream -n midstream --create-namespace -f my-values.yaml
```

[`docs/install.md`](../../docs/install.md) walks through a first installation, Secrets included.

## Requirements

- Kubernetes 1.29 or later; 1.33 with the Vertical Pod Autoscaler add-on for in-place CPU resizing.
- kaspr-operator 0.19.7 or later with its CRDs, and a Kaspr image 0.11.30 or later.
- A Kafka cluster with a SCRAM or plain user that may create topics.
- Object storage for the lake, and an identity that may read, write, delete and list objects in it:
  a Google Cloud Storage bucket, an Amazon S3 bucket (or a bucket on an S3-compatible server that
  honours conditional writes, such as SeaweedFS), or a container in an Azure storage account. The
  console needs only read and list.

## Values

| Value | Default | Meaning |
|---|---|---|
| `fullnameOverride` | the release name | the prefix of every derived name |
| `domain.id`, `domain.name` | the release name with `-` as `_`; the id | which domain this release is: its URL segment `/d/<id>`, its catalog in SQL, its level in the federated catalog; the name people read |
| `controlPlane.enabled` | `true` | off: the release deploys the shared console alone |
| `package.source` | `index` | `index` installs `midstream-platform==<package.index.version>`, the chart's appVersion unless set, from PyPI or `package.index.url`; `git` installs `package.git.repository` at `package.git.ref`, for development |
| `kaspr.bootstrapServers` | required | the Kafka bootstrap address every app and runtime uses |
| `kaspr.authentication` | required | Kaspr's Kafka client authentication: `type` (`scram-sha-512`, `scram-sha-256`, `plain`), `username`, `passwordSecret`; a listener without authentication takes `{type: plain}` alone |
| `kaspr.tls` | unset | TLS to the brokers, passed to every app and runtime as the KasprApp's `tls`: `{}` turns it on (SASL_SSL with authentication, SSL with `{type: plain}` alone) |
| `kaspr.config` | `{}` | Kaspr settings every app and runtime share, as the KasprApp CRD spells them (`topicReplicationFactor: 1` on a cluster with fewer than three brokers); the chart's own keys cannot be overridden |
| `kaspr.version` | the operator's default | the Kaspr image version |
| `kaspr.storageClass` | the cluster's default | the class of every persistent claim |
| `kaspr.nodeSelector`, `kaspr.tolerations`, `kaspr.affinity` | none | where every Kaspr pod, runtimes included, is placed; `controlPlane`, `maintenance`, `tail` and `controlPlane.runtime` take the same three keys to place their own pods instead |
| `kaspr.podLabels`, `kaspr.serviceAccountAnnotations` | none | the cloud identity of every Kaspr pod, runtimes included, for a storage backend without a Secret: labels for each pod, annotations for the ServiceAccount kaspr-operator gives each app and each runtime (`<app>-app`). IRSA takes `eks.amazonaws.com/role-arn`; AKS workload identity the label `azure.workload.identity/use: "true"` and the annotation `azure.workload.identity/client-id`. Every new runtime brings a ServiceAccount of its own, so the identity must trust them all: IRSA through a `StringLike` condition on `system:serviceaccount:<namespace>:*`; AKS workload identity and EKS Pod Identity take a federated credential or an association for each ServiceAccount |
| `console.nodeSelector`, `console.tolerations`, `console.affinity` | none | where the console runs; it does not follow `kaspr.*` |
| `console.podLabels`, `console.serviceAccountAnnotations` | none | the console's cloud identity, for a storage profile without a Secret: labels for its pods, annotations for its own ServiceAccount, `<release>-console` |
| `storage.gcs.credentialsSecret` | none | a Secret with `service-account.json`, a Google service-account key, mounted at `/etc/midstream/gcs` for the apps and the runtimes; empty: application default credentials (GKE workload identity) |
| `storage.s3.credentialsSecret`, `region`, `endpoint`, `pathStyle` | none, `us-east-1`, AWS, `false` | a Secret with `access-key-id` and `secret-access-key` (and `session-token` for temporary keys), read as `AWS_*`; empty: the pods' own identity, IRSA (`kaspr.serviceAccountAnnotations`), EKS Pod Identity or an instance role. An S3-compatible server's endpoint, `http(s)://host[:port]`, and path-style addressing, which most of them need |
| `storage.azure.credentialsSecret`, `endpoint` | none, Azure | a Secret with `connection-string`, or `account-key`, or `client-id`, `tenant-id` and `client-secret` (a service principal), each optional; empty: AKS workload identity (`kaspr.podLabels`, `kaspr.serviceAccountAnnotations`) or a managed identity. An emulator's blob endpoint, `http(s)://host[:port]`, to which the account is appended |
| `auth.existingSecret` | `<release>-auth` | the Secret with `signing-key`, `service-token`, `federation-key` (the same in every domain), `protection-key` (what data protection hashes with; the same in every domain whose masked columns should join), `bootstrap-admin-email`, `bootstrap-admin-password` |
| `auth.sso.*`, `auth.passwordSignIn` | off, `true` | the sign-in providers this domain takes — Entra ID, GitHub, Google, OpenID Connect — the same clients the console is registered with; its first-sign-in policy (`open` or `invited`), allowed email domains, and whether a password works here |
| `controlPlane.settings.defaultDestinationUri` | none | where new materializations' tables go — `gs://bucket[/prefix]`, `s3://bucket[/prefix]` or `abfss://container@account.dfs.core.windows.net[/prefix]` — with `storage.*` set for its backend; required to create one, here or on the Settings page. The chart refuses any other form, `file://` included |
| `controlPlane.settings.storagePrefix`, `runtimeNamePrefix` | the release name | the directory under the destination; the runtimes' name prefix |
| `controlPlane.runtime.*` | see values.yaml | the resources, autoscaling and Kaspr config of every runtime |
| `maintenance.replicas`, `maintenance.requestPartitions` | `1`, `6` | the maintenance app's members and the partitions that share the tables between them |
| `tail.enabled`, `tail.replicas`, `tail.requestPartitions` | `true`, `2`, `64` | the live tail; changing the partitions moves every table |
| `console.enabled`, `console.image.*`, `console.env`, `console.persistence` | `true`, `ghcr.io/vmirz/midstream-console`, none, off | the console, its `CONSOLE_*` variables and its Parquet cache volume |
| `console.url`, `console.domains[]` | the first console hostname; this release's domain alone | the console's public URL, and every domain it serves: `id`, `name`, `local: true` for this release's own, else `controlPlaneUrl`, `tailUrl` and `serviceTokenSecret`; exactly one `settingsHome` |
| `console.storage[]` | none: the console reads no table | where the console reads tables from, one profile per destination prefix, the longest matching scope winning: `name` (lower case letters, digits, hyphens; unique), `scope` (the prefix, ending with `/`), `kind` (`gcs`, `s3` or `azure`, matching the scope's scheme), `endpoint`, `region`, `pathStyle`, `access` (`direct` or `cached`) and `credentialsSecret`, a Secret whose keys, each optional, reach the console as `CONSOLE_STORAGE_<NAME>_*` variables: `hmac-key-id` and `hmac-secret` for gcs, with `service-account.json` mounted at `/etc/midstream-storage/<name>`; `access-key-id`, `secret-access-key` and `session-token` for s3; `connection-string`, `account-key`, `client-id`, `tenant-id` and `client-secret` for azure. Give it a read-only identity |
| `console.sso.*` | off | the OAuth clients the console signs people in with; the redirect URI is `<console.url>/api/auth/sso/<provider>/callback` |
| `route.*`, `ingress.*` | off | Gateway API HTTPRoutes or Ingresses: `hostnames` for this domain's API (its views, `/catalog`, `/tail/`), `consoleHostnames` for the shared console, `redirectFrom` for old hostnames sent on to `/d/<domain>/` |
| `rbac.create` | `true` | the Role that lets the control plane create runtimes |

Every app's `resources`, `verticalPodAutoscaler`, `storage.size`, `protocolLogging` and `extraEnv`
are values too; `values.yaml` documents them in place.

## Checks

The Python test suite renders the chart with each values file under `ci/` — one per installation
shape: every feature on GCS, a domain alone, a console alone, the defaults, a domain on S3 and one
on Azure — and checks the wiring between agents, tasks, tables and views, the authorization of every
view, and the autoscaling bounds (`python/tests/test_agent_manifests.py`); the domains — the
registry, a console alone, the routes and redirects, the sign-in clients, every rule the chart
refuses (`python/tests/test_chart_domains.py`); and the storage — each backend's credentials in
every app and runtime, the console's profiles, and the destinations and profiles the chart refuses
(`python/tests/test_chart_storage.py`). `helm lint charts/midstream -f charts/midstream/ci/test-values.yaml`
lints it.
