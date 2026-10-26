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
- A Kafka cluster with a SCRAM or plain user that may create topics; a GCS bucket and a service
  account that may read, write and delete objects in it.

## Values

| Value | Default | Meaning |
|---|---|---|
| `fullnameOverride` | the release name | the prefix of every derived name |
| `domain.id`, `domain.name` | the release name with `-` as `_`; the id | which domain this release is: its URL segment `/d/<id>`, its catalog in SQL, its level in the federated catalog; the name people read |
| `controlPlane.enabled` | `true` | off: the release deploys the shared console alone |
| `package.source` | `index` | `index` installs `midstream-platform==<package.index.version>`, the chart's appVersion unless set, from PyPI or `package.index.url`; `git` installs `package.git.repository` at `package.git.ref`, for development |
| `kaspr.bootstrapServers` | required | the Kafka bootstrap address every app and runtime uses |
| `kaspr.authentication` | required | Kaspr's Kafka client authentication: `type` (`scram-sha-512`, `scram-sha-256`, `plain`), `username`, `passwordSecret`; a listener without authentication takes `{type: plain}` alone |
| `kaspr.config` | `{}` | Kaspr settings every app and runtime share, as the KasprApp CRD spells them (`topicReplicationFactor: 1` on a cluster with fewer than three brokers); the chart's own keys cannot be overridden |
| `kaspr.version` | the operator's default | the Kaspr image version |
| `kaspr.storageClass` | the cluster's default | the class of every persistent claim |
| `kaspr.nodeSelector`, `kaspr.tolerations`, `kaspr.affinity` | none | where every Kaspr pod, runtimes included, is placed; `controlPlane`, `maintenance`, `tail` and `controlPlane.runtime` take the same three keys to place their own pods instead |
| `console.nodeSelector`, `console.tolerations`, `console.affinity` | none | where the console runs; it does not follow `kaspr.*` |
| `gcs.credentialsSecret` | none | a Secret with a service-account key under `gcs.credentialsKey`; empty uses application default credentials |
| `auth.existingSecret` | `<release>-auth` | the Secret with `signing-key`, `service-token`, `federation-key` (the same in every domain), `bootstrap-admin-email`, `bootstrap-admin-password` |
| `auth.sso.*`, `auth.passwordSignIn` | off, `true` | the sign-in providers this domain takes — Entra ID, GitHub, Google, OpenID Connect — the same clients the console is registered with; its first-sign-in policy (`open` or `invited`), allowed email domains, and whether a password works here |
| `controlPlane.settings.defaultDestinationUri` | none | the `gs://` prefix every materialization's tables go under; required to create one |
| `controlPlane.settings.storagePrefix`, `runtimeNamePrefix` | the release name | the directory under the destination; the runtimes' name prefix |
| `controlPlane.runtime.*` | see values.yaml | the resources, autoscaling and Kaspr config of every runtime |
| `maintenance.replicas`, `maintenance.requestPartitions` | `1`, `6` | the maintenance app's members and the partitions that share the tables between them |
| `tail.enabled`, `tail.replicas`, `tail.requestPartitions` | `true`, `2`, `64` | the live tail; changing the partitions moves every table |
| `console.enabled`, `console.image.*`, `console.env`, `console.persistence` | `true`, `ghcr.io/vmirz/midstream-console`, none, off | the console, its `CONSOLE_*` variables and its Parquet cache volume |
| `console.url`, `console.domains[]` | the first console hostname; this release's domain alone | the console's public URL, and every domain it serves: `id`, `name`, `local: true` for this release's own, else `controlPlaneUrl`, `tailUrl` and `serviceTokenSecret`; exactly one `settingsHome` |
| `console.sso.*` | off | the OAuth clients the console signs people in with; the redirect URI is `<console.url>/api/auth/sso/<provider>/callback` |
| `route.*`, `ingress.*` | off | Gateway API HTTPRoutes or Ingresses: `hostnames` for this domain's API (its views, `/catalog`, `/tail/`), `consoleHostnames` for the shared console, `redirectFrom` for old hostnames sent on to `/d/<domain>/` |
| `rbac.create` | `true` | the Role that lets the control plane create runtimes |

Every app's `resources`, `verticalPodAutoscaler`, `storage.size`, `protocolLogging` and `extraEnv`
are values too; `values.yaml` documents them in place.

## Checks

The Python test suite renders the chart with each values file under `ci/` and checks the wiring
between agents, tasks, tables and views, the authorization of every view, and the autoscaling bounds
(`python/tests/test_agent_manifests.py`), and the domains — the registry, a console alone, the
routes and redirects, the sign-in clients, every rule the chart refuses
(`python/tests/test_chart_domains.py`). `helm lint charts/midstream -f charts/midstream/ci/test-values.yaml`
lints it.
