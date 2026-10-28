# Installing Midstream

This walks through a first installation: the prerequisites, the Secrets, the chart, the first
sign-in, the first dataset and materialization, more domains and sign-in through a provider, and how
to upgrade and remove it. Every command is meant to be pasted as it stands; change the names in the
first block to yours.

One installation of the chart is a **domain**: a control plane with its maintenance and tail apps, its
own accounts, datasets and tables. The console serves every domain from one URL; the first install
below is one domain with the console beside it, and §7 adds more (handbook 20).

```bash
# The names this guide uses. Keep them, or change them once here.
export NS=midstream                # the namespace Midstream and its runtimes live in
export RELEASE=midstream           # the Helm release: every resource, topic and table is named after it
export KAFKA=kafka-bootstrap.kafka.svc.cluster.local:9092   # the Kafka bootstrap address, reachable in-cluster
export KAFKA_USER=midstream        # a SCRAM-SHA-512 user on that cluster
export BUCKET=gs://my-lake         # the GCS bucket the tables are written to
export ADMIN_EMAIL=you@example.com # the first administrator
export DOMAIN=domain_a             # this domain's id: its URL segment /d/<id> and its catalog in SQL
```

## 1. What you need first

| Requirement | Why | Check |
|---|---|---|
| Kubernetes 1.29 or later; 1.33 with the Vertical Pod Autoscaler add-on to resize pods in place | everything runs as Kaspr apps; the CPU requests follow use | `kubectl version` |
| [kaspr-operator](https://kaspr.dev) 0.19.7 or later, with its CRDs | Midstream is Kaspr applications: `KasprApp`, `KasprAgent`, `KasprTask`, `KasprTable`, `KasprWebView` | `kubectl get crd kasprapps.kaspr.io` |
| A Kafka cluster reachable from the namespace, with a SCRAM-SHA-512, SCRAM-SHA-256 or plain user. If the cluster enforces ACLs, the user needs the grants of handbook 21 §21.3: create, read and write the topics named `$RELEASE-…`, read the groups and write the transactional ids named so, read every topic you will materialize, and, to delete materializations, delete the topics and groups named `$RELEASE-mat-…` | the control plane, the apps and every runtime declare their own topics at every start and write them in transactions | `kubectl -n kafka get kafka` for Strimzi, or your cluster's tooling |
| A Google Cloud Storage bucket and a service account that may read, write and delete objects in it | the Iceberg tables, their metadata and the catalog pointers live there | `gcloud storage ls $BUCKET` |
| `helm` 3.12 or later and `kubectl` on your machine | | `helm version` |

The console reads the tables with the same service account; engines that attach to the Iceberg REST
catalog bring their own GCS access.

## 2. Create the namespace and the Secrets

```bash
kubectl create namespace "$NS"

# Kafka: the password of the SCRAM user.
read -rs -p "Kafka password for $KAFKA_USER: " KAFKA_PASSWORD; echo
kubectl -n "$NS" create secret generic "$RELEASE-kafka" --from-literal=password="$KAFKA_PASSWORD"
unset KAFKA_PASSWORD

# GCS: the service account's key file.
kubectl -n "$NS" create secret generic "$RELEASE-gcs" \
  --from-file=google_application_credentials.json=/path/to/service-account.json

# Sign-in: a signing key for sessions, the service token the console and the tail app use, the
# federation key access tokens are signed with - the same in every domain, so keep it for §7 - and
# the first administrator. The password is shown once; the first sign-in replaces it.
FEDERATION_KEY="$(openssl rand -base64 48)"
BOOTSTRAP_PASSWORD="$(openssl rand -base64 18)"
kubectl -n "$NS" create secret generic "$RELEASE-auth" \
  --from-literal=signing-key="$(openssl rand -base64 48)" \
  --from-literal=service-token="$(openssl rand -base64 48)" \
  --from-literal=federation-key="$FEDERATION_KEY" \
  --from-literal=bootstrap-admin-email="$ADMIN_EMAIL" \
  --from-literal=bootstrap-admin-password="$BOOTSTRAP_PASSWORD"
echo "bootstrap password for $ADMIN_EMAIL: $BOOTSTRAP_PASSWORD"
unset BOOTSTRAP_PASSWORD
```

The auth Secret's keys are read by name (`signing-key`, `service-token`, `federation-key`,
`bootstrap-admin-email`, `bootstrap-admin-password`, and `previous-signing-key` /
`previous-federation-key` when a key is rotated); the chart only needs its name. Without
`signing-key` nobody can sign in and every request is a guest's; without `federation-key` nobody can
make an access token for an engine.

A Kafka listener without authentication needs no Secret: skip the `$RELEASE-kafka` command above and
set `kaspr.authentication` to `{type: plain}` alone in §3.

On GKE with workload identity, skip the GCS Secret and leave `gcs.credentialsSecret` empty: the pods
use application default credentials.

## 3. Write the values

```bash
cat > midstream-values.yaml <<VALUES
domain:
  id: $DOMAIN
  name: Domain A
kaspr:
  bootstrapServers: $KAFKA
  authentication:
    type: scram-sha-512
    username: $KAFKA_USER
    passwordSecret:
      secretName: $RELEASE-kafka
      passwordKey: password
gcs:
  credentialsSecret: $RELEASE-gcs
controlPlane:
  settings:
    defaultDestinationUri: $BUCKET
VALUES
```

Everything else has a default; `charts/midstream/values.yaml` documents each value. The ones most
often changed: `kaspr.storageClass` when the cluster has no default storage class,
`kaspr.config.topicReplicationFactor` on a cluster with fewer than three brokers (Kaspr's default is 3), `kaspr.nodeSelector`
and `kaspr.tolerations` to place every pod, `tail.enabled: false` to run without the live tail,
`console.enabled: false` to run without the console, and `route` or `ingress` to expose the console
and the catalog.

`package.source` is `index` by default: every app installs the release the chart belongs to,
`midstream-platform==<appVersion>`, from PyPI, and the control plane pins its runtimes to the same
version. `package.source: git` installs this repository at `package.git.ref` instead, for development.
The console image for a release is `ghcr.io/vmirz/midstream-console:<version>`, the chart's default.

## 4. Install

```bash
# The published chart, at the version of the release you are installing (github.com/vmirz/midstream/releases) …
helm install "$RELEASE" oci://ghcr.io/vmirz/charts/midstream --version <version> -n "$NS" -f midstream-values.yaml
# … or the chart in a checkout of this repository
helm install "$RELEASE" charts/midstream -n "$NS" -f midstream-values.yaml

kubectl -n "$NS" get kasprapp -w          # three apps: control-plane, maintenance, tail
```

The first start of each app installs the Python package, which takes a minute or two; then the
operator reports every app `Ready`. If an app stays in its install step, look at its init
container: `kubectl -n "$NS" logs <pod> -c install-packages`.

## 5. Check it

```bash
kubectl -n "$NS" port-forward "svc/$RELEASE-control-plane-app-api" 6065:6065 &
curl -s localhost:6065/system/version                          # the control plane's version, no sign-in needed

curl -s -X POST localhost:6065/auth/login -H 'content-type: application/json' \
  -d "{\"email\":\"$ADMIN_EMAIL\",\"password\":\"<the bootstrap password>\"}"
```

The sign-in answers with `"password_change_required": true`: the administrator chooses a password in
the console. Open the console and sign in there:

```bash
kubectl -n "$NS" port-forward "svc/$RELEASE-console" 8787:8787 &
open http://localhost:8787
```

With `route.enabled` or `ingress.enabled` the console is reachable at its host instead
(`route.consoleHostnames`), and engines reach the Iceberg REST catalog at `https://<console host>/catalog`
— every domain's, its tables `lake.<domain>.<dataset>.<table>` — or one domain's at
`https://<console host>/d/<domain>/catalog`.

## 6. The first dataset and materialization

In the console: **Datasets › New dataset** (`sales`, say), then **Materializations › New**, pick a
topic, put its table in the dataset, and create. Or with the API, signed in:

```bash
TOKEN=$(curl -s -X POST localhost:6065/auth/login -H 'content-type: application/json' \
  -d "{\"email\":\"$ADMIN_EMAIL\",\"password\":\"<your password>\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
AUTH="authorization: Bearer $TOKEN"

curl -s -X POST localhost:6065/datasets -H 'content-type: application/json' -H "$AUTH" \
  -d '{"name":"sales","description":"Orders"}'
curl -s -X POST localhost:6065/materializations -H 'content-type: application/json' -H "$AUTH" \
  -d '{"name":"Shop","tables":[{"source_topic":"orders","dataset":"sales"}]}'
curl -s localhost:6065/materializations -H "$AUTH"
```

The materialization goes through `requested`, `discovering`, `deploying` and `running` within a few
minutes (handbook 12 §12.2); its runtime appears as a KasprApp named `$RELEASE-mat-…` in the same
namespace, and its table at `$BUCKET/$RELEASE/<materialization id>/orders`. The console's Data tab
shows the rows as they commit, and new records within seconds through the live tail.

## 7. More domains, and signing in with a provider

Each further domain is a release of its own, usually in its own cluster, with the console turned off
there and its API exposed for the shared console to reach (handbook 20 §20.2):

```yaml
# domain-b-values.yaml, installed as its own release with its own namespace and Secrets; its auth
# Secret holds the same federation-key as every other domain's.
domain: {id: domain_b, name: Domain B}
console: {enabled: false}
route:
  enabled: true
  hostnames: [midstream-b.example.com]          # this domain's API: /, /catalog, /tail/
  parentRefs: [{name: gateway, namespace: gateway-system}]
```

The release with the console then lists every domain; the console's own domain needs only
`local: true`, another domain its URLs and a Secret here holding its `service-token`:

```yaml
console:
  url: https://midstream.example.com
  domains:
    - {id: domain_a, name: Domain A, local: true, settingsHome: true}
    - id: domain_b
      name: Domain B
      controlPlaneUrl: https://midstream-b.example.com
      tailUrl: https://midstream-b.example.com
      serviceTokenSecret: {name: domain-b-service-token, key: service-token}
route:
  enabled: true
  consoleHostnames: [midstream.example.com]
  redirectFrom: [midstream-a.example.com]       # old hostnames, sent on to /d/domain_a/
```

**Signing in with a provider.** Register the console with Microsoft Entra ID, GitHub, Google or any
OpenID Connect provider, with the redirect URI `<console.url>/api/auth/sso/<entra|github|google|oidc>/callback`;
give the console its client (`console.sso.<provider>`), and every domain the same client id
(`auth.sso.<provider>`), so each can check what the provider says itself. Each domain decides who
gets in: its first-sign-in policy (`auth.sso.firstSignIn`, `open` or `invited`), its allowed email
domains, and whether a password still works there (`auth.passwordSignIn`) — all adjustable later on
its Settings page. Handbook 20 §20.7 has each provider's registration, step by step.

## 8. Upgrade and remove

```bash
helm upgrade "$RELEASE" oci://ghcr.io/vmirz/charts/midstream --version <version> -n "$NS" -f midstream-values.yaml   # re-applies every runtime too
helm uninstall "$RELEASE" -n "$NS"                                          # the apps and their claims; runtimes and topics stay until each materialization is deleted
```

Delete every materialization before uninstalling, with the console or `DELETE /materializations/{id}`:
that removes its runtime, its Kafka topics and its tables. The Secrets and the bucket are yours.

## Where next

- `docs/handbook/02-deployment-and-manifests.md`: every resource the chart deploys and why.
- `docs/handbook/12-operations-runbook.md`: diagnosis by phase, sizing, disaster recovery.
- `docs/handbook/18-identity-and-access.md`: people, groups, roles, datasets, access tokens and single sign-on.
- `docs/handbook/20-domains-and-the-shared-console.md`: domains, the shared console and everything across them.
- `docs/handbook/21-kafka-access.md`: the Kafka user's ACLs on a cluster that enforces them, and what breaks without each.
- `console/docs/13-operations-runbook.md`: running and sizing the console.
