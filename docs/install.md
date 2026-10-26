# Installing Midstream

This walks through a first installation: the prerequisites, the Secrets, the chart, the first
sign-in, the first dataset and materialization, and how to upgrade and remove it. Every command is
meant to be pasted as it stands; change the names in the first block to yours.

```bash
# The names this guide uses. Keep them, or change them once here.
export NS=midstream                # the namespace Midstream and its runtimes live in
export RELEASE=midstream           # the Helm release: every resource, topic and table is named after it
export KAFKA=kafka-bootstrap.kafka.svc.cluster.local:9092   # the Kafka bootstrap address, reachable in-cluster
export KAFKA_USER=midstream        # a SCRAM-SHA-512 user on that cluster
export BUCKET=gs://my-lake         # the GCS bucket the tables are written to
export ADMIN_EMAIL=you@example.com # the first administrator
```

## 1. What you need first

| Requirement | Why | Check |
|---|---|---|
| Kubernetes 1.29 or later; 1.33 with the Vertical Pod Autoscaler add-on to resize pods in place | everything runs as Kaspr apps; the CPU requests follow use | `kubectl version` |
| [kaspr-operator](https://kaspr.dev) 0.19.7 or later, with its CRDs | Midstream is Kaspr applications: `KasprApp`, `KasprAgent`, `KasprTask`, `KasprTable`, `KasprWebView` | `kubectl get crd kasprapps.kaspr.io` |
| A Kafka cluster reachable from the namespace, with a SCRAM-SHA-512, SCRAM-SHA-256 or plain user that may create topics | the control plane, the apps and every runtime declare their own topics | `kubectl -n kafka get kafka` for Strimzi, or your cluster's tooling |
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

# Sign-in: a signing key for sessions, the service token the console and the tail app use, and the
# first administrator. The password is shown once; the first sign-in replaces it.
BOOTSTRAP_PASSWORD="$(openssl rand -base64 18)"
kubectl -n "$NS" create secret generic "$RELEASE-auth" \
  --from-literal=signing-key="$(openssl rand -base64 48)" \
  --from-literal=service-token="$(openssl rand -base64 48)" \
  --from-literal=bootstrap-admin-email="$ADMIN_EMAIL" \
  --from-literal=bootstrap-admin-password="$BOOTSTRAP_PASSWORD"
echo "bootstrap password for $ADMIN_EMAIL: $BOOTSTRAP_PASSWORD"
unset BOOTSTRAP_PASSWORD
```

The auth Secret's keys are read by name (`signing-key`, `service-token`, `bootstrap-admin-email`,
`bootstrap-admin-password`, and `previous-signing-key` when a key is rotated); the chart only needs
its name. Without `signing-key` nobody can sign in and every request is a guest's.

On GKE with workload identity, skip the GCS Secret and leave `gcs.credentialsSecret` empty: the pods
use application default credentials.

## 3. Write the values

```bash
cat > midstream-values.yaml <<VALUES
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
often changed: `kaspr.storageClass` when the cluster has no default storage class, `kaspr.nodeSelector`
and `kaspr.tolerations` to place every pod, `tail.enabled: false` to run without the live tail,
`console.enabled: false` to run without the console, and `route` or `ingress` to expose the console
and the catalog.

`package.source` is `git` by default and installs this repository's `main` branch; set
`package.git.ref` to a tag or commit to pin it. `package.source: index` installs a published release
from PyPI instead (`package.index.version`, the chart's appVersion by default); the control plane then
pins its runtimes to the same version. The console image for a release is
`ghcr.io/vmirz/midstream-console:<version>`, the chart's default.

## 4. Install

```bash
helm install "$RELEASE" charts/midstream -n "$NS" -f midstream-values.yaml

kubectl -n "$NS" get kasprapp -w          # three apps: control-plane, maintenance, tail
```

The first start of each app installs the Python package, which takes a minute or two; then the
operator reports every app `Ready`. If an app stays in its install step, look at its init
container: `kubectl -n "$NS" logs <pod> -c python-packages-install`.

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

With `route.enabled` or `ingress.enabled` the console is reachable at its host instead, and engines
reach the Iceberg REST catalog at `https://<host>/catalog`.

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

## 7. Upgrade and remove

```bash
helm upgrade "$RELEASE" charts/midstream -n "$NS" -f midstream-values.yaml   # re-applies every runtime too
helm uninstall "$RELEASE" -n "$NS"                                          # the apps and their claims; runtimes and topics stay until each materialization is deleted
```

Delete every materialization before uninstalling, with the console or `DELETE /materializations/{id}`:
that removes its runtime, its Kafka topics and its tables. The Secrets and the bucket are yours.

## Where next

- `docs/handbook/02-deployment-and-manifests.md`: every resource the chart deploys and why.
- `docs/handbook/12-operations-runbook.md`: diagnosis by phase, sizing, disaster recovery.
- `docs/handbook/18-identity-and-access.md`: people, groups, roles, datasets and access tokens.
- `console/docs/13-operations-runbook.md`: running and sizing the console.
