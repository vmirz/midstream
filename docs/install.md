# Installing Midstream

This walks through a first installation: the prerequisites, the object store and the Secrets, the
chart, the first sign-in, the first dataset and materialization, more domains and sign-in through a
provider, and how to upgrade and remove it. Every command is meant to be pasted as it stands; change
the names in the first block, and your object store's in §2.1, to yours.

One installation of the chart is a **domain**: a control plane with its maintenance and tail apps, its
own accounts, datasets and tables. The console serves every domain from one URL; the first install
below is one domain with the console beside it, and §7 adds more (handbook 20).

```bash
# The names this guide uses. Keep them, or change them once here.
export NS=midstream                # the namespace Midstream and its runtimes live in
export RELEASE=midstream           # the Helm release: every resource, topic and table is named after it
export KAFKA=kafka-bootstrap.kafka.svc.cluster.local:9092   # the Kafka bootstrap address, reachable in-cluster
export KAFKA_USER=midstream        # a SCRAM-SHA-512 user on that cluster
export DESTINATION=s3://example-lake   # where the tables go, set again in §2.1 from your store's names:
                                       # gs://example-lake, s3://example-lake or
                                       # abfss://lake@exampleaccount.dfs.core.windows.net
export ADMIN_EMAIL=you@example.com # the first administrator
export DOMAIN=domain_a             # this domain's id: its URL segment /d/<id> and its catalog in SQL
```

## 1. What you need first

| Requirement | Why | Check |
|---|---|---|
| Kubernetes 1.29 or later; 1.33 with the Vertical Pod Autoscaler add-on to resize pods in place | everything runs as Kaspr apps; the CPU requests follow use | `kubectl version` |
| [kaspr-operator](https://kaspr.dev) 0.19.7 or later, with its CRDs | Midstream is Kaspr applications: `KasprApp`, `KasprAgent`, `KasprTask`, `KasprTable`, `KasprWebView` | `kubectl get crd kasprapps.kaspr.io` |
| A Kafka cluster reachable from the namespace, with a SCRAM-SHA-512, SCRAM-SHA-256 or plain user. If the cluster enforces ACLs, the user needs the grants of handbook 21 §21.3: create, read and write the topics named `$RELEASE-…`, read the groups and write the transactional ids named so, read every topic you will materialize, and, to delete materializations, delete the topics and groups named `$RELEASE-mat-…` | the control plane, the apps and every runtime declare their own topics at every start and write them in transactions | `kubectl -n kafka get kafka` for Strimzi, or your cluster's tooling |
| One object store for the tables: a Google Cloud Storage bucket, an Amazon S3 bucket (on AWS, or on an S3-compatible server that honours conditional writes; SeaweedFS is verified), or an Azure storage account with a container. An identity that may read, write, delete and list objects under the destination, and a read-only one for the console | the Iceberg tables, their metadata and the catalog pointers live there; every commit replaces a table's pointer by a conditional write, which the store must honour | `python -m midstream.lakehouse.store.check` (§2.2) |
| `helm` 3.12 or later and `kubectl` on your machine | | `helm version` |

The console reads the tables through storage profiles of its own, with the read-only identity (§3);
engines that attach to the Iceberg REST catalog bring their own access to the store, since the
catalog hands out no storage credentials.

## 2. Create the namespace and the Secrets

```bash
kubectl create namespace "$NS"

# Kafka: the password of the SCRAM user.
read -rs -p "Kafka password for $KAFKA_USER: " KAFKA_PASSWORD; echo
kubectl -n "$NS" create secret generic "$RELEASE-kafka" --from-literal=password="$KAFKA_PASSWORD"
unset KAFKA_PASSWORD

# Sign-in: a signing key for sessions, the service token the console and the tail app use, the
# federation key access tokens are signed with - the same in every domain, so keep it for §7 - the
# key data protection hashes with - the same in every domain whose masked columns should join - and
# the first administrator. The password is shown once; the first sign-in replaces it.
FEDERATION_KEY="$(openssl rand -base64 48)"
PROTECTION_KEY="$(openssl rand -base64 48)"
BOOTSTRAP_PASSWORD="$(openssl rand -base64 18)"
kubectl -n "$NS" create secret generic "$RELEASE-auth" \
  --from-literal=signing-key="$(openssl rand -base64 48)" \
  --from-literal=service-token="$(openssl rand -base64 48)" \
  --from-literal=federation-key="$FEDERATION_KEY" \
  --from-literal=protection-key="$PROTECTION_KEY" \
  --from-literal=bootstrap-admin-email="$ADMIN_EMAIL" \
  --from-literal=bootstrap-admin-password="$BOOTSTRAP_PASSWORD"
echo "bootstrap password for $ADMIN_EMAIL: $BOOTSTRAP_PASSWORD"
unset BOOTSTRAP_PASSWORD
```

The auth Secret's keys are read by name (`signing-key`, `service-token`, `federation-key`,
`protection-key`, `bootstrap-admin-email`, `bootstrap-admin-password`, and `previous-signing-key` /
`previous-federation-key` when a key is rotated); the chart only needs its name. Without
`signing-key` nobody can sign in and every request is a guest's; without `federation-key` nobody can
make an access token for an engine; without `protection-key` nothing can be hashed, so a
materialization created with the default protection — emails, phone numbers and national identifiers
hashed — is refused (handbook 22). Keep `protection-key` for the lake's lifetime, with the federation
key: a new one makes new hashes stop matching old ones.

A Kafka listener without authentication needs no Secret: skip the `$RELEASE-kafka` command above and
set `kaspr.authentication` to `{type: plain}` alone in §3.

The object store's Secrets come next.

### 2.1 The object store: pick one

The lake — every materialization's tables, their metadata and the catalog pointers — lives under the
destination, and a table's location names its backend by its scheme, for good: `gs://` is Google
Cloud Storage, `s3://` Amazon S3 or an S3-compatible server, `abfss://` Azure Blob Storage, each with
an optional `/<prefix>`. The chart refuses any other form, `file://` included, which serves tests and
a laptop only.

Every commit replaces a table's pointer on the condition that no other commit has moved it (handbook
11 §11.2), so the store must honour conditional writes; and every commit, compaction, expiry and drop
replaces or deletes objects, so the destination may carry no retention or immutability rule that
forbids it. Midstream's S3 store is verified against SeaweedFS and moto, and its Azure store against
Azurite: against Amazon S3 itself or a real storage account, §2.2 is the proof.

Each backend below makes two identities, puts their credentials in Secrets (or lets the pods use a
cloud identity instead), and ends with the values they need, written for §3's file, where the shell
fills in the names:

| Backend | Midstream: the control plane, the maintenance and tail apps, every runtime | The console |
|---|---|---|
| Google Cloud Storage | a service account with `roles/storage.objectAdmin` on the bucket | a service account with `roles/storage.objectViewer` on it, and that account's HMAC key for DuckDB |
| Amazon S3 | `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject` on the objects, `s3:ListBucket` on the bucket | `s3:GetObject` and `s3:ListBucket` |
| Azure Blob Storage | Storage Blob Data Contributor on the container, or the account's key | Storage Blob Data Reader on the container; never the account's key, alone or in a connection string, which always writes |
| SeaweedFS | an identity with `Read`, `List` and `Write` on the bucket | an identity with `Read` and `List` |

The console never writes a table, but DuckDB, which runs its statements, could: a statement's sandbox
does not stop `COPY … TO` a path it may read, only the SQL guard does (console 12 §12.4). Give it an
identity that cannot write.

One store per scheme serves a whole domain, with one identity and, for S3, one endpoint: a domain may
write to several backends, each with its block under `storage`, but not to two S3 servers.

#### Google Cloud Storage

```bash
export PROJECT=example-project               # the Google Cloud project
export BUCKET=example-lake
export DESTINATION=gs://$BUCKET              # or gs://$BUCKET/<prefix>

# The bucket, with uniform bucket-level access, near the cluster.
gcloud storage buckets create "gs://$BUCKET" --project="$PROJECT" --location=europe-west1 \
  --uniform-bucket-level-access

# Midstream's service account: read, write, delete and list the bucket's objects. Its key goes in
# Secret $RELEASE-gcs, under service-account.json.
gcloud iam service-accounts create midstream-lake --project="$PROJECT"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --role=roles/storage.objectAdmin \
  --member="serviceAccount:midstream-lake@$PROJECT.iam.gserviceaccount.com"
gcloud iam service-accounts keys create midstream-lake.json \
  --iam-account="midstream-lake@$PROJECT.iam.gserviceaccount.com"
kubectl -n "$NS" create secret generic "$RELEASE-gcs" --from-file=service-account.json=midstream-lake.json

# The console's: read and list, with a key for the metadata and an HMAC key for DuckDB's scans, the
# HMAC secret read here and never printed.
gcloud iam service-accounts create midstream-console --project="$PROJECT"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --role=roles/storage.objectViewer \
  --member="serviceAccount:midstream-console@$PROJECT.iam.gserviceaccount.com"
gcloud iam service-accounts keys create midstream-console.json \
  --iam-account="midstream-console@$PROJECT.iam.gserviceaccount.com"
read -r HMAC_KEY_ID HMAC_SECRET < <(gcloud storage hmac create --project="$PROJECT" \
  "midstream-console@$PROJECT.iam.gserviceaccount.com" --format='value(metadata.accessId,secret)')
kubectl -n "$NS" create secret generic "$RELEASE-console-gcs" \
  --from-file=service-account.json=midstream-console.json \
  --from-literal=hmac-key-id="$HMAC_KEY_ID" --from-literal=hmac-secret="$HMAC_SECRET"
unset HMAC_KEY_ID HMAC_SECRET
```

Delete the two key files once §2.2 has passed: the Secrets hold them.

On GKE with workload identity, Midstream needs no key: grant its role to the namespace instead of
making the `midstream-lake` service account, its key and its Secret, and leave
`storage.gcs.credentialsSecret` empty; the pods then use application default credentials. The grant
covers every pod in the namespace, the console's too: to keep the console read-only, run it in a
namespace of its own (a release that holds only the console) or give its profile its read-only key.

```bash
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --role=roles/storage.objectAdmin \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT.svc.id.goog/namespace/$NS"
```

The grant reaches every pod in the namespace, the console's included, so keep the console's read-only
keys: its profile reads with them, and DuckDB scans `gs://` with an HMAC key alone.

The values (§3):

```yaml
storage:
  gcs:
    credentialsSecret: $RELEASE-gcs          # empty with workload identity
controlPlane:
  settings:
    defaultDestinationUri: $DESTINATION
console:
  storage:
    - name: lake
      scope: $DESTINATION/
      kind: gcs
      credentialsSecret: $RELEASE-console-gcs
```

#### Amazon S3

```bash
export AWS_REGION=eu-west-1                  # the bucket's region
export BUCKET=example-lake
export DESTINATION=s3://$BUCKET              # or s3://$BUCKET/<prefix>

# The bucket, in its region (in us-east-1, leave out --create-bucket-configuration).
aws s3api create-bucket --bucket "$BUCKET" --region "$AWS_REGION" \
  --create-bucket-configuration LocationConstraint="$AWS_REGION"

# What Midstream may do, and what the console may.
cat > midstream-lake-policy.json <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::$BUCKET"},
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], "Resource": "arn:aws:s3:::$BUCKET/*"}
  ]
}
POLICY
cat > midstream-console-policy.json <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::$BUCKET"},
    {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::$BUCKET/*"}
  ]
}
POLICY
```

`s3:ListBucket` is what makes a read of a missing key answer 404: without it S3 answers 403, and
Midstream cannot tell a table that does not exist yet from a refusal, so creating one fails. A
conditional write that replaces an object (`If-Match`, as every commit's does) needs `s3:GetObject` as
well as `s3:PutObject`: without it every commit is refused.

Credentials as access keys: an IAM user per identity, its key read here and never printed, in Secrets
`$RELEASE-s3` and `$RELEASE-console-s3` (`access-key-id`, `secret-access-key`, and `session-token`
for temporary keys):

```bash
for who in lake console; do
  aws iam create-user --user-name "midstream-$who"
  aws iam put-user-policy --user-name "midstream-$who" --policy-name "midstream-$who" \
    --policy-document "file://midstream-$who-policy.json"
done
read -r KEY_ID KEY_SECRET < <(aws iam create-access-key --user-name midstream-lake \
  --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)
kubectl -n "$NS" create secret generic "$RELEASE-s3" \
  --from-literal=access-key-id="$KEY_ID" --from-literal=secret-access-key="$KEY_SECRET"
read -r KEY_ID KEY_SECRET < <(aws iam create-access-key --user-name midstream-console \
  --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)
kubectl -n "$NS" create secret generic "$RELEASE-console-s3" \
  --from-literal=access-key-id="$KEY_ID" --from-literal=secret-access-key="$KEY_SECRET"
unset KEY_ID KEY_SECRET
```

Or a pod identity, and no Secret. With IRSA, Midstream's role carries its policy, and its trust
policy lets the cluster's OIDC provider assume it for `system:serviceaccount:<namespace>:*-app`, a
`StringLike` condition: every app and every runtime runs under a ServiceAccount of its own named
`<app>-app`, and the console's, `$RELEASE-console`, stays out, so the console cannot assume the
read-write role.
`kaspr.serviceAccountAnnotations` names the role as `eks.amazonaws.com/role-arn`, and
`storage.s3.credentialsSecret` stays empty. The console's role, with the console's policy, goes in
`console.serviceAccountAnnotations`, and its profile takes no `credentialsSecret`. EKS Pod Identity
takes an association for each ServiceAccount instead, every new runtime's included.

`storage.s3.region` and the profile's `region` must be the bucket's region: requests are signed for
it, and both default to `us-east-1`.

The values (§3):

```yaml
storage:
  s3:
    credentialsSecret: $RELEASE-s3           # empty with a pod identity
    region: $AWS_REGION
controlPlane:
  settings:
    defaultDestinationUri: $DESTINATION
console:
  storage:
    - name: lake
      scope: $DESTINATION/
      kind: s3
      region: $AWS_REGION
      credentialsSecret: $RELEASE-console-s3
```

#### Azure Blob Storage

```bash
export RESOURCE_GROUP=example-group
export ACCOUNT=exampleaccount                # 3 to 24 lower case letters and digits, unique in Azure
export CONTAINER=lake                        # 3 to 63 lower case letters, digits and single hyphens
export DESTINATION=abfss://$CONTAINER@$ACCOUNT.dfs.core.windows.net   # or …/<prefix>

# A StorageV2 account with a hierarchical namespace, and the container.
az storage account create --name "$ACCOUNT" --resource-group "$RESOURCE_GROUP" --location westeurope \
  --kind StorageV2 --sku Standard_LRS --hns true --allow-blob-public-access false --min-tls-version TLS1_2
az storage container-rm create --storage-account "$ACCOUNT" --resource-group "$RESOURCE_GROUP" --name "$CONTAINER"
```

A hierarchical namespace is recommended, not required: Midstream and the console reach the account
through its Blob endpoint, which every account has, while engines read the tables' `abfss://`
locations through its DFS endpoint.

Midstream's credentials go in Secret `$RELEASE-azure`, as one of: a service principal with Storage
Blob Data Contributor on the container (`client-id`, `tenant-id` and `client-secret`), shown here; the
account's key (`account-key`); or a connection string (`connection-string`). A key or a connection
string serves its own account only, so destinations in several accounts take a service principal or
workload identity. The console gets a service principal of its own with Storage Blob Data Reader,
which reads and lists but cannot write; the account's key always writes, alone or in a connection
string, so never give it to the console.

```bash
CONTAINER_SCOPE="$(az storage account show --name "$ACCOUNT" --resource-group "$RESOURCE_GROUP" \
  --query id --output tsv)/blobServices/default/containers/$CONTAINER"
# Each service principal's id, tenant and secret, one to a line in az's tsv, read here and never printed.
{ read -r CLIENT_ID; read -r TENANT_ID; read -r CLIENT_SECRET; } < <(az ad sp create-for-rbac --name midstream-lake \
  --role "Storage Blob Data Contributor" --scopes "$CONTAINER_SCOPE" --query '[appId, tenant, password]' --output tsv)
kubectl -n "$NS" create secret generic "$RELEASE-azure" --from-literal=client-id="$CLIENT_ID" \
  --from-literal=tenant-id="$TENANT_ID" --from-literal=client-secret="$CLIENT_SECRET"
{ read -r CLIENT_ID; read -r TENANT_ID; read -r CLIENT_SECRET; } < <(az ad sp create-for-rbac --name midstream-console \
  --role "Storage Blob Data Reader" --scopes "$CONTAINER_SCOPE" --query '[appId, tenant, password]' --output tsv)
kubectl -n "$NS" create secret generic "$RELEASE-console-azure" --from-literal=client-id="$CLIENT_ID" \
  --from-literal=tenant-id="$TENANT_ID" --from-literal=client-secret="$CLIENT_SECRET"
unset CLIENT_ID TENANT_ID CLIENT_SECRET

# Or, for Midstream alone, the account's key instead of its service principal:
#   kubectl -n "$NS" create secret generic "$RELEASE-azure" --from-literal=account-key="$(az storage account \
#     keys list --account-name "$ACCOUNT" --resource-group "$RESOURCE_GROUP" --query '[0].value' --output tsv)"
```

A role assignment can take some minutes to apply; §2.2's check says when it has.

AKS workload identity needs no Secret: `kaspr.podLabels` carries `azure.workload.identity/use:
"true"`, `kaspr.serviceAccountAnnotations` carries `azure.workload.identity/client-id: <the identity's
client id>`, `storage.azure.credentialsSecret` stays empty, and the identity takes a federated
credential for each ServiceAccount. Every new materialization brings one,
`<runtimeNamePrefix>-<materialization id>-app`, whose runtime cannot write until its federated
credential exists. An identity takes at most 20 federated credentials, so workload identity suits a
domain of a few materializations; a service principal's Secret has no such limit. The console's own identity, with Storage Blob Data Reader, goes in
`console.podLabels` and `console.serviceAccountAnnotations` the same way, with a federated credential
for its one ServiceAccount, `$RELEASE-console`; its profile then takes no `credentialsSecret`.

The values (§3):

```yaml
storage:
  azure:
    credentialsSecret: $RELEASE-azure        # empty with workload identity
controlPlane:
  settings:
    defaultDestinationUri: $DESTINATION
console:
  storage:
    - name: lake
      scope: $DESTINATION/
      kind: azure
      credentialsSecret: $RELEASE-console-azure
```

#### An S3-compatible server in the cluster: SeaweedFS

For development and test clusters, SeaweedFS (Apache-2.0) serves S3 from one pod: `weed mini` runs
its master, volume server, filer and S3 gateway in one process, with the data on a persistent volume,
and the gateway honours conditional writes. One pod and one volume hold the whole lake. MinIO's
community edition is not suggested: it was archived in February 2026 and ships neither binaries nor
images. Nor is Garage, which cannot compare-and-swap, by design.

```bash
export BUCKET=example-lake                   # SeaweedFS creates it at start
export DESTINATION=s3://$BUCKET
kubectl create namespace seaweedfs

# Two identities, their keys generated here, handed to kubectl as files and never printed:
# midstream reads, writes, deletes and lists in the bucket; midstream-console reads and lists.
WORK="$(mktemp -d)"
for who in midstream console; do
  openssl rand -hex 10 | tr '[:lower:]' '[:upper:]' | tr -d '\n' > "$WORK/$who-access-key-id"
  openssl rand -hex 20 | tr -d '\n' > "$WORK/$who-secret-access-key"
done
cat > "$WORK/s3.json" <<IDENTITIES
{"identities": [
  {"name": "midstream", "actions": ["Read:$BUCKET", "List:$BUCKET", "Write:$BUCKET"],
   "credentials": [{"accessKey": "$(cat "$WORK/midstream-access-key-id")", "secretKey": "$(cat "$WORK/midstream-secret-access-key")"}]},
  {"name": "midstream-console", "actions": ["Read:$BUCKET", "List:$BUCKET"],
   "credentials": [{"accessKey": "$(cat "$WORK/console-access-key-id")", "secretKey": "$(cat "$WORK/console-secret-access-key")"}]}
]}
IDENTITIES
kubectl -n seaweedfs create secret generic seaweedfs-s3-identities --from-file=s3.json="$WORK/s3.json"
kubectl -n "$NS" create secret generic "$RELEASE-s3" \
  --from-file=access-key-id="$WORK/midstream-access-key-id" \
  --from-file=secret-access-key="$WORK/midstream-secret-access-key"
kubectl -n "$NS" create secret generic "$RELEASE-console-s3" \
  --from-file=access-key-id="$WORK/console-access-key-id" \
  --from-file=secret-access-key="$WORK/console-secret-access-key"
rm -rf "$WORK"

kubectl apply -f - <<MANIFESTS
apiVersion: v1
kind: Service
metadata:
  name: seaweedfs-s3
  namespace: seaweedfs
  labels: {app: seaweedfs}
spec:
  selector: {app: seaweedfs}
  ports:
    - {name: s3, port: 8333, targetPort: s3}
---
# The StatefulSet's governing Service.
apiVersion: v1
kind: Service
metadata:
  name: seaweedfs
  namespace: seaweedfs
  labels: {app: seaweedfs}
spec:
  clusterIP: None
  selector: {app: seaweedfs}
  ports:
    - {name: s3, port: 8333, targetPort: s3}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: seaweedfs
  namespace: seaweedfs
  labels: {app: seaweedfs}
spec:
  serviceName: seaweedfs
  replicas: 1                        # weed mini is one store: a second pod would be another
  selector:
    matchLabels: {app: seaweedfs}
  template:
    metadata:
      labels: {app: seaweedfs}
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: seaweedfs
          image: docker.io/chrislusf/seaweedfs:4.51
          args:
            - mini
            - -dir=/data
            # The components reach each other over loopback, whatever the pod's address after a
            # restart; clients only ever use the S3 port, through the Service.
            - -ip=127.0.0.1
            - -ip.bind=0.0.0.0
            - -s3.config=/etc/seaweedfs/s3.json
            - -bucket=$BUCKET
            - -master.volumeSizeLimitMB=1024
            # Not needed: the admin UI, WebDAV, and SeaweedFS's own Iceberg and Lance servers.
            - -admin.ui=false
            - -webdav=false
            - -s3.port.iceberg=0
            - -s3.port.lance=0
            # SeaweedFS reports anonymous cluster statistics once 10 GiB are stored, unless told not to.
            - -master.telemetry=false
          ports:
            - {name: s3, containerPort: 8333}
          readinessProbe:
            httpGet: {path: /healthz, port: s3}
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet: {path: /healthz, port: s3}
            initialDelaySeconds: 30
            periodSeconds: 20
            failureThreshold: 6
          resources:
            requests: {cpu: 250m, memory: 512Mi}
            limits: {cpu: "4", memory: 4Gi}
          volumeMounts:
            - {name: data, mountPath: /data}
            - {name: identities, mountPath: /etc/seaweedfs, readOnly: true}
      volumes:
        - name: identities
          secret:
            secretName: seaweedfs-s3-identities
  # The lake itself, on the cluster's default storage class; storageClassName under spec picks another.
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests: {storage: 100Gi}
MANIFESTS
kubectl -n seaweedfs rollout status statefulset/seaweedfs --timeout=300s
```

The cluster reaches it at `http://seaweedfs-s3.seaweedfs.svc.cluster.local:8333`, with path-style
addressing and any region. SeaweedFS reads its identities when it starts: after changing the Secret,
restart it (`kubectl -n seaweedfs rollout restart statefulset seaweedfs`).

The values (§3):

```yaml
storage:
  s3:
    credentialsSecret: $RELEASE-s3
    endpoint: http://seaweedfs-s3.seaweedfs.svc.cluster.local:8333
    pathStyle: true
controlPlane:
  settings:
    defaultDestinationUri: $DESTINATION
console:
  storage:
    - name: lake
      scope: $DESTINATION/
      kind: s3
      endpoint: http://seaweedfs-s3.seaweedfs.svc.cluster.local:8333
      pathStyle: true
      credentialsSecret: $RELEASE-console-s3
```

### 2.2 Check the store

The conformance suite proves, against the store itself, what the tables rely on: create-only writes
and creators racing for one key, compare-and-swap and its refusals, sixteen writers racing on one
object with exactly one winner per round, reads of missing objects, byte ranges, listings across
pages, streamed transfers and deletes. It works under `$DESTINATION/_check/<run id>/`, outside every
table, and deletes everything it wrote. Run it where the pods run, with their identity.

After installing (§4), from the control plane's pod, which has the chart's environment:

```bash
kubectl -n "$NS" exec "$RELEASE-control-plane-app-0" -c kaspr -- python -m midstream.lakehouse.store.check "$DESTINATION/_check"
```

Before installing, from a machine that reaches the store as the pods will, with the package of the
release you install (Python 3.12 or later) and the variables the chart would set:

```bash
python3 -m venv midstream-check && . midstream-check/bin/activate
python -m pip install "midstream-platform==<version>"
```

| Destination | Variables |
|---|---|
| `gs://` | `GOOGLE_APPLICATION_CREDENTIALS`, the path of Midstream's key file; unset, application default credentials (yours, on a machine: with workload identity, check from the pod) |
| `s3://` | `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with `AWS_SESSION_TOKEN` for temporary keys; `STORAGE_S3_REGION`; for an S3-compatible server `STORAGE_S3_ENDPOINT` and `STORAGE_S3_PATH_STYLE=true` |
| `abfss://` | `AZURE_STORAGE_CONNECTION_STRING`, or `AZURE_STORAGE_ACCOUNT_KEY`, or `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_SECRET` |

On S3, for example, with Midstream's keys read from their Secret and never printed:

```bash
export AWS_ACCESS_KEY_ID="$(kubectl -n "$NS" get secret "$RELEASE-s3" -o jsonpath='{.data.access-key-id}' | base64 --decode)"
export AWS_SECRET_ACCESS_KEY="$(kubectl -n "$NS" get secret "$RELEASE-s3" -o jsonpath='{.data.secret-access-key}' | base64 --decode)"
STORAGE_S3_REGION="$AWS_REGION" python -m midstream.lakehouse.store.check "$DESTINATION/_check"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
```

For the in-cluster SeaweedFS, forward its port first (`kubectl -n seaweedfs port-forward
svc/seaweedfs-s3 8333:8333 &`) and give `STORAGE_S3_ENDPOINT=http://localhost:8333
STORAGE_S3_PATH_STYLE=true` instead of the region.

A run takes a minute or two — SeaweedFS settles creates racing for one key slowly, about ten seconds a
key — and prints a line per check, then a summary; it exits 0 only when every check passed:

```
Checking S3Store under s3://example-lake/_check/8490a5fa9656/
PASS create_only                 0.25s  created at version 8b04d5e3775d298e78455efc5ca404d5; the second create was refused
PASS racing_creators            58.91s  6 new keys each won by one of 16 racing creators; 90 creates were refused
…
PASS concurrent_writers          7.24s  170 won and 230 were refused, 16 writers x 25 rounds
…
PASS conditional_delete          0.00s  skipped: the store does not support conditional deletes
PASS large_object                0.00s  skipped: no size given (large_bytes; --large-mb on the command line)
PASS cleanup                     0.59s  deleted 1,065 objects; nothing is left under s3://example-lake/_check/8490a5fa9656/
16 of 16 checks passed
```

A check that fails says what the store answered, the summary names it, and the run exits 1:

```
FAIL concurrent_writers          4.12s  the counter holds 171 after 186 compare-and-swaps succeeded: two writers won the same round, or a win was lost
15 of 16 checks passed; failed: concurrent_writers
```

- `create_only`, `racing_creators`, `compare_and_swap`, `missing_under_condition` or
  `concurrent_writers`: the store does not honour conditional writes, or not atomically. Do not use
  it: two commits could both replace a table's pointer, and one of them would be lost.
- `missing_object` with `StoreError (status 403, code AccessDenied)` on S3: the identity lacks
  `s3:ListBucket`.
- Status 401 or 403 elsewhere: the identity lacks a permission of §2.1, or a new Azure role assignment
  has not applied yet. Azure's answers name the problem: no data role, refused credentials, a missing
  container. On S3, `NoSuchBucket` means a missing bucket, or a `STORAGE_S3_ENDPOINT` or
  `STORAGE_S3_REGION` naming the wrong server.

`Cannot check …`, with exit status 2, means the URI has no scheme Midstream knows or a `STORAGE_*`
variable holds what cannot be used. `--large-mb 64` adds a 64 MiB file streamed up and down and
create-only uploads of such files racing for one key; `--help` lists the other options.

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
storage:
  s3:
    credentialsSecret: $RELEASE-s3
    region: $AWS_REGION
controlPlane:
  settings:
    defaultDestinationUri: $DESTINATION
console:
  storage:
    - name: lake
      scope: $DESTINATION/
      kind: s3
      region: $AWS_REGION
      credentialsSecret: $RELEASE-console-s3
VALUES
```

This is a domain on Amazon S3. On another backend, its `storage`, `controlPlane.settings` and
`console.storage` blocks from §2.1 take the place of these.

**The console's storage profiles.** `console.storage` says where the console reads tables from: one
profile per destination prefix, with credentials of its own. Every object is read through the profile
whose `scope` is the longest prefix of its URI, as DuckDB picks among scoped secrets. A profile has a
`name` (a lower case letter, then up to 39 lower case letters, digits or hyphens; its credentials
reach the console as `CONSOLE_STORAGE_<NAME>_*`), a `scope` (the destination prefix it serves, ending
with `/`), a `kind` (`gcs`, `s3` or `azure`, matching the scope's scheme) and, as its backend needs,
an `endpoint`, a `region`, `pathStyle`, `access` and a `credentialsSecret`; `values.yaml` documents
each. A `gcs` profile scans data files in place only with its HMAC key, and downloads each one first
without it. The `s3` profiles of one bucket name one endpoint: an `s3://` path names none. A table
under no profile's scope is listed, but its Table and Data tabs answer "no storage is configured for
<location>", and `helm install` warns when no profile serves this domain's destination.

Everything else has a default; `charts/midstream/values.yaml` documents each value. The ones most
often changed: `kaspr.storageClass` when the cluster has no default storage class,
`kaspr.config.topicReplicationFactor` on a cluster with fewer than three brokers (Kaspr's default is 3), `kaspr.nodeSelector`
and `kaspr.tolerations` to place every pod, `tail.enabled: false` to run without the live tail,
`console.enabled: false` to run without the console, and `route` or `ingress` to expose the console
and the catalog.

`package.source` is `index` by default: every app installs the release the chart belongs to,
`midstream-platform==<appVersion>`, from PyPI, and the control plane pins its runtimes to the same
version. `package.source: git` installs this repository at `package.git.ref` instead, for development.
The console image for a release is `ghcr.io/vmirz/midstream-console:<version>`, the chart's default;
every commit pushed to a branch has one too, `ghcr.io/vmirz/midstream-console-dev:sha-<commit>`, to
run with `package.source: git` at that commit (handbook 16 §16.3).

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

The store, from the control plane's pod (§2.2): every check passes.

```bash
kubectl -n "$NS" exec "$RELEASE-control-plane-app-0" -c kaspr -- python -m midstream.lakehouse.store.check "$DESTINATION/_check"
```

The console states at start how it reads with each storage profile, one line each:

```bash
kubectl -n "$NS" logs "deploy/$RELEASE-console" | grep 'storage profile'
```

```
{"level":30,"time":1793408706484,"service":"midstream-console","profile":"lake","kind":"s3","scope":"s3://example-lake/","mode":"direct","credentials":"found","readsWith":"an access key","msg":"storage profile: DuckDB reads its data files in place"}
```

`mode` is `direct` when DuckDB scans the data files in place and `cached` when the console downloads
them first; `credentials` is `not found` when the profile's Secret gave it none, and it then reads
with the console pod's own identity. A warning names any variable to fix, never its value, and
another reminds that profiles read in place must be read-only. The console's Settings page shows the
same under Object storage access.

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
namespace, and its table at `$DESTINATION/$RELEASE/<materialization id>/orders`. The console's Data
tab shows the rows as they commit, and new records within seconds through the live tail.

## 7. More domains, and signing in with a provider

Each further domain is a release of its own, usually in its own cluster, with the console turned off
there and its API exposed for the shared console to reach (handbook 20 §20.2). Its lake is its own,
on any backend of §2.1. In this example Domain A, the console's, writes to Google Cloud Storage and
Domain B to Amazon S3:

```yaml
# domain-b-values.yaml, installed as its own release with its own namespace and Secrets (its storage
# Secret midstream-b-s3 made as §2.1 shows); its auth Secret holds the same federation-key as every
# other domain's, and the same protection-key when its masked columns should join theirs.
domain: {id: domain_b, name: Domain B}
console: {enabled: false}
storage:
  s3: {credentialsSecret: midstream-b-s3, region: eu-west-1}
controlPlane:
  settings: {defaultDestinationUri: "s3://example-lake-b"}
route:
  enabled: true
  hostnames: [midstream-b.example.com]          # this domain's API: /, /catalog, /tail/
  parentRefs: [{name: gateway, namespace: gateway-system}]
```

The release with the console then lists every domain, and a storage profile for each destination
prefix of every domain. The console's own domain needs only `local: true`, another domain its URLs and
a Secret here holding its `service-token`; each profile a read-only identity made as §2.1 shows for its
backend, in a Secret here:

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
  storage:
    - name: lake-a                              # Domain A's destination, gs://example-lake
      scope: gs://example-lake/
      kind: gcs
      credentialsSecret: midstream-console-gcs
    - name: lake-b                              # Domain B's, s3://example-lake-b
      scope: s3://example-lake-b/
      kind: s3
      region: eu-west-1
      credentialsSecret: domain-b-console-s3    # access-key-id and secret-access-key, read-only
route:
  enabled: true
  consoleHostnames: [midstream.example.com]
  redirectFrom: [midstream-a.example.com]       # old hostnames, sent on to /d/domain_a/
```

Every object goes to the profile whose scope matches it best, so one statement may join tables of
both domains. Domains that share a bucket each write under their own storage prefix, the release name,
and one profile scoped to the bucket serves them all. A materialization keeps the destination it was
created with: when a domain's destination changes, add a profile for the new one and keep the old one
while tables live under it (handbook 11 §11.1, 20 §20.11).

**Signing in with a provider.** Register the console with Microsoft Entra ID, GitHub, Google or any
OpenID Connect provider, with the redirect URI `<console.url>/api/auth/sso/<entra|github|google|oidc>/callback`;
give the console its client (`console.sso.<provider>`), and every domain the same client id
(`auth.sso.<provider>`), so each can check what the provider says itself. Each domain decides who
gets in: its first-sign-in policy (`auth.sso.firstSignIn`, `open` or `invited`), its allowed email
domains, and whether a password still works there (`auth.passwordSignIn`) — all adjustable later on
its Settings page. Handbook 20 §20.7 has each provider's registration, step by step.

## 8. Upgrade and remove

Every installation of this release is new: an installation of an earlier release is not upgraded to
it, and this one is installed as this guide describes, with Secrets and values of its own (handbook
14 §14.1). `helm upgrade` applies changed values to an installation, and `helm uninstall` removes it:

```bash
helm upgrade "$RELEASE" oci://ghcr.io/vmirz/charts/midstream --version <version> -n "$NS" -f midstream-values.yaml   # re-applies every runtime too
helm uninstall "$RELEASE" -n "$NS"                                          # the apps and their claims; runtimes and topics stay until each materialization is deleted
```

A key added to the auth Secret afterwards — `protection-key` on an installation made without one —
reaches the apps when they restart; runtimes are re-applied with it by the control plane (handbook 22
§22.8). With `auth.existingSecret`, patch that Secret. Generate the key once, never printed, and patch
it into every domain whose masked columns should join, with each domain's kubectl context, namespace
and release, before unsetting it:

```bash
PROTECTION_KEY="$(openssl rand -base64 48)"
kubectl -n "$NS" patch secret "$RELEASE-auth" --type merge \
  -p "{\"stringData\":{\"protection-key\":\"$PROTECTION_KEY\"}}"
unset PROTECTION_KEY
kubectl -n "$NS" rollout restart statefulset "$RELEASE-control-plane-app" "$RELEASE-tail-app" "$RELEASE-maintenance-app"
```

Delete every materialization before uninstalling, with the console or `DELETE /materializations/{id}`:
that removes its runtime, its Kafka topics and its tables. Nothing else is removed: the Secrets, the
bucket or container and the identities of §2.1 are yours, and so is an in-cluster SeaweedFS, which
goes with its namespace (`kubectl delete namespace seaweedfs`): its claim, and with it every
object, unless the storage class retains released volumes.

## Where next

- `docs/handbook/02-deployment-and-manifests.md`: every resource the chart deploys and why.
- `docs/handbook/11-iceberg-tables-and-storage.md`: the tables, their pointers and the object stores they live in.
- `docs/handbook/12-operations-runbook.md`: diagnosis by phase, sizing, disaster recovery.
- `docs/handbook/18-identity-and-access.md`: people, groups, roles, datasets, access tokens and single sign-on.
- `docs/handbook/20-domains-and-the-shared-console.md`: domains, the shared console and everything across them.
- `docs/handbook/21-kafka-access.md`: the Kafka user's ACLs on a cluster that enforces them, and what breaks without each.
- `console/docs/07-data-access-and-duckdb.md`: how the console reads tables through its storage profiles.
- `console/docs/13-operations-runbook.md`: running and sizing the console.
