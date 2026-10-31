# 12 — Operations Runbook

Day-to-day operation: deploy, verify, diagnose, repair.

## 12.0 Conventions used in this runbook

Assumed environment: namespace `midstream`, release `midstream`, so every name below starts with
`midstream-`; substitute your release name.

Set these once per shell:

```bash
NS=midstream
MAT=mat-a1b2c3d4e5f6                 # the materialization id
RT=midstream-$MAT                    # runtime app name: <release>-<materialization id>
CP=midstream-control-plane
TAIL=midstream-tail       # the tail app (19)
```

Every API call but `GET /system/version` and the guest-visible list needs a session. Sign in once
per shell (the account needs the permissions the calls require — [18](./18-identity-and-access.md)
§18.3):

```bash
TOKEN=$(curl -s -X POST localhost:6065/auth/login -H 'content-type: application/json' \
  -d '{"email":"you@example.com","password":"…"}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
AUTH="authorization: Bearer $TOKEN"
```

Runtime resource names are all derived from `$RT` (verified against
`runtime/naming.py::runtime_component_names`):

| Resource | Name |
|----------|------|
| KasprApp | `$RT` |
| KasprTable | `$RT-state` |
| KasprAgent (writer) | `$RT-writer` |
| KasprAgent (committer) | `$RT-committer` |
| KasprAgent (settings) | `$RT-settings` |
| KasprTask (status) | `$RT-emit-status` |
| KasprTask (pipeline) | `$RT-pipeline` |
| KasprTask (commit timer) | `$RT-commit-timer` |
| Control topic | `$RT-commits` (one partition) |
| Consumer group | `$RT-app` |

Each table — one per source topic, in its dataset — lives at the location its record names
(`GET /materializations/$MAT/tables/$TABLE` → `location`),
`<destination>/<storage prefix>/<materialization id>/<table>`. The detail lists them all:

```bash
curl -s localhost:6065/materializations/$MAT -H "$AUTH" | python3 -c 'import json,sys; [print(t["identifier"], t["source_topic"], t["location"]) for t in json.load(sys.stdin)["tables"]]'
TABLE=orders
LOC=$(curl -s localhost:6065/materializations/$MAT/tables/$TABLE -H "$AUTH" | python3 -c 'import json,sys; print(json.load(sys.stdin)["location"])')
```

**Pod label selectors are set by the Kaspr operator and are not defined in this repository.**
The commands below therefore resolve pods by name prefix, which always works:

```bash
cp_pod()  { kubectl get pods -n $NS -o name | grep -m1 "$CP"; }
rt_pods() { kubectl get pods -n $NS -o name | grep "$RT"; }
tail_pods() { kubectl get pods -n $NS -o name | grep "$TAIL"; }

kubectl logs -n $NS $(cp_pod) --tail=100
for p in $(rt_pods); do kubectl logs -n $NS $p --tail=100; done
```

If your cluster does expose a stable label (check with
`kubectl get pods -n $NS --show-labels`), `-l <label>` is more convenient — the CRs themselves
carry `kaspr.io/app: $RT`.

The lake is read here the way Midstream reads it, whatever its backend: through the object store of
the URI's scheme (`lakehouse/store/`, §12.12a), inside a pod and with that pod's own storage
credentials — the control plane's unless another pod is named:

```bash
lake_cat() { kubectl exec -n $NS ${2:-$(cp_pod)} -c kaspr -- python -c 'import sys; from midstream.lakehouse.store import store_for; u = sys.argv[1]; sys.stdout.buffer.write(store_for(u).get(u)[0])' "$1"; }
lake_ls()  { kubectl exec -n $NS ${2:-$(cp_pod)} -c kaspr -- python -c 'import sys; from midstream.lakehouse.store import store_for; p = sys.argv[1]; [print(o.uri, o.size, o.last_modified) for o in store_for(p).list(p)]' "$1"; }

lake_cat "$LOC/metadata/catalog-pointer.json"           # an object, as the control plane reads it
lake_ls "$LOC/metadata/" $(rt_pods | head -1)           # a prefix, as a runtime member lists it
```

## 12.1 Deploy

```bash
# render and inspect before applying
helm template midstream charts/midstream -n midstream -f my-values.yaml | less

# apply
helm upgrade --install midstream charts/midstream -n midstream -f my-values.yaml

# watch the control plane come up
kubectl get kasprapp -n $NS -w
kubectl get pods -n $NS -o wide -w | grep $CP
```

### No upgrade from an earlier release

Every installation of this release is new, with `storage.*` and `console.storage` written for it:
nothing converts or adopts the values, tables or records an earlier installation left (14 §14.1,
`docs/install.md` §8). `helm upgrade` applies changed values to an installation of this release. The
four upgrades that follow are the development line's, before 1.0.0 (16 §16.4–§16.12), kept as a
record of what each changed.

### Upgrading to 1.0

A fresh start of the materialization plane that keeps every account and setting: §12.13.

### Upgrading to 2.0: the maintenance app

2.0 adds a KasprApp, `<release>-maintenance` (`charts/midstream/templates/maintenance/`): its own pod, its own
consumer group, sized for compaction (1 GiB request, 3 GiB limit, 2 CPUs). The control plane only
*sends* to its new topic, `<release>-maintenance-requests`, and consumes nothing new, so this
deploy needs no rebalance of the control plane. After it:

- the control plane re-applies every runtime (its package pin changed), which deletes each runtime's
  `<rt>-maintenance-timer` task; until a runtime is re-applied, its committer and the app may both
  maintain its tables, which the pointer's compare-and-swap keeps safe;
- within a minute the app logs `maintenance_snapshot_expiry_completed` for each table, and
  `GET …/tables/{table}` shows maintenance from `runtime_app_ref` = the app's name;
- runtime pods may be resized down: they no longer run compaction (§12.10).

The chart labels the maintenance app's agent and task as its own
(`test_the_maintenance_app_owns_its_agent_and_task`).

### Upgrading to 4.1: the live tail

4.1 adds a KasprApp, `<release>-tail` (`charts/midstream/templates/tail/`, 02 §2.10b): two members, each 1 CPU
and 2 GiB requested, 2 CPU and 4 GiB at most, in their own consumer group. The control plane only
*sends* to its new topic, `<release>-tail-requests`, so this deploy needs no rebalance of the
control plane. Before it:

- Kaspr 0.11.30 and kaspr-operator 0.19.7 or later, for the tail's views to answer refusals as JSON
  (`errorContentType`); older ones send the same body as bytes, which the console reads too;
- the auth Secret's `service-token`, which the tail app checks the console's requests against (it
  is the console's service token for the domain already).

After it, the console reads each domain's tail at its registry entry's `tailUrl` — the tail app's
Service, `http://$TAIL-app-api:6065`, which the chart sets for its own domain, or the tail's route
for a domain in another cluster (20 §20.2). Every materialization has a live tail, those created
before it included.

### Upgrading to 4.4: autoscaling and restart health

4.4 needs the control plane's role to `get` and `list` **pods** (02 §2.7), which every observation now
reads for its runtime's restarts: upgrade the release before the control plane restarts.
Without the rule the observations go on without the pods, the control plane logs it once, and a
runtime whose members keep restarting is not shown as such.

**Autoscaling is on**, for every materialization, those created before it included: a runtime more
than 100,000 records behind that would take longer than 30 minutes to catch up gets up to 16 members
until it has (07 §7.12a). Check that the cluster has room for that many members of the runtimes most
likely to fall behind — each member requests what the runtime spec says, its CPU then set by the
VPA — or lower *Autoscaling maximum replicas*, or turn *Autoscale runtimes* off, before the upgrade
(Settings › Materializations, or `CONTROL_PLANE_RUNTIME_AUTOSCAL…` in the manifest).

### Adding the protection key

The default protection masks emails, phone numbers and national identifiers with a keyed hash, and
hashing needs `protection-key` in the auth Secret, which `docs/install.md` §2 creates with it. An
installation made without one adds it, generated here and never printed (22 §22.9):

```bash
kubectl -n $NS patch secret midstream-auth --type merge \
  -p "{\"stringData\":{\"protection-key\":\"$(openssl rand -base64 48)\"}}"
```

It reaches the apps once they restart
(`kubectl -n $NS rollout restart statefulset $CP-app $TAIL-app midstream-maintenance-app`);
runtimes need nothing, as the control plane renders the key's id into their specs and the sweep
re-applies each once. Until then existing materializations run on unchanged, and a create that takes
the default is refused with `409 protection_key_not_configured`. Domains whose masked columns should
join share one key: generate it once and patch the same value into each (22 §22.9) - the command
above, run in each domain, would give each its own. Their `GET /system/version` then reports the
same `protection_key_id`.

### A later release that adds topics

The control plane uses static group membership, so the restart that deploys a release does not
rebalance its consumer group, and topics the release adds are never assigned. 0.5.0 added the
settings topics, 0.6.0 the identity topics, 1.0 `materialization-table-events`. After deploying such
a release, force one rebalance per environment by stopping the control plane for longer than the
broker session timeout:

```bash
kubectl patch kasprapp $CP -n $NS --type merge -p '{"spec":{"replicas":0}}'
# wait until the pod is gone, then longer than K_BROKER_SESSION_TIMEOUT (120 s by default)
kubectl patch kasprapp $CP -n $NS --type merge -p '{"spec":{"replicas":1}}'
```

Until then every save that sends to a new topic answers `500` and the new agent consumes nothing —
for 1.0, no table event reaches the control plane, so table records never advance. Why, and how to
confirm it: [17 — Settings](./17-settings.md) §17.12.

### Prerequisites (must exist before apply)

| Secret | Used for |
|--------|----------|
| `storage.gcs.credentialsSecret`, `storage.s3.credentialsSecret` or `storage.azure.credentialsSecret`, for each backend the destinations use | Read, write, delete and list on the destination, for the control plane, the maintenance and tail apps and every runtime: tables, their pointers, maintenance, drops. Keys: `service-account.json` (GCS); `access-key-id`, `secret-access-key` and optionally `session-token` (S3); `connection-string`, or `account-key`, or `client-id`, `tenant-id` and `client-secret` (Azure). Empty for the pods' own cloud identity (§12.12a, 02 §2.9a) |
| each `console.storage[].credentialsSecret` | The console's read-only access to the lake, one per storage profile (20 §20.11). Keys as above, and for a GCS profile `hmac-key-id` and `hmac-secret` for DuckDB's scans |
| the Secret `kaspr.authentication.passwordSecret` names (`<release>-kafka`) | the Kafka user's password |
| `<release>-auth` (`midstream-auth` in dev) | Session signing key, the console's service token, the first administrator. Optional to start, but without `signing-key` nobody can sign in |

The Kaspr operator must be installed and the `kaspr.io` CRDs registered. On a Kafka cluster that
enforces ACLs, the Kafka user needs the grants of [21 §21.3](./21-kafka-access.md) first: without
Create on the release's topics no app starts, even when the topics exist.

Creating the auth Secret (values never echoed; keep a copy of the bootstrap password somewhere
safe; a shared console in another release reads the `service-token` as
`CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`, through the Secret its `console.domains[]` entry names, and
every domain's Secret holds the same `federation-key` — make it once, then copy it into each):

```bash
FEDERATION_KEY="$(openssl rand -base64 48)"    # once, for every domain
kubectl create secret generic midstream-auth -n $NS \
  --from-literal=signing-key="$(openssl rand -base64 48)" \
  --from-literal=service-token="$(openssl rand -base64 48)" \
  --from-literal=federation-key="$FEDERATION_KEY" \
  --from-literal=bootstrap-admin-email=you@example.com \
  --from-literal=bootstrap-admin-password="$(openssl rand -base64 18)"
```

### Post-deploy verification

```bash
kubectl get kasprapp,kaspragent,kasprtask,kasprtable -n $NS
kubectl logs -n $NS $(cp_pod) --tail=100

kubectl port-forward -n $NS svc/$CP-app-api 6065:6065 &
curl -s localhost:6065/materializations | python3 -m json.tool
```

Expect `{"items": [], "limit": "30", "returned": 0}` on a fresh install. Note `limit` is
returned as a **string**.

Check the object store before the first materialization: the conformance suite, from the control
plane's pod, under a scratch prefix of the destination it then empties again (§12.12a). Every check
must pass:

```bash
kubectl exec -n $NS $(cp_pod) -c kaspr -- python -m midstream.lakehouse.store.check s3://example-lake/_check
```

Check sign-in end to end. On a fresh install the log says the bootstrap administrator was made
(`No active administrator existed; … is now one`); sign in as them, which answers with
`"password_change_required": true` until they choose a password in the console:

```bash
kubectl logs -n $NS $(cp_pod) | grep -i 'administrator'
curl -s -X POST localhost:6065/auth/login -H 'content-type: application/json' \
     -d '{"email":"you@example.com","password":"<bootstrap password>"}' | python3 -m json.tool
```

Check the settings path end to end too. A change that goes unapplied is the sign of the
unassigned topics above:

```bash
curl -s localhost:6065/settings/control-plane -H "$AUTH" | python3 -c 'import json,sys; print(json.load(sys.stdin)["revision"])'
curl -s -X PATCH localhost:6065/settings/control-plane -H 'content-type: application/json' \
     -H "$AUTH" -d '{"values":{"topic_analysis_max_topics":21}}'
curl -s 'localhost:6065/settings/control-plane/history?limit=1' -H "$AUTH" | python3 -m json.tool   # the change, within a second
curl -s -X PATCH localhost:6065/settings/control-plane -H 'content-type: application/json' \
     -H "$AUTH" -d '{"reset":["topic_analysis_max_topics"]}'
```

Check the tail app: both members in its group, each with its share of the 64 tick partitions.
The views need the console's credential and a bearer, both the service token (never echoed):

```bash
SERVICE_TOKEN=$(kubectl get secret -n $NS midstream-auth -o jsonpath='{.data.service-token}' | base64 -d)
kubectl port-forward -n $NS svc/$TAIL-app-api 6066:6065 &
curl -s localhost:6066/tail/v1/members -H "X-Console-Token: $SERVICE_TOKEN" \
     -H "authorization: Bearer $SERVICE_TOKEN" | python3 -m json.tool      # state Stable, two members, 32 partitions each
```

## 12.2 Create and watch a materialization

A table goes in a dataset; make one first if none fits (you become its owner):

```bash
curl -s -X POST localhost:6065/datasets -H 'Content-Type: application/json' -H "$AUTH" \
  -d '{"name":"sales","description":"Orders and refunds"}'
curl -s -X POST localhost:6065/materializations \
  -H 'Content-Type: application/json' \
  -H "$AUTH" \
  -d '{"name":"Shop","tables":[{"source_topic":"orders","dataset":"sales"},{"source_topic":"payments","dataset":"sales"}],"partition_granularity":"day"}'   # destination: the configured default
```

Returns `202` with:

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "request_id": "req-9f8e7d6c5b4a",
  "desired_phase": "running",
  "observed_phase": "requested",
  "phase_reason": "claiming_tables",
  "status_url": "/materializations/mat-a1b2c3d4e5f6",
  "accepted_at": "2026-01-15T10:30:00Z",
  "tables": [ … ],
  "claim_request_id": "idr-3e8f0a1b2c4d",
  "claim_status_url": "/identity/requests/idr-3e8f0a1b2c4d"
}
```

`GET $claim_status_url` says `applied` once the tables' names are the materialization's. A refused
claim — the name was taken meanwhile, the dataset deleted, write access taken away — marks the
materialization `deleted` with the reason in `phase_reason` and `phase_message` (07 §7.2a): fix the
name and create it again.

```bash
MAT=mat-a1b2c3d4e5f6
watch -n 5 "curl -s localhost:6065/materializations/$MAT -H '$AUTH' | python3 -m json.tool | head -30"
```

Expected phase progression (~60–180 s):

```
requested   / claiming_tables
discovering / discovering
discovered  / schema_discovered
discovered  / table_created
deploying   / runtime_apply_in_progress
deploying   / waiting_for_runtime_agents, waiting_for_runtime_ready, waiting_for_table
running     / processing
```

## 12.3 Diagnosis by phase

Always start here:

```bash
curl -s localhost:6065/materializations/$MAT -H "$AUTH" | python3 -m json.tool
```

### `requested` / `requested` — stuck

Request intake did not run.

```bash
kubectl logs -n $NS $(cp_pod) | grep request_intake
kubectl get kaspragent $CP-request-intake -n $NS -o yaml
```

Causes: the `request-intake` agent is not running; the requests topic does not exist; the
control-plane pod is crash-looping.

### `discovering` / `discovering` — stuck

Discovery cannot stay here for long: connecting is bounded at 60 s and the whole read at 75 s
(07 §7.2), after which the materialization reads `failed` / `start_discovery_failed`, with
`last_error` naming what timed out. A materialization that stays `discovering` for minutes means the
`workflow-apply` agent is not running, or has not reached the event yet.

```bash
kubectl logs -n $NS $(cp_pod) | grep -E 'schema_discovery|start_discovery'
```

`connecting to Kafka to sample topic … timed out after 60s` is a connection or authentication
failure: check `K_KAFKA_*` and the ACLs. An empty topic is not a failure: schema 0 then holds the
metadata columns only, and the runtime adds every field as data arrives (09 §9.7). The completion
event's `sampled_records` says how many records schema 0 was built from. A missing ACL times
nothing out: a topic the Kafka user may not describe is not listed, which the API refuses before
the create is accepted (`400`, `reason: source_topic_missing`), and discovery too if it gets that
far (`failed` / `source_topic_missing`); one it may describe but not read fails the step with
`TopicAuthorizationFailedError` ([21 §21.5](./21-kafka-access.md)).

### `discovered` / `schema_discovered` or `table_created` — stuck

`create_table` or the apply after it has not run: the `workflow-apply` agent is down or behind.
`failed` / `table_location_in_use` means another materialization's pointer is at the location. A
location is under the materialization's own id, so this takes a pointer written there by hand, or a
restore gone wrong; look at the pointer's `materialization_id`.

### `requested` / `claiming_tables` — stuck

The tables' names were never claimed or the answer never arrived. `GET /identity/requests/
{claim_request_id}`: `pending` means `identity-intake` has not applied it (is the agent up? did a
chart's `request-intake` template name its second output `<release>-identity-requests`?); `applied` means its answer did
not reach `workflow-apply` (is `identity-intake`'s output the work topic?). The answer is
re-sent if the claim is redelivered; the hourly sweep does not resend it.

### `deploying` / `runtime_apply_in_progress` — stuck

Resources were applied but never observed.

```bash
kubectl get kasprapp $RT -n $NS -o yaml
kubectl describe kasprapp $RT -n $NS
rt_pods
```

Common causes: image pull failure, insufficient cluster resources, missing secret, RBAC denial.
Note the control-plane role grants only `get/create/patch/delete` on
`kasprapps/kasprtables/kaspragents/kasprtasks` — no `list`/`watch`.

### `deploying` / `waiting_for_runtime_ready` — stuck

The KasprApp is Ready but no healthy heartbeat has arrived.

```bash
for p in $(rt_pods); do kubectl logs -n $NS $p --tail=200; done
kubectl get kasprtask $RT-emit-status -n $NS -o yaml
```

The members are not heartbeating: the `emit-status` task is not running, or the members cannot
reach Kafka.

### `deploying` / `waiting_for_table`

The members heartbeat but have not loaded their table: they read its pointer at startup and keep
trying.

```bash
lake_cat "$LOC/metadata/catalog-pointer.json"                        # as the control plane reads it
lake_cat "$LOC/metadata/catalog-pointer.json" $(rt_pods | head -1)   # as a member reads it
for p in $(rt_pods); do kubectl logs -n $NS $p; done | grep -iE 'load_table|pointer|table'
```

An unreadable pointer is a credential or permission problem — the error names it, in each backend's
words (§12.12a) — or a dropped or lost pointer: a dropped table has `catalog-pointer.dropped.json`
instead; a lost pointer can be rebuilt (§12.11). When the control plane reads it and a member cannot,
the runtime's pods lack what the control plane's have. With a cloud identity that trusts
ServiceAccounts one by one — AKS workload identity, EKS Pod Identity — that is the runtime's own
ServiceAccount, `$RT-app`, new with the materialization: give it its federated credential or
association (§12.12a), then restart the runtime's pods so that each starts with the identity
(`kubectl -n $NS rollout restart statefulset $RT-app`).

### `degraded` / `resource_ready_but_runtime_silent`

The 480 s observation budget expired with a Ready app and no heartbeat.

```bash
for p in $(rt_pods); do kubectl logs -n $NS $p --tail=300; done
kubectl get kasprtask $RT-emit-status -n $NS -o yaml
```

Either the `emit-status` task is not running, or the runtime container is up but the agents
never started.

### `degraded` / `commits_stalled`

A member's uploaded files have waited ten commit intervals (5 minutes at the defaults) for a commit:
the committer is not committing.

```bash
curl -s localhost:6065/materializations/$MAT/tables/$TABLE -H "$AUTH" | python3 -c "
import json,sys; t=json.load(sys.stdin); live=t.get('live') or {}
print(live.get('member_id'), t.get('last_commit_at'), live.get('pending_files'), t.get('last_error'))"
kubectl logs -n $NS <the committer pod> | grep -E 'runtime_commit_(started|completed|failed)'
```

Every table commits on its own: one table's stalled commits do not stall the others, so check the
table the status message names (`Table orders: …`).

`live.member_id` names the committer member. Look for `runtime_commit_failed` (and a
`commit_failed` table error with its message), a member stuck in its startup, or a crash loop. Right
after disruptions — repeated committer restarts — the reason can linger up to ten minutes after
commits resume (08 §8.6); the table record's `last_commit_at` is the live answer.

**The committer's log is quiet.** Sometimes nothing above shows: no `runtime_commit_failed`, no
error, and `live.pending_files` is 0, while the member that reports the stall keeps logging
`runtime_file_announced` and its `held_guards` / `unacknowledged_batches` grow (step 4 of §12.4). The
committer is then ignoring that member's announcements as stale (10 §10.6). It logs a warning for
them, at most once a minute per member and partition, and counts them in `live.stale_announcements`:

```bash
kubectl logs -n $NS <the committer pod> | grep 'from an older session' | tail -5
curl -s localhost:6065/materializations/$MAT/tables/$TABLE -H "$AUTH" | python3 -c "
import json,sys; live=json.load(sys.stdin).get('live') or {}
print(live.get('pending_files'), live.get('stale_announcements'))"
```

A runtime that predates session epochs did this, without a word, after a member restarted while
the committer ran on (10 §10.5). There is no warning and no `stale_announcements` on such a runtime.
Instead, the `<runtime>-commits` topic shows the member's `session_generation` falling after its
restart. Read the topic with a consumer whose `group_id` is `None`, so that it neither joins the group
nor commits. To unstick it, restart the **committer member** (`live.member_id`). The new committer
starts remembering nothing and hears every pending file again within `announce_retry_seconds`.
Restarting the stalled member does not help. On a runtime with session epochs, a stale announcement
is a member's own late message from a session it has already ended, and holds nothing back. If they
keep coming, report it with the warning lines.

### `degraded` / a table error: `commit_failed`, `schema_evolution_failed`, `schema_field_limit`, `compaction_failed`, `table_unavailable`

The committer published a `table_error` (06 §6.6); the status message names the table, and
`GET /materializations/$MAT/tables/$TABLE` → `last_error` has the message. The table's next
successful commit clears it, unless another table still has an error (08 §8.5).

| Reason | What to do |
|---|---|
| `commit_failed` | the committer's log names the object store's error (§12.12a) or the lost compare-and-swap; an engine committing through the REST facade in a tight loop must back off. Failing again and again on an S3-compatible server: run the conformance suite against it (§12.12a) |
| `schema_evolution_failed` | 09 §9.12 |
| `schema_field_limit` | the schema is at `max_schema_fields`; new fields' values go to `_unmapped`. Raise the limit (Settings → Schema) if they matter |
| `compaction_failed` | a partition failed compaction five times and is paused; the maintenance history has the error, the maintenance app's log `maintenance_compaction_failed`. Fix the cause, then **Run now** on the Table tab, or `POST …/maintenance {"job":"compaction"}`, to retry |
| `drop_failed` | a table being dropped could not be, after its retries (07 §7.9a); it stays `dropping`. Fix the cause (the store's delete permission, say), then resume the materialization: the apply drops it again |
| `table_unavailable` | the committer could not load the table: as `waiting_for_table` above |
| `protection_commit_failed` | the committer could not record a delivered protection policy in the table; it tries again on every event. Its member's log has `Recording protection revision …`; the writers keep the policy the table records meanwhile (22 §22.13) |
| `reprotect_failed` | a reprotect failed `compaction_max_attempts` times and was given up; the table's maintenance history has each attempt's error, the maintenance app's log `maintenance_reprotect_failed`. Fix the cause — most often the maintenance app's protection key — then ask again (*Mask rows already written*, or `POST …/maintenance {"job":"reprotect"}`); the next commit clears the error from the status meanwhile |

### `degraded` / `runtime_reported_degraded`, health reason `protection_key_missing` or `protection_key_mismatch`

A table masks values with a keyed hash and the member has no protection key, or another than the
control plane's: it writes nothing for that table, holding its records unacknowledged, and checks
again every 15 s. Missing: add `protection-key` to the auth Secret and restart the control plane — it
renders the key's id into the runtime's spec, and the sweep re-applies the runtime with it within two
minutes. Mismatch: the runtime was applied before the key changed, or the apps read different
Secrets; wait for the sweep, then check that every app reads the same Secret (22 §22.8).

### `degraded` / `runtime_data_validation_degraded`

Records a member could not write: a value that is not JSON, for example.

```bash
curl -s localhost:6065/materializations/$MAT -H "$AUTH" | python3 -c "
import json,sys; s=json.load(sys.stdin)['status']
print(s['runtime_health']); print(s.get('runtime_progress', {}).get('skip_reasons'))"
```

`records_skipped` climbing confirms it. Each member counts skipped records by reason, with the
partition, offset and schema id of the latest one, and the control plane folds them into
`runtime_progress.skip_reasons` (cumulative, most frequent first, bounded to 20 reasons); the
console shows them when the skipped count on the Overview tab is clicked. The raw latest malformed
record stays in the member's memory only; use the partition and offset to find it on the source
topic. A member clears the error once it writes again, so the reason is current.

Values the schema cannot hold are **not** skipped: they go to `_unmapped` (09 §9.8), counted in
`unmapped_values`.

`phase_reason` / `phase_message` describe **every member that heartbeated in the last 120
seconds**, not whichever member reported last: the worst state wins the reason, and the message
counts members per state. Per-member state is in `runtime_progress.member_progress`, and the
console's Overview lists it, marking the committer.

### `degraded` / `mixed_agent_states`

Some managed agents are running and some paused (or unknown) after the retry budget.

```bash
kubectl get kaspragent -n $NS | grep $MAT
```

Usually a partial apply, or an observation made while a member restarted. Trigger a re-apply with
`POST .../resume`, or wait for the sweep (08 §8.6).

### `degraded` / `runtime_members_restarting`

Members of the runtime keep restarting (08 §8.3, row 3d); the message says how many and why — *3 of
10 members restarted in the last 30 minutes: out of memory.* It clears on its own 30 minutes after
the last restart; a heartbeat cannot clear it sooner.

```bash
kubectl get pods -n $NS -l kaspr.io/app=$RT \
  -o custom-columns='POD:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount,LAST:.status.containerStatuses[0].lastState.terminated.reason,AT:.status.containerStatuses[0].lastState.terminated.finishedAt'
kubectl logs -n $NS <pod> -c kaspr --previous --tail=200
```

- **out of memory** (`OOMKilled`): a member passed its memory limit. A seal holds one row group
  whatever its batch since 4.4 (10 §10.4); before, a backfill's seals held one per partition value
  and needed up to 2 GiB. Raise the runtime spec's memory (§12.10), or lower
  `target_row_group_size_bytes` (Settings › Runtimes).
- **exited with code N**, or **crash-looping**: the previous container's log says why.

Each restart loses the member's uncommitted work, which it reads and writes again: nothing is lost
or written twice, but the runtime falls behind while it repeats. Autoscaling adds no members to it
meanwhile (07 §7.12a).

### `failed` / `<step>_failed`

A workflow step threw a non-`ValueError`.

```bash
curl -s localhost:6065/materializations/$MAT -H "$AUTH" | python3 -c "
import json,sys; print(json.load(sys.stdin)['status']['phase_message'])"
```

`failed` / `source_topic_missing` is a create whose topic Kafka no longer lists: it was deleted,
or the Kafka user lost Describe on it, after the API asked about it. Nothing was created; delete it,
and create it again once the topic can be read. `failed` / `table_missing` is a materialization
with no table at all — created before 1.0, or a create whose discovery failed and was then
resumed — and is deleted and created again (§12.13). `failed` / `discover_tables_failed` is a
change of topics whose new tables could not be sampled: resuming the materialization samples and
creates them, then applies the change. `failed` / `create_table_failed` is a create whose tables
could not be written: its message carries the object store's error — credentials missing or refused,
a permission, a missing bucket or container (§12.12a). Fix that, then resume: the tables still
missing are created and the runtime applied. `failed` / `invalid_destination` is a materialization
recorded with a destination that cannot hold a table; it is deleted and created again under a
destination of 04 §4.5.2.

**Nothing retries a `failed` status automatically.** Recover with:

```bash
curl -s -X POST localhost:6065/materializations/$MAT/resume -H "$AUTH"
```

### `deploying` / `changing_topics`

The runtime is held at zero members until its consumer group empties, after a change of topics
(07 §7.9a). The message says which group and its state; it empties a session timeout (120 s) after
the last member stopped. Expect about two minutes here, then `runtime_apply_in_progress` and a pod
start. Longer means a member is still alive:

```bash
kubectl get pods -n $NS | grep $RT
kubectl -n kafka exec <broker> -- bin/kafka-consumer-groups.sh --bootstrap-server … --describe --group $RT-app --members
```

### `failed` / `consumer_group_not_empty`

The group kept members for 15 minutes of checks after the runtime was held at zero. Find and stop
what still runs in the group (a pod that did not terminate, another app with the same group), then
resume: the runtime is drained again and the change goes on.

### `deleting` / `waiting_for_consumer_group_empty`

Normal for up to ~480 s after a delete — the pods are still leaving the consumer group.

```bash
kafka-consumer-groups --bootstrap-server $BROKERS --describe --group $RT-app
```

If it persists past 480 s the step raises a terminal `RuntimeError`. Check for stray pods still
consuming.

### `deleted` / `runtime_and_kafka_resources_deleted` — still listed

The runtime and its Kafka resources are gone and the table is being dropped, a pass at a time
(07 §7.10); the record goes when the last pass has run. Each pass logs
`control_plane_drop_table_progress` with the count so far. A warning `destination cleanup pass left
objects behind: could not delete …` names objects the store refused or kept failing to delete; the
next pass tries them again. `..._retry_scheduled` means a pass failed and will run again after a
backoff; five in a row end as `failed` / `drop_table_failed`, whose message carries the store's error
(§12.12a).

### `running` / `waiting_for_source_records`

Healthy. The source topic simply has no new records. Not an error.

Unless the topic does have records: on a cluster that enforces ACLs, a Kafka user that lost Read
on the topic after it was checked (the API refuses a topic it may not describe) gives a runtime
that runs without it, assigned none of its partitions, and the materialization's
`consumer_lag.error` reads `<topic>: topic does not exist`. Granting Read is not enough on its own;
stop the materialization past the session timeout and resume it ([21 §21.7](./21-kafka-access.md)).

## 12.4 Data not appearing in the table

Ordered checks:

```bash
# 1. Are members writing, and is the committer committing to this table?
curl -s localhost:6065/materializations/$MAT/tables/$TABLE -H "$AUTH" | python3 -c "
import json,sys; t=json.load(sys.stdin); print(t['totals'], t['last_commit_at'], t['valid_through'], t.get('live'))"

# 2. Seal, upload and announce failures, backpressure?
for p in $(rt_pods); do kubectl logs -n $NS $p; done \
  | grep -E 'runtime_batch_(seal|upload)_failed|runtime_source_paused_for_upload_backlog|runtime_seal_waiting_for_schema'

# 3. The committer's decisions and commits
kubectl logs -n $NS <the committer pod> | grep -E 'runtime_commit_(started|completed|failed)|runtime_file_decision' | tail -20

# 4. What is each member holding back? (protocol logging on)
for p in $(rt_pods); do kubectl logs -n $NS $p | grep runtime_processing_performance_window | tail -1; done \
  | grep -oE '"(acknowledgements|pipeline)":\{[^}]*\}'

# 5. The table's files
lake_ls "$LOC/data/" | tail
```

| Finding | Meaning |
|---------|---------|
| `rows_written > 0`, `files_written == 0` | batches never sealed — below every roll threshold, or seals failing (`runtime_batch_seal_failed`). A new materialization's first file per partition lands at 5,000 rows |
| files uploaded, `files_pending_commit` grows, no `runtime_commit_completed` | the committer is not committing: `commits_stalled` (§12.3) |
| `runtime_file_decision … deferred` | a file whose partition has an earlier batch the table lacks: it waits for that batch's re-announcement (10 §10.6) |
| `runtime_seal_waiting_for_schema` | a batch waits up to `seal_schema_wait_seconds` for a schema change to commit |
| `runtime_source_paused_for_upload_backlog` | uploads or seals fall behind; consumption resumes when the backlog drains |

Expected latency with defaults, record to table: the batch's age (`max_open_file_age_seconds`,
120 s, once it holds 10,000 rows or 8 MiB; the 600 s ceiling on a quiet topic), then the seal and
upload, then the next commit (`commit_interval_seconds`, 30 s). The Table tab's **Complete through**
(`valid_through`) says up to when every partition's records are in.

## 12.5 Querying the table

Anything that reads Iceberg reads it; 11 §11.9 has the snippets. By path, DuckDB, with a secret for
the backend the location names:

```sql
INSTALL iceberg; LOAD iceberg;
-- one of:
CREATE SECRET lake (TYPE GCS, KEY_ID '<HMAC key id>', SECRET '<HMAC secret>');                            -- gs://
CREATE SECRET lake (TYPE S3, KEY_ID '<access key id>', SECRET '<secret access key>', REGION 'us-east-1'); -- s3://
CREATE SECRET lake (TYPE AZURE, CONNECTION_STRING '<connection string>');  -- abfss://, after INSTALL azure; LOAD azure;
SELECT count(*) FROM iceberg_scan('<location>');
```

An S3-compatible server's secret adds `ENDPOINT '<host>:<port>'` (without the scheme),
`URL_STYLE 'path'` for path-style addressing and, over plain HTTP, `USE_SSL false`. The Table tab's
**Connect** panel prints these for the table's backend, filled with the console's own profile for it:
the endpoint, region and path style, never a credential.

Through the REST catalog, with an access token (`catalog.read` on the table's dataset, §12.7):
DuckDB's `ATTACH 'warehouse' AS lake (TYPE ICEBERG, …, ENDPOINT '<control plane>/catalog')`, then
`lake.<dataset>.<table>`, or PyIceberg's `RestCatalog`. The console's Data tab and its Table tab's
**Connect** panel are the quickest way.

Useful queries:

```sql
-- per-partition coverage: rows and offsets the table holds
SELECT _kafka_partition, count(*), min(_kafka_offset), max(_kafka_offset)
FROM iceberg_scan('<location>') GROUP BY 1 ORDER BY 1;

-- no duplicates, ever: this returns nothing
SELECT _kafka_partition, _kafka_offset, count(*) c
FROM iceberg_scan('<location>') GROUP BY 1, 2 HAVING c > 1;

-- what the snapshots committed
SELECT snapshot_id, timestamp_ms, operation, summary['materializer.records'] AS records
FROM iceberg_snapshots('<location>') ORDER BY timestamp_ms DESC LIMIT 20;

-- ingest lag
SELECT max(_materialized_at - _kafka_timestamp) FROM iceberg_scan('<location>');
```

The 0.x `reader/main.py` helper is gone: there is nothing it did that `iceberg_scan` does not.

## 12.6 Protocol logging

Off by default in code; the chart turns it on for the control plane and for runtimes
(`PROTOCOL_LOGGING_ENABLED`). Switch it per component on the console's Settings page,
with no restart: **Diagnostics → Protocol logging** for the control plane, and **Runtimes →
Protocol logging** for every runtime. The runtimes' slow-wait interval is beside it.

Changing the deployment value instead means the control-plane app's env, or for runtimes the
rendered spec (`CONTROL_PLANE_RUNTIME_APP_SPEC`), which re-applies every runtime.

Output is one structured line per event. Useful filters:

```bash
kubectl logs -n $NS $(cp_pod) | grep control_plane_work_completed
kubectl logs -n $NS $(cp_pod) | grep control_plane_work_failed
kubectl logs -n $NS $(cp_pod) | grep -E 'control_plane_(create_table|apply_runtime|drop_table)'
kubectl logs -n $NS $(cp_pod) | grep table_event_applied
kubectl logs -n $NS $(cp_pod) | grep control_plane_consumer_lag_refresh_completed
kubectl logs -n $NS $(cp_pod) | grep -E 'control_plane_(retention_purge_requested|purge_materialization_records_completed)'

for p in $(rt_pods); do kubectl logs -n $NS $p; done | grep -E 'runtime_commit_(started|completed|failed)'
for p in $(rt_pods); do kubectl logs -n $NS $p; done | grep -E 'runtime_schema_(change_requested|committed|commit_failed)'
for p in $(rt_pods); do kubectl logs -n $NS $p; done | grep -E 'runtime_(compaction|snapshot_expiry|orphan_cleanup)_'
for p in $(rt_pods); do kubectl logs -n $NS $p; done | grep runtime_processing_performance_window
```

Full event catalogue: [03 §3.7](./03-configuration-reference.md).

Only the committer member logs `runtime_commit_*` and `runtime_schema_committed`; the heartbeat's
`table.member_id` names it. The maintenance events, `maintenance_*`, are the maintenance app's, in
its own pod's log.

`runtime_settings_applied` marks a runtime member storing a new runtime-settings revision:

```bash
for p in $(rt_pods); do kubectl logs -n $NS $p; done | grep runtime_settings_applied
```

## 12.7 Common operations

### Stop / resume

```bash
curl -X POST localhost:6065/materializations/$MAT/stop   -H "$AUTH"
curl -X POST localhost:6065/materializations/$MAT/resume -H "$AUTH"
kubectl get kasprapp $RT -n $NS -o jsonpath='{.spec.replicas} {.status.availableMembers}{"\n"}'
```

Stop scales the runtime's KasprApp to zero replicas; it deletes nothing, and the materialization
keeps its `replicas` for the resume. Expect `stopped` / `runtime_scaled_to_zero` (members `0/0`)
within a minute or two, and `running` again a few minutes after resume, depending on the replica
count. Rows staged on the pods and not yet in the table are left behind, and read again from Kafka
on resume, since their offsets were never committed (10 §10.7); the committer refuses what the table
already holds.

| Phase while stopping | Meaning |
|---|---|
| `deploying` / `resource_generation_not_observed` | The operator has not reconciled the new spec yet |
| `deploying` / `waiting_for_runtime_scale_down` | Members are still shutting down |
| `degraded` / `runtime_scale_down_stalled` | Members still up after the 480 s observation budget; check the StatefulSet and its pods |

### Change the replica count

```bash
curl -X PATCH localhost:6065/materializations/$MAT -H 'Content-Type: application/json' \
     -H "$AUTH" -d '{"replicas": 8}'
```

Re-applies the runtime at the new count and rebalances the consumer group. On a stopped
materialization it only records the count, which the next resume uses. While autoscaling has the
runtime scaled up, a count at or above the scaled one ends the scaling; a lower one takes effect
once the runtime has caught up.

### Autoscaling

A runtime more than *Lag worth scaling for* (100,000) records behind, that would take longer than
the *Catch-up target* (30 minutes) at its pace, gets the members that pace says it needs — at most
*Autoscaling maximum replicas* (16), and no more than its topics' partitions keep busy — and goes
back to its own replica count after three measurements under the floor (07 §7.12a). Only a runtime
running and healthy is scaled up, and changes are at least *Time between scaling steps* (5 minutes)
apart. The materialization's detail says how it is scaled and why (`autoscale`, 04 §4.6); the
console's Overview shows *Scaled up to N members* with the reason.

```bash
# Off for one materialization: a scaled runtime goes back to its own count at once
curl -X PATCH localhost:6065/materializations/$MAT -H 'Content-Type: application/json' \
     -H "$AUTH" -d '{"autoscaling": false}'
```

Off for every materialization: Settings › Materializations › *Autoscale runtimes*; scaled runtimes go
back within a minute. Each change of members rebalances the runtime's consumer group, as a change of
replicas does. The control plane logs each change as `control_plane_runtime_autoscaled`.

### Change the topics

```bash
curl -X PATCH localhost:6065/materializations/$MAT -H 'Content-Type: application/json' -H "$AUTH" \
     -d '{"tables": [{"source_topic": "orders"}, {"source_topic": "refunds", "dataset": "sales"}], "removed_tables": {"payments": "keep"}}'
```

Every topic removed needs a decision for its table: `keep` (read-only, still in its dataset) or
`drop` (with its data, once the runtime has stopped reading it). A new topic gets a table in the
dataset given, named as given or after the topic, once its name is claimed (07 §7.9b); a kept table's
topic, added back, writes that table again from where it stopped. Each needs write access to the
datasets involved. The runtime is
stopped for its consumer group's session timeout plus a pod start - about four to five minutes on a
test cluster - and every table is written again exactly once from where it was (07 §7.9a). To drop a kept
table later: `-d '{"removed_tables": {"payments": "drop"}}'`. The name, the destination and the
partitioning cannot change.

### Delete

```bash
curl -X DELETE localhost:6065/materializations/$MAT -H "$AUTH"
```

Deletes the runtime's eight Kubernetes resources, its Kafka topics and consumer group, then **its
table** — every data file, manifest, metadata file and the pointer — and finally its control-plane
records. **This cannot be undone.** To keep the table, copy it first, or run the control plane with
`destination_cleanup_enabled` off (Settings → Deletion): the drop then only renames the pointer to
`catalog-pointer.dropped.json`, so the table leaves the catalog and stays readable by path
(07 §7.10).

The objects go in the store's batch deletes, in passes of about 10 s and at most 100 delete requests:
100 to a GCS batch request (passes of up to 10,000 objects), 1,000 to an S3 `DeleteObjects`
(100,000), 256 to an Azure blob batch (25,600). Where the store deletes one object per request, such
as GCS behind an emulator, a pass deletes up to 1,000. `pass_deleted_count` over `duration_ms` in the
progress log is the actual rate. The control plane keeps serving everything else meanwhile, and the
materialization stays listed as `deleted` until the last pass.

```bash
kubectl logs -n $NS $(cp_pod) -f | grep -E "control_plane_drop_table_(progress|retry_scheduled|completed)"
```

### Run table maintenance now

The maintenance app compacts, expires snapshots and removes orphans on its own schedule (11 §11.6),
for every table - kept ones, and those of a stopped materialization, included. To run a job now —
after fixing what made a partition's compaction fail, or to compact the partition still being
written:

```bash
curl -s -X POST localhost:6065/materializations/$MAT/maintenance -H "$AUTH" \
     -H 'content-type: application/json' -d '{"job":"compaction"}'      # or expire_snapshots, orphan_cleanup
```

It answers `202` with the tables it asked for; the app starts each at its next step, five seconds
apart at most, and the result lands in the table record's `maintenance` history (the console's Table
tab shows it, and the running job meanwhile). A requested compaction ignores the hot-partition delay,
and one with nothing to do reports `skipped` with the reason. On a test cluster, a requested compaction of 9
files took 6.6 s, and its result was in the record 3 s after that.

### Mask rows written before a protection change

An edit masks records written from then on; the rows written before keep their plain values. The
Protection tab counts them per table and masks them with *Mask rows already written*; through the API
(it needs Run table maintenance and Choose data protection on each table's dataset; 22 §22.14):

```bash
curl -s -X POST localhost:6065/materializations/$MAT/maintenance -H "$AUTH" \
     -H 'content-type: application/json' -d '{"job":"reprotect","purge":true}'
```

The request goes on each table's record and every tick carries it until the maintenance app has
rewritten every file holding the plain text — pass after pass, a gigabyte each — and, with `purge`,
expired every older snapshot and deleted the files only they held. Follow it on the Protection tab, in
the table's maintenance history (`job: reprotect`, `done: true` on the last pass), or in the app's log
(`maintenance_reprotect_*`). Without `purge`, time travel reads the old values until snapshot expiry
and orphan cleanup take the old files; ask again with `purge` to remove them at once.

### Give an engine read access

Make an access token on the console's Account page (or `POST /auth/tokens`, `{"name": …,
"permissions": ["catalog.read"], "expires_in_days": 90}`) and configure the engine with the catalog
URI `<control plane>/catalog` and the token (11 §11.8, 18 §18.15). The token is shown once. Revoke
it on the same page. The token opens the catalog only: the engine reads the data files with
credentials of its own for the table's backend, which the catalog does not vend — read and list on
the destination (§12.12a), set as the Connect panel shows (§12.5).

### Force reconciliation

The sweep runs every 2 minutes and re-applies any runtime whose rendered spec hash differs from
`last_applied_spec_hash`. To force it immediately, `POST .../resume`.

Corollary: hand-editing a runtime resource with `kubectl edit` is reverted within ~2 minutes.

### List with a different limit

```bash
curl -s "localhost:6065/materializations?limit=100"
curl -s "localhost:6065/materializations?limit=all"     # unbounded — use with care
```

Allowed: `10`, `30`, `100`, `all`. Anything else → `400`.

### Turn the live tail on or off

Every materialization has a live tail; there is no switch for one of them.

- For the deployment: Settings › Live tail › *Live tail* (`live_tail_enabled`). No ticks are sent,
  each member forgets its tables within five minutes, and consoles stop asking within two.
- For one console: its own *Live tail* setting (`liveTailEnabled`), or no `tailUrl` for the domain
  in its registry.

Stopping a materialization stops its tail too; resuming brings it back within a minute.

### Look at the tail app

```bash
curl -s 'localhost:6066/tail/v1/members?health=true' -H "X-Console-Token: $SERVICE_TOKEN" \
     -H "authorization: Bearer $SERVICE_TOKEN" | python3 -m json.tool
for p in $(tail_pods); do kubectl logs -n $NS $p --tail=50 | grep -E 'tail_|ERROR|WARN'; done
```

Each member's health lists its tables — `state`, `behind`, `lag_ms`, `rows`, `last_error` — and its
counters; 19 §19.16 reads them.

### Change a setting

Use the console's Settings page; it shows each setting's current value, where it comes from, and
what a reset returns to. Without the console:

```bash
curl -s localhost:6065/settings/runtime -H "$AUTH" | python3 -m json.tool          # values, rollout
curl -s -X PATCH localhost:6065/settings/runtime -H 'content-type: application/json' \
     -H "$AUTH" -d '{"values":{"max_open_file_age_seconds":60},"base_revision":<revision>}'
curl -s -X PATCH localhost:6065/settings/runtime -H 'content-type: application/json' \
     -H "$AUTH" -d '{"reset":["max_open_file_age_seconds"]}'   # undo a bad override
```

A runtime-scope change reaches running runtimes within seconds. `GET /settings/runtime` →
`rollout` shows which have it. Reference: [17 — Settings](./17-settings.md).

### Manage people and access

Use the console's Settings → Users, Groups, Roles and Audit log. Common cases:

| Task | In the console | Without it |
|---|---|---|
| Add someone | Users → Add user, with a generated password to pass on | `POST /users` |
| Someone forgot their password | Their user page → Reset password; they choose a new one at sign-in | `POST /users/{id}/password` |
| Someone is locked out | Their user page → Unlock, or wait out the lockout (15 min) | `POST /users/{id}/unlock` |
| Someone left | Deactivate (ends their sessions at once), delete later if wanted | `PATCH /users/{id}` `{"status":"deactivated"}` |
| A laptop was lost | Their user page → Sign out everywhere | `POST /users/{id}/sign-out` |
| Who did that? | Audit log, filtered by person or event | `GET /audit?q=<email>` |

**Every administrator is lost or locked out.** Set `bootstrap-admin-email` and
`bootstrap-admin-password` in the auth Secret and restart the control plane: at startup, when no
active administrator exists, that account is created — or restored, reactivated and added to Admin
— with that password, to be changed at first sign-in ([18 §18.8](./18-identity-and-access.md)).

**Rotating the session signing key.** Copy `signing-key` into `previous-signing-key`, set a new
`signing-key`, restart the control plane, and remove `previous-signing-key` after the maximum
session length (7 days by default). Nobody is signed out.

## 12.8 Inspecting state directly

State lives in the Kaspr table `<release>-state` (dev) / `state` (base), backed by a changelog
topic. There is no direct query API; use the HTTP endpoints, or read the changelog. The
changelog topic name is derived by Kaspr/faust from the app id and table name — confirm the
exact name from the broker before consuming:

```bash
kafka-topics --bootstrap-server $BROKERS --list | grep -i '<release>-state.*changelog'

kafka-console-consumer --bootstrap-server $BROKERS \
  --topic <the-changelog-topic-from-above> \
  --from-beginning --property print.key=true | grep "materialization:$MAT"
```

Key layout (from `storage/keys.py`):

| Key | Contents |
|-----|----------|
| `materialization:<id>` | `Materialization` envelope |
| `materialization-status:<id>` | `MaterializationStatus` |
| `materialization-table:<id>:<table>` | a table's record |
| `materialization-schema:<id>:<table>` | a table's current schema projection |
| `materialization-schema-version:<id>:<table>:<schema_id>` | a table's earlier projections |
| `runtime-binding:<id>` | `RuntimeBinding` |
| `materialization-op:<id>:<operation_id>` | operation record |

The table's own state is in its object store: `lake_cat "$LOC/metadata/catalog-pointer.json"`
(§12.0) names its current metadata file, and that file names its snapshots (11 §11.2, §11.4).

Observing the event topics live:

```bash
for t in requests work status table-events schema-authority-updates; do
  echo "== $t"; kafka-console-consumer --bootstrap-server $BROKERS \
    --topic midstream-materialization-$t --from-beginning --max-messages 5
done
kafka-console-consumer --bootstrap-server $BROKERS --topic $RT-commits --max-messages 20   # the runtime's control topic
```

## 12.9 Symptom → cause → action

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `400 name is required` on create | Missing/blank required field | Send `name`, `source_topics` (and `destination_uri` without a default) |
| `409` on create | `destination_not_configured`: no destination, or one that cannot hold tables, as the message says; `table_name_taken`; or `protection_key_not_configured`: the policy hashes and the deployment has no protection key | Configure a destination of 04 §4.5.2; choose another name; add the protection key (22 §22.9), or choose partial or withheld |
| Create returns 202, nothing happens | `request-intake` agent down, or requests topic missing | Check agent + topic |
| `400 invalid_request`, `reason: source_topic_missing`, on create or on adding a topic | Kafka does not list the topic: a typo, a topic not created yet, or one the Kafka user may not describe | Create the topic or grant Read on it ([21 §21.7](./21-kafka-access.md)), then send the request again |
| `503 unavailable` on create or on adding a topic, "Kafka cannot be asked about the topics right now" | Kafka did not answer the topic check within 10 s | Check the brokers; send the request again |
| `failed` / `source_topic_missing` | The topic left Kafka, or the Kafka user lost Describe on it, between the request and discovery | Delete; create again once the topic can be read |
| `failed` / `start_discovery_failed`, "connecting to Kafka … timed out" | Source topic unreachable / auth failure | Check `K_KAFKA_*` env and ACLs; delete and recreate once fixed |
| `failed` / `discover_tables_failed` after a change of topics | The added topics could not be sampled: Kafka unreachable, or the user may describe a topic but not read it | Fix the cause, then resume: the apply samples and creates the new tables first |
| `failed` / `create_table_failed`, the message naming a `401` or `403`, `Unable to locate credentials`, `Your default credentials were not found` or `DefaultAzureCredential failed to retrieve a token` | The control plane has no usable credentials, or no write permission, for the destination's backend | Fix `storage.<backend>` or the identity's grants (§12.12a), then resume |
| `failed` / `create_table_failed`, "the bucket does not exist", `NoSuchBucket` or `ContainerNotFound` | The destination's bucket or container is missing, or `storage.s3.endpoint` or `region` names another server | Create it, or correct the values; then resume |
| `failed` / `invalid_destination` | A materialization recorded with a destination that cannot hold a table | Delete it and create it again under a destination of 04 §4.5.2 |
| `failed` / `apply_runtime_failed` or `delete_runtime_failed`, "Kubernetes … timed out after 90s" | The API server never answered a call: past its own 60 s request timeout, so a lost connection rather than a slow server | Check the API server and the network path to it; resume or re-issue the delete. A failed re-apply left the deployed runtime as it was |
| `failed` / `cleanup_runtime_kafka_failed`, "Kafka admin … timed out after 120s" | A broker never answered an admin call | Check the brokers, then re-issue the delete |
| `failed` / `cleanup_runtime_kafka_failed`, "TopicAuthorizationFailedError … failed to delete topic" or "GroupAuthorizationFailedError … failed to delete consumer group" | The Kafka user may not delete the runtime's topics or group | Grant F1 and F2 ([21 §21.3](./21-kafka-access.md)), then re-issue the delete |
| An app's pods restart; its log: `Cannot create topic: … Authorization failed`, or `TransactionalIdAuthorizationFailed` | The Kafka user may not create the release's topics, which every start declares, or write its transactional ids | Grant R1 or R3 ([21 §21.3](./21-kafka-access.md)); the pods start on their next restart |
| A KasprApp never ready, its log silent after `Subscribed to topic(s)` | The Kafka user may not read the app's consumer group | Grant R2 ([21 §21.3](./21-kafka-access.md)), then restart the app's pods |
| `running`, a table that stays empty while its topic has records; `consumer_lag.error` "topic does not exist" | The Kafka user lost Read on the topic after it was checked, or the topic was deleted and created again | Grant Read, then stop, wait past the session timeout and resume ([21 §21.7](./21-kafka-access.md)) |
| Log: "Observing KasprApp … timed out; the next sweep observes it again" | The API server did not answer a read within 90 s | Nothing, if it recovers: the sweep observes again every two minutes |
| Stuck `deploying` | Pod scheduling, image pull, RBAC | `kubectl describe kasprapp $RT` |
| `deploying` / `waiting_for_table` on a new materialization whose tables exist | Its runtime's pods cannot read them: with AKS workload identity or EKS Pod Identity, the runtime's ServiceAccount `$RT-app`, new with the materialization, has no federated credential or association yet | Make it, then restart the runtime's pods (§12.3, §12.12a) |
| A runtime runs more members than its `replicas` | autoscaling: it is far behind (§12.7) | nothing; the detail's `autoscale.reason` says why. Turn it off for the materialization to stop it |
| `deploying` a few minutes after a runtime fell far behind | autoscaling added members, and the new pods are starting | nothing; it is `running` again once they are ready |
| `degraded` / `runtime_members_restarting` | members keep restarting: out of memory, most often | §12.3 |
| Log: "Cannot read the pods of …" | the control plane's role lacks `pods` (02 §2.7) | let the chart make the Role (`rbac.create`), or give your own its `pods` rule |
| Flapping `running` ↔ `degraded` | Heartbeat interval ≥ ⅓ of the 120 s staleness window | Keep heartbeat ≤ 40 s |
| `records_skipped` growing | Records a member cannot write — a value that is not JSON | The skip reasons name the latest partition and offset (§12.3) |
| `unmapped_values` growing | Values the schema cannot hold: past `max_schema_fields`, or a collapsed family's array element | 09 §9.8; raise the limit if the fields matter |
| Unexpected `__v2` columns | a genuine type change: a fork (09 §9.5) | the console reads the family as one column; other engines coalesce, guided by `materializer.fork-map` |
| A field became `__json` | its family passed `max_forks_per_field`, or a struct passed `max_struct_fields` | expected (09 §9.3); values are kept as JSON text |
| `failed` / `table_missing` | no table at all: created before 1.0, or a create whose discovery failed, resumed | delete and create it again (§12.13) |
| `failed` / `table_location_in_use` | another materialization's table at the location | §12.3 |
| `degraded` / `commits_stalled`, members healthy, commits flowing | the reason lingers after disruptions until the next resource observation (08 §8.6) | nothing; it clears within ten minutes |
| `degraded` / `commits_stalled` that never clears; `last_commit_at` stopped; the committer's log quiet | the committer ignores a member's announcements as stale: after that member restarted, on a runtime that predates session epochs (10 §10.5) | restart the committer member (§12.3) |
| Commits take many seconds | ~2 s plus ~16 ms per file (13 §13.8); a backfill commits hundreds of files at once | expected; raise `commit_interval_seconds` to commit less often |
| `commit_failed` again and again on an S3-compatible server, lost compare-and-swaps that no other writer explains | the server does not honour conditional writes | run the conformance suite against it (§12.12a); a server that fails it must not hold tables |
| Thousands of small files per day during a backfill | each seal writes a file per partition its rows touch; compaction works through past days a job a tick, 1 GiB at a time | expected; watch the Table tab's partitions shrink (11 §11.6) |
| An external engine's query fails on a snapshot with delete files | the console refuses delete files; an engine with `catalog.write` committed row-level deletes | 14 §14.2 |
| Scheduled work never arrives | Scheduler topic-name mismatch base vs dev | See [14](./14-known-gaps-and-roadmap.md) |
| Delete stuck > 480 s | Consumer group never empties | Look for stray pods; check the group |
| `RuntimeError: ... retry budget was exhausted` | Consumer group still non-empty | Delete the group manually, re-issue delete |
| A deleted materialization stays listed for hours | its table still holds objects; the drop deletes them a pass at a time, in the store's batch deletes | expected — follow `control_plane_drop_table_progress` (§12.7) |
| The control-plane log warns `Skipping materialization record mat-…, which this version cannot read` (or a `materialization-table` record) | a record an earlier, incompatible release wrote, typically a 1.0 copy stranded in another shard of the `state` table after its deletion. The list, the sweep, the catalog and the name check skip it, and its name is free again | nothing: once it says `deleted`, the retention sweep purges it `deleted_retention_hours` after its last write. One that does not say `deleted` belonged to a materialization not deleted before the upgrade to 2.0 (16 §16.5) |
| `failed` / `drop_table_failed` | five passes in a row failed: the store's access, a missing bucket or container, an outage — or only objects the store refuses are left (`403`; a retention policy, an object lock or hold) | fix access from the error in `phase_message` (§12.12a), or release the holds; what is left under the table's location must be removed by hand |
| The API stops answering — port-forwards and requests time out — while the pod is `Running` and `Ready` with no restarts | Something is holding the control plane's event loop. In the pod, the API port's line in `/proc/net/tcp` shows a full accept queue, and `/proc/1/task/1/wchan` sits in a blocking call. 0.x releases did this while deleting a large destination, compacting the object manifest, or waiting on a Kubernetes API or broker that stopped answering | upgrade; on such a release the stall ends when the work does, and a restart redelivers the same work |
| `POST /topics/analyze` answers `503 unavailable` | Kafka failed, or did not answer within 10 s; `details.reason` says which | Check the brokers, the `K_KAFKA_*` settings and ACLs; `GET /topics` fails the same way ([04 §4.9.1a](./04-http-api-reference.md)) |
| Consumer lag stops updating; the control-plane log shows `TimeoutError: Kafka did not answer within 30s` from the maintenance agent | The refresh's topic lookup timed out: Kafka is unreachable or not answering. Each refresh gives up after 30 s; the next, a minute later, tries again. A consumer group whose own lookup fails is measured as one that has committed nothing: from the runtime's heartbeat offsets | Fix Kafka connectivity; lag resumes with the first refresh that gets an answer |
| Local disk filling in a runtime pod | uploads failing: the backlog grows to its limits, then the source pauses | fix the store's access (§12.12a); batches retry and the backlog drains by itself |
| Runtime pods restart `OOMKilled`, or seals fail with `seal process exited with status -9`, and the partition never advances | the memory limit is below a seal's need, on top of the worker's own use | raise the runtime memory to 2 GiB (§12.10); nothing is lost meanwhile |
| The maintenance app restarts `OOMKilled`, or its compactions fail with `exited with status -9` | its limit is below a compaction child's need, times its concurrent jobs | raise its memory, or lower `MAINTENANCE_MAX_CONCURRENT_JOBS` (§12.10); the runtimes are not affected |
| Seals take minutes, and backlog pauses come and go on a busy topic | the runtime pod's CPU limit is one core, which the seal shares with the writer (the shipped limit before 4.2) | two CPUs per runtime pod (§12.10) |
| A pod's CPU request is not the one in its manifest, and changes over time | its VerticalPodAutoscaler sets it from the pod's use, in place (02 §2.10c) | nothing: `kubectl describe vpa <app>-app-vpa` shows the recommendation and its bounds |
| A KasprApp reports `VerticalPodAutoscalerReady=False` | `NotInstalled`: no Vertical Pod Autoscaler in the cluster; `Forbidden`: the operator may not manage VPAs; `Invalid`, `Conflict`: the VPA was refused, or one of that name is not the app's | nothing breaks meanwhile: the app runs with its manifest resources. Install the add-on, grant the operator's chart RBAC, or delete the stray VPA (02 §2.10c) |
| An app's new pod starts with its manifest's CPU request, with no `vpaUpdates` annotation, though its VPA has a recommendation; a minute later it is resized in place | the VPA admission controller still resolves the app's pods to their StatefulSet: it looked them up before the KasprApp CRD had its scale subresource (kaspr-operator before 0.19.5) and keeps every lookup while it runs | restart the VPA admission controller once (02 §2.10c); meanwhile the updater applies the recommendation in place |
| A runtime member restarts after an `EvictedByVPA` event | an in-place resize its node could not fit, or one that stalled; the pod was evicted to be created where it fits | nothing: it resumes from its committed offsets (10 §10.7). Frequent ones mean a full node |
| Consumer lag stays in the tens of thousands per partition on a caught-up materialization | normal: offsets commit once rows are in the table, so lag counts rows staged but not committed (10 §10.7) | nothing |
| One partition's lag stays near its whole size, its committed offset at 1 | twm-faust records no fetch gap after offset 0, so a compacted offset 1 is never acknowledged (14 §14.3) | nothing is lost — the committer refuses what the table holds; it needs the twm-faust fix (branch `fix-commit-gaps`) |
| A runtime member restarts with `Writing the source record … failed; … this member restarts` before it | an unexpected writer error; the member restarts on purpose so the record is read again (10 §10.7) | read the traceback; a persistent error crash-loops the member until fixed, losing nothing |
| `ConsistencyError: persisted offset … higher than the last offset` and the control plane crash-loops | a table's changelog topic was deleted but its RocksDB store on the volume was not | §12.13, step 3 |
| `POST /stop`, `/resume` or `PATCH` returns 202 but nothing changes | Mutation WebViews do not persist; only the topic path does | Check `request-intake`, and that the WebView's table and topic derive from the release (`test_agent_manifests`) |
| `409 conflict` on stop or `PATCH` | `runtime_not_deployed`: discovery has not finished; `materialization_deleting`: it is being deleted | Wait for `deploying`/`running`; a deleting materialization cannot be changed |
| `400` naming `tables.<i>.name` or `.dataset` on create | The name cannot be read unquoted ([04 §4.5.1](./04-http-api-reference.md)) | A lowercase letter first, then lowercase letters, digits and underscores; not a reserved word |
| `400` naming `tables` on create | No topic, a name Kafka would refuse, more than `max_materialization_topics`, or the 2.0 field `source_topics` | Send `tables`, each `{source_topic, dataset, name?}` |
| `409 table_name_taken` naming `tables.<i>.name` | Another materialization's table has `<dataset>.<name>` | Choose another name, or delete or drop the holder's table first |
| `403` naming `dataset.write` and a dataset | The caller may not add or drop tables there | A dataset owner grants Dataset Writer (Datasets › Access) |
| `409 topic_change_pending` on `PATCH` | An earlier change adding topics still waits for its names | Wait a moment; `GET` the materialization's `pending_topic_change` |
| A create reads `deleted` / `table_name_taken` (or `dataset_not_found`, `forbidden`) | Its claim was refused after the API accepted it: another request took the name first, or the dataset or access went meanwhile | Create it again with another name |
| `409 dataset_not_empty` deleting a dataset | It still holds tables — written, kept, or being dropped | Drop them, or delete their materializations, first |
| Control plane cannot apply runtimes | Installed from a local path, which no pod could repeat (`direct_url.json` has `dir_info`) | Install from git or from a package index (`package.source`) |
| Every settings save answers `500`; the control-plane log shows `KeyError: '…<release>-settings-requests'` | The consumer group was not rebalanced after the deploy that added the settings topics | Force one rebalance (§12.1, [17 §17.12](./17-settings.md)) |
| A settings save is refused: "These settings changed after this page loaded" | Someone saved the same scope first | Reload the page; unsaved edits are kept |
| An env var change has no effect | The Settings page overrides the same setting | Reset the override |
| The Settings page is read-only | The console cannot reach the control plane | Fix the control plane; control-plane settings reappear once it answers |
| A runtime stays **Applying** on the Runtimes settings page | A member has not refreshed yet, or is not consuming | See [17 §17.10](./17-settings.md) |
| Sign-in answers `503` "no session signing key" | The auth Secret is missing `signing-key`, or it is shorter than 32 characters | Fix the Secret; restart the control plane |
| Every request answers `503` `starting` for some seconds after a restart | The control plane is reading its tables back from their changelogs; until it has, it answers nothing it could get wrong (18 §18.7) | Nothing: it ends once the tables are read, ~15 s on a test cluster; before 4.1 it answered from a half-read directory and signed console users out |
| Sign-in and every user, group or role change answer `500`; the log shows `KeyError: '…<release>-identity-requests'` | The consumer group was not rebalanced after the 0.6.0 deploy | Force one rebalance (§12.1) |
| Table records never advance after a deploy, though runtimes commit | `materialization-table-events` was not assigned: no rebalance after the deploy that added it | force one rebalance (§12.1) |
| An engine's `ATTACH` or `load_table` answers `401` / `403` | no token, an expired or revoked one, or one without `catalog.read` | make a token (§12.7) |
| `/catalog/…` answers `503` | `catalog_api_enabled` is off | Settings → Catalog |
| API calls answer `401 sign_in_required` | No `Authorization` header: the caller is a guest | Sign in (§12.0) |
| `401 session_ended` or `session_expired` | Signed out elsewhere, password changed, deactivated, or idle past the timeout | Sign in again |
| `403 forbidden` naming a permission | The account lacks it | Add them to a group whose roles grant it |
| `403 password_change_required` | The account must replace a password someone else chose | Change it in the console first |
| The console shows only the list, never details, in one domain | The console's `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN` for it unset or different from its control plane's | Set it to the domain's Secret's `service-token` (the Secret its `console.domains[]` entry names) |
| The Data tab has no **Live** toggle | No `tailUrl` for the domain in the console's registry; the live tail off in this console or in Settings › Live tail; or the materialization is stopped | Turn on what is off (§12.7) |
| The Data tab says *Live is unavailable* | The tail app cannot be reached at the domain's `tailUrl`, or refuses the console (`403 console_only`: its `TAIL_SERVICE_TOKEN` is not the console's service token for the domain) | Fix the URL or the Secret; the console keeps asking |
| A domain is *misconfigured* in the console | The registry's `controlPlaneUrl` for it reaches a control plane that says it is another domain, or none | Correct the entry, or the control plane's `domain.id` (20 §20.4) |
| The Table and Data tabs say "no storage is configured for …" | No `console.storage` profile's scope covers the table's location; the console reads nothing outside its profiles | Add a profile for the destination's prefix, with read-only credentials (20 §20.11) |
| The console cannot read a table whose runtime commits | The storage profile serving it lacks credentials, or they are refused; the console's start log lists each profile, its mode and whether its credentials were found | Fix the profile's Secret (`console.storage[].credentialsSecret`) and restart the console (§12.12a) |
| Access tokens made in one domain are refused in another | The domains' `federation-key`s differ (a banner says so), or the owner has no account there (`token_unknown_owner`) | The same key in every domain's auth Secret (18 §18.11); add the person there |
| *Live is starting* for more than a few seconds | The table is seeding: its schema cannot be had from the control plane or from the table in its object store, or its topic has no leader | The table's `last_error` in the tail's health (§12.7) |
| *N records behind*, in amber on the Data tab's status line | The table is `backlogged`: its runtime is more than twice `live_tail_max_rows_per_table` behind the topic (a backfill, a burst, a stopped runtime) | Nothing: the tail starts once the runtime is within 75 % of the bound; **Peek** (under More actions) shows the topic's newest records meanwhile |
| *N records behind* that stays short of the newest | The table is paused at its bound or its rate ceiling | Routine? Raise `live_tail_max_rows_per_table` or `live_tail_max_records_per_second_per_table` |
| The tail app logs `tail_routing_mismatch` | A tick arrived on a partition its key does not hash to: the control plane's producer partitions differently from the tail's ownership rule | Report it: requests reach the wrong member until fixed (19 §19.5) |
| A tail app member restarts `OOMKilled` | `TAIL_MEMORY_BUDGET_BYTES` is too close to the pod's limit for its tables | Lower the budget or raise the limit (§12.10) |
| Nobody can manage users | No active administrator, no bootstrap secret | Set the bootstrap keys; restart (§12.7) |

## 12.10 Scaling and sizing

### Sizing a runtime member

A runtime member is one pod doing three kinds of work:

| Process | CPU | Memory |
|---------|-----|--------|
| The Faust worker — Kafka consumer, writer, uploads, and on one member the committer | never more than one core, and all of one on a busy topic: the per-record work runs on one event loop, under one GIL (14 §14.4) | typically 250–700 MiB |
| A seal, in a child process, one at a time (`seal_worker_count`) | one core while it runs | one row group of staged rows (`target_row_group_size_bytes`, 32 MiB of JSON, about 300 MiB as rows), whatever the batch's size or the spread of its timestamps, since the seal writes one partition value's file at a time from spools on disk (10 §10.4). It once held a row group for every partition value the batch touched: up to 1.36 GiB on the load test's backfill, 2.1 GiB measured for a 250,000-row batch over two years |
| A compaction, before 2.0 only: on the committer member, in a child process. Since 2.0 the maintenance app runs it (below) | one core while it runs, at `nice` 10 | its 128 MiB budget, one input row group decoded and one output row group sorted, over the process's own few hundred MiB: 0.75–1.3 GiB on the load test (1.8 GiB before e7f19a2, when an input was decoded whole) |

Measured with `kubectl top` every 30 s on the E13 load test — `materializer-loadtest-orders`,
16 members, no resource limits, the whole two-hour backfill of its second run (2026-10-19, 13
§13.8), with the committer member's processes read from `/proc` every 5 s:

| | Mean | p90 | Max |
|---|---|---|---|
| CPU, the 15 other members | 0.74 cores | 1.49 cores | 2.0 cores: a seal beside the worker |
| Memory, the 15 other members | 397 MiB | 627 MiB | 1.97 GiB, during seals |
| CPU, the committer member | 0.97 cores | 1.72 cores | 2.39 cores |
| Memory, the committer member | 1.25 GiB | 2.15 GiB | 2.98 GiB: worker ~0.6 GiB, a seal up to 1.36 GiB, a compaction up to 1.83 GiB, at once |

So:

- **Two CPUs per runtime pod** (the limit, shipped since 4.2) let a seal run beside the worker,
  while a runtime's members request what they use: about one core on a busy topic, 100m on a
  quiet one (02 §2.10c). A third gains nothing: the worker cannot use a second core, and one seal uses one. For more throughput
  add replicas, up to the members the source topics' partitions keep busy — topics of the same
  partition count share members, so the distinct counts summed (10 §10.1): on the load test each member moved about
  1,400 records a second.
- **With a one-CPU limit** the seal and the worker share a core; the seal runs at `nice` 10, so it
  takes many times as long, the upload backlog fills and pauses consumption until it catches up.
- **1 GiB of memory per runtime pod** (request and limit, as shipped) is enough since 4.4, when a
  seal came to hold one row group whatever its batch. On a test cluster (2026-10-25) a 10-member backfill
  of 5.5 million load-test records, which had members killed for memory every few minutes the hour
  before, ran without a kill (`kubectl top` at most 472 MiB a member). A 16-member backfill of the
  whole two-year topic, sampled each second for ten minutes, peaked at 550–773 MiB of anonymous
  memory a member — the most on the member whose worker was largest, about 520 MiB, beside a seal —
  with page cache from the spools and files filling the rest of the limit, which the kernel
  reclaims there (`memory.events` `max`), and no kill. That is a quarter of the limit to spare on
  the busiest member: a table with much wider rows, or a larger `target_row_group_size_bytes`,
  needs more. Before 4.4 a backfill's seals held a row group per partition value, and members
  reached 1.97 GiB; before 2.0 the committer member also ran compactions beside its own seals,
  2.98 GiB at its peak, and one spec sizes every member.
- **The maintenance app**: one member runs one compaction at a time by default, so about 0.3 GiB of
  its own plus up to 1.3 GiB for the child: the shipped 1 GiB request and 3 GiB limit leave room.
  More concurrent jobs (`MAINTENANCE_MAX_CONCURRENT_JOBS`) need about 1.3 GiB more each;
  more replicas (up to the topic's 6 partitions) share the tables between them. Past the limit
  the kernel kills a child or the pod; a seal or a compaction that dies retries, and nothing is lost,
  as no offset commits past an uncommitted row. Raising `target_row_group_size_bytes` or
  `seal_worker_count` raises a runtime member's need in proportion.
- **The tail app**: for each table someone is watching, a member holds the records its runtime has
  not committed — by default up to a batch's age (two minutes) and a commit interval (30 s) of them —
  and those committed in the last `live_tail_retention_seconds` (120), at about 1.06 bytes per raw
  byte (19 §19.15). At 1,000 records a second of 1 KB that can reach about 290 MB, past the
  per-table `live_tail_max_bytes_per_table` (256 MiB), where the table pauses: raise it for such a
  table. A member's tables together stay under `TAIL_MEMORY_BUDGET_BYTES` (2.5 GiB of
  the 4 GiB limit). Building rows costs about 100 µs of CPU a record of 1 KB, so one core builds
  about 10,000 records a second across a member's tables, and following a quiet table costs about
  1.4 millicores besides (19 §19.15). More members (up to the 64 tick partitions) share the tables;
  scaling restarts every member (14 §14.16).
- **The control plane's pod** does no sealing, uploading or committing. Its CPU (one core at most,
  requested from use between 100m and 900m) bounds how fast it reconciles and serves the API and the
  REST catalog, not how fast any materialization runs.

Runtime pods take `resources` from `CONTROL_PLANE_RUNTIME_APP_SPEC`, the same for every
materialization (02, *The embedded runtime app spec*). The chart's runtime spec limits each member to
2 CPUs and 1 GiB, and request 0.5 CPU and 1 GiB to start with. Every app's VerticalPodAutoscaler
then moves its CPU request to what it uses, in place (02 §2.10c), so one spec no longer reserves a
busy topic's CPU for every quiet one. Memory is not autoscaled; 1 GiB holds a backfill's seals since
4.4. A changed spec reaches running materializations when the control plane re-applies them, which
it does for every one after its own restart.

**The volume** (`storage_size` per member, `default_runtime_storage_size`, `64Mi` since 4.0) holds
only the state table's RocksDB store (10 §10.8), whose 4 MiB write buffer keeps its write-ahead log
under 8 MiB, so it does not grow with the topic: on a test cluster a runtime that had committed 6,000
records used 72 KiB of it (2026-10-23). The control plane's own volume and the maintenance app's are
their KasprApps' `storage` (1Gi), unchanged. Batches are staged in the
container's filesystem, on the node's disk, which bounds them instead. The size is the deployment's,
not the request's: a changed setting (Settings › Materializations › Storage per replica) applies to
runtimes created after it, and one created earlier keeps its own (`1Gi` before 4.0).

### Scaling notes

**Control plane** is 1 replica by default with `onLeader: true` sweeps. Scaling up adds
throughput on the work topic but does not multiply sweeps.

**A runtime's replicas** are its materialization's own: `replicas` on create, or `PATCH` it
(§12.7). Partitions split across members, so more replicas than source partitions leaves members
idle. While a runtime is far behind, autoscaling runs more members than its own count, never more
than its partitions keep busy, and returns it to its own once caught up (§12.7).

**Practical ceilings** to keep in mind:

- The sweep renders the full runtime spec for **every** materialization every 2 minutes to
  compute the drift hash. That is CPU-bound and grows linearly.
- Each table has one committer: a busy table's commits are one member's work, about 2 s plus 16 ms
  per file (13 §13.8), and more members means more files in each. Raising `commit_interval_seconds`
  commits less often, in larger commits; it does not make a commit faster.
- The runtime control topic is broadcast to every runtime, but carries only settings deliveries.
- The maintenance app gets one tick per table each minute, and one member runs one long job at a
  time: hundreds of tables with frequent compactions can queue behind each other. Raise
  `MAINTENANCE_MAX_CONCURRENT_JOBS` or the app's replicas (its memory with them).

## 12.11 Disaster recovery

| Loss | Impact | Recovery |
|------|--------|----------|
| Control-plane pod | Transient; work replays from the topic | Automatic |
| Control-plane state table | Severe — bindings, statuses, table records and projections gone; the tables themselves are untouched | Restore from the changelog topic; without it, runtimes keep running and committing, unmanaged |
| `settings` table | Every override and its history; each setting falls back to its deployment value. Runtimes keep the values last delivered to them, and ignore new deliveries until the restarted revision passes theirs | Restore from its changelog topic rather than starting over (17 §17.12) |
| `identity` table | Every account, custom group and role, session and access token; everyone is signed out and every engine loses its token | Restore from its changelog topic. Without it, the bootstrap secret recreates one administrator at the next start (18 §18.13) |
| `identity-audit` table | The audit log | Restore from its changelog topic; nothing else depends on it |
| Auth Secret's `signing-key` | Everyone is signed out; nobody can sign in until it is set again. Access tokens are unaffected: they are checked against their stored hash | Set a new key and restart |
| A runtime pod | Transient | Automatic — the new member reads its partitions again from their committed offsets; the committer refuses what the table already holds |
| The committer's pod | Commits pause until Kaspr assigns `<rt>-commits` to another member (or the pod returns) | Automatic — members re-announce what is not committed (10 §10.6) |
| Staging volume (`/tmp`), including every stop and pod restart | Staged and uncommitted batches | Automatic: their offsets were never committed, so they are read again (10 §10.7) |
| A runtime's `<rt>-state` table | The committer rebuilds it from the table at its next start | Automatic |
| A table's pointer (`metadata/catalog-pointer.json`) | The table is invisible: runtimes report `waiting_for_table`, engines cannot load it | Rebuild it (below) |
| A table's data or metadata files | Permanent for what they held | Delete and recreate the materialization to re-materialize from the topic's retained history |

The system is designed so that **Kafka is the source of truth and the table's snapshots are the
checkpoint**: the committed offsets live in the same snapshot as the rows. Losing derived state
costs reading again, never correctness.

### Rebuilding a lost pointer

Metadata files are written before the pointer moves, so the newest `*.metadata.json` with the
table's uuid is at least as new as the lost pointer (`lakehouse/pointer.py::find_latest_metadata`).
With the materialization stopped, in Python in the control plane's pod, which has the package and
the store's credentials — `kubectl exec -it -n $NS $(cp_pod) -c kaspr -- python` — or anywhere else
with both:

```python
from midstream.lakehouse.pointer import TablePointer, find_latest_metadata, utc_now, write_pointer
from midstream.lakehouse.store import store_for

LOC = "<the table record's location>"            # gs://…, s3://… or abfss://…/<storage prefix>/<materialization id>/<table>
UUID = "…"                                       # the table record's table_uuid
store = store_for(LOC)                           # the store of the location's scheme
latest = find_latest_metadata(store, LOC, table_uuid=UUID)
pointer = TablePointer(table_uuid=UUID, identifier=("<dataset>", "<table>"), materialization_id="mat-…",
                       location=LOC, metadata_location=latest, sequence=0,
                       updated_at=utc_now(), updated_by="operator")
write_pointer(store, LOC, pointer, expected_version=None)  # creates it; refuses if a pointer exists
```

Then resume the materialization. Check `metadata_location` against the table record's
`metadata_location`: the rebuilt one must be the same or newer.

## 12.12 Table maintenance

The maintenance app runs it, one long job at a time per member by default (10 §10.10, 11 §11.6); the
Table tab shows the history and the running job, and every job is a `maintenance_completed` table
event and a protocol event in the app's log (`kubectl logs -n $NS <maintenance pod> -c kaspr | grep maintenance_`).

| You see | It means | Do |
|---|---|---|
| Many small files in past partitions | waiting for the next compaction; a job takes whole small partitions, up to 1 GiB, and the next starts at the next minute's tick | nothing; or **Run now** |
| Many small files in today's (this hour's) partition | the hot-partition delay: it is compacted an hour after its last commit | nothing; **Run now** compacts it anyway |
| Small files in this month's partition (a month-partitioned table) | each waits until it is an hour old, then joins the next compaction; the Table tab marks the partition due once enough have | nothing; **Run now** takes them without waiting |
| `compaction_failed` on a partition | five failures in a row; it is paused until compaction is asked for again | read the error in the history; fix it; **Run now** |
| A job of several partitions failed "(its partitions are compacted one at a time from now on)" | which partition failed is unknown, so each is compacted alone until it succeeds | nothing; a partition that fails alone backs off and gives up as above |
| `metadata.json` large, many snapshots | retention keeps every snapshot of the last hour, then one an hour, for 72 hours | lower `snapshot_retention_hours` or `snapshot_keep_all_minutes` (Settings → Maintenance) |
| Files under `data/` no snapshot lists | uploads no commit took, or a compaction that died before its commit | orphan cleanup deletes them once older than `orphan_min_age_hours` (24) |
| No maintenance at all, history empty for hours | the maintenance app is down, or receives no ticks | its pod; `maintenance-step` errors in its log; the control plane's `schedule_table_maintenance` work, and the `midstream-maintenance-requests` topic's offsets |
| A job shown running for hours | the member that ran it went away without reporting | the next tick reports it `abandoned` once it has run longer than `compaction_timeout_minutes` + 10, or at once when the same member restarted |
| A reprotect stays *requested* | it waits for the table to record the edit's protection revision (the committer does on its next commit, seconds); for the batches still open under the older revision, up to the batch ceiling (600 s) and two minutes after that; or for an earlier failure's backoff | the Protection tab's rollout; the history's last attempt; `maintenance_reprotect_failed` in the app's log |
| A reprotect leaves files *that cannot be rewritten yet* | a number field's plain values wait for the family's text field, which the writers create with the first masked value; fields in lists of lists are never rewritten | ask again once such a record has been committed (22 §22.13) |
| A compaction or reprotect failed: *The commit's outcome is unknown, so the job's outputs are kept…* | its pointer write got no usable answer — the connection lost, or S3 refusing a write that met another in flight — and the pointer read back could not tell whether it landed; nothing the table may reference is deleted | nothing, once: the next plan reads the table as it is, and orphan cleanup removes the outputs if the table never took them. Again and again: the store's answers (§12.12a) |
| A job failed: *a different object already exists at …* | an output's name, which carries a fresh id, holds an object of another size: something other than Midstream writes under the table's `data/` | find the writer; nothing but the runtime, the maintenance app and an engine with `catalog.write` should write there |

Compaction is safe to interrupt anywhere: its outputs are only files until one `replace` commit
names them, and that commit checks every input is still live and the row counts match per partition
(11 §11.6).

## 12.12a The object store

Every table lives in the object store its location names — `gs://` Google Cloud Storage, `s3://`
Amazon S3 or an S3-compatible server, `abfss://` Azure Blob Storage — and every pod reaches it through
`lakehouse/store/`: one store per scheme per process, built from the pod's environment the first time
a location of that scheme is used (11 §11.1a). The control plane, the maintenance and tail apps and
every runtime get the same credentials from the chart; the console reads with storage profiles of its
own (20 §20.11). The destination's bucket or container must exist before the first materialization:
no store creates one.

| Backend | Values | What every app and runtime gets |
|---|---|---|
| GCS | `storage.gcs.credentialsSecret`, key `service-account.json` | the key at `/etc/midstream/gcs/service-account.json`, named by `GOOGLE_APPLICATION_CREDENTIALS`; without a Secret, application default credentials (GKE workload identity) |
| S3 | `storage.s3.credentialsSecret` — `access-key-id`, `secret-access-key`, optionally `session-token` — and `region`, `endpoint`, `pathStyle` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`; `STORAGE_S3_REGION` (`us-east-1` unless set), `STORAGE_S3_ENDPOINT` (an S3-compatible server; AWS without it), `STORAGE_S3_PATH_STYLE`; without a Secret, boto3's default chain (IRSA, EKS Pod Identity, an instance role) |
| Azure | `storage.azure.credentialsSecret` — `connection-string`, or `account-key`, or `client-id`, `tenant-id` and `client-secret`, each optional — and `endpoint` | `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_CLIENT_SECRET`; `STORAGE_AZURE_ENDPOINT` (an emulator's Blob endpoint); without them, `DefaultAzureCredential` (AKS workload identity, a managed identity) |

The Azure store takes a connection string first — it serves its own storage account only — then the
account key, for the account a location names, then `DefaultAzureCredential`. It reaches an account
through its Blob endpoint: a connection string's own, else `https://<account>.blob.core.windows.net`,
or, behind an emulator, `<STORAGE_AZURE_ENDPOINT>/<account>`. A rendered manifest names Secrets, never
a value (02 §2.9a). The Settings page's Deployment section shows the `STORAGE_*` values as the control
plane's stores read them (04 §4.9.3, 17 §17.3).

**What each needs.** Every app and runtime reads, writes, deletes and lists under the destination: on
GCS `roles/storage.objectAdmin`; on S3 `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and
`s3:ListBucket` — a compare-and-swap's `If-Match` needs `s3:GetObject` besides `s3:PutObject`; on
Azure a data role such as Storage Blob Data Contributor. The console reads and lists only:
`roles/storage.objectViewer` (with an HMAC key of that service account for DuckDB's scans),
`s3:GetObject` and `s3:ListBucket`, Storage Blob Data Reader. Give it nothing more: a statement's
sandbox does not tell reads from writes (console 12 §12.4).

**A cloud identity instead of a Secret.** A backend without a Secret uses the pod's own identity,
given by `kaspr.podLabels` and `kaspr.serviceAccountAnnotations` (the console's:
`console.podLabels`, `console.serviceAccountAnnotations`). kaspr-operator runs every app and every
runtime under a ServiceAccount of its own, `<app>-app` — `$CP-app`, `midstream-maintenance-app`,
`$TAIL-app`, and `$RT-app` for each runtime, a new one with every materialization — so the identity
must trust them all. IRSA does it with a role trusting `system:serviceaccount:<namespace>:*-app`,
which leaves the console's `<release>-console` out. AKS workload identity needs a federated
credential, at most 20 per identity, and EKS Pod Identity an association, for each ServiceAccount, a
new runtime's included: until it has one, and its pods have restarted with it, its members report
`deploying` / `waiting_for_table`. GKE workload identity needs none: grant the bucket's role to the
namespace, which covers the console's pods too unless the console runs in a namespace of its own.

### Checking a store: the conformance suite

`python -m midstream.lakehouse.store.check <uri>` (`lakehouse/store/check.py`) checks that the store for
a URI keeps the contract Midstream's tables rely on, with the environment and credentials of wherever
it runs. Run it from the control plane's pod — every app and runtime has the same — under a scratch
prefix of the destination, before the first materialization and after any change of server, endpoint
or credentials:

```bash
kubectl exec -n $NS $(cp_pod) -c kaspr -- python -m midstream.lakehouse.store.check s3://example-lake/_check
kubectl exec -n $NS $(cp_pod) -c kaspr -- python -m midstream.lakehouse.store.check \
  abfss://lake@exampleaccount.dfs.core.windows.net/_check --large-mb 64
```

Everything happens under `<uri>/<12 hex run id>/`, and the run deletes what it wrote. It prints one
line per check — `PASS` or `FAIL`, the check, its seconds and what it found — then `N of 16 checks
passed`, naming any that failed, and exits `0` only when all passed (`1` when one failed, `2` when no
store can be built for the URI).

11 §11.1a lists the sixteen checks: create-only writes and creators racing for one key,
compare-and-swap and its refusals, 16 writers racing 25 rounds each on one counter with exactly one
winner per round, reads of missing objects, a table pointer's bytes never repeating, ranges, sizes,
versions and times, a listing across pages, streamed transfers, deletes and the cleanup. The S3 store
reports `conditional_delete` skipped: it offers no conditional deletes, and nothing in Midstream sends
one. `large_object` runs only with `--large-mb`.

`--writers` (16), `--rounds` (25 compare-and-swaps a writer), `--race-rounds` (new keys raced: the
rounds, at most six, unless given), `--list-objects` (1,050), `--large-mb` and `--skip <check>`
(repeatable, for an emulator's known deviation; never `cleanup`) change the run.

| Failed | Means | Do |
|---|---|---|
| `concurrent_writers`, `compare_and_swap`, `missing_under_condition` | the server does not honour `If-Match`: a stale version accepted, a missing object created under a condition, two writers winning one round, or a win lost | **do not use it**: the pointer's compare-and-swap is what keeps every commit exactly once (11 §11.2), and nothing makes up for a server that does not honour conditional writes |
| `create_only`, `racing_creators`, `file_transfer`, `large_object` | the server does not honour `If-None-Match: *`, lets two creators win, or mixes two uploads | do not use it: two tables could take one location, two files one name |
| `missing_object` | a missing key does not read as missing. On S3, a `403` `AccessDenied` here means the identity lacks `s3:ListBucket`, without which S3 answers a missing key with 403 | grant `s3:ListBucket`; otherwise do not use the store: a missing pointer would not read as "no table" |
| `head_and_list` | no times, a clock more than 15 minutes off, or a listing that misses or repeats objects | correct the clock; a store whose listings miss objects cannot serve orphan cleanup or the drop |
| `delete`, `delete_many` | deletes refused or failing | the delete permission: without it no table can be dropped |
| any, naming a `401`, `403`, `NoSuchBucket`, `ContainerNotFound` or a configuration message | credentials, a permission, the bucket or the endpoint — not the server's semantics; a new Azure role assignment can take minutes to apply | fix it (below) and run the check again |
| `cleanup` | objects left under the run's prefix | delete them; the line names one |

CI runs the same checks on every change (13 §13.3a): against SeaweedFS 4.51 and moto 5.2.5 for S3,
Azurite 3.37.0 for Azure, fake-gcs-server 1.56.1 for GCS. moto models AWS's answers, not its
atomicity, and an emulator is not the service it imitates: S3 itself and a real Azure storage account
are verified once someone runs this check against them (14 §14.20).

### When the store refuses

Each store retries what may succeed later — no answer, `408`, `429`, a `5xx` — inside the operation,
through its SDK; only a batch delete is left to its caller, the drop, which sends it again itself
(07 §7.10). Anything else comes back at once — a refusal with a message that starts with the object's
URI, a credential that cannot be found as its SDK's own error — and shows where the request was made:

| Where | What shows |
|---|---|
| Creating the tables | `failed` / `create_table_failed`, the store's error in `phase_message`: fix it, then resume (§12.3) |
| A runtime | `deploying` / `waiting_for_table`, or a table's `table_unavailable` or `commit_failed` with the error in `last_error` |
| The maintenance app | the table's maintenance history; `maintenance_*_failed` in its log |
| A drop | `destination cleanup pass left objects behind`, `control_plane_drop_table_retry_scheduled`, and after five failed passes `failed` / `drop_table_failed` |
| The console | the Table and Data tabs say the table cannot be read; the console's log has the store's answer (`table metadata unreadable`) |

| | GCS | S3 | Azure |
|---|---|---|---|
| No credentials found | google-auth's `DefaultCredentialsError` | botocore's `NoCredentialsError: Unable to locate credentials` | azure-identity's `ClientAuthenticationError`: `DefaultAzureCredential failed to retrieve a token …` |
| Credentials refused | a deleted or revoked key: google-auth's `RefreshError` (`invalid_grant`) | `403` `InvalidAccessKeyId` or `SignatureDoesNotMatch` | `the account refused the credentials: check the account key, connection string or identity (403 AuthenticationFailed)`; a token: `… (401 InvalidAuthenticationInfo)` |
| A permission missing | `403 … does not have storage.objects.create access to the Google Cloud Storage object` | `403` `AccessDenied`, naming the operation: `PutObject`, `GetObject`, `DeleteObjects`, `ListObjectsV2` | `the identity has no data role on the account, such as Storage Blob Data Contributor (403 AuthorizationPermissionMismatch)`; a firewall: `… (403 AuthorizationFailure)` |
| The bucket or container missing | `the bucket does not exist` | `the bucket does not exist, or STORAGE_S3_ENDPOINT or STORAGE_S3_REGION names the wrong server (404 NoSuchBucket)` | `the container does not exist in this storage account (404 ContainerNotFound)` |
| Another server or account | — | `NoSuchBucket` as above, or a failure in transit with no status: `EndpointConnectionError` | `the endpoint does not serve this account or container: check the account and STORAGE_AZURE_ENDPOINT (404 ResourceNotFound)`; a connection string of another account: `AZURE_STORAGE_CONNECTION_STRING names the storage account '…', but the store was asked for '…'` |

A missing bucket or container, and Azure's configuration answers, are never taken for a missing
object — only an S3 HEAD, whose answer carries no code, cannot tell, and a pointer is read with a GET
— so a table never looks dropped because its store is misconfigured.

### Rotating credentials

Credentials reach a pod as environment variables from a Secret — the GCS key as a mounted file — and a
store reads them when it is first built, so a changed Secret takes effect at the pod's next start, as
the protection key does (22 §22.9). Without a gap:

1. Make the new credential while the old one still works: a second access key (S3), the account's
   other key (Azure's `key1` and `key2`), a new key of the service account (GCS), another key for the
   identity (SeaweedFS).
2. Put it in the Secret in place of the old one.
3. Restart the apps, every runtime — a member loses only its uncommitted work, which it reads again
   (10 §10.7) — and the console when a profile's Secret changed:

   ```bash
   kubectl -n $NS rollout restart statefulset $CP-app midstream-maintenance-app $TAIL-app
   for app in $(kubectl get kasprapp -n $NS -o name | grep -- '-mat-'); do
     kubectl -n $NS rollout restart statefulset "${app#*/}-app"
   done
   kubectl -n $NS rollout restart deployment midstream-console
   ```

4. Retire the old credential once every pod has restarted.

A cloud identity has nothing to rotate: each SDK refreshes its tokens itself.

### An endpoint that moves

`storage.s3.endpoint` and `storage.azure.endpoint` are no part of any table's location, so a server
that moves — another Service name, host or port — needs a values change and a `helm upgrade`, and
nothing in the bucket. The control plane and the maintenance and tail apps restart with the new
`STORAGE_*` value; the runtime spec carries it too, so the control plane re-applies every runtime once
it is back (§12.10). Change each console profile's `endpoint` with it. An Azure location keeps its
`abfss://<container>@<account>.dfs.core.windows.net` form behind an emulator, so the same objects in a
real account are the same table. A table never moves to another backend: its metadata names every
file by its full URI, so a move is a new materialization.

### SeaweedFS

The S3-compatible server for development and test clusters, verified at 4.51:

- `weed mini` runs the master, a volume server, the filer and the S3 gateway in one process: one pod
  on a persistent volume serves a test cluster. `-bucket <name>` creates the bucket at start. Let its
  components talk over loopback (`-ip=127.0.0.1`), so its data and conditions survive the pod coming
  back with another address.
- Values: `storage.s3.endpoint: http://<service>.<namespace>.svc.cluster.local:8333` and
  `pathStyle: true`; any region will do.
- Identities scoped per bucket — `Read:<bucket>`, `List:<bucket>`, `Write:<bucket>` — give the apps a
  read-write identity and the console a read-only one.
- The S3 gateway answers before its filer is ready: after a start, wait until a signed listing
  answers before running the check.
- It settles create-only writes racing for one key at about 0.6 s per writer, so `racing_creators`
  takes about ten seconds a key at 16 writers, and the whole check about a minute.

### Azurite

Microsoft's emulator: a stand-in for an Azure storage account while building and testing, verified at
3.37.0, and never a store for data anyone keeps.

- Start it as `azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck --disableProductStyleUrl`, with
  `--location /data` on a volume to keep its blobs. The SDKs send API versions newer than Azurite
  accepts: without `--skipApiVersionCheck` every request is refused, `the service refused a request
  header, usually the API version … (400 InvalidHeaderValue)`. Without `--disableProductStyleUrl` it
  takes the first label of a dotted host name, such as `azurite.<namespace>.svc`, for the account.
- Values: `storage.azure.endpoint: http://azurite.<namespace>.svc.cluster.local:10000`, to which the
  store appends the account; for credentials, a connection string whose `BlobEndpoint` is
  `<endpoint>/<account>`, or the account key. The development account `devstoreaccount1` and its key
  are published: never let them guard anything reachable from outside the cluster. `AZURITE_ACCOUNTS`
  sets accounts of your own.
- Azurite has no DFS endpoint. Midstream and the console read through the Blob endpoint and need none,
  but an engine that reads `abfss://` through the DFS endpoint cannot read a table there.
- A killed Azurite loses up to 5 s of writes it had acknowledged, a compare-and-swap among them: stop
  it gracefully, and never by force under a test.

## 12.13 The 1.0 rollout: a fresh start that keeps people and settings

*A milestone of the development line before 1.0.0 (16 §16.4), kept as the model of an upgrade that
keeps the directory and the settings while the materializations start over.*

1.0 converts nothing (decision D8): every 0.x materialization is deleted and created again, and the
materialization plane starts empty. The identity and settings stores are kept, so every account,
group, role, session, audit entry and settings override survives, and the administrator signs in
with the same email and password. This is the procedure followed on a test cluster on 2026-10-18.

1. **On 0.7.0, delete every materialization** through the API and wait for each to leave the list:
   the old delete chain removes its runtime, Kafka topics and bucket objects. Delete by hand anything
   left under `<destination>/materialization_id=…/`, `_runtime_state/` and `_manifest/`
   (`gcloud storage rm -r`), and any runtime left behind (its KasprApp, agents, tasks, table, PVCs).
2. **Stop the control plane**: `replicas: 0`, and wait past the broker session timeout (120 s). That
   is also the forced rebalance 1.0's new topic needs (§12.1).
3. **Wipe the materialization plane.**
   - Delete the Kafka topics `materialization-requests`, `materialization-work`,
     `materialization-status`, `materialization-schema-proposals`,
     `materialization-schema-authority-updates`, the scheduler's schedule, cron and timetable topics,
     the `state` table's changelog, and any stray `<runtime>-signals` (environment-prefixed names in
     dev).
   - **Keep** the `settings`, `identity` and `identity-audit` changelogs, the
     `<release>-settings-requests` and `<release>-identity-requests` topics, and the auth
     Secret.
   - **Remove the control plane's local stores of every deleted changelog.** Each Kaspr table keeps a
     RocksDB store on the control plane's volume, `/var/lib/data/tables/<table>-<n>.db` and
     `<table>-backups`, holding the deleted changelog's offsets; left there, the control plane
     crash-loops at start with Faust's `ConsistencyError: persisted offset … higher than the last
     offset`. With the control plane at zero, mount its PVC in a short-lived pod on the same node and
     remove only those stores (on a test cluster: the state table's and the scheduler's `cron-due-index`,
     `cron-registry`, `timetable`, `timetable-index`):

     ```bash
     kubectl run cp-store-fix -n $NS --rm -it --restart=Never --image=busybox \
       --overrides='{"spec":{"nodeName":"<node>","volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"<the control plane PVC>"}}],
                     "containers":[{"name":"c","image":"busybox","stdin":true,"tty":true,"volumeMounts":[{"name":"d","mountPath":"/data"}]}]}}'
     # inside: ls /data/tables; rm -rf /data/tables/midstream-state-*.db /data/tables/midstream-state-backups …
     ```

   - Delete the 0.x KasprAgents, KasprTasks and KasprWebViews the 1.0 manifests no longer define
     (`schema-proposals`, `compact-object-manifests`); the others are replaced by name.
4. **Deploy 1.0** with the control plane at `replicas: 1`, and verify the installed commit in the
   `install-packages` init container's log (`Resolved … to commit <sha>`). Set
   `CONTROL_PLANE_CATALOG_BASE_URL` to the address engines reach the control plane at.
5. **Check**: sign in as the existing administrator; the Settings page shows the kept overrides.
   An override of a setting 1.0 removed (the schema authority's, the object manifest's) stays in the
   `settings` table, unread: configuration is resolved only for settings the catalog defines, and
   the page lists only those. Create a materialization and watch it reach `running` and its first
   commit (§12.2).
6. **Guard.** A record that survived step 1 can do no harm: 1.0's `apply_runtime` refuses a
   materialization without a table record, `failed` / `table_missing` (07 §7.3). Delete it.

**A fully clean cluster** is the alternative: also delete the identity and settings changelogs and
the two request topics. `identity-bootstrap` then recreates the administrator from the Secret's
`bootstrap-admin-email` and `bootstrap-admin-password`, who chooses a new password at first sign-in,
and every other account must be added again. Use it only if the identity tables are suspect.

## 12.14 The 3.0 rollout: a clean slate

*A milestone of the development line before 1.0.0 (16 §16.6), kept as the model of a clean-slate
upgrade.*

3.0 puts tables in datasets (16 §16.6). It is one release on a clean slate: nothing is migrated.

1. **Before**, with the 2.0 control plane, delete every materialization (`DELETE /materializations/
   {id}`) and wait for each to leave the list: that drops its tables and its runtime. 3.0 cannot
   read a 2.0 materialization, so it can neither show nor delete one.
2. **Deploy 3.0.** Rename `CONTROL_PLANE_ICEBERG_NAMESPACE` to `…_STORAGE_PREFIX` in the
   overlay, bind the `directory` table where it bound `identity`, add the eight `datasets-*` WebViews,
   and patch `request-intake`'s second output and `identity-intake`'s output and `state` binding in a
   renaming overlay (02 §2.9). kaspr-operator 0.19.4 or later rebalances the control plane for the
   new `directory` changelog on its own.
3. **Sign in.** The `directory` table starts empty, so `identity-bootstrap` creates the
   administrator from the auth Secret again; they choose a new password at first sign-in. Every other
   account, group and custom role is added again. The 2.0 `identity` changelog is no longer read; it
   can be deleted once nobody wants it back.
4. **Datasets.** Someone with `datasets.create` (Operator) makes the datasets; their owners share
   them; then materializations are created into them.

**A 2.0 materialization left running** (step 1 skipped): 3.0 skips its records, logging each once
(`Skipping materialization record …`), and drops its runtime's status events (`Ignoring runtime
status from …`). Its runtime keeps writing its 2.0 tables, unmanaged. Remove it by hand: delete its
KasprApp (`kubectl delete kasprapp <runtime> -n midstream`), then its Kafka topics
(`<runtime>-app-assignor-leader`, `<runtime>-app-<runtime>-state-changelog`, `<runtime>-commits`),
its consumer group `<runtime>-app` once Empty (after the session timeout), and its tables' objects
under `<destination>/<old namespace>/<name>-<hex12>/`. Its records stay in the `state` table,
unread. On a test cluster (2026-10-22) this took `shop` down in about three minutes.
