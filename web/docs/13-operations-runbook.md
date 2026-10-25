# 13 — Operations Runbook

## 13.1 Deployment shape

The console is a single stateless-ish Node process. It needs:

| Requirement | Detail |
|-------------|--------|
| Network to the control plane | `CONTROL_PLANE_URL`, usually the in-cluster service `…-app-api:6065` |
| Network to object storage | For the Data and Table tabs: the console reads each table itself |
| GCP credentials | ADC to read tables' metadata and manifests, and in cached mode their data files; HMAC additionally for direct mode, where DuckDB reads `gs://` in place |
| TLS at the ingress | The session cookie is `Secure` only for requests that arrived over HTTPS (`CONSOLE_COOKIE_SECURE=auto`); the ingress must set `X-Forwarded-Proto` |
| `CONSOLE_SERVICE_TOKEN` | The control plane's `service-token` from its `materializer-auth` Secret; without it the live views show only what guests may see ([12](./12-security-model.md#122-authentication-and-authorization)) |
| A 3.0 control plane | 3.0.0 or later: it keeps each source topic of a materialization in an Iceberg table of its own, in a dataset, which is what the console reads and checks access by. Against an older one every page shows a banner (`controlPlaneCompatible: false`). Someone signs in as its bootstrap administrator first, adds people, and creates the first datasets |
| Optional persistent volume | For the Parquet cache (cached mode) |

Build order matters: **build the client, then start the server.** `@fastify/static` registers
routes for the files present at startup.

```dockerfile
# Illustrative only — pin the digest and use your approved base image.
FROM node:24-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY --from=build /app/server ./server
COPY --from=build /app/shared ./shared
COPY --from=build /app/package.json ./
USER node
EXPOSE 8787
CMD ["node", "server/main.ts"]
```

Note that `server/` and `shared/` ship as **TypeScript** — the runtime strips types. There is no
server build artefact.

> Repository standards require pinned image versions and a non-root user. Prefer an approved,
> hardened base image over `node:24-alpine` if one is available in your registry.

## 13.2 Probes

| Probe | Endpoint | Success condition |
|-------|----------|-------------------|
| Startup / liveness | `GET /api/health` | HTTP 200 (returned even when the control plane is down) |
| Readiness | `GET /api/health` | HTTP 200 **and** `controlPlaneConnected === true`, if you want to drain traffic during an upstream outage |

`/api/health` does not trigger a refresh, so probing it every few seconds is free.

Reasonable resources for a moderate fleet with the Data tab enabled:

```yaml
resources:
  requests: { cpu: "200m", memory: "512Mi" }
  limits:   { cpu: "2",    memory: "4Gi" }
```

Memory is dominated by DuckDB: budget at least `CONSOLE_DUCKDB_MEMORY_LIMIT` plus ~256 MiB for
Node and the live store. Setting a DuckDB limit above the container limit will get the pod OOM
killed rather than producing a clean DuckDB error.

## 13.3 Persistence

The console keeps nothing that matters: schemas, history and data are the control plane's and the
tables'.

| Path | Contents | Lose it and… |
|------|----------|--------------|
| `<CONSOLE_PARQUET_CACHE_DIR>/` | Mirrored Parquet (cached mode) | First query after restart re-downloads |
| `<CONSOLE_RUN_DIR>/` | A directory per statement run, holding its result; Parquet exports while they download | Nothing a restart does not lose anyway: runs live in memory |

Both default under `os.tmpdir()`, so by default nothing survives a restart. A restart leaves the
results of the runs it dropped in `CONSOLE_RUN_DIR`; on a persistent volume, delete the directory
while the console is stopped. To keep the Parquet cache across restarts, mount a volume:

```yaml
env:
  - name: CONSOLE_DATA_DIR
    value: /var/lib/materializer-console
volumeMounts:
  - name: console-data
    mountPath: /var/lib/materializer-console
```

Sizing: the Parquet cache is bounded by `CONSOLE_PARQUET_CACHE_MAX_BYTES` (8 GiB default); stored
results by `CONSOLE_RESULT_ROW_LIMIT` per run. In direct mode the cache is never used.

## 13.4 Running more than one replica

The console is **not** cleanly horizontally scalable. Each replica keeps its own:

- poll loop → N replicas means N× control-plane load,
- derived time series → charts differ per replica,
- table reads → each replica reads pointers, metadata and manifests itself,
- Parquet cache → the same objects are downloaded per replica.

If you must run more than one:

- use **sticky sessions** at the ingress so a browser keeps hitting the replica that holds its
  series,
- expect a chart discontinuity on failover,
- consider raising the poll intervals proportionally to keep upstream load flat.

Settings are the exception: they are shared. Every replica reads the console's overrides from
the control plane every 30 seconds, so a change saved through one replica reaches the others
within 30 seconds. Sessions need nothing either: the cookie holds a control-plane token any replica
can check, so a browser can move between replicas without signing in again. A query run lives on
the replica that started it, which is one more reason for sticky sessions.

For most deployments a single replica with a sensible restart policy is the right answer. The
console is a read-mostly operator tool; a few seconds of downtime during a rollout is acceptable.

## 13.5 Load characteristics

Per refresh pass with defaults:

```
1 × GET /materializations
+ W × GET /materializations/{id}          (W = materializations with an open detail page)
+ up to N/3 × GET /materializations/{id}  (rotation, ~6 s cycle at a 2 s tick)
+ T × GET /materializations/{id}/tables/{table}/schema   (only for a table whose schema drifted or is not cached, while its detail page is open)
```

Reading tables costs object storage, not the control plane: a pointer read per table at most every
`CONSOLE_TABLE_METADATA_FRESH_MS` (5 s) while its Data or Table tab is open, or every
`CONSOLE_SNAPSHOT_CACHE_MS` (2 s) for runs, and a metadata file and new manifests only when a commit
has replaced them (06 §6.2).

Concurrency is capped at `CONSOLE_DETAIL_CONCURRENCY` (6). Browser count does **not** affect
upstream load — only whether a detail page is open somewhere.

| Fleet size | Approximate upstream requests/second |
|------------|--------------------------------------|
| 10 | ~2 |
| 50 | ~9 |
| 200 | ~34 |

If that is too much, raise `CONSOLE_FLEET_DETAIL_POLL_MS` first — it directly divides the rotation
term while leaving the watched detail responsive.

## 13.6 Log reference

Structured JSON via Pino, `service: materializer-console`.

| Level | Message | Meaning |
|-------|---------|---------|
| `info` | `materializer console ready` | Listening; includes port, control-plane URL and `workerPool`, the size of Node's worker pool (§13.7) |
| `info` | `duckdb instance ready` | First query initialised DuckDB |
| `info` | `console log level changed` | A Settings page change to the log level was applied |
| `info` | `shutting down console` | SIGTERM/SIGINT received |
| `warn` | `live refresh failed` | A whole refresh pass failed; `err` has the cause |
| `info` | `data access mode: direct …` / `data access mode: cached …` | At start: how DuckDB reads data files |
| `warn` | `data access is disabled; the Data tab is unavailable` | At start, or after the Settings page turned the Data explorer off |
| `warn` | `parquet cache eviction failed` | Cache may exceed its budget |
| `warn` | `table could not be read` | A Data tab read or a run could not read a table; `err` has storage's own error, and the user got a `503` naming what failed (§13.7) |
| `warn` | `table metadata unreadable` | The same, for the Table tab's view |
| `warn` | `table refresh failed` | A background re-read of a table's pointer failed; the previous read is served until one succeeds |
| `warn` | `schema refresh failed` | The live store could not read a table's schema projection (`table` names it); the previous one is kept (`schema refresh still failing` at `debug` until it can) |
| `warn` | `topic catalog refresh failed` | The topic list for the create dialog could not be reloaded; the previous list is served |
| `warn` | `run failed` | A run failed for a reason other than its statement; `err` has the cause and the user sees the message |
| `warn` | `run eviction failed` | An expired run's stored result could not be deleted from `CONSOLE_RUN_DIR` |
| `warn` | `CONSOLE_SERVICE_TOKEN is not set: …` | At start: the live views read the control plane as a guest. Set it to the control plane's `MATERIALIZER_AUTH_SERVICE_TOKEN` |
| `warn` | `sign-out could not be recorded with the control plane` | The browser was signed out (cookie cleared), but the control plane did not hear it; the session ends at its idle timeout |
| `warn` | `ignoring stored console settings this console does not accept` | A stored override is unknown to this console or out of its bounds, probably written by another console version; `settings` lists them. The environment's value applies |
| `debug` | `detail refresh failed` | One materialization's detail failed; others unaffected |
| `debug` | `footer warming failed` | Reading Parquet footers ahead of a run failed; the run reads them itself |
| `debug` | `console error` | A handled `ConsoleError` was returned to a client |
| `debug` | `run statement failed` | A statement the engine rejected — the user's to fix, so not a warning |
| `debug` | `console settings could not be read; keeping the ones in force` | The 30-second settings refresh failed; the last overrides stay |
| `debug` | `session could not be checked with the control plane` | A session check failed; sessions checked in the last minute keep working |
| `debug` | `session renewal failed; the current session stays in use` | Renewing a cookie session failed; it stays valid until its own idle timeout |
| `error` | `unhandled console failure` | Unexpected error; client got a generic 500 |
| `fatal` | `console failed to start` | Process exits 1 |

Per-request logging is disabled, so log volume is flat regardless of traffic. To see
per-materialization failures, set **Console log level** to `debug` on the Settings page
(Diagnostics) for a while. It applies at once, with no restart.

## 13.7 Troubleshooting

### The header shows a control-plane error

```bash
curl -sS localhost:8787/api/health
kubectl -n kafka-sql get svc | grep app-api
kubectl -n kafka-sql port-forward svc/develop-materializer-control-plane-app-api 6065:6065
curl -sS localhost:6065/materializations?limit=10
```

The console keeps showing last-known data during an outage, by design. If the control plane is
healthy but the console disagrees, check `CONTROL_PLANE_URL` and any network policy.

### The Settings page is read-only

Its banner names the reason: the console cannot reach the control plane, which stores every
setting. Fix that as above. Until then the console keeps the console overrides it last read, and
the control plane's own settings are not shown.

### Every settings save fails with `500`

The control plane was upgraded to a release that added the settings topics, and its consumer group
was not rebalanced afterwards. See the main handbook,
[17 §17.12](../../docs/handbook/17-settings.md).

### Nobody can sign in

| Answer on the sign-in page | Cause | Fix |
|---|---|---|
| "Sign-in is not configured on this control plane" (`503`) | The control plane has no session signing key | Create its `materializer-auth` Secret with `signing-key` ([18 §18.12](../../docs/handbook/18-identity-and-access.md)) |
| `500`, and the control plane logs `KeyError: '…materializer-identity-requests'` | The control plane was upgraded to 0.6.0 without the forced rebalance | [17 §17.12](../../docs/handbook/17-settings.md) |
| "Email or password is incorrect." for the bootstrap administrator | The control plane made no administrator: no bootstrap keys in its Secret, or one already exists | Check its log for `is now one`; set the bootstrap keys and restart it |
| "Too many sign-in attempts" | This address tried more than `CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` times in a minute | Wait a minute |
| "Too many failed sign-ins. Try again in N minutes" | The control plane locked the email | Wait, or an administrator unlocks it (Settings → Users) |

### Everyone sees only the materialization list

`CONSOLE_SERVICE_TOKEN` is unset or does not match the control plane's, so the live views read as
a guest. The start-up log says so. Set it and restart.

### Someone was deactivated but still sees pages

For up to `CONSOLE_SESSION_CHECK_MS` (5 s) the console trusts its last check of their session;
an open stream re-checks every 30 s. Nothing they send in that window can change anything: the
control plane refuses their session at once.

### An environment variable change has no effect

The same setting is overridden on the Settings page; an override wins over the environment. Reset
it there to follow the deployment again.

### The Data tab has no Live toggle

Live follows every running materialization, in a console and a control plane that have the live
tail on. In order: `CONSOLE_TAIL_URL` unset (the Deployment section says *not configured*); the
console's own *Live tail* setting off (Settings › Live tail); the control plane's `live_tail_enabled`
off, which an open tab notices within two minutes; the materialization stopped; or, against a
control plane before 4.3, its live tail switched off there. A statement has *Auto re-run* instead.

### The Data tab says Live is unavailable

The run shows committed rows and keeps asking. *Live is unavailable* (in amber at the status line's right;
its tooltip carries the reason): the tail app cannot be reached at `CONSOLE_TAIL_URL`, or refuses the
console — `403 console_only` in the console's log means its `CONSOLE_SERVICE_TOKEN` is not the tail
app's `MATERIALIZER_TAIL_SERVICE_TOKEN`. *Live is starting* or *Live is reconnecting* for more than a
minute (the table warming up, or moving between members): a tail app member restarting, or the
table's topic without a leader. The tail app's health view names the table's state and last error
(handbook 12 §12.7, 19 §19.16).

### Rates show `—` or `0`

Expected in three cases:

1. **Fewer than two samples.** The first observation cannot produce a rate. Wait one cadence.
2. **No `runtime_progress`.** The control plane omits it entirely until the runtime reports
   progress, so a materialization that has never written a row produces no samples.
3. **Genuinely idle.** No new rows since the previous sample.

If rates alternate between zero and a spike, something has reintroduced differencing at the poll
interval — rates must come from each runtime member's own heartbeats; see
[05 §5.4](./05-live-store-and-realtime.md#54-throughput-derivation).

### The ingest rate dips every few minutes

Usually real. Each runtime member stops consuming while it seals a batch into Parquet and uploads
it — both run on its event loop — and a runtime's partitions tend to seal together, every five
minutes or so under the default batching policy. On the load-test topic every member ran at
~760 rows/s between seals and near zero for 30–60 s during them, which halves the ingest rate for
about a minute. **Show N members** in the Reconciliation panel shows each member's own rate; a dip
shared by every member at once is a seal wave, a dip in one member is that member.

### Data tab says the table could not be read

The message says what failed:

| Message ends with | Cause | Fix |
|---|---|---|
| `object storage answered 403` | The console's credentials may not read the table's bucket | Grant its service account (ADC) read on the bucket; in direct mode the HMAC key's account too |
| `object storage could not be reached` | No ADC, no network to GCS, or a timeout | `gcloud auth application-default login` locally; check egress in the cluster |
| `no object at gs://…` | A metadata file or manifest the table names is gone: usually a snapshot that expired mid-read, so a retry works | If it persists, the table is damaged; handbook 12 |
| `names … outside its location` / `says it is at …` | The table points at files the console will not read ([12 §12.6](./12-security-model.md#126-paths-a-table-names)) | Find the commit that did it (Table tab › Snapshots) |

```bash
kubectl -n kafka-sql logs deploy/materializer-console | grep 'table could not be read'
```

The log line carries storage's own error, which the page leaves out.

### A statement fails with a binder error naming a column

The column is not in the materialization's schema at all — every column the schema has ever held
is in the table, as `NULL` in objects written before it existed, so a column that some objects
lack is never an error. Columns are named by field path without the schema's `payload.` root, and
must be quoted: `"status"`, `"customer.email"`. A statement that still writes `"payload.status"` fails
with an error naming the column it meant. The column rail lists them.

### Filters over a wide range are slow

A filter has to read its columns from every object in the range, and from outside the cloud each
object is four round trips. Check, in order:

1. `workerPool` in the `materializer console ready` line. It should be at least
   `CONSOLE_RUN_CONCURRENCY × CONSOLE_RUN_SCAN_CONCURRENCY`; an explicit, smaller
   `UV_THREADPOOL_SIZE` caps every scan at that many objects in flight, whatever the scan width.
2. The range. Every preset shows its row count before it is run, and a narrower one is
   proportionally cheaper.
3. Distance. From outside the cloud each file costs a few ~50 ms round trips (07 §07.6); a console
   running near its bucket does better. Raising `CONSOLE_RUN_SCAN_CONCURRENCY` helps far from the
   bucket ([03](./03-configuration-reference.md)).
4. Uncompacted files. A table a backfill wrote has many small files until compaction works through
   them; the Table tab's partitions show `compactionDue` where the next compaction takes them. The
   partition still being written has none until it settles: an hour after its last commit, or, in
   this month's partition, file by file as each is an hour old.

### The create dialog says no materialization can be created

The control plane has no destination for tables, or one that is not `gs://`: set it on the Settings
page, under Materializations › Destination. The dialog learns of a change made there at once; one
made on the control plane directly (an environment variable and a restart, or the API) shows within
the minute the console keeps its defaults.

### Schema dates span a whole month, or the table's whole history

They are as precise as the table's files (06 §6.3): a month-partitioned table dates a change to its
month, and an unpartitioned table, once compacted, to the span its compacted files cover. Exact
dates need the data scanned — on the Data tab, `SELECT min(_kafka_timestamp) FROM <table> WHERE
<field> IS NOT NULL`.

### Browser shows a MIME-type error for a JS module

The client was rebuilt while the server was running. Restart the server.

### Cache filling the disk

```bash
du -sh "$CONSOLE_DATA_DIR/parquet-cache"
```

Lower `CONSOLE_PARQUET_CACHE_MAX_BYTES`, or switch to direct mode with HMAC credentials so nothing
is downloaded at all. The cache can also be deleted at any time while the console is running.

### High memory

Check `CONSOLE_DUCKDB_MEMORY_LIMIT` against the container limit, and
`CONSOLE_SERIES_POINTS × fleet size` for the live store. DuckDB is almost always the dominant
term.

## 13.8 Routine operations

| Task | Command |
|------|---------|
| Clear the Parquet cache | `rm -rf "$CONSOLE_DATA_DIR/parquet-cache"` (safe while running) |
| Clear stored results | `rm -rf "$CONSOLE_RUN_DIR"` while the console is stopped |
| Run a table's maintenance now | Table tab › **Run now** (`materializations.maintain`); the maintenance app runs it, stopped or not |
| Add or remove a materialization's topic | Detail › **Edit** › Source topics (`materializations.edit`, and write access to the datasets of the tables it adds, removes or drops); a removed topic's table is kept read-only or dropped, and the runtime is stopped for about five minutes |
| Create a dataset, or give someone access to one | Datasets › **New dataset** (`datasets.create`); a dataset's page › **Access**, for its owners |
| Find who may read a dataset | Its page › **Access**; a person's or group's page › **Dataset access** for theirs |
| Force a full refresh | Restart, or simply open the UI — subscribing calls `wake()` |
| Switch to direct mode | Set both HMAC variables and restart |
| Disable data access | Settings → Data explorer → **Data explorer** off; no restart |
| Change any other tunable | Settings page; see [03 §3.10](./03-configuration-reference.md) for which ones |
| Verify a deploy | `curl -sS <host>/api/health` then open the UI, sign in, and confirm the header reads `Live` |
| Sign someone out, unlock or reset a password | Settings → Users → their page |
| See who did what | Settings → Audit log |

## 13.9 Upgrade checklist

1. `npm install && npm audit` — expect 0 vulnerabilities.
2. `npm run typecheck` — both projects.
3. `npm run build` — confirm `QueryEditor` is still a separate chunk.
4. Run the verification harness in [14](./14-testing-and-verification.md).
5. Smoke test against a real control plane: fleet renders, a detail page streams, the Schema tab
   lists versions, the Table tab shows snapshots and partitions, the Data tab returns rows.
6. Deploy, then confirm `/api/health` reports `controlPlaneConnected: true`.
7. Open Settings and confirm it is not read-only. Settings need control plane 0.5.0 or later:
   against an older one the page is read-only, because the control plane does not answer
   `/settings`. Deploy the control plane first.
8. **0.14.0** needs control plane 0.6.0 (accounts): deploy the control plane first, create its
   auth Secret, set `CONSOLE_SERVICE_TOKEN`, and remove `CONSOLE_DEFAULT_SUBMITTED_BY` (ignored).
   Sign in as the bootstrap administrator and add people before announcing the upgrade: from
   then on, anyone not signed in sees only what the Guest group allows.
9. **1.0.0** needs control plane 1.0.0, which keeps every materialization in an Iceberg table:
   deploy the control plane first (handbook 12 §12.13). The console reads the tables itself, so its
   credentials need read access to the tables' bucket. It keeps no schema history any more —
   delete `<CONSOLE_DATA_DIR>/schema-history` — and the variables for the 0.x object manifest are
   gone (03 §3.3).
10. **2.0.0** needs control plane 2.0.0 (several topics per materialization, edits of the topics,
    the maintenance app): deploy the control plane and its maintenance app first (handbook 12
    §12.1). SQL names a table by its namespace now, `shop.orders`: a statement or share link that
    names a bare table fails, with the canonical names in its error.
11. **3.0.0** needs control plane 3.0.0 (datasets, table claims, per-dataset access), on a clean
    slate: 3.0 does not migrate 2.0's materializations (handbook 12 §12.14). Deploy the control
    plane first. SQL names a table by its dataset now, `sales.orders`, so statements and share links
    from 2.0 name tables that are gone. Data access is per dataset: someone who could query
    everything in 2.0 needs a role on each dataset, which its owner, or All Users' default Reader
    grant on a new dataset, gives.
12. **4.0.0** needs control plane 4.0.0, which takes every materialization's destination,
    namespace, runtime name and storage from the deployment and refuses a create that names one:
    deploy the control plane first, and set its destination (Settings › Materializations ›
    Destination, or `MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI`) before announcing the
    upgrade, since without one nobody can create a materialization. Scripts that create through the
    console's API drop those four fields. No migration: existing materializations keep what they
    were created with.
13. **4.1.0** adds the live tail. Deploy the control plane and its tail app first (handbook 12
    §12.1), then set `CONSOLE_TAIL_URL` to the tail app's Service and restart the console. Against a
    4.0 control plane, or without the variable, the console shows committed rows only, as before.
    New materializations have live tail on; existing ones keep it off until **Edit** turns it on.
14. **4.3.0** gives every materialization a live tail: the create dialog's checkbox and the edit
    dialog's switch are gone, and only the control plane's `live_tail_enabled` turns it off, for all
    of them. Against an older control plane it still honours a materialization switched off there.
15. **4.4.0** shows control plane 4.4's runtime autoscaling — an **Autoscale** switch in the create
    and edit dialogs, *Scaled up to N members* on the Overview — and labels a degraded runtime
    **Unhealthy**. Against an older control plane the switches and the line are not shown. Deploy
    the control plane first, with its role's new `pods` rule (handbook 12 §12.1).

## 13.10 `MalformedSecurityHeader` on every gs:// scan

```
HTTP GET error reading 'gs://…' in region '' (HTTP 400 Bad Request)
MalformedSecurityHeader: Invalid argument. Bad Request - this can be caused by
the S3 region being set incorrectly. * No region is provided.
```

**The region is a red herring.** GCS ignores it, the same text appears on requests that succeed,
and setting one changes nothing. The failure is credential shape: `CONSOLE_GCS_HMAC_KEY_ID` is not
a GCS HMAC access ID, so the signed `Authorization` header is malformed and GCS rejects it before
ever checking the signature.

The usual cause is pasting a service account email, a project id, or a path to a JSON key file into
the key id. A *well-formed but wrong* key fails differently — `403 SignatureDoesNotMatch` — which
is a useful way to tell the two apart.

| Check | Expected |
|-------|----------|
| `CONSOLE_GCS_HMAC_KEY_ID` | ~61 characters, begins `GOOG1E` |
| `CONSOLE_GCS_HMAC_SECRET` | 40 characters of base64 |
| `gcloud storage hmac list` | The key exists and is `ACTIVE` |

The console logs a warning naming the problem at startup, so check the boot log before the query
log. See [03](./03-configuration-reference.md) for how to create a key.
