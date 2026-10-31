# 13 — Operations Runbook

## 13.1 Deployment shape

The console is a single stateless-ish Node process, one for every domain: there are no per-domain
consoles. It is installed by the chart — beside a control plane, in the same release, or alone in a
platform cluster (`controlPlane.enabled=false`) — and needs:

| Requirement | Detail |
|-------------|--------|
| The domain registry | `console.domains[]` in the chart's values, rendered into `CONSOLE_DOMAINS_FILE`: each domain's id, name, `controlPlaneUrl` and optional `tailUrl`, and exactly one `settingsHome` ([03 §3.3](./03-configuration-reference.md), handbook 20) |
| Network to every domain | Each `controlPlaneUrl`: this release's own Service, or another cluster's API route over TLS; each `tailUrl` the same, for that domain's live tail |
| One service token per domain | `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`: that control plane's `service-token`, from its `<release>-auth` Secret; the chart reads it from the Secret each entry names. Without it the domain's live views show only what guests may see ([12](./12-security-model.md#122-authentication-and-authorization)) |
| Control planes that say who they are | Every domain's control plane at Midstream 1.0 or later, with `domain.id` set to the id the registry gives it: one that says it is another domain is shown nothing of (`misconfigured`). Someone signs in to each as its bootstrap administrator first, adds people, and creates the first datasets |
| One federation key | The same `federation-key` in every domain's auth Secret, for access tokens that work in every domain; the console's Domains section says when they differ |
| Network to object storage | For the Table and Data tabs and the Schema tab's timeline: the console reads each table itself, in whichever domain's lake |
| Storage profiles | `console.storage[]`, rendered into `CONSOLE_STORAGE_FILE`: one profile per destination prefix the domains' tables are on — `gs://`, `s3://` or `abfss://` — each with its Secret of **read-only** credentials (`CONSOLE_STORAGE_<NAME>_…`, a gcs key file) or, without one, the console's own cloud identity: `console.podLabels` and `console.serviceAccountAnnotations`, on the ServiceAccount the chart makes for it, `<release>-console` (IRSA's role ARN, AKS workload identity; GKE workload identity needs no value, only a role granted to the namespace). Without a profile covering a table's location the console reads nothing of it ([03 §3.6a](./03-configuration-reference.md)) |
| TLS at the ingress | The session cookies are `Secure` only for requests that arrived over HTTPS (`CONSOLE_COOKIE_SECURE=auto`); the ingress must set `X-Forwarded-Proto` |
| Sign-in providers (optional) | `CONSOLE_URL`, the console's public URL, and each provider's redirect URI registered with it: `<CONSOLE_URL>/api/auth/sso/<provider>/callback` ([03 §3.3a](./03-configuration-reference.md)). Each domain turns the provider on for itself (handbook 18 §18.17) |
| Optional persistent volume | For the Parquet cache (cached profiles) |

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
| Startup / liveness | `GET /api/health` | HTTP 200 (returned even when every control plane is down) |
| Readiness | `GET /api/health` | HTTP 200. Do not make readiness wait on `controlPlaneConnected`: it is true only while every domain is connected, and one domain down must not take the console away from the others. `domains[]` says which is not |

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
| `<CONSOLE_PARQUET_CACHE_DIR>/<profile>/<container>/` | Mirrored Parquet (cached profiles) | First query after restart re-downloads |
| `<CONSOLE_RUN_DIR>/` | A directory per statement run, holding its result; Parquet exports while they download | Nothing a restart does not lose anyway: runs live in memory |

Both default under `os.tmpdir()`, so by default nothing survives a restart. A restart leaves the
results of the runs it dropped in `CONSOLE_RUN_DIR`; on a persistent volume, delete the directory
while the console is stopped. To keep the Parquet cache across restarts, mount a volume:

```yaml
env:
  - name: CONSOLE_DATA_DIR
    value: /var/lib/midstream-console
volumeMounts:
  - name: console-data
    mountPath: /var/lib/midstream-console
```

Sizing: the Parquet cache is bounded by `CONSOLE_PARQUET_CACHE_MAX_BYTES` (8 GiB default); stored
results by `CONSOLE_RESULT_ROW_LIMIT` per run. Direct profiles never use the cache.

## 13.4 Running more than one replica

The console is **not** cleanly horizontally scalable. Each replica keeps its own:

- poll loops, one per domain → N replicas means N× load on every domain's control plane,
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
the replica that started it, which is one more reason for sticky sessions — and so is a sign-in
through a provider: its flow is held by the replica that started it, and a callback that reaches
another replica is asked to sign in again.

For most deployments a single replica with a sensible restart policy is the right answer. The
console is a read-mostly operator tool; a few seconds of downtime during a rollout is acceptable.

## 13.5 Load characteristics

Per refresh pass with defaults, for each domain:

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

Structured JSON via Pino, `service: midstream-console`.

| Level | Message | Meaning |
|-------|---------|---------|
| `info` | `midstream console ready` | Listening; includes port, every domain with its control-plane and tail URLs, the settings home, and `workerPool`, the size of Node's worker pool (§13.7) |
| `error` | `The console's registry sends <id> to <url>, but that control plane is domain <other>. …` | A registry entry reaches another domain's control plane (or one that names no domain): nothing of that domain is served until the entry is corrected (§13.7) |
| `info` | `duckdb instance ready` | First query initialised DuckDB |
| `info` | `console log level changed` | A Settings page change to the log level was applied |
| `info` | `shutting down console` | SIGTERM/SIGINT received |
| `warn` | `live refresh failed` | A whole refresh pass of one domain failed; `domain` names it, `err` has the cause |
| `info` | `storage profile: DuckDB reads its data files in place` / `storage profile: its data files are downloaded …` | At start, one per storage profile: `profile`, `kind`, `scope`, `mode`, `endpoint`, `credentials` found or not, what it `readsWith`, and for a cached one the budgets (§13.11) |
| `warn` | `no storage profiles are configured …` | At start: the console reads no object storage, and every table says so |
| `warn` | `storage profiles read in place hand their credentials to DuckDB: give them read-only access …` | At start, naming the direct profiles (12 §12.4) |
| `warn` | `storage profile <name>: …` | At start: something wrong with one profile, by its variables' names (§13.11) |
| `warn` | `data access is disabled; the Data tab is unavailable` | At start, when the Data explorer is off: the console reads no object storage |
| `warn` | `parquet cache eviction failed` | Cache may exceed its budget |
| `warn` | `table could not be read` | A Data tab read or a run could not read a table; `err` has storage's own error, and the user got a `503` naming what failed (§13.7) |
| `warn` | `table metadata unreadable` | The same, for the Table tab's view |
| `warn` | `table refresh failed` | A background re-read of a table's pointer failed; the previous read is served until one succeeds |
| `warn` | `schema refresh failed` | The live store could not read a table's schema projection (`table` names it); the previous one is kept (`schema refresh still failing` at `debug` until it can) |
| `warn` | `topic catalog refresh failed` | The topic list for the create dialog could not be reloaded; the previous list is served |
| `warn` | `run failed` | A run failed for a reason other than its statement; `err` has the cause and the user sees the message |
| `warn` | `run eviction failed` | An expired run's stored result could not be deleted from `CONSOLE_RUN_DIR` |
| `warn` | `this console has no service token for domain <id>: …` | At start: that domain's live views read its control plane as a guest. Set `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN` to its control plane's `AUTH_SERVICE_TOKEN` |
| `warn` | `a sign-in provider could not be reached` | Starting a sign-in through `provider` failed: its discovery document (generic OIDC) could not be read. The person is told to try again |
| `warn` | `a sign-in code could not be exchanged` | The provider refused the code, or could not be reached: a wrong client secret, a redirect URI it does not have registered, or an expired code. The person sees the provider's own reason |
| `warn` | `a GitHub sign-in token could not be revoked` | Harmless: GitHub's token for that sign-in expires on its own; the console never stored it |
| `warn` | `token revocations could not be handed to a domain; it is told again within five minutes` | A revoked access token is refused by the domain that made it and the others reached; `to` names the one that was not. The 5-minute sync retries |
| `warn` | `a domain's catalog could not be reached` / `… listed` | An engine's request through `/catalog` or `/d/<domain>/catalog` could not reach that domain; the engine got `503` naming the domain |
| `warn` | `sign-out could not be recorded with the control plane` | The browser was signed out (cookie cleared), but the control plane did not hear it; the session ends at its idle timeout |
| `warn` | `ignoring stored console settings this console does not accept` | A stored override is unknown to this console or out of its bounds, probably written by another console version; `settings` lists them. The environment's value applies |
| `debug` | `detail refresh failed` | One materialization's detail failed; others unaffected |
| `debug` | `footer warming failed` | Reading Parquet footers ahead of a run failed; the run reads them itself |
| `debug` | `console error` | A handled `ConsoleError` was returned to a client |
| `debug` | `run statement failed` | A statement the engine rejected — the user's to fix, so not a warning |
| `debug` | `console settings could not be read; keeping the ones in force` | The 30-second settings refresh failed; the last overrides stay |
| `debug` | `session could not be checked with the control plane` | A session check failed; sessions checked in the last minute keep working |
| `debug` | `control plane identity unavailable` | A domain's minute check of `/system/version` failed; it is shown unreachable until one succeeds |
| `debug` | `token revocations could not be read from a domain this pass` | The 5-minute sync could not read one domain's revocations; the next pass tries again from the same point |
| `debug` | `session renewal failed; the current session stays in use` | Renewing a cookie session failed; it stays valid until its own idle timeout |
| `error` | `unhandled console failure` | Unexpected error; client got a generic 500 |
| `fatal` | `console failed to start` | Process exits 1 |

Per-request logging is disabled, so log volume is flat regardless of traffic. To see
per-materialization failures, set **Console log level** to `debug` on the Settings page
(Diagnostics) for a while. It applies at once, with no restart.

## 13.7 Troubleshooting

### A domain shows as unreachable

```bash
curl -sS localhost:8787/api/health            # domains[]: each domain's status and error
kubectl -n midstream get svc | grep app-api
kubectl -n midstream port-forward svc/midstream-control-plane-app-api 6065:6065
curl -sS localhost:6065/system/version         # version, domain, federation_key_id
```

The console keeps showing that domain's last-known data during an outage, by design, and every
other domain as usual. If the control plane is healthy but the console disagrees, check the
domain's `controlPlaneUrl` (its Settings page, Deployment section) and any network policy between
the console and it — for a domain in another cluster, its API route and certificate.

### A domain is misconfigured

The console asked the URL its registry gives for the domain, and the control plane there says it is
another domain, or none: the banner and the start-up log name both. Nothing of that domain is
served — no views, no sign-in, no catalog — until the registry entry points at the right control
plane, or the control plane's `domain.id` is set to what the registry calls it. The console checks
again every minute; no restart is needed once the control plane answers as the right domain.

### Access tokens made in one domain are refused in another

The domains' federation keys differ: the console Settings page's Domains section shows each
domain's key id, and a banner says when they do not agree. Put the same `federation-key` in every
domain's auth Secret and restart their control planes (handbook 18 §18.11). A token refused with
`token_unknown_owner` is the other case: its owner has no account in that domain with the same
provider account or email.

### Sign-in through a provider fails

The sign-in page lists what each domain answered. If the page says the sign-in *expired, or was
started in another browser*, the callback came back without the flow cookie, to another replica, or
more than 10 minutes later. If the provider's own error is shown, the console's registration is
wrong: the redirect URI must be exactly `<CONSOLE_URL>/api/auth/sso/<provider>/callback`. If a
domain answers `provider_disabled`, it has not turned that provider on (its Settings › Sign-in);
`no_account`, its first-sign-in policy is `invited` and nobody added the person; `email_not_allowed`,
the email's domain is not one it allows; `invalid_credential`, the domain could not check the
provider's token — its own provider settings (client id, tenant, issuer) differ from the console's.

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
| "Sign-in is not configured on this control plane" (`503`) | The control plane has no session signing key | Create its `<release>-auth` Secret with `signing-key` ([18 §18.12](../../docs/handbook/18-identity-and-access.md)) |
| `500`, and the control plane logs `KeyError: '…<release>-identity-requests'` | The control plane was upgraded to 0.6.0 without the forced rebalance | [17 §17.12](../../docs/handbook/17-settings.md) |
| "Email or password is incorrect." for the bootstrap administrator | The control plane made no administrator: no bootstrap keys in its Secret, or one already exists | Check its log for `is now one`; set the bootstrap keys and restart it |
| "Too many sign-in attempts" | This address tried more than `CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` times in a minute | Wait a minute |
| "Too many failed sign-ins. Try again in N minutes" | The control plane locked the email | Wait, or an administrator unlocks it (Settings → Users) |

### Everyone sees only the materialization list

The domain's `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN` is unset or does not match its control plane's, so
that domain's live views read as a guest. The start-up log says so, and the domain's Settings page
says *not configured*. Set it and restart.

### Someone was deactivated but still sees pages

For up to `CONSOLE_SESSION_CHECK_MS` (5 s) the console trusts its last check of their session;
an open stream re-checks every 30 s. Nothing they send in that window can change anything: the
control plane refuses their session at once.

### An environment variable change has no effect

The same setting is overridden on the Settings page; an override wins over the environment. Reset
it there to follow the deployment again.

### The Data tab has no Live toggle

Live follows every running materialization, in a console and a control plane that have the live
tail on. In order: the domain's `tailUrl` unset (its Deployment section says *not configured*); the
console's own *Live tail* setting off (Settings › Live tail); the control plane's `live_tail_enabled`
off, which an open tab notices within two minutes; the materialization stopped; or, against a
control plane before 4.3, its live tail switched off there. A statement has *Auto re-run* instead.

### The Data tab says Live is unavailable

The run shows committed rows and keeps asking. *Live is unavailable* (in amber at the status line's right;
its tooltip carries the reason): the tail app cannot be reached at the domain's `tailUrl`, or refuses
the console — `403 console_only` in the console's log means the domain's
`CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN` is not its tail app's `TAIL_SERVICE_TOKEN`. *Live is starting* or *Live is reconnecting* for more than a
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
| `no storage is configured for …; add a storage profile whose scope covers it` | No storage profile's scope covers the table's location | Add a profile whose scope covers it (`console.storage[]`); the Settings page lists the profiles |
| `object storage answered 403` | The profile's credentials may not read the table's bucket or container | Grant its identity read (and list) on it: the key file's or ADC's service account and, for direct gcs, the HMAC key's; the S3 key's policy; the Azure role or SAS |
| `object storage could not be reached` | No credentials found, no network to the storage or its endpoint, or a timeout | `gcloud auth application-default login` locally for gcs; check the profile's endpoint and egress in the cluster |
| `no object at …` | A metadata file or manifest the table names is gone: usually a snapshot that expired mid-read, so a retry works | If it persists, the table is damaged; handbook 12 |
| `names … outside its location` / `says it is at …` | The table points at files the console will not read ([12 §12.6](./12-security-model.md#126-paths-a-table-names)) | Find the commit that did it (Table tab › Snapshots) |

```bash
kubectl -n midstream logs deploy/midstream-console | grep 'table could not be read'
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

1. `workerPool` in the `midstream console ready` line. It should be at least
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

The control plane has no destination for tables: set it on the Settings page, under Materializations
› Destination — `gs://bucket[/prefix]`, `s3://bucket[/prefix]` or
`abfss://container@account.dfs.core.windows.net[/prefix]`. The dialog learns of a change made there at once; one
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

Lower `CONSOLE_PARQUET_CACHE_MAX_BYTES`, or give the cached profiles direct access — an HMAC pair for
a gcs profile, a connection string or the account key for an Azure emulator, no `"access": "cached"`
for any — so nothing is downloaded at all. The cache can also be deleted at any time while the
console is running.

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
| Read a gcs profile's files in place | Set its `CONSOLE_STORAGE_<NAME>_GCS_HMAC_KEY_ID` and `_GCS_HMAC_SECRET` (its Secret's `hmac-key-id` and `hmac-secret`) and restart |
| Add a destination | Add a profile to `console.storage[]` — name, scope, kind, endpoint, region, path style — with a Secret of read-only credentials, and upgrade the console's release |
| Disable data access | Settings → Data explorer → **Data explorer** off; no restart |
| Change any other tunable | Settings page; see [03 §3.10](./03-configuration-reference.md) for which ones |
| Verify a deploy | `curl -sS <host>/api/health` — every domain `ok` — then open the UI, sign in, and confirm the header reads `Live` |
| Add a domain | Install its release, add its entry to `console.domains[]` with the Secret holding its service token, and upgrade the console's release; it appears in the switcher once its control plane answers as that domain (handbook 20) |
| Sign in to every domain at once | A provider's button on the sign-in page; **Sign out everywhere** in the account menu ends every domain's session |
| Sign someone out, unlock or reset a password | Settings → Users → their page |
| See who did what | Settings → Audit log |

## 13.9 Upgrade checklist

Items 8–16 are earlier releases' notes, kept as a record: no installation of an earlier release is
upgraded to this one, which is installed anew (handbook
[14](../../docs/handbook/14-known-gaps-and-roadmap.md) §14.1).

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
    Destination, or `CONTROL_PLANE_DEFAULT_DESTINATION_URI`) before announcing the
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
16. **Domains** (handbook 20): one console serves every domain, on a clean slate. Replace
    `CONTROL_PLANE_URL`, `CONSOLE_SERVICE_TOKEN` and `CONSOLE_TAIL_URL` with the registry and a
    service token per domain (§13.1); give every control plane its `domain.id` and the shared
    federation key; route the old per-domain hostnames to `/d/<domain>` (handbook 02 §2.12). SQL
    names a table with its domain now, `domain_a.sales.orders`, and an access token made before is
    refused: make a new one.

## 13.10 GCS refuses every scan of a gcs profile

```
HTTP Error: HTTP error on 'https://storage.googleapis.com/…' (HTTP 403 Forbidden)
Authentication Failure - GCS authentication failed.
```

or, from older DuckDB wording, `MalformedSecurityHeader … this can be caused by the S3 region being
set incorrectly. * No region is provided.` **The region is a red herring**: GCS ignores it. The
failure is the profile's HMAC key pair: usually its key id is not a GCS HMAC access ID (a service
account email, a project id or a key file's path pasted in its place), so the signature is malformed
and GCS rejects it before checking it. A *well-formed but wrong* key fails with
`SignatureDoesNotMatch` instead. The console appends to the message which of the profile's variables
to check — only for a gcs profile's objects: S3's messages are DuckDB's own and accurate.

| Check | Expected |
|-------|----------|
| `CONSOLE_STORAGE_<NAME>_GCS_HMAC_KEY_ID` | ~61 characters, begins `GOOG1E` |
| `CONSOLE_STORAGE_<NAME>_GCS_HMAC_SECRET` | 40 characters of base64 |
| `gcloud storage hmac list` | The key exists and is `ACTIVE` |

The console warns at startup about a key id or secret of the wrong shape, by the variable's name,
so check the boot log before the query log. See [03 §3.11](./03-configuration-reference.md) for how
to create a key.

## 13.11 Storage profiles at start

The boot log has a line per storage profile — its name, kind, scope, mode, and whether its
credentials were found — and a warning for anything wrong with one: half a key pair, a key file that
is not there, an Azure emulator DuckDB could not reach. With no profile at all it warns that no
object storage is read. A profile that breaks a rule of the registry stops the console, naming the
rule. The direct profiles are named in one more warning: they hand their credentials to DuckDB, so
they must be read-only (12 §12.4).
