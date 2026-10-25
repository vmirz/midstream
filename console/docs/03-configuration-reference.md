# 03 — Configuration Reference

Source: `server/config.ts`. This is the **only** module that reads `process.env`. Everything else
imports the frozen `config` object. Adding a setting anywhere else is a layering violation.

Since 0.13.0, these variables can also be overridden from the Settings page while the console
runs — 29 of them since 0.14.0. For those, `config` returns the override in force, else the environment's value.
§3.10 lists them and explains how an override reaches every replica.

## 3.1 How values are parsed

Four helpers normalise environment input:

| Helper | Behaviour |
|--------|-----------|
| `str(name, fallback)` | Returns the fallback when the variable is unset **or empty string** |
| `optional(name)` | Returns `null` when unset or empty; otherwise the raw string |
| `int(name, fallback)` | `Number.parseInt(raw, 10)`. **Throws at startup** if the result is not finite |
| `bool(name, fallback)` | True for `1`, `true`, `yes`, `on` (case-insensitive). Anything else is false |

Two consequences worth remembering:

- An empty string is treated as "unset", so `CONSOLE_GCS_HMAC_KEY_ID=` does not accidentally
  enable direct mode with an empty credential.
- A malformed integer is a **hard startup failure**, not a silent fallback. This is deliberate:
  a typo in a cadence variable should not quietly halve your polling load.

## 3.2 Server and process

| Variable | Type | Default | Effect |
|----------|------|---------|--------|
| `PORT` | int | `8787` | Listen port |
| `HOST` | string | `0.0.0.0` | Listen address |
| `LOG_LEVEL` | string | `info` | Pino level: `trace`, `debug`, `info`, `warn`, `error`, `fatal` |
| `CONSOLE_VERSION` | string | `0.1.0` | Reported by `/api/health` and `/api/config` |
| `HOSTNAME` | string | `os.hostname()` | Stored as `config.memberId`; currently informational only |
| `CONSOLE_SERVE_CLIENT` | bool | `true` | When false, the server does not serve `dist/` and returns JSON 404s for non-API paths |
| `CONSOLE_CORS_ORIGIN` | string | unset | When set, registers `@fastify/cors` with this origin. Leave unset when the client is same-origin |

## 3.3 Control plane

| Variable | Type | Default | Effect |
|----------|------|---------|--------|
| `CONTROL_PLANE_URL` | string | `http://localhost:6065` | Base URL. Trailing slashes are stripped |
| `CONTROL_PLANE_TIMEOUT_MS` | int | `10000` | Per-request timeout via `AbortSignal.timeout` |
| `CONSOLE_TAIL_URL` | string | unset | The tail app's base URL (handbook 19): in a cluster its Service, `http://materializer-tail-app-api:6065` (`develop-` prefixed in dev); from a laptop, the control plane's host where an HTTPRoute sends `/tail/` to that Service. Trailing slashes are stripped. Unset, the console shows committed rows only. Fixed by the deployment (shown under Deployment) |

`CONSOLE_DEFAULT_SUBMITTED_BY` was removed in 0.14.0: changes are attributed to the signed-in
person by the control plane. Console 1.0 retired `CONSOLE_OBJECT_MANIFEST_ENABLED`,
`CONSOLE_MANIFEST_TAIL_MAX_MARKERS`, `CONSOLE_MAX_LISTED_OBJECTS` and `CONSOLE_CATALOG_FRESH_MS`
with the object manifest and object listings they configured. Stored overrides for any of them
are ignored (`RETIRED_SETTINGS`, `server/settings/catalog.ts`).

## 3.3a Sign-in and sessions

| Variable | Type | Default | Effect |
|----------|------|---------|--------|
| `CONSOLE_SERVICE_TOKEN` | string | unset | The credential the console reads its shared live views with — the fleet, details, schemas, topics and its own settings. Must equal the control plane's `MATERIALIZER_AUTH_SERVICE_TOKEN`; it can read, never change. Unset, those views read as a guest and show only what guests may see, and the console logs a warning at start |
| `CONSOLE_SESSION_COOKIE` | string | `materializer_session` | Name of the HttpOnly cookie holding a browser's session. Renaming it signs everyone out |
| `CONSOLE_COOKIE_SECURE` | `auto` \| `true` \| `false` | `auto` | Whether the cookie is `Secure`. `auto` follows the scheme the request arrived with, as the ingress reports it (`trustProxy`), so it is Secure behind TLS and still works on `http://localhost` |
| `CONSOLE_SESSION_CHECK_MS` | int | `5000` | How long the console trusts a session it checked with the control plane before asking again. Deactivating someone reaches this console within this long. Overridable (Security) |
| `CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` | int | `10` | Sign-in attempts one client address may make per minute before the console answers `429`. Overridable (Security) |

Session lifetimes, password rules and lockouts are the control plane's settings
([18 §18.11](../../docs/handbook/18-identity-and-access.md)); the console follows them.

## 3.4 Live refresh cadence

These four variables define the entire real-time behaviour. See
[05 — Live store](./05-live-store-and-realtime.md#53-cadence-and-backoff) for how they interact.

| Variable | Type | Default | Applies when |
|----------|------|---------|--------------|
| `CONSOLE_LIST_POLL_MS` | int | `2000` | At least one subscriber, none watching a detail |
| `CONSOLE_DETAIL_POLL_MS` | int | `1500` | At least one detail stream is open. Also the staleness threshold for on-demand `getDetail` |
| `CONSOLE_FLEET_DETAIL_POLL_MS` | int | `6000` | Rotation interval for details nobody is watching |
| `CONSOLE_IDLE_POLL_MS` | int | `30000` | No subscribers at all |
| `CONSOLE_DETAIL_CONCURRENCY` | int | `6` | Parallel detail fetches per refresh pass |
| `CONSOLE_TOPIC_CATALOG_TTL_MS` | int | `15000` | How long the topic list is served before a read refreshes it in the background |
| `CONSOLE_TOPIC_CATALOG_MIN_REFRESH_MS` | int | `5000` | Floor between topic-list refreshes, however many browsers ask |
| `CONSOLE_SERIES_POINTS` | int | `240` | Points kept in every throughput series |
| `CONSOLE_SERIES_SAMPLE_MS` | int | `5000` | Least spacing between those points; the newest point still follows every poll |
| `CONSOLE_INGEST_RATE_WINDOW_MS` | int | `60000` | Span each runtime member's row rate is measured over, between its own heartbeats |
| `CONSOLE_WRITE_RATE_WINDOW_MS` | int | `900000` | Span each member's Parquet byte rate is fitted over (a least-squares slope) |
| `CONSOLE_LAG_HISTORY_MS` | int | `21600000` (6 h) | How long consumer-lag history is kept, one point per measurement |

### Sizing the series window

`CONSOLE_SERIES_POINTS` is a count, but points are at least `CONSOLE_SERIES_SAMPLE_MS` apart, so
the span is steady whoever is watching:

| Situation | Poll interval | Point spacing | 240 points cover |
|-----------|---------------|---------------|------------------|
| Detail tab open | ~1.5 s | 5 s | ~20 minutes |
| Fleet list open | ~2 s | 5–6 s | ~20–24 minutes |
| Background rotation | ~6 s | ~6 s | ~24 minutes |

Memory cost is roughly `240 samples × ~64 bytes × (materializations + 1)` — a few megabytes even
for a large fleet. Consumer-lag history adds at most 720 points per materialization.

### Rate windows

Rates come from each runtime member's own heartbeats (every 30 s by default), not from the poll
interval — see [05 §5.4](./05-live-store-and-realtime.md#54-throughput-derivation). The ingest
window trades responsiveness for smoothness: at the default minute, a member's rate averages two
heartbeats. Bytes are counted when a batch seals into a Parquet object, in waves every few
minutes, so the write window should span several waves; below about five minutes the write rate
swings between zero and a whole wave. If the runtime heartbeat interval is raised, raise the ingest
window to at least twice it.

## 3.5 Storage locations

| Variable | Type | Default | Contents |
|----------|------|---------|----------|
| `CONSOLE_DATA_DIR` | string | `<os.tmpdir()>/materializer-console` | Parent directory for all local state |
| `CONSOLE_PARQUET_CACHE_DIR` | string | `<CONSOLE_DATA_DIR>/parquet-cache` | Mirrored data files (cached mode only) |
| `CONSOLE_PARQUET_CACHE_MAX_BYTES` | int | `8589934592` (8 GiB) | Eviction budget for the file cache |

Nothing under `CONSOLE_DATA_DIR` needs to survive a restart: runs are discarded with the process,
and the file cache refills. 1.0 keeps no schema history of its own — the control plane does — so the
old `schema-history/` directory can be deleted. See
[13 — Operations runbook](./13-operations-runbook.md#133-persistence).

## 3.6 Data access

| Variable | Type | Default | Effect |
|----------|------|---------|--------|
| `CONSOLE_VERSION` | string | from `package.json` | Reported version; override only if a build system stamps something more specific |
| `CONSOLE_DATA_ACCESS_ENABLED` | bool | `true` | Master switch for the Data tab |
| `CONSOLE_GCS_HMAC_KEY_ID` | string | unset | GCS HMAC **access ID** (`GOOG1E…`). Both key and secret are required to enable direct mode |
| `CONSOLE_GCS_HMAC_SECRET` | string | unset | GCS HMAC secret (40 characters of base64) |
| `CONSOLE_DUCKDB_MEMORY_LIMIT` | string | `4GB` | Passed to DuckDB as `memory_limit`, for the shared instance and each statement's sandbox. Also bounds the in-memory cache of file bytes already read |
| `CONSOLE_DUCKDB_THREADS` | int | `32` | DuckDB worker threads. Remote reads block a thread per range request, so this bounds the requests one query has in flight; see below |
| `CONSOLE_QUERY_STATS_TIMEOUT_MS` | int | `8000` | Budget for column profiles; an overrun returns a `partial` profile |
| `CONSOLE_CACHED_MODE_MAX_FILES` | int | `400` | Files one scan may mirror in `cached` mode; a wider time range is refused with a request to narrow it. Pages are never refused |
| `CONSOLE_CACHED_MODE_MAX_BYTES` | int | `2147483648` (2 GiB) | The same, in bytes |
| `CONSOLE_SNAPSHOT_CACHE_MS` | int | `2000` | How old a table's pointer may be for a run: Run includes what was committed moments before it was pressed (06 §6.2) |
| `CONSOLE_TABLE_METADATA_FRESH_MS` | int | `5000` | How old a pointer the Data tab's summary and estimates accept as current |
| `CONSOLE_CATALOG_MAX_STALE_MS` | int | `300000` (5 min) | How old a pointer may be served at once while a fresh one is read in the background. Runs always ask for a fresh one |
| `CONSOLE_RUN_CONCURRENCY` | int | `4` | Runs scanning at once; more wait their turn. Runs the manifests answer alone never wait |
| `CONSOLE_RUN_SCAN_CONCURRENCY` | int | `48` | Files one run reads in parallel: a browse run counting, a statement reading footers |
| `UV_THREADPOOL_SIZE` | int | derived | Node's worker pool. Unset, the console sets it at startup to `RUN_CONCURRENCY × RUN_SCAN_CONCURRENCY + 32` (224 by default), because every DuckDB query in flight holds one of its threads; see below |
| `CONSOLE_RUN_RETENTION_MS` | int | `1800000` (30 min) | How long a run is kept after it was last read; its stored result goes with it |
| `CONSOLE_RUN_DIR` | string | `<data dir>/runs` | Where statement results are stored: a directory per statement run, removed with the run, and Parquet exports while they download |
| `CONSOLE_RESULT_ROW_LIMIT` | int | `1000000` | Rows a statement's result keeps for paging and export. Its total is always counted in full |
| `CONSOLE_EXPORT_ROW_LIMIT` | int | `1000000` | Most rows one export contains |
| `CONSOLE_PAGE_ROW_LIMIT` | int | `1000` | Most rows one page request returns |

### Threads, the worker pool, and scan width

Reading object storage is bound by round-trip latency, not CPU: a cold object costs four sequential
requests (a HEAD, two footer reads, then its column chunks in parallel), about 250 ms from a laptop.
What matters is how many objects are in flight, and three settings decide it.

- **Node's worker pool.** `@duckdb/node-api` runs each query on a libuv worker thread and holds it
  until the query finishes, including all the time it waits on GCS. The pool defaults to four
  threads, which silently capped every run at four objects in flight whatever the scan width said,
  and queued file reads and DNS lookups behind slow queries. `server/workerPool.ts` sizes it before
  anything starts it. Measured: 200 cold object counts took 12.8 s on the default pool, 4.9 s on 64.
- **`CONSOLE_RUN_SCAN_CONCURRENCY`** — objects one run reads at once. A browse run starts eight
  workers on the newest objects, which decide the first page, and adds the rest as soon as one
  finishes.
- **`CONSOLE_DUCKDB_THREADS`** — DuckDB's own threads, shared by every query. A single statement
  over many objects uses them for its parallelism; many small per-object queries split across them.

Measured from a laptop against GCS, cold, filtering all 1,866 objects (101 M rows, 25 GB) of the
load-test materialization:

| Pool | Scan width | DuckDB threads | First page ready | All counted |
|------|------------|----------------|------------------|-------------|
| 4 (Node default) | 24 | 32 | 1.5 s | 71–80 s |
| 224 | 48 | 32 | 0.65 s | 25 s |
| 224 | 64 / 96 | 32 | 1.1 / 1.2 s | 21 / 19 s |
| 224 | 48 | 96 / 192 | 0.4 / 0.5 s | 44 / 49 s |

More DuckDB threads slow browse runs, because each per-object query fans out across them and the
requests in flight multiply. They help a single large statement — an all-time aggregate scanned in
62 s on 32 threads and 43 s on 96 — but the setting is shared, and browse runs are the common case.
A pod short on CPU can lower the scan width; one far from its object store gains most from raising
it.

### Derived: `dataAccessMode`

```ts
export const dataAccessMode: 'direct' | 'cached' | 'disabled' = !config.dataAccessEnabled
  ? 'disabled'
  : config.gcsHmacKeyId && config.gcsHmacSecret
    ? 'direct'
    : 'cached';
```

| Mode | Requires | Behaviour |
|------|----------|-----------|
| `direct` | Both HMAC variables | DuckDB installs `httpfs`, registers a `GCS` secret, and scans `gs://` in place. Nothing is downloaded |
| `cached` | ADC credentials for listing and download | The pruned object set is mirrored to `CONSOLE_PARQUET_CACHE_DIR`, then scanned locally |
| `disabled` | — | `describeDataset` returns `available: false`; `buildDatasetScan` throws 503 |

Note that **listing always uses ADC** via `@google-cloud/storage`, in both direct and cached mode.
HMAC credentials only affect how DuckDB reads the object bytes. A deployment therefore needs ADC
regardless, and HMAC only as an optimisation.

## 3.7 Client-visible configuration

`GET /api/config` exposes a deliberately small subset so the UI can adapt without leaking
deployment detail:

```json
{
  "controlPlaneUrl": "http://localhost:6065",
  "dataAccessMode": "direct",
  "pageRowLimit": 1000,
  "exportRowLimit": 1000000,
  "resultRowLimit": 1000000,
  "controlPlaneVersion": "4.0.0",
  "controlPlaneCompatible": true,
  "materializationDefaults": {
    "destinationUri": "gs://kafka-materializations",
    "namespace": "midstream",
    "replicas": 1,
    "maxReplicas": 64,
    "storageSize": "64Mi",
    "partitionGranularity": "day",
    "labelMaxLength": 128,
    "datasetNameMaxLength": 63,
    "tableNameMaxLength": 63,
    "reservedNames": ["all", "alter", "…"],
    "reservedNamespaceNames": ["main", "memory", "…"],
    "maxTopics": 20
  },
  "pollIntervalMs": 2000,
  "version": "4.0.0"
}
```

`materializationDefaults` is the control plane's `GET /system/settings`, cached for a minute (a
failure is not cached; a setting saved through the console clears it at once). Its
`destinationUri`, `namespace` and `storageSize` are what every new materialization gets — the
create dialog shows the destination and the storage, and refuses to create without a
destination. It is `null` while the control plane cannot be reached or predates the
endpoint; the create dialog then names no destination, leaves the refusal to the control plane, and
checks dataset and table names against the console's own copy of the rules (`shared/tableNames.ts`).

Credentials, paths, and cache budgets are never exposed.

## 3.8 Full example: production values

```bash
CONTROL_PLANE_URL=http://develop-materializer-control-plane-app-api.midstream.svc.cluster.local:6065
PORT=8787
LOG_LEVEL=info

# A roomy volume for run results and the cached-mode file cache
CONSOLE_DATA_DIR=/var/lib/materializer-console
CONSOLE_PARQUET_CACHE_MAX_BYTES=4294967296

# Direct gs:// scanning — no downloads, no local cache growth
CONSOLE_GCS_HMAC_KEY_ID=GOOG1E...
CONSOLE_GCS_HMAC_SECRET=...

# Give DuckDB room on a larger pod
CONSOLE_DUCKDB_MEMORY_LIMIT=6GB
CONSOLE_DUCKDB_THREADS=32
```

## 3.9 Tuning cheat sheet

Everything below except the HMAC credentials and `CONSOLE_RUN_SCAN_CONCURRENCY` can also be changed
on the Settings page, without a restart (§3.10).

| Goal | Change |
|------|--------|
| Reduce control-plane load | Raise `CONSOLE_LIST_POLL_MS` and `CONSOLE_FLEET_DETAIL_POLL_MS` |
| Snappier detail view | Lower `CONSOLE_DETAIL_POLL_MS` (also shortens the series window) |
| Longer charts | Raise `CONSOLE_SERIES_POINTS` or `CONSOLE_SERIES_SAMPLE_MS` |
| Steadier or more responsive rates | Raise or lower `CONSOLE_INGEST_RATE_WINDOW_MS` |
| Large fleet, slow refresh | Raise `CONSOLE_DETAIL_CONCURRENCY` — but that multiplies concurrent control-plane requests |
| Bigger statement results or exports | Raise `CONSOLE_RESULT_ROW_LIMIT` / `CONSOLE_EXPORT_ROW_LIMIT`; results are stored on disk under `CONSOLE_RUN_DIR` |
| Stop downloading Parquet | Configure HMAC credentials to switch to direct mode |
| Faster cold scans far from the object store | Raise `CONSOLE_RUN_SCAN_CONCURRENCY` (64–96 counted the table above in 19–21 s instead of 25 s, with the first page ~0.5 s later) |
| Read-only deployment | `CONSOLE_DATA_ACCESS_ENABLED=false` hides the Data tab entirely |

## 3.10 Settings the Settings page can override

Precedence, for each of these: **an override chosen on the Settings page → the environment
variable → the built-in default.** The page shows which applies, who set an override and when,
and what a reset returns to.

| Section | Key → variable |
|---------|----------------|
| Live updates | `listPollIntervalMs` → `CONSOLE_LIST_POLL_MS`, `detailPollIntervalMs` → `CONSOLE_DETAIL_POLL_MS`, `fleetDetailPollIntervalMs` → `CONSOLE_FLEET_DETAIL_POLL_MS`, `idlePollIntervalMs` → `CONSOLE_IDLE_POLL_MS`, `detailConcurrency` → `CONSOLE_DETAIL_CONCURRENCY`, `seriesRetention` → `CONSOLE_SERIES_POINTS`, `seriesSampleMs` → `CONSOLE_SERIES_SAMPLE_MS`, `ingestRateWindowMs` → `CONSOLE_INGEST_RATE_WINDOW_MS`, `writeRateWindowMs` → `CONSOLE_WRITE_RATE_WINDOW_MS`, `lagHistoryMs` → `CONSOLE_LAG_HISTORY_MS` |
| Data explorer | `dataAccessEnabled` → `CONSOLE_DATA_ACCESS_ENABLED`, `pageRowLimit` → `CONSOLE_PAGE_ROW_LIMIT`, `resultRowLimit` → `CONSOLE_RESULT_ROW_LIMIT`, `exportRowLimit` → `CONSOLE_EXPORT_ROW_LIMIT`, `queryStatsTimeoutMs` → `CONSOLE_QUERY_STATS_TIMEOUT_MS`, `runIdleRetentionMs` → `CONSOLE_RUN_RETENTION_MS`, `snapshotCacheMs` → `CONSOLE_SNAPSHOT_CACHE_MS`, `tableMetadataFreshMs` → `CONSOLE_TABLE_METADATA_FRESH_MS`, `catalogMaxStaleMs` → `CONSOLE_CATALOG_MAX_STALE_MS`, `cachedModeMaxFiles` → `CONSOLE_CACHED_MODE_MAX_FILES`, `cachedModeMaxBytes` → `CONSOLE_CACHED_MODE_MAX_BYTES` |
| Topics & Kafka | `topicCatalogTtlMs` → `CONSOLE_TOPIC_CATALOG_TTL_MS`, `topicCatalogMinRefreshMs` → `CONSOLE_TOPIC_CATALOG_MIN_REFRESH_MS` |
| Diagnostics | `logLevel` → `LOG_LEVEL`, `controlPlaneTimeoutMs` → `CONTROL_PLANE_TIMEOUT_MS` |
| Security | `sessionCheckMs` → `CONSOLE_SESSION_CHECK_MS`, `signInAttemptsPerMinute` → `CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` |
| Live tail | `liveTailEnabled` → `CONSOLE_LIVE_TAIL_ENABLED` (on), `liveTailRefreshMs` → `CONSOLE_LIVE_TAIL_REFRESH_MS` (1,500; 500–60,000), `liveTailMaxRows` → `CONSOLE_LIVE_TAIL_MAX_ROWS` (100,000 rows one fetch takes; 1,000–10,000,000), `liveTailTimeoutMs` → `CONSOLE_LIVE_TAIL_TIMEOUT_MS` (3,000; 500–30,000: past it a run goes on without its tail) |

Each has bounds in `server/settings/catalog.ts` (for example 500 ms to 10 min for the fleet
refresh), and the page refuses a value outside them.

The Live tail section also holds the control plane's nine `live_tail_*` settings (17 §17.3): one
section, settings of two scopes, as Security has.

**How an override applies.** `config.ts` declares these 31 in `tunableDeployment` and defines
each as a getter on `config`, so every read sees the override in force. Code must read
`config.<key>` where the value is used, never copy it at startup. Overrides are stored by the
control plane, in its `console` settings scope. `ConsoleSettingsStore` (`server/settings/store.ts`)
reads them at start, every 30 seconds, and right after a save. It validates each against the
console's catalog and installs the valid ones with `applySettingOverrides`. So every replica
converges within 30 seconds. A stored value this console does not accept, perhaps written by an
older or newer console, is ignored and logged. A log-level change is applied to the logger at
once.

**Through an outage.** Until the control plane has been reached once, the console runs on the
environment's values. After that, the last overrides it read stay in force while the control
plane is unreachable. The Settings page is then read-only, because the overrides are stored there.

**Not overridable.** Everything that sizes the pod, wires it to its environment, or is read once
at startup stays with the deployment. The Settings page lists these read-only under
**Deployment**: `PORT`/`HOST`, `CONTROL_PLANE_URL`, `CONSOLE_TAIL_URL`, `CONSOLE_VERSION`, the HMAC credentials,
`CONSOLE_DUCKDB_MEMORY_LIMIT`, `CONSOLE_DUCKDB_THREADS`, `CONSOLE_RUN_CONCURRENCY`,
`CONSOLE_RUN_SCAN_CONCURRENCY` (they size `UV_THREADPOOL_SIZE` at start), `CONSOLE_DATA_DIR`,
`CONSOLE_PARQUET_CACHE_MAX_BYTES`, `CONSOLE_CORS_ORIGIN`, `CONSOLE_SERVE_CLIENT`, and the
sign-in wiring — `CONSOLE_SERVICE_TOKEN` (shown only as configured or not),
`CONSOLE_SESSION_COOKIE` and `CONSOLE_COOKIE_SECURE`.

Changing any of these settings needs `settings.manage`; reading them, `settings.view`.

The control plane's and runtimes' settings are on the same page. They are described in the main
handbook, [17 — Settings](../../docs/handbook/17-settings.md).

## 3.x Obtaining GCS HMAC credentials

Direct mode is not a separate switch: `dataAccessMode` is derived, and setting **both** variables
turns it on. They are read once at module load, so a restart is required.

An HMAC key is **not** a service account JSON key file. It is an S3-compatible credential pair
created *for* a service account, and it is what DuckDB needs: its `httpfs` extension speaks the S3
protocol, so it authenticates against the GCS XML API with HMAC and cannot use ADC or a JSON key.

The console therefore uses two different credential paths — ADC through the Node SDK for listing
objects, HMAC through DuckDB for scanning them. Listing working is no evidence that scanning will.

```bash
gcloud storage hmac create <service-account-email>
```

The `accessId` is ~61 characters beginning `GOOG1E`; the `secret` is 40 characters of base64 and is
shown **once**. The service account needs read access to the destination bucket, and a project
allows at most five keys per service account.

> **If the access ID is malformed**, GCS rejects the signed header and DuckDB reports
> `MalformedSecurityHeader … this can be caused by the S3 region being set incorrectly. * No region
> is provided.` That hint is wrong — GCS ignores the region, and the same text appears on requests
> that succeed. The console checks the credential shape at startup and rewrites this message so the
> real cause is named.
