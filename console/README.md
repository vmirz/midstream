# Materializer Console

Real-time web console for the Kafka materializer: its control plane, its runtimes, and the Iceberg
tables each materialization fills, one per source topic, each in a dataset. Console 3.0 needs
control plane 3.0.

The console is a self-contained application: a small Node backend-for-frontend (BFF) plus a
React client. It has **no source dependency on `python/` or `manifests/`** — it talks to the
control plane exclusively over its public HTTP API, so this directory can be lifted into its own
repository without changes beyond `CONTROL_PLANE_URL`.

> **Full reference:** [`docs/`](./docs/README.md) is a 17-document handbook covering architecture,
> configuration, server internals, the real-time model, reading Iceberg tables, DuckDB data access,
> the HTTP API, the client, the design system, sign-in and security, operations, testing, and
> extension recipes.
> Start at [docs/README.md](./docs/README.md).

## What it does

| Area | Capability |
|------|-----------|
| Fleet overview | Live phase distribution, ingest/write rates, totals, and an attention queue |
| Materializations | Live searchable/sortable list (by label, id, topic, dataset or table), create — one or more source topics, each becoming a table in the dataset chosen for it and named after it unless renamed — stop, resume, edit the label, topics and replicas, delete |
| Datasets | Every dataset with its owners and tables, locked ones marked; create (also inline while picking topics), edit the description, delete when empty; an Access tab where owners grant Dataset Reader, Writer and Owner to people and groups |
| Detail | Live ingest and write rates, the tables' rows and size (the Data tab's counts) and how complete they are, a panel with each table (`dataset.table`, locked where the person cannot read its dataset) and its rows, size, lag, schema and state, throughput and consumer-lag charts with a catch-up estimate, reconciliation, per-member state with the committer marked, spec |
| Table | Each Iceberg table: snapshots, maintenance history with **Run now**, files by partition, properties, and connection snippets for DuckDB, PyIceberg, Spark and Trino (`lake.<dataset>.<table>`) |
| Runtime | KasprApp observation, readiness conditions, managed agent states, runtime binding |
| Schema | Every schema each table has had — for the tables whose datasets the person may read — fork-aware field browser and tree with field ids, schema-to-schema diff by field id |
| Data | Any of the materialization's tables the person may query, chosen with a picker: the newest rows with an exact total at once — with live tail on, records seconds after they are produced, before the runtime commits them, followed by **Live** and each shown once; Splunk-style time ranges on the Kafka record time; searchable filters or SQL — over flat logical columns or the native tables, joining the materialization's tables — behind one **Run** button, with exact progress, instant paging and export of the finished result as CSV, nested or flat JSON, or Parquet, under **More actions** with **Share** and **Peek** |
| Settings | Change how the control plane, its runtimes and this console behave without a redeploy: each setting shows where its value comes from and can be reset; runtime changes show their delivery to running runtimes; a change history and the deployment's fixed settings |
| Accounts and access | Sign in with an email and password; your account, password, sessions and access tokens for external engines, optionally limited to some datasets; for administrators, users (add with a generated initial password, groups, reset, sign out, unlock, deactivate), groups, roles built from the permission catalog (each permission says whether it holds per dataset), every person's and group's dataset grants, and an audit log. Pages and actions follow each person's permissions, on each dataset; guests see the list and status |

Everything the control plane exposes is surfaced. Metrics that the control plane only publishes
as cumulative counters (rows, bytes, files, skipped) are turned by the BFF into per-second rates —
each runtime member's counters differenced between its own heartbeats, then summed — and a rolling
time series. Fields carry the logical names the schema gives them everywhere — `customer.email`,
`items[].sku`, one column per fork family — in the grid, filters, SQL and exports.

## Architecture

```
browser ──SSE──┐
               ├─ console server (Fastify)  ──HTTP──▶  control-plane WebView API :6065
browser ──HTTP─┘        │
                        ├─ live store        poll + per-member rates + lag history + fan-out
                        ├─ Iceberg reader    pointer → metadata → manifests, per snapshot (GCS)
                        └─ DuckDB            runs over one snapshot's files: pages, SQL, exports
```

### Why a BFF instead of calling the control plane directly

- **Real time.** The control-plane API is request/response only. The BFF polls once on behalf of
  every connected browser and fans results out over SSE, so ten open tabs cost the control plane
  the same as one. Polling backs off to 30 s when nobody is watching.
- **Derived metrics.** Throughput rates require following each runtime member's counters across
  its heartbeats; that state has to live somewhere shared.
- **Data access.** Reading Iceberg metadata and data files needs object-storage credentials and
  DuckDB, which belong on a server, not in a browser.

## Running locally

```bash
npm install
cp .env.example .env          # point CONTROL_PLANE_URL at your control plane

# In another shell, expose the dev control plane:
kubectl -n midstream port-forward svc/develop-materializer-control-plane-app-api 6065:6065

npm run dev                   # server on :8787, Vite client on :5173
```

Open http://localhost:5173. The Vite dev server proxies `/api` to the console server.

### Production

```bash
npm run build                 # emits dist/
npm start                     # serves the API and the built client on :8787
```

The server serves `dist/` when it exists, with SPA fallback for client routes.

## Configuration

All configuration starts from the environment; see [.env.example](.env.example) for the full list.
Most tunables can then be overridden from the Settings page while the console runs, and an
override wins until it is reset ([docs/03](docs/03-configuration-reference.md)).

| Variable | Default | Purpose |
|----------|---------|---------|
| `CONTROL_PLANE_URL` | `http://localhost:6065` | Control-plane API base URL |
| `CONSOLE_SERVICE_TOKEN` | unset | The control plane's `MATERIALIZER_AUTH_SERVICE_TOKEN`; lets the live views read for everyone |
| `PORT` | `8787` | Console server port |
| `CONSOLE_LIST_POLL_MS` | `2000` | Fleet refresh cadence while a client is connected |
| `CONSOLE_DETAIL_POLL_MS` | `1500` | Refresh cadence for a materialization with an open detail view |
| `CONSOLE_IDLE_POLL_MS` | `30000` | Cadence when no client is connected |
| `CONSOLE_GCS_HMAC_KEY_ID` / `_SECRET` | unset | Enables direct `gs://` scans in DuckDB |
| `CONSOLE_RUN_SCAN_CONCURRENCY` | `48` | Files one run reads in parallel; Node's worker pool is sized from it at startup |
| `CONSOLE_RESULT_ROW_LIMIT` | `1000000` | Rows a statement's result keeps for paging and export; its total is always counted |
| `CONSOLE_SNAPSHOT_CACHE_MS` / `CONSOLE_TABLE_METADATA_FRESH_MS` | `2000` / `5000` | How old a table's pointer may be for a run / for the summary |
| `CONSOLE_DATA_DIR` | OS temp dir | Run results and the cached-mode file cache |
| `CONSOLE_TAIL_URL` | unset | The tail app's Service: the newest records of each table before they are committed (live tail); unset, committed rows only |

### Data access modes

| Mode | When | Behaviour |
|------|------|-----------|
| `direct` | GCS HMAC credentials are configured | DuckDB reads `gs://` in place; nothing is downloaded |
| `cached` | No HMAC credentials (uses ADC) | The pruned file set is mirrored to a local LRU cache first |
| `disabled` | `CONSOLE_DATA_ACCESS_ENABLED=false` | The Data tab is unavailable |

`cached` mode is bounded by `CONSOLE_CACHED_MODE_MAX_FILES` and `CONSOLE_CACHED_MODE_MAX_BYTES`
per scan. Narrow the time range to stay inside them — files outside it are pruned on their manifest
bounds *before* anything is downloaded. Pages read one file at a time and are never refused.

## Query safety

The SQL console is deliberately powerful but sandboxed:

- Exactly one statement, which must begin with a projection keyword (`SELECT`, `WITH`, …).
- Statement kinds and file-access functions that could escape the prepared view are rejected
  (`ATTACH`, `COPY`, `INSTALL`, `read_csv`, `iceberg_scan`, `glob`, …). String literals and comments
  are stripped before keyword scanning so data cannot smuggle a keyword past the guard.
- Every statement runs in a DuckDB sandbox of its own: external access off and configuration locked,
  with only the run's snapshot files readable — whatever the guard misses, DuckDB refuses.
- Queries read a view of each of the materialization's tables they name, by `<dataset>.<table>`,
  and `iceberg.<dataset>.<table>` with the native columns, scoped to the run's snapshot and time
  range. A statement naming a table in a dataset the person may not query is refused, naming the
  dataset.
- A statement's stored result keeps at most `CONSOLE_RESULT_ROW_LIMIT` rows, at most
  `CONSOLE_RUN_CONCURRENCY` runs scan at once, **Cancel** interrupts a run's queries at once, and a
  run nobody has read for `CONSOLE_RUN_RETENTION_MS` is cancelled and deleted.

Filters never interpolate raw user input: identifiers are quoted and values are emitted as escaped
SQL literals.

## Sign-in and access

People sign in with the accounts the control plane keeps; there is no self-registration. The
console holds a browser's session in an HttpOnly cookie, checks the permission each `/api` route
needs — on the datasets a request is about, for a dataset permission — and sends every change with
the person's own session so the control plane checks it again. The live views are read once with
the console's own credential and cut down for each person before they are sent: a table's schema
reaches only those who may read its dataset.
Without a session a caller is a guest, who by default sees the materialization list and status.
On a new control plane, sign in as its bootstrap administrator and add people under
**Settings → Users**. See [docs/12](docs/12-security-model.md) and the control-plane handbook's
[18 — Identity and access](../docs/handbook/18-identity-and-access.md).

## Layout

```
docs/                    Handbook — read docs/README.md first
shared/types.ts          Wire contracts shared by server and client
server/
  config.ts              Environment-driven configuration
  controlPlane/client.ts Typed client for the control-plane WebViews
  settings/              The console's settings catalog and the store that applies overrides
  auth/                  Who is calling: session cookie, session checks, permit(), sign-in limits
  live/store.ts          Polling, rate derivation, SSE fan-out
  iceberg/               Reading Iceberg tables: pointer, metadata, Avro manifests, snapshots, schema projection
  schema/                Schema diff engine
  data/                  Snapshot catalog, logical columns, DuckDB and its sandbox, SQL guard, GCS
  runs/                  Runs: browse and statement execution, pages, exports, profiles
  routes/                HTTP surface
src/
  components/            Design system primitives and shared display components
  features/              Overview, materializations, detail, schema, data, settings, auth, access
  lib/                   API client, SSE hooks, the session (auth.tsx), formatting
```

## Commands

```bash
npm run dev          # server + client with reload
npm run build        # production client bundle
npm start            # production server
npm run typecheck    # both TypeScript projects
```
