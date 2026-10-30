# Midstream Console

The web console of Midstream: one console for every **domain** — each a Midstream deployment with
its own control plane, runtimes, accounts and the Iceberg tables its materializations fill, one per
source topic, each in a dataset. One URL, partitioned by domain (`/d/<domain>`), with a domain
switcher, pages that show every domain at once, search, SQL across domains, sign-in through Microsoft
Entra ID, GitHub, Google or any OpenID Connect provider, and one Iceberg REST catalog over every
domain. Each control plane must be of the same major version.

The console is a self-contained application: a small Node backend-for-frontend (BFF) plus a
React client. It has **no source dependency on `python/` or `charts/`** — it talks to each
control plane exclusively over its public HTTP API, so this directory can be lifted into its own
repository without changes beyond its domain registry (`CONSOLE_DOMAINS`).

> **Full reference:** [`docs/`](./docs/README.md) is a 17-document handbook covering architecture,
> configuration, server internals, the real-time model, reading Iceberg tables, DuckDB data access,
> the HTTP API, the client, the design system, sign-in and security, operations, testing, and
> extension recipes.
> Start at [docs/README.md](./docs/README.md).

## What it does

| Area | Capability |
|------|-----------|
| Domains | A switcher in the header between *All domains* and each domain; every page under `/d/<domain>` is that domain's, and the Overview, Materializations, Datasets and Search show every domain at once, each item saying its domain; a banner for a domain that cannot be reached, is misconfigured, or holds another federation key |
| Fleet overview | Live phase distribution, ingest/write rates, totals, and an attention queue — of one domain or all |
| Materializations | Live searchable/sortable list (by label, id, topic, dataset or table), create — one or more source topics, each becoming a table in the dataset chosen for it and named after it unless renamed — stop, resume, edit the label, topics and replicas, delete |
| Datasets | Every dataset with its owners and tables, locked ones marked; create (also inline while picking topics), edit the description, delete when empty; an Access tab where owners grant Dataset Reader, Writer and Owner to people and groups |
| Detail | Live ingest and write rates, the tables' rows and size (the Data tab's counts) and how complete they are, a panel with each table (`dataset.table`, locked where the person cannot read its dataset) and its rows, size, lag, schema and state, throughput and consumer-lag charts with a catch-up estimate, reconciliation, per-member state with the committer marked, spec |
| Table | Each Iceberg table: snapshots, maintenance history with **Run now**, files by partition, properties, and connection snippets for DuckDB, PyIceberg, Spark and Trino — PyIceberg and Spark through the federated catalog (`lake.<domain>.<dataset>.<table>`), DuckDB and Trino through the domain's own |
| Runtime | KasprApp observation, readiness conditions, managed agent states, runtime binding |
| Schema | Every schema each table has had — for the tables whose datasets the person may read — fork-aware field browser and tree with field ids, schema-to-schema diff by field id |
| Data | Any of the materialization's tables the person may query, chosen with a picker, or in SQL any table of any domain they may query there (`<domain>.<dataset>.<table>`): the newest rows with an exact total at once — with live tail on, records seconds after they are produced, before the runtime commits them, followed by **Live** and each shown once; Splunk-style time ranges on the Kafka record time; searchable filters or SQL — over flat logical columns or the native tables, joining the materialization's tables — behind one **Run** button, with exact progress, instant paging and export of the finished result as CSV, nested or flat JSON, or Parquet, under **More actions** with **Share** and **Peek** |
| Search | Materializations, tables and datasets of every domain, by name, id, topic or description (⌘K) |
| Settings | Two pages: the console's own (stored in the settings home; the domains and how each is reached; your account in every domain; every domain's history) and each domain's (its control plane and runtimes, its directory, its sign-in settings). Change behaviour without a redeploy: each setting shows where its value comes from and can be reset; runtime changes show their delivery to running runtimes; a change history and the deployment's fixed settings |
| Accounts and access | Sign in through a provider, in every domain at once, or with an email and password, one domain at a time; your account, password, sessions and federated access tokens for external engines, honoured in every domain, optionally limited to some datasets; for administrators, users (add with a generated initial password, groups, reset, sign out, unlock, deactivate), groups, roles built from the permission catalog (each permission says whether it holds per dataset), every person's and group's dataset grants, and an audit log. Pages and actions follow each person's permissions, on each dataset; guests see the list and status |

Everything the control plane exposes is surfaced. Metrics that the control plane only publishes
as cumulative counters (rows, bytes, files, skipped) are turned by the BFF into per-second rates —
each runtime member's counters differenced between its own heartbeats, then summed — and a rolling
time series. Fields carry the logical names the schema gives them everywhere — `customer.email`,
`items[].sku`, one column per fork family — in the grid, filters, SQL and exports.

## Architecture

```
browser ──SSE──┐                                  ┌─HTTP─▶ Domain A's control plane :6065 (+ tail app)
               ├─ console server (Fastify) ───────┼─HTTP─▶ Domain B's control plane (another cluster)
browser ──HTTP─┘        │                         └─HTTP─▶ …
                        ├─ a live store per domain  poll + per-member rates + lag history + fan-out
                        ├─ Iceberg reader           pointer → metadata → manifests, per snapshot (GCS, S3, Azure)
                        ├─ DuckDB                   runs over snapshots' files, across domains
                        ├─ sign-in providers        Entra ID, GitHub, Google, OpenID Connect
                        └─ /catalog                 one Iceberg REST catalog over every domain
```

### Why a BFF instead of calling the control plane directly

- **Real time.** The control-plane API is request/response only. The BFF polls each domain once on
  behalf of every connected browser and fans results out over SSE, so ten open tabs cost each
  control plane the same as one. Polling backs off to 30 s when nobody is watching.
- **One place for every domain.** Merging fleets, searching, joining tables across domains and
  signing in to every domain at once all need something that talks to every control plane.
- **Derived metrics.** Throughput rates require following each runtime member's counters across
  its heartbeats; that state has to live somewhere shared.
- **Data access.** Reading Iceberg metadata and data files needs object-storage credentials and
  DuckDB, which belong on a server, not in a browser.

## Running locally

```bash
npm install
cp .env.example .env          # unset CONSOLE_DOMAINS: one domain, `local`, at localhost:6065

# In another shell, expose the dev control plane:
kubectl -n midstream port-forward svc/midstream-control-plane-app-api 6065:6065

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
| `CONSOLE_DOMAINS_FILE` / `CONSOLE_DOMAINS` | one domain, `local`, at `http://localhost:6065` | The registry: each domain's id, name, `controlPlaneUrl`, optional `tailUrl` (its live tail), and which one is the settings home. The chart renders it from `console.domains[]` |
| `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN` | unset | Each domain's control plane's `AUTH_SERVICE_TOKEN`; lets that domain's live views read for everyone |
| `CONSOLE_URL` | the URL each request arrived at | The console's public URL: where sign-in providers send people back to |
| `CONSOLE_SSO_*` | unset | Each sign-in provider the console is registered with ([docs/03 §3.3a](docs/03-configuration-reference.md)) |
| `PORT` | `8787` | Console server port |
| `CONSOLE_LIST_POLL_MS` | `2000` | Fleet refresh cadence while a client is connected |
| `CONSOLE_DETAIL_POLL_MS` | `1500` | Refresh cadence for a materialization with an open detail view |
| `CONSOLE_IDLE_POLL_MS` | `30000` | Cadence when no client is connected |
| `CONSOLE_STORAGE_FILE` / `CONSOLE_STORAGE` | none: no object storage is read | The storage profiles: one per destination prefix (`gs://`, `s3://`, `abfss://`), its kind, endpoint, region, path style and access. The chart renders it from `console.storage[]` |
| `CONSOLE_STORAGE_<NAME>_…` | unset | Each profile's credentials: `GCS_HMAC_KEY_ID`/`_SECRET`; `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN`; `AZURE_CONNECTION_STRING`, `AZURE_ACCOUNT_KEY` or a service principal. Read-only ones |
| `CONSOLE_RUN_SCAN_CONCURRENCY` | `48` | Files one run reads in parallel; Node's worker pool is sized from it at startup |
| `CONSOLE_RESULT_ROW_LIMIT` | `1000000` | Rows a statement's result keeps for paging and export; its total is always counted |
| `CONSOLE_SNAPSHOT_CACHE_MS` / `CONSOLE_TABLE_METADATA_FRESH_MS` | `2000` / `5000` | How old a table's pointer may be for a run / for the summary |
| `CONSOLE_DATA_DIR` | OS temp dir | Run results and the cached-mode file cache |

### Storage profiles

Each object is read with the storage profile whose scope is the longest prefix of its URI, as DuckDB
picks among scoped secrets ([docs/07 §07.10](docs/07-data-access-and-duckdb.md)); a table outside every
scope answers "no storage is configured for …".

| Mode | When | Behaviour |
|------|------|-----------|
| `direct` | an S3 or Azure profile; a GCS profile with an HMAC key pair | DuckDB reads data files in place with the profile's scoped secret; nothing is downloaded |
| `cached` | a GCS profile without an HMAC pair, or `"access": "cached"` | Data files are mirrored to a local LRU cache first, under `<cache>/<profile>/<container>/…` |

`cached` profiles are bounded by `CONSOLE_CACHED_MODE_MAX_FILES` and `CONSOLE_CACHED_MODE_MAX_BYTES`
per scan. Narrow the time range to stay inside them — files outside it are pruned on their manifest
bounds *before* anything is downloaded. Pages read one file at a time and are never refused. With
`CONSOLE_DATA_ACCESS_ENABLED=false` the Data tab is unavailable.

## Query safety

The SQL console is deliberately powerful but sandboxed:

- Exactly one statement, which must begin with a projection keyword (`SELECT`, `WITH`, …).
- Statement kinds and file-access functions that could escape the prepared view are rejected
  (`ATTACH`, `COPY`, `INSTALL`, `read_csv`, `iceberg_scan`, `glob`, …). String literals and comments
  are stripped before keyword scanning so data cannot smuggle a keyword past the guard.
- Every statement runs in a DuckDB sandbox of its own: external access off and configuration locked,
  with only the run's snapshot files readable — whatever the guard misses, DuckDB refuses.
- Queries read a view of each table they name, in any domain, by `<domain>.<dataset>.<table>`,
  and `<domain>_iceberg.<dataset>.<table>` with the native columns, scoped to the run's snapshot and
  time range. A statement naming a table in a dataset the person may not query in its domain is
  refused, naming the dataset.
- A statement's stored result keeps at most `CONSOLE_RESULT_ROW_LIMIT` rows, at most
  `CONSOLE_RUN_CONCURRENCY` runs scan at once, **Cancel** interrupts a run's queries at once, and a
  run nobody has read for `CONSOLE_RUN_RETENTION_MS` is cancelled and deleted.

Filters never interpolate raw user input: identifiers are quoted and values are emitted as escaped
SQL literals.

## Sign-in and access

People sign in with the accounts each domain's control plane keeps: through a provider, in every
domain at once — a domain whose first-sign-in policy is `open` makes an account for someone it has
never seen — or with an email and password, one domain at a time. The console is not an identity
provider: each domain checks what the provider vouched for itself. The console holds a browser's
session in each domain in an HttpOnly cookie of its own, checks the permission each `/api` route
needs in the route's domain — on the datasets a request is about, for a dataset permission — and
sends every change with the person's own session there, so the control plane checks it again. The
live views are read once per domain with that domain's service token and cut down for each person
before they are sent: a table's schema reaches only those who may read its dataset.
Without a session a caller is a guest, who by default sees the materialization list and status.
On a new control plane, sign in as its bootstrap administrator and add people under the domain's
**Settings → Users**. See [docs/12](docs/12-security-model.md) and the control-plane handbook's
[18 — Identity and access](../docs/handbook/18-identity-and-access.md).

## Layout

```
docs/                    Handbook — read docs/README.md first
shared/types.ts          Wire contracts shared by server and client
server/
  config.ts              Environment-driven configuration
  domains/               The registry, and each domain: its control-plane client, live store, status
  controlPlane/client.ts Typed client for the control-plane WebViews, one per domain
  settings/              The console's settings catalog and the store that applies overrides
  auth/                  Who is calling, in each domain: session cookies, session checks, permit(),
                         sign-in limits, provider sign-in flows, token revocations across domains
  live/store.ts          Polling, rate derivation, SSE fan-out
  iceberg/               Reading Iceberg tables: pointer, metadata, Avro manifests, snapshots, schema projection
  schema/                Schema diff engine
  storage/               Storage profiles: the registry, GCS, S3 and Azure readers, DuckDB secrets, the cache
  data/                  Snapshot catalog, logical columns, DuckDB and its sandbox, SQL guard
  runs/                  Runs: browse and statement execution, pages, exports, profiles
  routes/                HTTP surface, the domain-scoped routes under /api/d/<domain>, the catalogs
src/
  components/            Design system primitives and shared display components
  features/              Overview, materializations, detail, schema, data, settings, auth, access
  lib/                   API client, SSE hooks, the sessions (auth.tsx), domain paths, the merged
                         fleet, formatting
```

## Commands

```bash
npm run dev          # server + client with reload
npm run build        # production client bundle
npm start            # production server
npm run typecheck    # both TypeScript projects
```
