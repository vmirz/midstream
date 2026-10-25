# Midstream Console Handbook

A complete, code-derived reference for the web application in `/console`. Every claim here is
traceable to a file in this directory; where behaviour is subtle or surprising, the document says
so explicitly rather than describing the intent.

This handbook exists so that a human or an LLM can answer any question about the console —
what it does, why it is built this way, exactly how each piece behaves, and where to change
things — without reading the whole codebase first.

## How to use this handbook

Load the document you need. Each one is self-contained and cross-links the others.

| # | Document | Read it when you need to… |
|---|----------|---------------------------|
| 01 | [Overview and architecture](./01-overview-and-architecture.md) | Understand what the console is, its two processes, and why a BFF exists |
| 02 | [Getting started](./02-getting-started.md) | Run it locally, understand the dev loop, or build for production |
| 03 | [Configuration reference](./03-configuration-reference.md) | Look up any environment variable, default, or derived setting, and which ones the Settings page can override |
| 04 | [Server internals](./04-server-internals.md) | Understand bootstrap, error handling, routing, logging, the control-plane client |
| 05 | [Live store and real time](./05-live-store-and-realtime.md) | Understand polling, cadence, throughput derivation, SSE fan-out |
| 06 | [Reading Iceberg tables and schemas](./06-iceberg-tables.md) | Understand how the console reads a table — pointer, metadata, manifests, snapshots — its schema and logical columns, and schema diffs |
| 07 | [Data access and DuckDB](./07-data-access-and-duckdb.md) | Understand runs over a snapshot, time pruning, paging, statements and their sandbox, exports |
| 08 | [Console API reference](./08-console-api-reference.md) | Call the console's own HTTP API: settings, sign-in, users, groups, roles and the audit log included |
| 09 | [Client architecture](./09-client-architecture.md) | Understand the React app, routing, state, data flow, and how pages follow permissions |
| 10 | [Design system](./10-design-system.md) | Change styling, add a component, or match the visual language |
| 11 | [Feature reference](./11-feature-reference.md) | Understand exactly what every screen shows and does: the Table tab, sign-in, Settings, your account and access tokens, users, groups, roles |
| 12 | [Security model](./12-security-model.md) | Review or extend sign-in, sessions, permission checks, trust boundaries, the SQL guard, and input validation |
| 13 | [Operations runbook](./13-operations-runbook.md) | Deploy it, size it, or debug it in production |
| 14 | [Testing and verification](./14-testing-and-verification.md) | Reproduce the end-to-end verification harness |
| 15 | [Extending the console](./15-extending-the-console.md) | Add an endpoint, a tab, a filter, a metric, or a chart |
| 16 | [Known gaps and roadmap](./16-known-gaps-and-roadmap.md) | Know what is deliberately missing before you assume a bug |
| 17 | [Glossary and FAQ](./17-glossary-and-faq.md) | Look up a term or a "why does it do that?" answer |

## The seven facts that explain most behaviour

1. **The console never writes to the control-plane state table.** Every mutation is a
   pass-through that returns the control plane's `202 Accepted`. The UI then waits for the live
   stream to show convergence. See [01](./01-overview-and-architecture.md#13-request-flow).

2. **Throughput is derived, not reported.** The control plane exposes cumulative counters only.
   The BFF differences each runtime member's counters between that member's own heartbeats, timed
   by the runtime's clock, and sums the members — so the rate is steady when the work is, and its
   dips are real. It keeps a rolling 20-minute series and six hours of consumer-lag measurements.
   See [05](./05-live-store-and-realtime.md#54-throughput-derivation).

3. **Polling cadence depends on who is watching.** 1.5 s for a materialization with an open
   detail stream, 2 s for the fleet list, 6 s rotation for everything else, 30 s when no browser
   is connected. REST reads force a refresh when the snapshot is stale.
   See [05](./05-live-store-and-realtime.md#53-cadence-and-backoff).

4. **The console reads each table itself.** The control plane says where a materialization's
   Iceberg table is; the console reads its pointer, metadata and manifests from GCS and its data
   files through DuckDB, pinned to one snapshot per run. Row counts come from the manifests, so the
   first page of any table is instant. Everything behind the pointer is immutable, so it is cached
   for the life of the process. See [06](./06-iceberg-tables.md).

5. **Queries read each of a materialization's tables by its canonical name**, `<dataset>.<table>`
   — one table per source topic, in the dataset chosen for it: `SELECT * FROM sales.orders`, with
   logical columns (`"customer.email"`, one column per fork family), and `iceberg.sales.orders` with
   the native ones — and may join them, across datasets. Each statement runs in a DuckDB sandbox
   that can read only the snapshot files of the tables it names; the SQL is also checked as a single
   read-only statement, and results are capped (`shared/tableNames.ts` derives table names as the
   control plane does). See [07](./07-data-access-and-duckdb.md) and [12](./12-security-model.md).

6. **Settings are stored with the control plane, even the console's own.** The Settings page
   overrides settings of the control plane, its runtimes and this console without a restart. An
   override wins over the environment variable until it is reset. The console reads its overrides
   back every 30 seconds, so every replica converges. See
   [03 §3.10](./03-configuration-reference.md) and [11 §11.8b](./11-feature-reference.md).

7. **Every request has a caller, and the control plane decides what they may do.** A browser
   signs in through the console, which keeps the control-plane session in an HttpOnly cookie;
   every `/api` route names the permission it needs, and changes travel with the person's own
   session, so the control plane checks them again. Access to data is per dataset: schemas, queries,
   exports and the catalog are checked on the datasets of the tables a request is about, and the
   shared detail is cut down for each person (`visibleDetail`). Shared live views are read once with
   a read-only service token. Without a session the caller is a guest, who by default sees the
   list and status only. See [12](./12-security-model.md) and the control-plane handbook's
   [18 — Identity and access](../../docs/handbook/18-identity-and-access.md).

## Source map

```
console/
  shared/types.ts              Wire contracts shared by server and client
  shared/fieldPaths.ts         Field naming without the `payload.` root; nested JSON from field paths
  shared/tableNames.ts         Dataset and table name rules, labels, and the table name each topic gets
  shared/access.ts             Dataset permissions: what a principal may do on one dataset, as the control plane resolves it
  server/
    main.ts                    Bootstrap: Fastify, error handler, routes, static, shutdown
    workerPool.ts              Sizes Node's worker pool before anything starts it
    config.ts                  Environment-driven configuration, live overrides, derived data access mode
    logger.ts                  Pino instance typed as FastifyBaseLogger
    errors.ts                  ConsoleError + constructors + identifier validation
    auth/guard.ts              The onRequest hook: resolve the caller, renew cookie sessions, refuse cross-site changes; permit(), permitAnywhere(), requireOnDatasets()
    auth/principal.ts          Session checks with the control plane, cached by token hash
    auth/cookies.ts            The session cookie
    auth/outcomes.ts           Waiting for an identity change to apply
    auth/rateLimit.ts          Sign-in attempts per client address
    controlPlane/client.ts     Typed client for the control-plane WebViews
    live/store.ts              Polling, series, lag history, SSE fan-out, snapshot cache, visibleDetail per person
    live/rates.ts              Rates from each runtime member's own heartbeats
    iceberg/json.ts            JSON parsing that keeps 64-bit snapshot ids exact
    iceberg/io.ts              Reading an object from GCS (or disk, in tests)
    iceberg/pointer.ts         The table's catalog pointer
    iceberg/metadata.ts        Metadata files: schemas, snapshots, the partition spec
    iceberg/manifests.ts       Avro manifest lists and manifests, with Iceberg's inheritance rules
    iceberg/snapshot.ts        One snapshot's data files, with counts, bounds, offsets; cached
    iceberg/table.ts           The table as last read: pointer freshness, parsed metadata
    iceberg/schema.ts          The schema walk with the fork map: the control plane's projection, ported
    schema/diff.ts             Schema comparison by field id
    data/gcs.ts                Downloads, the local mirror, cache eviction
    data/duckdb.ts             Instance lifecycle, the statement sandbox, query execution, value coercion, interrupts
    data/sql.ts                Identifier/literal quoting and the read-only statement guard
    data/catalog.ts            A snapshot's files per materialization: summaries, time pruning, warming
    data/logical.ts            Logical columns over forks, reads by field id, filters, the time predicate
    data/objects.ts            Per-file sources and footers
    runs/run.ts                A run: the only unit of query work, its progress and snapshot
    runs/browse.ts             Time range + filters, counted per file, paged by position
    runs/statement.ts          SQL over the materialization's tables in a sandbox, stored as local Parquet
    runs/store.ts              Run registry, admission, statement validation, the tables (and datasets) a run reads
    runs/view.ts               The logical (`<dataset>.<table>`) and native (`iceberg.<dataset>.<table>`) views over a snapshot's files, and the run-rows view
    runs/export.ts             CSV, JSON, JSON Lines and Parquet downloads of a run
    runs/profile.ts            Column profiles over a run's rows
    routes/system.ts           /api/health, /api/config
    routes/materializations.ts Fleet + detail + lifecycle mutations
    routes/schema.ts           Schema, versions, diff
    routes/table.ts            The Table tab's view, maintenance requests, the catalog connection
    routes/data.ts             Data summary, estimates, runs, pages, exports, run streams
    routes/stream.ts           Server-sent event channels
    routes/settings.ts         Settings: read every scope, save and wait for it to apply, history
    routes/auth.ts             Sign in and out, the session, your sessions, change your password
    routes/identity.ts         Users, groups, roles, the permission catalog, the audit log
    routes/datasets.ts         Datasets: list, create, edit, delete, their grants
    settings/catalog.ts        The console's overridable settings: sections, labels, bounds, validation
    settings/store.ts          Reads the console's overrides from the control plane and applies them
  src/
    main.tsx                   React root, providers
    App.tsx                    Route table
    styles.css                 Design tokens and base layer
    lib/                       API client, SSE hooks, the session (auth.tsx), formatting, class merging
    components/                Design system primitives and shared display components
    features/                  Overview, materializations, datasets, detail, schema, data, settings,
                               auth (sign-in, password, gates), access (account, users, groups, roles, audit)
```
