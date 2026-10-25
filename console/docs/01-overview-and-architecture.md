# 01 — Overview and Architecture

## 1.1 What the console is

The Materializer Console is a real-time web application for operating the
`kaspr-kafka-materializer` control plane. It provides:

- a live fleet view of every materialization,
- full lifecycle control (create, stop, resume, edit the label, topics and replica count, delete),
- datasets: the named groups the tables go in, `<dataset>.<table>` to SQL, and who may use each,
- complete visibility of every metric the control plane and runtime expose,
- a schema browser over every schema each table has had, with schema-to-schema diffs,
- a view of each of a materialization's Iceberg tables — one per source topic: snapshots, files,
  maintenance — with connection snippets for other engines,
- a data explorer that reads the tables, at any snapshot they keep, with DuckDB, and joins them in
  SQL, across datasets — and, where live tail is on, the records the runtimes have not committed yet,
  within seconds of being produced, following them live,

each part shown and served to a person as far as their roles allow, per dataset for schemas and data.

It is deliberately a **separate application**, not part of the Python package. It depends on the
control plane only through the public HTTP API — and on the tail app only through its own
(`/tail/v1`, handbook 19) — so `/console` can be lifted into its own repository by copying the directory
and setting `CONTROL_PLANE_URL` (and `CONSOLE_TAIL_URL`).

> **Hard boundary.** Nothing under `/console` may import from `python/`, and nothing in `python/` may
> import from `/console`. This is stated in the repository `AGENTS.md` and is the single most
> important architectural constraint here.

## 1.2 Two processes

```
┌───────────────────────────────┐         ┌──────────────────────────────────┐
│ Browser (React SPA)           │         │ Console server (Node + Fastify)  │
│                               │  SSE    │                                  │
│  useFleetStream()  ◄──────────┼─────────┤  /api/stream/fleet               │
│  useDetailStream() ◄──────────┼─────────┤  /api/stream/materializations/:id│
│                               │         │                                  │
│  TanStack Query    ──────────►┼─────────►  /api/... (REST)                 │
└───────────────────────────────┘         │        │                         │
                                          │        ├─ LiveStore              │
                                          │        ├─ Iceberg reader         │
                                          │        └─ DuckDB + GCS           │
                                          └────────┬─────────────────────────┘
                                                   │ HTTP
                                          ┌────────▼─────────────────────────┐
                                          │ Control plane WebView API :6065  │
                                          │ Tail app WebView API :6065       │
                                          │   (/tail/v1: records not yet     │
                                          │    committed, from Kafka)        │
                                          └────────┬─────────────────────────┘
                                                   │
                                          ┌────────▼─────────────────────────┐
                                          │ Iceberg tables in GCS (gs://)    │
                                          │ read directly by the console     │
                                          └──────────────────────────────────┘
```

In production both halves are served by the same Node process: the server serves the built SPA
from `dist/` and answers `/api/*`. In development Vite serves the client on `:5173` and proxies
`/api` to the server on `:8787`.

## 1.3 Request flow

### Reads

```
browser                console server                     control plane
   │  GET /api/stream/fleet   │                                 │
   ├─────────────────────────►│ subscribeFleet()                │
   │◄── event: fleet ─────────┤ (immediate snapshot)            │
   │                          │                                 │
   │                          │ ── every 1.5–2 s ──► GET /materializations
   │                          │ ── rotating ──────► GET /materializations/{id}
   │                          │ ── on drift ──────► GET /materializations/{id}/tables/{table}/schema
   │◄── event: fleet ─────────┤ (on every refresh)              │
```

One browser tab and ten browser tabs cost the control plane exactly the same, because the
`LiveStore` polls once and fans the result out to all subscribers. It polls with the console's
read-only service token; each subscriber is checked for the permission its stream needs.

### Who is calling

Every `/api` request is resolved to a caller first — a signed-in person from the session cookie, or
a guest — by asking the control plane, which owns accounts and permissions, and remembering the
answer for a few seconds. Each route names the permission it needs. See
[12 — Security model](./12-security-model.md) and [04 §4.9](./04-server-internals.md).


### Writes

```
browser              console server           control plane            request-intake agent
   │ POST /api/materializations  │                   │                        │
   ├────────────────────────────►│ POST /materializations (the person's own session)
   │                             │ permit('materializations.create')          │
   │                             ├──────────────────►│  202 Accepted          │
   │◄─────── 202 ────────────────┤                   ├─ publishes event ─────►│
   │                             │ liveStore.wake()  │                        │
   │◄── event: fleet (new state) ┤◄── GET /materializations ──────────────────┤
```

The console **never** optimistically mutates its own view. It calls `liveStore.wake()` so the
next refresh happens in ~50 ms, then renders whatever the control plane actually reports. This is
correct because control-plane mutations are asynchronous: the WebView returns 202 without
persisting, and the `request-intake` agent persists later. A UI that assumed read-after-write
would flicker or lie.

## 1.4 Why a backend-for-frontend

The console could, in principle, call the control plane directly from the browser. It does not,
for four reasons.

| Reason | Detail |
|--------|--------|
| **Real time** | The control-plane API is request/response only. Something must poll. Doing it once on the server and fanning out over SSE is dramatically cheaper than N browsers polling independently, and it lets the cadence back off to 30 s when nobody is watching. |
| **Derived metrics** | Rates require following each runtime member's counters across its heartbeats. That state must be shared, or every browser tab would compute its own rate from its own observations and disagree. |
| **Data access** | DuckDB is a native module, reading Iceberg metadata means decoding Avro manifests, and object-storage credentials must not reach a browser. Query execution has to be server-side. |

## 1.5 Technology choices

| Layer | Choice | Why |
|-------|--------|-----|
| Runtime | Node ≥ 24 | Native TypeScript type stripping means the server runs `.ts` directly with no build step |
| HTTP server | Fastify 5 | Fast, first-class async, plugin ecosystem for static and CORS |
| Validation | Zod 4 | Request-body validation at the trust boundary |
| Logging | Pino 10 | Structured JSON, typed as `FastifyBaseLogger` so Fastify keeps its default generics |
| Query engine | `@duckdb/node-api` 1.5 | Reads Parquet by field id natively; can read `gs://` directly with httpfs; sandboxes a statement with `enable_external_access` |
| Iceberg metadata | the console's own reader (`server/iceberg/`) with `avsc` 5 | Pointer, metadata and Avro manifests, per snapshot, with exact 64-bit ids |
| Object storage | `@google-cloud/storage` 8 | ADC-based download |
| UI | React 19 + Vite 8 | Current stable; Vite gives a fast dev loop and small production output |
| Routing | React Router 7 | URL-addressable tabs and deep links |
| Server state | TanStack Query 5 | On-demand resources (schema, dataset, query results). Live data does **not** use it — it comes over SSE |
| Styling | Tailwind CSS 4 | Token-driven, no runtime CSS-in-JS cost |
| Editor | CodeMirror 6 via `@uiw/react-codemirror` | Lazy-loaded so it costs nothing until SQL mode is opened |

There is no linter or formatter configured in this repository. Do not introduce one.

## 1.6 Build and run model

The server and client are built and executed differently on purpose.

| | Server | Client |
|---|--------|--------|
| Language | TypeScript | TypeScript + JSX |
| Build step | **None** — Node strips types at load | Vite build to `dist/` |
| Entry | `node server/main.ts` | `dist/index.html` |
| Import style | Relative with explicit `.ts` extensions | Bundler resolution with `@/` and `@shared/` aliases |
| tsconfig | `tsconfig.server.json` (`erasableSyntaxOnly`, `noEmit`) | `tsconfig.app.json` (`noEmit`) |

`erasableSyntaxOnly` is enabled for the server because Node's type stripping cannot handle
constructs that emit code. That means **no `enum`, no `namespace`, and no constructor parameter
properties** in `server/` or `shared/`. TypeScript enforces this at typecheck time.

## 1.7 Directory layout and responsibilities

```
console/
├── shared/types.ts      Wire contracts. Imported by both halves. Must stay dependency-free.
├── server/              Node BFF. No React, no DOM.
│   ├── main.ts          Composition root — the only file that wires everything together.
│   ├── config.ts        The only place that reads process.env.
│   ├── errors.ts        The only error type crossing the HTTP boundary.
│   ├── controlPlane/    Outbound: the control plane.
│   ├── live/            Stateful: polling, snapshots, derived metrics, subscriptions.
│   ├── iceberg/         Outbound: reading a table's pointer, metadata, manifests and snapshots.
│   ├── schema/          Schema comparison.
│   ├── data/            Outbound: object storage and DuckDB.
│   ├── runs/            Query execution: browse and statement runs, pages, exports.
│   └── routes/          Inbound: HTTP surface. Thin — validation and delegation only.
└── src/                 React SPA. No Node APIs.
    ├── lib/             Cross-cutting: API client, SSE hooks, formatting.
    ├── components/      Reusable and presentational. No feature knowledge.
    └── features/        Screens. Compose components; own their local state.
```

The layering rule is one-directional: `routes → services (live/schema/runs/data) → outbound clients (iceberg, controlPlane)`.
Routes never talk to `@google-cloud/storage` or `fetch` the control plane directly, and services
never import from `routes/`.

## 1.8 What the console is not

- **Not an identity provider.** People sign in through it, but accounts, groups, roles,
  permissions and sessions belong to the control plane; the console holds a browser's session in a
  cookie and checks each route's permission. See [12 — Security model](./12-security-model.md).
- **Not a source of truth.** Every piece of state it displays originates in the control plane or
  the tables themselves. It writes nothing to either; **Run now** on the Table tab asks the
  control plane, which hands the job to its maintenance app.
- **Not multi-cloud.** The data explorer is GCS-only, matching the runtime.
- **Not a general SQL workbench.** Queries are scoped to one materialization's tables at one
  snapshot and restricted to read-only statements, in a sandbox. Engines on the control plane's
  Iceberg REST catalog are the general workbench.
- **Not horizontally stateless.** Two console replicas each keep their own poll loop, their own
  derived series, and their own runs. See
  [13 — Operations runbook](./13-operations-runbook.md#134-running-more-than-one-replica).
