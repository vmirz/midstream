# 15 — Extending the Console

Recipes for the changes most likely to be asked for, with the constraints that must not be broken.

## 15.0 Non-negotiable constraints

| Constraint | Why |
|------------|-----|
| No imports between `/console` and `python/` in either direction | The console must stay extractable into its own repository |
| `shared/types.ts` stays dependency-free | It is consumed under two different module resolution modes |
| No `enum`, `namespace`, or constructor parameter properties in `server/` or `shared/` | Node's type stripping cannot emit code; `erasableSyntaxOnly` enforces it |
| Server imports use explicit `.ts` extensions and relative paths | There is no bundler on the server side |
| `config.ts` is the only module reading `process.env` | One place to audit and document |
| Routes contain no business logic | Keeps the layering testable and the surface reviewable |
| `Cp*` types change additively only | They mirror an API with the same rule |
| Every `/api` route declares `permit('<permission>')` or `signedIn()` | A route without one answers guests; leave it out only for what anyone may see |
| Changes go to the control plane with `callerOf(request)`, never `SERVICE` | The control plane authorizes the person; the service token can only read |

## 15.1 Adding a control-plane endpoint

The control plane gained `GET /materializations/{id}/tables/{table}/schema` for this console; the
same five steps apply to anything new.

1. **Add the type** to `shared/types.ts` with a `Cp` prefix.
2. **Add a client method** in `server/controlPlane/client.ts` — validate the id with
   `assertSafeIdentifier`, `encodeURIComponent` it, and let `request<T>` handle timeout and error
   mapping.
3. **Expose it** through `LiveStore` if it should be cached, polled, or streamed; otherwise call it
   straight from a route.
4. **Add a route** in the appropriate `routes/*.ts`, with `{ preHandler: permit('<permission>') }`
   naming what the control-plane view needs, and pass `callerOf(request)` to the client method —
   or `SERVICE` only for a read shared by everyone through `LiveStore`.
5. **Add a client API method** in `src/lib/api.ts` and use it via TanStack Query; hide the UI that
   leads to it with `useCan('<permission>')`.

If the control plane itself needs the endpoint, remember it is additive-only: new WebView manifest
(authorizing its caller first), new view function, extended `__init__` exports, tests, and the
kustomization entry.

## 15.2 Adding a metric to the live stream

Say the runtime starts reporting a new counter on its heartbeat.

1. `shared/types.ts` — add it to `CpRuntimeProgress` (and `CpMemberProgress` if members report it),
   `MaterializationSummary`, and `ThroughputSample` if it should be charted.
2. `live/rates.ts` — if it needs a rate, follow it per member like rows and bytes, so it is
   timed by the runtime's clock rather than by the console's polls.
3. `live/store.ts` — read it in `summarize()`; add it to `FleetTotals` and `aggregate()` if it
   aggregates.
4. Client — surface it in `StatCard`, the list column set, or the detail overview; for a line
   over time, `TimeSeriesChart`.

Rules that keep derived metrics honest:

- No rate until there are two readings — `null`, never `0`.
- Difference a runtime's counters between its own heartbeats, by `reported_at`. Differencing at
  the poll interval turns a steady rate into a picket fence of zeros and spikes.
- A counter below its last reading is a restart: count what it reports as new, never a negative.
- Aggregate rates by **summing per-member and per-materialization rates**, never by differencing
  summed counters.
- Something measured on a schedule — consumer lag — is kept one point per measurement, keyed by
  when it was measured.

## 15.3 Adding a detail tab

1. Create `src/features/<area>/<Name>Tab.tsx` taking `{ materializationId, snapshot }`.
2. Register it in `DetailPage.tsx`:
   ```tsx
   const TABS = [ …, { key: 'lineage', label: 'Lineage', permission: 'materializations.view_details' } ] as const;
   ```
   and add the branch in the render switch.
3. No routing change is needed — `/materializations/:id/:tab` already matches.
4. Follow the four-state contract: loading skeleton, empty state with a reason, error state with a
   recovery action, content.
5. A tab about one table takes it from `useSelectedTable(detail)` (`features/tables/selectedTable.ts`)
   and shows `<TablePicker>` for a materialization of several; the choice lives in `?table=`, which
   `DetailPage` keeps across tabs.

## 15.4 Gating something on a permission, or adding a permission

Accounts, groups, roles and the permission catalog belong to the control plane
([18 — Identity and access](../../docs/handbook/18-identity-and-access.md)); the console only
checks.

**Gate a route** with an existing permission:

```ts
app.post('/api/materializations/:materializationId/thing', { preHandler: permit('materializations.edit') }, async (request) =>
  controlPlane.thing(callerOf(request), materializationId(request)),
);
```

A guest without it gets `401`, a person `403`; `signedIn()` asks only that someone is signed in.
Streams also take `watchAccess(request, permission, onLost)` so they close when access is lost.

**Gate a route on a dataset permission** (`data.query`, `dataset.view_schema`, `catalog.read`, …) in
two steps: `permitAnywhere('<permission>')` as the pre-handler, which turns away anyone who holds it on
no dataset, then, once the route knows which tables the request is about,
`requireOnDatasets(authOf(request), '<permission>', datasets)` with each table's `{ id, name }`. A
refusal names the dataset. Never check a dataset permission with `permit()`: it asks whether the
permission is held across the deployment, which a dataset grant never is.

**Gate the UI** with `useCan('<permission>')` (a button, a tab) or `<RequirePermission>` (a whole
page), and `useCan('<permission>', datasetId)` for one on a table's dataset. Never rely on it: the
route must check too.

**A new permission** starts in the control plane's catalog (`identity/permissions.py`), which the
console reads through `GET /api/permissions`: the role editor shows it with no console change.
Then gate the control-plane view and the console route on it. One about a dataset's tables
(`scope: dataset`) also goes in `shared/access.ts::DATASET_PERMISSIONS`, which is how the console
tells it apart. The checklist is the repository's
`adding-permissions` skill (`.claude/skills/adding-permissions/SKILL.md`).

**Single sign-on** would be a new sign-in method in the control plane (`SIGN_IN_PROVIDERS`),
which the sign-in page already lists; the console's part is a route that starts the provider's
flow and stores the resulting control-plane token in the same cookie. Nothing else changes.

**Per-dataset access** is modelled (control-plane handbook 18 §18.16); per-materialization,
per-table and per-column access are not. A table-level grant would need a scope on grants in the
control plane, and checks wherever `requireOnDatasets` runs now, plus `visibleDetail` in the live
store.

## 15.5 Adding a filter operator

1. `shared/types.ts` — add it to the `FilterOperator` union.
2. `routes/requestSchemas.ts` — add it to the Zod `operator` enum.
3. `data/logical.ts::renderFilter` — add the case. **Quote identifiers, quote literals, escape
   `LIKE` metacharacters.** Never interpolate raw input. Decide what it means for a list column
   (`eq` becomes `list_contains`; ordering comparisons are refused) and add a case to
   `runs/runs.test.ts`, which executes every operator against real Parquet.
4. `src/features/data/FilterChips.tsx` — add it to `OPERATORS`, with `unary: true` if it takes no
   value.

Example of a correctly-built predicate:

```ts
case 'ends_with':
  return `CAST(${column} AS VARCHAR) ILIKE ${likeLiteral(filter.value ?? '', (v) => `%${v}`)}`;
```

## 15.6 Supporting another object store

The data path is GCS-only in four places:

| File | Coupling |
|------|----------|
| `data/gcs.ts` | `@google-cloud/storage`, `parseGcsUri`, `downloadObject`, the cached-mode mirror |
| `iceberg/io.ts` | `readObject`: a `gs://` table object through `data/gcs.ts`, anything else from local disk (the test fixtures) |
| `data/objects.ts` | Which data files to mirror in cached mode: `gs://` ones |
| `data/duckdb.ts` | Registers a `TYPE GCS` secret for direct mode |

A table's own objects are read whole through `readObject`, and data files by DuckDB, so a clean
approach is:

1. Define an `ObjectStore` interface: `read(uri)` for metadata objects, `download(uri, target)` for
   the mirror, and the DuckDB secret statement.
2. Implement `GcsStore` (move the existing code) and, say, `S3Store`.
3. Select the implementation from the table location's scheme, not from configuration — a single
   console may need to serve materializations with different destinations.
4. Keep `iceberg/io.ts::isUnderLocation` in front of every read, whatever the store
   ([12 §12.6](./12-security-model.md#126-paths-a-table-names)).

Note this only makes the **console** multi-cloud. The runtime and the control plane are GCS-only:
the catalog pointer depends on GCS's generation preconditions (handbook 11 §11.2).

## 15.7 Adding a chart

Do not add a charting library.

- A glyph beside a number (a stat card) → `Sparkline`, a fixed 240-unit viewBox stretched to its
  container.
- A series someone will read → `TimeSeriesChart`: real-pixel SVG with a round value axis
  (`baseline="zero"` for rates, `"auto"` to fit large slow-moving values), a clock-time axis, and a
  crosshair readout on hover and on the arrow keys. Give it a window (`from`, `to`) and points
  `{ t, value }`; nulls break the line.
- Proportions → reuse `DistributionBar`.
- Something genuinely new → write it as SVG in `components/`, use design tokens for colour, and
  render a "Collecting…" placeholder below two data points.

Charts re-render on every stream frame, so keep them to a single path computation.

## 15.8 Changing the polling model

The current model is subscriber-aware polling. If you move to something else:

- **Control-plane push** (webhooks/SSE from the control plane) would replace `LiveStore.refresh`
  but should keep the snapshot cache, series derivation, and fan-out intact — those are what the
  client depends on.
- **A shared cache (Redis)** would let replicas share the series and make horizontal scaling
  viable. `LiveStore` would become a thin reader over it.
- Whatever you do, **preserve the derived-rate contract**: first sample `null`, negative deltas
  clamped, aggregate rates summed from component rates.

## 15.9 Adding a persistent user preference

Two existing precedents, both in `localStorage` and both applied without a server round trip:

| Key | Applied by |
|-----|-----------|
| `console.theme` | Inline script in `index.html` before first paint |
| `console.data.sqlHeight`, `console.data.railWidth`, `console.data.jsonShape` | The Data tab |

For anything more than a handful of preferences, add a small `usePreference<T>(key, fallback)` hook
rather than scattering `localStorage` calls.

Preferences that should follow a person across browsers belong with their account. The Settings
page has an **Account** group for them (`audience: 'personal'`, `permission: null`), and the
control plane's settings storage is keyed by scope, so a `user:<id>` scope could sit beside
`console`; it would need a control-plane rule that only that user may write it.

## 15.9a Adding a setting

**A console setting**, one an administrator may change while the console runs:

1. Add it to `tunableDeployment` in `server/config.ts`, read from its environment variable with
   `int`/`bool`/`str`. It becomes a getter on `config`.
2. Read it as `config.<key>` where it is used. A value copied at startup, into a constant or a
   constructor, never sees a change. Pass a getter (`() => config.<key>`) to anything long-lived,
   as the topic catalog does.
3. Describe it in `CONSOLE_SETTINGS` (`server/settings/catalog.ts`): section, label, a short
   description of what it controls, kind, unit, bounds, and `effect` when it applies to something
   other than the next request (*charts as they gain points*). Mark it `advanced` if few people
   need it, `danger` if a change can remove data.
4. `server/settings/catalog.test.ts` fails until the catalog and `config.ts` agree. The page needs
   no change: it renders whatever the catalog describes, in the section it names.

A setting that sizes the pod, wires it to its environment, or is read once at startup stays a
plain deployment setting, and belongs in the console's Deployment list (`consoleDeployment` in
`routes/settings.ts`).

**A control-plane or runtime setting** is described by the control plane's catalog
(`python/src/midstream/settings/catalog.py`), not here. The console renders it
without a change; see the main handbook, [17 §17.13](../../docs/handbook/17-settings.md).

**A new section**: add it to `SETTINGS_SECTIONS` in `features/settings/sections.ts`, with its
group, audience, `kind` and the `permission` that shows it. Every setting names its section by id.
The full checklist for settings, on both sides, is the repository's `adding-settings` skill
(`.claude/skills/adding-settings/SKILL.md`).

## 15.10 Performance work

Before optimising, know where the time actually goes:

| Cost | Where | Current mitigation |
|------|-------|--------------------|
| Control-plane requests | `LiveStore.refresh` | Subscriber-aware cadence, rotation, concurrency cap |
| Table metadata | `iceberg/table.ts`, `iceberg/snapshot.ts` | A pointer read per freshness window; metadata, manifest lists and manifests parsed once each and kept (they never change); a snapshot's catalog kept per snapshot |
| Parquet download | `ensureLocalCopies` | Immutability skip, 6 workers, only the files a time range selects |
| Query execution | DuckDB | Files outside the time range never opened, reads by field id with no footer to bind, footer and byte caches on the shared instance, row limits |
| Client render | `DataGrid` | Row virtualization |
| Bundle size | `QueryEditor` | Lazy chunk |

The highest-leverage remaining wins, roughly in order:

1. **Server-side result streaming** for large query results instead of buffering to JSON.
2. **Incremental SSE payloads** — currently every frame carries the full fleet snapshot; a diff
   would cut bandwidth on large fleets.
3. **Column projection by default** in browse mode — `SELECT *` on a very wide schema is wasteful
   when the grid only shows a slice.
4. **A warm statement sandbox** per run, so a second statement does not re-read every footer the
   first one read (07 §07.6).

## 15.11 Definition of done

Before considering a change complete:

- [ ] `npm run typecheck` passes for both projects
- [ ] `npm run build` succeeds and `QueryEditor` is still a separate chunk
- [ ] `npm audit` reports 0 vulnerabilities
- [ ] Loading, empty, error, and content states all exist for any new async surface
- [ ] Light and dark themes both verified
- [ ] Behaviour verified with the control plane **down** as well as up
- [ ] No new `process.env` reads outside `config.ts`
- [ ] No new imports crossing the `/console` ↔ `python/` boundary
- [ ] Every new route has `permit(...)` or `signedIn()`, and the UI hides what a person cannot do
- [ ] Verified as a guest, as someone without the permission, and as someone with it
- [ ] A new tunable is on the Settings page (§15.9a), or deliberately a deployment setting
- [ ] This handbook updated — at minimum [03](./03-configuration-reference.md) for new settings and
      [08](./08-console-api-reference.md) for new endpoints
