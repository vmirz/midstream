# 09 — Client Architecture

Everything in this document lives under `console/src/`.

## 9.1 Composition

```
main.tsx
  └── QueryClientProvider              rootQueryClient: the console's own resources and the session
      └── BrowserRouter                React Router 7
          └── ToastProvider            Notification context
              └── AuthProvider         Who is using the page, in every domain (lib/auth.tsx)
                  └── App              SessionGate + SsoOutcomeToast + route table
                      ├── SignInPage   /sign-in, /sign-in/:domain
                      └── AppShell     Sidebar with the domain switcher, account menu, ⌘K, theme
                          ├── <console page>      Overview | Materializations | Datasets | Search | Settings: every domain
                          └── DomainScope         /d/:domain/*: the domain, its API and a query cache of its own
                              └── RequirePermission → <page>   Overview | Materializations | Detail | Datasets | Settings
```

`main.tsx` also reads `#root` and throws if it is missing — a fast, loud failure rather than a
silent no-op.

### Query client defaults

```ts
new QueryClient({
  defaultOptions: {
    queries: { refetchOnWindowFocus: false, retry: 1, staleTime: 10_000 },
  },
});
```

`refetchOnWindowFocus` is off because live data arrives over SSE; refetching on focus would
duplicate work and cause visible churn. TanStack Query is used **only** for on-demand resources:
schema versions, schema diffs, dataset summaries, and query results.

**One cache per domain** (`lib/queryClient.ts`). Each domain's pages read through a query client of
their own, `queryClientFor(domain)`, so nothing one domain answered is ever shown as another's under
the same key; the console's own resources — the config, the session, search, the merged fleet — use
`rootQueryClient`. `DomainScope` (under `/d/:domain/`) and `DomainProvider` (a dialog or section
about one domain on a console page) provide the domain, its API (`useApi()`: the domain's routes
under `/api/d/<domain>`) and its cache; `useDomain()`, `useDomainPath()` read them.

## 9.2 Routing

```tsx
<Route path="/sign-in"            element={<SignInPage />} />                     // the providers, or a password for a chosen domain
<Route path="/sign-in/:domain"    element={<SignInPage />} />                     // a password for that domain
// Every domain:
<Route path="/"                   element={<Overview />} />
<Route path="/materializations"   element={<Materializations />} />
<Route path="/datasets"           element={<Datasets />} />
<Route path="/search"             element={<SearchPage />} />
<Route path="/settings[/:section[/:itemId]]" element={<SettingsPage />} />        // the console's own sections
// One domain:
<Route path="/d/:domain/*" element={<DomainScope><DomainRoutes /></DomainScope>} />
  // index → Overview; materializations, materializations/:id[/:tab]; datasets, datasets/:name[/:tab];
  // settings[/:section[/:itemId]] → the domain's sections; anything else → the domain's overview
<Route path="*"                   element={<Navigate to="/" replace />} />
```

`RequirePermission` sends a guest to `/sign-in` — `/sign-in/<domain>` inside a domain — remembering
where they were going, and shows a signed-in person without the permission an **Access denied**
page naming the domain. Settings checks each section's own permission instead (§9.7). An unknown
domain in the path shows a page saying so; a misconfigured one, why it is not available.

The shell's **domain switcher** (*All domains*, then each domain with its live dot) keeps the section
open: from `/d/domain_a/datasets` to `/datasets` or `/d/domain_b/datasets`; search is always the
console's own. ⌘K or Ctrl+K opens search from anywhere.

Settings sections are URL segments too (`/settings/domains`, `/d/domain_a/settings/users/usr-…`);
a page and an unknown section redirect to the first section the person may see. Moving between
sections keeps the same `SettingsPage` mounted, so unsaved edits survive it.

Detail tabs are URL segments, so `/d/domain_a/materializations/mat-x/schema` is bookmarkable,
shareable, and survives a reload. `DetailPage` reads `tab` from params, defaults to `''` (Overview),
and switches tabs with `navigate(..., { replace: true })` so tab changes do not fill the history
stack. The table a tab shows is `?table=` (`features/tables/selectedTable.ts`: `useSelectedTable`
resolves it against the materialization's tables — the written ones, then the kept ones
(`readableTables`) — the first when absent or unknown); switching tabs keeps it, so the Schema, Data
and Table tabs stay on one table, and the Tables panel links into them with it set.

The server's SPA fallback (`main.ts`) is what makes deep links work on a cold load; the old
hostnames' redirect (handbook 20 §20.3) lands on `/d/<domain>/<path>`.

## 9.2a Who is using the page (`lib/auth.tsx`, `features/auth/`)

`AuthProvider` reads `GET /api/auth/session` once with TanStack Query (`['session']`), again every
minute and on window focus, and immediately when any request finds a session gone: who the browser
is **in every domain**. Everything else reads it through these hooks:

```ts
const all = useConsoleAuth();    // session, providers, signedInDomains, forDomain(id), overall, signOutEverywhere
const auth = useAuth();          // inside /d/<domain>/: that domain's; elsewhere `overall`, across domains
const canCreate = useCan('materializations.create');
const canQuery = useCan('data.query', table.dataset_id);   // a dataset permission, on one dataset
```

`forDomain(id)` gives one domain's `Auth` — principal, `signedIn`, `can()`, `canAnywhere()`, `signIn`
(a password, for that domain), `signOut`, `adopt` — and `overall` answers across domains: a
permission held in any, signed in to any, signing out of all.

- `can(permission)` is false for everything while a password must be changed, which keeps the
  page from offering what the server would refuse.
- `can(permission, datasetId)` answers a dataset permission on one dataset, from the session's
  `permissions` and `dataset_permissions` (`shared/access.ts`, the server's rule too);
  `canAnywhere(permission)` whether it is held on any dataset. Without a dataset id, a dataset
  permission is held only where it is held across the deployment.
- `generation` changes whenever the person using the page changes (sign-in, sign-out, a password
  change). Live streams reconnect on it, and every cached query is invalidated, because what the
  previous person could see is not what this one can.
- `SessionGate` (wrapping the routes) waits for the first answer; while a password someone else
  chose must be changed in any domain it shows only that domain's change-password form (or signing
  out of that domain); when the console cannot answer at all it shows a retry page rather than
  guessing.
- Back from a provider (`?signin=<id>`), `SsoOutcomeToast` says where the sign-in started a session
  and where not, and why; the sign-in page shows the same when no domain admitted the person.

The browser never holds the session token: it is an HttpOnly cookie the server sets. Requests
use `credentials: 'same-origin'`. A `401`, or a `403` saying the password must be changed, makes
`lib/api.ts` call `notifySessionLost()`, and `AuthProvider` re-reads the session (debounced by
250 ms), so a session that ended elsewhere turns the page into a guest's at once.

**UI follows permissions; the server enforces them.** Navigation hides what a person cannot open,
buttons for actions they cannot take are not shown, and pages ask only for what they may read.
None of that is a security boundary: every route checks again ([12](./12-security-model.md)).

## 9.3 Two data paths

This is the most important thing to understand about the client.

| Path | Mechanism | Used for | Cache |
|------|-----------|----------|-------|
| **Live** | `EventSource` via `useFleetStream` / `useDetailStream` | Fleet snapshot, detail snapshot, schema, series | React state inside the hook |
| **On-demand** | `fetch` via `api.*` wrapped in TanStack Query or mutations | Schema versions, diff, schema timeline, dataset, browse, SQL, export | TanStack Query cache |

Live data is never fetched through TanStack Query, and on-demand data is never pushed over SSE.
Mixing them would produce two sources of truth for the same value.

### `lib/live.ts`

```ts
function useEventStream<T>(url: string, eventName: string): { data: T | null; status: LiveStatus }
```

- `status` is `'connecting' | 'live' | 'reconnecting'`.
- `everConnected` is a ref, so a dropped connection reports `reconnecting` rather than
  `connecting` — an honest distinction the header surfaces.
- Malformed frames are ignored inside a `try`; the next tick supersedes them.
- Cleanup removes the listener and calls `source.close()`, which is what makes the server's
  `request.raw.on('close')` unsubscribe fire.
- The stream reconnects when `useAuth().generation` changes, so it carries the new person's
  session. An `auth` event — the server closing a stream whose caller lost access — and repeated
  failures trigger a session check, at most one every few seconds.

`EventSource` already reconnects automatically; the hook exists to make that state visible.

```ts
export function useTicker(intervalMs = 1000): number
```

Returns `Date.now()` on an interval so relative timestamps ("just now", "3 minutes ago") stay
current **without a data fetch**. Used by `RelativeTime` and `LiveIndicator`.

## 9.4 API client (`lib/api.ts`)

A thin `fetch` wrapper plus a typed method map.

```ts
async function request<T>(path: string, init: RequestInit = {}): Promise<T>
```

- Adds `accept: application/json` and `content-type` when a body is present, and sends the
  session cookie (`credentials: 'same-origin'`).
- On non-2xx, parses the error envelope and throws `ApiError(code, message, status, details)`; a
  lost session also notifies `onSessionLost` listeners (§9.2a).
- On success, returns parsed JSON (or `null` for an empty body).

`ApiError` carries `details`, which the UI uses for field-level highlighting in the create dialog
and for surfacing `details.reason` on object-storage failures.

### Sign-in and directory methods

`api.session`, `signIn`, `signOut`, `mySessions`, `changePassword`; `users`, `user`, `createUser`,
`updateUser`, `deleteUser`, `setUserPassword`, `signOutUser`, `unlockUser`; `groups`, `group`,
`createGroup`, `updateGroup`, `deleteGroup`; `roles`, `role`, `createRole`, `updateRole`,
`deleteRole`; `permissions`; `audit`. Directory changes resolve to `{ request_id, status:
'applied' | 'pending', entity_type, entity_id }` once the server has waited for them.

### CSV download

```ts
document.body.append(anchor);
anchor.click();
anchor.remove();
setTimeout(() => URL.revokeObjectURL(url), 10_000);
```

Both details matter and were bug fixes: a detached anchor does not reliably trigger a download in
Chromium, and revoking the object URL synchronously after `click()` races the download and can
produce a zero-byte file.

## 9.5 Formatting (`lib/format.ts`)

Pure functions, no React. All of them return `'—'` for `null`, `undefined`, and non-finite input,
so the UI never renders `NaN` or `undefined`.

| Function | Behaviour |
|----------|-----------|
| `formatCount` | Compact notation (`163.8K`) at ≥ 10 000, full grouping below |
| `formatExact` | Always full grouping — used where precision matters, e.g. Kafka offsets |
| `formatBytes` | **SI units** (1000-based): `B`, `KB`, `MB`, `GB`, `TB`, `PB`. One decimal below 100, none at/above |
| `formatRate(value, unit)` | Rounds to one decimal; values below `0.05` render as `0 <unit>` to avoid noisy dust |
| `formatBytesRate` | `formatBytes(value) + '/s'` |
| `formatDuration` | `ms` under 1 s, `s` under 1 min, `Xm Ys` above |
| `formatRelativeTime(iso, now)` | `Intl.RelativeTimeFormat`; "just now" within 5 s |
| `formatTimestamp` | Locale date-time; falls back to the raw string if unparseable |
| `humanize` | `waiting_for_source_records` → `Waiting for source records` |
| `displayName` | `name` if non-blank, else the materialization id |
| `formatCell` | Table cell rendering: objects are `JSON.stringify`ed, `null` becomes `''` |

> `formatBytes` uses 1000-based units deliberately, matching how object-storage providers report
> size. A 52 088 400-byte value displays as `52.1 MB`, not `49.7 MiB`.

## 9.6 State ownership

| State | Owner | Persistence |
|-------|-------|-------------|
| Fleet snapshot | `useFleetStream` hook instance | None — re-derived on reconnect |
| Detail snapshot | `useDetailStream` hook instance | None |
| Schema versions / diff / dataset | TanStack Query cache | In-memory, keyed |
| Schema timeline | TanStack Query `['schema-timeline', id, table, snapshot]` | In-memory; a new snapshot is a new key, so a commit re-reads it |
| Time range, filters, order, statement, page, page size, visible and pinned columns, the run on screen | `DataTab` state, mirrored to the query string | Survives reload — a reload re-attaches to the same run — and is shareable as a link |
| The run's snapshot | `useRun`, fed by the run's event stream (polling if it drops) | Lost on navigation; the run itself keeps going server-side until it finishes |
| Pages of a run | TanStack Query, keyed by run, offset and readiness | Complete pages never change, so they are cached for good and the next one is prefetched |
| SQL editor height, column rail width | `localStorage` (`console.data.sqlHeight`, `console.data.railWidth`) | Survives reload; guarded for private browsing |
| JSON layout for exports and the row drawer's copy | `localStorage` (`console.data.jsonShape`: `nested` or `flat`) | Survives reload; nested when unset |
| Consumer-lag window, throughput metric | `DetailOverviewTab` local state | Lost on navigation |
| Search text, phase filter, sort | `MaterializationsPage` local state | Lost on navigation |
| Selected schema version, compare version, field filter | `SchemaTab` local state | Lost on navigation |
| Who is signed in | TanStack Query `['session']`, from the HttpOnly session cookie | The cookie persists until sign-out or the session's maximum length |
| Users, groups, roles, the permission catalog, the audit log | TanStack Query (`['users']`, `['user', id]`, `['groups']`, `['group', id]`, `['roles']`, `['role', id]`, `['permissions']`, `['audit', …]`) | In-memory; every directory change invalidates all of them and reloads the session |
| Theme | `localStorage['console.theme']` + `documentElement.classList` | Persistent |
| Toasts | `ToastProvider` context | Auto-dismiss |
| Settings and their history | TanStack Query (`['settings']`, `['settings-history']`) | In-memory. Re-read after a save, and every 5 s on the Runtimes section while a rollout is applying |
| Unsaved settings edits, conflict notice | `SettingsPage` local state | Lost on reload; leaving the page with edits asks first (`beforeunload`) |

There is no global client store (no Redux/Zustand/Jotai) and none is needed: live data comes from
hooks, server data from TanStack Query, and everything else is genuinely local to one screen.

**Pane sizes are deliberately not in the query string.** How wide the column rail needs to be
depends on how deep a materialization's field paths run, and how tall the editor needs to be depends
on what someone is typing — both are working preferences of one person at one screen size, not part
of the view a **Share** link should reproduce. Each read is wrapped in `try`/`catch`, so a browser
blocking storage still gets working panes, just not remembered ones.

> Known consequence: navigating away from the Data tab discards your filters and SQL. See
> [16 — Known gaps](./16-known-gaps-and-roadmap.md).

## 9.7 Feature modules

```
features/
  overview/OverviewPage.tsx                 Fleet dashboard
  materializations/
    MaterializationsPage.tsx                Live list: search, filter, sort
    CreateMaterializationDialog.tsx         Create form
    actions.ts                              useLifecycleActions + phase predicates
  detail/
    DetailPage.tsx                          Header, actions, tab shell, stop and delete confirmations
    EditMaterializationDialog.tsx           Changes the topics, the replica count and the data protection: each removed topic's table kept or dropped, kept tables dropped; shows what is fixed at creation
    topicChange.ts                          No React: what a choice of topics does to the tables (planTopicChange), and keepOrder
    TablesPanel.tsx                         The Overview's tables: written, kept, being dropped; what runs on each
    TableTab.tsx                            One table: figures, snapshots, maintenance and Run now, partitions, properties, Connect
    connect.ts                              No React: the Connect snippets per engine, following the location's scheme and its storage profile (tested)
    OverviewTab.tsx                         Rates, committed data, throughput, reconciliation, lag, spec
    lagTrend.ts                             Which way consumer lag is heading, how fast, and the time left
    RuntimeTab.tsx                          KasprApp observation, agents, binding
  schema/
    SchemaTab.tsx                           Version rail (dated by the data) + fields/changes/timeline switch
    SchemaFieldTable.tsx                    Field rows, type chips, first and last seen
    SchemaTreeView.tsx                      The fields nested by path
    SchemaDiffView.tsx                      Diff rendering
    SchemaTimelineView.tsx                  The changes by the day the data had them
    timeline.ts                             Dates from the data, in UTC: spans, version dates, the timeline's groups (no React; tested)
  data/
    DataTab.tsx                             Toolbar, status, rail, grid and pager; owns the view's state
    useRun.ts                               One run at a time: start, follow over SSE, cancel, re-attach; pages
    timeRange.ts                            Time range presets, resolution to instants, labels, URL form
    TimeRangePicker.tsx                     Splunk-style picker with per-preset row estimates
    StatusBar.tsx                           At the top of the table: progress while a run works, the exact row count after, Live's warning at its right
    MoreActions.tsx                         The toolbar's ⋮, right of Run: Peek, Share and Export, disabled when they do not apply
    Pager.tsx                               First / previous / page box / next / last, rows per page
    ExportOptions.tsx                       Formats for a finished run's result, and the JSON layout
    jsonShape.ts                            The remembered JSON layout (nested or flat)
    ColumnRail.tsx                          Grouped column list, visibility, pinning, inline profiles
    FilterChips.tsx                         Active filters and the builder: field search, then the value
    fieldSearch.ts                          Ranked, highlighted search over column names
    SqlPanel.tsx / QueryEditor.tsx          Statement editor (CodeMirror, lazy-loaded) with error markers
    RowDrawer.tsx                           Single-row detail, including hidden columns
    columns.ts                              Column grouping, default visibility, labels, older names
    viewState.ts                            Read/write the view in the query string
  protection/
    protection.ts                           No React: choices and policies, their text, what changes between two (tested)
    ProtectionControl.tsx                   A row per kind with its masks; the create and edit dialogs' and the Settings page's control
    ProtectionField.tsx                     The dialogs' block: the choice in a line, locked per dataset, the no-key warning
    ProtectionSummary.tsx                   Chips, the rollout badge, the shield on protected fields and columns
    ProtectionTab.tsx                       The Detail tab: policy and rollout, what is about to change, per-table fields and history
  auth/
    SignInPage.tsx                          Email and password, the providers the control plane offers
    AccessGate.tsx                          SessionGate, RequirePermission, AccessDenied, SignInPrompt
    ChangePasswordForm.tsx                  Current and new password, with the control plane's minimum length
    PasswordInput.tsx                       Password field with show/hide
  access/
    AccessSections.tsx                      Settings sections that are pages: account, users, groups, roles, audit
    AccountSection.tsx                      Your profile, password and sessions
    UsersSection.tsx / UserDetail.tsx       Users list (filter, search, sort) and one person's account
    AddUserDialog.tsx / GroupPicker.tsx     Add a user with a generated password; groups you may grant
    GroupsSection.tsx / GroupDetail.tsx     Groups, members and roles
    RolesSection.tsx / RoleEditor.tsx       Roles and the permission editor, with implications
    AuditSection.tsx                        The audit log, a page at a time
    SessionList.tsx / ConfirmDialog.tsx     Shared pieces
    shared.tsx / logic.ts                   Hooks and helpers; logic.ts has no React and is unit-tested
  settings/
    SettingsPage.tsx                        Section navigation, drafts, save bar, conflict and read-only banners
    sections.ts                             The sections, their groups and audiences, in navigation order
    SettingRow.tsx                          One setting: label, description, provenance, control, Reset / Undo
    RuntimeRollout.tsx                      How far the runtime-settings revision has reached
    DeploymentSection.tsx                   The deployment's read-only settings
    HistorySection.tsx                      Every scope's changes, newest first
    values.ts                               No React: parsing, validation, drafts → save request, formatting
```

### `features/settings`

The page renders whatever the settings API describes. A new setting in the control plane's
catalog or the console's appears in its section with no client change. Each setting names its
section, and `sections.ts` only orders sections and groups them (Account, Access, Platform,
Console, System).

Each section declares the `permission` that shows it (`null`: anyone signed in) and a `kind`:
`settings`, `deployment` and `history` read and save settings; `account`, `users`, `groups`,
`roles` and `audit` are pages of their own (`features/access`). The navigation lists only the
sections a person may see (`visibleSections`). Settings sections are read-only without
`settings.manage`, and the settings themselves are loaded only for someone with `settings.view`.

An edit is a **draft** until saved: `{ reset: false, input }` for a typed value, or
`{ reset: true }` to return to the deployment value. `values.ts` turns drafts into one change per
scope against the revision the page read (`buildChanges`). It checks each draft the way the server
will (`parseInput`), so the save button stays disabled while any draft is invalid. All of it is
unit-tested in `values.test.ts`.

A save sends every scope's changes in one request and reacts to the answer:

| Answer | The page |
|--------|----------|
| `200` / `202` | Replaces its data with the answer, clears the drafts, re-reads the history and `/api/config`. On `202` it says the control plane is still applying, and re-reads 3 s later |
| `400` naming a field | Shows the message under that setting and in a toast; drafts stay |
| `409` | Shows a banner with **Reload settings**; drafts stay. A change made since the page loaded reads as one sentence (`conflictNotice`) |

While the save bar shows, the page sets `--toast-offset` on the document, so toasts stack above the
bar rather than over its Save button.

### `features/materializations/actions.ts`

```ts
export function useLifecycleActions(): { run: (action, id) => void; pending: LifecycleAction | null }
// LifecycleAction = 'stop' | 'resume' | 'delete'
export function canStop(detail):   boolean  // desired running (or legacy paused), has a runtime, not being deleted
export function canResume(detail): boolean  // desired stopped (or legacy paused), not being deleted
export function canEdit(detail):   boolean  // has a runtime (a schema), not being deleted
export function canDelete(detail): boolean  // observed anything except deleting | deleted
```

Stop and Resume follow the **desired** phase, not the observed one: a materialization converging
toward running can be stopped, and one that is still stopping already offers Resume. Stop and Edit
need a runtime to apply, which exists once discovery has written a schema (`schema_version`); the
control plane refuses either before then with `409 runtime_not_deployed`.

### `lib/defaults.ts`

`useMaterializationDefaults()` reads the control plane's defaults for a new materialization from
the console config (`materializationDefaults`), asking again when its copy is over a minute old,
and derives the dataset and table name rules from them — the control plane's own reserved-word
list when it has reported one, the console's copy (`shared/tableNames.ts`) otherwise — with the
label's length and the topic limit. The create and edit dialogs, and the dataset dialogs, use it.

The mutation deliberately performs **no optimistic update**. Its `onSuccess` shows a toast that
tells the user to watch the phase converge; the actual state change arrives over the stream. The
`pending` value drives per-button spinners.

## 9.8 Performance decisions

| Technique | Where | Why |
|-----------|-------|-----|
| SSE fan-out | `lib/live.ts` | One connection per page, not one request per component |
| `useDeferredValue` | `MaterializationsPage` search | Typing stays responsive while the list re-filters |
| `useMemo` on derived lists | Overview, list, schema, data | Filtering and sorting re-run only when inputs change |
| Row virtualization | `DataGrid` | Only the visible slice of up to 5 000 rows is mounted |
| `lazy()` + `Suspense` | `QueryEditor` | Keeps ~145 kB gzip of CodeMirror out of first load |
| Hand-rolled SVG charts | `Sparkline`, `TimeSeriesChart` | No charting library; a chart re-render is a few path strings |
| Snapshot reuse in `emitAll` | server | One serialization shared by all fleet subscribers |

### `DataGrid` virtualization

```ts
const ROW_HEIGHT = 32;
const OVERSCAN = 12;

const first   = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
const visible = Math.ceil(height / ROW_HEIGHT) + OVERSCAN * 2;
```

Fixed row height keeps the scroll math exact and jitter-free. Column widths are measured once from
the first 40 rows:

```ts
Math.min(Math.max(widest * 7.6 + 28, 110), 420)   // px
```

The header is a sticky CSS grid; rows are translated by `start * ROW_HEIGHT` inside a spacer of
`rows.length * ROW_HEIGHT`.

## 9.9 Accessibility

- Tabs use `role="tablist"` / `role="tab"` with `aria-selected`.
- The dialog is `role="dialog" aria-modal="true"` with an accessible label, closes on `Escape`,
  locks body scroll, and restores the previous overflow on unmount.
- Toasts are `role="status"` (polite), so they are announced without interrupting.
- Icon-only buttons carry `aria-label` (`Copy <id>`, `Toggle theme`, `Dismiss`, `Previous page`).
- Focus is visible globally via `:focus-visible { outline: 2px solid var(--color-accent) }`.
- `@media (prefers-reduced-motion: reduce)` disables the pulse and fade animations.
- All text meets contrast requirements in both themes; phase colours are always paired with a text
  label, never colour alone.

## 9.10 Error and empty states

Every asynchronous surface has four states, and all four are implemented:

| State | Rendering |
|-------|-----------|
| Loading | `Skeleton` blocks matching the final layout, so there is no reflow |
| Empty | `EmptyState` with a title, an explanation of *why* it is empty, and usually an action |
| Error | Inline panel (data) or an `EmptyState` variant (dataset), with a recovery action |
| Success | The content |

The distinction between "empty" and "error" is enforced: `DataTab` checks `summaryQuery.error`
before it says a table has no data yet, so *No data to query yet* and *<table> is not readable* are
never confused. Getting this wrong was an actual bug caught during verification.
