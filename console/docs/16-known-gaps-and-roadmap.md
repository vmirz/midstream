# 16 — Known Gaps and Roadmap

Deliberate omissions and real limitations of the console. Check here before filing something as a
bug. The control plane's own list is handbook [14](../../docs/handbook/14-known-gaps-and-roadmap.md).

## 16.1 Security and access

| Gap | Impact | Notes |
|-----|--------|-------|
| **Access is not enforced by the storage** | Anyone holding credentials that read the bucket — the console's own included — reads every table in it, whatever their roles | Roles decide what the control plane, the REST catalog and the console serve (handbook 14 §14.8) |
| Data access is per dataset, not per table or column | A Dataset Reader on `sales` reads every table in it, all of each | No per-table or per-column policy ([15 §15.4](./15-extending-the-console.md)) |
| Lifecycle permissions are per action | Anyone who may stop or delete may stop or delete any materialization — deleting, editing the topics and dropping tables also need write access to the tables' datasets | A grant cannot be scoped to a materialization (handbook 14 §14.8) |
| Names are not secret | Anyone who may see datasets sees every dataset's name and its tables' names, locked ones included; anyone who may see materializations sees their topics, totals and status | Schemas and data are what a dataset role guards |
| ~~An access token reads every table~~ | **Resolved** in 3.0: a token holds no more than its maker on each dataset, and can be limited to datasets (handbook 18 §18.15) | |
| ~~No single sign-on~~ | **Resolved**: Entra ID, GitHub, Google or any OpenID Connect provider, into every domain at once, each domain checking the result itself (handbook 20 §20.7) | |
| No password recovery, no email | A password is reset by an administrator | Sign-in through a provider needs neither |
| The federation key is one secret for every domain | Whoever holds it can make an access token any domain honours, for anyone there | Keep it in each domain's auth Secret only; rotate it everywhere together ([12 §12.2](./12-security-model.md)) |
| A provider's sign-in in progress lives in one console process | With several replicas, the callback must reach the replica that started it; a restart forgets flows in progress | Sticky sessions, as for runs ([13 §13.4](./13-operations-runbook.md)) |
| No `request` first-sign-in policy | Someone a domain has never seen is either made an account (`open`) or refused (`invited`); there is no pending account for an administrator to approve | Handbook 20 §20.16 |
| The sign-in limit trusts reported addresses | A client reaching the pod without the ingress can choose its address and evade the per-address limit | The control plane's per-email lockout still applies |
| No CSP header | Weaker XSS defence in depth | React escaping is the current control; CodeMirror needs `style-src 'unsafe-inline'` |
| No rate limiting beyond sign-in | A signed-in client can open many SSE connections or issue many queries | DuckDB limits bound each query, not the request rate |
| ~~A hand-written statement could read any file~~ | **Resolved** in 1.0: every statement runs in a DuckDB sandbox that can read only its run's files ([07 §07.6](./07-data-access-and-duckdb.md)) and write only its own directory | |
| ~~A table could name files anywhere~~ | **Resolved** in 1.0: the console reads nothing outside the location the control plane recorded ([12 §12.6](./12-security-model.md#126-paths-a-table-names)) | |

## 16.2 Data and correctness

| Gap | Impact |
|-----|--------|
| ~~Schema history starts when the console does; the archive is per replica~~ | **Resolved** in 1.0: the control plane keeps every schema the table has had, and the console keeps none of its own (06) |
| The console reads the table, not the topic | It shows what is committed; records staged on the runtime's members are not visible until their commit — **Complete through** says up to when everything is in |
| A snapshot with delete files is refused | The materializer writes none; an external engine with `catalog.write` might, and the console would rather refuse than show rows a delete removed |
| So is one with data files outside the table's location | An engine's `add_files` of files kept elsewhere; the console reads only under the location |
| A run pinned to an older snapshot can find it expired | Only through the runs API's `asOf`: the Data tab always reads the latest. Snapshot expiry keeps every snapshot of the last hour, then one an hour for 72 hours (the defaults); a pinned run on an expired one answers that it may have expired |
| Lag history and rate series live in the console's memory | A console restart starts the lag and throughput charts over |
| A `deleting` materialization is in no `FleetTotals` phase bucket | The four sub-counts do not always sum to `materializations` |
| Schema dates are as precise as the table's files | Read from manifest statistics (06 §6.3): a UTC day in a day table, up to a month in a month table; in an unpartitioned table compaction merges files across the table's whole span, after which its changes read that wide. Exact dates would need a scan per field (handbook 14 §14.6) |
| A version's date is its earliest field's | A version that only collapsed or forked fields is dated by the new members' first values; one whose fields hold no value yet keeps the commit time (*committed 2 days ago*) |

## 16.3 Data explorer

### Behaviour at scale

File **count**, not row count, is what the console is sensitive to: every data file is a manifest
entry to decode and, when a range or filter reaches it, a footer and row groups to read. A busy topic
in steady state writes about a file per partition per batch, and compaction joins a partition's small
files once it settles, so a day ends with a few files. A **backfill** of old data is the hard case:
each seal writes a file for every day its rows touch. Loading two years of the load-test topic, the
table held 14,816 files over 618 days at 55 M rows while compaction took one day at a time; with a
backfill's past days compacted many at a time it never held more than 6,000 (handbook 13 §13.8).
Until a day is compacted, a query over it reads its small files — correct, slower.

| Gap | Impact |
|-----|--------|
| An exact count under a filter needs a pass over the range | Unavoidable: matching rows have to be read. The pass reads only the filtered columns, reports exact progress, and pages appear as it goes; a narrower range makes it proportionally cheaper |
| Column headers do not sort | Deliberate: an arbitrary sort has to read every row in range before showing one. Browse order is newest or oldest first; a statement's `ORDER BY` sorts anything |
| Newest first is per file, not a global sort | Files are ordered by the latest Kafka timestamp they hold and rows within each by offset. Records in different partitions with interleaved timestamps are not merged row by row |
| A statement's result keeps at most `CONSOLE_RESULT_ROW_LIMIT` rows | Its total is always exact; paging and export stop at the kept rows and say so |
| Exports stop at `CONSOLE_EXPORT_ROW_LIMIT` | A browser download is the wrong tool for a whole dataset |
| Runs live in the console process | A restart drops in-flight runs and stored results, and replicas cannot share them (§16.4) |
| Cold scans are latency-bound | From outside the cloud each file costs a few ~50 ms round trips; overlapping 48 files per run and caching footers and bytes for the process's life (files are immutable) keep it bounded |
| A statement's sandbox starts cold | ~30 ms to set up, and none of the shared instance's footer and byte caches: a statement re-reads footers that a browse run already had |
| All-time statements are slow from far away | A statement is one DuckDB query, bounded by `CONSOLE_DUCKDB_THREADS`. The time range is the lever; running the console near the bucket is the fix |
| **GCS only** | Matches the runtime, which is GCS-only |
| Cached mode downloads whole files | Bounded by `CONSOLE_CACHED_MODE_MAX_FILES` / `_BYTES` per scan; pages read one file at a time and are never refused. Direct mode avoids downloads entirely |
| No query history or saved queries | A view lives in the address bar and can be shared, but nothing is kept server-side |
| No cross-materialization queries | A statement reads and joins the tables of one materialization; another materialization's tables are not there, and `iceberg.<other>` is refused. Engines on the REST catalog can join any tables |
| No result charting or timeline | Results render as a grid; the manifests already know where rows sit in time |

### The live tail

| Gap | Impact |
|-----|--------|
| Live follows the first page only, newest first | New rows would shift every page being read; past page 1, with a row open, or hidden, Live waits and says so. Oldest first, new rows go at the end and every page keeps its rows |
| A statement is refreshed by running it again | Its result is materialized: Auto re-run starts a new run five seconds after each one, rather than appending rows |
| A tail member's restart pauses its tables' tails | Static membership keeps its tables with it while it is away, about the time its package takes to install (~25 s on the lab); the tab says the tail is unavailable meanwhile and shows committed rows. Scaling the tail app restarts every member |
| The bound is soft by one fetch | A table pauses at `live_tail_max_rows_per_table` after the fetch that crossed it, so it can hold up to one fetch more |
| Engines see committed rows only | The tail is the console's: an engine on the REST catalog reads the snapshot, as before |
| A snapshot older than the tail's retention has no tail | A run left on screen past a commit and `live_tail_retention_seconds` is replaced by Live on its next look; a run paged without Live keeps its committed rows |
| The deployment's switches reach open tabs within two minutes | `live_tail_enabled` changed outside this console shows here within a minute (the cached control-plane settings), and an open Data tab reads the console's config every minute; a materialization's own switch, a stop and a resume show at once |

## 16.4 Real-time model

| Gap | Impact |
|-----|--------|
| **Not horizontally scalable** | Each replica polls independently and holds its own series and runs. See [13 §13.4](./13-operations-runbook.md#134-running-more-than-one-replica) |
| Series are in memory only | A restart resets every chart to "Collecting…" |
| Full snapshot per SSE frame | Bandwidth grows linearly with fleet size |
| Rates need two samples | A freshly opened page shows `—` for one cadence interval |

## 16.5 Control-plane coverage

The console exposes everything the control-plane API offers. Missing because the API does not offer
it:

| Missing | Why |
|---------|-----|
| Changing any setting but the topics, the replica count and autoscaling | `PATCH` changes only those, and the label; destination and partitioning are fixed with the tables |
| Choosing a materialization's destination, namespace, runtime name or storage | The deployment's since 4.0: the control plane refuses a create that names one (handbook 04 §4.5.2) |
| Forcing an immediate observation or reconcile | No endpoint |
| Rewinding or seeking the source consumer | No endpoint; the table's offsets are the checkpoint |
| Operation records | `materialization-op:` records exist but are not exposed |
| Bulk operations | No endpoint; the console has no multi-select either |

Adding any of these is a control-plane change first, then a console change — see
[15 §15.1](./15-extending-the-console.md#151-adding-a-control-plane-endpoint).

## 16.5a Settings

| Gap | Impact | Notes |
|-----|--------|-------|
| Settings need the control plane | While it is unreachable nothing can be saved, and its own settings are not shown | The console keeps applying the console overrides it last read |
| Console replicas converge within 30 s | After a save, another replica may serve the previous value for up to 30 s | The replica that saved applies it at once |
| One conflict check per scope | Two people editing different settings of one scope conflict, and the second must reload | Their edits are kept across the reload |
| No deployment-locked settings | A deployment cannot stop an administrator overriding a setting | Handbook [17 §17.2](../../docs/handbook/17-settings.md) |
| No per-person preferences | Everything on the page is shared by all users | A per-user scope would hold them ([15 §15.9](./15-extending-the-console.md)) |

## 16.6 UI and UX

| Gap | Impact |
|-----|--------|
| Mobile layout is functional, not designed | Below 768 px the sidebar is a nav strip; the account menu and theme controls are unreachable |
| No keyboard shortcuts beyond `⌘↵` and `Esc` | No command palette, no `/` to search |
| The materializations list shows schema ids as `v0`, `v1`… | The Schema tab says *schema 2*; same id, two spellings |
| No bulk selection | One materialization at a time |
| Toasts are the only mutation feedback | No persistent activity log; the Table tab's maintenance history is the one exception |

## 16.7 Engineering

| Gap | Impact |
|-----|--------|
| No `react-hooks` lint | The repository runs no linter by design; `src/features/data/hooks.test.ts` is the narrow substitute |
| No CI wiring for `/console` | `npm run typecheck && npm test && npm run build` should be added to the pipeline |
| No Dockerfile in-repo | [13 §13.1](./13-operations-runbook.md#131-deployment-shape) has an illustrative one |
| No OpenAPI document | [08](./08-console-api-reference.md) is the contract |
| No OpenTelemetry instrumentation | Only Pino logs exist today |
| The Avro and Iceberg readers are the console's own | `server/iceberg/` decodes manifests with `avsc` and follows Iceberg's inheritance rules itself; E5 and `iceberg.test.ts` check it against PyIceberg, but a table written by another engine with features the materializer does not use (v3, deletion vectors, other transforms) may not read |

## 16.8 Suggested roadmap

Ordered by value per unit of effort.

### Near term

1. **CI wiring** — typecheck, build and test for `/console`.
2. **A timeline over the time axis**, from manifest bounds while a run is planned and exact counts once
   it is done.
3. **OpenTelemetry traces** on control-plane calls and DuckDB queries.
4. **Keep a statement sandbox warm** per run, so a second statement does not start cold.

### Medium term

5. **Sorting the structured view** by spooling a sorted statement behind a header click.
6. **Persist column visibility and page size** per materialization in `localStorage`.
7. **Query history** per materialization, reopenable while their results last.
8. **Incremental SSE payloads** — send changed items rather than the whole snapshot.
9. **Time travel by timestamp** — the runs API's `asOf` at a moment rather than a snapshot (the Data
   tab no longer offers older snapshots).

### Longer term

10. **Shared state for horizontal scaling** — series and run state in Redis or similar.
11. **Per-materialization access**, with per-table access tokens.
12. **A second execution tier** for arbitrary queries at much larger scale: engines on the REST
    catalog already read the same tables.

## 16.9 Explicit non-goals

These are not gaps; they are decisions.

- **Not a general BI tool.** No dashboards, no saved reports, no scheduled queries. Engines on the
  REST catalog are.
- **Not a Kafka console.** Topics, consumer groups and partitions appear only as materializer
  metadata.
- **Not a Kubernetes console.** The Runtime tab shows what the control plane observed, not live pod
  state.
- **Not a write path for data.** The console never writes to a table or to object storage; even
  **Run now** asks the runtime to do the work.
- **Not part of the Python package.** The boundary is the point.
