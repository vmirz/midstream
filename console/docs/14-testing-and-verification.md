# 14 — Testing and Verification

Verification has two layers since 1.0: a unit suite run by `node --test` (§14.6), which runs the
Iceberg reader and the run engine against a real table the control plane's own library wrote
(§14.2) and against Parquet the tests write; and verification on the lab, through the API and in a
browser, against real tables (§14.4). Releases before 1.0 also had an end-to-end harness with a
stand-in control plane and a fake object store (§14.3); a fake listing cannot stand in for an
Iceberg table, so the fixture table and the lab replaced it. This document is the reproducible
procedure.

## 14.1 Static checks

```bash
npm run typecheck   # tsc -p tsconfig.app.json --noEmit && tsc -p tsconfig.server.json --noEmit
npm run build       # vite build
npm audit           # expect: found 0 vulnerabilities
```

The server typecheck also enforces `erasableSyntaxOnly`, which is what prevents someone from
introducing an `enum` or a constructor parameter property that Node's type stripping cannot run.

## 14.2 The fixture table

`testdata/iceberg/orders/` is a materializer table written by the control plane's own library:
three appends over two days, a schema change that forks `total` (long → double) and
`customer.phone` (string → struct) and adds `coupon`, a fourth append under the new schema, and a
compaction of the first day — 12 rows in three live files, five snapshots, two schemas. Every
object is kept at its path below the table's location, `gs://lake/materializer/mat-a1b2c3d4e5f6/orders`,
and `expected.json` records what PyIceberg says the current snapshot holds and the control plane's
projection of each schema (E5).

| Used by | For |
|---|---|
| `server/iceberg/iceberg.test.ts` | The reader against PyIceberg: pointer, metadata, the snapshot's files with counts, sizes, partitions and bounds, an older snapshot, 64-bit ids, the schema projection field for field |
| `server/runs/table.test.ts` | Runs end to end: totals from manifests, folded forks, a compacted file, struct filters, as-of, statements over the logical and native tables, a join of two of a materialization's tables in two datasets, only the tables a statement names read, the statement sandbox, a run unable to read another's result, nothing read outside the table's location, unreadable tables answered `503` |

`iceberg/testing.ts::fixtureReader` serves the `gs://` objects from disk through the reader's
test seam (`setObjectReader`), and `rewriteAvro` gives a test a manifest that names files
elsewhere.

To regenerate it — after a change to what the control plane writes:

```bash
python python/tests/console_fixture.py           # writes console/testdata/iceberg/orders
```

Every run makes new snapshot ids and UUIDs, which some tests name, so commit the table and its
`expected.json` together and update those tests.

## 14.3 The 0.x harness

Releases 0.1 to 0.14 were verified through three processes: a mock control plane (port 6065)
serving the WebView payloads with mutable state, a schema promotion on a timer and lifecycle
delays; a fake GCS (port 4443, through `STORAGE_EMULATOR_HOST`) serving Parquet in the 0.x object
layout; and the real console server pointed at both. A script asserted system, fleet, detail,
schema, data, SQL, export, streaming and mutation checks from a clean state. Two of its lessons
still hold for any harness:

- A streaming rate check needs **at least 8 frames**: with no detail subscriber the rotation
  cadence is 6 s, so per-materialization rates appear only around the fourth frame.
- Run convergence checks **with no SSE subscriber**. That is the condition under which REST reads
  served stale snapshots (defect 2 below), and it is invisible in a browser.

## 14.4 Browser verification

### 1.0.0-rc.1 on the lab: the console from its image

Verified on 2026-10-26 with Midstream 1.0.0-rc.1 installed from the published chart and the console
from `ghcr.io/vmirz/midstream-console:1.0.0-rc.1` in the cluster (the chart's Deployment, with the
HMAC Secret for direct `gs://` scans), reached through the chart's HTTPRoute and a port-forward,
Playwright (Chromium), light theme (handbook 13 §13.8i).

| Check | Result |
|---|---|
| `/api/health` through the route | `status: ok`, `controlPlaneConnected: true`, `version: 1.0.0-rc.1`; the pod's log at start, *data access mode: direct* |
| A guest | The Overview with the fleet at zero and *Sign in* in the navigation |
| The bootstrap sign-in | `/sign-in`, then *Choose a new password* with the current, new and confirming fields, **Set password and continue** disabled until all three are filled; afterwards signed in as *Admin*, with Datasets and Settings in the navigation |
| Datasets › New dataset | `sales` with a description: *Dataset created*, its page with *Tables (0)* and `sales.<table>` spelled out |
| Materializations › New materialization | *Select topics*, the search listing `orders` with its partitions and the internal topics hidden (29 of them); the one dataset preselected and the table named after the topic, *SQL reads each table as sales.orders*; **Create** → the detail page *Deploying*, *Reconciling toward Running*, the toast *Materialization accepted* |
| An empty topic | *Running*, *Healthy*; the Data tab with the seven Kafka columns and *No rows match* |
| The live tail | On a materialization of six JSON orders: the Data tab *6 rows*, the inferred fields (`order_id` … `status`, `items[].sku`, `items[].qty`) with the Kafka metadata collapsed to the timestamp, before the first commit |
| After the first commit | The Overview *ROWS COMMITTED 6*, *complete through* the records' time, *TABLE SIZE 7.5 KB · 1 data file · 1 snapshots kept*; the Table tab's snapshot row *append +6 +1 6 p0 5*, took 1.2 s, *Orphan cleanup succeeded · 0 files deleted · 3 listed*, the properties naming `sales.midstream_smoke_orders` and its location; the Data tab unchanged, the same six rows now from the snapshot |

### 4.4.0 on the lab: autoscaling and restart health

Verified on 2026-10-25 with the control plane at `feature/runtime-autoscaling` on the lab, the console
server on a laptop through the HTTPRoute, Playwright (Chromium) in light and dark — every check passed
in both themes, with no page errors (handbook 13 §13.8h).

| Check | Result |
|---|---|
| `GET /api/config` | `autoscaling: { enabled: true, maxReplicas: 16, targetMinutes: 30 }` |
| The Overview of a runtime scaled up | The Reconciliation panel: *Scaled up to 16 members 13 minutes ago: 110,869,158 records behind, about 22 h 43 min to catch up with 1 member. Back to 1 once caught up.* The Specification: *Replicas 1 · 16 while catching up*, *Autoscaling On · up to 16* |
| The edit dialog | **Autoscale** checked, *Running 16 members while it catches up; back to 1 once caught up.*; unchecked, *Turned off, it goes back to 1 at once.* and **Save** enabled. Saved, the toast *Autoscale test: autoscaling off*, and the runtime at one member within 10 s; switched on again, *autoscaling on* |
| Once caught up | The control plane put the runtime back at one member; the Overview without its *Scaled up* line, *Replicas 1*, *Autoscaling On · up to 16*, *Running* |
| The create dialog | *Advanced settings* summarised *Partitioning, replicas, autoscaling, labels, tombstones*; **Autoscale** checked, *Adds members while it is far behind its topics, up to 16, and goes back to its replicas once caught up. Can be changed later.* |
| A runtime whose members keep restarting | The header's health **Unhealthy**, its tooltip *8 of 10 members restarted in the last 30 minutes: out of memory.*; the fleet's attention list *Runtime members restarting* |

### 4.3.0 on the lab: a live tail for every materialization

Verified on 2026-10-24 with the control plane, maintenance app and tail app at d1592b4 on the lab, the
console server on a laptop through the HTTPRoute, Playwright (Chromium) in light and dark — every check
passed in both themes, with no page errors (handbook 13 §13.8g).

| Check | Result |
|---|---|
| The create dialog | *Advanced settings* hold partitioning, replicas, labels and tombstones, and no live tail option |
| The edit dialog | *Change its name, the topics it reads and its scale*: no live tail switch |
| The Overview | No *Live tail* row |
| The Data tab | Live on by default for a materialization created without a word of live tail, *Live · through now*, its rows from the tail before the first commit; a record produced while the tab watched shown in 6.1 s and 2.9 s, the producer's own start included |

### 4.1.0 on the lab: the live tail

Verified on 2026-10-23 and 24 on the lab with the control plane, runtimes and tail app at
`feature/live-tail`, the console server on a laptop at the branch's head reaching the tail app
through the lab's HTTPRoute, Playwright (Chromium) in light and dark. A producer numbered and timed
probe records on `fresh.orders`; the lab steps themselves are in handbook 13 §13.8e. For the states
the lab cannot be put in on demand, a second console server read the tail through a small stub
server, a test aid not kept in the repository, that passed the real tail app's answers through or
answered as a table in another state would: refusals for warming up, moving, unavailable, off and
backlogged, and headers rewritten for truncated, settling and behind.

| Check | Result |
|---|---|
| Freshness | Produce → visible on the Data tab in Live: p50 0.93 s, p95 1.63 s over 300 probes; over an hour's soak with two tabs open, p50 1.13 s, p95 1.9 s, p99 2.7 s over 3,540 probes each — 5 of 7,080 over 5 s, at most 6.7 s — after the tail began fetching in batches (19 §19.7) |
| Rows without Run | New probes at the top of the grid while nobody pressed anything; in *Last 15 minutes* too, whose run slides with each commit |
| The status line | Through the stub, each in light and dark: *The newest 3,000 records are not shown yet …* (truncated), *2 rows are waiting for a schema change*, *Live tail is 7 s behind*, *warming up*, *moving between members*, *unavailable — showing committed rows and 32 rows read before*, *not following this table yet*; each back to *Live · through now* when the stub passed the tail again — settling at the commit that replaced the run, since the rows the run holds stay as they were read |
| The peek | During a stubbed backlog, *The runtime is 2,500,000 records behind …* and **Peek at the newest records**: the panel with the topic's newest records in the table's columns, labelled *not in the table yet*, read again while open, and closed by itself once the table was live. On a real backlog: handbook 13 §13.8e, step 11 |
| The toggle | Off written to the address bar as `live=0`, kept through a reload, on again removing it; *Live paused while this tab is hidden* and resumed when seen; no toggle in SQL, whose *Auto re-run* ran the statement every 5 s (3 runs in 16 s) and stopped when turned off |
| Switches | The edit dialog turning a materialization's live tail off and on: the toggle gone, then back; the create dialog pre-checked from `live_tail_default`, sending `live_tail: false` unchecked and nothing when checked (its request stopped by the check, nothing created); a stop and resume, the deployment-wide switch, as handbook 13 §13.8e step 9 |
| Signed out while following | The sign-in prompt, and no refresh after it |
| Fifty tables in rotation | During the hour's soak, a third browser opening each of 50 tables' Data tab for 20 s, parked or not: all 180 visits *Live · through now*, p50 1.2 s and p95 2.0 s after the page opened |
| Console restart | Two Live tabs open while the console stopped for 4 s: each ran its view again (*Starting…*) and followed the tail 6 s after the stop, every probe of the window shown once; the browsers' only errors were the dev proxy's `502`s while it was down |
| Control-plane restart | A Live tab through a control-plane restart: before the fix in f382b0a, *Live stopped: sign in again* as the restarted control plane answered from a directory it had not read back yet, and the console cleared its cookie; after it, the control plane answered `503 starting` meanwhile and the tab followed through the restart, picking the tail back up by itself (handbook 13 §13.8e, step 15) |
| Bursts, a member killed | 50,000 records, the owner deleted 20 s in: *N rows are waiting for a schema change* (the burst carried a new field), *the newest records are not shown yet*, then *unavailable*, *moving between members*, *warming up*, the catching-up notice and *Live*, in that order; 1,000,000 records: *The runtime is 517,755 records behind; the live tail starts once it catches up* with **Peek**, then *Live tail is 241 s behind* and *Live*, 4 minutes after the burst |
| A real backlog | A 600,000-record topic's new table: the backlog notice counting down as the runtime committed, the peek open, then *Live · through now* by itself 2 minutes later (handbook 13 §13.8e, step 11). Held backlogged by the stub while Live started a run at each commit, the peek stayed open through four of them and showed every one of 88 probes, p50 0.9 s after it was produced — once the fix for 30 was in |
| Newer data | *Newer data available* never on screen while Live followed a table through commits for two minutes (57 % of the time before the fix, §14.5, 29); with Live off, shown once a commit landed after the run |

It found four defects in the console, fixed before release (§14.5, 27–30), and in the tail app and
the control plane those listed in handbook 13 §13.8e — among them a restarted control plane signing
everyone out of the console.

**Delivery to runtimes (32).** At 633be69, the Settings page's Runtimes section on a laptop console, in light
and dark: *Runtimes pick up revision 19 …* and *1 of 1 running current* while the lab's one runtime ran
the current lineage; once it ran another — a later lineage sent to it alone — *0 of 1 running current*
and the materialization listed as **Applying** with no revision, its hint naming settings from before
this installation. The page follows a rollout only while one is applying when it loads, so it showed
the change on the next load (handbook 13 §13.8e, step 20).

**GCS reads (31).** The console's reader at 13d58f7 against the lab's bucket with a laptop's
credentials: a table's pointer and a metadata file read 30 times each, 15 one after another and 15
at once, every read the object's bytes (size and MD5 as GCS reports them) and its CRC32C checked; a
data file mirrored into a scratch cache whole, with no part file left; a missing object
`GcsReadError 404`; and not one `MaxListenersExceededWarning`, where the client library's `download()`
printed two for each read.

### 4.0.0 on the lab: month partitions, the deployment's place and scale, dates from the data

Verified on 2026-10-23 on the lab with control plane and maintenance app 4.0.0
(`feature/partitions-create-schema-history`, 396027a), the console server on a laptop at the
branch's head, Playwright (Chromium) in light and dark. One topic was made for it, `history.orders`:
three partitions of 2,000 records each, produced with Kafka timestamps spread from 2025-01-01 to
2026-10-19 in four shapes — `device`, `items[].tax` and `promo_code` from 2025-04-15;
`customer.phone` an object, `total` a double and `coupon` from 2025-09-01; `customer.email` and
`promo_code` gone and `customer.email_hash` and `fraud` from 2026-02-10. Two materializations read
it into the dataset `history`: *History by day* (`orders_by_day`) and *History by month*
(`orders_by_month`).

| Check | Result |
|---|---|
| Create, in the browser | No destination, namespace, runtime name or storage field; Advanced settings naming `gs://my-lake` and `128Mi`, and `64Mi` once that became the default; partitioning offering By hour, By day, By month and Not partitioned. With the destination cleared on the control plane, the alert and **Create** disabled — after the minute the console keeps its defaults |
| Create, through the API | Each of the four refused `400`, naming the deployment's value; with no destination `409 destination_not_configured`; the topic analysis recommending replicas alone |
| Runtimes | Both in `midstream`, named `<release>-mat-<id>`, each member with a `128Mi` volume (the default then), 72 KiB of it used once committed; the three 3.x runtimes, re-applied by the upgrade, kept their `1Gi`. At the `64Mi` default a third materialization's member had 55 MiB usable, its store 67 KiB with a 4 MiB RocksDB write buffer, and its table every offset once |
| Month partitions | 22 partitions, `2025-01` to `2026-10`, on the Table tab and in PyIceberg (`month` transform, months since 1970); every file's timestamps inside its partition; DuckDB 2.0 through the REST catalog with a token limited to `history`: each partition's offsets 0–1,999 once, rows per month and per day as produced, every field's count as its shapes say |
| Dates from the data | The day table's Timeline and field rows: first values on Apr 15, 2025, Sep 1, 2025 and Feb 10, 2026, last values on Aug 31, 2025 and Feb 9, 2026 — the producer's boundaries to the day; the month table's the same months, `Apr 1 – 30, 2025`; schema 0 *Initial · sampled*, schema 1 *data from Apr 15, 2025*; the first records' five fields behind **Show**; a field name opening it in the Fields view |
| Access | With All Users' grant taken off `history`, the timeline `403` to Rita and Otto and `200` to the owner, and no Schema tab for Rita |
| Compaction | The day table in one job, 657 partitions: 1,971 files to 657 in 4 minutes. The month table's 21 past months, 63 files to 21, the open month left alone — no *compact* mark — until its files were an hour old, marked then, and compacted at the next pass, 3 files to 1; exactly once after each |

It found three defects in the console, fixed before release (§14.5, 24–26): two in the new dates,
and an older one that month partitions make visible. It found none in the control plane.

### 3.0.0 on the lab: datasets and per-dataset access

Verified on 2026-10-22 on the lab cluster with control plane 3.0.0 (`feature/datasets`,
4cdb65b), on a clean slate: every 2.0 materialization, its topics, consumer group and files deleted,
and the directory started empty. The console server ran on a laptop against the lab's control plane
and bucket, with the dev client and Playwright (Chromium) signed in as four people: Olive, who owns
the datasets `sales` and `finance`; Walt, a Writer on `sales`; Rita, a Reader on `finance`; and Otto,
with no dataset role. `finance` had All Users' Reader grant removed, so it was locked to Otto. Three
topics were produced for it: `sales.orders` (3,005 records), `finance.invoices` (3,005) and
`sales.refunds` (405), three partitions each.

| Check | Result |
|---|---|
| Names | `Sales`, `main`, `select` and `sales-eu` refused as dataset names (`400`), a dataset name in use refused (`409`); in the dialog, upper case refused as typed, a name the dataset holds said on its row at once, the new row started in the only dataset Walt may write, `finance` locked in his picker |
| Two creates racing for `sales.returns` | Exactly one won; the other was refused `409 table_name_taken` on its row, and its materialization marked deleted with that reason |
| Per-dataset access, through the API | Grants set by Olive (Guest refused, Otto refused `403`), each person's access per dataset as the list reports it, then 24 checks across the four: schemas and their versions, each table's `readable`/`queryable` in the detail, resolving `finance.finance_invoices` (its location to readers only), the list's search and labels, and changes refused without write access on every dataset they touch — adding a `finance` table, removing one's topic, deleting its materialization — while a relabel needs none |
| Per-dataset access, in the browser | 36 Playwright checks: Otto sees `finance` listed and locked, its tables' names but no Access tab, its table locked in the Tables panel and the pickers, the Schema tab falling back to `sales.refunds`, a statement naming `finance` refused, naming it; Olive's Access tab with the storage gap stated; Rita's statement joining `sales.refunds` with `finance.finance_invoices`, the native `iceberg.finance.finance_invoices`, and the Connect panel naming `lake.finance.finance_invoices` |
| The REST catalog | Namespaces are the datasets the token may read: both for Rita's token, `sales` alone for her token limited to it and for Otto's, `finance.finance_invoices` a `404` to both; a namespace's properties carry its description. DuckDB joined `lake.sales.orders` with `lake.finance.finance_invoices` — tables of two materializations — on `order_id`: 3,005 rows |
| Editing the topics | Adding `sales.refunds` to Orders as `sales.order_refunds` through the dialog: a held name refused as typed, the datasets named, saved once the name was claimed (about 0.1 s) |
| Delete and claim again | Deleting the materialization holding `sales.returns` freed the name; a new one claimed it, and the catalog resolved it to the new table |
| Exactly once | Every table's offsets per partition against its topic: each offset once, none missing — `finance_invoices`, `orders`, `refunds`, `returns` and `order_refunds` |

It found one defect, in the control plane: the runtime-status agent crash-looped on a heartbeat from
a materialization it no longer held — the deleted 2.0 `shop`'s runtime, still reporting while it
stopped. Status for an unknown materialization is now dropped and logged once (4cdb65b).

### 2.0.0 on the lab: several topics, one runtime

Verified on 2026-10-20 on the lab cluster with control plane 2.0.0 (51ca6ab): the console
server on a laptop against the lab's control plane and bucket, the dev client, and Playwright
(Chromium, 1512×950) signed in as the lab's administrator, once in each theme. `shop` read
`mtm-orders` (4 partitions), `mtm-payments` (2) and `mtm.shipments.v1` (3) with 2 replicas; `audit`
read `mtm-orders` too, with a runtime of its own. 120,005, 80,005 and 60,005 records, the payments'
second half with a new `currency` field.

| Area | Verified (21 checks, each theme) |
|---|---|
| List | The **Tables** column; `shop` with its topics under its name |
| Create | Three topics chosen by search, each listed with its table (`mtm.shipments.v1` → `mtm_shipments_v1`) and **Materialized** (×2 for `mtm-orders`); "3 topics, one runtime: 9 partitions in all, which keep at most 9 runtime members busy"; the name suggested from the first topic and the engine name `lake.mtm_orders.mtm_orders` in the hint |
| Overview | Rows committed 260,015 over 3 tables; the **Tables** panel with each table's rows, size, files, lag, schema and state; the namespace named; lag *Caught up* over all 9 partitions |
| Data | `mtm_orders` first, its exact 120,005; the table picker to `mtm_payments`, its 80,005 and the evolved `currency` column, `?table=` in the address bar; a join of `mtm_orders` and `mtm_payments` grouped by status equal to the produced data (paid 26,723 / 6,731,673.10, pending 26,796 / 6,758,937.28, shipped 26,486 / 6,684,815.63) in 686 ms, the status line naming the two tables read; `iceberg.orders` refused with the three native tables named |
| Schema | `mtm_payments`: schemas 0 and 1, `currency` *since schema 1*; the picker to `mtm_shipments_v1`, which has no `currency` |
| Table | `mtm_shipments_v1` chosen by `?table=`, the Connect snippets naming `shop.mtm_shipments_v1` |

No browser error was logged beyond the two `400`s the refused statement answers on purpose.

### 2.0.0 on the lab: editing the topics, the maintenance app

Verified on 2026-10-20 on the lab with control plane 0d27b75 and the maintenance app, the console
server and dev client on a laptop, Playwright signed in as the administrator, on `shop`
(`mtm-orders`, `mtm-payments`, `mtm-refunds`). Each change was also checked through the API and in
SQL (handbook 13 §13.8b).

| Area | Verified |
|---|---|
| Edit: remove | The dialog lists each topic with its table; removing `mtm-refunds` asks *The removed topic's table* — **Keep read-only** or **Drop with its data** — and **Save** waits for the choice; the note says the runtime stops until its consumer group has emptied; the toast *keeping mtm_refunds read-only* |
| The drain | The page shows "Changing the runtime's topics" and **Deploying**, then **Running** again with *Tables (2 · 1 kept)* and the table **kept · read-only** |
| A kept table | `SELECT count(*) AS n FROM shop.mtm_refunds` reads its 3,005 rows; its Table tab names the maintenance app, and **Run now** of snapshot expiry is accepted and its outcome shown |
| Drop a kept table | **Kept tables** offers *Drop shop.mtm_refunds and its data*; saving drops it without a restart, and it leaves the panel |
| Edit: add | `mtm-refunds` added back through the picker shows *new table*, and the note that new topics are sampled and their tables created first; after the drain the new table holds all 3,005 records — on 0d27b75: before it, a table re-created under its old name received none (handbook 13 §13.8b) |
| Drop during a drain | Dropping a table its own drain had just kept shows it **dropping** until the drain is over, then it is gone (a defect this run found, handbook 13 §13.8b) |
| Stop dialog | Says the maintenance app goes on compacting the tables |
| Exactly once | A statement over `shop.mtm_orders`, `shop.mtm_payments` and `shop.mtm_refunds`: per partition, count = distinct offsets = the topic's end offset |

No browser error was logged.

### 1.0.0 on the lab

Verified on the lab cluster with the control plane and runtimes deployed from the
feature branch: the console server on a laptop against the lab's control plane and bucket, the
dev client, and Playwright (Chromium) driving it signed in as the lab's administrator, once in the
light theme and once in the dark. The materialization was a smoke table: 39 rows, three schemas
with forks, a renamed key and a header map, hour partitions, a compaction.

| Area | Verified (39 checks, each theme) |
|---|---|
| Fleet and Overview | The materialization listed; rows, table size and snapshots from the table; the Iceberg table named |
| Schema | Schemas 0–2 in the rail; fork badges and field ids; the renamed key `ref.code` shown as written to `ref_code`; superseded forks on request; the tree view of a struct; the diff of schema 1 → 2 by field id |
| Data | The exact count from manifests with nothing read; the status line's completeness; a map labelled a map; folded and nested columns; an exact count under a struct filter; statements over the logical table and over `iceberg.<name>`; a statement naming `/etc/hosts` refused with DuckDB's permission error |
| As of | The first snapshot: fewer rows, marked pinned, the snapshot in the address bar, back to latest |
| Export | A CSV holding every row, columns in order |
| Table tab | Snapshots with the compaction, an hour partition named as the runtime names it, the maintenance history and **Run now**, properties, the Connect panel with the lab's catalog URI, the snippet switching engines |
| Settings | Commits, Maintenance, Catalog, Runtimes and Schema sections, the runtime ones following the rollout |
| Access tokens | Shown once as `tok_…`, listed, revoked |

No check failed and no browser error was logged. The run found what babc45b fixed: hour
partitions named differently from the runtime's, a map column labelled by the text it holds,
wrapping snapshot headers, and a skipped job's note shown as a warning.

### 0.x, against the harness

Driven with Playwright against the §14.3 harness at 1512×950.

| Area | Verified |
|------|----------|
| Overview | Stat cards, distribution bar, live sparklines, attention queue, busiest list |
| List | Search narrows to one row; a no-match search shows "No matches"; each filter segment selects the right subset; sorting by Rows flips between ascending and descending |
| Create | Create disabled until required fields are filled; malformed labels raise "Invalid labels" and do not submit; success toasts and navigates; `created_by` reflects the identity header |
| Detail | Header badges, reconciliation grid, spec, labels, partition offsets |
| Runtime | Observation grid, Ready/Progressing condition cards, agent states, binding with copyable spec hash |
| Schema | Version rail; superseded toggle changes the row count; field filter; fields table |
| Schema diff | v1 → v2 renders `2 added, 1 changed, 3 unchanged`, with the fork as an *added* v2 entry and the original as *changed* with `is_current: true → false` |
| Data | See below |
| Lifecycle | Pause → observed `Paused` with reason `All managed agents paused` → Resume button appears → Resume → `Running` |
| Delete | Confirmation dialog; Cancel dismisses; confirming toasts and the row disappears from the fleet |
| Outage | Killing the control plane shows the error in the header while last-known data stays; restarting returns to `Live · just now` |
| Storage outage | Killing the fake GCS shows "Object storage is not reachable" with a Retry action; restoring it recovers |
| Themes | Light and dark verified on every screen |

### The Data view against real data (0.10.0)

The 0.10.0 Data view was verified with Playwright (Chromium, 1512×950) against the production build
served by the console, reading `mat-7946751be871` — the `materializer-loadtest-orders` topic, 101 M
rows in ~1,870 objects (25 GB) on GCS — from a laptop. Each scenario starts from a fresh page load,
and the server was restarted cold before the first:

| Scenario | Observed |
|----------|----------|
| Opening view | First rows 1.9 s after navigation on a cold server; total `101,379,839 rows`, counted from the manifest in 2 ms, nothing read |
| Paging | Next page 58–170 ms (Playwright round trip included); last page 61 ms, `101,379,801–101,379,839`; jump to page 1,234 in 0.7 s |
| Time picker | Presets with per-range row estimates, custom relative, date and date-time forms, the zone named |
| Time range | Oct 10–12, 2024: `284,671 rows`, exact, 32 of 1,866 objects read, in about 1 s |
| Filter, all time | `payload.status = cancelled`: dirty state shown before Run; first rows 1.9–2.4 s after Run, with exact progress and time left meanwhile; done in 19–28 s (network-bound) |
| SQL | A `GROUP BY` over all time returned 10 rows in 55–56 s; its `cancelled` count matched the filter run's total exactly in the same session (331,924, then 333,232 as the table grew). A bad column is marked in the editor while typing |
| Cancel | A scan stopped by **Stop** reads `Cancelled after 801 ms` |
| Refresh | Reloading during a scan re-attaches to the same run id, which carries on to `0 rows … 28.7 s` |
| Export | CSV of a one-day run: `97,079` data rows, equal to the run's total |
| Dark theme | Grid, status and rail legible; `_kafka_timestamp` rendered in local time with its zone in the header |

Downloads are captured with Playwright's `acceptDownloads` and `download.saveAs`, and the file's
rows are counted against the run's total.

### Overview, field names and exports (0.11.0)

Verified against `mat-c6eda3eb7b59`, a temporary eight-member materialization of
`materializer-loadtest-orders` processing its backlog, with the dev console (Vite + `node --watch`)
driven by Playwright (Chromium, 1512×950) and by direct API reads.

| Check | Observed |
|---|---|
| Rates before the change | Per-poll ingest rate `0, 0, 0, 1555, 0, 0, 0, 2857, 0, 0, 0, 11432, 19603, …` for steady work |
| Rates after | ~6,200 rows/s held within ±2% between seal waves; each member's rate equal to its counter's change between its own heartbeats, computed independently |
| Dips | Real: recorded heartbeats show every member at 750–830 rows/s, dropping to 1–600 rows/s (and its heartbeat slipping from 30 s to 34–50 s) while it seals and uploads; all eight seal together, so the total halves for about a minute every ~5 minutes |
| Rows committed | Equal to the Data tab's exact All-time count (manifest) |
| Consumer lag | One point per measurement; `Catching up · −5,914/s · about 4h 38m to go` from four minutes of measurements; partitions collapsed behind **Show 32 partitions** |
| Field names | No `payload.` in the rail, grid, filters, SQL hint, schema table, tree or diff; `"payload.status"` in a statement fails with `write "status"` |
| Older links | `f=[{"column":"payload.channel",…}]`, `cols=payload.order_id,…`, `pin=payload.order_id` rewritten to `channel`, `order_id`; the run's request echoes `channel` |
| Filter builder | Typing `email` lists `customer.email` with the match emphasised; `↓` `Enter` chooses it |
| Nested export | One day (108,463 rows, 81 columns): all 8,785,503 values of the flat export found in the nested one, no leftover flat keys, 35% smaller. A UI download of a filtered run: 34,047 lines, the run's total |
| Export memory | Before bounding page reads, exporting 33,805 rows from 171 objects failed with DuckDB `Out of Memory`; after, it streams |
| Themes | Light and dark |

### Table names, default destination, Stop and Edit (0.12.0, control plane 0.4.0)

Verified on the lab cluster with the control plane deployed from the feature branch, the
dev console (Vite + `node --watch`) driven by Playwright (Chromium, 1512×950), and direct API reads.

| Check | Observed |
|---|---|
| Create — names | `orders-lake`, `1orders`, `select`, an existing name (`orders`, any case) and 64 characters each show their reason under the field and keep **Create** disabled; a cleared name follows the topic again (`materializer_stop_test`) |
| Create — destination | Advanced settings show the control plane's default, `gs://my-lake`; created with the field empty, the materialization's `destination_uri` was that default |
| Create — control plane down | A console pointed at an unreachable control plane shows **Destination required** and keeps **Create** disabled until one is typed; names are still checked |
| Name reuse | Deleting `stop_test` freed the name; recreating `stop_test` on another topic was accepted |
| Subtitles | Detail header and list rows show the source topic only |
| SQL on `orders` | Starter `SELECT * FROM orders LIMIT 1000`; header "orders holds the rows in All time"; `SELECT count(*) AS n FROM orders` → 102,034,562 over 1,885 objects in 6.7 s; `FROM materialized` is underlined as typed and fails with "write FROM orders" |
| Stop at scale | `orders` (16 replicas): pods gone within seconds, `stopped` / `runtime_scaled_to_zero` with members `0/0` in about a minute, `replicas` still 16; overview counts **1 stopped**, the **Stopped** filter lists it |
| Resume at scale | Back to `running` at 16/16 about four minutes later, pods ramping 1 → 5 → 10 → 16 |
| Edit while stopped | 1 → 2: stays `stopped`, KasprApp still at 0, `spec_version` 2; resume came back at 2/2 |
| Edit while running | 2 → 3: `running` at 3/3 about a minute later |
| Edit dialog | Save disabled until the value changes; `0`, `65` and `x` refused; the hint warns when members would outnumber partitions |
| Themes | Stop, Edit and Create dialogs in light and dark |
| Rows staged at a stop | 30,003 rows staged and committed in Kafka (lag 0), none sealed; after stop and resume the runtime read nothing and a query found none of them — see handbook [14 §14.2](../../docs/handbook/14-known-gaps-and-roadmap.md) |

## 14.5 Defects this process found

Every one of these shipped a fix; they are listed because they are the exact classes of bug this
process exists to catch. 1–19 are from the 0.x releases, 20 on from 1.0.

| # | Defect | Root cause | Fix |
|---|--------|-----------|-----|
| 1 | Hive partition columns missing from every result | Cached mode mirrored objects to a flat hashed filename, destroying the path DuckDB derives partitions from | Preserve the hive layout under `<cache>/<bucket>/<objectName>` |
| 2 | REST reads served minutes-old data; a deleted materialization still appeared | The poller backs off to 30 s with no SSE subscriber, and REST simply returned the cached snapshot | `ensureFresh()` on the list route, staleness-checked `getDetail`, refreshes serialized via one in-flight promise |
| 3 | Fleet summaries had no runtime metrics on a cold REST read | `detailRefreshTargets` skipped the stale rotation entirely when `subscriberCount === 0` | An explicit REST read passes `includeStaleDetails: true` |
| 4 | Switching partitions after sorting produced a raw binder error | The sort column came from the previous result's columns, which differ per schema version | Selector changes clear the sort; the error panel offers **Reset filters and sort** |
| 5 | CSV download silently produced nothing | The anchor was never attached to the DOM and the object URL was revoked synchronously | Append, click, remove, revoke after 10 s |
| 6 | Storage failures rendered as "no data yet" | Only `dataset.available` was checked, not `datasetQuery.error` | Distinct error state showing `message: reason` with a Retry action |
| 7 | Fleet rate alternated between 0 and a spike | Differencing the *summed* counters while details refresh on a rotating cadence | Sum each materialization's independently derived rate |
| 8 | Missing assets returned `index.html` | SPA fallback was too greedy | 404 for `/api/*` and any path with a file extension |
| 9 | Server would not start | Fastify 5.12 requires a `LogController` **instance** | `new LogController({ disableRequestLogging: true })` |
| 10 | Every `ingest_hour` filter would have matched nothing | The declared scan cast `ingest_hour` to `BIGINT`, following this handbook rather than the data. DuckDB binds it `VARCHAR` — the runtime zero-pads, and `01` is not a number — so `"01"` became `1` | Declare it as text; assert the declared types *differentially* against what hive binds, on files carrying a leading zero |
| 11 | A running job looked hung: `Scanning · 566,000 of 63,330 rows`, bar pinned at 100% | DuckDB's `rows_processed` counts tuples crossing pipeline operators, not result rows — it reached 791,000 on a 63,438-row dataset. It was being shown as "rows read", and against the manifest's total | Remove `rowsProcessed`/`rowsTotal` from the contract; report only DuckDB's percentage, which is self-consistent as a ratio |
| 12 | Raising `CONSOLE_JOB_THRESHOLD_FILES` turned a working job into a 504 | Declaring the scan's schema removed the per-file *binding* cost, but browse's stable keyset `ORDER BY` still reads everything: 51 s on a 2,581-object scope against a 45 s interrupt | Keep the threshold at 200; make being over it cheap instead, via the job head |
| 13 | Ingest and write rates spiked between zero and many times the true rate | Counters were differenced at the console's poll interval; each runtime member's counters only change on its own 30 s heartbeat | Difference each member's counters between its own heartbeats, by `reported_at`, and sum the members |
| 14 | A nested export repeated a record's discounts under flat keys | An array the schema materialized whole sat beside its elements' own columns, and equal values in different encodings (`3.01`, `"3.01"`) were treated as conflicts | Merge overlapping columns; equal values agree whatever their encoding |
| 15 | Exporting a filtered run ran DuckDB out of memory | A 5,000-row export page under a sparse filter read every object it spans at once | Read at most four objects per page at once |
| 16 | A failed settings save's toast covered the **Save changes** button for the 8 s it stayed up | Toasts and the save bar share the bottom of the screen, and toasts sit above it | While the save bar shows, the page sets `--toast-offset` and toasts stack above the bar |
| 17 | The conflict banner said the same thing twice (*Reload them and try again … reload to see the current values, then save again*) with revision numbers | The server's message was shown, then the page's own advice appended | `conflictNotice`: one plain sentence for a stale revision; any other refusal keeps the server's reason |
| 18 | A rollout that had finished, but left a runtime that is not reporting, still looked in progress | The badge was blue whenever not every runtime was current | Blue while a member is still applying, green when all are current, amber when delivery has settled with runtimes left behind |
| 19 | With the control plane down, each settings section said it *may be unreachable or older than this console* | The empty state did not know the page was read-only | *Not available right now: these settings are stored with the control plane and appear here once it can be reached* |
| 20 | Hour partitions named differently on the Table tab and in the maintenance history | The console derived its own names for hour partitions | Name them as the runtime does: `2026-10-19-02`, `__null__` |
| 21 | A statement could read another person's stored result | Every statement's sandbox was allowed the whole run directory, where every result is stored, and the keyword guard can be dodged (`"read_parquet"(…)`) | A directory per run, the only one its sandbox may touch ([12 §12.4](./12-security-model.md)) |
| 22 | A table could make the console read objects anywhere its credentials reach, local files included | Paths in the pointer, metadata and manifests were used as written, and `readObject` reads non-`gs://` paths from disk | Everything confined to the location the control plane recorded ([12 §12.6](./12-security-model.md#126-paths-a-table-names)); the REST catalog refuses commits that move files |
| 23 | A storage outage answered `500 the console failed to handle this request` | Table reads threw storage's own errors, which the error handler hides | `503` naming what failed, storage's own message only in the log |
| 24 | Every date from a day table's data read as two days — *first seen Aug 31 – Sep 1, 2025* — anywhere west of UTC | Dates were told in the reader's zone, and a day partition's file spans a UTC day | Tell them in UTC, the partitions' zone, and say so |
| 25 | A month table's Timeline headed April's changes *Apr 1, 2025* | A group was headed by the first day of its entries' files | Head it by the span its entries cover: *Apr 1 – 30, 2025* |
| 26 | The Table tab marked the partition still being written *compact* while the maintenance app rightly left it alone | It counted small files and ignored the planner's hot-partition delay, which a month's partition, open for a month, makes visible | Apply the planner's own rule (`server/iceberg/compaction.ts`) |
| 27 | A new materialization with live tail on showed nothing on its Data tab until its first commit, minutes later | The tab waited for the summary to say the table held data | A table with live tail on is queryable once it exists, its records from the tail (`live.ts::tableQueryable`) |
| 28 | An open tab kept its Live toggle when live tail was switched off for the whole deployment; after a stop and a resume, or the switch on again, Live stayed off until Run was pressed | The console's config was read once per tab; a run whose tail went off held Live for good; the tail app's `404` for a table it did not follow yet was taken as final | The config read each minute; a run switched off is replaced once the switch is on again, and the `404` asked again (`routes/system.ts::liveTailOff`, `runs/store.ts::refreshRun`, `data/tail.ts::refusal`) |
| 29 | *Newer data available* for up to 30 s after every commit while Live followed the table, offering a run that would add nothing | The summary is read every 30 s and Live replaces the run at once, so the run was ahead of it; the notice only asked whether the two snapshots differed | Newer means committed later (`live.ts::newerSnapshot`), and the summary is read again when Live moves the run to a new snapshot |
| 30 | The head peek closed itself within seconds of being opened during a backfill, before a record produced at the head could show in it | Live starts a new run at every commit, whose tail reads `empty` until it is fetched, and the tab closed the peek for any tail neither backlogged nor truncated | Only a finished run that has caught up closes it (`live.ts::peekDone`) |
| 31 | `MaxListenersExceededWarning` twice for every GCS object the console read — every few seconds per table followed by Live | The GCS client library's `download()` streams each object through teeny-request, which gives that stream more listeners than Node allows by default | A plain GET of the JSON API with the library's endpoint, credentials, retries and CRC32C check (`data/gcs.ts::readFromGcs`); cached mode's files written beside their place and renamed in whole |
| 32 | With no runtime setting changed, *Delivery to runtimes* said every runtime ran its deployed values while a new one ran an earlier installation's, read from the delivery topic | Runtimes compared revisions only, and the panel showed nothing at revision 0 | The control plane's lineage (handbook 17 §17.7); the panel lists a runtime still applying at revision 0 too |

## 14.6 The unit suite

```bash
npm test          # node --test over server/, src/ and shared/ (*.test.ts)
npm run typecheck # both TypeScript projects
```

`node --test` runs the `.ts` files natively, so there is no transform step. **403 tests, 99 suites.**

| File | Covers |
|------|--------|
| `server/config.test.ts` | Environment parsing and derived `dataAccessMode` |
| `server/iceberg/iceberg.test.ts` | The Iceberg reader against the fixture table (§14.2) and PyIceberg's own answers: the pointer, or none; 64-bit snapshot ids kept exact; schemas, partition spec and fork map; exactly the live data files with their counts, sizes, timestamp spans, partitions and offsets; committed offsets and valid-through; an older snapshot as it was; partitions named as the runtime names them, a month's `2026-10`; a snapshot the table does not hold refused; the schema projection field for field; one pointer read while a read is fresh |
| `server/runs/table.test.ts` | Runs over the fixture table: the exact total from manifests and pages newest first; forks folded across the schema change and a compacted file read like any other; a struct filter; as of an older snapshot; statements over the logical and the native table; a join of `shop.orders` and `finance.payments`, tables in two datasets, each at its own snapshot, with only the tables a statement names read, every one bounded by the run's time range, and `as of` pinning the run's own table while the others are read as they are now; browsing the table asked for, by its name in any case; binding while typed without opening anything, and a file a statement names refused; another run's stored result out of reach; nothing read outside the table's location, whatever its pointer, metadata or manifests name; no table yet; a table that cannot be read answered `503` without storage's own message |
| `server/iceberg/presence.test.ts` | When each field held values, from the fixture's manifests: the table's span, a field written from the first records, a fork and the member it took over from, an added field whatever the schema it arrived in, a struct dated by its fields, a field no record holds left out, no snapshot |
| `server/iceberg/compaction.test.ts` | The Table tab's *compact* by the planner's rule: a partition's span from its key, a past partition taken whole, today and this hour waiting for no commit, this month's files settling one by one, files without a timestamp and an unpartitioned table by age, only small files counted |
| `src/features/schema/timeline.test.ts` | Dates from the data: spans as tight as their bounds, in UTC whatever the reader's zone; a field stopped arriving only a day before the table's last record; a version dated by the earliest data among its fields; the history by day, newest first, an object and its keys one entry, the first records one group; a month table's group headed by its span |
| `server/schema/diff.test.ts` | Schema diffs by field id: every kind of change named, a type promotion and a nullability change on one field, the first schema against nothing |
| `server/routes/system.test.ts` | Control-plane compatibility: 4.0 or later, unknown for an unknown version; the console's live tail configuration, and why a run reads no tail — no tail app, this console, the control plane, a materialization not running, and, on a control plane before 4.3, the switch for one; a create taking no live-tail default; the control plane's autoscaling — whether, how far and for what catch-up time — and none known before 4.4 or while it cannot be reached |
| `server/runs/runs.test.ts` | The run engine against real Parquet: counts from the manifest, time pruning and the 999 ms upper bound, filters (including list columns), streaming first pages, the page cache, cancellation, statements bounded by range, truncation with an exact total, error positions, a column some objects lack, statements reading a table by its name in any case, a name that is none of the tables answered with the list of those that are (DuckDB's own guess kept when it is one of them), CSV export, nested and flat JSON exports, profiles, logical columns over forks, names without the `payload.` root (and where a metadata name keeps it), `payload.`-rooted names from older links, the hint for a statement that still uses one, which fields hold JSON text; tables read by their canonical names, `<dataset>.<table>` — `shop.orders` and `iceberg.shop.orders` — and a bare name pointed at its canonical one |
| `server/live/rates.test.ts` | Rates from heartbeats against a simulated runtime — eight members on staggered 30 s heartbeats, second-precision timestamps, polled every 1.5 s — within 4% of the true rate on every poll; no total until every member has reported twice; restarts; members leaving; a control plane without per-member progress; bytes sealed in irregular waves read within 15%; series spacing and lag-history thinning |
| `server/live/store.test.ts` | What the materializations list and the fleet Overview count: the rows, bytes and files of each table's current snapshot as its committer reported them, summed — never the runtime's written counters, which a crash-looping runtime inflated to three times the table on the lab; unknown before any table is reported |
| `shared/fieldPaths.test.ts` | Field names without the `payload.` root, renaming paths in runtime messages, and nested JSON: objects, lists lining up across columns, lists inside lists, nulls, fields that changed shape, overlapping columns merging, conflicts kept under a flat name, JSON text decoded, `__proto__` |
| `src/features/data/fieldSearch.test.ts` | The filter builder's field search: ranking, every term required, emphasis ranges |
| `src/features/detail/lagTrend.test.ts` | Lag trend: drain rate and time left from real measurements, growing, steady, caught up, too few measurements, weighting the last ten minutes |
| `src/lib/ticks.test.ts` | Chart axes: round steps, zero kept for rates, a large slow-moving lag fitted, flat series, clock-aligned time ticks |
| `server/routes/requestSchemas.test.ts` | Shared request validation |
| `server/routes/materializations.test.ts` | The create schema takes `tables`, each topic once with the dataset its table goes in, in their order, a dataset and table name SQL can read, an optional table name two tables cannot share, and an optional label of free text; passes `skip_tombstones` through and rejects non-booleans, takes a month as a partitioning and no week, refuses the destination, namespace, runtime name and storage the deployment chooses and any key it does not know, `live_tail` among them — every materialization has a live tail; may turn `autoscaling` off, and leaves it to the control plane when omitted; the update schema takes `replicas`, `autoscaling` on or off, alone or with a count, a label (empty going back to the id), and a change of topics — new ones with their dataset — with what becomes of each removed table, and refuses a change that says nothing, topics that are not topics, and any other setting |
| `shared/tableNames.test.ts` | Dataset and table name rules and their messages, lowercase only; DuckDB's own catalogs and schemas refused as a dataset but not as a table; rules a control plane reports; case-insensitive comparison; `<dataset>.<table>`; labels of free text, trimmed; table names derived from topics as the control plane derives them (the same vectors as its `test_table_names.py`), `_2`/`_3` in the topics' order for names that would collide, an edit's tables kept and its new topics named apart from every name in use |
| `shared/access.test.ts` | Dataset permissions as the control plane resolves them: told apart from deployment ones, held on the datasets granted and nowhere else, held everywhere when held across the deployment, all held by an administrator, a token limited to datasets holding them on those alone, none held while a password must be changed, nothing extra for a principal from before datasets |
| `server/routes/datasets.test.ts` | The dataset routes as the person: listing with the search passed on, creating and answering once the directory has, a name SQL cannot read refused before the control plane is asked, changes turned away from someone who manages no dataset, a grant set; creating a materialization answered once its names are claimed, and a name another request took first refused on its row; a run over a table in a dataset the person may not query refused naming it, someone who may query no dataset turned away, a schema and its timeline shown only to those who may see its dataset's, the timeline read from the fixture's manifests, and each detail subscriber sent only the schemas they may read, with their own access to each table |
| `src/features/datasets/tableDrafts.test.ts` | The dialogs' table rows: a new topic in the dataset last chosen with a derived name unique among the rows and clear of the materialization's other names, each row's first problem (the dataset first), the request's tables with the names shown, the dataset a new row starts in, and the row a refused claim names, by field or by index |
| `src/features/detail/topicChange.test.ts` | The Edit dialog's plan: a removed topic's table needs a decision and kept tables may be dropped, a kept table's topic written again in its dataset, new topics named apart, the topics as they are changing nothing; the topics already read kept in their order |
| `server/data/tableNames.test.ts` | Against the console's DuckDB: every `reserved`/`type_function` keyword is refused, every guard token is refused, and every other keyword the rules allow can be created, queried unquoted in any case, and gets past the guard |
| `server/topics/catalog.test.ts` | Topic catalog: one upstream request for 200 concurrent readers, TTL and stale-while-revalidate, refresh spacing, stale and unavailable states, ETag and gzip, and `GET /api/topics` headers and status codes |
| `src/features/materializations/topicSearch.test.ts` | Topic search ranking, multi-term matching, hidden-topic counts, emphasis ranges, and a 50,000-topic timing bound |
| `src/features/data/hooks.test.ts` | A rules-of-hooks guard, plus its own detector tests |
| `src/features/data/timeRange.test.ts` | Preset resolution in local time, Sunday weeks, labels, and the URL encoding of every range kind |
| `src/lib/format.test.ts` | Instants rendered in local time across DST, and only zoned timestamps treated as instants |
| `server/data/gcs.test.ts` | GCS reads against a fake of its JSON API: an object read whole and fresh each time, without warnings, and never through the client library's download stream; the library's credentials sent, none to an emulator; a missing object said at once, and read by the table reader as no table yet; `408`/`429`/`5xx` retried, a refusal not; bytes failing their CRC32C refused after retrying; a data file mirrored once, whole or not at all |
| `server/data/tail.test.ts` | The tail client against a stub tail app: the rows and every header, asked with the person's session and the console's credential; every refusal an outcome the status line tells apart, the `404` for a table not followed yet asked again; a backlog's size and offsets; an answer that is not a tail, a timeout; the console fixture's tail files read by field id, each with the rows the tail app built, inside their time bounds; the head peek in the table's columns, newest first |
| `server/runs/tail.test.ts` | Runs with a tail, over the fixture table and its tail files: the records past the snapshot first; every record once, paged either way; time pruning and filters; no tail as of an older snapshot; the files in the run's own directory; statements over each table and a join of two, natively too; a refresh appending only past `through` (its revision bumped), a newer snapshot a new run, a tail switched off replaced once on again, one not configured never, one not followed yet asked again; a tail that cannot be read or does not answer in time leaving the committed rows; refreshes serialized; an export holding every row once; a lost tail file; a replaced run expiring |
| `server/routes/data.test.ts` | The run, refresh and head routes as the person: the tail read with their session unless asked not to; off in the control plane, or for the materialization on a control plane before 4.3; a refresh appending to the same run, refused once the dataset is no longer theirs; the head peek's rows, headers and bounds, its `data.query` on the table's dataset, and its `404` |
| `src/features/data/live.test.ts` | Live as plain functions: page keys by revision; when Live asks and every hold (a statement, an older snapshot, a failed run, hidden, a row open, past page 1, a range ended, signed out, a tail off for good) and asking again for one switched off; availability for every materialization while it runs (this console, the control plane, the materialization's switch on a control plane before 4.3, not before the materialization is known); a table queryable before its first commit; what the Data tab says of each tail state, nothing while Live follows, in plain words that never name the parts of the system, a backlog counted by its topic's measured lag; Auto re-run's delay; newer data as a later commit; the head peek kept open through the runs Live starts during a backfill |
| `server/settings/catalog.test.ts` | The console settings catalog matches `config.ts`: every overridable setting described once with its env var, a description for every primary setting, deployment defaults accepted, values checked like the page checks them, unacceptable stored overrides dropped; and live overrides served through `config` until reset, the Data explorer turned off and on while running, deployment settings left fixed |
| `src/features/settings/values.test.ts` | The Settings page's logic: parsing every kind of value with its bounds, which edits count as changes, drafts becoming one change per scope against its revision, the conflict notice, durations and sizes as people say them, section ordering |
| `server/auth/auth.test.ts` | The session cookie (HttpOnly, SameSite, Secure behind TLS) and reading it back; cross-site refusal by `Sec-Fetch-Site` and by origin; the per-address sign-in limit; `401` for a guest, `403` for a person, nothing until a chosen password is replaced, an unreachable control plane reported rather than refused; session checks cached per interval, dropped when refused, ridden through a short outage; and through the routes: the token only ever in the cookie, sign-in refusals passed on, floods turned away, cross-site changes refused first, the cookie cleared on sign-out and when the session ended, a due session renewed into a new cookie |
| `src/features/access/logic.test.ts` | Permission implications as the control plane resolves them (chains, `*`, unknown keys), generated passwords (length, alphabet without lookalikes, every character from the random source), describing a browser from its user agent, and audit details as before → after |

Route tests install a principal with `installTestAuth` (`server/auth/testing.ts`), so they run as
any guest or person without a control plane.

Two of these are worth knowing about specifically.

**`runs.test.ts` executes its SQL.** A generated statement that typechecks but does not bind is
worth nothing, and the failure modes here are silent — a dropped column, a fork read from one side
only, a row group pruned that held a matching row. So the tests write real Parquet with DuckDB —
timed and untimed objects, a forked field, list columns, 25-row row groups so pruning has something
to skip — and assert run results against it.

**`hooks.test.ts` tests its own detector.** The repository deliberately runs no linter, so this is
the substitute for `react-hooks/rules-of-hooks`: within a component body, no hook may follow an
early return. The scan is per function body and resets at each top-level declaration — without that
reset, a guard clause in a plain helper leaks into the component below it and flags every hook in
the file. Both properties are asserted directly, because a guard that can be defanged by a
plausible-looking "fix" is worse than none.

Anything touching `LiveStore` as a whole, or how GCS itself behaves, is still better served by the
lab (§14.4) than by mocks — mocking those is where the real bugs hide; `gcs.test.ts` covers only
what the console does with GCS's answers. DuckDB and the Iceberg reader are the
exceptions: they run in-process against local files — the fixture table among them — so they are
cheap to test against directly, and the tests above do.

## 14.7 Settings (0.13.0)

The Settings page saves to a control plane that applies changes asynchronously, through an agent,
and reports them back. That is what most of its behaviour depends on, so it was verified through
the real path rather than against a mock of the console's own API.

### Harness

| Process | Port | Role |
|---------|------|------|
| Stand-in control plane | 6066 | A small Python HTTP server that serves the four settings endpoints and `/system/settings` by calling the control-plane package's own WebView functions (`web/settings.py`, `web/system.py`), as the WebView YAML does. Accepted changes are applied on a background thread through the real agent handler (`handlers/settings.py`), after a configurable delay. Tables are in memory |
| Console server | 8788 | The real thing, with `CONTROL_PLANE_URL` pointed at the stand-in and a clean `CONSOLE_DATA_DIR` |
| Vite | 5175 | The client, proxying `/api` to the console server |

The stand-in also simulates runtimes. Five materializations report `settings_revision` on each
heartbeat: two whose members catch up with a new revision after 2–3 s and 2–25 s, one too old to
report it, one that stopped reporting ten minutes ago, and one stopped. It also has switches for
an apply delay, a publish failure (`500`, as the lab returned before its rebalance), and an
unreachable mode that drops every connection unanswered. The deployment sets
`CONTROL_PLANE_MAX_RUNTIME_REPLICAS=16`, a default destination and a runtime spec
with two env values, so all three sources (default, deployment, override) appear.

### Browser flows

Playwright (Chromium, 1440×900, plus 390×844 for mobile), each flow asserting both the page and
the stored state through the API:

| Flow | Verified |
|------|----------|
| Validation | `0`, `5000` and `abc` in a 1–1,024 field give *At least 1*, *At most 1,024*, *Enter a whole number*; Save disabled, the bar says *1 needs fixing*; **Undo** restores the value |
| Override | A saved value is stored once (one new revision), attributed (*Changed by …*, *deployment value 16 replicas*), enforced by the control plane (`/system/settings`), and followed by `/api/config` |
| Reset | **Reset** previews *Returns to the deployment value, 16 replicas, when saved*; after saving, the source is the deployment again and the env var is named |
| Destination | `s3://` refused; a trailing slash trimmed; an empty field stores *None*, so creates must name a destination; reset returns to the deployment's |
| Destructive switch | **Delete object data** carries *Destructive*; off is stored, and reset returns to the default |
| Discard | Edits in two sections mark both in the navigation; **Discard** clears both |
| Three scopes, one save | A control-plane, a runtime and a console setting saved together: each scope advances one revision, `/api/config` shows the console value at once, the control plane applies its own, and the agent emits one runtime delivery addressed to `*` with only the overridden value |
| Rollout | Right after a runtime change: *0 of 4 running current*, the two reporting runtimes *Applying* on revision 0; the page follows to *2 of 4* as members catch up, then lists only *Not reporting*, *Next deploy* and *Stopped* |
| Conflict | A change saved elsewhere after the page loaded is refused with the banner; the edit survives **Reload settings** and then saves |
| Agent refusal | Two changes accepted against the same revision: the agent applies the first and refuses the second, which the page reports as a conflict |
| Slow apply | With a 12 s apply delay the save answers `202` with *still applying*, and the page shows the value once it applies |
| Failure | A `500` leaves the edit in place with *Settings were not saved*; the toast does not overlap the save bar; the save succeeds once the control plane recovers |
| History | Every recorded change is listed, newest first, with before and after values and their sources |
| Deployment | Read-only values for all three components, env var names included; nothing editable |
| Read-only | Control plane unreachable: the banner names it, control-plane settings are not shown, console settings are shown locked with their last values; recovers on reload |
| Leave guard | Leaving with an unsaved edit raises the `beforeunload` prompt |
| Mobile | The section picker replaces the side navigation; no horizontal scroll |
| Dark theme | Every section, the rollout panel and a validation error |

No console errors were logged across the run. The only failed requests were the expected
refusals: two `409`s and one `500`. The flows found defects 16–19 in §14.5, which were fixed and
the flows re-run.

### On the lab

Against the lab control plane, running 0.5.0 on the lab, reads and validation answered
as above and every section rendered in both themes. Every save answered `500` there: the
control plane's consumer group had kept an assignment from before the settings topics existed
(main handbook [17 §17.12](../../docs/handbook/17-settings.md)). The save flows above were
therefore verified against the stand-in. They are to be repeated on the lab once its group has
been rebalanced.

## 14.8 Sign-in and access (0.14.0)

Accounts belong to the control plane, and most of what the console does with them depends on
how the control plane answers, so this was verified against the control-plane package's own code
rather than a mock of the console's API.

### Harness

| Process | Port | Role |
|---------|------|------|
| Stand-in control plane | 6066 | The settings stand-in of §14.7, extended with every `/auth`, `/users`, `/groups`, `/roles`, `/permissions`, `/audit` and `/identity/requests` endpoint, each calling the package's WebView functions (`web/auth.py`, `web/identity.py`) and `authorize()` as the WebView YAML does. Identity requests are applied on a background thread by the real agent handler (`handlers/identity.py`); bootstrap runs at start with a bootstrap administrator from the environment |
| Console server | 8789 | The real thing, with `CONSOLE_SERVICE_TOKEN` matching the stand-in's |
| Vite | 5176 | The client |

### Browser flows

Playwright (Chromium, 1440×900), light and dark:

| Flow | Verified |
|------|----------|
| Guest | The overview and list render; opening a materialization or Settings sends the browser to sign-in, which returns there afterwards |
| Wrong password | "Email or password is incorrect.", nothing else revealed |
| Bootstrap administrator | Signs in, is shown only **Change password**, changes it, and lands on the page they asked for, signed in with a new session |
| Add user | Generated password, groups limited to those the administrator may grant; the user appears with *Password to change* |
| User page | Profile, groups, where each permission comes from, sessions; actions offered only where allowed |
| Groups | List, a built-in group's members and roles, a new custom group |
| Roles | The editor's categories, implications shown as included, built-in roles read-only with **Duplicate** |
| Security settings | The control plane's seven and the console's two, editable by an administrator |
| Audit log | Sign-ins, the failed attempt and every change above, filterable |
| Editor | Signs in with the generated password, must change it, then sees no Users, Groups or Roles and no Settings sections beyond their account; materialization actions follow their role |

No console errors beyond the harness's own limits: data summaries answer `503` there because the
stand-in has no object store, and the favicon is absent.
