# 05 — Live Store and Real Time

Sources: `server/live/store.ts` (the store) and `server/live/rates.ts` (rate derivation). This is the
most behaviour-dense part of the console, and the source of every "why is the number doing that?"
question.

## 5.1 Responsibilities

The `LiveStore`:

1. polls the control plane on a subscriber-aware cadence,
2. caches the fleet list, per-materialization details, and each table's current schema projection,
3. derives per-second throughput from each runtime member's own heartbeats,
4. maintains rolling time series for each materialization and for the fleet, and a history of
   consumer-lag measurements,
5. fans snapshots out to SSE subscribers,
6. serves fresh-enough data to REST callers on demand,
7. detects schema drift, table by table, while a detail page is open.

A single module-level singleton is exported: `export const liveStore = new LiveStore()`.

## 5.2 Internal state

| Field | Type | Contents |
|-------|------|----------|
| `list` | `Map<id, CpListItem>` | Last list projection per materialization |
| `details` | `Map<id, CpDetail>` | Last detail payload per materialization |
| `schemas` | `Map<id, Map<table, CpSchema>>` | The current schema projection of each table of each materialization (handbook 05 §5.5) |
| `schemaErrors` | `Map<id, Map<table, string>>` | Why a table's schema could not be read, while the rest of its detail is healthy |
| `series` | `Map<id, ThroughputSample[]>` | Rolling per-materialization samples |
| `rateTrackers` | `Map<id, RateTracker>` | Each runtime member's recent heartbeats, for rates |
| `rates` | `Map<id, CurrentRates>` | The rates as of the last refresh: per member and summed |
| `lagHistory` | `Map<id, LagSample[]>` | One point per consumer-lag measurement |
| `lastDetailFetch` | `Map<id, number>` | Epoch ms of the last successful detail fetch |
| `fleetSeries` | `ThroughputSample[]` | Rolling fleet-wide samples |
| `fleetListeners` | `Set<Listener<FleetSnapshot>>` | Fleet SSE subscribers |
| `detailListeners` | `Map<id, Set<Listener<DetailSnapshot>>>` | Detail SSE subscribers, keyed by id |
| `inFlight` | `Promise<void> \| null` | The current refresh pass, if any |
| `lastRefreshAt` | `number` | Epoch ms of the last completed refresh |
| `controlPlaneError` | `string \| null` | Message from the most recent failed refresh |
| `generatedAt` | ISO string | Timestamp stamped onto every snapshot |

All of it is in-memory and per-process. Restarting the console clears every series; charts restart
from "Collecting…".

## 5.3 Cadence and backoff

```ts
private nextInterval(): number {
  if (this.subscriberCount === 0) return config.idlePollIntervalMs;      // 30 s
  if (this.detailListeners.size > 0) return config.detailPollIntervalMs; //  1.5 s
  return config.listPollIntervalMs;                                      //  2 s
}
```

`subscriberCount` sums fleet listeners plus all detail listeners across all ids.

### Which details get refreshed

```ts
private detailRefreshTargets(now: number, includeStale: boolean): string[] {
  const watched = [...detailListeners.keys()].filter((id) => list.has(id));
  if (!includeStale) return watched;

  const stale = [...list.keys()]
    .filter((id) => !detailListeners.has(id))
    .filter((id) => now - (lastDetailFetch.get(id) ?? 0) >= config.fleetDetailPollIntervalMs)
    .sort((a, b) => (lastDetailFetch.get(a) ?? 0) - (lastDetailFetch.get(b) ?? 0));

  return [...new Set([...watched, ...stale])];
}
```

- **Watched** materializations (someone has the detail page open) refresh on every tick.
- **Everything else** rotates: only ids not fetched within `CONSOLE_FLEET_DETAIL_POLL_MS` are
  eligible, oldest first.
- `includeStale` is `false` only for the background tick when `subscriberCount === 0`. An explicit
  REST read passes `true`, because a request is by definition not idle.

Fetches run through `mapWithConcurrency(targets, CONSOLE_DETAIL_CONCURRENCY, …)` — a cursor-based
worker pool, not `Promise.all`, so a 200-materialization fleet issues at most 6 concurrent
control-plane requests.

### Cost model

For a fleet of *N* materializations with *W* watched details, per refresh pass:

```
1 list request + (W + up to N/(fleetDetailPollMs / tickInterval)) detail requests
```

With defaults, one browser on the fleet list produces roughly `1 list request / 2 s` plus
`N detail requests / 6 s`. A 50-materialization fleet is therefore about 9 requests/second total —
independent of how many browsers are connected.

### `wake()`

```ts
wake(): void {
  if (!this.running || this.inFlight) return;
  if (this.timer) clearTimeout(this.timer);
  this.timer = setTimeout(() => void this.tick(), 50);
}
```

Called on every new subscription and after every accepted mutation. It collapses the normal wait
to ~50 ms so a stop click produces a visible state change almost immediately. It is a no-op while
a refresh is already in flight.

### `ensureFresh(maxAgeMs)`

```ts
async ensureFresh(maxAgeMs: number): Promise<void> {
  if (Date.now() - this.lastRefreshAt <= maxAgeMs) return;
  await this.runRefresh(true);
}
```

Used by `GET /api/materializations`. Guarantees a REST reader never sees a snapshot older than
`CONSOLE_LIST_POLL_MS`, even if the poller is in its 30-second idle mode.

### Refresh serialization

```ts
private async runRefresh(includeStaleDetails: boolean): Promise<void> {
  this.inFlight ??= (async () => {
    try { await this.refresh(includeStaleDetails); }
    catch (cause) { this.controlPlaneError = …; logger.warn(…); this.emitAll(); }
    finally { this.inFlight = null; }
  })();
  await this.inFlight;
}
```

Concurrent callers — the timer tick, several simultaneous REST reads — share one pass. There is
never more than one outstanding refresh, so the control plane cannot be stampeded by a burst of
console traffic.

> Subtlety: a caller that arrives while a refresh started with `includeStaleDetails: false` is
> running will await *that* pass, not a new one with `true`. The next tick corrects it. This is
> accepted as a bounded, self-healing staleness of at most one interval.

## 5.4 Throughput derivation

The control plane reports **cumulative** counters (`rows_written`, `bytes_written`,
`files_written`, `records_skipped`), both as materialization totals and per runtime member in
`runtime_progress.member_progress`. Rates are derived here, in `server/live/rates.ts`.

### Why not difference the totals

Each runtime member reports its counters on its own heartbeat — every 30 s by default — and the
control plane folds every report into the materialization's totals. Differencing those totals at
the console's poll interval divides whatever heartbeats happen to land between two polls by the
1.5 s between them: one member's 30 seconds of rows over 1.5 s on one poll, nothing on the next
dozen. A steady 6,200 rows/s then draws as a picket fence of zeros and 20,000 rows/s spikes. That
was measured on the load-test topic before the change (`0, 0, 0, 1555, 0, 0, 0, 2857, …, 23393`),
and it is why the charts looked spiky however evenly the members were working.

### Per-member rates

```
for each member in member_progress:
  readings += (reported_at, rows, bytes)      when reported_at advances
                                               (a counter below its last report is a restart:
                                                everything it reports is new progress)
  rows rate  = Δrows  / Δt between the latest reading and the one closest to 60 s before it
  bytes rate = least-squares slope of bytes over the readings of the last 15 minutes
materialization rate = Σ over members heard from in the last 120 s
```

| Property | Consequence |
|----------|-------------|
| Time comes from `reported_at`, the runtime's own clock | Polling faster or slower, or a slow control plane delivering heartbeats late, does not change the answer |
| A member's readings are made monotonic | A pod restart is a counter reset, not a negative rate or a spike — the same rule the control plane folds totals with |
| No total until every member has reported twice | A total over the members heard so far would climb for half a minute as if throughput were ramping up; it shows "Collecting…" instead. A member that joins later counts from its second heartbeat |
| A member silent for 120 s drops out | A scaled-away or replaced pod stops counting; a stopped runtime reads `—` |
| Rows use a one-minute window (`CONSOLE_INGEST_RATE_WINDOW_MS`) | Two heartbeats per member: live, and steady when the work is steady |
| Bytes use a 15-minute least-squares fit (`CONSOLE_WRITE_RATE_WINDOW_MS`) | `bytes_written` grows when a batch is sealed into a Parquet object, and a runtime's partitions tend to seal together every few minutes. A difference between two readings flips between nothing and a whole wave; the slope stays within about a tenth of the true rate |
| Control planes without `member_progress` | The totals are followed as one member, timed by `last_heartbeat` |

The rates are therefore what the runtime is actually doing, a minute at a time — including its
pauses. On the load-test topic, every member runs at ~760 rows/s between seals and drops to near
zero for 30–60 s while it seals and uploads a wave of Parquet objects (both run on the runtime's
event loop, which also delays its heartbeat). The partitions seal together, so the ingest rate dips
by half for about a minute every five or so. Those dips are real, and the per-member list in the
Reconciliation panel shows them member by member.

### Series

`recordSample` runs on every successful detail fetch. It always feeds the member readings to the
tracker; it adds a sample to the series only when `runtime_progress` is present, so a
materialization that has never written anything has no series and its chart stays at
"Collecting…".

Points are at least `CONSOLE_SERIES_SAMPLE_MS` (5 s) apart: the newest point is replaced on every
poll until it is far enough from the one before it, then kept. The series therefore spans about
20 minutes whatever the poll cadence, and its end is never older than the last poll. Each sample
carries the cumulative counters and the rates as of that poll; rates are `null` until every
member has reported twice.

### Fleet samples

`recordFleetSample` runs once per refresh pass. Counters are summed over the cached details; rates
are summed from each materialization's own current rates — never by differencing summed counters,
because details refresh on a rotating cadence and a summed counter only moves for the subset
refreshed this tick. The same sum backs `FleetTotals.rowsPerSecond`, computed in `aggregate()`.
While any materialization is still warming up — its members have not all reported twice since the
console started watching — the fleet's rates are `null` rather than a sum that leaves it out.

### Consumer-lag history

The control plane measures each materialization's consumer-group lag on a schedule (once a minute
by default) and the detail carries only the latest measurement. `recordLag` keeps one point per
measurement, keyed by `consumer_lag.measured_at` rather than by when it was polled, so a chart of
it is the measurements themselves and not a staircase of repeated polls. History is kept for
`CONSOLE_LAG_HISTORY_MS` (6 h); past 720 points the points older than an hour thin out, every other
one at a time, so the last hour always stays at full resolution.

## 5.5 Snapshots

### `fleetSnapshot(): FleetSnapshot`

```ts
{
  generatedAt, connected, controlPlaneError,
  totals: FleetTotals,
  items: MaterializationSummary[],   // sorted by updated_at descending
  series: ThroughputSample[],
}
```

`connected` is simply `controlPlaneError === null`.

`summarize(item)` joins the list projection with the cached detail and series:

| Summary field | Source | When null |
|---------------|--------|-----------|
| everything in `CpListItem` | list projection | never |
| `runtimeHealth` | `detail.status.runtime_health.level` | falls back to `'unknown'` |
| `lastHeartbeat` | `detail.status.last_heartbeat` | no detail yet |
| `rowsCommitted` / `tableBytes` / `tableFiles` | `detail.status.runtime_progress.tables[*]`: each table's `total_records`, `total_bytes`, `total_files` as its committer last reported its current snapshot, summed (`committedTotals`) | no table reported yet |
| `rowsWritten` / `bytesWritten` / `filesWritten` / `recordsSkipped` | `detail.status.runtime_progress`: the runtime's counters across restarts, which count again what a member wrote and lost to a restart | progress omitted |
| `rowsPerSecond` / `bytesPerSecond` | the materialization's current rates (§5.4) | until every runtime member has reported twice |
| `agentsRunning` / `agentsTotal` | `detail.status.resource_observation` | no observation |
| `lag` | `detail.status.consumer_lag.total_lag` (every topic's, summed), else the list item's `consumer_lag` | no lag measured yet |

### `FleetTotals` phase buckets

```ts
running : observed_phase === 'running'
stopped : observed_phase ∈ { stopped, paused }   // paused: records from before Stop replaced Pause
failing : observed_phase ∈ { failed, degraded }
pending : observed_phase ∈ { requested, discovering, discovered, deploying }
```

> A **deleted** materialization (observed and desired phase both `deleted`) stays in `items` for the
> recently-deleted view but counts only in `deleted`: not in `materializations`, and not in the
> throughput sums. One still **`deleting`** is counted in `materializations` but in none of the four
> sub-counts, so the four will not always sum to the total. This is intentional — a deleting
> materialization is neither healthy nor a problem.

### `detailSnapshot(id): DetailSnapshot`

```ts
{ generatedAt, materializationId, detail, schemas, error, schemaErrors, series,
  rowsPerSecond, bytesPerSecond, memberRates, lagHistory }
```

`memberRates` holds each member still reporting, by member id, with its own rates; `lagHistory`
is the consumer-lag history of §5.4.

`detail` is `null` when nothing has been fetched yet; `schemas` and `schemaErrors` are keyed by
table name and hold the tables read so far. `error` mirrors the fleet-level
`controlPlaneError`, so the detail page can show the same honest freshness state.

The store reads with the service token, which sees every dataset, so a snapshot is never sent as it
is: `visibleDetail(snapshot, principal)` keeps only the `schemas` and `schemaErrors` of tables whose
datasets the person may see the schemas of, and sets each table's `readable` and `queryable` for
them. The detail route applies it per request, the detail stream per subscriber, with the principal
its 30-second access check last returned (`routes/stream.ts`).

## 5.6 On-demand reads

```ts
async getDetail(id): Promise<CpDetail> {
  const age = Date.now() - (lastDetailFetch.get(id) ?? 0);
  const cached = details.get(id);
  if (cached && age <= config.detailPollIntervalMs) return cached;

  const detail = await this.refreshDetail(id, Date.now());
  if (detail === null) throw new ConsoleError('not_found', …, 404, { resource, id });
  return detail;
}
```

- Serves the cache only while it is younger than `CONSOLE_DETAIL_POLL_MS` (1.5 s).
- `refreshDetail` returns `null` specifically when the upstream said 404, which is translated into
  a console 404 and the id is forgotten.
- Any other upstream failure propagates as a `ConsoleError` from the control-plane client.

```ts
async getSchema(id, table): Promise<CpSchema> {
  const detail = await this.getDetail(id);              // which tables there are, and their schema ids
  const spec = tableSpec(detail, table);                // 404 naming the tables, for one it does not have
  const cached = schemas.get(id)?.get(spec.name);
  if (cached && !schemaDrifted(cached, detail)) return cached;
  return this.fetchSchema(id, spec.name);               // GET /materializations/{id}/tables/{table}/schema
}
```

Reading the detail first is what lets a read pick up a schema the table evolved to since it was
cached; it is at most 1.5 s old (`getDetail`).

## 5.7 Schema drift detection

```ts
private async refreshSchemas(id: string, detail: CpDetail): Promise<void> {
  const known = this.schemas.get(id);
  const due = detail.materialization.tables
    .map((spec) => spec.name)
    .filter((name) => known?.get(name) === undefined || schemaDrifted(known.get(name), detail));
  await mapWithConcurrency(due, SCHEMA_CONCURRENCY /* 4 */, (table) => this.fetchSchema(id, table).catch(() => undefined));
}
// schemaDrifted: the table's record (`detail.tables[].schema_id`) names another schema than the cached one
```

While a detail page is open, each refresh reads the schema of every table the console has **no**
cached schema for, or whose table record reports a different schema id than the cached copy —
four at a time. Each table evolves on its own, so a change to one reads only that one. Steady state
costs zero schema requests. The history of
schemas is the control plane's (06 §6.3); the console keeps none.

A schema-refresh failure is recorded per table and swallowed — a transient schema error must not
fail the whole detail refresh; the Schema tab shows it for its table.

## 5.8 Lifecycle of a materialization in the store

```
appears in list  ─► cached in `list`
                 ─► detail fetched (watched every tick / rotating otherwise)
                 ─► every member's latest heartbeat recorded; first sample (rates null)
                 ─► each table's schema fetched (no cache), while a detail page is open
                 ─► every member's next heartbeat seen (≤ 30 s): rates populated
                 ─► … steady state …
disappears from list  ─► forget(id): list, details, schemas, series, rate trackers, lag history
                         and lastDetailFetch all deleted
detail returns 404    ─► forget(id) immediately
```

`forget` is what makes a deleted materialization vanish from the UI within one refresh, and it
also releases its series memory.

## 5.9 Subscription and fan-out

```ts
subscribeFleet(listener): () => void {
  fleetListeners.add(listener);
  listener(this.fleetSnapshot());   // immediate, so a new tab renders instantly
  this.wake();                      // and force a fresh pass
  return () => fleetListeners.delete(listener);
}
```

`subscribeDetail` is the same shape but keyed by id, and removes the id's `Set` entirely when the
last listener leaves — which is what makes `detailListeners.size` a correct "is anyone watching a
detail" signal for cadence selection.

`emitAll()` builds **one** fleet snapshot and reuses it for every fleet listener, then one detail
snapshot per watched id. It is called at the end of every refresh and also on refresh failure, so
subscribers learn about an outage on the normal cadence rather than only on recovery.

## 5.10 Failure behaviour

| Failure | Store behaviour | UI behaviour |
|---------|-----------------|--------------|
| Control plane unreachable | `controlPlaneError` set, `emitAll()` fires, caches retained | Header shows the error message; last-known data stays on screen |
| Control plane recovers | Next successful refresh sets `controlPlaneError = null` | Header returns to `Live · just now` |
| One detail 404s | That id is forgotten | It disappears from the list |
| One detail errors otherwise | Logged at `debug`, caught by `.catch(() => undefined)` in the refresh loop | Its metrics go stale but nothing else is affected |
| Schema fetch fails | Logged at `debug`, swallowed | Schema tab shows the previous version |

The guiding principle: **never blank the screen because of a transient upstream failure, but never
pretend the data is fresh either.**
