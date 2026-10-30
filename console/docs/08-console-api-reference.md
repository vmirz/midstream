# 08 — Console API Reference

The console exposes its own HTTP API under `/api`. It is not a proxy: read endpoints serve the
live store's derived state, and write endpoints pass through to a domain's control plane.

- **Base path:** `/api`
- **Port:** `8787` by default
- **Content type:** `application/json`, except CSV export and the SSE streams
- **Auth:** one session cookie per domain, set by signing in to it, or `Authorization: Bearer <token>`; without one the caller is that domain's guest. Each route needs the permission in §8.1, in its domain — see §8.25 and [12 — Security model](./12-security-model.md)

## 8.0 Domains in the path

The console serves any number of domains (handbook 20). **Every route about one domain is under
`/api/d/<domain>/`**: this document names them by what follows — `GET /api/materializations/:id` is
`GET /api/d/domain_a/materializations/:id`, `POST /api/auth/login` is
`POST /api/d/domain_a/auth/login` — and each is authorized in that domain. An unknown domain is
`404 unknown_domain`; one whose control plane says it is another domain, `503 domain_misconfigured`.

The **console-level** routes, about every domain at once, have no domain in the path:

| Route | Answers |
|---|---|
| `GET /api/config`, `GET /api/health` | Every domain as this person may know it (§8.4); each domain's connectivity |
| `GET /api/auth/session`, `POST /api/auth/logout` | Who the browser is in every domain; signing out of every one (§8.25) |
| `GET /api/auth/sso/:provider/start`, `GET …/callback`, `GET /api/auth/sso/outcomes/:id` | Signing in through a provider, to every domain at once (§8.25) |
| `GET /api/fleet`, `GET /api/stream/fleet` | Every domain's fleet in one answer (§8.21) |
| `GET /api/search?q=` | Materializations, tables and datasets across domains (§8.29) |
| `GET`/`PATCH /api/settings`, `GET /api/settings/history` | The console's own settings, stored in the settings home (§8.24) |
| `/api/runs/:runId…`, `/api/stream/runs/:runId` | Runs, addressed by id: a statement may read several domains (§8.17) |

Outside `/api`: the federated Iceberg REST catalog at `/catalog` and each domain's own at
`/d/<domain>/catalog` (§8.30).

## 8.1 Endpoint summary

| Method | Path | Needs | Source of truth | Notes |
|--------|------|-------|-----------------|-------|
| `GET` | `/api/health` | — | live store | Liveness plus control-plane connectivity |
| `GET` | `/api/config` | — | config | Client feature flags |
| `GET` | `/api/materializations` | `materializations.view` | live store | Forces a refresh if stale |
| `POST` | `/api/materializations` | `materializations.create` | control plane | 202 once its tables' names are claimed (§8.6) |
| `GET` | `/api/topics` | `topics.view` | topic catalog | Every Kafka topic; ETag and gzip; `?refresh=1` reloads |
| `GET` | `/api/materializations/:id` | `materializations.view_details` | live store | Refreshes if older than 1.5 s |
| `POST` | `/api/materializations/:id/stop` | `materializations.stop` | control plane | 202 pass-through |
| `POST` | `/api/materializations/:id/resume` | `materializations.resume` | control plane | 202 pass-through |
| `PATCH` | `/api/materializations/:id` | `materializations.edit`; the control plane checks `dataset.protect` for `protection`, and `materializations.maintain` for `protection.reprotect` | control plane | 202; `name`, `replicas`, `autoscaling`, `tables`, `removed_tables`, `protection` only; a change adding tables answers once their names are claimed |
| `GET` | `/api/materializations/:id/protection` | `materializations.view_details`; the control plane leaves out tables whose schemas the caller may not see | control plane, as the person | The protection review (control-plane handbook 22 §22.11) |
| `DELETE` | `/api/materializations/:id` | `materializations.delete` | control plane | 202 pass-through |
| `GET` | `/api/materializations/:id/tables/:table/schema` | `materializations.view_details`; `dataset.view_schema` on the table's dataset | live store | One table's current projection; the store notices a new `schema_id` |
| `GET` | `/api/materializations/:id/tables/:table/schema/versions` | as above | control plane | Every schema the table has had |
| `GET` | `/api/materializations/:id/tables/:table/schema/versions/:schemaId` | as above | control plane | One of them; 404 for an id the table never had |
| `GET` | `/api/materializations/:id/tables/:table/schema/diff` | as above | control plane + diff | `from`/`to` optional |
| `GET` | `/api/materializations/:id/tables/:table/head` | `data.query` on the table's dataset | tail app | The newest records on the topic, committed or not, in the table's columns (§8.17a) |
| `GET` | `/api/materializations/:id/tables/:table/schema/timeline` | as above | the table | When each field first and last held a value, by Kafka timestamp, from the manifests (§8.12a) |
| `GET` | `/api/materializations/:id/tables/:table` | `materializations.view_details` | control plane + the table | The Table tab for one table: record, snapshots, partitions, connection |
| `POST` | `/api/materializations/:id/maintenance` | `materializations.maintain`; for a `reprotect` the control plane also checks `dataset.protect` on each table's dataset | control plane, as the person | 202 pass-through: the maintenance app runs a job now, on one `table` or on all, kept ones included |
| `GET` | `/api/catalog/connection` | `catalog.read` on a dataset | control plane | Where engines reach the REST catalog |
| `GET` | `/api/datasets`, `/api/datasets/:dataset` | `datasets.view` | control plane | Every dataset, locked ones marked; one with its tables (§8.28) |
| `POST` | `/api/datasets` | `datasets.create` | control plane | Waits for it to apply; `201` |
| `PATCH`, `DELETE` | `/api/datasets/:dataset` | `dataset.manage` on a dataset; the control plane checks this one | control plane | Waits for it to apply |
| `GET` | `/api/datasets/:dataset/grants` | as above | control plane | Its grants, the roles grantable there, the candidates |
| `PUT` | `/api/datasets/:dataset/grants/:principalId` | as above | control plane | Set someone's roles on it; `[]` removes the grant |
| `GET` | `/api/materializations/:id/data/summary` | `materializations.view_details` | the table | What a snapshot of one table holds; reads no data; `?table=&asof=` |
| `POST` | `/api/materializations/:id/data/estimate` | `data.query` on the table's dataset | the table | What time ranges would read of one table |
| `POST` | `/api/materializations/:id/data/validate` | `data.query` on every table it names | DuckDB | Bind a statement without running it |
| `POST` | `/api/materializations/:id/runs` | `data.query` on every table it reads | run engine | Start a run; `201` |
| `GET` | `/api/runs/:runId` | `data.query` on its datasets, own run | run engine | Snapshot |
| `POST` | `/api/runs/:runId/cancel` | `data.query` anywhere, own run | run engine | Stop it |
| `POST` | `/api/runs/:runId/refresh` | `data.query` on every dataset the run reads | run store + tail app | The live tail's newest records appended, or a new run in its place (§8.17a) |
| `GET` | `/api/runs/:runId/rows` | `data.query` on its datasets, own run | run engine | A page of rows |
| `POST` | `/api/runs/:runId/profile` | `data.query` on its datasets, own run | run engine | One column's distribution over the run's rows |
| `GET` | `/api/runs/:runId/export` | `data.export` on its datasets, own run | run engine | `csv` / `json` / `jsonl` / `parquet` |
| `GET` | `/api/stream/runs/:runId` | `data.query` on its datasets, own run | run engine | SSE progress |
| `GET` | `/api/stream/fleet` | `materializations.view` | live store | SSE |
| `GET` | `/api/stream/materializations/:id` | `materializations.view_details` | live store | SSE |
| `GET` | `/api/settings` | `settings.view` | control plane + console catalog | Every scope's settings, the deployment's, the runtime rollout |
| `PATCH` | `/api/settings` | `settings.manage` | control plane | Change and reset settings in up to three scopes; waits for them to apply |
| `GET` | `/api/settings/history` | `settings.view` | control plane | Every scope's changes, newest first |
| `GET` | `/api/auth/session` | — | control plane | Who the caller is; never refuses |
| `POST` | `/api/auth/login` | — | control plane | Sets the session cookie |
| `POST` | `/api/auth/logout` | — | control plane | Clears the cookie (`scope: current`) |
| `GET` | `/api/auth/sessions` | session | control plane | Your sessions |
| `POST` | `/api/auth/password` | session | control plane | Change your password; signs you back in |
| `GET` | `/api/auth/tokens` | session | control plane | Your access tokens |
| `POST` | `/api/auth/tokens` | `catalog.read` on a dataset | control plane | Make one, optionally limited to datasets; `202`, the token shown once, `cache-control: no-store` |
| `DELETE` | `/api/auth/tokens/:tokenId` | session | control plane | Revoke one of yours; `users.manage` revokes anyone's |
| `GET` | `/api/users`, `/api/users/:userId` | `users.view`, `users.view_details` | control plane | |
| `POST`, `PATCH`, `DELETE` | `/api/users`, `/api/users/:userId` | `users.manage` | control plane | Waits for the change to apply |
| `POST` | `/api/users/:userId/password`, `/sign-out`, `/unlock` | `users.manage` | control plane | Waits for the change to apply |
| `GET` | `/api/groups`, `/api/groups/:groupId` | `groups.view` | control plane | |
| `POST`, `PATCH`, `DELETE` | `/api/groups`, `/api/groups/:groupId` | `groups.manage` | control plane | Waits for the change to apply |
| `GET` | `/api/roles`, `/api/roles/:roleId` | `roles.view` | control plane | |
| `POST`, `PATCH`, `DELETE` | `/api/roles`, `/api/roles/:roleId` | `roles.manage` | control plane | Waits for the change to apply |
| `GET` | `/api/permissions` | session | control plane | The permission catalog |
| `GET` | `/api/audit` | `audit.view` | control plane | One page of the audit log |

*Own run*: a run is visible only to the person who started it; anyone else gets `404`. A guest
asking for something it may not do gets `401`; a signed-in person, `403`.

*On a dataset*: a dataset permission is checked on the datasets the request is about — every table a
run reads, the table a schema is of, the datasets a run read for its rows and export — after a guard
(`permitAnywhere`) that turns away anyone holding it on no dataset. A refusal names the dataset:
`details.permission`, `details.dataset`, and “it needs “Query data” on dataset finance”.

## 8.2 Common conventions

### Request headers

| Header | Required | Effect |
|--------|----------|--------|
| `cookie: midstream_session_<domain>=…` | for anything a guest may not do in that domain | The browser's session in each domain, set by `POST /api/d/<domain>/auth/login` or a provider's sign-in |
| `authorization: Bearer <token>` | instead of the cookies | For scripts: a federated access token, which every domain checks, or one domain's session token from its `POST /auth/login`, which only that domain takes |
| `content-type: application/json` | on POST with a body | |

`x-console-user` is gone: changes are attributed to whoever is signed in. A change from another
site — `Sec-Fetch-Site` other than `same-origin`, or an `Origin` naming another host — is refused
with `403` (`reason: cross_site_request`).

### Error body

```json
{ "error": { "code": "invalid_request", "message": "…", "details": { "field": "sql" } } }
```

| Code | Status | Raised by |
|------|--------|-----------|
| `invalid_request` | 400 | Zod validation, SQL guard, column validation, cache limits |
| `unauthenticated` | 401 | No usable session for something a guest may not do: `details.reason` is `sign_in_required`, `session_expired`, `session_ended`, `invalid_session` or `account_deactivated` |
| `forbidden` | 403 | Signed in, but without `details.permission`; `reason: password_change_required` until the password is changed; `cross_site_request` |
| `too_many_attempts` | 429 | Sign-in attempts from one address over `CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` (`retry-after` header), or the control plane's lockout |
| `not_found` | 404 | Unknown materialization, a snapshot the table does not have, unmatched route, another person's run |
| `unavailable` | 503 | Data access disabled; a table that cannot be read (§8.13) |
| `control_plane_unreachable` | 503 | `fetch` to the control plane threw |
| `control_plane_error` | pass-through | Control plane returned non-2xx |
| `query_failed` | 400 | DuckDB rejected the statement or filter; `details.position` locates it when the engine said where |
| `cancelled` | 409 | A read was asked of a run's query that was cancelled while it ran |
| `settings_rejected` | 409 | The control plane accepted a settings change and then refused to apply it (§8.24) |
| `internal_error` | 500 | Anything unexpected — message is fixed, details empty |

### Path parameter validation

`:id` must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`; `:schemaId`, like `from` and `to`, must be a whole number from 0.

---

## 8.3 `GET /api/health`

```json
{
  "status": "ok",
  "controlPlaneConnected": true,
  "controlPlaneError": null,
  "generatedAt": "2026-08-31T23:09:52.544Z",
  "subscribers": 1,
  "version": "3.0.0"
}
```

`status` is `ok` when connected, `degraded` otherwise. The endpoint returns **200 in both cases** —
a control-plane outage is not a console liveness failure. Use it for a Kubernetes liveness probe;
for a readiness probe that gates traffic on upstream health, check `controlPlaneConnected`.

This endpoint does **not** trigger a refresh, so it is safe to probe frequently.

## 8.4 `GET /api/config`

```json
{
  "domains": [
    {
      "id": "domain_a", "name": "Domain A", "settingsHome": true,
      "status": "ok", "statusMessage": null, "version": "1.0.0", "federationKeyId": "3f9c2a1b", "connected": true,
      "liveTail": { "configured": true, "enabled": true, "refreshMs": 1500, "maxRows": 100000 },
      "autoscaling": { "enabled": true, "maxReplicas": 16, "targetMinutes": 30 },
      "materializationDefaults": { "destinationUri": "gs://my-lake", "namespace": "midstream",
        "replicas": 1, "maxReplicas": 64, "storageSize": "64Mi", "partitionGranularity": "day",
        "labelMaxLength": 128, "datasetNameMaxLength": 63, "tableNameMaxLength": 63,
        "reservedNames": ["…"], "reservedNamespaceNames": ["main", "memory", "…"], "maxTopics": 20 }
    },
    { "id": "domain_b", "name": "Domain B", "settingsHome": false, "status": "unreachable", "…": "…" }
  ],
  "settingsHome": "domain_a",
  "federationKeysAgree": true,
  "dataAccessEnabled": true,
  "pageRowLimit": 1000,
  "exportRowLimit": 1000000,
  "resultRowLimit": 1000000,
  "pollIntervalMs": 2000,
  "version": "1.0.0"
}
```

Anyone may read it, signed in or not; what only some people need is left out for the rest. Each
domain's `status` is `ok`, `unreachable`, `misconfigured` (its control plane says it is another
domain) or `incompatible` (a release this console does not work with), with `statusMessage` saying
why; `federationKeyId` the start of the key it signs access tokens with, and `federationKeysAgree`
false when two reachable domains' differ (handbook 20 §20.8). `liveTail.configured` says the domain
has a tail app in the registry; `enabled` that it is on in this console and not off in its control
plane; `refreshMs` is how often Live looks, `maxRows` the most rows one fetch of a tail takes.
`autoscaling` is the domain's runtime autoscaling, `null` while it cannot be reached.
`materializationDefaults` is `null` without `materializations.create` in that domain, or while it
cannot be reached ([03 §3.7](./03-configuration-reference.md)). The defaults and `pollIntervalMs` are
the values in force, so an override saved on a Settings page shows here at once.

---

## 8.5 `GET /api/materializations`

Calls `liveStore.ensureFresh(CONSOLE_LIST_POLL_MS)` first, so the snapshot is never older than the
list cadence. Returns a `FleetSnapshot`.

```json
{
  "generatedAt": "2026-08-31T23:17:34.293Z",
  "connected": true,
  "controlPlaneError": null,
  "totals": {
    "materializations": 3, "running": 1, "stopped": 1, "failing": 1, "pending": 0, "deleted": 0,
    "rowsCommitted": 426600, "tableBytes": 135660000, "tableFiles": 12,
    "rowsWritten": 426612, "bytesWritten": 135662616, "filesWritten": 16, "recordsSkipped": 4,
    "rowsPerSecond": 405.9, "bytesPerSecond": 129076.2
  },
  "items": [ /* MaterializationSummary[] */ ],
  "series": [ /* ThroughputSample[] */ ]
}
```

### `MaterializationSummary`

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "name": "Shop",
  "source_topics": ["orders", "payments.v2"],
  "tables": ["sales.orders", "finance.payments_v2"],
  "datasets": ["finance", "sales"],
  "destination_uri": "gs://my-lake",
  "desired_phase": "running",
  "observed_phase": "running",
  "phase_reason": "processing",
  "updated_at": "2026-08-31T23:17:34.295Z",
  "storage_size": "64Mi",
  "replicas": 2,
  "consumer_lag": 1204,

  "runtimeHealth": "healthy",
  "lastHeartbeat": "2026-08-31T23:17:34.302Z",
  "rowsCommitted": 163800,
  "tableBytes": 52088400,
  "tableFiles": 4,
  "rowsWritten": 163800,
  "bytesWritten": 52088400,
  "filesWritten": 6,
  "recordsSkipped": 2,
  "rowsPerSecond": 616.4,
  "bytesPerSecond": 196015.2,
  "agentsRunning": 3,
  "agentsTotal": 3,
  "lag": 1204
}
```

The fields down to `consumer_lag` are the control-plane list projection verbatim: `name` the label
(the id when unset), `source_topics` in the order the materialization reads them, `tables` as SQL
reads them, `<dataset>.<table>`, one per topic in the same order, and `datasets` every dataset its
tables are in. The list page searches the label, id, topics, datasets and tables. The rest
are console-derived, summed over every table, and are `null` until the corresponding detail arrives. `rowsPerSecond` and `bytesPerSecond` are the
materialization's rates from its runtime members' own heartbeats
([05 §5.4](./05-live-store-and-realtime.md#54-throughput-derivation)), `null` until every member has
reported twice. `lag` is the latest consumer-group lag the control plane measured.

Items are sorted by `updated_at` descending. `totals` bucket rules are in
[05 §5.5](./05-live-store-and-realtime.md#55-snapshots): a deleted materialization counts only in
`deleted`, and one still `deleting` counts in `materializations` but in none of the phase buckets.

## 8.6 `POST /api/materializations`

Body (Zod-validated):

| Field | Type | Required | Constraint |
|-------|------|----------|-----------|
| `tables` | `{source_topic, dataset, name?}[]` | **yes** | 1 or more, each topic once (a Kafka topic name: letters, digits, `.`, `_`, `-`; ≤ 249 chars). The control plane caps how many (`max_materialization_topics`, 20). `dataset` and `name` follow `shared/tableNames.ts`: a lowercase letter first, then lowercase letters, digits and underscores, ≤ 63 chars, not a reserved word — and a dataset not one of DuckDB's own namespaces. `name` omitted, the control plane derives it from the topic (`payments.v2` → `payments_v2`); two rows may not share one. Fixed once created |
| `name` | string | no | A label: free text, ≤ 128 chars once trimmed, no control characters. Omitted, the materialization goes by its id |
| `runtime_mode` | `dedicated` \| `shared` | no | `shared` is rejected later by the control plane |
| `resource_profile` | string \| null | no | 1–63 chars |
| `replicas` | integer | no | 1–1000 here; the control plane enforces its own `max_runtime_replicas` |
| `autoscaling` | boolean | no | sent only as `false`; omitted, the control plane turns it on |
| `skip_tombstones` | boolean | no | omitted when unchecked; the control plane defaults it to `false` |
| `partition_granularity` | `hour` \| `day` \| `month` \| `none` | no | how the tables are partitioned, fixed with them. Omitted, the control plane's default (`materializationDefaults.partitionGranularity`) |
| `labels` | object | no | keys 1–253 chars, values ≤ 1024 |
| `annotations` | object | no | same |
| `protection` | `{kinds: {<kind>: "hash" \| "partial" \| "redact"}}` or `{kinds: [<kind>…]}` | no | what its writers mask (control-plane handbook 22). Only the shape is checked here; the control plane knows the kinds and the masks each allows. Omitted, the domain's *Protected by default*. Other than the default, the control plane wants `dataset.protect` on every table's dataset (`403`); a choice that hashes in a domain without a protection key is `409 protection_key_not_configured` |

The schema is strict: any other key is a `400`. That includes `destination_uri`, `namespace`,
`runtime_name` and `storage_size`, which 3.x took. Every materialization gets the deployment's
destination, namespace and storage, and a runtime name generated from its id, and the control
plane refuses them too (handbook 04 §4.5.2). Without a destination configured
(`materializationDefaults.destinationUri` null) every create is `409`, `destination_not_configured`.

```bash
curl -sS -X POST localhost:8787/api/materializations \
  -H 'content-type: application/json' -H "authorization: Bearer $TOKEN" \
  -d '{"name":"Shop","tables":[{"source_topic":"orders","dataset":"sales"},{"source_topic":"payments.v2","dataset":"finance"}]}'
```

The console sends the request, then **waits for the directory to claim the tables' names** — up to
10 seconds, polling `GET /identity/requests/{claim_request_id}` as the person (`awaitClaim`,
`routes/materializations.ts`). **202** returns the control plane's acceptance with `claim_status`:

```json
{
  "materialization_id": "mat-ad8e10e41586",
  "request_id": "req-9f8e7d6c5b4a",
  "desired_phase": "running",
  "observed_phase": "requested",
  "phase_reason": "claiming_tables",
  "status_url": "/materializations/mat-ad8e10e41586",
  "accepted_at": "2026-08-31T23:18:13.940Z",
  "name": "Shop",
  "tables": [{ "name": "orders", "source_topic": "orders", "dataset": "sales", "dataset_id": "ds-…" }, "…"],
  "claim_request_id": "idr-3e8f0a1b2c4d",
  "claim_status_url": "/identity/requests/idr-3e8f0a1b2c4d",
  "claim_status": "applied"
}
```

`claim_status` is `applied` once the names are the materialization's, `pending` when the directory
had not answered in time. A refused claim is the request's error: `409` (`table_name_taken`,
`materialization_deleted`), `404` (`dataset_not_found`) or `403` (`forbidden`), with `details.field`
(`tables.<index>.<dataset|name>`), `index`, `dataset`, `table`, `source_topic` and the
`materialization_id`, which the control plane has marked deleted. A conflict the control plane can see
at once comes back the same way, under `details.upstream_details`; the dialog shows either on the row
it names (`features/datasets/tableDrafts.ts::claimRow`).

`liveStore.wake()` fires afterwards, so the new record appears within ~50 ms plus one control-plane
round trip. **Persistence is asynchronous upstream**, so a `GET` immediately after may still 404
briefly.

## 8.6.1 `GET /api/topics`

Every topic in the cluster, sorted by name, from the process-wide topic catalog
([04 §4.6.1](./04-server-internals.md)). The create dialog searches it in the browser.

```json
{
  "topics": [
    { "name": "__consumer_offsets", "partitions": 50, "replicationFactor": 2, "category": "kafka" },
    { "name": "orders", "partitions": 32, "replicationFactor": 2 }
  ],
  "fetchedAt": "2026-10-06T23:30:00Z",
  "stale": false,
  "error": null
}
```

| Field | Meaning |
|-------|---------|
| `category` | `kafka`, `materializer`, or `stream` for topics hidden by default; absent for data topics |
| `fetchedAt` | When the control plane last asked Kafka |
| `stale` / `error` | The list could not be refreshed and is an earlier copy; `error` says why |

Headers: `Cache-Control: private, no-cache`, a weak `ETag`, `Vary: Accept-Encoding`, and
`Content-Encoding: gzip` when accepted. A request whose `If-None-Match` matches gets `304` with no
body, so a browser revalidates an unchanged list for the cost of the headers.

`?refresh=1` (or `true`) asks for a reload, honoured at most once per
`CONSOLE_TOPIC_CATALOG_MIN_REFRESH_MS`; otherwise the current list is returned.

Errors: `503 unavailable` while no list has ever loaded, with `details.reason` from the failure,
for example `control plane at … is unreachable: fetch failed` or `the control plane does not list
topics yet; upgrade it to enable the topic picker`.

## 8.7 `GET /api/materializations/:id`

Returns a `DetailSnapshot` with `detail` guaranteed non-null (a 404 is thrown otherwise):

```json
{
  "generatedAt": "2026-08-31T23:25:41.488Z",
  "materializationId": "mat-a1b2c3d4e5f6",
  "detail": { "materialization": {…}, "status": {…}, "runtime_binding": {…}, "table": {…} },
  "schema": { /* CpSchema or null */ },
  "error": null,
  "series": [ /* ThroughputSample[], at least 5 s apart, ~20 minutes */ ],
  "rowsPerSecond": 701.6,
  "bytesPerSecond": 223108.8,
  "memberRates": {
    "midstream-mat-a1b2c3d4e5f6-app-0": { "rowsPerSecond": 351.2, "bytesPerSecond": 110423.0 },
    "midstream-mat-a1b2c3d4e5f6-app-1": { "rowsPerSecond": 350.4, "bytesPerSecond": 112685.8 }
  },
  "lagHistory": [ { "t": 1791428100000, "lag": 107357698 }, { "t": 1791428160000, "lag": 106938384 } ]
}
```

| Field | Meaning |
|---|---|
| `rowsPerSecond` | Rows per second: each runtime member's rows over about its last minute of heartbeats, timed by the runtime's clock, summed over the members heard from in the last two minutes |
| `bytesPerSecond` | Parquet bytes sealed per second, as each member's least-squares trend over 15 minutes, summed |
| `memberRates` | The same, per member still reporting |
| `lagHistory` | One point per consumer-lag measurement (`t` is when the control plane measured it), oldest first, for six hours |

`detail` is the control plane's enriched detail projection: full materialization spec, status
including `resource_observation` and `runtime_progress`, the runtime binding including
`last_applied_spec_hash`, and `tables` — each table's summary (dataset, location, partitioning, schema
id, current snapshot, last commit, `valid_through`, totals, last error), as far as created.

The live store reads it with the console's own credential, which may see every dataset's schemas;
what this route and the stream send is cut down for the caller (`visibleDetail`, `live/store.ts`):
`schemas` and `schemaErrors` hold only the tables whose datasets they may see the schemas of, and each
table's `readable` and `queryable` are theirs. `detail.protection` is the policy's rollout — its
`state`, revision, the tables and members behind, and each table's masked and withheld totals — and
`detail.materialization.protection` the policy; neither names a field.

## 8.8 `POST …/stop`, `POST …/resume`, `PATCH …/:id`, `DELETE …/:id`

All four: 202, control-plane acceptance passed through, `liveStore.wake()` after. Only `PATCH` has
a body, of at least one of six keys and nothing else (a strict schema: another key is a 400 here,
so a setting that cannot change is refused rather than silently dropped):

| Key | Rules |
|---|---|
| `name` | a new label, as on create; empty goes back to the id |
| `replicas` | an integer, 1–1000 here; the control plane enforces `max_runtime_replicas` |
| `autoscaling` | a boolean; off, a runtime autoscaling has scaled up goes back to `replicas` at once |
| `tables` | the topics to read from now on, in order, `{source_topic, dataset?, name?}`: a topic it already has a table of needs neither; a new one needs a dataset, as on create (§8.6) |
| `removed_tables` | `{ "<table>": "keep" \| "drop" }`: what becomes of each table whose topic `tables` removes, and kept tables to drop |
| `protection` | the whole policy from now on, as on create, and `apply_rules: true` to record the policy in force again with the current rules. The control plane wants `dataset.protect` on every dataset written into, those of tables the same edit adds included; the 202 carries `protection: {before, after, revision, applies_to}`. Delivered to the running runtime: nothing restarts |

The control plane plans a change of topics and refuses one it cannot apply with a `400` naming
`tables` or `removed_tables` — a removed topic's table with no decision, above all — or a `403`
naming a dataset the caller may not write. A change that adds tables answers, like a create, once
their names are claimed (`claim_status`), and is refused `409 topic_change_pending` while an earlier
one still waits. The 202 carries `topic_change`: the tables afterwards, and which it adds, resumes,
keeps and drops (handbook 04 §4.8.1). The runtime is then stopped until its consumer group has emptied, about five
minutes all told (`changing_topics`, handbook 07 §7.9a).

```bash
curl -sS -X POST   localhost:8787/api/materializations/mat-a1b2c3d4e5f6/stop   -H "authorization: Bearer $TOKEN"
curl -sS -X PATCH  localhost:8787/api/materializations/mat-a1b2c3d4e5f6        -H "authorization: Bearer $TOKEN" \
     -H 'content-type: application/json' -d '{"replicas": 8}'
curl -sS -X PATCH  localhost:8787/api/materializations/mat-a1b2c3d4e5f6        -H "authorization: Bearer $TOKEN" \
     -H 'content-type: application/json' -d '{"tables": [{"source_topic": "orders"}, {"source_topic": "refunds", "dataset": "sales"}], "removed_tables": {"payments": "keep"}}'
curl -sS -X POST   localhost:8787/api/materializations/mat-a1b2c3d4e5f6/resume -H "authorization: Bearer $TOKEN"
curl -sS -X DELETE localhost:8787/api/materializations/mat-a1b2c3d4e5f6        -H "authorization: Bearer $TOKEN"
```

`observed_phase` and `phase_reason` in the response reflect the **current** persisted state, not
the target. Watch the stream for convergence.

---

## 8.9 `GET …/tables/:table/schema`

The control plane's projection of one table's current schema (handbook 05 §5.5), as the live store
holds it; the store notices a new `schema_id` on its next poll (05). Every table of a materialization
has a schema of its own, evolving on its own. A `:table` the materialization does not have is a
`404` naming its tables. One entry per field of the Iceberg schema, superseded fork members
included, metadata columns too (`kind: "metadata"`):

```json
{
  "materialization_id": "mat-d89c130365f6",
  "table": "orders",
  "source_topic": "materializer-loadtest-orders",
  "schema_id": 1,
  "since": "2026-10-19T05:10:03.256Z",
  "metadata_location": "gs://my-lake/midstream/orders_e13b-d89c130365f6/metadata/00005-5ed08140-….metadata.json",
  "field_count": 140,
  "fields": [
    { "field_id": 13, "path": ["status"], "name": "status", "type": "string", "required": false,
      "fork_version": 1, "state": "current", "kind": "value", "since_schema_id": 0 },
    { "field_id": 22, "path": ["customer", "phone"], "name": "customer.phone", "type": "string",
      "required": false, "fork_version": 1, "state": "superseded", "kind": "value", "since_schema_id": 0 },
    { "field_id": 90, "path": ["customer", "phone__v2"], "name": "customer.phone", "type": "struct",
      "required": false, "fork_version": 2, "state": "current", "kind": "container",
      "since_schema_id": 0, "fork_of": 22 }
  ],
  "changes": [
    { "kind": "added", "path": ["payments", "[]", "refunded_amount"], "field_id": 160,
      "physical_path": ["payments", "[]", "refunded_amount"], "type": "double" }
  ],
  "unmapped": { "total_values": 0, "last_seen_at": null }
}
```

| Field | Meaning |
|---|---|
| `path` / `name` | The physical path in the Iceberg schema (`[]` is a list's element) and the logical name people read: every member of a fork family has the family's one name |
| `kind` | `value`, `container` (struct or list), `json` (a JSON collapse) or `metadata` |
| `state` | `current`, `superseded` (a newer fork took over) or `collapsed` |
| `fork_version` / `fork_of` | A family's members are numbered from 1; a fork names the field it forked from |
| `since_schema_id` | The schema the field first appeared in |
| `field_count` | Fields that are not metadata columns |
| `changes` | What the evolution to this schema did: `added`, `forked`, `collapsed`, `rejected` (with a `reason`) |
| `unmapped` | Values written to `_unmapped`; on the current schema only |

Before the table exists: `schema_id: null`, `since: null`, `fields: []`.

## 8.10 `GET …/tables/:table/schema/versions`

Every schema the table has had, newest first, from the control plane:

```json
{
  "materialization_id": "mat-d89c130365f6",
  "table": "orders",
  "source_topic": "materializer-loadtest-orders",
  "current_schema_id": 1,
  "versions": [
    { "schema_id": 1, "since": "2026-10-19T05:10:03.256Z", "field_count": 140,
      "change_counts": { "added": 1 }, "current": true },
    { "schema_id": 0, "since": "2026-10-19T05:05:31Z", "field_count": 139,
      "change_counts": { "added": 58, "forked": 2 }, "current": false }
  ],
  "returned": 2
}
```

`since` is when the schema was committed: schema 0 when the table was created, every later one when
the committer evolved it. Every replica and every console sees the same list. It says when the
materializer met a shape, not when the topic's records had it; the Schema tab dates versions by
their data instead (§8.12a).

## 8.11 `GET …/tables/:table/schema/versions/:schemaId`

The projection of one of the table's schemas, the shape of §8.9 without `unmapped`. An id the table
never had is a `404` (`control_plane_error`, the control plane's `not_found` in
`details.upstream_code`); one that is not a whole number is a `400`.

## 8.12 `GET …/tables/:table/schema/diff?from=&to=`

| Parameter | Default |
|-----------|---------|
| `to` | the current `schema_id` |
| `from` | the highest schema id below `to`; `null` when there is none |

Schema 0, or any `to` with no schema before it, is a **baseline diff**: every field is new —
`added`, `forked` for a family member after its first, `collapsed` for a family's JSON field. Fields
are matched by **field id** (06 §6.4).

```json
{
  "materialization_id": "mat-d89c130365f6",
  "from": 0,
  "to": 1,
  "summary": { "added": 1, "forked": 0, "collapsed": 0, "type_widened": 0, "required_changed": 0,
               "superseded": 0, "removed": 0, "unchanged": 139 },
  "entries": [
    { "field_id": 160, "name": "payments[].refunded_amount", "path": ["payments", "[]", "refunded_amount"],
      "kind": "added", "before": null, "after": { … }, "changes": [] },
    { "field_id": 14, "name": "channel", "path": ["channel"], "kind": "unchanged",
      "before": { … }, "after": { … }, "changes": [] }
  ]
}
```

| Kind | Meaning |
|---|---|
| `added` | A field only `to` has |
| `forked` | A new member of an existing family |
| `collapsed` | The family's JSON field, or a member that became collapsed |
| `type_widened` | An Iceberg type promotion (`int` → `long`, `float` → `double`): never made by the materializer, which forks instead |
| `required_changed` | Nullability changed |
| `superseded` | A member a newer fork replaced |
| `removed` | A field only `from` has: never produced by the materializer, kept for a table an engine changed |
| `unchanged` | Everything else |

`changes` lists each attribute that differs, as `{ "attribute", "from", "to" }`. Metadata columns are
left out. Entries sort by kind in the order above, then by name.

## 8.12a `GET …/tables/:table/schema/timeline`

When the table held values of each of its fields, by the records' Kafka timestamps, as of its
current snapshot. The console reads it from the statistics every manifest entry keeps
(`server/iceberg/presence.ts`) and opens no data file. A file holds values of a field when its
`value_counts` exceed its `null_value_counts`, a list's elements counted. Of the files that do, the
earliest record with a value lies between the smallest lower `_kafka_timestamp` bound and the
smallest upper bound, and the latest between the largest of each. A struct, list or map has no
statistics of its own and spans its fields. So a date is as tight as the table's files: a UTC day
in a day-partitioned table, an hour in an hour's, up to a month in a month's, and whatever compaction
merged in an unpartitioned one (handbook 14 §14.6). The order the runtime read the records in does
not matter.

```json
{
  "materializationId": "mat-b668b3db7371",
  "table": "orders_by_day",
  "available": true,
  "reason": null,
  "snapshotId": "4717454761431520987",
  "granularity": "day",
  "dataFrom": "2025-01-01T00:00:00.000Z",
  "dataThrough": "2026-10-19T21:22:19.200Z",
  "fields": {
    "13": { "first": { "from": "2025-01-01T00:00:00.000Z", "to": "2025-01-01T23:39:07.200Z" },
            "last": { "from": "2026-10-19T00:20:52.800Z", "to": "2026-10-19T21:22:19.200Z" },
            "files": null, "values": null },
    "14": { "first": { "from": "2025-01-01T00:00:00.000Z", "to": "2025-01-01T23:39:07.200Z" },
            "last": { "from": "2026-02-09T01:19:12.000Z", "to": "2026-02-09T22:20:38.400Z" },
            "files": 405, "values": 3699 },
    "25": { "first": { "from": "2025-04-15T00:36:00.000Z", "to": "2025-04-15T21:37:26.400Z" },
            "last": { "from": "2026-10-19T00:20:52.800Z", "to": "2026-10-19T21:22:19.200Z" },
            "files": 553, "values": 5050 }
  }
}
```

On a test cluster (2026-10-23), a day table of `history.orders` whose records' timestamps run from
2025-01-01: `customer` (13) and `customer.email` (14) from the first day, the email's last value on
2026-02-09, and `device` (25) from 2025-04-15 — the days the producer changed its records' shape.

`fields` is keyed by field id; a field no record holds a value of is absent, and `files` and
`values` are `null` for a container. The same permissions as the schema (`dataset.view_schema` on
the table's dataset). When the table cannot answer — not created, data access off, nothing
committed, unreadable — it is still a `200`, with `available: false` and the `reason`. Each
manifest's aggregate is kept with the manifest's entries in the snapshot cache, so a later snapshot
reads only its new manifests.

---

## 8.13 `GET …/data/summary?table=&asof=`

What one snapshot of one of the materialization's tables holds, from its manifests alone. No data is
read. `table` names the table, the first (the first topic's) when absent; one the materialization
does not have is a `400` naming its tables. Without `asof`, the current snapshot; `asof=<snapshot
id>` names an older one of that table.

```json
{
  "materializationId": "mat-d89c130365f6",
  "available": true,
  "reason": null,
  "objects": 3559,
  "bytes": 5744093103,
  "rows": 33132184,
  "rowsExact": true,
  "untimedRows": 0,
  "timeMin": "2024-10-05T00:00:00.450Z",
  "timeMax": "2025-11-11T01:53:15.535Z",
  "newestCommit": "2026-10-19T05:36:40.613Z",
  "source": "iceberg",
  "asOf": "2026-10-19T05:37:00.391Z",
  "snapshotId": "9221613250211573719",
  "snapshotAt": "2026-10-19T05:36:40.613Z",
  "currentSnapshotId": "9221613250211573719",
  "validThrough": "2025-05-03T14:44:53.497Z",
  "deleteFiles": 0
}
```

| Field | Meaning |
|---|---|
| `available` / `reason` | `false` with a reason while the table has not been created, or nothing has been committed to it |
| `objects` / `bytes` / `rows` | The snapshot's live data files, their size and their rows; `rows` comes from the manifests and is exact |
| `untimedRows` | Rows without a Kafka timestamp; they appear only under All time |
| `timeMin` / `timeMax` | The span of Kafka timestamps on record |
| `newestCommit` / `snapshotAt` | When the snapshot was committed |
| `asOf` | When the table was read |
| `snapshotId` / `currentSnapshotId` | The snapshot described, and the table's current one; they differ when `asof` pinned an older one. Snapshot ids are strings: they use all 64 bits (06 §6.1) |
| `validThrough` | Every record with a Kafka timestamp before this instant is in the snapshot, across every partition that has committed (handbook 11 §11.4) |
| `deleteFiles` | Delete files another engine committed. Runs refuse such a snapshot (§8.16) |

Errors: `503 unavailable` when data access is turned off, or when a table that exists cannot be
read — object storage unreachable or refusing, a metadata file or manifest missing — with which in
the message; `404 not_found` for an `asof` the table does not have ("it may have expired").

## 8.14 `POST …/data/estimate`

What each time range would read of one table. Body: `{ "ranges": [{ "from": <ms|null>, "to": <ms|null> }, …],
"asOf": <snapshot id|null>, "table": <name|null> }` (1–32 ranges; `table` as in §8.13). Response: `{ "asOf", "estimates": ScopeEstimate[] }`,
one per range:

```json
{ "range": { "from": 1730419200000, "to": 1733011200000 }, "objects": 68, "bytes": 715665695,
  "rows": 4520549, "rowsExact": true }
```

`rows` sums every data file overlapping the range, so it is an upper bound unless every one of them
lies wholly inside it, which `rowsExact` reports. Day and hour partitions keep most files wholly
inside a calendar range.

## 8.15 `POST …/data/validate`

Bind a statement without running it: `{ "sql": "…" }` → `{ "ok": true }`, or a `400`
`query_failed` whose `details.position` (`{ line, column }`, 1-based, against the statement as
submitted) is where the engine pointed. The editor calls this as you type.

## 8.16 `POST …/runs`

Start a run — the only way the console executes a query. Returns `201` with a `RunSnapshot` at
once; progress follows on the event stream (§8.20).

```json
{ "kind": "browse",
  "range": { "from": 1759100000000, "to": null },
  "rangeLabel": "Last 7 days",
  "filters": [{ "column": "status", "operator": "eq", "value": "cancelled" }],
  "order": "newest" }
```

```json
{ "kind": "sql",
  "table": "orders",
  "range": { "from": null, "to": null },
  "rangeLabel": "All time",
  "sql": "SELECT o.\"status\", count(*) FROM sales.orders o JOIN finance.payments p ON p.order_id = o.order_id GROUP BY 1" }
```

| Field | Rules |
|---|---|
| `table` | The table a browse run reads, and the one `asOf` pins for a statement; the first when absent. One the materialization does not have is a `400` naming its tables |
| `range` | Epoch milliseconds, `[from, to)`; `null` leaves a side open. `to` must follow `from`. A statement's range applies to every table it reads |
| `rangeLabel` | Echoed back for display; ≤ 120 chars |
| `filters` | ≤ 20; `column` must be one of the view's columns. Columns are field paths without the schema's `payload.` root — `status`, `customer.email`. A `payload.`-rooted name from an older link is accepted, and the run's `request` echoes the column's current name |
| `operator` | `eq` `neq` `contains` `starts_with` `gt` `gte` `lt` `lte` `is_null` `is_not_null`. On a list column (`…[]…`), `eq`/`neq` mean "an element is / no element is"; ordering comparisons are refused |
| `sql` | One read-only statement over any of the materialization's tables — each by `<dataset>.<table>`, or `iceberg.<dataset>.<table>` for its native columns — ≤ 20,000 chars. It may join them, across datasets; only the tables it names are read (see [07](./07-data-access-and-duckdb.md)). The caller needs `data.query` on every one of their datasets, and on the run's `table`'s |
| `asOf` | A snapshot id, as text, to read `table` as of; the current snapshot when absent, and always for the statement's other tables. An id the table does not have is a `404`; a snapshot holding delete files, or data files outside the table's location, is a `400` naming the table |
| `liveTail` | Boolean, optional. Read each table's records newer than its snapshot from the live tail, every materialization's (07 §07.9). On unless `false`; never with `asOf` |

### `RunSnapshot`

```json
{
  "id": "0d9e…", "materializationId": "mat-7946751be871",
  "request": { "kind": "browse", "range": {…}, "rangeLabel": "Last 7 days", "filters": […], "order": "newest" },
  "state": "running", "phase": "scanning",
  "startedAt": "…", "finishedAt": null, "elapsedMs": 2140, "asOf": "…",
  "snapshotId": "9221613250211573719", "snapshotAt": "…", "pinned": false, "validThrough": "…",
  "scope": { "objects": 48, "bytes": 412000000, "rows": 3120000 },
  "progress": { "rowsTotal": 3120000, "rowsDone": 1460000, "objectsTotal": 48, "objectsDone": 22,
                "objectsRead": 22, "bytesTotal": 412000000, "fraction": 0.468 },
  "rows": 51230, "totalExact": false, "rowsReady": 48700,
  "columns": [{ "name": "status", "type": "VARCHAR" }, { "name": "customer.phone", "type": "VARCHAR", "json": true }, …],
  "truncated": false, "rowsRetained": null,
  "error": null, "expiresAt": "…",
  "revision": 0,
  "tail": { "status": "complete", "state": "live", "reason": null, "message": null, "rows": 42, "segments": 1,
            "through": { "0": 688, "1": 612, "2": 655 }, "endOffsets": { "0": 688, "1": 612, "2": 655 },
            "behind": 0, "settling": 0, "lagMs": 0, "member": "materializer-tail-app-0", "generatedAt": "…" }
}
```

| Field | Meaning |
|---|---|
| `state` | `running`, `succeeded`, `failed`, `cancelled` |
| `snapshotId` / `snapshotAt` | The snapshot the run reads, fixed when it starts, so paging and exports stay stable while commits keep arriving |
| `pinned` | The run was asked (`asOf`) for an older snapshot than the table's current one |
| `validThrough` | As in §8.13, for the run's snapshot |
| `phase` | `planning`, `preparing` (statement: reading footers, `objectsDone` counts them), `scanning`, `counting` (statement: counting a result larger than kept), `done` |
| `progress.fraction` | 0–1. Exact for browse runs — rows of objects read over rows in range; DuckDB's estimate for statements; `null` while indeterminate |
| `progress.objectsRead` | Files actually opened; `objectsDone` also counts the ones the manifests answered without a read |
| `rows` / `totalExact` | Rows matched so far, and whether that is final. A finished run always has `totalExact: true` |
| `rowsReady` | Leading rows that can be paged now |
| `columns` | Known at once for browse runs; when a statement finishes. `json: true` marks a column carrying a `json` field's JSON text |
| `truncated` / `rowsRetained` | A statement's result larger than `CONSOLE_RESULT_ROW_LIMIT`: only the first `rowsRetained` are kept, `rows` is still exact |
| `revision` | Bumped each time a refresh appends the live tail's newest records to the run: a page read before may differ, so a client keys pages by it |
| `tail` | The live tail of the run's own table; `tables[].tail` has each table's. Absent when the run asked for none (`liveTail: false`, `asOf`). `status`: `complete`, `truncated` (a cap or a paused partition: the newest records are not shown yet), `empty`, `backlogged` (the runtime is too far behind; `behind` says how far), `unavailable` (`reason` says why; the run shows committed rows), `off` (`reason`: `materialization`, `console`, `control_plane`, `stopped`, `not_configured`); `unavailable` with `reason` `not_tailed` when the tail app does not follow the table yet, asked again on the next refresh. `through` is, per partition, the last offset the run holds; `behind` the records on the topic it does not show yet; `settling` rows waiting for a schema change; `lagMs` how far behind the topic the tail's reading is |

## 8.17 `GET /api/runs/:runId`, `POST /api/runs/:runId/cancel`

The current snapshot; cancel interrupts every scan the run has in flight and returns the cancelled
snapshot. An unknown or expired run is a `404`.

## 8.17a `POST /api/runs/:runId/refresh` and `GET …/tables/:table/head`

**Refresh** brings a run up to date with the live tail (07 §07.9). Body, optional:
`{ "range": { "from": …, "to": … } }` — the run's range resolved again, which a run that replaces it
takes. Answers `200`:

```json
{ "replaced": false, "run": { "id": "0d9e…", "revision": 3, "rows": 18440, "tail": { "status": "complete", "rows": 118, "segments": 4, "…": "…" }, "…": "…" } }
```

`replaced: false`: the same run, with the records past its tail's `through` appended (`revision`
bumped when any arrived). `replaced: true`: a new run in its place — the table has a newer snapshot,
the run failed or was cancelled, it is a statement, or its tail was off on this side and is on now;
the old run expires within a minute unless it is read. A run as of an older snapshot, or still
running, is answered as it is. The caller needs `data.query` on every dataset the run reads, now;
an expired run is a `404`. The Data tab's Live mode calls it every `liveTailRefreshMs`, and a
statement's Auto re-run five seconds after each run.

**Head** — `GET /api/materializations/:id/tables/:table/head?rows=200` (1–10,000; the tail app gives
at most its `live_tail_peek_max_rows`): the newest records on the table's topic, committed or not,
in the table's logical columns, newest first. Needs `data.query` on the table's dataset.

```json
{ "rows": [{ "order_id": "O-12", "…": "…" }], "columns": [{ "name": "order_id", "type": "VARCHAR" }],
  "peek": { "rows": 200, "offsets": { "0": [40211, 40278] }, "endOffsets": { "0": 40278 }, "behind": 2500000,
            "state": "backlogged", "settling": 0, "generatedAt": "…" } }
```

`404 live_tail_off` when live tail is off for the materialization, it is stopped, or the tail app does not tail the table (`details.reason`: `not_tailed`);
`503 tail_unavailable` (with `details.reason`) when the tail app cannot answer. It is never part of a
run, an export or a profile.

## 8.18 `GET /api/runs/:runId/rows?offset=&limit=`

A page of the run, answered at once with whatever of it is ready:

```json
{ "runId": "0d9e…", "offset": 100, "rows": [{ … }], "columns": [ … ],
  "complete": true, "total": 51230, "totalExact": false }
```

`complete: false` means more rows may still arrive in this range — ask again as `rowsReady` grows.
`limit` is capped at `CONSOLE_PAGE_ROW_LIMIT` (1,000).

## 8.19 `GET /api/runs/:runId/export?format=` and `POST /api/runs/:runId/profile`

Export a **succeeded** run as `csv`, `json`, `jsonl` or `parquet` — every row of its result in the
order shown, up to `CONSOLE_EXPORT_ROW_LIMIT`; the `x-export-rows` header says how many. Anything but
a succeeded run is a `400`.

`shape` lays out `json` and `jsonl` rows: `flat` (the default) keeps one key per column,
`{"customer.email": …}`; `nested` rebuilds the record from the column paths,
`{"customer": {"email": …}, "items": [{"sku": …}]}`, decoding `json` fields
([07 §07.7](./07-data-access-and-duckdb.md#077-exports-and-profiles)). It is ignored for `csv` and
`parquet`.

```bash
curl -OJ 'localhost:8787/api/runs/<run-id>/export?format=jsonl&shape=nested'
```

Profile one column over the run's rows: `{ "column": "status" }` →

```json
{ "column": "status", "partial": false, "total": 51230, "nonNull": 51230, "distinct": 4,
  "min": "cancelled", "max": "shipped", "topValues": [{ "value": "shipped", "count": 30112 }, …] }
```

Profiles run under `CONSOLE_QUERY_STATS_TIMEOUT_MS`; `partial: true` means a statement overran it.

## 8.20 `GET /api/stream/runs/:runId`

Server-sent events, one `RunSnapshot` per `data:` frame: the current one at once — so a client that
connects mid-run, after a page refresh, is never blank — then at most every 200 ms while it works,
and the terminal one, after which the stream closes. A `: ping` comment every 15 s keeps proxies
from closing an idle stream.

## 8.21 `GET /api/stream/fleet`, `GET /api/fleet`

```
event: fleet
data: {"generatedAt":"…","domains":[{"domain":"domain_a","name":"Domain A","access":"ok","snapshot":{"connected":true,"totals":{…},"items":[…],"series":[…]}},
                                     {"domain":"domain_b","name":"Domain B","access":"sign_in_required","snapshot":null}]}
```

- Every domain's fleet in one frame: each domain's `snapshot` is what
  `GET /api/d/<domain>/materializations` answers, when the caller may see that domain's
  materializations (`access: ok`); otherwise `sign_in_required`, `forbidden` or `unavailable`, and no
  snapshot. The client merges them (`src/lib/fleetMerge.ts`).
- One `fleet` frame immediately on connect, then one whenever any domain's fleet refreshes.
- `: keep-alive` comment frames every 20 s. `GET /api/fleet` answers the same once.

```bash
curl -sS -N http://localhost:8787/api/stream/fleet
```

## 8.22 `GET /api/stream/materializations/:id`

```
event: detail
data: {"generatedAt":"…","materializationId":"…","detail":{…},"schemas":{…},"series":[…]}
```

Subscribing switches the whole store to the 1.5-second detail cadence for as long as the
connection is open. Payload matches `GET /api/materializations/:id`, except `detail` may be `null`
before the first fetch completes — and is cut down for the subscriber in the same way, as of their
latest access check (every 30 seconds): a grant given or taken away reaches an open stream within
that.

## 8.23 Client integration notes

- **Prefer the streams.** The REST read endpoints exist for scripting and for the initial render;
  the streams are the intended live path.
- **Do not poll `GET /api/materializations` faster than `CONSOLE_LIST_POLL_MS`** — `ensureFresh`
  will short-circuit and you will just get the same snapshot.
- **Treat every mutation as a request, not a result.** Watch `observed_phase` converge on
  `desired_phase`.
- **`EventSource` reconnects on its own.** The client's `useEventStream` hook only adds honest
  status reporting on top. A stream whose caller loses access sends `event: auth` and closes;
  reload the session (`GET /api/auth/session`) before reconnecting.

---

## 8.24 Settings

The Settings pages' API (handbook 17 §17.8). `GET`/`PATCH /api/settings` read and save the console's
own settings, stored in the settings home's `console` scope (needing `settings.view`/
`settings.manage` there); `GET`/`PATCH /api/d/<domain>/settings` a domain's control-plane and runtime
settings. The console describes and validates its own settings. Reference for what each setting does: [03 §3.10](./03-configuration-reference.md)
and the main handbook's [17 — Settings](../../docs/handbook/17-settings.md).

### `GET /api/settings`

```json
{
  "generatedAt": "2026-10-09T13:15:52.000Z",
  "controlPlane": { "reachable": true, "error": null },
  "scopes": {
    "control-plane": { "scope": "control-plane", "revision": 13, "updatedAt": "…", "updatedBy": "jane", "settings": [ /* SettingView */ ] },
    "runtime":       { "scope": "runtime", "revision": 1, "…": "…" },
    "console":       null
  },
  "deployment": [ { "component": "console", "key": "controlPlaneUrl", "label": "Control plane",
                    "value": "https://midstream-b-api.example.com", "envVar": "CONSOLE_DOMAINS_FILE", "description": "…" } ],
  "rollout": { "revision": 1, "total": 5, "current": 2, "pending": 0, "stopped": 1,
               "notReporting": 1, "unsupported": 1, "materializations": [ /* … */ ] },
  "historyRetained": 100
}
```

That is a domain's page, `GET /api/d/domain_b/settings`. The console's, `GET /api/settings`, has
the same shape with only `console` set, `rollout` null, and the console's deployment — every
domain it serves, the settings home, the console URL and its sign-in providers — in `deployment`.

A `SettingView` (`shared/types.ts`) is the same for every scope: `scope`, `key`, `section`,
`label`, `description`, `kind` (`integer`, `boolean`, `destination_uri`, `quantity`, `enum`, `string`,
`domain_list`, `choice_map`), `unit`, `minimum`, `maximum`, `options`, `choiceOptions` and
`choiceLabels` (a `choice_map`'s options — each with the choices it allows — and what each choice is
called), `maxLength`, `nullable`, `advanced`, `danger`, `effect`, `envVar`, `permission` (one more
permission saving it needs, besides `settings.manage`: `settings.manage_protection` for *Protected by
default*), and the provenance: `value`, `source` (`override` / `deployment` / `default`),
`defaultValue`, `deploymentValue`, `deploymentSet`, `updatedAt`, `updatedBy`.

When the control plane cannot be reached, `controlPlane.reachable` is `false` with the reason, its
two scopes are `null`, and `console` holds the overrides last read. The page is then read-only.

### `PATCH /api/settings`

```json
{
  "changes": [
    { "scope": "control-plane", "values": { "max_runtime_replicas": 32 }, "reset": [], "baseRevision": 13 },
    { "scope": "console", "values": { "listPollIntervalMs": 3000 }, "reset": [], "baseRevision": 1 }
  ]
}
```

One change per scope, each against the revision the page read. The server validates console
values against its own catalog, then forwards control-plane, then runtime, then console changes,
and stops at the first refusal. It then waits up to 10 s for the control plane's settings agent to
apply each one. The answer is the `GET` body plus `pending`, the scopes not yet applied:

| Status | When |
|--------|------|
| `200` | Every change applied |
| `202` | Accepted, but a scope was not applied within 10 s; `pending` names it. The page re-reads 3 s later |
| `400` | `invalid_request` (a console value, or nothing to change) or `control_plane_error` for a control-plane value; `details.field` and `details.scope` name the setting |
| `409` | `control_plane_error` with `upstream_details.reason = "settings_changed"` when a scope changed since it was read, or `settings_rejected` when the agent refused it while applying |

```json
{ "error": { "code": "invalid_request", "message": "Fleet refresh must be at least 500",
             "details": { "field": "listPollIntervalMs", "scope": "console" } } }
```

If a later scope is refused after an earlier one was saved, the message says which were saved and
`details.saved` lists them.

### `GET /api/settings/history?limit=`

```json
{
  "changes": [
    { "scope": "control-plane", "revision": 13, "requestId": "set-45f6da8fef2e",
      "updatedAt": "2026-10-09T13:15:51Z", "updatedBy": "jane",
      "changes": [ { "key": "max_runtime_replicas", "before": 64, "after": 32,
                     "beforeSource": "default", "afterSource": "override" } ] }
  ],
  "retainedPerScope": 100,
  "errors": []
}
```

All three scopes merged, newest first. `limit` (1–100, default 100) applies per scope. A scope that
could not be read is listed in `errors`, and the others are still returned.

## 8.25 Sign-in, sessions and access

Who people are and what they may do is the control plane's
([18 — Identity and access](../../docs/handbook/18-identity-and-access.md)). These routes pass
through to it, with two differences: the browser's session lives in an HttpOnly cookie the page
never sees, and a change to users, groups or roles is answered once it has been applied.

### `GET /api/auth/session`

Who the browser is in every domain, and the providers this console signs people in with:

```json
{
  "domains": {
    "domain_a": {
      "state": {
        "authenticated": true,
        "principal": {
          "kind": "user", "user_id": "usr-3f9c2a1b7d4e", "email": "jane@example.com",
          "first_name": "Jane", "last_name": "Doe", "display_name": "Jane Doe",
          "group_ids": ["guest", "all-users", "editor"],
          "role_ids": ["status-viewer", "viewer", "operator"],
          "permissions": ["datasets.create", "datasets.view", "materializations.create", "…"],
          "all_permissions": false,
          "dataset_permissions": { "ds-5c1e…": ["catalog.read", "data.export", "data.query", "dataset.view_schema"] },
          "dataset_limit": null,
          "password_change_required": false
        },
        "session": {
          "session_id": "ses-8c1e0f2a9b3d4c5e", "method": "sso:oidc",
          "signed_in_at": "2026-10-26T09:00:00Z", "issued_at": "2026-10-26T09:00:00Z",
          "expires_at": "2026-10-26T17:00:00Z", "session_expires_at": "2026-11-02T09:00:00Z",
          "refresh_after": "2026-10-26T09:10:00Z"
        },
        "providers": [{ "id": "password", "type": "password", "label": "Email and password", "administrators_only": false },
                      { "id": "oidc", "type": "sso", "label": "Dex" }],
        "passwordPolicy": { "min_length": 12, "max_length": 256 },
        "sessionError": null
      },
      "error": null
    },
    "domain_b": { "state": { "authenticated": false, "principal": { "kind": "guest", "…": "…" }, "…": "…" }, "error": null }
  },
  "providers": [{ "id": "oidc", "label": "Dex" }]
}
```

Each domain's `state` is what `GET /api/d/<domain>/auth/session` answers — that domain's principal,
a guest's where the browser has no session there — and `null`, with `error`, while that domain cannot
check the session. `dataset_permissions` lists, by dataset id, what grants on each dataset add to
`permissions`; `console/shared/access.ts::can(principal, key, datasetId)` answers from it.
`providers` at the top are the console's own registrations (03 §3.3a); a domain's `providers` say
which it accepts.

### `POST /api/d/<domain>/auth/login`

Body `{"email", "password"}`: a password signs in to one domain. `200` answers that domain's session
and sets its cookie; a session this browser already had there is ended in the background. Refusals
pass the control plane's through (`401` wrong email or password, `403` deactivated or
`password_sign_in_disabled`, `429` locked out, `503` sign-in not configured), and
`429 too_many_attempts` (`reason: rate_limited`) comes from the console itself when one address
tries too often.

### Signing in through a provider

`GET /api/auth/sso/<provider>/start?return=<path>` (`entra`, `github`, `google`, `oidc`) redirects to
the provider, with PKCE, a `state` bound to the browser by the cookie `<CONSOLE_SESSION_COOKIE>-flow`
and, for OpenID Connect, a nonce. The provider sends the browser back to
`GET /api/auth/sso/<provider>/callback?code&state`, which exchanges the code, hands the credential to
every domain's `POST /auth/login` at once, sets a session cookie for each that admitted the person,
and redirects to the return path — or `/sign-in` when none did — with `?signin=<id>`.
`GET /api/auth/sso/outcomes/<id>` then says what each domain answered, for five minutes:

```json
{ "provider": "oidc", "label": "Dex", "error": null,
  "domains": [ { "domain": "domain_a", "name": "Domain A", "status": "signed_in", "reason": null, "message": null },
               { "domain": "domain_b", "name": "Domain B", "status": "refused", "reason": "no_account",
                 "message": "ada@example.com has no account here. Ask an administrator to add you, then sign in again." } ] }
```

`status` is `signed_in`, `refused` (with the domain's reason) or `unreachable`; `error` says why the
sign-in went nowhere before any domain was asked (refused at the provider, expired, another browser).
An unknown provider is `404`; too many starts from one address `429`.

### `POST /api/d/<domain>/auth/logout`, `POST /api/auth/logout`

Body `{"scope": "current" | "others" | "session", "session_id": "…"}`, `current` by default.
`current` clears that domain's cookie even if its control plane cannot record the sign-out.
`POST /api/auth/logout` signs out of every domain the browser has a session in, and answers
`{"signed_out": true, "domains": ["domain_a", "domain_b"]}`.

Access tokens for external engines are in §8.27.

### `GET /api/auth/sessions`, `POST /api/auth/password`

`GET /api/auth/sessions` answers the control plane's `{current_session_id, sessions}`.

`POST /api/auth/password`, body `{"current_password", "new_password"}`, changes the caller's
password. Every session of theirs ends when it applies, so the console signs them straight back in
with the new password and answers `{"status": "applied", "session": <the new session>}` with a new
cookie. `{"status": "pending", …}` (`202`) means the control plane accepted it but has not applied
it within 10 s; the old password works until it does.

### Users, groups, roles, permissions, audit

Request bodies and query parameters are the control plane's
([04 §4.13](../../docs/handbook/04-http-api-reference.md)), validated here first by strict
schemas: an unknown field is a `400`. Reads pass the control plane's answer through.

Changes answer once the identity agent has applied them:

```json
{ "request_id": "idr-5b2d8e1f0a3c", "status": "applied", "entity_type": "user", "entity_id": "usr-9d0e1f2a3b4c" }
```

`200` when applied; `202` with `"status": "pending"` when not applied within 10 s; and when the
agent refused it, the refusal as an error — `400 invalid_request`, `403 forbidden`,
`404 not_found`, `409 already_exists` or `409 conflict`, with `details.reason` and, where the
refusal names one, `details.field`. After an applied change every cached session check is dropped,
so what anyone may do takes effect at their next request.

---

## 8.26 The Table tab: `GET …/tables/:table` and `POST …/maintenance`

`GET /api/materializations/:id/tables/:table` joins the control plane's record of one of the
materialization's tables with what the console reads from the table itself
([06 §6.5](./06-iceberg-tables.md#65-the-table-tabs-view)). Either half may be missing; the other
still answers. A `:table` the materialization does not have is a `404` naming its tables.

```json
{
  "materializationId": "mat-d89c130365f6",
  "table": "orders",
  "record": {
    "location": "gs://my-lake/midstream/shop-d89c130365f6/orders",
    "partition_granularity": "day", "schema_id": 1, "current_snapshot_id": "5204668659448186455",
    "sequence_number": 57, "snapshots": 57, "last_commit_at": "2026-10-19T05:40:42.627Z",
    "last_commit_duration_ms": 2211, "valid_through": "2025-05-09T19:07:06.794Z",
    "totals": { "records": 35417539, "files": 2870, "bytes": 6031082354 },
    "offsets": { "0": 1000207, "…": "…" }, "last_error": null,
    "maintenance": [
      { "job": "compaction", "completed_at": "2026-10-19T05:38:47.789Z", "outcome": "succeeded",
        "duration_ms": 195370, "snapshot_id": "216270159839468854",
        "partition": "2025-03-31 … 2025-05-24 (55 partitions)",
        "counts": { "partitions": 55, "inputs": 1378, "outputs": 93, "records": 5490697,
                    "input_bytes": 1073192635, "output_bytes": 899230245 }, "message": null }
    ],
    "live": { "member_id": "…", "pending_files": 4, "last_commit_duration_ms": 2211,
              "maintenance": null, "…": "…" },
    "…": "…"
  },
  "metadata": {
    "formatVersion": 2, "currentSchemaId": 1, "currentSnapshotId": "9221613250211573719",
    "partitionSpec": "day(_kafka_timestamp)", "sortOrder": "_kafka_timestamp ASC NULLS FIRST",
    "snapshotCount": 52,
    "totals": { "files": 3559, "bytes": 5744093103, "records": 33132184, "smallFiles": 3559, "deleteFiles": 0 },
    "smallFileBytes": 67108864,
    "snapshots": [ { "snapshotId": "9221613250211573719", "operation": "append", "addedFiles": 170,
                     "addedRecords": 599062, "validThrough": "2025-05-03T14:44:53.497Z",
                     "runtime": "midstream-mat-d89c130365f6", "current": true, "…": "…" } ],
    "partitions": [ { "key": "2025-11-11", "files": 1, "bytes": 208026, "records": 743,
                      "smallFiles": 1, "compactionDue": false } ],
    "properties": { "history.expire.min-snapshots-to-keep": "20", "materializer.fork-map": "…", "…": "…" },
    "…": "…"
  },
  "metadataError": null,
  "connection": { "uri": "https://midstream.example.com/catalog", "configured": true, "enabled": true },
  "storage": { "name": "lake", "kind": "s3", "scope": "s3://example-lake/", "endpoint": "http://seaweedfs-s3.seaweedfs.svc:8333",
               "region": "us-east-1", "pathStyle": true, "mode": "direct", "account": null }
}
```

| Part | Notes |
|---|---|
| `record` | The control plane's `GET /materializations/{id}/tables/{table}` (handbook 04 §4.6.4), from the runtime's commit events and the maintenance app's; `state` is `active`, `retired` (kept), `dropping` or `dropped`, `maintenance_job` the compaction or orphan cleanup the app runs now, `live` the committer's own report on its heartbeat (files pending). `null` before the table exists |
| `metadata` | Read from the table: the newest 200 snapshots, newest first, every partition with files, newest first. A file is small below `smallFileBytes` (the runtimes' `compaction_small_file_bytes`); `compactionDue` marks a partition the maintenance app's next compaction takes: one with `compaction_min_input_files` small files that compaction may take now, by its planner's rule (`server/iceberg/compaction.ts`, mirroring `lakehouse/maintenance.py`) — today or this hour only once no commit has added to it for `compaction_hot_partition_delay_minutes`, this month's files each once they are that old. A compacted day is usually still "small" by that measure — what matters is how many there are |
| `metadataError` | Why `metadata` is `null`: the table has not been created, data access is turned off, no storage profile covers its location (`no storage is configured for …`), or the table could not be read (§8.13) |
| `connection` | As `GET /api/catalog/connection` (§8.27). An engine names the table `lake.<dataset>.<table>`: its dataset is its Iceberg namespace |
| `storage` | The console's storage profile for the table's location — name, kind, scope, endpoint, region, path style, mode, and an Azure profile's account — for the Connect panel's snippets; never a credential, nor which credentials the console holds. `null` when no profile covers the location |

`metadata.properties` leaves out the fork map and Iceberg's `schema.*` properties for a caller who may
not see the table's dataset's schemas; snapshots, files and maintenance are anyone's who may see the
materialization.

The two halves are read separately and can disagree for a moment: `metadata` follows the table's
freshness rule (06 §6.2), answering from a read up to five minutes old while it refreshes, so the
record may already show a newer commit.

`POST /api/materializations/:id/maintenance` (`materializations.maintain`) asks the maintenance app
to run a job now. Body `{ "job": "compaction" | "expire_snapshots" | "orphan_cleanup" | "reprotect", "table": <name|null>, "purge": <bool> }`,
nothing else, `purge` only with `reprotect` (`400` otherwise): on that table, written or kept, or with no
`table` on every table, kept ones included. The Table tab sends the table it shows; the Protection
tab's *Mask rows already written* sends `reprotect` with no table and the dialog's *Purge the plain
text now*. A reprotect also needs `dataset.protect` on each table's dataset, which the control plane
checks as the person (control-plane handbook 22 §22.14). `202` passes the control plane's acceptance
through:

```json
{ "materialization_id": "mat-d89c130365f6", "request_id": "mnt-…", "job": "compaction",
  "table": "orders", "tables": ["orders"], "accepted_at": "2026-10-19T05:41:02Z" }
```

The maintenance app runs the job at its next opportunity, whether or not the runtime runs; while it
runs it shows as `record.maintenance_job`, and its outcome lands in `record.maintenance`. The
control plane refuses with `409` (`control_plane_error`, `upstream_details.reason`) when the
materialization is being deleted (`materialization_deleting`) or the table has not been created yet
(`table_not_created`); a `table` it does not have, or one being dropped, is a `400`. A stopped
materialization is no longer refused (`materialization_not_running` was 1.0's).

## 8.27 Access tokens and the catalog connection

Access tokens let external engines read — and, with `catalog.write`, commit to — tables through the
Iceberg REST catalogs (handbook 18 §18.15). A token is made in one domain and is good in **every
domain** where its owner has an account (handbook 20 §20.8). Neither the console nor any domain
stores its text.

### `GET /api/d/<domain>/auth/tokens`

The tokens this domain made for the caller (any signed-in person), never their text:

```json
{
  "domain": "domain_a",
  "tokens": [
    { "token_id": "b7dd6de1da6c", "name": "DuckDB on my laptop", "permissions": ["catalog.read"],
      "datasets": [{ "name": "domain_a.sales" }, { "name": "domain_b.finance" }], "issuer": "domain_a",
      "created_at": "2026-10-26T03:01:16Z", "expires_at": "2027-01-24T03:01:16Z",
      "revoked_at": null, "revoked_by": null, "active": true }
  ],
  "limit": 20,
  "permissions": [
    { "key": "catalog.read", "label": "Connect external tools to the Iceberg catalog", "granted": true },
    { "key": "catalog.write", "label": "Commit to tables from external tools", "granted": false }
  ],
  "datasets": [{ "dataset_id": "ds-5c1e…", "name": "domain_a.sales", "permissions": ["catalog.read"] }],
  "max_datasets": 100,
  "default_expires_in_days": 90,
  "max_expires_in_days": 365
}
```

`permissions[].granted` says whether the caller holds that permission somewhere in this domain;
`datasets`, this domain's datasets they read through the catalog, which a token may be limited to.
The Account page asks every domain the person is signed in to and lists every token at once.

### `POST /api/d/<domain>/auth/tokens`

Needs `catalog.read` on some dataset of the domain. Body, strictly validated:

```json
{ "name": "DuckDB on my laptop", "permissions": ["catalog.read"], "expires_in_days": 90, "datasets": ["domain_a.sales", "domain_b.finance"] }
```

| Field | Rules |
|---|---|
| `name` | 1–64 characters, trimmed; name it after where it will be used |
| `permissions` | `catalog.read`, `catalog.write` or both; each one the caller holds in this domain, or `403` |
| `expires_in_days` | 1–365 |
| `datasets` | optional; `<domain>.<dataset>` names of any domain (a bare name is this domain's), ≤ 100: the token then holds nothing on any other |

The console waits until the domain has recorded the token, then answers `201`, with
`cache-control: no-store`, its text — `tok_<claims>.<signature>` — this once, beside its id, name,
permissions, `issuer`, `datasets` and `expires_at`; `202` if it is accepted but not yet recorded. A
token the domain refuses to record (over the limit of 20, a dataset gone) is never shown: the refusal
is the answer, and the domain revokes it. Only a person signed in makes tokens: a request authorized
by an access token is refused (`403`, `reason: session_required`).

### `DELETE /api/d/<domain>/auth/tokens/:tokenId`

Revoke a token made in this domain: your own, or anyone's with `users.manage`. The console waits for
the domain to record the revocation, then hands it — signed with the federation key — to every other
domain, and answers which recorded it now:

```json
{ "request_id": "idr-…", "status": "applied", "domains": { "domain_b": "applied", "domain_c": "failed" } }
```

A domain that could not be told is told again within five minutes (the console reads each domain's
revocations and hands them on). An unknown id, or another person's without `users.manage`, is `404`.

### `GET /api/d/<domain>/catalog/connection`

Needs `catalog.read` on some dataset of the domain. Where engines reach the catalogs:

```json
{ "uri": "https://midstream.example.com/d/domain_a/catalog", "federatedUri": "https://midstream.example.com/catalog",
  "domain": "domain_a", "configured": true, "enabled": true }
```

| Field | Meaning |
|---|---|
| `uri` | The domain's own catalog through the console, `<console>/d/<domain>/catalog`: one level of namespaces, the domain's datasets |
| `federatedUri` | The federated catalog, `<console>/catalog`: namespaces `<domain>.<dataset>` |
| `configured` | `false` when the console's URL is not configured, and the URIs are the address a request arrived at |
| `enabled` | The domain's control-plane setting `catalog_api_enabled`; when `false` its catalog refuses every request |

## 8.28 Datasets

A dataset is a named group of tables and their Iceberg namespace (handbook 18 §18.16). The routes
pass the control plane's answers through, asked as the person, so `locked` and a dataset's
`permissions` describe them (`routes/datasets.ts`). A change waits for the directory to apply it, as a
user or group change does (`settle`): `200` (`201` for a new dataset), `202` when still pending after
10 seconds, or the refusal.

| Route | Body / query | Answer |
|---|---|---|
| `GET /api/datasets` | `?q=` searches names, descriptions and table names | `{ datasets: CpDataset[], returned, can_create }` (handbook 04 §4.15) |
| `POST /api/datasets` | `{ name, description? }`; the name checked as SQL reads it before it leaves | `{ request_id, status, entity_id, name }`; the creator owns it, All Users reads it |
| `GET /api/datasets/:dataset` | — | The dataset with `tables`: name, topic, state (`pending` until created), materialization id and label |
| `PATCH /api/datasets/:dataset` | `{ description, base_revision? }` | The description changed; a name never changes |
| `DELETE /api/datasets/:dataset` | — | Refused `409 dataset_not_empty` while it holds a table |
| `GET /api/datasets/:dataset/grants` | — | `{ grants, roles (each grantable or not), candidates: { users, groups } }` |
| `PUT /api/datasets/:dataset/grants/:principalId` | `{ role_ids, principal_type? }` | The roles set; `[]` removes the grant. Open detail streams pick the change up on their next refresh |

The guards turn away a guest and anyone without the permission on any dataset; the control plane
decides on the dataset itself, so an owner of one dataset changing another is refused there (`403`).

## 8.29 `GET /api/search?q=&limit=`

Materializations by label, id or topic, tables by `<dataset>.<table>` or `<domain>.<dataset>.<table>`,
and datasets by name or description — in each domain where the caller may see them
(`materializations.view`, `datasets.view` there), from the console's live state; `limit` 1–200,
default 50:

```json
{ "query": "orders", "truncated": false,
  "results": [ { "domain": "domain_a", "kind": "table", "id": "sales.orders", "label": "domain_a.sales.orders",
                 "detail": "orders", "materializationId": "mat-a1b2c3d4e5f6" } ] }
```

`kind` is `materialization`, `table` or `dataset`; the best matches come first.

## 8.30 The Iceberg REST catalogs: `/catalog` and `/d/<domain>/catalog`

Outside `/api`, for engines: the federated catalog, whose namespaces are `<domain>.<dataset>`
(`domain_a%1Fsales` in a path), handing each request to the domain the namespace names with the
caller's bearer token unchanged; and each domain's own catalog, handed through as it is (handbook 20
§20.10). `GET /catalog/v1/namespaces` lists one namespace per domain whose catalog lists something for
the token; `?parent=domain_a` that domain's datasets; a table is loaded or committed as its domain
answers; creating and dropping namespaces or tables, and transactions, are `501`. A domain that cannot
be reached is `503 ServiceUnavailableException`, naming it.

