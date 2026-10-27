# 04 — HTTP API Reference

The API is exposed by `KasprWebView` custom resources attached to the control-plane
`KasprApp`. There is no standalone HTTP server in this repository.

- **Port:** `6065` (`spec.config.webPort`)
- **Base path:** `/` (`spec.config.webBasePath`)
- **Content type:** `application/json` on every response
- **Service (dev):** `midstream-control-plane-app-api`

Local access:

```bash
kubectl -n midstream port-forward svc/midstream-control-plane-app-api 6065:6065
```

The tail app's endpoints, under `/tail/v1` on its own Service (`midstream-tail-app-api`),
are not part of this API: they answer the console alone and may change between releases
([19 §19.9](./19-live-tail.md)).

## 4.1 Endpoint summary

**Needs** is the permission the view authorizes its caller for
([18 — Identity and access](./18-identity-and-access.md) §18.3): `—` answers anyone, guests
included; *session* needs a signed-in person; *any* answers every caller, as a guest when there
is no session.

| Method | Path | WebView | Needs | Success | Persists? | Emits? |
|--------|------|---------|-------|---------|-----------|--------|
| `GET` | `/materializations` | `materializations-list` | `materializations.view` | 200 | no | no |
| `POST` | `/materializations` | `materializations-create` | `materializations.create`, and `dataset.write` on each table's dataset | **202** | **no** | `materialization-requests` |
| `GET` | `/materializations/{materialization_id}` | `materializations-detail` | `materializations.view_details` | 200 | no | no |
| `GET` | `/materializations/{materialization_id}/tables/{table_name}/schema` | `materializations-schema` | `materializations.view_details`, and `dataset.view_schema` on the table's dataset | 200 | no | no |
| `GET` | `/materializations/{materialization_id}/tables/{table_name}/schema/versions` | `materializations-schema-versions` | as above | 200 | no | no |
| `GET` | `/materializations/{materialization_id}/tables/{table_name}/schema/versions/{schema_id}` | `materializations-schema-version` | as above | 200 | no | no |
| `GET` | `/materializations/{materialization_id}/tables/{table_name}` | `materializations-table` | `materializations.view_details` | 200 | no | no |
| `POST` | `/materializations/{materialization_id}/maintenance` | `materializations-maintenance` | `materializations.maintain` | **202** | **no** | `<release>-maintenance-requests`, one message per table |
| `POST` | `/materializations/{materialization_id}/stop` | `materializations-stop` | `materializations.stop` | **202** | **no** | `materialization-requests` |
| `POST` | `/materializations/{materialization_id}/resume` | `materializations-resume` | `materializations.resume` | **202** | **no** | `materialization-requests` |
| `PATCH` | `/materializations/{materialization_id}` | `materializations-update` | `materializations.edit`, and `dataset.write` on each dataset a change of topics touches | **202** | **no** | `materialization-requests` |
| `DELETE` | `/materializations/{materialization_id}` | `materializations-delete` | `materializations.delete`, and `dataset.write` on each dataset it has tables in | **202** | **no** | `materialization-requests` |
| `GET` | `/datasets` | `datasets-list` | `datasets.view` | 200 | no | no |
| `POST` | `/datasets` | `datasets-create` | `datasets.create` | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/datasets/{dataset}` | `datasets-detail` | `datasets.view` | 200 | no | no |
| `PATCH`, `DELETE` | `/datasets/{dataset}` | `datasets-update`, `datasets-delete` | `dataset.manage` on it | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/datasets/{dataset}/grants` | `datasets-grants` | `dataset.manage` on it | 200 | no | no |
| `PUT` | `/datasets/{dataset}/grants/{principal_id}` | `datasets-grant-update` | `dataset.manage` on it | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/datasets/{dataset}/tables/{table}` | `datasets-table` | `datasets.view` | 200 | no | no |
| `GET` | `/topics` | `topics-list` | `topics.view` | 200 | no | no |
| `POST` | `/topics/analyze` | `topics-analyze` | `topics.view` | 200 | no | no |
| `GET` | `/system/version` | `system-version` | — (public) | 200 | no | no |
| `GET` | `/system/settings` | `system-settings` | `materializations.view` | 200 | no | no |
| `GET` | `/settings/{scope}` | `settings-read` | `settings.view` | 200 | no | no |
| `PATCH` | `/settings/{scope}` | `settings-update` | `settings.manage` | **202** | **no** | `<release>-settings-requests` |
| `GET` | `/settings/{scope}/history` | `settings-history` | `settings.view` | 200 | no | no |
| `POST` | `/auth/login` | `auth-login` | — | 200 | no | `<release>-identity-requests` |
| `POST` | `/auth/refresh` | `auth-refresh` | session | 200 | no | `<release>-identity-requests` |
| `POST` | `/auth/logout` | `auth-logout` | any | 200 | no | `<release>-identity-requests` |
| `GET` | `/auth/session` | `auth-session` | any | 200 | no | no |
| `GET` | `/auth/sessions` | `auth-sessions` | session | 200 | no | no |
| `POST` | `/auth/password` | `auth-password` | session | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/auth/tokens` | `auth-tokens-list` | session | 200 | no | no |
| `POST` | `/auth/tokens` | `auth-tokens-create` | `catalog.read` on a dataset | **202** | **no** | `<release>-identity-requests` |
| `DELETE` | `/auth/tokens/{token_id}` | `auth-tokens-revoke` | session: their own token, or anyone's with `users.manage` | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/auth/tokens/revocations` | `auth-tokens-revocations-list` | the console's service token, or `users.manage` | 200 | no | no |
| `POST` | `/auth/tokens/revocations` | `auth-tokens-revocations-apply` | — (each revocation's federation-key signature) | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/users` | `users-list` | `users.view` | 200 | no | no |
| `GET` | `/users/{user_id}` | `users-detail` | `users.view_details` | 200 | no | no |
| `POST` | `/users` | `users-create` | `users.manage` | **202** | **no** | `<release>-identity-requests` |
| `PATCH` | `/users/{user_id}` | `users-update` | `users.manage` | **202** | **no** | `<release>-identity-requests` |
| `DELETE` | `/users/{user_id}` | `users-delete` | `users.manage` | **202** | **no** | `<release>-identity-requests` |
| `POST` | `/users/{user_id}/password` | `users-password` | `users.manage` | **202** | **no** | `<release>-identity-requests` |
| `POST` | `/users/{user_id}/sign-out` | `users-sign-out` | `users.manage` | **202** | **no** | `<release>-identity-requests` |
| `POST` | `/users/{user_id}/unlock` | `users-unlock` | `users.manage` | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/groups`, `/groups/{group_id}` | `groups-list`, `groups-detail` | `groups.view` | 200 | no | no |
| `POST`, `PATCH`, `DELETE` | `/groups`, `/groups/{group_id}` | `groups-create`, `groups-update`, `groups-delete` | `groups.manage` | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/roles`, `/roles/{role_id}` | `roles-list`, `roles-detail` | `roles.view` | 200 | no | no |
| `POST`, `PATCH`, `DELETE` | `/roles`, `/roles/{role_id}` | `roles-create`, `roles-update`, `roles-delete` | `roles.manage` | **202** | **no** | `<release>-identity-requests` |
| `GET` | `/permissions` | `permissions-list` | session | 200 | no | no |
| `GET` | `/audit` | `audit-list` | `audit.view` | 200 | no | no |
| `GET` | `/identity/requests/{request_id}` | `identity-request` | any (§4.13.4) | 200 | no | no |
| `GET`, `HEAD`, `POST`, `DELETE` | `/catalog/v1/…` | the ten `catalog-*` views | `catalog.read` on the dataset; `catalog.write` to commit | see §4.14 | no | no |

A permission held on a dataset is checked on the dataset the request is about (18 §18.7): a refusal
is `403 forbidden` with `details.permission` and `details.dataset`, and an unknown dataset in a path
is `404`.

`POST …/pause` was removed in control plane 0.4.0. It re-applied an identical runtime spec — no
template rendered a paused state — so it never paused anything; `stop` replaces it.

> **The most important fact about this API:** mutations do **not** write to the state table.
> They validate, build a `MaterializationRequestEvent`, publish it to
> `materialization-requests`, and return 202. Persistence happens later, in the
> `request-intake` agent. `python/tests/test_web.py` asserts this for every mutation.
>
> Consequence: immediately after a `201`-style success you may still get `404` from
> `GET /materializations/{id}` for a short window. Poll `status_url` rather than assuming
> read-after-write consistency.
>
> `PATCH /settings/{scope}` follows the same rule: it publishes to
> `<release>-settings-requests`, and the `settings-intake` agent writes the `settings` table.
> See §4.9.3. So do the user, group, role, dataset and grant changes and `POST /auth/password`,
> which publish to `<release>-identity-requests` for the `identity-intake` agent; see §4.13 and
> §4.15. A create, and an edit adding topics, are also accepted before their tables' names are
> claimed: `claim_request_id` is what a client waits on (§4.5).

## 4.2 Common request headers

| Header | Required | Default | Effect |
|--------|----------|---------|--------|
| `Authorization: Bearer <token>` | for anything a guest may not do | none: the caller is a guest | The session token from `POST /auth/login`, or the console's service token. A token that cannot be used is refused with `401`, never treated as a guest's (§4.13) |
| `x-forwarded-for`, `x-forwarded-user-agent` | no | the connection's own address and `user-agent` | Where a request came from, as the console reports it for the browser; recorded on sessions and audit entries |
| `Content-Type: application/json` | for request bodies | — | Body is parsed with `await request.json()` |

Every mutation is attributed to the signed-in caller: their email becomes `created_by` on the
materialization, `submitted_by` on the request event and `updated_by` on a settings override, and
their user id `created_by_id` / `submitted_by_id`. The `x-submitted-by` header of earlier releases
is no longer read.

The examples below set `TOKEN` as §4.13.1 shows.

## 4.3 Identifier generation

```python
# code-derived (web/materializations.py)
def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"
```

- `materialization_id` → `mat-<12 hex chars>`, e.g. `mat-a1b2c3d4e5f6`
- `request_id` → `req-<12 hex chars>`, e.g. `req-9f8e7d6c5b4a`

Both can be injected in tests via keyword arguments (`materialization_id=`, `request_id=`),
but the WebView templates never pass them, so in production they are always generated.

---

## 4.4 `GET /materializations`

List materializations with joined status fields, bounded by `limit`.

### Query parameters

| Name | Allowed values | Default | Error |
|------|----------------|---------|-------|
| `limit` | `10`, `30`, `100`, `all` | `30` | `ValueError: limit must be one of: 10, 30, 100, all` |
| `q` | any text | none | Keeps materializations whose label, id, topics, datasets or tables (`dataset.table`) contain it, without regard to case; echoed back as `q` |

The WebView resolves `limit` by probing three request shapes in order:
`request.query_params` → `request.query` → `request.rel_url.query`.

### Response `200`

```json
{
  "items": [
    {
      "materialization_id": "mat-a1b2c3d4e5f6",
      "name": "Shop",
      "source_topics": ["sales.orders", "finance.invoices"],
      "tables": ["sales.orders", "finance.invoices"],
      "datasets": ["finance", "sales"],
      "destination_uri": "gs://my-lake/materialized",
      "desired_phase": "running",
      "observed_phase": "running",
      "phase_reason": "processing",
      "storage_size": "64Mi",
      "replicas": 16,
      "live_tail": true,
      "updated_at": "2026-01-15T10:30:00Z"
    }
  ],
  "limit": "30",
  "returned": 1
}
```

### Semantics

- Implemented as a prefix scan over `materialization:` in the `state` table.
- `name` is the label (its id when it has none); `tables` are the written tables as SQL reads them,
  `<dataset>.<table>`, in the order of the topics; `datasets`, every dataset its tables (kept ones
  too) are in, by name; `live_tail`, always `true` — every materialization has a live tail (19) — and
  still sent for 4.1 consoles, which offer Live only when they read it.
- For each materialization, `materialization-status:<id>` is looked up and joined.
- If **no status record exists**, the fallback is:
  - `observed_phase` = `materialization.desired_phase`
  - `phase_reason` = `materialization.desired_phase`
  - `updated_at` = `materialization.updated_at`
- If a status record exists, `updated_at` is `status.last_transition_time` (not the
  materialization's `updated_at`).
- Envelopes whose `entity_type` is not `materialization` are silently skipped.
- The scan **stops as soon as `limit` items are collected**; it does not scan the whole table
  first. There is no cursor, offset, or ordering guarantee — ordering is whatever
  `table.prefix_scan` yields.

### `limit=all`

`normalize_list_limit("all")` returns `"all"`, which sets `max_items = None`, scanning every
materialization. Use with care on large deployments.

---

## 4.5 `POST /materializations`

Create a new materialization.

### Request body

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `tables` | list of `{source_topic, dataset, name?}` | **yes** | — | Each Kafka topic to materialize, at most `max_materialization_topics` (20), with the **dataset** its table goes in and, optionally, the table's **name**; the order given orders the tables. A missing name is derived from the topic (§4.5.1). Each dataset must exist and the caller must be able to add tables to it (`dataset.write`); each `<dataset>.<name>` must be free. A topic once only. The 2.0 field `source_topics` is refused, naming this one |
| `name` | string | no | the id | A **label**: free text, 1–128 characters once trimmed (runs of space as one), no control characters, not unique, of no meaning in SQL or storage. It can be changed (§4.8.1) |
| `runtime_mode` | string | no | `"dedicated"` | Must be `dedicated` or `shared`; `shared` fails later during rendering |
| `resource_profile` | string | no | `null` | Carried through; not consumed by any template today |
| `replicas` | integer | no | `default_runtime_replicas` | Runtime members, `1`–`max_runtime_replicas`. Can change later, as can the topics (§4.8.1) |
| `autoscaling` | boolean | no | `true` | Whether the control plane may run its runtime with more members than `replicas` while it is far behind its topics, and back at `replicas` once caught up (07 §7.12a). The deployment's `runtime_autoscaling_enabled` overrides it. Can change later (§4.8.1) |
| `skip_tombstones` | boolean | no | `false` | When `true`, null-value records (tombstones) are acknowledged without a row; when `false`, each is a row with a null payload. Fixed at creation |
| `partition_granularity` | string | no | `default_partition_granularity` (`day`) | `hour`, `day`, `month` or `none`: the tables' partitioning by the Kafka timestamp (11 §11.3). Fixed at creation |
| `live_tail` | boolean | no | — | Not a choice: every materialization has a live tail (19), and only the deployment's `live_tail_enabled` turns it off, for all of them. `true`, which a 4.1 console may send, changes nothing; anything else is `400`, `field: "live_tail"` |
| `labels` | object | no | `{}` | Must be an object |
| `annotations` | object | no | `{}` | Must be an object |

A request does **not** choose where its tables go, where its runtime runs or what storage it gets:
`destination_uri`, `namespace`, `runtime_name` and `storage_size` are the deployment's, and a body
that names one is refused (§4.5.2).

Minimal request:

```bash
curl -sS -X POST localhost:6065/materializations \
  -H 'Content-Type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"tables": [{"source_topic": "sales.orders", "dataset": "sales", "name": "orders"}]}'
```

Full request:

```json
{
  "name": "Shop",
  "tables": [
    { "source_topic": "sales.orders", "dataset": "sales", "name": "orders" },
    { "source_topic": "finance.invoices", "dataset": "finance", "name": "invoices" },
    { "source_topic": "orders.v2", "dataset": "sales" }
  ],
  "runtime_mode": "dedicated",
  "replicas": 16,
  "partition_granularity": "day",
  "labels": { "team": "data-platform" },
  "annotations": { "owner": "data-eng@example.com" }
}
```

### Response `202`

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "request_id": "req-9f8e7d6c5b4a",
  "desired_phase": "running",
  "observed_phase": "requested",
  "phase_reason": "claiming_tables",
  "status_url": "/materializations/mat-a1b2c3d4e5f6",
  "accepted_at": "2026-01-15T10:30:00Z",
  "name": "Shop",
  "live_tail": true,
  "tables": [
    { "name": "orders", "source_topic": "sales.orders", "dataset": "sales", "dataset_id": "ds-5c1e…" },
    { "name": "invoices", "source_topic": "finance.invoices", "dataset": "finance", "dataset_id": "ds-9a0b…" },
    { "name": "orders_v2", "source_topic": "orders.v2", "dataset": "sales", "dataset_id": "ds-5c1e…" }
  ],
  "claim_request_id": "idr-3e8f0a1b2c4d",
  "claim_status_url": "/identity/requests/idr-3e8f0a1b2c4d"
}
```

`desired_phase` is always `"running"` for create; `observed_phase` is `"requested"`, and
`phase_reason` `"claiming_tables"` until the directory has claimed the tables' names. `tables` is
what SQL will call each topic's rows, `<dataset>.<name>`, fixed from now on. The names are claimed
next (18 §18.16): `GET /identity/requests/{claim_request_id}` says `applied` once they are the
materialization's, or `rejected` with the `reason` — `table_name_taken`, `dataset_not_found`,
`forbidden` — and `field`/`details` naming the table. A rejected create is marked `deleted` with that
reason, nothing having been created (07 §7.2a). The console waits on it for up to 10 seconds.

### Emitted event

Topic `materialization-requests`, key `materialization:mat-a1b2c3d4e5f6`:

```json
{
  "request_id": "req-9f8e7d6c5b4a",
  "materialization_id": "mat-a1b2c3d4e5f6",
  "action": "create",
  "submitted_at": "2026-01-15T10:30:00Z",
  "submitted_by": "jane@example.com",
  "spec": {
    "name": "Shop",
    "tables": [
      { "name": "orders", "source_topic": "sales.orders", "dataset": "sales", "dataset_id": "ds-5c1e…" },
      { "name": "invoices", "source_topic": "finance.invoices", "dataset": "finance", "dataset_id": "ds-9a0b…" },
      { "name": "orders_v2", "source_topic": "orders.v2", "dataset": "sales", "dataset_id": "ds-5c1e…" }
    ],
    "destination_uri": "gs://my-lake",
    "runtime_mode": "dedicated",
    "runtime_name": "midstream-mat-a1b2c3d4e5f6",
    "namespace": "midstream",
    "resource_profile": null,
    "storage_size": "64Mi",
    "replicas": 16,
    "skip_tombstones": false,
    "autoscaling": true,
    "partition_granularity": "day",
    "labels": {},
    "annotations": {}
  },
  "submitted_by_id": "usr-3f9c2a1b7d4e",
  "claim_request_id": "idr-3e8f0a1b2c4d"
}
```

`submitted_by_id` is present when a signed-in person sent the request; `claim_request_id` is the
id request intake gives the `claim_tables` request it sends on (06 §6.4).

The spec carries what the API resolved from the deployment when it accepted the request: its
destination, its namespace, the storage per replica, and in `runtime_name` the runtime's name,
generated from the id and length-bounded. Request intake records that name as the materialization's
`resolved_runtime_name`; there is no other (4.0 removed the client-chosen `runtime_name`).

### Validation order

1. Body must be a `dict` → `invalid_request("request body must be an object")`.
2. A body with `source_topics` and no `tables` → `invalid_request("source_topics is now tables: …", field="tables")`.
3. `destination_uri`, `namespace`, `runtime_name`, `storage_size`, in that order: one that is present
   and not `null` → `invalid_request("<field> is not chosen per materialization: <why>", field="<field>")`,
   the reason naming the deployment's value — `storage_size is not chosen per materialization: every
   runtime member gets the deployment's storage, 64Mi`.
4. The deployment has a destination, and it is a `gs://` URI → otherwise `409 conflict`,
   `reason: destination_not_configured` (§4.5.2). Nothing else is checked: no create can succeed.
5. `tables` (§4.5.1): present, a list of at most `max_materialization_topics` objects; each with a
   `source_topic` that is a Kafka topic name (letters, digits, `.`, `_` and `-`, at most 249
   characters, not `.` or `..`) and chosen once, a `dataset` name that exists (`404`,
   `reason: dataset_not_found`), and an optional `name` that follows the rules and is unique in the
   request → `invalid_request(..., field="tables.<index>.<source_topic|dataset|name>")`.
6. `name`, when given, must be a valid label → `invalid_request(..., field="name")`.
7. `labels` must be a `dict` → `invalid_request("labels must be an object", field="labels")`.
8. `annotations` must be a `dict` → `invalid_request("annotations must be an object", field="annotations")`.
9. `skip_tombstones`, when present and not `null`, must be a JSON boolean →
   `invalid_request("skip_tombstones must be a boolean", field="skip_tombstones")`; so must
   `autoscaling` (`field="autoscaling"`).
10. `partition_granularity` must be `hour`, `day`, `month` or `none`, and `replicas` is validated
    (`scale.py`) → `invalid_request(..., field=...)`.
11. `MaterializationRequestSpec.__post_init__` runs; a `ValueError` is converted to
    `invalid_request` with a best-effort `field` inferred by substring-matching the message.
12. The claim of the tables' names is previewed against the directory, with the agent's own checks
    (18 §18.16): the caller may not add tables to a dataset → `403 forbidden`
    (`permission: dataset.write`); a `<dataset>.<name>` is taken → `409 conflict`
    (`reason: table_name_taken`, `materialization_id` of the holder). Each names its row in
    `details.field`, `index`, `dataset`, `table` and `source_topic`.

A duplicate check runs *before* normalization: if `materialization:<id>` already exists,
`already_exists` (409) is raised. In practice this can only fire when `materialization_id` is
injected, since generated ids are random.

### 4.5.1 Names: datasets, tables, and the label

A materialization reads each of its topics into a table, in a dataset of the request's choosing; SQL
reads it as `<dataset>.<table>`, in the console and as `lake.<dataset>.<table>` through the REST
catalog (§4.14). Both parts follow one rule (`table_names.py`), so a person can type them unquoted in
any engine:

| Rule | Why |
|------|-----|
| A lowercase letter first, then lowercase letters, digits and underscores | A leading digit does not parse; `$` is legal in DuckDB but read as a dollar-quoted string by SQL front ends. Lowercase only: Trino folds unquoted names to lowercase, and DuckDB and Spark compare them without regard to case, so one spelling keeps every engine and the directory's keys in agreement. `Orders` is refused with the lowercase spelling suggested |
| At most 63 characters | A bound, not a DuckDB limit |
| Not a DuckDB `reserved` or `type_function` keyword (`order`, `select`, `table`, `join`, …) | `SELECT * FROM sales.order` is a syntax error. Verified against `duckdb_keywords()` |
| Not a statement verb or file-reading function (`update`, `copy`, `load`, `read_parquet`, …), nor `iceberg`, `catalog`, `lake` | Read-only SQL front ends — the console among them — refuse a statement that contains one; the last three name the console's own SQL surface |
| A dataset only: not one of DuckDB's own catalogs and schemas (`main`, `memory`, `temp`, `information_schema`, …) | A dataset is a namespace in SQL; one of these would be ambiguous or shadow DuckDB's own |

- A **dataset's name** is chosen when the dataset is created (§4.15) and never changes.
- A **table's name** is chosen for each topic, here or when an edit adds the topic. Omitted, it is
  derived from the topic — lower case, every run of characters other than letters and digits becomes
  `_`, leading and trailing `_` dropped, `t_` before a leading digit, `_topic` after a reserved word,
  at most 63 characters — so `orders-v2` becomes `orders_v2`; two topics of one request that derive
  the same name are told apart in the order given (`orders_v2`, `orders_v2_2`). A table's name is
  unique in its dataset across the deployment, and in its materialization whatever the dataset
  (counting kept tables, and tables dropped with their files kept). A table's dataset and name never
  change once it exists.
- The **materialization's name** is a label (free text, not unique) that defaults to its id.

Different materializations may read the same topic; each has a table of its own, under a name of its
own in its dataset.

### 4.5.2 What the deployment decides: destination, namespace, runtime name, storage

Every materialization takes these from the deployment, resolved when its request is accepted and
recorded on it (`web/materializations.py::_normalize_request_spec`); a later change to a setting
applies to materializations created after it.

| Field | Is | Settings page |
|---|---|---|
| `destination_uri` | `default_destination_uri`. The tables are created at `<destination_uri>/<storage_prefix>/<materialization_id>/<table>`, one directory each (11 §11.1) | Materializations › Destination |
| `namespace` | `default_namespace`: where the runtime's KasprApp runs | read-only |
| `runtime_name` | `<runtime_name_prefix>-<materialization_id>`, bounded to a Kubernetes name (`runtime/naming.py`); the record's `resolved_runtime_name` | — |
| `storage_size` | `default_runtime_storage_size` (`64Mi`): each runtime member's persistent volume, which holds only its state table (10 §10.8). The control plane's and the maintenance app's own volumes are their manifests' (1Gi) | Materializations › Storage per replica |

The destination must be a `gs://bucket[/prefix]` (`config.py::is_gcs_destination_uri`, the rule the
Settings page applies): tables live in Google Cloud Storage, and their pointer's compare-and-swap is a
GCS precondition. With none configured, or one that is not `gs://`, no materialization can be
created: `409 conflict`, `reason: destination_not_configured`, and the console's create dialog says
so. A request that names any of the four is refused (`400`, step 3 above) rather than quietly
overruled; 3.x let a request choose them (16 §16.8). An update may still repeat their current values
(§4.8.1), as a client sends back what it read.

---

## 4.6 `GET /materializations/{materialization_id}`

### Response `200`

```json
{
  "materialization": {
    "materialization_id": "mat-a1b2c3d4e5f6",
    "name": "Shop",
    "tables": [
      { "name": "orders", "source_topic": "orders", "dataset": "sales", "dataset_id": "ds-5c1e…" },
      { "name": "payments", "source_topic": "payments", "dataset": "finance", "dataset_id": "ds-9a0b…" }
    ],
    "retired_tables": [ { "name": "refunds", "source_topic": "refunds", "dataset": "sales", "dataset_id": "ds-5c1e…" } ],
    "source_topics": ["orders", "payments"],
    "datasets": ["finance", "sales"],
    "pending_topic_change": null,
    "destination_uri": "gs://my-lake",
    "runtime_mode": "dedicated",
    "namespace": "midstream",
    "replicas": 4,
    "autoscaling": true,
    "partition_granularity": "day",
    "live_tail": true
  },
  "autoscale": {
    "enabled": true,
    "own_replicas": 4,
    "replicas": 12,
    "reason": "5,470,010 records behind, about 2 h 3 min to catch up with 4 members",
    "changed_at": "2026-10-18T10:20:00Z",
    "eta_seconds": 1310,
    "lag": 4180115,
    "measured_at": "2026-10-18T10:35:00Z"
  },
  "status": {
    "desired_phase": "running",
    "observed_phase": "running",
    "phase_reason": "processing",
    "phase_message": "Runtime is actively processing source records.",
    "last_transition_time": "2026-10-18T10:32:15Z",
    "last_heartbeat": "2026-10-18T10:35:00Z",
    "last_resource_observation_time": "2026-10-18T10:34:00Z",
    "runtime_version": "1.0.0",
    "runtime_health": { "level": "healthy", "reason": null, "message": null },
    "runtime_progress": {
      "rows_written": 152340,
      "bytes_written": 48210944,
      "files_written": 31,
      "latest_offsets": { "orders": { "0": 51234, "1": 50880, "2": 50226 }, "payments": { "0": 9120 } },
      "member_progress": { "midstream-mat-a1b2c3d4e5f6-app-0": { "…": "per member" } },
      "tables": {
        "orders": { "member_id": "midstream-mat-a1b2c3d4e5f6-app-0", "…": "the committer's heartbeat block (06 §6.5)" },
        "payments": { "member_id": "midstream-mat-a1b2c3d4e5f6-app-0", "…": "" }
      },
      "cumulative_unmapped_by_table": { "orders": 3 }
    },
    "consumer_lag": {
      "total_lag": 4200,
      "topics": {
        "orders": { "total_lag": 4100, "partition_lag": { "0": 1400, "1": 1500, "2": 1200 }, "partitions": 3 },
        "payments": { "total_lag": 100, "partition_lag": { "0": 100 }, "partitions": 1 }
      },
      "measured_at": "2026-10-18T10:35:10Z"
    }
  },
  "runtime_binding": {
    "kaspr_app_name": "midstream-mat-a1b2c3d4e5f6",
    "namespace": "midstream",
    "managed_agent_names": [
      "midstream-mat-a1b2c3d4e5f6-writer",
      "midstream-mat-a1b2c3d4e5f6-committer",
      "midstream-mat-a1b2c3d4e5f6-settings"
    ]
  },
  "tables": [
    {
      "dataset": "sales",
      "dataset_id": "ds-5c1e…",
      "name": "orders",
      "source_topic": "orders",
      "identifier": "sales.orders",
      "location": "gs://my-lake/midstream/mat-a1b2c3d4e5f6/orders",
      "partition_granularity": "day",
      "schema_id": 2,
      "current_snapshot_id": 8410567672455764292,
      "last_commit_at": "2026-10-18T10:34:41Z",
      "valid_through": "2026-10-18T10:34:12Z",
      "totals": { "records": 152001, "files": 29, "bytes": 41875202 },
      "last_error": null,
      "state": "active",
      "maintenance_job": null,
      "readable": true,
      "queryable": true
    },
    { "dataset": "finance", "name": "payments", "identifier": "finance.payments", "readable": false, "queryable": false, "…": "" },
    { "dataset": "sales", "name": "refunds", "identifier": "sales.refunds", "state": "retired", "…": "" }
  ]
}
```

### Field notes

- `runtime_progress` is **omitted entirely** when empty (all counters `None` and no offsets).
- `runtime_binding` is `null` until `apply_runtime` completes. `tables` lists the tables created so
  far, each with its `state` (11 §11.7): the written ones in the order of the topics, then the kept
  ones (`materialization.retired_tables`), then any being dropped or dropped — empty until
  `create_table` has created the first. A table's full record is `GET …/tables/{table_name}` (§4.6.4).
- `materialization.name` is the label; `datasets` every dataset its tables are in, by name. Each
  table summary carries its `dataset` and `dataset_id`, `identifier` (`<dataset>.<name>`), and, for
  the caller, whether they may see its schema (`readable`, `dataset.view_schema` on its dataset) and
  query it (`queryable`, `data.query`).
- `live_tail`: always `true`, for 4.1 consoles: every materialization's written tables have a live
  tail while it runs and the deployment's `live_tail_enabled` is on (19).
- `autoscale` is how the control plane scaled the runtime (07 §7.12a): `replicas` the members it runs
  while scaled up, more than its own `own_replicas` (`materialization.replicas`), and `null` while it
  runs its own count; `reason` why, in words for a person, `null` likewise; `changed_at` when the
  count last changed, up or back; `eta_seconds` the time to catch up at the pace last measured, `null`
  while there is no telling (no drain yet, or too few measurements); `lag` and `measured_at` the last
  measurement it read. `enabled` is the materialization's own `autoscaling`: the deployment's switch is
  `GET /system/settings`' `autoscaling_enabled`. Present since 4.4.
- `pending_topic_change`, while an edit adding topics waits for their names (§4.8.1):
  `{request_id, claim_request_id, submitted_at, added}`.
- `materialization.source_topics` and `tables` are what the runtime should read; while a change of
  topics drains the runtime (07 §7.9a) `runtime_binding.source_topics` still names what it was
  applied with.
- `latest_offsets` maps each **table** to its partitions' offsets; partition keys are **numeric
  strings**, not integers. `runtime_progress.tables` is each table as the committer member last
  reported it.
- `consumer_lag` counts records not yet committed **to the tables** (10 §10.7), per **topic** and in
  all; `error` names each topic that could not be measured.
- The projection also carries the materialization's `resolved_runtime_name`, `resource_profile`,
  `storage_size`, `skip_tombstones`, `created_at`, `updated_at`, `created_by`, `created_by_id`,
  `labels`, `annotations` and `spec_version`; the status's `observation_source`, `last_error` and
  full `resource_observation`; and the binding's `runtime_mode`, `template_version`,
  `source_topics`, `last_applied_spec_hash`, `last_apply_operation_id` and timestamps.
- Schemas are **not** part of this payload; use `GET …/tables/{table_name}/schema`.

### When status is missing

If `materialization-status:<id>` does not exist, `_fallback_status()` synthesizes:

```python
# code-derived
MaterializationStatus(
    materialization_id=...,
    desired_phase=m.desired_phase,
    observed_phase=m.desired_phase,       # mirrors desired
    phase_reason=m.desired_phase,         # e.g. literally "running"
    last_transition_time=m.updated_at,
    observation_source="hybrid",
    runtime_health=RuntimeHealth(level="unknown"),
    resource_observation=ResourceObservation(
        namespace=m.namespace,
        kaspr_app_name=resolve_runtime_app_name(m),
        resource_exists=False,
        managed_agent_states={},
    ),
)
```

A `phase_reason` that is exactly a phase name (e.g. `"running"`, `"paused"`) is a reliable
signal that **no real status record exists yet**.

### Response `404`

```json
{
  "error": {
    "code": "not_found",
    "message": "materialization mat-unknown was not found",
    "details": { "resource": "materialization", "id": "mat-unknown" }
  }
}
```

---

## 4.6.1 `GET /materializations/{materialization_id}/tables/{table_name}/schema`

One table's current schema, as the control plane's projection of it (05 §5.5, 09 §9.9). Each table
has schemas of its own, evolved from its topic's records. Read-only; it never opens the table.
`?raw=true` adds the Iceberg schema and the fork map as the table stores them, its last column id
and the paths the schema policy refused (`last_column_id`, `rejected_paths`): what a row is
conformed with, which the tail app seeds a table's live tail from (19). `table_name` is
compared without regard to case; a name the materialization has no table of is `404 not_found`
(`resource: "materialization-table"`), naming the tables it has. The caller needs
`dataset.view_schema` on the table's dataset, as for the two endpoints below: otherwise `403`, naming
the dataset.

### Response `200`

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "table": "orders",
  "dataset": "sales",
  "dataset_id": "ds-5c1e…",
  "source_topic": "orders",
  "schema_id": 3,
  "since": "2026-10-18T10:41:12Z",
  "metadata_location": "gs://…/mat-a1b2c3d4e5f6/orders/metadata/00041-….metadata.json",
  "field_count": 24,
  "fields": [
    { "field_id": 1, "path": ["_kafka_timestamp"], "name": "_kafka_timestamp", "type": "timestamptz",
      "required": false, "fork_version": 1, "state": "current", "kind": "metadata", "since_schema_id": 0 },
    { "field_id": 14, "path": ["total"], "name": "total", "type": "double", "required": false,
      "fork_version": 1, "state": "superseded", "kind": "value", "since_schema_id": 0 },
    { "field_id": 31, "path": ["total__v2"], "name": "total", "type": "string", "required": false,
      "fork_version": 2, "state": "current", "kind": "value", "since_schema_id": 3, "fork_of": 14 }
  ],
  "changes": [
    { "kind": "forked", "path": ["total"], "field_id": 31, "physical_path": ["total__v2"], "type": "string" }
  ],
  "unmapped": { "total_values": 0, "last_seen_at": null }
}
```

### Semantics

- One entry per field, the metadata columns first (`kind: metadata`), then the payload in schema
  order with each fork family's members together. `name` is the logical name every member of a
  family shares; `path` is the physical one.
- `field_count` counts payload fields, nested ones included.
- `changes` is what this schema changed from the one before it (`added`, `forked`, `collapsed`,
  `rejected`).
- `since` is when the runtime committed this schema to the table — when the materializer met the
  shape, not when the topic's records had it: a backfill reads partitions at their own pace, and
  schema 0 is sampled from both ends of the topic (09 §9.6). When the data had each field is in the
  table itself; the console reads it from the manifests (console 08, `GET …/schema/timeline`).
- `unmapped` counts values the runtime put in this table's `_unmapped`, and when one last went
  there (09 §9.8).
- Before discovery completes there is no projection: the same shape, with `schema_id: null` and
  `fields: []`.
- Every schema is also archived under `materialization-schema-version:<id>:<table>:<schema_id>`,
  readable through the two endpoints below, the newest `schema_history_limit` (50) kept per table.

---

## 4.6.2 `GET /materializations/{materialization_id}/tables/{table_name}/schema/versions`

A table's archived schemas, newest first. Accepts `?limit=<n>` or `?limit=all` (default all).

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "table": "orders",
  "source_topic": "orders",
  "current_schema_id": 3,
  "versions": [
    { "schema_id": 3, "since": "2026-10-18T10:41:12Z", "field_count": 24,
      "change_counts": { "forked": 1 }, "current": true },
    { "schema_id": 0, "since": "2026-10-18T10:30:10Z", "field_count": 23,
      "change_counts": { "added": 23 }, "current": false }
  ],
  "returned": 2
}
```

Returns `404 not_found` for an unknown materialization, and an empty `versions` array before
discovery.

---

## 4.6.3 `GET /materializations/{materialization_id}/tables/{table_name}/schema/versions/{schema_id}`

One archived schema of a table, in the same shape as `GET …/schema` (and `?raw=true` likewise).

- A non-numeric or negative id → `400 invalid_request` with `details.field = "schema_id"`.
- An id that was never archived, or was evicted by the retention limit → `404 not_found` with
  `details.resource = "materialization-schema-version"`. The current schema always resolves.

---

## 4.6.4 `GET /materializations/{materialization_id}/tables/{table_name}`

One table's record (05 §5.12), plus `identifier` and `live` — the committer member's last heartbeat
block for the table, such as files waiting for a commit (06 §6.5); `live` is `null` until a runtime
reports.

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "source_topic": "orders",
  "dataset": "sales",
  "dataset_id": "ds-5c1e…",
  "name": "orders",
  "identifier": "sales.orders",
  "location": "gs://my-lake/midstream/mat-a1b2c3d4e5f6/orders",
  "table_uuid": "3f2a…",
  "partition_granularity": "day",
  "metadata_location": "gs://…/metadata/00042-….metadata.json",
  "current_snapshot_id": 8410567672455764292,
  "sequence_number": 41,
  "last_commit_at": "2026-10-18T10:34:41Z",
  "last_commit_duration_ms": 2367,
  "totals": { "records": 152001, "files": 29, "bytes": 41875202 },
  "offsets": { "0": 49998, "1": 49870 },
  "valid_through": "2026-10-18T10:34:12Z",
  "snapshots": 41,
  "schema_id": 3,
  "maintenance": [
    { "job": "compaction", "completed_at": "2026-10-18T12:00:03Z", "outcome": "succeeded",
      "partition": "2026-10-17", "duration_ms": 8211, "snapshot_id": 6001234567890123456,
      "counts": { "partitions": 1, "inputs": 32, "outputs": 1, "records": 98160,
                  "input_bytes": 16334703, "output_bytes": 14920111 },
      "started_at": "2026-10-18T11:59:55Z" }
  ],
  "last_error": null,
  "state": "active",
  "maintenance_job": { "job": "compaction", "started_at": "2026-10-18T12:15:00Z", "partition": "2026-10-18",
                       "attempt": 1, "member_id": "midstream-maintenance-app-0" },
  "live": { "member_id": "midstream-mat-a1b2c3d4e5f6-app-0", "pending_files": 2 },
  "readable": true,
  "queryable": true
}
```

`state` is the table's place in the materialization (11 §11.7): `active`, `retired` (kept read-only
after its topic was removed), `dropping`, or `dropped` (with its files kept). `maintenance_job` is the
compaction or orphan cleanup the maintenance app runs on it now, `null` otherwise.

`404 not_found` (`resource: "materialization-table"`) before the table exists, and for a name the
materialization has no table of - a kept table is found, one being dropped is not. `snapshot_id`s are 64-bit: a client that parses JSON into doubles
must keep them as text (the console does, console 06).

---

## 4.6.5 `POST /materializations/{materialization_id}/maintenance`

Run a table maintenance job now:
`{"job": "compaction" | "expire_snapshots" | "orphan_cleanup", "table": "<table name>"}`. With
`table` the job runs on that table, written or kept; without it, on every table, kept ones included.
The WebView publishes one `maintenance_request` per table on `<release>-maintenance-requests`,
keyed by the table (06); the maintenance app runs each at its next opportunity, ahead of scheduled
jobs, whether or not the runtime runs - a stopped materialization's tables included (10 §10.10). The
outcome arrives as a `maintenance_completed` table event in the table record's `maintenance` history,
and a running compaction or orphan cleanup shows as the record's `maintenance_job` (§4.6.4).

### Response `202`

```json
{ "materialization_id": "mat-a1b2c3d4e5f6", "request_id": "mnt-3c4d5e6f7a8b",
  "job": "compaction", "table": null, "tables": ["orders", "payments"], "accepted_at": "2026-10-18T11:00:00Z" }
```

| Error | When |
|---|---|
| `400 invalid_request`, `field: "job"` | an unknown job |
| `400 invalid_request`, `field: "table"` | a table the materialization does not have, or one being dropped |
| `409 conflict`, `reason: "table_not_created"` | the table named — or, without one, every table — is not created yet |
| `409`, `materialization_deleting` | it is being deleted |

Before 2.0 a stopped materialization refused this (`materialization_not_running`): its runtime ran
maintenance, and only while it ran.

A requested compaction ignores the hot-partition delay; one with nothing to do reports `skipped`
with the reason. Asking for compaction also retries partitions it had given up on (11 §11.6).

---

## 4.7 `POST /materializations/{id}/stop`

Sets `desired_phase = "stopped"` and emits `action = "stop"` → work step `apply_runtime`, which
renders the runtime's KasprApp with `replicas: 0`. The materialization's own `replicas` is left
alone, so a resume scales it back to what it was.

```bash
curl -sS -X POST localhost:6065/materializations/mat-a1b2c3d4e5f6/stop \
     -H "authorization: Bearer $TOKEN"
```

### Response `202`

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "request_id": "req-1a2b3c4d5e6f",
  "desired_phase": "stopped",
  "observed_phase": "running",
  "phase_reason": "processing",
  "status_url": "/materializations/mat-a1b2c3d4e5f6",
  "accepted_at": "2026-01-15T11:00:00Z"
}
```

`observed_phase` / `phase_reason` reflect the **current** persisted status, not the target.

### What stop does

The Kaspr operator deletes the runtime's HPA and scales its StatefulSet to zero; every pod exits.
The KasprApp, its agents, tasks and table, the Kafka topics, the consumer group and its committed
offsets, and the persistent volumes (`whenScaled: Retain`) all stay. Observation confirms the stop
from the operator's member count: `observed_phase = "stopped"`,
`phase_reason = "runtime_scaled_to_zero"` once `available_members` reads `0/0`
([08](./08-observation-and-status.md)). On the lab cluster a 16-member runtime stopped in about a
minute.

> **Rows staged on the pods are read again, not lost.** Since 0.7.0 a runtime commits a record's
> offset to Kafka only once its row is in object storage ([10 §10.16](./10-runtime-internals.md)).
> Stopping removes the pods and every row staged since the last batch became durable, but those
> records' offsets were never committed, so the resumed runtime reads them again from Kafka. Runtimes
> before 0.7.0 committed on staging and lost them: 30,003 rows, in one measured stop.

| Response | When |
|----------|------|
| `404 not_found` | The materialization does not exist |
| `409 conflict`, `reason: materialization_deleting` | It is being deleted |
| `409 conflict`, `reason: runtime_not_deployed` | Discovery has not produced a schema yet, so there is no runtime to apply |

---

## 4.8 `POST /materializations/{id}/resume`

Sets `desired_phase = "running"` and emits `action = "resume"`, also step `apply_runtime`: the
runtime is rendered at the materialization's `replicas` again. Response shape is identical to stop
with `desired_phase: "running"`. Resuming a materialization that is already meant to run re-applies
its runtime, which is how a `failed` one is retried.

On the lab cluster a 16-member runtime resumed in about four minutes: the operator scales the
StatefulSet to its initial step and its HPA doubles it from there.

`404 not_found` for an unknown materialization; `409 conflict` (`materialization_deleting`) for one
being deleted.

---

## 4.8.1 `PATCH /materializations/{id}`

Change what can change after creation - the label, the runtime's `replicas` and whether it is
autoscaled, and the topics it reads - and re-apply its runtime when the topics or replicas change, or
when turning autoscaling off takes members away. Emits `action = "update"`. The
desired phase is not touched: a stopped materialization stays stopped and runs with the new settings
once resumed. The destination and everything else stay as created.

```bash
curl -sS -X PATCH localhost:6065/materializations/mat-a1b2c3d4e5f6 \
     -H 'Content-Type: application/json' -H "authorization: Bearer $TOKEN" \
     -d '{"tables": [{"source_topic": "orders"}, {"source_topic": "refunds", "dataset": "sales", "name": "refunds"}],
          "removed_tables": {"payments": "keep"}}'
```

| Body | Result |
|------|--------|
| `name` | A new label; empty goes back to the id. Applies at once and touches nothing else |
| `live_tail` | Not a setting: every materialization has a live tail (19). `true`, which a 4.1 console sends back, is accepted and changes nothing; anything else is `400`, `field: "live_tail"`, saying that only the deployment's `live_tail_enabled` turns it off |
| `replicas` | An integer from 1 to `max_runtime_replicas`. While autoscaling has the runtime scaled up, a count at or above the scaled one ends the scaling; one below it takes effect once the runtime has caught up (07 §7.12a) |
| `autoscaling` | A boolean. Turned off while the runtime is scaled up, the runtime goes back to `replicas` at once; turned on, the next lag measurements decide. Not part of the runtime's spec: `spec_version` stays |
| `tables` | The topics to read from now on, in order, each `{source_topic, dataset?, name?}`, at most `max_materialization_topics`. A topic already read, or kept a table of, keeps its table: it may only repeat its dataset and name, and to put it in another dataset its table must be dropped first. Any other topic needs a `dataset`, and gets a new table there named `name` or after the topic, apart from every name the materialization holds (07 §7.9a) |
| `removed_tables` | `{"<table>": "keep" \| "drop"}`: for **each** table whose topic `tables` removes, `keep` it read-only or `drop` it with its data; may also `drop` a kept table. Required for every removed topic's table |
| At least one of the five | otherwise `400 invalid_request`, `field: "replicas"` |
| Any other creation setting (`destination_uri`, `storage_size`, …) at its **current** value | Accepted and ignored, so a client can send back what it read |
| A creation setting at a **different** value | `400 invalid_request`, `"<field> cannot be changed after creation"`, `details.field` set |
| Anything else | `400 invalid_request`, `"<field> is not a setting that can be changed"` |

Refused with `400 invalid_request` and `details.field`: topics that are not topic names, or too many,
a new topic without a dataset, a name taken in the materialization (`tables.<index>.<field>`); a
removed topic's table with no decision, a table that does not exist, a table still read named in
`removed_tables`, an action other than `keep`/`drop`, or dropping a kept table while adding its topic
back (`removed_tables`). A change of topics needs `dataset.write` on the dataset of every table it
adds, writes again, removes or drops (`403`, naming the dataset), and a new table's name must be free
there (`409 table_name_taken`, as for a create).

**Claimed first.** A change that adds tables is kept on the materialization as its
`pending_topic_change` and claims the new names — releasing the names of tables it drops in the same
request — before anything else happens; once `tables_claimed` arrives it applies as planned, and a
`claim_rejected` drops it, leaving the materialization as it was, with the reason in its status
message (07 §7.9b). Only one waits at a time: another change of topics meanwhile is `409 conflict`,
`reason: topic_change_pending`. The answer then carries `claim_request_id` and `claim_status_url`,
as a create's does. A change that adds nothing applies at once, releasing dropped tables' names.

`404` and `409 conflict` (`materialization_deleting`, `runtime_not_deployed` - also while tables an
earlier change added are still being created) as for stop. The response is the usual 202 acceptance
and, for a change of topics, what it does to each table:

```json
{ "materialization_id": "mat-a1b2c3d4e5f6", "request_id": "req-…", "desired_phase": "running", …,
  "name": "Shop",
  "topic_change": {
    "tables":  [{"name": "orders", "source_topic": "orders", "dataset": "sales", "dataset_id": "ds-5c1e…"},
                {"name": "refunds", "source_topic": "refunds", "dataset": "sales", "dataset_id": "ds-5c1e…"}],
    "added":   [{"name": "refunds", "source_topic": "refunds", "dataset": "sales", "dataset_id": "ds-5c1e…"}],
    "resumed": [],
    "kept":    [{"name": "payments", "source_topic": "payments", "dataset": "finance", "dataset_id": "ds-9a0b…"}],
    "dropped": [] },
  "claim_request_id": "idr-…", "claim_status_url": "/identity/requests/idr-…" }
```

`spec_version` increments once per request that changes something the runtime runs with - replicas,
topics or both - and a redelivered request does not increment it twice; a new label leaves it alone,
as does turning autoscaling on or off.
The answer carries `name` as it now is, and `live_tail: true`. A change of topics holds the runtime at zero
members until its consumer group has emptied, about two minutes, then starts it again; new topics are
sampled and their tables created first (07 §7.9a). Repeating the topics as they are changes nothing.

---

## 4.9 `DELETE /materializations/{id}`

Sets `desired_phase = "deleting"` and emits `action = "delete"` → work step `delete_runtime`. Its
tables leave their datasets, so the caller needs `dataset.write` on every dataset it has tables in
(written or kept): otherwise `403`, naming the first. Request intake releases every name it holds at
once (18 §18.16), so the names are free to use again while its files are still being deleted.

```bash
curl -sS -X DELETE localhost:6065/materializations/mat-a1b2c3d4e5f6 \
     -H "authorization: Bearer $TOKEN"
```

### Response `202`

```json
{
  "materialization_id": "mat-a1b2c3d4e5f6",
  "request_id": "req-7f8e9d0c1b2a",
  "desired_phase": "deleting",
  "observed_phase": "running",
  "phase_reason": "processing",
  "status_url": "/materializations/mat-a1b2c3d4e5f6",
  "accepted_at": "2026-01-15T12:00:00Z"
}
```

Delete is **asynchronous and multi-stage**:

1. `delete_runtime` — removes the eight Kubernetes resources in a fixed order.
2. Observation confirms the KasprApp is gone → `observed_phase = "deleted"`,
   `phase_reason = "resource_missing_after_delete"`.
3. `cleanup_runtime_kafka` — deletes the consumer group and all topics prefixed with the
   runtime name. Retries with backoff while `ConsumerGroupNotEmptyError` (error code 68).
4. `drop_table` — deletes the table's every object, in bounded passes, the pointer last; with
   `destination_cleanup_enabled` off it only retires the pointer, leaving the table readable by
   path. A large table can take a while; the API keeps answering throughout, and the materialization
   reads `deleted` meanwhile.
5. `purge_materialization_records` — removes its control-plane records (unless
   `purge_records_on_delete` is off), after which it leaves `GET /materializations`.

See [07 §7.10](./07-control-plane-workflows.md).

If the materialization does not exist → `404 not_found`.

---

## 4.9.1 `GET /topics`

Every topic in the Kafka cluster, for choosing a source topic. One cluster-wide metadata request
(aiokafka `describe_topics(None)`) on the metadata client shared with topic analysis, cached per
control-plane process.

### Query parameters

| Param | Effect |
|-------|--------|
| `refresh=true` | Reload instead of answering from the cache. Still spaced: no load starts within `topic_catalog_min_refresh_seconds` of the previous one |

### Response `200`

```json
{
  "items": [
    { "topic": "__consumer_offsets", "partitions": 50, "replication_factor": 2, "category": "kafka" },
    { "topic": "midstream-materialization-work", "partitions": 3, "replication_factor": 1, "category": "materializer" },
    { "topic": "joiner-app-orders-changelog", "partitions": 3, "replication_factor": 1, "category": "stream" },
    { "topic": "orders", "partitions": 3, "replication_factor": 1 }
  ],
  "returned": 4,
  "fetched_at": "2026-10-06T23:30:00Z"
}
```

- Sorted by name. `category` is omitted for an ordinary data topic.
- When a reload fails but an earlier catalog exists, the earlier one is returned with
  `"stale": true` and the `"error"` that prevented the reload.
- A topic this client may not describe (`TOPIC_AUTHORIZATION_FAILED`) is left out; a topic whose
  leader is moving is kept with whatever partitions the broker reported.

### Categories

`services/topic_catalog.py::classify_topic`. A category only lets a client hide a topic by
default; every topic stays selectable.

| `category` | Rule |
|------------|------|
| `kafka` | Kafka's own `is_internal` flag, or a `__` prefix |
| `materializer` | One of the five configured control-plane topics; a topic prefixed with this app's `K_APP_NAME`; or a runtime topic, `<runtime_name_prefix>-mat-<12 hex>-…` |
| `stream` | Ends in `-changelog`, `-repartition`, `-assignor-leader`, `-__assignor-__leader`, or `-schedule-{actions,requests,rejections}` |

### Load on Kafka

`TopicCatalogCache` bounds how many metadata requests clients can cause, whatever they do:

- concurrent requests share one in-flight load;
- a loaded catalog is reused for `topic_catalog_ttl_seconds` (15);
- after any attempt, forced or failed, no load starts for `topic_catalog_min_refresh_seconds` (5),
  so an outage is answered from memory rather than with a wait per request;
- a load is abandoned after 10 seconds (the admin client's own timeout is 40).

The load runs on the metadata client's event loop and is awaited through `asyncio.wrap_future`,
so the WebView loop keeps serving while Kafka answers; an abandoned load is cancelled on the
client's loop too.

### Response `503`

No catalog has ever loaded and Kafka cannot be asked:

```json
{ "error": { "code": "unavailable",
             "message": "Kafka topics cannot be listed right now",
             "details": { "reason": "Kafka did not answer within 10s" } } }
```

## 4.9.1a `POST /topics/analyze`

Checks that topics exist and recommends the replicas to materialize each, from its partition count
(the storage every member gets is the deployment's, §4.5.2). One metadata request describes every topic, then the
beginning and end offsets of their partitions are read, on the metadata client shared with
`GET /topics` (`web/topics.py::analyze_topics_view_async`).

### Request body

```json
{ "topics": ["orders", "ghost"] }
```

`topics` is a list of names or one comma-separated string; `topic` is accepted too. Blank names are
dropped and repeats collapsed; at most `topic_analysis_max_topics` (20) may remain.

### Response `200`

```json
{
  "items": [
    {
      "topic": "orders",
      "exists": true,
      "partitions": 6,
      "estimated_records": 15000000,
      "end_offsets": { "0": 2501000, "1": 2501000, "2": 2501000, "3": 2501000, "4": 2501000, "5": 2501000 },
      "recommendation": {
        "replicas": 3,
        "rationale": "6 partition(s) -> 3 replica(s) (1 replica per 2 partitions above 4 partitions)"
      }
    },
    { "topic": "ghost", "exists": false, "partitions": 0, "error": "topic does not exist" }
  ],
  "requested": 2,
  "valid": 1
}
```

- Items follow the request's order. `valid` counts the topics that exist.
- `estimated_records` sums each partition's end offset minus its beginning offset — an over-count
  on a compacted topic. It is reported, not used: before 4.0 it sized the storage.
- A topic that does not exist, or that Kafka reports an error for, has `exists: false`, an `error`
  and no `recommendation`; it does not fail the request.
- With more than one existing topic the answer also carries `combined`: the replicas for **one
  runtime reading all of them**, as a materialization of several topics does
  (`scale.recommend_combined_scale`). Replicas follow the partitions of every topic together, but no
  more than the members the assignor can keep busy: topics with the same partition count are read by
  the same members, so 32, 32 and 8 partitions keep at most 40 members busy (10 §10.1).

### Errors

| Status | When | `details` |
|---|---|---|
| `400 invalid_request` | The body is not an object; it has neither `topics` nor `topic`; it names no topic, or more than the maximum. Kafka is not asked | `{"field": "topics"}`, or `{}` for a body that is not an object |
| `503 unavailable` | Kafka failed, or did not answer within 10 s | `{"reason": <the error>}` |

```json
{ "error": { "code": "unavailable",
             "message": "Kafka topics cannot be analyzed right now",
             "details": { "reason": "Kafka did not answer within 10s" } } }
```

### Waiting on Kafka

The view awaits the metadata client, whose requests run on that client's own event loop, so the
API keeps serving other requests while Kafka answers. An analysis waits at most 10 s
(`services/topic_analysis.py::ANALYZE_TIMEOUT_SECONDS`, as long as `GET /topics` waits); past that
its request is cancelled on the client's loop and the view answers `503`. The client's own deadline,
30 s (`kafka/metadata.py::DEFAULT_REQUEST_TIMEOUT_SECONDS`), bounds its other callers, the
consumer-lag refresh among them.

Earlier releases asked Kafka synchronously on the event loop that serves the API, with no deadline:
every analysis held all other requests until Kafka answered, and a Kafka failure was a `500`.

## 4.9.2 `GET /system/settings`

What a new materialization takes: the deployment's destination, namespace and storage, which a
create request cannot choose (§4.5.2), and the defaults of what it can. A client shows them rather
than guessing.

```json
{
  "default_destination_uri": "gs://my-lake",
  "default_namespace": "midstream",
  "default_replicas": 1,
  "max_replicas": 64,
  "max_topics": 20,
  "default_storage_size": "64Mi",
  "default_partition_granularity": "day",
  "live_tail_enabled": true,
  "live_tail_default": true,
  "autoscaling_enabled": true,
  "autoscale_max_replicas": 16,
  "autoscale_target_minutes": 30,
  "label_max_length": 128,
  "dataset_name_max_length": 63,
  "table_name_max_length": 63,
  "reserved_names": ["all", "alter", "analyse", "…"],
  "reserved_namespace_names": ["information_schema", "main", "…"]
}
```

`default_destination_uri` is `null` when none is configured, and no materialization can then be
created (`409`, `destination_not_configured`). `reserved_names` is the full lower-case list a name cannot be, and `reserved_namespace_names`
the names a dataset cannot take besides (§4.5.1). `max_topics` is `max_materialization_topics`, the
most topics one materialization reads; `table_name_max_length` the longest name a topic's table is
given — with `reserved_names`, what a client needs to show the table names a create will produce.
`live_tail_enabled` says whether this deployment serves live tails at all — every materialization's
when it does (19). `live_tail_default` is always `true`, for a 4.1 console's create dialog.
`autoscaling_enabled` says whether the control plane scales runtimes that are far behind at all,
`autoscale_max_replicas` the most members it gives one, and `autoscale_target_minutes` the catch-up
time it scales for (07 §7.12a); a create may turn autoscaling off for its own materialization (§4.5).

The values are the **effective** ones: an override chosen on the console's Settings page wins
over the environment variable (§4.9.3).

## 4.9.3 Settings

Three scopes: `control-plane`, `runtime` and `console`. Any other scope answers `404` with
`details.resource = "settings-scope"`. What each scope holds, and the precedence
*override → deployment value → default*, is described in [17 — Settings](./17-settings.md).

### `GET /settings/{scope}`

For `control-plane` and `runtime`, every overridable setting with its definition and where its
value comes from, plus the deployment's read-only settings:

```json
{
  "scope": "control-plane",
  "revision": 13,
  "updated_at": "2026-10-09T13:15:51Z",
  "updated_by": "jane",
  "recent_request_ids": ["set-45f6da8fef2e", "…"],
  "last_rejection": null,
  "lineage": null,
  "settings": [
    {
      "key": "max_runtime_replicas",
      "scope": "control-plane",
      "section": "materializations",
      "label": "Maximum replicas",
      "description": "The most replicas a create or an edit may ask for. …",
      "kind": "integer",
      "unit": "replicas",
      "minimum": 1,
      "maximum": 1024,
      "nullable": false,
      "advanced": false,
      "danger": false,
      "effect": "new creates and edits",
      "env_var": "CONTROL_PLANE_MAX_RUNTIME_REPLICAS",
      "value": 16,
      "source": "deployment",
      "default_value": 64,
      "deployment_value": 16,
      "deployment_set": true,
      "overridden": false,
      "updated_at": null,
      "updated_by": null
    }
  ],
  "deployment": [
    {
      "key": "default_namespace",
      "component": "control-plane",
      "label": "Runtime namespace",
      "description": "Namespace new runtimes deploy into. …",
      "env_var": "CONTROL_PLANE_DEFAULT_NAMESPACE",
      "value": "midstream"
    }
  ]
}
```

| Field | Meaning |
|---|---|
| `revision` | Applied changes to this scope; `0` means it was never changed. Send it back as `base_revision` |
| `recent_request_ids` | The last 20 requests applied, oldest first. A client that sent a change waits for its `request_id` to appear here |
| `last_rejection` | `{request_id, reason, message, rejected_at}` of the last change the agent refused, or `null` |
| `lineage` | `runtime` only: the history its revisions belong to, begun at the control plane's first start (17 §17.7); `null` for the other scopes |
| `value`, `source` | The value in force, and whether it is an `override`, the `deployment` value, or the `default` |
| `deployment_set`, `deployment_value` | Whether the deployment's environment sets this, and to what. A reset returns to this, else to `default_value` |
| `kind` | `integer`, `boolean`, `gcs_uri` or `quantity`, with `minimum`/`maximum` and `nullable` where they apply |
| `effect` | What the change applies to, e.g. `new materializations`; `null` when it applies at once |
| `advanced`, `danger` | Rarely changed; changing it can remove data |

`runtime` adds `rollout`, how far the current revision of the current lineage has reached the
runtimes:

```json
"rollout": {
  "revision": 1, "total": 5,
  "current": 2, "pending": 0, "stopped": 1, "not_reporting": 1, "unsupported": 1,
  "materializations": [
    { "materialization_id": "mat-…", "name": "legacy_events", "state": "unsupported", "revision": null, "members": 1 }
  ]
}
```

An item's `revision` is the lowest its members run, `null` when a member runs another lineage's
settings (`pending`) or none were ever reported (17 §17.7).

`console` returns the stored overrides only, as `values: { <key>: { value, updated_at, updated_by } }`.
The console describes and validates its own settings.

### `PATCH /settings/{scope}`

```json
{ "values": { "max_runtime_replicas": 32 }, "reset": ["default_destination_uri"], "base_revision": 13 }
```

| Field | Notes |
|---|---|
| `values` | Settings to override, by key. `null` is allowed only for a nullable setting (no default destination) |
| `reset` | Settings to return to their deployment value or default. A key may not be in both |
| `base_revision` | Optional. The revision the client last read; a stale one is refused with `409`. Clients should send it |
| `fallbacks` | `console` only: what each changed setting reads as when not overridden, for the history |

`202`:

```json
{ "scope": "control-plane", "request_id": "set-1ce02c047587", "base_revision": 13,
  "accepted_at": "2026-10-12T23:18:51Z", "status_url": "/settings/control-plane" }
```

The change applies when the `settings-intake` agent processes it, usually well under a second.
Poll `status_url`: the change has applied once `request_id` appears in `recent_request_ids`, and
was refused if it is `last_rejection.request_id`. The agent refuses a change it finds stale
(`settings_changed`) or no longer valid together with the stored values (`invalid_setting`).
A change that alters nothing applies without a new revision.

| Status | `code` | When |
|---|---|---|
| `400` | `invalid_request` | Unknown key, a value of the wrong type or out of bounds, a cross-field rule (`details.field` names the setting), a key both set and reset, or nothing to change |
| `404` | `not_found` | Unknown scope |
| `409` | `conflict` | `base_revision` is not the current revision; `details.reason = "settings_changed"` |

```json
{ "error": { "code": "invalid_request",
             "message": "Default replicas (20) cannot exceed maximum replicas (16)",
             "details": { "field": "default_runtime_replicas" } } }
```

### `GET /settings/{scope}/history`

The scope's applied changes, newest first. `limit` is a positive integer or `all` (the default).
The newest 100 per scope are kept.

```json
{
  "scope": "control-plane",
  "changes": [
    {
      "scope": "control-plane", "revision": 13, "request_id": "set-45f6da8fef2e",
      "updated_at": "2026-10-09T13:15:51Z", "updated_by": "jane",
      "changes": [
        { "key": "schema_proposal_debounce_seconds", "before": 5, "after": 10,
          "before_source": "default", "after_source": "override" }
      ]
    }
  ],
  "returned": 1,
  "retained": 100
}
```

## 4.10 Error contract

All errors are raised as `WebError`, a frozen dataclass that subclasses `Exception`:

```python
# code-derived (web/errors.py)
@dataclass(frozen=True, slots=True)
class WebError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    status_code: int = 400

    def to_response(self) -> dict[str, Any]:
        return {"error": {"code": self.code, "message": self.message, "details": dict(self.details)}}
```

### Error catalogue

| Constructor | `code` | `status_code` | `details` |
|-------------|--------|---------------|-----------|
| `invalid_request(msg, field_name=...)` | `invalid_request` | `400` | `{"field": <name>}` or `{}` |
| `not_found(msg, resource=..., identifier=...)` | `not_found` | `404` | `{"resource": ..., "id": ...}` |
| `already_exists(msg, resource=..., identifier=..., field_name=...)` | `already_exists` | `409` | `{"resource": ..., "id": ...}`, plus `"field"` when given |
| `conflict(msg, reason=...)` | `conflict` | `409` | `{"reason": ...}` — the materialization's state does not allow the request, or (`settings_changed`) the settings changed since the client read them |
| `unavailable(msg, reason=...)` | `unavailable` | `503` | `{"reason": ...}` |
| `unauthenticated(msg, reason=...)` | `unauthenticated` | `401` | `{"reason": ...}`: `sign_in_required` (a guest asked for more than guests may do), `invalid_session`, `session_expired`, `session_ended`, `account_deactivated`, `sign_in_unavailable`, `invalid_credentials` |
| `forbidden(msg, reason=..., permission=...)` | `forbidden` | `403` | `{"reason": "forbidden", "permission": <key>}` when a signed-in person lacks a permission; `{"reason": "password_change_required"}` until they replace a password someone else chose; also directory changes the agent would refuse for lack of access |
| `too_many_requests(msg, reason=...)` | `too_many_attempts` | `429` | `{"reason": "busy"}` while too many passwords wait to be hashed. A locked-out sign-in answers the same code with `reason: locked_out` and `retry_after` |

Examples:

```json
{ "error": { "code": "invalid_request",
             "message": "source_topics is required: name at least one topic",
             "details": { "field": "source_topics" } } }
```

```json
{ "error": { "code": "invalid_request",
             "message": "runtime_mode must be one of: dedicated, shared",
             "details": { "field": "runtime_mode" } } }
```

```json
{ "error": { "code": "already_exists",
             "message": "materialization mat-a1b2c3d4e5f6 already exists",
             "details": { "resource": "materialization", "id": "mat-a1b2c3d4e5f6" } } }
```

```json
{ "error": { "code": "forbidden",
             "message": "You do not have permission to do this: it needs “Delete materializations”.",
             "details": { "reason": "forbidden", "permission": "materializations.delete" } } }
```

### Status codes

The WebView templates define no `onError` selector, and need none: when a handler raises, Kaspr
builds the response from the exception itself, taking `status_code` for the status and
`to_response()` for the body. A `WebError` therefore surfaces with its own status. Verified on
the dev cluster on 2026-10-06: `GET /materializations/mat-doesnotexist00` returned `404` with the
`not_found` body above.

## 4.11 Polling pattern for clients

```bash
# 1. Create
ID=$(curl -sS -X POST localhost:6065/materializations \
       -H 'Content-Type: application/json' -H "authorization: Bearer $TOKEN" \
       -d '{"name":"shop","tables":[{"source_topic":"orders","dataset":"sales"},{"source_topic":"payments","dataset":"finance"}]}' \
     | python -c 'import json,sys; print(json.load(sys.stdin)["materialization_id"])')

# 2. Poll until observed_phase == running (expect ~30-90s: discovery + deploy + first heartbeat)
until curl -sS "localhost:6065/materializations/$ID" -H "authorization: Bearer $TOKEN" \
      | python -c 'import json,sys; s=json.load(sys.stdin)["status"]; print(s["observed_phase"], s["phase_reason"]); exit(0 if s["observed_phase"]=="running" else 1)'
do sleep 5; done
```

Expected phase progression:

```
requested → discovering → discovered → deploying → running
```

with `phase_reason` moving through
`requested` → `discovering` → `schema_discovered` → `runtime_apply_in_progress` →
`waiting_for_runtime_ready` → `waiting_for_seed_schema` →
`waiting_for_source_records` → `processing`.

If it stalls, see [08 — Observation and status](./08-observation-and-status.md) and
[12 — Operations runbook](./12-operations-runbook.md).

## 4.12 Not implemented

There is no endpoint for:

- Changing any setting but `replicas` (`PATCH` refuses the rest; the name never changes).
- Changing the schema: the runtime evolves it, and an engine with `catalog.write` can (14 §14.2).
- Forcing an immediate observation or reconcile.
- Reading rows: engines read the table (§4.14, 11 §11.9); the console reads it itself.
- Rewinding or seeking the source consumer.
- Any bulk operation.

These are the most obvious extension points — see
[14 — Known gaps and roadmap](./14-known-gaps-and-roadmap.md).

## 4.13 Identity and access

The model, the rules and the storage behind these endpoints are in
[18 — Identity and access](./18-identity-and-access.md). Directory changes follow the same
accept-then-apply pattern as materialization mutations: the view validates the change, runs the
agent's own checks against the directory as it stands (refusing at once with the error the agent
would give), publishes it keyed `identity`, and answers `202`. Sign-in, renewal and sign-out answer
at once.

### 4.13.1 `POST /auth/login`

```bash
TOKEN=$(curl -s -X POST localhost:6065/auth/login -H 'content-type: application/json' \
  -d '{"email":"jane@example.com","password":"…"}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
```

Response `200`, the session payload every auth view shares (§4.13.2) plus the token:

```json
{
  "authenticated": true,
  "principal": {
    "kind": "user",
    "user_id": "usr-3f9c2a1b7d4e",
    "email": "jane@example.com",
    "first_name": "Jane",
    "last_name": "Doe",
    "display_name": "Jane Doe",
    "group_ids": ["guest", "all-users", "editor"],
    "role_ids": ["status-viewer", "viewer", "data-reader", "operator"],
    "permissions": ["data.export", "data.query", "materializations.create", "materializations.delete",
                    "materializations.edit", "materializations.resume", "materializations.stop",
                    "materializations.view", "materializations.view_details",
                    "materializations.view_schema", "topics.view"],
    "all_permissions": false,
    "password_change_required": false
  },
  "session": {
    "session_id": "ses-8c1e0f2a9b3d4c5e",
    "method": "password",
    "signed_in_at": "2026-10-14T09:00:00Z",
    "issued_at": "2026-10-14T09:00:00Z",
    "expires_at": "2026-10-14T17:00:00Z",
    "session_expires_at": "2026-10-21T09:00:00Z",
    "refresh_after": "2026-10-14T09:10:00Z"
  },
  "providers": [{ "id": "password", "type": "password", "label": "Email and password" }],
  "password_policy": { "min_length": 12, "max_length": 256 },
  "token": "v1.eyJhbXIiOiJwYXNzd29yZCIs….3q2-7w",
  "token_type": "Bearer"
}
```

| Status | `details.reason` | When |
|---|---|---|
| `400 invalid_request` | `field: email \| password` | Missing email or password |
| `401 unauthenticated` | `invalid_credentials` | Wrong email or wrong password: the same message for both, "Email or password is incorrect." |
| `403 forbidden` | `account_deactivated` | The right password for a deactivated account |
| `403 forbidden` | `password_sign_in_disabled` | The right password, but password sign-in is off here and the account is not an administrator's |
| `429 too_many_attempts` | `locked_out` (+ `retry_after` seconds), `busy` | Too many failed attempts for this email; too many sign-ins at once |
| `503 unavailable` | `sign_in_unavailable` | The control plane has no signing key |

Emits `sign_in_succeeded` or `sign_in_failed` (§4.13.5).

**Through an identity provider** (18 §18.17; the shared console sends it, 20 §20.7):

```json
{"provider": "oidc", "credential": {"id_token": "eyJhbGciOiJSUzI1NiIs…"}, "nonce": "c1f…"}
{"provider": "github", "credential": {"access_token": "gho_…"}, "nonce": null}
```

`200` as above, `session.method` `sso:<provider>`; the first sign-in of someone the domain has never
seen answers once their account exists. Refused with `400 invalid_request` (an unknown provider),
`401 invalid_credential`, `403 provider_disabled`, `email_not_allowed`, `email_unverified`,
`no_account`, `account_deactivated`, `409` (the account could not be made), `503
provider_unreachable` or `provisioning`. Emits `sign_in_succeeded`, `link_identity` or
`provision_user`, or `sign_in_failed` with `method: sso:<provider>`.

### 4.13.2 Sessions: `GET /auth/session`, `POST /auth/refresh`, `POST /auth/logout`, `GET /auth/sessions`

`GET /auth/session` answers the payload above without a token, and never refuses: a caller
without a usable session is described as a guest, with `session_error` saying why
(`session_expired`, `session_ended`, `invalid_session`, `account_deactivated`) when it presented
one.

`POST /auth/refresh` answers a new token for the same session, its idle timeout restarted and
never past `session_expires_at`; `401` without a usable session.

`POST /auth/logout`, body `{"scope": "current" | "others" | "session", "session_id": "…"}`
(`session_id` only for `session`), answers `200 {"signed_out": true, "scope": …}`. A session that is
already unusable is reported signed out.

`GET /auth/sessions` lists the caller's recorded sessions, newest first:

```json
{
  "current_session_id": "ses-8c1e0f2a9b3d4c5e",
  "sessions": [
    { "session_id": "ses-8c1e0f2a9b3d4c5e", "method": "password",
      "created_at": "2026-10-14T09:00:00Z", "last_seen_at": "2026-10-14T09:40:00Z",
      "expires_at": "2026-10-21T09:00:00Z", "ip": "10.1.2.3",
      "user_agent": "Mozilla/5.0 (Macintosh; …) Chrome/141.0 Safari/537.36",
      "current": true, "active": true, "ended_at": null, "ended_reason": null }
  ]
}
```

`ended_reason` is one of `signed_out`, `signed_out_elsewhere`, `signed_out_everywhere`,
`signed_out_by_administrator`, `password_changed`, `password_reset`, `deactivated`.

`POST /auth/password`, body `{"current_password", "new_password"}`, answers `202` with
`{request_id, action: "change_password", entity_type: "user", entity_id, accepted_at, status_url}`.
`400` with `details.field` for a wrong current password or a new one the rules refuse. Once it
applies, every session of the caller ends, this one included.

A person whose password someone else chose may call only these five endpoints, plus
`GET /identity/requests/{id}`, until they change it; everything else answers
`403 password_change_required`.

### 4.13.3 Users, groups, roles

`GET /users?status=&sort=&q=`: `status` is `all` (default), `active`, `deactivated`, `locked` or
`password_change`; `sort` is `name` (default), `last_active` or `created`; `q` searches names and
emails.

```json
{
  "users": [
    {
      "user_id": "usr-3f9c2a1b7d4e", "email": "jane@example.com",
      "first_name": "Jane", "last_name": "Doe", "display_name": "Jane Doe",
      "status": "active",
      "groups": [{ "group_id": "editor", "name": "Editor" }],
      "must_change_password": false, "has_password": true, "locked_until": null,
      "last_sign_in_at": "2026-10-14T09:00:00Z", "last_active_at": "2026-10-14T09:40:00Z",
      "created_at": "2026-10-01T12:00:00Z", "created_by": "admin@example.com",
      "updated_at": "2026-10-01T12:00:00Z", "updated_by": "admin@example.com",
      "deactivated_at": null, "deactivated_by": null,
      "revision": 1, "administrator": false, "manageable": true, "self": false
    }
  ],
  "counts": { "all": 12, "active": 11, "deactivated": 1, "locked": 0, "password_change": 2 },
  "returned": 1,
  "can_manage": true
}
```

`groups` lists the groups the person was added to; Guest and All Users are implied. `manageable`
says whether the caller may change this account: they hold `users.manage` and every permission
the person holds.

`GET /users/{user_id}` adds `memberships` (every group including the automatic ones, with their
roles), `permissions` (`[{key, via: [{group_id, group, role_id, role}]}]`, so a page can say where
each permission comes from), `all_permissions`, `failed_sign_ins`, `sessions` and
`sign_in_methods`.

| Request | Body | Notes |
|---|---|---|
| `POST /users` | `{first_name, last_name, email, password, group_ids}` | The password must be changed at first sign-in |
| `PATCH /users/{id}` | any of `{first_name, last_name, email, group_ids, status, base_revision}` | `group_ids` is the full list; `status` is `active` or `deactivated` |
| `DELETE /users/{id}` | — | Only a deactivated account |
| `POST /users/{id}/password` | `{password}` | Ends their sessions; they must change it at next sign-in |
| `POST /users/{id}/sign-out` | `{session_id}` or nothing | One session, or every one |
| `POST /users/{id}/unlock` | — | Clears failed sign-ins for their email |
| `POST /groups` | `{name, description, role_ids, member_ids}` | |
| `PATCH /groups/{id}` | any of `{name, description, role_ids, add_member_ids, remove_member_ids, base_revision}` | `role_ids` is the full list |
| `DELETE /groups/{id}` | — | Not a built-in group |
| `POST /roles` | `{name, description, permissions}` | Keys from `GET /permissions`; never `*` |
| `PATCH /roles/{id}` | any of `{name, description, permissions, base_revision}` | Not a built-in role |
| `DELETE /roles/{id}` | — | Removes it from every group |

An unknown body field is refused with `400` naming it. Every change answers `202`:

```json
{
  "request_id": "idr-5b2d8e1f0a3c",
  "action": "create_user",
  "entity_type": "user",
  "entity_id": "usr-9d0e1f2a3b4c",
  "accepted_at": "2026-10-14T09:45:00Z",
  "status_url": "/identity/requests/idr-5b2d8e1f0a3c"
}
```

Refusals, at once or as the request's outcome: `400 invalid_request` (`invalid`), `403 forbidden`
(`forbidden`: the caller lacks the access the change involves), `404 not_found` (`not_found`),
`409 already_exists` (`already_exists`, with `details.field`), `409 conflict` with
`details.reason` `changed`, `self_action`, `last_administrator`, `built_in` or `locked_role`.

`GET /groups` answers `{groups, can_manage}`; each group has `group_id`, `name`, `description`,
`membership` (`assigned`, `authenticated`, `guests`), `built_in`, `locked_role_ids`, `role_ids`,
`role_names`, `member_count` (`null` for Guest), `administrators`, `revision`, `manageable` and
`grantable` (whether the caller holds everything it grants, so may add people to it).
`GET /groups/{id}` adds `roles`, `permissions`, `all_permissions` and `members` (`null` without
`users.view`).

`GET /roles` answers `{roles, can_manage}`; each role has `role_id`, `name`, `description`,
`built_in`, `permissions` (as stored, `*` for Administrator), `effective_permissions` (with
implications), `all_permissions`, `groups` and `revision`.

`GET /permissions` answers the catalog:

```json
{
  "categories": [{ "id": "users", "label": "User management" }, …],
  "permissions": [
    { "key": "users.view", "category": "users", "label": "View users page with emails",
      "description": "See every user with their email, groups, status and last activity.",
      "implies": [], "danger": false },
    …
  ]
}
```

### 4.13.4 `GET /audit` and `GET /identity/requests/{request_id}`

`GET /audit?limit=&before=&action=&outcome=&q=`: `limit` 1–200 (default 100); `before` is the
`next` cursor of the previous page; `action` is an exact action or a prefix ending in `.`, such as
`user.`; `outcome` is `success` or `failure`; `q` searches who did it and what it was done to.
Newest first, within `retention_days`:

```json
{
  "entries": [
    {
      "entry_id": "aud-065dc9c8f28bd27c2e", "occurred_at": "2026-10-14T09:45:01Z",
      "action": "user.created", "outcome": "success",
      "actor_id": "usr-1a2b3c4d5e6f", "actor_email": "admin@example.com",
      "target_type": "user", "target_id": "usr-9d0e1f2a3b4c",
      "target_label": "Ada Lovelace <ada@example.com>",
      "details": { "email": "ada@example.com", "name": "Ada Lovelace", "groups": ["Editor"] },
      "ip": "10.1.2.3", "user_agent": "Mozilla/5.0 …",
      "cursor": "audit:2026-10-14T09:45:01Z:aud-065dc9c8f28bd27c2e"
    }
  ],
  "returned": 1,
  "next": null,
  "retention_days": 90
}
```

Actions: `session.signed_in`, `session.sign_in_failed`, `session.locked_out`, `session.signed_out`;
`user.created`, `user.updated`, `user.deactivated`, `user.reactivated`, `user.deleted`,
`user.password_reset`, `user.password_changed`, `user.signed_out`, `user.session_ended`,
`user.unlocked`, `user.bootstrapped`, `user.bootstrap_restored`; `group.created`,
`group.updated`, `group.deleted`; `role.created`, `role.updated`, `role.deleted`;
`directory.seeded`.

`GET /identity/requests/{request_id}` answers `{"request_id", "status": "pending"}` until the
agent has applied or refused the request, then its outcome:

```json
{ "request_id": "idr-5b2d8e1f0a3c", "action": "create_user", "status": "rejected",
  "completed_at": "2026-10-14T09:45:01Z", "actor_id": "usr-1a2b3c4d5e6f",
  "entity_type": null, "entity_id": null,
  "reason": "already_exists", "message": "ada@example.com already has an account.", "field": "email" }
```

Only the person who sent the request, or someone with `users.view`, may read an outcome
(`403` otherwise). Outcomes are kept for a day.

### 4.13.6 Access tokens: `GET`, `POST /auth/tokens`, `DELETE /auth/tokens/{token_id}`, revocations

Credentials for tools — an engine reading the REST catalog (§4.14) — carrying some of their
owner's permissions, good in **every domain** where the owner has an account (18 §18.15, 20 §20.8).
`GET /auth/tokens` lists the tokens made in this domain for the caller (never their text), the
permissions a new one could carry, and this domain's datasets it could be limited to, named
`<domain>.<dataset>`.

```bash
curl -s -X POST localhost:6065/auth/tokens -H "authorization: Bearer $TOKEN" \
     -H 'content-type: application/json' \
     -d '{"name": "DuckDB on my laptop", "permissions": ["catalog.read"], "expires_in_days": 90, "datasets": ["domain_a.sales", "domain_b.finance"]}'
```

`202`, with the token itself — **shown once, never again**:

```json
{
  "request_id": "idr-…", "action": "create_access_token", "entity_type": "access_token",
  "entity_id": "5500b621f521", "accepted_at": "2026-10-26T20:00:00Z",
  "status_url": "/identity/requests/idr-…",
  "token": "tok_eyJleHAiOjE4…", "token_id": "5500b621f521", "name": "DuckDB on my laptop",
  "permissions": ["catalog.read"], "issuer": "domain_a",
  "datasets": [{"name": "domain_a.sales"}, {"name": "domain_b.finance"}],
  "expires_at": "2027-01-24T20:00:00Z"
}
```

`name` is required, up to 64 characters; `permissions` ⊆ `catalog.read`, `catalog.write` (default
`catalog.read`), each one the caller holds here, on the named datasets of this domain or on some
dataset here; `expires_in_days` 1–365 (default 90); `datasets`, optional, limits the token, as
`<domain>.<dataset>` (a bare name is this domain's) — at most 100. At most 20 live tokens per person
in a domain (`409 limit`). Without a federation key: `503 no_federation_key`. A token acts as its
owner with at most its permissions, on each dataset, in whichever domain answers — never more than
the owner holds there, nor outside its limits — and stops working when revoked, expired, or its owner
is deactivated or unknown there (`401 token_unknown_owner`).

`DELETE /auth/tokens/{token_id}` revokes one made here, the caller's or, with `users.manage`,
anyone's; the answer carries the signed revocation, `"revocation": {"jti", "exp", "signature"}`, for
the console to hand to the other domains. `POST /auth/tokens/revocations` with
`{"revocations": [{"jti", "exp", "signature"}], "source": "domain_a"}` (at most 100) records revocations
another domain signed — `403 invalid_signature` for any not signed with this domain's federation keys
— and `GET /auth/tokens/revocations?since=<instant>` lists the revocations held here, oldest first.
All changes go through `<release>-identity-requests` like any directory change, and the audit log
records them (`token.created`, `token.revoked`, `token.revocation_applied`).

### 4.13.5 The identity request topic

Every request is published to `<release>-identity-requests` with the key `identity`
([06](./06-event-and-topic-contracts.md) §6.15):

```json
{
  "request_id": "idr-5b2d8e1f0a3c",
  "action": "create_user",
  "submitted_at": "2026-10-14T09:45:00Z",
  "actor_id": "usr-1a2b3c4d5e6f",
  "actor_email": "admin@example.com",
  "payload": {
    "user_id": "usr-9d0e1f2a3b4c", "email": "ada@example.com",
    "first_name": "Ada", "last_name": "Lovelace", "group_ids": ["editor"],
    "password_hash": "$scrypt$ln=15,r=8,p=3$…$…"
  },
  "base_revision": null,
  "context": { "ip": "10.1.2.3", "user_agent": "Mozilla/5.0 …" }
}
```

A request never carries a password, only its hash.

## 4.14 The Iceberg REST catalog: `/catalog/v1/…`

The control plane serves the Iceberg REST catalog protocol, so engines attach the lake as a
catalog: catalog URI **`<control plane>/catalog`** (clients append `/v1/…` themselves), and a bearer
token — an access token (§4.13.6) or a session's. The full endpoint table, with what each answers,
is 11 §11.8; in short:

| Endpoint | Needs | Answer |
|---|---|---|
| `GET /catalog/v1/config` | `catalog.read` on a dataset | defaults and the endpoints served; no `prefix`, no vended credentials |
| `GET /catalog/v1/namespaces`, `GET /catalog/v1/namespaces/{ns}` | `catalog.read` on the dataset | one namespace per **dataset** the caller may read through the catalog; a locked one is not listed and answers `404`. A namespace's `properties` carry its `description` |
| `GET /catalog/v1/namespaces/{ns}/tables` | `catalog.read` on it | the dataset's tables, whichever materialization writes each |
| `GET /catalog/v1/namespaces/{ns}/tables/{table}` | `catalog.read` on it | the table's metadata location and metadata, resolved through its claim |
| `POST /catalog/v1/namespaces/{ns}/tables/{table}` | `catalog.write` on it | a commit, through the pointer's compare-and-swap; `409` when another landed first; `400` for `set-location` or a property that moves files (`write.data.path`, `write.metadata.path`, `write.py-location-provider.impl`) |
| `POST …/tables/{table}/metrics` | `catalog.read` on it | `204` |
| table creation, drop, multi-table transactions | — | `501`: tables are made and dropped with materializations; datasets with `POST /datasets` |

Errors use the Iceberg shape, `{"error": {"message", "type", "code"}}`, refused authorizations
included. With `catalog_api_enabled` off every endpoint answers `503`. `snapshot-id` values are
64-bit integers; a client must parse them without losing precision.

```sql
-- DuckDB: each dataset is a namespace; tables of any datasets and materializations join
CREATE SECRET materializer (TYPE ICEBERG, TOKEN 'tok_…');
ATTACH 'warehouse' AS lake (TYPE ICEBERG, SECRET materializer, ENDPOINT 'https://<control plane>/catalog');
SELECT count(*) FROM lake.sales.orders;
SELECT o.status, sum(i.amount) FROM lake.sales.orders o JOIN lake.finance.invoices i USING (order_id) GROUP BY 1;
```

The engine reads data files itself, with its own GCS access (an HMAC key for DuckDB's `gcs`
secret, application default credentials for PyIceberg).

## 4.15 Datasets: `/datasets`

A dataset is a named group of tables and their Iceberg namespace (18 §18.16). Paths name a dataset
by its name; its id (`ds-<12 hex>`) is what records refer to. Changes are directory requests: each
answers `202` with a `request_id`, and `GET /identity/requests/{request_id}` reports the outcome.

### `GET /datasets`

Every dataset, by name, locked ones included. `?q=` keeps those whose name, description or a table's
name contains it.

```json
{
  "datasets": [
    {
      "dataset_id": "ds-9a0b1c2d3e4f", "name": "finance", "description": "Invoices and payments",
      "revision": 1, "created_at": "…", "created_by": "olive@example.com", "updated_at": "…", "updated_by": "…",
      "owners": [{ "type": "user", "id": "usr-…", "name": "Olive Owner", "email": "olive@example.com" }],
      "tables": ["invoices"], "table_count": 1,
      "permissions": [], "locked": true
    }
  ],
  "returned": 1,
  "can_create": false
}
```

`owners` are the people and groups whose grants there include `dataset.manage`; `tables`, the names
claimed in it; `permissions`, the dataset permissions the caller holds on it; `locked`, that they may
not see its schemas (`dataset.view_schema`).

### `POST /datasets`

Body `{"name": "sales", "description": "Orders and refunds"}`. The name follows §4.5.1 and must be
free (`409 already_exists` at once); the description is optional, at most 500 characters. The
creator gets Dataset Owner on it, and All Users Dataset Reader.

### `GET /datasets/{dataset}`

The dataset as listed, with `tables` in full: each `{name, source_topic, state, materialization_id,
materialization_name, created_at, last_commit_at}`. `state` is `pending` while a claimed table is not
created yet, else the table record's (`active`, `retired`, `dropping`). `404` for an unknown dataset.

### `PATCH /datasets/{dataset}` and `DELETE /datasets/{dataset}`

`PATCH` body `{"description": "…", "base_revision": 2}`; a `name` other than its own is refused —
a dataset's name never changes. `DELETE` is refused `409 dataset_not_empty` while the dataset holds a
table — written, kept, or still being dropped — naming them; it deletes the dataset's grants and frees
its name.

### `GET /datasets/{dataset}/grants` and `PUT /datasets/{dataset}/grants/{principal_id}`

```json
{
  "dataset": "finance", "dataset_id": "ds-9a0b1c2d3e4f",
  "grants": [
    { "principal_type": "user", "principal_id": "usr-…", "name": "Olive Owner", "email": "olive@example.com",
      "role_ids": ["dataset-owner"], "roles": [{ "role_id": "dataset-owner", "name": "Dataset Owner", "built_in": true, "permissions": ["…"] }],
      "updated_at": "…", "updated_by": "…" }
  ],
  "roles": [{ "role_id": "dataset-reader", "name": "Dataset Reader", "built_in": true, "permissions": ["…"], "grantable": true }],
  "candidates": { "users": [{ "user_id": "usr-…", "name": "Rita Reader", "email": "rita@example.com" }],
                  "groups": [{ "group_id": "all-users", "name": "All Users" }] }
}
```

`roles` are the roles that can be granted on a dataset (dataset roles only), each `grantable` when
the caller holds all its permissions there. `candidates` are whom it can be shared with: the active
people and every group but Guest.

`PUT` body `{"role_ids": ["dataset-writer"], "principal_type": "user"}` sets the roles a person or a
group holds on the dataset; `[]` removes the grant. `principal_type` may be left out when the id is
unambiguous. Refused: Guest (`400`), a role that is not a dataset role (`400`), an unknown person or
group (`404`), a role the caller does not hold on the dataset (`403`).

### `GET /datasets/{dataset}/tables/{table}`

Resolves `<dataset>.<table>` to the table and the materialization that writes it:

```json
{
  "dataset": "sales", "dataset_id": "ds-5c1e…", "name": "orders", "qualified_name": "sales.orders",
  "source_topic": "sales.orders", "state": "active",
  "materialization_id": "mat-a1b2c3d4e5f6", "materialization_name": "Shop",
  "readable": true, "queryable": true,
  "location": "gs://…/develop/mat-a1b2c3d4e5f6/orders", "table_uuid": "…", "metadata_location": "…",
  "current_snapshot_id": 8410567672455764292, "schema_id": 3, "partition_granularity": "day",
  "last_commit_at": "…", "valid_through": "…", "totals": { "records": 152001, "files": 29, "bytes": 41875202 }
}
```

Where it lives and its metadata only for a caller who may read the dataset; `404` for a name nobody
holds there.
