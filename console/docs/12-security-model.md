# 12 — Security Model

An honest account of what the console defends against, what it does not, and where the real trust
boundaries are.

## 12.1 Trust boundaries

```
┌─────────────┐   untrusted   ┌──────────────────┐  person's session   ┌───────────────┐
│  Browser    │ ────────────► │  Console server  │ ──────────────────► │ Control plane │
│ (a guest,   │   cookie      │                  │  or service token   │  (authorizes  │
│  or someone │               │  who is calling  │                     │   every call) │
│  signed in) │               │  permit() checks │ ──────────────────► │  Object store │
└─────────────┘               │  validation      │                     └───────────────┘
                              │  sandbox         │
                              └──────────────────┘
```

Everything crossing the browser→server boundary is untrusted: path parameters, request bodies,
headers, and SQL. So is what a table names — its pointer, metadata and manifests are written by
whoever commits to it (§12.6). Everything the server sends upstream is constructed from validated
input. Who
may do what is decided by the control plane; the console asks it, enforces the answer on its own
routes, and sends every change with the person's own session so the control plane checks again.

## 12.2 Authentication and authorization

People sign in with an email and password; accounts, groups, roles and permissions live in the
control plane ([18 — Identity and access](../../docs/handbook/18-identity-and-access.md)). There is
no self-registration: an administrator adds people with an initial password they must change.

| Control | How |
|---------|-----|
| **Every route declares a permission** | `permit('<permission>')` or `signedIn()` on each `/api` route; a route with neither answers anyone (`/api/health`, `/api/config`, `/api/auth/session`, `/api/auth/login`, `/api/auth/logout`). A guest gets `401`, a person without the permission `403` |
| **Data permissions are checked per dataset** | A dataset permission (`dataset.view_schema`, `data.query`, `data.export`, `catalog.read`, …) is checked on the datasets a request is about: every table a statement names, the table a schema is of, the datasets a run read (for its rows, stream and export, again on every request). The route's guard, `permitAnywhere(permission)`, turns away anyone holding it on no dataset; `requireOnDatasets` then refuses with `403`, `details.permission` and `details.dataset` (`auth/guard.ts`, the rule in `shared/access.ts`). A token limited to datasets holds no dataset permission on any other |
| **The control plane checks again** | Changes are sent with the person's own session, never the console's credential, so a bug in a console route cannot grant what the control plane refuses. The datasets routes are the control plane's to decide: the console only passes them through |
| **The session never reaches page scripts** | The control-plane token is kept in an `HttpOnly`, `SameSite=Lax` cookie, `Secure` behind TLS. XSS cannot read it |
| **Cross-site changes are refused** | `POST`/`PUT`/`PATCH`/`DELETE` need `Sec-Fetch-Site: same-origin` (or `none`), or else an `Origin` naming this host. With `SameSite=Lax`, this is the CSRF defence; no token is needed |
| **Revocation is prompt** | Session checks are cached for `CONSOLE_SESSION_CHECK_MS` (5 s), and the control plane checks every request against the current directory: deactivating someone or signing them out applies within seconds. Streams re-check every 30 s and close |
| **Sign-in is rate-limited** | `CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` per client address here, and the control plane locks an email after repeated wrong passwords whatever the address |
| **Shared views read with least privilege** | The live store polls with `CONSOLE_SERVICE_TOKEN`, which the control plane lets read, never change, on every dataset. Everything it serves is still filtered by each route's permission, and a materialization's detail by the caller's datasets: `visibleDetail` (`live/store.ts`) leaves out the schemas and schema errors of tables whose datasets they may not see the schemas of, and the Table tab's record leaves out the fork map and `schema.*` properties — on the detail route and on every refresh of the detail stream |
| **Runs are private** | A query run belongs to whoever started it; anyone else gets `404`. A statement's sandbox reaches only its own run's directory (§12.4) |
| **The live tail is asked as the person** | A run's tail, a refresh and the head peek call the tail app with the person's own session — the same token the console sends the control plane — and with the console's service token in `X-Materializer-Console`, which the tail app requires on every view (handbook 19 §19.10). The tail app checks the session with the control plane and `data.query` on the table's dataset itself, so a console bug cannot widen what a person reads. The console refuses first: a run, a refresh and a peek need `data.query` on the table's dataset here too, so a person without it causes no tail request at all. The tail's rows land in the run's own directory and nowhere else |
| **Access tokens are shown once** | `POST /api/auth/tokens` answers a token's text once, `cache-control: no-store`; the console never stores it and the control plane keeps only its hash. Listings never carry secrets |
| **No secrets in the browser** | `localStorage` holds the theme and pane sizes; the session is the cookie |

Guests are real callers: by default they may see the materialization list and status. An
administrator can give the Guest group less, or more, from the Groups page.

**What still belongs to the deployment.** Serve the console over TLS (the cookie is `Secure`
only when the request arrived over HTTPS, `CONSOLE_COOKIE_SECURE=auto`), keep the control plane's
port off any public address, and treat the pod's ADC/HMAC credentials and the service token as
sensitive. Ingress authentication in front of the console is still a reasonable extra layer, but
no longer the only one.

**Limits worth knowing.** Data permissions are per dataset, not per table or per materialization:
someone who may query `sales` may query every table in it, whatever materialization writes it. What
is not data is not secret: anyone who may see materializations sees each one's topics, status, totals
and table names, locked datasets' included. Access is not storage access: the console's own
credential reads every table, and so does anyone holding credentials that read the bucket — GCS
enforces none of this (control-plane handbook 14). The per-address sign-in limit trusts the address
the ingress reports (`trustProxy`); a client that reaches the pod directly can choose its own. There
is no single sign-on yet, no "forgot password" (an administrator resets it), and no email.

## 12.3 Input validation

### Path parameters

```ts
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
```

Every `:materializationId` passes `assertSafeIdentifier` before it is used anywhere. The id reaches
two sinks:

| Sink | Attack blocked |
|------|----------------|
| Upstream URL path | Path injection into the control-plane API |
| An export's `content-disposition` filename | Header injection |

It no longer names a file or an object prefix: a table's objects are found from the location the
control plane recorded (§12.6). A run id must be a UUID (`/^[0-9a-f-]{36}$/i`) — it names the run's
directory — and a snapshot id decimal digits.

Verified: `GET /api/materializations/..%2F..%2Fetc%2Fpasswd` returns 400.

### Settings

A settings save is validated twice. The console checks its own settings against its catalog
(`settings/catalog.ts`): known keys only, each value of the right type and within bounds, strings
capped in length. The control plane validates its own two scopes. A stored console value is
checked again when it is read back, so a value written by another console version cannot reach
`config` unchecked. The save body is a Zod schema: at most one change per scope, values that are
strings of at most 2,048 characters, finite numbers, booleans or null, and keys of 1–64
characters.

`:version` must parse as an integer ≥ 1, else 400.

### Request bodies

Every POST body is parsed with Zod before reaching business logic. Beyond type checking, the
schemas bound size:

| Field | Bound |
|-------|-------|
| `tables` | 1–100 rows, each a Kafka topic name (≤ 249 chars of letters, digits, `.`, `_`, `-`), each topic once, with a dataset and an optional table name of ≤ 63 chars; the control plane caps the count lower (`max_materialization_topics`) and checks the names |
| `name` (a materialization's label) | ≤ 128 chars once trimmed, no control characters |
| `datasets` (an access token's) | ≤ 100 names |
| Any key a schema does not list | refused: the create and update schemas are strict, so a create naming the deployment's `destination_uri`, `namespace`, `runtime_name` or `storage_size` is a `400` |
| `labels` / `annotations` keys | ≤ 253 chars; values ≤ 1024 |
| `columns` | ≤ 200 entries |
| `filters` | ≤ 20 entries, values ≤ 4096 chars |
| `limit` | ≤ 10 000 |
| `offset` | ≤ 10 000 000 |
| `sql` | ≤ 20 000 chars |
| Whole body | ≤ 1 MiB (Fastify `bodyLimit`) |

Zod failures become a 400 naming the field. Unvalidated fields are dropped, not passed through.

### Header injection

The browser's address and user agent are forwarded to the control plane for its audit log
(`x-forwarded-for`, `x-forwarded-user-agent`). Both are cut to a safe charset — `[\w.:-]` for
the address, printable ASCII for the agent — and 256 characters before forwarding, so CR/LF
cannot split a header. The control plane records them as reported and never decides anything on
them.

## 12.4 The SQL guard

The console intentionally exposes a SQL console. Defence is layered rather than relying on any
single control.

### Layer 1 — the sandbox (the real control)

A statement runs in a **DuckDB instance of its own** (`data/duckdb.ts::withSandbox`), made for it
and closed after it:

| Setting | Effect |
|---|---|
| `allowed_paths` | the snapshot's data files in the run's time range — nothing else can be opened |
| `allowed_directories` | the run's own directory, `<CONSOLE_RUN_DIR>/<run id>/`, where its result is written |
| `enable_external_access = false` | every other file, URL and object is refused, however the statement names it |
| `lock_configuration = true` | the statement cannot change any of the above |

DuckDB enforces these instance-wide and will not undo them (E10). The instance holds two views of
each table the statement names, both over exactly that table's files, read by field id: the table's
name with logical columns, and `iceberg.<name>`, in a schema of the sandbox's own, with the native
Iceberg columns. Only the materialization's own tables are there, and only those the statement
names: no other materialization's data, no database file. Validating a statement while it is
typed binds it in a sandbox allowed nothing at all, over empty views.

Browse runs, profiles and exports run only SQL the console writes, on the shared instance.

### Layer 2 — statement validation

```
1. single statement (semicolons outside string literals are rejected)
2. must start with SELECT | WITH | DESCRIBE | SUMMARIZE | PIVOT | UNPIVOT | FROM | TABLE | VALUES
3. must not reference a forbidden token
4. ≤ 20 000 characters
```

Forbidden tokens cover both statement kinds and file-access functions:

```
attach detach copy export import install load create drop alter insert update delete truncate
call pragma set reset checkpoint vacuum analyze begin commit rollback grant revoke
read_text read_blob read_csv read_json read_parquet read_avro avro_scan
iceberg_scan iceberg_metadata iceberg_snapshots iceberg_table_properties glob sniff_csv shell system
```

String literals and comments are blanked before scanning, so:

| Statement | Result |
|-----------|--------|
| `SELECT * FROM orders WHERE x = 'a; DROP TABLE y'` | **allowed** — the semicolon is data |
| `SELECT 1; DROP TABLE orders` | rejected — real statement separator |
| `SELECT /* copy */ 1 FROM orders` | allowed — comment stripped |
| `SELECT * FROM read_csv('/etc/passwd')` | rejected — forbidden function |

This layer is a keyword scanner, not a parser, and it can be dodged: `"read_csv"('/etc/hosts')`
(quoted identifiers are blanked), `read_csv_auto(…)`, `parquet_scan('gs://…')`. It exists to refuse
the obvious with a clear message; the sandbox is what holds, and refuses each of those with a
permission error (`server/runs/table.test.ts`, E10). Neither a materialization nor a table can be
named after a forbidden token: a table takes its topic's name by the same rules, and a topic named
after one (`update`) makes the table `update_topic` (`shared/tableNames.ts`, the control plane's
`table_names.py`). So the guard never refuses a statement for naming one of its own tables.

### Layer 3 — resource limits

| Limit | Default |
|-------|---------|
| Rows one page returns | 1,000 (`CONSOLE_PAGE_ROW_LIMIT`) |
| Rows a statement's result keeps | 1,000,000 (`CONSOLE_RESULT_ROW_LIMIT`); its total is still counted in full |
| Rows one export contains | 1,000,000 (`CONSOLE_EXPORT_ROW_LIMIT`) |
| Runs scanning at once | 4 (`CONSOLE_RUN_CONCURRENCY`); more queue |
| Wall clock | None while someone is watching — **Stop** interrupts every query a run has open. A run nobody has read for 30 min (`CONSOLE_RUN_RETENTION_MS`) is cancelled and its stored result deleted |
| DuckDB memory | 4 GB per instance (`CONSOLE_DUCKDB_MEMORY_LIMIT`) |
| DuckDB threads | 32 (`CONSOLE_DUCKDB_THREADS`) |
| Cached-mode download | 400 files / 2 GiB per scan |

### Honest limitations

- **A user can read every table in a dataset they may query** — including every payload column
  and all Kafka metadata. There is no column-level or row-level authorization: `data.query` on a
  dataset reads all of each of its tables.
- **Expensive queries are possible** within the limits: a cross join can hold CPU and up to
  `CONSOLE_DUCKDB_MEMORY_LIMIT` of memory until someone cancels it, or nobody has watched it for 30
  minutes. Each concurrent statement is an instance of its own, so the memory limit applies per
  statement, up to `CONSOLE_RUN_CONCURRENCY` of them.
- **The sandbox's own directory was once shared.** As first built, every statement's sandbox was
  allowed the whole run directory, where every run's result is stored, so a statement could read
  another person's result by naming it. Each run now has a directory of its own (E10).

If you extend the guard, keep the layering: never widen the sandbox to make validation simpler.

## 12.5 Injection in generated SQL

Browse-mode filters never interpolate raw user input:

```ts
export function quoteIdentifier(value: string): string { return `"${value.replace(/"/g, '""')}"`; }
export function quoteLiteral(value: string): string    { return `'${value.replace(/'/g, "''")}'`; }
```

Plus, before quoting, column names must match `^[A-Za-z_][A-Za-z0-9_]{0,255}$`. So a column name
is validated *and* quoted, and a value is quoted. `LIKE` patterns additionally escape `\`, `%`, and
`_` with an explicit `ESCAPE '\'`.

`LIMIT` and `OFFSET` are numeric and clamped, never string-interpolated from raw input.

## 12.6 Paths a table names

A table's pointer names its metadata file, the metadata names manifest lists, and manifests name
data files. Whoever may commit to the table chooses those names — an engine with `catalog.write`,
or anyone who can write the bucket — so the console checks every one against the location the
control plane recorded for the materialization (`iceberg/io.ts::isUnderLocation`):

| Named by the table | Outside the location |
|---|---|
| The metadata file, and the metadata's own `location` | the table is not read: `503`, saying what it named |
| A manifest list, or a manifest | the same, before the object is read |
| A data file | never listed or read; the snapshot counts it, and a run over it is refused (`400`) |

A path under the location must also not climb out of it: a `.` or `..` segment fails the check. So
a table can never make the console read another bucket, another table's files, or a file on the
console's own disk — and through it, show them to everyone who may query. The control plane's REST
catalog refuses the commits that would move a table's files in the first place: `set-location`,
and setting `write.data.path`, `write.metadata.path` or `write.py-location-provider.impl`
(handbook 11 §11.8).

In cached mode, `localPathFor` mirrors an object to `<CONSOLE_PARQUET_CACHE_DIR>/<bucket>/<name>`:

```ts
const segments = file.name.split('/')
  .filter((s) => s.length > 0 && s !== '.' && s !== '..')
  .map((s) => s.replace(/[\0]/g, ''));
```

Empty, `.`, and `..` segments are dropped and null bytes removed, so the join cannot escape the
cache directory even for a name that passed the location check. The bucket name is the first
segment, keeping buckets apart.

## 12.7 Outbound request safety

- Every control-plane request carries `AbortSignal.timeout(CONTROL_PLANE_TIMEOUT_MS)`, so a hung
  upstream cannot exhaust the event loop.
- `CONTROL_PLANE_URL` is operator-configured, never user-supplied — there is no SSRF surface.
- Ids are `encodeURIComponent`-escaped after passing the identifier pattern.
- Upstream status codes are relayed; upstream error bodies are relayed under
  `details.upstream_details` rather than being merged into the console's own error shape.

## 12.8 Information disclosure

| Channel | Control |
|---------|---------|
| Unexpected server errors | Fixed message `the console failed to handle this request`, empty details, 500. The real error is logged server-side only |
| A table that cannot be read | `503` naming what failed — storage's status code, or the missing object — never storage's own message, which can name the console's service account |
| `GET /api/config` | Exposes mode, row limits, poll interval and versions to anyone; the control plane's URL only with `settings.view`, the create defaults only with `materializations.create`. No credentials or budgets |
| Logs | `authorization` and `cookie` headers are removed; per-request logging is disabled |
| DuckDB errors | Relayed to the user (they are needed to fix a query) but stripped of the statement echo. They can reveal column names of the tables being queried — acceptable, since the user can already `DESCRIBE` them; a table in a dataset they may not query never reaches a sandbox |
| A table in a dataset locked to the caller | Its schema, schema errors, fork map and `schema.*` properties are left out of the detail, the detail stream and the table record. Its name, topic, counts, snapshots and maintenance stay: names are not secret, and totals are a materialization's status |
| Object storage credentials | Never leave the server; the browser never receives a signed URL or a bucket path it could not already infer |

## 12.9 Browser-side considerations

- No `dangerouslySetInnerHTML` anywhere. All rendering is through React's escaping.
- Cell values are rendered as text, including JSON-stringified objects.
- The only cookie is the session, `HttpOnly; SameSite=Lax`. Cross-site changes are refused by
  `Sec-Fetch-Site`/`Origin` (§12.2). `CONSOLE_CORS_ORIGIN` is unset by default, so browsers block
  cross-origin reads; set it only to a specific trusted origin, never `*` — that origin may then
  make changes with the cookie.
- `localStorage` holds only the theme and pane sizes. No tokens.
- Hiding a button is a courtesy, never a control: every route checks the permission itself.
- CSP is **not** currently set. Adding one is a good hardening step; note that CodeMirror requires
  `style-src 'unsafe-inline'` unless you extract its styles.

## 12.10 Dependency posture

- `npm audit` reports **0 vulnerabilities** at the pinned versions.
- One `overrides` entry pins `uuid` to `^11.1.1`, resolving a moderate advisory reachable through
  `gaxios` → `@google-cloud/storage`.
- `@duckdb/node-api` is pinned to the exact version `1.5.5-r.4` because it publishes
  prerelease-tagged versions where a caret range would behave unpredictably.

Re-run `npm audit` after any dependency change; the console pulls in native code
(DuckDB) and a cloud SDK, both of which warrant attention.

## 12.11 Threat summary

| Threat | Mitigated? | By what |
|--------|-----------|---------|
| Path traversal via materialization id | Yes | `assertSafeIdentifier` |
| A table naming files elsewhere | Yes | Every object confined to the recorded location; the REST catalog refuses commits that move files |
| Path traversal via object name | Yes | The location check, then segment filtering in `localPathFor` |
| SQL injection via browse filters | Yes | Column pattern + identifier/literal quoting |
| Arbitrary file read via SQL | Yes | The statement sandbox; the keyword guard only makes refusals clearer |
| Data modification via SQL | Yes | Forbidden statements; files are read, never a database; the sandbox writes only its own directory |
| Header injection via forwarded address or user agent | Yes | Charset filter + length cap |
| Invalid or out-of-range setting | Yes | Catalog validation in the console and the control plane; re-validated when read back |
| Unauthorized settings change | Yes | `settings.manage`, checked by the console and again by the control plane |
| Resource exhaustion via query | Partly | Row limits, memory limit, run concurrency, cache caps; no wall clock while a run is watched |
| Resource exhaustion via body size | Yes | 1 MiB body limit + per-field bounds |
| SSRF to arbitrary hosts | Yes | Upstream URL is operator-configured |
| Stack-trace disclosure | Yes | Fixed 500 message |
| Unauthorized access | Yes | Sign-in; a permission on every route, checked again by the control plane |
| Session theft via XSS | Yes | `HttpOnly` cookie; no `dangerouslySetInnerHTML` |
| Cross-site request forgery | Yes | `SameSite=Lax` + `Sec-Fetch-Site`/`Origin` check on every change |
| Password guessing | Yes | Per-address limit here; per-email lockout in the control plane; scrypt hashes |
| Another person's query results | Yes | Runs belong to their owner; a statement's sandbox reaches only its own run's directory |
| Data or schemas of a dataset the person has no role on | Yes | Per-dataset checks on every schema, data and run route; `visibleDetail` on the detail and its stream; the control plane and its REST catalog check again |
| **Reading a table's files with bucket credentials** | **No** | Storage enforces no dataset: anyone who can read the bucket reads every table in it |
| Denial of service by connection count | No | No per-IP SSE connection cap |
