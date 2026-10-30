# 12 — Security Model

An honest account of what the console defends against, what it does not, and where the real trust
boundaries are.

## 12.1 Trust boundaries

```
┌─────────────┐   untrusted   ┌──────────────────┐  the person's session  ┌──────────────────┐
│  Browser    │ ────────────► │  Console server  │  in that domain, or    │ Domain A's       │
│ (a guest,   │  one cookie   │                  │  its service token     │ control plane    │
│  or someone │  per domain   │  which domain,   │ ─────────────────────► │ (authorizes      │
│  signed in) │               │  who is calling  │ ─────────────────────► │  every call) …   │
└─────────────┘               │  permit() checks │                        │ Domain B's, C's  │
       │                      │  validation      │ ─────────────────────► │ Object stores    │
       │  sign-in through     │  sandbox         │                        └──────────────────┘
       ▼  a provider          └──────────────────┘
┌─────────────┐  code → ID token    │  ▲
│ Entra ID,   │ ◄───────────────────┘  │  each domain checks the ID token
│ GitHub,     │ ◄──────────────────────┼─ against the provider itself
│ Google, OIDC│                        │
└─────────────┘
```

Everything crossing the browser→server boundary is untrusted: path parameters, request bodies,
headers, and SQL. So is what a table names — its pointer, metadata and manifests are written by
whoever commits to it (§12.6) — and what a sign-in provider sends back. Everything the server sends
upstream is constructed from validated input. Who may do what is decided by each domain's control
plane, for that domain only; the console asks it, enforces the answer on its own routes, and sends
every change with the person's own session there, so the control plane checks again. The console
is not an identity provider: it runs a provider's sign-in and hands what the provider vouched for
to every domain, and each domain decides for itself whether that is someone it knows (control-plane
handbook 18 §18.17).

## 12.2 Authentication and authorization

Accounts, groups, roles and permissions live in each domain's control plane
([18 — Identity and access](../../docs/handbook/18-identity-and-access.md)); a domain knows nothing of
another's. People sign in two ways: with an email and password, in one domain at a time, or through
a sign-in provider — Microsoft Entra ID, GitHub, Google or any OpenID Connect provider — in every
domain at once ([20 — Domains and the shared console](../../docs/handbook/20-domains-and-the-shared-console.md)).
A password account is added by an administrator, with an initial password they must change. A
provider's sign-in can make an account the first time someone signs in to a domain, when that
domain's first-sign-in policy is `open` (the default) and their email is in one of its allowed email
domains; with `invited`, only the people an administrator added get in. Either way the account is
made by the domain, never the console, and it starts in the domain's default groups — a provider's
groups never decide any.

| Control | How |
|---------|-----|
| **Every route declares a permission** | `permit('<permission>')` or `signedIn()` on each `/api` route; a route with neither answers anyone (`/api/health`, `/api/config`, `/api/auth/session`, `/api/auth/login`, `/api/auth/logout`, `/api/auth/sso/*`). A guest gets `401`, a person without the permission `403` |
| **A domain's routes are checked in that domain only** | Every route under `/api/d/<domain>/` resolves the caller in that domain — its session cookie, else a guest there — and checks the permission there: holding `materializations.manage` in Domain A grants nothing in Domain B. The console's own routes (the merged fleet, search, runs across domains) resolve the caller in every domain and show what comes from each only as that domain allows |
| **The catalogs are the domains' to decide** | `/catalog` and `/d/<domain>/catalog` check nothing themselves: they hand the caller's bearer token to the domain a namespace names, unchanged, and the domain checks it as it checks an engine calling it directly (§12.7) |
| **Data permissions are checked per dataset** | A dataset permission (`dataset.view_schema`, `data.query`, `data.export`, `catalog.read`, …) is checked on the datasets a request is about, each in its own domain: every table a statement names — a statement joining two domains needs `data.query` on each dataset, each where it lives — the table a schema is of, the datasets a run read (for its rows, stream and export, again on every request). The route's guard, `permitAnywhere(permission)`, turns away anyone holding it on no dataset; `requireOnDatasets` then refuses with `403`, `details.permission` and `details.dataset` (`auth/guard.ts`, the rule in `shared/access.ts`). A token limited to datasets holds no dataset permission on any other |
| **The control plane checks again** | Changes are sent with the person's own session, never the console's credential, so a bug in a console route cannot grant what the control plane refuses. The datasets routes are the control plane's to decide: the console only passes them through |
| **The session never reaches page scripts** | Each domain's session token is kept in a cookie of its own, `<CONSOLE_SESSION_COOKIE>_<domain>`, `HttpOnly`, `SameSite=Lax`, `Secure` behind TLS. XSS cannot read them, and a session is only ever sent to the domain that issued it |
| **Cross-site changes are refused** | `POST`/`PUT`/`PATCH`/`DELETE` need `Sec-Fetch-Site: same-origin` (or `none`), or else an `Origin` naming this host. With `SameSite=Lax`, this is the CSRF defence; no token is needed |
| **Revocation is prompt** | Session checks are cached for `CONSOLE_SESSION_CHECK_MS` (5 s), and the control plane checks every request against the current directory: deactivating someone or signing them out applies within seconds. Streams re-check every 30 s and close |
| **Sign-in is rate-limited** | `CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` per client address here, for passwords and for starting a provider's sign-in, and each domain locks an email after repeated wrong passwords whatever the address |
| **A provider's sign-in finishes only where it started** | The console starts a flow with a random `state`, a `nonce` and a PKCE verifier (S256), and binds it to the browser with a short-lived cookie, `<CONSOLE_SESSION_COOKIE>-flow` (`Path=/api/auth/sso`, `HttpOnly`); it keeps only the cookie's SHA-256. The callback finishes a flow once, for the same provider, from a browser holding that cookie, within 10 minutes — else the person is asked to sign in again (`auth/sso.ts::takeFlow`). A code intercepted on its way back is useless without the verifier |
| **Every domain checks the provider's answer itself** | The console exchanges the code for an ID token (GitHub: an access token) and hands it to every domain. Each checks the ID token's signature against the provider's published keys, its issuer (one Entra ID tenant; Google's, with its hosted domain when set), its audience, expiry, issue time and the flow's nonce — once: a nonce used is refused again — and GitHub's token against GitHub's API with the app's own credentials (control-plane handbook 18 §18.17). The console revokes GitHub's token once every domain has answered; it never stores either |
| **Back to this console only** | Where a sign-in returns to is a path of this console (`safeReturnPath`): another host, `//host`, `/\host`, `/api/…` and `/sign-in` all become `/`. There is no open redirect |
| **Password sign-in can be turned off, per domain** | A domain that turns it off refuses a password unless its account holds every permission, so an administrator can still get in when a provider is down; the console only shows the choice |
| **Shared views read with least privilege** | Each domain's live store polls with that domain's own service token, `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`, which its control plane lets read, never change, on every dataset there — and nowhere else: a domain's token is never sent to another. Everything it serves is still filtered by each route's permission, and a materialization's detail by the caller's datasets: `visibleDetail` (`live/store.ts`) leaves out the schemas and schema errors of tables whose datasets they may not see the schemas of, and the Table tab's record leaves out the fork map and `schema.*` properties — on the detail route and on every refresh of the detail stream |
| **Runs are private** | A query run belongs to whoever started it; anyone else gets `404`. A statement's sandbox reaches only its own run's directory (§12.4) |
| **The live tail is asked as the person** | A run's tail, a refresh and the head peek call the tail app with the person's own session — the same token the console sends the control plane — and with the console's service token in `X-Console-Token`, which the tail app requires on every view (handbook 19 §19.10). The tail app checks the session with the control plane and `data.query` on the table's dataset itself, so a console bug cannot widen what a person reads. The console refuses first: a run, a refresh and a peek need `data.query` on the table's dataset here too, so a person without it causes no tail request at all. The tail's rows land in the run's own directory and nowhere else |
| **Access tokens are shown once** | `POST /api/d/<domain>/auth/tokens` answers a token's text once, `cache-control: no-store`. Nobody stores it: the console never does, and the domain that makes it keeps its id, owner, permissions and expiry, but not its text — every domain checks a token by its signature, with the federation key they share. Listings never carry secrets |
| **A revoked token is refused everywhere** | Revoking signs a revocation the console hands to every other domain at once, and again every 5 minutes to any domain that missed it (`auth/revocations.ts`); each domain refuses a revoked token from then until it would have expired |
| **No secrets in the browser** | `localStorage` holds the theme and pane sizes; the session is the cookie |

Guests are real callers: by default they may see the materialization list and status. An
administrator can give the Guest group less, or more, from the Groups page.

**What still belongs to the deployment.** Serve the console over TLS (the cookies are `Secure`
only when the request arrived over HTTPS, `CONSOLE_COOKIE_SECURE=auto`), keep each control plane's
port off any public address — a control plane in another cluster is reached through its API route,
over TLS — and treat every storage profile's credentials (`CONSOLE_STORAGE_<NAME>_…`, a gcs key
file), every domain's service token and each provider's client secret as sensitive, and make the
storage credentials read-only (§12.4). The federation key most of all: every domain honours a token
it signs, for anyone in that domain's directory, administrators included, so whoever holds it can act
as anyone anywhere (control-plane handbook 18 §18.11). Keep it in the auth Secret of each domain's
release and nowhere else, and rotate it in every domain together. Ingress authentication in front of the console is still a reasonable extra layer, but
no longer the only one.

**Limits worth knowing.** Data permissions are per dataset, not per table or per materialization:
someone who may query `sales` may query every table in it, whatever materialization writes it. What
is not data is not secret: anyone who may see materializations sees each one's topics, status, totals
and table names, locked datasets' included. Access is not storage access: the console's own
credentials read every table under their profiles' scopes, and so does anyone holding credentials
that read the destination — storage enforces none of this (control-plane handbook 14). The per-address sign-in limit trusts the address
the ingress reports (`trustProxy`); a client that reaches the pod directly can choose its own. There
is no "forgot password" (an administrator resets it), and no email. The same email is the same
person in every domain: a federated token made in one domain acts, in another, for the account
there with the same provider account, else the same email — so a domain should add people under the
emails they use everywhere. A provider's flow lives in the console process that started it, as runs
do: with more than one replica, the callback must reach the same one (session affinity).

## 12.3 Input validation

### Path parameters

```ts
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
```

Every `:domain` must be a domain this console serves — anything else is a `404` (`unknown_domain`)
before any route runs — so it picks one of the registry's URLs and never names a host. Every
`:materializationId` passes `assertSafeIdentifier` before it is used anywhere. The id reaches
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

Before the lock, the instance gets the storage access its sources need and no more: the extensions
and secrets of the direct storage profiles whose scopes serve the run's sources (07 §07.10). Other
profiles' secrets are not there to list.

DuckDB enforces these instance-wide and will not undo them (E10). The instance holds two views of
each table the statement names, both over exactly that table's files, read by field id:
`<domain>.<dataset>.<table>` with logical columns, and `<domain>_iceberg.<dataset>.<table>` with the
native Iceberg columns, each domain an in-memory catalog attached for the statement and gone with
it. Only the tables the statement names are there, in whichever domains, each after `data.query`
on its dataset in its own domain: no other table's data, no database file. Validating a statement while it is
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

Forbidden functions are refused as calls, however the call is written — the name quoted or not, in
any case, qualified with a catalog or schema or not — and whatever surrounds it:

```
duckdb_secrets which_secret query json_execute_serialized_sql
```

`duckdb_secrets()` and `which_secret()` list the secrets of the instance a statement runs in — every
storage profile's name, scope, endpoint, region and key id; the secret values are redacted, and
`duckdb_secrets(redact := false)` is refused by DuckDB itself. They open no file, so the sandbox cannot
refuse them, and before this check they passed the guard. `query()` and `json_execute_serialized_sql()`
run SQL given as a string, which no keyword check sees into. Being calls only, these reserve no table
name: `FROM sales.which_secret` and a column named `query` stay queryable. `duckdb_settings()` and
`current_setting()` were checked against a sandbox holding every kind of secret and name no profile.

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
- **The sandbox does not tell reads from writes.** `COPY … TO` an object path the sandbox may read
  succeeds in a locked sandbox when the profile's credentials can write it; only the guard's refusal
  of `COPY` stops it. Give every storage profile read-only credentials — S3 `GetObject` and
  `ListBucket`, a read-only SeaweedFS identity, Azure's "Storage Blob Data Reader" or a read-and-list
  SAS, GCS `objectViewer` for the HMAC key's service account. The console cannot tell whether a
  profile's identity can write and does not try; it warns at start for every direct profile.
- **The secret-listing functions passed the guard.** Until `FORBIDDEN_FUNCTIONS`, a statement could
  call `duckdb_secrets()` and see the scope, endpoint, region and key id of every secret its sandbox
  held — across domains, and the GCS HMAC key id. A sandbox now holds only the secrets its own sources
  need, and the guard refuses the calls.

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

These checks run on the canonical URIs — `gs://`, `s3://`, `abfss://…dfs.core.windows.net` — whatever
the backend; only the hand-over of an Azure path to DuckDB spells it `az://<account>.blob.core.windows.net/…`
(07 §07.10). A location that passes them must still be covered by a storage profile, or nothing under
it is read: `503 storage_not_configured`.

For a cached profile, `storage/cache.ts::cachePathFor` mirrors an object to
`<CONSOLE_PARQUET_CACHE_DIR>/<profile>/<container>/<key>`:

```ts
const segments = location.key.split('/')
  .filter((s) => s.length > 0 && s !== '.' && s !== '..')
  .map((s) => s.replace(/[\0]/g, ''));
```

Empty, `.`, and `..` segments are dropped and null bytes removed, so the join cannot escape the
cache directory even for a name that passed the location check; a container of `.` or `..` is not
one (`storage/uris.ts`). The profile's name and the container are the first segments, keeping
destinations and buckets apart.

## 12.7 Outbound request safety

- Every control-plane request carries `AbortSignal.timeout(CONTROL_PLANE_TIMEOUT_MS)`, so a hung
  upstream cannot exhaust the event loop; a domain that does not answer is marked unreachable and
  the others go on.
- Every domain's URLs come from the registry the chart renders (`CONSOLE_DOMAINS_FILE`), never from
  a request: a `:domain` segment, or a federated namespace's first level, picks one of them — there
  is no SSRF surface.
- Sign-in providers' endpoints are fixed for Entra ID, GitHub and Google; a generic OpenID Connect
  provider's come from the operator-configured issuer's discovery document, kept an hour. Every
  provider request has a 15 s timeout.
- The catalogs forward to a registered domain only, with a path rebuilt from the parsed namespace,
  a 30 s timeout and a 16 MiB body limit; a commit's body is handed on byte for byte, never parsed.
  They answer `501` to making or dropping a namespace or table, and to multi-table transactions,
  without asking any domain.
- Ids are `encodeURIComponent`-escaped after passing the identifier pattern.
- Upstream status codes are relayed; upstream error bodies are relayed under
  `details.upstream_details` rather than being merged into the console's own error shape.

## 12.8 Information disclosure

| Channel | Control |
|---------|---------|
| Unexpected server errors | Fixed message `the console failed to handle this request`, empty details, 500. The real error is logged server-side only |
| A table that cannot be read | `503` naming what failed — storage's status code, or the missing object — never storage's own message, which can name the console's service account |
| `GET /api/config` | Exposes every domain's id, name, status, version, federation-key id and live-tail state, the mode, row limits, poll interval and versions to anyone; each domain's create defaults only with `materializations.create` there. No URLs, credentials or budgets: how each domain is reached is on its Settings page, with `settings.view` there |
| `GET /api/auth/sso/outcomes/:id` | What a provider's sign-in did in each domain — signed in, refused and why, unreachable — to whoever holds the id: an unguessable 18-byte value in the redirect, kept 5 minutes. No token, no email |
| A domain that refuses a sign-in | Its reason (`no_account`, `email_not_allowed`, `provider_disabled`, `password_sign_in_disabled`, …) is shown to the person, since it is about them; a wrong password still says only that the email or password is wrong |
| Logs | `authorization` and `cookie` headers are removed; per-request logging is disabled |
| DuckDB errors | Relayed to the user (they are needed to fix a query) but stripped of the statement echo. They can reveal column names of the tables being queried — acceptable, since the user can already `DESCRIBE` them; a table in a dataset they may not query never reaches a sandbox |
| A table in a dataset locked to the caller | Its schema, schema errors, fork map and `schema.*` properties are left out of the detail, the detail stream and the table record. Its name, topic, counts, snapshots and maintenance stay: names are not secret, and totals are a materialization's status |
| Object storage credentials | Never leave the server, and never appear in a log — at most a variable's name; the browser never receives a signed URL or a bucket path it could not already infer. The Table tab's `storage` names the profile serving the table, its endpoint, region, path style, mode and Azure account, for the Connect snippets; the Settings page (`settings.view`) adds what each profile reads with, by the names of its variables |
| A location no profile covers | `503 storage_not_configured`, naming the location: never read, never handed to DuckDB, which would send it anonymously to the public cloud |

## 12.9 Browser-side considerations

- No `dangerouslySetInnerHTML` anywhere. All rendering is through React's escaping.
- Cell values are rendered as text, including JSON-stringified objects.
- The only cookies are the sessions, one per domain, `HttpOnly; SameSite=Lax`, and, during a
  provider's sign-in, its flow cookie, sent only to `/api/auth/sso`. Cross-site changes are refused by
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
| Unauthorized access | Yes | Sign-in; a permission on every route, checked in its domain and again by that domain's control plane |
| A permission in one domain used in another | Yes | Sessions, grants and service tokens are each domain's own; a route is checked only in the domain its path names |
| Login CSRF, or a sign-in finished in another browser | Yes | `state` bound to the flow cookie's hash; one use; 10 minutes |
| An intercepted authorization code | Yes | PKCE (S256): the verifier never leaves the console |
| A replayed or misdirected ID token | Yes | Every domain checks signature, issuer, audience, expiry, issue time and a one-use nonce |
| Open redirect after sign-in | Yes | `safeReturnPath` |
| A forged access token | Yes | HMAC-SHA256 with the federation key, checked by every domain |
| A revoked access token used in another domain | Yes | The revocation is handed to every domain at once, and again every 5 minutes to any that missed it; a domain down then refuses it once it is back and synced |
| **The federation key leaked** | **No** | Whoever holds it can act as anyone in every domain: keep it secret and rotate it everywhere together |
| Session theft via XSS | Yes | `HttpOnly` cookie; no `dangerouslySetInnerHTML` |
| Cross-site request forgery | Yes | `SameSite=Lax` + `Sec-Fetch-Site`/`Origin` check on every change |
| Password guessing | Yes | Per-address limit here; per-email lockout in the control plane; scrypt hashes |
| Another person's query results | Yes | Runs belong to their owner; a statement's sandbox reaches only its own run's directory |
| Data or schemas of a dataset the person has no role on | Yes | Per-dataset checks on every schema, data and run route; `visibleDetail` on the detail and its stream; the control plane and its REST catalog check again |
| **Reading a table's files with bucket credentials** | **No** | Storage enforces no dataset: anyone who can read the bucket reads every table in it |
| Denial of service by connection count | No | No per-IP SSE connection cap |
