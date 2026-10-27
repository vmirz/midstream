# 04 — Server Internals

Everything in this document lives under `console/server/`. Reading tables and running queries —
`iceberg/` (the pointer, metadata and manifests), `data/` (the snapshot's catalog, logical columns,
DuckDB, GCS) and `runs/` (the run engine) — is in [06](./06-iceberg-tables.md) and
[07](./07-data-access-and-duckdb.md); `schema/diff.ts` in 06 §6.4; `live/` in
[05](./05-live-store-and-realtime.md). The domains the console serves (`domains/`, §4.10) are what
nearly everything else hangs off: each has its own control-plane client, live store and topic
catalog.

## 4.1 Bootstrap (`main.ts`)

`main.ts` is the composition root — the only file that wires modules together. Order matters, and
its very first import is `./workerPool.ts`, which sets `UV_THREADPOOL_SIZE` before anything can
start Node's worker pool: every DuckDB query holds one of those threads while it waits on object
storage, and the default four would cap every scan at four objects in flight
([03](./03-configuration-reference.md)).

```ts
const app = Fastify({
  loggerInstance: logger,
  bodyLimit: 1024 * 1024,                                        // 1 MiB
  trustProxy: true,
  logController: new LogController({ disableRequestLogging: true }),
});

if (config.corsOrigin) await app.register(cors, { origin: config.corsOrigin, credentials: false });

app.setErrorHandler(...);

const served = domains();             // the registry; a registry that breaks a rule stops here
await registerAuth(app);              // who is calling, in each domain, before every route

// The console's own routes: config, the session everywhere, sign-in through a provider, the
// catalogs, the merged fleet, search, runs and the console's own settings.
await registerSystemRoutes(app);
await registerConsoleAuthRoutes(app);
await registerSsoRoutes(app);
await registerCatalogRoutes(app);
await registerFleetStreamRoutes(app);
await registerSearchRoutes(app);
await registerRunRoutes(app);
await registerConsoleSettingsRoutes(app);

// Everything about one domain, under /api/d/<domain>/.
await app.register(async (scope) => {
  await registerAuthRoutes(scope);
  await registerIdentityRoutes(scope);
  await registerMaterializationRoutes(scope);
  await registerDatasetRoutes(scope);
  await registerTopicRoutes(scope);
  await registerSchemaRoutes(scope);
  await registerTableRoutes(scope);
  await registerDataRoutes(scope);
  await registerStreamRoutes(scope);
  await registerSettingsRoutes(scope);
}, { prefix: '/api/d/:domain' });

if (config.serveClient && existsSync(join(clientDir, 'index.html'))) {
  await app.register(fastifyStatic, { root: clientDir, index: false, wildcard: false });
  app.setNotFoundHandler(...);
}

consoleSettings().start();            // the console's overrides, from the settings home
served.start();                       // every domain's live store, topic catalog and identity check
new RevocationSync(() => served.served()).start();
process.on('SIGTERM', ...); process.on('SIGINT', ...);
await app.listen({ port: config.port, host: config.host });
```

Notable decisions:

| Decision | Reason |
|----------|--------|
| `bodyLimit: 1 MiB` | The largest legitimate body is a SQL statement capped at 20 000 characters. 1 MiB is generous and bounds abuse |
| `trustProxy: true` | The console is expected to sit behind an ingress; this makes `request.ip` and protocol detection correct |
| `logController: new LogController({ disableRequestLogging: true })` | Fastify 5.12 removed the top-level `disableRequestLogging` option in favour of a `LogController` **instance**. Passing a plain object throws `FST_ERR_LOG_INVALID_LOG_CONTROLLER` at startup |
| Routes registered before static | So `/api/*` always wins over a hypothetical file of the same name |
| One scope for a domain's routes, prefixed `/api/d/:domain` | A route is written once and serves every domain; `request.domain` says which (§4.9). The console's own routes stay outside it |
| `registerAuth` before every route | Its `onRequest` hook resolves the domain and the caller before any route or guard runs |
| `wildcard: false` on static | Lets unmatched paths reach `setNotFoundHandler`, which is what makes SPA deep links work |
| `served.start()` after routes | Each domain's polling begins only once the process is otherwise ready |
| `consoleSettings().start()` just before it | The console's settings overrides are read from the settings home's control plane before the first poll, so the poll cadence they set applies from the start (§4.8) |
| `RevocationSync` last | Hands every domain the access-token revocations it missed, every 5 minutes (§4.11) |

### Static serving and SPA fallback

```ts
app.setNotFoundHandler((request, reply) => {
  if (request.url.startsWith('/api/') || /\.[a-z0-9]+(\?|$)/i.test(request.url)) {
    return reply.code(404).send({ error: { code: 'not_found', ... } });
  }
  return reply.sendFile('index.html');
});
```

Three cases:

| Request | Result |
|---------|--------|
| `/api/does-not-exist` | JSON 404 |
| `/assets/index-abc123.js` (missing) | JSON 404 — because the path has a file extension |
| `/materializations/mat-x/data` | `index.html`, so the SPA router handles it |

The extension check exists because an earlier version returned `index.html` for missing assets,
which produced a confusing `Failed to load module script … MIME type of "text/html"` error in the
browser instead of an honest 404.

### Shutdown

`SIGTERM` and `SIGINT` stop the revocation sync, every domain's timers and the settings store, close Fastify (draining in-flight requests and
ending SSE responses), and exit 0. In-flight DuckDB queries are not cancelled; they die with the
process.

## 4.2 Configuration (`config.ts`)

See [03 — Configuration reference](./03-configuration-reference.md). Key architectural point: the
exported `config` is frozen, so it is readonly to every consumer. It has two kinds of member:
deployment settings, plain values fixed at start, and the 28 overridable settings, which are
getters returning the override in force (§4.8). Read them as `config.<key>` where the value is
used; a copy taken at startup would miss every later change.

## 4.3 Logging (`logger.ts`)

```ts
export const logger: FastifyBaseLogger = pino({
  level: config.logLevel,
  base: { service: 'midstream-console' },
  redact: { paths: ['req.headers.authorization', 'req.headers.cookie'], remove: true },
});
```

The explicit `FastifyBaseLogger` annotation is load-bearing. Without it, `pino()` returns
`Logger<never, boolean>`, Fastify specialises its generics to that type, and every
`registerXRoutes(app: FastifyInstance)` call fails to typecheck with a long, opaque error about
`childLoggerFactory` and `msgPrefix`. Annotating the export keeps Fastify on its default generics.

Per-request logging is disabled, so the log contains lifecycle events, warnings, and errors only.
Log volume is therefore flat regardless of traffic — appropriate given the polling architecture.

Authorization and cookie headers are removed (not masked) from any logged request object.

## 4.4 Errors (`errors.ts`)

```ts
export class ConsoleError extends Error {
  readonly code: string;
  readonly statusCode: number;
  readonly details: Record<string, unknown>;
  toBody(): ApiErrorBody;   // { error: { code, message, details } }
}
```

Constructors:

| Constructor | `code` | Status | Typical `details` |
|-------------|--------|--------|-------------------|
| `badRequest(message, details)` | `invalid_request` | 400 | `{ field }`, `{ token }`, `{ files, limit }` |
| `notFound(message, details)` | `not_found` | 404 | `{ resource, id }` |
| `unavailable(message, details)` | `unavailable` | 503 | `{ reason }`; `{ location }` for a table that cannot be read (`data/catalog.ts::unreadableTable`) |
| `upstreamError(message, status, details)` | `control_plane_error` | pass-through | `{ upstream_code, upstream_details }` |

Two more codes are constructed directly rather than through a helper:

| Where | `code` | Status |
|-------|--------|--------|
| `controlPlane/client.ts` when `fetch` throws | `control_plane_unreachable` | 503 |
| `data/duckdb.ts`, `runs/` on a statement or filter the engine rejects | `query_failed` | 400 |
| `data/duckdb.ts` when a run is cancelled mid-query | `cancelled` | 409 |

The global error handler in `main.ts` returns `error.toBody()` with the carried status for any
`ConsoleError`, and a generic `internal_error` 500 for anything else. **Unexpected errors never
leak a stack trace or message to the client** — they are logged at `error` level with the URL and
answered with a fixed message.

### Identifier validation

```ts
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
export function assertSafeIdentifier(value: string, field = 'materialization_id'): string
```

Every materialization id from a URL passes through this before it is used in a filesystem path, a
SQL literal, or an upstream URL. It rejects path traversal (`..%2F..%2Fetc%2Fpasswd`), null bytes,
slashes, and anything longer than 128 characters.

## 4.5 Control-plane client (`controlPlane/client.ts`)

A thin typed wrapper over the control-plane WebViews. It is the **only** place the console
performs an outbound HTTP call to a control plane. Each domain has its own client,
`new ControlPlaneClient(controlPlaneUrl, serviceToken)`, made from its registry entry (§4.10); no
client is shared between domains. Every method takes a `Caller` first: who the request is made as.

| Caller | Sent as | Used for |
|---|---|---|
| `SERVICE` (`{ kind: 'service' }`) | `authorization: Bearer <the domain's service token>` (`CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`); nothing when it has none | What the console reads once for everyone: the domain's live store, topics, datasets for search, `/system/version`, and — in the settings home — the console's settings scope. Read-only on the control plane |
| `UserCaller` (`{ kind: 'user', token, ip, userAgent }`) | the person's session token in that domain, or nothing for a guest | Everything a person asks for or changes: the control plane authorizes the person, not the console |

`callerOf(request, domain?)` (`auth/guard.ts`) builds a `UserCaller` from a request: in the
request's own domain, or in the one given, with the caller's session there.

### Request construction

```ts
const timeout = AbortSignal.timeout(config.controlPlaneTimeoutMs);
const composed = signal ? AbortSignal.any([signal, timeout]) : timeout;
```

Every request carries a timeout; callers may additionally pass their own signal, and the two are
composed so whichever fires first aborts the request.

Headers sent:

```
accept: application/json
authorization: Bearer <token>        (the caller's session, or the service token; none for a guest)
x-forwarded-for: <browser address>   (UserCaller only; for the control plane's audit log)
x-forwarded-user-agent: <browser>    (UserCaller only)
content-type: application/json       (only when a body is present)
```

The forwarded address and user agent come from the browser, so they are cut to a safe charset
and 256 characters before they are sent — a **header-injection guard**. The control plane records
them as reported, never trusts them for a decision.

### Error mapping

| Situation | Result |
|-----------|--------|
| `fetch` throws (DNS, connection refused, timeout) | `ConsoleError('control_plane_unreachable', …, 503, { reason })` |
| `404`, `502`, `503` or `504` whose body is not the control plane's JSON error — a gateway answering for a control plane that is not there: a route removed, a cluster down | The same `control_plane_unreachable` `503` (`reason: http_<status>`), so a gateway's 404 is never read as a materialization the control plane says is gone (`isControlPlaneError`) |
| Non-2xx response with a `{ error: { code, message, details } }` body | `upstreamError(message, response.status, { upstream_code, upstream_details })` |
| Non-2xx response with a non-JSON body | Same, with `message` set to the first 512 characters of the raw text |
| 2xx with an unparseable body | `safeJsonParse` returns a synthetic `invalid_upstream_payload` error object |

**Upstream status codes are passed through unchanged.** A control-plane 404 becomes a console 404.
A `401 unauthenticated` or `403 forbidden` keeps its code and `details` (`reason`, `permission`),
so the client can tell "sign in" from "not allowed".
This matters because the control plane's own error-status wiring is documented as unverified —
whatever it returns, the console faithfully relays.

### Methods

| Method | Upstream call |
|--------|---------------|
| `list(caller, limit = 'all', signal?)` | `GET /materializations?limit=…` |
| `topics(caller, refresh = false, signal?)` | `GET /topics`, with `?refresh=true` when asked to reload |
| `detail(caller, id, signal?)` | `GET /materializations/{id}` |
| `schema(caller, id, table, signal?)` | `GET /materializations/{id}/tables/{table}/schema` |
| `schemaVersions(caller, id, table, signal?)`, `schemaVersion(caller, id, table, schemaId, signal?)` | `GET /materializations/{id}/tables/{table}/schema/versions[/{schemaId}]` |
| `table(caller, id, table, signal?)` | `GET /materializations/{id}/tables/{table}` |
| `maintain(caller, id, job, table = null)` | `POST /materializations/{id}/maintenance`, `{ job, table }`: one table, or with `null` every table |
| `create(caller, input)` | `POST /materializations` |
| `settings(caller, signal?)` | `GET /system/settings` |
| `settingsScope(caller, scope, signal?)` | `GET /settings/{scope}` |
| `updateSettings(caller, scope, input)` | `PATCH /settings/{scope}` |
| `settingsHistory(caller, scope, limit?, signal?)` | `GET /settings/{scope}/history?limit=…` |
| `stop(caller, id)` | `POST /materializations/{id}/stop` |
| `update(caller, id, input)` | `PATCH /materializations/{id}` |
| `resume(caller, id)` | `POST /materializations/{id}/resume` |
| `remove(caller, id)` | `DELETE /materializations/{id}` |
| `datasets`, `dataset`, `createDataset`, `updateDataset`, `deleteDataset`, `datasetGrants`, … | `/datasets…` |
| `login`, `session`, `refresh`, `logout`, `changePassword`, `mySessions` | `/auth/*` |
| `ssoLogin(caller, { provider, credential, nonce })` | `POST /auth/login` with what a provider vouched for (handbook 18 §18.17); a 25 s timeout, as the domain may wait for an account it makes |
| `accessTokens`, `createAccessToken`, `revokeAccessToken` | `GET`/`POST /auth/tokens`, `DELETE /auth/tokens/{tokenId}` — the revoke answers the signed revocation |
| `tokenRevocations(caller, since)`, `applyTokenRevocations(caller, revocations, source)` | `GET /auth/tokens/revocations`, `POST /auth/tokens/revocations` |
| `users`, `user`, `createUser`, `updateUser`, `deleteUser`, `setUserPassword`, `signOutUser`, `unlockUser` | `/users…` |
| `groups`, `group`, `createGroup`, `updateGroup`, `deleteGroup`; `roles`, `role`, `createRole`, `updateRole`, `deleteRole` | `/groups…`, `/roles…` |
| `permissions`, `audit`, `identityRequest` | `GET /permissions`, `GET /audit`, `GET /identity/requests/{id}` |

`raw()` returns the status and body instead of throwing on a non-2xx answer; `login` and
`ssoLogin` use it, since a refused sign-in is an expected answer.

Every id-taking method calls `assertSafeIdentifier` and then `encodeURIComponent`; a table name
goes through `encodeURIComponent` too (`tablePath`). There is no singleton: a route reaches a
domain's client as `domainOf(request).controlPlane`, and tests make a client pointed at a stub.

## 4.6 Route layer (`routes/`)

Routes are intentionally thin: validate, delegate, shape. No route contains business logic.

### Shared helpers (`routes/materializations.ts`)

```ts
export function materializationId(request): string        // params + assertSafeIdentifier
export function parseBody<T>(schema: T, body: unknown)    // zod parse → ConsoleError on failure
```

`parseBody` converts the first Zod issue into a `badRequest` whose message is
`"<path>: <message>"` and whose `details.field` is the dotted path — so the client can highlight
the offending input.

### Registration map

The console's own routes, about every domain at once:

| Module | Routes |
|--------|--------|
| `system.ts` | `GET /api/health`, `GET /api/config` |
| `auth.ts` (`registerConsoleAuthRoutes`) | `GET /api/auth/session` (every domain's session), `POST /api/auth/logout` (every domain's, or the ones named) |
| `sso.ts` | `GET /api/auth/sso/:provider/start`, `GET /api/auth/sso/:provider/callback`, `GET /api/auth/sso/outcomes/:id` |
| `catalog.ts` | `/catalog/v1/…` (the federated catalog) and `/d/:domain/catalog/*` (each domain's own) — outside `/api`: an engine's base URI |
| `stream.ts` (`registerFleetStreamRoutes`) | `GET /api/fleet`, `GET /api/stream/fleet` — every domain's fleet, merged |
| `search.ts` | `GET /api/search` |
| `data.ts` (`registerRunRoutes`) | `GET /api/runs/:runId`, `POST …/refresh`, `POST …/cancel`, `GET …/rows`, `POST …/profile`, `GET …/export`, `GET /api/stream/runs/:runId` — a run may read several domains |
| `settings.ts` (`registerConsoleSettingsRoutes`) | `GET`/`PATCH /api/settings`, `GET /api/settings/history` — the console scope, stored in the settings home |

Everything about one domain, each path under `/api/d/:domain`:

| Module | Routes |
|--------|--------|
| `materializations.ts` | `GET /materializations`, `POST /materializations`, `GET/PATCH/POST/DELETE /materializations/:id…` |
| `datasets.ts` | `GET`/`POST /datasets`, `GET`/`PATCH`/`DELETE /datasets/:dataset`, `GET /datasets/:dataset/grants`, `PUT /datasets/:dataset/grants/:principalId` |
| `schema.ts` | `GET …/tables/:table/schema`, `…/schema/versions`, `…/schema/versions/:schemaId`, `…/schema/diff` |
| `table.ts` | `GET …/tables/:table`, `POST …/maintenance`, `GET /catalog/connection` |
| `data.ts` (`registerDataRoutes`) | `GET …/data/summary`, `POST …/data/estimate`, `POST …/data/validate`, `POST …/runs`, `GET …/tables/:table/head` |
| `stream.ts` (`registerStreamRoutes`) | `GET /stream/materializations/:id` |
| `topics.ts` | `GET /topics` |
| `settings.ts` (`registerSettingsRoutes`) | `GET`/`PATCH /settings`, `GET /settings/history` — the domain's control-plane and runtime scopes |
| `auth.ts` (`registerAuthRoutes`) | `GET /auth/session`, `POST /auth/login`, `POST /auth/logout`, `GET /auth/sessions`, `POST /auth/password`, `GET`/`POST /auth/tokens`, `DELETE /auth/tokens/:tokenId` |
| `identity.ts` | `/users…`, `/groups…`, `/roles…`, `GET /permissions`, `GET /audit` |

Every route declares who may call it in its options: `{ preHandler: permit('<permission>') }`,
`{ preHandler: permitAnywhere('<permission>') }` for a dataset permission, or `{ preHandler:
signedIn() }` (§4.9) — each checked in the route's domain. The console's own routes use
`permitInHome('<permission>')` (the console's settings: the settings home decides) or
`permitInSomeDomain('<permission>')` (runs: the run then checks each dataset it reads, in its own
domain). A route with none of them answers anyone, guests included. A route guarded by
`permitAnywhere` then checks the datasets its request is about with `requireOnDatasets`.

Full request and response shapes are in
[08 — Console API reference](./08-console-api-reference.md).

### Freshness on read routes

```ts
app.get('/materializations', async (request) => {
  const domain = domainOf(request);
  await domain.live.ensureFresh(config.listPollIntervalMs);
  return domain.live.fleetSnapshot();
});
```

Without this, a REST-only consumer would receive whatever the idle 30-second loop last observed —
including materializations that had already been deleted. The detail route relies on the
equivalent staleness check inside `LiveStore.getDetail`, and `GET /api/fleet` on each domain's.

### Server-sent events (`routes/stream.ts`)

```ts
reply.raw.writeHead(200, {
  'content-type': 'text/event-stream',
  'cache-control': 'no-cache, no-transform',
  connection: 'keep-alive',
  'x-accel-buffering': 'no',
});
reply.raw.write(': stream open\n\n');
const heartbeat = setInterval(() => reply.raw.write(': keep-alive\n\n'), 20_000);
```

- `x-accel-buffering: no` prevents nginx-class proxies from buffering the stream.
- A comment frame every 20 s keeps idle intermediaries from closing the connection.
- `request.raw.on('close')` unsubscribes from the live store and clears the heartbeat, so a
  closed tab immediately stops costing anything.
- Frames are `event: <name>\ndata: <json>\n\n`; the client listens for the named event.

SSE was chosen over WebSockets because the stream is strictly server→client, `EventSource`
reconnects automatically in every browser, and it traverses ordinary HTTP infrastructure without
an upgrade handshake.

## 4.6.1 Topic catalog (`topics/catalog.ts`)

The create dialog's topic picker reads every topic in the cluster each time it opens, so the
catalog is built for many readers at once. One `TopicCatalog` per process holds the list.

| Property | How |
|----------|-----|
| One upstream request per refresh | Concurrent reads share the request in flight (`inFlight ??=`) |
| No waiting once loaded | A list older than `CONSOLE_TOPIC_CATALOG_TTL_MS` is still answered at once while a background refresh replaces it; only the first read after start waits |
| Cheap reads | The JSON body, its gzip encoding, and a weak ETag are built once per refresh; a read is one buffer write, or a bodiless `304` |
| Bounded refreshes | No refresh starts within `CONSOLE_TOPIC_CATALOG_MIN_REFRESH_MS` of the previous attempt: explicit reloads, stale reads, and retries after a failure alike |
| Idle costs nothing | There is no timer; only reads cause refreshes. `main.ts` calls `warm()` once at start |

Failure handling:

- A failed refresh keeps the last good list, re-prepared with `stale: true` and the error.
- With nothing loaded, a read fails fast with `503 unavailable` instead of retrying per request
  inside the refresh floor.
- A control plane without `GET /topics` (older than the console) is reported as "the control plane
  does not list topics yet; upgrade it to enable the topic picker" rather than a bare 404.
- `refresh()` never rejects: background refreshes are not awaited, and Node exits on an unhandled
  rejection.

Measured on one console process serving 27,000 topics (2.3 MB of JSON, 147 KB gzipped): 6,000
requests from 200 concurrent clients, 70% revalidations, completed in 0.45 s (about 13,000
requests per second, p95 21 ms) and caused one request to the control plane.

## 4.7 Shared types (`shared/types.ts`)

Imported by both halves — the client via the `@shared/*` alias, the server via a relative
`.ts` path. The file has **zero imports** and must stay that way, since it is consumed under two
different module resolution modes.

Two families of types:

| Prefix | Meaning | Stability |
|--------|---------|-----------|
| `Cp*` | Mirrors a control-plane payload exactly | Additive-only, like the API it describes |
| everything else | Console-derived (`FleetSnapshot`, `DetailSnapshot`, `ThroughputSample`, `SchemaDiff`, `DatasetSummary`, `QueryResult`, …) | Owned by the console; change freely alongside both halves |

`LIFECYCLE_PHASES` is exported as a runtime tuple **and** a derived union type, so phase→tone maps
in the UI can be exhaustively checked.

## 4.8 Settings (`settings/`, `routes/settings.ts`)

The Settings page edits three scopes. The control plane describes, validates and stores its own
two (`control-plane`, `runtime`). The console describes and validates its own (`console`), but
stores the overrides with the control plane too: the console keeps no durable state of its own.

| Module | Role |
|--------|------|
| `settings/catalog.ts` | `CONSOLE_SETTINGS`: each overridable console setting's key, env var, section, label, description, kind, unit, bounds and effect. `consoleSettingProblem` validates one value; `validConsoleOverrides` splits a stored set into valid and rejected |
| `settings/store.ts` | `ConsoleSettingsStore`: reads the `console` scope from the settings home at start, every 30 s and after a save; installs the valid overrides with `applySettingOverrides`; applies a new log level to the logger; describes the scope for the page |
| `config.ts` | `tunableDeployment` (the 29 overridable settings, as the environment sets them), `deploymentSettings`, and `config`, whose getters return the override in force |
| `routes/settings.ts` | `/api/settings` — the console scope, in the settings home — and `/api/d/:domain/settings` — a domain's control-plane and runtime scopes ([08 §8.24](./08-console-api-reference.md)) |

There are two Settings pages, so two sets of routes: the console's own settings are one scope,
stored in the settings home and guarded by `permitInHome`; each domain's are its own two, guarded
in that domain.

**Reading.** `GET /api/d/:domain/settings` reads the domain's two control-plane scopes in
parallel; if either read fails, the page is marked read-only, with the reason. `GET /api/settings`
refreshes the console scope from the settings home, and describes the console's deployment —
every domain it serves and how it reaches each — besides.

**Saving.** A `PATCH` takes one change per scope, of the scopes its route serves:

1. Validate console values against the catalog. The control plane validates its own scopes.
2. Forward the changes in order (control plane, runtime, console), stopping at the first refusal.
   A console change carries `fallbacks`, what each changed setting reads as without an override,
   because the control plane cannot know the console's defaults and the history should read on
   its own.
3. Wait, polling every 150 ms for up to 10 s, until each request id is in its scope's
   `recent_request_ids` (applied) or is its `last_rejection` (refused).
4. Install an applied console scope at once, so this replica answers with the new values, and
   drop the cached materialization defaults that `/api/config` serves.
5. Answer `200` with the settings as they now are, `202` naming the scopes still pending, or
   `409 settings_rejected`.

**Other replicas** pick a console change up on their next 30-second refresh. A control-plane or
runtime change needs nothing from the console: the control plane applies it.

**History.** Each history route reads its scopes' histories in parallel and merges them newest
first. A scope that fails is reported in `errors` without hiding the others. The client merges the
console's history with every domain's it may see, on the console Settings page.

## 4.9 Authentication and authorization (`auth/`)

The console has no accounts of its own: people, sessions and permissions belong to each domain's
control plane ([18 — Identity and access](../../docs/handbook/18-identity-and-access.md)). The
console resolves who is calling in each domain, keeps a browser's session there in a cookie per
domain, and checks each route's permission, in its domain, before doing any work.

| Module | Role |
|---|---|
| `auth/guard.ts` | `registerAuth`: an `onRequest` hook on every `/api/*` request. `permit(permission)`, `permitAnywhere(permission)`, `permitInHome`, `permitInSomeDomain`, `requireOnDatasets(auth, permission, datasets)`, `signedIn({ allowPasswordChange })`, `domainOf`, `authOf`, `authIn(request, domain)`, `callerOf`, `presentedToken`, the cookie setters, `crossSite`, `watchAccess` |
| `auth/principal.ts` | `resolveAuth(domain, token)`: asks the domain's `GET /auth/session` who a token belongs to, and caches the answer by domain and SHA-256 of the token for `CONSOLE_SESSION_CHECK_MS` (5 s); `guestAuth(domain)` the same for a guest. `toAuthState`, `can(principal, permission, datasetId?)` and `canAnywhere` (both `shared/access.ts`), `rememberSession`, `forgetSession`, `forgetAllSessions(domain)` |
| `auth/cookies.ts` | Parsing cookies; a domain's session cookie, `<CONSOLE_SESSION_COOKIE>_<domain>`, `HttpOnly; SameSite=Lax; Path=/`, `Secure` per `CONSOLE_COOKIE_SECURE`; the provider sign-in's flow cookie, `<CONSOLE_SESSION_COOKIE>-flow`, `Path=/api/auth/sso` |
| `auth/sso.ts` | Signing in through a provider: the configured providers, pending flows (`state`, `nonce`, PKCE verifier, the flow cookie's hash), the code exchange, GitHub's token revocation, and the outcomes kept 5 minutes (§4.11) |
| `auth/revocations.ts` | `handOn` (one revocation to every other domain) and `RevocationSync` (§4.11) |
| `auth/outcomes.ts` | `settle` and `waitForOutcome`: after a user, group or role change, poll `GET /identity/requests/{id}` every 150 ms for up to 10 s; `refusal` maps an outcome to a `ConsoleError` |
| `auth/rateLimit.ts` | `AttemptLimiter`: sign-in attempts per client address per minute |
| `auth/testing.ts` | `installTestAuth`, `testPrincipal`: route tests run as any principal without a control plane |

**Every request.** The hook, in order:

1. A `POST`, `PUT`, `PATCH` or `DELETE` from another site is refused: `403 forbidden`,
   `reason: cross_site_request`. `Sec-Fetch-Site` decides when the browser sends it; otherwise the
   `Origin` must name this host (or `CONSOLE_CORS_ORIGIN`).
2. `/api/health` needs no caller.
3. Which domains: a path under `/api/d/<domain>/` names one — `404 unknown_domain` for one the
   console does not serve, `503 domain_misconfigured` for one whose control plane says it is
   another — and `request.domain` is it. Any other `/api` path is resolved in every domain served.
4. In each, the token is read from `Authorization: Bearer` (scripts: an access token works in
   every domain, a session only in its own) or the domain's session cookie, and resolved with `resolveAuth`. A cookie whose session is no longer
   usable is cleared. A cookie session past its `refresh_after` is renewed with
   `POST /auth/refresh`, and the new token set in a new cookie; concurrent requests share one
   renewal.
5. The result is `request.auths`: by domain id, the caller's `AuthState` (principal, session), the
   token, and any error checking it. `authOf(request)` is the one for the request's domain.

**Routes.** `permit(permission)` answers `401 unauthenticated` (`sign_in_required`) to a guest
and `403 forbidden` (with `permission`) to a person who lacks it; a person who must change their
password gets `403 password_change_required`. `permitAnywhere(permission)` is the same for a
dataset permission held on at least one dataset; `requireOnDatasets` then answers `403` with
`permission` and the first `dataset` it is missing on, in a message naming both. `signedIn()`
requires a session, any permissions. None of them calls the control plane: they read
`request.auth`, whose principal carries its `dataset_permissions` and any token `dataset_limit`.

**When a control plane is unreachable,** a session there checked within the last
`CONSOLE_SESSION_CHECK_MS` + 60 s keeps being honoured, so a brief outage does not sign everyone
out; past that, a request about that domain fails with `503`, and the console's own routes go on
with the other domains, marking that one unreachable.

**Streams.** An SSE route checks its caller every 30 s (`watchAccess`). When access is gone —
signed out, deactivated, a permission removed — it sends `event: auth` and closes; the client
reloads its session.

**Changes.** Mutations are made with the person's own session in the route's domain
(`callerOf(request)`), so that control plane authorizes them again and records who made them. After
a user, group or role change applies, every cached session of that domain is dropped
(`forgetAllSessions(domain)`), because what anyone may do there may have changed.

**Runs.** A query run belongs to the person who started it (`ownerId`); another caller asking for
it gets `404`, as if it did not exist.

## 4.10 Domains (`domains/`)

Every domain the console serves is a `Domain`, made once from the registry
([20 — Domains and the shared console](../../docs/handbook/20-domains-and-the-shared-console.md)).

| Module | Role |
|---|---|
| `domains/registry.ts` | `loadRegistry`: the JSON the chart renders (`CONSOLE_DOMAINS_FILE`), else `CONSOLE_DOMAINS` inline, else one development domain, `local`, at `http://localhost:6065`. `validateRegistry` refuses what the chart refuses — an id that cannot be a domain's (`shared/tableNames.ts::domainIdProblem`), the same id twice, a URL that is not http(s), anything but exactly one settings home — and the console stops at start, naming the rule. Each domain's service token is read from `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`, never from the file |
| `domains/domain.ts` | `Domain`: the domain's id, name, settings-home flag, tail URL and service token; its own `ControlPlaneClient`, `LiveStore` and `TopicCatalog`; its status; and what it caches — system settings a minute, datasets half a minute |
| `domains/index.ts` | `domains()`: the `Domains` of the registry — `all()`, `served()`, `get(id)`, `require(id)` (`404 unknown_domain`), `home`. `consoleSettings()`: the console's settings store, over the home's client. `useDomains` replaces them in route tests (`domains/testing.ts::installTestDomains`) |

**Which domain a control plane is.** Each `Domain` asks its control plane `GET /system/version`
at start, every minute, and at once whenever its live store's polls stop or start succeeding: its
version, the domain it says it is, and its federation key's id. That gives its status:

| Status | When | What the console does |
|---|---|---|
| `ok` | It answered, as the domain the registry names, with a version this console works with | Serves it |
| `unreachable` | No answer yet, or the latest check failed (a domain last seen as another stays `misconfigured`) | Serves it, marked: its views say so, the merged fleet shows what it last read — or, once the caller cannot be checked there, that it is unavailable — with a banner, search answers from what it last read, the federated catalog's listing leaves it out |
| `misconfigured` | It answered as another domain, or as none | Serves nothing of it (`503 domain_misconfigured`): a wrong URL never shows one domain's data as another's |
| `incompatible` | Its major version is older than the console's (`REQUIRED_CONTROL_PLANE_MAJOR`) | Serves it, with a banner saying so |

Routes wait on a domain's first check (`domain.ready()`), so a console just started never serves a
domain it has not identified. `GET /api/config` reports every domain's status, and whether their
federation keys agree (`federationKeysAgree`): the client says so when two domains would not honour
each other's access tokens.

## 4.11 Sign-in through a provider, revocations and the catalogs

**Providers (`auth/sso.ts`, `routes/sso.ts`).** The providers are those whose client id and secret
are configured (`CONSOLE_SSO_*`, [03 §3.3a](./03-configuration-reference.md)); Entra ID's, GitHub's
and Google's endpoints are fixed, a generic OpenID Connect provider's come from its issuer's
discovery document. `start` keeps a pending flow by its `state` — nonce, PKCE verifier, return
path, the flow cookie's SHA-256 — for 10 minutes, at most 10,000 at once, and redirects to the
provider. `callback` takes the flow (once; same provider; the same browser), exchanges the code,
and calls `ssoLogin` on every served domain at once. Each domain checks the credential itself and
answers a session, a refusal or nothing; the console sets a cookie for each session, ends any
session of that domain the browser held before, revokes a GitHub token, and keeps the outcome 5
minutes under an id it adds to the redirect (`?signin=<id>`) for the page to show. Flows and
outcomes are in memory: a console restart forgets them, and the person signs in again.

**Revocations (`auth/revocations.ts`).** Revoking a federated access token in one domain answers a
signed revocation; the route hands it at once to every other served domain (`handOn`) and answers
the person when they have. `RevocationSync` then, every 5 minutes, reads each domain's revocations
since the last pass and applies those another domain is missing — what reaches a domain that was
down at the time.

**Catalogs (`routes/catalog.ts`).** `/catalog` is one Iceberg REST catalog over every domain: its
namespaces are `<domain>` and `<domain>.<dataset>`, and every request about a dataset is handed to
its domain with the namespace cut to the domain's own single level and the caller's bearer token
unchanged. `/d/<domain>/catalog` hands everything to that domain's catalog as it is. Both keep
nothing; see [08 §8.30](./08-console-api-reference.md) and handbook 20.
