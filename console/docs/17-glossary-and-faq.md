# 17 — Glossary and FAQ

## 17.1 Glossary

### Materializations and tables

| Term | Meaning |
|------|---------|
| **Control plane** | The Kaspr application, one per domain, that owns its domain's desired and observed state, creates and drops the tables, projects their schemas, and serves the domain's Iceberg REST catalog, identity and settings. With each domain's tail app, the console's upstreams besides object storage |
| **Runtime** | The dedicated Kaspr application deployed per materialization: its members consume the source topics and upload Parquet files, and one of them, the committer, commits them to the tables |
| **Maintenance app** | The one Kaspr application that compacts, expires and cleans up every materialization's tables, stopped ones and kept ones included; **Run now** asks it |
| **Tail app** | The Kaspr application that reads the topics of the tables people are watching and serves the records their runtimes have not committed yet, built into rows as the seal will write them (handbook 19). Reached through each domain's `tailUrl` |
| **Live tail** | A table's records past its committed snapshot, from the tail app: a run reads them as one more file, so its snapshot and its tail hold every record up to `through` exactly once. Every materialization has one, while the deployment's `live_tail_enabled` is on |
| **Live** (mode) | The Data tab's browse view following the live tail: every 1.5 s it appends what arrived, and a commit replaces the run with one on the new snapshot |
| **`through`** | Per Kafka partition, the last offset a run holds, from its snapshot or its tail: the next fetch asks for the records after it |
| **Materialization** | One or more Kafka topics, each continuously materialized into an Iceberg table of its own, in the dataset chosen for it, by one runtime. Identified by `mat-<12 hex>`; its name is a label for people, which can change |
| **Dataset** | A named group of tables in one domain, and their Iceberg namespace: SQL reads `<domain>.<dataset>.<table>`. Its tables may come from any of the domain's materializations; access is granted per dataset, in its domain. Its name is fixed once created |
| **Claim** | A table's `<dataset>.<table>` taken for a materialization by its domain's directory, before the table exists; the name is unique in the domain, and freed when the table is dropped or its materialization deleted. Of two requests for one name, the first claimed wins |
| **Kept table** | A table whose topic was removed from its materialization and that was kept: read-only, still readable and maintained, written again if its topic is added back |
| **Desired phase** | What the operator asked for: `running`, `stopped`, `deleting` (`paused` on records from before Stop) |
| **Observed phase** | What the control plane concluded is actually true, from the 11-value `LIFECYCLE_PHASES` |
| **Phase reason** | A machine token explaining the observed phase, e.g. `processing`, `waiting_for_runtime_ready`, `all_managed_agents_paused` |
| **Runtime progress** | Cumulative counters reported by the runtime — rows, bytes, files, skipped, per-partition offsets — with each member's own and the committer's report on the table |
| **Resource observation** | What the control plane last read from the Kubernetes `KasprApp`: conditions, generations, member counts, agent states |
| **Autoscaling** | The control plane running a runtime with more members than its replicas while it is far behind its topics, and with its own count again once caught up; on for every materialization unless it or the deployment turns it off. The Overview says *Scaled up to N members* and why |
| **Unhealthy** | The health badge for a `degraded` runtime: a member's error, or members that keep restarting — killed for memory, most often — whose message is the badge's tooltip |
| **Runtime binding** | The record of what was applied: app name, template version, managed agents, and `last_applied_spec_hash` |
| **Iceberg table** | Where a materialization's records are: Parquet data files plus metadata — the schemas, snapshots and manifests — under the table's location, `<destination>/<storage prefix>/<materialization id>/<table>` |
| **Catalog pointer** | `<location>/metadata/catalog-pointer.json`: which metadata file is current. A commit replaces it with a compare-and-swap; reading it is how anyone learns of newer data |
| **Snapshot** | The table as one commit left it, with a 64-bit id. Its summary carries the committed Kafka offsets and **valid through** |
| **Valid through** | Every record with a Kafka timestamp before this instant is in the snapshot, across every partition that has committed — "is my record there yet?". The console says **Complete through** |
| **Manifest** | An Avro file listing data files with each one's row count and column bounds. The console reads counts and time spans from manifests without opening a data file |
| **Committer** | The one runtime member that commits uploaded files to the tables — exactly once — and evolves their schemas |
| **Partition** | A UTC day (`2026-10-17`), hour (`2026-10-17-10`) or month (`2026-10`) of Kafka timestamps, by the table's partitioning; `__null__` holds rows without one, and an unpartitioned table has none. Not a Kafka partition |
| **Compaction** | Joining a partition's small files into a few large ones, in a `replace` snapshot; the Table tab's partitions say where it is due |
| **Schema projection** | The control plane's reading of a table schema: one entry per field with its id, logical name, physical path, type, state and fork version. Every schema the table has had is kept |
| **Field id** | Iceberg's number for a field, never reused. 1–9 are the metadata columns, payload fields start at 10. Data files are read by field id, so a renamed or forked field is matched by it |
| **Fork** | When a key's values change type in a way Iceberg cannot widen, the schema gains a sibling field `<name>__v<n>` of the new type, with an id of its own; the old member is `superseded`. Data is never rewritten, and the console folds the family into one column |
| **Collapse** | A family with too many forks, a struct with too many keys, or nesting past the limit goes to `<name>__json`, as JSON text |
| **Physical path** | Where a field is in the Iceberg schema: `["customer", "phone__v2"]`, with `[]` for a list's element. The logical name, `customer.phone`, is what people read |
| **REST catalog** | The control plane's Iceberg REST catalog, at `<control plane>/catalog`, through which external engines read — and with `catalog.write`, commit to — the tables |
| **Access token** | `tok_<id>_<secret>`: lets a tool use the REST catalog as its maker, with `catalog.read` and optionally `catalog.write`, on every dataset its maker may read or only on those it is limited to. Shown once |

### Console-specific

| Term | Meaning |
|------|---------|
| **BFF** | Backend-for-frontend. The Node server in `server/` |
| **Live store** | What polls one domain's control plane, caches, derives rates, and fans out snapshots: one per domain |
| **Merged fleet** | Every domain's fleet in one list, each item tagged with its domain: the Overview and Materializations pages at `/`, with a domain's own at `/d/<domain>` |
| **Fleet snapshot** | The whole-console view: totals, per-materialization summaries, and the fleet series |
| **Detail snapshot** | One materialization's detail, schema, and series |
| **Series** | A rolling array of `ThroughputSample`, capped at `CONSOLE_SERIES_POINTS`, points at least `CONSOLE_SERIES_SAMPLE_MS` apart |
| **Member rate** | One runtime member's rows (or bytes) per second, from the change in its counters between its own heartbeats |
| **Field name** | A field's path without the schema's `payload.` root — `customer.email` — the way the console shows it and SQL refers to it |
| **Sample** | One observation: cumulative counters plus rates derived against the previous sample |
| **Storage profile** | How the console reads one destination prefix (its scope): a kind (`gcs`, `s3`, `azure`), an endpoint, credentials from `CONSOLE_STORAGE_<NAME>_…`, and a mode. Each object is read with the profile whose scope is its longest prefix (07 §07.10) |
| **Direct mode** | A storage profile's data files are scanned in place by DuckDB, with the profile's scoped secret: an S3 or Azure profile and a GCS profile with an HMAC key pair, except where **Cached mode** applies |
| **Cached mode** | A storage profile's data files are mirrored to local disk, under `<cache>/<profile>/<container>/<key>`, then scanned: a profile that says `"access": "cached"`, a GCS profile without an HMAC key pair, an Azure emulator with neither a connection string nor the account key |
| **Baseline diff** | A diff against nothing, where every field is new |
| **Table name** | What SQL and engines call one of a materialization's tables, inside its dataset: its topic's name made an identifier (`orders`, `payments_v2`) unless another was typed, fixed once the table exists. In a statement, always with its domain: `<domain>.<dataset>.<table>` (`domain_a.sales.orders`) is a view over the snapshot's data files in the chosen time range, one column per field with forks folded in; `<domain>_iceberg.<dataset>.<table>` is the same rows as the native Iceberg table. Engines read `lake.<domain>.<dataset>.<table>` through the federated catalog, or `lake.<dataset>.<table>` through a domain's own |
| **Sandbox** | The DuckDB instance a statement runs in, made for it: it can open only the run's data files and the run's own directory, and holds only the secrets of the storage profiles those files are read with |
| **As of** | Reading a table as of an older snapshot: the runs API's `asOf`. The Data tab always reads the latest; a link from before carrying `asof=` opens on the latest |
| **Stopped** | A materialization whose runtime is scaled to zero replicas; resume scales it back to its replica count |
| **Run** | One execution of the view on screen — time range plus filters, or a statement. The only way a query executes; pages and exports read from it. Pinned to the snapshot it started on, or the one **As of** names |
| **Time range** | The window of Kafka record timestamps a run covers. Files outside it are never opened; the manifests record each file's span |
| **Override** | A setting value chosen on the Settings page. Stored with the control plane; wins over the environment variable until it is reset |
| **Deployment value** | What a setting is set to by its environment variable. A reset returns a setting to it, or to the built-in default when the deployment sets none |
| **Settings scope** | `control-plane`, `runtime` or `console`: the three groups settings are stored, versioned and conflict-checked in. Every domain has its own control-plane and runtime scopes; the console's is stored in the settings home |
| **Revision** | A scope's count of applied changes. A save names the revision it read, so it cannot overwrite a change it never saw |
| **Draft** | An unsaved edit on the Settings page: a value as typed, or a pending reset |
| **Rollout** | How far the current runtime-settings revision has reached the runtimes, shown on the Settings page's Runtimes section |

### Accounts and access

| Term | Meaning |
|------|---------|
| **Principal** | Who is making a request, in one domain: a signed-in person (`user`), an access token acting for one (`token`), a `guest`, or the console's read-only `service` credential |
| **Guest** | Anyone not signed in. Holds what the Guest group grants: by default the materialization list and status |
| **Permission** | One thing someone may do, such as `materializations.create` or `data.export`. Every console route and control-plane view names the one it needs. A *dataset* permission — reading schemas, querying, exporting, the catalog, adding and dropping tables, managing — is held on a dataset: granted there, or everywhere through a group's role |
| **Role** | A set of permissions. Eight are built in: Administrator, User Manager, Operator, Viewer, Status Viewer, and the dataset roles Dataset Reader, Dataset Writer and Dataset Owner; administrators make more |
| **Dataset grant** | Roles given to a person or group on one dataset, by its owners, from roles of dataset permissions only. A new dataset's creator owns it, and All Users may read it |
| **Locked** | A dataset someone may not read: they see its name and its tables' names, never their schemas or data |
| **Group** | People and the roles they hold through it. Built in: Guest, All Users, Viewer, Editor, Admin |
| **Session** | One sign-in to one domain: its control plane's token in this browser's HttpOnly cookie for that domain, `midstream_session_<domain>`. Ends after 8 h unused or 7 days in any case, at sign-out, or when an administrator ends it. A provider's sign-in starts one in every domain that admits the person |
| **Session check** | The console asking the control plane who a token belongs to; trusted for `CONSOLE_SESSION_CHECK_MS` (5 s) |
| **Service token** | `CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN`: the read-only credential a domain's live store polls it with, shared by everyone's views; each domain has its own |
| **Bootstrap administrator** | The control plane's first administrator, from its auth Secret; must change the password at first sign-in |
| **Audit log** | Sign-ins, failed sign-ins and every change to users, groups and roles, newest first; each domain keeps its own |

### Domains

| Term | Meaning |
|------|---------|
| **Domain** | One Midstream deployment — a control plane with its maintenance and tail apps, in one cluster, with its own accounts, datasets, tables, lake and settings — as the console serves it. Its id (`domain_a`) is its URL segment (`/d/domain_a`), its catalog in SQL and its first level in the federated catalog; its name (*Domain A*) is what people read |
| **Shared console** | The one console serving every domain, at one URL. There are no per-domain consoles: an old per-domain hostname redirects to `/d/<domain>` |
| **Registry** | The domains the console serves and how it reaches each — `controlPlaneUrl`, `tailUrl`, the Secret with its service token — from `console.domains[]` in the chart, as `CONSOLE_DOMAINS_FILE` |
| **Settings home** | The one domain that stores the console's own settings (its `console` scope), and whose `settings.view`/`settings.manage` decide who may see and change them |
| **Domain switcher** | The header's control for where you are: *All domains*, or one domain. Pages under `/d/<domain>` are that domain's; the others show every domain at once |
| **Misconfigured** | A domain whose control plane says it is another domain, or none: the console serves nothing of it until the registry or the control plane is corrected |
| **Sign-in provider** | Microsoft Entra ID, GitHub, Google or a generic OpenID Connect provider (`oidc`), registered with the console and turned on by each domain for itself. One sign-in through it starts a session in every domain that admits the person |
| **First-sign-in policy** | What a domain does with someone a provider vouches for that it has never seen: `open` makes them an account in All Users, `invited` refuses them until an administrator adds one |
| **Allowed email domains** | The email domains a domain admits through a provider (`example.com`); empty, any |
| **Linked identity** | A provider account tied to a domain's user, by the provider's subject: how the domain finds them next time, even if their email changes |
| **Federated access token** | An access token any domain honours: signed with the federation key, carrying its owner, permissions and datasets by `<domain>.<dataset>`; in each domain it acts for the account with the same provider account, else the same email, with no more than that account holds there |
| **Federation key** | The secret every domain shares to sign and check federated access tokens and their revocations; its id is shown on the console's Domains section, and a banner says when two domains' differ |
| **Revocation** | A signed record that a token was revoked, handed to every domain at once and again by the console every 5 minutes |
| **Federated catalog** | The console's `/catalog`: one Iceberg REST catalog over every domain, its namespaces `<domain>` and `<domain>.<dataset>`. Each domain's own is at `/d/<domain>/catalog` |

## 17.2 FAQ — behaviour


**Why does a new row show in the Data tab within seconds, but not in DuckDB on the catalog?** The
tab reads the table's snapshot and the live tail past it; an engine reads the snapshot alone, and the
row is in it once the runtime commits (every few minutes). The console's count less the engine's is
the tail's rows at that moment.

**Why did Live pause?** It waits while the tab is hidden, past the first page, or with a row open,
and says so in the status line; it stops for a statement or a range whose end has passed.

**I changed an environment variable, restarted, and nothing changed.**
The same setting is overridden on the Settings page, and an override wins over the environment.
The setting's line on the page says where its value comes from; **Reset** it to follow the
deployment again.

**Does changing a setting restart anything?**
No. Console settings apply to the next request or poll; control-plane settings to the next event;
runtime settings reach running runtimes within seconds. The Runtimes section shows which runtimes
run the new values.

**I saved a console setting, but another console replica still shows the old value.**
Replicas read the console's settings from the control plane every 30 seconds. The one you saved
through applies it at once.

**Why is the Settings page read-only?**
Settings are stored with the control plane, and the console cannot reach it; the banner says why.
Nothing can be saved until it can.

**Why does the rate show `—` right after I open a page?**
Rates come from each runtime member's own heartbeats, and a member needs two before it has a rate.
The console shows no total until every member has one — about half a minute after it starts
watching — rather than a total that climbs as members are heard from.

**Why does the ingest rate dip every few minutes?**
It really does. A runtime member stops consuming while it seals and uploads Parquet, and a
runtime's partitions tend to seal together. The rates are measured on the runtime's own clock, so
these pauses show as they happen, where the old poll-based rates buried them in sampling spikes.
See [13](./13-operations-runbook.md).

**Why is the write rate smoother than the ingest rate, and slower to move?**
Bytes are counted when a batch is sealed, a whole Parquet object at a time, in waves every few
minutes. The write rate is a 15-minute trend through them; anything shorter swings between zero and
a whole wave.

**Why do the Overview's row and byte counts match the Data tab but not the runtime's counters?**
Both read the table's current snapshot: what is committed. The runtime's counters also include rows
still being batched or waiting for a commit, and across a pod restart they drop the few counted
since its last heartbeat and count again the rows it reads a second time — up to a batch per
partition. When the table cannot be read, the Overview falls back to the runtime's counters and
says so.

**Why does a materialization show no metrics at all?**
The control plane omits `runtime_progress` entirely when every counter is null, so a materialization
that has never written a row produces no samples and no metrics. It is not an error.

**Why did my chart reset?**
Series live in memory. A console restart, or a failover to another replica, starts them over.

**The four phase counts do not add up to the total.**
A `deleting` materialization is counted in `materializations` but in none of
running/pending/stopped/failing — it is neither healthy nor a problem. One already `deleted` counts
only in `deleted`.

**I stopped it, but the phase still says Deploying.**
Correct and expected. The control plane accepts mutations asynchronously: the WebView returns 202
without persisting, an agent persists later, and observation confirms later still — a stop once the
KasprApp reports no members, usually within a minute or two. Watch the phase converge; the console
never fakes it.

**What is my table called?**
`<dataset>.<table>`: the dataset chosen for its topic, and a name that defaults to the topic's —
`orders` for `orders`, `payments_v2` for `payments.v2`, `update_topic` for a topic named after a
reserved word — and can be typed over when the topic is picked. Both show on the topic's row when it
is chosen, in the Tables panel, and in the SQL panel's header. A statement reads any of a
materialization's tables by that name — `sales.orders` — and can join them, across datasets; other
engines read `lake.sales.orders`, and can join it with any table they may read.

**Why can't I rename a dataset or a table?**
Statements, share links and external engines are written against `<dataset>.<table>`; a rename would
break every one of them, so both are fixed. A materialization's name is only a label, and **Edit**
changes it. The topics are not fixed either: **Edit** adds a topic, which gets a new table in the
dataset chosen for it, and removes one, keeping its table read-only or dropping it. Each such change stops the runtime for about five minutes while its
consumer group empties.

**Why is a dataset locked to me, and what do I see of it?**
Nobody gave you, or a group of yours, a role on it — or All Users' Reader grant was taken away. You
see its name, its owners and its tables' names; never their schemas or data. Ask one of its owners,
named on its page.

**Does a dataset role protect the files?**
No. Roles decide what the control plane, the REST catalog and this console show and serve. Anyone
holding credentials that read the destination — an S3 or Azure key, or a GCS HMAC key for DuckDB —
reads every table in it.

**I deleted it and it is still listed.**
Deletion is multi-stage: runtime teardown, observation confirmation, Kafka cleanup, dropping the
table, then record removal. The row disappears when the control plane stops listing it.

**Why does the header show an error but the data is still on screen?**
Deliberate. Blanking the screen on a transient outage destroys context. The console keeps the last
known state and tells you plainly that it is not fresh.

**Why is only one schema listed?**
The table has had only one. Every schema it has had is kept by the control plane, from schema 0 at
creation; a new one appears when the committer evolves the table. See
[06](./06-iceberg-tables.md).

**Why does schema 0 say *Initial · sampled* instead of a date?**
Schema 0 is what discovery sampled when the table was created, from the topic's oldest records and
its newest together, so it can hold shapes from any part of the topic's history, and a date would
claim more than is known. Each of its fields says when the data first had it instead (*first seen*).
Later schemas read *data from …*: when the records first had a field the version brought. That is
the date the change happened in the topic. *Committed …*, the API's `since`, is when the
materializer met it, which a backfill or a sampled schema 0 can put out of order. See
[06 §6.3](./06-iceberg-tables.md#63-schemas-the-projection-and-the-table).

**Why are schema dates in UTC when everything else is in my time zone?**
A date from the data is as precise as the table's files, and a day partition's file holds one UTC
day: told in your zone, every date of a day table would read as two days (*Aug 31 – Sep 1*). Times
of rows, on the Data tab, stay in your zone.

**A field shows as `Forked` and another with the same name as `Superseded` in one diff.**
That is a type fork rendered correctly. The new member has a field id of its own and takes the new
values; the old one keeps the values written before. The console reads both as one column.

**Why are some fields called containers?**
A struct or a list holds other fields; its leaves are the values. The Schema tab's tree shows them
nested, and the grid shows a struct's fields as columns of their own.

## 17.3 FAQ — data explorer

**Why does the row count sometimes end in a `+`?**
Because the run is still counting. A filter has to read its columns from every object in range, so
the footer shows the rows counted so far — `24,000+` — and the exact figure when the run finishes;
every finished run shows its exact total. With no filter, files wholly inside the range are
counted from the manifests, so the opening view is exact at once.

**Why do rows appear before a run finishes?**
Objects are counted newest first, and every object counted so far, back to the newest, has known
row positions, so those pages can be read at once — the first page usually within a second or two.
Pages further back fill in as counting reaches them. A statement shows its rows when it finishes,
since its first row can depend on all of them.

**Why does a statement sit at "Running" with no percentage at first?**
DuckDB cannot estimate progress for the opening stretch of a large remote scan, so the status shows
elapsed time and the scope until it can. Its row counters are not a substitute: `rows_processed`
counts tuples moving between pipeline operators, not rows of the answer. A filter's progress is
always exact, because it is counted from the manifests file by file.

**Why does a filter over All time take tens of seconds?**
It reads the filtered columns from every data file, and from far away each file costs a few round
trips. The first page arrives in a second or two, progress and time left are exact, and the time
picker shows how many rows each preset covers — a narrower range is proportionally faster. A table
a backfill wrote has many small files until compaction works through them.

**Why does my statement fail with `Referenced column "total__v2" not found`?**
The table names columns by logical name — `"total"`, quoted — and folds every fork into it. The
physical members, `total` and `total__v2`, are columns of the native table, `iceberg.orders`.

**Where did `payload.` go?**
The control plane roots every inferred field at `payload.` so record fields cannot collide with the
runtime's metadata columns. It is not part of anyone's record, so the console leaves it out
everywhere: the grid, filters, SQL (`"customer.email"`), exports, and the schema. A top-level field
that would take a metadata column's name keeps it (`payload._kafka_offset`). Links that still name
`payload.status` keep working; a statement that does fails and names the column it meant.

**What is the difference between nested and flat JSON exports?**
Flat keeps one key per column, `{"customer.email": …}`, exactly as the grid shows it. Nested rebuilds
the record — `{"customer": {"email": …}, "items": [{"sku": …}]}` — with `json` fields decoded.
The choice under **Export** is remembered and also applies to the row drawer's JSON copy.

**Why are there no `schema_version` or `ingest_date` columns?**
They were 0.x storage layout, not data. A row's Kafka timestamp, partition, offset, key and headers
are in the Kafka group of the column rail, and `_materialized_at` says when the runtime wrote it.

**Why are timestamps shown in my time zone?**
So they agree with the time picker, which works in local time: a row inside "Today" reads as today.
The column header names the zone and the tooltip shows UTC. Copy, filters and exports use the stored
UTC value.

**Why is my `DECIMAL` column a string in the JSON?**
`toJsonValue` stringifies values that would lose precision through IEEE-754. The same applies to
`bigint` outside the safe-integer range.

**Why is `SELECT * FROM orders WHERE x = 'a; DROP TABLE y'` allowed?**
The guard blanks string literals before scanning for statement separators, so a semicolon inside a
literal is correctly treated as data.

**Why is `read_parquet` forbidden in my query but used by the console?**
The guard applies only to user-authored SQL. The console's own statement builds the table's views
before yours runs; a statement has no file of its own to read. The guard is a keyword scan and can
be dodged — the sandbox is what refuses every other file ([12 §12.4](./12-security-model.md)).

**Why is export limited?**
An export is the run's whole result in the order shown, up to `CONSOLE_EXPORT_ROW_LIMIT`
(1,000,000 rows) — the export menu says how many it will contain. A browser download is the wrong
tool for a whole dataset: read the table with an engine through the REST catalog (Table ›
Connect).

**Why is the first Data tab load slow for some tables?**
A table read through a cached storage profile downloads each data file the first time a page or scan
needs it. Later reads reuse the cache — data files are immutable, so nothing is downloaded twice. A
direct profile avoids downloads entirely: give a GCS profile its HMAC key pair, an Azure emulator's
profile a connection string or the account key, and no profile `"access": "cached"`.

**What does "no storage is configured for …" mean?**
No storage profile's scope covers the table's location, so the console reads nothing of it — it
never guesses at credentials, and never lets DuckDB send the path anonymously to a public cloud. Add
a profile whose scope covers the location (`console.storage[]`, 03 §3.6a).

**Why can't I query a snapshot another engine wrote?**
A snapshot with delete files — rows an engine deleted — or with data files outside the table's
location is refused: the console reads data files directly and would show deleted rows, or would
have to read outside the table. Query it with an engine through the REST catalog.

**Why does a run as of an older snapshot say it may have expired?**
Snapshot expiry keeps every snapshot of the last hour, then one an hour for 72 hours, and the
newest 20 whatever their age (the defaults; Settings › Maintenance). A snapshot it removed cannot be
read; ask the runs API for a newer one. The Data tab always reads the latest.

## 17.4 FAQ — architecture

**Why does the console ask the control plane who I am on every request?**
It does not, quite: it asks once per `CONSOLE_SESSION_CHECK_MS` (5 s) per session and remembers the
answer. Accounts live in the control plane, so it is the only one that knows whether you were
deactivated a moment ago; the short cache keeps that true without a call per request.

**Why can I see the list without signing in?**
The Guest group holds the Status Viewer role by default. An administrator can take it away (or
give guests more) on the Groups page.

**Why did a button disappear?**
It needs a permission you do not hold — or no longer hold: a change to your groups applies at
your next request. Your account page lists what you can do.

**Why not call the control plane directly from the browser?**
Real-time fan-out, shared rate derivation, and server-side data access: the browser cannot hold
the object store's credentials or run DuckDB over a table.
See [01 §1.4](./01-overview-and-architecture.md#14-why-a-backend-for-frontend).

**Why SSE and not WebSockets?**
The stream is strictly server→client. `EventSource` reconnects automatically in every browser and
traverses ordinary HTTP infrastructure without an upgrade handshake.

**Why is there no server build step?**
Node ≥ 24 strips TypeScript types natively. Fewer moving parts, and the deployed source is the
source you read. The cost is `erasableSyntaxOnly`: no `enum`, no `namespace`, no parameter
properties.

**Why does the server import with explicit `.ts` extensions?**
Native type stripping does not resolve extensionless specifiers. The client uses bundler
resolution, so it does not need them.

**Why does `logger.ts` annotate the export as `FastifyBaseLogger`?**
Without it, Fastify specialises its generics to pino's concrete `Logger` type and every
`registerXRoutes(app: FastifyInstance)` call fails to typecheck.

**Why is `@fastify/static` registered with `wildcard: false`?**
So unmatched paths reach `setNotFoundHandler`, which serves `index.html` for client routes and a
JSON 404 for anything with a file extension. The cost is that a client rebuild requires a server
restart.

**Why does the fleet rate sum per-materialization rates instead of differencing the total?**
Details refresh on a rotating cadence, so on any given tick only a subset of counters moved.
Differencing the aggregate produced alternating zeros and spikes. The same reasoning goes one level
down: a materialization's rate is the sum of its members' rates, each measured between that
member's own heartbeats.

**Why is TanStack Query not used for live data?**
Live data arrives over SSE. Putting it in a query cache too would create two sources of truth for
the same value.

**Can I run more than one replica?**
Technically yes, but each keeps its own poll loops, series, runs and provider sign-ins in flight.
See [13 §13.4](./13-operations-runbook.md#134-running-more-than-one-replica).

**Why does one console serve every domain, instead of one console each?**
So people find everything in one place: one URL, one sign-in through a provider, search and SQL
across domains, one catalog for engines. Each domain still decides everything about itself — its
accounts, permissions, data and settings — and the console asks each one, as the person, for each
thing it shows (handbook 20).

**Why must SQL name the domain?**
So a statement reads the same wherever it is pasted, and two domains' `sales.orders` are never
confused. `domain_a.sales.orders`; a statement naming `sales.orders` is told how to write it.

**Why did a provider's sign-in sign me in to only some domains?**
Each domain decides for itself: it may not have the provider turned on, may admit only the people
an administrator added (`invited`), or only some email domains. The sign-in page lists what each
domain answered and why.

**Why is my access token refused in one domain?**
Either that domain's federation key differs from the one the token was signed with (a banner says
so), or you have no account there with the same provider account or email
(`token_unknown_owner`), or the token is limited to datasets of other domains.

## 17.5 FAQ — development

**Where do I add an environment variable?**
`server/config.ts`, then document it in [03](./03-configuration-reference.md) and add it to
`.env.example`. Nowhere else may read `process.env`.

**Where do I add a shared type?**
`shared/types.ts`. Keep it import-free. Use a `Cp` prefix for anything mirroring a control-plane
payload, and change those additively only.

**How do I add a tab, a filter operator, a metric, or an endpoint?**
Recipes in [15 — Extending the console](./15-extending-the-console.md).

**How do I test a change?**
`npm run typecheck && npm test && npm run build`, then against a real control plane and table as
in [14 — Testing and verification](./14-testing-and-verification.md). Always verify with the control
plane **down** as well as up.

**Why is `npm audit` clean when `@google-cloud/storage` pulls in a vulnerable `uuid`?**
An `overrides` entry pins `uuid` to `^11.1.1`.

**Why is `@duckdb/node-api` pinned exactly?**
It publishes prerelease-tagged versions (`1.5.5-r.4`), where a caret range behaves unpredictably.

**Can I move `/console` into its own repository?**
Yes — that is the design goal. Copy the directory, give it a registry (`CONSOLE_DOMAINS`, or none for
one control plane at `localhost:6065`), and it works unchanged. Nothing under `/console` imports from
`python/`.

## 17.6 Quick reference

### Ports

| Port | Process |
|------|---------|
| 5173 | Vite dev server |
| 8787 | Console server |
| 6065 | Control-plane WebView API |

### Local state

| Location | Contents |
|----------|----------|
| `localStorage['console.theme']` | `light` \| `dark` |
| `midstream_session_<domain>` cookies (HttpOnly) | This browser's session in each domain; scripts cannot read them |
| `midstream_session-flow` cookie (HttpOnly, `/api/auth/sso` only) | A provider's sign-in in progress, for up to 10 minutes |
| `localStorage['console.data.sqlHeight']` | SQL editor height, px |
| `localStorage['console.data.railWidth']` | Column rail width, px (264–720) |
| `localStorage['console.data.jsonShape']` | JSON layout for exports and the row drawer: `nested` or `flat` |
| The settings home's `settings` table, `console` scope | This console's settings overrides; the console keeps no copy on disk |
| `<CONSOLE_DATA_DIR>/parquet-cache/<profile>/<container>/<key>` | Mirrored Parquet of cached storage profiles |
| `<CONSOLE_DATA_DIR>/runs/<run id>/` | One statement run's result, removed with the run |

### Key defaults

| Setting | Default |
|---------|---------|
| List poll | 2 s |
| Detail poll | 1.5 s |
| Fleet detail rotation | 6 s |
| Idle poll | 30 s |
| Series window | 240 points, ≥ 5 s apart (~20 min) |
| Rate windows | rows 1 min, bytes 15 min, per runtime member |
| Consumer-lag history | 6 h, one point per measurement |
| Rows per page | up to 1,000 |
| Statement result kept | 1,000,000 rows; the total is always counted |
| Export | 1,000,000 rows |
| Runs scanning at once | 4 |
| Objects one run reads at once | 48 |
| Run kept after its last read | 30 min |
| A table read served as current | 2 s for a run, 5 s for the Data tab; up to 5 min served while it refreshes |
| DuckDB memory / threads | 4 GB / 32 |
| Cached-mode limits | 400 files / 2 GiB |
| Parquet cache budget | 8 GiB |
