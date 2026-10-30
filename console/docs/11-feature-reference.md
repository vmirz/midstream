# 11 — Feature Reference

Exactly what every screen shows, where each value comes from, and what every control does.

## 11.1 App shell

Persistent around every page (`components/AppShell.tsx`).

| Element | Behaviour |
|---------|-----------|
| Domain switcher | Above the navigation, where the console serves several domains: **All domains**, then each domain with its live dot (green live, amber reconnecting or unreachable, red misconfigured). Choosing one keeps the section open — the overview, the lists, settings |
| Sidebar (≥ 768 px) | Logo, **Overview**, **Materializations**, **Datasets**, **Search**; **Settings** at the foot, above the account button; active item highlighted via `NavLink`. In a domain the items are that domain's; under **All domains** every domain's. Items a person may not open are left out |
| Mobile nav (< 768 px) | Horizontal strip replacing the sidebar, Settings last; **Sign in** for a guest |
| Account button | Signed in to any domain: an avatar with initials and the person's name; opens a menu with their email, the domains they are signed in to (and **sign in to more**), **Your account** and **Sign out everywhere**. Signed in nowhere: **Sign in** |
| ⌘K / Ctrl+K | Opens **Search** from anywhere (§11.12) |
| Theme toggle | Flips `.dark` and persists `console.theme` |
| Domain banners | Across the top, one per domain that is misconfigured (its control plane says it is another domain) or runs a release this console does not work with, saying which |
| About this deployment | The console's version and every domain's control plane's; a warning when the domains sign access tokens with different federation keys |

### Signing in (`/sign-in`, `/sign-in/:domain`)

**Sign in to Midstream.** Where the console is registered with providers, a button for each —
**Sign in with Microsoft**, **GitHub**, **Google**, or the OpenID Connect provider's own label — signs
in to **every domain that admits the person** at once (handbook 20 §20.7). Below, **or with a
password**: a password signs in to one domain, chosen in a **Domain** field where the console serves
several (`/sign-in/<domain>` names it, and the page that sent the person here preselects its own);
each domain has its own accounts. A wrong email and a wrong password read the same — "Email or
password is incorrect." — and a locked-out or deactivated account says so, as does a domain whose
password sign-in is off for all but administrators. "Forgot your password? Ask an administrator of
<domain> to set a new one." Accounts are made by each domain's administrators, or — under the `open`
policy — at someone's first sign-in through a provider.

Back from a provider, a toast says where the sign-in started a session and where not, and why
(*Signed in to Domain A · Domain B: no account there*); when no domain admitted the person, the sign-in
page shows each domain's answer. After signing in, the page returns to wherever the person was going.
A guest who opens a page that needs more is sent here first; a signed-in person without the permission
sees **Access denied** naming what the page needs, in which domain.

**A password someone else chose** — a new account, an administrator's reset, the bootstrap
administrator — must be replaced before anything else, in the domain that set it: the console shows
only **Change password** for that domain (current password, new password, confirm), with its minimum
length, and **Sign out of <domain>**. Once it is changed, the person is signed straight back in and
carries on. A session started through a provider is never held back by it.

A session that ends elsewhere — signed out by an administrator, deactivated, expired — turns the
page into that domain's guest's at the next request, and open streams close within 30 seconds.

### `LiveIndicator`

| Condition | Display |
|-----------|---------|
| `status === 'live'` and no error | Pulsing green dot + `Live · <relative generatedAt>` |
| `status === 'connecting'` | Amber dot + `Connecting…` |
| Reconnecting or control-plane error | Amber dot + the error message, e.g. `control plane at http://… is unreachable` |

The relative timestamp updates every second via `useTicker`, so a frozen stream becomes visible as
a growing age rather than a silently stale screen.

---

## 11.2 Overview (`/`)

Fleet-wide health and throughput. Data: `useFleetStream()`.

### Stat row

| Card | Value | Detail |
|------|-------|--------|
| Materializations | `totals.materializations` | `DistributionBar` over running/pending/stopped/failing, plus the counts as text |
| Ingest rate | `totals.rowsPerSecond` | Every runtime member · 1-min window; sparkline of `series[].rowsPerSecond`, accent colour |
| Write rate | `totals.bytesPerSecond` | Parquet · 15-min trend; sparkline of `series[].bytesPerSecond`, green |
| Materialized | `totals.rowsCommitted`: the rows every table holds, as its committer last reported its current snapshot | `tableBytes` · `tableFiles` parquet files · `recordsSkipped` skipped |

### Fleet throughput

The same rows/s series over the last ~20 minutes, as a chart with a value axis, clock times and a
hover readout. The fleet's rate is the sum of every materialization's, each measured per runtime
member between its own heartbeats ([05 §5.4](./05-live-store-and-realtime.md#54-throughput-derivation)).

### Needs attention

Shows up to six materializations matching **any** of:

- `observed_phase ∈ { failed, degraded }`
- `runtimeHealth ∈ { degraded, unreachable }`
- `desired_phase !== observed_phase` and `observed_phase !== 'deleted'`

Each row: warning icon, name, humanized `phase_reason`, relative `updated_at`, phase badge. Empty
state is "All clear — No failures, degraded runtimes, or pending reconciliation."

### Busiest materializations

Top eight by `rowsPerSecond`, tie-broken by `rowsCommitted`, filtered to those with any throughput or
any rows. Columns: name and topics (the first two, then `+N`), rows/s, bytes/s, rows committed. "View all" links to the list.

Skeletons render until the first frame arrives.

---

## 11.3 Materializations (`/materializations`)

Live, searchable, sortable list. Data: `useFleetStream()`.

### Header

Subtitle: `N total · M running · R rows/s`. Actions: **New materialization**.

### Search

Case-insensitive substring across `materialization_id`, the label (`name`), every source topic,
every dataset and every table (`sales.orders`) — what a row shows is what a search matches. Wrapped in `useDeferredValue`, so typing never
blocks. The destination is not searched: rows do not
show it, and most materializations share the default one.

### Filters

| Segment | Predicate |
|---------|-----------|
| All | everything |
| Running | `observed_phase === 'running'` |
| Pending | `observed_phase ∈ { requested, discovering, discovered, deploying }` |
| Stopped | `observed_phase ∈ { stopped, paused }` (`paused` only on records from before Stop) |
| Attention | `observed_phase ∈ { failed, degraded }` **or** `desired_phase !== observed_phase` |

### Columns

| Column | Sortable | Content |
|--------|----------|---------|
| Materialization | yes (by display name) | The label, or the id; below it the source topics, the first two then `+N`, every one in the tooltip, and `→` the datasets their tables are in, the tables in the tooltip |
| Phase | no | `PhaseBadge` with the reason as `title`; a `→ <desired>` line appears when reconciling |
| Rate | yes | `formatRate(rowsPerSecond)` |
| Rows | yes | `formatCount(rowsCommitted)`: the rows its tables' current snapshots hold, as their committer last reported them — the Overview tab's *Rows committed*. Not `rowsWritten`, the runtime's counter, which also counts what a member wrote and lost to a restart, then wrote again |
| Size | yes | `formatBytes(tableBytes)`: those snapshots' data files — the Overview tab's *Table size* |
| Tables | no | How many tables it writes: one per source topic |
| Updated | yes (default, desc) | `RelativeTime` |

Clicking a sorted column flips direction; a new column defaults to ascending for name, descending
for numbers. Whole rows are links to the detail page.

Empty states differentiate "No matches" (with **Clear filters**) from "No materializations yet"
(with **New materialization**).

### Create dialog

| Field | Required | Notes |
|-------|----------|-------|
| Source topics | yes | One or more of the cluster's topics, up to the control plane's `max_materialization_topics` (20); see [Topic picker](#topic-picker). Each becomes a table, in the dataset chosen on its row (see [Tables](#tables-dataset-and-name)). Focused when the dialog opens. Fixed once created |
| Name | no | A label for people: free text, at most 128 characters, not unique, changeable at any time with **Edit**. Left empty, the materialization goes by its id. It names no table: SQL reads `<dataset>.<table>`, as the line under the topics shows with the first table |
| Data protection | no | Beside the name, not behind Advanced: the policy in a line — *Email addresses hashed · Phone numbers hashed · … · default* — opening into a row per kind of sensitive data with **Off**, **Hashed**, **Partial** and **Withheld** (only the masks the kind allows: a secret is never hashed), each kind's examples and what its partial keeps. Pre-set from the domain's *Protected by default*; **Use the default** returns to it. Choosing otherwise needs *Choose data protection* on every dataset the tables go in: without it the control is locked, naming the datasets, and a changed choice blocks **Create**. A choice that hashes in a domain with no protection key says the control plane will refuse it (control-plane handbook [22](../../docs/handbook/22-data-protection.md)) |
| **Advanced settings** | | Collapsed; its summary reads *Partitioning, replicas, autoscaling, labels, tombstones*. Opens on its own when the control plane rejects one of its fields. It begins with where the tables go and what each member gets — *Its tables are created under the deployment's destination, `gs://my-lake`, and each runtime member gets `64Mi` of storage* — which the dialog does not ask for |
| Partitioning | no | **By day** (the default, from `default_partition_granularity`), **By hour**, **By month** or **Not partitioned**: how the tables are partitioned by each record's Kafka timestamp. Fixed for the tables' life |
| Replicas | no | Placeholder shows the default; checked against the control plane's maximum. The members share every topic's partitions; the picker says how many of them the topics can keep busy. Can be changed later with **Edit** |
| Autoscale | no | Checkbox, on by default: *Adds members while it is far behind its topics, up to 16, and goes back to its replicas once caught up.* With autoscaling off for the deployment it says so, and left on, the materialization autoscales once it is turned on there. Sent only when off. Absent against a control plane before 4.4. Can be changed later with **Edit** |
| Labels | no | `key=value` pairs, comma separated |
| Skip tombstones | no | Checkbox, off by default. When on, null-value records are acknowledged without a row; off, each is a row with a null payload |

The destination, the runtime's namespace and name, and its storage are the deployment's since 4.0
(handbook 04 §4.5.2): the dialog has no field for them. While the control plane reports no
destination, an alert at the top says *No materialization can be created yet: this control plane has
no destination for tables*, pointing to Settings › Materializations › Destination, and **Create** is
disabled. The defaults come with the console's config, kept for a minute, so a destination set
elsewhere than the console's Settings page shows within that.

### Topic picker

The source topics are chosen from every topic in the cluster (`GET /api/topics`), searched in the
browser so typing never waits on the network. Choosing a topic adds it and keeps the list open for
the next; choosing a chosen one removes it.

| Behaviour | Detail |
|-----------|--------|
| Search | Every whitespace-separated term must appear in the name. Exact name first, then names that start with a term, then terms that start a word (after `.` `-` `_` `/` `:`), then substrings; ties go to the shorter name. Matches are emphasised |
| Internal topics | Topics with a `category` are hidden behind **Show internal topics (N)**. A search that matches only hidden topics says so and offers to show them. Shown ones carry an **Internal** badge whose tooltip names the category |
| Chosen topics | Listed under the field in the order chosen — the order of the tables — each with a remove button and its table's row: the dataset and the table's name (below) |
| Already materialized | A topic read by a live materialization carries **Materialized** (or **Materialized ×N**), in the list and among the chosen; the tooltip names them. Each materialization reads a topic independently, with a consumer group of its own |
| Partitions | One column, with replication factor in the tooltip. Under the field: one topic's partitions and replication factor, or for several the partitions in all and how many runtime members they can keep busy — topics with as many partitions share members (`assignableMembers`, the control plane's `scale.py`) |
| The limit | At `max_materialization_topics`, unchosen topics are disabled and the hint says so |
| Long names | Truncated in the middle, so the end, where versions and suffixes differ, stays visible |
| Keyboard | Typing on the closed field opens the search with that character. `↑` `↓` `PageUp` `PageDown` move, `Enter` chooses, `Esc` closes the list without closing the dialog |
| Unlisted names | A typed name that is not listed can be used through **Use "name"**. The field then warns that the name is not in the cluster's topic list. On **Create** the control plane asks Kafka again, and refuses a topic it still does not list (below) |
| Freshness | **Updated N ago** in the footer; the reload button asks Kafka again. A stale list says Kafka is unreachable |
| No topic list | If the list cannot load, the picker says why and still accepts a typed name, marked as unchecked |

Only the rows in view are rendered, so tens of thousands of topics scroll and filter like a dozen:
on 25,000 topics, results render about 17 ms after each keystroke.

### Tables: dataset and name

Each chosen topic's row reads `→ [dataset] . [table]`, as SQL will read it
(`features/datasets/TableDraftRow.tsx`, the logic in `tableDrafts.ts`):

| Part | Behaviour |
|---|---|
| Dataset | A searchable list of every dataset (`DatasetPicker.tsx`): name, description and how many tables it holds; those the person may add tables to first. A dataset they cannot write is listed locked, *no write access*, and cannot be chosen. A new row starts with the dataset chosen last, or the only one the person may write. With `datasets.create`, **New dataset** at the foot creates one in place — a name, fixed once made, and an optional description; *You become its owner, and everyone signed in can read it* — and chooses it once the directory has applied it |
| Table name | Derived from the topic as the control plane derives it (`payments.v2` → `payments_v2`; `tableNamesForTopics`, `_2`, `_3` for names two rows would share) and following the topic until typed over |
| Checks, as typed | A dataset chosen, and one the person may write; a name SQL can read (a lowercase letter first, then lowercase letters, digits and underscores, at most 63 characters, not a reserved word); no two rows with one name; not a name the dataset already holds — *sales.orders is taken; choose another name*. *Choose the dataset its table goes in* waits for a submit |

**Create** stays disabled until a topic is chosen, every row is clean, the label is valid, and a
destination is available (given, or the default). Labels are parsed client-side;
a malformed pair raises an "Invalid labels" toast naming the offending pair and does not submit.
Submitting waits until the tables' names are claimed (08 §8.6), so a name someone else claimed first
is said on its row — *sales.orders is taken* — and the dialog stays open, the dataset list refreshed.
A topic Kafka does not list — missing, or hidden from the deployment's Kafka user — is refused the
same way, on its row: *Kafka has no topic ghost, or the deployment's Kafka user may not describe it.*
(handbook 04 §4.5). That row keeps **Create** enabled: nothing on the row can fix it, and once the
topic exists, or the user may read it, sending again succeeds.
On success: a toast (*…is being discovered and deployed*, or that its names are still being claimed
when the directory had not answered in 10 seconds), the form resets, the dialog closes, and the app
navigates to the new detail page. Any other failure: an error toast, and `ApiError.details.field`
highlights the offending input.

---

## 11.4 Detail (`/materializations/:id[/:tab]`)

Data: `useDetailStream(id)`. The stream subscription switches the whole store to the 1.5-second
detail cadence while the page is open.

### Header

Title is the label (`name`) or the id. Subtitle is the source topics (the first three, then `+N`); the destination
is in the Specification panel. The meta row carries: a back
link, the copyable id, `PhaseBadge`, `HealthBadge`, a "Reconciling toward X" note when desired and
observed disagree, and the `LiveIndicator`. The health badge says **Healthy**, **Unhealthy** (the
`degraded` level), **Unreachable** or **Unknown**; its tooltip is the health's message — *8 of 10
members restarted in the last 30 minutes: out of memory.* — or its reason.

### Actions

Each action also needs its permission (`materializations.resume`, `.stop`, `.edit`, `.delete`); a
person without it does not see the button.

| Button | Shown when | Behaviour |
|--------|-----------|-----------|
| Resume | the desired phase is `stopped` (also while it is still stopping) | 202, toast, converge; scales back to the replica count |
| Stop | the desired phase is `running` and the materialization has a runtime | Opens a confirmation dialog, then 202, toast, converge to **Stopped** |
| Edit | it has a runtime and is not being deleted | Opens the edit dialog (below) |
| Delete | `observed_phase ∉ { deleting, deleted }` | Opens a confirmation dialog |

The stop dialog names the topics it stops consuming and the replica count Resume restores, says that
the tables stay readable while the maintenance app goes on compacting them and expiring old
snapshots, and that rows still being batched are read again on resume, without duplicates: the
committed offsets only cover rows in the table (handbook [10 §10.7](../../docs/handbook/10-runtime-internals.md)). While stopping, the
header shows **Deploying** and "Reconciling toward Stopped"; once the KasprApp has no members it
shows **Stopped** with health **Unknown**.

### Edit dialog

Changes what can change after creation: the **Name**, the **Source topics**, the **Replicas** and
**Autoscale** (`features/detail/EditMaterializationDialog.tsx`; the plan in `topicChange.ts`).

**Name** is the label, as on create; changing it alone touches nothing else, and needs no access to
any dataset.

**Source topics** is the create dialog's topic picker, filled with the topics read now. Each chosen
topic shows the table it writes: its own, `sales.orders`, fixed, for a topic already read; the kept
table, *kept table, written again*, for a kept table's topic; and for any other a new row, a dataset
and a name, as on create, the name apart from every name the materialization has — kept tables' and
dropped ones' included. Topics already read keep their order, so reordering alone restarts nothing
(`keepOrder`). A topic the change starts reading that Kafka does not list is refused as on create:
on its row, or under the topics for a kept table's topic, which has no row of its own. While an earlier change's new tables are still being claimed, the dialog says so —
*Adding sales.order_refunds: their names are being claimed* — and the topics cannot change until it is
done.

| When the topics change | The dialog |
|---|---|
| A topic is removed | **The removed topic's table**: for each, *Keep read-only* or *Drop with its data*, under a note that dropping cannot be undone. **Save** waits until each has a choice |
| The materialization has kept tables whose topics stay removed | **Kept tables**: a checkbox each, *Drop finance.payments and its data · topic payments* |
| Topics are added, kept tables written again, or tables removed | A note: which datasets it changes tables in; that new tables' names are claimed first, then their topics sampled and their tables created; then that the runtime stops until its consumer group has emptied and starts again with the new set, each table where it left off — or, stopped, that it reads them once resumed |
| A table it would add, write again, keep or drop is in a dataset the person cannot write | Its choices are locked, and a note in red names the tables and their datasets: *ask one of its owners for write access*. **Save** stays disabled |

**Replicas** starts at the current count, and must be a whole number from 1 to the control plane's
maximum. The hint gives the current count and the chosen topics' partitions, and warns when members
would outnumber what the topics can keep busy — topics with as many partitions share members, so 32,
32 and 8 partitions keep at most 40 busy ("2 would have none to read"); on a stopped materialization
it says the count applies on resume.

**Data protection** is the create dialog's block, starting from the policy in force (*· current*;
**Keep the current protection** returns to it). Changed, it says what changes — *Phone numbers:
hashed → partial* — and that it applies to records written from now on, within seconds, without a
restart, while rows already written keep their values. It needs *Choose data protection* on every
dataset the materialization writes into, those of topics the edit adds or writes again included;
otherwise the block is locked, naming them, and a changed choice blocks **Save**. A protection-only
edit restarts nothing and leaves the materialization **Running**. With a changed policy, someone who
may also run table maintenance sees **Also mask rows already written** (off), and under it **Purge the
plain text now** (on): the edit's `protection.reprotect`, rewriting the files written before once each
table records the change (§11.10a); the toast then says *for records written from now on and then for
the rows already written*.

**Autoscale** is a checkbox, as on create; changing it alone is a valid change, and leaves the
runtime as it is unless it takes members away. Its hint says what it does now: *Running 12 members
while it catches up. Turned off, it goes back to 4 at once.* while scaled up, or that autoscaling is
off for every materialization when the deployment turned it off. The toast says *autoscaling on* or
*off*. Below, read-only: the id, storage per replica and the destination — fixed when the
materialization was created.

**Save** is enabled once something changes. A change adding tables waits for their names to be
claimed, as a create does. The toast says what the control plane planned — *Shop: adding
sales.refunds; keeping finance.payments read-only. Watch the phase for convergence.* — and a refusal
shows under the field it names, a name taken on its row. Dropping only kept tables restarts nothing; any other change of
topics shows the materialization **Deploying** with "Changing the runtime's topics" for about five
minutes (handbook [07 §7.9a](../../docs/handbook/07-control-plane-workflows.md)).

The delete dialog states plainly that the runtime, its Kafka topics and its consumer group are
removed, and so are its Iceberg tables with all their data — unless the control plane runs with
destination cleanup turned off, when the tables leave their datasets but stay readable by path — and
that this cannot be undone. It lists every table it takes out of its dataset, `sales.orders`, kept
ones and ones being dropped included: anyone's queries of them stop working, and their names are free
again. Deleting needs write access to every one of those datasets; without it a table carries a lock,
the dialog names the datasets — *ask their owners* — and **Delete** is disabled.

If the control plane is unreachable and no detail has ever been fetched, the page renders a
"Cannot reach the control plane" panel with the error instead of an empty shell.

### Tabs

URL segments: `` (Overview), `schema`, `data`, `table`, `protection`, `runtime`. The Schema, Data and Table tabs
show one table at a time, named by `?table=` (the first table when absent) and chosen with the
table picker each shows when there are two or more; switching tabs keeps `?table=`. Every tab needs
`materializations.view_details`; beyond it, **Schema** and **Protection** need `dataset.view_schema` and **Data**
`data.query`, each on at least one of the tables' datasets. Those tabs choose among the tables the
person may use there: the others are in the picker, *· locked*, and never shown — a `?table=` naming
one opens the first allowed instead. A tab someone may not open is not shown, and a link to it opens
the Overview. **Export** on the Data tab needs `data.export` on every dataset the run read, and **Run
now** on the Table tab `materializations.maintain`. The rule is the server's (12 §12.2): the console
only follows it.

---

## 11.5 Detail → Overview

| Card | Value | Hint |
|------|-------|------|
| Ingest rate | `snapshot.rowsPerSecond` | Members reporting · 1-min window; sparkline |
| Write rate | `snapshot.bytesPerSecond` | Parquet bytes sealed · 15-min trend; sparkline, green |
| Rows committed | The rows in every table's current snapshot together (`GET …/data/summary` per table), exact | Tables · last commit · **Complete through**, the earliest across the tables; the skipped-record count, which opens the reasons |
| Table size (**Tables' size** for several) | The current snapshots' bytes, together | Data files · average size · snapshots kept |

Both rate cards come from each runtime member's own heartbeats, summed
([05 §5.4](./05-live-store-and-realtime.md#54-throughput-derivation)); they read `—` until every
member has reported twice, about half a minute after the console starts watching.

**Rows committed** and **Table size** read the tables themselves, refreshed every 15 s — the same
source the Data tab opens with, so the two always agree. The runtime's own counters are close but are not
the same thing: they include rows still being batched or committed, and count again the rows a member
reads a second time after a restart (the table does not hold them twice). They stand in, labelled as
such, only while the table cannot be read or nothing has been committed yet.

**Throughput** — the last ~20 minutes as a chart with a value axis, clock times and a hover (or
arrow-key) readout; **Rows / Bytes** switches the series. It grows to the height of the panel
beside it.

**Reconciliation** — `desired_phase`, `observed_phase`, `phase_reason`, `observation_source`,
`last_transition_time`, `last_heartbeat`, `last_resource_observation_time`, **Tables** (`N of M
created` until every table exists), **Last commit** (the newest of any table) and the runtime
version. Below the grid, `phase_message` in muted text and
`last_error` in red when present. While autoscaling has the runtime scaled up, a line with an
upward chevron: *Scaled up to 16 members 5 minutes ago: 110,869,158 records behind, about 22 h 43
min to catch up with 1 member. Back to 1 once caught up.* With two or more runtime members, a summary of their states and
**Show N members**: each member's state (*loading the table* while it reads its pointer), its own
rows/s, when it last reported, and a **committer** badge on the member that commits.

**Tables** — one row per table, in the order of its topics, then the kept tables, then any being
dropped: the table as SQL reads it, `sales.orders` — its dataset a link to the dataset's page, its
name a link into the Data tab with it chosen when the person may query it, else a lock naming the
dataset; an icon into the Table tab — above its topic, its rows, size and files from the control plane's record, its topic's lag, its schema id,
its last commit, and its state — **not created yet** until the control plane has sampled its topic
and created it, **kept · read-only**, **dropping**, **dropped · files kept**, its last error in red,
or what runs on it (files pending, a maintenance job). The title counts both, *Tables (2 · 1 kept)*
(`features/detail/TablesPanel.tsx`).

**Consumer lag** — how far the tables trail their source topics, all together. It is measured from the runtime
consumer group's committed offsets, and a runtime commits an offset only once its row is in the
table, so lag counts records not yet committed: a caught-up materialization of a busy topic shows up
to a batch per partition plus a commit interval, and its lag falls in steps as commits land
(control-plane handbook [10 §10.7](../../docs/handbook/10-runtime-internals.md)):

| Part | Content |
|------|---------|
| Headline | The latest measured lag, records behind |
| Trend | **Catching up** with the drain rate and the time left at that pace (`−5,914/s · about 4h 38m to go`), **Falling behind** with the growth rate, **Holding steady**, or **Caught up** — the least-squares slope of the last ten minutes of measurements |
| Chart | One point per measurement over **15m / 1h / 6h**, on an axis fitted to the values, so a large backlog falling slowly is a slope rather than a flat line at the top of a zero-based chart |
| Partitions | `N of M partitions behind`, and **Show M partitions** for each partition's lag with a bar against the largest; with several topics each is named `<topic> P<n>`, and each topic's own lag is in the Tables panel |
| Footer | When it was last measured and how often measurements arrive |

**Specification** — the **Source topics**, the **Datasets** its tables are in, how many
**Tables**, the tables' **Location** (the directory each table's own sits under),
**Replicas** (with "none running while stopped" when stopped, or "16 while catching up" while
autoscaling has it scaled up), **Autoscaling** (**On · up to 16**, **Off**, or **Off for every
materialization**; absent before 4.4), storage per replica, `namespace`,
`runtime_mode`, `resolved_runtime_name`, `created_by`, `created_at`, `resource_profile`,
`spec_version`, **Tombstones** (**Ignored** or **Kept as rows**), **Partitioned by** (day, hour,
month or nothing) and **Data protection** — the kinds in force as chips, *Email addresses · hashed*,
or *None: values are written as they arrive*, with how far the policy has reached: **In force ·
revision 2**, **Applying revision 3** (its tooltip naming the tables and members behind), **Applies
on resume** or **No member reporting** — then label and annotation chips when non-empty.

---

## 11.6 Detail → Runtime

Everything the control plane observed about the Kubernetes resource.

**Kubernetes resource** — `kaspr_app_name`, `namespace`, `resource_exists`, `available_members`,
`metadata_generation / observed_generation`, rollout state, `rebalance_required`,
`rebalancing_members`, and the observation timestamp. Below: two condition cards (Ready,
Progressing) each showing status badge, humanized reason, message, and when it last changed.

**Managed agents** — one row per entry in `managed_agent_states`, with a state dot coloured by
`running` / `paused` / `stopped` / `failed`. The header summarises
`R running · P paused · U unknown of T`.

**Runtime binding** — `kaspr_app_name`, `namespace`, `runtime_mode`, `template_version`,
**Schema when applied**, applied-at, the copyable `last_applied_spec_hash`, and the managed agent names
as badges. Empty state explains the binding is written when `apply_runtime` completes.

---

## 11.7 Detail → Schema

One table's schema, as the control plane projects it (06 §6.3). Each table of a materialization has
a schema of its own that evolves on its own; with two or more, a table picker above the rail
chooses which (`?table=`): the tables whose datasets the person may see the schemas of
(`dataset.view_schema`), the others listed locked. Two columns: a rail of every schema the table has
had and the selected one.

### Versions rail

"Every schema the table has had, by its data." One entry per schema id, newest first, with `current`
on the table's own and what it changed — `3 added · 1 forked` — and when its data begins, read from
the table (06 §6.3, *When the data had each field*):

- **Schema 0** reads *Initial · sampled*: discovery sampled it from both ends of the topic, so it
  can hold shapes from any part of the topic's history. Its tooltip says so, with when it was
  committed.
- **A later schema** reads *data from Apr 15, 2025*: the earliest first value among the fields it
  added, forked or collapsed, in UTC, as precise as the table's partitions — `Apr 1 – 30, 2025` for a
  month table. A version is numbered by when the runtime met its shape, which a backfill or the
  sampled schema 0 can put out of the data's order; the date says when the records had it.
- Until the table can answer (not created, nothing committed, data access off) it falls back to
  *committed 2 days ago*, the control plane's `since`.

The selected schema's description reads `19 fields · data from Apr 15, 2025 UTC · committed Oct 23,
2026, 11:18:49 AM`. Selecting one shows it; the current one comes from the live snapshot with no
extra request.

### Callouts

Above the current schema, when they apply:

| Callout | When |
|---|---|
| *N fields are kept as JSON text: their values changed type too often, or their objects grew too wide, for columns of their own.* | a family collapsed (handbook 09) |
| *The schema is at its field limit: new fields are not added, and their values go to `_unmapped`.* | a change was rejected at `max_schema_fields`; it points to Settings › Schema evolution |
| *Values went to `_unmapped` in the last hour (N in all, last …).* | the runtime reported unmapped values recently |

### Fields

Payload fields first, in schema order with each fork family together, then the Kafka metadata.

| Column | Content |
|--------|---------|
| Field | Its logical name, ancestors dimmed (`customer.` + **email**); below it, for a fork or a renamed key, `written to total__v2`, `since schema 3` for a field newer than schema 0, `first seen Sep 1, 2025` for a field that was not in the table's first records, and `last seen Feb 9, 2026` for one superseded or no longer arriving (its last value a day or more before the table's last record). Dates are UTC |
| Type | The Iceberg type as a chip (`long`, `struct`, `list<string>`); a JSON field reads `json` |
| Required | `required` when the field is |
| State | `current`, `superseded` (a newer fork took over), `collapsed` |
| Id | The Iceberg field id |

A protected field carries a shield beside its name, its tooltip saying its kind, its mask and the
schema it was protected from (*Masked · Email addresses · hashed since schema 0*).

**Tree** nests the same fields by path. A filter matches name or type; **Hide superseded** hides the
older members of each family, rendered at 55% opacity otherwise.

### Timeline

**Timeline** lists the table's changes by the day the data had them, newest first, whatever schema
version holds each: a field's first value (*first value*, with its type), a type change (*changed
type from string to struct · its string values run until Aug 31, 2025*), a family kept as JSON from
then on, a field's last value when it stopped arriving, and values first going to `_unmapped`. A
field that arrived inside a struct or list added the same day is folded into it, so a new object is
one entry. Each day is headed by the span its entries' files cover — `Apr 15, 2025` in a day table,
`Apr 1 – 30, 2025` in a month table — and the panel says how precise that is for the table's
partitioning. The table's first records are one group, collapsed behind **Show**: *first records: 5
fields held values from the start*. Clicking a field name shows it in the Fields view.

### Changes

**Changes** diffs the selected schema against an earlier one (06 §6.4): each entry with its kind —
added, forked, collapsed, type widened, required changed, superseded — its field and type, and for a
changed field what changed. A short hint explains each kind; a fork, for example, says the older
field keeps the values written before it.

---

## 11.8 Detail → Data

One way to ask a question: choose a time range, add filters or write SQL, press **Run**. Everything
on the tab — the opening view, paging, profiles, exports — reads from the run that produced what is
on screen. How a run executes is the server's business; there are no modes, previews, or jobs to
choose between (see [07](./07-data-access-and-duckdb.md)).

### Opening the tab

The tab runs the preview of the chosen table at once — the first table, or the one `?table=` names —
every committed row of its current snapshot, newest first, with its **exact** row count — `✓ 26,060,201 rows · All time` — and the first page. The
manifests know every file's row count, so this reads nothing but the page itself and is on screen at
any table size. When the URL
names a run (after a refresh, say), the tab re-attaches to it instead, progress and all.

| Outcome | Rendering |
|---|---|
| Loading | Skeletons |
| Nothing committed yet | "No data to query yet" + *Rows appear here once the first records are saved* + **Check again** — unless live tail is on: the tab then runs at once, its rows from the tail until the first commit |
| The table cannot be read | "Object storage is not reachable" + the reason + **Retry** |

With the live tail on — every materialization has one — every run also holds the records past its
snapshot that the runtime has not committed yet, from the tail app ([07 §07.9](./07-data-access-and-duckdb.md)): the
newest of all, first in newest-first order.

### Toolbar

| Control | Behaviour |
|---|---|
| Column list toggle | Shows or hides the column rail |
| **Table** | With two or more tables (not in SQL, which names its own): which one the structured view reads, among those the person may query; the others are listed *· locked*. Choosing one runs its preview at once, with the filters and column choices cleared, since its columns are its own; the choice is in the address bar (`table=`) |
| **Time range** | Splunk-style picker (below). Choosing a range does not run anything |
| **Newest first / Oldest first** | Row order of the structured view |
| Filters | Chips, and **Filter** to add one: find the field, then choose an operator and a value, with value suggestions from the run on screen |
| **SQL** | Opens the editor with a statement for the current filters |
| **Live** | On a running materialization — every one has a live tail — in a console and control plane with it on, in the structured view: on by default. The toggle goes at once when the materialization is stopped, and within two minutes when the deployment's switch is (the tab reads the console's config every minute); turned back on, it returns and Live picks the tail up by itself. While it is on, the tab is seen, the run has succeeded, the first page is on screen (newest first) and no row is open, the run takes in what arrived every `liveTailRefreshMs` (1.5 s); a commit replaces the run with one pinned to the new snapshot. Its dot pulses while it follows and turns amber while it waits; following says nothing more. Off is remembered in the address bar (`live=0`) |
| **Run** / **Stop** | The tab's only Run. While a run works it is **Stop**. `⌘↵` / `Ctrl+↵` anywhere on the tab |
| **More actions** (⋮) | Right of **Run**: **Peek**, **Share** and **Export…** (below) |

Changing the time range, a filter, the order or the statement **does not run anything**. The Run
button gains a ring and the status line says **Changes not run yet**, so the screen never disagrees
with the controls silently.

### Time range

Presets in three columns, like Splunk's:

| Relative | Last | Other |
|---|---|---|
| Today, Week to date, Month to date, Year to date, Yesterday, Previous week, Previous month, Previous year | 15 minutes, 60 minutes, 4 hours, 24 hours, 7 days, 30 days | All time |

Below them, collapsible: **Relative** (last *N* minutes/hours/days/weeks), **Date range** (whole
local days, both ends included) and **Date & time range** (local, end exclusive).

Every preset shows how many rows it covers, from the manifests — `≤1.2M` when files only overlap
the range, an exact figure when they lie wholly inside — so a narrower, faster question is visible
before it is asked. The footer says where the data sits on the time axis, and how many rows have no
timestamp (those appear only under All time).

Ranges filter on each record's **Kafka timestamp**, the `_kafka_timestamp` column, in the browser's
time zone, which the picker names (`America/New_York`); weeks start on Sunday. A relative range is
resolved when Run is pressed, so a run is pinned to the window the user meant. The table in SQL
is bounded by the same range.

### Status line

The status line is a row of its own at the top of the table, between the toolbar and the grid. It
says what the run is doing or what it found, in a few plain words, and nothing the reader has to decode: not the range, which the toolbar shows, nor files, bytes or snapshots. Every
run reads the table as it is now; the Data tab offers no older snapshot (the runs API still takes
`asOf`, 08 §8.16).

| State | Shows |
|---|---|
| Running, structured view | `Scanning · 1,460,000 of 3,120,000 rows · 46% · 51,230 matched so far`, elapsed, `~3 s left`, a progress bar; the toolbar's **Stop** ends it |
| Running, statement | `Preparing · 26%`, then `Running · 45%` (or indeterminate), then `Counting rows` when it outgrew what is kept |
| Succeeded | `✓ 1,234,567 rows` (how long it took in its tooltip) |
| Failed | The error; a statement's is also marked in the editor |
| Cancelled | How long it ran and how many rows it had found |

**More actions** (⋮), right of **Run**, lists **Peek**, **Share** and **Export…**, always all three:
one that does not apply is disabled, the reason in its tooltip — Peek *Available while this table is
catching up*, Export until the run has succeeded, without `data.export`, or with nothing to export.
**Export…** opens its formats in the same menu, with a way back.

With live tail, every run holds the records past its snapshot. What the tab says of them is in plain
words — how far behind the view is, never which part of the system is. Nothing while Live simply
follows: the toggle's pulsing dot says so. How far behind the newest records the view is, or why
Live stopped, is at the status line's right, in amber — an hourglass beside a count, turning every
3 s (still when the system asks for reduced motion). It stays while a run Live started at a commit
works. A backlog's count is the
table's topic lag as the Overview measures it, once a minute, so it moves at the Overview's pace
whether or not Live refreshes the run; until a measurement is in, the tail's own count. Anything
else is a few muted words on the status line.

| Tail | Shows |
|---|---|
| Following | Nothing beyond the toggle's pulsing dot |
| Live off, or a statement | Nothing |
| Paused while hidden, past page 1, or with a row open | *Live paused while this tab is hidden* (… *past the first page*, … *while a row is open*) |
| Short of the head (`truncated`) | *4,000 records behind* — the records not shown — in amber at the status line's right |
| Backlogged | *2,500,000 records behind* — the topic's measured lag — in amber at the status line's right; **Peek** is enabled |
| Waiting for a schema change | *2 rows have new fields* — their new fields are under `_unmapped` until the table adds them |
| Behind the topic | *Live is 7 s behind* |
| Warming up, not followed yet | *Live is starting*; asked again each refresh |
| Moving between members | *Live is reconnecting* |
| Unavailable | *Live is unavailable* in amber at the status line's right, the reason in its tooltip; the run shows the rows saved so far |
| The range has ended | *This time range has ended* — and Live asks nothing |
| Session refused | *Live stopped: sign in again to follow new records*, in amber at the status line's right |
| Switched off while followed | *Live is off*, the reason in its tooltip, until the toggle goes |

**Peek** opens a panel over the grid, *Newest incoming records*, with the newest
200 records on the topic, saved or not, in the table's columns, labelled *not in the table yet*; it
reads again every `liveTailRefreshMs` while open and seen, quietly — its spinner shows on the first
read only — and closes itself once the table has caught up. It is never part of the run, its export
or its profiles.

The figures for the structured view are exact: rows of the files counted so far over rows of every
file in range, both from the manifests, so the bar moves at the rate work is actually done. When the
table has a newer snapshot than the one a result read, **Newer data available** offers to run again.

The row count is always shown when a run finishes. A statement's result larger than
`CONSOLE_RESULT_ROW_LIMIT` keeps its first rows for paging and export — `first 1,000,000 kept` — but
its total is still counted exactly.

### Grid and paging

Rows stream in as a run counts: the first page appears as soon as the newest files are counted,
long before the total is final. While a filter has matched nothing yet the grid says **No matching
rows yet** rather than showing placeholder rows. The footer offers first, previous, a page box
(`Page 2 of 31,201`, with `+` while counting), next and last, and rows per page (50–1,000); the
exact count is the status line's, at the top.

Any page is one direct read of one file's rows, so page 2 and page 40,000 cost the same, and the
next page is read ahead — on the server and in the browser — so turning pages does not wait. The
toolbar's order: **Newest first** means files holding the latest Kafka timestamps first, latest
offsets first within each; a global sort is a statement's `ORDER BY`.

Column headers do not sort. Clicking a cell copies it; the row gutter opens the row drawer (below).
`null` renders as italic `null`.

Timestamps with a time zone — `_kafka_timestamp`, and any `TIMESTAMP WITH TIME ZONE` a statement
returns — render in the reader's time zone as `2026-10-04 19:59:59.685`, the zone named in the
column header (`ET`) and the UTC value in the tooltip, so a row inside "Today" reads as today.
Copying, filtering and exporting use the stored UTC value. Payload strings that happen to hold a
time are shown as written.

### Filter builder

**Filter** opens a builder in two steps, starting fresh each time.

1. **Find a field.** A search box over every column — fields first, then the Kafka metadata — with
   the best matches first: every typed term must appear, and an exact name, a name starting with a
   term, or a term starting a path segment (`email` in `customer.email_hash`) ranks ahead of a plain
   substring. Matches are emphasised. `↑` `↓` `PageUp` `PageDown` move, `Enter` chooses, `Esc`
   closes. It replaces a select of every column, which on a wide schema was a long scroll.
2. **Say what it should be.** The chosen field heads the step with **Change** to go back; then the
   operator — ordering comparisons only for numbers and times — and the value, with the field's
   most frequent values in the run on screen offered as one-click choices. That list has four
   distinct states — loading, values, none, and failed — because a single spinner cannot tell
   "still working" from "this will never resolve"; it says **Partial** when the range was too wide to
   summarise in time.

### Column rail

**Fields** and **Kafka metadata**, grouped, searchable, with visibility checkboxes, **only**, and a
pin that freezes a column beside the row numbers. By default the grid shows the record's timestamp
first, then the fields; the other metadata is a click away. Fields follow schema order and carry
their logical names; a forked field appears once with its whole history folded in. Only the
runtime's own columns count as Kafka metadata (`_kafka_*`, `_materialized_at`, `_unmapped`), so a
field such as `_id` stays with the fields. A column's type reads compactly — `STRUCT`, `MAP`, `LIST`
— before the scalar types. A masked column carries a shield, its tooltip saying its kind and mask,
read from the table's own `materializer.protection` (06), so it is right whatever the materialization's
policy says now.

Expanding a column profiles it over the run's rows — distinct count, null share, top values, range —
under the 8-second stats budget, labelled `partial` when it ran out of time. A top value filters to it.

Drag the divider to widen the rail (264–720 px); the width is remembered per browser.

### SQL

**SQL** opens the editor above the grid with a statement for the current filters on the chosen
table — `SELECT * FROM sales.orders WHERE … LIMIT 1000`. Every table of the materialization the
person may query, kept ones included, is there by its canonical name, `<dataset>.<table>` — the name
an engine on the catalog uses — so a statement can read any of them and join them, across datasets
(`… FROM sales.orders o JOIN finance.payments_v2 p ON p.order_id = o.order_id`); the header names
them and says they hold the rows of the chosen time range. Completion offers those tables, the
columns of the one being typed after, and `iceberg.<dataset>.<table>` for each — the same rows with
the native columns, structs and lists as an external engine sees them. A name that is none of the
tables, a bare `orders` included, fails with the list of those that are; one in a dataset the person
may not query is refused, naming the dataset (08 §8.1). Columns carry their logical
names, quoted: `"status"`, `"customer.email"`. A statement runs in a sandbox that can read nothing
but the snapshot's files (07 §07.6). **Back to filters** leaves SQL.

The statement is checked half a second after typing stops — bound, never run — and an error is
underlined in the editor at the token the engine pointed at. A failed run marks the same way. CodeMirror
loads only when the editor first opens.

With the live tail on, a statement reads each table's tail once, when it runs, and
the panel offers **Auto re-run**, off by default: a new run five seconds after the last one
succeeded, never while one runs, and not while the editor holds another statement than the one on
screen.

### Export

Appears only once a run has succeeded, for someone who may export from every dataset it read. It
downloads that run's result — every row, in the order
shown — as CSV, JSON, JSON Lines or Parquet, and says how many rows that is. It never re-runs the
query and never exports just the page on screen. Text formats stream, so the download starts at once.

**JSON layout** chooses how JSON and JSON Lines lay out a row, and is remembered per browser:

| Layout | A row reads |
|---|---|
| Nested (default) | `{"customer": {"email": …}, "items": [{"sku": …, "qty": …}]}` — the record rebuilt from the column paths, `json` fields decoded |
| Flat | `{"customer.email": …, "items[].sku": [ … ]}` — one key per column, as the grid shows it |

CSV and Parquet are always flat.

### Sharing and refreshing

The address bar is the view: time range, filters, order, statement, page, page size, visible and
pinned columns, Live (`live=0` when off), and the run on screen. **Share**, under More actions, copies it. A link made
before snapshots left the Data tab (`asof=`) opens on the latest data. A link made before columns dropped the
`payload.` root still opens: its filters, columns and pin are rewritten to the current names once
the run's columns are known. Reloading the page re-attaches to the
same run if it still exists — a long query keeps going and the status picks up where it was — and
otherwise runs the view again.

### Row drawer

Every field of one row on one vertical axis, grouped like the rail and including hidden columns, with
per-field copy, filter-to-this-value, whole-row JSON copy (in the JSON layout chosen under
**Export…**), and `↑`/`↓` (or `j`/`k`) to step rows. A masked field carries the rail's shield. A timestamp shows its local time with the UTC
value beneath it.

---

## 11.8a Versions

The sidebar footer carries the console's version; clicking it opens **About this deployment**,
listing the console and the control plane side by side. They are deployed independently, so the
dialog says plainly that they can differ.

A materialization's **runtime** version appears on its Overview, in the Reconciliation panel beside
Schema — a runtime keeps the build it was created with until its spec is re-applied, so
this is the one that answers "what code is actually processing my data". It is blank until that
runtime has reported status at least once.

See [handbook 16 — Versioning](../../docs/handbook/16-versioning.md) for what each number means.

---

## 11.8b Settings (`/settings/:section`, `/d/<domain>/settings/:section`)

Where an administrator changes how the console, each domain's control plane and its runtimes behave,
without a redeploy. `features/settings/`. Two pages share one layout (handbook 17 §17.8):

- **The console's** (`/settings`, under **All domains**): the console's own settings, stored in the
  settings home, the **Domains** in its registry, **Your account** in each domain, and every domain's
  **Change history** merged.
- **A domain's** (`/d/<domain>/settings`): its control plane's and runtimes' settings, its directory
  — users, groups, roles, the audit log — and **Sign-in**.

### Layout

A section list on the left (a picker under 768 px) in groups. Each section shows only to people with
its permission — on the console's page, in the settings home; on a domain's, in that domain:

| Page | Group | Sections | Needs |
|------|-------|----------|-------|
| Console | Account | Your account | signed in to any domain |
| Console | Console | Live updates, Data explorer, Live tail, Security, Topics & Kafka, Diagnostics | `settings.view` in the settings home |
| Console | System | Domains | signed in |
| Console | System | Deployment | `settings.view` in the settings home |
| Console | System | Change history | `settings.view` in any domain |
| Domain | Access | Users, Groups, Roles, Security, Sign-in, Audit log | `users.view`, `groups.view`, `roles.view`, `settings.view`, `settings.view`, `audit.view` |
| Domain | Platform | Materializations, Data protection, Deletion & retention, Schema evolution, Runtimes, Commits, Live tail, Compaction & retention, Iceberg catalog, Background jobs | `settings.view` |
| Domain | System | Topics & Kafka, Diagnostics, Deployment, Change history | `settings.view` |

Without `settings.manage` the settings sections are read-only: no save bar, no Reset. A domain's
Security holds its session, password, lockout and audit settings; the console's Security its two
(session check interval, sign-in attempts per minute). **Sign-in** turns each provider on or off in
the domain, sets what a first sign-in does (`open` or `invited`), the email domains that may sign in
through a provider (any, when empty), and whether people may sign in with a password (administrators
always may).

**Domains** lists every domain in the registry: its status and why (*Live*, *Its control plane
cannot be reached*, *says it is another domain*), its version, the id of the key it signs access
tokens with (*differs* when it is not the one most domains share, with a warning above the list),
whether it is the settings home, whether you are signed in there — **Sign in** when not — and
**Open**.

**Data protection** holds *Protected by default*: a row per kind of sensitive data, **Off**,
**Hashed**, **Partial** or **Withheld**, what every new materialization masks unless it chooses
otherwise. Saving it also needs *Manage data protection defaults*: without it the row is read-only,
saying so, while the rest of the page may still be saved. A change applies to materializations
created from then on, never to existing ones. While the domain has no protection key, a warning
above the rows says so: nothing can be hashed, so a create that keeps a default that hashes is
refused until the key is added (control-plane handbook
[22](../../docs/handbook/22-data-protection.md) §22.9). The
domain's **Deployment** section shows the protection key's id, or *not configured: nothing can be
hashed*.

A section with unsaved edits carries a dot (a `•` in the picker). Settings marked advanced sit in a
collapsed **Advanced** panel, which opens by itself when one of them has an edit.

### A setting

| Part | Shows |
|------|-------|
| Label | With **Destructive** when changing it can remove data, and **Unsaved** while it has an edit |
| Description | What it controls and, where useful, when to change it. Only where the label is not enough |
| Provenance | *Default*, *Set by the deployment `ENV_VAR`*, or *Changed by jane · 5 minutes ago · deployment value 16 replicas* |
| Effect | What a change applies to, when that is not "at once": *Applies to new materializations* |
| Control | A number field with its unit, a switch, a text field, a select for an `enum` setting such as **Default partitioning** (its `options`), or a row of choices per option for a `choice_map` such as **Protected by default**. Large numbers keep thousands separators, and a readable equivalent shows under the field (`= 15 min`, `= 8 MiB`) |
| Reset / Undo | **Reset** returns an override to the deployment value or default (named in its tooltip). **Undo** drops an unsaved edit |

### Saving

Edits are drafts until saved. A bar at the foot counts them and names their sections, and offers
**Discard** and **Save changes**. Save stays disabled while an edit is invalid: the field says why
(`At least 1`, `A gs:// URI such as gs://bucket/prefix`). One save covers every section and scope.

| Outcome | What the page does |
|---------|--------------------|
| Applied | Toast **Settings saved**: *They apply from now on*, or *Running runtimes pick the change up within seconds* when a runtime setting changed |
| Accepted, still applying (after 10 s) | Toast saying so; the page re-reads 3 s later |
| A value refused | The message under that setting and in a toast; edits stay |
| Someone changed the scope first | A banner: *These settings changed after this page loaded. Your edits are kept: reload to see the current values, then save again.* with **Reload settings** |
| The control plane is unreachable | A banner, *Settings are read-only*, naming the URL it cannot reach. The control plane's settings are not shown; the console's are shown locked |

Leaving the page with unsaved edits asks first.

### Runtimes: delivery to runtimes

Above the runtime settings — on Runtimes, Commits, Schema evolution and Compaction & retention,
every section that holds some — **Delivery to runtimes** shows how far the current runtime-settings
revision has reached. A badge counts the running materializations that run it: blue while one is
still applying, green when all do, amber when delivery has settled but some cannot take it yet.
Below the badge, each materialization that is not current is listed with its state: **Applying**
(with the revision it is on, or none while it runs settings from before this installation),
**Not reporting**, **Next deploy** (its runtime predates live settings), or **Stopped**. While one
is applying, the page re-reads every 5 s.

Before any runtime setting is changed (revision 0) the panel only says so — unless a runtime still
runs settings an earlier installation left on the delivery topic: it is then listed as
**Applying** until it drops them, within seconds (handbook 17 §17.7).

### Deployment

The settings only a deployment can change, per component (control plane, runtimes, this console),
each with its value and environment variable, locked.

### Change history

Every scope's changes, newest first: who, when, the scope, the revision, and each setting's value
before and after; a value that was not an override is marked *(default)* or *(deployment)*. The newest 100 per
scope are kept.

## 11.8c Your account (`/settings/account[/:domain]`)

Everyone signed in to a domain. Where the console serves several, **Your domains** first: each
domain, with who you are signed in there as and how (*with a password*, *through Google*), **Show**
and **Sign out**, or **Sign in**; **Sign out everywhere**. Then **Access tokens**, every domain's at
once, and below them your account in the domain shown (`/settings/account/<domain>`, your first by
default): your name and email ("An administrator manages your name, email and groups"); **What you
can do** there, by category; **Password** — changing it signs you out of that domain everywhere else,
and this browser carries on with a new session; and **Sessions** there: each browser signed in, with
its address, when it signed in and was last active, **Sign out** on any other one, and **Sign out
other sessions**.

**Access tokens**: credentials for DuckDB, Spark, Trino or PyIceberg to read the Iceberg REST catalogs
as you, in every domain where you have an account (handbook 18 §18.15). Each token shows its name,
permissions, expiry, the domain it was **made in**, and the datasets it is limited to — named
`<domain>.<dataset>` — (*Every dataset you may read, in every domain* when it is not), with **Revoke**,
which reaches every domain. **New token** asks for a name, the domain to make it in (one where you
hold `catalog.read`), the permissions (those you hold there; **Commit to tables** is marked as able to
rewrite data), the **Datasets** — every one you may read, now and later, in every domain, or **Only
these datasets**, chosen from those you read in any domain you are signed in to — and a lifetime,
then shows the token **once** with **Copy**: *Copy it now: it is not shown again.* — with the
federated catalog's URI, and a domain's own for engines that read one level of namespaces. With one
domain the page is that domain's account, as before.

## 11.8d Users (`/settings/users[/:userId]`)

The list (`users.view`): tabs **All users**, **Active**, **Deactivated**, **Locked out**,
**Password to change**, each with its count; search by name or email; sort by name, last active
or date added. Each row shows name and email, groups, status badges (*Deactivated*, *Locked out*,
*Password to change*, *Administrator*, *You*) and when they were last active.

**Add user** (`users.manage`): first and last name, email, an **Initial password** — generated,
20 characters from an alphabet without lookalikes, with **Copy password** and **Generate a new
password** — and **Groups**. Only groups whose access the person adding holds can be chosen
(`grantable`); everyone is in All Users anyway. The new user must change the password at first
sign-in; pass it on out of band.

A user's page (`users.view_details`):

| Panel | Shows / does |
|-------|--------------|
| Profile | Name and email; editable with `users.manage` |
| Groups | The groups they were added to, editable (only groups you may grant); All Users and Guest are implicit |
| Access | Every permission they hold, by category, each with the groups and roles it comes from |
| Dataset access | Their roles on single datasets, their own and their groups' (*through Finance team*), each dataset a link to its Access tab |
| Activity | Added, last signed in, last active, failed sign-ins |
| Sessions | Their sessions; **Sign out** on one |

Actions, with `users.manage` and only for someone whose access you hold yourself: **Unlock** (when
locked out), **Reset password** (a generated one to pass on; ends their sessions; they must change
it), **Sign out everywhere**, **Deactivate** / **Reactivate** (deactivating ends every session at
once), and **Delete** (only once deactivated; frees the email). You cannot deactivate or delete
yourself, and the last active administrator cannot be removed.

## 11.8e Groups (`/settings/groups[/:groupId]`)

The list (`groups.view`): name, members, roles, created on; built-in groups first, marked with a
lock. **New group** (`groups.manage`): name, description, roles.

A group's page: **Settings** (name, description — fixed for built-in groups), **Roles** (which
roles it grants; the Admin group always keeps Administrator), **Access** (the permissions those
roles amount to), **Dataset access** (its roles on single datasets), and **Members** with **Add
people** and remove (`users.view` to see them). Guest
and All Users have no member list: their membership is automatic. Changes need `groups.manage`
and, for anything that grants access, holding that access yourself.

## 11.8f Roles (`/settings/roles[/:roleId]`)

The list (`roles.view`): role, type (*Built in* / *Custom*), description, the groups holding it.
**Create role** (`roles.manage`).

The editor: **Role details** (name, description), **Permissions** by category with a filter by
type, each permission's description, *Can remove data or access* on the dangerous ones, and what
each implies — choosing **Export data** also shows **Query data** as included. A dataset permission
carries a **Per dataset** badge: held through a group it applies on every dataset, granted on a
dataset (the dataset roles, 11.11) on that one only. **Permissions preview** sums up what the role grants and **Held by** lists
its groups. Built-in roles are read-only — "they gain new permissions as the application does" —
with **Duplicate** to start a custom role from one. You can only grant permissions you hold.

## 11.8g Audit log (`/settings/audit`)

`audit.view`. Newest first, 50 at a time with **Load older entries**: when, the event (*Signed
in*, *Failed sign-in*, *Locked out*, *Added a user*, *Changed a group*, *Made an access token*,
*Revoked an access token*, …, with **Failed** on failures), who did it, and what it was done to. Filter by activity (sign-ins, users, groups,
roles), by outcome, and search by email, name or address. A row opens to the address and browser,
and what changed, before and after. Entries are kept for the audit retention setting (90 days).

## 11.9 Cross-cutting behaviour

| Behaviour | Where |
|-----------|-------|
| Every mutation shows a toast naming the materialization and the requested phase | All lifecycle actions |
| No optimistic updates | All lifecycle actions |
| Relative timestamps update every second without refetching | Everywhere `RelativeTime` is used |
| Ids are copyable on hover | `Identifier` |
| Loading states preserve final layout | Skeletons sized to their content |
| Control-plane outage keeps last-known data visible | Header states the outage |
| Deep links work on cold load | Server SPA fallback |
| Pages, navigation and buttons follow the person's permissions; the server checks every request again | `useCan`, `RequirePermission`, route `permit()` |
| A table's dataset decides what of it a person sees: its schema, data, snapshots as of a time, connection snippets. Tables of datasets locked to them are listed, with a lock, never hidden: names are not secret | `useCan(permission, datasetId)`, `shared/access.ts` |
| A change to users, groups or roles waits for the control plane to apply it before the toast | `useIdentityChange` |

## 11.10 Detail → Table

One of the materialization's Iceberg tables: what is in it, how it is kept, and how to reach it from
other engines (`features/detail/TableTab.tsx`; data from `GET /api/materializations/:id/tables/:table`,
06 §6.5). With two or more tables, a table picker chooses which (`?table=`). Its header names the
topic and the dataset, a link to the dataset's page.

| Panel | Shows |
|---|---|
| Figures | **Rows**, with *Complete through* (every record with a Kafka timestamp before then is in the table); **Size**; **Data files**, with how many are small; **Last commit**, with its duration |
| Snapshots | The newest 200: when committed, the operation (append, a compaction's replace, a **reprotect**'s, …), files and rows added, the offsets reached. The current one's row opens the Data tab, for someone who may query the table's dataset; the Data tab reads the latest data only, so older rows do not |
| Maintenance | **Compaction**, **Snapshot expiry** and **Orphan cleanup**: each job's last run — when, its outcome, its partitions, `32 files → 1`, bytes, snapshots expired and kept, orphans deleted of those listed, how long it took; a skipped run's note in muted text and a failure's in amber; the job running now. The maintenance app runs them on its own schedule, whether or not the runtime runs, kept tables included. **Run now** (`materializations.maintain`) asks it to run one on this table next; the panel follows the result. Once a reprotect has been asked for, a **Reprotect** row too — *requested*, running, or its last run: files rewritten, values masked, snapshots expired — asked from the Protection tab, not here |
| Files by partition | Every partition, newest first — `2026-10-23`, `2026-10-23-14` or `2026-10` by the table's partitioning: files, bytes, records and small files, with a mark (*compact*) on a partition the next compaction takes, by the maintenance app's own rule: what is still being written waits for the hot partition delay (06 §6.5); **Files / Bytes** switches the bars. Rows without a timestamp read *no timestamp* |
| Properties | Identifier, location, metadata file, format version, schema id, partition spec, sort order, current snapshot, table UUID, and the table properties — without the fork map, `materializer.protection` and `schema.*` for someone who may not see the dataset's schemas |
| Connect | With `catalog.read` on the table's dataset: snippets for **DuckDB**, **PyIceberg**, **Spark**, **Trino** and **By path** (DuckDB's `iceberg_scan` on the location), filled with this deployment's catalog URI and `lake.<dataset>.<table>`, each with **Copy**; a link to make an access token |

The tab reads the table itself, so it works while the runtime is stopped. A table that cannot be read
shows why (*The table could not be read*), and one not created yet says so.

## 11.10a Detail → Protection

What the materialization masks, and what each of its tables records of it
(`features/protection/ProtectionTab.tsx`; data from `GET /api/materializations/:id/protection`, the
control plane's review, refreshed every 15 s). Shown to people who may see schemas on at least one of
the tables' datasets; tables in the others are left out, and counted at the foot.

| Panel | Shows |
|---|---|
| Data protection | The policy as chips, its revision (*None: created before data protection* at 0), who chose it — *· current rules applied* after *Apply the current rules* — the members masking with it (`3 of 4`) and the tables recording it, with **In force**, **Applying revision N**, **Applies on resume** or **No member reporting**. A change applies to records written from then on; rows already written keep their values until a reprotect masks them |
| Rows already written | Per table, kept ones included (*kept*): the files, rows and bytes holding in plain text what it masks now, as the maintenance app last counted them (*not counted yet* before), files that cannot be rewritten yet and why, files of unknown lineage; **requested**, **rewriting** or the last run's outcome — *masked*, *nothing to mask*, *failed* — with what it rewrote, masked and purged. **Mask rows already written** — for someone who may run table maintenance and choose protection on every dataset — opens a dialog listing each table's count, with **Purge the plain text now** checked: once every file is rewritten, each table's older snapshots expire and the files they alone held are deleted, ending time travel to before; unchecked, the old files stay readable through older snapshots until they expire. **Mask rows** asks for a reprotect of every table (control-plane handbook [22 §22.14](../../docs/handbook/22-data-protection.md)). Hidden for a control plane before reprotect |
| *N fields revision R is about to protect otherwise* | While a table has not recorded the current revision yet: what recording it will change, field by field, `plain → Email addresses hashed`. No action: each table records it on its next commit |
| *N fields the current rules would protect otherwise* | Fields a newer release's rules match that their table recorded otherwise. **Apply the current rules** — for someone who may edit the materialization and choose protection on every dataset it writes into — records the policy again, revision + 1, so the rules apply from then on |
| One per table, `sales.orders` | Its protected fields — name, kind, mask, the schema it was protected from — and those written plain again (*Plain since schema 4*), the values masked and withheld so far, the revision it records (**Recording revision N** while behind), and its history of policies: revision, when, by whom, at which schema |

Choosing another policy is the **Edit** dialog's (§11.4).

## 11.11 Datasets (`/datasets`, `/datasets/:name[/:tab]`)

A dataset is a named group of tables, and their namespace: SQL reads each table as
`<dataset>.<table>`, in the console and through the Iceberg catalog. Access is granted per dataset
(control-plane handbook [18 §18.16](../../docs/handbook/18-identity-and-access.md)). Every page needs
`datasets.view` (`features/datasets/`).

**The list** — every dataset, *including those locked to the person*: names and tables are no secret,
schemas and data are. Search by name, description or table. Each row: the name (a lock when locked),
its description, its tables (how many, the first four, every one in the tooltip), its owners, and the
person's access in a word — **owner**, **writer**, **reader** or **locked** (*Its schemas and data
need a role on sales: ask one of its owners*). **New dataset** (`datasets.create`): a name — the
dataset rules, checked as typed, fixed once created — and a description; the creator owns it and All
Users may read it.

**A dataset's page** — its name, description, the person's access and its owners; **Edit** (the
description) and **Delete** for its owners (`dataset.manage`). Deleting is refused while it holds a
table — the dialog says so and only closes; once empty, its grants go with it and its name is free.

| Tab | Shows |
|---|---|
| Tables | Every table in it, `sales.orders` above its topic: the materialization writing it (its label and id), its state — **being created**, **written**, **kept · read-only**, **being dropped** — and its last commit. A table links to its Table tab for someone who may read the dataset. Locked to the person: a note that they see the names, not the schemas or data |
| Access | Its owners only. **Who may use it**: each person and group with a role on it, their roles as badges, **Change** (roles as checkboxes) and **Remove**; below, **Add a person or group…** with the roles to grant. Reader sees schemas, queries, exports and reads through the catalog; Writer also adds and drops tables; Owner also manages the dataset and its access. An owner grants only roles they hold on it themselves, and cannot take away a role they could not grant. A note says plainly what access does not do: anyone holding credentials that read the bucket reads every table in it |

A change of access reloads the person's own session, since what they may do may have changed with it;
open detail pages of others pick it up within 30 seconds (08 §8.22).

## 11.12 Search (`/search`)

One box over every domain: materializations by name, id or topic, tables by name
(`<dataset>.<table>` or in full, `<domain>.<dataset>.<table>`), and datasets by name or description —
in each domain where the person may see them. Results are grouped (**Materializations**, **Tables**,
**Datasets**), each with its domain's chip, and open in their domain: a materialization's page, a
table on its materialization's **Table** tab, a dataset's page. ↑ and ↓ move through them, ↵ opens one;
⌘K or Ctrl+K from anywhere focuses the box.

## 11.13 Every domain at once

Under **All domains**, the **Overview** shows the fleet's totals and throughput summed over every
domain, a **Domains** panel (each domain's materializations, rates and status), and *Needs attention*
across domains, each row with its domain's chip; **Materializations** lists every domain's in one
table with a **Domain** column and domain filter chips, and **New materialization** asks which
domain first; **Datasets** lists every domain's, grouped by domain, and **New dataset** asks which
domain first. A domain the person may not see, or that cannot be asked now, is named under the list
with **Sign in** where that would help. Inside a domain each page is as before, about that domain.

