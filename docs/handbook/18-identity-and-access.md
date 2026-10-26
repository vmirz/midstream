# 18 — Identity and access

How people sign in, what each of them may do — across the deployment and on each dataset — where
accounts, groups, roles, datasets, their grants and sessions are stored, and how every request to
the control plane and the console is authorized.

Source: `identity/` (`permissions.py`, `principals.py`, `authorization.py`, `tokens.py`,
`passwords.py`, `secrets.py`, `directory.py`, `validation.py`), `models/identity.py`,
`storage/identity.py`, `services/identity.py`, `handlers/identity.py`, `web/auth.py`,
`web/identity.py`, `web/datasets.py`, `web/tokens.py`, `services/table_claims.py`; chart templates
`tables/{directory,identity-audit}.yaml`, `agents/identity-intake.yaml`, `tasks/identity-*.yaml`,
`webviews/{auth,users,groups,roles,permissions,audit,identity,datasets}-*.yaml`; console:
`console/server/auth/`, `console/server/routes/{auth,identity,datasets}.ts`, `console/shared/access.ts`,
`console/src/features/{auth,access,datasets}/`, `console/src/lib/auth.tsx`.

## 18.1 The model in one paragraph

Every request is made by a **principal**: a signed-in person, a guest (no session), the console's
read-only service credential, or an access token. A person belongs to **groups**; a group holds
**roles**; a role is a set of **permissions**, the fixed vocabulary of things the application lets
someone do. A permission holds across the deployment, or — for what concerns a dataset's tables —
on one **dataset** at a time: a **dataset grant** gives a person or a group roles on one dataset.
What a person may do is the union of everything their groups grant, and on a dataset that plus
every grant on it to them or their groups. People, custom groups and roles, datasets, their grants
and the claims of table names live in the control plane's `directory` KasprTable; the API validates
a change and publishes it, and the `identity-intake` agent, the table's only writer, applies changes
one at a time in order. Every WebView authorizes its caller against that table before it does
anything, so a change to anyone's access applies to their next request.

## 18.2 Principals

| Kind | How a request becomes one | What it may do | Changes recorded as |
|---|---|---|---|
| `user` | `Authorization: Bearer <session token>` from `POST /auth/login` | Everything the Guest group, the All Users group and their own groups grant | Their email |
| `guest` | No `Authorization` header | What the Guest group grants: by default the materialization list and status | `guest` (a guest cannot change anything by default) |
| `service` | `Authorization: Bearer <AUTH_SERVICE_TOKEN>` | A fixed read-only set: `materializations.view`, `.view_details`, `datasets.view`, `dataset.view_schema` on every dataset, `topics.view`, `settings.view`. The console filters what it reads with it for each person (§18.9) | `service:console` |
| `token` | `Authorization: Bearer tok_<id>_<secret>`, an access token (§18.15) | The token's permissions — `catalog.read`, optionally `catalog.write` — on each dataset only as far as its owner still holds them there, and only on the datasets it is limited to, if any | Its owner's email |

- A token that cannot be used is **refused** with `401`, never quietly treated as a guest's, so a
  client knows to sign in again. `GET /auth/session` is the one exception: it answers as a guest
  and reports why in `session_error`.
- A guest asking for something it may not do gets `401 unauthenticated`
  (`details.reason = sign_in_required`); a signed-in person gets `403 forbidden` with
  `details.permission` naming what they lack.
- Signing in never takes access away: a signed-in person is also in Guest and All Users.
- A refusal for a dataset permission names the dataset: `details.permission` and
  `details.dataset`, and a message such as “it needs “View schemas” on dataset finance”.
- Only `user` principals change people, groups, roles, datasets, grants or tokens. The service
  credential can read the fleet, datasets, schemas, topics and settings for the console's live views
  and nothing more; a `token` principal reaches only what its catalog permissions allow.

## 18.3 Permissions, roles and groups

### Permissions

The catalog is code (`identity/permissions.py::PERMISSIONS`); roles refer to its keys. A
permission may **imply** others, so a role that can create materializations can also open them.
Each permission has a **scope**: `deployment` — held across the deployment or not at all — or
`dataset` — about a dataset's tables, and held on one dataset at a time. Implications stay within a
scope: a grant of `data.query` on one dataset never implies the deployment's
`materializations.view_details`. `GET /permissions` returns the catalog, with categories and each
permission's `scope`, for the console's role editor.

| Key | Scope | Label | Implies | Danger |
|---|---|---|---|---|
| `users.view` | deployment | View users page with emails | — | |
| `users.view_details` | deployment | View account details | `users.view` | |
| `users.manage` | deployment | Manage users | `users.view_details`, `groups.view` | yes |
| `groups.view` | deployment | View groups | — | |
| `groups.manage` | deployment | Manage groups | `groups.view`, `roles.view`, `users.view` | |
| `roles.view` | deployment | View roles | — | |
| `roles.manage` | deployment | Manage roles | `roles.view` | |
| `materializations.view` | deployment | View materializations (list and status) | — | |
| `materializations.view_details` | deployment | View materialization details | `materializations.view` | |
| `materializations.create` | deployment | Create materializations | `materializations.view_details` | |
| `materializations.edit` | deployment | Edit materializations | `materializations.view_details` | |
| `materializations.stop` | deployment | Stop materializations | `materializations.view_details` | |
| `materializations.resume` | deployment | Resume materializations | `materializations.view_details` | |
| `materializations.delete` | deployment | Delete materializations (and, unless turned off, their tables) | `materializations.view_details` | yes |
| `materializations.maintain` | deployment | Run table maintenance (compaction, snapshot expiry, orphan cleanup) now | `materializations.view_details` | |
| `datasets.view` | deployment | See datasets: names, descriptions, owners and table names, locked ones too | — | |
| `datasets.create` | deployment | Create datasets | `datasets.view` | |
| `dataset.view_schema` | dataset | View schemas: a dataset's tables' schemas, their versions and what changed | — | |
| `dataset.write` | dataset | Add and drop tables: put tables in a dataset, remove or drop its tables, delete materializations that have tables in it | `dataset.view_schema` | |
| `dataset.manage` | dataset | Manage datasets: the description, who may use it, deleting it once empty | `dataset.view_schema` | |
| `data.query` | dataset | Query data (rows and SQL in the Data explorer) | `dataset.view_schema` | |
| `data.export` | dataset | Export data (CSV, JSON, Parquet) | `data.query` | |
| `catalog.read` | dataset | Connect external tools to the Iceberg catalog, and make access tokens for them | `dataset.view_schema` | |
| `catalog.write` | dataset | Commit to tables from external tools | `catalog.read` | yes |
| `topics.view` | deployment | Browse Kafka topics | — | |
| `settings.view` | deployment | View settings | — | |
| `settings.manage` | deployment | Manage settings | `settings.view` | yes |
| `audit.view` | deployment | View audit log | — | |

A dataset permission held through a group — the deployment scope — holds on every dataset, those
created later included; granted on one dataset, it holds there only. `materializations.view_schema`
is gone: schemas are `dataset.view_schema`, on the table's dataset.

`danger` only changes how the console presents a permission. `*` is not a permission: it is what
the built-in Administrator role holds, and it means every permission, on every dataset, including
ones a later release adds. Custom roles cannot hold `*`.

### Roles

Built-in roles are defined in code and cannot be edited or deleted; administrators duplicate one
to make their own. Custom roles are stored in the table.

| Role id | Name | Permissions |
|---|---|---|
| `administrator` | Administrator | `*` |
| `user-manager` | User Manager | `users.manage`, `groups.manage`, `roles.view`, `audit.view` |
| `operator` | Operator | `materializations.create`, `.edit`, `.stop`, `.resume`, `.delete`, `.maintain`, `datasets.create`, `topics.view` |
| `viewer` | Viewer | `materializations.view_details`, `datasets.view` |
| `status-viewer` | Status Viewer | `materializations.view` |
| `dataset-reader` | Dataset Reader | `dataset.view_schema`, `data.query`, `data.export`, `catalog.read` |
| `dataset-writer` | Dataset Writer | Dataset Reader, plus `dataset.write` |
| `dataset-owner` | Dataset Owner | Dataset Writer, plus `dataset.manage` |

A role whose permissions are all dataset permissions is a **dataset role** (`dataset_role` in
`GET /roles`): it can be granted on a single dataset (§18.16), as well as held by a group. The three
built-in dataset roles nest; no built-in dataset role holds `catalog.write` — only Administrator
does, and a custom role can. The 2.0 role Data Reader is gone: a new dataset gives All Users
Dataset Reader on it instead, so readers lose nothing, and an owner who narrows a dataset really
narrows it.

### Groups

| Group id | Name | Membership | Roles by default |
|---|---|---|---|
| `guest` | Guest | `guests`: everyone, signed in or not | Status Viewer |
| `all-users` | All Users | `authenticated`: everyone signed in | Viewer |
| `viewer` | Viewer | `assigned` | Viewer |
| `editor` | Editor | `assigned` | Viewer, Operator |
| `admin` | Admin | `assigned` | Administrator (locked: the group always keeps it) |

What the Viewer and Editor groups may do with each dataset's tables comes from its grants: All Users
reads every new dataset, and its creator — an Editor, say — owns it.

Built-in groups cannot be renamed, re-described or deleted, but their roles can change: an
administrator may, for example, take Status Viewer from Guest so that nothing is visible without
signing in. Nobody is added to Guest or All Users; their membership is automatic. Custom groups
are `assigned`.

### Effective permissions

```text
groups(person)       = Guest + All Users + person.group_ids
roles                = every role of those groups (unknown ids grant nothing)
permissions          = every role's permissions, with what each implies, transitively
all                  = some role holds "*"
on(person, dataset)  = the dataset permissions among `permissions`
                     + every dataset permission of the roles granted on the dataset
                       to the person or to one of their groups
token(t, dataset)    = t.permissions ∩ on(owner, dataset), and nothing on a dataset
                       outside t.datasets when the token is limited to some
```

`identity/principals.py::Grants` holds both: `permissions` across the deployment, and `datasets`,
by dataset id, what grants add there (`dataset_limit` for a limited token). `Principal.can(p,
dataset_id)` answers on a dataset; `can(p)` without one answers across the deployment, and
`can_anywhere(p)` whether `p` is held on at least one dataset. `GET /auth/session` reports the
compact form: `dataset_permissions`, by dataset id, only what grants add beyond `permissions`, and
`dataset_limit`. The console checks a permission on a dataset from it without a round trip
(`console/shared/access.ts`).

`identity/principals.py::cached_grants` caches the result per person and **directory revision**,
a counter in the `directory` record that every change to people, groups, roles, datasets or grants
raises. A change therefore applies to the next request on each replica, without any cache to clear.

### Rules every change obeys

Checked by the agent as it applies each change, against the directory as it then stands:

| Rule | Refusal `reason` |
|---|---|
| One account per email, without regard to case | `already_exists` |
| **Nobody grants access they do not hold.** Adding someone to a group, removing someone from one, giving a group a role, putting a permission in a role, deleting a group or role: the actor must hold every permission involved — across the deployment, and on each dataset where the group holds grants. Granting or taking away a role on a dataset needs every permission of that role on that dataset. Administrators hold everything | `forbidden` |
| **Nobody manages an account with more access than their own.** Editing, resetting, signing out or deleting a person requires holding everything that person holds | `forbidden` |
| **There is always an active administrator.** A change that would leave none is refused | `last_administrator` |
| You cannot deactivate or delete yourself, or reset your own password as an administrator | `self_action` |
| An account must be deactivated before it is deleted | `invalid` |
| Built-in groups keep their name and description; built-in roles cannot change | `built_in` |
| The Admin group keeps the Administrator role | `locked_role` |
| A change made from a stale page (`base_revision` differs) | `changed` |
| An id that no longer exists | `not_found` |

The actor must also still be active and still hold the permission the change needs when the
agent applies it, not only when the API accepted it.

## 18.4 Storage

Two **global** KasprTables, JSON, 3 partitions, written only by `identity-intake`:

| Table (base / dev) | Holds |
|---|---|
| `directory` / `<release>-directory` | The directory — people, groups, roles, datasets, their grants and the claims of table names — sessions, sign-in state and the outcome of each directory change. Called `identity` before 3.0 |
| `identity-audit` / `<release>-identity-audit` | The audit log |

Global, because every replica authorizes every request from them and a WebView cannot route a
read to the replica that owns a partition. Every write arrives on one Kafka key (§18.5), so each
record lives in one changelog partition; reads still keep the newest copy of a key, as the
`state` table's do, in case partition counts ever change.

| Key | Entity type | Model | Holds |
|---|---|---|---|
| `user:<user_id>` | `user` | `User` | Name, email, status, `group_ids`, password hash, `external_identities`, `session_epoch`, `revision` |
| `user-email:<email>` | `user-email` | — | `{email, user_id}`: the unique email index, lower-cased |
| `group:<group_id>` | `group` | `Group` | Name, description, `role_ids`, `membership`, `external_groups`, `revision`. Built-in groups read their code defaults until first stored |
| `role:<role_id>` | `role` | `Role` | Custom roles only; built-in roles always come from code |
| `session:<user_id>:<session_id>` | `session` | `Session` | When a sign-in started, was last seen and ends; address and browser; when and why it ended |
| `token:<token_id>` | `access-token` | `AccessToken` | An access token: owner, name, permissions, the datasets it is limited to, the **hash** of its secret, expiry, revocation |
| `dataset:<dataset_id>` | `dataset` | `Dataset` | A dataset: its name, description, `revision`, who made it |
| `dataset-name:<name>` | `dataset-name` | — | `{name, dataset_id}`: the unique name index, lower-case |
| `dataset-grant:<dataset_id>:<user\|group>:<id>` | `dataset-grant` | `DatasetGrant` | The roles a person or a group holds on a dataset |
| `dataset-table:<dataset_id>:<table>` | `table-claim` | `TableClaim` | A table name taken in a dataset: the materialization and topic it belongs to, and the request that claimed it (§18.16) |
| `claim-release:<materialization_id>` | `claim-release` | `ClaimRelease` | That a materialization gave up every name, as it is deleted; kept 7 days so a late claim is refused |
| `sign-in:<email>` | `sign-in-guard` | `SignInGuard` | Failed attempts for an email and `locked_until`. Per email, not per account, so an unknown email is locked out the same way |
| `activity:<user_id>` | `user-activity` | `UserActivity` | Last sign-in, last activity, sign-in count |
| `request:<request_id>` | `identity-request` | `IdentityRequestOutcome` | Whether a directory change was applied or refused, with `reason`, `message`, `field`, and `details` — for a claim, the table and dataset it was about |
| `directory` | `directory` | `DirectoryState` | `revision`, `seeded_at` |
| `audit:<occurred_at>:<entry_id>` (audit table) | `audit-entry` | `AuditEntry` | Who did what to whom, from where, with `details` of what changed |

Generated ids: `usr-<12 hex>`, `grp-<12 hex>`, `rol-<12 hex>`, datasets `ds-<12 hex>`, sessions
`ses-<16 hex>`, requests `idr-<12 hex>`, audit entries `aud-<14 hex µs since the epoch><4 hex>` (time-ordered, so entries
of the same second list in the order they were recorded). Emails are compared trimmed and
lower-cased.

Passwords are stored only as scrypt hashes, in PHC-like form:
`$scrypt$ln=15,r=8,p=3$<salt>$<hash>` (N = 2^15, about 32 MiB and 50–100 ms per hash).

Session **tokens are never stored**: a token is valid on its signature alone, and the `session`
record exists for the account pages and for ending one session.

## 18.5 Changing the directory

```text
POST /users  (users-create WebView; the same shape for every directory change)
  authorize(request, directory_table, "users.manage")
  validate the body; password rules; hash the password (never sent in clear to Kafka)
  preview: run the agent's own handler against an in-memory overlay of the table
           → refuse now with the error the agent would give (400/403/404/409)
  → <release>-identity-requests   key "identity"   (no table write)
  ← 202 { request_id, action, entity_type, entity_id, accepted_at, status_url }

identity-intake agent → services/identity.py::apply_identity_request
  request already has an outcome → return it (redelivery changes nothing)
  check again against the directory as it is now
  refused → outcome { status: "rejected", reason, message, field }
  applied → write records, raise directory revision, append audit entry,
            outcome { status: "applied", entity_type, entity_id }

GET /identity/requests/{request_id}   → pending | applied | rejected
```

**One key, one partition.** Every identity request — directory changes, sign-in events and the
control plane's own housekeeping — is published with the key `identity`. They are therefore
consumed strictly in order by one agent instance, which is what makes rules spanning many records
(unique email, last administrator, no escalation) hold without locks. The cost is that identity
writes cannot scale out; each is a handful of table writes, and password hashing never happens in
the agent (except once at bootstrap), so one partition is ample for a console's users.

**Preview.** `preview_identity_request` runs the agent's handler against `_Overlay`, a table whose
writes stay in memory. The API refuses a change for exactly the reasons the agent would, and the
agent still checks again because another change may land in between.

**Optimistic concurrency.** User, group and role updates accept `base_revision`, the record's
`revision` as the client read it; a mismatch is refused with `changed`.

**Waiting.** The console waits up to 10 seconds for the outcome (`console/server/auth/outcomes.ts`)
and answers `200` when applied, `202` when still pending, and the matching error when refused.
Outcomes are kept for a day. Only the person who sent a request, or someone with `users.view`,
may read its outcome.

| Action | Payload | Permission | Notes |
|---|---|---|---|
| `create_user` | `user_id`, `email`, `first_name`, `last_name`, `group_ids`, `password_hash` | `users.manage` | Password must be changed at first sign-in |
| `update_user` | any of `first_name`, `last_name`, `email`, `group_ids`, `status` | `users.manage` | Deactivating ends every session |
| `set_user_password` | `user_id`, `password_hash` | `users.manage` | Must be changed at next sign-in; ends every session; lifts a lockout |
| `change_password` | `user_id`, `password_hash` | (own account) | Ends every session, this one included |
| `sign_out_user` | `user_id`, optional `session_id` | `users.manage`, or own account | One session, or all |
| `unlock_user` | `user_id` | `users.manage` | Clears the email's failed-attempt count |
| `delete_user` | `user_id` | `users.manage` | Deactivated accounts only; frees the email |
| `create_group` / `update_group` / `delete_group` | `name`, `description`, `role_ids`, `member_ids` / `add_member_ids`, `remove_member_ids` | `groups.manage` | Membership is stored on each user |
| `create_role` / `update_role` / `delete_role` | `name`, `description`, `permissions` | `roles.manage` | Deleting a role removes it from every group and every dataset grant |
| `create_dataset` | `dataset_id`, `name`, `description` | `datasets.create` | Grants its creator Dataset Owner and All Users Dataset Reader (§18.16) |
| `update_dataset` | `dataset_id`, `description` | `dataset.manage` on it | The name never changes |
| `delete_dataset` | `dataset_id` | `dataset.manage` on it | Refused `dataset_not_empty` while it holds a table; deletes its grants and frees its name |
| `set_dataset_grant` | `dataset_id`, `principal_type`, `principal_id`, `role_ids` | `dataset.manage` on it | Empty `role_ids` removes the grant; dataset roles only; never Guest |
| `claim_tables` | `materialization_id`, `purpose`, `tables`, `release` | `dataset.write` on each table's dataset | Sent by request intake, not the API: claims new tables' names, all or none (§18.16) |
| `release_tables` | `materialization_id`, `tables` or `all` | — | Sent by request intake as tables leave the catalog — a delete, a change that drops tables; no outcome |
| `sign_in_succeeded`, `sign_in_failed`, `session_refreshed`, `sign_out` | — | — | Published by the auth views; no outcome |
| `bootstrap`, `maintenance` | — | — | Published by the control plane's own tasks (§18.8) |

## 18.6 Signing in and sessions

### Signing in

`POST /auth/login` answers at once; nothing waits on the agent:

1. No signing key configured → `503 unavailable` (`sign_in_unavailable`); still reading the
   directory back after a start → `503 unavailable` (`starting`), counting no failed attempt.
2. The email is locked out → `429 too_many_attempts` (`locked_out`, `retry_after` in seconds).
3. Look up the account by email and verify the password in a worker thread. An unknown email is
   verified against a dummy hash, so it takes as long as a wrong password. Wrong email or wrong
   password → `401 unauthenticated` "Email or password is incorrect." (`invalid_credentials`),
   and a `sign_in_failed` event that counts towards a lockout.
4. A deactivated account with the right password → `403 forbidden` (`account_deactivated`).
5. Otherwise issue a token and publish `sign_in_succeeded`, which records the session, the
   person's activity, clears failed attempts and writes the audit entry.

Each replica hashes on a pool of two threads, about 64 MiB at most. When more than 32 hashes are
already waiting, sign-in — and setting or changing a password — answers `429 too_many_attempts`
(`busy`) rather than adding to the pile-up.

### Tokens

```text
v1.<base64url JSON claims>.<base64url HMAC-SHA256(v1.<claims>, signing key)>

claims: sid  session id          sub  user id          ep   session epoch
        iat  issued at           exp  idle expiry       max  absolute expiry
        auth signed in at        amr  sign-in method    kid  signing key id (first 8 hex of its SHA-256)
```

The claims are signed, not encrypted, and hold only ids and times. Tokens longer than 2,048
characters are refused unread; 30 seconds of clock skew between replicas is tolerated.

### Lifetimes and renewal

| Limit | Setting | Default |
|---|---|---|
| Idle timeout: a token's `exp` | `session_idle_timeout_minutes` | 480 (8 h) |
| Maximum session length: `max`, fixed at sign-in | `session_max_lifetime_hours` | 168 (7 d) |
| Renewal interval: `refresh_after` in the session payload | `session_refresh_minutes` | 10 |

`POST /auth/refresh` returns a new token for the same session with a fresh idle timeout, never
past `max`, and publishes `session_refreshed`, which updates the session's `last_seen_at` and the
person's last activity. The console renews a cookie session on the first request after
`refresh_after`, so an active browser stays signed in until the maximum length.

### Ending sessions

Two mechanisms, both checked on every request:

- **Epoch.** Every user has a `session_epoch`, copied into each token as `ep`. Deactivating someone,
  resetting or changing their password, or signing them out everywhere raises it, and every token
  with an older epoch is refused with `401 session_ended`. One write ends every session.
- **Ended record.** Signing out one session marks its `session` record ended. A token whose record
  is ended is refused. A sign-out that arrives before the sign-in was recorded writes an ended
  record from the token's own expiry, so it cannot be revived.

### Lockout

Failed sign-ins are counted per email. At `sign_in_max_failed_attempts` (10) in a row the email is
locked for `sign_in_lockout_minutes` (15), even with the right password. A success clears the
count; so does an administrator's **Unlock** or password reset. Counting starts over when a lockout
runs out. The console also limits sign-in attempts per client address
(`CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE`, 10), before the control plane sees them.

### Passwords

Rules (`identity/passwords.py::password_problem`), following NIST SP 800-63B: at least
`password_min_length` (12) and at most 256 characters, not only spaces, not the person's email,
email name or name, and at least 4 distinct characters. No character-class rules.

A password someone else chose — at account creation, an administrator's reset, the bootstrap
administrator — must be replaced at first sign-in (`must_change`). Until it is, the person may
only read their session and sessions, change the password, renew and sign out; everything else is
`403 forbidden` with `details.reason = password_change_required`. Changing it ends every session,
so the console signs the person straight back in with the new password.

There is no email delivery and no "forgot password": an administrator resets the password and
tells the person the new one.

## 18.7 Authorizing a request

The first line of every WebView's Python:

```python
# code-derived (charts/midstream/templates/webviews/materializations-detail.yaml)
principal = authorize(request, directory_table, "materializations.view_details")

# A dataset permission is checked on the dataset a request names
# (charts/midstream/templates/webviews/datasets-grants.yaml)
principal = authorize(request, directory_table, "dataset.manage", dataset=dataset)
```

`authorize` (`identity/authorization.py`):

0. Until a restarted control plane has read its tables back from their changelogs — Kaspr serves
   its WebViews before that, and the directory holds only some of its records meanwhile — every
   caller gets `503 unavailable` (`starting`), as Faust's `app.tables.recovery.completed` says
   (`directory_ready`). A session must not read as ended, nor a list as empty, for not having been
   read yet: the console and the tail app keep the sessions they last checked through a 503. The
   session view passes the 503 on rather than reporting a guest, and sign-in answers it too.
1. No bearer token → guest.
2. Token equal to the service token (constant-time compare) → service.
3. Otherwise verify the token: signature, `exp`, `max`. Then read the user: missing →
   `invalid_session`; deactivated → `account_deactivated`; token epoch older → `session_ended`;
   session record ended → `session_ended`. Each is `401`.
4. Password still to change → `403 password_change_required`, unless the view allows it.
5. The permission is not held → `401 sign_in_required` for a guest, `403 forbidden` for a person.
   With `dataset=` (a name or an id), a dataset permission is checked on that dataset, and an
   unknown dataset answers `404`. With `any_dataset=True` it is enough to hold it on one dataset —
   the REST catalog's views and token creation, which then check each dataset they touch.
   `authorize` raises `ValueError` when either is given for a deployment permission.

A view whose dataset comes from its records rather than its path checks after reading them, with
`identity/authorization.py::require(principal, permission, dataset=...)`: the schema views on the
table's dataset, a create or an edit on each dataset it adds tables to or removes them from.

`permission=None` asks only who the caller is. `authorize` raises `ValueError` for a permission
key the catalog does not hold, and `tests/test_agent_manifests.py::AuthorizationWiringTests`
catches that before a release: it parses every WebView, fails when one does not call `authorize`
with a known permission, and allows only `system-version` to be public and the auth views to
authenticate on their own.

What each view needs is in [04 — HTTP API reference](./04-http-api-reference.md) §4.1.

Mutations record `principal.attribution` — the signed-in person's email — as `created_by`,
`submitted_by` or `updated_by`, and `principal.user_id` as `created_by_id` / `submitted_by_id`
on a materialization and its request event. The `x-submitted-by` header is no longer read.

## 18.8 Bootstrap and maintenance

**`identity-bootstrap`** (KasprTask, at startup, leader only) publishes a `bootstrap` request. The
agent stores the built-in groups that are not stored yet, records `seeded_at`, and, **when no
active administrator exists**, makes the bootstrap administrator from the Secret
(`AUTH_BOOTSTRAP_ADMIN_EMAIL` / `_PASSWORD`, optional `_FIRST_NAME` / `_LAST_NAME`,
default "Admin User"):

- no account with that email → created in the Admin group (`user.bootstrapped`);
- an account exists → reactivated, added to Admin, its password replaced and its sessions ended
  (`user.bootstrap_restored`).

Either way the person must change the password at first sign-in. When an administrator exists,
the bootstrap secret is ignored, so it is safe to leave in place; it is also the way back in if
every administrator is lost. Without a bootstrap secret and without an administrator, the
control plane logs a warning at every start and nobody can manage users.

**`identity-maintenance`** (KasprTask, cron `41 * * * *`, leader only) publishes a `maintenance`
request. The agent deletes session records a day after they expired or ended, failed-attempt
counts a day after the last failure (once not locked), directory-change outcomes after a day, and
audit entries older than `audit_log_retention_days` (90), at most 5,000 per pass.

## 18.9 The console

The console (`/console`) is a client of this API like any other, with three additions of its own.

**The session lives in a cookie.** `POST /api/auth/login` forwards the credentials to the control
plane and stores the returned token in an `HttpOnly`, `SameSite=Lax` cookie
(`CONSOLE_SESSION_COOKIE`, default `console_session`), `Secure` behind TLS
(`CONSOLE_COOKIE_SECURE=auto`). The page's scripts never see the token. Scripts may instead send
`Authorization: Bearer` to the console.

**Every route declares a permission.** An `onRequest` hook resolves the caller of every `/api/*`
request by asking the control plane's `GET /auth/session`, caching the answer for
`CONSOLE_SESSION_CHECK_MS` (5 s) keyed by a hash of the token; routes declare
`permit('<permission>')` or `signedIn()`. Changes are sent to the control plane with the person's
own token, so the control plane authorizes them again. A route for a dataset permission declares
`permitAnywhere('<permission>')` — held on at least one dataset — and checks it on the datasets the
request is about: a Data tab run needs `data.query` on every table it reads, an export
`data.export`, a schema read `dataset.view_schema` on the table's dataset. Live views — the fleet,
details, schemas, topics, the console's own settings — are read once for everyone with
`CONSOLE_SERVICE_TOKEN`, which must equal the control plane's `AUTH_SERVICE_TOKEN`;
without it they read as a guest. What a live view sends each person is cut down to what they may
see (`live/store.ts::visibleDetail`): a table's schema reaches only those who may read its dataset.
Event streams check their caller every 30 seconds and close when access is lost.

**Changes from other sites are refused.** A `POST`/`PUT`/`PATCH`/`DELETE` whose `Sec-Fetch-Site`
is not `same-origin`, or whose `Origin` is another host, is refused with `403`
(`cross_site_request`), on top of `SameSite=Lax`.

**The live tail is read with the person's own session.** A run that reads a table's live tail calls
the tail app with the person's token as its bearer and `CONSOLE_SERVICE_TOKEN` in
`X-Console-Token`. The tail app answers the console alone — a request without that header is
`403 console_only` — and checks the person itself: it resolves the token with `GET /auth/session`,
rebuilds the principal from its description (`principal_from_description`), and requires
`data.query` on the table's dataset, a token's dataset limit included, exactly as the control plane
would. It trusts a check for 5 s, and for 60 s while the control plane cannot be reached. The
service credential reads the tail app's health and member map, never rows ([19 §19.10](./19-live-tail.md)).

The client (`console/src/lib/auth.tsx`) loads the session once and exposes `useAuth()` and
`useCan(permission, datasetId?)`. Navigation, pages and buttons follow permissions — on each
table's dataset, for a dataset permission; a guest who opens a page
that needs more is sent to `/sign-in` and returned afterwards; a person with a password to change
sees only the change-password form. Settings gains **Account** (profile, password, sessions) for
everyone signed in, and **Users**, **Groups**, **Roles** and **Audit log** for those allowed;
**Datasets** has an Access tab for each dataset's owners.
See the console handbook, [11 — Feature reference](../../console/docs/11-feature-reference.md) and
[12 — Security model](../../console/docs/12-security-model.md).

## 18.10 API

| Method | Path | Needs | Answer |
|---|---|---|---|
| `POST` | `/auth/login` | — | `200` session with `token`; `400`, `401`, `403`, `429`, `503` (§18.6) |
| `POST` | `/auth/refresh` | a session | `200` session with a new `token` |
| `POST` | `/auth/logout` | — | `200 {signed_out, scope}`; body `{scope: current\|others\|session, session_id}`. Never refuses an unusable session: it is already signed out |
| `GET` | `/auth/session` | — | `{authenticated, principal, session, providers, password_policy, session_error?}`; the principal carries `dataset_permissions` (by dataset id) and `dataset_limit` |
| `GET` | `/auth/sessions` | a session | Your sessions, newest first, `current` marked |
| `POST` | `/auth/password` | a session | `202`; body `{current_password, new_password}` |
| `GET` | `/users` | `users.view` | `?status=all\|active\|deactivated\|locked\|password_change&sort=name\|last_active\|created&q=`; `{users, counts, returned, can_manage}` |
| `GET` | `/users/{id}` | `users.view_details` | The user with `memberships`, `permissions` (each with the groups and roles it comes from), `dataset_grants` (theirs and their groups'), `sessions`, `failed_sign_ins` |
| `POST` | `/users` | `users.manage` | `202`; body `{first_name, last_name, email, password, group_ids}` |
| `PATCH` | `/users/{id}` | `users.manage` | `202`; body any of `{first_name, last_name, email, group_ids, status, base_revision}` |
| `DELETE` | `/users/{id}` | `users.manage` | `202` |
| `POST` | `/users/{id}/password` | `users.manage` | `202`; body `{password}` |
| `POST` | `/users/{id}/sign-out` | `users.manage` | `202`; body `{session_id}` for one session, empty for all |
| `POST` | `/users/{id}/unlock` | `users.manage` | `202` |
| `GET` | `/groups`, `/groups/{id}` | `groups.view` | Groups with `member_count`, `manageable`, `grantable`; the detail adds `roles`, `permissions`, `dataset_grants`, `members` (with `users.view`) |
| `POST`, `PATCH`, `DELETE` | `/groups`, `/groups/{id}` | `groups.manage` | `202`; `PATCH` body `{name, description, role_ids, add_member_ids, remove_member_ids, base_revision}` |
| `GET` | `/roles`, `/roles/{id}` | `roles.view` | Roles with `effective_permissions`, the groups holding them, and `dataset_role` |
| `POST`, `PATCH`, `DELETE` | `/roles`, `/roles/{id}` | `roles.manage` | `202`; body `{name, description, permissions, base_revision}` |
| `GET` | `/permissions` | signed in | The catalog: `categories`, `permissions` with `implies`, `danger` and `scope` |
| `GET` | `/auth/tokens` | a session | Your access tokens, the token permissions you could give one, and the datasets you could limit one to |
| `POST` | `/auth/tokens` | `catalog.read` on a dataset | `202` with the token, shown once; body `{name, permissions, expires_in_days, datasets?}` |
| `GET` | `/datasets`, `/datasets/{dataset}` | `datasets.view` | Every dataset, locked ones included; one dataset with its tables (§18.16) |
| `POST` | `/datasets` | `datasets.create` | `202`; body `{name, description}` |
| `PATCH`, `DELETE` | `/datasets/{dataset}` | `dataset.manage` on it | `202`; `PATCH` body `{description, base_revision}` |
| `GET` | `/datasets/{dataset}/grants` | `dataset.manage` on it | Its grants, the roles you can grant there, and whom to (`candidates`) |
| `PUT` | `/datasets/{dataset}/grants/{principal}` | `dataset.manage` on it | `202`; body `{role_ids, principal_type?}`; `[]` removes the grant |
| `GET` | `/datasets/{dataset}/tables/{table}` | `datasets.view` | `<dataset>.<table>` resolved to its materialization; location and metadata for readers only |
| `DELETE` | `/auth/tokens/{id}` | a session | `202`; your own token, or anyone's with `users.manage` |
| `GET` | `/audit` | `audit.view` | `?limit=1-200&before=<cursor>&action=user.&outcome=failure&q=`; `{entries, next, retention_days}` |
| `GET` | `/identity/requests/{id}` | the sender, or `users.view` | `{status: pending}` or the outcome |

Full shapes: [04 — HTTP API reference](./04-http-api-reference.md) §4.13 and §4.15.

```bash
# Sign in and call the API as yourself
TOKEN=$(curl -s -X POST localhost:6065/auth/login -H 'content-type: application/json' \
  -d '{"email":"you@example.com","password":"…"}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
curl -s localhost:6065/users -H "authorization: Bearer $TOKEN" | python3 -m json.tool
```

## 18.11 Settings and secrets

Overridable from the Settings page's **Security** section (`control-plane` scope, §17.3):

| Key | Env var (`CONTROL_PLANE_…`) | Default | Bounds | Takes effect for |
|---|---|---|---|---|
| `session_idle_timeout_minutes` | `SESSION_IDLE_TIMEOUT_MINUTES` | 480 | 5–10,080, ≤ max length | new and renewed sessions |
| `session_max_lifetime_hours` | `SESSION_MAX_LIFETIME_HOURS` | 168 | 1–2,160 | new sign-ins |
| `session_refresh_minutes` | `SESSION_REFRESH_MINUTES` | 10 | 1–60, < idle timeout | new and renewed sessions |
| `password_min_length` | `PASSWORD_MIN_LENGTH` | 12 | 8–128 | new passwords |
| `sign_in_max_failed_attempts` | `SIGN_IN_MAX_FAILED_ATTEMPTS` | 10 | 3–100 | the next failed sign-in |
| `sign_in_lockout_minutes` | `SIGN_IN_LOCKOUT_MINUTES` | 15 | 1–1,440 | the next lockout |
| `audit_log_retention_days` | `AUDIT_LOG_RETENTION_DAYS` | 90 | 1–3,650 | the next hourly cleanup |

The console's own Security settings: `CONSOLE_SESSION_CHECK_MS` (5,000; 0–60,000) and
`CONSOLE_SIGN_IN_ATTEMPTS_PER_MINUTE` (10; 1–1,000).

Never overridable, from the **`<release>-auth`** Secret (`midstream-auth` in dev),
every key optional. The Settings page's Deployment section says only whether each is configured.

| Secret key | Env var | Purpose |
|---|---|---|
| `signing-key` | `AUTH_SIGNING_KEY` | Signs sessions. At least 32 characters; without it nobody can sign in and every request is a guest's |
| `previous-signing-key` | `AUTH_PREVIOUS_SIGNING_KEY` | Still verifies sessions it signed, for rotation |
| `service-token` | `AUTH_SERVICE_TOKEN` | The console's read-only credential; at least 32 characters; same value as the console's `CONSOLE_SERVICE_TOKEN` |
| `bootstrap-admin-email`, `bootstrap-admin-password` | `AUTH_BOOTSTRAP_ADMIN_EMAIL`, `…_PASSWORD` | The first administrator (§18.8) |

A key or token shorter than 32 characters is ignored and logged.

## 18.12 Operating it

**First deployment.**

```bash
kubectl create secret generic materializer-auth -n midstream \
  --from-literal=signing-key="$(openssl rand -base64 48)" \
  --from-literal=service-token="$(openssl rand -base64 48)" \
  --from-literal=bootstrap-admin-email=you@example.com \
  --from-literal=bootstrap-admin-password="$(openssl rand -base64 18)"
```

Give the console the same service token as `CONSOLE_SERVICE_TOKEN`. Deploy, force the one
rebalance a release that adds topics needs (§17.12: 0.6.0 added `<release>-identity-requests`
and the changelogs of `identity` and `identity-audit`; 3.0.0 the changelog of `directory`, which
kaspr-operator 0.19.4 or later rebalances for on its own), sign in as the bootstrap administrator,
choose a password, and add people. A people's datasets come next: someone with `datasets.create`
creates one, and its owners share it (§18.16).

**Rotating the signing key.** Move the current value to `previous-signing-key`, put a new one in
`signing-key`, and restart the control plane. Sessions signed with the old key keep working; after
the maximum session length (7 days), remove `previous-signing-key`. Replacing the key without
moving it signs everyone out.

**The console's service token.** There is no separate console token: a console reads with the
control plane's own `service-token`, given to it as `CONSOLE_SERVICE_TOKEN`. To point a console -
a developer's included - at a running control plane, copy the value from the Secret
(`midstream-auth` in dev) without printing it:

```bash
printf 'CONSOLE_SERVICE_TOKEN=%s\n' \
  "$(kubectl -n midstream get secret materializer-auth -o jsonpath='{.data.service-token}' | base64 -d)" \
  >> console/.env  # then restart the console; the npm scripts read console/.env at start
```

To replace it, put a new value (at least 32 characters, `openssl rand -base64 48`) in the
Secret's `service-token` and in every console's `CONSOLE_SERVICE_TOKEN`, then restart the control
plane and the consoles. Until both hold the same value, a console's live views read as a guest:
the list, never details.

| Symptom | Cause | Action |
|---|---|---|
| Sign-in answers `503`, "no session signing key" | `signing-key` missing or shorter than 32 characters | Fix the Secret, restart the control plane |
| Sign-in and every change to users, groups or roles answer `500`, and the control-plane log shows `KeyError: '…<release>-identity-requests'`; no administrator was bootstrapped | The consumer group runs on an assignment made before this release added the identity topic: the rebalance was not forced | §17.12, once per environment |
| A create stays in `claiming_tables` | The claim never reached `identity-intake`, or its answer never reached `workflow-apply`: both have outputs the chart names after the release (02 §2.9) | Check the rendered agents; `GET /identity/requests/{claim_request_id}` says whether the directory answered |
| Someone cannot see a dataset's schemas or data | The dataset is locked to them: no grant on it to them or their groups, and no dataset permission held everywhere | A dataset owner grants them a role on it (Datasets › the dataset › Access) |
| The console shows only the list, never details, for everyone | `CONSOLE_SERVICE_TOKEN` unset or not equal to the control plane's | Set it; the console logs a warning at start |
| Someone is locked out | 10 wrong passwords in a row | **Unlock** on their user page, or wait 15 minutes |
| Nobody can manage users | No active administrator and no bootstrap secret | Set `bootstrap-admin-email` / `-password`, restart the control plane |
| Everyone was signed out | The signing key changed without being moved to the previous slot | Expected; sign in again |

Inspecting the store:

```bash
kafka-console-consumer --bootstrap-server $BROKERS --from-beginning --property print.key=true \
  --topic midstream-control-plane-app-develop-directory-changelog
```

The changelog holds password hashes; treat it as sensitive.

## 18.13 Design notes and known edges

**Why inside the control plane.** Every WebView authorizes against the directory, and Kaspr binds
only an app's own tables to its operations; a separate identity app would leave the control plane
reading a synchronised copy fed by a topic, plus a second deployable that could be down or on
another version. Identity is therefore a self-contained module of the control plane — its own
tables, topic, agent, tasks and views — which is the seam to cut along if it ever becomes a
service of its own. It is the same decision as for settings (§17.12).

**Why tokens are not stored.** A token is usable the moment it is issued, so sign-in needs no
round trip through the agent, and any replica can verify it. Revocation still applies at once,
because each request reads the user (epoch, status) and the session record from the global table.

**Consistency.** A change applies when the agent has written it and the replica serving a request
has read the changelog — typically well under a second. The console adds up to
`CONSOLE_SESSION_CHECK_MS` for its own checks; changes still go through the control plane, which
checks again. Open event streams notice within 30 seconds.

**Throughput.** Every sign-in, failed attempt, renewal and sign-out is one event on one
partition. At 10-minute renewals, a thousand active sessions are under two events a second.

**Reported addresses.** The console trusts `X-Forwarded-For` from its ingress (Fastify
`trustProxy`) and forwards the browser's address and user agent to the control plane, which
records them in sessions and the audit log as reported. A client that reaches the console without
passing through the ingress could choose its own address and evade the per-address sign-in limit;
the per-email lockout still holds.

**Single sign-on.** Designed in, not built: users carry `external_identities` (provider,
subject), groups carry `external_groups`, tokens record the sign-in method (`amr`), and
`GET /auth/session` lists `providers`, which the sign-in page renders. Adding an OIDC provider
means a login route that verifies the provider's ID token, finds or provisions the user by
`external_identities` or email, maps `external_groups` to groups, and issues the same session
token with `amr = "sso:<provider>"`; nothing downstream changes.

**Losing the table.** Losing `directory` and its changelog loses every account, every dataset and
grant, and every claim of a table name; bootstrap then recreates the administrator from the Secret.
Claims could be rebuilt from the materializations (the hourly sweep repairs missing ones, §18.16),
datasets and grants could not. Restore the changelog rather than recreating the table.

## 18.14 Adding a permission

1. Add a `PermissionDefinition` to `PERMISSIONS` in `identity/permissions.py`, with its category,
   label, description, `implies` (within its scope) and `scope` — `dataset` for anything about a
   dataset's tables; add it to the built-in roles that should hold it. A dataset permission also
   goes in `console/shared/access.ts::DATASET_PERMISSIONS`.
2. Name it in `authorize(...)` in the WebView that needs it — with `dataset=` for a dataset
   permission — and in `permit(...)` (or `permitAnywhere(...)` and a per-dataset check) on the
   console route; gate the UI with `useCan(...)`.
3. `AuthorizationWiringTests` fails for a view that does not authorize or names an unknown
   permission.

The full checklist, including the console and the docs, is the `adding-permissions` skill
(`.claude/skills/adding-permissions/SKILL.md`).

## 18.15 Access tokens

A person makes an access token for a tool — DuckDB on a laptop, a notebook's PyIceberg, a Spark job
— so the tool can read the Iceberg REST catalog (11 §11.8) as them without a session that expires in
hours or a password on disk (`identity/access_tokens.py`, `web/tokens.py`).

**The token.** `tok_<token_id>_<secret>`: a 12-hex public id and a random secret. It is returned once,
in the `202` of `POST /auth/tokens`, and never again. The identity table keeps only the secret's
hash (`secret_hash`) under `token:<token_id>`; the request travels on the identity topic with the
hash, never the secret. Lost tokens are revoked and replaced, not recovered.

**What it may do.** Its `permissions` are a subset of `TOKEN_PERMISSIONS` — `catalog.read` (the
default) and `catalog.write` — each one its maker held, on some dataset, when making it. It may also
be **limited to datasets** (`datasets`, by name in the request, kept by id; at most 100), each one
its maker could read through the catalog. On every request `_token_principal`
(`identity/authorization.py`) checks the secret against the hash, revocation, expiry and the owner's
account, then grants the token's permissions **intersected with what the owner holds now, on each
dataset**, and nothing outside its datasets when it is limited: a token never outlives its owner's
access. The principal's `kind` is `token`, and changes it makes are attributed to its owner.

**Limits.** A name of up to 64 characters; at most 20 live tokens per person; 1–365 days of life
(90 when none is given). Access is per dataset, not per table: a token that reads one table of a
dataset reads all of them (14 §14.8).

**Managing them.** The console's Account page lists a person's tokens with their permissions and
expiry (last use is not tracked), makes new ones — the token is shown once — and revokes them (web 11). A user
manager can revoke anyone's token through the API. Making and revoking a token are identity requests
like any directory change — applied by `identity-intake` in order, recorded in the audit log as
`token.created` and `token.revoked`.

**Who gets one.** The built-in Dataset Reader role holds `catalog.read`, and All Users holds it on
every new dataset, so anyone signed in can connect tools to the datasets they read; `catalog.write`
is held by no role but Administrator, because an external commit can rewrite what the runtime wrote
(14 §14.2).

## 18.16 Datasets, grants and table names

A **dataset** is a named group of tables and their Iceberg namespace: SQL reads `<dataset>.<table>`,
in the console and as `lake.<dataset>.<table>` through the REST catalog (11 §11.8). A dataset is
keyed by its id, `ds-<12 hex>`, with its name as a unique index, as a person is keyed by id with
their email as an index: a deleted dataset's name can be used again without inheriting its grants or
its tables. A dataset's name is fixed once made; its owners change its description.

**Making one.** Anyone with `datasets.create` (the Operator role) makes a dataset, from the Datasets
page or inline while choosing a topic's dataset; the inline one is its own request, applied before
the materialization is submitted. Its creator gets Dataset Owner on it, and All Users Dataset
Reader. Its owners change either, and grant roles to people and groups (Access tab, `PUT
…/grants/{principal}`). A grant holds dataset roles only (§18.3); Guest cannot be granted one. A
dataset can be deleted only when it holds no table: none written, kept, or still being dropped.

**Who sees what.** Everyone with `datasets.view` (the Viewer role, so everyone signed in) sees every
dataset: its name, description, owners and its tables' names. A dataset they cannot read is
**locked** to them (`locked`: they lack `dataset.view_schema` there): its schemas, rows and catalog
entries need a role on it. Names are not secret — a table called `layoffs_2027` says what it holds.
The REST catalog does not list a locked dataset at all: engines list namespaces to load them, and a
locked one would only produce errors.

**Table names.** `<dataset>.<table>` is unique across the deployment, and a table's name is unique
in its materialization too (its records, runtime state and storage directory are keyed by it). The
one writer of the directory keeps the first exact: before a table is created, request intake sends
`claim_tables` on the directory's key (`services/table_claims.py`), and `identity-intake` writes a
`dataset-table:<dataset_id>:<table>` claim for each new table, or none:

| Check, in order | Refusal `reason` |
|---|---|
| The materialization was not released for deletion meanwhile | `materialization_deleted` |
| Each dataset still exists | `dataset_not_found` |
| The requester still may add tables to each (`dataset.write`) | `forbidden` |
| Each name is free, or already this materialization's | `table_name_taken` |
| The request is well formed | `invalid` |

The refusal names the table: `field` is `tables.<index>.<dataset|name>`, and `details` gives the
`index`, `dataset`, `table` and `source_topic`. The agent answers on the work topic, keyed by the
materialization: `tables_claimed` lets the workflow go on (discovery for a create, the change of
topics for an edit), `claim_rejected` marks a create deleted with the reason, or drops an edit's
pending change (07 §7.2a). The API checks the same things first, against the directory as it stands
(`preview_identity_request`), so a conflict it can see is refused at once — `409
table_name_taken`, `404`, `403` — and only a race reaches the agent.

**Releases.** A name is released when its table leaves the catalog: a topic's table dropped, a kept
table dropped, a materialization deleted — not when its files are gone. A delete releases every name
and leaves a `claim-release` marker, so a claim that arrives after it, from the same
materialization, is refused rather than taking a name nothing would release. Removing a topic with
**keep** keeps the claim: the kept table stays in the catalog. An edit that adds tables and drops
others claims and releases in one request, all or none.

**The sweep.** Hourly, in identity maintenance, the agent compares the claims with the materializations
in the `state` table: a claim older than an hour whose materialization is gone, being deleted, or no
longer has that table is released; a written or kept table without a claim gets one back when its
name is free, and is logged otherwise. A lost release, or a claim lost with the table, therefore heals
within the hour.

**Audit.** `dataset.created`, `dataset.updated`, `dataset.deleted`, `dataset.access_changed`, and
`dataset.table_claimed` / `dataset.table_released` for each name, with the materialization in
`details` (`reason: sweep` for the sweep's own).

**What it does not protect.** Grants decide what the control plane, the REST catalog and the console
show and serve. They do not protect the files: an HMAC key that reads the bucket reads every table in
it, and so do Google credentials that PyIceberg uses. Restricting a dataset therefore restricts only
people without such credentials (14 §14.8).
