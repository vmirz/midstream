# 22 — Data protection

How a materialization keeps sensitive values out of its tables: the kinds of sensitive data the
rules recognise by field name, the masks a protected value takes, where masking happens, what each
table records of it, how a policy is chosen and changed without a restart, the protection key, and
how to operate it.

Source: `protection/` (`catalog.py`, `rules.py`, `mask.py`, `policy.py`, `plan.py`, `secrets.py`),
`runtime_app/handlers/writer.py` (step 2a), `runtime_app/committer.py` (`_record_protection`,
`_protection_with_new_fields`), `runtime_app/protection_state.py`, `runtime_app/table_state.py`
(`SchemaRecord.protection`, `SchemaView.plan`), `services/protection.py`, `services/workflow.py`
(discovery, `_initial_protection_json`), `services/request_intake.py` (`_apply_protection_update`),
`web/materializations.py` (create, update, `protection_review_view`), `tail_app/ingest.py`
(`protect`), `tail_app/table_tail.py`, `lakehouse/files.py` (`protection_revision_of`),
`lakehouse/maintenance.py`, `maintenance_app/compact_worker.py`; chart:
`charts/midstream/templates/_helpers.tpl` (`midstream.protectionKeyEnv`),
`webviews/materializations-protection.yaml`; console: `console/src/features/protection/`,
`console/server/iceberg/schema.ts` (`parseProtection`). Design: `docs/design/data-protection.md`.

## 22.1 The model in one paragraph

Topics carry personal data, and a materialization writes what it reads: without protection a value
reaches the node's staging disk, the data files in the bucket, the live tail and every reader of the
table, including engines that read the bucket with their own credentials, which access control
cannot stop (14 §14.8). With protection, the **writer masks** the values of fields that look
sensitive **before it stages them**, so nothing after the writer ever holds them. Which fields look
sensitive is decided by **rules** that recognise **kinds** of sensitive data from field names
(`email`, `phone_number`, `ssn`, `password`, …), because a schema is discovered, never declared. A
materialization's **policy** says which kinds it masks and how: **hashed** (a keyed hash: equal
values stay equal, so joins and counts still work), **partial** (a readable fragment such as the
last four digits) or **withheld** (not stored at all). A deployment-wide default, **Protected by
default**, is resolved into each new materialization; each may choose otherwise at creation or by
an edit, which reaches its running runtime within seconds, without a restart. Each table records
what it protects in the `materializer.protection` property, and each data file says in its name
which policy revision masked it.

## 22.2 Kinds and their rules

A field is recognised by its **record key** at each level of the record — the logical name, before
the schema policy makes it a field name — normalised to lowercase letters and digits:
`phone_number`, `phoneNumber`, `Phone-Number` and `PHONE NUMBER` are all `phonenumber`. A rule
matches the normalised name exactly or by its ending, some only under a parent (`name` only under
`customer`, `user`, `passenger` and other person-like parents). A plural name matches as its
singular when the plural itself matches nothing (`emails`, `phone_numbers`, `addresses`).

| Kind | Label | Default mask | Recognises, for example | Partial keeps |
|---|---|---|---|---|
| `email` | Email addresses | hashed | `email`, `email_address`, `billing_email` | the domain: `****@example.com` |
| `phone` | Phone numbers | hashed | `phone`, `phone_number`, `mobile`, `fax`, `msisdn` | the last four digits: `****9848` |
| `person_name` | People's names | hashed | `first_name`, `last_name`, `full_name`, `customer.name` | the first letter: `J****` |
| `national_id` | National identifiers | hashed | `ssn`, `tax_id`, `passport_number`, `drivers_license` | the last four characters: `****6789` |
| `card_number` | Payment card numbers | hashed | `card_number`, `credit_card`, `pan` | the last four digits: `****4242` |
| `bank_account` | Bank account numbers | hashed | `iban`, `account_number`, `routing_number` | the last four characters: `****3000` |
| `address` | Street addresses | hashed | `street_address`, `billing_address`, `address.line1` | nothing: `****` |
| `ip_address` | IP addresses | hashed | `ip`, `client_ip`, `remote_addr` | the first two octets: `203.0.*.*` |
| `date_of_birth` | Dates of birth | hashed | `dob`, `date_of_birth`, `birthdate` | the year: `1984-**-**` |
| `device_id` | Device identifiers | hashed | `device_id`, `imei`, `mac_address` | the last four characters: `****1481` |
| `secret` | Passwords, tokens and keys | withheld | `password`, `api_key`, `access_token`, `cvv` | nothing: `****` |

The full lists, with the names each kind deliberately does not match (`zip`, `city`, `username`,
`microphone`, `token` alone, …), are `protection/catalog.py`, and `python/tests/test_protection_rules.py`
holds every one as a fixture: the precision test. Rules are conservative on purpose: a false
positive masks a column people wanted to read, which is worse than a miss.

**Containers.** An object whose name matches a kind is walked, never masked whole: its leaves are
classified by their own names, and a generic leaf (`number`, `value`, `line1`, `first`, …) takes
the container's kind, so `phone: {"number": "…", "type": "mobile"}` masks `number` and leaves
`type`. Every leaf under a `secret` takes the kind, whatever its name. A list of values under a
matching name masks each value; a list of objects is walked.

**Never masked:** a null (a missing value stays missing) and a boolean (no kind's values are true or
false, so `mobile: true` stays a boolean). A boolean column is never recorded as protected either, so
no shield claims it is masked; should a string arrive under its name, the rules decide it.

`secret` allows partial and withheld only: a hashed password is a cracking target, and the API
refuses `hash` for it.

## 22.3 The masks

| Mask | Shown as | What a value becomes | Type |
|---|---|---|---|
| `hash` | Hashed | `HMAC-SHA256(protection key, text)`, the first 128 bits as 32 lowercase hex characters | string |
| `partial` | Partial | `****` with the fragment the kind keeps (§22.2) | string |
| `redact` | Withheld | nothing: the key is dropped from the record, so a field withheld from the start never exists. An object or list left empty by it is dropped too, so not even its shape reaches `_unmapped` | — |

Every mask works on a value's **canonical text**, its JSON text: a string as it is, a number as its
decimal text, so a phone number arriving as `5551234567` and as `"5551234567"` masks alike.

**Hashed** values are deterministic across every table of the deployment — and of every domain that
shares the key (§22.8) — so `count(distinct email)`, joins and `group by` work on masked columns.
Without the key a low-entropy value such as a phone number cannot be enumerated back. A hashed
column is readable by frequency: choose withheld for a low-cardinality field when equality is not
needed.

**Partial** values are for reading, not for de-identification: thousands of numbers end in `9848`,
so a partial value does not join. The hidden part is always exactly four asterisks, so a value's
length leaks nothing, and a partial never shows more than half of a value's significant characters,
nor more than four (`12345` → `****45`, `1` → `****`).

No content is normalised before hashing: `John@Example.com` and `john@example.com` hash apart.

## 22.4 The policy, and the default

A policy names kinds and their masks:

```json
{"kinds": {"email": "hash", "phone": "partial", "national_id": "hash", "secret": "redact"}}
```

**The default.** `default_protection` (Settings › Data protection, *Protected by default*;
`CONTROL_PLANE_DEFAULT_PROTECTION`) is what a create request gets when it names no `protection`. It
ships as `email=hash,phone=hash,national_id=hash,secret=redact`: emails, phone numbers and national
identifiers hashed, secrets withheld. It is stored as that text — kinds in the catalog's order, a
kind alone taking its default mask, empty for nothing — and its kind is `choice_map` (17 §17.3).
Changing it needs **Manage data protection defaults** (`settings.manage_protection`) besides
Manage settings. It is resolved when a create is accepted, like the other defaults: a change applies
to materializations created from then on, and **never touches one that exists**.

**Per materialization.** The resolved policy is recorded on the materialization with its
**revision**: 1 at creation, one more per edit. A materialization created before data protection has
revision 0 and protects nothing until an edit gives it a policy. Masking otherwise than the default,
at creation or by any edit, needs **Choose data protection** (`dataset.protect`) on every dataset
the materialization writes into (§22.10).

**An edit** sends the whole policy (`PATCH /materializations/{id}` with `protection`), which
replaces the one in force and raises the revision; repeating the policy in force changes nothing.
`"apply_rules": true` records the same policy again, so the current rules are applied to every
field (§22.6). Request intake applies an edit only to the revision the API read: a request
delivered twice changes nothing the second time, and one overtaken by another person's edit is
refused, its status saying so, rather than undo the newer choice.

## 22.5 Where masking happens

```
writer, per record (10 §10.2)
0. table by topic   1. session   2. replay guard
2a. value = plan.protect(value)            ← masked here; a new protection revision rolls the open batch
3. drift(value)   4. stage(value)   5. acknowledge
```

Drift detection sees the masked shape — a hashed `ssn` that arrived as a number is a string — so the
schema evolves to the types masked values have, and everything after the writer (the seal, its row
builder, the conformer, uploads, the committer, compaction) handles masked strings like any other
value and needs no change.

- **The plan** is built from the table's record of its protection (§22.6) and the schema, once per
  schema view: for a key whose scalar values already have a field, the table's record decides —
  protected as recorded, or plain — whatever today's rules say; a key with no such field yet (new, or
  so far only seen as an object) is decided by the rules against the policy, and the committer then
  records the new field the same way (§22.6). A record with nothing to mask is returned as the same
  object, and a table that protects nothing skips the walk.
- **Discovery** masks its sampled records before inferring schema 0, with a placeholder key: only
  the shapes of masked values matter there, and the samples are dropped.
- **The live tail** masks each record with the same plan before its own staging (`tail_app/ingest.py::protect`),
  so its buffer holds only what the table will hold and its rows are the seal's
  (`test_tail_parity.py`, with protection on).
- **Batches and files.** A batch holds rows of one protection revision: a record masked under a newer
  revision rolls the partition's open batch first. The seal names each file it writes with the
  batch's revision — `p03-o0000000100-0000000199-<member>-<seal>-0-r2.parquet` — so which policy masked
  a file's rows is known from the manifests alone, forever (`lakehouse/files.py::protection_revision_of`; no suffix
  is revision 0: written before protection, or by another writer). Compaction never mixes revisions
  in one output, and names its outputs the same way.

A member that must hash and has no key — or another key than the deployment's (§22.8) — writes
nothing: it holds the record unacknowledged, reports itself degraded (`protection_key_missing` or
`protection_key_mismatch`) and checks again every 15 s. It never stages a protected value plain.

## 22.6 What a table records

The committer owns the `materializer.protection` property beside `materializer.fork-map` — the
control plane writes it once, when it creates the table, and maintenance commits carry it forward
unchanged:

```json
{
  "version": 1,
  "revision": 2,
  "policy": {"kinds": {"phone": "partial", "national_id": "hash", "secret": "redact"}},
  "key_id": "3f9a1c2e",
  "fields": {
    "8": {"path": ["ssn"], "kind": "national_id", "mask": "hash", "since": 0},
    "10": {"path": ["customer", "email"], "kind": "email", "mask": "hash", "since": 0, "until": 3},
    "12": {"path": ["phone"], "kind": "phone", "mask": "partial", "since": 0}
  },
  "history": [
    {"revision": 1, "policy": {"kinds": {"email": "hash", "phone": "partial", "national_id": "hash", "secret": "redact"}},
     "at": "2026-11-02T10:00:00Z", "by": "jane@example.com", "schema_id": 0, "sequence_number": 0, "key_id": "3f9a1c2e"},
    {"revision": 2, "policy": {"kinds": {"phone": "partial", "national_id": "hash", "secret": "redact"}},
     "at": "2026-11-20T15:12:41Z", "by": "ops@example.com", "schema_id": 3, "sequence_number": 412, "key_id": "3f9a1c2e"}
  ]
}
```

`fields` holds every field that is or was protected, by field id, with its physical path; `since` is
the schema id from which its values were masked, and `until`, when set, the schema id from which they
are written plain again. `history` keeps the last 100 policies, each with who chose it, the schema
and the table's sequence number when it was recorded, and the key it hashed with.

| When | What the committer does |
|---|---|
| The table is created (`create_table`, on the control plane) | metadata version 0 carries the property: schema 0's fields classified by the policy, at the materialization's revision — 1 for a new materialization, the current one for a table a change of topics adds |
| A newer policy has been delivered (§22.7) | every field is classified again, family by family: a recorded kind still in the policy keeps it with the policy's mask; one that left is written plain again (`until`); a field the rules match with a kind in the policy is protected from the current schema. One metadata commit, no snapshot; then a `schema_changed` event with the same schema id carries the property to the control plane |
| A schema change adds fields | in the same commit as the schema: a field added to a family that already held values takes the family's recorded state, so a type fork of a protected field is protected; a field of a new family is classified by the rules — what the writers did to its values |

**Rules change between releases.** A release may add rules or remove one; neither touches a table by
itself: the writers follow the record for every field the schema holds, and the committer records new
fields only. A column never flips from plain to masked because the package was upgraded. Applying the
current rules to existing fields is a deliberate act: an edit, or *Apply the current rules* on the
Protection tab (`apply_rules`).

**Old rows.** An edit applies to records written from then on. Rows already written keep what they
were written with: adding a kind leaves old values plain (a string column then holds both, a numeric
`ssn` forks into `ssn__v2: string`), removing one leaves old values masked, and nothing can turn a hash
back into the value. The file names say which files hold what.

The control plane keeps the property on each schema projection (`MaterializationSchema.protection`)
and annotates each protected field (`fields[].protection: {kind, mask, since_schema_id}`), taking the
protection revision as the tie-break when a projection of the same schema id arrives. Readers of the
table through the REST catalog get the property with the table's metadata.

## 22.7 A policy edit, live

The policy is not part of the runtime's spec: it travels on the runtime control topic
(`materialization-schema-authority-updates`, 06 §6.7), which already carries runtime settings, so an
edit needs no new topic and restarts nothing.

| Step | Who | When |
|---|---|---|
| `PATCH /materializations/{id}` with `protection` | the API | validates, checks `dataset.protect` and the key, publishes the request |
| The materialization records the policy, revision +1, and the runtime's apply is queued | `request-intake` | seconds |
| The runtime's spec renders unchanged: nothing is applied to Kubernetes; the apply sends a `protection_policy` message addressed to the runtime | `workflow-apply` | seconds |
| The message is stored in the runtime's state table (`protection-policy`), the newest revision only | the runtime's settings agent | at once |
| The policy is recorded in each table (§22.6) | the committer | its next event: the commit tick comes every 5 s |
| Writers rebuild their plans and mask with the new revision, rolling a partition's open batch at its next record | every member | one changelog hop of the state table: seconds |
| The tail drops its buffer and reads the uncommitted records again under the new plan | the tail app | the schema event, or the next tick |

Every apply of a runtime sends the current policy — its creation, an edit, a resume, the sweep's
re-apply, an upgrade — so a runtime that missed one catches up at its next apply; a runtime from
before data protection drops the message kind unread.

**How far it has reached.** Each member reports on its heartbeat the protection revision it masks
each table with and the id of the key it loaded; the control plane keeps them per member, and the
masked and withheld values per table as monotonic totals. `GET /materializations/{id}` answers
`protection`:

| `state` | Meaning |
|---|---|
| `current` | every member heard from in the last two minutes masks with the current revision, and every table records it |
| `applying` | a table has not recorded it yet (`tables_behind`), or a member does not mask with it yet (`members_behind`) |
| `stopped` | the materialization is stopped: it takes the policy when it resumes |
| `not_reporting` | no member has reported in the last two minutes |

## 22.8 The protection key

`protection-key` in the auth Secret (`<release>-auth`, or `auth.existingSecret`; 18 §18.11), at least
32 characters. The chart gives it, as `PROTECTION_KEY` from `secretKeyRef` with `optional: true`, to
the control plane, the tail app, the maintenance app (which does not read it yet: the job that masks
rows already written will) and every runtime pod (`midstream.runtimeSpec`). It is never a setting and
never leaves the Secret; only its **id** — the first eight hex characters of its SHA-256 — is shown,
on the Deployment section, in `GET /system/version` (`protection_key_id`), in each table's property,
and rendered into every runtime's spec as `PROTECTION_KEY_ID`.

| Situation | What happens |
|---|---|
| No key, and a create or edit would hash | `409 conflict`, `protection_key_not_configured`, saying what to do; partial and withheld need no key. The create dialog says so before the request |
| No key, on the default as it ships | every create that takes the default is refused until the key is added (§22.9) |
| A key is added or changed | once the control plane restarts with it, it renders the new id into every runtime's spec, so the sweep re-applies each runtime once, within about two minutes. A pod reads the Secret when it starts, so a changed key reaches a runtime's members with that re-apply's restart; until then they hash with the key they started with |
| A member started without the expected key | it writes nothing for a table that hashes and reports itself degraded until re-applied with it |
| The tail app has no key, or another | a table that hashes is answered `503`, `protection_key_missing`, rather than served plain |
| The key changes | new values hash under the new key and stop joining with old ones; the table's history records the new id. A key is for the lifetime of the lake: back it up with the federation key |

**Across domains.** A token made in one domain reads tables in every domain (handbook 20). Masked
columns join across domains only when the domains share the protection key — the same choice as the
federation key, made by whoever installs them.

## 22.9 Upgrading an existing deployment

The default hashes, and hashing needs a key no installation has yet, so upgrading to the release that
brings data protection takes **one step**: add `protection-key` to the auth Secret. Best before
`helm upgrade`, which then starts every app with it:

```bash
NS=midstream; RELEASE=midstream          # the deployment's namespace and release

# Add the key to the auth Secret, generated here and never printed.
kubectl -n "$NS" patch secret "$RELEASE-auth" --type merge \
  -p "{\"stringData\":{\"protection-key\":\"$(openssl rand -base64 48)\"}}"

helm upgrade "$RELEASE" oci://ghcr.io/vmirz/charts/midstream --version <version> -n "$NS" -f midstream-values.yaml
```

Added after the upgrade instead, the key reaches the apps once they restart:

```bash
kubectl -n "$NS" rollout restart statefulset "$RELEASE-control-plane-app" "$RELEASE-tail-app" "$RELEASE-maintenance-app"
```

Runtimes need nothing: the control plane renders the key's id into every runtime's spec, so the sweep
re-applies each of them once. With `auth.existingSecret`, patch that Secret. Several domains whose
masked columns should join share one key: generate it once and patch it into each domain's Secret.
Until the key is there, existing materializations run on unchanged, and a create with the default is
refused with `409 protection_key_not_configured`; one that chooses partial or withheld is accepted.

## 22.10 Who may do what

| Permission | Scope | Held by default | Allows |
|---|---|---|---|
| `settings.manage_protection` — Manage data protection defaults | deployment | Administrator only; Manage settings does not imply it | changing *Protected by default*, besides `settings.manage` |
| `dataset.protect` — Choose data protection | dataset | Dataset Owner (and Administrator) | a create or an edit that masks otherwise than the default, on every dataset the materialization writes into; *Apply the current rules* |

Both are `danger` permissions: they decide what personal data the lake holds. Matching the default at
creation needs nothing beyond creating; any edit of the policy needs `dataset.protect` on every
dataset written into. Granted through a group, `dataset.protect` holds on every dataset, so a "data
steward" role works across the deployment.

## 22.11 API

| Request | What it says or takes |
|---|---|
| `GET /system/settings` | `protection`: every kind (`id`, `label`, `description`, `default_mask`, `masks`, `partial_example`, `examples`), the masks, `default` (kind → mask), `key_configured`, `key_id` |
| `POST /materializations` | `protection`: `{"kinds": {kind: mask}}` or `{"kinds": [kind, …]}`; absent, the default. The answer's `protection` is the resolved policy and revision |
| `PATCH /materializations/{id}` | `protection`: the whole policy, and `apply_rules`. The answer's `protection` says `before`, `after`, `revision` and what it applies to |
| `GET /materializations/{id}` | `materialization.protection`, `protection_revision`, and `protection` (§22.7) |
| `GET /materializations/{id}/protection` | the **review**: per table the recorded revision and policy, every protected field (`kind`, `mask`, `since_schema_id`, `until_schema_id`, `active`), `would_change` — what the policy with the current rules would protect otherwise — and the history; tables the caller may not see are left out and counted |
| `GET …/tables/{t}/schema` | `fields[].protection`, and `protection`: the table's property with the masked and withheld totals |
| `GET /system/version` | `protection_key_id` |

| Error | When |
|---|---|
| `400 invalid_request`, `field: protection…` | not an object; an unknown kind; a mask the kind does not allow (`secret` hashed); `fields` or `tables`, which are reserved for later versions |
| `403 forbidden`, `permission: dataset.protect` | a policy other than the default, or any edit, on a dataset the caller may not choose protection on |
| `403 forbidden`, `permission: settings.manage_protection` | setting or resetting `default_protection` without it |
| `409 conflict`, `protection_key_not_configured` | a policy that hashes, with no key in the deployment |
| tail `503`, `protection_key_missing` | the table hashes and the tail app has no protection key, or another than the deployment's |

## 22.12 The console

| Where | What |
|---|---|
| Settings › **Data protection** | *Protected by default*: a row per kind — Off, Hashed, Partial, Withheld — read-only without Manage data protection defaults; without a protection key, a warning above it that nothing can be hashed (§22.9). The Deployment section shows the protection key's id, or that none is configured |
| Create and edit dialogs | a **Data protection** block: the policy in a line, opening into the same control; locked, with the dataset named, without Choose data protection; a warning when a hashing choice meets a domain with no key; the edit dialog says what changes and that it applies to records written from now on |
| Detail › Overview | *Data protection*: the kinds in force as chips, and whether the revision has reached every member and table |
| Detail › **Protection** | the review: the policy and its rollout; what a revision still reaching the tables is about to change; per table, the protected fields — and those written plain again — with their history; the fields a newer release's rules would protect otherwise, with *Apply the current rules* for whoever may choose protection on every dataset |
| Detail › Schema, Data | a shield on every protected field and column, saying its kind and mask; the Data tab reads it from the table's own property (`console/server/iceberg/schema.ts::parseProtection`) |

## 22.13 Operating it

| Symptom | Cause | Action |
|---|---|---|
| Creates answer `409 protection_key_not_configured` | the deployment has no `protection-key` and the policy hashes | §22.9; or choose partial or withheld |
| A runtime is degraded, `protection_key_missing`, and writes nothing | its pods started without the key while a table hashes | add the key and restart the control plane: the sweep re-applies the runtime with it |
| A runtime is degraded, `protection_key_mismatch` | its pods hold another key than the control plane's — two apps reading different Secrets, or a runtime not yet re-applied after the key changed | wait two minutes for the sweep; if it stays, check that every app reads the same Secret |
| The Data tab says *Live is unavailable*: the live tail cannot mask the table's records | the tail app has no key, or another, and the table hashes | restart the tail app after adding the key; committed rows still show |
| *Applying revision N* does not end | a member does not report the revision (`members_behind`), or the committer has not recorded it (`tables_behind`) | the runtime's committer log (`runtime_protection_recorded`, or `protection_commit_failed` as a table error); a member stuck on an old revision has not refreshed its schema record |
| A column people need is masked | a rule matched its name | Choose data protection: drop the kind, or show it partial; existing rows keep their masks |
| A sensitive column is not masked | no rule matches its name; or a newer release's rules match it and the table recorded it plain, which the Protection tab lists under what the rules would change | *Apply the current rules* on the Protection tab; a name no rule matches waits for per-field choices (§22.14) |

## 22.14 Limits, and what comes next

- **Names only.** A sensitive value under a name no rule matches (`contact`, `note: "call 555-…"`), a
  bare scalar record, the record key and headers are not masked. Value-based detection and masking of
  the record key fit the same walk (design §15.5).
- **Rows already written** keep their values. A maintenance job of the maintenance app that rewrites
  the files a newer policy would mask — picked from the revision in their names — is designed
  (design §15.3), not built; its key is already wired into the maintenance app.
- **Per-field choices** (`protection.fields`) and **per-table policies** (`protection.tables`) are
  reserved in the API and refused in this version (design §15.1, §15.4).
- **No reveal.** Nobody can read a masked value back; reversible encryption with a reveal permission
  is the designed-for path (design §15.5).
- Whoever holds the protection key can enumerate low-entropy hashed values: the key lives only in the
  auth Secret, like the federation key.
- An engine with `catalog.write` can commit plaintext (14 §14.2): grant it to maintenance tools only.
