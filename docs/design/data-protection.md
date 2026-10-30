# Data protection: masking sensitive fields before they reach the lake

**Status:** built (2026-10-30). Written 2026-10-29 against `main` at b4fcbc9 (1.0.0-rc.4) and revised
the same day with the owner's decisions (§16). Handbook 22 describes what was built; §17 lists where
the build departs from this design — above all, a data file's protection revision is in its name,
not in a `sequence_runs` property. The rest is kept as the design that was agreed.

**Audience:** the owner, for review; then the implementer, who should be able to build from this
without coming back to its author.

---

## 0. Summary

Topics carry personal data — emails, phone numbers, card numbers, national identifiers, passwords —
and today a materialization writes every value as it arrived: into staged JSONL on the node, into
Parquet in the bucket, into the live tail's memory, and out through the console, the REST catalog
and any engine with bucket credentials. Access control (handbook 18) decides who may *query* a
table; it does not change what the files hold, and whoever can read the bucket reads every table in
it (14 §14.8).

The design adds **data protection**: a materialization **masks** the values of fields that look like
sensitive data **before they are staged**, so no component downstream of the writer ever holds them —
not the seal, not the data files, not the live tail, not a query. What is masked is decided by
**rules** that recognise **kinds** of sensitive data from field names (`email`, `phone_number`,
`ssn`, `card_number`, `password`, …), because a materialization's schema is discovered, never
declared. Which kinds a materialization masks, and how, is its **protection policy**:

- a deployment-wide **default** (`default_protection`, Settings › Data protection) applies to every
  materialization created after it is set, as `default_partition_granularity` does. It ships **on**:
  `email`, `phone` and `national_id` hashed, `secret` withheld. Changing it changes nothing already
  created;
- each materialization may **choose otherwise**, at creation and by an edit — deselecting `email`,
  showing phone numbers partially, adding `address` — and carries its own policy from then on. An
  edit reaches the running runtime **live**, through the runtime control topic that already carries
  settings: no restart, no new topic;
- changing the default needs a permission of its own, `settings.manage_protection`, held by
  administrators only by default; choosing a policy other than the default for a materialization
  needs `dataset.protect` on every dataset its tables are in, which the Dataset Owner role holds.

Three **masks**. `hash` is a keyed hash (HMAC-SHA256 under a deployment-wide protection key, 32 hex
characters): equal values stay equal, so joins, counts and group-bys on a masked column still work
across tables and materializations, while the plaintext cannot be recovered or guessed without the
key. `partial` keeps a readable fragment and hides the rest — `540-948-9848` becomes `****9848`,
`jane@example.com` becomes `****@example.com` — enough to help someone reading a row, not enough to
identify anyone; every kind defines what its partial keeps. `redact` withholds the value: nothing is
stored, not even a hash; it is the only choice for secrets besides `partial`, which for a secret
keeps nothing but the fact that there was one.

The table records what is protected: a `materializer.protection` property beside the fork map,
naming the policy, every protected field with its kind and mask, the history of changes, and which
data files were written under which policy. The Schema and Data tabs show a shield on protected
columns; a **Protection** tab reviews every table's protected fields; the REST catalog hands the
property to every engine. Masking happens once per record on the writer's event loop, a dictionary
lookup per key and one HMAC or one string operation per protected value: about 1–2 % of a member's
per-record budget, and nothing at all for a materialization that protects nothing.

Upgrading needs **one operator step**: a `protection-key` in the auth Secret, because the default
hashes. The step is one `kubectl patch`, documented with the release (§13.1); until it is done, a
create that would hash is refused with a message that says exactly that.

What the design does **not** do in this version, deliberately, and designs for (§15): mask rows
written before a policy was chosen (the `reprotect` maintenance job); detect sensitive values by
their content; choose protection per field or per table within a materialization; let anyone see
the plaintext of a masked column (reversible encryption with a reveal permission); mask the Kafka
record key.

---

## 1. Requirements, as acceptance criteria

| # | Requirement | How this design meets it | Verified by |
|---|---|---|---|
| R1 | A materialization can protect sensitive values so that no query — console, REST catalog, DuckDB on the bucket — sees them in plain text | Values are masked **before staging**: the writer hashes, shortens or withholds them, so staged lines, Parquet files and the tail hold only masked values (§4, §5.4) | `test_runtime_writer.py`: staged lines hold no plaintext; a test-cluster scan of a protected table's files and a member's staging directory (§12) |
| R2 | Protection needs no field list up front: fields are recognised as sensitive by predefined rules, from their names, as the schema is discovered and as it evolves | A catalog of **kinds** with name rules (§5.1); applied at discovery, on every record the writer stages, and by the committer when it adds a field (§5.5, §5.6) | `test_protection_rules.py`, the precision table of §5.1 |
| R3 | A deployment-wide choice of kinds, each with its mask, applies to every materialization created after it, and changing it touches nothing already running | `default_protection`, resolved into the materialization's own policy when the request is accepted, as the other defaults are; never delivered to a runtime (§5.8) | `test_web.py`, `test_settings.py` |
| R4 | A materialization can choose otherwise, at creation and by an edit, kind by kind and mask by mask, without a restart | `protection` on the create and update requests; an edit records a new policy revision and delivers it on the runtime control topic; the committer records it; the writers follow the table (§5.3, §5.6, §7.1) | `test_web.py`, `test_request_intake.py`, `test_runtime_settings.py`, `test_committer.py` |
| R5 | Managing the default is a permission, administrators only by default; overriding on a materialization needs authority over it | `settings.manage_protection` (deployment; no built-in role but Administrator); `dataset.protect` (dataset; Dataset Owner) on every dataset the materialization writes into (§5.9) | `test_identity.py`, `AuthorizationWiringTests`, console route tests |
| R6 | Masked rows the console shows before a commit are the rows the commit will hold | The tail applies the same plan before its staging and re-reads its uncommitted records when the plan changes; `build_row` is untouched, so the parity test's contract holds with protection on (§5.7, §8) | `test_tail_parity.py` with protected kinds |
| R7 | No measurable cost for materializations that protect nothing; a small, bounded cost for those that do | The walk is skipped when the plan is empty; otherwise one dictionary lookup per key and one HMAC or string operation per protected value (§9) | a micro-benchmark in `test_protection_mask.py`; per-record figures from a test cluster (§12) |
| R8 | What is protected is visible and durable: the table says which columns are masked, by what, since when, and which files were written under which policy | The `materializer.protection` table property with its `sequence_runs`, the schema projection's annotations, the console's shield and Protection tab (§5.6, §6.2) | `test_committer.py`, `test_schema_projection.py`, console tests |
| R9 | Upgrading an installation is one documented step, with the command to run | The protection key in the auth Secret, a `kubectl patch`, in the release note, the install guide and the Settings page's warning (§13.1) | the chart tests render the key into every app; the test-cluster upgrade of §12 |
| R10 | Room to grow without a redesign: per-field and per-table choices, a protection review, masking rows already written, more kinds and masks, reversible masks | Kinds and masks are registries in code; the policy is an object with fields reserved for `fields` and `tables`; the plan is built per table already; files are attributable to a policy revision from the manifests alone, which is what `reprotect` needs (§15) | — |

Out of scope, stated as limits (§10, §15): the Kafka topic itself; rows written before protection
was chosen, until `reprotect` ships; data outside the lake (the topic picker shows raw records to
anyone with `topics.view`); engines with `catalog.write`.

---

## 2. Where a value goes today, and what the design builds on

The facts the design rests on, each with its source:

| Fact | Where |
|---|---|
| The writer stages each record **as it arrived**: `{"m": {...}, "v": <value>}`, one JSON line, under `/tmp/materializer/staging/...` on the node's disk. Nothing is normalised per record | `runtime_app/handlers/writer.py::stage_source_record`, `writer/batches.py::staged_line`, handbook 10 §10.2 |
| Before staging, the writer waits for the table's schema record in the runtime's state table (`ensure_view`), then drift detection walks the whole record against the schema index | `runtime_app/table_state.py::ensure_view`, `runtime_app/schema/drift.py::DriftDetector.observations` |
| A batch is rolled by a size and age policy (`roll_batch`), sealed in a child process into one Parquet file per partition value, uploaded, and announced to the committer as a `Seal` (offsets, member, session); the committer accepts seals that continue each partition's committed offsets, several per snapshot | `runtime_app/pipeline.py`, `writer/seal.py::run_seal`, `lakehouse/commits.py::Seal`, `plan_append`, 10 §10.4 |
| The seal and the tail build rows with the same `build_row` → `Conformer.conform` | `lakehouse/rows.py`, `tail_app/ingest.py`, `test_tail_parity.py` |
| The tail keeps every uncommitted record's staged line, zstd-compressed, in memory, and re-reads from Kafka past the committed offsets | `tail_app/buffer.py::Segment`, handbook 19 |
| Discovery reads up to 200 raw records per topic and infers schema 0 from them | `schema/discovery.py`, `services/workflow.py::_process_start_discovery` |
| The runtime control topic `materialization-schema-authority-updates` carries addressed messages (`kind`, `materialization_id` or `*`), which every runtime's settings agent consumes **from the topic's start**, stores in the runtime's globally replicated state table, and every member installs within 5 s; `workflow-apply` re-sends the current settings at **every** apply, so a new or re-applied runtime catches up | `runtime_app/templates/settings-agent.yaml.j2`, `runtime_app/handlers/runtime_settings.py`, `services/settings.py::runtime_settings_output_for_apply`, `handlers/workflow_apply.py`, handbook 17 §17.7, 06 §6.7 |
| A runtime's spec hash is the hash of **every rendered resource**; the sweep re-applies — restarts — a runtime whose rendered spec differs from the last applied. Anything rendered into a template therefore restarts the runtime when it changes | `runtime/templates.py::spec_hash`, `services/observation.py` (desired hash against `last_applied_spec_hash`), `services/workflow.py` |
| Defaults are resolved when a request is accepted and recorded on the materialization; a later settings change does not affect it | `web/materializations.py::_normalize_request_spec`, handbook 17 §17.6 |
| The auth Secret is the operator's: the chart renders no Secret and reads its keys by name (`signing-key`, `service-token`, `federation-key`, …); adding a key means patching the Secret and restarting the apps that read it | `values.yaml` (`auth.existingSecret`), `docs/install.md` §2, handbook 18 §18.12 |
| The table property `materializer.fork-map` already records per-field metadata that the runtime, the control plane and the console all read; a commit may set properties and append files in one transaction | handbook 09 §9.5, `lakehouse/tables.py`, `lakehouse/commits.py` |
| Every data file's manifest entry carries its **data sequence number**, which snapshot expiry never touches; PyIceberg 0.12 writes an explicit sequence number on an added entry when given one (`ManifestWriterV2.prepare_entry` refuses only an unassigned one on a non-added entry), which is how Iceberg rewrites keep a file's sequence number | `pyiceberg/manifest.py`, `lakehouse/maintenance.py::_ReplaceFiles` |
| Compaction rewrites small files per partition into one `replace` snapshot that carries the offsets forward; the maintenance app runs it, stopped materializations included, and takes on-demand jobs through `POST /materializations/{id}/maintenance` (`MAINTENANCE_JOBS`) | `lakehouse/maintenance.py`, `maintenance_app/worker.py`, `web/materializations.py::request_maintenance`, 11 §11.6 |
| A member moves about 1,400 records/s of 1.4 KB on one core; the tail ingests at 101 µs a record | handbook 14 §14.4, 19 §19.15 |

So a sensitive value is exposed, in order: the topic (not ours), the writer's memory, staged JSONL on
disk, the seal's memory, Parquet in GCS, the tail's memory and answers, the console's grids, exports
and run directories, the REST catalog's readers, and anyone with bucket credentials. Masking **at the
writer** cuts the chain at its second link.

---

## 3. Alternatives considered

### 3.1 Masking at query time — rejected as the primary mechanism

Mask columns in the console's logical view and in the REST facade according to a permission
("reveal"). Pro: reversible by permission, no change to the files. Con: the files hold plaintext;
DuckDB on the bucket, PyIceberg with Google credentials and every engine that reads data files
directly bypasses it (14 §14.8), and the live tail, exports and run directories would each need the
same filter. It protects nothing at rest. Worth having **in addition**, once masks can be reversible
(§15), never instead.

### 3.2 Parquet modular encryption — rejected for now

Encrypt sensitive columns in the Parquet files with per-column keys (pyarrow supports writing them).
Pro: standards-based, reversible for key holders, plaintext never at rest. Con: every reader needs the
keys registered — DuckDB per connection, Spark through a KMS client — PyIceberg 0.12 does not read
encrypted files at all, and the Iceberg table-encryption specification is not final; the console
would have to decrypt in its sandbox for everyone who may query, which hands the key to the console.
Masking needs nothing of readers. Revisit when engines agree on column keys (§15).

### 3.3 Masking in the seal — rejected

Apply the mask inside `build_row`, the seam the seal and the tail already share. Pro: one place. Con:
the staged JSONL on the node and the tail's buffer would still hold plaintext; drift detection would
evolve the schema to the raw type (an `ssn` arriving as a number would be a `long` column that cannot
hold masked strings, so a fork), and discovery would infer raw types too, so three places would need
the plan anyway. Masking before staging makes the seal, the conformer and the schema policy need to
know nothing.

### 3.4 Dropping sensitive fields — covered by one mask, rejected as the default

Withholding every sensitive value loses what analytics needs most: equality. "How many distinct
customers ordered" needs a stable pseudonym, not a hole. Withholding is right for secrets, which have
no analytical value and real risk even hashed (a password hash is a cracking target), so it is the
default mask for the `secret` kind and available for any kind.

### 3.5 Masking upstream, in Kafka — out of scope

A masked copy of each topic (a stream processor, Kafka Connect transforms) would serve every
consumer, not only Midstream. It is a reasonable organisational choice, but not this system's, and it
would leave Midstream's topic picker as the only place to see the raw topic. The design neither
depends on it nor precludes it.

### 3.6 Declaring sensitive fields up front — rejected

Materializations are created before their schema is known (09 §9.6), and fields arrive for years
afterwards. A field list would be empty at creation and stale forever. Rules by name are what make
the feature hands-free, like the schema itself; per-field choices are an **override** on top of the
rules (§15), not the mechanism.

### 3.7 The policy in the runtime's template, a restart per edit — rejected after review

The first draft rendered the policy into the writer and committer agents' arguments and treated an
edit as a spec change: `spec_version`, re-apply, restart, as a replica change does. It is the
simplest wiring, but the runtime control topic already does what an edit needs — an addressed
message, stored in the runtime's state table, re-sent at every apply so a runtime that missed it
catches up (17 §17.7) — and the owner asked for live delivery whenever it needs no new topic. It
needs none. The policy therefore stays **out of the template**: rendering it would also make the
sweep restart the runtime after every live edit, since the spec hash covers every rendered resource.

---

## 4. Recommended architecture

```
                 deployment default                       per-materialization policy
                 default_protection          ──►          Materialization.protection, protection_revision
                 (Settings › Data protection;              (create, PATCH; dataset.protect)
                  settings.manage_protection)                        │
                                      ┌──────────────────────────────┼──────────────────────────────┐
                                      ▼                              ▼                              ▼
                        discovery (control plane)       workflow-apply, at every apply       tail app (tick target)
                        protect(sample) → infer         → protection_policy message on       plan from the table's record
                        → schema 0; create_table        the runtime control topic            + the policy; protect(value)
                        writes materializer.protection             │                         → stage → build_row → answer
                                                                   ▼
                                                   runtime: settings agent stores it (state table)
                                                   committer: records it in materializer.protection
                                                   (fields, history, sequence_runs) → SchemaRecord
                                                   writers: plan from the SchemaRecord;
                                                   protect(value) → drift → stage (JSONL);
                                                   a batch holds one revision; the seal, build_row,
                                                   conformer unchanged; one revision per snapshot
                                                                   │
                                                                   ▼
                                     control plane projection → Schema tab shield, Protection tab, API,
                                     REST catalog (table property) → every engine
```

Six decisions make the design:

1. **Mask before staging, by name.** The writer applies the table's protection plan to each
   record's value after the replay guard and before drift detection and staging
   (`stage_source_record`, between its steps 2 and 3). Downstream nothing changes: the seal,
   `build_row`, `Conformer`, the schema policy and compaction handle masked strings like any other
   value. The tail applies the same plan before its own staging; discovery before inference. The
   three callers share one function, `protection.plan.ProtectionPlan.protect(value)`.

2. **Kinds, rules and masks are registries in code**, like permissions and settings:
   `protection/catalog.py`. A kind has an id, a label, a description, a default mask, its name
   rules, the masks it allows and what its `partial` keeps. A mask has a name, a function, a
   result type and whether it needs the key. The policy names kinds and masks; nothing else is
   configurable in this version, and the registries are where the next versions grow (§15).

3. **The policy belongs to the materialization and travels live.** It is resolved from the
   deployment default when a request is accepted and recorded on the `Materialization` with a
   `protection_revision`; an edit replaces it and bumps the revision. Every apply of the runtime —
   the creation's, an edit's, a resume's, the sweep's, an upgrade's — sends the current policy as a
   `protection_policy` message on the runtime control topic, exactly as it sends the runtime
   settings, and an edit is simply an apply whose rendered spec has not changed, so nothing
   restarts. The runtime's settings agent stores the newest revision in the state table.

4. **The table records what is protected, and the writers follow the table.** The committer — the
   one writer of a table's metadata — turns a delivered policy into the table's record: a
   `materializer.protection` property beside `materializer.fork-map` holding the policy in force,
   every protected field by id with its kind, mask and the schema id it was protected from, the
   history of policy changes, and `sequence_runs`: which data sequence numbers were written under
   which revision. The writers build their plan from the table's record, as they build their drift
   detector from its schema, so every member masks the same way within one state-table hop, and a
   new runtime has its plan the moment it has its schema. A batch holds rows of one revision; a
   snapshot holds files of one revision; compaction never mixes revisions. Every data file is
   therefore attributable to a policy from the manifests alone, forever (§5.6).

5. **Three masks, one key per deployment.** `hash` is HMAC-SHA256 under the `protection-key` of the
   auth Secret, truncated to 128 bits, 32 lowercase hex characters, of the value's canonical text:
   equal values hash equal across every table of the deployment, so masked columns still join; the
   key never leaves the Secret, and without it a 10-digit phone number cannot be enumerated back.
   `partial` keeps a kind-specific fragment behind `****`, readable and deliberately not unique.
   `redact` withholds the value: null, and the field is never created.

6. **The key's id is part of every runtime's spec.** The control plane renders the id of the key it
   holds (eight hex characters of its SHA-256, never the key) into each runtime's spec as
   `PROTECTION_KEY_ID`, so adding or changing the key re-applies every runtime once, within the
   sweep's two minutes, and every member can check that the key it loaded is the one the deployment
   hashes with. A runtime without the expected key stages nothing for a table whose policy hashes —
   never plaintext.

### Vocabulary

| Term | Meaning |
|---|---|
| **Data protection** | The feature: Settings › Data protection; a materialization's **Protection**; the Protection tab |
| **Kind** | A kind of sensitive data the rules recognise: `email`, `phone`, `person_name`, `national_id`, `card_number`, `bank_account`, `address`, `ip_address`, `date_of_birth`, `device_id`, `secret` |
| **Rule** | How a kind is recognised from a field's name (§5.1) |
| **Mask** | What a protected value becomes: `hash`, `partial` or `redact` (shown as "hashed", "partial", "withheld") |
| **Policy** | A materialization's choice: which kinds, each with its mask (`{"kinds": {"email": "hash", …}}`), and its `protection_revision` |
| **Protected field** | A field of a table whose values are masked, as the table's `materializer.protection` records |
| **Plan** | The policy compiled against the rules and a table's record: what the writer, the tail and discovery apply to a record |
| **Revision** | A materialization's policy counter: 1 at creation (0 for materializations created before this release), one more per edit; the table records the revision it runs |

---

## 5. Components and data flow

### 5.1 The catalog: kinds and their rules (`protection/catalog.py`, `protection/rules.py`)

A field is recognised by its **record key** at each struct level — the logical name, before the
schema policy sanitises it — normalised to lowercase letters and digits only, so `phone_number`,
`phoneNumber`, `Phone-Number` and `PHONE NUMBER` are all `phonenumber`. A rule matches the
normalised **leaf** key exactly or by suffix, optionally only **under** a parent key. Rules are
deliberately conservative: a false positive hashes a column people wanted to read (`product_name`),
which is worse than a miss, and misses have an override path (§15).

A container (an object, or an array of objects) whose key matches is never masked as a whole: the
walk recurses into it and classifies its leaves by their own names, with one **inheritance** rule —
a generic leaf of a matching container (`number`, `value`, `raw`, `text`, `formatted`, `display`,
`original`, `normalized`, `e164`, `national`, `international`, `full`, `address`, `addr`, `line`,
`line1`, `line2`, `line3`, `street`, `digits`) takes the container's kind, so
`phone: {"number": "+1…", "type": "mobile"}` masks `number` and leaves `type`. An array of scalars
under a matching key masks each element.

The kinds this version ships. Tokens are normalised forms; `*x` means "ends with x". The last
column is what `partial` keeps (§5.2).

| Kind | Label | Default mask | Leaf rules | Deliberately not matched | `partial` keeps |
|---|---|---|---|---|---|
| `email` | Email addresses | hash | `email`, `emailaddress`, `emailaddr`, `emailid`; `*email`, `*emailaddress` (`billingemail`, `contactemail`) | `emailverified`, `emaildomain`, `emailcount`, `mail` | the domain: `****@example.com` |
| `phone` | Phone numbers | hash | `phone`, `phonenumber`, `phoneno`, `phonenum`, `telephone`, `telephonenumber`, `tel`, `mobile`, `mobilenumber`, `mobileno`, `cellphone`, `cellnumber`, `fax`, `faxnumber`, `msisdn`; `*phone`, `*phonenumber`, `*mobile`, `*msisdn` | `phonetype`, `phonecountrycode`, `cell` | the last four digits: `****9848` |
| `person_name` | People's names | hash | `firstname`, `lastname`, `surname`, `givenname`, `familyname`, `middlename`, `fullname`, `maidenname`, `legalname`; `*firstname`, `*lastname`, `*fullname`; and `name`, `displayname` **under** a person-like parent: `customer`, `user`, `person`, `contact`, `employee`, `member`, `passenger`, `patient`, `driver`, `rider`, `guest`, `owner`, `cardholder`, `accountholder`, `recipient`, `sender`, `buyer`, `seller`, `payer`, `payee`, `applicant`, `subscriber`, `traveler`, `traveller`, `student`, `guardian`, `beneficiary`, `insured`, `policyholder` | `name` anywhere else (`product.name`, `event.name`, a top-level `name`), `username`, `nickname`, `initials` | the first letter: `J****` |
| `national_id` | National identifiers | hash | `ssn`, `socialsecuritynumber`, `socialsecurityno`, `nationalid`, `nationalidnumber`, `nationalinsurancenumber`, `nino`, `taxid`, `taxidnumber`, `taxpayerid`, `tin`, `itin`, `passport`, `passportnumber`, `passportno`, `driverslicense`, `driverslicence`, `driverlicense`, `driverlicence`, `drivinglicense`, `drivinglicence`, `dlnumber`, `idcardnumber`, `identitynumber`, `identificationnumber`, `personalid`, `personalnumber`, `personnummer`, `bsn`, `cpf`, `curp`, `dni`, `nif`, `pesel`, `aadhaar`, `aadhaarnumber`, `sin`, `nric`, `hkid` | `ein` (a business), `licensenumber` (software too), `id`, `userid`, `customerid` | the last four letters or digits: `****6789` |
| `card_number` | Payment card numbers | hash | `cardnumber`, `cardno`, `cardnum`, `creditcard`, `creditcardnumber`, `creditcardno`, `debitcard`, `debitcardnumber`, `ccnumber`, `ccnum`, `pan`, `cardpan`, `primaryaccountnumber`; `*cardnumber` | `cc` (carbon copy), `last4`, `lastfour`, `maskedpan`, `cardbrand`, `expiry` | the last four digits: `****4242` |
| `bank_account` | Bank account numbers | hash | `iban`, `accountnumber`, `accountno`, `acctnumber`, `acctno`, `bankaccount`, `bankaccountnumber`, `routingnumber`, `routingno`, `aba`, `sortcode`, `bsb`, `clabe` | `swift`, `bic` (institutions), `accountid` | the last four letters or digits: `****3000` |
| `address` | Street addresses | hash | `address`, `streetaddress`, `street`, `addressline`, `addressline1`–`3`, `address1`–`3`, `homeaddress`, `billingaddress`, `shippingaddress`, `mailingaddress`, `postaladdress`, `residentialaddress`, `housenumber`; `line1`–`3` **under** an address-like parent (`address`, `billingaddress`, `shippingaddress`, `homeaddress`, `mailingaddress`, `residence`) | `city`, `state`, `region`, `country`, `postalcode`, `zip`, `zipcode` (quasi-identifiers people aggregate by, and readable beside a partial address; a dataset-level choice, §15) | nothing: `****` |
| `ip_address` | IP addresses | hash | `ip`, `ipaddress`, `ipaddr`, `clientip`, `remoteip`, `sourceip`, `srcip`, `remoteaddr`, `remoteaddress`, `userip`, `ipv4`, `ipv6`, `xforwardedfor`, `forwardedfor`; `*ipaddress`, `*ipaddr` | `*ip` (`zip`, `tooltip`, `membership`) | the first two octets or groups: `203.0.*.*`, `2001:db8:****` |
| `date_of_birth` | Dates of birth | hash | `dob`, `dateofbirth`, `birthdate`, `birthday`; `*dateofbirth`, `*birthdate` | `age`, `birthyear` | the year: `1984-**-**` |
| `device_id` | Device identifiers | hash | `deviceid`, `imei`, `imsi`, `meid`, `macaddress`, `mac`, `hwid`, `hardwareid`, `androidid`, `advertisingid`, `idfa`, `idfv`, `gaid`, `adid`, `udid` | `serialnumber`, `devicetype`, `devicemodel` | the last four letters or digits: `****1481` |
| `secret` | Passwords, tokens and keys | redact | `password`, `passwd`, `pwd`, `passphrase`, `secret`, `secretkey`, `clientsecret`, `apikey`, `apisecret`, `accesstoken`, `refreshtoken`, `idtoken`, `authtoken`, `bearertoken`, `sessiontoken`, `csrftoken`, `privatekey`, `encryptionkey`, `signingkey`, `cvv`, `cvc`, `cvv2`, `cvc2`, `securitycode`, `cardverificationvalue`, `pin`, `pincode`, `otp`, `otpcode`, `onetimecode`, `mfacode`, `totp`, `recoverycode`, `securityanswer`; `*password`, `*passwd`, `*secret`, `*apikey`, `*privatekey`, `*accesstoken`, `*refreshtoken` | `token` alone (a word in text data), `cid`, `authorization` (a payments object), `key` alone | nothing: `****` (there was a value) |

Each kind also declares which masks it allows: every kind allows `hash`, `partial` and `redact`,
except `secret`, which allows `partial` and `redact` only — a hashed password is a cracking target,
and the catalog refuses the choice rather than let a policy make it (`400 invalid_request`,
`field: protection.kinds.secret`).

The match is memoised per process on `(parent key, leaf key)` with a bounded cache, as drift's
`RequestDeduper` is, so a key costs one pass over the rules the first time and a dictionary hit
after.

The table above is the design's **precision test**: `test_protection_rules.py` holds every row as
fixtures — the matched names, the deliberately unmatched ones, the partial examples, and a list of
common unrelated field names (`id`, `status`, `total`, `created_at`, `name`, `description`, `items`,
`sku`, `quantity`, `price`, `currency`, `country`, `city`, `zip`, `type`, `category`, `url`,
`message`, `count`, `score`) that must classify as nothing. Adding a rule means adding both its hits
and its near-misses.

### 5.2 The masks (`protection/mask.py`)

| Mask | Shown as | Value | Type | Needs the key | Why |
|---|---|---|---|---|---|
| `hash` | hashed | `HMAC-SHA256(key, canonical(value))[:16]` as 32 lowercase hex characters | always `string` | yes | Deterministic: equal plaintexts give equal hashes across every table and materialization of the deployment, so joins and `count(distinct …)` keep working. Keyed: without the key a low-entropy input (a phone number, 10¹⁰ possibilities) cannot be enumerated; 128 bits keep collisions negligible at any lake size and halve the storage of a full digest (32 bytes a value against 64; a hex digit compresses to about 4 bits, so about 16 bytes on disk) |
| `partial` | partial | `****` followed by, or around, the fragment the kind keeps (§5.1) | always `string` | no | Readable: a support agent sees `****9848` and can confirm a caller's number; a report shows which email domains order. Not unique — thousands of numbers end in `9848` — so it identifies nobody and **does not join**; equality and `group by` give coarse groups. Deterministic, so the same value shows the same fragment everywhere |
| `redact` | withheld | null | none | no | The key is dropped from the record. Nothing is stored, no field is ever created for it, nothing goes to `_unmapped` (conform skips null values). The writer counts it as withheld |

`canonical(value)`: a string as its UTF-8; an integer as decimal text; a float as its shortest
round-trip text (`repr`); a boolean as `true`/`false`. The canonical text is the value's **JSON**
text where that is defined, so a phone number arriving as `5551234567` (a number) and `"5551234567"`
(a string) hash alike, and show the same partial. A null is never masked: a missing value stays
missing. A list of scalars is masked element by element; a container is walked, never masked whole
(§5.1).

**The partial mask, precisely.** It works on the canonical text. The hidden part is always exactly
four asterisks, however long it was, so a value's length leaks nothing (a password's, an
identifier's). What is kept is the kind's choice, in its own position:

| Kind | Significant characters | Kept | Examples |
|---|---|---|---|
| `phone`, `card_number` | the digits | the last four, after `****` | `540-948-9848` → `****9848`; `+1 (540) 948-9848` → `****9848`; `4242 4242 4242 4242` → `****4242` |
| `national_id`, `bank_account`, `device_id` | letters and digits | the last four, after `****` | `123-45-6789` → `****6789`; `DE89 3704 0044 0532 0130 00` → `****3000`; `35-209900-176148-1` → `****1481` |
| `email` | — | the domain after the last `@`, behind `****@` | `jane.doe@example.com` → `****@example.com`; no `@` → `****` |
| `person_name` | letters | the first, before `****` | `Johansson` → `J****`; `Jane Doe` → `J****` |
| `ip_address` | — | IPv4: the first two octets, `.*.*` after; IPv6: the first two groups, `:****` after | `203.0.113.42` → `203.0.*.*`; `2001:db8:85a3::8a2e:370:7334` → `2001:db8:****`; unparsable → `****` |
| `date_of_birth` | — | the four leading digits of an ISO date, `-**-**` after | `1984-07-21` → `1984-**-**`; `1984-07-21T00:00:00Z` → `1984-**-**`; anything else → `****` |
| `address`, `secret` | — | nothing | `221B Baker Street` → `****`; a password → `****` |

A tail never shows more than **half** of the significant characters, rounded down, and never more
than four: `12345` → `****45`, `123` → `****3`, `1` → `****`. A kind added later declares its own
rule, or inherits "the last four", which is what the `PartialRule` default is.

No normalisation of content before hashing in this version — `John@Example.com` and
`john@example.com` hash apart — because changing a value before hashing is a semantic decision
(emails are case-insensitive by convention, phone numbers have formats) that is better offered as a
per-kind option later (`"hash": {"normalize": "email"}`, §15) than imposed. The partial of an email
keeps the domain as written.

Masks are a registry, `MASKS: dict[str, Mask]`, each with `apply(kind, value)`, `result_type` and
`needs_key`, so `generalize` or `encrypt` (§15) is an entry and a policy value, not a redesign.

**The key:** `protection-key` in the `<release>-auth` Secret (handbook 18 §18.11), at least 32
characters, read once at start into a frozen object as `identity/secrets.py` does, refused and
logged when weak; `PROTECTION_KEY` in the control plane's, every runtime's, the tail app's and the
maintenance app's environment (§5.11). Its id — the first 8 hex of its SHA-256, as `kid` and
`federation_key_id` are — is recorded wherever hashes are recorded and rendered into every runtime's
spec, so a key that changed is visible rather than silent (§7.3).

### 5.3 The policy (`protection/policy.py`, `models/materialization.py`)

```json
{"kinds": {"email": "hash", "phone": "partial", "national_id": "hash", "secret": "redact"}}
```

`ProtectionPolicy` is a frozen model with `to_dict`/`from_dict`: `kinds`, a map of kind id to mask,
each kind from the catalog and each mask one the kind allows. `ProtectionPolicy.NONE` is the empty
policy. A request may give `kinds` as a list of ids — "the defaults for these kinds" — and the API
resolves each to its default mask, so the stored policy always says what it does; a later release
that changes a kind's default mask changes nothing already created. Two keys are **reserved** for
the extensions of §15 and refused in this version (`400`, `field: protection.fields` /
`protection.tables`, "not supported in this version"): `fields`, per-field choices, and `tables`,
per-table policies within a materialization. Any other key is refused too. The record key is not
masked in this version (D11); `kafka_key` will be a third additive key.

Where it lives:

| Where | Field | Notes |
|---|---|---|
| `Materialization` | `protection: ProtectionPolicy`, `protection_revision: int` | `from_dict` of a record without them (created before this release) reads `NONE` and `0`: nothing changes for them. A create records the resolved policy as revision 1; an edit replaces the policy and adds one. `to_dict` always emits both |
| `MaterializationRequestSpec` | `protection: ProtectionPolicy \| None` | on a create, `None` is the deployment default, resolved by the API before the event is published (the event carries the resolved policy, as it carries `partition_granularity`); on an update, `None` leaves it as it is |
| The runtime control topic | a `protection_policy` message | the policy and its revision, sent by `workflow-apply` at every apply of the runtime (§6.4) |
| The runtime's state table | `protection-policy` | the newest delivered revision, stored by the settings agent, read by the committer (§5.6) |
| The table | `materializer.protection` | the policy and revision the committer recorded, with the fields, the history and the sequence runs (§6.2) |
| `SchemaRecord` (state table) | `protection` | the table's property, parsed; the writers' and the tail's source of their plan |
| `LiveTailTarget` | `protection`, `protection_revision` | the tail's copy, refreshed every minute with the tick |

Not in the runtime's template, deliberately (§3.7): a runtime's spec hash does not change with its
policy, so an edit never restarts it, and a runtime without protection is rendered exactly as today.

### 5.4 The plan and the walk (`protection/plan.py`; `runtime_app/handlers/writer.py`)

A `ProtectionPlan` is built once per schema view from the table's record — the policy in force and
every field's recorded state (`SchemaRecord.protection`) — and the rules, and cached on the
`SchemaView`, beside the drift detector. Its one hot method is `protect(value) -> value`:

```python
# protection/plan.py (shape)
class ProtectionPlan:
    revision: int                      # the record's protection revision this plan runs
    def protect(self, value: Any) -> Any: ...
        # the record's value with protected leaves masked; `value` itself when nothing matched
    def decide(self, parent: str | None, leaf: str) -> Decision | None: ...
        # (kind, mask) for a key, memoised per schema
    def classify_schema(self, index: SchemaIndex) -> dict[int, FieldProtection]: ...
        # the committer's: every field's recorded or ruled state
    @property
    def empty(self) -> bool: ...
    counters: Counter[str]
        # masked and withheld values by kind, drained into the heartbeat
```

The walk mirrors `Conformer._struct_into` and `DriftDetector._struct`: one pass over the record's
keys, recursing into objects and arrays of objects. For each leaf key it asks `decide(parent, leaf)`:

1. If the key already has a **family** in the schema index at that struct, the **recorded** state of
   the family — its newest member's entry in the map — decides: protected as recorded, or plain when
   the table records nothing for it, whatever today's rules say. This is what keeps a column
   homogeneous when a later release adds or removes a rule (§7.2).
2. Otherwise — a key the schema does not know yet — the **rules** decide against the policy: a kind in
   the policy's `kinds` gives its mask; any other kind, or no kind, gives nothing. The answer is
   memoised for the schema.

The walk is copy-on-write: a container is rebuilt only along a path that changed, and a record with
nothing to mask comes back as the same object, so an unprotected record costs the walk and no
allocation. With `plan.empty` — the policy `NONE` and no recorded field — `stage_source_record`
does not call it at all.

In the writer, `protect` runs after the replay guard (step 2 of 10 §10.2: a record the table already
holds does no work) and before drift detection and staging:

```
0. table by topic   1. session   2. replay guard
2a. value = plan.protect(value)                                      ← new
2b. if the partition's open batch was staged under another revision, roll it   ← new
3. drift(value)   4. stage(value)   5. acknowledge
```

so drift observes the masked shape (`ssn` arrives as a string), the staged line holds the masked
value, and everything after — seal, upload, commit, tail — never sees the plaintext.
`RuntimeMalformedRecord.raw_sample` is taken after the walk too, so a skipped record's sample is
masked as well (14 §14.5).

Step 2b is what makes a data file attributable to one policy: `RuntimeBatchState` gains
`protection_revision`, set when the batch opens; a record whose plan has a newer revision first
rolls the open batch (`roll_batch`, reason `protection_changed`) and opens a new one. The seal job
and the announced `Seal` carry the batch's revision (§6.5). A plan changes when the committer
publishes a `SchemaRecord` with a new protection revision, which `current_view` notices as it
notices a new `schema_id`; members pick it up within the state table's refresh, so for a few
seconds after an edit members may stage under two revisions — in different batches, which is all
the invariant needs.

The committer gets the same plan for a different purpose (§5.6): `classify_schema` walks a schema
index and returns, for every payload field, its recorded state if any, else the rules' decision for
its logical path — what the table's protection map should say for that schema.

### 5.5 Discovery and table creation (`schema/discovery.py`, `services/workflow.py`, `lakehouse/lifecycle.py`)

`start_discovery` and `discover_tables` build a plan from the materialization's policy and the rules
alone (there is no table yet) and apply `protect` to each sampled record before `evolve`, so schema 0
has the masked types: a protected `ssn` sampled as a number is a `string` field, a withheld
`password` is no field. Only the types matter here — the samples are dropped, as today — and the
control plane has the key for it (`PROTECTION_KEY`, §5.11).

`create_table` writes `materializer.protection` into metadata version 0 (`table_properties` gains
`protection_json`): the policy and its revision (1), the fields of schema 0 the plan classifies as
protected, each with `since: 0`, the key id when a hash mask is in use, a one-entry history (`at`,
`by` the creator, `revision: 1`, `sequence_number: 0`) and `sequence_runs: [[1, 1]]` — every data
file from the first snapshot on is under revision 1 until the committer says otherwise. The
projection the step persists carries the same annotations (§6.3).

### 5.6 The committer and the protection map (`runtime_app/committer.py`, `lakehouse/commits.py`, `runtime_app/table_state.py`, `lakehouse/maintenance.py`)

The committer is the one writer of a table's metadata, so it owns `materializer.protection`, as it
owns the fork map. It has three moments, and one rule for the maintenance app.

**When a newer policy has been delivered.** Each cycle — where it already refreshes its live
settings — the committer reads the state table's `protection-policy` entry (§6.4). When its revision
is above the table's recorded revision, the committer **records** it:

1. Every payload field of the current schema is re-classified by the new policy and the current
   rules: a recorded field whose kind is still in the policy keeps its entry, with the policy's mask
   for that kind (a change from `hash` to `partial` applies to existing columns); a recorded field
   whose kind left the policy gets `until`: the current schema id; a field with no entry that the
   rules now match — because the kind was added, or because a newer release's rules match it
   (§7.2) — gets one, `since` the current schema id.
2. The property is committed with the new `policy`, `revision`, `fields`, a `history` entry (`at`,
   `by` from the delivery, `revision`, `policy`, `key_id`, `sequence_number`: the table's
   `last-sequence-number`, informative) — one metadata commit through the pointer's compare-and-swap,
   no snapshot, as `commit_schema` does.
3. The new `SchemaRecord`, with `protection`, goes to the state table, and a `schema_changed` event
   with the same `schema_id` and `changes: []` carries the property to the control plane (§6.3). The
   projection's "an older schema never replaces a newer one" rule compares `schema_id`, so it takes
   the protection `revision` as the tie-break.

Until that commit the members run the recorded states; from it, every member's next state-table
refresh rebuilds its view (`current_view` watches the record's protection revision as it watches
`schema_id`), and the writers switch plans and cut their batches (§5.4). A materialization created
by this release has its property from `create_table`, so a brand-new runtime's writers have their
plan as soon as they have their schema: nothing new to wait for.

**At every schema commit** (`_schema_cycle`): the fields the evolution added or forked are classified
by the plan and written into the property in the same commit as the schema, with `since` the new
schema id. A fork of a protected field (a `long` `ssn` that an edit started hashing, §7.1) is
classified by its logical path like any new field, so the fork is protected and the superseded
member keeps its recorded state.

**At every append commit** (`_append_cycle`): pending complete seals are grouped by the
`protection_revision` their batches were staged under, and **one group is committed per
snapshot**, the lowest revision first — which also keeps each partition's offsets contiguous, since
a partition's later batch always has the higher revision. When the group's revision differs from the
last entry of `sequence_runs`, the same transaction sets the property with a new run,
`[<this snapshot's sequence number>, <revision>]` (the metadata's `last-sequence-number + 1`,
recomputed on a retry). Every append snapshot's summary carries `materializer.protection-revision`.
A seal that arrives late under an old revision — a slow member's last batch — simply starts a short
run of its own: runs are a list, not a single boundary, and they grow by at most two entries per late
seal, which there can be at most one of per partition per edit.

**Nothing between**: the committer never changes a recorded field on its own. Rule changes in a
later release reach a table only through a policy edit (§7.2).

The invariant all this buys: **the revision a data file was written under is a function of its data
sequence number**, `revision_of(sequence_number) = the revision of the last run whose first sequence
number ≤ it`. Snapshot expiry removes snapshots and their summaries, never a live file's manifest
entry, so the mapping outlives retention; it is what `reprotect` (§15) and the Protection tab's file
view rest on, and it costs no reads: manifests carry the sequence numbers.

**Compaction keeps the invariant** (`lakehouse/maintenance.py`, the maintenance app): `LiveFile`
gains `sequence_number` from its manifest entry; `plan_compaction` groups a partition's small files
by `revision_of(sequence_number)` and never puts two revisions in one plan; and `_ReplaceFiles`
writes each output with an explicit data sequence number — the greatest of its inputs' — instead of
leaving it to inherit the replace snapshot's, so a compacted file stays in its inputs' run. PyIceberg
0.12's manifest writer accepts that (§2); it is the same rule Iceberg's own rewrites follow so that
older deletes keep applying. A table without the property has one run, revision 0, and compacts as
today. Snapshot expiry and orphan cleanup are untouched; `project_batch` moves masked strings like
any strings.

The `SchemaRecord` in the runtime's state table gains `protection` (the property, parsed), so every
member builds its plan from the same map within one changelog hop; `read_schema_record` and
`same_incarnation` cover it with the rest.

### 5.7 The live tail (`tail_app/ingest.py`, `tail_app/table_tail.py`, `tail_app/buffer.py`, `services/live_tail.py`)

The tail must build the row the seal will write (R6), so it must mask the same values the same way,
from the same record. `LiveTailTarget` gains `protection` and `protection_revision` (from the
materialization record, refreshed with every tick); the schema the tail seeds from —
`GET …/schema?raw=true` — gains the table's protection map; `TailSchema` builds the same
`ProtectionPlan` the writer builds, from the record and the policy, and `ingest.stage` applies
`protect` to the decoded value before `staged_line`. `build_row` and `build` are untouched. The
buffer's compressed staged lines therefore hold masked values only.

Because the lines are masked already, a change of plan cannot be applied to them: when a tick
carries a newer revision, or the table's record changes its protection revision, the tail **drops
the table's buffer and reads its uncommitted records again** from the committed offsets, under the
new plan — the same path as a fresh watch, a few seconds for a tail's usual few thousand rows. A
schema change alone rebuilds rows from the lines as today.

The tail app reads the key from its own environment (`PROTECTION_KEY`, from the same Secret as
`TAIL_SERVICE_TOKEN`), and the tick carries the deployment's `protection_key_id`. Without the key, or
with another, a table whose policy hashes is answered `503 protection_key_missing` rather than
served plain: the console shows committed rows and says why, as for any unavailable tail.

A policy edit reaches the tail within a minute (the tick) and the runtime within seconds; the window
between is the usual settling one — the tail's rows are the seal's once both run the new plan — and
the console's status line already says when rows are settling.

### 5.8 The control plane: settings, API, intake, delivery

**The default.** `default_protection`, a control-plane setting in a new Settings section
**Data protection** (`protection`, group platform):

| Key | Env var | Default | Kind | Label |
|---|---|---|---|---|
| `default_protection` | `CONTROL_PLANE_DEFAULT_PROTECTION` | `email=hash,phone=hash,national_id=hash,secret=redact` | `choice_map`: kind → mask | Protected by default — the kinds of sensitive data every new materialization masks, and how; a materialization may choose otherwise |

`choice_map` is a new setting kind: a map from each of the definition's `options` to one of its
`choices`, or to nothing; stored as an object, written `k=v,k=v` in the environment as `domain_list`
is a list. Its `options` carry a label, a description and the choices each allows
(`SettingDefinition.options` becomes a tuple of `{value, label, description, choices}`; `enum` keeps
plain values), so the Settings page renders the kinds with their own words and the console needs no
copy of the catalog. The definition gains `permission="settings.manage_protection"` — a generic
field: a setting that needs a permission beyond `settings.manage` to change (§5.9). The Deployment
section reports **Protection key**: configured or not, and its id, never the value.

The default is **resolved into each materialization at creation and never delivered**: changing it
changes what the next create gets, and nothing that runs (R3, D2). The Settings page says so under
the control ("Applies to materializations created from now on").

Why the default ships on (D4): the owner's call, and the reason the upgrade has a step (§13.1): a
deployment that has not added the key refuses creates that would hash until it does, with the
message below, rather than writing anything plain by default.

**`GET /system/settings`** gains `protection`: the catalog (`kinds`: id, label, description, default
mask, allowed masks, partial example), `default` as a materialization would get it (kind → mask),
`key_configured` and `key_id`. The console's create and edit dialogs read it as they read the other
defaults (`/api/config` → `describeDomain`).

**`POST /materializations`** takes `protection`: `{"kinds": [...]}`, `{"kinds": {kind: mask}}`, or
absent for the default. Validation, after step 10 of 04 §4.5: an object with `kinds` only (`fields`
and `tables` refused as reserved, §5.3); known kinds (`field: protection.kinds.<n>` or `.<kind>`);
allowed masks. Then two checks of the resolved policy: if it differs from the deployment default,
`require(principal, "dataset.protect", dataset=…)` for every dataset of the request's tables (`403`,
naming the dataset); if any mask is `hash` and the key is not configured, `409 conflict`,
`reason: protection_key_not_configured`, message "This deployment has no protection key, so nothing
can be hashed. Add `protection-key` to the auth Secret and restart the control plane (install guide
§2 and §8), or choose partial or withheld for these kinds." — as `destination_not_configured`
refuses a create nothing could finish. The accepted response and the request event carry the
resolved policy and `protection_revision: 1`; request intake stores both on the materialization.

**`PATCH /materializations/{id}`** takes `protection` (the whole policy, not a delta — the console
sends the choice as it stands): the same validation, `dataset.protect` on every dataset of the
written tables whenever the policy changes, `materializations.edit` as for any edit, the `409` when
the new policy hashes and there is no key. Repeating the current policy changes nothing. A changed
policy joins `changes`; intake (`_apply_settings_update`) records it with `protection_revision + 1`
and queues the runtime's **apply** work, as a replica change does — but `spec_version` is not
bumped and nothing rendered changes, so `workflow-apply` finds the spec hash equal, applies no
resource, and sends its deliveries (below). The answer says what changed: `protection: {"before":
{...}, "after": {...}, "revision": 4, "applies_to": "records written from now on, within seconds,
without a restart; rows already written keep their values"}`. A stopped materialization records it
at once and delivers it at resume; one whose topics are changing delivers it with the apply that
ends the drain; one at revision 0 (created before this release) goes to revision 1.

**Delivery** (`handlers/workflow_apply.py`, `services/protection.py`). Where the apply step yields
`runtime_settings_output_for_apply`, it also yields `protection_policy_output_for_apply`: the
materialization's current policy and revision as a `protection_policy` message on the runtime
control topic, addressed to that runtime (§6.4). Every apply sends it — creation, edit, resume, the
sweep's re-apply, an upgrade — so a runtime that missed one catches up at its next apply, as with
settings; and because the runtime's settings agent reads the topic from its start and the state
table keeps the newest revision, nothing is lost between. A runtime on a package before this release
drops the message unread (`handle_control_message` ignores kinds it does not know).

**Reads.** `GET /materializations/{id}` carries `protection`, `protection_revision` and, per table,
`protection: {recorded_revision, kinds}` from the projection, plus the protected and withheld totals,
and a `protection_state`: `current` when every reporting member runs the materialization's revision
and every table records it, `applying` while one lags (between an edit and the committer's record),
`stopped`, `not_reporting` — the shape of the settings rollout (17 §17.7). The schema endpoints
annotate fields (§6.3) and add `protection: {policy, revision, key_id, history}`.

**`GET /materializations/{id}/protection`** — the **protection review**, the read half of the
extension of §15 and the Protection tab's source: per table, the recorded policy and revision, every
protected field (`field_id`, `path`, `kind`, `mask`, `since`, `until`), `would_change`: the fields the
current rules and the materialization's policy would classify differently from the record
(`classify_schema` over the projection — "two more fields would be masked by the current rules"),
the `sequence_runs`, the history, and the counters. Needs `dataset.view_schema` on each table's
dataset; tables the caller may not see are left out.

**Rollout.** Each member reports `protection_revision` (its plan's) and `protection_key_id` on its
heartbeat (§6.6); `RuntimeProgress` keeps them per member, as it keeps `settings_revision`.

### 5.9 Permissions (`identity/permissions.py`)

Two permissions, following the `adding-permissions` skill:

| Key | Scope | Category | Label | Implies | Danger | Held by default |
|---|---|---|---|---|---|---|
| `settings.manage_protection` | deployment | configuration | Manage data protection defaults | `settings.view` | yes | Administrator only (`*`); **not** implied by `settings.manage` |
| `dataset.protect` | dataset | datasets | Choose data protection | `dataset.view_schema` | yes | Dataset Owner (built-in role), Administrator |

`settings.manage_protection` is deliberately independent of `settings.manage`: the point of a
separate permission is that a role tuned for operating the platform does not also decide what
personal data the lake holds. `settings-update` checks `settings.manage` as today and, when the
request sets or resets a key whose definition names a `permission`, that permission too (`403
forbidden`, naming it); `settings-scope-view` needs `settings.view` as before and reports each
definition's `permission`, so the Settings page renders the section read-only to people who hold
`settings.manage` but not this.

`dataset.protect` is a dataset permission because protection decides what a dataset's tables hold —
the dataset owner's concern — and because a materialization writes tables into several datasets:
choosing a policy other than the deployment's default needs the permission on **every** dataset of
the tables it writes (create: the request's tables; edit: the written tables), checked with
`require(principal, "dataset.protect", dataset=...)` once the datasets are known, as `dataset.write`
is for a change of topics. Held through a group it holds everywhere, so a "data steward" role works
across the deployment. Matching the default needs nothing beyond the usual: a creator who leaves the
protection block alone is making no decision. Dataset Writer does not get it (D9).

Console: `permit('materializations.edit')` plus `requireOnDatasets(auth, 'dataset.protect', …)` on
the edit route when the body carries `protection`, and the same on create; on the settings routes the
control plane's refusal is mirrored in the UI from the definition's `permission`;
`useCan('dataset.protect', datasetId)` gates the protection control of both dialogs;
`DATASET_PERMISSIONS` in `console/shared/access.ts` gains the key.

### 5.10 The console

One control, **Choose data protection**, serves every place a policy is chosen: a row per kind with
its label and description, a segmented choice of the masks the kind allows — Off · Hashed · Partial ·
Withheld — and the kind's partial example (`****9848`) under "Partial". A one-line legend: "Hashed:
equal values stay equal, so joins work. Partial: readable, not joinable. Withheld: not stored."

| Where | What |
|---|---|
| Settings › **Data protection** (new section, platform group, `permission: 'settings.view'`) | The control for `default_protection`, titled "Protected by default", with "Applies to materializations created from now on"; the Deployment section's **Protection key** line (configured, id); a warning when a hashed kind is chosen and no key is configured, linking the install guide's step. Read-only without `settings.manage_protection` ("Changing the defaults needs Manage data protection defaults") |
| Create dialog | A **Protection** block beside the tables, not behind Advanced: the control, pre-set from the default; each dataset the tables go in must grant `dataset.protect` for it to be enabled, else it is locked with "Choosing other protection needs Choose data protection on dataset finance". With no key configured and a hashed kind chosen, the dialog says the create will be refused and why |
| Edit dialog | The same block, with "Applies to records written from now on, within seconds, without a restart. Rows already written keep their values." |
| Detail › Overview | A **Protection** line: the kinds in force as chips with their masks, "applying" while a table or a member has not taken the revision yet, or "None" |
| Detail › **Protection** (new tab) | The review (§5.8): per table, the protected fields with kind, mask and since; "2 fields the current rules would also protect — edit the policy to apply the current rules"; the history; the counters; the runs as a timeline ("files from snapshot 413 on are under revision 2"). Read-only in this version; the actions of §15 (per-field choices, "mask existing rows") belong here |
| Detail › Schema | A shield on every protected field, with its kind and mask in the tooltip and the field table; the versions rail marks the schema a protection change happened at; a callout counts values withheld |
| Detail › Data | The column rail and grid header carry the shield; a masked value renders as it is (`6f1c…`, `****9848`), with "masked · email · hashed" in the row drawer. Filters work on masked columns as on any string |
| Detail › Table | The property in the metadata view; the Connect panel's snippets unchanged |
| Datasets › tables | Per table, the protected kinds |

The console's TypeScript port of the schema index (`server/iceberg/schema.ts`) parses
`materializer.protection` and `logicalColumns` carries `protection` per column;
`console/testdata/iceberg` is regenerated with a protected field so the port is tested against the
control plane's output (`python/tests/console_fixture.py`).

### 5.11 The key in the chart (`charts/midstream/`)

`protection-key` joins the auth Secret's keys (the `auth.existingSecret` comment in `values.yaml`,
`docs/install.md` §2): `PROTECTION_KEY` from `secretKeyRef` with `optional: true` in the control
plane's app (`control-plane/app.yaml`), the tail app's (`tail/app.yaml`), the maintenance app's
(`maintenance/app.yaml`, for `reprotect` later, so that job needs no second Secret step) and the
runtime spec helper (`_helpers.tpl::midstream.runtimeSpec`, `kasprContainer.env`), so every runtime
pod has it. Adding an env entry to the runtime spec changes every runtime's spec hash once, at the
upgrade that also changes their package pin, so it costs no restart of its own.

The control plane renders `PROTECTION_KEY_ID` into each runtime's KasprApp env from the key it holds
(`build_dedicated_runtime_template_input` adds `protection_key_id`; rendered `{% if protection_key_id %}`),
so a deployment without a key renders its runtimes exactly as today, and one that adds or changes the
key re-applies every runtime once, within the sweep's two minutes (D14). A member compares the id of
the key it loaded with the one expected and reports a mismatch rather than hash with the wrong key
(§11).

---

## 6. Contracts

### 6.1 The policy

```json
{"kinds": {"email": "hash", "phone": "partial", "national_id": "hash", "secret": "redact"}}
```

Accepted on input also as `{"kinds": ["email", "phone", "secret"]}` (default masks). `kinds` may be
empty; `{"kinds": {}}` is `NONE`. `fields` and `tables` are reserved and refused (§5.3).

### 6.2 The table property `materializer.protection`

```json
{
  "version": 1,
  "revision": 3,
  "policy": {"kinds": {"email": "hash", "phone": "partial", "secret": "redact"}},
  "key_id": "3f9a1c2e",
  "fields": {
    "14": {"path": ["customer", "email"], "kind": "email", "mask": "hash", "since": 0},
    "22": {"path": ["customer", "phone"], "kind": "phone", "mask": "partial", "since": 0},
    "31": {"path": ["contact_email"], "kind": "email", "mask": "hash", "since": 2},
    "40": {"path": ["ssn__v2"], "kind": "national_id", "mask": "hash", "since": 7, "until": 9}
  },
  "history": [
    {"at": "2026-11-02T10:00:00Z", "by": "jane@example.com", "revision": 1, "sequence_number": 0,
     "key_id": "3f9a1c2e",
     "policy": {"kinds": {"email": "hash", "phone": "hash", "national_id": "hash", "secret": "redact"}}},
    {"at": "2026-11-20T15:12:41Z", "by": "jane@example.com", "revision": 2, "sequence_number": 412,
     "key_id": "3f9a1c2e",
     "policy": {"kinds": {"email": "hash", "phone": "hash", "secret": "redact"}}},
    {"at": "2026-12-01T09:30:05Z", "by": "ops@example.com", "revision": 3, "sequence_number": 1180,
     "key_id": "3f9a1c2e",
     "policy": {"kinds": {"email": "hash", "phone": "partial", "secret": "redact"}}}
  ],
  "sequence_runs": [[1, 1], [413, 2], [419, 1], [420, 2], [1181, 3]]
}
```

`fields` is keyed by field id, as the fork map is, and holds each field's **current** state; `path`
is the physical path; `since` and `until` are schema ids: the schema the field was first written
masked in, and the first schema it was written plain in again (absent while it is protected).
`history` records each policy in force, who chose it, the key in use and the table's
`last-sequence-number` when it was recorded. `sequence_runs` is the machine truth: `[first data
sequence number, revision]`, ascending; a data file with sequence number `s` was written under the
revision of the last run with first ≤ `s` — above, files 413–418 under revision 2, file 419 a late
seal under revision 1, 420 on under revision 2, 1181 on under revision 3 — and `history[revision]`
says what that revision masked, and how. `key_id` is the key the hashes were made with; a change of
key is recorded in `history` as a new entry with the new id.

### 6.3 The schema projection and API

Each field entry of a projection (`render.py::_member_entry`) gains, when the table's map has it:

```json
{"field_id": 14, "path": ["customer", "email"], "name": "customer.email", "type": "string",
 "kind": "value", "state": "current",
 "protection": {"kind": "email", "mask": "hash", "since_schema_id": 0}}
```

and the schema answer (`GET …/schema`, `…/schema/versions/{id}`, and `?raw=true`) gains
`protection: {policy, revision, key_id, history, withheld: {"secret": 1204}}` — `withheld` from the
status counters. `MaterializationSchema` stores `protection` (the property) beside `fork_map`;
`schema_changed` events carry it as `protection`.

### 6.4 The delivery and the tick

On the runtime control topic (`materialization-schema-authority-updates`, 06 §6.7), beside
`runtime_settings`:

```json
{
  "kind": "protection_policy",
  "materialization_id": "mat_7f3a…",
  "revision": 3,
  "policy": {"kinds": {"email": "hash", "phone": "partial", "secret": "redact"}},
  "changed_by": "ops@example.com",
  "published_at": "2026-12-01T09:30:04Z"
}
```

`materialization_id` is the runtime's own, so the settings agent's filter lets it through and every
other runtime's drops it; there is no broadcast form. `handle_control_message` dispatches on `kind`;
`apply_protection_policy_message` stores it in the state table under `protection-policy` —
`{"revision", "policy", "changed_by", "published_at", "applied_at"}` — when its revision is above the
stored one, and does nothing otherwise, as settings deliveries do. A runtime from before this release
drops the kind unread. Ordering needs no lineage: the revision counts one materialization's edits,
and a materialization id is never reused.

`LiveTailTarget` gains `protection` (the policy dict), `protection_revision` and
`protection_key_id`; `from_dict` reads a missing policy as `NONE`, revision `0`.

### 6.5 The seal, the announcement and the snapshot

`RuntimeBatchState.protection_revision` (the revision the batch was staged under; `0` when absent),
`SealJob.protection_revision` and the announced `Seal.protection_revision` (`None` from an older
member reads as `0`). Every append snapshot's summary carries `materializer.protection-revision`;
compaction's `replace` carries the revision of its inputs' run. A new summary property is additive
(16 §16.3, rule 6 names the existing ones as the contract; this adds one).

### 6.6 The heartbeat and status

`RuntimeStatusEvent` gains `protection_revision` (the member's plan) and `protection_key_id` (the key
it loaded, or null); `table_counters[table]` gains `protected_values` (values masked),
`withheld_values` (values withheld) and `protected_by_kind: {kind: n}`; `RuntimeProgress` keeps them
per member and cumulative per table (`cumulative_protected_by_table`, …) as it keeps
`cumulative_unmapped_by_table`; the detail API, the review and the schema answer's `withheld` read
them.

### 6.7 Settings

```python
_cp(
    "default_protection",
    section="protection",
    label="Protected by default",
    kind="choice_map",
    options=protection_kind_options(),          # ({value, label, description, choices}, …) from the catalog
    default={"email": "hash", "phone": "hash", "national_id": "hash", "secret": "redact"},
    permission="settings.manage_protection",
    description="The kinds of sensitive data every new materialization masks, and how. "
                "A materialization may choose otherwise. Applies to materializations created from now on.",
    effect="new materializations",
)
```

`CONTROL_PLANE_DEFAULT_PROTECTION=email=hash,phone=hash,national_id=hash,secret=redact` in the
environment. `GET /settings/control-plane` reports `permission` on the definition; `PATCH` refuses a
change to it without that permission.

### 6.8 Errors

| Status | `reason` / `field` | When |
|---|---|---|
| `400 invalid_request` | `protection`, `protection.kinds.<n>`, `protection.kinds.<kind>`, `protection.fields`, `protection.tables` | not an object; an unknown kind; a mask the kind does not allow; a reserved key |
| `403 forbidden` | `permission: dataset.protect`, `dataset` | a policy other than the default on a dataset the caller may not choose protection for |
| `403 forbidden` | `permission: settings.manage_protection` | changing `default_protection` without it |
| `409 conflict` | `protection_key_not_configured` | a `hash` mask with no protection key in the deployment, on create or edit |
| tail `503` | `protection_key_missing` | the tail app has no key, or another key, for a table whose policy hashes |
| runtime phase reason | `protection_key_missing`, `protection_key_mismatch` | a member without the expected key; it stages nothing for a table whose policy hashes (§11) |

---

## 7. Behaviour on change

### 7.1 A policy edit

An edit applies to **records written from now on**. Rows already in the table keep what they hold,
and the runs say which files hold what. The sequence, with its timings:

| Step | Who | When |
|---|---|---|
| `PATCH …/materializations/{id}` with `protection` | the API | validates, checks `dataset.protect` and the key, publishes the mutation request |
| The materialization records the policy, revision +1; the apply work is queued | `request-intake` | seconds |
| The runtime's spec is rendered and found unchanged: no resource applied; the `protection_policy` message is sent to the runtime | `workflow-apply` | seconds |
| The message is stored in the runtime's state table | the runtime's settings agent | at once |
| The policy is recorded in the table: fields re-classified, property committed, `SchemaRecord` published, `schema_changed` sent | the committer | its next cycle: within the commit interval, 30 s by default |
| Writers rebuild their plans, roll their open batches, stage under the new revision | every member | within the state table's 5 s refresh |
| The first snapshot under the new revision opens a new run | the committer | its next append |
| The tail re-reads its uncommitted records under the new plan | the tail app | the next tick, within a minute |
| The console shows "current" | the control plane | once every member and every table report the revision |

Three cases, all recorded in the table:

| Change | New rows | Old rows | Column |
|---|---|---|---|
| A kind added; the field is a `string` (`email`) | masked | plain | one column, mixed by time; `sequence_runs` says which files are which, `fields[id].since` which schema. The `reprotect` job masks the old rows (§15) |
| A kind added; the field is not a string (`ssn` arrived as a number, `long`) | masked → the drift detector sees a `string` for a `long` family → the schema policy **forks** `ssn__v2: string`, which the committer classifies protected | plain in `ssn` (superseded) | two columns, each homogeneous; the console folds them as it folds any fork (the fold mixes a number and a hash in one logical column — the shield on the logical column says so) |
| A kind's mask changed (`hash` → `partial`) | partial | hashed | one column, mixed; `history` and the runs say which files hold which. Nothing can turn a hash into a partial |
| A kind removed | plain | masked | one column, mixed; `until` records it. Nothing can recover the old plaintext |

The edit is the explicit moment at which **the current rules** are applied to every field (the
committer re-classifies the whole schema): a field a newer release's rules would now match becomes
protected at the next edit, not silently in between (§7.2). The Protection tab shows the difference
beforehand (`would_change`, §5.8), so an administrator who wants the new rules applied re-saves the
policy unchanged — a same-policy save is a no-op at the API, so the console sends it with
`"apply_rules": true`, which bumps the revision without changing the policy.

### 7.2 Rules change between releases

A release may add rules (more hits) or remove one (a false positive found). Neither touches a table
by itself: the writer follows the **recorded** state for every field the schema has, and the
committer records new fields only. A column therefore never flips from plain to masked because the
runtime's package pin advanced. New fields arriving after the upgrade are classified by the new
rules, as they should be. An administrator who wants the new rules applied to existing fields edits
the policy, or applies the rules from the Protection tab (§7.1).

### 7.3 The key changes

Hashes under a new key do not equal hashes under the old: joins across the change break, and a column
holds two incomparable kinds of hash. The design makes it visible and consistent — the key id is
rendered into every runtime's spec, so a changed key re-applies every runtime within two minutes and
no member hashes with the old one for longer than its restart; the committer records the new id in
the property's `history`; the Deployment section shows the current id; `GET /system/version`
reports `protection_key_id` so a shared console can flag domains that disagree (as it does for
`federation_key_id`). Otherwise the key is **for the lifetime of the lake**: the install guide says
to back it up with the federation key, and the roadmap holds no rotation for hashes, because
re-hashing needs plaintext that no longer exists (§15).

### 7.4 Across domains

A token made in one domain reads tables in every domain (handbook 20). Masked columns join across
domains only when the domains share the protection key (D7); the console's About dialog says whether
they do, as it does for the federation key. Sharing it is the operator's choice: the same trade as
the federation key, documented beside it.

### 7.5 The default changes

Nothing running changes (R3). The next create resolves the new default; every existing
materialization keeps the policy it has, and the console's edit dialog shows how a materialization's
policy differs from the current default ("differs from the deployment default: phone is partial
here").

---

## 8. Fidelity: forks, collapses, `_unmapped`, the tail

- **`build_row`, `Conformer`, the seal and `test_tail_parity.py` do not change**; both producers mask
  before staging, so the parity test's premise — the same staged line — still holds, and the test
  gains protected kinds and all three masks in its random records so it checks that too.
- **`_unmapped`** holds the JSON of values no field could take: with masking before staging, a
  protected value that lands there is its mask. A withheld value lands nowhere, and neither does an
  object or list that withholding emptied (`credentials: {secret: {…}}` under `secret`): it is
  dropped with its values rather than leave `{}` for `_unmapped` (found on a test cluster).
- **Collapsed families** (`__json` fields) hold JSON text of masked values. A collapsed struct that
  contained a protected leaf holds the mask inside its JSON.
- **Forks**: a masked field is always a string, so protection itself never forks a field that was
  protected from its first record; a field protected by a later edit forks when its column was not a
  string (§7.1); a type that forks for its own reasons (a string `phone` becoming a struct, as on the
  load-test topic) forks as today, and each member is classified by its logical path
  (`customer.phone` and `customer.phone.number`, through inheritance).
- **Tombstones and non-objects**: a null value or a bare scalar record has no keys; the plan returns
  it unchanged. A bare string record (`"john@example.com"` as the whole value) goes to `_unmapped`
  under `$` as today, unmasked: rules need a key. Named in §10 as a limit; value-based detection
  (§15) would cover it.
- **The record key and headers**: not masked in this version (D11, §15); `_kafka_key` and
  `_kafka_headers` are written as today.
- **The tail after an edit**: its buffer is dropped and its uncommitted records re-read under the
  new plan (§5.7), so the rows it shows are the rows the seal writes once both run the new revision.

---

## 9. Performance budget

| Work | Where | Cost | Against |
|---|---|---|---|
| Policy `NONE`, no recorded field | writer, tail, discovery | one attribute check per record | — |
| The walk, no match | writer event loop | one dictionary lookup per key (memoised decision), no allocation; about 5–10 µs for a 28-key record in CPython | a member's ~700 µs per record on the load-test topic (14 §14.4): about 1 % |
| A hashed value | writer event loop | one HMAC-SHA256 of a short input (~1.5 µs) plus the hex rendering; a container rebuilt along the path | a record with three protected fields: ~5 µs more |
| A partial value | writer event loop | a scan for the significant characters and one concatenation (~1 µs) | less than a hash |
| A policy switch | writer, committer | each partition's open batch rolled once (one more, smaller file per partition); one extra snapshot for the first group under the new revision; the tail re-reads its uncommitted rows once | per edit, not per record |
| The tail | tail ingest | the same walk and masks; against 101 µs a record (19 §19.15): under 10 % at three protected fields, less otherwise | — |
| Discovery | control plane, per create | up to 200 records per topic, once | negligible |
| Classification | committer, per schema commit and per recorded policy | one pass over the rules per field, memoised | negligible |
| Compaction grouping | maintenance app, per plan | a binary search in `sequence_runs` per live file; no reads | negligible |
| Storage | the table | a hashed value is 32 bytes before compression, about 16 after; a partial is 4–20; an email of ~22 bytes or a phone number of 12 compresses to a few. A hashed column costs about 10–14 bytes a value more on disk than its plaintext | a 100 M-row table with two hashed columns: about 2.5 GB more |

The acceptance test is a micro-benchmark in `test_protection_mask.py` (10,000 records through
`protect`, asserting a per-record ceiling of 25 µs on the CI runner with three protected fields, one
of each mask) and a test cluster's per-record throughput measured with and without protection on the
load-test topic (§12).

---

## 10. Security model

**What it protects.** From the writer on, a protected value exists only as its mask or not at all:
staged JSONL on the node, the seal's memory, data files in the bucket, the tail's buffer and answers,
the console's run directories, grids and exports, the REST catalog's readers, DuckDB or Spark on the
bucket with their own credentials — the readers that access control cannot reach (14 §14.8). Logs:
`raw_sample` is taken after the walk; protocol events carry shapes only; nothing in the runtime logs
a value.

**What each mask gives away, by design.**

| Mask | What a reader learns |
|---|---|
| `hash` | that two rows hold the same value (the point of it), and so the frequency of each value: a hashed low-cardinality column (a country, a gender) is readable by frequency. Choose `redact` where equality is not needed |
| `partial` | the fragment: the last four digits of a phone number leave about a million candidates in one area code; a name's initial; an email's domain, which for a personal domain is the person. A partial is for reading, not for de-identification, and the owner chose it knowing that |
| `redact` | that the field existed in the schema, nothing else |

**What it does not protect.**

| Exposure | Why it remains | Mitigation |
|---|---|---|
| The Kafka topic | not the lake's | upstream controls; `topics.view` governs the console's topic picker, which shows raw records |
| Rows written before protection was chosen | files are immutable; the edit applies going forward | the `reprotect` job (§15); until then the runs record where the plaintext is |
| A sensitive value under a key the rules do not recognise (`contact: "john@…"`, `note: "call 555-…"`) | names only | add a rule; value-based detection (§15); per-field choices (§15) |
| A bare scalar record, the record key, headers | no key to match; not in this version | §15 |
| Whoever holds the protection key can enumerate low-entropy inputs | the key is what makes the hash safe | the key lives only in the auth Secret, like the federation key; the console never has it; it is never a config value or a setting. Its **id** is rendered into runtime specs and reported by the API, which reveals nothing of it |
| An engine with `catalog.write` can commit plaintext | 14 §14.2 already warns | grant it to maintenance tools only |
| Time travel to a snapshot before a `reprotect` | old files are referenced by old snapshots | the job expires them (§15) |

**Who may weaken protection.** Only `settings.manage_protection` changes the default; only
`dataset.protect` on every dataset a materialization writes into lets it mask less (or more) than
the default; both are `danger` permissions that no built-in role but Administrator and Dataset Owner
holds. Every change is recorded in the table's history with who made it, and the materialization's
request events carry `submitted_by` as today.

---

## 11. Failure modes

| Situation | What happens |
|---|---|
| No protection key, a create or edit with a `hash` kind | `409 protection_key_not_configured` at the API, with the step to take; nothing is accepted. The dialogs warn beforehand; the Settings page shows the key as not configured |
| The control plane was upgraded before the key was added | Every create with the default is refused with that `409` until the key is in the Secret and the control plane restarted (§13.1); existing materializations run on. Creating with partial or withheld masks works meanwhile |
| A member starts without the key, or with another key, and a table's policy hashes | It reports `protection_key_missing` / `protection_key_mismatch` (phase `deploying`, as `waiting_for_table` is) and stages nothing for that table — never plaintext. Tables whose policy does not hash are written. With the key id in the spec, this lasts only until the sweep re-applies the runtime, so it is a sign of two apps reading different Secrets |
| The key is removed from the Secret | The control plane, restarted, renders no key id: every runtime is re-applied without the key and stages nothing for hashing tables; the tail answers `503`; the Settings page warns. Restore the key |
| The key is replaced | Every runtime is re-applied once; new rows hash under the new key; the committer records the new key id in the history; joins across the change break (§7.3) |
| A `protection_policy` delivery is lost or arrives before the runtime exists | The runtime's agent reads the topic from its start, and every later apply — the sweep's included — re-sends the current policy; the materialization shows `applying` until a member reports the revision |
| A runtime older than this release receives the message | It drops the kind unread. Impossible in practice: upgrading re-applies every runtime onto this package within minutes, and a materialization's policy is edited after that |
| The committer cannot commit the property (the CAS keeps losing) | Retried with the next cycle, as schema commits are; members keep the recorded states meanwhile; a `table_error` `protection_commit_failed` degrades the materialization until it lands |
| A late seal under the old revision arrives after the first snapshot of the new | Committed in a snapshot of its own; `sequence_runs` gains a short run for it (§5.6). Nothing is mixed |
| A policy edit while the materialization is stopped | Recorded at once; delivered by the apply at resume; the committer records it at load |
| A rule in a new release misclassifies a common field | It affects new fields only, and only in materializations whose policy includes the kind; the fix is a rule change, and the recorded state keeps columns created meanwhile as they are |
| The tail seeds from GCS because the control plane is down | The property is in the table's metadata, so the tail builds the same plan from the table itself; it has no policy for *new* keys until the control plane answers again, and masks what the record names |
| Compaction meets a table whose property predates `sequence_runs` (none exist: the property ships with the runs) or has none | One run, revision 0: it compacts as today |

---

## 12. Test plan

**Unit** (`python/tests/`):

| Module | Covers |
|---|---|
| `test_protection_rules.py` | the §5.1 table as fixtures: every hit, every near-miss, the unrelated-names list; normalisation; inheritance under containers; `secret` refusing `hash`; each kind's partial examples |
| `test_protection_mask.py` | determinism and canonical text (a number and its string mask alike; a float's repr); hash length and alphabet; the key id; every partial rule and the half-and-four bound; `redact`; the walk's copy-on-write (identity when nothing matched); the per-record ceiling |
| `test_protection_plan.py` | recorded state beats the rules; novel keys follow the rules; `classify_schema` over a fixture schema with forks; counters; `revision` |
| `test_runtime_writer.py` | a staged line holds masks and no plaintext (the test scans the staging directory); drift observes `string` for a masked number; a withheld key raises no observation; `raw_sample` masked; the open batch rolled when the plan's revision changes and the new batch's `protection_revision` |
| `test_runtime_settings.py` | `protection_policy` messages: stored when newer, ignored when not, another runtime's dropped, malformed dropped; an unknown kind dropped |
| `test_discovery.py`, `test_workflow.py` | schema 0 types under protection; `create_table` writes the property with runs; `discover_tables` with a policy |
| `test_committer.py` | a delivered policy recorded (fields, `until`, `history`, `revision`, the property-only `schema_changed`); the property written with a schema commit (`since`); one revision per snapshot, lowest first, and the run opened in the same transaction; a late old-revision seal's short run; the summary property; CAS loss retried |
| `test_maintenance.py` (lakehouse) | compaction groups by run; outputs carry the greatest input sequence number; a table without the property compacts as today; `revision_of` |
| `test_schema_projection.py`, `test_table_events.py` | field annotations; `protection` on the projection; the revision tie-break |
| `test_tail_parity.py`, `test_tail_app.py` | protected kinds and all three masks in the random records; the tail's rows are the seal's with protection on; the buffer dropped and re-read on a new revision; `503` without the key |
| `test_web.py` | create and update: defaults resolved, list and map forms, unknown kinds, disallowed masks, reserved keys, `dataset.protect` on each dataset, `409` without the key, the revision, a repeated policy a no-op, `apply_rules`, the response's `protection`; the review endpoint and its `would_change`; `protection_state` |
| `test_request_intake.py` | the policy and revision stored at create and replaced on update; the apply work queued; `spec_version` unchanged |
| `test_workflow_apply.py` | the `protection_policy` output at every apply, addressed to the runtime; none for a materialization without a runtime |
| `test_runtime_templates.py` | the policy **not** in the template; the key id rendered only when configured; the spec hash unchanged by a policy; the key in the env |
| `test_settings.py` | `choice_map` parsed from the environment and validated against options and choices, options with labels and choices, `permission` reported and enforced; the default value |
| `test_identity.py` | the two permissions, their scopes and implications; Dataset Owner holds `dataset.protect`; the default-groups pin updated deliberately |
| `test_agent_manifests.py`, `test_chart_*.py` | authorization wiring of every changed view; settings wiring; the chart renders the key into the four apps |
| `test_models.py` | round-trips of the policy, the record fields, the event fields, the tick target, the batch and seal fields |

**Console** (`node --test`): the settings section for `choice_map`; the dialogs' permission gating
and the key warning; the Protection tab's rendering of the review; the schema port parsing the
property and the runs; logical columns' `protection`; the fixture table with a protected field.

**A test cluster** (handbook 13's practice, console 14's checks), on a topic with emails, phones, a numeric
`ssn`, a `password` and a struct `phone`:

1. **Upgrade first, key second**, as an operator might: deploy the release without the key; confirm
   a create with the default is refused with the `409` and its message, and that one with partial
   masks is accepted. Then follow §13.1: patch the Secret, restart the apps; confirm every runtime is
   re-applied once, and the Settings page shows the key.
2. Create a materialization with the defaults and one deselecting `email` and making `phone`
   partial — as a Dataset Owner, then as a Writer expecting `403`.
3. Verify: DuckDB through the REST catalog finds no plaintext email or phone in the protected table,
   finds `ssn` as a string column of hashes, `phone` as `****9848`-shaped strings and no `password`
   column; a scan of a member's staging directory finds no plaintext; the live tail's rows before the
   first commit equal the committed rows after it; the Schema tab shows the shields; the Protection
   tab shows the fields; `materializer.protection` is in the table metadata.
4. Edit the policy to add `email` and change `phone` to `hash`: confirm **no restart** (pod ages),
   the state `applying` then `current` within a minute, the new run in `sequence_runs`, mixed values
   either side of it, the extra snapshot, and the tail following within a minute.
5. Let compaction run across the boundary: confirm no output file mixes revisions
   (`revision_of` of each output against its rows) and the outputs' sequence numbers.
6. Throughput with and without protection on the load-test topic, 16 members, against §9.
7. Remove the key from the Secret, restart the control plane: every runtime re-applied, hashing
   tables stage nothing, `protection_key_missing`; restore.

---

## 13. Rollout and compatibility

- A **minor release** (1.1.0, handbook 16 §16.3): additive API fields and one new endpoint; a new
  setting, section and setting kind; two permissions and a built-in role change (Dataset Owner); a
  new message kind on an existing topic, addressed so older runtimes drop it; a new snapshot summary
  property; an optional Secret key with one env entry in four apps.
- Records, events and tables **without** a policy read as `NONE`, revision 0: nothing changes for
  existing materializations, their runtimes' spec hashes change only by the package pin and the new
  env entry as at every upgrade, and the console shows no shield for them.
- The tail app and the control plane of this release serve older tables (no property) as plain.
- Mixed versions: a 1.0 runtime drops the new message kind (§11). The one-rebalance rule for
  releases that add topics (17 §17.12) does not apply: no topic is added.
- Docs: a new handbook chapter, **22 — Data protection**, and the touched sections of 03, 04, 05, 06,
  09, 10, 11, 16, 17, 18, 19 and the console's 03, 06, 07, 11, 12; `docs/install.md` §2 and §8.

### 13.1 Upgrading: the protection key

The default hashes, and hashing needs a key that no installation has yet, so the upgrade has one
step the operator must take. Nothing is lost by taking it after the upgrade: existing
materializations run on either way; only creates that would hash are refused in between.

```bash
NS=midstream; RELEASE=midstream          # as in the install guide

# 1. Add the key to the auth Secret. It is generated here and never printed.
kubectl -n "$NS" patch secret "$RELEASE-auth" --type merge \
  -p "{\"stringData\":{\"protection-key\":\"$(openssl rand -base64 48)\"}}"

# 2. Restart the apps that read the Secret at start: the control plane, the tail app and the
#    maintenance app (kaspr-operator names each app's StatefulSet `<KasprApp>-app`).
kubectl -n "$NS" rollout restart statefulset "$RELEASE-control-plane-app" "$RELEASE-tail-app" "$RELEASE-maintenance-app"

# 3. Nothing to do for runtimes: the control plane renders the key's id into every runtime's spec,
#    so the sweep re-applies each of them once, within about two minutes.
```

Keep the key with the federation key: it is for the lifetime of the lake (§7.3). Several domains that
should join on masked columns across domains use the **same** key (D7) — generate it once and patch it
into each domain's Secret. With `auth.existingSecret`, patch that Secret instead. On a **new**
installation the install guide's §2 adds `--from-literal=protection-key="$(openssl rand -base64 48)"`
to the `create secret` command, and there is no step.

What the operator sees if the step is skipped: the Settings page's Data protection section says the
key is not configured; a create with the default answers `409 protection_key_not_configured` with the
message of §5.8; the console's create dialog says so before the attempt. The StatefulSet names above were confirmed
on a test cluster, and so was the restart under kaspr-operator: it leaves the restart annotation in
place (its drift check does not compare template annotations), each app restarts once, and the sweep
re-applied every runtime once. The install guide's §8 and handbook 22 §22.9 carry the same commands.

### 13.2 The release note (handbook 16, a new §16.16)

> ## 16.16 What 1.1.0 changed: data protection
>
> 1.1.0 masks sensitive fields — emails, phone numbers, national identifiers, payment cards, passwords
> and more, recognised by their names — before a materialization writes them, so the lake never holds
> them in plain text (handbook 22). It is a minor release: nothing an older peer cannot ignore, and
> 1.0.x upgrades to it in place with `helm upgrade`. It has **one operator step**: a protection key.
>
> | Change | Who must care |
> |---|---|
> | **The auth Secret needs a `protection-key`** (at least 32 characters; `openssl rand -base64 48`), read at start by the control plane, the tail app, the maintenance app and every runtime. The default protection hashes emails, phone numbers and national identifiers, and hashing needs the key: until it is set, creating a materialization with the default answers `409 protection_key_not_configured`. Add it and restart the three apps: `kubectl -n <ns> patch secret <release>-auth --type merge -p "{\"stringData\":{\"protection-key\":\"$(openssl rand -base64 48)\"}}"`, then `kubectl -n <ns> rollout restart statefulset <release>-control-plane-app <release>-tail-app <release>-maintenance-app`. Every runtime is then re-applied once by itself. Domains that should join on masked columns across domains share one key, like `federation-key` | every deployer, once |
> | **Protected by default**: Settings › Data protection, `default_protection` (`CONTROL_PLANE_DEFAULT_PROTECTION`), ships as `email=hash,phone=hash,national_id=hash,secret=redact` and applies to materializations **created from now on**; existing ones are untouched (`NONE`). Changing it needs the new permission `settings.manage_protection`, which only Administrator holds; `settings.manage` does not imply it | administrators who want other defaults, or none |
> | **Per materialization**: `protection` on `POST` and `PATCH /materializations` (`{"kinds": {"email": "hash", "phone": "partial", …}}`); choosing other than the default needs the new dataset permission `dataset.protect` on every dataset the materialization writes into, which Dataset Owner holds. An edit applies live, without a restart, to records written from then on; `GET /materializations/{id}/protection` reviews what each table protects | dataset owners; scripts that create materializations and want no protection (`{"kinds": {}}`) |
> | **Tables** gain the property `materializer.protection`, and a data file masked under a protection revision ends `-r<revision>.parquet`; compaction never mixes revisions in one output | readers of table metadata; nothing changes for queries |
> | The runtime control topic carries a second message kind, `protection_policy`, addressed to one runtime; a 1.0 runtime drops it. No topic is added: no rebalance is needed | none |

---

## 14. Implementation phases, mapped to files

Each phase leaves the suite green and is reviewable alone.

| # | Phase | Files |
|---|---|---|
| 1 | **The library**: catalog, rules, three masks, policy, plan, secrets; tests | `python/src/midstream/protection/{__init__,catalog,rules,mask,policy,plan,secrets}.py`, `models/materialization.py` (`ProtectionPolicy`, `protection_revision`), `tests/test_protection_*.py` |
| 2 | **The table's record**: property with runs, committer recording of a delivered policy, classification at schema commits, one revision per snapshot, state table record, event and projection; compaction grouping and sequence numbers | `lakehouse/tables.py` (`PROPERTY_PROTECTION`, `table_properties`), `lakehouse/commits.py` (`Seal.protection_revision`, `protection_of`, the property commit, the run in the append transaction, the summary property), `lakehouse/maintenance.py` (`LiveFile.sequence_number`, grouping, `_ReplaceFiles` explicit sequence numbers), `runtime_app/committer.py`, `runtime_app/table_state.py` (`SchemaRecord.protection`, `current_view`), `lakehouse/schema/render.py`, `services/table_events.py`, `services/schema_projection.py`, `web/materializations.py` (schema views) |
| 3 | **The runtime's walk and the delivery**: writer, batch roll, counters, seal and announcement fields, the settings agent's new kind, heartbeat, discovery, creation | `runtime_app/handlers/writer.py`, `runtime_app/models/processing_state.py`, `runtime_app/writer/{batches,seal}.py`, `runtime_app/pipeline.py`, `runtime_app/handlers/runtime_settings.py`, `runtime_app/state.py` (counters), `runtime_app/handlers/status.py`, `runtime/templates.py` (`protection_key_id`), `runtime_app/templates/*.j2` (the env entry), `schema/discovery.py`, `services/workflow.py`, `lakehouse/lifecycle.py`, `models/status.py`, `models/events.py` |
| 4 | **The control plane**: setting and kind, permissions, API (create, update, review, state), intake, the apply's delivery, system settings and version, chart and install guide | `settings/catalog.py` (`choice_map`, `permission`), `settings/effective.py`, `web/settings.py`, `config.py`, `identity/permissions.py`, `web/materializations.py` (create, update, `protection_review`), `services/request_intake.py`, `services/protection.py` (`protection_policy_output_for_apply`), `handlers/workflow_apply.py`, `web/system.py`, `charts/midstream/templates/{control-plane/app.yaml,tail/app.yaml,maintenance/app.yaml,_helpers.tpl,webviews/materializations-protection.yaml}`, `values.yaml`, `docs/install.md` |
| 5 | **The tail** | `models/live_tail.py`, `services/live_tail.py`, `tail_app/{ingest,table_tail,buffer,worker,config,views}.py` |
| 6 | **The console** | `console/server/settings/` (`choice_map`), `server/routes/{settings,materializations,data}.ts`, `server/iceberg/schema.ts`, `server/data/logical.ts`, `shared/access.ts`, `src/features/{settings,materializations,detail,schema,data}/…` (the control, the Protection tab), `python/tests/console_fixture.py` and `console/testdata/iceberg` |
| 7 | **Docs and a test cluster** | handbook 22, 16 §16.16 and the touched chapters; console docs; the test-cluster run of §12 |

Phases 1–3 make a materialization protect data end to end for anyone setting the policy by hand on a
record; 4 makes it a product; 5 and 6 make the console truthful. The order lets the first review be
of the smallest dangerous thing — the rules and the masks — before anything touches a table.

---

## 15. Extension points and roadmap

The owner asked that these be designed for, not merely listed. Each adds to a registry, to the policy
object or to the walk without changing how a value reaches the table; the first four are designed
below to the point where their implementation is a contract addition.

### 15.1 Per-field choices

"Mask `notes`", "never mask `support_email`", "hash this `phone` even though the policy says partial":
an override on one materialization, consulted **before** the recorded state and the rules.

- **Contract**: the reserved `fields` key of the policy, `{"<logical path>": "hash" | "partial" |
  "redact" | "plain"}`, with an optional kind for a field the rules know nothing about
  (`{"notes": {"mask": "hash", "kind": "free_text"}}`); refused today, accepted then. Paths are
  logical (`customer.email`), as the schema API names them.
- **Plan**: `decide(parent, leaf)` checks the field map first; `classify_schema` records a chosen
  field with `chosen: true` and the choice's kind, so the Schema tab can say "chosen" rather than
  "rule".
- **Committer**: a change of `fields` is a policy edit like any other — revision, delivery,
  re-classification — so `since`/`until` and the runs cover chosen fields too.
- **Console**: the Protection tab's rows get the action: "Mask this field", "Leave this field plain",
  each writing the policy's `fields` through `PATCH`; the shield on a chosen field says so.
- **Permission**: `dataset.protect`, as for any deviation from the default.

### 15.2 The protection review, with actions

The read half ships in this version (`GET /materializations/{id}/protection`, the Protection tab). The
actions come with 15.1 and 15.3: "Apply the current rules" (`apply_rules`, §7.1, shipped),
per-field choices, "Mask rows already written" (`reprotect`), and, per table, a per-table policy
(15.4). The review is also where a **dataset owner** sees every materialization writing into the
dataset (Datasets › the dataset › Protection), reading the same endpoint per materialization.

### 15.3 `reprotect`: masking rows written before a policy change

*Built, with the departures in §17; handbook 22 §22.14 describes it as it is.*

A maintenance job of the existing maintenance app, planned like compaction, that rewrites the data
files holding plaintext a later policy masks.

- **What it rests on**: `sequence_runs` (§5.6). A file in run `r` was written under policy `r`; the
  fields it holds plain that the current policy `R` masks are exactly `fields` protected under `R`
  whose `since` is after the file's revision, or whose kind `R` masks and `r` did not — computable
  from the property alone, by field id, with no reads.
- **Request**: `POST /materializations/{id}/maintenance {"job": "reprotect", "table": …}`
  (`MAINTENANCE_JOBS` gains it), needing `materializations.maintain` **and** `dataset.protect` on the
  table's dataset; from the Protection tab, "Mask rows already written", which shows how many files
  and rows the runs say are affected before asking.
- **Plan** (`lakehouse/maintenance.py::plan_reprotect`): the live files whose run revision `< R` and
  which hold at least one affected field, oldest first, within `compaction_max_input_bytes` and
  `MAX_PLAN_INPUT_FILES`; the job re-queues itself while candidates remain, as the compaction backlog
  does.
- **Rewrite** (the compaction child process, `maintenance_app/compact_worker.py`): each input is read,
  each affected column's values are masked with `R`'s mask for the field's kind — the same
  `protection/mask.py` the writer uses, with `PROTECTION_KEY` from the maintenance app's environment
  (wired in this version, §5.11) — and a field that forked for type (`ssn` → `ssn__v2`, §7.1) is
  masked into the fork column and nulled in the old one. Outputs are written per partition as
  compaction writes them, then committed in one `replace` snapshot (`commit_reprotect`): offsets
  carried forward unchanged, `materializer.operation: reprotect`, `materializer.protection-revision:
  R`, and the outputs under the snapshot's **own** sequence number — they now conform to `R`, so they
  belong in the current run. Liveness and row-count checks as compaction's.
- **What it cannot do**: turn a hash into a partial or a plaintext (there is none), or unmask a kind
  a policy dropped — those columns stay as they were, and the review says so. A field masked under
  `r` and plain under `R` is left masked in a file now labelled `R`: over-protection, recorded by
  `until`.
- **After**: the replaced files stay referenced by older snapshots until expiry (72 h by default) and
  are deleted by orphan cleanup after `orphan_min_age_hours`; the job's completion event says so, and
  an operator who needs the plaintext gone sooner runs `expire_snapshots` and `orphan_cleanup` for the
  table from the same endpoint. Time travel to a snapshot before the job shows the old files until
  then (§10).
- **Idempotent**: a rewritten file is in run `R`; a second run finds nothing.

### 15.4 Per-table policies within a materialization

A materialization of several topics whose tables need different protection.

- **Contract**: the reserved `tables` key, `{"<table name>": {"kinds": {...}, "fields": {...}}}`:
  a table's own policy replaces the materialization's for that table (not a delta), so a table's
  policy is always one object. Refused today, accepted then.
- **Already in place**: the plan is built per table from the table's own record; `create_table`,
  the committer, the tail's target and the review are per table; the delivery message carries the
  whole policy object, and `ProtectionPolicy.for_table(name)` is the one place that resolves it —
  today returning the materialization's. The API, the dialogs (a per-table section of the control)
  and `dataset.protect` per table are the work.

### 15.5 More masks and detection

| Extension | What it adds | Where it plugs in |
|---|---|---|
| **`generalize`** | A coarser value rather than a fragment: an age band from a date of birth, a /24 from an IP, an outward postal code, a salary band | a `MASKS` entry with per-kind targets; the policy already stores a mask per kind. `partial` already keeps the year of a date of birth and the first octets of an IP, so this is for numeric kinds added later |
| **Value-based detection** | Recognise an email, an E.164 number, a Luhn-valid card number, an IP, in a string under *any* key, and in a bare scalar record | a `values` rule per kind; the same walk, applied to string leaves the name rules did not classify; a policy switch `"detect": ["names", "values"]`; the per-string regex cost measured first |
| **Content normalisation** | lowercase emails, digits-only phone numbers before hashing | a parameter of `hash` per kind (`{"hash": {"normalize": "email"}}`) |
| **Reversible masks and a reveal permission** | `encrypt`: deterministic AES-GCM-SIV under a deployment key, equality preserved; `data.reveal` on a dataset lets the console — and only it — decrypt on read; engines see ciphertext | the mask registry; a console-side decrypt in the run's projection; key custody as for the protection key. This is the designed-for path to "authorised people see the plaintext" (§3.1) |
| **The record key and headers** (D11) | `kafka_key: hash \| partial`, `kafka_headers` | the policy; `stage` in both producers; `protect_key` on the plan |
| **Dataset-level policy** | A dataset owner sets a floor every materialization writing into the dataset must meet | `Dataset.protection`; the create and update views check that the materialization's policy covers the floor; a `set_dataset_protection` identity request |
| **Key rotation** | Not possible for hashes (no plaintext to re-hash); possible for `encrypt` (decrypt and re-encrypt in a `reprotect`-like job) | the `encrypt` mask |

---

## 16. Decisions

All taken by the owner on 2026-10-29. D1–D12 are the first draft's, as decided; D13–D16 arose from
the review and are stated here for the record.

| # | Decision | Decided |
|---|---|---|
| D1 | Where masking happens | Before staging, in the writer (and the tail, and discovery) |
| D2 | How a materialization gets its policy | A deployment default resolved at creation; the materialization's own policy from then on; an edit replaces it. Changing the default never touches a running materialization |
| D3 | How a policy edit reaches the runtime | **Live**, on the runtime control topic — allowed because it needs no new topic: the topic already carries addressed messages that every runtime's settings agent stores, and every apply re-sends the current state. Had it needed a topic, an edit would have been a spec change with a restart |
| D4 | The default on a new deployment | `email`, `phone` and `national_id` hashed, `secret` withheld — on, and adjustable in Settings |
| D5 | The masks | `hash` (128 bits truncated, 32 hex), `redact`, and `partial` — applicable to every kind, each kind defining what it keeps (§5.2) |
| D6 | The masks for secrets | `redact`, with `hash` refused; `partial` keeps nothing but the fact of a value |
| D7 | The key's scope | One per deployment, the same across domains when cross-domain joins on masked columns are wanted |
| D8 | Who manages the default | A permission of its own, `settings.manage_protection`, not implied by `settings.manage` |
| D9 | Who overrides on a materialization | `dataset.protect` on every dataset the materialization writes into; Dataset Owner holds it; Dataset Writer does not |
| D10 | Rule changes between releases | Recorded states hold; new fields follow the new rules; an edit re-applies the rules |
| D11 | Record key masking | Later (§15.5) |
| D12 | Content normalisation before hashing | None in this version |
| D13 | Names | "Data protection", "kinds", "mask", "hashed / partial / withheld", "Protected by default", "Choose data protection", "Manage data protection defaults" |
| D14 | The key's id in every runtime's spec | Yes: adding or changing the key re-applies every runtime once, so no member hashes with a stale key and a missing key is a transient, not a stall. The alternative — render it only for runtimes whose policy hashes — would make the first hashing edit of a materialization a restart |
| D15 | Files attributable to a policy | Yes, from manifests alone: one revision per batch and per snapshot, `sequence_runs` in the property, compaction grouping by run and explicit sequence numbers on compacted files. The cheaper alternative, a marker in each Parquet footer, would cost a footer read per file at every compaction plan |
| D16 | The default carries masks, not only kinds | `default_protection` is kind → mask (`choice_map`), since `partial` makes the mask a real choice; the first draft's set of kinds (`choice_set`) could not say "phone numbers partial by default" |

Still to confirm, all details:

1. **The partial fragments of §5.2** — the last four for numbers and identifiers, the domain for
   emails, the initial for names, the first two octets for IPs, the year for dates of birth, nothing
   for addresses and secrets — and the half-and-four bound.
2. **D14**: the one-time re-apply of every runtime when the key is added or changed, in exchange for
   no stale-key state.
3. **`reprotect`** (§15.3) is designed but not scheduled: ship it in 1.1.0 behind the Protection
   tab, or as 1.2.0 after the rest has run on a test cluster? *Decided 2026-10-30: built after the
   rest ran on a test cluster, with a purge asked each time and a request on its own or with an edit
   (§17).*
4. **The `secret` kind's `partial`** keeps nothing (`****`), so that choosing partial for everything
   never shows part of a password. Agreed, or should `secret` refuse `partial` as it refuses `hash`?

---

## 17. As built: where the implementation departs from this design

Built on `feature/data-protection`, 2026-10-30. Handbook 22 is the reference; these are the
differences from the sections above, each for the reason given.

| Design | As built | Why |
|---|---|---|
| D15, §5.6: `sequence_runs` in the property, one revision per snapshot, compaction grouping by run, explicit data sequence numbers on compacted files, a `materializer.protection-revision` snapshot summary | A data file's revision is in its **name**, `…-r<revision>.parquet` (`lakehouse/files.py::protection_revision_of`; none for revision 0). A batch holds rows of one revision — the writer rolls its open batch when the revision changes — so every file a seal writes has one. Compaction never puts files of two revisions in one output and names each output with its inputs' revision. No per-snapshot grouping, no sequence-number bookkeeping, no summary property | Nothing reads data file names, so the name is free to carry it; the manifests alone still say which policy masked each file, with no grouping in the committer and no bookkeeping in compaction |
| §5.8: `choice_map` stored as an object; its options in `options` | Stored as **text**, `kind=mask` separated by commas in the catalog's order, a kind alone taking its default mask, empty for none (`protection/catalog.py::normalize_protection_text`); the definition carries `choice_options` and `choice_labels`, and `enum` keeps `options` | Every other setting is a scalar: the settings table, its history and the env var keep one shape |
| §5.8: an edit is the whole policy | Also `apply_rules: true`, recording the policy in force again with the current rules (the Protection tab's *Apply the current rules*); and intake applies an edit only to the revision the API read (`protection_base_revision`): a redelivery changes nothing, an overtaken edit is refused in the status | The review's action needed an API; without the base revision a delayed request could undo a newer choice |
| §5.8: an edit is an apply with an unchanged spec | So it is, and that apply now keeps a healthy runtime **running** instead of showing it deploying (`spec_unchanged` in `_should_preserve_running_status_during_reapply`) | Nothing in the cluster changes, so there was nothing to wait for |
| §5.9: `dataset.protect` on every dataset written into | Also on the datasets of tables the same edit adds or writes again | They are masked by the new policy too |
| §5.7: `LiveTailTarget` carries `protection` and `protection_revision` | The tail takes the table's protection with its schema — the seed's `?raw=true` schema view and every `schema_changed` event carry the property — the target carries the deployment's `protection_key_id`, and the hints the table's `protection_revision` | One source for the schema and its protection, already ordered by position |
| §6.6: heartbeat | `protection_revisions` (table → revision) and `protection_key_id`; masked and withheld values in each table's `table_counters`. A member that must hash and cannot reports `runtime_reported_degraded` with `health_reason` `protection_key_missing` or `protection_key_mismatch` | Health reasons are where the console and the control plane already look |
| §5.1: rules | Booleans and nulls are never masked, and a boolean column is never recorded as protected (`has_email` matched the email rule on a test cluster); a plural name matches as its singular when the plural matches nothing; every leaf under a `secret` takes the kind (`inherit_all`); `pincode` left the secret rules | Precision: `mobile: true` stays a boolean; `emails` and `addresses` are common; a secret object's every value is secret; `pincode` is often a postal code |
| §5.9: the console checks `dataset.protect` on the edit route | The console's route checks `materializations.edit` and calls the control plane as the person, which checks `dataset.protect` — as it does `dataset.write` for a change of topics | One authority; the dialog locks the control from the same permission |
| — | The Table tab hides `materializer.protection` with the fork map from people who may not see the dataset's schemas | It names the fields and their kinds |
| §15.3 `reprotect` | Built on `feature/reprotect` (2026-10-30), with the departures below; handbook 22 §22.14 is the reference | The owner asked for it after the rest had run on a test cluster |
| §15.3: a file's plain fields from its run revision and the property's `fields` | Each protected field records what the writers did to it under every revision (`FieldProtection.states`, the last 100; `known_from` where a record from before kept none). A field is **born** under the revision in force when the first schema holding it was committed, and a revision before its birth is judged by the rules and that revision's policy, as a lagging writer decided. The manifests' `value_counts`/`null_value_counts` say which fields a file holds values of | `since`/`until` say when a field changed, not what each revision did; after two edits a file's revision alone could not say whether a field was plain in it. Births make a key a lagging writer met first exact. Statistics spare files that hold no value of a field |
| §15.3: outputs under the current run's sequence number, a rewritten file "in run `R`" | A rewritten file's name carries its whole **lineage**, newest first: `reprotect-<id>-r3p1.parquet` (`lakehouse/files.py::protection_label_of`); compaction groups by the whole lineage | A rewritten file still holds values masked under its older revisions; its lineage is what says which, so a later reprotect neither misses nor re-masks them |
| §15.3: the rewrite in the compaction child, outputs per partition | Its own child, `maintenance_app/reprotect_worker.py`: one output per input, the same rows in the same order, so the commit checks each output against its input. Values masked before stay as they are; a number field's values move masked into the family's newest string field of the same list depth, and a file waits (**blocked**, left whole) while there is none; fields in lists of lists are blocked | One-for-one keeps the checks exact and the outputs' lineage single. Rewriting a file in part would make its name claim what it does not hold |
| §15.3: a `replace` commit as compaction's | `_RewriteFiles`: the snapshot keeps no `deleted` manifest entry of the files it removes | A deleted entry keeps the file's column bounds, which hold the very plain text the job masks, in the current snapshot |
| §15.3: replaced files go with expiry and orphan cleanup; an operator may run them sooner | **`purge`**, asked each time (the owner's choice): once nothing is left to rewrite, every older snapshot is expired and the files only they held deleted, whatever their age; the console's dialogs check it by default, the API defaults to `false` | Time travel would otherwise read the plain text for 72 hours or more; the purge ends time travel to before, so it stays a choice |
| §15.3: a request through `POST …/maintenance`, from the Protection tab | Also `protection.reprotect` on `PATCH`, and *Also mask rows already written* in the edit dialog (the owner's choice: on request, or after an edit). Either is one `reprotect` request event: request intake puts a `ReprotectRequest` on each table's record, which every tick carries until the app reports it done, waiting for the edit's revision | A one-shot message would be lost with a restarted member, and an edit's revision takes seconds to reach each table |
| §15.3: the Protection tab shows how many files and rows are affected | The maintenance app counts each table's plain text from the manifests when its revision changes, its compaction is planned or a reprotect commits, and reports it (`plaintext_counted`) when it changed; the record keeps it (`PlaintextReport`), the review shows it | The console reads the review only; counting where the files are planned anyway costs nothing extra |

The four points of detail left in §16 were built as designed: each kind's partial fragment as in
§5.2, the key's id in every runtime's spec (D14), `reprotect` later — now built, above — and a
secret's partial `****`.
