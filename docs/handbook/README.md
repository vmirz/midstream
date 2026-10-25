# Kafka Materializer Handbook

This directory is the **deep reference manual** for `kaspr-kafka-materializer` 4.1, which
materializes Kafka topics into Apache Iceberg tables. It is written to be read by humans doing
enhancement work and to be loaded as context by LLMs answering detailed design and implementation
questions.

Everything here is derived from the code and manifests in this repository, not from
aspirational design prose.

---

## Which document do I read?

| # | Document | Answers questions like |
|---|----------|------------------------|
| — | [`README.md`](./README.md) (this file) | Where do I find X? What are the doc conventions? |
| 01 | [System overview](./01-system-overview.md) | What is this thing? How do the pieces fit? What are the control loops? |
| 02 | [Deployment and manifests](./02-deployment-and-manifests.md) | What Kubernetes resources exist? What does the dev overlay change? What RBAC/secrets are needed? |
| 03 | [Configuration reference](./03-configuration-reference.md) | What environment variables exist? What are the defaults? Which are actually wired? |
| 04 | [HTTP API reference](./04-http-api-reference.md) | What endpoints exist? Request/response shapes? Status codes? Error payloads? |
| 05 | [Data model reference](./05-data-model-reference.md) | What are all the models and fields? What is persisted, under which key? |
| 06 | [Event and topic contracts](./06-event-and-topic-contracts.md) | What topics exist? What is on them? What do routed outputs look like? |
| 07 | [Control-plane workflows](./07-control-plane-workflows.md) | Step-by-step create (discovery, table creation, apply)/stop/resume/update/changing the topics (the drain)/delete (table drop)/reconcile; runtime autoscaling; the maintenance ticks. Idempotency. Failure paths. |
| 08 | [Observation and status](./08-observation-and-status.md) | How is `observed_phase` derived? Backoff? Staleness? Sweeps? |
| 09 | [Schema evolution and forks](./09-schema-evolution-and-forks.md) | Inference, the evolution rules, forks and collapses, the fork map, `_unmapped`, discovery, the control plane's projection. |
| 10 | [Runtime internals](./10-runtime-internals.md) | The writer, batches and seals, the committer and exactly once, acknowledgement, the maintenance app, the heartbeat. |
| 11 | [Iceberg tables and storage](./11-iceberg-tables-and-storage.md) | The table's layout, the catalog pointer, the table definition, snapshots, compaction and expiry, creating and dropping, the REST catalog, reading a table by hand. |
| 12 | [Operations runbook](./12-operations-runbook.md) | How do I deploy, verify, and debug this in a cluster? |
| 13 | [Testing and development](./13-testing-and-development.md) | Exact commands. Which test covers which behavior. How to add tests. |
| 14 | [Known gaps and roadmap](./14-known-gaps-and-roadmap.md) | What is unfinished, inconsistent, or risky? Where would I extend it? |
| 15 | [Glossary and FAQ](./15-glossary-and-faq.md) | What does this term mean? Quick answers to recurring questions. |
| 16 | [Versioning](./16-versioning.md) | What version is each component running, where to see it, and how to cut a release. |
| 17 | [Settings](./17-settings.md) | What can be changed from the console without a redeploy? What wins over an env var? Where are overrides stored, and how do runtimes get them? |
| 18 | [Identity and access](./18-identity-and-access.md) | How do people sign in? What may each of them do, on each dataset? Where are users, groups, roles, datasets, grants and sessions stored? How are table names kept unique? How is every request authorized? |
| 19 | [The live tail](./19-live-tail.md) | How do a table's newest records reach the console within seconds? How is each shown exactly once with the committed snapshot? What does the tail app hold, which member serves a table, and who may read it? |

---

## Related documents outside this directory

| Path | Purpose |
|------|---------|
| `/AGENTS.md` | Short always-on agent instructions: commands, project map, guardrails. Read first, kept under 300 lines. |
| `/KAFKA_MATERIALIZER_SOURCE_OF_TRUTH.md` | Condensed single-file end-to-end narrative. Good for a one-shot overview; this handbook is the deep reference. |
| `/web/docs/` | The Materializer Console's own handbook (17 documents), cited here as `web <n>`. |
| `/control-plane/` | Original design docs and contracts. **Historical intent, not authoritative.** |

---

## Authority order

When two sources disagree, trust them in this order:

1. `manifests/` — what is actually deployed (topic names, agent wiring, env vars, cron schedules).
2. `python/src/materializer_control_plane/` — what the code actually does.
3. `python/tests/` — executable specification; asserts exact payload shapes and edge cases.
4. `control-plane/*.md` and other prose — design intent, may be stale.

This handbook follows that order. Where a document in `control-plane/` conflicts with code,
the conflict is called out explicitly (see [14 — Known gaps](./14-known-gaps-and-roadmap.md)).

---

## Conventions used in this handbook

- **File references** use repo-relative paths, e.g. `python/src/materializer_control_plane/services/workflow.py`.
- **Line references** (e.g. `workflow.py:288`) were accurate at time of writing; treat them as
  navigation hints, not guarantees.
- **`<runtime-app>`**, or **`<rt>`**, is a placeholder for the resolved runtime KasprApp name,
  e.g. `kaspr-mat-a1b2c3d4e5f6` (base) or `develop-mat-a1b2c3d4e5f6` (dev overlay).
- **`web <n>`** is document `<n>` of the console handbook, `web/docs/`.
- **`<location>`** is a table's location, `<destination>/<storage prefix>/<materialization id>/<table>`.
- **Timestamps** are always UTC ISO-8601 with a `Z` suffix and second precision, e.g.
  `2026-01-15T10:30:00Z`. Produced by
  `datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")`.
- **Names in `dev`** carry a `develop-` prefix. Base names are used throughout unless the
  dev overlay is explicitly being discussed.
- Code blocks marked `# code-derived` are reproduced or paraphrased from the repository.
- **spec §n**, **D1–D10** and **E1–E14** cite the 1.0 design specification, its decisions and its
  experiments, kept only in the repository's history since they were removed from it (b729a28).

---

## Reading paths by task

You rarely need the whole handbook. Load only what the task requires.

| I am… | Read, in order |
|-------|----------------|
| New to the project | `01` → `15` (glossary) → `02` |
| Debugging a stuck materialization | `12` → `08` → `07` |
| Debugging missing data or a slow table | `12 §12.4` → `10` → `11` |
| Debugging an unexpected column change | `09` → `06` |
| Reading a table from another engine | `11 §11.9` → `11 §11.8` → `18 §18.15` |
| Adding an HTTP endpoint | `04` → `05` → `02` → `13` |
| Adding a workflow step | `07` → `06` → `13 §13.6` |
| Changing observation/phase logic | `08` → `07` → `13` |
| Changing schema evolution behavior | `09` → `10 §10.6` → `05` |
| Changing the write or commit path | `10` → `11` → `03` |
| Changing table maintenance | `11 §11.6` → `10 §10.10` → `17` |
| Changing manifests or templates | `02` → `03` → `13 §13.5` |
| Planning a larger enhancement | `14` → `01` → the relevant deep doc |
| Answering "why is it built this way?" | `14 §14.10` → `01` |
| Changing a setting, or adding a new one | `17` → `03` → `13 §13.6` |
| Debugging a setting that did not apply | `17 §17.10` → `12 §12.9` |
| Adding a permission, or gating a new endpoint | `18 §18.14` → `18 §18.3` → `04 §4.1` |
| Datasets: creating one, sharing it, a refused or stuck claim of a table name | `18 §18.16` → `07 §7.2a` → `04 §4.15` → `12 §12.3` |
| Debugging a sign-in or a refused request | `18 §18.12` → `18 §18.6` → `18 §18.7` |
| Debugging a live tail that is behind, unavailable or moving | `19 §19.16` → `19 §19.6` → `19 §19.14` |
| Changing the live tail | `19` → `10 §10.4` → `13 §13.3` |
| A runtime with more members than its replicas, or changing autoscaling | `07 §7.12a` → `12 §12.7` → `17` |
| A runtime shown Unhealthy whose members keep restarting | `12 §12.3` → `08 §8.3` → `12 §12.10` |

---

## Fast orientation (60 seconds)

```
HTTP  ──▶ KasprWebView ──▶ materialization-requests
                                    │
                                    ▼
                            request-intake agent  ──▶ materialization-work
                                                            │
                                                            ▼
                                                  workflow-apply agent
                                     discovery │ create_table │ apply_runtime │ drop_table
                                                            │
                    creates dedicated runtime ◀─────────────┘──▶ scheduler (delayed work)
                                    │
                                    ▼
      runtime KasprApp — every member: writer ─▶ batches ─▶ pipeline (seal, upload, announce)
                                                                   │
                                                          <rt>-commits (one partition)
                                                                   │
                         one member: committer ◀───────────────────┘
                              │   commit · schema · compaction · expiry · orphans
                              ▼
      Iceberg table in GCS: data/ + metadata/ + catalog-pointer.json  ◀── engines (REST catalog,
                              │                                            iceberg_scan) and
                              ▼                                            the console
              materialization-table-events ──▶ table-events agent ──▶ control-plane `state` table
      heartbeats ──▶ materialization-status ──▶ runtime-status agent ─┘   ▲
                                                 resource-observation agent + sweeps
```

Two planes:

- **Control plane** — one long-lived `KasprApp` named `materializer-control-plane`. Owns the
  HTTP API and the REST catalog, all desired and observed state, creating and dropping tables, and
  Kubernetes reconciliation. It never writes data files.
- **Data plane (runtime)** — one dedicated `KasprApp` per materialization, generated from Jinja2
  templates and applied via server-side apply. Every member reads its partitions of the source topics
  and uploads Parquet files; one member, the committer, commits them to their tables — one per topic,
  exactly once — evolves each table's schema and maintains them.

Settings take their own path: `PATCH /settings/{scope}` → `materializer-settings-requests` →
`settings-intake` agent → `settings` table, which every control-plane handler reads its effective
configuration from. Runtime settings continue on the runtime control topic to every runtime.
See [17](./17-settings.md).

The live tail takes a third: the control plane's scheduled maintenance step also names every tailed
table each minute on `materializer-tail-requests`; the **tail app** (`materializer-tail`) reads those
tables' topics and serves the records past a snapshot's offsets, which the console reads beside the
snapshot so that each record shows exactly once, within seconds. See [19](./19-live-tail.md).

Identity takes the same shape: every WebView first authorizes its caller against the `directory`
table; changes to users, groups, roles, datasets, grants and access tokens go `POST /users` (etc.) →
`materializer-identity-requests` → `identity-intake` agent → `directory` and `identity-audit`
tables, and so do the claims of table names request intake sends. Signing in is answered at once with a signed session token. See
[18](./18-identity-and-access.md).

There is **no standalone HTTP server process** in this repository. The API only exists as
`KasprWebView` custom resources attached to the control-plane app.

---

## Handbook stats

19 documents. Every factual claim is traced to a file in this repository or to a measurement on the
lab cluster; where the code is ambiguous or a source disagrees, the handbook says so rather than
guessing.

Verified for 4.1.0 against: 1,174 passing tests (`python -m unittest discover -s python/tests -p 'test_*.py'`),
`kubectl kustomize` of every overlay, the console's `npm run typecheck && npm test && npm run build`
(387 tests), and the lab cluster (13 §13.8–§13.8e).
