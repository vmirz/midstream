# 21 — Kafka Access and ACLs

Which Kafka permissions a deployment's user needs, for a cluster that enforces ACLs — Kafka's
authorizer with `allow.everyone.if.no.acl.found=false`, Strimzi's `authorization: simple`, a
managed Kafka — where someone other than the person installing the chart makes the user and its
ACLs. Each grant is traced to the code that needs it: Midstream's own Kafka clients
(`python/src/midstream/kafka/`, `schema/discovery.py`, `tail_app/`) and, under every app, Kaspr
0.11.31 on twm-faust 1.17.23 and aiokafka 0.14.0. §21.9 says how the list was checked against a
broker that enforced exactly these ACLs.

## 21.1 One Kafka user for the whole deployment

The chart gives every Kaspr app the same Kafka client authentication, `kaspr.authentication`
(02 §2.3): the control plane, the maintenance app, the tail app and, through the runtime spec,
every runtime the control plane creates. A deployment is therefore one Kafka principal,
`User:<kaspr.authentication.username>`, which needs the union of what its apps do (§21.4). The
chart has no way to give one app a user of its own, so a runtime holds the control plane's grants,
deleting other runtimes' topics included.

- **SASL only.** The user signs in with SCRAM-SHA-512, SCRAM-SHA-256 or PLAIN, over TLS when
  `kaspr.tls` is set. The principal is the SASL user name: kaspr-operator refuses TLS client
  certificates (`TLS authentication is not supported`).
- **What needs no Kafka user.** kaspr-operator acts through Kubernetes and the apps' web API alone;
  the console reaches the control plane and the tail app over HTTP; engines use the REST catalog.
- **Several domains on one cluster.** Give each release (20) a user of its own, granted on its own
  name (§21.2). A prefixed grant matches every name that starts with it, so `midstream-` also
  covers a release named `midstream-b`: name releases so that none, followed by `-`, begins
  another's — `domain-a` and `domain-b`, not `midstream` and `midstream-b`.

## 21.2 The names the grants cover

Everything a deployment creates in Kafka is named after its release. `<release>` is the chart's
`midstream.fullname`: the release name or `fullnameOverride`, cut to 30 characters (02 §2.9).
kaspr-operator gives each Kaspr app the id `<KasprApp name>-app` (`K_APP_NAME`), and Faust uses
that id as the app's consumer group and to name its own topics and transactional ids. Runtimes are
named `<runtimeNamePrefix>-mat-<12 hex>`, and `controlPlane.settings.runtimeNamePrefix` is the
release name unless set, so every runtime's names start with `<release>-mat-`.

| What | Names | Named by |
|---|---|---|
| App ids, which are also the consumer groups | `<release>-control-plane-app`, `<release>-maintenance-app`, `<release>-tail-app`; `<release>-mat-<id>-app` for each runtime | kaspr-operator |
| The deployment's topics | `<release>-materialization-requests`, `-work`, `-status`, `-table-events`, `-schema-authority-updates`; `<release>-settings-requests`, `-identity-requests`, `-maintenance-requests`, `-tail-requests` (06 §6.1) | the chart |
| Every app's own topics | `<app id>-assignor-leader`, its leader election; `<app id>-<table>-changelog` for each of its tables, `<table>` being the KasprTable's name: `midstream-control-plane-app-midstream-state-changelog` | Kaspr (`kaspr/core/leader_assignor.py`), Faust (`faust/tables/base.py`) |
| The control plane's scheduler | `<app id>-schedule-requests`, `-schedule-actions`, `-schedule-rejections`; the changelogs of its tables `timetable`, `timetable-index`, `cron-registry`, `cron-due-index` | Kaspr (`kaspr/scheduler/manager.py`) |
| Each runtime's topics | `<rt>-commits`, `<rt>-app-assignor-leader`, `<rt>-app-<rt>-state-changelog` | the runtime templates (10 §10.1), Kaspr, Faust |
| Transactional ids | `<app id>-<topic group>-<partition>`, one for each partition a member processes: `midstream-control-plane-app-1-2`, `midstream-mat-d6c6880d2bdb-app-0-0` | Faust (`faust/transport/consumer.py`) |
| Source topics | The topics people materialize, which Midstream never creates, writes, reconfigures or deletes | their owners |

Three prefixed grants on `<release>-` — topics, groups, transactional ids — therefore cover all the
deployment owns, and `<release>-mat-` covers what deleting a materialization removes. The trailing
dash matters: `<release>-mat-` never matches the control plane's `<release>-materialization-*`
topics. With a `runtimeNamePrefix` of your own, grant R1–R3 and F1–F2 (§21.3) for that prefix too.

## 21.3 The grants

**Required**, without which the deployment does not start or moves no data:

| | Resource | Name and pattern | Operations | For |
|---|---|---|---|---|
| R1 | Topic | `<release>-`, prefixed | Create, Read, Write | Every app declares its topics at every start, then consumes and produces them (§21.4) |
| R2 | Group | `<release>-`, prefixed | Read | Every app's consumer group; the control plane's and the tail app's reads of the groups' offsets and members |
| R3 | TransactionalId | `<release>-`, prefixed | Write | Every app runs exactly once, with transactional producers |
| R4 | Topic | each source topic, literal, or a prefix of yours | Read | The runtime reads it; the control plane samples it and sets a runtime's offsets on it; the tail app reads it |

**Needed for a feature:**

| | Resource | Name and pattern | Operations | For | Without it |
|---|---|---|---|---|---|
| F1 | Topic | `<release>-mat-`, prefixed | Delete | Deleting a materialization: its runtime's topics | The delete fails once the runtime is gone (§21.5) |
| F2 | Group | `<release>-mat-`, prefixed | Delete | Deleting a materialization: its runtime's consumer group | The same |
| F3 | Topic | the topics people may choose from | Describe | The topic list (`GET /topics`) and topic analysis behind the console's topic picker | A topic the user may not describe is not listed, and analysis calls it missing |

Kafka counts Describe as granted wherever Read, Write or Delete is, so none of R1–R4, F1 and F2
needs a Describe of its own; F3 is for topics the user may not read. Prefer granting Read: a topic
people can pick but the user cannot read fails its materialization's discovery.

**Not needed**, and best left out:

- **Nothing on the cluster.** CreateTopics is authorized per topic (R1). Listing consumer groups
  returns the groups the user may describe. Idempotent writes are not used: the transactional
  producers are authorized by their transactional ids, and Faust's other sends are not
  idempotent. No Alter, AlterConfigs, DescribeConfigs or ClusterAction either.
- **Nothing but Read on source topics.** Midstream never writes, deletes or reconfigures them, and
  Create on them is harmful on a broker that creates topics on demand (§21.7).
- **No configuration grants.** A topic's settings — `cleanup.policy=compact` for a changelog,
  `retention.ms` for a runtime's commits topic — travel in its CreateTopics request, which R1
  covers. Nothing describes or alters a topic's configuration afterwards.
- **Nothing on Kafka's internal topics** (`__consumer_offsets`, `__transaction_state`), on
  delegation tokens or on users.

## 21.4 What each app does with Kafka

### Every app: Kaspr on Faust

- **A consumer group** named after the app id, with static membership (`group.instance.id` is the
  pod's name): coordinator lookups, joins, syncs, heartbeats, offset fetches. That is R2, and Read
  on every topic the app consumes.
- **Exactly once.** `kaspr/app.py` creates every app with `processing_guarantee="exactly_once"`.
  Each member opens one transactional producer for each partition it is assigned, standby
  partitions aside, named after the partition and its topic group; it sends inside transactions
  and commits its consumer's offsets in them (`AddOffsetsToTxn`, `TxnOffsetCommit`). That takes
  R3, Write on every topic it sends to, and Read on the group and on the topics whose offsets it
  commits. Its consumers read `read_committed`, and so do the tail app's (19 §19.7).
- **Topics declared at every start.** Kaspr declares an agent input marked `declare: true` when
  the app starts, and an output marked so at its first send; Faust declares the leader topic and
  the changelogs when it subscribes. For each, Faust sends a CreateTopics request unless its
  client's metadata lists the topic, and an app that has just started lists none
  (`faust/transport/drivers/aiokafka.py::_really_create_topic`). A broker answers
  `TOPIC_ALREADY_EXISTS` only to a user allowed to create the topic, and anyone else gets
  `TOPIC_AUTHORIZATION_FAILED`, for a topic that exists too. So R1's Create is needed at every
  start, not only the first, and creating the topics beforehand does not remove the need.
- **The leader.** Every member consumes `<app id>-assignor-leader`, one partition; the member
  holding it runs the `onLeader` tasks.
- **Tables.** Each KasprTable writes its changelog on every change and reads it to recover; a
  global table is read by every member.

### The control plane

Its agents, tasks and views consume and produce the deployment's topics (06 §6.1). It declares the
maintenance and tail request topics and the runtime control topic as it first sends to them, and
its scheduler keeps three topics and four tables of its own, all under R1. Its own Kafka clients
use the same user:

| Call | Kafka requests | Needs | Where |
|---|---|---|---|
| The topic list, topic analysis | Metadata for every topic, ListOffsets | Describe on each topic shown (R4, F3) | `GET /topics`, `POST /topics/analyze` (04 §4.9.1, §4.9.1a) |
| The topic check of a create or a change of topics | Metadata for every topic | Describe on each topic it starts reading (R4) | `POST /materializations`, `PATCH /materializations/{id}` (04 §4.5) |
| Discovery | A consumer of no group: Metadata for every topic, ListOffsets, Fetch | Read on the source topics (R4) | `start_discovery`, `discover_tables` (07 §7.2) |
| The lag refresh, every minute | Metadata, ListOffsets; OffsetFetch for each runtime's group | Describe on the source topics (R4) and on `<rt>-app` (R2) | `consumer_lag`, which autoscaling reads (07 §7.12a) |
| The drain, when a change of topics adds one | DescribeGroups; OffsetCommit for the emptied `<rt>-app`, at generation −1 | Read on the group (R2) and on each added topic (R4) | `await_runtime_drained` (07 §7.9a) |
| A delete's Kafka cleanup | Metadata, ListGroups; DeleteTopics for every topic whose name starts with the runtime's; DeleteGroups for `<rt>-app` | Delete on them (F1, F2) | `cleanup_runtime_kafka` (07 §7.10) |

### Each runtime

Its writer reads the source topics (R4) in the runtime's group. Its committer reads `<rt>-commits`,
which it declares with one partition and an hour's retention. Its settings agent declares and reads
the runtime control topic, `<release>-materialization-schema-authority-updates`. Its members send
heartbeats to `<release>-materialization-status`, and its committer sends table events to
`<release>-materialization-table-events`. That is R1–R3. A runtime has no Kafka client of its own:
records are acknowledged through Faust's commits (10 §10.7).

### The maintenance app

Declares and reads `<release>-maintenance-requests`, and sends table events to
`<release>-materialization-table-events` (R1–R3).

### The tail app

Declares and reads `<release>-tail-requests` in its group (R1–R3). Its own consumers have no group
and commit nothing: one reads the source topics of the tables it tails (R4), another
`<release>-materialization-table-events` (R1). It describes its own group for the member map (R2,
19 §19.5). `tail.enabled: false` removes no grant: the app's names are under the same prefixes.

## 21.5 What breaks without a grant

What each missing grant did on a broker that enforced the rest (§21.9):

| Missing | What happens | Where it shows |
|---|---|---|
| Create on `<release>-` topics | Every app stops as it starts, whether its topics exist or not | `TopicAuthorizationFailedError: Cannot create topic: <topic> (29): Authorization failed.`; the pods restart |
| Read or Write on `<release>-` topics | Without Read the app crashes at its next fetch: taking Read away for a second crashed the running control plane and tail app. Without Write its sends are refused | `Fetcher]: Crashed reason=TopicAuthorizationFailedError('<topic>')` |
| Write on the transactional ids | Every app crashes as soon as it is assigned partitions | `Crashed reason=TransactionalIdAuthorizationFailed('<app id>-0-0')` |
| Read on the groups | The app subscribes, then waits without end and logs nothing. It did not recover when the grant came back; a restart did | The KasprApp is never ready; the broker's authorizer log: `Denied operation = DESCRIBE … Group:LITERAL:<app id> for request = FindCoordinator` |
| Read on a source topic | Kafka does not list the topic to the user, so the create, or the change that adds the topic, is refused before anything is made (04 §4.5) | `400 invalid_request`, `reason: source_topic_missing`: `Kafka has no topic <topic>, or the deployment's Kafka user may not describe it.` |
| Read on a source topic the user may describe | Discovery fails | `failed` / `start_discovery_failed`, `TopicAuthorizationFailedError` |
| Read on a source topic, lost while its materialization runs | Its runtime is assigned no partition of the topic, and the table stops growing (14 §14.2) | `running`; the runtime logs `Topic <topic> is not authorized for this client` once; the materialization's `consumer_lag.error` reads `<topic>: topic does not exist` |
| Delete on `<release>-mat-` topics or groups | `DELETE /materializations/{id}` stops after removing the runtime from Kubernetes: its topics and group stay, its tables are not dropped and its records not purged | `failed` / `cleanup_runtime_kafka_failed`, `failed to delete topic …` or `failed to delete consumer group …`; grant F1 and F2, then delete again |
| Describe on a topic | `GET /topics` does not list it; analysis and the lag call it missing; a create or change that would read it is refused | `topic does not exist`; `source_topic_missing` |

## 21.6 Granting them

### With `kafka-acls.sh`

For any Kafka that implements its ACLs, Apache Kafka 2.0 or later (prefixed patterns, Create on a
topic). Make the user with your cluster's tooling first, then, with an administrator's client
settings in `admin.properties`:

```bash
RELEASE=midstream                  # the release name, or fullnameOverride
PRINCIPAL=User:midstream           # User:<kaspr.authentication.username>
BOOTSTRAP=kafka-bootstrap.kafka:9092
acl() { kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config admin.properties \
          --add --allow-principal "$PRINCIPAL" "$@"; }

# R1-R3: everything the deployment names
acl --resource-pattern-type prefixed --topic "$RELEASE-" --operation Create --operation Read --operation Write
acl --resource-pattern-type prefixed --group "$RELEASE-" --operation Read
acl --resource-pattern-type prefixed --transactional-id "$RELEASE-" --operation Write
# R4: each source topic, or a family of them with --resource-pattern-type prefixed --topic sales.
acl --topic sales.orders --operation Read
# F1, F2: deleting materializations
acl --resource-pattern-type prefixed --topic "$RELEASE-mat-" --operation Delete
acl --resource-pattern-type prefixed --group "$RELEASE-mat-" --operation Delete

kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config admin.properties --list --principal "$PRINCIPAL"
```

### With Strimzi

A `KafkaUser` carries its ACLs:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: midstream                  # the user name: kaspr.authentication.username
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource: {type: topic, name: midstream-, patternType: prefix}             # R1
        operations: [Create, Read, Write]
      - resource: {type: group, name: midstream-, patternType: prefix}             # R2
        operations: [Read]
      - resource: {type: transactionalId, name: midstream-, patternType: prefix}   # R3
        operations: [Write]
      - resource: {type: topic, name: sales.orders, patternType: literal}          # R4
        operations: [Read]
      - resource: {type: topic, name: midstream-mat-, patternType: prefix}         # F1
        operations: [Delete]
      - resource: {type: group, name: midstream-mat-, patternType: prefix}         # F2
        operations: [Delete]
```

The `Kafka` resource must enforce ACLs (`spec.kafka.authorization.type: simple`), or the rules are
kept and nothing checks them. The User Operator writes the user's password to a Secret named after
it, key `password`, in the KafkaUser's namespace. A pod cannot read a Secret in another namespace,
so copy the password into the Secret `kaspr.authentication.passwordSecret` names, in Midstream's
namespace (`docs/install.md` §2).

### Changing grants under a running deployment

Add a new rule before removing the one it replaces. The apps read and write without pause, and a
moment without Read on their topics crashed the control plane and the tail app on the test broker
(§21.5). In Kubernetes their pods restart; the runtimes resume from their committed offsets, so
nothing is lost (10 §10.7), but every member pays a restart.

## 21.7 Source topics

- **Grant Read before the topic is materialized.** A create, or a change that starts reading a
  topic, asks Kafka for it first, and is refused with `source_topic_missing` when the user may not
  describe the topic: grant Read and send it again. A topic the user may describe but not read
  passes that check and fails discovery instead (§21.5); nothing runs without it either way.
- **Per topic or per family.** A literal grant for each topic keeps the user to what has been
  materialized; a prefixed one (`sales.`) lets people materialize a family of topics without
  asking each time.
- **Granted late.** A runtime that lost Read on a topic, or started without it before the create
  was checked, keeps running without it once the grant comes. Static membership hands a restarted member its stored assignment back, and the
  topic was never in it (14 §14.3). On the test broker neither ten minutes nor a restart brought it
  in; stopping the runtime for longer than the session timeout (`K_BROKER_SESSION_TIMEOUT`, 120 s)
  did. Stop the materialization, wait until
  `kafka-consumer-groups.sh --describe --group <rt>-app --state` reads `Empty`, and resume it. A
  change of topics drains the group anyway (07 §7.9a), so a topic added that way needs only its
  grant before the change.
- **Never Create.** With `auto.create.topics.enable=true`, Kafka's default, a consumer that
  subscribes to a missing topic makes the broker create it, if the user may create it. On the test
  broker, a subscription like a runtime's to a missing topic created it, with the broker's default
  single partition, once Create on it was granted; without Create it stayed missing. A misspelt or
  deleted source topic would come back, empty.
- **The topic picker** lists the topics the user may describe (F3).

## 21.8 Broker settings that go with the grants

- **Transactions.** Every app's producers are transactional (§21.4), so the cluster's transaction
  log, `__transaction_state`, must fit it: on fewer than three brokers, lower
  `transaction.state.log.replication.factor` (3 by default) and `transaction.state.log.min.isr`
  (2), or no transaction starts.
- **Replication.** Kaspr creates topics with a replication factor of 3
  (`K_TOPIC_REPLICATION_FACTOR`); on a cluster of fewer brokers, set
  `kaspr.config.topicReplicationFactor` (`values.yaml`).
- **Creating topics on demand** (`auto.create.topics.enable`) is not needed: every app declares the
  topics it reads (§21.4). Where it is on, a client that merely names a topic creates it if the
  user may, which is one more reason to keep Create to the release's prefix.
- **Transactional ids are not deleted.** A deleted runtime's ids stay with the broker until
  `transactional.id.expiration.ms` (7 days by default) passes. There is nothing to clean up.

## 21.9 How this was checked

- **The code.** The calls of §21.4: `kafka/admin.py`, `kafka/metadata.py`, `schema/discovery.py`,
  `tail_app/consumer.py`, `tail_app/worker.py`, `services/workflow.py`, `services/consumer_lag.py`;
  Kaspr's app, builder, outputs and scheduler (`kaspr/app.py`, `kaspr/core/`,
  `kaspr/types/models/topicout.py`, `kaspr/scheduler/manager.py`); Faust's transactions, producer
  and topic creation (`faust/transport/consumer.py`, `faust/transport/drivers/aiokafka.py`,
  `faust/topics.py`, `faust/tables/base.py`, `faust/assignor/leader_assignor.py`); and
  kaspr-operator's names and environment (`kaspr/types/models/kasprapp_resources.py`,
  `kaspr/resources/kasprapp.py`).
- **The lab** (2026-10-28): the topics, groups and transactional ids of §21.2, as
  `kafka-topics.sh --list`, `kafka-consumer-groups.sh --list` and `kafka-transactions.sh list` show
  them for its two releases. Its brokers enforce no ACLs, so it could not check the grants.
- **A broker enforcing them** (2026-10-28): Kafka 4.1.1 in KRaft mode with the StandardAuthorizer
  and `allow.everyone.if.no.acl.found=false`, and a user holding R1–R4 (on one source topic), F1 and
  F2, and nothing else. The four kinds of app ran from the definitions kaspr-operator mounts — the
  control plane, the maintenance app, the tail app and a runtime, on Kaspr 0.11.31, twm-faust
  1.17.23 and aiokafka 0.14.0 — and so did the control plane's own calls: the topic list and
  analysis, discovery, the lag's offsets, the tail's consumers and member map, the drain's commit
  and a deleted runtime's cleanup. The broker denied nothing. Each grant was then taken away in
  turn: §21.5 and §21.7 are what happened. GCS was out of reach on purpose, so no table was
  written; the Kafka side of a commit, its table event, went through.
