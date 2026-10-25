"""Every setting an administrator can change from the console, and what each one means.

A setting starts from the environment its component is deployed with — the deployment value —
or, when the environment does not set it, from its built-in default. An administrator may override
it from the console; the override wins until it is reset. Settings that must match the deployment
itself (topic names, namespaces, resources, credentials) are not here: they stay environment-only
and are reported read-only as `DEPLOYMENT_SETTINGS`.

The labels and descriptions below are shown on the console's Settings page as they are written, so
they say what a setting controls in the reader's terms and, where it helps, when to change it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from materializer_control_plane.config import PARTITION_GRANULARITIES, is_gcs_destination_uri, normalize_destination_uri
from materializer_control_plane.runtime_app.live_settings import LIVE_SETTING_ENV_VARS
from materializer_control_plane.scale import InvalidScaleError, normalize_storage_size

#: Value kinds a catalog setting can have. The console renders a control for each.
SETTING_KINDS = ("boolean", "integer", "gcs_uri", "quantity", "enum")



class SettingValueError(ValueError):
    """A value a setting cannot take, or a combination of values that cannot work together."""

    def __init__(self, message: str, *, key: str) -> None:
        self.key = key
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class SettingDefinition:
    """One editable setting: what it is, how it is validated, and how the console presents it.

    `key` is also the field of `ControlPlaneConfig` (scope `control-plane`) or `RuntimeAppConfig`
    (scope `runtime`) that the setting configures. `section` names the Settings page it is shown
    on. `effect` says when a change takes effect, where that is not immediately. An `enum` setting
    takes one of its `options`.
    """

    key: str
    scope: str
    section: str
    label: str
    kind: str
    env_var: str
    description: str | None = None
    unit: str | None = None
    minimum: int | None = None
    maximum: int | None = None
    nullable: bool = False
    advanced: bool = False
    danger: bool = False
    effect: str | None = None
    options: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.kind not in SETTING_KINDS:
            raise ValueError(f"kind must be one of: {', '.join(SETTING_KINDS)}")
        if (self.kind == "enum") != bool(self.options):
            raise ValueError("an enum setting needs options, and only an enum setting has them")

    def describe(self) -> dict[str, Any]:
        """The definition as the settings API reports it."""
        return {
            "key": self.key,
            "scope": self.scope,
            "section": self.section,
            "label": self.label,
            "description": self.description,
            "kind": self.kind,
            "unit": self.unit,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "nullable": self.nullable,
            "advanced": self.advanced,
            "danger": self.danger,
            "effect": self.effect,
            "options": list(self.options) if self.options else None,
            "env_var": self.env_var,
        }


@dataclass(frozen=True, slots=True)
class DeploymentSetting:
    """A setting fixed by the deployment, reported read-only so administrators can see it."""

    key: str
    component: str
    label: str
    env_var: str | None = None
    description: str | None = None


def _cp(key: str, **fields: Any) -> SettingDefinition:
    return SettingDefinition(
        key=key,
        scope="control-plane",
        env_var=fields.pop("env_var", f"MATERIALIZER_CONTROL_PLANE_{key.upper()}"),
        **fields,
    )


def _rt(key: str, **fields: Any) -> SettingDefinition:
    return SettingDefinition(key=key, scope="runtime", env_var=LIVE_SETTING_ENV_VARS[key], **fields)


CONTROL_PLANE_SETTINGS: tuple[SettingDefinition, ...] = (
    _cp(
        "default_destination_uri",
        section="materializations",
        label="Destination",
        kind="gcs_uri",
        nullable=True,
        description=(
            "Where every new materialization's tables are created: a gs://bucket[/prefix]. A "
            "materialization keeps the destination it was created with. Empty, no materialization "
            "can be created until one is set."
        ),
        effect="new materializations",
    ),
    _cp(
        "default_runtime_replicas",
        section="materializations",
        label="Default replicas",
        kind="integer",
        unit="replicas",
        minimum=1,
        maximum=1024,
        description="Runtime members a new materialization starts with when its request names none.",
        effect="new materializations",
    ),
    _cp(
        "max_runtime_replicas",
        section="materializations",
        label="Maximum replicas",
        kind="integer",
        unit="replicas",
        minimum=1,
        maximum=1024,
        description=(
            "The most replicas a create or an edit may ask for. Lowering it does not scale down "
            "materializations that already run more."
        ),
        effect="new creates and edits",
    ),
    _cp(
        "runtime_autoscaling_enabled",
        section="materializations",
        label="Autoscale runtimes",
        kind="boolean",
        description=(
            "Give a runtime that is far behind its topics more members, so it catches up within the "
            "catch-up target, and return it to its own replica count once it has. Each materialization "
            "can turn it off for itself. Off, scaled runtimes go back to their own count within a minute."
        ),
        effect="the next lag measurement, within a minute",
    ),
    _cp(
        "runtime_autoscale_max_replicas",
        section="materializations",
        label="Autoscaling maximum replicas",
        kind="integer",
        unit="replicas",
        minimum=1,
        maximum=1024,
        description=(
            "The most members autoscaling gives a runtime. It never gives more than its topics' "
            "partitions keep busy, and never takes a runtime below its own replica count."
        ),
        effect="the next scaling decision",
    ),
    _cp(
        "runtime_autoscale_target_minutes",
        section="materializations",
        label="Catch-up target",
        kind="integer",
        unit="minutes",
        minimum=1,
        maximum=1440,
        description=(
            "How soon a runtime should catch up with its topics. One that would take longer at its pace "
            "gets members enough to make it, up to the maximum."
        ),
        effect="the next scaling decision",
    ),
    _cp(
        "runtime_autoscale_min_lag_records",
        section="materializations",
        label="Lag worth scaling for",
        kind="integer",
        unit="records",
        minimum=1,
        maximum=1_000_000_000,
        advanced=True,
        description=(
            "Fewer records behind than this, a runtime is caught up: it is not scaled up, and one that "
            "was goes back to its own replica count after three measurements under it."
        ),
        effect="the next scaling decision",
    ),
    _cp(
        "runtime_autoscale_cooldown_seconds",
        section="materializations",
        label="Time between scaling steps",
        kind="integer",
        unit="seconds",
        minimum=60,
        maximum=3600,
        advanced=True,
        description=(
            "How long after a change of members before autoscaling adds more: each change moves the "
            "runtime's partitions between its members, and its new pace needs time to show."
        ),
        effect="the next scaling decision",
    ),
    _cp(
        "max_materialization_topics",
        section="materializations",
        label="Topics per materialization",
        kind="integer",
        unit="topics",
        minimum=1,
        maximum=100,
        description=(
            "The most source topics one materialization may read, each into a table of its own. One "
            "runtime commits to and maintains every table of its materialization, a table at a time, "
            "so more topics make each table's commits and maintenance wait longer for their turn. "
            "Lowering it does not change materializations that read more."
        ),
        effect="new materializations",
    ),
    _cp(
        "default_runtime_storage_size",
        section="materializations",
        label="Storage per replica",
        kind="quantity",
        description=(
            "The persistent volume each runtime member of a new materialization gets, as a "
            "Kubernetes quantity such as 64Mi. It holds only the runtime's state table: the "
            "batches a member stages are on its node's disk. Fixed once a materialization is "
            "created."
        ),
        effect="new materializations",
    ),
    _cp(
        "default_partition_granularity",
        section="materializations",
        label="Default partitioning",
        kind="enum",
        options=PARTITION_GRANULARITIES,
        description=(
            "How a new materialization's tables are partitioned when its create request does not "
            "say: by the hour, day or month of each record's Kafka timestamp, or not at all. Day "
            "suits most topics; hour suits topics that write many gigabytes an hour, and month "
            "topics that write a few megabytes a day. Fixed once a materialization is created."
        ),
        effect="new materializations",
    ),
    _cp(
        "destination_cleanup_enabled",
        section="deletion",
        label="Delete object data",
        kind="boolean",
        danger=True,
        description=(
            "Deleting a materialization also drops its Iceberg table and deletes its files: data, "
            "manifests and metadata. Turn off to keep the files after a delete; the table is then "
            "unregistered, not deleted."
        ),
        effect="deletes that have not reached that step",
    ),
    _cp(
        "purge_records_on_delete",
        section="deletion",
        label="Purge records on delete",
        kind="boolean",
        description=(
            "Remove a deleted materialization's control-plane records as the delete's last step. "
            "Off, it stays listed as deleted until the retention window below has passed."
        ),
        effect="deletes that have not reached that step",
    ),
    _cp(
        "deleted_retention_hours",
        section="deletion",
        label="Deleted record retention",
        kind="integer",
        unit="hours",
        minimum=1,
        maximum=8760,
        description=(
            "How long the hourly retention sweep keeps the records of a deleted materialization "
            "that a delete did not purge. Also the safety net for deletes that did not finish."
        ),
        effect="the next hourly sweep",
    ),
    _cp(
        "deleted_purge_batch_limit",
        section="deletion",
        label="Records purged per sweep",
        kind="integer",
        unit="materializations",
        minimum=1,
        maximum=10000,
        advanced=True,
        description="Most deleted materializations one retention sweep removes.",
        effect="the next hourly sweep",
    ),
    _cp(
        "discovery_sample_records",
        section="schema",
        label="Discovery sample",
        kind="integer",
        unit="records",
        minimum=1,
        maximum=10000,
        description=(
            "Records read from the topic, from both ends of every partition, to infer a new table's "
            "first schema. A field the sample misses becomes a column when the first record with it "
            "arrives."
        ),
        effect="new materializations",
    ),
    _cp(
        "discovery_sample_seconds",
        section="schema",
        label="Discovery time limit",
        kind="integer",
        unit="seconds",
        minimum=1,
        maximum=60,
        description=(
            "Longest discovery reads the topic. The table is then created from what was read, "
            "even from nothing."
        ),
        effect="new materializations",
    ),
    _cp(
        "schema_history_limit",
        section="schema",
        label="Schema versions kept",
        kind="integer",
        unit="versions",
        minimum=1,
        maximum=10000,
        description=(
            "Schema versions the control plane keeps per materialization for the Schema tab's "
            "history, newest first. The table itself keeps every schema; lowering this drops the "
            "oldest from the history at the next schema change."
        ),
        effect="the next schema change",
    ),
    _cp(
        "catalog_api_enabled",
        section="catalog",
        label="Iceberg REST catalog",
        kind="boolean",
        description=(
            "Serve the Iceberg REST catalog that engines such as Spark, Trino, DuckDB and PyIceberg "
            "read materialized tables through. Off, it answers 503 and the console's Connect panel "
            "says so; the tables stay readable straight from object storage."
        ),
    ),
    _cp(
        "catalog_metadata_cache_seconds",
        section="catalog",
        label="Table metadata cache",
        kind="integer",
        unit="seconds",
        minimum=1,
        maximum=300,
        advanced=True,
        description=(
            "How long the REST catalog answers from a table's metadata it already read before "
            "reading the table's pointer again. An engine sees a new commit at most this much later."
        ),
    ),
    _cp(
        "consumer_lag_refresh_batch_limit",
        section="jobs",
        label="Lag measurements per pass",
        kind="integer",
        unit="materializations",
        minimum=1,
        maximum=10000,
        advanced=True,
        description="Most materializations whose consumer-group lag is measured each minute.",
        effect="the next pass",
    ),
    _cp(
        "topic_catalog_ttl_seconds",
        section="topics",
        label="Topic list cache",
        kind="integer",
        unit="seconds",
        minimum=1,
        maximum=3600,
        description=(
            "How long the control plane answers the topic list from memory before asking Kafka "
            "again."
        ),
    ),
    _cp(
        "topic_catalog_min_refresh_seconds",
        section="topics",
        label="Minimum topic list refresh",
        kind="integer",
        unit="seconds",
        minimum=1,
        maximum=3600,
        description=(
            "Least time between two topic-list loads from Kafka, forced reloads included, however "
            "many clients ask."
        ),
    ),
    _cp(
        "topic_analysis_max_topics",
        section="topics",
        label="Topics per analysis",
        kind="integer",
        unit="topics",
        minimum=1,
        maximum=1000,
        description="Most topics one analyze request may inspect.",
    ),
    _cp(
        "protocol_logging_enabled",
        section="diagnostics",
        label="Protocol logging",
        kind="boolean",
        env_var="MATERIALIZER_PROTOCOL_LOGGING_ENABLED",
        description=(
            "Log one structured line per control-plane protocol event, such as a work step "
            "completing or a table being created."
        ),
    ),
    _cp(
        "session_idle_timeout_minutes",
        section="security",
        label="Idle timeout",
        kind="integer",
        unit="minutes",
        minimum=5,
        maximum=7 * 24 * 60,
        description=(
            "How long a session lasts without being used. Each request renews it, up to the "
            "maximum session length."
        ),
        effect="new sign-ins and renewed sessions",
    ),
    _cp(
        "session_max_lifetime_hours",
        section="security",
        label="Maximum session length",
        kind="integer",
        unit="hours",
        minimum=1,
        maximum=90 * 24,
        description="How long a session can last however active it is. After it, the person signs in again.",
        effect="new sign-ins",
    ),
    _cp(
        "session_refresh_minutes",
        section="security",
        label="Session renewal interval",
        kind="integer",
        unit="minutes",
        minimum=1,
        maximum=60,
        advanced=True,
        description=(
            "How often a client renews an active session. Also how precisely a person's last "
            "activity is recorded."
        ),
        effect="new sign-ins and renewed sessions",
    ),
    _cp(
        "password_min_length",
        section="security",
        label="Minimum password length",
        kind="integer",
        unit="characters",
        minimum=8,
        maximum=128,
        description=(
            "Shortest password anyone may choose or be given. Length is what makes a password "
            "strong, so there are no rules about character classes."
        ),
        effect="new passwords",
    ),
    _cp(
        "sign_in_max_failed_attempts",
        section="security",
        label="Failed sign-ins before lockout",
        kind="integer",
        unit="attempts",
        minimum=3,
        maximum=100,
        description=(
            "Wrong passwords in a row for one email before it is locked out. An administrator can "
            "unlock it from the Users page."
        ),
        effect="the next failed sign-in",
    ),
    _cp(
        "sign_in_lockout_minutes",
        section="security",
        label="Lockout duration",
        kind="integer",
        unit="minutes",
        minimum=1,
        maximum=24 * 60,
        description="How long a locked-out email cannot sign in, even with the right password.",
        effect="the next lockout",
    ),
    _cp(
        "audit_log_retention_days",
        section="security",
        label="Audit log retention",
        kind="integer",
        unit="days",
        minimum=1,
        maximum=3650,
        description="How long sign-ins and changes to users, groups and roles are kept in the audit log.",
        effect="the next hourly cleanup",
    ),
    _cp(
        "live_tail_enabled",
        section="live-tail",
        label="Live tail",
        kind="boolean",
        description=(
            "Serve the newest records of every materialization's tables from Kafka before they are "
            "committed. Off, nothing is tailed and the console shows committed rows only."
        ),
        effect="within a minute",
    ),
    _cp(
        "live_tail_max_rows_per_table",
        section="live-tail",
        label="Records per table",
        kind="integer",
        unit="records",
        minimum=1000,
        maximum=10_000_000,
        description=(
            "Most uncommitted records the tail keeps for one table, and how far behind the topic's "
            "head a runtime may be for the tail to apply: further behind - a new materialization's "
            "backfill - the table is backlogged and the tail reads nothing until the runtime catches "
            "up; past it during a burst the tail pauses, and queries say the newest records are not "
            "shown yet."
        ),
        effect="within a minute",
    ),
    _cp(
        "live_tail_max_bytes_per_table",
        section="live-tail",
        label="Memory per table",
        kind="integer",
        unit="bytes",
        minimum=8 * 1024 * 1024,
        maximum=4 * 1024 * 1024 * 1024,
        description=(
            "Most memory the tail spends on one table. Roughly the table's uncommitted records' "
            "size; the tail app's members are sized for the sum (handbook 19)."
        ),
        effect="within a minute",
    ),
    _cp(
        "live_tail_retention_seconds",
        section="live-tail",
        label="Committed records kept",
        kind="integer",
        unit="seconds",
        minimum=10,
        maximum=3600,
        advanced=True,
        description=(
            "How long committed records stay in the tail, so a query that read the table a moment "
            "before a commit still finds them."
        ),
        effect="within a minute",
    ),
    _cp(
        "live_tail_fetch_wait_ms",
        section="live-tail",
        label="Kafka read wait",
        kind="integer",
        unit="ms",
        minimum=50,
        maximum=2000,
        advanced=True,
        description=(
            "Longest the tail waits for new records on each read of Kafka. Lower is fresher and "
            "costs more broker requests."
        ),
        effect="within a minute",
    ),
    _cp(
        "live_tail_max_records_per_second_per_table",
        section="live-tail",
        label="Read rate per table",
        kind="integer",
        unit="records/s",
        minimum=100,
        maximum=100_000,
        advanced=True,
        description=(
            "Fastest the tail reads one table, so one topic's burst cannot slow the others on its "
            "member. A table producing faster falls behind at this pace and says so."
        ),
        effect="within a minute",
    ),
    _cp(
        "live_tail_peek_max_rows",
        section="live-tail",
        label="Records per peek",
        kind="integer",
        unit="records",
        minimum=10,
        maximum=10_000,
        advanced=True,
        description="Most records one peek at a topic's newest records reads. The console asks for 200.",
        effect="within a minute",
    ),
    _cp(
        "live_tail_idle_seconds",
        section="live-tail",
        label="Tail after the last query",
        kind="integer",
        unit="seconds",
        minimum=0,
        maximum=86_400,
        description=(
            "How long a table stays tailed after the last query asked for it. Past it the tail lets "
            "go of the table until the next query, which then waits a second or two for it. 0 keeps "
            "every table on."
        ),
        effect="within a minute",
    ),
)

RUNTIME_SETTINGS: tuple[SettingDefinition, ...] = (
    _rt(
        "max_open_file_age_seconds",
        section="runtimes",
        label="Seal after",
        kind="integer",
        unit="seconds",
        minimum=10,
        maximum=86400,
        description=(
            "Age at which an open batch seals into data files, once it holds the minimum rows or "
            "size below. Lower it for fresher data; compaction folds the extra small files later."
        ),
    ),
    _rt(
        "max_open_file_age_ceiling_seconds",
        section="runtimes",
        label="Always seal after",
        kind="integer",
        unit="seconds",
        minimum=10,
        maximum=86400,
        description=(
            "Age at which a batch seals however little it holds. Bounds how stale a quiet topic's "
            "data can get, and how many staged rows a pod restart reads again."
        ),
    ),
    _rt(
        "min_rows_per_file",
        section="runtimes",
        label="Minimum rows per file",
        kind="integer",
        unit="rows",
        minimum=1,
        maximum=100_000_000,
        description="Between the two ages, a batch seals once it holds this many rows or the minimum size.",
    ),
    _rt(
        "min_file_size_bytes",
        section="runtimes",
        label="Minimum file size",
        kind="integer",
        unit="bytes",
        minimum=1,
        maximum=16 * 1024**3,
        description="Staged data, before compression, that lets a batch seal between the two ages.",
    ),
    _rt(
        "max_rows_per_file",
        section="runtimes",
        label="Maximum rows per file",
        kind="integer",
        unit="rows",
        minimum=1,
        maximum=100_000_000,
        description="A batch seals as soon as it holds this many rows, whatever its age.",
    ),
    _rt(
        "commit_interval_seconds",
        section="commits",
        label="Commit interval",
        kind="integer",
        unit="seconds",
        minimum=5,
        maximum=3600,
        description=(
            "How often a runtime commits the files its members uploaded to its table. A row becomes "
            "queryable at the first commit after its file is uploaded; a commit also happens as soon "
            "as the limits below are reached."
        ),
    ),
    _rt(
        "max_files_per_commit",
        section="commits",
        label="Files per commit",
        kind="integer",
        unit="files",
        minimum=1,
        maximum=10000,
        description="Most data files one commit adds. More waiting files are committed by the next commit at once.",
    ),
    _rt(
        "max_bytes_per_commit",
        section="commits",
        label="Bytes per commit",
        kind="integer",
        unit="bytes",
        minimum=1024**2,
        maximum=1024**4,
        advanced=True,
        description="Most data, compressed, that one commit adds.",
    ),
    _rt(
        "announce_retry_seconds",
        section="commits",
        label="Announcement retry",
        kind="integer",
        unit="seconds",
        minimum=10,
        maximum=3600,
        advanced=True,
        description=(
            "How often a member announces an uploaded file again while no commit covers it. This is "
            "how a restarted committer learns about files it missed."
        ),
    ),
    _rt(
        "schema_evolution_debounce_seconds",
        section="schema",
        label="Schema change batching",
        kind="integer",
        unit="seconds",
        minimum=1,
        maximum=60,
        description=(
            "How long a runtime collects new fields before it changes its table's schema once for "
            "all of them."
        ),
    ),
    _rt(
        "seal_schema_wait_seconds",
        section="schema",
        label="Seal wait for schema",
        kind="integer",
        unit="seconds",
        minimum=0,
        maximum=600,
        advanced=True,
        description=(
            "Longest a batch with new fields waits for the schema change that adds them before it "
            "seals anyway. Values with no column yet go to _unmapped. Zero never waits."
        ),
    ),
    _rt(
        "max_forks_per_field",
        section="schema",
        label="Type forks per field",
        kind="integer",
        unit="forks",
        minimum=1,
        maximum=10,
        description=(
            "Columns one field may have for incompatible types, such as price and price__v2, before "
            "its values are kept as JSON text instead."
        ),
    ),
    _rt(
        "max_struct_fields",
        section="schema",
        label="Fields per object",
        kind="integer",
        unit="fields",
        minimum=10,
        maximum=10000,
        description=(
            "Most fields one object may have as columns. An object with more, often a map keyed by "
            "ids, is kept as one JSON column instead."
        ),
    ),
    _rt(
        "max_nesting_depth",
        section="schema",
        label="Nesting depth",
        kind="integer",
        unit="levels",
        minimum=2,
        maximum=32,
        description="Deepest nesting kept as columns. Anything deeper is kept as JSON text.",
    ),
    _rt(
        "max_schema_fields",
        section="schema",
        label="Fields per table",
        kind="integer",
        unit="fields",
        minimum=50,
        maximum=100000,
        description="Most columns a table may have. New fields past it go to _unmapped.",
    ),
    _rt(
        "compaction_enabled",
        section="maintenance",
        label="Compaction",
        kind="boolean",
        description=(
            "Rewrite each partition's small files into fewer, larger ones, which keeps queries fast. "
            "Rows and their order of arrival do not change."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_interval_minutes",
        section="maintenance",
        label="Compaction interval",
        kind="integer",
        unit="minutes",
        minimum=1,
        maximum=1440,
        description=(
            "How often the maintenance app looks for partitions worth compacting in each table. "
            "While it keeps finding them it compacts again at once, a job at a time."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_min_input_files",
        section="maintenance",
        label="Files worth compacting",
        kind="integer",
        unit="files",
        minimum=2,
        maximum=10000,
        description="Small files a partition must have before it is compacted.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_small_file_bytes",
        section="maintenance",
        label="Small file size",
        kind="integer",
        unit="bytes",
        minimum=1024**2,
        maximum=2 * 1024**3,
        description="Files smaller than this are compacted. Must be smaller than the target size.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_target_file_size_bytes",
        section="maintenance",
        label="Target file size",
        kind="integer",
        unit="bytes",
        minimum=8 * 1024**2,
        maximum=2 * 1024**3,
        description="Size compaction aims for in each file it writes.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_max_input_bytes",
        section="maintenance",
        label="Data per compaction",
        kind="integer",
        unit="bytes",
        minimum=8 * 1024**2,
        maximum=1024**4,
        advanced=True,
        description=(
            "Most data one compaction reads. Small partitions are compacted together up to this "
            "much; a partition with more is compacted over several passes."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_hot_partition_delay_minutes",
        section="maintenance",
        label="Hot partition delay",
        kind="integer",
        unit="minutes",
        minimum=0,
        maximum=1440,
        advanced=True,
        description=(
            "The partition still being written - today, or this hour - is compacted only once no "
            "commit has added to it for this long, so it is left alone until it settles; in this "
            "month's partition, each file waits this long instead. Past partitions are compacted "
            "however recently a backfill added to them. Zero compacts the current partition while "
            "it is written."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_timeout_minutes",
        section="maintenance",
        label="Compaction time limit",
        kind="integer",
        unit="minutes",
        minimum=5,
        maximum=240,
        advanced=True,
        description="A compaction still running after this long is stopped and tried again later.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "compaction_max_attempts",
        section="maintenance",
        label="Compaction attempts",
        kind="integer",
        unit="attempts",
        minimum=1,
        maximum=20,
        advanced=True,
        description=(
            "Failed compactions in a row before a partition is left alone and the table reports "
            "it. Its data stays correct, only in more files."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "snapshot_retention_hours",
        section="maintenance",
        label="Snapshot retention",
        kind="integer",
        unit="hours",
        minimum=1,
        maximum=8760,
        description=(
            "How long older versions of a table stay readable, for queries as of a past time. Files "
            "only an expired version used are deleted later."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "min_snapshots_to_keep",
        section="maintenance",
        label="Snapshots always kept",
        kind="integer",
        unit="snapshots",
        minimum=1,
        maximum=10000,
        description="Newest table versions kept however old they are.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "snapshot_keep_all_minutes",
        section="maintenance",
        label="Keep every snapshot for",
        kind="integer",
        unit="minutes",
        minimum=1,
        maximum=10080,
        advanced=True,
        description=(
            "Every table version newer than this is kept. Older ones, within the retention, are "
            "thinned to one per hour, which keeps a busy table's metadata small."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "snapshot_expiry_interval_minutes",
        section="maintenance",
        label="Snapshot expiry interval",
        kind="integer",
        unit="minutes",
        minimum=5,
        maximum=1440,
        advanced=True,
        description="How often the maintenance app expires each table's versions past the retention.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "orphan_cleanup_enabled",
        section="maintenance",
        label="Orphan file cleanup",
        kind="boolean",
        description=(
            "Delete files in a table's location that no table version uses, such as those left by "
            "a crash between an upload and its commit."
        ),
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "orphan_min_age_hours",
        section="maintenance",
        label="Orphan minimum age",
        kind="integer",
        unit="hours",
        minimum=1,
        maximum=720,
        advanced=True,
        description="An unused file is deleted only once it is this old, so a file about to be committed is never one.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "orphan_cleanup_interval_hours",
        section="maintenance",
        label="Orphan cleanup interval",
        kind="integer",
        unit="hours",
        minimum=1,
        maximum=168,
        advanced=True,
        description="How often the maintenance app looks for unused files in each table.",
        effect="the maintenance app's next tick, within a minute",
    ),
    _rt(
        "protocol_logging_enabled",
        section="runtimes",
        label="Protocol logging",
        kind="boolean",
        description=(
            "Log one structured line per runtime protocol event, such as a file announced or a "
            "commit completed."
        ),
    ),
    _rt(
        "protocol_log_slow_wait_seconds",
        section="runtimes",
        label="Slow wait log interval",
        kind="integer",
        unit="seconds",
        minimum=1,
        maximum=3600,
        advanced=True,
        description="How often a runtime logs that a batch is still waiting for a schema change.",
    ),
)

#: Catalog settings by scope. The `console` scope is described by the console itself.
CATALOG: dict[str, tuple[SettingDefinition, ...]] = {
    "control-plane": CONTROL_PLANE_SETTINGS,
    "runtime": RUNTIME_SETTINGS,
}

DEPLOYMENT_SETTINGS: tuple[DeploymentSetting, ...] = (
    DeploymentSetting(
        "default_namespace",
        "control-plane",
        "Runtime namespace",
        "MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE",
        "Namespace new runtimes deploy into. It needs the runtimes' role and secrets, so it is set "
        "with the deployment.",
    ),
    DeploymentSetting(
        "default_runtime_name_prefix",
        "control-plane",
        "Runtime name prefix",
        "MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX",
        "Runtimes are named <prefix>-<materialization id>; deleting one removes the Kafka topics "
        "that start with that name.",
    ),
    DeploymentSetting(
        "default_runtime_template_version",
        "control-plane",
        "Runtime template version",
        "MATERIALIZER_CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION",
    ),
    DeploymentSetting("kaspr_app_name", "control-plane", "Control-plane app", "K_APP_NAME"),
    DeploymentSetting(
        "materialization_requests_topic",
        "control-plane",
        "Requests topic",
        "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC",
    ),
    DeploymentSetting(
        "materialization_work_topic",
        "control-plane",
        "Work topic",
        "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC",
    ),
    DeploymentSetting(
        "materialization_status_topic",
        "control-plane",
        "Status topic",
        "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC",
    ),
    DeploymentSetting(
        "schema_authority_updates_topic",
        "control-plane",
        "Runtime control topic",
        "MATERIALIZER_CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC",
        "Carries settings changes and maintenance requests to running runtimes. Its name predates 1.0.",
    ),
    DeploymentSetting(
        "table_events_topic",
        "control-plane",
        "Table events topic",
        "MATERIALIZER_CONTROL_PLANE_TABLE_EVENTS_TOPIC",
        "Carries every runtime's schema changes, commits and maintenance runs to the control plane.",
    ),
    DeploymentSetting(
        "storage_prefix",
        "control-plane",
        "Storage prefix",
        "MATERIALIZER_CONTROL_PLANE_STORAGE_PREFIX",
        "The prefix in the destination that every materialization's tables are stored under, which "
        "keeps deployments sharing a bucket apart. Not a namespace: datasets are.",
    ),
    DeploymentSetting(
        "catalog_base_url",
        "control-plane",
        "Catalog URL",
        "MATERIALIZER_CONTROL_PLANE_CATALOG_BASE_URL",
        "The address external engines reach the Iceberg REST catalog at, as the Connect panel shows it.",
    ),
    DeploymentSetting(
        "settings_requests_topic",
        "control-plane",
        "Settings requests topic",
        "MATERIALIZER_CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC",
    ),
    DeploymentSetting(
        "identity_requests_topic",
        "control-plane",
        "Identity requests topic",
        "MATERIALIZER_CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC",
        "Carries every change to users, groups, roles and sessions to the identity agent.",
    ),
    DeploymentSetting(
        "auth_signing_key",
        "control-plane",
        "Session signing key",
        "MATERIALIZER_AUTH_SIGNING_KEY, MATERIALIZER_AUTH_PREVIOUS_SIGNING_KEY",
        "Signs every session; set from a Secret. Without it nobody can sign in. To rotate it, move "
        "the current key to the previous slot, so sessions it signed keep working.",
    ),
    DeploymentSetting(
        "auth_service_token",
        "control-plane",
        "Console service token",
        "MATERIALIZER_AUTH_SERVICE_TOKEN",
        "What the console reads its live views with. It can read, never change; set the same value "
        "as the console's CONSOLE_SERVICE_TOKEN.",
    ),
    DeploymentSetting(
        "bootstrap_admin_email",
        "control-plane",
        "First administrator",
        "MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_EMAIL, MATERIALIZER_AUTH_BOOTSTRAP_ADMIN_PASSWORD",
        "Made an administrator, with the password from the Secret, whenever no active administrator "
        "exists. They must choose a new password when they first sign in.",
    ),
    DeploymentSetting(
        "package_revision",
        "runtime",
        "Package revision",
        None,
        "The control-plane commit every runtime is pinned to when it is deployed.",
    ),
    DeploymentSetting(
        "resources",
        "runtime",
        "Resources per replica",
        "MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC",
        "CPU and memory each runtime member requests and is limited to. A request that vertical "
        "autoscaling controls is only where a member starts.",
    ),
    DeploymentSetting(
        "vertical_autoscaling",
        "runtime",
        "Vertical autoscaling",
        "MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC",
        "Whether a VerticalPodAutoscaler sets each runtime's requests from what its members use, and "
        "between which bounds. The limits stay the ceilings unless it controls them too.",
    ),
    DeploymentSetting(
        "storage_class",
        "runtime",
        "Storage class",
        "MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC",
    ),
    DeploymentSetting(
        "heartbeat_interval",
        "runtime",
        "Heartbeat interval",
        None,
        "How often each runtime member reports its status. Built in.",
    ),
    DeploymentSetting(
        "upload_interval",
        "runtime",
        "Pipeline interval",
        None,
        "How often each runtime member seals due batches, uploads them and announces their files. Built in.",
    ),
)


def definition_for(scope: str, key: str) -> SettingDefinition | None:
    """The catalog entry for a setting, or None when the scope does not have it."""
    for definition in CATALOG.get(scope, ()):
        if definition.key == key:
            return definition
    return None


def coerce_setting_value(definition: SettingDefinition, raw: Any) -> Any:
    """Validate a value for a setting and return it in its stored form."""
    key = definition.key
    if definition.kind == "boolean":
        if not isinstance(raw, bool):
            raise SettingValueError(f"{definition.label} must be true or false", key=key)
        return raw
    if definition.kind == "integer":
        if isinstance(raw, bool) or not isinstance(raw, int):
            if isinstance(raw, float) and raw.is_integer():
                raw = int(raw)
            else:
                raise SettingValueError(f"{definition.label} must be a whole number", key=key)
        if definition.minimum is not None and raw < definition.minimum:
            raise SettingValueError(f"{definition.label} must be at least {definition.minimum}", key=key)
        if definition.maximum is not None and raw > definition.maximum:
            raise SettingValueError(f"{definition.label} must be at most {definition.maximum}", key=key)
        return raw
    if definition.kind == "gcs_uri":
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            if definition.nullable:
                return None
            raise SettingValueError(f"{definition.label} is required", key=key)
        if not isinstance(raw, str):
            raise SettingValueError(f"{definition.label} must be a gs:// URI", key=key)
        value = normalize_destination_uri(raw)
        if not is_gcs_destination_uri(value):
            raise SettingValueError(
                f"{definition.label} must be a gs:// URI such as gs://bucket/prefix",
                key=key,
            )
        return value
    if definition.kind == "enum":
        if not isinstance(raw, str) or raw not in (definition.options or ()):
            raise SettingValueError(
                f"{definition.label} must be one of: {', '.join(definition.options or ())}",
                key=key,
            )
        return raw
    if definition.kind == "quantity":
        if raw in (None, ""):
            raise SettingValueError(f"{definition.label} is required", key=key)
        try:
            return normalize_storage_size(raw)
        except InvalidScaleError as error:
            raise SettingValueError(
                f"{definition.label} must be a Kubernetes quantity such as 1Gi, 512Mi or 10G",
                key=key,
            ) from error
    raise SettingValueError(f"{definition.label} has an unsupported kind", key=key)


def validate_scope_values(scope: str, values: dict[str, Any]) -> None:
    """Check that a scope's effective values work together.

    `values` holds every catalog setting of the scope at the value it would take.
    """
    if scope == "control-plane":
        default = values.get("default_runtime_replicas")
        maximum = values.get("max_runtime_replicas")
        if isinstance(default, int) and isinstance(maximum, int) and default > maximum:
            raise SettingValueError(
                f"Default replicas ({default}) cannot exceed maximum replicas ({maximum})",
                key="default_runtime_replicas",
            )
        idle = values.get("session_idle_timeout_minutes")
        lifetime = values.get("session_max_lifetime_hours")
        refresh = values.get("session_refresh_minutes")
        if isinstance(idle, int) and isinstance(lifetime, int) and idle > lifetime * 60:
            raise SettingValueError(
                f"Idle timeout ({idle} min) cannot be longer than the maximum session length "
                f"({lifetime} h)",
                key="session_idle_timeout_minutes",
            )
        if isinstance(idle, int) and isinstance(refresh, int) and refresh >= idle:
            raise SettingValueError(
                f"Session renewal interval ({refresh} min) must be shorter than the idle timeout "
                f"({idle} min)",
                key="session_refresh_minutes",
            )
    if scope == "runtime":
        seal = values.get("max_open_file_age_seconds")
        ceiling = values.get("max_open_file_age_ceiling_seconds")
        if isinstance(seal, int) and isinstance(ceiling, int) and ceiling < seal:
            raise SettingValueError(
                f"Always seal after ({ceiling} s) cannot be shorter than seal after ({seal} s)",
                key="max_open_file_age_ceiling_seconds",
            )
        minimum_rows = values.get("min_rows_per_file")
        maximum_rows = values.get("max_rows_per_file")
        if isinstance(minimum_rows, int) and isinstance(maximum_rows, int) and minimum_rows > maximum_rows:
            raise SettingValueError(
                f"Minimum rows per file ({minimum_rows}) cannot exceed maximum rows per file "
                f"({maximum_rows})",
                key="min_rows_per_file",
            )
        small = values.get("compaction_small_file_bytes")
        target = values.get("compaction_target_file_size_bytes")
        if isinstance(small, int) and isinstance(target, int) and small >= target:
            raise SettingValueError(
                f"Small file size ({small} bytes) must be smaller than the target file size "
                f"({target} bytes)",
                key="compaction_small_file_bytes",
            )
        wait = values.get("seal_schema_wait_seconds")
        if isinstance(wait, int) and isinstance(ceiling, int) and wait > ceiling:
            raise SettingValueError(
                f"Seal wait for schema ({wait} s) cannot be longer than always seal after ({ceiling} s)",
                key="seal_schema_wait_seconds",
            )
