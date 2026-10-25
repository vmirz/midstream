"""Control-plane configuration and shared defaults."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import os
import re
from collections.abc import Mapping
from typing import Any

import yaml

ALLOWED_LIST_LIMITS = ("10", "30", "100", "all")
DEFAULT_LIST_LIMIT = "30"
DEFAULT_NAMESPACE = "kafka-sql"
DEFAULT_RUNTIME_NAME_PREFIX = "kaspr"
DEFAULT_RUNTIME_TEMPLATE_VERSION = "v1"
DEFAULT_RUNTIME_APP_SPEC: dict[str, Any] = {}
CONTROL_PLANE_TABLE_NAME = "state"
MATERIALIZATION_REQUESTS_TOPIC = "materialization-requests"
MATERIALIZATION_WORK_TOPIC = "materialization-work"
MATERIALIZATION_STATUS_TOPIC = "materialization-status"
# Carries runtime settings (and maintenance requests) to every runtime. The name predates 1.0, when
# it also carried schema decisions; it is kept so no overlay has to rename a topic.
SCHEMA_AUTHORITY_UPDATES_TOPIC = "materialization-schema-authority-updates"
TABLE_EVENTS_TOPIC = "materialization-table-events"
SETTINGS_REQUESTS_TOPIC = "materializer-settings-requests"
IDENTITY_REQUESTS_TOPIC = "materializer-identity-requests"
PROTOCOL_LOGGING_ENABLED = False
PROTOCOL_LOG_SLOW_WAIT_SECONDS = 30
SCHEMA_HISTORY_LIMIT = 50
# Iceberg: the prefix in the destination every materialization's tables are stored under (it keeps
# deployments sharing a bucket apart; datasets, not this, are the tables' namespaces), and the
# tables' partitioning.
STORAGE_PREFIX = "materializer"
DEFAULT_PARTITION_GRANULARITY = "day"
PARTITION_GRANULARITIES = ("hour", "day", "month", "none")
# Discovery samples up to this many records, from both ends of every partition, for schema 0.
DISCOVERY_SAMPLE_RECORDS = 200
DISCOVERY_SAMPLE_SECONDS = 5
# The Iceberg REST catalog facade the control plane serves at /catalog/v1.
CATALOG_API_ENABLED = True
CATALOG_METADATA_CACHE_SECONDS = 5
# The public base URL of that facade, for the snippets the console shows; unset hides them.
CATALOG_BASE_URL: str | None = None
DELETED_RETENTION_HOURS = 168
# Deleting a materialization removes everything it owns, including its control-plane records.
# Disable to keep a tombstone until the retention sweep collects it instead.
PURGE_RECORDS_ON_DELETE = True
DELETED_PURGE_BATCH_LIMIT = 50
# The persistent volume of every runtime member: it holds only the runtime's state table. The control
# plane's own volume is its KasprApp's, set in its manifest.
DEFAULT_RUNTIME_STORAGE_SIZE = "64Mi"
DEFAULT_RUNTIME_REPLICAS = 1
MAX_RUNTIME_REPLICAS = 64
# Runtime autoscaling: a runtime far behind its topics gets more members, up to
# RUNTIME_AUTOSCALE_MAX_REPLICAS, so it catches up within the target; it goes back to its own replica
# count once it has. Each materialization can turn it off for itself (services/autoscaling.py).
RUNTIME_AUTOSCALING_ENABLED = True
RUNTIME_AUTOSCALE_MAX_REPLICAS = 16
RUNTIME_AUTOSCALE_TARGET_MINUTES = 30
RUNTIME_AUTOSCALE_MIN_LAG_RECORDS = 100_000
RUNTIME_AUTOSCALE_COOLDOWN_SECONDS = 300
# Most source topics one materialization may read. Each is a table its one committer commits to and
# maintains, one at a time, so the bound keeps a commit cycle and the maintenance rotation short.
MAX_MATERIALIZATION_TOPICS = 20
# Where every new materialization's tables are created. Unset, no materialization can be created.
DEFAULT_DESTINATION_URI: str | None = None
DESTINATION_CLEANUP_ENABLED = True
CONSUMER_LAG_REFRESH_BATCH_LIMIT = 100
TOPIC_ANALYSIS_MAX_TOPICS = 20
# The topic catalog answers from memory for this long before asking Kafka again; a forced refresh
# still waits this many seconds after the previous load, however many clients ask for one.
TOPIC_CATALOG_TTL_SECONDS = 15
TOPIC_CATALOG_MIN_REFRESH_SECONDS = 5
# Sign-in: a session ends after this long without a request, and after the maximum however
# active it is. Clients renew an active session at the refresh interval, which is also how often
# a person's last activity is recorded.
SESSION_IDLE_TIMEOUT_MINUTES = 8 * 60
SESSION_MAX_LIFETIME_HOURS = 7 * 24
SESSION_REFRESH_MINUTES = 10
PASSWORD_MIN_LENGTH = 12
# Failed sign-ins for one email, in a row, that lock it out for the lockout duration.
SIGN_IN_MAX_FAILED_ATTEMPTS = 10
SIGN_IN_LOCKOUT_MINUTES = 15
AUDIT_LOG_RETENTION_DAYS = 90
# The live tail (handbook 19): the newest records of a materialization's tables, served from Kafka
# by the tail app before they are committed. Every materialization has one; `LIVE_TAIL_ENABLED` is
# the deployment's switch, for all of them. The rest travel with the tail app's
# ticks: what one table's tail may hold, how long committed records stay in it, how long a read of
# Kafka waits, how fast one table is read, the most records one peek at a topic's head reads, and how
# long a table stays tailed after the last query asked for it.
LIVE_TAIL_ENABLED = True
LIVE_TAIL_MAX_ROWS_PER_TABLE = 250_000
LIVE_TAIL_MAX_BYTES_PER_TABLE = 256 * 1024 * 1024
LIVE_TAIL_RETENTION_SECONDS = 120
LIVE_TAIL_FETCH_WAIT_MS = 200
LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE = 5_000
LIVE_TAIL_PEEK_MAX_ROWS = 1_000
LIVE_TAIL_IDLE_SECONDS = 600


#: A destination a table can live under: a GCS bucket, and optionally a prefix in it.
GCS_DESTINATION_PATTERN = re.compile(r"^gs://[a-z0-9][a-z0-9._-]*[a-z0-9](/\S*)?$")
GCS_DESTINATION_MAX_LENGTH = 1024


def is_gcs_destination_uri(value: str | None) -> bool:
    """Whether `value` (normalized) is a `gs://bucket[/prefix]` a table can be created under."""
    return value is not None and len(value) <= GCS_DESTINATION_MAX_LENGTH and GCS_DESTINATION_PATTERN.match(value) is not None


def normalize_destination_uri(value: str | None) -> str | None:
    """Trim a destination prefix, dropping trailing slashes so object paths join cleanly.

    Blank is no destination at all, which is how an unset default reads from the environment.
    """
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed.rstrip("/") or trimmed


def normalize_list_limit(limit: str | None) -> str:
    """Validate and normalize the list page-size value used by web views."""
    if limit in (None, ""):
        return DEFAULT_LIST_LIMIT
    if limit not in ALLOWED_LIST_LIMITS:
        allowed = ", ".join(ALLOWED_LIST_LIMITS)
        raise ValueError(f"limit must be one of: {allowed}")
    return limit


@dataclass(frozen=True, slots=True)
class ControlPlaneConfig:
    """Immutable runtime configuration loaded from environment variables."""

    default_namespace: str = DEFAULT_NAMESPACE
    default_runtime_name_prefix: str = DEFAULT_RUNTIME_NAME_PREFIX
    default_runtime_template_version: str = DEFAULT_RUNTIME_TEMPLATE_VERSION
    runtime_app_spec: dict[str, Any] = field(default_factory=dict)
    default_list_limit: str = DEFAULT_LIST_LIMIT
    control_plane_table_name: str = CONTROL_PLANE_TABLE_NAME
    materialization_requests_topic: str = MATERIALIZATION_REQUESTS_TOPIC
    materialization_work_topic: str = MATERIALIZATION_WORK_TOPIC
    materialization_status_topic: str = MATERIALIZATION_STATUS_TOPIC
    schema_authority_updates_topic: str = SCHEMA_AUTHORITY_UPDATES_TOPIC
    table_events_topic: str = TABLE_EVENTS_TOPIC
    settings_requests_topic: str = SETTINGS_REQUESTS_TOPIC
    protocol_logging_enabled: bool = PROTOCOL_LOGGING_ENABLED
    protocol_log_slow_wait_seconds: int = PROTOCOL_LOG_SLOW_WAIT_SECONDS
    schema_history_limit: int = SCHEMA_HISTORY_LIMIT
    storage_prefix: str = STORAGE_PREFIX
    default_partition_granularity: str = DEFAULT_PARTITION_GRANULARITY
    discovery_sample_records: int = DISCOVERY_SAMPLE_RECORDS
    discovery_sample_seconds: int = DISCOVERY_SAMPLE_SECONDS
    catalog_api_enabled: bool = CATALOG_API_ENABLED
    catalog_metadata_cache_seconds: int = CATALOG_METADATA_CACHE_SECONDS
    catalog_base_url: str | None = CATALOG_BASE_URL
    purge_records_on_delete: bool = PURGE_RECORDS_ON_DELETE
    deleted_retention_hours: int = DELETED_RETENTION_HOURS
    deleted_purge_batch_limit: int = DELETED_PURGE_BATCH_LIMIT
    default_runtime_storage_size: str = DEFAULT_RUNTIME_STORAGE_SIZE
    default_runtime_replicas: int = DEFAULT_RUNTIME_REPLICAS
    max_runtime_replicas: int = MAX_RUNTIME_REPLICAS
    runtime_autoscaling_enabled: bool = RUNTIME_AUTOSCALING_ENABLED
    runtime_autoscale_max_replicas: int = RUNTIME_AUTOSCALE_MAX_REPLICAS
    runtime_autoscale_target_minutes: int = RUNTIME_AUTOSCALE_TARGET_MINUTES
    runtime_autoscale_min_lag_records: int = RUNTIME_AUTOSCALE_MIN_LAG_RECORDS
    runtime_autoscale_cooldown_seconds: int = RUNTIME_AUTOSCALE_COOLDOWN_SECONDS
    max_materialization_topics: int = MAX_MATERIALIZATION_TOPICS
    default_destination_uri: str | None = DEFAULT_DESTINATION_URI
    destination_cleanup_enabled: bool = DESTINATION_CLEANUP_ENABLED
    consumer_lag_refresh_batch_limit: int = CONSUMER_LAG_REFRESH_BATCH_LIMIT
    topic_analysis_max_topics: int = TOPIC_ANALYSIS_MAX_TOPICS
    topic_catalog_ttl_seconds: int = TOPIC_CATALOG_TTL_SECONDS
    topic_catalog_min_refresh_seconds: int = TOPIC_CATALOG_MIN_REFRESH_SECONDS
    #: This control plane's own Kaspr app name; its internal topics are prefixed with it.
    kaspr_app_name: str | None = None
    identity_requests_topic: str = IDENTITY_REQUESTS_TOPIC
    session_idle_timeout_minutes: int = SESSION_IDLE_TIMEOUT_MINUTES
    session_max_lifetime_hours: int = SESSION_MAX_LIFETIME_HOURS
    session_refresh_minutes: int = SESSION_REFRESH_MINUTES
    password_min_length: int = PASSWORD_MIN_LENGTH
    sign_in_max_failed_attempts: int = SIGN_IN_MAX_FAILED_ATTEMPTS
    sign_in_lockout_minutes: int = SIGN_IN_LOCKOUT_MINUTES
    audit_log_retention_days: int = AUDIT_LOG_RETENTION_DAYS
    live_tail_enabled: bool = LIVE_TAIL_ENABLED
    live_tail_max_rows_per_table: int = LIVE_TAIL_MAX_ROWS_PER_TABLE
    live_tail_max_bytes_per_table: int = LIVE_TAIL_MAX_BYTES_PER_TABLE
    live_tail_retention_seconds: int = LIVE_TAIL_RETENTION_SECONDS
    live_tail_fetch_wait_ms: int = LIVE_TAIL_FETCH_WAIT_MS
    live_tail_max_records_per_second_per_table: int = LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE
    live_tail_peek_max_rows: int = LIVE_TAIL_PEEK_MAX_ROWS
    live_tail_idle_seconds: int = LIVE_TAIL_IDLE_SECONDS

    def __post_init__(self) -> None:
        if self.default_partition_granularity not in PARTITION_GRANULARITIES:
            raise ValueError(
                "default_partition_granularity must be one of: " + ", ".join(PARTITION_GRANULARITIES)
            )

    def runtime_name_for_materialization(self, materialization_id: str) -> str:
        """The name of a materialization's dedicated runtime: always generated, never chosen."""
        return f"{self.default_runtime_name_prefix}-{materialization_id}"


@lru_cache(maxsize=1)
def get_config() -> ControlPlaneConfig:
    """Load and cache the control-plane configuration from the environment."""
    return ControlPlaneConfig(
        control_plane_table_name=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_TABLE_NAME",
            CONTROL_PLANE_TABLE_NAME,
        ),
        default_namespace=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE",
            DEFAULT_NAMESPACE,
        ),
        default_runtime_name_prefix=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_RUNTIME_NAME_PREFIX",
            DEFAULT_RUNTIME_NAME_PREFIX,
        ),
        default_runtime_template_version=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_RUNTIME_TEMPLATE_VERSION",
            DEFAULT_RUNTIME_TEMPLATE_VERSION,
        ),
        runtime_app_spec=_load_runtime_app_spec(
            os.getenv("MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC")
        ),
        default_list_limit=normalize_list_limit(
            os.getenv(
                "MATERIALIZER_CONTROL_PLANE_DEFAULT_LIST_LIMIT",
                DEFAULT_LIST_LIMIT,
            )
        ),
        materialization_requests_topic=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_REQUESTS_TOPIC",
            MATERIALIZATION_REQUESTS_TOPIC,
        ),
        materialization_work_topic=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_WORK_TOPIC",
            MATERIALIZATION_WORK_TOPIC,
        ),
        materialization_status_topic=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_MATERIALIZATION_STATUS_TOPIC",
            MATERIALIZATION_STATUS_TOPIC,
        ),
        schema_authority_updates_topic=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_SCHEMA_AUTHORITY_UPDATES_TOPIC",
            SCHEMA_AUTHORITY_UPDATES_TOPIC,
        ),
        table_events_topic=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_TABLE_EVENTS_TOPIC",
            TABLE_EVENTS_TOPIC,
        ),
        settings_requests_topic=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_SETTINGS_REQUESTS_TOPIC",
            SETTINGS_REQUESTS_TOPIC,
        ),
        protocol_logging_enabled=_parse_bool_env(
            os.getenv("MATERIALIZER_PROTOCOL_LOGGING_ENABLED"),
            default=PROTOCOL_LOGGING_ENABLED,
        ),
        protocol_log_slow_wait_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS"),
            default=PROTOCOL_LOG_SLOW_WAIT_SECONDS,
            minimum=1,
            name="MATERIALIZER_PROTOCOL_LOG_SLOW_WAIT_SECONDS",
        ),
        schema_history_limit=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_SCHEMA_HISTORY_LIMIT"),
            default=SCHEMA_HISTORY_LIMIT,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_SCHEMA_HISTORY_LIMIT",
        ),
        storage_prefix=os.getenv("MATERIALIZER_CONTROL_PLANE_STORAGE_PREFIX", STORAGE_PREFIX).strip().strip("/")
        or STORAGE_PREFIX,
        default_partition_granularity=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_DEFAULT_PARTITION_GRANULARITY",
            DEFAULT_PARTITION_GRANULARITY,
        ).strip(),
        discovery_sample_records=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_DISCOVERY_SAMPLE_RECORDS"),
            default=DISCOVERY_SAMPLE_RECORDS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_DISCOVERY_SAMPLE_RECORDS",
        ),
        discovery_sample_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_DISCOVERY_SAMPLE_SECONDS"),
            default=DISCOVERY_SAMPLE_SECONDS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_DISCOVERY_SAMPLE_SECONDS",
        ),
        catalog_api_enabled=_parse_bool_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_CATALOG_API_ENABLED"),
            default=CATALOG_API_ENABLED,
        ),
        catalog_metadata_cache_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_CATALOG_METADATA_CACHE_SECONDS"),
            default=CATALOG_METADATA_CACHE_SECONDS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_CATALOG_METADATA_CACHE_SECONDS",
        ),
        catalog_base_url=(os.getenv("MATERIALIZER_CONTROL_PLANE_CATALOG_BASE_URL") or "").strip().rstrip("/") or None,
        purge_records_on_delete=_parse_bool_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_PURGE_RECORDS_ON_DELETE"),
            default=PURGE_RECORDS_ON_DELETE,
        ),
        deleted_retention_hours=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_DELETED_RETENTION_HOURS"),
            default=DELETED_RETENTION_HOURS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_DELETED_RETENTION_HOURS",
        ),
        deleted_purge_batch_limit=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_DELETED_PURGE_BATCH_LIMIT"),
            default=DELETED_PURGE_BATCH_LIMIT,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_DELETED_PURGE_BATCH_LIMIT",
        ),
        default_runtime_storage_size=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_DEFAULT_RUNTIME_STORAGE_SIZE",
            DEFAULT_RUNTIME_STORAGE_SIZE,
        ),
        default_runtime_replicas=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_DEFAULT_RUNTIME_REPLICAS"),
            default=DEFAULT_RUNTIME_REPLICAS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_DEFAULT_RUNTIME_REPLICAS",
        ),
        max_runtime_replicas=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_MAX_RUNTIME_REPLICAS"),
            default=MAX_RUNTIME_REPLICAS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_MAX_RUNTIME_REPLICAS",
        ),
        runtime_autoscaling_enabled=_parse_bool_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALING_ENABLED"),
            default=RUNTIME_AUTOSCALING_ENABLED,
        ),
        runtime_autoscale_max_replicas=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_MAX_REPLICAS"),
            default=RUNTIME_AUTOSCALE_MAX_REPLICAS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_MAX_REPLICAS",
        ),
        runtime_autoscale_target_minutes=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_TARGET_MINUTES"),
            default=RUNTIME_AUTOSCALE_TARGET_MINUTES,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_TARGET_MINUTES",
        ),
        runtime_autoscale_min_lag_records=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_MIN_LAG_RECORDS"),
            default=RUNTIME_AUTOSCALE_MIN_LAG_RECORDS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_MIN_LAG_RECORDS",
        ),
        runtime_autoscale_cooldown_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_COOLDOWN_SECONDS"),
            default=RUNTIME_AUTOSCALE_COOLDOWN_SECONDS,
            minimum=60,
            name="MATERIALIZER_CONTROL_PLANE_RUNTIME_AUTOSCALE_COOLDOWN_SECONDS",
        ),
        max_materialization_topics=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_MAX_MATERIALIZATION_TOPICS"),
            default=MAX_MATERIALIZATION_TOPICS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_MAX_MATERIALIZATION_TOPICS",
        ),
        default_destination_uri=normalize_destination_uri(
            os.getenv("MATERIALIZER_CONTROL_PLANE_DEFAULT_DESTINATION_URI")
        ),
        destination_cleanup_enabled=_parse_bool_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_DESTINATION_CLEANUP_ENABLED"),
            default=DESTINATION_CLEANUP_ENABLED,
        ),
        consumer_lag_refresh_batch_limit=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_CONSUMER_LAG_REFRESH_BATCH_LIMIT"),
            default=CONSUMER_LAG_REFRESH_BATCH_LIMIT,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_CONSUMER_LAG_REFRESH_BATCH_LIMIT",
        ),
        topic_analysis_max_topics=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_TOPIC_ANALYSIS_MAX_TOPICS"),
            default=TOPIC_ANALYSIS_MAX_TOPICS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_TOPIC_ANALYSIS_MAX_TOPICS",
        ),
        topic_catalog_ttl_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_TOPIC_CATALOG_TTL_SECONDS"),
            default=TOPIC_CATALOG_TTL_SECONDS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_TOPIC_CATALOG_TTL_SECONDS",
        ),
        topic_catalog_min_refresh_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_TOPIC_CATALOG_MIN_REFRESH_SECONDS"),
            default=TOPIC_CATALOG_MIN_REFRESH_SECONDS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_TOPIC_CATALOG_MIN_REFRESH_SECONDS",
        ),
        kaspr_app_name=os.getenv("K_APP_NAME") or None,
        identity_requests_topic=os.getenv(
            "MATERIALIZER_CONTROL_PLANE_IDENTITY_REQUESTS_TOPIC",
            IDENTITY_REQUESTS_TOPIC,
        ),
        session_idle_timeout_minutes=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_SESSION_IDLE_TIMEOUT_MINUTES"),
            default=SESSION_IDLE_TIMEOUT_MINUTES,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_SESSION_IDLE_TIMEOUT_MINUTES",
        ),
        session_max_lifetime_hours=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_SESSION_MAX_LIFETIME_HOURS"),
            default=SESSION_MAX_LIFETIME_HOURS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_SESSION_MAX_LIFETIME_HOURS",
        ),
        session_refresh_minutes=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_SESSION_REFRESH_MINUTES"),
            default=SESSION_REFRESH_MINUTES,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_SESSION_REFRESH_MINUTES",
        ),
        password_min_length=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_PASSWORD_MIN_LENGTH"),
            default=PASSWORD_MIN_LENGTH,
            minimum=8,
            name="MATERIALIZER_CONTROL_PLANE_PASSWORD_MIN_LENGTH",
        ),
        sign_in_max_failed_attempts=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_SIGN_IN_MAX_FAILED_ATTEMPTS"),
            default=SIGN_IN_MAX_FAILED_ATTEMPTS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_SIGN_IN_MAX_FAILED_ATTEMPTS",
        ),
        sign_in_lockout_minutes=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_SIGN_IN_LOCKOUT_MINUTES"),
            default=SIGN_IN_LOCKOUT_MINUTES,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_SIGN_IN_LOCKOUT_MINUTES",
        ),
        audit_log_retention_days=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_AUDIT_LOG_RETENTION_DAYS"),
            default=AUDIT_LOG_RETENTION_DAYS,
            minimum=1,
            name="MATERIALIZER_CONTROL_PLANE_AUDIT_LOG_RETENTION_DAYS",
        ),
        live_tail_enabled=_parse_bool_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_ENABLED"),
            default=LIVE_TAIL_ENABLED,
        ),
        live_tail_max_rows_per_table=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_ROWS_PER_TABLE"),
            default=LIVE_TAIL_MAX_ROWS_PER_TABLE,
            minimum=1000,
            name="MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_ROWS_PER_TABLE",
        ),
        live_tail_max_bytes_per_table=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_BYTES_PER_TABLE"),
            default=LIVE_TAIL_MAX_BYTES_PER_TABLE,
            minimum=8 * 1024 * 1024,
            name="MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_BYTES_PER_TABLE",
        ),
        live_tail_retention_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_RETENTION_SECONDS"),
            default=LIVE_TAIL_RETENTION_SECONDS,
            minimum=10,
            name="MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_RETENTION_SECONDS",
        ),
        live_tail_fetch_wait_ms=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_FETCH_WAIT_MS"),
            default=LIVE_TAIL_FETCH_WAIT_MS,
            minimum=50,
            name="MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_FETCH_WAIT_MS",
        ),
        live_tail_max_records_per_second_per_table=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE"),
            default=LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE,
            minimum=100,
            name="MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_MAX_RECORDS_PER_SECOND_PER_TABLE",
        ),
        live_tail_peek_max_rows=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_PEEK_MAX_ROWS"),
            default=LIVE_TAIL_PEEK_MAX_ROWS,
            minimum=10,
            name="MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_PEEK_MAX_ROWS",
        ),
        live_tail_idle_seconds=_parse_int_env(
            os.getenv("MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_IDLE_SECONDS"),
            default=LIVE_TAIL_IDLE_SECONDS,
            minimum=0,
            name="MATERIALIZER_CONTROL_PLANE_LIVE_TAIL_IDLE_SECONDS",
        ),
    )


def deployment_environment() -> Mapping[str, str]:
    """The process environment, to report which settings the deployment sets explicitly.

    `get_config()` folds the environment and the built-in defaults together; the settings page
    also needs to say which of the two a value came from.
    """
    return os.environ


def _load_runtime_app_spec(raw_value: str | None) -> dict[str, Any]:
    if raw_value in (None, ""):
        return dict(DEFAULT_RUNTIME_APP_SPEC)
    parsed = yaml.safe_load(raw_value)
    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise ValueError(
            "MATERIALIZER_CONTROL_PLANE_RUNTIME_APP_SPEC must parse to a mapping"
        )
    return parsed


def _parse_bool_env(raw_value: str | None, *, default: bool) -> bool:
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        "boolean environment values must be one of: "
        "1, 0, true, false, yes, no, on, off"
    )


def _parse_int_env(
    raw_value: str | None,
    *,
    default: int,
    minimum: int,
    name: str,
) -> int:
    if raw_value is None:
        return default
    try:
        parsed = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed