"""Runtime app configuration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, replace
import logging
import os
from typing import Literal

from materializer_control_plane.runtime_app.live_settings import (
    LIVE_SETTING_ENV_VARS,
    LIVE_SETTING_FIELDS,
    LIVE_SETTING_ZERO_ALLOWED,
    current_runtime_settings,
    current_settings_position,
)

LOGGER = logging.getLogger(__name__)

MalformedRecordMode = Literal["continue_with_error", "strict"]

MIB = 1024 * 1024
GIB = 1024 * MIB

# Lineage and revision whose overrides were last refused, so a bad combination is logged once,
# not per call.
_refused_position: tuple[str, int] | None = None


def _validate_choice(name: str, value: str, choices: tuple[str, ...]) -> None:
    if value not in choices:
        allowed = ", ".join(choices)
        raise ValueError(f"{name} must be one of: {allowed}")


@dataclass(frozen=True, slots=True)
class RuntimeAppConfig:
    #: KasprTask intervals, rendered into the runtime's spec (fixed by the deployment).
    heartbeat_interval: str = "30s"
    upload_interval: str = "5s"
    commit_timer_interval: str = "5s"
    staging_root_path: str = "/tmp/materializer/staging"
    target_file_size_bytes: int = 256 * MIB
    target_row_group_size_bytes: int = 32 * MIB
    # -- batches (live) --------------------------------------------------------------------------
    max_rows_per_file: int = 250_000
    #: A batch past this age rolls once it holds enough to be worth a file.
    max_open_file_age_seconds: int = 120
    #: Below these a batch past the soft age keeps filling rather than sealing a near-empty file.
    min_rows_per_file: int = 10_000
    min_file_size_bytes: int = 8 * MIB
    #: Hard age bound, applied regardless of how little the batch holds.
    max_open_file_age_ceiling_seconds: int = 600
    upload_worker_count: int = 4
    #: Batches sealed at once, each in a child process holding its whole batch in memory.
    seal_worker_count: int = 1
    max_pending_upload_batches: int = 32
    max_pending_upload_bytes: int = 2 * GIB
    upload_initial_backoff_seconds: int = 5
    malformed_record_mode: MalformedRecordMode = "continue_with_error"
    # -- commits (live) --------------------------------------------------------------------------
    #: The committer commits pending files at most this often (sooner at the file or byte limit).
    commit_interval_seconds: int = 30
    max_files_per_commit: int = 500
    max_bytes_per_commit: int = 8 * GIB
    #: A member announces an uploaded file again after this long while no commit covers it.
    announce_retry_seconds: int = 60
    # -- schema (live) ---------------------------------------------------------------------------
    #: The committer gathers schema change requests for this long, then commits them together.
    schema_evolution_debounce_seconds: int = 2
    #: A batch holding values its schema could not take waits this long for evolution to land.
    seal_schema_wait_seconds: int = 30
    max_forks_per_field: int = 3
    max_struct_fields: int = 300
    max_nesting_depth: int = 10
    max_schema_fields: int = 2_000
    # -- compaction and retention (live) ---------------------------------------------------------
    compaction_enabled: bool = True
    compaction_interval_minutes: int = 15
    compaction_min_input_files: int = 8
    compaction_small_file_bytes: int = 64 * MIB
    compaction_target_file_size_bytes: int = 256 * MIB
    compaction_max_input_bytes: int = 1 * GIB
    compaction_hot_partition_delay_minutes: int = 60
    compaction_timeout_minutes: int = 30
    compaction_max_attempts: int = 5
    snapshot_retention_hours: int = 72
    min_snapshots_to_keep: int = 20
    #: Snapshots younger than this are all kept; older ones are thinned to one an hour.
    snapshot_keep_all_minutes: int = 60
    snapshot_expiry_interval_minutes: int = 60
    orphan_cleanup_enabled: bool = True
    orphan_min_age_hours: int = 24
    orphan_cleanup_interval_hours: int = 24
    # -- diagnostics (live) ----------------------------------------------------------------------
    protocol_logging_enabled: bool = False
    protocol_log_slow_wait_seconds: int = 30

    def __post_init__(self) -> None:
        for name in (
            "heartbeat_interval",
            "upload_interval",
            "commit_timer_interval",
            "staging_root_path",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        for item in fields(self):
            value = getattr(self, item.name)
            if isinstance(value, bool) or not isinstance(value, int):
                continue
            minimum = 0 if item.name in LIVE_SETTING_ZERO_ALLOWED else 1
            if value < minimum:
                raise ValueError(f"{item.name} must be >= {minimum}")
        if self.max_open_file_age_ceiling_seconds < self.max_open_file_age_seconds:
            raise ValueError("max_open_file_age_ceiling_seconds must be >= max_open_file_age_seconds")
        if self.compaction_small_file_bytes >= self.compaction_target_file_size_bytes:
            raise ValueError("compaction_small_file_bytes must be < compaction_target_file_size_bytes")
        if self.seal_schema_wait_seconds > self.max_open_file_age_ceiling_seconds:
            raise ValueError("seal_schema_wait_seconds must be <= max_open_file_age_ceiling_seconds")
        _validate_choice(
            "malformed_record_mode",
            self.malformed_record_mode,
            ("continue_with_error", "strict"),
        )


def get_runtime_app_config(environ: Mapping[str, str] | None = None) -> RuntimeAppConfig:
    """The runtime's configuration: its environment, with any live overrides applied.

    Live overrides come from the control plane while the runtime runs; see `LIVE_SETTING_FIELDS`.
    A combination the overrides make invalid against this environment — a seal-age ceiling below
    the soft age, say — is refused and logged once, and the environment's values apply instead, so
    a bad override can never stop a runtime from processing records.

    Passing `environ` reads that mapping instead of the process environment and applies no
    overrides: the control plane uses it to work out what a runtime's environment amounts to.
    """
    global _refused_position
    base = runtime_config_from_environment(os.environ if environ is None else environ)
    if environ is not None:
        return base
    revision, overrides = current_runtime_settings()
    if not overrides:
        return base
    try:
        return replace(base, **overrides)
    except (TypeError, ValueError) as error:
        position = current_settings_position()
        if _refused_position != position:
            _refused_position = position
            LOGGER.warning(
                "Ignoring runtime settings revision %s: %s. The runtime keeps its deployed values.",
                revision,
                error,
            )
        return base


def runtime_config_from_environment(environ: Mapping[str, str]) -> RuntimeAppConfig:
    """Build the runtime's configuration from an environment mapping alone."""
    defaults = RuntimeAppConfig()
    values: dict[str, object] = {}
    for key, kind in LIVE_SETTING_FIELDS.items():
        name = LIVE_SETTING_ENV_VARS[key]
        raw = environ.get(name)
        if raw is None:
            continue
        if kind is bool:
            values[key] = _parse_bool_env(raw, default=getattr(defaults, key))
        else:
            minimum = 0 if key in LIVE_SETTING_ZERO_ALLOWED else 1
            values[key] = _parse_int_env(raw, default=getattr(defaults, key), minimum=minimum, name=name)
    return replace(defaults, **values)


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
