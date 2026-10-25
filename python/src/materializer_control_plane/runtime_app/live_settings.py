"""Runtime settings the control plane changes while a runtime is running.

A change arrives on the schema-authority-updates topic, which a single member of each runtime
consumes, and is written into the runtime's globally replicated state table, which every member
reads. The member that received it installs it at once; the others pick it up from the table on
their next record or heartbeat, at most `REFRESH_INTERVAL_SECONDS` apart. A restarted member reads
it back from the table the same way, so a change outlives restarts without a deploy.

Every delivery carries a revision, and from 4.1 the lineage of the settings it belongs to: one
history of the control plane's runtime settings, begun when the control plane first starts on a
`settings` table without one (`services/settings.py`). A member keeps the newest by lineage, then by
revision (`settings_position`), so a control plane installed afresh over a delivery topic that kept
an earlier one's deliveries is followed even though its revisions start again at 0.

The installed overrides are process-local on purpose: `get_runtime_app_config()` runs on the hot
path and must not read the table itself. This module imports nothing from the package, so the
runtime config module can depend on it without a cycle.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import threading
import time
from typing import Any

#: Key of the stored overrides in the runtime's state table.
RUNTIME_SETTINGS_KEY = "runtime-settings"

#: Longest lineage a delivery may carry; the control plane's are 25 characters.
MAX_LINEAGE_LENGTH = 64

#: Least time between two reads of the stored overrides on one member.
REFRESH_INTERVAL_SECONDS = 5.0

#: Runtime settings an administrator may change while runtimes run: the type each must be, and the
#: least value an integer may take.
#:
#: Every one is read through `get_runtime_app_config()` at its point of use, which is what makes a
#: change apply to the next batch, commit, job or log line instead of at the next deploy. The
#: control plane's settings catalog must describe exactly these fields; a test holds the two together.
LIVE_SETTING_FIELDS: dict[str, type] = {
    # Batches
    "max_rows_per_file": int,
    "max_open_file_age_seconds": int,
    "min_rows_per_file": int,
    "min_file_size_bytes": int,
    "max_open_file_age_ceiling_seconds": int,
    # Commits
    "commit_interval_seconds": int,
    "max_files_per_commit": int,
    "max_bytes_per_commit": int,
    "announce_retry_seconds": int,
    # Schema
    "schema_evolution_debounce_seconds": int,
    "seal_schema_wait_seconds": int,
    "max_forks_per_field": int,
    "max_struct_fields": int,
    "max_nesting_depth": int,
    "max_schema_fields": int,
    # Compaction and retention
    "compaction_enabled": bool,
    "compaction_interval_minutes": int,
    "compaction_min_input_files": int,
    "compaction_small_file_bytes": int,
    "compaction_target_file_size_bytes": int,
    "compaction_max_input_bytes": int,
    "compaction_hot_partition_delay_minutes": int,
    "compaction_timeout_minutes": int,
    "compaction_max_attempts": int,
    "snapshot_retention_hours": int,
    "min_snapshots_to_keep": int,
    "snapshot_keep_all_minutes": int,
    "snapshot_expiry_interval_minutes": int,
    "orphan_cleanup_enabled": bool,
    "orphan_min_age_hours": int,
    "orphan_cleanup_interval_hours": int,
    # Diagnostics
    "protocol_logging_enabled": bool,
    "protocol_log_slow_wait_seconds": int,
}

#: Integers that may be zero (every other one is at least 1).
LIVE_SETTING_ZERO_ALLOWED = frozenset({"seal_schema_wait_seconds", "compaction_hot_partition_delay_minutes"})

#: The environment variable each live setting is read from when no override applies.
LIVE_SETTING_ENV_VARS: dict[str, str] = {
    key: (
        f"MATERIALIZER_{key.upper()}"
        if key.startswith("protocol_")
        else f"MATERIALIZER_RUNTIME_{key.upper()}"
    )
    for key in LIVE_SETTING_FIELDS
}

_lock = threading.Lock()
_lineage = ""
_revision = 0
_values: dict[str, Any] = {}
_last_refresh: float | None = None


def settings_position(lineage: str | None, revision: int) -> tuple[str, int]:
    """Where a set of overrides stands among every other: by lineage, then by revision.

    A lineage begins with the time it began, in a fixed width, so a later one sorts after an
    earlier one whatever revisions that one reached. Overrides without a lineage come from a
    control plane that predates lineages, and sort before every lineage.
    """
    return (lineage or "", revision)


def current_runtime_settings() -> tuple[int, dict[str, Any]]:
    """The installed revision and its overrides; revision 0 means none were ever delivered."""
    with _lock:
        return _revision, dict(_values)


def current_settings_position() -> tuple[str, int]:
    """The installed lineage and revision; ("", 0) when nothing was ever delivered."""
    with _lock:
        return _lineage, _revision


def install_runtime_settings(revision: int, values: Mapping[str, Any] | None, lineage: str | None = None) -> bool:
    """Install `values` as this process's overrides when they are newer than the installed ones.

    Newer by `settings_position`: a delivery that arrives after a newer one, or twice, changes
    nothing, and one from a later lineage replaces whatever an earlier lineage reached.
    """
    global _lineage, _revision, _values
    sanitized = sanitize_runtime_settings(values)
    with _lock:
        if settings_position(lineage, revision) <= (_lineage, _revision):
            return False
        _lineage = lineage or ""
        _revision = revision
        _values = sanitized
        return True


def refresh_runtime_settings(
    table: Any,
    *,
    clock: Callable[[], float] = time.monotonic,
    force: bool = False,
) -> int:
    """Install whatever overrides the runtime's table holds, at most every few seconds.

    Returns the installed revision. The runtime reports it on its heartbeat with its lineage
    (`current_settings_position`), so the control plane can tell which runtimes run the current
    settings.
    """
    global _last_refresh
    now = clock()
    with _lock:
        if not force and _last_refresh is not None and now - _last_refresh < REFRESH_INTERVAL_SECONDS:
            return _revision
        _last_refresh = now
    stored = read_stored_runtime_settings(table)
    if stored is not None:
        install_runtime_settings(*stored)
    with _lock:
        return _revision


def read_stored_runtime_settings(table: Any) -> tuple[int, dict[str, Any], str | None] | None:
    """The revision, overrides and lineage stored in the runtime's table, or None when none are.

    The lineage is None when the stored overrides have none: stored before 4.1, or delivered by a
    control plane that predates lineages.
    """
    if table is None:
        return None
    raw = table.get(RUNTIME_SETTINGS_KEY)
    if not isinstance(raw, dict):
        return None
    revision = raw.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        return None
    lineage = raw.get("lineage")
    if not lineage or not is_valid_lineage(lineage):
        lineage = None
    return revision, sanitize_runtime_settings(raw.get("values")), lineage


def is_valid_lineage(value: Any) -> bool:
    """Whether a delivered lineage can be compared: none at all, or a string of bounded length."""
    return value is None or (isinstance(value, str) and len(value) <= MAX_LINEAGE_LENGTH)


def sanitize_runtime_settings(values: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep the fields this runtime knows, with values of the right type; drop the rest.

    A newer control plane may deliver fields an older runtime has never heard of, and a value
    that would fail the runtime's own validation must not reach `get_runtime_app_config()`.
    """
    if not isinstance(values, Mapping):
        return {}
    sanitized: dict[str, Any] = {}
    for key, expected in LIVE_SETTING_FIELDS.items():
        if key not in values:
            continue
        value = values[key]
        if expected is bool:
            if isinstance(value, bool):
                sanitized[key] = value
            continue
        minimum = 0 if key in LIVE_SETTING_ZERO_ALLOWED else 1
        if isinstance(value, int) and not isinstance(value, bool) and value >= minimum:
            sanitized[key] = value
    return sanitized


def reset_runtime_settings() -> None:
    """Forget every installed override. Tests use this between cases."""
    global _lineage, _revision, _values, _last_refresh
    with _lock:
        _lineage = ""
        _revision = 0
        _values = {}
        _last_refresh = None
