"""The maintenance app's own configuration, from its environment.

What its jobs do - intervals, sizes, retention - is not here: those are the runtimes' maintenance
settings, which the control plane resolves, overrides included, and sends with every tick.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os

#: Long jobs - a compaction child, an orphan cleanup - one member runs at once, across all its tables.
MAX_CONCURRENT_JOBS = 1
#: A table no tick has named for this long is forgotten, and its job stopped: its partition moved to
#: another member, or the control plane stopped maintaining it.
FORGET_AFTER_SECONDS = 300
APP_NAME = "materializer-maintenance"


@dataclass(frozen=True, slots=True)
class MaintenanceAppConfig:
    max_concurrent_jobs: int = MAX_CONCURRENT_JOBS
    forget_after_seconds: int = FORGET_AFTER_SECONDS
    #: Names the app in its commits' snapshot summaries and its table events.
    app_name: str = APP_NAME

    def __post_init__(self) -> None:
        if self.max_concurrent_jobs < 1:
            raise ValueError("max_concurrent_jobs must be >= 1")
        if self.forget_after_seconds < 60:
            raise ValueError("forget_after_seconds must be >= 60")
        if not self.app_name:
            raise ValueError("app_name is required")


def get_maintenance_app_config(environ: Mapping[str, str] | None = None) -> MaintenanceAppConfig:
    """The app's configuration: `MAINTENANCE_*` from its environment, defaults otherwise."""
    env = os.environ if environ is None else environ
    return MaintenanceAppConfig(
        max_concurrent_jobs=_int(env, "MAINTENANCE_MAX_CONCURRENT_JOBS", MAX_CONCURRENT_JOBS),
        forget_after_seconds=_int(env, "MAINTENANCE_FORGET_AFTER_SECONDS", FORGET_AFTER_SECONDS),
        app_name=env.get("MAINTENANCE_APP_NAME") or APP_NAME,
    )


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
