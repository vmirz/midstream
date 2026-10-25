"""The tail app's own configuration, from its environment, and each table's settings, from its ticks.

The app's variables (`MATERIALIZER_TAIL_*`) size the member and wire it to its deployment: they are
read once at start and fixed. What the app does with each table - how much it holds, how long it
keeps committed records, how fast it reads - is the control plane's live-tail settings, resolved with
the Settings page's overrides, which every tick carries (`TailSettings.from_tick`): a change applies
within a minute, without a restart.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
import logging
import os
from typing import Any

LOGGER = logging.getLogger(__name__)

APP_NAME = "materializer-tail"
TABLE_EVENTS_TOPIC = "materialization-table-events"
REQUESTS_TOPIC = "materializer-tail-requests"
#: A table no tick has named for this long is forgotten: its buffer dropped, its partitions let go.
FORGET_AFTER_SECONDS = 300
#: Resident buffer bytes across every table of the member before retained records are dropped and
#: the largest table pauses.
MEMORY_BUDGET_BYTES = 2_684_354_560
#: How long a checked session is trusted, and how much longer while the control plane is unreachable.
SESSION_CHECK_SECONDS = 5
SESSION_GRACE_SECONDS = 60
#: Answers built at once on the member's threads, and head peeks fetched at once.
SERVE_CONCURRENCY = 4
PEEK_CONCURRENCY = 2
#: How often the partition -> member map is read from the app's consumer group.
MEMBER_MAP_REFRESH_SECONDS = 15
WEB_PORT = 6065
#: The shortest service token taken: a shorter one is ignored, as the control plane ignores it.
MIN_SECRET_LENGTH = 32


@dataclass(frozen=True, slots=True)
class TailAppConfig:
    """The member's configuration: fixed by the deployment, read once at start."""

    app_name: str = APP_NAME
    #: Where the control plane's API is, for session checks and schema fetches.
    control_plane_url: str | None = None
    table_events_topic: str = TABLE_EVENTS_TOPIC
    requests_topic: str = REQUESTS_TOPIC
    forget_after_seconds: int = FORGET_AFTER_SECONDS
    memory_budget_bytes: int = MEMORY_BUDGET_BYTES
    session_check_seconds: int = SESSION_CHECK_SECONDS
    session_grace_seconds: int = SESSION_GRACE_SECONDS
    serve_concurrency: int = SERVE_CONCURRENCY
    peek_concurrency: int = PEEK_CONCURRENCY
    member_map_refresh_seconds: int = MEMBER_MAP_REFRESH_SECONDS
    #: This member's name: its pod's.
    member_id: str = "unknown-member"
    #: The app's consumer group, which is its Kaspr app name (`K_APP_NAME`): whose members own which
    #: tables.
    group_id: str | None = None
    #: This member's own address, as Kaspr publishes it to the group (`K_WEB_HOST`, `K_WEB_PORT`).
    web_host: str | None = None
    web_port: int = WEB_PORT

    def __post_init__(self) -> None:
        if self.forget_after_seconds < 60:
            raise ValueError("forget_after_seconds must be >= 60")
        if self.memory_budget_bytes < 64 * 1024 * 1024:
            raise ValueError("memory_budget_bytes must be >= 64 MiB")
        if self.session_check_seconds < 0:
            raise ValueError("session_check_seconds must be >= 0")
        for name in ("serve_concurrency", "peek_concurrency", "member_map_refresh_seconds"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1")
        if not self.app_name:
            raise ValueError("app_name is required")

    @property
    def own_url(self) -> str | None:
        """This member's address, as the other members reach it."""
        if not self.web_host:
            return None
        return f"http://{self.web_host}:{self.web_port}"


def get_tail_app_config(environ: Mapping[str, str] | None = None) -> TailAppConfig:
    """The member's configuration: `MATERIALIZER_TAIL_*` and Kaspr's own variables, defaults otherwise."""
    env = os.environ if environ is None else environ
    url = (env.get("MATERIALIZER_TAIL_CONTROL_PLANE_URL") or "").strip().rstrip("/") or None
    return TailAppConfig(
        app_name=env.get("MATERIALIZER_TAIL_APP_NAME") or APP_NAME,
        control_plane_url=url,
        table_events_topic=env.get("MATERIALIZER_TAIL_TABLE_EVENTS_TOPIC") or TABLE_EVENTS_TOPIC,
        requests_topic=env.get("MATERIALIZER_TAIL_REQUESTS_TOPIC") or REQUESTS_TOPIC,
        forget_after_seconds=_int(env, "MATERIALIZER_TAIL_FORGET_AFTER_SECONDS", FORGET_AFTER_SECONDS),
        memory_budget_bytes=_int(env, "MATERIALIZER_TAIL_MEMORY_BUDGET_BYTES", MEMORY_BUDGET_BYTES),
        session_check_seconds=_int(env, "MATERIALIZER_TAIL_SESSION_CHECK_SECONDS", SESSION_CHECK_SECONDS),
        serve_concurrency=_int(env, "MATERIALIZER_TAIL_SERVE_CONCURRENCY", SERVE_CONCURRENCY),
        peek_concurrency=_int(env, "MATERIALIZER_TAIL_PEEK_CONCURRENCY", PEEK_CONCURRENCY),
        member_map_refresh_seconds=_int(env, "MATERIALIZER_TAIL_MEMBER_MAP_REFRESH_SECONDS", MEMBER_MAP_REFRESH_SECONDS),
        member_id=env.get("MATERIALIZER_RUNTIME_MEMBER_ID") or env.get("HOSTNAME") or "unknown-member",
        group_id=env.get("K_APP_NAME") or env.get("KASPR_APP_NAME") or None,
        web_host=env.get("K_WEB_HOST") or env.get("KASPR_WEB_HOST") or None,
        web_port=_int(env, "K_WEB_PORT", WEB_PORT),
    )


def service_token(environ: Mapping[str, str] | None = None) -> str | None:
    """The read-only service credential (`MATERIALIZER_TAIL_SERVICE_TOKEN`), never a config field.

    It is the control plane's service token: the app fetches schemas with it, and it is the console's
    own credential, which every request must carry. One shorter than 32 characters is ignored.
    """
    env = os.environ if environ is None else environ
    token = (env.get("MATERIALIZER_TAIL_SERVICE_TOKEN") or "").strip()
    if not token:
        return None
    if len(token) < MIN_SECRET_LENGTH:
        LOGGER.error("MATERIALIZER_TAIL_SERVICE_TOKEN is shorter than %s characters and is ignored", MIN_SECRET_LENGTH)
        return None
    return token


@dataclass(frozen=True, slots=True)
class TailSettings:
    """What the tail does with one table: the control plane's live-tail settings, from its tick."""

    max_rows_per_table: int = 250_000
    max_bytes_per_table: int = 256 * 1024 * 1024
    retention_seconds: int = 120
    fetch_wait_ms: int = 200
    max_records_per_second_per_table: int = 5_000
    peek_max_rows: int = 1_000
    idle_seconds: int = 600
    protocol_logging_enabled: bool = False

    def __post_init__(self) -> None:
        bounds = {
            "max_rows_per_table": (1_000, 10_000_000),
            "max_bytes_per_table": (8 * 1024 * 1024, 4 * 1024 * 1024 * 1024),
            "retention_seconds": (10, 3_600),
            "fetch_wait_ms": (50, 2_000),
            "max_records_per_second_per_table": (100, 100_000),
            "peek_max_rows": (10, 10_000),
            "idle_seconds": (0, 86_400),
        }
        for name, (low, high) in bounds.items():
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
                raise ValueError(f"{name} must be an integer from {low} to {high}")
        if not isinstance(self.protocol_logging_enabled, bool):
            raise ValueError("protocol_logging_enabled must be a boolean")

    @classmethod
    def from_tick(cls, settings: Mapping[str, Any], *, previous: "TailSettings | None" = None) -> "TailSettings":
        """The settings a tick carries over the previous ones; the previous ones when they do not fit."""
        base = previous or default_settings()
        fields = {
            "max_rows_per_table": "live_tail_max_rows_per_table",
            "max_bytes_per_table": "live_tail_max_bytes_per_table",
            "retention_seconds": "live_tail_retention_seconds",
            "fetch_wait_ms": "live_tail_fetch_wait_ms",
            "max_records_per_second_per_table": "live_tail_max_records_per_second_per_table",
            "peek_max_rows": "live_tail_peek_max_rows",
            "idle_seconds": "live_tail_idle_seconds",
            "protocol_logging_enabled": "protocol_logging_enabled",
        }
        changes = {name: settings[key] for name, key in fields.items() if key in settings}
        try:
            return replace(base, **changes)
        except (TypeError, ValueError) as error:
            LOGGER.warning("Ignoring live-tail settings that do not fit: %s", error)
            return base


def default_settings(environ: Mapping[str, str] | None = None) -> TailSettings:
    """The settings a table has before any tick: the defaults, with the app's own protocol logging."""
    env = os.environ if environ is None else environ
    logging_enabled = (env.get("MATERIALIZER_PROTOCOL_LOGGING_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}
    return TailSettings(protocol_logging_enabled=logging_enabled)


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
