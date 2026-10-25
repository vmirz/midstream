"""Low-noise runtime performance window tracking.

The status task reads the window with `reset_window=True` on every heartbeat, so its numbers
describe the last ~30 s; cumulative counters live on the partition state instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import RLock

from midstream.protocol_logging import timestamp_latency_ms


@dataclass(slots=True)
class _StageWindow:
    count: int = 0
    total_ms: int = 0
    max_ms: int = 0

    def record(self, duration_ms: int) -> None:
        bounded = max(0, duration_ms)
        self.count += 1
        self.total_ms += bounded
        self.max_ms = max(self.max_ms, bounded)

    def to_dict(self) -> dict[str, int]:
        payload = {"count": self.count, "total_ms": self.total_ms, "max_ms": self.max_ms}
        if self.count > 0:
            payload["avg_ms"] = self.total_ms // self.count
        return payload


@dataclass(slots=True)
class _RuntimePerformanceWindow:
    materialization_id: str
    window_started_at: str
    last_updated_at: str | None = None
    source_records_processed: int = 0
    schema_requests: int = 0
    batches_sealed: int = 0
    upload_batches_committed: int = 0
    upload_failures: int = 0
    commits: int = 0
    commit_failures: int = 0
    files_committed: int = 0
    records_committed: int = 0
    write: _StageWindow = field(default_factory=_StageWindow)
    seal: _StageWindow = field(default_factory=_StageWindow)
    source_record_total: _StageWindow = field(default_factory=_StageWindow)
    upload_batch: _StageWindow = field(default_factory=_StageWindow)
    commit: _StageWindow = field(default_factory=_StageWindow)


_PERFORMANCE_WINDOWS: dict[str, _RuntimePerformanceWindow] = {}
_PERFORMANCE_LOCK = RLock()


def reset_runtime_performance() -> None:
    with _PERFORMANCE_LOCK:
        _PERFORMANCE_WINDOWS.clear()


def record_source_record_processing(
    *,
    materialization_id: str,
    observed_at: str,
    total_ms: int,
    write_ms: int,
    schema_requests: int = 0,
) -> None:
    with _PERFORMANCE_LOCK:
        window = _window_for(materialization_id, started_at=observed_at)
        window.last_updated_at = observed_at
        window.source_records_processed += 1
        window.schema_requests += schema_requests
        window.write.record(write_ms)
        window.source_record_total.record(total_ms)


def record_seal_processing(*, materialization_id: str, observed_at: str, seal_ms: int) -> None:
    """Count one batch sealed by the pipeline, and how long its seal took."""
    with _PERFORMANCE_LOCK:
        window = _window_for(materialization_id, started_at=observed_at)
        window.last_updated_at = observed_at
        window.batches_sealed += 1
        window.seal.record(seal_ms)


def record_upload_processing(
    *,
    materialization_id: str,
    observed_at: str,
    upload_batch_ms: int,
    uploaded_batches: int,
    failures: int,
) -> None:
    with _PERFORMANCE_LOCK:
        window = _window_for(materialization_id, started_at=observed_at)
        window.last_updated_at = observed_at
        if uploaded_batches > 0:
            window.upload_batch.record(upload_batch_ms)
            window.upload_batches_committed += uploaded_batches
        window.upload_failures += max(0, failures)


def record_commit_processing(
    *,
    materialization_id: str,
    observed_at: str,
    commit_ms: int,
    files: int,
    records: int,
    failed: bool = False,
) -> None:
    """Count one commit cycle of the committer member."""
    with _PERFORMANCE_LOCK:
        window = _window_for(materialization_id, started_at=observed_at)
        window.last_updated_at = observed_at
        if failed:
            window.commit_failures += 1
            return
        window.commits += 1
        window.files_committed += files
        window.records_committed += records
        window.commit.record(commit_ms)


def build_runtime_performance_snapshot(
    materialization_id: str,
    *,
    reset_window: bool = False,
) -> dict[str, object] | None:
    with _PERFORMANCE_LOCK:
        window = _PERFORMANCE_WINDOWS.get(materialization_id)
        if window is None or window.last_updated_at is None:
            return None
        window_ended_at = window.last_updated_at
        duration_ms = timestamp_latency_ms(window.window_started_at, ended_at=window_ended_at)
        duration_seconds = 1 if duration_ms in (None, 0) else max(1, duration_ms // 1000)
        snapshot: dict[str, object] = {
            "window_started_at": window.window_started_at,
            "window_ended_at": window_ended_at,
            "window_duration_seconds": duration_seconds,
            "source_records_processed": window.source_records_processed,
            "source_records_per_second": round(window.source_records_processed / duration_seconds, 2),
            "schema_requests": window.schema_requests,
            "batches_sealed": window.batches_sealed,
            "upload_batches_committed": window.upload_batches_committed,
            "upload_failures": window.upload_failures,
            "commits": window.commits,
            "commit_failures": window.commit_failures,
            "files_committed": window.files_committed,
            "records_committed": window.records_committed,
            "write": window.write.to_dict(),
            "seal": window.seal.to_dict(),
            "source_record_total": window.source_record_total.to_dict(),
            "upload_batch": window.upload_batch.to_dict(),
            "commit": window.commit.to_dict(),
        }
        if reset_window:
            _PERFORMANCE_WINDOWS[materialization_id] = _RuntimePerformanceWindow(
                materialization_id=materialization_id,
                window_started_at=window_ended_at,
            )
        return snapshot


def _window_for(materialization_id: str, *, started_at: str) -> _RuntimePerformanceWindow:
    existing = _PERFORMANCE_WINDOWS.get(materialization_id)
    if existing is not None:
        return existing
    window = _RuntimePerformanceWindow(materialization_id=materialization_id, window_started_at=started_at)
    _PERFORMANCE_WINDOWS[materialization_id] = window
    return window


def _current_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
