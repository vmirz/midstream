"""Table maintenance for every materialization, off the runtimes: compaction, expiry, orphan cleanup.

The control plane ticks each table every minute (`services/table_maintenance.py`), with where it is,
the maintenance settings as they are now, and when each job last finished. A member keeps a
`TableMaintenance` for each table it has been ticked for - its schedule and its failures - and one
`MaintenanceWorker` shares the member between them: at most `max_concurrent_jobs` long jobs at once,
a compaction child or an orphan cleanup, the tables visited in turn so each gets its jobs. A snapshot
expiry, or a compaction plan that finds nothing, is quick and takes no slot.

A table starts at most one job at a time, in priority order: what someone asked for; a reprotect
its record asks for (`lakehouse/reprotect.py`), once the table has recorded the protection revision
it waits for, and pass after pass until nothing is left to rewrite; compaction, every
`compaction_interval_minutes`; snapshot expiry, every `snapshot_expiry_interval_minutes`; orphan
cleanup, every `orphan_cleanup_interval_hours`; and, while the last compaction found work, the next
compaction at once - so a backlog is worked through a job a step, not a job an interval. What a
table holds in plain text that its protection masks is counted whenever its compaction is planned,
its recorded protection revision changes, or a reprotect commits, and reported when it changed.

The runtime's committer appends to the table meanwhile. Both commit through the catalog pointer's
compare-and-swap and reload and retry when the other got there first; a replace commit checks that
every input is still live and that the outputs hold exactly the inputs' rows, and carries the
committed offsets forward, so a runtime never finds its exactly-once accounting changed.

Fault tolerance (spec §7.9.1):

- compaction runs in a child process (`compact_worker.py`) that only *adds* files, as does reprotect
  (`reprotect_worker.py`); this member then commits one `replace` snapshot. A job that dies before
  that commit leaves orphans; one that dies after it is done;
- a job is bounded by `compaction_timeout_minutes`, after which the child is killed; it is also
  killed when the table is cancelled (dropped, or its materialization deleted), forgotten, or the
  member stops;
- a partition whose compaction fails backs off from 5 minutes to 6 hours, and after
  `compaction_max_attempts` failures in a row is skipped, with a `compaction_failed` table error,
  until someone asks for compaction again. A job of several partitions that fails counts against
  none of them: each is compacted alone from then on, until it succeeds;
- expiry is one metadata commit; orphan cleanup deletes only what no retained snapshot references
  and is older than `orphan_min_age_hours`, so running either again is harmless. A file a runtime
  uploaded but has not committed that long - a stall of a day, by default - may be among them: the
  runtime's committer then finds it missing and seals its rows again.

The member holds nothing a restart loses that matters: the ticks say when each job last finished,
and the table record says which job was running, which a restarted member reports abandoned.
"""

from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import json
import logging
from pathlib import Path
import shutil
import sys
import tempfile
import time
from typing import Any
import uuid

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.typedef import Record

from midstream.lakehouse.commits import fork_map_of, protection_json_of, table_totals
from midstream.lakehouse.files import build_data_file, read_parquet_footer
from midstream.lakehouse.gcs import delete_object, shared_storage_client
from midstream.lakehouse.lifecycle import schema_to_json
from midstream.lakehouse.maintenance import (
    CompactionInputGone,
    CompactionPlan,
    LiveFile,
    PurgeResult,
    commit_compaction,
    commit_reprotect,
    expire_snapshots,
    find_orphans,
    live_data_files,
    plan_compaction,
    plan_expiry,
    purge_before,
    referenced_files,
    small_file_count,
)
from midstream.lakehouse.lifecycle import schema_to_json as _schema_json
from midstream.lakehouse.reprotect import ReprotectBacklog, ReprotectPlan, field_births, plan_reprotect
from midstream.maintenance_app.config import MaintenanceAppConfig, get_maintenance_app_config
from midstream.models import MaintenanceMessage, MaintenanceTarget, ReprotectRequest, RunningMaintenance
from midstream.protection.policy import TableProtection
from midstream.models.table import MAINTENANCE_JOBS
from midstream.protocol_logging import log_protocol_event
from midstream.runtime_app.config import RuntimeAppConfig
from midstream.runtime_app.table_state import TableContext, granularity_of, load_table
from midstream.storage import materialization_routing_key

LOGGER = logging.getLogger(__name__)

COMPACT_WORKER_MODULE = "midstream.maintenance_app.compact_worker"
REPROTECT_WORKER_MODULE = "midstream.maintenance_app.reprotect_worker"
#: How long a reprotect waits before looking again: for a table that has not recorded the revision it
#: waits for, or for the batches still open under an older one.
REPROTECT_WAIT_SECONDS = 30.0
#: Beyond the batch ceiling, what a reprotect allows the batches a revision rolled to seal, upload
#: and commit before it calls the table done.
REPROTECT_SETTLE_MARGIN_SECONDS = 120.0
#: Backoff of a partition whose compaction failed: doubling from the first, capped at the second.
COMPACTION_BACKOFF_SECONDS = (300, 6 * 3600)
#: Files orphan cleanup deletes at most in one pass.
ORPHAN_PASS_LIMIT = 1000
#: Commit attempts against the runtime's appends, which may keep landing first.
REPLACE_ATTEMPTS = 8
#: Pause between those attempts: about the spacing of a busy runtime's commits.
REPLACE_RETRY_SECONDS = 1.0
#: A job the table record says has run this much longer than any job may is over, whoever ran it.
STALE_JOB_MARGIN_SECONDS = 600

# Planning, commits, expiry and orphan listing block on GCS: a few threads, shared by every table.
_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="maintenance")


@dataclass(slots=True)
class _Job:
    kind: str
    started_at: float
    started_wall: str
    partition: str | None = None
    attempt: int = 1
    task: asyncio.Task | None = None
    process: Any = None
    work_dir: str | None = None
    plan: CompactionPlan | None = None
    reprotect_plan: ReprotectPlan | None = None
    request: ReprotectRequest | None = None


@dataclass(slots=True)
class _Finished:
    plan: CompactionPlan
    outputs: list[dict[str, Any]]
    started_at: float
    started_wall: str
    attempt: int


@dataclass(slots=True)
class _ReprotectReady:
    """A reprotect pass whose rewrites are written and wait for their commit."""

    plan: ReprotectPlan
    request: ReprotectRequest
    outputs: list[dict[str, Any]]
    counts: dict[str, Any]
    started_at: float
    started_wall: str


@dataclass(slots=True)
class _Failure:
    attempts: int = 0
    next_at: float = 0.0
    gave_up: bool = False


class CompactionWorkerError(RuntimeError):
    """The compaction or reprotect process failed; its last line of output says why."""


async def _run_worker(module: str, job_file: Path, job: "_Job") -> dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        module,
        str(job_file),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    job.process = process
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        lines = stderr.decode("utf-8", errors="replace").strip().splitlines()
        raise CompactionWorkerError(lines[-1] if lines else f"the {job.kind} process exited with status {process.returncode}")
    return json.loads(stdout.decode("utf-8").strip().splitlines()[-1])


async def compact_in_subprocess(job_file: Path, job: "_Job") -> dict[str, Any]:
    """Run one compaction in a child process (`compact_worker.py`); return what it wrote."""
    return await _run_worker(COMPACT_WORKER_MODULE, job_file, job)


async def compact_in_process(job_file: Path, job: "_Job") -> dict[str, Any]:
    """Compact in this process, blocking its event loop throughout. For tests and tools only."""
    from midstream.maintenance_app.compact_worker import run

    return run(json.loads(job_file.read_text(encoding="utf-8")))


async def reprotect_in_subprocess(job_file: Path, job: "_Job") -> dict[str, Any]:
    """Run one reprotect pass in a child process (`reprotect_worker.py`); return what it wrote."""
    return await _run_worker(REPROTECT_WORKER_MODULE, job_file, job)


async def reprotect_in_process(job_file: Path, job: "_Job") -> dict[str, Any]:
    """Rewrite in this process, blocking its event loop throughout. For tests and tools only."""
    from midstream.maintenance_app.reprotect_worker import run

    return run(json.loads(job_file.read_text(encoding="utf-8")))


def job_settings(settings: dict[str, Any], *, previous: RuntimeAppConfig | None = None) -> RuntimeAppConfig:
    """The maintenance settings a tick carries, over the defaults; the previous ones if they do not fit."""
    base = RuntimeAppConfig()
    known = {name: value for name, value in settings.items() if hasattr(base, name)}
    try:
        return replace(base, **known)
    except (TypeError, ValueError) as error:
        LOGGER.warning("Ignoring maintenance settings that do not fit together: %s", error)
        return previous or base


class TableMaintenance:
    """One table's maintenance on this member: its schedule, its failures and the job it runs."""

    #: How a compaction runs: in a child process, so its minutes of work never hold the member.
    compactor = staticmethod(compact_in_subprocess)
    #: How a reprotect pass runs: in a child process too.
    reprotector = staticmethod(reprotect_in_subprocess)

    def __init__(self, target: MaintenanceTarget, settings: RuntimeAppConfig, *, client: Any = None, app: MaintenanceAppConfig | None = None) -> None:
        self.target = target
        self.settings = settings
        self.client = client
        self.app = app or get_maintenance_app_config()
        self.last_seen = time.monotonic()
        self.job: _Job | None = None
        self.ready: _Finished | _ReprotectReady | None = None
        self.results: deque[dict[str, Any]] = deque()
        self.requested: deque[str] = deque()
        #: When each job last ran, epoch seconds: from ticks, and from the jobs run here.
        self.last_runs: dict[str, float] = {}
        self.failures: dict[str, _Failure] = {}
        #: Partitions only ever compacted alone: they were in a job of several that failed.
        self.solo: set[str] = set()
        #: The last compaction found work, so the next starts at the next step.
        self.compaction_backlog = False
        self.small_files: int | None = None
        #: When each job this member ran started (`started_wall`), so a tick naming one is not taken
        #: for a job a predecessor left running.
        self.started_here: set[str] = set()
        #: A reprotect the table's record asks for, as the ticks carry it.
        self.reprotect: ReprotectRequest | None = None
        #: Reprotects finished here that the ticks may still carry until the record catches up.
        self.reprotect_done: set[str] = set()
        self.reprotect_failure = _Failure()
        self.reprotect_failure_for: str | None = None
        self.reprotect_wait_until = 0.0
        #: What the passes of the reprotect asked for have rewritten so far, for its last report.
        self.reprotect_totals: tuple[str | None, dict[str, int]] = (None, {})
        #: The protection revision the table recorded, as the ticks carry it.
        self.protection_revision: int | None = None
        #: What the table holds in plain text, as last reported; counted again when due.
        self.plaintext_reported: tuple | None = None
        self.plaintext_due = False

    @property
    def context(self) -> TableContext:
        target = self.target
        return TableContext(
            materialization_id=target.materialization_id,
            location=target.location,
            namespace=target.namespace,
            name=target.table,
            partition_granularity=target.partition_granularity,
            source_topic=target.source_topic,
        )

    @property
    def busy(self) -> bool:
        """Whether a job runs, or has finished and waits for its commit."""
        return self.job is not None or self.ready is not None

    # -- what the control plane says -------------------------------------------------------------

    def observe(self, message: MaintenanceMessage) -> list[dict[str, Any]]:
        """Take a tick's target, settings and history; report a job its record names that nothing runs."""
        self.target = message.target
        self.settings = job_settings(message.settings, previous=self.settings)
        self.last_seen = time.monotonic()
        for job, at in message.last_runs.items():
            seconds = _parse_time(at)
            if seconds is not None and seconds > self.last_runs.get(job, 0.0):
                self.last_runs[job] = seconds
        self.reprotect = message.reprotect
        carried = None if message.reprotect is None else message.reprotect.request_id
        self.reprotect_done = {request_id for request_id in self.reprotect_done if request_id == carried}
        revision = message.protection_revision
        if revision is not None and revision != self.protection_revision:
            self.protection_revision = revision
            self.plaintext_due = revision > 0
        return self._settle_reported(message.running)

    def request(self, message: MaintenanceMessage) -> None:
        self.target = message.target
        if message.settings:
            self.settings = job_settings(message.settings, previous=self.settings)
        self.last_seen = time.monotonic()
        job = message.job
        if job not in MAINTENANCE_JOBS:
            return
        if job == "compaction":
            # Asking again is how someone retries partitions compaction gave up on.
            self.failures.clear()
        if job not in self.requested:
            self.requested.append(job)

    def cancel(self, *, reason: str) -> list[dict[str, Any]]:
        """Stop whatever runs, for a table being dropped: report it cancelled, so the drop goes on."""
        job, ready = self.job, self.ready
        self.stop()
        if ready is not None:
            # Finished but not committed: its outputs are nobody's.
            self._delete_outputs(ready)
        outputs = list(self.results)
        self.results.clear()
        kind = "reprotect" if isinstance(ready, _ReprotectReady) else "compaction"
        running = job or (None if ready is None else _Job(kind=kind, started_at=ready.started_at, started_wall=ready.started_wall))
        if running is not None:
            outputs.append(
                self._event(
                    "maintenance_completed",
                    {
                        "job": running.kind,
                        "outcome": "cancelled",
                        "partition": running.partition,
                        "started_at": running.started_wall,
                        "message": f"Stopped: {reason.replace('_', ' ')}.",
                        "counts": {},
                    },
                )
            )
        return outputs

    def stop(self) -> None:
        """Kill a running job and forget its result."""
        job, self.job = self.job, None
        self.ready = None
        if job is not None:
            _kill(job)

    def _settle_reported(self, running: RunningMaintenance | None) -> list[dict[str, Any]]:
        """A job the record says runs, which nothing here runs: one a predecessor of this member left.

        Reported abandoned, so the record and the console stop showing it. A job another member may
        still run is left alone, unless it has run longer than any job can.
        """
        if running is None or self.job is not None or self.ready is not None:
            return []
        if running.started_at in self.started_here:
            # Run here and finished: its report is on its way.
            return []
        started = _parse_time(running.started_at)
        limit = self.settings.compaction_timeout_minutes * 60 + STALE_JOB_MARGIN_SECONDS
        stale = started is not None and time.time() - started > limit
        if running.member_id not in (None, current_member_id()) and not stale:
            return []
        return [
            self._event(
                "maintenance_completed",
                {
                    "job": running.job,
                    "outcome": "abandoned",
                    "partition": running.partition,
                    "started_at": running.started_at,
                    "message": "Its maintenance member stopped before it finished; it runs again when it is next due.",
                    "counts": {},
                },
            )
        ]

    # -- the step --------------------------------------------------------------------------------

    async def step(self) -> list[dict[str, Any]]:
        """Finish what is done: report results, commit a finished compaction, stop one past its time."""
        outputs: list[dict[str, Any]] = []
        while self.results:
            outputs.append(self.results.popleft())
        if self.ready is not None:
            outputs.extend(await self._commit_ready())
        job = self.job
        if job is not None and job.kind in ("compaction", "reprotect"):
            if time.monotonic() - job.started_at > self.settings.compaction_timeout_minutes * 60:
                self.job = None
                _kill(job)
                message = f"the {job.kind} ran past compaction_timeout_minutes and was stopped"
                if job.kind == "reprotect":
                    outputs.extend(self._reprotect_failed(job.request, message, started_wall=job.started_wall))
                else:
                    outputs.extend(self._compaction_failed(job.plan, message, started_wall=job.started_wall))
        return outputs

    async def start_next(self) -> list[dict[str, Any]]:
        """Start the next job that is due, if none runs: a compaction or orphan cleanup runs on after
        this returns (`busy`); a snapshot expiry, or a compaction plan that finds nothing, is done."""
        if self.busy:
            return []
        outputs: list[dict[str, Any]] = []
        if self.plaintext_due:
            self.plaintext_due = False
            outputs.extend(await self._count_plaintext())
        kind, requested = self._next_job()
        if kind is None:
            return outputs
        self.last_runs[kind] = time.time()
        if kind == "reprotect":
            return outputs + await self._start_reprotect()
        if kind == "compaction":
            return outputs + await self._start_compaction(requested=requested)
        if kind == "expire_snapshots":
            return outputs + await self._expire(requested=requested)
        if kind == "orphan_cleanup":
            return outputs + self._start_orphan_cleanup()
        return outputs

    @property
    def asked(self) -> bool:
        """Whether someone is waiting for a job here: one requested, or a reprotect due."""
        return bool(self.requested) or self._reprotect_due()

    def _reprotect_due(self) -> bool:
        request = self.reprotect
        if request is None or request.request_id in self.reprotect_done:
            return False
        now = time.monotonic()
        if now < self.reprotect_wait_until:
            return False
        if self.reprotect_failure_for != request.request_id:
            # A new request starts with a clean slate, whatever became of the last.
            self.reprotect_failure, self.reprotect_failure_for = _Failure(), request.request_id
        if self.reprotect_failure.gave_up or self.reprotect_failure.next_at > now:
            return False
        return not (request.revision and self.protection_revision is not None and self.protection_revision < request.revision)

    def _next_job(self) -> tuple[str | None, bool]:
        """The job to run now, and whether someone asked for it (`POST .../maintenance`)."""
        if self.requested:
            return self.requested.popleft(), True
        if self._reprotect_due():
            return "reprotect", True
        config = self.settings
        now = time.time()
        if config.compaction_enabled and self._due("compaction", config.compaction_interval_minutes * 60, now):
            return "compaction", False
        if self._due("expire_snapshots", config.snapshot_expiry_interval_minutes * 60, now):
            return "expire_snapshots", False
        if config.orphan_cleanup_enabled and self._due("orphan_cleanup", config.orphan_cleanup_interval_hours * 3600, now):
            return "orphan_cleanup", False
        if config.compaction_enabled and self.compaction_backlog:
            return "compaction", False
        return None, False

    def _due(self, kind: str, interval: float, now: float) -> bool:
        last = self.last_runs.get(kind)
        return last is None or now - last >= interval

    # -- compaction ------------------------------------------------------------------------------

    async def _start_compaction(self, *, requested: bool = False) -> list[dict[str, Any]]:
        config = self.settings
        now = time.monotonic()
        skip = {key for key, failure in self.failures.items() if failure.gave_up or failure.next_at > now}
        loop = asyncio.get_running_loop()
        try:
            table, plan, small, plaintext = await loop.run_in_executor(_POOL, self._plan_compaction, skip, requested)
        except Exception as error:  # noqa: BLE001 - planning reads the table; retried next interval
            LOGGER.warning("Planning a compaction of %s table %s failed: %s", self.target.materialization_id, self.target.table, error)
            if requested:
                return [self._completed_event({"job": "compaction", "outcome": "failed", "message": str(error), "counts": {}})]
            return []
        self.small_files = small
        self.compaction_backlog = plan is not None
        counted = self._plaintext_event(plaintext)
        if plan is None:
            if not requested:
                return counted
            # Asked for, so say what was found: nothing worth rewriting.
            return counted + [
                self._completed_event(
                    {
                        "job": "compaction",
                        "outcome": "skipped",
                        "message": (
                            f"No partition has {config.compaction_min_input_files} or more files smaller than "
                            f"{config.compaction_small_file_bytes} bytes."
                        ),
                        "counts": {"small_files": small},
                    }
                )
            ]
        attempt = self.failures.get(plan.partition_key, _Failure()).attempts + 1 if len(plan.partition_keys) == 1 else 1
        work_dir = tempfile.mkdtemp(prefix=f"compact-{plan.job_id[:8]}-", dir=_work_root())
        job = _Job(
            kind="compaction",
            started_at=time.monotonic(),
            started_wall=_utc_now(),
            partition=plan.label,
            attempt=attempt,
            work_dir=work_dir,
            plan=plan,
        )
        self.job = job
        self.started_here.add(job.started_wall)
        job_file = Path(work_dir) / "job.json"
        job_file.write_text(
            json.dumps(
                {
                    "plan": plan.to_dict(),
                    "schema": schema_to_json(table.schema()),
                    "work_dir": work_dir,
                    "location": self.target.location,
                    "granularity": granularity_of(table),
                    "target_file_size_bytes": config.compaction_target_file_size_bytes,
                }
            ),
            encoding="utf-8",
        )
        job.task = loop.create_task(self._run_compaction(job, job_file))
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="maintenance_compaction_started",
            materialization_id=self.target.materialization_id,
            table=self.target.table,
            job_id=plan.job_id,
            partition=plan.label,
            partitions=len(plan.partition_keys),
            inputs=len(plan.inputs),
            input_bytes=plan.input_bytes,
            attempt=attempt,
        )
        return counted + [self._started_event(job)]

    def _plan_compaction(self, skip: set[str], requested: bool = False):
        config = self.settings
        table = load_table(self.context, self.client)
        files = live_data_files(table)
        plan = plan_compaction(
            table,
            now=datetime.now(tz=UTC),
            min_input_files=config.compaction_min_input_files,
            small_file_bytes=config.compaction_small_file_bytes,
            max_input_bytes=config.compaction_max_input_bytes,
            # Someone asked for a compaction now: the partition being written is not left out.
            hot_partition_delay_minutes=0 if requested else config.compaction_hot_partition_delay_minutes,
            skip=skip,
            solo=self.solo,
            files=files,
        )
        # Counted from what is at hand: the files are listed already.
        _, plaintext = plan_reprotect(table, files=files, max_input_bytes=config.compaction_max_input_bytes)
        return table, plan, small_file_count(files, small_file_bytes=config.compaction_small_file_bytes), plaintext

    async def _run_compaction(self, job: _Job, job_file: Path) -> None:
        try:
            result = await type(self).compactor(job_file, job)
            if self.job is not job:
                return
            self.ready = _Finished(
                plan=job.plan,
                outputs=list(result.get("outputs") or []),
                started_at=job.started_at,
                started_wall=job.started_wall,
                attempt=job.attempt,
            )
            self.job = None
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - counted as a failed attempt
            if self.job is job:
                self.job = None
                self.results.extend(self._compaction_failed(job.plan, str(error), started_wall=job.started_wall))
        finally:
            if job.work_dir:
                shutil.rmtree(job.work_dir, ignore_errors=True)

    async def _commit_ready(self) -> list[dict[str, Any]]:
        ready, self.ready = self.ready, None
        if ready is None:
            return []
        if isinstance(ready, _ReprotectReady):
            return await self._commit_reprotect_ready(ready)
        try:
            table, snapshot_id = await asyncio.get_running_loop().run_in_executor(_POOL, self._commit_compaction, ready)
        except CompactionInputGone as error:
            # The runtime, expiry or an engine changed those files first: the outputs are not needed.
            self._delete_outputs(ready)
            LOGGER.info("Abandoned compaction %s of table %s %s: %s", ready.plan.job_id, self.target.table, ready.plan.label, error)
            return [self._completed(ready, snapshot_id=None, outcome="abandoned", message=str(error))]
        except Exception as error:  # noqa: BLE001 - CompactionMismatch, GCS errors, lost races
            self._delete_outputs(ready)
            return self._compaction_failed(ready.plan, str(error), started_wall=ready.started_wall)
        for key in ready.plan.partition_keys:
            self.failures.pop(key, None)
            self.solo.discard(key)
        log_protocol_event(
            LOGGER,
            enabled=self.settings.protocol_logging_enabled,
            event="maintenance_compaction_completed",
            materialization_id=self.target.materialization_id,
            table=self.target.table,
            job_id=ready.plan.job_id,
            partition=ready.plan.label,
            partitions=len(ready.plan.partition_keys),
            inputs=len(ready.plan.inputs),
            outputs=len(ready.outputs),
            records=ready.plan.input_records,
            snapshot_id=snapshot_id,
        )
        return [self._completed(ready, snapshot_id=snapshot_id, outcome="succeeded", table=table)]

    def _commit_compaction(self, ready: _Finished):
        client = self.client or shared_storage_client()
        last_error: BaseException | None = None
        for _ in range(REPLACE_ATTEMPTS):
            table = load_table(self.context, self.client)
            granularity = granularity_of(table)
            data_files = []
            for output in ready.outputs:
                footer, size = read_parquet_footer(client, output["uri"])
                data_files.append(
                    build_data_file(
                        table.metadata,
                        file_path=output["uri"],
                        file_size=size,
                        parquet_metadata=footer,
                        granularity=granularity,
                        partition=Record(*(output["partition"] if "partition" in output else ready.plan.partition)),
                    )
                )
            bounds = [(output.get("timestamp_min"), output.get("timestamp_max")) for output in ready.outputs]
            try:
                snapshot_id = commit_compaction(table, ready.plan, data_files, runtime=self.app.app_name, output_bounds=bounds)
            except CommitFailedException as error:
                last_error = error
                time.sleep(REPLACE_RETRY_SECONDS)
                continue
            return load_table(self.context, self.client), snapshot_id
        raise RuntimeError(f"the replace commit kept losing to other commits: {last_error}")

    def _delete_outputs(self, ready: "_Finished | _ReprotectReady") -> None:
        client = self.client or shared_storage_client()
        for output in ready.outputs:
            try:
                delete_object(client, output["uri"])
            except Exception:  # noqa: BLE001 - orphan cleanup collects them
                pass

    def _compaction_failed(self, plan: CompactionPlan | None, message: str, *, started_wall: str | None = None) -> list[dict[str, Any]]:
        if plan is None:
            return []
        config = self.settings
        # A failure slows the cadence back to the interval: a broken bucket is not retried every step.
        self.compaction_backlog = False
        if len(plan.partition_keys) > 1:
            # Which partition failed is unknown: each is compacted alone until it succeeds.
            self.solo.update(plan.partition_keys)
            LOGGER.warning(
                "Compaction of %s table %s partitions %s failed; compacting them one at a time: %s",
                self.target.materialization_id,
                self.target.table,
                plan.label,
                message,
            )
            log_protocol_event(
                LOGGER,
                enabled=config.protocol_logging_enabled,
                event="maintenance_compaction_failed",
                materialization_id=self.target.materialization_id,
                table=self.target.table,
                job_id=plan.job_id,
                partition=plan.label,
                partitions=len(plan.partition_keys),
                attempt=1,
                retry_in_seconds=0,
                error=message,
            )
            return [
                self._completed_event(
                    {
                        "job": "compaction",
                        "outcome": "failed",
                        "partition": plan.label,
                        "started_at": started_wall,
                        "message": f"{message} (its partitions are compacted one at a time from now on)",
                        "attempt": 1,
                        "counts": {"inputs": len(plan.inputs), "partitions": len(plan.partition_keys)},
                    }
                )
            ]
        failure = self.failures.setdefault(plan.partition_key, _Failure())
        failure.attempts += 1
        backoff = min(COMPACTION_BACKOFF_SECONDS[0] * 2 ** (failure.attempts - 1), COMPACTION_BACKOFF_SECONDS[1])
        failure.next_at = time.monotonic() + backoff
        LOGGER.warning(
            "Compaction of %s table %s partition %s failed (attempt %s): %s",
            self.target.materialization_id,
            self.target.table,
            plan.partition_key,
            failure.attempts,
            message,
        )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="maintenance_compaction_failed",
            materialization_id=self.target.materialization_id,
            table=self.target.table,
            job_id=plan.job_id,
            partition=plan.partition_key,
            attempt=failure.attempts,
            retry_in_seconds=backoff,
            error=message,
        )
        outputs = [
            self._completed_event(
                {
                    "job": "compaction",
                    "outcome": "failed",
                    "partition": plan.partition_key,
                    "started_at": started_wall,
                    "message": message,
                    "attempt": failure.attempts,
                    "counts": {"inputs": len(plan.inputs)},
                }
            )
        ]
        if failure.attempts >= config.compaction_max_attempts and not failure.gave_up:
            failure.gave_up = True
            outputs.append(
                self._event(
                    "table_error",
                    {
                        "reason": "compaction_failed",
                        "message": (
                            f"Compacting partition {plan.partition_key} failed {failure.attempts} times and is paused "
                            f"until compaction is run again: {message}"
                        ),
                    },
                )
            )
        return outputs

    def _completed(self, ready: _Finished, *, snapshot_id: int | None, outcome: str, message: str | None = None, table=None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "job": "compaction",
            "outcome": outcome,
            "partition": ready.plan.label,
            "snapshot_id": snapshot_id,
            "started_at": ready.started_wall,
            "duration_ms": int((time.monotonic() - ready.started_at) * 1000),
            "message": message,
            "counts": {
                "partitions": len(ready.plan.partition_keys),
                "inputs": len(ready.plan.inputs),
                "outputs": len(ready.outputs),
                "records": ready.plan.input_records,
                "input_bytes": ready.plan.input_bytes,
                "output_bytes": sum(int(item.get("file_size") or 0) for item in ready.outputs),
            },
        }
        if table is not None:
            # The replace changed the table's files and snapshot; its records stay the same.
            snapshot = table.current_snapshot()
            payload.update(
                {
                    "totals": table_totals(table),
                    "metadata_location": table.metadata_location,
                    "sequence_number": None if snapshot is None else snapshot.sequence_number,
                    "snapshots": len(table.metadata.snapshots),
                }
            )
        return self._completed_event(payload)

    # -- reprotect -------------------------------------------------------------------------------

    async def _start_reprotect(self) -> list[dict[str, Any]]:
        request = self.reprotect
        if request is None:
            return []
        loop = asyncio.get_running_loop()
        started_wall = _utc_now()
        try:
            table, plan, backlog = await loop.run_in_executor(_POOL, self._plan_reprotect)
        except Exception as error:  # noqa: BLE001 - planning reads the table; tried again after a backoff
            LOGGER.warning("Planning a reprotect of %s table %s failed: %s", self.target.materialization_id, self.target.table, error)
            return self._reprotect_failed(request, str(error), started_wall=started_wall)
        outputs = self._plaintext_event(backlog)
        if request.revision and backlog.revision < request.revision:
            # The edit asking for it has not reached the table yet.
            self.reprotect_wait_until = time.monotonic() + REPROTECT_WAIT_SECONDS
            return outputs
        if plan is None:
            if self._settling(table):
                # Nothing to rewrite yet; a batch open under the older revision may still commit some.
                self.reprotect_wait_until = time.monotonic() + REPROTECT_WAIT_SECONDS
                return outputs
            return outputs + await self._finish_reprotect(request, backlog, started_wall=started_wall, started_at=time.monotonic())
        work_dir = tempfile.mkdtemp(prefix=f"reprotect-{plan.job_id[:8]}-", dir=_work_root())
        job = _Job(
            kind="reprotect",
            started_at=time.monotonic(),
            started_wall=started_wall,
            work_dir=work_dir,
            reprotect_plan=plan,
            request=request,
            attempt=self.reprotect_failure.attempts + 1,
        )
        self.job = job
        self.started_here.add(job.started_wall)
        job_file = Path(work_dir) / "job.json"
        protection = TableProtection.from_json(protection_json_of(table.metadata))
        job_file.write_text(
            json.dumps(
                {
                    "plan": plan.to_dict(),
                    "schema": _schema_json(table.schema()),
                    "fork_map": fork_map_of(table.metadata).to_dict(),
                    "last_column_id": table.metadata.last_column_id,
                    "protection": protection.to_dict(),
                    "births": {str(field_id): revision for field_id, revision in field_births(table.metadata.schemas, protection).items()},
                    "work_dir": work_dir,
                    "location": self.target.location,
                    "granularity": granularity_of(table),
                }
            ),
            encoding="utf-8",
        )
        job.task = loop.create_task(self._run_reprotect(job, job_file))
        log_protocol_event(
            LOGGER,
            enabled=self.settings.protocol_logging_enabled,
            event="maintenance_reprotect_started",
            materialization_id=self.target.materialization_id,
            table=self.target.table,
            job_id=plan.job_id,
            revision=plan.revision,
            inputs=len(plan.inputs),
            input_bytes=plan.input_bytes,
            files_left=backlog.files,
            attempt=job.attempt,
        )
        return outputs + [self._started_event(job)]

    def _add_to_totals(self, ready: _ReprotectReady) -> None:
        totals_for, totals = self.reprotect_totals
        if totals_for != ready.request.request_id:
            totals = {}
        for key, value in _pass_counts(ready).items():
            totals[key] = totals.get(key, 0) + value
        self.reprotect_totals = (ready.request.request_id, totals)

    def _settling(self, table: Any) -> bool:
        """Whether a batch still open under an older revision may yet commit rows the table now masks.

        A writer's batch keeps the revision it opened under, and one that takes no more records seals
        at the batch ceiling: until that long after the table recorded its revision - and a margin to
        seal, upload and commit - files of the older revision may still arrive.
        """
        protection = TableProtection.from_json(protection_json_of(table.metadata))
        recorded = next((change.at for change in reversed(protection.history) if change.revision == protection.revision), None)
        at = _parse_time(recorded)
        if at is None:
            return False
        return _wall_clock() < at + self.settings.max_open_file_age_ceiling_seconds + REPROTECT_SETTLE_MARGIN_SECONDS

    def _plan_reprotect(self) -> tuple[Any, ReprotectPlan | None, ReprotectBacklog]:
        table = load_table(self.context, self.client)
        plan, backlog = plan_reprotect(table, max_input_bytes=self.settings.compaction_max_input_bytes)
        return table, plan, backlog

    async def _run_reprotect(self, job: _Job, job_file: Path) -> None:
        try:
            result = await type(self).reprotector(job_file, job)
            if self.job is not job:
                return
            self.ready = _ReprotectReady(
                plan=job.reprotect_plan,
                request=job.request,
                outputs=list(result.get("outputs") or []),
                counts=dict(result.get("counts") or {}),
                started_at=job.started_at,
                started_wall=job.started_wall,
            )
            self.job = None
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - counted as a failed attempt
            if self.job is job:
                self.job = None
                self.results.extend(self._reprotect_failed(job.request, str(error), started_wall=job.started_wall))
        finally:
            if job.work_dir:
                shutil.rmtree(job.work_dir, ignore_errors=True)

    async def _commit_reprotect_ready(self, ready: _ReprotectReady) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        try:
            table, snapshot_id = await loop.run_in_executor(_POOL, self._commit_reprotect_blocking, ready)
        except CompactionInputGone as error:
            # Compaction, expiry or an engine changed those files first: plan again at once.
            self._delete_outputs(ready)
            LOGGER.info("Abandoned reprotect %s of table %s: %s", ready.plan.job_id, self.target.table, error)
            return [self._reprotect_completed(ready.request, ready, snapshot_id=None, outcome="abandoned", message=str(error), done=False)]
        except Exception as error:  # noqa: BLE001 - CompactionMismatch, GCS errors, lost races
            self._delete_outputs(ready)
            return self._reprotect_failed(ready.request, str(error), started_wall=ready.started_wall)
        self.reprotect_failure = _Failure()
        self.reprotect_failure_for = ready.request.request_id
        self._add_to_totals(ready)
        try:
            _, more, backlog = await loop.run_in_executor(_POOL, self._plan_reprotect)
        except Exception as error:  # noqa: BLE001 - the next pass plans again
            LOGGER.warning("Counting what is left to reprotect in table %s failed: %s", self.target.table, error)
            more, backlog = ready.plan, None
        log_protocol_event(
            LOGGER,
            enabled=self.settings.protocol_logging_enabled,
            event="maintenance_reprotect_completed",
            materialization_id=self.target.materialization_id,
            table=self.target.table,
            job_id=ready.plan.job_id,
            revision=ready.plan.revision,
            inputs=len(ready.plan.inputs),
            records=ready.plan.input_records,
            masked=ready.counts.get("masked", 0),
            withheld=ready.counts.get("withheld", 0),
            snapshot_id=snapshot_id,
            files_left=None if backlog is None else backlog.files,
        )
        outputs = [] if backlog is None else self._plaintext_event(backlog)
        if more is not None or self._settling(table):
            # More to rewrite: the next pass starts at the next step. Or a batch open under the older
            # revision may still commit some: the next pass looks once the wait is over.
            if more is None:
                self.reprotect_wait_until = time.monotonic() + REPROTECT_WAIT_SECONDS
            return outputs + [self._reprotect_completed(ready.request, ready, snapshot_id=snapshot_id, outcome="succeeded", table=table, done=False, backlog=backlog)]
        return outputs + await self._finish_reprotect(
            ready.request, backlog, started_wall=ready.started_wall, started_at=ready.started_at, ready=ready, snapshot_id=snapshot_id, table=table
        )

    def _commit_reprotect_blocking(self, ready: _ReprotectReady):
        client = self.client or shared_storage_client()
        inputs: dict[str, LiveFile] = {item.path: item for item in ready.plan.inputs}
        last_error: BaseException | None = None
        for _ in range(REPLACE_ATTEMPTS):
            table = load_table(self.context, self.client)
            granularity = granularity_of(table)
            rewritten = []
            for output in ready.outputs:
                footer, size = read_parquet_footer(client, output["uri"])
                data_file = build_data_file(
                    table.metadata,
                    file_path=output["uri"],
                    file_size=size,
                    parquet_metadata=footer,
                    granularity=granularity,
                    partition=Record(*output["partition"]),
                )
                rewritten.append((inputs[output["input"]], data_file))
            try:
                snapshot_id = commit_reprotect(table, ready.plan, rewritten, runtime=self.app.app_name)
            except CommitFailedException as error:
                last_error = error
                time.sleep(REPLACE_RETRY_SECONDS)
                continue
            return load_table(self.context, self.client), snapshot_id
        raise RuntimeError(f"the replace commit kept losing to other commits: {last_error}")

    async def _finish_reprotect(
        self,
        request: ReprotectRequest,
        backlog: ReprotectBacklog | None,
        *,
        started_wall: str,
        started_at: float,
        ready: _ReprotectReady | None = None,
        snapshot_id: int | None = None,
        table: Any = None,
    ) -> list[dict[str, Any]]:
        """Nothing is left to rewrite: purge if asked, and report the request done."""
        purge: PurgeResult | None = None
        message = None
        purged_table = None
        if request.purge:
            try:
                purge = await asyncio.get_running_loop().run_in_executor(_POOL, self._purge_blocking)
                purged_table = await asyncio.get_running_loop().run_in_executor(_POOL, load_table, self.context, self.client)
            except Exception as error:  # noqa: BLE001 - the rewrite stands; expiry and orphan cleanup finish the job
                LOGGER.warning("Purging table %s after a reprotect failed: %s", self.target.table, error)
                message = f"Every file is rewritten, but the purge failed: {error}. Snapshot expiry and orphan cleanup remove the old files in time."
        self.reprotect_done.add(request.request_id)
        if message is None and backlog is not None and (backlog.blocked_files or backlog.unknown_files):
            parts = []
            if backlog.blocked_files:
                parts.append(f"{backlog.blocked_files} file(s) cannot be rewritten yet: " + "; ".join(backlog.blocked))
            if backlog.unknown_files:
                parts.append(f"{backlog.unknown_files} file(s) were written under revisions whose masking the table's record no longer says")
            message = ". ".join(parts) + "."
        # A purge alone - an earlier run rewrote every file without one - still removes what the
        # older snapshots hold.
        purged = purge is not None and bool(purge.expired or purge.deleted)
        totals_for, totals = self.reprotect_totals
        rewrote = ready is not None or (totals_for == request.request_id and bool(totals.get("inputs")))
        if message is None and ready is None:
            message = ("No file holds a value the table's protection masks in plain text any more" if rewrote else "No file holds a value the table's protection masks in plain text") + (
                f"; {purge.expired} older snapshot(s) expired and {purge.deleted} file(s) deleted." if purged else "."
            )
        return [
            self._reprotect_completed(
                request,
                ready,
                snapshot_id=snapshot_id,
                outcome="succeeded" if rewrote or purged else "skipped",
                message=message,
                table=table,
                done=True,
                backlog=backlog,
                purge=purge,
                purged_table=purged_table,
                started_wall=started_wall,
                started_at=started_at,
            )
        ]

    def _purge_blocking(self) -> PurgeResult:
        client = self.client or shared_storage_client()
        last_error: BaseException | None = None
        for _ in range(REPLACE_ATTEMPTS):
            table = load_table(self.context, self.client)
            if table.metadata.current_snapshot_id is None:
                return PurgeResult()
            try:
                return purge_before(
                    lambda: load_table(self.context, self.client),
                    table.metadata.current_snapshot_id,
                    client=client,
                    now=datetime.now(tz=UTC),
                    orphan_min_age_hours=self.settings.orphan_min_age_hours,
                    delete=lambda uri: delete_object(client, uri),
                )
            except CommitFailedException as error:
                last_error = error
                time.sleep(REPLACE_RETRY_SECONDS)
        raise RuntimeError(f"the purge's expiry kept losing to other commits: {last_error}")

    def _reprotect_completed(
        self,
        request: ReprotectRequest | None,
        ready: _ReprotectReady | None,
        *,
        snapshot_id: int | None,
        outcome: str,
        done: bool,
        message: str | None = None,
        table: Any = None,
        backlog: ReprotectBacklog | None = None,
        purge: PurgeResult | None = None,
        purged_table: Any = None,
        started_wall: str | None = None,
        started_at: float | None = None,
    ) -> dict[str, Any]:
        counts: dict[str, Any] = {}
        if ready is not None:
            counts.update(_pass_counts(ready))
        totals_for, totals = self.reprotect_totals
        if done and request is not None and totals_for == request.request_id and totals:
            # The last report says what the whole request rewrote, every pass of it.
            counts.update(totals)
        if backlog is not None:
            counts.update({"files_left": backlog.files, "blocked_files": backlog.blocked_files, "unknown_files": backlog.unknown_files})
        if purge is not None:
            counts.update({"expired_snapshots": purge.expired, "deleted_files": purge.deleted})
        started = started_at if started_at is not None else (ready.started_at if ready is not None else time.monotonic())
        payload: dict[str, Any] = {
            "job": "reprotect",
            "outcome": outcome,
            "request_id": None if request is None else request.request_id,
            "done": done,
            "revision": None if ready is None else ready.plan.revision,
            "purged": purge is not None,
            "snapshot_id": snapshot_id,
            "started_at": started_wall or (None if ready is None else ready.started_wall),
            "duration_ms": int((time.monotonic() - started) * 1000),
            "message": message,
            "counts": counts,
        }
        if table is not None and outcome == "succeeded" and snapshot_id is not None:
            payload.update({"totals": table_totals(table), **_metadata_state(table)})
        if purged_table is not None:
            # The purge's expiry commits metadata without a snapshot: the record takes it as it is now.
            payload.update(_metadata_state(purged_table))
        return self._completed_event(payload)

    def _reprotect_failed(self, request: ReprotectRequest | None, message: str, *, started_wall: str | None = None) -> list[dict[str, Any]]:
        config = self.settings
        failure = self.reprotect_failure
        failure.attempts += 1
        backoff = min(COMPACTION_BACKOFF_SECONDS[0] * 2 ** (failure.attempts - 1), COMPACTION_BACKOFF_SECONDS[1])
        failure.next_at = time.monotonic() + backoff
        give_up = failure.attempts >= config.compaction_max_attempts
        LOGGER.warning(
            "Reprotect of %s table %s failed (attempt %s): %s",
            self.target.materialization_id,
            self.target.table,
            failure.attempts,
            message,
        )
        log_protocol_event(
            LOGGER,
            enabled=config.protocol_logging_enabled,
            event="maintenance_reprotect_failed",
            materialization_id=self.target.materialization_id,
            table=self.target.table,
            attempt=failure.attempts,
            retry_in_seconds=None if give_up else backoff,
            error=message,
        )
        outputs = [
            self._completed_event(
                {
                    "job": "reprotect",
                    "outcome": "failed",
                    "request_id": None if request is None else request.request_id,
                    "done": give_up,
                    "started_at": started_wall,
                    "message": message if give_up else f"{message} (tried again in {backoff // 60} minutes)",
                    "attempt": failure.attempts,
                    "counts": {},
                }
            )
        ]
        if give_up and not failure.gave_up:
            failure.gave_up = True
            if request is not None:
                self.reprotect_done.add(request.request_id)
            outputs.append(
                self._event(
                    "table_error",
                    {
                        "reason": "reprotect_failed",
                        "message": f"Masking the rows already written failed {failure.attempts} times and was given up: {message}",
                    },
                )
            )
        return outputs

    # -- plaintext -------------------------------------------------------------------------------

    async def _count_plaintext(self) -> list[dict[str, Any]]:
        try:
            _, _, backlog = await asyncio.get_running_loop().run_in_executor(_POOL, self._plan_reprotect)
        except Exception as error:  # noqa: BLE001 - counted again at the next compaction plan
            LOGGER.warning("Counting what table %s holds in plain text failed: %s", self.target.table, error)
            return []
        return self._plaintext_event(backlog)

    def _plaintext_event(self, backlog: ReprotectBacklog | None) -> list[dict[str, Any]]:
        """Report what the table holds in plain text when it changed since the last report."""
        if backlog is None:
            return []
        seen = (backlog.revision, backlog.files, backlog.rows, backlog.bytes, backlog.blocked_files, backlog.blocked, backlog.unknown_files)
        if seen == self.plaintext_reported:
            return []
        if self.plaintext_reported is None and backlog.revision <= 0:
            # A table that never masked anything: nothing to report.
            return []
        self.plaintext_reported = seen
        return [self._event("plaintext_counted", {**backlog.to_dict(), "counted_at": _utc_now()})]

    # -- expiry ----------------------------------------------------------------------------------

    async def _expire(self, *, requested: bool = False) -> list[dict[str, Any]]:
        started = time.monotonic()
        try:
            table, expired = await asyncio.get_running_loop().run_in_executor(_POOL, self._expire_blocking)
        except Exception as error:  # noqa: BLE001 - retried at the next interval
            LOGGER.warning("Expiring snapshots of %s table %s failed: %s", self.target.materialization_id, self.target.table, error)
            return [self._completed_event({"job": "expire_snapshots", "outcome": "failed", "message": str(error), "counts": {}})]
        log_protocol_event(
            LOGGER,
            enabled=self.settings.protocol_logging_enabled,
            event="maintenance_snapshot_expiry_completed",
            materialization_id=self.target.materialization_id,
            table=self.target.table,
            expired=expired,
            remaining=len(table.metadata.snapshots),
        )
        if not expired and not requested:
            return []
        return [
            self._completed_event(
                {
                    "job": "expire_snapshots",
                    "outcome": "succeeded" if expired else "skipped",
                    "message": None if expired else "No snapshot is past its retention.",
                    "duration_ms": int((time.monotonic() - started) * 1000),
                    "counts": {"expired": expired, "remaining": len(table.metadata.snapshots)},
                    # Metadata without a snapshot: the record takes it at the same sequence number.
                    **(_metadata_state(table) if expired else {}),
                }
            )
        ]

    def _expire_blocking(self):
        config = self.settings
        for _ in range(REPLACE_ATTEMPTS):
            table = load_table(self.context, self.client)
            ids = plan_expiry(
                table.metadata,
                now=datetime.now(tz=UTC),
                retention_hours=config.snapshot_retention_hours,
                min_snapshots_to_keep=config.min_snapshots_to_keep,
                keep_all_minutes=config.snapshot_keep_all_minutes,
            )
            if not ids:
                return table, 0
            try:
                expired = expire_snapshots(table, ids)
            except CommitFailedException:
                time.sleep(REPLACE_RETRY_SECONDS)
                continue
            return load_table(self.context, self.client), expired
        raise RuntimeError("snapshot expiry kept losing to other commits")

    # -- orphan cleanup --------------------------------------------------------------------------

    def _start_orphan_cleanup(self) -> list[dict[str, Any]]:
        job = _Job(kind="orphan_cleanup", started_at=time.monotonic(), started_wall=_utc_now())
        self.job = job
        self.started_here.add(job.started_wall)

        async def run() -> None:
            try:
                deleted, listed = await asyncio.get_running_loop().run_in_executor(_POOL, self._orphans_blocking)
            except Exception as error:  # noqa: BLE001 - retried at the next interval
                LOGGER.warning("Orphan cleanup of %s table %s failed: %s", self.target.materialization_id, self.target.table, error)
                if self.job is job:
                    self.results.append(
                        self._completed_event(
                            {"job": "orphan_cleanup", "outcome": "failed", "started_at": job.started_wall, "message": str(error), "counts": {}}
                        )
                    )
            else:
                log_protocol_event(
                    LOGGER,
                    enabled=self.settings.protocol_logging_enabled,
                    event="maintenance_orphan_cleanup_completed",
                    materialization_id=self.target.materialization_id,
                    table=self.target.table,
                    deleted=deleted,
                    listed=listed,
                )
                if self.job is job:
                    self.results.append(
                        self._completed_event(
                            {
                                "job": "orphan_cleanup",
                                "outcome": "succeeded",
                                "started_at": job.started_wall,
                                "duration_ms": int((time.monotonic() - job.started_at) * 1000),
                                "counts": {"deleted": deleted, "listed": listed},
                            }
                        )
                    )
            finally:
                if self.job is job:
                    self.job = None

        job.task = asyncio.get_running_loop().create_task(run())
        return [self._started_event(job)]

    def _orphans_blocking(self) -> tuple[int, int]:
        config = self.settings
        client = self.client or shared_storage_client()
        table = load_table(self.context, self.client)
        scan = find_orphans(
            client,
            self.target.location,
            referenced_files(table),
            now=datetime.now(tz=UTC),
            min_age_hours=config.orphan_min_age_hours,
            limit=ORPHAN_PASS_LIMIT,
        )
        deleted = 0
        for uri in scan.candidates:
            if delete_object(client, uri):
                deleted += 1
        if scan.truncated:
            # More to do: the next pass comes at the next step rather than a whole interval later.
            self.last_runs.pop("orphan_cleanup", None)
        return deleted, scan.listed

    # -- events ----------------------------------------------------------------------------------

    def _event(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = {
            "event_id": f"evt-table-{kind}-{uuid.uuid4().hex[:16]}",
            "kind": kind,
            "materialization_id": self.target.materialization_id,
            "table": self.target.table,
            "occurred_at": _utc_now(),
            "runtime_app_ref": self.app.app_name,
            "member_id": current_member_id(),
            "payload": payload,
        }
        return {"route": "events", "key": materialization_routing_key(self.target.materialization_id), "value": event}

    def _started_event(self, job: _Job) -> dict[str, Any]:
        return self._event(
            "maintenance_started",
            {"job": job.kind, "started_at": job.started_wall, "partition": job.partition, "attempt": job.attempt},
        )

    def _completed_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._event("maintenance_completed", payload)


class MaintenanceWorker:
    """Every table this member maintains, sharing its job slots between them, each in its turn.

    Every step finishes what is done on every table. Then, while a slot is free, it starts the next
    job: a table someone asked for first, then the tables in rotation from where the last step
    stopped. Quick work - a snapshot expiry, a compaction plan that finds nothing - takes no slot, so
    the visit goes on to the next table until the slots are full, every table has been visited, or
    `STEP_BUDGET_SECONDS` of quick work has gone by; the next step carries on from there.
    """

    #: Quick maintenance work one step does across tables before it leaves the rest to the next.
    STEP_BUDGET_SECONDS = 10.0

    def __init__(self, *, client: Any = None, app: MaintenanceAppConfig | None = None) -> None:
        self.client = client
        self.app = app
        self.tables: dict[str, TableMaintenance] = {}
        self._rotation = 0

    def config(self) -> MaintenanceAppConfig:
        return self.app or get_maintenance_app_config()

    def handle(self, message: MaintenanceMessage) -> list[dict[str, Any]]:
        """Take one message from the control plane; return the table events it settles at once."""
        key = message.key
        if message.kind == "maintenance_cancel":
            table = self.tables.pop(key, None)
            if table is not None:
                return table.cancel(reason=message.reason or "cancelled")
            if message.running is not None:
                # Nothing runs here, yet the record says something does: report it over.
                return TableMaintenance(message.target, job_settings(message.settings), client=self.client, app=self.config())._settle_reported(message.running)
            return []
        table = self.tables.get(key)
        if table is None:
            table = self.tables[key] = TableMaintenance(
                message.target,
                job_settings(message.settings),
                client=self.client,
                app=self.config(),
            )
        if message.kind == "maintenance_request":
            table.request(message)
            return []
        return table.observe(message)

    async def step(self, *, should_stop: bool = False) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []
        if should_stop:
            self.stop()
            return outputs
        self._forget_stale(outputs)
        tables = list(self.tables.values())
        for table in tables:
            outputs.extend(await table.step())
        if not tables:
            return outputs
        # A table that is busy runs a compaction or an orphan cleanup: each holds a slot.
        free = self.config().max_concurrent_jobs - sum(1 for table in tables if table.busy)
        if free <= 0:
            return outputs
        start = self._rotation % len(tables)
        rotation = tables[start:] + tables[:start]
        asked = [table for table in rotation if table.asked]
        order = asked + [table for table in rotation if table not in asked]
        began = time.monotonic()
        visited = None
        for table in order:
            if table.busy:
                continue
            visited = table
            outputs.extend(await table.start_next())
            if table.busy:
                free -= 1
            if free <= 0 or time.monotonic() - began >= self.STEP_BUDGET_SECONDS:
                break
        if visited is not None:
            self._rotation = (tables.index(visited) + 1) % len(tables)
        return outputs

    def running_jobs(self) -> int:
        return sum(1 for table in self.tables.values() if table.busy)

    def stop(self) -> None:
        for table in self.tables.values():
            table.stop()

    def _forget_stale(self, outputs: list[dict[str, Any]]) -> None:
        """Forget the tables no tick has named lately, stopping their jobs: another member has them."""
        limit = self.config().forget_after_seconds
        now = time.monotonic()
        for key, table in list(self.tables.items()):
            if now - table.last_seen < limit:
                continue
            del self.tables[key]
            outputs.extend(table.cancel(reason="no_longer_ticked"))


_WORKER: MaintenanceWorker | None = None


def get_worker() -> MaintenanceWorker:
    """This member's worker: the agent hands it messages, the step task drives it."""
    global _WORKER
    if _WORKER is None:
        _WORKER = MaintenanceWorker()
    return _WORKER


def reset_worker(worker: MaintenanceWorker | None = None) -> None:
    """Stop every job and start afresh. Tests use this between cases."""
    global _WORKER
    if _WORKER is not None:
        _WORKER.stop()
    _WORKER = worker


def current_member_id() -> str:
    """This member's name: its pod's, as the manifest sets `RUNTIME_MEMBER_ID`."""
    from midstream.runtime_app.writer.context import current_member_id as member_id

    return member_id()


def _kill(job: _Job) -> None:
    process = job.process
    if process is not None and getattr(process, "returncode", 0) is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    if job.task is not None and not job.task.done():
        job.task.cancel()
    if job.work_dir:
        shutil.rmtree(job.work_dir, ignore_errors=True)


def _work_root() -> str:
    """The compaction work root, made if need be: the worker refuses a work directory outside it."""
    from midstream.maintenance_app.compact_worker import compaction_work_root

    root = compaction_work_root()
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


def _metadata_state(table: Any) -> dict[str, Any]:
    """What a table record keeps of the table's metadata, as it is now."""
    snapshot = table.current_snapshot()
    return {
        "metadata_location": table.metadata_location,
        "sequence_number": None if snapshot is None else snapshot.sequence_number,
        "snapshots": len(table.metadata.snapshots),
    }


def _pass_counts(ready: _ReprotectReady) -> dict[str, int]:
    """What one reprotect pass rewrote."""
    return {
        "inputs": len(ready.plan.inputs),
        "outputs": len(ready.outputs),
        "records": ready.plan.input_records,
        "input_bytes": ready.plan.input_bytes,
        "output_bytes": sum(int(item.get("file_size") or 0) for item in ready.outputs),
        "masked": int(ready.counts.get("masked", 0)),
        "withheld": int(ready.counts.get("withheld", 0)),
        "moved": int(ready.counts.get("moved", 0)),
    }


def _wall_clock() -> float:
    """Now, as a table's history records times: what a reprotect's settle check reads."""
    return time.time()


def _parse_time(value: str | None) -> float | None:
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.timestamp()


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


__all__ = ["MaintenanceWorker", "TableMaintenance", "get_worker", "job_settings", "reset_worker"]
