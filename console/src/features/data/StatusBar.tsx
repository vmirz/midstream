import { AlertTriangle, Ban, CheckCircle2, Hourglass, Loader2, RotateCw } from 'lucide-react';
import type { ReactNode } from 'react';

import { newerSnapshot, type TailNotice } from './live';
import { cn } from '@/lib/cn';
import { formatDuration, formatExact } from '@/lib/format';
import type { DataSummary, RunSnapshot } from '@shared/types';

/**
 * What the run on screen is doing, or what it found, in a few plain words.
 *
 * While it works: how far through it is, and roughly how long is left; the toolbar's Stop ends it.
 * The figures are exact for the structured view — rows of the files read so far, out of every row
 * the time range selected, both from the table's manifests — so the bar moves at the rate work is
 * done.
 *
 * When it finishes: the exact number of rows, and nothing the reader has to decode — not the
 * files, the snapshot or the range, which the toolbar already shows. It is a row of its own at the
 * top of the table. Live gets a muted word here when it is paused or slow, and how far behind the
 * newest records the view is, or why Live stopped, in amber at the row's right (`LiveWarning`).
 * Peek, Share and Export are under More actions, in the toolbar.
 */
export function StatusBar({
  run,
  starting,
  summary,
  dirty,
  onRerun,
  tailNotice = null,
  warning = null,
}: {
  run: RunSnapshot | null;
  starting: boolean;
  summary: DataSummary | null;
  /** The controls describe a different query than the one on screen. */
  dirty: boolean;
  onRerun: () => void;
  /** A muted word on Live (`live.ts::tailNotice`). */
  tailNotice?: TailNotice | null;
  /** How far behind the newest records the view is, or why Live stopped: Live's warning, kept while a run Live started works. */
  warning?: TailNotice | null;
}): ReactNode {
  const running = starting || run?.state === 'running';

  return (
    <div className="relative border-b border-[var(--color-line)] bg-[var(--color-surface)]" data-role="status-line">
      <div className="flex min-h-[40px] flex-wrap items-center gap-x-3 gap-y-1 px-3 py-1.5">
        <div className="flex min-w-0 flex-1 items-center gap-2" aria-live="polite" data-role="run-status">
          {starting || !run ? (
            <>
              <Loader2 className="size-4 shrink-0 animate-spin text-[var(--color-accent)]" />
              <span className="text-[12px] text-[var(--color-ink-muted)]">Starting…</span>
            </>
          ) : (
            <RunSummary run={run} />
          )}
        </div>

        <div className="flex shrink-0 items-center gap-1.5">
          {run && run.state !== 'running' && tailNotice ? (
            <span className="numeric truncate text-[11px] text-[var(--color-ink-muted)]" title={tailNotice.detail ?? undefined} data-role="tail-status">
              {tailNotice.text}
            </span>
          ) : null}
          {dirty && !running ? (
            <span className="rounded-full bg-[var(--color-accent-soft)] px-2 py-0.5 text-[11px] font-medium text-[var(--color-accent)]">
              Changes not run yet
            </span>
          ) : null}
          {!dirty && run?.state === 'succeeded' && hasNewerData(run, summary) ? (
            <button
              type="button"
              onClick={onRerun}
              className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] text-[var(--color-accent)] hover:bg-[var(--color-accent-soft)]"
              title="New rows have arrived since this result was read. Run again to include them."
            >
              <RotateCw className="size-3" />
              Newer data available
            </button>
          ) : null}
          {warning ? <LiveWarning notice={warning} /> : null}
        </div>
      </div>
      {run?.state === 'running' ? <ProgressBar run={run} /> : null}
    </div>
  );
}

/** How far behind the newest records the view is, or why Live stopped: at the status line's right, in Live's amber. */
function LiveWarning({ notice }: { notice: TailNotice }): ReactNode {
  const Icon = notice.behind ? Hourglass : AlertTriangle;
  return (
    <span
      role="status"
      data-role="live-notice"
      title={notice.detail ?? undefined}
      className="inline-flex min-w-0 items-center gap-1.5 text-[12px] font-medium text-[var(--color-phase-paused)]"
    >
      <Icon className={cn('size-3.5 shrink-0', notice.behind && 'hourglass-turn')} aria-hidden />
      <span className="numeric truncate">{notice.text}</span>
    </span>
  );
}

function RunSummary({ run }: { run: RunSnapshot }): ReactNode {
  if (run.state === 'running') {
    return (
      <>
        <Loader2 className="size-4 shrink-0 animate-spin text-[var(--color-accent)]" />
        <span className="numeric min-w-0 truncate text-[12px] text-[var(--color-ink)]">
          {runningText(run)}
        </span>
        <span className="numeric shrink-0 text-[11px] text-[var(--color-ink-faint)]">
          {formatDuration(run.elapsedMs)}
          {remaining(run)}
        </span>
      </>
    );
  }

  if (run.state === 'failed') {
    return (
      <>
        <AlertTriangle className="size-4 shrink-0 text-[var(--color-phase-failed)]" />
        <span className="numeric min-w-0 truncate text-[12px] text-[var(--color-phase-failed)]" title={run.error?.message}>
          {run.error?.message ?? 'The run failed'}
        </span>
      </>
    );
  }

  if (run.state === 'cancelled') {
    return (
      <>
        <Ban className="size-4 shrink-0 text-[var(--color-ink-faint)]" />
        <span className="numeric text-[12px] text-[var(--color-ink-muted)]">
          Cancelled after {formatDuration(run.elapsedMs)}
          {run.rowsReady > 0 ? ` · ${formatExact(run.rowsReady)} rows found before stopping` : ''}
        </span>
      </>
    );
  }

  return (
    <>
      <CheckCircle2 className="size-4 shrink-0 text-[var(--color-phase-running)]" />
      <span className="numeric shrink-0 text-[13px] font-semibold text-[var(--color-ink)]" title={`Took ${formatDuration(run.elapsedMs)}`}>
        {formatExact(run.rows)} {run.rows === 1 ? 'row' : 'rows'}
      </span>
      {run.truncated && run.rowsRetained !== null ? (
        <span className="numeric shrink-0 text-[11px] text-[var(--color-phase-paused)]" title="Only the first rows of a large result are kept for paging and export; the total is exact.">
          first {formatExact(run.rowsRetained)} kept
        </span>
      ) : null}
      {readAcross(run)}
    </>
  );
}

/** For a statement that read tables of several domains, which domains: each table is its domain's. */
function readAcross(run: RunSnapshot): ReactNode {
  const domains = [...new Set(run.tables.map((table) => table.domain))];
  if (run.request.kind !== 'sql' || domains.length < 2) return null;
  return (
    <span
      className="numeric min-w-0 truncate text-[11px] text-[var(--color-ink-muted)]"
      title={run.tables.map((table) => `${table.domain}.${table.dataset}.${table.name}`).join('\n')}
    >
      across {domains.join(', ')}
    </span>
  );
}

function runningText(run: RunSnapshot): string {
  const { progress } = run;
  if (run.phase === 'planning') return 'Preparing';
  if (run.phase === 'preparing') {
    return progress.objectsTotal > 0 ? `Preparing · ${Math.floor((progress.objectsDone / progress.objectsTotal) * 100)}%` : 'Preparing';
  }
  if (run.phase === 'counting') return 'Counting rows';
  if (run.request.kind === 'sql') {
    return progress.fraction === null ? 'Running' : `Running · ${Math.floor(progress.fraction * 100)}%`;
  }
  const percent = progress.fraction === null ? '' : ` · ${Math.floor(progress.fraction * 100)}%`;
  return (
    `Scanning · ${formatExact(progress.rowsDone)} of ${formatExact(progress.rowsTotal)} rows${percent}` +
    ` · ${formatExact(run.rows)} matched so far`
  );
}

/** Time left, extrapolated from the rate so far; only once there is a rate worth trusting. */
function remaining(run: RunSnapshot): string {
  const fraction = run.progress.fraction;
  if (fraction === null || fraction < 0.03 || fraction >= 1 || run.elapsedMs < 1_500) return '';
  const left = (run.elapsedMs * (1 - fraction)) / fraction;
  return ` · ~${formatDuration(Math.max(1_000, left))} left`;
}

function ProgressBar({ run }: { run: RunSnapshot }): ReactNode {
  const fraction = run.progress.fraction;
  return (
    <div className="absolute inset-x-0 bottom-0 h-0.5 overflow-hidden bg-transparent" aria-hidden>
      <div
        className={cn(
          'h-full bg-[var(--color-accent)] transition-[width] duration-300 ease-out',
          // A phase that cannot say how far it has got pulses rather than inventing a width.
          fraction === null && 'w-1/4 animate-pulse',
        )}
        style={fraction === null ? undefined : { width: `${Math.max(2, Math.round(fraction * 100))}%` }}
      />
    </div>
  );
}

/** A newer snapshot of the table on screen than the one the run read, which only a run following the table can miss. */
function hasNewerData(run: RunSnapshot, summary: DataSummary | null): boolean {
  if (run.pinned || !summary?.snapshotId) return false;
  return newerSnapshot(
    run.tables.find((table) => table.materializationId === summary.materializationId && table.name === summary.table),
    summary,
  );
}
