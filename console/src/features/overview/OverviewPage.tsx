import { AlertTriangle, Plus } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { CreateMaterializationDialog } from '../materializations/CreateMaterializationDialog';
import { LiveIndicator, PageBody, PageHeader } from '@/components/AppShell';
import { PhaseBadge } from '@/components/PhaseBadge';
import { DistributionBar } from '@/components/Sparkline';
import { StatCard } from '@/components/StatCard';
import { TimeSeriesChart } from '@/components/TimeSeriesChart';
import { Button, EmptyState, Panel, PanelHeader, Skeleton } from '@/components/ui/primitives';
import { RelativeTime } from '@/components/Inline';
import {
  displayName,
  formatBytes,
  formatBytesRate,
  formatCount,
  formatRate,
  humanize,
  topicsLabel,
} from '@/lib/format';
import { useCan } from '@/lib/auth';
import { useFleetStream } from '@/lib/live';
import { isDeletedPhase, type MaterializationSummary } from '@shared/types';

const ATTENTION_PHASES = new Set(['failed', 'degraded']);

export function OverviewPage(): ReactNode {
  const { data, status } = useFleetStream();
  const [createOpen, setCreateOpen] = useState(false);
  const canCreate = useCan('materializations.create');

  // Deleted materializations remain in the fleet payload for the recently-deleted view,
  // so every overview panel works off the active subset.
  const activeItems = useMemo(
    () => (data?.items ?? []).filter((item) => !isDeletedPhase(item)),
    [data?.items],
  );

  const rowSeries = useMemo(
    () => (data?.series ?? []).map((point) => ({ t: point.t, value: point.rowsPerSecond })),
    [data?.series],
  );
  const byteSeries = useMemo(
    () => (data?.series ?? []).map((point) => ({ t: point.t, value: point.bytesPerSecond })),
    [data?.series],
  );

  const attention = useMemo(
    () =>
      activeItems.filter(
        (item) =>
          ATTENTION_PHASES.has(item.observed_phase) ||
          item.runtimeHealth === 'degraded' ||
          item.runtimeHealth === 'unreachable' ||
          item.desired_phase !== item.observed_phase,
      ),
    [activeItems],
  );

  const busiest = useMemo(
    () =>
      [...activeItems]
        .filter((item) => (item.rowsPerSecond ?? 0) > 0 || (item.rowsCommitted ?? 0) > 0)
        .sort((a, b) => (b.rowsPerSecond ?? 0) - (a.rowsPerSecond ?? 0) || (b.rowsCommitted ?? 0) - (a.rowsCommitted ?? 0))
        .slice(0, 8),
    [activeItems],
  );

  const totals = data?.totals;

  return (
    <>
      <PageHeader
        title="Overview"
        subtitle="Fleet-wide health and throughput across every materialization."
        meta={
          <LiveIndicator
            status={status}
            generatedAt={data?.generatedAt}
            error={data?.controlPlaneError ?? null}
          />
        }
        actions={
          canCreate ? (
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              <Plus className="size-4" />
              New materialization
            </Button>
          ) : null
        }
      />

      <PageBody className="space-y-6">
        {!data ? (
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            {Array.from({ length: 4 }, (_, index) => (
              <Skeleton key={index} className="h-[132px]" />
            ))}
          </div>
        ) : (
          <>
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
              <StatCard
                label="Materializations"
                value={formatCount(totals?.materializations ?? 0)}
                hint={
                  <span className="space-y-2">
                    <DistributionBar
                      className="w-full"
                      segments={[
                        { label: 'Running', value: totals?.running ?? 0, color: 'var(--color-phase-running)' },
                        { label: 'Pending', value: totals?.pending ?? 0, color: 'var(--color-phase-pending)' },
                        { label: 'Stopped', value: totals?.stopped ?? 0, color: 'var(--color-phase-paused)' },
                        { label: 'Failing', value: totals?.failing ?? 0, color: 'var(--color-phase-failed)' },
                      ]}
                    />
                    <span className="block">
                      {totals?.running ?? 0} running · {totals?.pending ?? 0} pending ·{' '}
                      {totals?.stopped ?? 0} stopped · {totals?.failing ?? 0} failing
                    </span>
                  </span>
                }
              />
              <StatCard
                label="Ingest rate"
                value={formatRate(totals?.rowsPerSecond ?? null, '')}
                unit="rows/s"
                hint="Every runtime member · 1-min window"
                series={rowSeries}
                color="var(--color-accent)"
              />
              <StatCard
                label="Write rate"
                value={formatBytesRate(totals?.bytesPerSecond ?? null)}
                hint="Parquet · 15-min trend"
                series={byteSeries}
                color="var(--color-phase-running)"
              />
              <StatCard
                label="Materialized"
                value={formatCount(totals?.rowsCommitted ?? 0)}
                unit="rows"
                hint={`${formatBytes(totals?.tableBytes ?? 0)} · ${formatCount(totals?.tableFiles ?? 0)} parquet files · ${formatCount(totals?.recordsSkipped ?? 0)} skipped`}
              />
            </div>

            <div className="grid gap-6 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
              <Panel>
                <PanelHeader
                  title="Fleet throughput"
                  description="Rows per second across every materialization, measured per runtime member between its own heartbeats."
                />
                <div className="px-3 pt-3 pb-2">
                  <TimeSeriesChart
                    points={rowSeries}
                    from={Math.max(data.series[0]?.t ?? 0, (Date.parse(data.generatedAt) || Date.now()) - 30 * 60_000)}
                    to={Date.parse(data.generatedAt) || Date.now()}
                    height={190}
                    formatValue={(value) => formatRate(value, 'rows/s')}
                    formatTick={formatCount}
                    label="Fleet ingest rate"
                    empty="Collecting — rates appear once every runtime member has reported twice."
                  />
                </div>
              </Panel>

              <Panel className="overflow-hidden">
                <PanelHeader
                  title="Needs attention"
                  description={
                    attention.length === 0
                      ? 'Everything is converged.'
                      : `${attention.length} materialization${attention.length === 1 ? '' : 's'} not in a steady state.`
                  }
                />
                {attention.length === 0 ? (
                  <EmptyState
                    title="All clear"
                    description="No failures, degraded runtimes, or pending reconciliation."
                    className="py-12"
                  />
                ) : (
                  <ul className="divide-y divide-[var(--color-line)]">
                    {attention.slice(0, 6).map((item) => (
                      <AttentionRow key={item.materialization_id} item={item} />
                    ))}
                  </ul>
                )}
              </Panel>
            </div>

            <Panel className="overflow-hidden">
              <PanelHeader
                title="Busiest materializations"
                description="Ranked by current ingest rate."
                actions={
                  <Link
                    to="/materializations"
                    className="text-[12px] font-medium text-[var(--color-accent)] hover:underline"
                  >
                    View all
                  </Link>
                }
              />
              {busiest.length === 0 ? (
                <EmptyState
                  title="No throughput yet"
                  description="Runtimes report progress once they commit their first parquet batch."
                  className="py-12"
                />
              ) : (
                <ul className="divide-y divide-[var(--color-line)]">
                  {busiest.map((item) => (
                    <li key={item.materialization_id}>
                      <Link
                        to={`/materializations/${item.materialization_id}`}
                        className="grid grid-cols-[minmax(0,2fr)_repeat(3,minmax(0,1fr))] items-center gap-4 px-5 py-3 transition-colors hover:bg-[var(--color-raised)]"
                      >
                        <div className="min-w-0">
                          <p className="truncate text-[13px] font-medium">{displayName(item)}</p>
                          <p className="truncate text-[12px] text-[var(--color-ink-muted)]" title={item.source_topics.join('\n')}>
                            {topicsLabel(item.source_topics, 2)}
                          </p>
                        </div>
                        <span className="numeric text-right text-[13px]">
                          {formatRate(item.rowsPerSecond, 'rows/s')}
                        </span>
                        <span className="numeric text-right text-[13px] text-[var(--color-ink-muted)]">
                          {formatBytesRate(item.bytesPerSecond)}
                        </span>
                        <span className="numeric text-right text-[13px] text-[var(--color-ink-muted)]">
                          {formatCount(item.rowsCommitted)} rows
                        </span>
                      </Link>
                    </li>
                  ))}
                </ul>
              )}
            </Panel>
          </>
        )}
      </PageBody>

      {canCreate ? (
        <CreateMaterializationDialog
          open={createOpen}
          onClose={() => setCreateOpen(false)}
          materializations={data?.items}
        />
      ) : null}
    </>
  );
}

function AttentionRow({ item }: { item: MaterializationSummary }): ReactNode {
  return (
    <li>
      <Link
        to={`/materializations/${item.materialization_id}`}
        className="flex items-center gap-3 px-5 py-3 transition-colors hover:bg-[var(--color-raised)]"
      >
        <AlertTriangle className="size-4 shrink-0 text-[var(--color-phase-paused)]" />
        <div className="min-w-0 flex-1">
          <p className="truncate text-[13px] font-medium">{displayName(item)}</p>
          <p className="truncate text-[12px] text-[var(--color-ink-muted)]">
            {humanize(item.phase_reason)} · <RelativeTime value={item.updated_at} />
          </p>
        </div>
        <PhaseBadge phase={item.observed_phase} reason={item.phase_reason} />
      </Link>
    </li>
  );
}
