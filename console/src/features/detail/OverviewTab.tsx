import { useQueries } from '@tanstack/react-query';
import { ChevronsUp, CircleCheck, Minus, TrendingDown, TrendingUp } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import { formatEta, lagTrend, type LagTrend } from './lagTrend';
import { TablesPanel } from './TablesPanel';
import { PageBody } from '@/components/AppShell';
import { RelativeTime } from '@/components/Inline';
import { StatCard } from '@/components/StatCard';
import { TimeSeriesChart, type ChartPoint } from '@/components/TimeSeriesChart';
import { Badge, Button, EmptyState, KeyValue, Panel, PanelHeader, SegmentedControl } from '@/components/ui/primitives';
import { cn } from '@/lib/cn';
import { useAutoscaling } from '@/lib/defaults';
import { useApi } from '@/lib/domains';
import {
  formatBytes,
  formatBytesRate,
  formatCount,
  formatExact,
  formatRate,
  formatTimestamp,
  humanize,
  topicsLabel,
} from '@/lib/format';
import { withFieldNames } from '@shared/fieldPaths';
import type {
  ConsoleAutoscaling,
  CpAutoscale,
  CpDetail,
  CpMemberProgress,
  CpRuntimeProgress,
  CpSkipReason,
  CpTopicLag,
  DataSummary,
  DetailSnapshot,
  LagSample,
  MemberRate,
} from '@shared/types';
import { readableTables } from '@shared/types';

/** What every table of a materialization holds together, from their snapshots' manifests. */
interface StorageSummary {
  tables: number;
  rows: number;
  bytes: number;
  objects: number;
  /** The earliest of the tables' `validThrough`: complete through it on every table; null when any is unknown. */
  validThrough: string | null;
  newestCommit: string | null;
}

function combineSummaries(summaries: DataSummary[]): StorageSummary | null {
  const available = summaries.filter((summary) => summary.available);
  if (available.length === 0) return null;
  const instant = (value: string | null): number => (value ? Date.parse(value) : Number.NaN);
  const through = summaries.map((summary) => summary.validThrough);
  const commits = available.map((summary) => summary.newestCommit).filter((value): value is string => value !== null);
  return {
    tables: summaries.length,
    rows: available.reduce((sum, summary) => sum + summary.rows, 0),
    bytes: available.reduce((sum, summary) => sum + summary.bytes, 0),
    objects: available.reduce((sum, summary) => sum + summary.objects, 0),
    validThrough: through.some((value) => value === null)
      ? null
      : through.reduce<string | null>((first, value) => (first === null || instant(value) < instant(first) ? value : first), null),
    newestCommit: commits.reduce<string | null>((last, value) => (last === null || instant(value) > instant(last) ? value : last), null),
  };
}

/** How many of the tables the runtime writes exist yet, and how many more are kept read-only. */
function TableCounts({ detail }: { detail: CpDetail }): ReactNode {
  const written = new Set(detail.materialization.tables.map((table) => table.name));
  const created = detail.tables.filter((table) => written.has(table.name)).length;
  const kept = detail.materialization.retired_tables?.length ?? 0;
  const total = detail.materialization.tables.length;
  return (
    <>
      {created === total ? formatExact(created) : `${formatExact(created)} of ${formatExact(total)} created`}
      {kept > 0 ? <span className="text-[var(--color-ink-muted)]"> · {formatExact(kept)} kept</span> : null}
    </>
  );
}

/** Where the materialization's tables live: the directory each table's own is in. */
function materializationLocation(detail: CpDetail): string {
  const table = detail.tables[0];
  if (!table) return detail.materialization.destination_uri;
  return table.location.endsWith(`/${table.name}`) ? table.location.slice(0, -table.name.length - 1) : table.location;
}

/**
 * The materializer build this materialization's runtime is executing.
 *
 * Runtimes are deployed per materialization and keep their image until the spec is re-applied, so
 * this can legitimately differ between two materializations and from the control plane itself.
 * It stays blank until the runtime has reported at least once.
 */
function RuntimeVersion({ value }: { value?: string | null }): ReactNode {
  if (!value) {
    return (
      <span className="text-[var(--color-ink-faint)]" title="No runtime status reported yet">
        —
      </span>
    );
  }
  return <>v{value}</>;
}

type ThroughputMetric = 'rows' | 'bytes';
type LagWindow = '15m' | '1h' | '6h';

const LAG_WINDOWS: Record<LagWindow, number> = { '15m': 15 * 60_000, '1h': 60 * 60_000, '6h': 6 * 60 * 60_000 };

export function DetailOverviewTab({ snapshot }: { snapshot: DetailSnapshot }): ReactNode {
  const api = useApi();
  const detail = snapshot.detail;
  const progress = detail?.status.runtime_progress;
  const consumerLag = detail?.status.consumer_lag;
  const [showSkips, setShowSkips] = useState(false);
  const [metric, setMetric] = useState<ThroughputMetric>('rows');
  const autoscalingPolicy = useAutoscaling();

  // What is in the tables, from their current snapshots' manifests — the same counts the Data tab opens with.
  // Kept tables too: they hold data, and stay readable.
  const materialization = detail?.materialization;
  const tableNames = useMemo(() => (materialization ? readableTables(materialization) : []).map((table) => table.name), [materialization]);
  const summaryQueries = useQueries({
    queries: tableNames.map((table) => ({
      queryKey: ['data-summary', snapshot.materializationId, table],
      queryFn: () => api.dataSummary(snapshot.materializationId, table),
      refetchInterval: 15_000,
      retry: 0,
    })),
  });
  const summaries = summaryQueries.map((query) => query.data).filter((value): value is DataSummary => value !== undefined);
  const summary = summaries.length === tableNames.length ? combineSummaries(summaries) : null;
  const summaryError = summaryQueries.find((query) => query.error)?.error ?? null;
  const summaryPending = summaryQueries.some((query) => query.isPending);
  const liveTables = Object.values(progress?.tables ?? {});

  const now = Date.parse(snapshot.generatedAt) || Date.now();
  const rowSeries = useMemo<ChartPoint[]>(
    () => snapshot.series.map((point) => ({ t: point.t, value: point.rowsPerSecond })),
    [snapshot.series],
  );
  const byteSeries = useMemo<ChartPoint[]>(
    () => snapshot.series.map((point) => ({ t: point.t, value: point.bytesPerSecond })),
    [snapshot.series],
  );
  const seriesFrom = snapshot.series[0]?.t ?? now;

  const members = Object.keys(snapshot.memberRates).length;
  const recordsSkipped = progress?.cumulative_records_skipped ?? progress?.records_skipped ?? null;

  if (!detail) {
    return (
      <PageBody>
        <EmptyState
          title="Not found"
          description="This materialization is no longer present in the control plane."
        />
      </PageBody>
    );
  }

  const skipped = recordsSkipped ? (
    <button
      type="button"
      onClick={() => setShowSkips((value) => !value)}
      aria-expanded={showSkips}
      className="text-[var(--color-phase-paused)] underline decoration-dotted underline-offset-2 hover:text-[var(--color-ink)]"
    >
      {formatExact(recordsSkipped)} records skipped
    </button>
  ) : null;

  return (
    <PageBody className="space-y-6">
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Ingest rate"
          value={formatRate(snapshot.rowsPerSecond, '')}
          unit="rows/s"
          hint={
            <span title="Each runtime member's rows over its last minute of heartbeats, summed across members.">
              {members > 1 ? `${members} members` : 'Runtime'} · 1-min window
            </span>
          }
          series={rowSeries}
        />
        <StatCard
          label="Write rate"
          value={formatBytesRate(snapshot.bytesPerSecond)}
          hint={
            <span title="Parquet bytes sealed per second. Batches seal in waves every few minutes, so this is a 15-minute trend.">
              Parquet · 15-min trend
            </span>
          }
          series={byteSeries}
          color="var(--color-phase-running)"
        />
        <StorageCards
          summary={summary}
          summaryError={summaryError}
          loading={summaryPending}
          progress={progress}
          snapshots={liveTables.length > 0 ? liveTables.reduce((sum, table) => sum + (table.snapshots ?? 0), 0) : null}
          skipped={skipped}
        />
      </div>

      {showSkips && recordsSkipped ? (
        <SkippedRecords
          total={recordsSkipped}
          reasons={progress?.skip_reasons ?? []}
          onClose={() => setShowSkips(false)}
        />
      ) : null}

      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
        <Panel className="flex flex-col">
          <PanelHeader
            title="Throughput"
            description={
              metric === 'rows'
                ? 'Rows per second, measured per runtime member between its own heartbeats and summed.'
                : 'Parquet bytes per second, as a 15-minute trend: batches seal in waves, not continuously.'
            }
            actions={
              <SegmentedControl
                value={metric}
                onChange={setMetric}
                options={[
                  { value: 'rows', label: 'Rows' },
                  { value: 'bytes', label: 'Bytes' },
                ]}
              />
            }
          />
          {/* Grows with the panel beside it, so the chart uses the row's height instead of leaving it empty. */}
          <div className="min-h-[220px] flex-1 px-3 pt-3 pb-2">
            <TimeSeriesChart
              points={metric === 'rows' ? rowSeries : byteSeries}
              from={Math.max(seriesFrom, now - 30 * 60_000)}
              to={now}
              color={metric === 'rows' ? 'var(--color-accent)' : 'var(--color-phase-running)'}
              formatValue={metric === 'rows' ? (value) => formatRate(value, 'rows/s') : formatBytesRate}
              formatTick={metric === 'rows' ? formatCount : (value) => formatBytes(value)}
              label={metric === 'rows' ? 'Ingest rate' : 'Write rate'}
              empty="Collecting — rates appear once every runtime member has reported twice."
            />
          </div>
        </Panel>

        <Panel>
          <PanelHeader title="Reconciliation" description="What the control plane last concluded." />
          <dl className="grid grid-cols-2 gap-x-4 gap-y-4 p-5">
            <KeyValue label="Desired">{humanize(detail.status.desired_phase)}</KeyValue>
            <KeyValue label="Observed">{humanize(detail.status.observed_phase)}</KeyValue>
            <KeyValue label="Reason">{humanize(detail.status.phase_reason)}</KeyValue>
            <KeyValue label="Source">{humanize(detail.status.observation_source)}</KeyValue>
            <KeyValue label="Last transition">
              <RelativeTime value={detail.status.last_transition_time} />
            </KeyValue>
            <KeyValue label="Last heartbeat">
              <RelativeTime value={detail.status.last_heartbeat} />
            </KeyValue>
            <KeyValue label="Last observation">
              <RelativeTime value={detail.status.last_resource_observation_time} />
            </KeyValue>
            <KeyValue label="Tables">
              <TableCounts detail={detail} />
            </KeyValue>
            <KeyValue label="Last commit">
              <RelativeTime value={newestCommit(detail)} />
            </KeyValue>
            <KeyValue label="Runtime version" mono>
              <RuntimeVersion value={detail.status.runtime_version} />
            </KeyValue>
          </dl>
          {detail.status.phase_message ? (
            <p className="border-t border-[var(--color-line)] px-5 py-3 text-[13px] leading-relaxed text-[var(--color-ink-muted)]">
              {withFieldNames(detail.status.phase_message)}
            </p>
          ) : null}
          <ScaledUp autoscale={detail.autoscale} stopped={detail.status.desired_phase !== 'running'} />
          <MemberStates members={progress?.member_progress} rates={snapshot.memberRates} committer={liveTables[0]?.member_id ?? null} />
          {detail.status.last_error ? (
            <p className="border-t border-[var(--color-line)] px-5 py-3 text-[13px] leading-relaxed text-[var(--color-phase-failed)]">
              {withFieldNames(detail.status.last_error)}
            </p>
          ) : null}
        </Panel>
      </div>

      <TablesPanel detail={detail} />

      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
        <ConsumerLagPanel
          history={snapshot.lagHistory}
          now={now}
          totalLag={consumerLag?.total_lag ?? null}
          topics={consumerLag?.topics}
          measuredAt={consumerLag?.measured_at ?? null}
          error={consumerLag?.error ?? null}
          ingestRate={snapshot.rowsPerSecond}
        />

        <Panel>
          <PanelHeader title="Specification" description="Desired state recorded by the control plane." />
          <dl className="grid grid-cols-2 gap-x-4 gap-y-4 p-5">
            <KeyValue label={detail.materialization.source_topics.length === 1 ? 'Source topic' : 'Source topics'} className="col-span-2">
              <span className="block break-all" title={detail.materialization.source_topics.join('\n')}>
                {topicsLabel(detail.materialization.source_topics, 6)}
              </span>
            </KeyValue>
            <KeyValue label={detail.materialization.datasets.length === 1 ? 'Dataset' : 'Datasets'} mono>
              {detail.materialization.datasets.length > 0 ? detail.materialization.datasets.join(', ') : '—'}
            </KeyValue>
            <KeyValue label="Tables">
              {formatExact(detail.materialization.tables.length)}
              {detail.materialization.retired_tables?.length ? (
                <span className="text-[var(--color-ink-muted)]"> · {formatExact(detail.materialization.retired_tables.length)} kept</span>
              ) : null}
            </KeyValue>
            <KeyValue label="Location" className="col-span-2">
              <span className="block break-all">{materializationLocation(detail)}</span>
            </KeyValue>
            <KeyValue label="Replicas">
              {detail.materialization.replicas ?? 1}
              {detail.status.desired_phase === 'stopped' ? (
                <span className="text-[var(--color-ink-muted)]"> · none running while stopped</span>
              ) : detail.autoscale?.replicas ? (
                <span className="text-[var(--color-ink-muted)]"> · {detail.autoscale.replicas} while catching up</span>
              ) : null}
            </KeyValue>
            {detail.materialization.autoscaling !== undefined ? (
              <KeyValue label="Autoscaling">{autoscalingLabel(detail.materialization.autoscaling, autoscalingPolicy)}</KeyValue>
            ) : null}
            <KeyValue label="Storage per replica">{detail.materialization.storage_size ?? '—'}</KeyValue>
            <KeyValue label="Namespace">{detail.materialization.namespace}</KeyValue>
            <KeyValue label="Runtime mode">{detail.materialization.runtime_mode}</KeyValue>
            <KeyValue label="Runtime name">
              {detail.materialization.resolved_runtime_name ?? '—'}
            </KeyValue>
            <KeyValue label="Created by">{detail.materialization.created_by ?? '—'}</KeyValue>
            <KeyValue label="Created">{formatTimestamp(detail.materialization.created_at)}</KeyValue>
            <KeyValue label="Resource profile">{detail.materialization.resource_profile ?? '—'}</KeyValue>
            <KeyValue label="Spec version">{detail.materialization.spec_version ?? 1}</KeyValue>
            <KeyValue label="Tombstones">
              {detail.materialization.skip_tombstones ? 'Ignored' : 'Kept as rows'}
            </KeyValue>
            <KeyValue label="Partitioned by">
              {partitionLabel(detail.tables[0]?.partition_granularity ?? detail.materialization.partition_granularity ?? null)}
            </KeyValue>
          </dl>
          <LabelList title="Labels" entries={detail.materialization.labels} />
          <LabelList title="Annotations" entries={detail.materialization.annotations} />
        </Panel>
      </div>
    </PageBody>
  );
}

/**
 * The runtime running more members than its own count, and why: the control plane scaled it up
 * while it is far behind. Nothing while it runs its own count.
 */
function ScaledUp({ autoscale, stopped }: { autoscale: CpAutoscale | undefined; stopped: boolean }): ReactNode {
  if (!autoscale?.replicas || stopped) return null;
  return (
    <p className="flex items-start gap-2 border-t border-[var(--color-line)] px-5 py-3 text-[13px] leading-relaxed text-[var(--color-ink-muted)]">
      <ChevronsUp className="mt-[3px] size-3.5 shrink-0 text-[var(--color-accent)]" aria-hidden />
      <span>
        <span className="font-medium text-[var(--color-ink)]">Scaled up to {autoscale.replicas} members</span>{' '}
        <RelativeTime value={autoscale.changed_at} />
        {autoscale.reason ? `: ${autoscale.reason}` : ''}. Back to {autoscale.own_replicas} once caught up.
      </span>
    </p>
  );
}

/** Whether the control plane may scale this runtime: its own switch, and the deployment's over it. */
function autoscalingLabel(enabled: boolean, policy: ConsoleAutoscaling | null): string {
  if (!enabled) return 'Off';
  if (policy && !policy.enabled) return 'Off for every materialization';
  return policy ? `On · up to ${policy.maxReplicas}` : 'On';
}

/** The newest commit to any of the materialization's tables. */
function newestCommit(detail: CpDetail): string | null {
  const commits = [
    ...detail.tables.map((table) => table.last_commit_at),
    ...Object.values(detail.status.runtime_progress?.tables ?? {}).map((table) => table.last_commit_at ?? null),
  ].filter((value): value is string => typeof value === 'string');
  return commits.reduce<string | null>((last, value) => (last === null || Date.parse(value) > Date.parse(last) ? value : last), null);
}

/**
 * What the materialization's tables hold, together.
 *
 * Read from each table's current snapshot, so these are exactly the rows and bytes queries over All
 * time see — the Data tab opens with the same counts. The runtime's own counters are close but not
 * the same thing: they include rows still being batched, and a pod restart drops the few counted
 * since its last heartbeat. They stand in only when the tables cannot be read.
 */
function StorageCards({
  summary,
  summaryError,
  loading,
  progress,
  snapshots,
  skipped,
}: {
  summary: StorageSummary | null;
  summaryError: unknown;
  loading: boolean;
  progress: CpRuntimeProgress | undefined;
  /** Snapshots the table keeps, as the committer last reported. */
  snapshots: number | null;
  skipped: ReactNode;
}): ReactNode {
  if (summary) {
    const average = summary.objects > 0 ? summary.bytes / summary.objects : null;
    return (
      <>
        <StatCard
          label="Rows committed"
          value={formatExact(summary.rows)}
          hint={
            <>
              <span
                title={
                  summary.tables > 1
                    ? "Rows in the current snapshots of every table, together — each table's count is in the Tables panel."
                    : "Rows in the table's current snapshot — the count the Data tab shows for All time."
                }
              >
                {summary.tables > 1 ? `${formatExact(summary.tables)} tables · ` : null}
                {summary.validThrough ? (
                  <span title="Every record with a Kafka timestamp before this moment is in the table.">
                    complete through {formatTimestamp(summary.validThrough)} ·{' '}
                  </span>
                ) : null}
                last commit <RelativeTime value={summary.newestCommit} />
              </span>
              {skipped ? <span className="mt-0.5 block">{skipped}</span> : null}
            </>
          }
        />
        <StatCard
          label={summary.tables > 1 ? 'Tables’ size' : 'Table size'}
          value={formatBytes(summary.bytes)}
          hint={`${formatExact(summary.objects)} data ${summary.objects === 1 ? 'file' : 'files'}${average === null ? '' : ` · ${formatBytes(average)} each`}${snapshots === null ? '' : ` · ${formatExact(snapshots)} snapshots kept`}`}
        />
      </>
    );
  }

  // The table cannot be read, or holds nothing yet: say which, and fall back to the runtime.
  const rows = progress?.cumulative_rows_written ?? progress?.rows_written ?? null;
  const bytes = progress?.cumulative_bytes_written ?? progress?.bytes_written ?? null;
  const files = progress?.cumulative_files_written ?? progress?.files_written ?? null;
  const why = loading ? 'Reading the table…' : summaryError ? 'Table unreadable · runtime counters' : 'Nothing committed yet · runtime counters';
  return (
    <>
      <StatCard
        label="Rows committed"
        value={loading ? '—' : formatCount(rows)}
        hint={
          <>
            <span>{why}</span>
            {skipped ? <span className="mt-0.5 block">{skipped}</span> : null}
          </>
        }
      />
      <StatCard label="Table size" value={loading ? '—' : formatBytes(bytes)} hint={loading ? why : `${formatCount(files)} files sealed · runtime counters`} />
    </>
  );
}

const LAG_STATE: Record<LagTrend['state'], { label: string; tone: 'running' | 'paused' | 'neutral'; icon: ReactNode }> = {
  'caught-up': { label: 'Caught up', tone: 'running', icon: <CircleCheck className="size-3" /> },
  draining: { label: 'Catching up', tone: 'neutral', icon: <TrendingDown className="size-3" /> },
  growing: { label: 'Falling behind', tone: 'paused', icon: <TrendingUp className="size-3" /> },
  steady: { label: 'Holding steady', tone: 'neutral', icon: <Minus className="size-3" /> },
  unknown: { label: 'Measuring', tone: 'neutral', icon: <Minus className="size-3" /> },
};

/**
 * How far the runtime trails its source topic, and which way that is heading.
 *
 * One point per lag measurement, kept for six hours, on an axis fitted to the values: a backlog of
 * a hundred million records falling by a few thousand a second is a clear slope rather than a flat
 * line pinned to the top of a zero-based chart. The trend line reads the slope out — records per
 * second, and the time left at that pace — so nobody has to estimate it from the drawing.
 */
function ConsumerLagPanel({
  history,
  now,
  totalLag,
  topics,
  measuredAt,
  error,
  ingestRate,
}: {
  history: LagSample[];
  now: number;
  totalLag: number | null;
  /** Each source topic's lag; its partitions are listed under it when there are several topics. */
  topics: Record<string, CpTopicLag> | undefined;
  measuredAt: string | null;
  error: string | null;
  ingestRate: number | null;
}): ReactNode {
  const [lagWindow, setLagWindow] = useState<LagWindow>('1h');
  const [showPartitions, setShowPartitions] = useState(false);

  const trend = useMemo(() => lagTrend(history), [history]);
  const points = useMemo<ChartPoint[]>(() => history.map((point) => ({ t: point.t, value: point.lag })), [history]);
  const several = Object.keys(topics ?? {}).length > 1;
  const partitions = useMemo(
    () =>
      Object.entries(topics ?? {})
        .sort(([a], [b]) => a.localeCompare(b))
        .flatMap(([topic, lag]) =>
          Object.entries(lag.partition_lag ?? {})
            .sort(([a], [b]) => Number(a) - Number(b))
            .map(([partition, value]): [string, number] => [several ? `${topic} P${partition}` : `P${partition}`, value]),
        ),
    [topics, several],
  );
  const behind = partitions.filter(([, lag]) => lag > 0).length;
  const largest = Math.max(1, ...partitions.map(([, lag]) => lag));
  const cadence = useMemo(() => measurementCadence(history), [history]);
  const state = LAG_STATE[trend.state];

  return (
    <Panel className="flex flex-col">
      <PanelHeader
        title="Consumer lag"
        description={several ? 'Records on the source topics the runtime has not yet committed, all together.' : 'Records on the source topic the runtime has not yet committed.'}
        actions={
          <SegmentedControl
            value={lagWindow}
            onChange={setLagWindow}
            options={(Object.keys(LAG_WINDOWS) as LagWindow[]).map((value) => ({ value, label: value }))}
          />
        }
      />
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1.5 px-5 pt-4">
        <span className="numeric text-[22px] leading-none font-semibold tracking-tight">{formatCount(totalLag)}</span>
        <span className="text-[12px] text-[var(--color-ink-muted)]">records behind</span>
        <Badge tone={state.tone}>
          <span className="flex items-center gap-1">
            {state.icon}
            {state.label}
          </span>
        </Badge>
        <span className="numeric text-[12px] text-[var(--color-ink-muted)]">
          {trend.state === 'draining' && trend.perSecond !== null
            ? `−${formatCount(Math.round(-trend.perSecond))}/s · about ${formatEta(trend.etaMs ?? Number.NaN)} to go`
            : trend.state === 'growing' && trend.perSecond !== null
              ? `+${formatCount(Math.round(trend.perSecond))}/s${ingestRate !== null ? ` while ingesting ${formatRate(ingestRate, 'rows/s')}` : ''}`
              : null}
        </span>
      </div>
      {/* Grows with the panel beside it, like the throughput chart. */}
      <div className="min-h-[200px] flex-1 px-3 pt-3 pb-1">
        <TimeSeriesChart
          points={points}
          from={now - LAG_WINDOWS[lagWindow]}
          to={now}
          baseline="auto"
          formatValue={(value) => `${formatExact(value)} records`}
          formatTick={formatCount}
          label="Consumer lag"
          empty={
            history.length === 1
              ? 'One measurement so far — the trend appears with the next.'
              : error
                ? `Lag could not be measured: ${error}`
                : 'No lag measured yet. The control plane measures it about once a minute.'
          }
        />
      </div>

      {partitions.length > 0 ? (
        <div className="border-t border-[var(--color-line)] px-5 py-3">
          <div className="flex flex-wrap items-center gap-1.5">
            <Badge tone={behind > 0 ? 'neutral' : 'running'}>
              {behind > 0 ? `${behind} of ${partitions.length} partitions behind` : `All ${partitions.length} partitions caught up`}
            </Badge>
            <button
              type="button"
              onClick={() => setShowPartitions((value) => !value)}
              aria-expanded={showPartitions}
              className="ml-auto text-[12px] text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]"
            >
              {showPartitions ? 'Hide partitions' : `Show ${partitions.length} partitions`}
            </button>
          </div>
          {showPartitions ? (
            <ul className="mt-3 grid grid-cols-1 gap-x-6 sm:grid-cols-2">
              {partitions.map(([partition, lag]) => (
                <li key={partition} className="flex items-center gap-3 border-b border-[var(--color-line)] py-1.5 text-[12px]">
                  <span className={cn('numeric shrink-0 truncate text-[var(--color-ink-muted)]', several ? 'w-40' : 'w-8')} title={partition}>
                    {partition}
                  </span>
                  <span className="relative h-1.5 min-w-0 flex-1 overflow-hidden rounded-full bg-[var(--color-raised)]">
                    <span
                      className="absolute inset-y-0 left-0 rounded-full bg-[var(--color-accent)] opacity-60"
                      style={{ width: `${(lag / largest) * 100}%` }}
                    />
                  </span>
                  <span className="numeric w-24 shrink-0 text-right">{formatExact(lag)}</span>
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}

      {measuredAt ? (
        <p className="border-t border-[var(--color-line)] px-5 py-2 text-[11px] text-[var(--color-ink-faint)]">
          Measured <RelativeTime value={measuredAt} />
          {cadence ? ` · every ${cadence}` : ''}
        </p>
      ) : null}
    </Panel>
  );
}

/** How often lag is measured, from the spacing of recent measurements: `~1 min`, `~15 s`. */
function measurementCadence(history: LagSample[]): string | null {
  const recent = history.slice(-6);
  if (recent.length < 3) return null;
  const gaps = recent.slice(1).map((point, index) => point.t - (recent[index] as LagSample).t).sort((a, b) => a - b);
  const median = gaps[Math.floor(gaps.length / 2)] as number;
  return median >= 90_000 ? `~${Math.round(median / 60_000)} min` : median >= 50_000 ? '~1 min' : `~${Math.round(median / 1000)} s`;
}

function LabelList({
  title,
  entries,
}: {
  title: string;
  entries: Record<string, string> | undefined;
}): ReactNode {
  const pairs = Object.entries(entries ?? {});
  if (pairs.length === 0) return null;
  return (
    <div className="border-t border-[var(--color-line)] px-5 py-3">
      <p className="text-[11px] font-medium tracking-wide text-[var(--color-ink-faint)] uppercase">
        {title}
      </p>
      <div className="mt-2 flex flex-wrap gap-1.5">
        {pairs.map(([key, value]) => (
          <span
            key={key}
            className="numeric rounded-md bg-[var(--color-raised)] px-2 py-0.5 text-[11px] text-[var(--color-ink-muted)]"
          >
            {key}={value}
          </span>
        ))}
      </div>
    </div>
  );
}

/**
 * Why records were skipped, opened from the skipped count so it only takes space when asked for.
 *
 * Reasons are tracked from the runtime release that introduced them, so they can sum to less than
 * the cumulative total: the remainder was skipped before reasons were recorded.
 */
function SkippedRecords({
  total,
  reasons,
  onClose,
}: {
  total: number;
  reasons: CpSkipReason[];
  onClose: () => void;
}): ReactNode {
  const attributed = reasons.reduce((sum, reason) => sum + reason.count, 0);
  const unattributed = Math.max(total - attributed, 0);

  return (
    <Panel>
      <PanelHeader
        title="Skipped records"
        description="Records the runtime could not write, grouped by reason. Each row names the latest occurrence so the record can be found on the source topic."
        actions={
          <Button size="sm" variant="ghost" onClick={onClose}>
            Close
          </Button>
        }
      />
      {reasons.length === 0 ? (
        <p className="px-5 py-4 text-[13px] text-[var(--color-ink-muted)]">
          No reasons have been reported yet. Runtimes report why they skip records once they are
          running a release that tracks it.
        </p>
      ) : (
        <ul className="divide-y divide-[var(--color-line)]">
          {reasons.map((reason) => (
            <li key={reason.category} className="grid grid-cols-[6rem_minmax(0,1fr)] gap-4 px-5 py-3">
              <span className="numeric text-right text-[13px] font-medium">{formatExact(reason.count)}</span>
              <div className="min-w-0">
                <p className="numeric text-[13px] break-words">{withFieldNames(reason.category)}</p>
                <p className="mt-0.5 text-[12px] text-[var(--color-ink-muted)]">
                  Latest
                  {reason.last_partition !== undefined && reason.last_offset !== undefined
                    ? ` at partition ${reason.last_partition}, offset ${formatExact(reason.last_offset)}`
                    : ''}
                  {reason.last_schema_version !== undefined ? ` · schema ${reason.last_schema_version}` : ''}
                  {reason.last_seen_at ? (
                    <>
                      {' · '}
                      <RelativeTime value={reason.last_seen_at} />
                    </>
                  ) : null}
                </p>
                {reason.last_reason !== reason.category ? (
                  <p className="mt-0.5 text-[12px] break-words text-[var(--color-ink-faint)]">
                    {withFieldNames(reason.last_reason)}
                  </p>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      )}
      {unattributed > 0 && reasons.length > 0 ? (
        <p className="border-t border-[var(--color-line)] px-5 py-2 text-[11px] text-[var(--color-ink-faint)]">
          {formatExact(unattributed)} earlier skips predate reason tracking.
        </p>
      ) : null}
    </Panel>
  );
}

/** Members silent for longer than this are gone (scaled away or replaced); mirrors the control plane. */
const MEMBER_FRESHNESS_MS = 120_000;

const MEMBER_STATES: Record<string, { label: string; tone: 'running' | 'paused' | 'failed' | 'neutral' }> = {
  degraded: { label: 'reporting errors', tone: 'failed' },
  waiting_for_table: { label: 'loading tables', tone: 'neutral' },
  processing: { label: 'processing', tone: 'running' },
  waiting_for_source_records: { label: 'idle', tone: 'neutral' },
};

function memberState(member: CpMemberProgress): string {
  return member.health === 'degraded' ? 'degraded' : (member.phase_reason ?? 'unknown');
}

/**
 * Per-member breakdown behind the reconciliation message.
 *
 * The message already says how many members are in each state; this names them, so a split such
 * as "3 of 16 reporting errors" can be traced to the pods involved — and each member's own rate
 * shows which of them are doing the work. One member also commits every member's files to the
 * table; it is marked.
 */
function MemberStates({
  members,
  rates,
  committer,
}: {
  members?: Record<string, CpMemberProgress>;
  rates: Record<string, MemberRate>;
  /** The member running the committer, from its own report of the table. */
  committer: string | null;
}): ReactNode {
  const [open, setOpen] = useState(false);

  const fresh = useMemo(() => {
    const entries = Object.entries(members ?? {}).filter(([, member]) => member.health && member.reported_at);
    const newest = Math.max(...entries.map(([, member]) => Date.parse(member.reported_at ?? '')), 0);
    return entries
      .filter(([, member]) => newest - Date.parse(member.reported_at ?? '') <= MEMBER_FRESHNESS_MS)
      .sort(([a], [b]) => a.localeCompare(b, undefined, { numeric: true }));
  }, [members]);

  const counts = useMemo(() => {
    const tally = new Map<string, number>();
    for (const [, member] of fresh) tally.set(memberState(member), (tally.get(memberState(member)) ?? 0) + 1);
    return [...tally.entries()];
  }, [fresh]);

  if (fresh.length < 2) return null;

  return (
    <div className="border-t border-[var(--color-line)] px-5 py-3">
      <div className="flex flex-wrap items-center gap-1.5">
        {counts.map(([state, count]) => (
          <Badge key={state} tone={MEMBER_STATES[state]?.tone ?? 'neutral'}>
            {count} {MEMBER_STATES[state]?.label ?? humanize(state)}
          </Badge>
        ))}
        <button
          type="button"
          onClick={() => setOpen((value) => !value)}
          aria-expanded={open}
          className="ml-auto text-[12px] text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]"
        >
          {open ? 'Hide members' : `Show ${fresh.length} members`}
        </button>
      </div>
      {open ? (
        <ul className="mt-3 divide-y divide-[var(--color-line)] rounded-[var(--radius-control)] border border-[var(--color-line)]">
          {fresh.map(([memberId, member]) => {
            const state = memberState(member);
            const rate = rates[memberId]?.rowsPerSecond ?? null;
            return (
              <li key={memberId} className="flex items-center gap-3 px-3 py-1.5 text-[12px]">
                <span className="numeric w-16 shrink-0 truncate" title={memberId}>
                  {memberId.split('-').slice(-2).join('-')}
                </span>
                <Badge tone={MEMBER_STATES[state]?.tone ?? 'neutral'}>
                  {MEMBER_STATES[state]?.label ?? humanize(state)}
                </Badge>
                {memberId === committer ? (
                  <Badge tone="accent" title="This member commits every member's files to the table, and runs its maintenance.">
                    committer
                  </Badge>
                ) : null}
                <span
                  className="min-w-0 flex-1 truncate text-[var(--color-ink-muted)]"
                  title={withFieldNames(member.last_error ?? member.phase_message ?? '') || undefined}
                >
                  {state === 'degraded' && member.last_error
                    ? withFieldNames(member.last_error)
                    : (member.files_pending_commit ?? 0) > 0
                      ? `${formatExact(member.files_pending_commit)} ${member.files_pending_commit === 1 ? 'file' : 'files'} awaiting commit`
                      : null}
                </span>
                <span
                  className={cn('numeric shrink-0 text-right', rate === null ? 'text-[var(--color-ink-faint)]' : null)}
                  title="Rows per second, over this member's last minute of heartbeats"
                >
                  {formatRate(rate, 'rows/s')}
                </span>
                <span className="w-20 shrink-0 text-right text-[var(--color-ink-faint)]">
                  <RelativeTime value={member.reported_at ?? null} />
                </span>
              </li>
            );
          })}
        </ul>
      ) : null}
    </div>
  );
}

function partitionLabel(granularity: string | null): string {
  if (granularity === 'day') return 'Day of the Kafka timestamp';
  if (granularity === 'hour') return 'Hour of the Kafka timestamp';
  if (granularity === 'month') return 'Month of the Kafka timestamp';
  if (granularity === 'none') return 'Not partitioned';
  return '—';
}
