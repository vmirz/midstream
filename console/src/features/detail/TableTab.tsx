import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Check, ChevronDown, Copy, Loader2, Play, TriangleAlert } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';

import { reprotectCounts } from '../protection/protection';
import { TABLE_PARAM, useSelectedTable } from '../tables/selectedTable';
import { TablePicker } from '../tables/TablePicker';
import { PageBody } from '@/components/AppShell';
import { RelativeTime } from '@/components/Inline';
import { StatCard } from '@/components/StatCard';
import { useToast } from '@/components/ui/Toast';
import { Badge, Button, EmptyState, KeyValue, Panel, PanelHeader, SegmentedControl, Skeleton } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useAuth, useCan } from '@/lib/auth';
import { cn } from '@/lib/cn';
import { formatBytes, formatCount, formatDuration, formatExact, formatTimestamp, humanize, shortSnapshotId } from '@/lib/format';
import { useApi, useDomainPath } from '@/lib/domains';
import type {
  CatalogConnection,
  CpMaintenanceRun,
  CpTable,
  CpTableSpec,
  DetailSnapshot,
  MaintenanceJob,
  TableMetadataView,
  TablePartitionRow,
  TableSnapshotRow,
} from '@shared/types';

/**
 * One of the materialization's Iceberg tables: its snapshots, its files by partition, the
 * maintenance that keeps it fast, its properties, and how to read it from another engine.
 *
 * Two sources, each enough on its own: the control plane's record, which the runtime's commit and
 * maintenance events keep, and the table itself, which the console reads directly.
 */
export function TableTab({ snapshot }: { snapshot: DetailSnapshot }): ReactNode {
  const { table, tables, select } = useSelectedTable(snapshot.detail);
  if (!table) {
    return (
      <PageBody>
        <EmptyState title="Not found" description="This materialization is no longer present." />
      </PageBody>
    );
  }
  return <TableDetail key={table.name} snapshot={snapshot} table={table} tables={tables} onSelectTable={select} />;
}

function TableDetail({
  snapshot,
  table,
  tables,
  onSelectTable,
}: {
  snapshot: DetailSnapshot;
  table: CpTableSpec;
  tables: CpTableSpec[];
  onSelectTable: (name: string) => void;
}): ReactNode {
  const domainPath = useDomainPath();
  const api = useApi();
  const materializationId = snapshot.materializationId;
  const tableQuery = useQuery({
    queryKey: ['table', materializationId, table.name],
    queryFn: () => api.table(materializationId, table.name),
    refetchInterval: 10_000,
  });
  // Which table, for a materialization of several; nothing to choose otherwise.
  const tableBar =
    tables.length > 1 ? (
      <div className="flex flex-wrap items-center gap-2 text-[12px] text-[var(--color-ink-muted)]">
        <TablePicker tables={tables} value={table.name} onChange={onSelectTable} />
        <span>
          Topic <code className="numeric text-[var(--color-ink)]">{table.source_topic}</code>, in dataset{' '}
          <Link to={domainPath(`/datasets/${encodeURIComponent(table.dataset)}`)} className="numeric text-[var(--color-ink)] hover:text-[var(--color-accent)] hover:underline">
            {table.dataset}
          </Link>
        </span>
      </div>
    ) : null;

  if (tableQuery.isPending) {
    return (
      <PageBody className="space-y-4">
        {tableBar}
        <Skeleton className="h-24" />
        <Skeleton className="h-64" />
      </PageBody>
    );
  }
  if (tableQuery.isError || !tableQuery.data) {
    return (
      <PageBody className="space-y-4">
        {tableBar}
        <Panel>
          <EmptyState
            icon={<TriangleAlert className="size-6" />}
            title="The table could not be read"
            description={tableQuery.error instanceof Error ? tableQuery.error.message : 'Try again in a moment.'}
          />
        </Panel>
      </PageBody>
    );
  }

  const { record, metadata, metadataError, connection } = tableQuery.data;
  if (!record && !metadata) {
    return (
      <PageBody className="space-y-4">
        {tableBar}
        <Panel>
          <EmptyState
            title="No table yet"
            description={
              metadataError && metadataError !== 'the table has not been created yet'
                ? metadataError
                : `The control plane creates the table right after it discovers the schema of ${table.source_topic}.`
            }
          />
        </Panel>
      </PageBody>
    );
  }

  return (
    <PageBody className="space-y-6">
      {tableBar}
      <TableFigures record={record} metadata={metadata} />
      {metadataError ? (
        <p className="rounded-lg border border-[var(--color-line)] px-4 py-3 text-[12px] text-[var(--color-ink-muted)]">
          Showing the control plane’s record only: {metadataError}.
        </p>
      ) : null}
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)]">
        <SnapshotsPanel materializationId={materializationId} table={table} defaultTable={tables[0]?.name ?? null} metadata={metadata} />
        <MaintenancePanel materializationId={materializationId} table={table.name} several={tables.length > 1} record={record} />
      </div>
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <PartitionsPanel metadata={metadata} />
        <PropertiesPanel record={record} metadata={metadata} />
      </div>
      <ConnectPanel connection={connection} record={record} metadata={metadata} table={table} />
    </PageBody>
  );
}

function TableFigures({ record, metadata }: { record: CpTable | null; metadata: TableMetadataView | null }): ReactNode {
  const records = metadata?.totals.records ?? record?.totals.records ?? null;
  const bytes = metadata?.totals.bytes ?? record?.totals.bytes ?? null;
  const files = metadata?.totals.files ?? record?.totals.files ?? null;
  const small = metadata?.totals.smallFiles ?? record?.live?.small_files ?? null;
  const snapshots = metadata?.snapshotCount ?? record?.snapshots ?? null;
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <StatCard
        label="Rows"
        value={formatExact(records)}
        hint={
          record?.valid_through ? (
            <span title="Every record with a Kafka timestamp before this moment is in the table.">
              complete through {formatTimestamp(record.valid_through)}
            </span>
          ) : (
            'in the current snapshot'
          )
        }
      />
      <StatCard
        label="Size"
        value={formatBytes(bytes)}
        hint={files ? `${formatBytes((bytes ?? 0) / files)} per file on average` : 'no data files yet'}
      />
      <StatCard
        label="Data files"
        value={formatExact(files)}
        hint={
          small !== null && files ? (
            <span title="Files below the runtimes’ small-file size, which compaction merges.">
              {formatExact(small)} small ({Math.round((small / files) * 100)}%)
            </span>
          ) : null
        }
      />
      <StatCard
        label="Last commit"
        value={record?.last_commit_at ? <RelativeTime value={record.last_commit_at} /> : '—'}
        hint={`${snapshots === null ? '—' : formatExact(snapshots)} snapshots kept${record?.last_commit_duration_ms ? ` · took ${formatDuration(record.last_commit_duration_ms)}` : ''}`}
      />
    </div>
  );
}

function offsetsText(offsets: Record<string, number> | null): string {
  if (!offsets) return '—';
  const entries = Object.entries(offsets).sort(([a], [b]) => Number(a) - Number(b));
  if (entries.length === 0) return '—';
  const shown = entries.slice(0, 4).map(([partition, offset]) => `p${partition} ${formatExact(offset)}`);
  return entries.length > 4 ? `${shown.join(' · ')} · +${entries.length - 4} more` : shown.join(' · ');
}

function operationLabel(snapshot: TableSnapshotRow): ReactNode {
  if (snapshot.operation === 'append') return <Badge tone="running">append</Badge>;
  // Files rewritten to mask what was written in plain text (handbook 22 §22.15).
  if (snapshot.materializerOperation === 'reprotect') return <Badge tone="accent">reprotect</Badge>;
  if (snapshot.operation === 'replace') return <Badge tone="accent">compaction</Badge>;
  return <Badge>{snapshot.operation}</Badge>;
}

function SnapshotsPanel({
  materializationId,
  table: spec,
  defaultTable,
  metadata,
}: {
  materializationId: string;
  table: CpTableSpec;
  /** The materialization's first table, which the Data tab shows without being told. */
  defaultTable: string | null;
  metadata: TableMetadataView | null;
}): ReactNode {
  const domainPath = useDomainPath();
  const navigate = useNavigate();
  const table = spec.name;
  // Reading the table as it was is a query, on its dataset.
  const canQuery = useCan('data.query', spec.dataset_id);
  const [showAll, setShowAll] = useState(false);
  const snapshots = metadata?.snapshots ?? [];
  const shown = showAll ? snapshots : snapshots.slice(0, 12);
  return (
    <Panel className="overflow-hidden">
      <PanelHeader
        title="Snapshots"
        description={
          metadata
            ? `${formatExact(metadata.snapshotCount)} kept. Each commit is one; ${canQuery ? 'open one to read the table as it was.' : 'the newest is current.'}`
            : 'Read from the table itself, which this console cannot reach right now.'
        }
      />
      {snapshots.length === 0 ? (
        <EmptyState title="No snapshots yet" description="The first commit creates the first snapshot." />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[640px] text-[12px]">
            <thead>
              <tr className="border-b border-[var(--color-line)] bg-[var(--color-raised)] text-left text-[11px] tracking-wide text-[var(--color-ink-muted)] uppercase">
                <th className="px-5 py-2 font-medium">Committed</th>
                <th className="px-2 py-2 font-medium">Operation</th>
                <th className="px-2 py-2 text-right font-medium whitespace-nowrap">Added</th>
                <th className="px-2 py-2 text-right font-medium">Files</th>
                <th className="px-2 py-2 text-right font-medium">Rows</th>
                <th className="px-5 py-2 font-medium">Offsets reached</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--color-line)]">
              {shown.map((item) => {
                // The Data tab reads the latest data only, so only the current snapshot opens it.
                const opens = canQuery && item.current;
                return (
                  <tr
                    key={item.snapshotId}
                    className={cn(opens && 'cursor-pointer hover:bg-[var(--color-raised)]')}
                    title={`Snapshot ${item.snapshotId}${opens ? ' — open it in the Data tab' : ''}`}
                    onClick={
                      opens
                        ? () => {
                            const params = new URLSearchParams();
                            if (table !== defaultTable) params.set(TABLE_PARAM, table);
                            const query = params.toString();
                            navigate(domainPath(`/materializations/${encodeURIComponent(materializationId)}/data${query ? `?${query}` : ''}`));
                          }
                        : undefined
                    }
                  >
                    <td className="px-5 py-2 whitespace-nowrap">
                      <span className="numeric">{formatTimestamp(item.committedAt)}</span>
                      {item.current ? (
                        <Badge tone="accent" className="ml-2">
                          current
                        </Badge>
                      ) : null}
                    </td>
                    <td className="px-2 py-2">{operationLabel(item)}</td>
                    <td className="numeric px-2 py-2 text-right">
                      {item.operation === 'append' && item.addedRecords !== null ? `+${formatExact(item.addedRecords)}` : '—'}
                    </td>
                    <td className="numeric px-2 py-2 text-right text-[var(--color-ink-muted)]">
                      {item.addedFiles !== null ? `+${formatExact(item.addedFiles)}` : ''}
                      {item.deletedFiles ? ` −${formatExact(item.deletedFiles)}` : ''}
                    </td>
                    <td className="numeric px-2 py-2 text-right">{formatExact(item.totalRecords)}</td>
                    <td className="numeric max-w-[260px] truncate px-5 py-2 text-[var(--color-ink-muted)]">{offsetsText(item.offsets)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {snapshots.length > 12 ? (
            <button
              type="button"
              onClick={() => setShowAll((value) => !value)}
              className="flex w-full items-center justify-center gap-1 border-t border-[var(--color-line)] py-2 text-[12px] text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]"
            >
              <ChevronDown className={cn('size-3.5 transition-transform', showAll && 'rotate-180')} />
              {showAll ? 'Show fewer' : `Show all ${snapshots.length}`}
            </button>
          ) : null}
        </div>
      )}
    </Panel>
  );
}

const JOBS: Array<{ job: MaintenanceJob; label: string; description: string }> = [
  {
    job: 'compaction',
    label: 'Compaction',
    description: 'Merges a partition’s small files into large ones, so reads open fewer files.',
  },
  {
    job: 'expire_snapshots',
    label: 'Snapshot expiry',
    description: 'Forgets snapshots past the retention window; the table’s history starts later.',
  },
  {
    job: 'orphan_cleanup',
    label: 'Orphan cleanup',
    description: 'Deletes files no snapshot references, such as those a compaction replaced.',
  },
];

/** Shown once asked for, from the Protection tab, which runs it on every table. */
const REPROTECT = {
  job: 'reprotect' as const,
  label: 'Reprotect',
  description: 'Rewrites the files holding in plain text what the table masks now. Asked from the Protection tab.',
};

function MaintenancePanel({
  materializationId,
  table,
  several,
  record,
}: {
  materializationId: string;
  table: string;
  /** Whether the materialization has other tables, which these jobs leave alone. */
  several: boolean;
  record: CpTable | null;
}): ReactNode {
  const api = useApi();
  const canMaintain = useCan('materializations.maintain');
  const toast = useToast();
  const queryClient = useQueryClient();
  const maintain = useMutation({
    mutationFn: (job: MaintenanceJob) => api.maintain(materializationId, job, table),
    onSuccess: (_accepted, job) => {
      toast.success(
        `${JOBS.find((item) => item.job === job)?.label ?? humanize(job)} of ${table} requested`,
        'The maintenance app runs it next — one job at a time — and reports the outcome here, whether or not the runtime runs.',
      );
      void queryClient.invalidateQueries({ queryKey: ['table', materializationId, table] });
    },
    onError: (error) => toast.error('Maintenance could not be requested', error instanceof ApiError ? error.message : String(error)),
  });

  const latest = useMemo(() => {
    const byJob = new Map<string, CpMaintenanceRun>();
    for (const run of record?.maintenance ?? []) {
      const known = byJob.get(run.job);
      if (!known || Date.parse(run.completed_at) > Date.parse(known.completed_at)) byJob.set(run.job, run);
    }
    return byJob;
  }, [record?.maintenance]);
  // The maintenance app's report; a runtime before 2.0 ran its own and said so on its heartbeat.
  const running = record?.maintenance_job ?? record?.live?.maintenance ?? null;
  const jobs = latest.has('reprotect') || record?.reprotect || running?.job === 'reprotect' ? [...JOBS, REPROTECT] : JOBS;

  return (
    <Panel className="overflow-hidden">
      <PanelHeader
        title="Maintenance"
        description={
          several
            ? 'The maintenance app runs these on every table in turn, one job at a time, whether or not the runtime runs (Settings › Compaction & retention). Run now asks for this table.'
            : 'The maintenance app runs these on its own schedule, whether or not the runtime runs (Settings › Compaction & retention).'
        }
      />
      <ul className="divide-y divide-[var(--color-line)]">
        {jobs.map((item) => {
          const last = latest.get(item.job) ?? null;
          const active = running?.job === item.job;
          const asked = item.job === 'reprotect' && !active ? (record?.reprotect ?? null) : null;
          return (
            <li key={item.job} className="flex items-start gap-3 px-5 py-3">
              <div className="min-w-0 flex-1">
                <p className="flex flex-wrap items-center gap-2 text-[13px] font-medium">
                  {item.label}
                  {active ? (
                    <Badge tone="pending">
                      <Loader2 className="size-3 animate-spin" />
                      running{running?.partition ? ` · ${running.partition}` : ''}
                    </Badge>
                  ) : asked ? (
                    <Badge tone="pending" title={`Asked by ${asked.requested_by}`}>
                      requested
                    </Badge>
                  ) : last ? (
                    <OutcomeBadge outcome={last.outcome} />
                  ) : null}
                </p>
                <p className="mt-0.5 text-[12px] text-[var(--color-ink-muted)]">{item.description}</p>
                {last ? (
                  <p className="numeric mt-1 text-[12px] text-[var(--color-ink-muted)]">
                    <RelativeTime value={last.completed_at} />
                    {last.partition ? ` · ${last.partition}` : ''}
                    {countsText(last) ? ` · ${countsText(last)}` : ''}
                    {last.duration_ms ? ` · ${formatDuration(last.duration_ms)}` : ''}
                  </p>
                ) : (
                  <p className="mt-1 text-[12px] text-[var(--color-ink-faint)]">Not run yet.</p>
                )}
                {last?.message && last.outcome !== 'succeeded' ? (
                  <p
                    className={cn(
                      'mt-1 text-[12px]',
                      last.outcome === 'skipped' ? 'text-[var(--color-ink-muted)]' : 'text-[var(--color-phase-paused)]',
                    )}
                  >
                    {last.message}
                  </p>
                ) : null}
              </div>
              {canMaintain && item.job !== 'reprotect' ? (
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={maintain.isPending || active}
                  onClick={() => maintain.mutate(item.job)}
                  title={`Run ${item.label.toLowerCase()} now`}
                >
                  <Play className="size-3.5" />
                  Run now
                </Button>
              ) : null}
            </li>
          );
        })}
      </ul>
    </Panel>
  );
}

function OutcomeBadge({ outcome }: { outcome: string }): ReactNode {
  if (outcome === 'succeeded') return <Badge tone="running">succeeded</Badge>;
  if (outcome === 'skipped') return <Badge>nothing to do</Badge>;
  if (outcome === 'failed') return <Badge tone="failed">{outcome}</Badge>;
  // Stopped for a drop, or by a maintenance member that went away: it runs again when it is next due.
  if (outcome === 'abandoned' || outcome === 'cancelled') return <Badge tone="pending">{outcome}</Badge>;
  return <Badge>{humanize(outcome)}</Badge>;
}

/** The counts a job reports, in words: `12 files → 1`, `40 snapshots expired`, `3 files deleted`. */
function countsText(run: CpMaintenanceRun): string {
  const counts = run.counts ?? {};
  const count = (key: string): number | undefined => (typeof counts[key] === 'number' ? counts[key] : undefined);
  const parts: string[] = [];
  if (run.job === 'compaction') {
    if (count('inputs') !== undefined && count('outputs') !== undefined) {
      parts.push(`${formatExact(count('inputs'))} files → ${formatExact(count('outputs'))}`);
    } else if (count('inputs') !== undefined) parts.push(`${formatExact(count('inputs'))} files`);
    if (count('input_bytes') !== undefined) parts.push(formatBytes(count('input_bytes')));
  } else if (run.job === 'expire_snapshots') {
    if (count('expired') !== undefined) parts.push(`${formatExact(count('expired'))} snapshots expired`);
    if (count('remaining') !== undefined) parts.push(`${formatExact(count('remaining'))} kept`);
  } else if (run.job === 'orphan_cleanup') {
    if (count('deleted') !== undefined) parts.push(`${formatExact(count('deleted'))} files deleted`);
    if (count('listed') !== undefined) parts.push(`${formatExact(count('listed'))} listed`);
  } else if (run.job === 'reprotect') {
    parts.push(...reprotectCounts(run));
  }
  return parts.join(' · ');
}

function PartitionsPanel({ metadata }: { metadata: TableMetadataView | null }): ReactNode {
  const [measure, setMeasure] = useState<'files' | 'bytes'>('files');
  const partitions = metadata?.partitions ?? [];
  const peak = Math.max(1, ...partitions.map((row) => (measure === 'files' ? row.files : row.bytes)));
  const due = partitions.filter((row) => row.compactionDue).length;
  return (
    <Panel className="overflow-hidden">
      <PanelHeader
        title="Files by partition"
        description={
          metadata
            ? `${formatExact(partitions.length)} ${partitions.length === 1 ? 'partition' : 'partitions'} · files under ${formatBytes(metadata.smallFileBytes)} count as small${due > 0 ? ` · ${due} due for compaction` : ''}`
            : 'Read from the table itself, which this console cannot reach right now.'
        }
        actions={
          <SegmentedControl
            value={measure}
            onChange={setMeasure}
            options={[
              { value: 'files', label: 'Files' },
              { value: 'bytes', label: 'Bytes' },
            ]}
          />
        }
      />
      {partitions.length === 0 ? (
        <EmptyState title="No data files" description="Partitions appear with the first commit." />
      ) : (
        <ul className="max-h-[360px] space-y-1.5 overflow-y-auto px-5 py-4">
          {partitions.map((row) => (
            <PartitionBar key={row.key ?? 'none'} row={row} measure={measure} peak={peak} />
          ))}
        </ul>
      )}
    </Panel>
  );
}

function PartitionBar({ row, measure, peak }: { row: TablePartitionRow; measure: 'files' | 'bytes'; peak: number }): ReactNode {
  const value = measure === 'files' ? row.files : row.bytes;
  const smallShare = row.files > 0 ? row.smallFiles / row.files : 0;
  return (
    <li className="grid grid-cols-[110px_minmax(0,1fr)_120px] items-center gap-3 text-[12px]">
      <span className="numeric truncate text-[var(--color-ink-muted)]">
        {row.key === null ? 'unpartitioned' : row.key === '__null__' ? 'no timestamp' : row.key}
      </span>
      <span
        className="relative h-3 overflow-hidden rounded-sm bg-[var(--color-raised)]"
        title={`${formatExact(row.files)} files (${formatExact(row.smallFiles)} small) · ${formatBytes(row.bytes)} · ${formatExact(row.records)} rows`}
      >
        <span className="absolute inset-y-0 left-0 rounded-sm bg-[var(--color-accent)]" style={{ width: `${Math.max(2, (value / peak) * 100)}%`, opacity: 0.35 + 0.65 * (1 - smallShare) }} />
      </span>
      <span className="numeric flex items-center justify-end gap-1.5 text-right">
        {measure === 'files' ? formatCount(row.files) : formatBytes(row.bytes)}
        {row.compactionDue ? (
          <Badge tone="paused" title="Enough small files that the next compaction pass takes this partition.">
            compact
          </Badge>
        ) : null}
      </span>
    </li>
  );
}

function PropertiesPanel({ record, metadata }: { record: CpTable | null; metadata: TableMetadataView | null }): ReactNode {
  const [showAll, setShowAll] = useState(false);
  const properties = Object.entries(metadata?.properties ?? {});
  const shown = showAll ? properties : properties.filter(([key]) => !key.startsWith('commit.') && !key.startsWith('write.metadata'));
  return (
    <Panel className="overflow-hidden">
      <PanelHeader title="Properties" description="How the table is defined, as engines read it." />
      <dl className="grid grid-cols-2 gap-x-4 gap-y-4 p-5">
        <KeyValue label="Identifier" mono className="col-span-2">
          {record?.identifier ?? '—'}
        </KeyValue>
        <KeyValue label="Location" mono className="col-span-2">
          <span className="block break-all whitespace-normal">{metadata?.location ?? record?.location ?? '—'}</span>
        </KeyValue>
        <KeyValue label="Metadata file" mono className="col-span-2">
          <span className="block break-all whitespace-normal">{metadata?.metadataLocation ?? record?.metadata_location ?? '—'}</span>
        </KeyValue>
        <KeyValue label="Format">{metadata ? `Iceberg v${metadata.formatVersion}` : '—'}</KeyValue>
        <KeyValue label="Schema">{metadata ? `schema ${metadata.currentSchemaId}` : record?.schema_id != null ? `schema ${record.schema_id}` : '—'}</KeyValue>
        <KeyValue label="Partition spec" mono>
          {metadata?.partitionSpec ?? '—'}
        </KeyValue>
        <KeyValue label="Sort order" mono>
          {metadata?.sortOrder ?? '—'}
        </KeyValue>
        <KeyValue label="Current snapshot" mono>
          <span title={metadata?.currentSnapshotId ?? undefined}>{metadata?.currentSnapshotId ? shortSnapshotId(metadata.currentSnapshotId) : '—'}</span>
        </KeyValue>
        <KeyValue label="Table UUID" mono>
          {metadata?.tableUuid ?? record?.table_uuid ?? '—'}
        </KeyValue>
      </dl>
      {properties.length > 0 ? (
        <div className="border-t border-[var(--color-line)]">
          <ul className="divide-y divide-[var(--color-line)]">
            {shown.map(([key, value]) => (
              <li key={key} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)] gap-3 px-5 py-1.5 text-[12px]">
                <span className="numeric truncate text-[var(--color-ink-muted)]" title={key}>
                  {key}
                </span>
                <span className="numeric truncate" title={value}>
                  {value}
                </span>
              </li>
            ))}
          </ul>
          {shown.length < properties.length || showAll ? (
            <button
              type="button"
              onClick={() => setShowAll((current) => !current)}
              className="w-full border-t border-[var(--color-line)] py-2 text-[12px] text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]"
            >
              {showAll ? 'Hide commit and metadata settings' : `Show all ${properties.length} properties`}
            </button>
          ) : null}
        </div>
      ) : null}
    </Panel>
  );
}

type Engine = 'duckdb' | 'duckdb-path' | 'pyiceberg' | 'spark' | 'trino';

const ENGINES: Array<{ value: Engine; label: string }> = [
  { value: 'duckdb', label: 'DuckDB' },
  { value: 'pyiceberg', label: 'PyIceberg' },
  { value: 'spark', label: 'Spark' },
  { value: 'trino', label: 'Trino' },
  { value: 'duckdb-path', label: 'By path' },
];

/**
 * How each engine reads the table through a catalog, which attaches as `lake` (handbook 20).
 *
 * PyIceberg and Spark read nested namespaces, so they attach the federated catalog and name the
 * table in full, `lake.<domain>.<dataset>.<table>`: one catalog for every domain. DuckDB and Trino
 * read one level of namespaces, so they attach the domain's own catalog, one per domain, and name
 * the table `lake.<dataset>.<table>`.
 */
export function snippet(engine: Engine, connection: CatalogConnection, namespace: string, table: string, location: string): string {
  const federated = `${connection.domain}.${namespace}`;
  switch (engine) {
    case 'duckdb':
      return [
        'INSTALL iceberg; LOAD iceberg;',
        "CREATE SECRET midstream (TYPE ICEBERG, TOKEN '<access token>');",
        '-- DuckDB reads the data files itself, with a GCS HMAC key that can read the bucket:',
        "CREATE SECRET gcs (TYPE GCS, KEY_ID '<HMAC key id>', SECRET '<HMAC secret>');",
        `-- One level of namespaces: this domain's own catalog; attach another domain's beside it.`,
        `ATTACH 'warehouse' AS lake (TYPE ICEBERG, SECRET midstream, ENDPOINT '${connection.uri}');`,
        '',
        `SELECT * FROM lake.${namespace}.${table} LIMIT 10;`,
      ].join('\n');
    case 'pyiceberg':
      return [
        'from pyiceberg.catalog.rest import RestCatalog',
        '',
        '# Every domain in one catalog. Data files are read with your Google credentials (ADC).',
        `catalog = RestCatalog("lake", uri="${connection.federatedUri}", token="<access token>")`,
        `table = catalog.load_table("${federated}.${table}")`,
        'rows = table.scan(row_filter="_kafka_timestamp >= \'2026-01-01T00:00:00+00:00\'").to_arrow()',
      ].join('\n');
    case 'spark':
      return [
        '# Every domain in one catalog: tables are lake.<domain>.<dataset>.<table>.',
        'spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.lake.type=rest',
        `spark.sql.catalog.lake.uri=${connection.federatedUri}`,
        'spark.sql.catalog.lake.token=<access token>',
        'spark.sql.catalog.lake.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO',
        '',
        `SELECT * FROM lake.${federated}.${table} LIMIT 10;`,
      ].join('\n');
    case 'trino':
      return [
        `# etc/catalog/lake.properties: this domain's own catalog, one level of namespaces.`,
        'connector.name=iceberg',
        'iceberg.catalog.type=rest',
        `iceberg.rest-catalog.uri=${connection.uri}`,
        'iceberg.rest-catalog.security=OAUTH2',
        'iceberg.rest-catalog.oauth2.token=<access token>',
        'fs.native-gcs.enabled=true',
        '',
        `SELECT * FROM lake.${namespace}."${table}" LIMIT 10;`,
      ].join('\n');
    case 'duckdb-path':
      return [
        '-- No catalog: read the newest snapshot straight from object storage.',
        'INSTALL iceberg; LOAD iceberg;',
        "CREATE SECRET gcs (TYPE GCS, KEY_ID '<HMAC key id>', SECRET '<HMAC secret>');",
        '',
        `SELECT count(*) FROM iceberg_scan('${location}');`,
      ].join('\n');
  }
}

function ConnectPanel({
  connection,
  record,
  metadata,
  table,
}: {
  connection: CatalogConnection | null;
  record: CpTable | null;
  metadata: TableMetadataView | null;
  /** The table, in its dataset: its namespace on the catalog. */
  table: CpTableSpec;
}): ReactNode {
  const { can } = useAuth();
  const [engine, setEngine] = useState<Engine>('duckdb');
  const [copied, setCopied] = useState(false);
  // The catalog lists a dataset's tables to those who may read them through it.
  if (!can('catalog.read', table.dataset_id) || !connection) return null;
  const location = metadata?.location ?? record?.location ?? '<table location>';
  const text = snippet(engine, connection, table.dataset, table.name, location);
  return (
    <Panel className="overflow-hidden">
      <PanelHeader
        title="Connect"
        description={
          <>
            Read <code className="numeric">lake.{table.dataset}.{table.name}</code> from another engine through the Iceberg REST
            catalog at <code className="numeric">{connection.uri}</code>, and join it with any table you may read, in any dataset.
            Make an access token under{' '}
            <Link to="/settings/account" className="text-[var(--color-accent)] hover:underline">
              Your account
            </Link>
            .
          </>
        }
        actions={<SegmentedControl value={engine} onChange={setEngine} options={ENGINES} />}
      />
      {!connection.enabled ? (
        <p className="border-b border-[var(--color-line)] px-5 py-2.5 text-[12px] text-[var(--color-phase-paused)]">
          The REST catalog is turned off (Settings › Iceberg catalog); engines can still read the table by path.
        </p>
      ) : null}
      {!connection.configured ? (
        <p className="border-b border-[var(--color-line)] px-5 py-2.5 text-[12px] text-[var(--color-ink-muted)]">
          This is the address the console reaches the control plane at. Engines outside the cluster need the one set in
          CONTROL_PLANE_CATALOG_BASE_URL.
        </p>
      ) : null}
      <div className="relative">
        <pre className="numeric overflow-x-auto bg-[var(--color-raised)] px-5 py-4 text-[12px] leading-relaxed">{text}</pre>
        <Button
          size="sm"
          variant="ghost"
          className="absolute top-2 right-2"
          onClick={() => {
            void navigator.clipboard.writeText(text);
            setCopied(true);
            setTimeout(() => setCopied(false), 1_500);
          }}
        >
          {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
          {copied ? 'Copied' : 'Copy'}
        </Button>
      </div>
    </Panel>
  );
}
