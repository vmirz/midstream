import { Database, Lock, Table2, TriangleAlert } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';

import { TABLE_PARAM } from '../tables/selectedTable';
import { RelativeTime } from '@/components/Inline';
import { Badge, Panel, PanelHeader } from '@/components/ui/primitives';
import { useAuth } from '@/lib/auth';
import { formatBytes, formatCount, formatExact } from '@/lib/format';
import type { CpDetail, CpLiveTable, CpTableSpec, CpTableSummary, TableState } from '@shared/types';
import { useDomainPath } from '@/lib/domains';

/** A row of the panel: a table, its record once created, and its place in the materialization. */
interface TableRow {
  spec: CpTableSpec;
  record: CpTableSummary | null;
  state: TableState;
}

/** The written tables in the order of their topics, the kept ones, then any being dropped or dropped. */
function tableRows(detail: CpDetail): TableRow[] {
  const records = new Map(detail.tables.map((record) => [record.name, record]));
  const leaving = detail.tables.filter((record) => record.state === 'dropping' || record.state === 'dropped');
  return [
    ...detail.materialization.tables.map((spec) => ({ spec, record: records.get(spec.name) ?? null, state: 'active' as const })),
    ...(detail.materialization.retired_tables ?? []).map((spec) => ({ spec, record: records.get(spec.name) ?? null, state: 'retired' as const })),
    ...leaving.map((record) => ({
      spec: { name: record.name, source_topic: record.source_topic, dataset: record.dataset, dataset_id: record.dataset_id },
      record,
      state: record.state as TableState,
    })),
  ];
}

/**
 * Every table of the materialization: one per source topic, each in its dataset, with what it holds,
 * how far its topic is behind, and anything wrong with it; then the tables kept read-only after their
 * topics were removed, and any being dropped.
 *
 * Totals come from the control plane's table records, which every commit updates; lag from its
 * last measurement of each topic. A table's name opens it in the Data tab, for someone who may query
 * its dataset; its icon, in the Table tab; its dataset, the dataset's page. A lock marks a table
 * whose dataset the person may not read.
 */
export function TablesPanel({ detail }: { detail: CpDetail }): ReactNode {
  const domainPath = useDomainPath();
  const { can } = useAuth();
  const canSeeDatasets = can('datasets.view');
  const materializationId = detail.materialization.materialization_id;
  const rows = tableRows(detail);
  const kept = detail.materialization.retired_tables?.length ?? 0;
  const live = detail.status.runtime_progress?.tables ?? {};
  const lag = detail.status.consumer_lag?.topics ?? {};
  const first = detail.materialization.tables[0]?.name;
  const linkTo = (tab: 'data' | 'table', table: string): string =>
    domainPath(`/materializations/${encodeURIComponent(materializationId)}/${tab}${table === first ? '' : `?${TABLE_PARAM}=${encodeURIComponent(table)}`}`);

  return (
    <Panel className="overflow-hidden">
      <PanelHeader
        title={`Tables (${detail.materialization.tables.length}${kept ? ` · ${kept} kept` : ''})`}
        description={
          <>
            One per source topic, each in its dataset: SQL reads each as <code className="numeric">&lt;dataset&gt;.&lt;table&gt;</code>,
            and so does any engine on the catalog. One runtime writes them all
            {kept ? '; a kept table is no longer written, and stays readable' : ''}.
          </>
        }
      />
      <div className="overflow-x-auto">
        <table className="w-full min-w-[760px] text-[12px]">
          <thead>
            <tr className="border-b border-[var(--color-line)] bg-[var(--color-raised)] text-left text-[11px] tracking-wide text-[var(--color-ink-muted)] uppercase">
              <th className="px-5 py-2 font-medium">Table · topic</th>
              <th className="px-2 py-2 text-right font-medium">Rows</th>
              <th className="px-2 py-2 text-right font-medium">Size</th>
              <th className="px-2 py-2 text-right font-medium">Files</th>
              <th className="px-2 py-2 text-right font-medium" title="Records on the topic the runtime has not yet committed">
                Lag
              </th>
              <th className="px-2 py-2 font-medium">Schema</th>
              <th className="px-2 py-2 font-medium">Last commit</th>
              <th className="px-5 py-2 font-medium">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--color-line)]">
            {rows.map(({ spec, record, state }) => {
              const written = state === 'active';
              const readable = written || state === 'retired';
              const block = written ? (live[spec.name] ?? null) : null;
              const topicLag = written ? lag[spec.source_topic] : undefined;
              const locked = !can('dataset.view_schema', spec.dataset_id);
              const queryable = can('data.query', spec.dataset_id);
              const qualified = `${spec.dataset}.${spec.name}`;
              return (
                <tr key={spec.name} className={state === 'dropping' || state === 'dropped' ? 'align-top opacity-60' : 'align-top'}>
                  <td className="px-5 py-2">
                    <span className="flex items-center gap-1.5">
                      {canSeeDatasets ? (
                        <Link
                          to={domainPath(`/datasets/${encodeURIComponent(spec.dataset)}`)}
                          className="numeric text-[var(--color-ink-muted)] hover:text-[var(--color-accent)] hover:underline"
                          title={`Dataset ${spec.dataset}`}
                        >
                          {spec.dataset}.
                        </Link>
                      ) : (
                        <span className="numeric text-[var(--color-ink-muted)]">{spec.dataset}.</span>
                      )}
                      {!readable ? (
                        <span className="numeric -ml-1.5 font-medium line-through decoration-[var(--color-ink-faint)]">{spec.name}</span>
                      ) : queryable ? (
                        <Link
                          to={linkTo('data', spec.name)}
                          className="numeric -ml-1.5 font-medium text-[var(--color-ink)] hover:text-[var(--color-accent)] hover:underline"
                          title={`Query ${qualified} in the Data tab`}
                        >
                          {spec.name}
                        </Link>
                      ) : (
                        <span className="numeric -ml-1.5 font-medium">{spec.name}</span>
                      )}
                      {locked ? (
                        <span title={`Dataset ${spec.dataset} is locked to you: its schemas and data need a role on it`}>
                          <Lock className="size-3 text-[var(--color-ink-faint)]" aria-label="locked" />
                        </span>
                      ) : null}
                      {readable ? (
                        <Link to={linkTo('table', spec.name)} className="text-[var(--color-ink-faint)] hover:text-[var(--color-ink)]" title={`${qualified} in the Table tab`}>
                          <Table2 className="size-3.5" />
                        </Link>
                      ) : null}
                    </span>
                    <span className="numeric block break-all text-[11px] text-[var(--color-ink-muted)]">{spec.source_topic}</span>
                  </td>
                  <td className="numeric px-2 py-2 text-right">{record ? formatExact(record.totals.records ?? 0) : '—'}</td>
                  <td className="numeric px-2 py-2 text-right">{record ? formatBytes(record.totals.bytes ?? 0) : '—'}</td>
                  <td className="numeric px-2 py-2 text-right text-[var(--color-ink-muted)]">{record ? formatExact(record.totals.files ?? 0) : '—'}</td>
                  <td className="numeric px-2 py-2 text-right" title={topicLag?.error ?? (written ? undefined : 'Not read any more')}>
                    {topicLag?.error ? <span className="text-[var(--color-phase-paused)]">?</span> : written ? formatCount(topicLag?.total_lag ?? null) : '—'}
                  </td>
                  <td className="numeric px-2 py-2 text-[var(--color-ink-muted)]">{record?.schema_id != null ? `schema ${record.schema_id}` : '—'}</td>
                  <td className="px-2 py-2 whitespace-nowrap text-[var(--color-ink-muted)]">
                    <RelativeTime value={record?.last_commit_at ?? block?.last_commit_at ?? null} />
                  </td>
                  <td className="px-5 py-2">
                    <TableStatus record={record} block={block} state={state} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function TableStatus({ record, block, state }: { record: CpTableSummary | null; block: CpLiveTable | null; state: TableState }): ReactNode {
  if (state === 'dropping' && !record?.last_error) {
    return (
      <Badge tone="pending" title="Its files and records are being deleted. It left its dataset, and freed its name there, when its drop was asked for.">
        dropping
      </Badge>
    );
  }
  if (state === 'dropped') {
    return (
      <Badge title={`Dropped with object deletion off: its files stay at ${record?.location ?? 'its location'}, so this materialization does not use its name again; its dataset may.`}>
        dropped · files kept
      </Badge>
    );
  }
  if (state === 'retired' && !record?.last_error) {
    return (
      <Badge title="Its topic was removed: the table is no longer written, and stays readable. Adding the topic back writes it again.">
        kept · read-only
      </Badge>
    );
  }
  if (!record) {
    return (
      <Badge tone="pending" title="The control plane creates each table after it has sampled its topic.">
        <Database className="size-3" />
        not created yet
      </Badge>
    );
  }
  if (record.last_error) {
    return (
      <span className="flex min-w-0 items-start gap-1.5 text-[var(--color-phase-failed)]" title={record.last_error.message}>
        <TriangleAlert className="mt-0.5 size-3.5 shrink-0" />
        <span className="line-clamp-2">{record.last_error.message}</span>
      </span>
    );
  }
  const running = record.maintenance_job ?? block?.maintenance ?? null;
  if (running) {
    return (
      <Badge tone="pending" title={running.partition ? `${running.job.replace(/_/g, ' ')} · ${running.partition}` : undefined}>
        {running.job === 'compaction' ? 'compacting' : running.job === 'reprotect' ? 'masking rows' : running.job.replace(/_/g, ' ')}
      </Badge>
    );
  }
  if ((block?.pending_files ?? 0) > 0) {
    return (
      <span className="text-[var(--color-ink-muted)]">
        {formatExact(block?.pending_files ?? 0)} {block?.pending_files === 1 ? 'file' : 'files'} awaiting commit
      </span>
    );
  }
  return <Badge tone="running">ok</Badge>;
}
